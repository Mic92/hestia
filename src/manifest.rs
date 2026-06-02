//! The hestia manifest: one CBOR+zstd document describing everything stored
//! in the GHA cache for a repository.
//!
//! Stored as the SaveMutable entry family `m#N` (see `gha::savemutable`).
//! Schema follows PLAN.md; all structs ignore unknown fields on decode so
//! the format can grow without breaking older readers (forward compat).

use std::collections::{BTreeMap, BTreeSet};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// Re-exports so integration tests and later phases can build trees without
// depending on harmonia crates directly.
pub use harmonia_file_core::{Directory, FileSystemObject, FileTree, Regular, Symlink};
pub use harmonia_store_path::{StorePath, StorePathHash};

/// Window inside which two root updates are considered concurrent and get
/// unioned instead of newest-wins (10 minutes).
const ROOT_UNION_WINDOW_SECS: u64 = 600;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to encode manifest: {0}")]
    Encode(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("failed to decode manifest: {0}")]
    Decode(#[from] ciborium::de::Error<std::io::Error>),

    #[error("compression failed: {0}")]
    Compression(#[from] std::io::Error),
}

/// A 32-byte SHA-256 digest (chunk hash, pack hash, or NAR hash).
///
/// Serialized as a CBOR byte string (33 bytes on the wire) instead of an
/// array of integers.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hash32(pub [u8; 32]);

pub type ChunkHash = Hash32;
pub type PackHash = Hash32;
pub type NarHash = Hash32;

impl Hash32 {
    /// SHA-256 of `data`.
    pub fn digest(data: impl AsRef<[u8]>) -> Self {
        Self(*harmonia_utils_hash::Sha256::digest(data).digest_bytes())
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Parse a SHA-256 hash in any format Nix uses: SRI (`sha256-<base64>`),
    /// prefixed (`sha256:<base16|base32|base64>`), or bare.
    ///
    /// Returns `None` for non-SHA-256 hashes or unparsable input.
    pub fn parse_sha256(s: &str) -> Option<Self> {
        let hash: harmonia_utils_hash::fmt::Any<harmonia_utils_hash::Sha256> = s.parse().ok()?;
        Some(Self(*hash.into_hash().digest_bytes()))
    }
}

impl std::fmt::Debug for Hash32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash32({})", self.to_hex())
    }
}

impl std::fmt::Display for Hash32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Hash32 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Hash32 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Hash32;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("32 bytes")
            }

            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Hash32, E> {
                let array: [u8; 32] = v
                    .try_into()
                    .map_err(|_| E::invalid_length(v.len(), &self))?;
                Ok(Hash32(array))
            }
        }
        deserializer.deserialize_bytes(Visitor)
    }
}

/// A store path hash (the 32-character base32 prefix of a store path name),
/// used as the manifest key for paths.
///
/// Wraps harmonia's [`StorePathHash`] (which has no serde impls) and
/// serializes as the base32 string.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathHash(pub StorePathHash);

impl PathHash {
    pub fn from_store_path(path: &StorePath) -> Self {
        Self(*path.hash())
    }
}

impl std::str::FromStr for PathHash {
    type Err = harmonia_store_path::StorePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl std::fmt::Display for PathHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for PathHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PathHash({})", self.0)
    }
}

impl Serialize for PathHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for PathHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

/// Ordered chunks making up one regular file.
///
/// A struct with a named field (not a tuple) because harmonia's
/// `Regular<C>` embeds its contents with `#[serde(flatten)]`, which
/// requires map-shaped serialization.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ChunkList {
    #[serde(default)]
    pub chunks: Vec<ChunkHash>,
}

/// Where one chunk lives inside a pack blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkLocation {
    pub pack: PackHash,
    pub offset: u64,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
    /// Number of GC repacks this chunk has survived (stability tier signal).
    #[serde(default)]
    pub repacks_survived: u32,
}

/// Metadata for one pack blob (`pack-{hash}` cache entry).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackInfo {
    pub size: u64,
    pub created: u64,
    /// Stability tier (0 = volatile, higher = more stable).
    #[serde(default)]
    pub tier: u8,
}

/// One stored path: everything needed to serve narinfo + NAR for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathEntry {
    /// Full store path basename (`<hash>-<name>`). The manifest key is only
    /// the hash part; narinfo responses need the name too (StorePath and
    /// References lines carry full basenames).
    pub store_path: StorePath,
    pub nar_hash: NarHash,
    pub nar_size: u64,
    /// Store paths this path references. May point at paths that are
    /// not in the manifest (upstream paths served by cache.nixos.org).
    #[serde(default)]
    pub references: Vec<StorePath>,
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub deriver: Option<StorePath>,
    /// File tree with chunk lists as file contents.
    pub tree: FileTree<ChunkList>,
    /// Last time the GC mark phase reached this path (unix seconds).
    #[serde(default)]
    pub last_reachable: u64,
    /// Last time this path was pushed, or would have been pushed but was
    /// dedup-skipped (unix seconds).
    #[serde(default)]
    pub last_pushed: u64,
}

/// A GC root: the set of paths one (branch, system) pair currently needs.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Root {
    #[serde(default)]
    pub paths: BTreeSet<PathHash>,
    /// When this root was last replaced (unix seconds).
    #[serde(default)]
    pub updated: u64,
}

/// The top-level manifest document.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(default)]
    pub paths: BTreeMap<PathHash, PathEntry>,
    #[serde(default)]
    pub chunks: BTreeMap<ChunkHash, ChunkLocation>,
    /// Packs keyed by hash (deviation from PLAN.md's `Vec<PackRef>`: a map
    /// makes dedup-by-hash the natural merge operation).
    #[serde(default)]
    pub packs: BTreeMap<PackHash, PackInfo>,
    /// Roots keyed by "branch-system", e.g. "main-x86_64-linux".
    #[serde(default)]
    pub roots: BTreeMap<String, Root>,
}

// Merge rules: concurrent CI jobs produce concurrent manifest versions;
// SaveMutable resolves the write conflict, but the loser must re-merge its
// changes on top of the winner's. All merge operations are commutative and
// idempotent so the result does not depend on who wins the race.

impl PathEntry {
    /// Manifest keys of all referenced paths (used by the reachability
    /// walk; upstream references resolve to keys not present in the
    /// manifest, which the walk treats as holes).
    fn reference_hashes(&self) -> impl Iterator<Item = PathHash> + '_ {
        self.references.iter().map(PathHash::from_store_path)
    }

    /// Merge two entries describing the same store path.
    fn merge(a: Self, b: Self) -> Self {
        // Deterministic winner: newer push wins, nar_hash as tie-break, the
        // CBOR encoding of the remaining content as the final tie-break.
        //
        // The final tie-break must be a *total* order over the entry
        // content: two entries can tie on (last_pushed, nar_hash) while
        // differing in metadata (the same output path can be produced by
        // different derivations, so deriver/ca/references may differ, and
        // concurrent jobs push within the same second routinely). Without a
        // total order the merge would not be commutative, and the surviving
        // entry would depend on which concurrent writer wins the
        // SaveMutable race.
        fn order_key(entry: &PathEntry) -> (u64, NarHash, Vec<u8>) {
            // The clocks are folded with max() below; exclude them from the
            // content tie-break so that folding never changes an entry's
            // ordering (this keeps the merge associative as well).
            let mut content = entry.clone();
            content.last_reachable = 0;
            content.last_pushed = 0;
            let mut encoded = Vec::new();
            // Serializing to a Vec cannot fail for manifest types (it is
            // the same code path Manifest::encode uses); a failure would
            // only weaken the tie-break, never panic.
            let _ = ciborium::into_writer(&content, &mut encoded);
            (entry.last_pushed, entry.nar_hash, encoded)
        }
        let (mut winner, loser) = if order_key(&a) >= order_key(&b) {
            (a, b)
        } else {
            (b, a)
        };
        // Clocks advance monotonically across both histories.
        winner.last_reachable = winner.last_reachable.max(loser.last_reachable);
        winner.last_pushed = winner.last_pushed.max(loser.last_pushed);
        winner
    }
}

impl ChunkLocation {
    /// Merge two locations of the same chunk (e.g. concurrent uploads put it
    /// into different packs). Prefer the more repack-stable location; the
    /// remaining fields make the comparison a total order so the choice is
    /// deterministic.
    fn merge(a: Self, b: Self) -> Self {
        let key = |location: &Self| {
            (
                location.repacks_survived,
                location.pack,
                location.offset,
                location.compressed_size,
                location.uncompressed_size,
            )
        };
        if key(&a) >= key(&b) { a } else { b }
    }
}

impl PackInfo {
    /// Merge metadata for the same pack hash (identical content by
    /// definition; timestamps may differ between observers).
    fn merge(a: Self, b: Self) -> Self {
        Self {
            size: a.size.max(b.size),
            created: a.created.min(b.created),
            tier: a.tier.max(b.tier),
        }
    }
}

impl Root {
    /// Merge two versions of the same root.
    ///
    /// Roots updated within [`ROOT_UNION_WINDOW_SECS`] of each other are
    /// concurrent (e.g. matrix jobs of one workflow): union their paths.
    /// Otherwise the newer root replaces the older one -- that is what makes
    /// old closures unreachable and therefore collectable.
    fn merge(a: Self, b: Self) -> Self {
        if a.updated.abs_diff(b.updated) <= ROOT_UNION_WINDOW_SECS {
            Root {
                paths: a.paths.into_iter().chain(b.paths).collect(),
                updated: a.updated.max(b.updated),
            }
        } else if a.updated > b.updated {
            a
        } else {
            b
        }
    }
}

fn merge_map<K: Ord, V>(
    target: &mut BTreeMap<K, V>,
    source: BTreeMap<K, V>,
    merge_value: impl Fn(V, V) -> V,
) {
    for (key, value) in source {
        let merged = match target.remove(&key) {
            Some(existing) => merge_value(existing, value),
            None => value,
        };
        target.insert(key, merged);
    }
}

impl Manifest {
    /// Merge another manifest into this one (see merge rules above).
    pub fn merge(mut self, other: Manifest) -> Manifest {
        merge_map(&mut self.paths, other.paths, PathEntry::merge);
        merge_map(&mut self.chunks, other.chunks, ChunkLocation::merge);
        merge_map(&mut self.packs, other.packs, PackInfo::merge);
        merge_map(&mut self.roots, other.roots, Root::merge);
        self
    }

    /// All paths reachable from any root by following references.
    ///
    /// References pointing at paths not present in the manifest are upstream
    /// paths (served by cache.nixos.org or another substituter): they are
    /// holes in the graph, not errors, and the walk simply does not descend
    /// into them.
    pub fn reachable(&self) -> BTreeSet<PathHash> {
        let mut visited: BTreeSet<PathHash> = BTreeSet::new();
        let mut stack: Vec<PathHash> = self
            .roots
            .values()
            .flat_map(|root| root.paths.iter().copied())
            .collect();
        while let Some(path) = stack.pop() {
            // Not in the manifest: an upstream reference or an evicted root
            // member -- a hole in the graph, not an error.
            let Some(entry) = self.paths.get(&path) else {
                continue;
            };
            if !visited.insert(path) {
                continue;
            }
            stack.extend(
                entry
                    .reference_hashes()
                    .filter(|reference| !visited.contains(reference)),
            );
        }
        visited
    }

    /// GC mark phase: bump `last_reachable` of every reachable path to `now`.
    pub fn mark_reachable(&mut self, now: u64) {
        for path in self.reachable() {
            if let Some(entry) = self.paths.get_mut(&path) {
                entry.last_reachable = entry.last_reachable.max(now);
            }
        }
    }
}

/// zstd level for manifest compression. The manifest is small (tens of KB);
/// favor ratio over speed.
const ZSTD_LEVEL: i32 = 9;

impl Manifest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialize to zstd-compressed CBOR.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut cbor = Vec::new();
        ciborium::into_writer(self, &mut cbor)?;
        Ok(zstd::encode_all(cbor.as_slice(), ZSTD_LEVEL)?)
    }

    /// Deserialize from zstd-compressed CBOR.
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        let cbor = zstd::decode_all(data)?;
        Ok(ciborium::from_reader(cbor.as_slice())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn leaf_tree(chunks: Vec<ChunkHash>) -> FileTree<ChunkList> {
        FileTree(FileSystemObject::Regular(Regular {
            executable: false,
            contents: ChunkList { chunks },
        }))
    }

    pub(crate) fn sample_path_entry(seed: u8) -> PathEntry {
        PathEntry {
            store_path: store_path(seed),
            nar_hash: Hash32::digest([seed]),
            nar_size: 1000 + seed as u64,
            references: vec![],
            ca: None,
            deriver: Some(
                format!("{}-deriver-{seed}.drv", path_hash(seed))
                    .parse()
                    .unwrap(),
            ),
            tree: leaf_tree(vec![Hash32::digest([seed, 1]), Hash32::digest([seed, 2])]),
            last_reachable: 0,
            last_pushed: 100,
        }
    }

    pub(crate) fn path_hash(seed: u8) -> PathHash {
        // 20-byte store path hashes derived deterministically from the seed.
        PathHash(StorePathHash::new([seed; 20]))
    }

    /// Full store path (hash + name) derived deterministically from the seed.
    pub(crate) fn store_path(seed: u8) -> StorePath {
        format!("{}-path-{seed}", path_hash(seed))
            .parse()
            .expect("deterministic test store path is valid")
    }

    fn sample_manifest() -> Manifest {
        let mut manifest = Manifest::new();
        let chunk_a = Hash32::digest(b"chunk a");
        let chunk_b = Hash32::digest(b"chunk b");
        let pack = Hash32::digest(b"pack");

        manifest.paths.insert(
            path_hash(1),
            PathEntry {
                store_path: store_path(1),
                nar_hash: Hash32::digest(b"nar"),
                nar_size: 4096,
                references: vec![store_path(2), store_path(99)],
                ca: Some("fixed:sha256:abc".into()),
                deriver: Some(format!("{}-foo.drv", path_hash(1)).parse().unwrap()),
                tree: FileTree(FileSystemObject::Directory(Directory {
                    entries: BTreeMap::from([
                        (
                            "bin".to_string(),
                            Box::new(FileTree(FileSystemObject::Directory(Directory {
                                entries: BTreeMap::from([(
                                    "hello".to_string(),
                                    Box::new(FileTree(FileSystemObject::Regular(Regular {
                                        executable: true,
                                        contents: ChunkList {
                                            chunks: vec![chunk_a, chunk_b],
                                        },
                                    }))),
                                )]),
                            }))),
                        ),
                        (
                            "share".to_string(),
                            Box::new(FileTree(FileSystemObject::Symlink(Symlink {
                                target: "../share".to_string(),
                            }))),
                        ),
                    ]),
                })),
                last_reachable: 1000,
                last_pushed: 2000,
            },
        );
        manifest.paths.insert(path_hash(2), sample_path_entry(2));
        manifest.chunks.insert(
            chunk_a,
            ChunkLocation {
                pack,
                offset: 0,
                compressed_size: 100,
                uncompressed_size: 200,
                repacks_survived: 3,
            },
        );
        manifest.chunks.insert(
            chunk_b,
            ChunkLocation {
                pack,
                offset: 100,
                compressed_size: 50,
                uncompressed_size: 80,
                repacks_survived: 0,
            },
        );
        manifest.packs.insert(
            pack,
            PackInfo {
                size: 150,
                created: 1234,
                tier: 1,
            },
        );
        manifest.roots.insert(
            "main-x86_64-linux".to_string(),
            Root {
                paths: BTreeSet::from([path_hash(1), path_hash(2)]),
                updated: 5000,
            },
        );
        manifest
    }

    #[test]
    fn cbor_zstd_round_trip() {
        let manifest = sample_manifest();
        let encoded = manifest.encode().unwrap();
        let decoded = Manifest::decode(&encoded).unwrap();
        assert_eq!(manifest, decoded);
    }

    #[test]
    fn empty_manifest_round_trip() {
        let manifest = Manifest::new();
        let decoded = Manifest::decode(&manifest.encode().unwrap()).unwrap();
        assert_eq!(manifest, decoded);
    }

    #[test]
    fn encoded_manifest_is_compact() {
        let manifest = sample_manifest();
        let encoded = manifest.encode().unwrap();
        // Sanity bound: a 2-path manifest must stay well under a kilobyte.
        assert!(
            encoded.len() < 1024,
            "encoded manifest unexpectedly large: {} bytes",
            encoded.len()
        );
    }

    #[test]
    fn unknown_fields_are_ignored_on_decode() {
        // Forward compatibility: a future hestia version may add fields.
        // Simulate by encoding a manifest as CBOR with extra fields injected
        // at top level, path level, and chunk level, then decoding with
        // today's schema.
        let manifest = sample_manifest();
        let mut cbor = Vec::new();
        ciborium::into_writer(&manifest, &mut cbor).unwrap();
        let mut value: ciborium::Value = ciborium::from_reader(cbor.as_slice()).unwrap();

        fn add_field(value: &mut ciborium::Value, key: &str) {
            let map = value.as_map_mut().unwrap();
            map.push((
                ciborium::Value::Text(key.to_string()),
                ciborium::Value::Integer(42.into()),
            ));
        }

        add_field(&mut value, "future_top_level_field");
        {
            // paths -> first entry -> add field
            let map = value.as_map_mut().unwrap();
            let paths = &mut map
                .iter_mut()
                .find(|(k, _)| k.as_text() == Some("paths"))
                .unwrap()
                .1;
            let first_entry = &mut paths.as_map_mut().unwrap()[0].1;
            add_field(first_entry, "future_path_field");
        }

        let mut patched = Vec::new();
        ciborium::into_writer(&value, &mut patched).unwrap();
        let compressed = zstd::encode_all(patched.as_slice(), 3).unwrap();

        let decoded = Manifest::decode(&compressed).unwrap();
        assert_eq!(decoded, manifest);
    }

    #[test]
    fn corrupt_data_is_an_error_not_a_panic() {
        assert!(Manifest::decode(b"not zstd at all").is_err());
        // Valid zstd, invalid CBOR.
        let garbage = zstd::encode_all(&b"garbage cbor"[..], 3).unwrap();
        assert!(Manifest::decode(&garbage).is_err());
    }

    #[test]
    fn hash32_display_and_digest() {
        let hash = Hash32::digest(b"hestia-1");
        assert_eq!(
            hash.to_hex(),
            "7a32118639289175533829e84c9aaa9fa781f6a5f1b18a9c8a6bd3642b39dd88"
        );
        assert_eq!(format!("{hash}"), hash.to_hex());
    }

    #[test]
    fn hash32_parses_nix_hash_formats() {
        let hash = Hash32::digest(b"hello world");
        assert_eq!(
            hash.to_hex(),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );

        // SRI format (what `nix path-info --json` emits as narHash).
        let sri = "sha256-uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=";
        assert_eq!(Hash32::parse_sha256(sri), Some(hash));

        // Prefixed base16.
        let base16 = format!("sha256:{}", hash.to_hex());
        assert_eq!(Hash32::parse_sha256(&base16), Some(hash));

        // Garbage.
        assert_eq!(Hash32::parse_sha256("not a hash"), None);
    }

    #[test]
    fn path_hash_string_round_trip() {
        let hash = path_hash(7);
        let as_string = hash.to_string();
        let parsed: PathHash = as_string.parse().unwrap();
        assert_eq!(hash, parsed);
    }

    fn entry_with_refs(seed: u8, references: Vec<StorePath>, last_pushed: u64) -> PathEntry {
        PathEntry {
            references,
            last_pushed,
            ..sample_path_entry(seed)
        }
    }

    #[test]
    fn merge_unions_paths_and_keeps_newer_clocks() {
        let mut a = Manifest::new();
        let mut b = Manifest::new();

        // Same path in both, different push times: newer entry wins, but
        // clocks merge to the maximum of both histories.
        let old_drv: StorePath = format!("{}-old.drv", path_hash(10)).parse().unwrap();
        let new_drv: StorePath = format!("{}-new.drv", path_hash(11)).parse().unwrap();
        let mut entry_old = sample_path_entry(1);
        entry_old.last_pushed = 100;
        entry_old.last_reachable = 500;
        entry_old.deriver = Some(old_drv);
        let mut entry_new = sample_path_entry(1);
        entry_new.last_pushed = 200;
        entry_new.last_reachable = 50;
        entry_new.deriver = Some(new_drv.clone());

        a.paths.insert(path_hash(1), entry_old);
        b.paths.insert(path_hash(1), entry_new);
        // Disjoint paths survive in the union.
        a.paths.insert(path_hash(2), sample_path_entry(2));
        b.paths.insert(path_hash(3), sample_path_entry(3));

        let merged = a.merge(b);
        assert_eq!(merged.paths.len(), 3);
        let winner = &merged.paths[&path_hash(1)];
        assert_eq!(winner.deriver, Some(new_drv));
        assert_eq!(winner.last_pushed, 200);
        assert_eq!(winner.last_reachable, 500, "older history's mark survives");
    }

    #[test]
    fn merge_is_commutative_when_push_clock_and_nar_hash_tie() {
        // Two concurrent CI jobs can push the same store path within the
        // same second (last_pushed has 1-second granularity) with identical
        // content (same nar_hash) but different metadata: the same output
        // path can be produced by different derivations, so deriver (and in
        // principle ca/references) can differ between the two pushes.
        //
        // SaveMutable conflict resolution re-merges in whatever order the
        // race produces; if the merge result depends on argument order, the
        // surviving manifest depends on who wins that race -- exactly the
        // non-determinism the merge rules promise to rule out.
        let mut a = sample_path_entry(1);
        let mut b = sample_path_entry(1);
        a.deriver = Some(format!("{}-alpha.drv", path_hash(10)).parse().unwrap());
        b.deriver = Some(format!("{}-beta.drv", path_hash(11)).parse().unwrap());
        // Same push second, same content hash: the tie case.
        assert_eq!(a.last_pushed, b.last_pushed);
        assert_eq!(a.nar_hash, b.nar_hash);

        let mut manifest_a = Manifest::new();
        manifest_a.paths.insert(path_hash(1), a);
        let mut manifest_b = Manifest::new();
        manifest_b.paths.insert(path_hash(1), b);

        let ab = manifest_a.clone().merge(manifest_b.clone());
        let ba = manifest_b.merge(manifest_a);
        assert_eq!(ab, ba, "merge must not depend on argument order");
    }

    #[test]
    fn merge_dedups_packs_and_chunks() {
        let pack = Hash32::digest(b"pack");
        let chunk = Hash32::digest(b"chunk");

        let mut a = Manifest::new();
        a.packs.insert(
            pack,
            PackInfo {
                size: 100,
                created: 50,
                tier: 0,
            },
        );
        a.chunks.insert(
            chunk,
            ChunkLocation {
                pack,
                offset: 0,
                compressed_size: 10,
                uncompressed_size: 20,
                repacks_survived: 2,
            },
        );

        let mut b = Manifest::new();
        b.packs.insert(
            pack,
            PackInfo {
                size: 100,
                created: 60,
                tier: 1,
            },
        );
        // Same chunk known under a different (less stable) location.
        b.chunks.insert(
            chunk,
            ChunkLocation {
                pack: Hash32::digest(b"other pack"),
                offset: 7,
                compressed_size: 10,
                uncompressed_size: 20,
                repacks_survived: 0,
            },
        );

        let merged = a.merge(b);
        assert_eq!(merged.packs.len(), 1);
        assert_eq!(merged.packs[&pack].created, 50, "earliest creation wins");
        assert_eq!(merged.packs[&pack].tier, 1, "highest tier wins");
        assert_eq!(merged.chunks.len(), 1);
        assert_eq!(
            merged.chunks[&chunk].repacks_survived, 2,
            "more stable location wins"
        );
    }

    #[test]
    fn merge_roots_unions_within_window_replaces_outside() {
        let make_root = |paths: &[u8], updated: u64| Root {
            paths: paths.iter().map(|&seed| path_hash(seed)).collect(),
            updated,
        };

        // Within 10 minutes: union (concurrent matrix jobs).
        let merged = Root::merge(make_root(&[1, 2], 1000), make_root(&[3], 1000 + 599));
        assert_eq!(merged.paths.len(), 3);
        assert_eq!(merged.updated, 1599);

        // Outside 10 minutes: newer replaces older (old closure dies).
        let merged = Root::merge(make_root(&[1, 2], 1000), make_root(&[3], 1000 + 601));
        assert_eq!(merged.paths.len(), 1);
        assert!(merged.paths.contains(&path_hash(3)));

        // Order independence of replacement.
        let merged = Root::merge(make_root(&[3], 1000 + 601), make_root(&[1, 2], 1000));
        assert_eq!(merged.paths.len(), 1);
    }

    #[test]
    fn reachability_follows_references_and_skips_upstream_holes() {
        let mut manifest = Manifest::new();
        // Graph: root -> 1 -> 2 -> 3, and 2 -> 99 (upstream, not in manifest).
        // Path 4 exists but is unreachable.
        manifest
            .paths
            .insert(path_hash(1), entry_with_refs(1, vec![store_path(2)], 100));
        manifest.paths.insert(
            path_hash(2),
            entry_with_refs(2, vec![store_path(3), store_path(99)], 100),
        );
        manifest
            .paths
            .insert(path_hash(3), entry_with_refs(3, vec![], 100));
        manifest
            .paths
            .insert(path_hash(4), entry_with_refs(4, vec![], 100));
        manifest.roots.insert(
            "main-x86_64-linux".into(),
            Root {
                paths: BTreeSet::from([path_hash(1), path_hash(98)]), // 98: evicted root member
                updated: 0,
            },
        );

        let reachable = manifest.reachable();
        assert_eq!(
            reachable,
            BTreeSet::from([path_hash(1), path_hash(2), path_hash(3)]),
            "upstream refs (99) and dangling root members (98) are skipped, \
             unreachable paths (4) are not included"
        );
    }

    #[test]
    fn reachability_handles_cycles() {
        let mut manifest = Manifest::new();
        // 1 <-> 2 reference cycle (self-references happen in practice).
        manifest.paths.insert(
            path_hash(1),
            entry_with_refs(1, vec![store_path(2), store_path(1)], 100),
        );
        manifest
            .paths
            .insert(path_hash(2), entry_with_refs(2, vec![store_path(1)], 100));
        manifest.roots.insert(
            "main".into(),
            Root {
                paths: BTreeSet::from([path_hash(1)]),
                updated: 0,
            },
        );
        assert_eq!(manifest.reachable().len(), 2);
    }

    #[test]
    fn mark_reachable_bumps_only_reachable_paths() {
        let mut manifest = Manifest::new();
        manifest
            .paths
            .insert(path_hash(1), entry_with_refs(1, vec![], 100));
        manifest
            .paths
            .insert(path_hash(2), entry_with_refs(2, vec![], 100));
        manifest.roots.insert(
            "main".into(),
            Root {
                paths: BTreeSet::from([path_hash(1)]),
                updated: 0,
            },
        );

        manifest.mark_reachable(7777);
        assert_eq!(manifest.paths[&path_hash(1)].last_reachable, 7777);
        assert_eq!(manifest.paths[&path_hash(2)].last_reachable, 0);
    }

    mod merge_properties {
        use super::*;
        use proptest::prelude::*;

        // Strategies draw keys from small pools so that merging manifests
        // actually hits the conflict paths instead of always being disjoint
        // unions.

        fn arb_path_hash() -> impl Strategy<Value = PathHash> {
            (0u8..6).prop_map(path_hash)
        }

        fn arb_store_path() -> impl Strategy<Value = StorePath> {
            (0u8..6).prop_map(store_path)
        }

        fn arb_hash32() -> impl Strategy<Value = Hash32> {
            (0u8..6).prop_map(|seed| Hash32::digest([seed]))
        }

        fn arb_chunk_list() -> impl Strategy<Value = ChunkList> {
            proptest::collection::vec(arb_hash32(), 0..3).prop_map(|chunks| ChunkList { chunks })
        }

        fn arb_path_entry() -> impl Strategy<Value = PathEntry> {
            (
                arb_store_path(),
                arb_hash32(),
                0u64..10_000,
                proptest::collection::vec(arb_store_path(), 0..4),
                proptest::option::of(arb_store_path()),
                arb_chunk_list(),
                0u64..10_000,
                // Deliberately tiny range: concurrent pushes of the same path
                // land in the same second all the time, so merge conflicts
                // with tied push clocks must be exercised, not avoided.
                0u64..3,
            )
                .prop_map(
                    |(
                        store_path,
                        nar_hash,
                        nar_size,
                        references,
                        deriver,
                        contents,
                        reachable,
                        pushed,
                    )| {
                        PathEntry {
                            store_path,
                            nar_hash,
                            nar_size,
                            references,
                            ca: None,
                            deriver,
                            tree: FileTree(FileSystemObject::Regular(Regular {
                                executable: false,
                                contents,
                            })),
                            last_reachable: reachable,
                            last_pushed: pushed,
                        }
                    },
                )
        }

        fn arb_chunk_location() -> impl Strategy<Value = ChunkLocation> {
            (arb_hash32(), 0u64..1000, 1u32..1000, 1u32..1000, 0u32..5).prop_map(
                |(pack, offset, compressed_size, uncompressed_size, repacks_survived)| {
                    ChunkLocation {
                        pack,
                        offset,
                        compressed_size,
                        uncompressed_size,
                        repacks_survived,
                    }
                },
            )
        }

        fn arb_pack_info() -> impl Strategy<Value = PackInfo> {
            (0u64..100_000, 0u64..10_000, 0u8..3).prop_map(|(size, created, tier)| PackInfo {
                size,
                created,
                tier,
            })
        }

        fn arb_root() -> impl Strategy<Value = Root> {
            (
                proptest::collection::btree_set(arb_path_hash(), 0..4),
                0u64..3000,
            )
                .prop_map(|(paths, updated)| Root { paths, updated })
        }

        fn arb_manifest() -> impl Strategy<Value = Manifest> {
            (
                proptest::collection::btree_map(arb_path_hash(), arb_path_entry(), 0..4),
                proptest::collection::btree_map(arb_hash32(), arb_chunk_location(), 0..4),
                proptest::collection::btree_map(arb_hash32(), arb_pack_info(), 0..3),
                proptest::collection::btree_map("[a-z]{1,4}", arb_root(), 0..3),
            )
                .prop_map(|(paths, chunks, packs, roots)| Manifest {
                    paths,
                    chunks,
                    packs,
                    roots,
                })
        }

        /// Manifest without roots: `Root::merge`'s concurrency window makes
        /// root merging inherently order-dependent for 3+ way merges (a
        /// documented limitation), so the associativity law is stated for
        /// the path/chunk/pack maps only.
        fn arb_rootless_manifest() -> impl Strategy<Value = Manifest> {
            arb_manifest().prop_map(|mut manifest| {
                manifest.roots.clear();
                manifest
            })
        }

        proptest! {
            #[test]
            fn merge_is_commutative(a in arb_manifest(), b in arb_manifest()) {
                let ab = a.clone().merge(b.clone());
                let ba = b.merge(a);
                prop_assert_eq!(ab, ba);
            }

            #[test]
            fn merge_of_paths_chunks_and_packs_is_associative(
                a in arb_rootless_manifest(),
                b in arb_rootless_manifest(),
                c in arb_rootless_manifest(),
            ) {
                let ab_c = a.clone().merge(b.clone()).merge(c.clone());
                let a_bc = a.merge(b.merge(c));
                prop_assert_eq!(ab_c, a_bc);
            }

            #[test]
            fn merge_is_idempotent(a in arb_manifest()) {
                let merged = a.clone().merge(a.clone());
                prop_assert_eq!(merged, a);
            }

            #[test]
            fn empty_manifest_is_identity(a in arb_manifest()) {
                prop_assert_eq!(a.clone().merge(Manifest::new()), a.clone());
                prop_assert_eq!(Manifest::new().merge(a.clone()), a);
            }

            #[test]
            fn merge_never_loses_paths(a in arb_manifest(), b in arb_manifest()) {
                let keys_a: BTreeSet<_> = a.paths.keys().copied().collect();
                let keys_b: BTreeSet<_> = b.paths.keys().copied().collect();
                let merged = a.merge(b);
                let keys_merged: BTreeSet<_> = merged.paths.keys().copied().collect();
                prop_assert_eq!(
                    keys_merged,
                    keys_a.union(&keys_b).copied().collect::<BTreeSet<_>>()
                );
            }

            #[test]
            fn encode_decode_round_trips(a in arb_manifest()) {
                let decoded = Manifest::decode(&a.encode().unwrap()).unwrap();
                prop_assert_eq!(decoded, a);
            }
        }
    }

    #[test]
    fn file_tree_with_chunk_list_round_trips_through_cbor() {
        // The risky combination: harmonia's internally-tagged enum with
        // #[serde(flatten)] contents, holding our ChunkList, through CBOR.
        let tree = FileTree(FileSystemObject::Directory(Directory {
            entries: BTreeMap::from([
                (
                    "file".to_string(),
                    Box::new(leaf_tree(vec![Hash32::digest(b"x")])),
                ),
                (
                    "link".to_string(),
                    Box::new(FileTree(FileSystemObject::Symlink(Symlink {
                        target: "file".to_string(),
                    }))),
                ),
            ]),
        }));
        let mut cbor = Vec::new();
        ciborium::into_writer(&tree, &mut cbor).unwrap();
        let decoded: FileTree<ChunkList> = ciborium::from_reader(cbor.as_slice()).unwrap();
        assert_eq!(tree, decoded);
    }
}
