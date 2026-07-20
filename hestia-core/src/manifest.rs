//! The hestia manifest: one CBOR+zstd document describing everything stored
//! in the GHA cache for a repository.
//!
//! Stored as the SaveMutable entry family `m#N` (see `gha::savemutable`).
//! All structs ignore unknown fields on decode so the format can grow
//! without breaking older readers.

use std::collections::{BTreeMap, BTreeSet};

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// Re-exports so integration tests and other modules can build trees without
// depending on harmonia crates directly.
pub use harmonia_file_core::{Directory, FileSystemObject, FileTree, Regular, Symlink};
pub use harmonia_store_path::{StorePath, StorePathHash};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to encode manifest: {0}")]
    Encode(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("failed to decode manifest: {0}")]
    Decode(#[from] ciborium::de::Error<std::io::Error>),

    #[error("compression failed: {0}")]
    Compression(#[from] std::io::Error),

    #[error("invalid manifest encoding: {0}")]
    InvalidEncoding(String),
}

/// Full SHA-256 digest of `data`. Used for NAR hashes, which Nix records
/// and hestia must reproduce byte-for-byte.
fn sha256_digest(data: &[u8]) -> [u8; 32] {
    *harmonia_utils_hash::Sha256::digest(data).digest_bytes()
}

/// Full BLAKE3 digest of `data`. Used for hestia's internal content
/// addresses (chunk and pack hashes): ~3x faster than SHA-256 and not
/// constrained by any Nix format.
fn blake3_digest(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// Generates a fixed-size hash digest type: CBOR byte-string serialization,
/// hex display, and construction via `$digest` (truncated to the type's
/// length).
macro_rules! hash_newtype {
    ($(#[$doc:meta])* $name:ident, $len:expr, $digest:path) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub [u8; $len]);

        impl $name {
            /// Hash length in bytes.
            pub const LEN: usize = $len;

            /// Digest of `data`, truncated to [`Self::LEN`] bytes.
            pub fn digest(data: impl AsRef<[u8]>) -> Self {
                let digest = $digest(data.as_ref());
                let mut bytes = [0u8; $len];
                bytes.copy_from_slice(&digest[..$len]);
                Self(bytes)
            }

            pub fn to_hex(self) -> String {
                self.0.iter().map(|b| format!("{b:02x}")).collect()
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.to_hex())
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.to_hex())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_bytes(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct Visitor;
                impl<'de> serde::de::Visitor<'de> for Visitor {
                    type Value = $name;

                    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                        write!(f, "{} bytes", $len)
                    }

                    fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<$name, E> {
                        let array: [u8; $len] = v
                            .try_into()
                            .map_err(|_| E::invalid_length(v.len(), &self))?;
                        Ok($name(array))
                    }
                }
                deserializer.deserialize_bytes(Visitor)
            }
        }
    };
}

hash_newtype!(
    /// A full 32-byte SHA-256 digest. Used for NAR hashes only.
    Hash32,
    32,
    sha256_digest
);

hash_newtype!(
    /// A full 32-byte BLAKE3 digest, used for pack content addresses.
    Blake3Pack,
    32,
    blake3_digest
);

hash_newtype!(
    /// A BLAKE3 digest truncated to 16 bytes.
    ///
    /// Used for chunk hashes: 128 bits keeps collisions out of reach
    /// (birthday bound 2^64 chunks) while halving the dominant cost of the
    /// manifest, which stores one hash per chunk.
    Blake3Chunk,
    16,
    blake3_digest
);

pub type ChunkHash = Blake3Chunk;
pub type PackHash = Blake3Pack;
pub type NarHash = Hash32;

impl Hash32 {
    /// Parse a SHA-256 hash in any format Nix uses: SRI (`sha256-<base64>`),
    /// prefixed (`sha256:<base16|base32|base64>`), or bare.
    ///
    /// Returns `None` for non-SHA-256 hashes or unparsable input.
    pub fn parse_sha256(s: &str) -> Option<Self> {
        let hash: harmonia_utils_hash::fmt::Any<harmonia_utils_hash::Sha256> = s.parse().ok()?;
        Some(Self(*hash.into_hash().digest_bytes()))
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
    /// Reference occurrences rewritten to a sentinel before chunking. Empty
    /// for reference-free files. Offsets are relative to the file content;
    /// indices point into the path's sorted reference set (see
    /// [`crate::refnorm`]).
    #[serde(default)]
    pub rewrites: Vec<Rewrite>,
}

/// One reference occurrence normalized out of a file's content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rewrite {
    pub offset: u64,
    pub ref_index: u32,
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
///
/// Generic over the file contents `C`: chunk hashes in memory
/// ([`ChunkList`]), chunk-table indices on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
// FileTree's Deserialize impl wants DeserializeOwned, not Deserialize<'de>.
#[serde(bound(deserialize = "C: serde::de::DeserializeOwned"))]
pub struct PathEntry<C = ChunkList> {
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
    /// File tree with `C` as file contents.
    pub tree: FileTree<C>,
    /// Last time the GC mark phase reached this path (unix seconds).
    #[serde(default)]
    pub last_reachable: u64,
    /// Last time this path was pushed, or would have been pushed but was
    /// dedup-skipped (unix seconds).
    #[serde(default)]
    pub last_pushed: u64,
}

impl<C> PathEntry<C> {
    /// Convert the file contents with `f`, keeping everything else.
    fn map_contents<D, E>(self, f: &mut impl FnMut(&C) -> Result<D, E>) -> Result<PathEntry<D>, E> {
        Ok(PathEntry {
            store_path: self.store_path,
            nar_hash: self.nar_hash,
            nar_size: self.nar_size,
            references: self.references,
            ca: self.ca,
            deriver: self.deriver,
            tree: map_tree(&self.tree, f)?,
            last_reachable: self.last_reachable,
            last_pushed: self.last_pushed,
        })
    }
}

/// A GC root: the set of paths one (branch, system) pair currently needs.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Root {
    #[serde(default)]
    pub paths: BTreeSet<PathHash>,
    /// When this root was last replaced (unix seconds).
    #[serde(default)]
    pub updated: u64,
    /// Workflow run that wrote this root (`$GITHUB_RUN_ID`); `None` for
    /// local builds and manifests written before this field existed.
    #[serde(default)]
    pub run_id: Option<String>,
}

/// The top-level manifest document.
///
/// Deliberately not `Serialize`/`Deserialize`: the in-memory layout is
/// optimized for merging, not for storage. [`Manifest::encode`] /
/// [`Manifest::decode`] translate to and from the columnar wire form.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Manifest {
    pub paths: BTreeMap<PathHash, PathEntry>,
    pub chunks: BTreeMap<ChunkHash, ChunkLocation>,
    /// Packs keyed by hash, so dedup-by-hash is the natural merge operation.
    pub packs: BTreeMap<PackHash, PackInfo>,
    /// Roots keyed by "branch-system", e.g. "main-x86_64-linux".
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
        // Identical entries are the common case (drain commits merge
        // near-identical manifests on every retry); skip the tie-break
        // below, which clones and CBOR-serializes both entries.
        if a == b {
            return a;
        }
        // Deterministic winner: newer push wins, nar_hash as tie-break, the
        // CBOR encoding of the remaining content as the final tie-break.
        // The final tie-break must be a total order. Entries can tie on
        // (last_pushed, nar_hash) while differing in metadata, and without
        // a total order the merge would not be commutative. The expensive
        // encoding is computed only on a (last_pushed, nar_hash) tie.
        fn content_key(entry: &PathEntry) -> Vec<u8> {
            // Exclude the max()-folded clocks so folding never changes an
            // entry's ordering (keeps the merge associative).
            let mut content = entry.clone();
            content.last_reachable = 0;
            content.last_pushed = 0;
            let mut encoded = Vec::new();
            // A serialization failure only weakens the tie-break, never panics.
            let _ = ciborium::into_writer(&content, &mut encoded);
            encoded
        }
        let quick_key = |entry: &PathEntry| (entry.last_pushed, entry.nar_hash);
        let a_wins = match quick_key(&a).cmp(&quick_key(&b)) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Less => false,
            std::cmp::Ordering::Equal => content_key(&a) >= content_key(&b),
        };
        let (mut winner, loser) = if a_wins { (a, b) } else { (b, a) };
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
    /// Roots from the same workflow run are concurrent (matrix legs of one
    /// workflow, which can drain hours apart): union their paths.
    /// Otherwise the newer root replaces the older one -- that is what
    /// makes old closures unreachable and therefore collectable.
    fn merge(a: Self, b: Self) -> Self {
        if a.run_id.is_some() && a.run_id == b.run_id {
            Root {
                paths: a.paths.into_iter().chain(b.paths).collect(),
                updated: a.updated.max(b.updated),
                run_id: a.run_id,
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

// --- Wire format ------------------------------------------------------------
//
// Encoding the in-memory maps directly stores every chunk hash twice (chunk
// index + file tree) and repeats field names and pack hashes per entry. The
// wire form is columnar instead: hashes concatenated into byte strings, each
// stored once; locations as parallel integer arrays; trees referencing
// chunks by table index. Production manifest: 5.8 MB -> 2.2 MB.

/// A CBOR byte string (`Vec<u8>` would serialize as an array of integers).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Blob(Vec<u8>);

impl Serialize for Blob {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Blob;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a byte string")
            }

            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Blob, E> {
                Ok(Blob(v.to_vec()))
            }

            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Blob, E> {
                Ok(Blob(v))
            }
        }
        // deserialize_byte_buf, not deserialize_bytes: ciborium's
        // deserialize_bytes rejects byte strings larger than its scratch
        // buffer (4 KiB), and hash columns are megabytes.
        deserializer.deserialize_byte_buf(Visitor)
    }
}

/// File contents on the wire: indices into the chunk table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct WireChunkList {
    #[serde(default)]
    chunks: Vec<u64>,
    #[serde(default)]
    rewrites: Vec<Rewrite>,
}

/// Columnar wire representation.
///
/// The hash tables are concatenated, sorted hashes; a hash's position is
/// its table index. Location rows are parallel arrays sorted by
/// (pack index, offset), with offsets delta-encoded within each pack.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct WireManifest {
    #[serde(default)]
    chunk_hashes: Blob,
    #[serde(default)]
    pack_hashes: Blob,
    #[serde(default)]
    location_chunks: Vec<u64>,
    #[serde(default)]
    location_packs: Vec<u64>,
    #[serde(default)]
    location_offsets: Vec<u64>,
    #[serde(default)]
    location_compressed_sizes: Vec<u32>,
    #[serde(default)]
    location_uncompressed_sizes: Vec<u32>,
    #[serde(default)]
    location_repacks_survived: Vec<u32>,
    /// Pack metadata keyed by pack table index.
    #[serde(default)]
    pack_infos: BTreeMap<u64, PackInfo>,
    #[serde(default)]
    paths: BTreeMap<PathHash, PathEntry<WireChunkList>>,
    #[serde(default)]
    roots: BTreeMap<String, Root>,
}

/// Rebuild a tree with its file contents transformed by `f`.
fn map_tree<A, B, E>(
    tree: &FileTree<A>,
    f: &mut impl FnMut(&A) -> Result<B, E>,
) -> Result<FileTree<B>, E> {
    Ok(FileTree(match &tree.0 {
        FileSystemObject::Regular(regular) => FileSystemObject::Regular(Regular {
            executable: regular.executable,
            contents: f(&regular.contents)?,
        }),
        FileSystemObject::Symlink(symlink) => FileSystemObject::Symlink(symlink.clone()),
        FileSystemObject::Directory(directory) => FileSystemObject::Directory(Directory {
            entries: directory
                .entries
                .iter()
                .map(|(name, child)| Ok((name.clone(), Box::new(map_tree(child, f)?))))
                .collect::<Result<BTreeMap<_, _>, E>>()?,
        }),
    }))
}

/// Visit the contents of every regular file in a tree.
fn visit_contents<C>(tree: &FileTree<C>, visit: &mut impl FnMut(&C)) {
    match &tree.0 {
        FileSystemObject::Regular(regular) => visit(&regular.contents),
        FileSystemObject::Symlink(_) => {}
        FileSystemObject::Directory(directory) => {
            for child in directory.entries.values() {
                visit_contents(child, visit);
            }
        }
    }
}

/// zstd level for manifest compression: the columnar form leaves mostly
/// incompressible hash bytes, so higher levels gain little.
const ZSTD_LEVEL: i32 = 9;

/// Decompression bound for stored manifest blobs.
///
/// The GHA cache is not trusted storage. Without a bound, a small zstd bomb
/// would abort every serve, drain, and GC in the allocator. A 28 GiB NAR /
/// 4.4k-path workload measured ~70 MiB of manifest CBOR, so 256 MiB leaves
/// headroom while an over-long stream surfaces as a normal decode error.
const MAX_MANIFEST_CBOR_BYTES: u64 = 256 * 1024 * 1024;

impl Manifest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Serialize to zstd-compressed columnar CBOR.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut cbor = Vec::new();
        ciborium::into_writer(&self.to_wire()?, &mut cbor)?;
        Ok(zstd::encode_all(cbor.as_slice(), ZSTD_LEVEL)?)
    }

    /// Deserialize from zstd-compressed columnar CBOR.
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        use std::io::Read as _;
        let mut cbor = Vec::new();
        // Read one byte past the bound: enough to detect an oversized
        // stream without buffering its full payload.
        zstd::Decoder::with_buffer(data)?
            .take(MAX_MANIFEST_CBOR_BYTES + 1)
            .read_to_end(&mut cbor)?;
        if cbor.len() as u64 > MAX_MANIFEST_CBOR_BYTES {
            return Err(Error::InvalidEncoding(format!(
                "manifest decompresses past the {MAX_MANIFEST_CBOR_BYTES}-byte bound \
                 (corrupt or malicious data)"
            )));
        }
        Self::from_wire(ciborium::from_reader(cbor.as_slice())?)
    }

    fn to_wire(&self) -> Result<WireManifest, Error> {
        // Chunk table: every chunk hash the manifest mentions. File trees
        // may reference chunks the chunk index does not know (their
        // location lives in another manifest version), so this is a union.
        let mut chunk_table: BTreeSet<ChunkHash> = self.chunks.keys().copied().collect();
        for entry in self.paths.values() {
            visit_contents(&entry.tree, &mut |list: &ChunkList| {
                chunk_table.extend(list.chunks.iter().copied());
            });
        }
        let chunk_index: BTreeMap<ChunkHash, u64> = chunk_table
            .iter()
            .enumerate()
            .map(|(index, hash)| (*hash, index as u64))
            .collect();

        // Same for the pack table.
        let mut pack_table: BTreeSet<PackHash> = self.packs.keys().copied().collect();
        pack_table.extend(self.chunks.values().map(|location| location.pack));
        let pack_index: BTreeMap<PackHash, u64> = pack_table
            .iter()
            .enumerate()
            .map(|(index, hash)| (*hash, index as u64))
            .collect();

        let mut wire = WireManifest {
            chunk_hashes: Blob(chunk_table.iter().flat_map(|hash| hash.0).collect()),
            pack_hashes: Blob(pack_table.iter().flat_map(|hash| hash.0).collect()),
            pack_infos: self
                .packs
                .iter()
                .map(|(hash, info)| (pack_index[hash], info.clone()))
                .collect(),
            roots: self.roots.clone(),
            ..WireManifest::default()
        };

        let mut rows: Vec<(&ChunkHash, &ChunkLocation)> = self.chunks.iter().collect();
        rows.sort_by_key(|(_, location)| (location.pack, location.offset));
        let mut previous_offset: BTreeMap<u64, u64> = BTreeMap::new();
        for (hash, location) in rows {
            let pack = pack_index[&location.pack];
            let previous = previous_offset.insert(pack, location.offset).unwrap_or(0);
            wire.location_chunks.push(chunk_index[hash]);
            wire.location_packs.push(pack);
            wire.location_offsets.push(location.offset - previous);
            wire.location_compressed_sizes
                .push(location.compressed_size);
            wire.location_uncompressed_sizes
                .push(location.uncompressed_size);
            wire.location_repacks_survived
                .push(location.repacks_survived);
        }

        wire.paths = self
            .paths
            .iter()
            .map(|(hash, entry)| {
                let entry = entry.clone().map_contents(&mut |list: &ChunkList| {
                    Ok::<_, Error>(WireChunkList {
                        chunks: list.chunks.iter().map(|hash| chunk_index[hash]).collect(),
                        rewrites: list.rewrites.clone(),
                    })
                })?;
                Ok((*hash, entry))
            })
            .collect::<Result<_, Error>>()?;

        Ok(wire)
    }

    fn from_wire(wire: WireManifest) -> Result<Self, Error> {
        let invalid = |message: String| Error::InvalidEncoding(message);

        if !wire.chunk_hashes.0.len().is_multiple_of(ChunkHash::LEN) {
            return Err(invalid(format!(
                "chunk hash table length {} is not a multiple of {}",
                wire.chunk_hashes.0.len(),
                ChunkHash::LEN
            )));
        }
        if !wire.pack_hashes.0.len().is_multiple_of(PackHash::LEN) {
            return Err(invalid(format!(
                "pack hash table length {} is not a multiple of {}",
                wire.pack_hashes.0.len(),
                PackHash::LEN
            )));
        }
        let chunk_table: Vec<ChunkHash> = wire
            .chunk_hashes
            .0
            .chunks_exact(ChunkHash::LEN)
            .map(|bytes| Blake3Chunk(bytes.try_into().expect("chunks_exact yields exact lengths")))
            .collect();
        let pack_table: Vec<PackHash> = wire
            .pack_hashes
            .0
            .chunks_exact(PackHash::LEN)
            .map(|bytes| Blake3Pack(bytes.try_into().expect("chunks_exact yields exact lengths")))
            .collect();
        let chunk_at = |index: u64| {
            chunk_table
                .get(index as usize)
                .copied()
                .ok_or_else(|| invalid(format!("chunk index {index} out of range")))
        };
        let pack_at = |index: u64| {
            pack_table
                .get(index as usize)
                .copied()
                .ok_or_else(|| invalid(format!("pack index {index} out of range")))
        };

        let rows = wire.location_chunks.len();
        if [
            wire.location_packs.len(),
            wire.location_offsets.len(),
            wire.location_compressed_sizes.len(),
            wire.location_uncompressed_sizes.len(),
            wire.location_repacks_survived.len(),
        ]
        .iter()
        .any(|&len| len != rows)
        {
            return Err(invalid("location columns have differing lengths".into()));
        }

        let mut chunks = BTreeMap::new();
        let mut previous_offset: BTreeMap<u64, u64> = BTreeMap::new();
        for row in 0..rows {
            let pack = wire.location_packs[row];
            // Checked: deltas come straight from untrusted CBOR, and an
            // overflowing accumulation must be a decode error, not a
            // debug-build panic or a silently wrapped offset.
            let offset = previous_offset
                .get(&pack)
                .copied()
                .unwrap_or(0)
                .checked_add(wire.location_offsets[row])
                .ok_or_else(|| invalid(format!("chunk offset overflow in row {row}")))?;
            previous_offset.insert(pack, offset);
            chunks.insert(
                chunk_at(wire.location_chunks[row])?,
                ChunkLocation {
                    pack: pack_at(pack)?,
                    offset,
                    compressed_size: wire.location_compressed_sizes[row],
                    uncompressed_size: wire.location_uncompressed_sizes[row],
                    repacks_survived: wire.location_repacks_survived[row],
                },
            );
        }

        let packs = wire
            .pack_infos
            .into_iter()
            .map(|(index, info)| Ok((pack_at(index)?, info)))
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        let paths = wire
            .paths
            .into_iter()
            .map(|(hash, entry)| {
                let entry = entry.map_contents(&mut |list: &WireChunkList| {
                    Ok::<_, Error>(ChunkList {
                        chunks: list
                            .chunks
                            .iter()
                            .map(|&index| chunk_at(index))
                            .collect::<Result<Vec<_>, Error>>()?,
                        rewrites: list.rewrites.clone(),
                    })
                })?;
                Ok((hash, entry))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        Ok(Manifest {
            paths,
            chunks,
            packs,
            roots: wire.roots,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn leaf_tree(chunks: Vec<ChunkHash>) -> FileTree<ChunkList> {
        FileTree(FileSystemObject::Regular(Regular {
            executable: false,
            contents: ChunkList {
                chunks,
                ..Default::default()
            },
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
            tree: leaf_tree(vec![
                ChunkHash::digest([seed, 1]),
                ChunkHash::digest([seed, 2]),
            ]),
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
        let chunk_a = ChunkHash::digest(b"chunk a");
        let chunk_b = ChunkHash::digest(b"chunk b");
        let pack = PackHash::digest(b"pack");

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
                                            ..Default::default()
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
                run_id: None,
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
    fn large_manifest_round_trip() {
        // Regression: ciborium's deserialize_bytes rejects byte strings
        // larger than its 4 KiB scratch buffer ("invalid type: bytes,
        // expected bytes"). The columnar hash blobs exceed that on any
        // real manifest, so use enough chunks to cross the threshold.
        let mut manifest = sample_manifest();
        let pack = PackHash::digest(b"big pack");
        for i in 0u32..1000 {
            manifest.chunks.insert(
                ChunkHash::digest(i.to_le_bytes()),
                ChunkLocation {
                    pack,
                    offset: u64::from(i) * 100,
                    compressed_size: 100,
                    uncompressed_size: 150,
                    repacks_survived: 0,
                },
            );
        }
        let decoded = Manifest::decode(&manifest.encode().unwrap()).unwrap();
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
        // Simulate by patching extra fields into the wire CBOR at top level
        // and path level, then decoding with today's schema.
        let manifest = sample_manifest();
        let cbor = zstd::decode_all(manifest.encode().unwrap().as_slice()).unwrap();
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
    fn corrupt_wire_indices_are_an_error_not_a_panic() {
        // A tree referencing a chunk index that is not in the table must
        // surface as InvalidEncoding.
        let manifest = sample_manifest();
        let cbor = zstd::decode_all(manifest.encode().unwrap().as_slice()).unwrap();
        let mut value: ciborium::Value = ciborium::from_reader(cbor.as_slice()).unwrap();
        // Truncate the chunk hash table to length 0.
        for (key, field) in value.as_map_mut().unwrap() {
            if key.as_text() == Some("chunk_hashes") {
                *field = ciborium::Value::Bytes(Vec::new());
            }
        }
        let mut patched = Vec::new();
        ciborium::into_writer(&value, &mut patched).unwrap();
        let compressed = zstd::encode_all(patched.as_slice(), 3).unwrap();

        let result = Manifest::decode(&compressed);
        assert!(
            matches!(result, Err(Error::InvalidEncoding(_))),
            "expected InvalidEncoding, got {result:?}"
        );
    }

    #[test]
    fn overflowing_wire_offsets_are_an_error_not_a_panic() {
        // Delta-encoded offsets come from untrusted CBOR: two rows for the
        // same pack whose deltas sum past u64::MAX must surface as
        // InvalidEncoding instead of panicking (debug) or wrapping
        // (release).
        let wire = WireManifest {
            chunk_hashes: Blob([1u8; 2 * ChunkHash::LEN].to_vec()),
            pack_hashes: Blob([2u8; PackHash::LEN].to_vec()),
            location_chunks: vec![0, 1],
            location_packs: vec![0, 0],
            location_offsets: vec![u64::MAX, 1],
            location_compressed_sizes: vec![1, 1],
            location_uncompressed_sizes: vec![1, 1],
            location_repacks_survived: vec![0, 0],
            ..WireManifest::default()
        };
        let result = Manifest::from_wire(wire);
        assert!(
            matches!(result, Err(Error::InvalidEncoding(_))),
            "expected InvalidEncoding, got {result:?}"
        );
    }

    #[test]
    fn manifest_zstd_bomb_is_an_error_not_an_allocation() {
        // A small zstd stream of zeros decompressing far past any real
        // manifest must be rejected at the size bound, before the full
        // payload is buffered.
        let bomb_payload = vec![0u8; 512 * 1024 * 1024];
        let bomb = zstd::encode_all(bomb_payload.as_slice(), 3).unwrap();
        assert!(bomb.len() < 1024 * 1024, "bomb must be small on the wire");
        let result = Manifest::decode(&bomb);
        assert!(
            matches!(result, Err(Error::InvalidEncoding(_))),
            "expected InvalidEncoding, got {result:?}"
        );
    }

    #[test]
    fn wire_format_stores_each_chunk_hash_once() {
        // A chunk hash referenced by both the chunk index and a file tree
        // must appear exactly once in the encoded bytes.
        let manifest = sample_manifest();
        let encoded = manifest.encode().unwrap();
        let raw = zstd::decode_all(encoded.as_slice()).unwrap();
        for hash in manifest.chunks.keys() {
            let count = raw.windows(ChunkHash::LEN).filter(|w| *w == hash.0).count();
            assert_eq!(count, 1, "chunk hash {hash} appears {count} times");
        }
    }

    #[test]
    fn chunk_hashes_are_16_bytes() {
        assert_eq!(ChunkHash::LEN, 16);
        // Pack and NAR hashes stay full SHA-256.
        assert_eq!(PackHash::LEN, 32);
        assert_eq!(NarHash::LEN, 32);
    }

    #[test]
    fn truncated_chunk_digest_is_a_blake3_prefix() {
        let full = Blake3Pack::digest(b"same input");
        let truncated = ChunkHash::digest(b"same input");
        assert_eq!(truncated.0[..], full.0[..16]);
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
        // Concurrent CI jobs can push the same store path within the same
        // second with identical content but different metadata (e.g.
        // deriver). The merge result must not depend on argument order, or
        // the surviving manifest would depend on who wins the SaveMutable
        // race.
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
        let pack = PackHash::digest(b"pack");
        let chunk = ChunkHash::digest(b"chunk");

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
                pack: PackHash::digest(b"other pack"),
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
    fn merge_roots_unions_same_run_replaces_other_runs() {
        let make_root = |paths: &[u8], updated: u64, run_id: Option<&str>| Root {
            paths: paths.iter().map(|&seed| path_hash(seed)).collect(),
            updated,
            run_id: run_id.map(str::to_string),
        };

        // Same workflow run: union, however far apart the drains are.
        let merged = Root::merge(
            make_root(&[1, 2], 1000, Some("run-1")),
            make_root(&[3], 1000 + 7200, Some("run-1")),
        );
        assert_eq!(merged.paths.len(), 3);
        assert_eq!(merged.updated, 8200);
        assert_eq!(merged.run_id.as_deref(), Some("run-1"));

        // Different run: newer replaces, even seconds later.
        let merged = Root::merge(
            make_root(&[1, 2], 1000, Some("run-1")),
            make_root(&[3], 1001, Some("run-2")),
        );
        assert_eq!(merged.paths.len(), 1);
        assert!(merged.paths.contains(&path_hash(3)));

        // No run id (local builds): never union.
        let merged = Root::merge(make_root(&[1, 2], 1000, None), make_root(&[3], 1001, None));
        assert_eq!(merged.paths.len(), 1);

        // Order independence of replacement.
        let merged = Root::merge(
            make_root(&[3], 1001, Some("run-2")),
            make_root(&[1, 2], 1000, Some("run-1")),
        );
        assert_eq!(merged.paths.len(), 1);
        assert!(merged.paths.contains(&path_hash(3)));
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
                run_id: None,
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
                run_id: None,
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
                run_id: None,
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

        fn arb_pack_hash() -> impl Strategy<Value = PackHash> {
            (0u8..6).prop_map(|seed| PackHash::digest([seed]))
        }

        fn arb_chunk_hash() -> impl Strategy<Value = ChunkHash> {
            (0u8..6).prop_map(|seed| ChunkHash::digest([seed]))
        }

        fn arb_chunk_list() -> impl Strategy<Value = ChunkList> {
            proptest::collection::vec(arb_chunk_hash(), 0..3).prop_map(|chunks| ChunkList {
                chunks,
                ..Default::default()
            })
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
            (arb_pack_hash(), 0u64..1000, 1u32..1000, 1u32..1000, 0u32..5).prop_map(
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
                prop_oneof![
                    Just(None),
                    Just(Some("run-1".to_string())),
                    Just(Some("run-2".to_string())),
                ],
            )
                .prop_map(|(paths, updated, run_id)| Root {
                    paths,
                    updated,
                    run_id,
                })
        }

        fn arb_manifest() -> impl Strategy<Value = Manifest> {
            (
                proptest::collection::btree_map(arb_path_hash(), arb_path_entry(), 0..4),
                proptest::collection::btree_map(arb_chunk_hash(), arb_chunk_location(), 0..4),
                proptest::collection::btree_map(arb_pack_hash(), arb_pack_info(), 0..3),
                proptest::collection::btree_map("[a-z]{1,4}", arb_root(), 0..3),
            )
                .prop_map(|(paths, chunks, packs, roots)| Manifest {
                    paths,
                    chunks,
                    packs,
                    roots,
                })
        }

        /// Manifest without roots: `Root::merge` mixes union (same run)
        /// and replacement (different run), which is inherently
        /// order-dependent for 3+ way merges (a documented limitation), so
        /// the associativity law is stated for the path/chunk/pack maps
        /// only.
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
                    Box::new(leaf_tree(vec![ChunkHash::digest(b"x")])),
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
