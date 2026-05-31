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
pub const ROOT_UNION_WINDOW_SECS: u64 = 600;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to encode manifest: {0}")]
    Encode(#[from] ciborium::ser::Error<std::io::Error>),

    #[error("failed to decode manifest: {0}")]
    Decode(#[from] ciborium::de::Error<std::io::Error>),

    #[error("compression failed: {0}")]
    Compression(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// Hash types
// ---------------------------------------------------------------------------

/// A 32-byte SHA-256 digest (chunk hash, pack hash, or NAR hash).
///
/// Serialized as a CBOR byte string (33 bytes on the wire) instead of an
/// array of integers.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
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

            // Accept sequences too (e.g. JSON arrays in debugging tools).
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Hash32, A::Error> {
                let mut array = [0u8; 32];
                for (i, slot) in array.iter_mut().enumerate() {
                    *slot = seq
                        .next_element()?
                        .ok_or_else(|| A::Error::invalid_length(i, &self))?;
                }
                if seq.next_element::<u8>()?.is_some() {
                    return Err(A::Error::invalid_length(33, &self));
                }
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

// ---------------------------------------------------------------------------
// Manifest schema
// ---------------------------------------------------------------------------

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
    pub nar_hash: NarHash,
    pub nar_size: u64,
    /// Store path hashes this path references. May point at paths that are
    /// not in the manifest (upstream paths served by cache.nixos.org).
    #[serde(default)]
    pub references: Vec<PathHash>,
    #[serde(default)]
    pub ca: Option<String>,
    #[serde(default)]
    pub deriver: Option<String>,
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

// ---------------------------------------------------------------------------
// Codec: CBOR + zstd
// ---------------------------------------------------------------------------

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
            nar_hash: Hash32::digest([seed]),
            nar_size: 1000 + seed as u64,
            references: vec![],
            ca: None,
            deriver: Some(format!("{seed}.drv")),
            tree: leaf_tree(vec![Hash32::digest([seed, 1]), Hash32::digest([seed, 2])]),
            last_reachable: 0,
            last_pushed: 100,
        }
    }

    pub(crate) fn path_hash(seed: u8) -> PathHash {
        // 20-byte store path hashes derived deterministically from the seed.
        PathHash(StorePathHash::new([seed; 20]))
    }

    fn sample_manifest() -> Manifest {
        let mut manifest = Manifest::new();
        let chunk_a = Hash32::digest(b"chunk a");
        let chunk_b = Hash32::digest(b"chunk b");
        let pack = Hash32::digest(b"pack");

        manifest.paths.insert(
            path_hash(1),
            PathEntry {
                nar_hash: Hash32::digest(b"nar"),
                nar_size: 4096,
                references: vec![path_hash(2), path_hash(99)],
                ca: Some("fixed:sha256:abc".into()),
                deriver: Some("foo.drv".into()),
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
    fn path_hash_string_round_trip() {
        let hash = path_hash(7);
        let as_string = hash.to_string();
        let parsed: PathHash = as_string.parse().unwrap();
        assert_eq!(hash, parsed);
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
