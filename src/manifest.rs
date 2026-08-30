//! Content addresses and the in-memory shape of one stored path.

pub use harmonia_file_core::{Directory, FileSystemObject, FileTree, Regular, Symlink};
pub use harmonia_store_path::{StorePath, StorePathHash};

/// Full SHA-256 digest of `data`. Used for NAR hashes, which Nix records
/// and hestia must reproduce byte-for-byte.
fn sha256_digest(data: &[u8]) -> [u8; 32] {
    *harmonia_utils_hash::Sha256::digest(data).digest_bytes()
}

/// BLAKE3 for chunk hashes (the hot path). Whole objects use SHA-256 so
/// their name doubles as an OCI blob digest.
fn blake3_digest(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

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

            /// Lowercase hex only, so names built from it are canonical.
            pub fn from_hex(s: &str) -> Option<Self> {
                if s.len() != $len * 2 || !s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) {
                    return None;
                }
                let mut bytes = [0u8; $len];
                for (i, b) in bytes.iter_mut().enumerate() {
                    *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
                }
                Some(Self(bytes))
            }
        }

        impl<C> minicbor::Encode<C> for $name {
            fn encode<W: minicbor::encode::Write>(
                &self,
                e: &mut minicbor::Encoder<W>,
                _: &mut C,
            ) -> Result<(), minicbor::encode::Error<W::Error>> {
                e.bytes(&self.0)?.ok()
            }
        }

        impl<'b, C> minicbor::Decode<'b, C> for $name {
            fn decode(d: &mut minicbor::Decoder<'b>, _: &mut C) -> Result<Self, minicbor::decode::Error> {
                let p = d.position();
                d.bytes()?
                    .try_into()
                    .map(Self)
                    .map_err(|_| minicbor::decode::Error::message(concat!(stringify!($name), " length")).at(p))
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
    };
}

hash_newtype!(
    /// SHA-256 of a NAR.
    Hash32,
    32,
    sha256_digest
);

hash_newtype!(
    /// SHA-256 of a pack blob.
    PackHash,
    32,
    sha256_digest
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

hash_newtype!(
    /// SHA-256 of a `.meta`, `.tree` or head record.
    SegDigest,
    32,
    sha256_digest
);

pub type ChunkHash = Blake3Chunk;
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

/// Ordered chunks making up one regular file.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChunkList {
    pub chunks: Vec<ChunkHash>,
    /// Reference occurrences rewritten to a sentinel before chunking.
    /// Offsets are relative to the file content, indices point into the
    /// path's sorted reference set (see [`crate::refnorm`]).
    pub rewrites: Vec<Rewrite>,
}

/// One reference occurrence normalized out of a file's content.
#[derive(Debug, Clone, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
pub struct Rewrite {
    #[n(0)]
    pub offset: u64,
    #[n(1)]
    pub ref_index: u32,
}

/// Where one chunk lives inside a pack blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkLocation {
    pub pack: PackHash,
    pub offset: u64,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
}

/// One stored path: everything needed to serve narinfo + NAR for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathEntry {
    pub store_path: StorePath,
    pub nar_hash: NarHash,
    pub nar_size: u64,
    /// May point at paths not stored here (served by an upstream cache).
    pub references: Vec<StorePath>,
    pub ca: Option<String>,
    pub deriver: Option<StorePath>,
    pub tree: FileTree<ChunkList>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_hashes_are_16_bytes() {
        assert_eq!(ChunkHash::LEN, 16);
        // Pack and NAR hashes stay full SHA-256.
        assert_eq!(PackHash::LEN, 32);
        assert_eq!(NarHash::LEN, 32);
    }

    #[test]
    fn truncated_chunk_digest_is_a_blake3_prefix() {
        let truncated = ChunkHash::digest(b"same input");
        assert_eq!(truncated.0[..], blake3_digest(b"same input")[..16]);
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
        let hash = PathHash(StorePathHash::new([7; 20]));
        let as_string = hash.to_string();
        let parsed: PathHash = as_string.parse().unwrap();
        assert_eq!(hash, parsed);
    }
}
