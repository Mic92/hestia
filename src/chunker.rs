//! Content-defined chunking (FastCDC v2020) and pack assembly.
//!
//! Files are split into chunks at content-defined boundaries so that small
//! changes to a file (or the same file appearing in different store paths)
//! produce mostly identical chunks: the unit of dedup. Chunks are
//! individually zstd-compressed and concatenated into pack blobs; each chunk
//! stays independently extractable via `(offset, compressed_size)` Range
//! reads against the pack.

use std::collections::BTreeSet;

use bytes::Bytes;

use crate::manifest::{ChunkHash, ChunkLocation, Hash32, PackHash};

/// FastCDC parameters. Pinned: changing them changes every chunk boundary
/// and therefore invalidates all existing chunks in the cache.
pub const MIN_CHUNK_SIZE: u32 = 16 * 1024;
pub const AVG_CHUNK_SIZE: u32 = 64 * 1024;
pub const MAX_CHUNK_SIZE: u32 = 256 * 1024;

/// zstd level for individual chunk compression inside packs.
///
/// Level 3 (zstd default): pack uploads happen on CI time, so favor speed;
/// the dedup comes from chunking, not from squeezing the last compression
/// percent.
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("compression failed: {0}")]
    Compression(#[from] std::io::Error),

    #[error("chunk hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: Hash32, actual: Hash32 },

    #[error("invalid NAR event stream: {0}")]
    InvalidNar(String),
}

// ---------------------------------------------------------------------------
// Chunking
// ---------------------------------------------------------------------------

/// One content-defined chunk of a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// SHA-256 of the uncompressed chunk data.
    pub hash: ChunkHash,
    /// Uncompressed chunk data (zero-copy slice of the source).
    pub data: Bytes,
}

/// Split file contents into content-defined chunks.
///
/// Deterministic: the same input always produces the same chunk boundaries
/// and hashes (FastCDC is seeded with a constant).
pub fn chunk_data(data: &Bytes) -> Vec<Chunk> {
    if data.is_empty() {
        return Vec::new();
    }
    fastcdc::v2020::FastCDC::new(
        data,
        MIN_CHUNK_SIZE as usize,
        AVG_CHUNK_SIZE as usize,
        MAX_CHUNK_SIZE as usize,
    )
    .map(|cut| {
        let slice = data.slice(cut.offset..cut.offset + cut.length);
        Chunk {
            hash: Hash32::digest(&slice),
            data: slice,
        }
    })
    .collect()
}

// ---------------------------------------------------------------------------
// Pack assembly
// ---------------------------------------------------------------------------

/// Position of one chunk inside a pack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackedChunk {
    pub offset: u64,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
}

/// Builds a pack blob: individually zstd-compressed chunks, concatenated.
///
/// Chunks must be added in `(file path, file offset)` order — the natural
/// order when consuming a NAR event stream — so that chunks of the same file
/// end up adjacent and a reader can fetch them with one Range request.
#[derive(Debug, Default)]
pub struct PackBuilder {
    buffer: Vec<u8>,
    chunks: Vec<(ChunkHash, PackedChunk)>,
    seen: BTreeSet<ChunkHash>,
}

impl PackBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Compress and append a chunk. Chunks already in this pack are skipped
    /// (dedup); returns whether the chunk was actually added.
    pub fn add(&mut self, chunk: &Chunk) -> Result<bool, Error> {
        if !self.seen.insert(chunk.hash) {
            return Ok(false);
        }
        let offset = self.buffer.len() as u64;
        let compressed = zstd::encode_all(chunk.data.as_ref(), ZSTD_LEVEL)?;
        self.buffer.extend_from_slice(&compressed);
        self.chunks.push((
            chunk.hash,
            PackedChunk {
                offset,
                compressed_size: compressed.len() as u32,
                uncompressed_size: chunk.data.len() as u32,
            },
        ));
        Ok(true)
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Current compressed size of the pack under construction.
    pub fn size(&self) -> u64 {
        self.buffer.len() as u64
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Finalize: the pack hash is the SHA-256 of the complete blob, which
    /// makes packs content-addressed (`pack-{hash}` cache keys).
    pub fn finish(self) -> Pack {
        Pack {
            hash: Hash32::digest(&self.buffer),
            data: self.buffer,
            chunks: self.chunks,
        }
    }
}

/// A finished pack blob ready for upload.
#[derive(Debug, Clone)]
pub struct Pack {
    pub hash: PackHash,
    /// The blob: concatenated zstd frames.
    pub data: Vec<u8>,
    /// Chunk positions, in insertion order.
    pub chunks: Vec<(ChunkHash, PackedChunk)>,
}

impl Pack {
    /// Cache key for this pack.
    pub fn cache_key(&self) -> String {
        format!("pack-{}", self.hash.to_hex())
    }

    /// Manifest chunk locations pointing into this pack.
    pub fn locations(&self) -> impl Iterator<Item = (ChunkHash, ChunkLocation)> + '_ {
        self.chunks.iter().map(|(hash, packed)| {
            (
                *hash,
                ChunkLocation {
                    pack: self.hash,
                    offset: packed.offset,
                    compressed_size: packed.compressed_size,
                    uncompressed_size: packed.uncompressed_size,
                    repacks_survived: 0,
                },
            )
        })
    }
}

/// Decompress and verify one chunk extracted from pack bytes.
///
/// `compressed` is the byte slice at `[offset, offset + compressed_size)` of
/// the pack blob — exactly what a Range request against the pack returns.
/// The hash check is mandatory: the GHA cache is not trusted storage and a
/// corrupt chunk must never be served onward.
pub fn extract_chunk(compressed: &[u8], expected: &ChunkHash) -> Result<Vec<u8>, Error> {
    let data = zstd::decode_all(compressed)?;
    let actual = Hash32::digest(&data);
    if actual != *expected {
        return Err(Error::HashMismatch {
            expected: *expected,
            actual,
        });
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random data (xorshift), realistic enough to
    /// produce multiple chunks with varied boundaries.
    fn test_data(len: usize, seed: u64) -> Bytes {
        let mut state = seed | 1;
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            out.extend_from_slice(&state.to_le_bytes());
        }
        out.truncate(len);
        Bytes::from(out)
    }

    #[test]
    fn chunking_is_deterministic() {
        let data = test_data(1024 * 1024, 42);
        let first = chunk_data(&data);
        let second = chunk_data(&data);
        assert_eq!(first, second);
        assert!(
            first.len() > 4,
            "1 MiB should produce several chunks, got {}",
            first.len()
        );
    }

    #[test]
    fn chunks_respect_size_bounds_and_cover_input() {
        let data = test_data(2 * 1024 * 1024, 7);
        let chunks = chunk_data(&data);

        let mut reassembled = Vec::new();
        for (i, chunk) in chunks.iter().enumerate() {
            reassembled.extend_from_slice(&chunk.data);
            let is_last = i == chunks.len() - 1;
            assert!(
                chunk.data.len() <= MAX_CHUNK_SIZE as usize,
                "chunk {i} exceeds max size"
            );
            if !is_last {
                assert!(
                    chunk.data.len() >= MIN_CHUNK_SIZE as usize,
                    "chunk {i} below min size"
                );
            }
        }
        assert_eq!(reassembled, data.as_ref(), "chunks must cover the input");
    }

    #[test]
    fn empty_and_small_inputs() {
        assert!(chunk_data(&Bytes::new()).is_empty());

        // Below the minimum chunk size: one chunk containing everything.
        let small = test_data(100, 1);
        let chunks = chunk_data(&small);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data, small);
        assert_eq!(chunks[0].hash, Hash32::digest(&small));
    }

    #[test]
    fn content_defined_boundaries_survive_prefix_insertion() {
        // The point of CDC over fixed-size blocks: shifting the data does not
        // shift every chunk boundary. Prepend bytes and verify that most
        // chunk hashes from the original data still appear.
        let original = test_data(1024 * 1024, 99);
        let mut shifted_vec = b"some prefix bytes inserted at the start".to_vec();
        shifted_vec.extend_from_slice(&original);
        let shifted = Bytes::from(shifted_vec);

        let original_hashes: BTreeSet<ChunkHash> =
            chunk_data(&original).iter().map(|c| c.hash).collect();
        let shifted_hashes: BTreeSet<ChunkHash> =
            chunk_data(&shifted).iter().map(|c| c.hash).collect();

        let surviving = original_hashes.intersection(&shifted_hashes).count();
        assert!(
            surviving * 2 > original_hashes.len(),
            "expected most chunks to survive a prefix shift, got {surviving}/{}",
            original_hashes.len()
        );
    }

    #[test]
    fn pack_chunks_extractable_by_offset() {
        let chunks: Vec<Chunk> = [test_data(100_000, 1), test_data(200_000, 2)]
            .iter()
            .flat_map(chunk_data)
            .collect();

        let mut builder = PackBuilder::new();
        for chunk in &chunks {
            assert!(builder.add(chunk).unwrap());
        }
        let pack = builder.finish();
        assert_eq!(pack.hash, Hash32::digest(&pack.data));
        assert_eq!(pack.cache_key(), format!("pack-{}", pack.hash.to_hex()));

        // Every chunk must be recoverable from its (offset, compressed_size)
        // slice alone — this is the Range-read contract.
        let by_hash: std::collections::BTreeMap<ChunkHash, Bytes> =
            chunks.iter().map(|c| (c.hash, c.data.clone())).collect();
        for (hash, location) in pack.locations() {
            let start = location.offset as usize;
            let end = start + location.compressed_size as usize;
            let extracted = extract_chunk(&pack.data[start..end], &hash).unwrap();
            assert_eq!(extracted, by_hash[&hash].as_ref());
            assert_eq!(extracted.len(), location.uncompressed_size as usize);
            assert_eq!(location.pack, pack.hash);
        }

        // Offsets tile the pack exactly: no gaps, no overlaps.
        let mut expected_offset = 0u64;
        for (_, packed) in &pack.chunks {
            assert_eq!(packed.offset, expected_offset);
            expected_offset += packed.compressed_size as u64;
        }
        assert_eq!(expected_offset, pack.data.len() as u64);
    }

    #[test]
    fn pack_dedups_identical_chunks() {
        let data = test_data(50_000, 3);
        let chunks = chunk_data(&data);

        let mut builder = PackBuilder::new();
        for chunk in &chunks {
            assert!(builder.add(chunk).unwrap());
        }
        // Adding the same chunks again must be a no-op.
        for chunk in &chunks {
            assert!(!builder.add(chunk).unwrap());
        }
        let pack = builder.finish();
        assert_eq!(pack.chunks.len(), chunks.len());
    }

    #[test]
    fn extract_chunk_detects_corruption() {
        let data = test_data(50_000, 4);
        let chunks = chunk_data(&data);
        let mut builder = PackBuilder::new();
        builder.add(&chunks[0]).unwrap();
        let pack = builder.finish();

        // Wrong expected hash -> HashMismatch.
        let wrong_hash = Hash32::digest(b"something else");
        let result = extract_chunk(&pack.data, &wrong_hash);
        assert!(matches!(result, Err(Error::HashMismatch { .. })));

        // Corrupted compressed bytes -> decompression error or hash mismatch,
        // but never silently wrong data.
        let mut corrupted = pack.data.clone();
        let middle = corrupted.len() / 2;
        corrupted[middle] ^= 0xff;
        assert!(extract_chunk(&corrupted, &chunks[0].hash).is_err());
    }

    #[test]
    fn identical_files_share_all_chunks() {
        // The dedup property across store paths: same content, same chunks.
        let data = test_data(500_000, 5);
        let copy = Bytes::from(data.to_vec());
        let hashes_a: Vec<ChunkHash> = chunk_data(&data).iter().map(|c| c.hash).collect();
        let hashes_b: Vec<ChunkHash> = chunk_data(&copy).iter().map(|c| c.hash).collect();
        assert_eq!(hashes_a, hashes_b);
    }
}
