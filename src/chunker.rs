//! Content-defined chunking (FastCDC v2020) and pack assembly.
//!
//! Files are split into chunks at content-defined boundaries so that small
//! changes to a file (or the same file appearing in different store paths)
//! produce mostly identical chunks: the unit of dedup. Chunks are
//! individually zstd-compressed and concatenated into pack blobs; each chunk
//! stays independently extractable via `(offset, compressed_size)` Range
//! reads against the pack.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::Poll;

use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _};
use harmonia_file_nar::{NarByteStream, NarEvent, NarWriter};

use crate::manifest::{
    ChunkHash, ChunkList, ChunkLocation, Directory, FileSystemObject, FileTree, Hash32, PackHash,
    Regular, Symlink,
};
use crate::refnorm::RefTable;

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

/// Largest zstd window log accepted when decoding chunk frames.
///
/// Frames produced at [`ZSTD_LEVEL`] declare a window log of 21; anything
/// larger in pack data is corrupt or malicious. Pinned together with
/// [`ZSTD_LEVEL`]: raising the level can raise the declared window log,
/// which would make existing extraction reject legitimate frames.
const MAX_CHUNK_WINDOW_LOG: u32 = 21;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O or compression error: {0}")]
    Io(#[from] std::io::Error),

    #[error("chunk hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        expected: ChunkHash,
        actual: ChunkHash,
    },

    #[error("invalid NAR event stream: {0}")]
    InvalidNar(String),

    #[error("chunk {0} referenced by the file tree but not provided")]
    MissingChunk(ChunkHash),

    #[error(
        "chunk decompresses past the {MAX_CHUNK_SIZE}-byte chunk size limit \
         (corrupt or malicious pack data)"
    )]
    OversizedChunk,

    #[error("reference restore failed: {0}")]
    Restore(#[from] crate::refnorm::Error),
}

/// One content-defined chunk of a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// BLAKE3 (truncated) of the uncompressed chunk data.
    pub hash: ChunkHash,
    /// Uncompressed chunk data (zero-copy slice of the source).
    pub data: Bytes,
}

/// Segment size for parallel chunking: FastCDC runs independently per
/// segment, forcing a cut at each seam. A fixed constant (not core count),
/// so chunk boundaries -- and dedup -- stay identical across machines.
const CHUNK_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

/// Split file contents into content-defined chunks.
///
/// Deterministic: same input, same chunks, independent of core count.
/// Boundaries depend only on MIN/AVG/MAX_CHUNK_SIZE, the fastcdc version,
/// and the CHUNK_SEGMENT_SIZE seam cuts.
///
/// A multi-segment file chunks across cores, so one big output -- where a
/// closure's bytes concentrate -- is not stuck on one core (inter-path
/// concurrency cannot split a single file).
pub fn chunk_data(data: &Bytes) -> Vec<Chunk> {
    if data.is_empty() {
        return Vec::new();
    }
    let segments = data.len().div_ceil(CHUNK_SEGMENT_SIZE);
    if segments <= 1 {
        return chunk_segment(data, 0);
    }
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(segments);
    if workers <= 1 {
        return (0..segments)
            .flat_map(|s| chunk_segment(data, s * CHUNK_SEGMENT_SIZE))
            .collect();
    }
    let per_worker = segments.div_ceil(workers);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let first = w * per_worker;
                let last = ((w + 1) * per_worker).min(segments);
                scope.spawn(move || {
                    (first..last)
                        .flat_map(|s| chunk_segment(data, s * CHUNK_SEGMENT_SIZE))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut out = Vec::new();
        for handle in handles {
            out.extend(handle.join().expect("chunking worker panicked"));
        }
        out
    })
}

/// Chunk one segment starting at `offset`, with file-absolute data slices.
fn chunk_segment(data: &Bytes, offset: usize) -> Vec<Chunk> {
    let end = (offset + CHUNK_SEGMENT_SIZE).min(data.len());
    fastcdc::v2020::FastCDC::new(
        &data[offset..end],
        MIN_CHUNK_SIZE as usize,
        AVG_CHUNK_SIZE as usize,
        MAX_CHUNK_SIZE as usize,
    )
    .map(|cut| {
        let start = offset + cut.offset;
        let slice = data.slice(start..start + cut.length);
        Chunk {
            hash: ChunkHash::digest(&slice),
            data: slice,
        }
    })
    .collect()
}

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

    /// Append an already-compressed chunk frame without recompressing it.
    ///
    /// Used by the write pipeline (frames freshly produced by
    /// [`compress_chunks`]; integrity covered by the NAR-hash gate over the
    /// uncompressed data) and by GC repack (frames Range-read from source
    /// packs and copied byte-identically after verification via
    /// [`extract_chunk`]). Returns whether the chunk was actually added
    /// (duplicates are skipped).
    pub fn add_compressed(
        &mut self,
        hash: ChunkHash,
        frame: &[u8],
        uncompressed_size: u32,
    ) -> bool {
        if !self.seen.insert(hash) {
            return false;
        }
        let offset = self.buffer.len() as u64;
        self.buffer.extend_from_slice(frame);
        self.chunks.push((
            hash,
            PackedChunk {
                offset,
                compressed_size: frame.len() as u32,
                uncompressed_size,
            },
        ));
        true
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Compressed bytes buffered so far.
    pub fn compressed_size(&self) -> u64 {
        self.buffer.len() as u64
    }

    /// Finalize: the pack hash is the BLAKE3 of the complete blob, which
    /// makes packs content-addressed (`pack-{hash}` cache keys).
    pub fn finish(self) -> Pack {
        Pack {
            hash: PackHash::digest(&self.buffer),
            data: Bytes::from(self.buffer),
            chunks: self.chunks,
        }
    }
}

/// A chunk compressed into its pack frame, ready for
/// [`PackBuilder::add_compressed`].
#[derive(Debug)]
pub struct CompressedChunk {
    pub hash: ChunkHash,
    pub frame: Vec<u8>,
    pub uncompressed_size: u32,
}

/// Chunks per worker below which parallel compression is not worth the
/// thread-spawn cost. The pipeline already compresses many paths at once,
/// so only a path large enough to fill several workers (e.g. a big binary,
/// which is where a closure's bytes concentrate) spreads over cores.
const CHUNKS_PER_COMPRESS_WORKER: usize = 64;

/// Compress a path's chunks, preserving order.
///
/// zstd is the dominant CPU cost of a drain. The pipeline compresses many
/// paths concurrently, so small paths stay single-threaded (spawning a
/// thread pool for tens of chunks costs more than it saves); a large path
/// -- often a single big output holding most of a closure's bytes -- spans
/// cores, since inter-path concurrency alone cannot split it.
/// Blocking: call via `spawn_blocking` from async code.
pub fn compress_chunks(chunks: Vec<Chunk>) -> Result<Vec<CompressedChunk>, Error> {
    let compress_one = |chunk: &Chunk| -> Result<CompressedChunk, Error> {
        Ok(CompressedChunk {
            hash: chunk.hash,
            frame: zstd::encode_all(chunk.data.as_ref(), ZSTD_LEVEL)?,
            uncompressed_size: chunk.data.len() as u32,
        })
    };
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .min(chunks.len() / CHUNKS_PER_COMPRESS_WORKER);
    if workers <= 1 {
        return chunks.iter().map(compress_one).collect();
    }
    // Static partitioning balances well: CDC keeps chunk sizes near the
    // average, so equal-count slices carry roughly equal work.
    let per_worker = chunks.len().div_ceil(workers);
    std::thread::scope(|scope| {
        let handles: Vec<_> = chunks
            .chunks(per_worker)
            .map(|slice| scope.spawn(move || slice.iter().map(compress_one).collect()))
            .collect();
        let mut out = Vec::with_capacity(chunks.len());
        for handle in handles {
            let frames: Result<Vec<CompressedChunk>, Error> =
                handle.join().expect("compression worker panicked");
            out.extend(frames?);
        }
        Ok(out)
    })
}

/// A finished pack blob ready for upload.
#[derive(Debug, Clone)]
pub struct Pack {
    pub hash: PackHash,
    /// The blob: concatenated zstd frames (`Bytes`, so upload retries
    /// clone it cheaply).
    pub data: Bytes,
    /// Chunk positions, in insertion order.
    pub chunks: Vec<(ChunkHash, PackedChunk)>,
}

/// Group offset-sorted pack ranges into runs of adjacent frames, so each
/// run can be fetched with one Range request. `span` returns an item's
/// `(offset, compressed_size)`.
pub fn coalesce_adjacent<T>(
    items: impl IntoIterator<Item = T>,
    span: impl Fn(&T) -> (u64, u32),
) -> Vec<Vec<T>> {
    let mut runs: Vec<Vec<T>> = Vec::new();
    for item in items {
        let (offset, _) = span(&item);
        let adjacent = runs.last().and_then(|run| run.last()).is_some_and(|last| {
            let (last_offset, last_size) = span(last);
            // Checked: locations can come from a corrupt manifest; an
            // offset near u64::MAX must not panic the comparison.
            last_offset.checked_add(u64::from(last_size)) == Some(offset)
        });
        match runs.last_mut() {
            Some(run) if adjacent => run.push(item),
            _ => runs.push(vec![item]),
        }
    }
    runs
}

/// GHA cache key for a pack blob (`pack-<blake3 hex>`).
pub fn pack_cache_key(hash: &PackHash) -> String {
    format!("pack-{}", hash.to_hex())
}

impl Pack {
    /// Cache key for this pack.
    pub fn cache_key(&self) -> String {
        pack_cache_key(&self.hash)
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

/// A node of the manifest file tree (convenience alias).
pub type TreeNode = FileSystemObject<ChunkList, Box<FileTree<ChunkList>>>;

/// Result of walking and chunking one store path.
#[derive(Debug)]
pub struct ChunkedPath {
    /// File tree with per-file chunk lists (goes into `PathEntry::tree`).
    pub tree: FileTree<ChunkList>,
    /// Unique chunks in first-seen order, which is `(file path, file offset)`
    /// order because the NAR walk visits files in sorted order and chunks
    /// each file front to back. This is the order chunks should enter packs.
    pub chunks: Vec<Chunk>,
}

impl ChunkedPath {
    /// Chunk data indexed by hash (input for [`nar_hash_from_chunks`] and
    /// pack assembly).
    pub fn chunk_map(&self) -> BTreeMap<ChunkHash, Bytes> {
        self.chunks
            .iter()
            .map(|chunk| (chunk.hash, chunk.data.clone()))
            .collect()
    }
}

/// Directory entries under construction.
type DirEntries = BTreeMap<String, Box<FileTree<ChunkList>>>;

/// Builds a [`FileTree`] from the NAR event stream's
/// StartDirectory/EndDirectory bracketing.
struct TreeBuilder {
    stack: Vec<(String, DirEntries)>,
    root: Option<FileTree<ChunkList>>,
}

impl TreeBuilder {
    fn new() -> Self {
        Self {
            stack: Vec::new(),
            root: None,
        }
    }

    fn place(&mut self, name: String, node: FileTree<ChunkList>) -> Result<(), Error> {
        match self.stack.last_mut() {
            Some((_, entries)) => {
                if entries.insert(name.clone(), Box::new(node)).is_some() {
                    return Err(Error::InvalidNar(format!("duplicate entry {name:?}")));
                }
            }
            None => {
                if self.root.is_some() {
                    return Err(Error::InvalidNar("multiple root nodes".into()));
                }
                self.root = Some(node);
            }
        }
        Ok(())
    }

    fn start_directory(&mut self, name: String) {
        self.stack.push((name, DirEntries::new()));
    }

    fn end_directory(&mut self) -> Result<(), Error> {
        let (name, entries) = self.stack.pop().ok_or_else(|| {
            Error::InvalidNar("EndDirectory without matching StartDirectory".into())
        })?;
        self.place(
            name,
            FileTree(FileSystemObject::Directory(Directory { entries })),
        )
    }

    fn finish(self) -> Result<FileTree<ChunkList>, Error> {
        if !self.stack.is_empty() {
            return Err(Error::InvalidNar(
                "unclosed directory in event stream".into(),
            ));
        }
        self.root
            .ok_or_else(|| Error::InvalidNar("empty event stream".into()))
    }
}

/// NAR permits arbitrary bytes in entry names and symlink targets, but the
/// manifest stores them as UTF-8 (a representation limit); `what` names the
/// field so the diagnostic points at the right one.
fn name_to_string(name: &[u8], what: &str) -> Result<String, Error> {
    String::from_utf8(name.to_vec())
        .map_err(|_| Error::InvalidNar(format!("non-UTF-8 {what}: {name:?}")))
}

/// Walk `path` with harmonia's NAR dumper, splitting every regular file into
/// content-defined chunks.
///
/// Reference occurrences are normalized out of each file with `refs` before
/// chunking, so chunks stay stable when a dependency's
/// hash changes; the position table needed to restore them is recorded in
/// each file's [`ChunkList::rewrites`]. Pass an empty [`RefTable`] to chunk
/// verbatim.
///
/// Returns the file tree (for the manifest `PathEntry`) plus the unique
/// chunks in pack order. Identical chunks appearing in multiple files are
/// returned once.
pub async fn chunk_path(path: impl Into<PathBuf>, refs: &RefTable) -> Result<ChunkedPath, Error> {
    let mut events = harmonia_file_nar::dump(path.into());
    let mut builder = TreeBuilder::new();
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut seen: BTreeSet<ChunkHash> = BTreeSet::new();

    while let Some(event) = events.next().await {
        match event? {
            NarEvent::StartDirectory { name } => {
                builder.start_directory(name_to_string(&name, "entry name")?);
            }
            NarEvent::EndDirectory => {
                builder.end_directory()?;
            }
            NarEvent::Symlink { name, target } => {
                builder.place(
                    name_to_string(&name, "entry name")?,
                    FileTree(FileSystemObject::Symlink(Symlink {
                        target: name_to_string(&target, "symlink target")?,
                    })),
                )?;
            }
            NarEvent::File {
                name,
                executable,
                size: _,
                reader,
            } => {
                let data = reader.into_bytes();
                let (normalized, rewrites) = refs.normalize(&data);
                let file_chunks = chunk_data(&normalized);
                let list = ChunkList {
                    chunks: file_chunks.iter().map(|chunk| chunk.hash).collect(),
                    rewrites,
                };
                for chunk in file_chunks {
                    if seen.insert(chunk.hash) {
                        chunks.push(chunk);
                    }
                }
                builder.place(
                    name_to_string(&name, "entry name")?,
                    FileTree(FileSystemObject::Regular(Regular {
                        executable,
                        contents: list,
                    })),
                )?;
            }
        }
    }

    Ok(ChunkedPath {
        tree: builder.finish()?,
        chunks,
    })
}

/// NAR hash and size of a path, computed by streaming harmonia's
/// [`NarByteStream`] (an independent walk of the on-disk path) into
/// SHA-256.
///
/// Test-only oracle: gives tests a hash derived without going through the
/// chunk pipeline to compare against. The production by-construction
/// guarantee lives in [`nar_hash_from_chunks`] / [`nar_from_chunks`],
/// which share NAR event synthesis with the serving path.
pub async fn nar_hash_and_size(path: impl Into<PathBuf>) -> Result<(Hash32, u64), Error> {
    let mut stream = NarByteStream::new(path.into());
    let mut context = harmonia_utils_hash::Context::new(harmonia_utils_hash::Algorithm::SHA256);
    let mut size: u64 = 0;
    while let Some(bytes) = stream.next().await {
        let bytes = bytes?;
        context.update(&bytes);
        size += bytes.len() as u64;
    }
    finish_sha256(context, size)
}

/// Extract the SHA-256 digest from a finished hash context.
fn finish_sha256(context: harmonia_utils_hash::Context, size: u64) -> Result<(Hash32, u64), Error> {
    match context.finish() {
        harmonia_utils_hash::Hash::SHA256(sha) => Ok((Hash32(*sha.digest_bytes()), size)),
        other => Err(Error::InvalidNar(format!(
            "hash context returned unexpected algorithm {other:?}"
        ))),
    }
}

/// Flatten a tree into `(relative path, node)` pairs, depth-first.
/// The root node has the empty relative path.
pub fn flatten_tree(tree: &FileTree<ChunkList>) -> Vec<(String, &TreeNode)> {
    fn walk<'tree>(
        prefix: &str,
        tree: &'tree FileTree<ChunkList>,
        out: &mut Vec<(String, &'tree TreeNode)>,
    ) {
        out.push((prefix.to_string(), &tree.0));
        if let FileSystemObject::Directory(directory) = &tree.0 {
            for (name, child) in &directory.entries {
                let child_path = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                };
                walk(&child_path, child, out);
            }
        }
    }
    let mut out = Vec::new();
    walk("", tree, &mut out);
    out
}

// NAR replay (tree + chunk data -> NAR bytes -> hash) is the read-side
// serialization path: the substituter rebuilds NARs from the manifest tree +
// fetched chunks exactly like this. Using it on the write side to compute
// nar_hash means a path's stored representation is proven to reproduce the
// original NAR before anything is uploaded.

/// Tokio `AsyncWrite` sink that hashes and counts bytes instead of storing
/// them.
struct HashSink {
    context: harmonia_utils_hash::Context,
    size: u64,
}

impl HashSink {
    fn new() -> Self {
        Self {
            context: harmonia_utils_hash::Context::new(harmonia_utils_hash::Algorithm::SHA256),
            size: 0,
        }
    }

    fn finish(self) -> Result<(Hash32, u64), Error> {
        finish_sha256(self.context, self.size)
    }
}

impl tokio::io::AsyncWrite for HashSink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        this.context.update(buf);
        this.size += buf.len() as u64;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Synthesize the NAR event sequence for a tree node, depth-first.
///
/// Directory entries come out in `BTreeMap` order, which is the sorted order
/// the NAR format requires (and the order `NarDumper` emits).
fn collect_events(
    name: Bytes,
    node: &TreeNode,
    chunks: &BTreeMap<ChunkHash, Bytes>,
    refs: &RefTable,
    out: &mut Vec<NarEvent<Cursor<Bytes>>>,
) -> Result<(), Error> {
    match node {
        FileSystemObject::Regular(regular) => {
            let rewrites = &regular.contents.rewrites;
            let data = if rewrites.is_empty() {
                // Verbatim (v1 or reference-free): reuse chunk Bytes without
                // copying in the single-chunk common case.
                match regular.contents.chunks.as_slice() {
                    [] => Bytes::new(),
                    [hash] => chunks.get(hash).ok_or(Error::MissingChunk(*hash))?.clone(),
                    hashes => Bytes::from(concat_chunks(hashes, chunks)?),
                }
            } else {
                // Normalized (v2): reference occurrences must be restored
                // before the bytes go back into the NAR.
                let mut data = concat_chunks(&regular.contents.chunks, chunks)?;
                refs.restore(&mut data, rewrites)?;
                Bytes::from(data)
            };
            out.push(NarEvent::File {
                name,
                executable: regular.executable,
                size: data.len() as u64,
                reader: Cursor::new(data),
            });
        }
        FileSystemObject::Symlink(symlink) => {
            out.push(NarEvent::Symlink {
                name,
                target: Bytes::copy_from_slice(symlink.target.as_bytes()),
            });
        }
        FileSystemObject::Directory(directory) => {
            out.push(NarEvent::StartDirectory { name });
            for (child_name, child) in &directory.entries {
                collect_events(
                    Bytes::copy_from_slice(child_name.as_bytes()),
                    &child.0,
                    chunks,
                    refs,
                    out,
                )?;
            }
            out.push(NarEvent::EndDirectory);
        }
    }
    Ok(())
}

/// Concatenate a file's chunk data in order.
fn concat_chunks(
    hashes: &[ChunkHash],
    chunks: &BTreeMap<ChunkHash, Bytes>,
) -> Result<Vec<u8>, Error> {
    let mut data = Vec::new();
    for hash in hashes {
        let chunk = chunks.get(hash).ok_or(Error::MissingChunk(*hash))?;
        data.extend_from_slice(chunk);
    }
    Ok(data)
}

/// NAR hash and size of a path, computed from its *stored representation*
/// (file tree + chunk data) by replaying synthesized events through
/// harmonia's [`NarWriter`].
///
/// The write pipeline compares this against the nar_hash recorded in the Nix
/// database: equality proves the chunked representation reproduces the
/// original NAR byte-identically, so it is safe to upload and serve.
pub async fn nar_hash_from_chunks(
    tree: &FileTree<ChunkList>,
    chunks: &BTreeMap<ChunkHash, Bytes>,
    refs: &RefTable,
) -> Result<(Hash32, u64), Error> {
    let mut sink = HashSink::new();
    write_nar(tree, chunks, refs, &mut sink).await?;
    sink.finish()
}

/// Serialize a complete NAR from a path's *stored representation* (file
/// tree + chunk data).
///
/// This is the read-side code path: the substituter rebuilds NARs from
/// manifest trees plus fetched chunks with exactly this function. It shares
/// the event synthesis with [`nar_hash_from_chunks`], which the write
/// pipeline uses as its integrity gate — so the bytes served here are by
/// construction the bytes whose hash was verified before upload.
pub async fn nar_from_chunks(
    tree: &FileTree<ChunkList>,
    chunks: &BTreeMap<ChunkHash, Bytes>,
    refs: &RefTable,
) -> Result<Vec<u8>, Error> {
    let mut buffer: Vec<u8> = Vec::new();
    write_nar(tree, chunks, refs, &mut buffer).await?;
    Ok(buffer)
}

/// Synthesize the NAR event sequence for `tree` and serialize it through
/// harmonia's [`NarWriter`] into `sink`.
async fn write_nar<W: tokio::io::AsyncWrite + Unpin>(
    tree: &FileTree<ChunkList>,
    chunks: &BTreeMap<ChunkHash, Bytes>,
    refs: &RefTable,
    sink: &mut W,
) -> Result<(), Error> {
    let mut events = Vec::new();
    // The root node's name is ignored by the NAR format (only nested entries
    // carry names), so any value works here.
    collect_events(Bytes::new(), &tree.0, chunks, refs, &mut events)?;

    let mut writer = NarWriter::new(sink);
    for event in events {
        writer.feed(event).await?;
    }
    writer.close().await?;
    Ok(())
}

/// Decompress and verify one chunk extracted from pack bytes.
///
/// `compressed` is the byte slice at `[offset, offset + compressed_size)` of
/// the pack blob — exactly what a Range request against the pack returns.
/// The hash check is mandatory: the GHA cache is not trusted storage and a
/// corrupt chunk must never be served onward.
///
/// Decompression is bounded at [`MAX_CHUNK_SIZE`]: no legitimate chunk can
/// be larger (chunking splits at that bound), so anything bigger is corrupt
/// or malicious pack data and gets rejected *before* its decompressed
/// payload is buffered (zstd ratios above 1000:1 would otherwise let a tiny
/// frame allocate gigabytes).
pub fn extract_chunk(compressed: &[u8], expected: &ChunkHash) -> Result<Vec<u8>, Error> {
    use std::io::Read as _;
    let mut data = Vec::new();
    let mut decoder = zstd::Decoder::with_buffer(compressed)?;
    // Reject oversized declared windows at header parse time: a crafted
    // frame in untrusted pack data could otherwise force a large window
    // allocation (up to 128 MiB with libzstd's default cap) before any
    // output is read.
    decoder.window_log_max(MAX_CHUNK_WINDOW_LOG)?;
    // Read at most one byte past the limit: enough to detect oversize
    // without buffering the full payload.
    decoder
        .take(u64::from(MAX_CHUNK_SIZE) + 1)
        .read_to_end(&mut data)?;
    if data.len() > MAX_CHUNK_SIZE as usize {
        return Err(Error::OversizedChunk);
    }
    let actual = ChunkHash::digest(&data);
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
    fn multi_segment_chunking_covers_and_is_deterministic() {
        // Spans several CHUNK_SEGMENT_SIZE segments, exercising the parallel
        // path: chunks must stay in order, cover the input, and match a
        // second run byte-for-byte (core count must not affect boundaries).
        let data = test_data(CHUNK_SEGMENT_SIZE * 3 + 1234, 99);
        let chunks = chunk_data(&data);
        assert_eq!(chunks, chunk_data(&data));

        let reassembled: Vec<u8> = chunks.iter().flat_map(|c| c.data.to_vec()).collect();
        assert_eq!(reassembled, data.as_ref());

        // Every segment seam forces a cut, so a chunk must end exactly on
        // each interior segment boundary.
        let mut offset = 0u64;
        let ends: std::collections::BTreeSet<u64> = chunks
            .iter()
            .map(|c| {
                offset += c.data.len() as u64;
                offset
            })
            .collect();
        for seam in 1..data.len().div_ceil(CHUNK_SEGMENT_SIZE) {
            assert!(ends.contains(&((seam * CHUNK_SEGMENT_SIZE) as u64)));
        }
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
        assert_eq!(chunks[0].hash, ChunkHash::digest(&small));
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
        assert_eq!(pack.hash, PackHash::digest(&pack.data));
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
    fn add_compressed_copies_frames_byte_identically() {
        // Build a pack the normal way, then rebuild it from its own frames
        // via add_compressed: the result must be byte-identical (this is
        // what makes repacked stable packs deterministic and the CAS no-op
        // trap reasoning sound).
        let chunks = chunk_data(&test_data(300_000, 9));
        let mut builder = PackBuilder::new();
        for chunk in &chunks {
            builder.add(chunk).unwrap();
        }
        let original = builder.finish();

        let mut copier = PackBuilder::new();
        for (hash, packed) in &original.chunks {
            let start = packed.offset as usize;
            let end = start + packed.compressed_size as usize;
            let frame = &original.data[start..end];
            // Verify-then-copy, exactly like GC repack does.
            let decompressed = extract_chunk(frame, hash).unwrap();
            assert!(copier.add_compressed(*hash, frame, decompressed.len() as u32));
            // Duplicate adds are skipped.
            assert!(!copier.add_compressed(*hash, frame, decompressed.len() as u32));
        }
        let copy = copier.finish();
        assert_eq!(copy.data, original.data);
        assert_eq!(copy.hash, original.hash);
        assert_eq!(copy.chunks, original.chunks);
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
    fn extract_chunk_rejects_frames_larger_than_max_chunk_size() {
        // Pack bytes come from the GHA cache, which is not trusted storage.
        // No legitimate chunk exceeds MAX_CHUNK_SIZE (chunk_data splits with
        // that bound), but zstd compresses runs of zeros at ratios well above
        // 1000:1, so a small corrupt or malicious frame can decompress to a
        // huge payload. extract_chunk must reject such frames at the size
        // limit instead of buffering the full decompressed payload in memory
        // (memory amplification in the substituter and GC repack).
        let bomb_payload = vec![0u8; 64 * 1024 * 1024];
        let frame = zstd::encode_all(bomb_payload.as_slice(), 3).unwrap();
        assert!(
            frame.len() < 1024 * 1024,
            "the bomb frame itself must be small, got {} bytes",
            frame.len()
        );

        // Even with the "correct" hash of the oversized payload, extraction
        // must fail: the size bound is enforced, not just integrity.
        let expected = ChunkHash::digest(&bomb_payload);
        assert!(matches!(
            extract_chunk(&frame, &expected),
            Err(Error::OversizedChunk)
        ));
    }

    #[test]
    fn extract_chunk_rejects_oversized_window_declarations() {
        // A crafted frame can declare a huge decoder window in its header,
        // forcing libzstd to allocate it before any output is read. Such
        // frames must fail at header parse time, without the allocation.
        let payload = test_data(1024, 6);
        let mut encoder = zstd::Encoder::new(Vec::new(), 3).unwrap();
        encoder.window_log(MAX_CHUNK_WINDOW_LOG + 3).unwrap();
        // Streaming without a pledged content size forces the frame header
        // to carry the window descriptor.
        std::io::Write::write_all(&mut encoder, &payload).unwrap();
        let frame = encoder.finish().unwrap();

        let expected = ChunkHash::digest(&payload);
        assert!(matches!(
            extract_chunk(&frame, &expected),
            Err(Error::Io(_))
        ));

        // Frames produced the production way (compress_chunks at
        // ZSTD_LEVEL) still extract fine, up to the largest chunk size.
        let big = test_data(MAX_CHUNK_SIZE as usize, 8);
        for data in [payload, big] {
            let expected = ChunkHash::digest(&data);
            let normal = zstd::encode_all(data.as_ref(), ZSTD_LEVEL).unwrap();
            assert_eq!(extract_chunk(&normal, &expected).unwrap(), data);
        }
    }

    #[test]
    fn extract_chunk_detects_corruption() {
        let data = test_data(50_000, 4);
        let chunks = chunk_data(&data);
        let mut builder = PackBuilder::new();
        builder.add(&chunks[0]).unwrap();
        let pack = builder.finish();

        // Wrong expected hash -> HashMismatch.
        let wrong_hash = ChunkHash::digest(b"something else");
        let result = extract_chunk(&pack.data, &wrong_hash);
        assert!(matches!(result, Err(Error::HashMismatch { .. })));

        // Corrupted compressed bytes -> decompression error or hash mismatch,
        // but never silently wrong data.
        let mut corrupted = pack.data.to_vec();
        let middle = corrupted.len() / 2;
        corrupted[middle] ^= 0xff;
        assert!(extract_chunk(&corrupted, &chunks[0].hash).is_err());
    }

    #[tokio::test]
    async fn normalized_path_round_trips_and_dedups() {
        use crate::manifest::StorePath;

        // Two "builds" of the same file differing only in an embedded
        // dependency hash: they must chunk to identical normalized chunks
        // (dedup) yet each reassemble byte-identically to its own original
        // NAR (correctness).
        const DEP_A: &str = "0d71ygfwbmy1xjlbj1v027dfmy9cjm9c";
        const DEP_B: &str = "1a2b3c4d5f6g7h8j9k0lmnpqrsvwxyz1";

        async fn build(dep: &str) -> (ChunkedPath, (Hash32, u64), RefTable) {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("out");
            std::fs::create_dir_all(&root).unwrap();
            // Embed the dependency's store-path hash in a binary-ish blob
            // large enough to span several chunks.
            let mut content = test_data(400_000, 3).to_vec();
            let marker = format!("/nix/store/{dep}-libdep.so");
            content.splice(200_000..200_000, marker.bytes());
            std::fs::write(root.join("prog"), &content).unwrap();

            let reference: StorePath = format!("{dep}-libdep").parse().unwrap();
            let refs = RefTable::new(std::slice::from_ref(&reference));
            let chunked = chunk_path(&root, &refs).await.unwrap();
            let from_disk = nar_hash_and_size(&root).await.unwrap();
            (chunked, from_disk, refs)
        }

        let (a, disk_a, refs_a) = build(DEP_A).await;
        let (b, disk_b, refs_b) = build(DEP_B).await;

        // Correctness: each reassembles to its own original NAR.
        assert_eq!(
            nar_hash_from_chunks(&a.tree, &a.chunk_map(), &refs_a)
                .await
                .unwrap(),
            disk_a
        );
        assert_eq!(
            nar_hash_from_chunks(&b.tree, &b.chunk_map(), &refs_b)
                .await
                .unwrap(),
            disk_b
        );
        assert_ne!(disk_a, disk_b, "the two builds are genuinely different");

        // Dedup: the normalized chunk sets are identical despite the
        // dependency hash differing.
        let set_a: BTreeSet<ChunkHash> = a.chunks.iter().map(|c| c.hash).collect();
        let set_b: BTreeSet<ChunkHash> = b.chunks.iter().map(|c| c.hash).collect();
        assert_eq!(
            set_a, set_b,
            "reference normalization must dedup the chunks"
        );
    }

    #[tokio::test]
    async fn nar_hash_from_chunks_matches_disk_walk() {
        // Build a small tree on disk, chunk it, then verify that replaying
        // the chunked representation produces the same NAR hash as hashing
        // the disk walk directly (NarByteStream).
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("fixture");
        std::fs::create_dir_all(root.join("sub")).unwrap();
        std::fs::write(root.join("small"), b"small file contents\n").unwrap();
        std::fs::write(root.join("empty"), b"").unwrap();
        std::fs::write(root.join("sub/large"), test_data(700_000, 11)).unwrap();
        std::os::unix::fs::symlink("small", root.join("sub/link")).unwrap();

        let chunked = chunk_path(&root, &RefTable::new(&[])).await.unwrap();
        let from_disk = nar_hash_and_size(&root).await.unwrap();
        let from_chunks =
            nar_hash_from_chunks(&chunked.tree, &chunked.chunk_map(), &RefTable::new(&[]))
                .await
                .unwrap();
        assert_eq!(from_chunks, from_disk);
    }

    #[tokio::test]
    async fn nar_hash_from_chunks_single_file_root() {
        // Bare-file NARs (root node is a file, not a directory).
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("single");
        std::fs::write(&file, test_data(200_000, 12)).unwrap();

        let chunked = chunk_path(&file, &RefTable::new(&[])).await.unwrap();
        let from_disk = nar_hash_and_size(&file).await.unwrap();
        let from_chunks =
            nar_hash_from_chunks(&chunked.tree, &chunked.chunk_map(), &RefTable::new(&[]))
                .await
                .unwrap();
        assert_eq!(from_chunks, from_disk);
    }

    #[tokio::test]
    async fn nar_from_chunks_reproduces_disk_serialization() {
        // The substituter's NAR synthesis must produce byte-identical output
        // to harmonia's disk walk (NarByteStream), not just the same hash.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("fixture");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("file"), b"contents\n").unwrap();
        std::fs::write(root.join("nested/blob"), test_data(400_000, 21)).unwrap();
        std::os::unix::fs::symlink("file", root.join("link")).unwrap();

        let chunked = chunk_path(&root, &RefTable::new(&[])).await.unwrap();
        let nar = nar_from_chunks(&chunked.tree, &chunked.chunk_map(), &RefTable::new(&[]))
            .await
            .unwrap();

        let mut from_disk = Vec::new();
        let mut stream = NarByteStream::new(root.clone());
        while let Some(bytes) = stream.next().await {
            from_disk.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(nar, from_disk, "synthesized NAR must match the disk walk");

        // And it agrees with the hash helper used by the write pipeline.
        let (hash, size) =
            nar_hash_from_chunks(&chunked.tree, &chunked.chunk_map(), &RefTable::new(&[]))
                .await
                .unwrap();
        assert_eq!(Hash32::digest(&nar), hash);
        assert_eq!(nar.len() as u64, size);
    }

    #[tokio::test]
    async fn nar_from_chunks_fails_on_missing_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("fixture");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("file"), test_data(100_000, 22)).unwrap();

        let chunked = chunk_path(&root, &RefTable::new(&[])).await.unwrap();
        let mut chunks = chunked.chunk_map();
        chunks.pop_first().unwrap();

        assert!(matches!(
            nar_from_chunks(&chunked.tree, &chunks, &RefTable::new(&[])).await,
            Err(Error::MissingChunk(_))
        ));
    }

    #[tokio::test]
    async fn nar_hash_from_chunks_fails_on_missing_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("fixture");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("file"), test_data(100_000, 13)).unwrap();

        let chunked = chunk_path(&root, &RefTable::new(&[])).await.unwrap();
        let mut chunks = chunked.chunk_map();
        let (missing_hash, _) = chunks.pop_first().unwrap();

        let result = nar_hash_from_chunks(&chunked.tree, &chunks, &RefTable::new(&[])).await;
        match result {
            Err(Error::MissingChunk(hash)) => assert_eq!(hash, missing_hash),
            other => panic!("expected MissingChunk error, got {other:?}"),
        }
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
