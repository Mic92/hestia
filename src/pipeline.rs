//! The write pipeline: store paths → chunks → packs → segment + head.
//!
//! Runs on drain (action post-step or idle-exit). Steps:
//!
//! 1. Query path info from the store database for every buffered path,
//!    expanded to its runtime closure unless disabled.
//! 2. Filter: invalid paths, upstream-signed paths (when the upstream
//!    cache filter is enabled. Derivation closures bypass it unless
//!    explicitly configured otherwise), paths already stored.
//! 3. Chunk each new path (FastCDC over NAR events) and verify the chunked
//!    representation reproduces the NAR hash recorded by Nix.
//! 4. Pack new chunks, upload each pack with its index.
//! 5. Publish everything this job pushed, found stored or substituted as
//!    one segment plus head under this root.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::backend::Backend;
use crate::chunker::{self, PackBuilder, chunk_path, compress_chunks, nar_hash_from_chunks};
use crate::gha::Error as GhaError;
use crate::manifest::{PathEntry, PathHash};
use crate::pathinfo::{Error as PathInfoError, Lookup, PathInfo, StoreDatabase};
use crate::protocol::DrainStats;
use crate::refnorm::RefTable;
use crate::segment::SegmentWriter;
use crate::store::{self, Snapshot};
use crate::substituter::ManifestStore;
use crate::upstream::UpstreamFilter;
use futures_util::{StreamExt as _, TryStreamExt as _};

/// Compressed bytes per pack before a new pack is started.
pub const PACK_TARGET_SIZE: u64 = 64 * 1024 * 1024;

/// How many packs upload concurrently during a drain.
const UPLOAD_CONCURRENCY: usize = 4;

/// Upper bound on paths chunked and NAR-verified concurrently; the actual
/// width is capped at the CPU count.
const CHUNK_CONCURRENCY: usize = 32;

/// Upper bound on the summed NAR size of paths chunked and verified
/// concurrently. The path-count cap alone does not bound memory: a few
/// multi-hundred-MiB paths in flight at once would stack their buffers.
/// Large paths serialize against this budget instead; small paths are
/// unaffected.
const CHUNK_INFLIGHT_NAR_BYTES: u64 = 1024 * 1024 * 1024;

/// Semaphore permits for one path's chunk-and-verify stage: its NAR size,
/// clamped so a path larger than the whole budget still runs (alone).
fn chunk_permits(nar_size: u64) -> u32 {
    nar_size.clamp(1, CHUNK_INFLIGHT_NAR_BYTES) as u32
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("GHA cache error: {0}")]
    Gha(#[from] GhaError),

    #[error("chunking error: {0}")]
    Chunker(#[from] chunker::Error),

    #[error("store database error: {0}")]
    PathInfo(#[from] PathInfoError),

    #[error(transparent)]
    Store(#[from] store::Error),
}

/// Shared record of paths served through the substituter.
///
/// narinfo hits double as the liveness signal: an accessed path joins this
/// run's root even though it was not rebuilt, which keeps it (and its
/// closure) alive across GC. The substituter records hits; the pipeline
/// reads a snapshot at drain time.
///
/// Cloning is cheap (shared state): the daemon hands one clone to the
/// substituter and keeps one for drains.
#[derive(Debug, Default, Clone)]
pub struct AccessLog {
    inner: Arc<Mutex<BTreeSet<PathHash>>>,
}

impl AccessLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a path was served (or asked for and found).
    pub fn record(&self, hash: PathHash) {
        self.inner
            .lock()
            .expect("access log lock poisoned")
            .insert(hash);
    }

    /// All paths accessed so far.
    pub fn snapshot(&self) -> BTreeSet<PathHash> {
        self.inner.lock().expect("access log lock poisoned").clone()
    }
}

/// The Nix system string for the machine hestia runs on
/// (`x86_64-linux`, `aarch64-darwin`, …).
pub fn current_system() -> String {
    let os = match std::env::consts::OS {
        "macos" => "darwin",
        os => os,
    };
    // Rust arch names diverge from Nix system spellings on some platforms;
    // the value defaults the root key, so an unmapped spelling
    // fragments (or collides) GC roots against jobs passing --system with
    // the Nix spelling.
    let arch = match std::env::consts::ARCH {
        "x86" => "i686",
        // Rust reports "arm" for all 32-bit ARM; armv7l is the common
        // case. armv6l hosts must pass --system explicitly.
        "arm" => "armv7l",
        arch => arch,
    };
    format!("{arch}-{os}")
}

/// Root key for a branch + system pair, e.g. `main-x86_64-linux`.
pub fn root_key(branch: &str, system: &str) -> String {
    format!("{branch}-{system}")
}

/// Current unix time in seconds.
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before 1970")
        .as_secs()
}

/// Upload one pack. `false` when the backend already had it. Pack keys
/// are content-addressed, so an existing entry holds identical content.
/// That case touches the existing pack so its LRU clock and GC's age
/// guard see this writer's dependency before the head lands.
pub async fn upload_pack(backend: &Backend, pack: &chunker::Pack) -> Result<bool, GhaError> {
    let key = pack.cache_key();
    let created = backend.put(&key, pack.data.clone()).await?;
    if !created {
        backend.touch(&key).await?;
    }
    Ok(created)
}

/// Everything the pipeline needs to talk to the world.
pub struct PipelineContext {
    pub backend: Backend,
    pub store: StoreDatabase,
    pub upstream: UpstreamFilter,
    /// Expand hooked paths to their runtime closure before pushing.
    /// Substituted dependencies never trigger the post-build-hook, so
    /// without expansion they are never cached.
    pub expand_closure: bool,
    /// Apply the upstream filter to derivation closure members instead of
    /// keeping those closures self-contained.
    pub filter_drv_closures: bool,
    /// Root key, e.g. `main-x86_64-linux`.
    pub root_key: String,
    /// Compressed bytes per pack ([`PACK_TARGET_SIZE`] in production; tests
    /// use small values to exercise pack splitting).
    pub pack_target_size: u64,
    /// The write pipeline is skipped so a drain is a clean no-op. Set by
    /// `serve --read-only`, or by a background probe at startup
    /// ([`crate::serve`]) when the runtime token has no writable cache
    /// scope (`check_run`, fork `pull_request`) and the first reservation
    /// would fail anyway.
    pub read_only: Arc<AtomicBool>,
    /// Where the published segment is handed to the substituter, so the
    /// paths this drain pushed are served even while listings lag.
    pub publish: Option<ManifestStore>,
}

/// A path that chunked and passed NAR verification.
struct ReadyPath {
    info: PathInfo,
    chunked: chunker::ChunkedPath,
    nar_hash: crate::manifest::NarHash,
    nar_size: u64,
    elapsed: std::time::Duration,
}

/// Result of the concurrent chunk-and-verify stage for one path.
enum Verified {
    // Boxed: far larger than the failure variants.
    Ready(Box<ReadyPath>),
    ChunkFailed,
    VerifyFailed,
}

impl PipelineContext {
    /// Run the write pipeline.
    ///
    /// `paths`: absolute store paths buffered from hooks.
    /// `accessed`: path hashes recorded by the substituter ([`AccessLog`]).
    pub async fn run(
        &self,
        paths: BTreeSet<String>,
        accessed: BTreeSet<PathHash>,
    ) -> Result<DrainStats, Error> {
        let mut stats = DrainStats {
            paths_received: paths.len(),
            ..DrainStats::default()
        };

        if paths.is_empty() && accessed.is_empty() {
            return Ok(stats);
        }

        if self.read_only.load(Ordering::Relaxed) {
            return Ok(stats);
        }

        let load_started = std::time::Instant::now();
        let snapshot = match self.publish.as_ref().and_then(ManifestStore::snapshot) {
            Some(s) => s,
            None => Arc::new(
                Snapshot::load(
                    self.backend.clone(),
                    std::slice::from_ref(&self.root_key),
                    None,
                )
                .await?,
            ),
        };
        // Blocking sqlite I/O happens off the async runtime.
        let store = self.store.clone();
        let expand_closure = self.expand_closure;
        let filter_drv_closures = self.filter_drv_closures;
        let (lookups, upstream_filter_bypass) = tokio::task::spawn_blocking(move || {
            let bypass_roots: BTreeSet<String> = if expand_closure && !filter_drv_closures {
                paths
                    .iter()
                    .filter(|path| path.ends_with(".drv"))
                    .cloned()
                    .collect()
            } else {
                BTreeSet::new()
            };
            let lookups = if expand_closure {
                store.query_closure(paths)?
            } else {
                store.query_batch(paths)?
            };
            let bypass: BTreeSet<String> = store
                .query_closure(bypass_roots)?
                .into_iter()
                .map(|(path, _)| path)
                .collect();
            Ok::<_, PathInfoError>((lookups, bypass))
        })
        .await
        .expect("store database query task panicked")?;

        let mut root_paths: BTreeSet<PathHash> = accessed;
        // Paths that need chunking + upload.
        let mut to_push: Vec<(String, PathInfo)> = Vec::new();

        for (path, lookup) in lookups {
            let info = match lookup {
                Lookup::Found(info) => *info,
                Lookup::Unknown => {
                    eprintln!("hestia: skipping {path}: not a valid path in the local store");
                    stats.skipped_invalid += 1;
                    continue;
                }
                Lookup::Malformed { reason } => {
                    eprintln!("hestia: skipping {path}: {reason}");
                    stats.skipped_invalid += 1;
                    continue;
                }
            };

            if !upstream_filter_bypass.contains(&path)
                && self.upstream.is_upstream_signed(&info.signatures)
            {
                stats.skipped_upstream += 1;
                continue;
            }

            let hash = info.path_hash();

            if snapshot.contains(&hash) {
                root_paths.insert(hash);
                stats.skipped_existing += 1;
                continue;
            }

            to_push.push((path, info));
        }
        // A rebuild mostly repeats the chunks of the previous build under
        // the same name, so those pack indexes are worth loading.
        let names = to_push
            .iter()
            .map(|(_, i)| i.store_path.name().as_ref())
            .collect();
        snapshot.load_indexes_for(&names).await?;
        let mut known_chunks = snapshot.known_chunks();

        stats.load_ms = load_started.elapsed().as_millis() as u64;

        // Three stages joined below, each feeding the next over a bounded
        // channel: prepare (chunk + verify concurrently, then dedup),
        // pack (compress concurrently, then seal packs), upload. The
        // CPU-heavy chunk/verify and compress steps run across cores; the
        // dedup and packing glue is serial but cheap.
        let concurrency = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(CHUNK_CONCURRENCY);
        let (chunks_tx, chunks_rx) = tokio::sync::mpsc::channel::<Vec<chunker::Chunk>>(concurrency);
        let (pack_tx, pack_rx) = tokio::sync::mpsc::channel::<chunker::Pack>(2);

        let prepare = async {
            let mut prepared: Vec<PathEntry> = Vec::new();
            // Summed as a Duration, converted once: per-path as_millis()
            // truncation would underreport drains of many small paths.
            let mut chunk_time = std::time::Duration::ZERO;
            let mut failed_chunking = 0usize;
            let mut failed_verification = 0usize;
            // Chunks already emitted for this batch (cross-path dedup).
            let mut batch_chunks: BTreeSet<crate::manifest::ChunkHash> = BTreeSet::new();

            // Per-path work is single-threaded, so running several at once
            // is what fills the cores. Chunking or verification failures are
            // skipped, not propagated: a pipeline error would re-buffer the
            // whole batch, and a deterministic failure would then keep every
            // later drain (including the shutdown drain) from caching
            // anything.
            let inflight = Arc::new(tokio::sync::Semaphore::new(
                CHUNK_INFLIGHT_NAR_BYTES as usize,
            ));
            let mut verified = futures_util::stream::iter(to_push)
                .map(|(path, info)| {
                    let inflight = inflight.clone();
                    tokio::spawn(async move {
                        let _permit = inflight
                            .acquire_many(chunk_permits(info.nar_size))
                            .await
                            .expect("in-flight NAR byte semaphore is never closed");
                        let started = std::time::Instant::now();
                        // The path's own references drive both normalization
                        // (so chunks stay stable across dependency-hash
                        // changes) and the read-side restore.
                        let refs = RefTable::new(&info.references);
                        let chunked = match chunk_path(&path, &refs).await {
                            Ok(chunked) => chunked,
                            Err(err) => {
                                eprintln!("hestia: NOT uploading {path}: chunking failed: {err}");
                                return Verified::ChunkFailed;
                            }
                        };
                        let chunk_map = chunked.chunk_map();
                        // Integrity gate: the chunked representation must
                        // reproduce the NAR hash Nix recorded. A mismatch
                        // means hestia would serve corrupt data; never upload.
                        let (nar_hash, nar_size) =
                            match nar_hash_from_chunks(&chunked.tree, &chunk_map, &refs).await {
                                Ok(result) => result,
                                Err(err) => {
                                    eprintln!(
                                        "hestia: NOT uploading {path}: NAR replay failed: {err}"
                                    );
                                    return Verified::ChunkFailed;
                                }
                            };
                        if nar_hash != info.nar_hash || nar_size != info.nar_size {
                            eprintln!(
                                "hestia: NOT uploading {path}: chunked NAR hash {nar_hash} (size \
                                 {nar_size}) does not match the store's record {} (size {}); \
                                 this indicates a chunker bug or store corruption",
                                info.nar_hash, info.nar_size
                            );
                            return Verified::VerifyFailed;
                        }
                        Verified::Ready(Box::new(ReadyPath {
                            info,
                            chunked,
                            nar_hash,
                            nar_size,
                            elapsed: started.elapsed(),
                        }))
                    })
                })
                .buffer_unordered(concurrency);

            while let Some(joined) = verified.next().await {
                let ready = match joined.expect("chunk task panicked") {
                    Verified::Ready(ready) => ready,
                    Verified::ChunkFailed => {
                        failed_chunking += 1;
                        continue;
                    }
                    Verified::VerifyFailed => {
                        failed_verification += 1;
                        continue;
                    }
                };
                let ReadyPath {
                    info,
                    chunked,
                    nar_hash,
                    nar_size,
                    elapsed,
                } = *ready;
                chunk_time += elapsed;

                let new_chunks: Vec<chunker::Chunk> = chunked
                    .chunks
                    .into_iter()
                    .filter(|chunk| {
                        !known_chunks.contains(&chunk.hash) && batch_chunks.insert(chunk.hash)
                    })
                    .collect();

                prepared.push(PathEntry {
                    // Verbatim, including any self-reference: this list
                    // becomes the narinfo References line, and stripping
                    // self would diverge substituted clients' store
                    // metadata from the builder's.
                    references: info.references,
                    store_path: info.store_path,
                    nar_hash,
                    nar_size,
                    ca: info.ca,
                    deriver: info.deriver,
                    tree: chunked.tree,
                });

                if !new_chunks.is_empty() && chunks_tx.send(new_chunks).await.is_err() {
                    // Packer gone: it failed, and try_join below reports its
                    // error; stop producing.
                    break;
                }
            }
            drop(chunks_tx);
            Ok::<_, Error>((prepared, chunk_time, failed_chunking, failed_verification))
        };

        let pack = async {
            let mut pack_time = std::time::Duration::ZERO;
            let mut builder = PackBuilder::new();
            // Compress paths' new-chunk sets concurrently; frames arrive out
            // of order, which is fine -- packs are content-addressed.
            let chunk_stream = futures_util::stream::unfold(chunks_rx, |mut rx| async move {
                rx.recv().await.map(|chunks| (chunks, rx))
            });
            let compressed = chunk_stream
                .map(|new_chunks| tokio::task::spawn_blocking(move || compress_chunks(new_chunks)))
                .buffer_unordered(concurrency);
            tokio::pin!(compressed);

            'pack: while let Some(joined) = compressed.next().await {
                let frames = joined.expect("compression task panicked")?;
                let mut pack_started = std::time::Instant::now();
                for frame in frames {
                    builder.add_compressed(frame.hash, &frame.frame, frame.uncompressed_size);
                    if builder.compressed_size() >= self.pack_target_size {
                        let sealed = std::mem::take(&mut builder).finish();
                        // Pause the pack timer across the send: a full
                        // channel blocks on upload backpressure, which must
                        // not be booked as packing time.
                        pack_time += pack_started.elapsed();
                        if pack_tx.send(sealed).await.is_err() {
                            break 'pack;
                        }
                        pack_started = std::time::Instant::now();
                    }
                }
                pack_time += pack_started.elapsed();
            }
            if !builder.is_empty() {
                let _ = pack_tx.send(builder.finish()).await;
            }
            // pack_tx drops here, ending the uploader's stream.
            drop(pack_tx);
            Ok::<_, Error>(pack_time)
        };

        let upload_started = std::time::Instant::now();
        let consumer = async {
            let pack_stream = futures_util::stream::unfold(pack_rx, |mut rx| async move {
                rx.recv().await.map(|pack| (pack, rx))
            });
            pack_stream
                .map(|mut pack| async move {
                    let uploaded = upload_pack(&self.backend, &pack).await?;
                    // Only metadata is read after upload; dropping the blob
                    // here keeps peak memory bounded by the in-flight packs
                    // instead of growing with the drain's total compressed
                    // size.
                    let size = pack.data.len() as u64;
                    pack.data = bytes::Bytes::new();
                    Ok::<_, Error>((uploaded, size, pack))
                })
                .buffer_unordered(UPLOAD_CONCURRENCY)
                .try_collect::<Vec<(bool, u64, chunker::Pack)>>()
                .await
        };

        let ((prepared, chunk_time, failed_chunking, failed_verification), pack_time, uploads) =
            tokio::try_join!(prepare, pack, consumer)?;
        stats.failed_chunking += failed_chunking;
        stats.failed_verification += failed_verification;
        stats.chunk_ms = chunk_time.as_millis() as u64;
        stats.pack_ms = pack_time.as_millis() as u64;
        // Stage times overlap now: chunk/pack are producer busy times,
        // upload is the wall time of the whole pipelined section.
        stats.upload_ms = upload_started.elapsed().as_millis() as u64;

        for (uploaded, size, pack) in uploads {
            if uploaded {
                stats.packs_uploaded += 1;
                stats.bytes_uploaded += size;
            }
            stats.new_chunks += pack.chunks.len();
            known_chunks.add(pack.hash, &pack.index());
        }

        // The segment is this drain's whole claim on the root: pushed,
        // already stored, and substituted paths. GC keeps what drains since
        // its last run named and drops the rest of the root.
        let mut writer = SegmentWriter::default();
        for entry in &prepared {
            store::push_entry(&mut writer, entry, &known_chunks)
                .expect("every chunk is either known or in a pack of this drain");
        }
        stats.pushed = prepared.len();
        for hash in &root_paths {
            if !writer.contains(hash) {
                snapshot.copy_entry(hash, &mut writer).await?;
            }
        }
        if writer.is_empty() {
            return Ok(stats);
        }
        let commit_started = std::time::Instant::now();
        let sealed = writer.seal().map_err(store::Error::from)?;
        stats.head =
            Some(store::publish(&self.backend, &snapshot.view, &self.root_key, &sealed).await?);
        stats.commit_ms = commit_started.elapsed().as_millis() as u64;
        let next = match snapshot.refresh_with(&sealed).await {
            Ok(next) => next,
            Err(err) => {
                eprintln!("hestia: cannot refresh the served segments: {err}");
                return Ok(stats);
            }
        };
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        match next
            .maybe_compact(&self.root_key, now_unix(), f64::from(nanos) / 1e9)
            .await
        {
            Ok(Some(name)) => eprintln!("hestia: compacted {} into {name}", self.root_key),
            Ok(None) => {}
            Err(err) => eprintln!("hestia: compaction skipped: {err}"),
        }
        if let Some(publish) = &self.publish {
            publish.set_snapshot(Arc::new(next));
        }
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_system_matches_nix_convention() {
        // Assert the arch-os shape rather than enumerating blessed values:
        // the function must work on any host the binary is built for.
        let system = current_system();
        let (arch, os) = system.split_once('-').expect("system has arch-os form");
        assert!(!arch.is_empty() && !os.is_empty(), "system: {system}");
        assert!(!["x86", "arm", "macos"].contains(&arch), "arch: {arch}");
        assert_ne!(os, "macos", "os must use the Nix spelling");
    }

    #[test]
    fn chunk_permits_clamp_to_the_budget() {
        assert_eq!(chunk_permits(0), 1);
        assert_eq!(chunk_permits(4096), 4096);
        // A path bigger than the whole budget must still get permits it can
        // actually acquire (it runs alone).
        assert_eq!(u64::from(chunk_permits(u64::MAX)), CHUNK_INFLIGHT_NAR_BYTES);
    }

    #[test]
    fn root_key_layout() {
        assert_eq!(root_key("main", "x86_64-linux"), "main-x86_64-linux");
        assert_eq!(
            root_key("feature/foo", "aarch64-darwin"),
            "feature/foo-aarch64-darwin"
        );
    }

    #[test]
    fn access_log_is_shared_between_clones() {
        let log = AccessLog::new();
        let clone = log.clone();
        assert!(log.snapshot().is_empty());

        let hash: PathHash = "00000000000000000000000000000000"
            .parse()
            .expect("valid path hash");
        clone.record(hash);

        assert_eq!(log.snapshot(), BTreeSet::from([hash]));
        // Recording the same hash twice is idempotent.
        log.record(hash);
        assert_eq!(log.snapshot().len(), 1);
    }
}
