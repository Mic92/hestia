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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::stream;
use futures_util::{StreamExt as _, TryStreamExt as _};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::spawn_blocking;

use crate::backend::Backend;
use crate::chunker::{
    self, Chunk, MAX_CHUNK_SIZE, Pack, PackBuilder, compress_chunks, ingest_path,
};
use crate::gha::Error as GhaError;
use crate::manifest::{ChunkHash, ChunkList, FileTree, NarHash, PathEntry, PathHash};
use crate::pathinfo::{Error as PathInfoError, Lookup, PathInfo, StoreDatabase};
use crate::protocol::DrainStats;
use crate::refnorm::RefTable;
use crate::segment::SegmentWriter;
use crate::store::{self, Snapshot};
use crate::substituter::ManifestStore;
use crate::trust::Trust;
use crate::upstream::UpstreamFilter;

/// Compressed bytes per pack before a new pack is started.
pub const PACK_TARGET_SIZE: u64 = 64 * 1024 * 1024;

/// How many packs upload concurrently during a drain.
const UPLOAD_CONCURRENCY: usize = 4;

/// Upper bound on paths chunked and NAR-verified concurrently; the actual
/// width is capped at the CPU count.
const CHUNK_CONCURRENCY: usize = 32;

/// Budget for bytes in flight: files being read and chunked, and chunk
/// batches from compression until packed.
const CHUNK_INFLIGHT_NAR_BYTES: u64 = 512 * 1024 * 1024;
/// A file's new chunks go to the compressor in batches of at most this.
const COMPRESS_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Clamped so a file larger than the budget still runs (alone).
fn chunk_permits(size: u64) -> u32 {
    size.clamp(1, CHUNK_INFLIGHT_NAR_BYTES) as u32
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
pub async fn upload_pack(backend: &Backend, pack: &Pack) -> Result<bool, GhaError> {
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
    pub trust: Trust,
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
    /// Unix seconds for head names.
    pub clock: Clock,
}

pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub fn system_clock() -> Clock {
    Arc::new(now_unix)
}

/// A path that chunked and passed NAR verification.
struct ReadyPath {
    info: PathInfo,
    tree: FileTree<ChunkList>,
    nar_hash: NarHash,
    nar_size: u64,
    elapsed: Duration,
}

/// Result of the concurrent chunk-and-verify stage for one path.
enum Verified {
    // Boxed: far larger than the failure variants.
    Ready(Box<ReadyPath>),
    ChunkFailed,
    VerifyFailed,
}

/// Ends a path's ingest early when the packer is gone.
enum IngestError {
    Chunk(chunker::Error),
    PackerGone,
}

impl From<chunker::Error> for IngestError {
    fn from(e: chunker::Error) -> Self {
        IngestError::Chunk(e)
    }
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

        let load_started = Instant::now();
        // Relisted every drain: a GC since the last one may have retired
        // segments the served snapshot still names. Their bodies are reused.
        let previous = self.publish.as_ref().and_then(ManifestStore::snapshot);
        let roots = previous
            .as_ref()
            .map_or_else(|| vec![self.root_key.clone()], |p| p.roots.clone());
        let snapshot = Arc::new(
            Snapshot::load(
                self.backend.clone(),
                self.trust.clone(),
                &roots,
                previous.as_deref(),
            )
            .await?,
        );
        // Blocking sqlite I/O happens off the async runtime.
        let store = self.store.clone();
        let expand_closure = self.expand_closure;
        let filter_drv_closures = self.filter_drv_closures;
        let (lookups, upstream_filter_bypass) = spawn_blocking(move || {
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

            // A path whose pack the substituter saw evicted goes up again.
            let cached = match &self.publish {
                Some(p) => p.available(&hash).await,
                None => snapshot.contains(&hash),
            };
            if cached {
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
        let known_chunks = Arc::new(snapshot.known_chunks());

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
        let (chunks_tx, chunks_rx) =
            mpsc::channel::<(Vec<Chunk>, OwnedSemaphorePermit)>(concurrency);
        let (pack_tx, pack_rx) = mpsc::channel::<Pack>(2);

        let prepare = async {
            let mut prepared: Vec<PathEntry> = Vec::new();
            // Summed as a Duration, converted once: per-path as_millis()
            // truncation would underreport drains of many small paths.
            let mut chunk_time = Duration::ZERO;
            let mut failed_chunking = 0usize;
            let mut failed_verification = 0usize;
            // Chunks already emitted for this batch (cross-path dedup).
            let batch_chunks: Arc<Mutex<BTreeSet<ChunkHash>>> = Arc::default();
            let inflight = Arc::new(Semaphore::new(CHUNK_INFLIGHT_NAR_BYTES as usize));

            // Several paths at once fill the cores; each file's new chunks
            // leave for the compressor as soon as they are cut. Failures are
            // skipped, not propagated: a pipeline error would re-buffer the
            // whole batch, and a deterministic one would then block every
            // later drain. A path failing verification has already fed
            // chunks into packs: dead weight, never referenced.
            let shared = (inflight, known_chunks.clone(), batch_chunks, chunks_tx);
            let mut verified = stream::iter(to_push)
                .map(move |(path, info)| {
                    let (inflight, known_chunks, batch_chunks, chunks_tx) = shared.clone();
                    tokio::spawn(async move {
                        let started = Instant::now();
                        // The path's own references drive both normalization
                        // (so chunks stay stable across dependency-hash
                        // changes) and the read-side restore.
                        let refs = RefTable::new(&info.references);
                        let reserve = |size: u64| {
                            let inflight = inflight.clone();
                            async move {
                                inflight
                                    .acquire_many_owned(chunk_permits(size))
                                    .await
                                    .expect("in-flight byte semaphore is never closed")
                            }
                        };
                        let emit = |file_chunks: Vec<Chunk>, reading: OwnedSemaphorePermit| {
                            let (inflight, known_chunks, batch_chunks, chunks_tx) =
                                (&inflight, &known_chunks, &batch_chunks, &chunks_tx);
                            async move {
                                let mut new: Vec<Chunk> = {
                                    let mut batch = batch_chunks.lock().unwrap();
                                    file_chunks
                                        .into_iter()
                                        .filter(|c| {
                                            !known_chunks.contains(&c.hash) && batch.insert(c.hash)
                                        })
                                        .collect()
                                };
                                // Batches take their own share until packed; a
                                // budget-sized file must not starve them.
                                drop(reading);
                                while !new.is_empty() {
                                    let mut bytes = 0;
                                    let n = new
                                        .iter()
                                        .take_while(|c| {
                                            bytes += c.data.len();
                                            bytes <= COMPRESS_BATCH_BYTES
                                        })
                                        .count()
                                        .max(1);
                                    let rest = new.split_off(n);
                                    let bytes: usize = new.iter().map(|c| c.data.len()).sum();
                                    let permit = inflight
                                        .clone()
                                        .acquire_many_owned(chunk_permits(bytes as u64))
                                        .await
                                        .expect("in-flight byte semaphore is never closed");
                                    chunks_tx
                                        .send((new, permit))
                                        .await
                                        .map_err(|_| IngestError::PackerGone)?;
                                    new = rest;
                                }
                                Ok(())
                            }
                        };
                        let ingested = match ingest_path(&path, &refs, reserve, emit).await {
                            Ok(ingested) => ingested,
                            Err(IngestError::PackerGone) => return Verified::ChunkFailed,
                            Err(IngestError::Chunk(err)) => {
                                eprintln!("hestia: NOT uploading {path}: chunking failed: {err}");
                                return Verified::ChunkFailed;
                            }
                        };
                        // Integrity gate: the chunked representation must
                        // reproduce the NAR hash Nix recorded.
                        let nar_hash = ingested.nar_hash;
                        if nar_hash != info.nar_hash || ingested.nar_size != info.nar_size {
                            eprintln!(
                                "hestia: NOT uploading {path}: chunked NAR hash {nar_hash} (size \
                                 {}) does not match the store's record {} (size {}); \
                                 this indicates a chunker bug or store corruption",
                                ingested.nar_size, info.nar_hash, info.nar_size
                            );
                            return Verified::VerifyFailed;
                        }
                        Verified::Ready(Box::new(ReadyPath {
                            nar_size: ingested.nar_size,
                            tree: ingested.tree,
                            info,
                            nar_hash,
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
                chunk_time += ready.elapsed;
                prepared.push(PathEntry {
                    // Verbatim, including any self-reference: this list
                    // becomes the narinfo References line, and stripping
                    // self would diverge substituted clients' store
                    // metadata from the builder's.
                    references: ready.info.references,
                    store_path: ready.info.store_path,
                    nar_hash: ready.nar_hash,
                    nar_size: ready.nar_size,
                    ca: ready.info.ca,
                    deriver: ready.info.deriver,
                    realises: ready.info.realises,
                    tree: ready.tree,
                });
            }
            Ok::<_, Error>((prepared, chunk_time, failed_chunking, failed_verification))
        };

        let pack = async {
            let mut pack_time = Duration::ZERO;
            let new_builder = || {
                PackBuilder::with_capacity(self.pack_target_size as usize + MAX_CHUNK_SIZE as usize)
            };
            let mut builder = new_builder();
            // Compress paths' new-chunk sets concurrently; frames arrive out
            // of order, which is fine -- packs are content-addressed.
            let chunk_stream = stream::unfold(chunks_rx, |mut rx| async move {
                rx.recv().await.map(|chunks| (chunks, rx))
            });
            let compressed = chunk_stream
                .map(|(new_chunks, permit)| {
                    spawn_blocking(move || Ok::<_, Error>((compress_chunks(new_chunks)?, permit)))
                })
                .buffer_unordered(concurrency);
            tokio::pin!(compressed);

            'pack: while let Some(joined) = compressed.next().await {
                // The permit goes once the frames sit in a pack buffer.
                let (frames, _permit) = joined.expect("compression task panicked")?;
                let mut pack_started = Instant::now();
                for frame in frames {
                    builder.add_compressed(frame.hash, &frame.frame, frame.uncompressed_size);
                    if builder.compressed_size() >= self.pack_target_size {
                        let sealed = std::mem::replace(&mut builder, new_builder()).finish();
                        // Pause the pack timer across the send: a full
                        // channel blocks on upload backpressure, which must
                        // not be booked as packing time.
                        pack_time += pack_started.elapsed();
                        if pack_tx.send(sealed).await.is_err() {
                            break 'pack;
                        }
                        pack_started = Instant::now();
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

        let upload_started = Instant::now();
        let consumer = async {
            let pack_stream = stream::unfold(pack_rx, |mut rx| async move {
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
                    pack.data = Bytes::new();
                    Ok::<_, Error>((uploaded, size, pack))
                })
                .buffer_unordered(UPLOAD_CONCURRENCY)
                .try_collect::<Vec<(bool, u64, Pack)>>()
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

        let mut known_chunks = Arc::into_inner(known_chunks).expect("prepare stage done");
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
        let commit_started = Instant::now();
        let sealed = writer.seal().map_err(store::Error::from)?;
        let now = (self.clock)();
        stats.head = Some(
            store::publish(
                &self.backend,
                &self.trust,
                &snapshot.view,
                &self.root_key,
                &sealed,
                now,
            )
            .await?,
        );
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
            .maybe_compact(&self.root_key, now, f64::from(nanos) / 1e9)
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
