//! The write pipeline: store paths → chunks → packs → manifest commit.
//!
//! Runs on drain (action post-step or idle-exit). Steps:
//!
//! 1. Query path info from the store database for every buffered path,
//!    expanded to its runtime closure unless disabled.
//! 2. Filter: invalid paths, upstream-signed paths (when the upstream
//!    cache filter is enabled), paths already in the manifest (those get
//!    their `last_pushed` clock bumped instead).
//! 3. Chunk each new path (FastCDC over NAR events) and verify the chunked
//!    representation reproduces the NAR hash recorded by Nix.
//! 4. Pack new chunks, upload the pack (Twirp reserve → Azure PUT →
//!    finalize; `already_exists` means an identical pack is already there).
//! 5. Commit the manifest: new path entries, chunk locations, pack ref, and
//!    the root for this branch+system = pushed ∪ accessed paths.
//!    SaveMutable handles write conflicts by re-merging.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::chunker::{self, PackBuilder, chunk_path, compress_chunks, nar_hash_from_chunks};
use crate::gha::Error as GhaError;
use crate::gha::blob;
use crate::gha::savemutable::SaveMutable;
use crate::gha::twirp::{DownloadUrl, Reservation, TwirpClient};
use crate::manifest::{Manifest, PackInfo, PathEntry, PathHash, Root};
use crate::pathinfo::{Error as PathInfoError, Lookup, PathInfo, StoreDatabase};
use crate::protocol::DrainStats;
use crate::refnorm::RefTable;
use crate::substituter::ManifestStore;
use crate::upstream::UpstreamFilter;
use futures_util::{StreamExt as _, TryStreamExt as _};

/// SaveMutable family prefix for the manifest ("m2" → keys `m2#1`, `m2#2`,
/// …). Bumped from "m" when reference normalization changed the chunk
/// format: the pre-existing `m#N` sequence chunked NARs verbatim, so those
/// chunks would never dedup against normalized ones. A fresh namespace
/// ignores the old manifest outright rather than carrying dead entries
/// forward; its orphaned packs age out through GC and eviction.
pub const MANIFEST_PREFIX: &str = "m2";

/// Compressed bytes per pack before a new pack is started.
pub const PACK_TARGET_SIZE: u64 = 64 * 1024 * 1024;

/// How many packs upload concurrently during a drain.
const UPLOAD_CONCURRENCY: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("GHA cache error: {0}")]
    Gha(#[from] GhaError),

    #[error("chunking error: {0}")]
    Chunker(#[from] chunker::Error),

    #[error("manifest error: {0}")]
    Manifest(#[from] crate::manifest::Error),

    #[error("store database error: {0}")]
    PathInfo(#[from] PathInfoError),
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
    // the value defaults the manifest root key, so an unmapped spelling
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

/// Manifest root key for a branch + system pair, e.g. `main-x86_64-linux`.
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

/// Decode a stored manifest blob, falling back to an empty manifest when
/// the blob is corrupt (truncated upload, garbage data, eviction race).
///
/// A corrupt manifest must never make the daemon fail: every drain and
/// every substituter lookup goes through it, so failing here would break
/// caching for the repository until someone deletes the entry by hand.
/// Starting from an empty manifest instead means cache misses (paths get
/// rebuilt and re-pushed) and the next commit overwrites the corrupt
/// version — self-healing, never CI-breaking.
pub fn decode_manifest_or_empty(data: &[u8]) -> Manifest {
    match Manifest::decode(data) {
        Ok(manifest) => manifest,
        Err(err) => {
            eprintln!(
                "hestia: stored manifest is corrupt ({err}); starting from an empty manifest \
                 (previously cached paths will be rebuilt and re-pushed)"
            );
            Manifest::new()
        }
    }
}

/// Upload one pack blob (Twirp reserve → Azure PUT → finalize); shared by
/// the write pipeline and GC repack. Returns
/// `false` when the cache already has it: pack keys are content-addressed,
/// so an existing entry is guaranteed to hold identical content.
///
/// The no-op case touches the existing pack (1-byte Range read): the
/// caller now depends on an entry it never transferred, and the touch
/// resets the LRU clock and makes the dependency visible to GC's recency
/// guard — without it, a concurrent GC could delete the pack before this
/// writer commits the manifest referencing it (see docs/gc.als).
pub async fn upload_pack(
    twirp: &TwirpClient,
    http: &reqwest::Client,
    pack: &chunker::Pack,
) -> Result<bool, GhaError> {
    let key = pack.cache_key();

    match twirp.create_cache_entry(&key).await? {
        Reservation::AlreadyExists => {
            // A Miss means the entry vanished between reserve and lookup
            // (evicted); nothing to touch — the commit still goes through
            // and the next GC heals the path.
            if let DownloadUrl::Hit { url, .. } = twirp.get_download_url(&key, &[]).await? {
                blob::get(http, &url, Some(0..1)).await?;
            }
            Ok(false)
        }
        Reservation::Created { upload_url } => {
            twirp
                .upload_and_finalize(http, &key, upload_url, pack.data.clone())
                .await?;
            Ok(true)
        }
    }
}

/// Everything the pipeline needs to talk to the world.
pub struct PipelineContext {
    pub twirp: TwirpClient,
    pub http: reqwest::Client,
    pub store: StoreDatabase,
    pub upstream: UpstreamFilter,
    /// Expand hooked paths to their runtime closure before pushing.
    /// Substituted dependencies never trigger the post-build-hook, so
    /// without expansion they are never cached.
    pub expand_closure: bool,
    /// Manifest root key, e.g. `main-x86_64-linux`.
    pub root_key: String,
    /// Workflow run id (`$GITHUB_RUN_ID`); see [`Root::merge`].
    pub run_id: Option<String>,
    /// SaveMutable family prefix (always [`MANIFEST_PREFIX`] in production;
    /// tests use distinct prefixes to isolate scenarios).
    pub manifest_prefix: String,
    /// Compressed bytes per pack ([`PACK_TARGET_SIZE`] in production; tests
    /// use small values to exercise pack splitting).
    pub pack_target_size: u64,
    /// Where committed manifests are published for the substituter.
    ///
    /// Read-your-writes: the cache service's lookups are eventually
    /// consistent, so re-loading the manifest right after a commit can
    /// return a stale version that misses the paths this very drain just
    /// pushed. Publishing the committed manifest directly guarantees the
    /// substituter can serve them immediately.
    pub publish: Option<ManifestStore>,
}

/// One new path, fully prepared for upload.
struct PreparedPath {
    hash: PathHash,
    entry: PathEntry,
}

impl PipelineContext {
    fn save_mutable(&self) -> SaveMutable<'_> {
        SaveMutable::new(&self.twirp, &self.http, &self.manifest_prefix)
    }

    /// Load the current manifest, or an empty one if none exists yet or
    /// the stored blob is corrupt (see [`decode_manifest_or_empty`]).
    pub async fn load_manifest(&self) -> Result<Manifest, Error> {
        Ok(self.load_manifest_versioned().await?.1)
    }

    /// Like [`Self::load_manifest`], but also returns the SaveMutable index
    /// the manifest was loaded from (0 when none exists yet).
    pub async fn load_manifest_versioned(&self) -> Result<(u64, Manifest), Error> {
        match self.save_mutable().load().await? {
            Some(entry) => Ok((entry.index, decode_manifest_or_empty(&entry.data))),
            None => Ok((0, Manifest::new())),
        }
    }

    /// Run the write pipeline.
    ///
    /// `paths`: absolute store paths buffered from hooks.
    /// `accessed`: path hashes recorded by the substituter ([`AccessLog`]).
    /// `now`: unix timestamp for all clocks written by this run.
    pub async fn run(
        &self,
        paths: BTreeSet<String>,
        accessed: BTreeSet<PathHash>,
        now: u64,
    ) -> Result<DrainStats, Error> {
        let mut stats = DrainStats {
            paths_received: paths.len(),
            ..DrainStats::default()
        };

        if paths.is_empty() && accessed.is_empty() {
            return Ok(stats);
        }

        let load_started = std::time::Instant::now();
        let (loaded_version, loaded) = self.load_manifest_versioned().await?;

        // Read-your-writes: cache lookups may lag behind this daemon's own
        // commits, so fold in the manifest we are currently serving (it is
        // at least as new as anything we wrote).
        let (known_version, known) = match &self.publish {
            Some(store) => store.versioned(),
            None => (0, Manifest::new()),
        };
        // `current` is the basis for every dedup decision below; the commit
        // at the end must include all of it (see the merge closure).
        let current = loaded.merge(known);
        // Reservation floor: never reserve at or below a version we have
        // already seen, even when commit-time lookups regress below it
        // (non-monotonic eventually consistent reads).
        let floor = known_version.max(loaded_version);

        // Blocking sqlite I/O happens off the async runtime.
        let store = self.store.clone();
        let expand_closure = self.expand_closure;
        let lookups = tokio::task::spawn_blocking(move || {
            if expand_closure {
                store.query_closure(paths)
            } else {
                store.query_batch(paths)
            }
        })
        .await
        .expect("store database query task panicked")?;

        let mut root_paths: BTreeSet<PathHash> = accessed;
        // Existing entries whose last_pushed clock gets bumped (dedup-skips).
        let mut bumped: BTreeMap<PathHash, PathEntry> = BTreeMap::new();
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

            if self.upstream.is_upstream_signed(&info.signatures) {
                stats.skipped_upstream += 1;
                continue;
            }

            let hash = info.path_hash();

            if let Some(existing) = current.paths.get(&hash) {
                // Already stored: bump the push clock so push-TTL-based
                // liveness keeps protecting it.
                root_paths.insert(hash);
                let mut entry = existing.clone();
                entry.last_pushed = now;
                bumped.insert(hash, entry);
                stats.skipped_existing += 1;
                continue;
            }

            to_push.push((path, info));
        }

        stats.load_ms = load_started.elapsed().as_millis() as u64;

        // Pipelined chunk → compress → pack → upload: the producer chunks
        // paths sequentially, compresses their new chunks in parallel
        // across cores, and seals packs at the target size; sealed packs
        // flow through a bounded channel to the uploader, so network
        // transfer overlaps with the CPU work for later paths.
        let (pack_tx, pack_rx) = tokio::sync::mpsc::channel::<chunker::Pack>(2);

        let producer = async {
            let mut prepared: Vec<PreparedPath> = Vec::new();
            // Stage timers accumulate as Durations and convert to ms once:
            // per-path as_millis() truncation would drop up to ~1 ms per
            // path, systematically underreporting drains of many small
            // paths.
            let mut chunk_time = std::time::Duration::ZERO;
            let mut pack_time = std::time::Duration::ZERO;
            // Chunks of earlier prepared paths in this batch (cross-path dedup).
            let mut batch_chunks: BTreeSet<crate::manifest::ChunkHash> = BTreeSet::new();
            let mut builder = PackBuilder::new();

            'paths: for (path, info) in to_push {
                let chunk_started = std::time::Instant::now();
                // Per-path chunking failures (unreadable files, NAR contents
                // hestia cannot represent) are skipped, not propagated: a
                // pipeline error would re-buffer the whole batch, and a
                // deterministic failure would then keep every later drain --
                // including the final shutdown drain -- from caching
                // anything at all.
                // Normalize reference occurrences out before chunking so
                // chunks stay stable across dependency-hash changes; the
                // path's own references drive both normalization and the
                // read-side restore.
                let refs = RefTable::new(&info.references);
                let chunked = match chunk_path(&path, &refs).await {
                    Ok(chunked) => chunked,
                    Err(err) => {
                        eprintln!("hestia: NOT uploading {path}: chunking failed: {err}");
                        stats.failed_chunking += 1;
                        continue;
                    }
                };
                let chunk_map = chunked.chunk_map();

                // Integrity gate: the chunked representation must reproduce
                // the NAR hash Nix recorded for this path. A mismatch means
                // hestia would serve corrupt data; never upload it.
                let (nar_hash, nar_size) =
                    match nar_hash_from_chunks(&chunked.tree, &chunk_map, &refs).await {
                        Ok(result) => result,
                        Err(err) => {
                            eprintln!("hestia: NOT uploading {path}: NAR replay failed: {err}");
                            stats.failed_chunking += 1;
                            continue;
                        }
                    };
                chunk_time += chunk_started.elapsed();
                if nar_hash != info.nar_hash || nar_size != info.nar_size {
                    eprintln!(
                        "hestia: NOT uploading {path}: chunked NAR hash {nar_hash} (size \
                         {nar_size}) does not match the store's record {} (size {}); \
                         this indicates a chunker bug or store corruption",
                        info.nar_hash, info.nar_size
                    );
                    stats.failed_verification += 1;
                    continue;
                }

                let new_chunks: Vec<chunker::Chunk> = chunked
                    .chunks
                    .into_iter()
                    .filter(|chunk| {
                        !current.chunks.contains_key(&chunk.hash) && batch_chunks.insert(chunk.hash)
                    })
                    .collect();

                let mut pack_started = std::time::Instant::now();
                let frames = tokio::task::spawn_blocking(move || compress_chunks(new_chunks))
                    .await
                    .expect("compression task panicked")?;
                for frame in frames {
                    builder.add_compressed(frame.hash, &frame.frame, frame.uncompressed_size);
                    if builder.compressed_size() >= self.pack_target_size {
                        let pack = std::mem::take(&mut builder).finish();
                        // Pause the pack timer across the send: a full
                        // channel blocks on upload backpressure, which must
                        // not be booked as packing time.
                        pack_time += pack_started.elapsed();
                        if pack_tx.send(pack).await.is_err() {
                            // Uploader gone: it failed, and try_join below
                            // reports its error; stop producing.
                            break 'paths;
                        }
                        pack_started = std::time::Instant::now();
                    }
                }
                pack_time += pack_started.elapsed();

                prepared.push(PreparedPath {
                    hash: info.path_hash(),
                    entry: PathEntry {
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
                        last_reachable: 0,
                        last_pushed: now,
                    },
                });
            }
            if !builder.is_empty() {
                // A send failure means the uploader failed; try_join below
                // reports its error, the producer's result is discarded.
                let _ = pack_tx.send(builder.finish()).await;
            }
            // pack_tx drops here, ending the consumer's stream.
            drop(pack_tx);
            stats.chunk_ms = chunk_time.as_millis() as u64;
            stats.pack_ms = pack_time.as_millis() as u64;
            Ok::<_, Error>(prepared)
        };

        let upload_started = std::time::Instant::now();
        let consumer = async {
            let pack_stream = futures_util::stream::unfold(pack_rx, |mut rx| async move {
                rx.recv().await.map(|pack| (pack, rx))
            });
            pack_stream
                .map(|mut pack| async move {
                    let uploaded = upload_pack(&self.twirp, &self.http, &pack).await?;
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

        let (prepared, uploads) = tokio::try_join!(producer, consumer)?;
        // Paths the producer rejected (failed verification or chunking)
        // must not enter the committed root: it would pin hashes the
        // manifest cannot serve.
        root_paths.extend(prepared.iter().map(|path| path.hash));
        // Stage times overlap now: chunk/pack are producer busy times,
        // upload is the wall time of the whole pipelined section.
        stats.upload_ms = upload_started.elapsed().as_millis() as u64;

        let mut delta = Manifest::new();
        let mut packs: Vec<(u64, chunker::Pack)> = Vec::new();
        for (uploaded, size, pack) in uploads {
            if uploaded {
                stats.packs_uploaded += 1;
                stats.bytes_uploaded += size;
            }
            packs.push((size, pack));
        }
        stats.new_chunks = packs.iter().map(|(_, pack)| pack.chunks.len()).sum();

        for (size, pack) in &packs {
            for (chunk_hash, location) in pack.locations() {
                delta.chunks.insert(chunk_hash, location);
            }
            delta.packs.insert(
                pack.hash,
                PackInfo {
                    size: *size,
                    created: now,
                    tier: 0,
                },
            );
        }

        for path in prepared {
            stats.pushed += 1;
            delta.paths.insert(path.hash, path.entry);
        }
        delta.paths.extend(bumped);

        if delta.paths.is_empty() && root_paths.is_empty() {
            // Everything was filtered out; nothing worth a manifest version.
            return Ok(stats);
        }

        delta.roots.insert(
            self.root_key.clone(),
            Root {
                paths: root_paths,
                updated: now,
                run_id: self.run_id.clone(),
            },
        );

        // Skip commits that would only refresh the root's `updated` clock:
        // the access log is never cleared and the SIGTERM final drain runs
        // unconditionally, so otherwise every job that substituted a path
        // burns a redundant manifest version at teardown. Only skip when
        // the committed root comes from this very run: a CI job lives far
        // shorter than the root TTL, so the stale clock cannot expire the
        // root. Without a run id (local builds) always commit, because the
        // clock drives root-TTL liveness in GC.
        let refresh_only = {
            let mut probe = current.clone().merge(delta.clone());
            match (
                probe.roots.get_mut(&self.root_key),
                current.roots.get(&self.root_key),
            ) {
                (Some(probed), Some(committed))
                    if self.run_id.is_some() && committed.run_id == self.run_id =>
                {
                    probed.updated = committed.updated;
                    probe == current
                }
                _ => false,
            }
        };
        if refresh_only {
            return Ok(stats);
        }

        // The merge closure keeps the manifest it encoded so the committed
        // version can be published without re-loading it from the cache.
        let commit_started = std::time::Instant::now();
        let mut committed: Option<Manifest> = None;
        let version = self
            .save_mutable()
            .save_with_floor(floor, |existing| {
                // A corrupt base manifest is replaced, not merged with: the
                // commit must not fail because of it (never crash CI).
                let base = match existing {
                    Some(entry) => decode_manifest_or_empty(&entry.data),
                    None => Manifest::new(),
                };
                // `current` covers the gap when `existing` is a stale read:
                // the commit must contain everything the dedup decisions
                // above were based on. Merging anything less can drop a
                // concurrent writer's paths and leave this delta's entries
                // referencing chunks whose locations are missing (dangling,
                // unservable, and never healed because later drains see the
                // path as already stored).
                let merged = base.merge(current.clone()).merge(delta.clone());
                let encoded = merged
                    .encode()
                    .map_err(|err| GhaError::InvalidResponse(err.to_string()))?;
                committed = Some(merged);
                Ok(encoded)
            })
            .await?;
        stats.commit_ms = commit_started.elapsed().as_millis() as u64;
        stats.manifest_version = version;

        // Publish exactly what was committed (includes concurrent writers'
        // paths, since the merge ran against the latest visible version).
        if let (Some(store), Some(manifest)) = (&self.publish, committed) {
            store.set_version(manifest, version);
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
