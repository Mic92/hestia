//! The substituter: Nix binary cache protocol served from the manifest.
//!
//! Routes (axum), mounted into `hestia serve`:
//!
//! * `GET /nix-cache-info` — store dir, mass-query flag, priority.
//! * `GET /{hash}.narinfo` — manifest lookup; a hit is recorded in the
//!   [`AccessLog`] (narinfo hits are the liveness signal: accessed paths
//!   join this run's GC root).
//! * `GET /nar/{narhash}.nar` — chunks are fetched from packs (batched
//!   Range requests, parallel across packs, signed URLs cached and
//!   refreshed on 403), the NAR is synthesized from the manifest tree, and
//!   its hash is verified before a single byte leaves the process. Any
//!   failure (evicted pack, missing chunk, hash mismatch) turns into a 404
//!   so Nix falls through to the next substituter — never partial or
//!   corrupt data.
//! * `GET /closure/{hashes}` — the closure of the given path hashes
//!   (comma-separated), restricted to manifest members, streamed in
//!   `nix-store --export` format for a one-request prefetch via
//!   `nix-store --import`.
//!
//! A semaphore caps concurrent pack reads so parallel narinfo queries
//! from Nix (`WantMassQuery: 1`) do not flood the GHA cache API.
//!
//! Responses are unsigned: the action configures the store URL with
//! `?trusted=true`.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use serde::Deserialize;

use harmonia_store_nar_info::{build_narinfo, format_narinfo_txt};
use harmonia_store_path::StoreDir;
use harmonia_store_path_info::{NarHash, UnkeyedValidPathInfo, ValidPathInfo};

use crate::chunker::{self, extract_chunk, flatten_tree, nar_from_chunks, pack_cache_key};
use crate::gha::twirp::{DownloadUrl, TwirpClient};
use crate::gha::{Error as GhaError, blob};
use crate::manifest::{
    ChunkHash, ChunkLocation, FileSystemObject, Hash32, Manifest, PackHash, PathEntry, PathHash,
};
use crate::pipeline::AccessLog;
use crate::refnorm::RefTable;

/// Priority advertised in /nix-cache-info. Lower wins: 30 puts hestia ahead
/// of cache.nixos.org (40), so Nix asks the local cache first and only falls
/// through to upstream on a miss.
const PRIORITY: u32 = 30;

/// How long a signed pack download URL is reused before asking Twirp for a
/// fresh one. Real SAS URLs live much longer; the 403-refresh path is the
/// backstop for when this estimate is wrong.
const PACK_URL_TTL: Duration = Duration::from_secs(10 * 60);

/// Upper bound for decompressed chunks kept in memory across NAR requests.
/// Oldest chunks are dropped first.
const CHUNK_CACHE_BUDGET: usize = 256 * 1024 * 1024;

/// Chunks of one pack whose gap is at most this are fetched in one Range
/// read: dedup and generational scatter punch small holes into otherwise
/// contiguous chunk runs, and re-downloading the hole is far cheaper than
/// another ~66 ms round trip.
const PACK_FETCH_GAP_BYTES: u64 = 128 * 1024;

/// Every Range read is extended to at least this many bytes (clamped to
/// the last known chunk of the pack) and all chunks inside the region go into the
/// chunk cache. Packs are written in drain order, so the neighbours of a
/// requested chunk are what nix asks for next; read-ahead turns thousands
/// of per-path round trips into a few large reads.
const PACK_READ_AHEAD_BYTES: u64 = 4 * 1024 * 1024;

/// Maximum number of pack reads in flight across all NAR requests. A pack
/// read is the unit of GHA cache API traffic (one Twirp URL lookup plus
/// Azure Range requests), so this bounds the total API concurrency no
/// matter how the packs distribute over paths.
const MAX_CONCURRENT_PACK_FETCHES: usize = 8;

/// How many times a pack Range read is retried after a transient failure
/// (connection drop, timeout, 5xx) before the whole NAR request gives up
/// and returns 404.
const TRANSIENT_READ_RETRIES: u32 = 2;

/// One pack's chunk locations sorted by offset. Lets the fetcher answer
/// "which chunks live in this byte range" (read-ahead) with a range scan;
/// the manifest itself only indexes chunks by hash.
struct PackChunkIndex {
    /// End of the last known chunk; reads are never extended past it.
    end: u64,
    /// (offset, compressed size, hash), sorted by offset.
    chunks: Vec<(u64, u32, ChunkHash)>,
}

/// One manifest version plus the indexes the substituter needs.
#[derive(Default)]
struct ManifestView {
    manifest: Manifest,
    /// NAR hash → manifest path key, for `/nar/{narhash}.nar` requests that
    /// arrive without the `?hash=` parameter.
    by_nar_hash: BTreeMap<Hash32, PathHash>,
    /// Per-pack chunk layout, for read-ahead.
    pack_index: HashMap<PackHash, Arc<PackChunkIndex>>,
    /// SaveMutable index this manifest was loaded from / committed as
    /// (0 = unknown or no manifest yet).
    version: u64,
}

impl ManifestView {
    fn new(manifest: Manifest, version: u64) -> Self {
        let by_nar_hash = manifest
            .paths
            .iter()
            .map(|(path_hash, entry)| (entry.nar_hash, *path_hash))
            .collect();
        let mut by_pack: HashMap<PackHash, Vec<(u64, u32, ChunkHash)>> = HashMap::new();
        for (hash, location) in &manifest.chunks {
            by_pack.entry(location.pack).or_default().push((
                location.offset,
                location.compressed_size,
                *hash,
            ));
        }
        let pack_index = by_pack
            .into_iter()
            .map(|(pack, mut chunks)| {
                chunks.sort_unstable_by_key(|(offset, ..)| *offset);
                let end = chunks
                    .last()
                    .map_or(0, |(offset, size, _)| offset + u64::from(*size));
                (pack, Arc::new(PackChunkIndex { end, chunks }))
            })
            .collect();
        Self {
            manifest,
            by_nar_hash,
            pack_index,
            version,
        }
    }
}

/// Shared, replaceable manifest: the substituter reads it on every request,
/// the daemon replaces it at startup and after every successful drain.
///
/// Cloning is cheap (shared state).
#[derive(Clone, Default)]
pub struct ManifestStore {
    inner: Arc<RwLock<Arc<ManifestView>>>,
}

impl ManifestStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the served manifest (version unknown).
    pub fn set(&self, manifest: Manifest) {
        self.set_version(manifest, 0);
    }

    /// Replace the served manifest, recording the SaveMutable index it came
    /// from. The version is what the pipeline uses for read-your-writes:
    /// it merges this manifest into every commit base and never reserves an
    /// index at or below it.
    pub fn set_version(&self, manifest: Manifest, version: u64) {
        *self.inner.write().expect("manifest lock poisoned") =
            Arc::new(ManifestView::new(manifest, version));
    }

    /// Replace the served manifest only if `version` is newer than the
    /// served one. Used by the startup load, which runs concurrently with
    /// the daemon: a drain may commit (and publish) a newer manifest before
    /// the initial load finishes, and that newer version must win.
    pub fn set_version_if_newer(&self, manifest: Manifest, version: u64) {
        let mut inner = self.inner.write().expect("manifest lock poisoned");
        if version > inner.version {
            *inner = Arc::new(ManifestView::new(manifest, version));
        }
    }

    /// The served manifest and its version (clone; manifests are small).
    pub fn versioned(&self) -> (u64, Manifest) {
        let view = self.view();
        (view.version, view.manifest.clone())
    }

    fn view(&self) -> Arc<ManifestView> {
        Arc::clone(&self.inner.read().expect("manifest lock poisoned"))
    }

    /// SaveMutable version of the served manifest (0 = none loaded yet).
    pub fn version(&self) -> u64 {
        self.view().version
    }

    /// Number of paths currently servable.
    pub fn path_count(&self) -> usize {
        self.view().manifest.paths.len()
    }
}

#[derive(Debug, thiserror::Error)]
enum FetchError {
    #[error("GHA cache error: {0}")]
    Gha(#[from] GhaError),

    #[error("chunk {0} has no location in the manifest")]
    UnknownChunk(ChunkHash),

    #[error("pack {} is not in the cache (evicted?)", pack_cache_key(.0))]
    PackUnavailable(PackHash),

    #[error("chunk extraction failed: {0}")]
    Chunker(#[from] chunker::Error),
}

/// Decompressed chunks kept in memory, evicted least-recently-used first
/// once over budget: chunks shared across paths (dedup) and repeated NAR
/// requests keep hitting early-inserted chunks, so insertion-order
/// eviction would drop the hot set first.
#[derive(Default)]
struct ChunkCache {
    chunks: HashMap<ChunkHash, Bytes>,
    order: VecDeque<ChunkHash>,
    total: usize,
}

impl ChunkCache {
    fn get(&mut self, hash: &ChunkHash) -> Option<Bytes> {
        let data = self.chunks.get(hash).cloned()?;
        // Move-to-back on hit (entry counts are small enough for the
        // linear scan): a hit must postpone eviction.
        if let Some(position) = self.order.iter().position(|entry| entry == hash) {
            let entry = self.order.remove(position).expect("position is valid");
            self.order.push_back(entry);
        }
        Some(data)
    }

    fn insert(&mut self, hash: ChunkHash, data: Bytes) {
        if self.chunks.contains_key(&hash) {
            return;
        }
        self.total += data.len();
        self.chunks.insert(hash, data);
        self.order.push_back(hash);
        while self.total > CHUNK_CACHE_BUDGET {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(dropped) = self.chunks.remove(&oldest) {
                self.total -= dropped.len();
            }
        }
    }
}

/// Fetches chunks from pack blobs in the GHA cache.
struct ChunkFetcher {
    twirp: TwirpClient,
    http: reqwest::Client,
    /// Signed download URLs per pack, with issue time (TTL-based reuse).
    url_cache: Mutex<HashMap<PackHash, (String, Instant)>>,
    /// Decompressed chunks (filled by NAR requests).
    chunk_cache: Mutex<ChunkCache>,
    /// Per-path serialization: concurrent NAR requests for the same path
    /// must not fetch the same chunks twice.
    path_locks: Mutex<HashMap<PathHash, Arc<tokio::sync::Mutex<()>>>>,
    /// Caps pack reads that hit the GHA cache API. Acquired per pack,
    /// *after* the per-path lock and the cache check, so idle waiters and
    /// cache hits never pin a permit. FIFO: a many-pack path cannot
    /// starve others.
    fetch_semaphore: Semaphore,
}

impl ChunkFetcher {
    fn new(twirp: TwirpClient, http: reqwest::Client) -> Self {
        Self {
            twirp,
            http,
            url_cache: Mutex::new(HashMap::new()),
            chunk_cache: Mutex::new(ChunkCache::default()),
            path_locks: Mutex::new(HashMap::new()),
            fetch_semaphore: Semaphore::new(MAX_CONCURRENT_PACK_FETCHES),
        }
    }

    fn path_lock(&self, path: PathHash) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.path_locks.lock().expect("path lock map poisoned");
        // Drop locks no request holds anymore: without pruning the map
        // grows by one entry per distinct path for the process lifetime.
        locks.retain(|_, lock| Arc::strong_count(lock) > 1);
        Arc::clone(locks.entry(path).or_default())
    }

    /// Get a signed download URL for a pack, reusing a cached one if it is
    /// fresh enough. `force` bypasses the cache (after a 403).
    async fn pack_url(&self, pack: PackHash, force: bool) -> Result<String, FetchError> {
        if !force {
            let cache = self.url_cache.lock().expect("url cache poisoned");
            if let Some((url, issued)) = cache.get(&pack)
                && issued.elapsed() < PACK_URL_TTL
            {
                return Ok(url.clone());
            }
        }
        let key = pack_cache_key(&pack);
        match self.twirp.get_download_url(&key, &[]).await? {
            DownloadUrl::Hit { url, .. } => {
                let mut cache = self.url_cache.lock().expect("url cache poisoned");
                // Expired entries are only ever bypassed, never overwritten
                // unless the same pack is fetched again — prune them here so
                // URLs of packs GC has repacked away do not accumulate for
                // the process lifetime.
                cache.retain(|_, (_, issued)| issued.elapsed() < PACK_URL_TTL);
                cache.insert(pack, (url.clone(), Instant::now()));
                Ok(url)
            }
            DownloadUrl::Miss => Err(FetchError::PackUnavailable(pack)),
        }
    }

    /// Range-read one byte range of a pack.
    ///
    /// Two failure modes are recovered from, everything else propagates
    /// (and ultimately turns the NAR request into a 404):
    ///
    /// * expired signed URL (401/403) → refresh via Twirp, once;
    /// * transient network/server failure (connection drop, timeout, 5xx)
    ///   → retry the same URL up to [`TRANSIENT_READ_RETRIES`] times.
    async fn read_pack_range(
        &self,
        pack: PackHash,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes, FetchError> {
        let mut url = self.pack_url(pack, false).await?;
        let mut refreshed = false;
        let mut transient_left = TRANSIENT_READ_RETRIES;
        loop {
            match blob::get(&self.http, &url, Some(range.clone())).await {
                Err(GhaError::Status { status, .. })
                    if (status == 403 || status == 401) && !refreshed =>
                {
                    refreshed = true;
                    url = self.pack_url(pack, true).await?;
                }
                Err(err) if blob::is_transient(&err) && transient_left > 0 => {
                    transient_left -= 1;
                    eprintln!(
                        "hestia substituter: transient error reading pack {}, retrying: {err}",
                        pack_cache_key(&pack)
                    );
                }
                result => return Ok(result?),
            }
        }
    }

    /// Fetch all chunks of `entry`, using cached chunks where possible.
    async fn fetch_path_chunks(
        &self,
        view: &ManifestView,
        path: PathHash,
        entry: &PathEntry,
    ) -> Result<BTreeMap<ChunkHash, Bytes>, FetchError> {
        // Serialize per path so concurrent NAR requests for the same
        // path do the work once.
        let lock = self.path_lock(path);
        let _guard = lock.lock().await;
        self.fetch_chunks(view, entry_chunks(entry)).await
    }

    /// Fetch a set of chunks, using cached chunks where possible.
    ///
    /// Missing chunks are grouped by pack; nearby chunks within a pack are
    /// coalesced into single Range requests (with read-ahead, see
    /// [`PACK_READ_AHEAD_BYTES`]); packs are fetched in parallel. Every
    /// chunk is hash-verified during extraction.
    async fn fetch_chunks(
        &self,
        view: &ManifestView,
        needed: BTreeSet<ChunkHash>,
    ) -> Result<BTreeMap<ChunkHash, Bytes>, FetchError> {
        let mut result: BTreeMap<ChunkHash, Bytes> = BTreeMap::new();
        let mut missing: BTreeMap<PackHash, Vec<(ChunkHash, ChunkLocation)>> = BTreeMap::new();
        {
            let mut cache = self.chunk_cache.lock().expect("chunk cache poisoned");
            for chunk in needed {
                if let Some(data) = cache.get(&chunk) {
                    result.insert(chunk, data);
                    continue;
                }
                let location = view
                    .manifest
                    .chunks
                    .get(&chunk)
                    .ok_or(FetchError::UnknownChunk(chunk))?;
                missing
                    .entry(location.pack)
                    .or_default()
                    .push((chunk, location.clone()));
            }
        }

        // Fetch packs in parallel; each fetch holds one global permit
        // while it talks to the GHA cache API. The semaphore is never
        // closed, so acquire only fails after close.
        let fetches = missing.into_iter().map(|(pack, chunks)| {
            // Present by construction: the index covers every pack the
            // manifest's chunk table mentions, and the locations above came
            // from that table.
            let index = Arc::clone(&view.pack_index[&pack]);
            async move {
                let _permit = self
                    .fetch_semaphore
                    .acquire()
                    .await
                    .expect("fetch semaphore closed");
                self.fetch_from_pack(pack, chunks, index).await
            }
        });
        for fetched in futures_util::future::try_join_all(fetches).await? {
            let mut cache = self.chunk_cache.lock().expect("chunk cache poisoned");
            for (hash, data) in fetched {
                cache.insert(hash, data.clone());
                result.insert(hash, data);
            }
        }

        Ok(result)
    }

    /// Fetch a set of chunks from one pack with as few Range requests as
    /// possible (gap coalescing plus read-ahead, see [`plan_pack_reads`]).
    ///
    /// Returns the requested chunks plus every other manifest chunk that
    /// happened to fall inside the fetched regions (read-ahead); the caller
    /// caches all of them.
    async fn fetch_from_pack(
        &self,
        pack: PackHash,
        mut chunks: Vec<(ChunkHash, ChunkLocation)>,
        index: Arc<PackChunkIndex>,
    ) -> Result<Vec<(ChunkHash, Bytes)>, FetchError> {
        chunks.sort_by_key(|(_, location)| location.offset);
        let chunk_count = chunks.len();
        let spans: Vec<(u64, u32)> = chunks
            .iter()
            .map(|(_, location)| (location.offset, location.compressed_size))
            .collect();
        let ranges = plan_pack_reads(&spans, index.end);

        let started = Instant::now();
        let range_count = ranges.len();
        let mut range_bytes = 0u64;
        let mut fetched = Vec::new();
        for range in ranges {
            let start = range.start;
            range_bytes += range.end - range.start;
            // Everything inside the fetched region gets extracted and
            // cached: the requested chunks by construction, plus their
            // neighbours pulled in by read-ahead.
            let in_range: Vec<(u64, u32, ChunkHash)> = chunks_in_range(&index, &range).collect();
            let data = self.read_pack_range(pack, range).await?;

            // Decompression + hash verification are CPU-bound: off the
            // runtime workers, like the write pipeline's compression
            // stages, so concurrent fetches cannot starve the hook socket.
            let extracted = tokio::task::spawn_blocking(move || {
                let mut extracted = Vec::with_capacity(in_range.len());
                for (offset, size, hash) in in_range {
                    let from = (offset - start) as usize;
                    let to = from + size as usize;
                    // In bounds by construction: blob::get errors unless
                    // the ranged response is exactly range.end - range.start
                    // bytes, and plan_pack_reads/chunks_in_range only select
                    // chunks fully inside the range. extract_chunk verifies
                    // the SHA-256 of the decompressed data; corrupt or
                    // truncated cache contents cannot pass.
                    let chunk = extract_chunk(&data[from..to], &hash)?;
                    extracted.push((hash, Bytes::from(chunk)));
                }
                Ok::<_, FetchError>(extracted)
            })
            .await
            .expect("chunk extraction task panicked")?;
            fetched.extend(extracted);
        }
        // One line per pack fetch (= per burst of GHA cache traffic): shows
        // whether chunks coalesce into few large Range reads or degrade
        // into many small ones. A busy job produces hundreds of these, so
        // they only appear when the workflow is re-run with debug logging
        // (RUNNER_DEBUG=1).
        if std::env::var_os("RUNNER_DEBUG").is_some_and(|value| value == "1") {
            eprintln!(
                "hestia substituter: pack {}: {chunk_count} chunks requested, {} extracted \
                 in {range_count} range reads ({}, {:.1}s)",
                pack_cache_key(&pack),
                fetched.len(),
                crate::drain::human_bytes(range_bytes),
                started.elapsed().as_secs_f64(),
            );
        }
        Ok(fetched)
    }
}

/// Plan the Range reads for one pack: coalesce chunk spans whose gap is at
/// most [`PACK_FETCH_GAP_BYTES`], extend each read to at least
/// [`PACK_READ_AHEAD_BYTES`] (clamped to `pack_end`), and merge reads that
/// the extension made overlap. `spans` are `(offset, size)`, sorted by
/// offset.
fn plan_pack_reads(spans: &[(u64, u32)], pack_end: u64) -> Vec<std::ops::Range<u64>> {
    let mut runs: Vec<std::ops::Range<u64>> = Vec::new();
    for &(offset, size) in spans {
        // Checked: offsets come from the manifest; a corrupt value near
        // u64::MAX must not panic.
        let end = offset.saturating_add(u64::from(size));
        match runs.last_mut() {
            Some(run) if offset <= run.end.saturating_add(PACK_FETCH_GAP_BYTES) => {
                run.end = run.end.max(end);
            }
            _ => runs.push(offset..end),
        }
    }
    let mut reads: Vec<std::ops::Range<u64>> = Vec::new();
    for run in runs {
        let read_ahead = run
            .start
            .saturating_add(PACK_READ_AHEAD_BYTES)
            .min(pack_end);
        let end = run.end.max(read_ahead);
        match reads.last_mut() {
            Some(read) if run.start <= read.end => read.end = read.end.max(end),
            _ => reads.push(run.start..end),
        }
    }
    reads
}

/// All chunks of a pack that lie fully inside `range`, as
/// `(offset, size, hash)`.
fn chunks_in_range<'a>(
    index: &'a PackChunkIndex,
    range: &'a std::ops::Range<u64>,
) -> impl Iterator<Item = (u64, u32, ChunkHash)> + 'a {
    let start = index
        .chunks
        .partition_point(|(offset, ..)| *offset < range.start);
    index.chunks[start..]
        .iter()
        .take_while(move |(offset, ..)| *offset < range.end)
        .filter(move |(offset, size, _)| offset + u64::from(*size) <= range.end)
        .copied()
}

/// Reloads the served manifest from the cache backend (the daemon wires
/// this to a SaveMutable load + ManifestStore::set_version_if_newer). The
/// NAR handler invokes it when a pack the current view points at is gone:
/// a concurrent gc repack moves live chunks into new packs and deletes the
/// old ones, so the committed manifest knows where the data went while the
/// daemon's view does not.
pub type ManifestReload =
    Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>;

/// Callback invoked on every substituter request (the daemon uses it to
/// reset its idle-exit timer: an actively substituting Nix counts as
/// activity). The returned guard is held for the whole request so that
/// long downloads count as in-flight work instead of only touching the
/// idle clock once at request start.
pub type ActivityHook = Arc<dyn Fn() -> Box<dyn Send> + Send + Sync>;

/// Signals that the startup manifest load (including a
/// `--wait-manifest-version` wait) has finished. Narinfo requests block on
/// it so an early `nix build` cannot race the load and see spurious misses.
pub type ManifestReady = tokio::sync::watch::Receiver<bool>;

/// The substituter's shared state and configuration.
pub struct Substituter {
    store_dir: StoreDir,
    manifest: ManifestStore,
    access_log: AccessLog,
    fetcher: ChunkFetcher,
    activity_hook: Option<ActivityHook>,
    manifest_reload: Option<ManifestReload>,
    manifest_ready: Option<ManifestReady>,
}

impl Substituter {
    pub fn new(
        store_dir: StoreDir,
        manifest: ManifestStore,
        access_log: AccessLog,
        twirp: TwirpClient,
        http: reqwest::Client,
    ) -> Self {
        Self {
            store_dir,
            manifest,
            access_log,
            fetcher: ChunkFetcher::new(twirp, http),
            activity_hook: None,
            manifest_reload: None,
            manifest_ready: None,
        }
    }

    /// Install a callback invoked on every request.
    pub fn with_activity_hook(mut self, hook: ActivityHook) -> Self {
        self.activity_hook = Some(hook);
        self
    }

    /// Install a manifest-reload callback (see [`ManifestReload`]).
    pub fn with_manifest_reload(mut self, reload: ManifestReload) -> Self {
        self.manifest_reload = Some(reload);
        self
    }

    /// Install a startup-load gate (see [`ManifestReady`]).
    pub fn with_manifest_ready(mut self, ready: ManifestReady) -> Self {
        self.manifest_ready = Some(ready);
        self
    }

    /// Block until the startup manifest load finished (no-op without a
    /// gate, or once it fired).
    async fn manifest_ready(&self) {
        if let Some(ready) = &self.manifest_ready {
            // Only fails if the sender is dropped without sending; serve
            // treats that as "nothing to wait for".
            let _ = ready.clone().wait_for(|ready| *ready).await;
        }
    }

    /// Build the axum router serving the binary cache protocol.
    pub fn into_router(self) -> Router {
        let state = Arc::new(self);
        Router::new()
            .route("/nix-cache-info", get(nix_cache_info))
            .route("/{file}", get(narinfo))
            .route("/nar/{file}", get(nar))
            .route("/closure/{hashes}", get(closure))
            .with_state(state)
    }

    /// Mark this request as in-flight work for the daemon's idle-exit
    /// timer; the guard must live until the response is built.
    fn touch(&self) -> Option<Box<dyn Send>> {
        self.activity_hook.as_ref().map(|hook| hook())
    }
}

async fn nix_cache_info(State(state): State<Arc<Substituter>>) -> Response {
    let _activity = state.touch();
    let body = format!(
        "StoreDir: {}\nWantMassQuery: 1\nPriority: {PRIORITY}\n",
        state.store_dir
    );
    ([(header::CONTENT_TYPE, "text/x-nix-cache-info")], body).into_response()
}

/// Convert a manifest entry into the narinfo metadata harmonia's formatter
/// expects.
fn narinfo_for_entry(store_dir: &StoreDir, entry: &PathEntry, hash: &str) -> Vec<u8> {
    let info = UnkeyedValidPathInfo {
        deriver: entry.deriver.clone(),
        nar_hash: NarHash::from_slice(&entry.nar_hash.0).expect("nar hash is always 32 bytes"),
        references: entry.references.iter().cloned().collect(),
        registration_time: None,
        nar_size: entry.nar_size,
        ultimate: false,
        // Unsigned: the store URL carries ?trusted=true.
        signatures: BTreeSet::new(),
        ca: entry.ca.as_deref().and_then(|ca| match ca.parse() {
            Ok(ca) => Some(ca),
            // Served without a CA line the path silently degrades to
            // input-addressed on the substituting side; leave a trace.
            Err(err) => {
                eprintln!(
                    "hestia substituter: dropping unparsable CA string {ca:?} for {}: {err}",
                    entry.store_path
                );
                None
            }
        }),
        store_dir: store_dir.clone(),
    };
    let narinfo = build_narinfo(
        store_dir,
        ValidPathInfo {
            path: entry.store_path.clone(),
            info,
        },
        hash,
        &[],
    );
    format_narinfo_txt(store_dir, &narinfo)
}

async fn narinfo(State(state): State<Arc<Substituter>>, Path(file): Path<String>) -> Response {
    let _activity = state.touch();
    state.manifest_ready().await;
    let Some(hash_str) = file.strip_suffix(".narinfo") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(path_hash) = hash_str.parse::<PathHash>() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let view = state.manifest.view();
    let Some(entry) = view.manifest.paths.get(&path_hash) else {
        // Miss: Nix falls through to the next substituter.
        return StatusCode::NOT_FOUND.into_response();
    };

    // A narinfo hit is the liveness signal: the accessed path joins this
    // run's GC root at the next drain.
    state.access_log.record(path_hash);

    let body = narinfo_for_entry(&state.store_dir, entry, hash_str);
    ([(header::CONTENT_TYPE, "text/x-nix-narinfo")], body).into_response()
}

#[derive(Deserialize)]
struct NarQuery {
    /// Store path hash, present when the URL came from one of our narinfo
    /// responses (`nar/<narhash>.nar?hash=<pathhash>`).
    hash: Option<String>,
}

async fn nar(
    State(state): State<Arc<Substituter>>,
    Path(file): Path<String>,
    // Result: an unparsable query string must yield the same 404 as every
    // other NAR failure (the module contract Nix relies on to fall through
    // to the next substituter), not axum's 400 extractor rejection.
    query: Result<Query<NarQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let _activity = state.touch();
    let Some(nar_hash_str) = file.strip_suffix(".nar") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(nar_hash) = Hash32::parse_sha256(&format!("sha256:{nar_hash_str}")) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let view = state.manifest.view();

    // Resolve the path entry: by ?hash= if present, otherwise via the
    // NAR-hash index.
    let query = match query {
        Ok(Query(query)) => query,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let path_hash = match &query.hash {
        Some(hash) => match hash.parse::<PathHash>() {
            Ok(path_hash) => path_hash,
            Err(_) => return StatusCode::NOT_FOUND.into_response(),
        },
        None => match view.by_nar_hash.get(&nar_hash) {
            Some(path_hash) => *path_hash,
            None => return StatusCode::NOT_FOUND.into_response(),
        },
    };
    let Some(entry) = view.manifest.paths.get(&path_hash) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if entry.nar_hash != nar_hash {
        // The URL's NAR hash does not match the entry: stale URL.
        return StatusCode::NOT_FOUND.into_response();
    }
    let mut entry = entry.clone();
    let mut manifest_view = view;

    // A NAR download is an access (the GC liveness signal), just like a
    // narinfo hit. Nix caches narinfo lookups locally and may fetch a NAR
    // without re-requesting the narinfo, so recording only narinfo hits
    // would let GC collect paths that are actively being substituted.
    state.access_log.record(path_hash);

    // Fetch all chunks (concurrency-capped inside the fetcher); any
    // failure means 404 (Nix rebuilds or falls through), never partial
    // data. A missing pack gets one retry against a freshly loaded
    // manifest (see [`ManifestReload`]).
    let mut reloaded = false;
    let chunks = loop {
        match state
            .fetcher
            .fetch_path_chunks(&manifest_view, path_hash, &entry)
            .await
        {
            Ok(chunks) => break chunks,
            Err(err @ (FetchError::PackUnavailable(_) | FetchError::UnknownChunk(_)))
                if !reloaded && state.manifest_reload.is_some() =>
            {
                reloaded = true;
                eprintln!(
                    "hestia substituter: {err}; reloading the manifest (concurrent gc repack?)"
                );
                (state.manifest_reload.as_ref().expect("checked above"))().await;
                manifest_view = state.manifest.view();
                match manifest_view.manifest.paths.get(&path_hash) {
                    Some(fresh) if fresh.nar_hash == nar_hash => entry = fresh.clone(),
                    _ => return StatusCode::NOT_FOUND.into_response(),
                }
            }
            Err(err) => {
                eprintln!("hestia substituter: cannot serve NAR for {path_hash}: {err}");
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    let nar = match assemble_verified_nar(&entry, Arc::new(chunks)).await {
        Ok(nar) => nar,
        Err(err) => {
            eprintln!("hestia substituter: cannot serve NAR for {path_hash}: {err}");
            return StatusCode::NOT_FOUND.into_response();
        }
    };

    // axum derives Content-Length from the sized body; because the NAR is
    // fully assembled and verified before responding, the length is always
    // exact (= nar_size, asserted above).
    ([(header::CONTENT_TYPE, "application/x-nix-nar")], nar).into_response()
}

/// All chunks referenced by an entry's file tree (deduplicated).
fn entry_chunks(entry: &PathEntry) -> BTreeSet<ChunkHash> {
    flatten_tree(&entry.tree)
        .into_iter()
        .filter_map(|(_, node)| match node {
            FileSystemObject::Regular(regular) => Some(regular.contents.chunks.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// Assemble the full NAR of `entry` from its fetched chunks and verify it
/// against the recorded hash/size.
///
/// CPU-bound and a single non-yielding poll (the Vec sink never pends), so
/// it runs off the runtime workers: with many NARs assembling at once, a
/// multi-hundred-MiB path would otherwise pin every worker thread and
/// starve the hook socket.
async fn assemble_verified_nar(
    entry: &PathEntry,
    chunks: Arc<BTreeMap<ChunkHash, Bytes>>,
) -> Result<Vec<u8>, String> {
    let tree = entry.tree.clone();
    let nar_size = entry.nar_size;
    let expected_hash = entry.nar_hash;
    // Reference occurrences normalized out on the write side (dedup v2) are
    // restored from the path's own references; v1 entries carry no rewrites,
    // so the table is unused for them.
    let refs = RefTable::new(&entry.references);
    tokio::task::spawn_blocking(move || {
        use futures_util::FutureExt as _;
        let nar = nar_from_chunks(&tree, &chunks, &refs)
            .now_or_never()
            .expect("NAR synthesis into a Vec sink never pends")
            .map_err(|err| format!("NAR synthesis failed: {err}"))?;
        // Final integrity gate: the served bytes must hash to exactly the
        // NAR hash the manifest (and the narinfo we served) promised.
        if nar.len() as u64 != nar_size || Hash32::digest(&nar) != expected_hash {
            return Err(
                "synthesized NAR does not match its recorded hash/size; refusing to serve \
                 corrupt data"
                    .to_string(),
            );
        }
        Ok(nar)
    })
    .await
    .expect("NAR synthesis task panicked")
}

// ---------------------------------------------------------------------------
// Closure export (prefetch)
// ---------------------------------------------------------------------------

/// Magic marker between the NAR and the metadata of one exported path
/// (nix's `exportMagic`).
const EXPORT_MAGIC: u64 = 0x4558494e;

/// NAR-byte budget of one closure-export window (one chunk-fetch batch).
/// Large batches turn thousands of latency-bound Range reads into a few
/// big ones; sizing by bytes (not path count) bounds peak memory (frames
/// plus chunks, times the stream lookahead of 2) no matter how the closure
/// splits into tiny and huge paths.
const CLOSURE_EXPORT_WINDOW_BYTES: u64 = 32 * 1024 * 1024;

/// Split a closure into windows of roughly [`CLOSURE_EXPORT_WINDOW_BYTES`]
/// of NAR data, keeping the closure order (a path bigger than the budget
/// gets its own window).
fn export_windows(manifest: &Manifest, order: &[PathHash]) -> Vec<Vec<PathHash>> {
    let mut windows = Vec::new();
    let mut window = Vec::new();
    let mut window_bytes = 0u64;
    for &path_hash in order {
        let nar_size = manifest.paths[&path_hash].nar_size;
        if !window.is_empty() && window_bytes + nar_size > CLOSURE_EXPORT_WINDOW_BYTES {
            windows.push(std::mem::take(&mut window));
            window_bytes = 0;
        }
        window.push(path_hash);
        window_bytes += nar_size;
    }
    if !window.is_empty() {
        windows.push(window);
    }
    windows
}

/// Append a u64 in Nix wire format (8-byte little endian).
fn export_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Append a string in Nix wire format (length, bytes, zero-padded to 8).
fn export_str(out: &mut Vec<u8>, value: &str) {
    export_u64(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
    out.resize(out.len() + (8 - value.len() % 8) % 8, 0);
}

/// One path framed for the export stream: entry marker, NAR, then path
/// metadata (what `nix-store --import` expects per path).
fn export_frame(store_dir: &StoreDir, entry: &PathEntry, nar: &[u8]) -> Vec<u8> {
    let full_path = |path: &crate::manifest::StorePath| format!("{store_dir}/{path}");
    let mut out = Vec::with_capacity(nar.len() + 512);
    export_u64(&mut out, 1);
    out.extend_from_slice(nar);
    export_u64(&mut out, EXPORT_MAGIC);
    export_str(&mut out, &full_path(&entry.store_path));
    export_u64(&mut out, entry.references.len() as u64);
    for reference in &entry.references {
        export_str(&mut out, &full_path(reference));
    }
    export_str(
        &mut out,
        &entry.deriver.as_ref().map(&full_path).unwrap_or_default(),
    );
    // Legacy signature slot, always empty.
    export_u64(&mut out, 0);
    out
}

/// The closure of `roots` restricted to manifest members, references
/// before referrers (`nix-store --import` registers paths in stream
/// order). References pointing outside the manifest (upstream paths) are
/// skipped. Iterative DFS: drv chains can be deep.
fn closure_order(manifest: &Manifest, roots: &[PathHash]) -> Vec<PathHash> {
    let mut order = Vec::new();
    let mut seen = BTreeSet::new();
    for &root in roots {
        let mut stack = vec![(root, false)];
        while let Some((hash, children_done)) = stack.pop() {
            let Some(entry) = manifest.paths.get(&hash) else {
                continue;
            };
            if children_done {
                order.push(hash);
                continue;
            }
            if !seen.insert(hash) {
                continue;
            }
            stack.push((hash, true));
            for reference in &entry.references {
                let child = PathHash::from_store_path(reference);
                if child != hash && !seen.contains(&child) {
                    stack.push((child, false));
                }
            }
        }
    }
    order
}

/// Fetch and frame one window of a closure export.
async fn export_window(
    state: &Substituter,
    view: &ManifestView,
    window: &[PathHash],
) -> Result<Vec<u8>, String> {
    let entries: Vec<(PathHash, &PathEntry)> = window
        .iter()
        .map(|hash| (*hash, &view.manifest.paths[hash]))
        .collect();
    let needed: BTreeSet<ChunkHash> = entries
        .iter()
        .flat_map(|(_, entry)| entry_chunks(entry))
        .collect();
    let chunks = Arc::new(
        state
            .fetcher
            .fetch_chunks(view, needed)
            .await
            .map_err(|err| {
                eprintln!("hestia substituter: closure export chunk fetch failed: {err}");
                err.to_string()
            })?,
    );

    let mut out = Vec::new();
    for (path_hash, entry) in entries {
        // Prefetched paths are accesses (GC liveness), same as narinfo hits.
        state.access_log.record(path_hash);
        let nar = assemble_verified_nar(entry, Arc::clone(&chunks))
            .await
            .map_err(|err| {
                eprintln!("hestia substituter: closure export failed at {path_hash}: {err}");
                err
            })?;
        out.extend_from_slice(&export_frame(&state.store_dir, entry, &nar));
    }
    Ok(out)
}

async fn closure(State(state): State<Arc<Substituter>>, Path(hashes): Path<String>) -> Response {
    let _activity = state.touch();
    state.manifest_ready().await;

    let mut roots = Vec::new();
    for part in hashes.split(',').filter(|part| !part.is_empty()) {
        match part.parse::<PathHash>() {
            Ok(hash) => roots.push(hash),
            Err(_) => return StatusCode::NOT_FOUND.into_response(),
        }
    }
    let view = state.manifest.view();
    // Every requested root must be servable; a partial closure would make
    // the import succeed and the subsequent build fail confusingly.
    if roots.is_empty()
        || !roots
            .iter()
            .all(|root| view.manifest.paths.contains_key(root))
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let order = closure_order(&view.manifest, &roots);

    // Stream the closure in windows: each window's chunks are fetched as
    // one batch, and the next window downloads while the current one is
    // being sent. A fetch/assembly failure ends the stream mid-transfer,
    // which fails the client's import (never a silently truncated but
    // well-formed stream).
    use futures_util::StreamExt as _;
    let windows = export_windows(&view.manifest, &order);
    let frames = futures_util::stream::iter(windows)
        .map(move |window| {
            let state = state.clone();
            let view = view.clone();
            async move {
                let result = export_window(&state, &view, &window).await;
                result.map(Bytes::from).map_err(std::io::Error::other)
            }
        })
        .buffered(2)
        .chain(futures_util::stream::once(async {
            // End-of-stream marker.
            Ok(Bytes::from_static(&[0u8; 8]))
        }));
    // Stop after the first error: everything behind it (including the end
    // marker) is dropped so the client sees a truncated stream.
    let stream = frames.scan(false, |failed, item| {
        let stop = *failed;
        *failed = *failed || item.is_err();
        futures_util::future::ready((!stop).then_some(item))
    });

    (
        [(header::CONTENT_TYPE, "application/x-nix-export")],
        axum::body::Body::from_stream(stream),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{ChunkList, FileTree, Regular};

    fn test_path_hash(seed: u8) -> PathHash {
        PathHash(crate::manifest::StorePathHash::new([seed; 20]))
    }

    fn test_entry(seed: u8) -> PathEntry {
        PathEntry {
            store_path: format!("{}-test-{seed}", test_path_hash(seed))
                .parse()
                .unwrap(),
            nar_hash: Hash32::digest([seed]),
            nar_size: 100,
            references: vec![],
            ca: None,
            deriver: None,
            tree: FileTree(FileSystemObject::Regular(Regular {
                executable: false,
                contents: ChunkList::default(),
            })),
            last_reachable: 0,
            last_pushed: 0,
        }
    }

    #[test]
    fn manifest_store_indexes_nar_hashes() {
        let store = ManifestStore::new();
        assert_eq!(store.path_count(), 0);

        let mut manifest = Manifest::new();
        manifest.paths.insert(test_path_hash(1), test_entry(1));
        manifest.paths.insert(test_path_hash(2), test_entry(2));
        store.set(manifest);

        assert_eq!(store.path_count(), 2);
        let view = store.view();
        assert_eq!(
            view.by_nar_hash.get(&Hash32::digest([1])),
            Some(&test_path_hash(1))
        );
        assert_eq!(view.by_nar_hash.get(&Hash32::digest([99])), None);
    }

    #[tokio::test(start_paused = true)]
    async fn narinfo_waits_for_the_startup_manifest_load() {
        let mut manifest = Manifest::new();
        manifest.paths.insert(test_path_hash(1), test_entry(1));
        let store = ManifestStore::new();
        store.set(manifest);

        let (ready_tx, ready_rx) = tokio::sync::watch::channel(false);
        let state = Arc::new(
            Substituter::new(
                StoreDir::default(),
                store,
                AccessLog::new(),
                TwirpClient::new(reqwest::Client::new(), "http://unused", "token"),
                reqwest::Client::new(),
            )
            .with_manifest_ready(ready_rx),
        );

        let request = tokio::spawn(narinfo(
            State(state),
            Path(format!("{}.narinfo", test_path_hash(1))),
        ));
        // The gate is closed: the request must still be pending.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!request.is_finished(), "narinfo answered before the load");

        ready_tx.send(true).unwrap();
        let response = request.await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn export_windows_split_by_nar_bytes() {
        let mut manifest = Manifest::default();
        let sizes = [
            CLOSURE_EXPORT_WINDOW_BYTES / 2,
            CLOSURE_EXPORT_WINDOW_BYTES / 2, // fills the first window
            CLOSURE_EXPORT_WINDOW_BYTES * 3, // oversized: its own window
            1,
            1,
        ];
        let order: Vec<PathHash> = sizes
            .iter()
            .enumerate()
            .map(|(seed, &nar_size)| {
                let hash = test_path_hash(seed as u8);
                let mut entry = test_entry(seed as u8);
                entry.nar_size = nar_size;
                manifest.paths.insert(hash, entry);
                hash
            })
            .collect();
        let windows = export_windows(&manifest, &order);
        assert_eq!(
            windows,
            vec![
                vec![order[0], order[1]],
                vec![order[2]],
                vec![order[3], order[4]],
            ]
        );
    }

    #[test]
    fn plan_pack_reads_coalesces_gaps_and_reads_ahead() {
        let gap = PACK_FETCH_GAP_BYTES;
        let ahead = PACK_READ_AHEAD_BYTES;
        let pack_size = 100 * ahead;

        // Small gap merges, big gap splits.
        let spans = [(0, 100), (gap + 100, 100), (10 * ahead, 100)];
        let reads = plan_pack_reads(&spans, pack_size);
        assert_eq!(reads, vec![0..ahead, 10 * ahead..11 * ahead]);

        // Read-ahead never runs past the pack end...
        let reads = plan_pack_reads(&[(pack_size - 50, 50)], pack_size);
        assert_eq!(reads, vec![pack_size - 50..pack_size]);

        // ...and never truncates a run that is already larger.
        let reads = plan_pack_reads(&[(0, u32::MAX)], 100);
        assert_eq!(reads, vec![0..u64::from(u32::MAX)]);

        // Runs that only overlap after extension merge into one read.
        let reads = plan_pack_reads(&[(0, 100), (2 * gap, 100)], pack_size);
        assert_eq!(reads, vec![0..2 * gap + ahead]);
    }

    #[test]
    fn chunks_in_range_selects_only_fully_contained_chunks() {
        let index = PackChunkIndex {
            end: 1000,
            chunks: vec![
                (0, 100, ChunkHash::digest([0])),
                (100, 100, ChunkHash::digest([1])),
                (300, 100, ChunkHash::digest([2])),
                (350, 100, ChunkHash::digest([3])), // straddles range end
            ],
        };
        let selected: Vec<ChunkHash> = chunks_in_range(&index, &(100..400))
            .map(|(.., hash)| hash)
            .collect();
        assert_eq!(
            selected,
            vec![ChunkHash::digest([1]), ChunkHash::digest([2])]
        );
    }

    #[test]
    fn unused_path_locks_are_pruned() {
        let fetcher = ChunkFetcher::new(
            TwirpClient::new(reqwest::Client::new(), "http://unused", "token"),
            reqwest::Client::new(),
        );
        let held = fetcher.path_lock(test_path_hash(1));
        drop(fetcher.path_lock(test_path_hash(2)));
        // The next call prunes everything no request holds.
        let _other = fetcher.path_lock(test_path_hash(3));
        let locks = fetcher.path_locks.lock().unwrap();
        assert!(locks.contains_key(&test_path_hash(1)), "held lock kept");
        assert!(
            !locks.contains_key(&test_path_hash(2)),
            "released lock pruned"
        );
        drop(held);
    }

    #[test]
    fn chunk_cache_evicts_oldest_when_over_budget() {
        let mut cache = ChunkCache::default();
        // Three chunks of 100 MiB each: the third insert must evict the first.
        let big = Bytes::from(vec![0u8; 100 * 1024 * 1024]);
        for seed in 0..3u8 {
            cache.insert(ChunkHash::digest([seed]), big.clone());
        }
        assert!(
            cache.get(&ChunkHash::digest([0])).is_none(),
            "oldest evicted"
        );
        assert!(cache.get(&ChunkHash::digest([2])).is_some(), "newest kept");
        assert!(cache.total <= CHUNK_CACHE_BUDGET);
    }

    #[test]
    fn chunk_cache_hits_refresh_recency() {
        let mut cache = ChunkCache::default();
        let big = Bytes::from(vec![0u8; 100 * 1024 * 1024]);
        cache.insert(ChunkHash::digest([0]), big.clone());
        cache.insert(ChunkHash::digest([1]), big.clone());
        assert!(cache.get(&ChunkHash::digest([0])).is_some());
        cache.insert(ChunkHash::digest([2]), big.clone());
        assert!(cache.get(&ChunkHash::digest([0])).is_some(), "hit kept");
        assert!(
            cache.get(&ChunkHash::digest([1])).is_none(),
            "least recently used evicted"
        );
    }

    #[test]
    fn chunk_cache_insert_is_idempotent() {
        let mut cache = ChunkCache::default();
        let data = Bytes::from_static(b"chunk data");
        let hash = ChunkHash::digest(&data);
        cache.insert(hash, data.clone());
        cache.insert(hash, data.clone());
        assert_eq!(cache.total, data.len(), "no double counting");
    }

    #[test]
    fn narinfo_text_has_required_fields() {
        let store_dir = StoreDir::default();
        let mut entry = test_entry(7);
        entry.references = vec![
            format!("{}-dep-a", test_path_hash(8)).parse().unwrap(),
            format!("{}-dep-b", test_path_hash(9)).parse().unwrap(),
        ];

        let hash = test_path_hash(7).to_string();
        let text = String::from_utf8(narinfo_for_entry(&store_dir, &entry, &hash)).unwrap();

        assert!(
            text.contains(&format!(
                "StorePath: /nix/store/{}-test-7\n",
                test_path_hash(7)
            )),
            "narinfo:\n{text}"
        );
        assert!(text.contains("Compression: none\n"), "narinfo:\n{text}");
        assert!(text.contains("NarSize: 100\n"), "narinfo:\n{text}");
        assert!(text.contains("NarHash: sha256:"), "narinfo:\n{text}");
        assert!(
            text.contains("URL: nar/") && text.contains(&format!(".nar?hash={hash}\n")),
            "narinfo:\n{text}"
        );
        // References: both deps, full basenames.
        assert!(
            text.contains(&format!("{}-dep-a", test_path_hash(8))),
            "narinfo:\n{text}"
        );
        assert!(
            text.contains(&format!("{}-dep-b", test_path_hash(9))),
            "narinfo:\n{text}"
        );
        // No signature lines: hestia serves unsigned (?trusted=true).
        assert!(!text.contains("Sig: "), "narinfo:\n{text}");
    }
}
