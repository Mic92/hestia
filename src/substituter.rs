//! The substituter: Nix binary cache protocol served from the manifest.
//!
//! Three routes (axum), mounted into `hestia serve`:
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

use crate::chunker::{
    self, coalesce_adjacent, extract_chunk, flatten_tree, nar_from_chunks, pack_cache_key,
};
use crate::gha::twirp::{DownloadUrl, TwirpClient};
use crate::gha::{Error as GhaError, blob};
use crate::manifest::{
    ChunkHash, ChunkLocation, FileSystemObject, Hash32, Manifest, PackHash, PathEntry, PathHash,
};
use crate::pipeline::AccessLog;

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

/// Maximum number of pack reads in flight across all NAR requests. A pack
/// read is the unit of GHA cache API traffic (one Twirp URL lookup plus
/// Azure Range requests), so this bounds the total API concurrency no
/// matter how the packs distribute over paths.
const MAX_CONCURRENT_PACK_FETCHES: usize = 8;

/// How many times a pack Range read is retried after a transient failure
/// (connection drop, timeout, 5xx) before the whole NAR request gives up
/// and returns 404.
const TRANSIENT_READ_RETRIES: u32 = 2;

/// One manifest version plus the indexes the substituter needs.
#[derive(Default)]
struct ManifestView {
    manifest: Manifest,
    /// NAR hash → manifest path key, for `/nar/{narhash}.nar` requests that
    /// arrive without the `?hash=` parameter.
    by_nar_hash: BTreeMap<Hash32, PathHash>,
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
        Self {
            manifest,
            by_nar_hash,
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

/// Decompressed chunks kept in memory, evicted oldest-first once over
/// budget.
#[derive(Default)]
struct ChunkCache {
    chunks: HashMap<ChunkHash, Bytes>,
    order: VecDeque<ChunkHash>,
    total: usize,
}

impl ChunkCache {
    fn get(&self, hash: &ChunkHash) -> Option<Bytes> {
        self.chunks.get(hash).cloned()
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
        Arc::clone(
            self.path_locks
                .lock()
                .expect("path lock map poisoned")
                .entry(path)
                .or_default(),
        )
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
                self.url_cache
                    .lock()
                    .expect("url cache poisoned")
                    .insert(pack, (url.clone(), Instant::now()));
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
    ///
    /// Missing chunks are grouped by pack; adjacent chunks within a pack are
    /// coalesced into single Range requests; packs are fetched in parallel.
    /// Every chunk is hash-verified during extraction.
    async fn fetch_path_chunks(
        &self,
        manifest: &Manifest,
        path: PathHash,
        entry: &PathEntry,
    ) -> Result<BTreeMap<ChunkHash, Bytes>, FetchError> {
        // Serialize per path so concurrent NAR requests for the same
        // path do the work once.
        let lock = self.path_lock(path);
        let _guard = lock.lock().await;

        // All chunks the path's tree references (deduplicated).
        let needed: BTreeSet<ChunkHash> = flatten_tree(&entry.tree)
            .into_iter()
            .filter_map(|(_, node)| match node {
                FileSystemObject::Regular(regular) => Some(regular.contents.chunks.clone()),
                _ => None,
            })
            .flatten()
            .collect();

        let mut result: BTreeMap<ChunkHash, Bytes> = BTreeMap::new();
        let mut missing: BTreeMap<PackHash, Vec<(ChunkHash, ChunkLocation)>> = BTreeMap::new();
        {
            let cache = self.chunk_cache.lock().expect("chunk cache poisoned");
            for chunk in needed {
                if let Some(data) = cache.get(&chunk) {
                    result.insert(chunk, data);
                    continue;
                }
                let location = manifest
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
        let fetches = missing.into_iter().map(|(pack, chunks)| async move {
            let _permit = self
                .fetch_semaphore
                .acquire()
                .await
                .expect("fetch semaphore closed");
            self.fetch_from_pack(pack, chunks).await
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
    /// possible (adjacent chunks share a request).
    async fn fetch_from_pack(
        &self,
        pack: PackHash,
        mut chunks: Vec<(ChunkHash, ChunkLocation)>,
    ) -> Result<Vec<(ChunkHash, Bytes)>, FetchError> {
        chunks.sort_by_key(|(_, location)| location.offset);

        // Coalesce adjacent chunks into runs.
        let runs = coalesce_adjacent(chunks, |(_, location)| {
            (location.offset, location.compressed_size)
        });

        let mut fetched = Vec::new();
        for run in runs {
            let start = run[0].1.offset;
            let last = &run[run.len() - 1].1;
            let end = last.offset + u64::from(last.compressed_size);
            let data = self.read_pack_range(pack, start..end).await?;

            // Decompression + hash verification are CPU-bound: off the
            // runtime workers, like the write pipeline's compression
            // stages, so concurrent fetches cannot starve the hook socket.
            let extracted = tokio::task::spawn_blocking(move || {
                let mut extracted = Vec::with_capacity(run.len());
                for (hash, location) in run {
                    let from = (location.offset - start) as usize;
                    let to = from + location.compressed_size as usize;
                    // extract_chunk verifies the SHA-256 of the
                    // decompressed data; corrupt or truncated cache
                    // contents cannot pass.
                    let chunk = extract_chunk(&data[from..to], &hash)?;
                    extracted.push((hash, Bytes::from(chunk)));
                }
                Ok::<_, FetchError>(extracted)
            })
            .await
            .expect("chunk extraction task panicked")?;
            fetched.extend(extracted);
        }
        Ok(fetched)
    }
}

/// Callback invoked on every substituter request (the daemon uses it to
/// reset its idle-exit timer: an actively substituting Nix counts as
/// activity). The returned guard is held for the whole request so that
/// long downloads count as in-flight work instead of only touching the
/// idle clock once at request start.
pub type ActivityHook = Arc<dyn Fn() -> Box<dyn Send> + Send + Sync>;

/// The substituter's shared state and configuration.
pub struct Substituter {
    store_dir: StoreDir,
    manifest: ManifestStore,
    access_log: AccessLog,
    fetcher: ChunkFetcher,
    activity_hook: Option<ActivityHook>,
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
        }
    }

    /// Install a callback invoked on every request.
    pub fn with_activity_hook(mut self, hook: ActivityHook) -> Self {
        self.activity_hook = Some(hook);
        self
    }

    /// Build the axum router serving the binary cache protocol.
    pub fn into_router(self) -> Router {
        let state = Arc::new(self);
        Router::new()
            .route("/nix-cache-info", get(nix_cache_info))
            .route("/{file}", get(narinfo))
            .route("/nar/{file}", get(nar))
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
    Query(query): Query<NarQuery>,
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

    // A NAR download is an access (the GC liveness signal), just like a
    // narinfo hit. Nix caches narinfo lookups locally and may fetch a NAR
    // without re-requesting the narinfo, so recording only narinfo hits
    // would let GC collect paths that are actively being substituted.
    state.access_log.record(path_hash);

    // Fetch all chunks (concurrency-capped inside the fetcher); any
    // failure means 404 (Nix rebuilds or falls through), never partial
    // data.
    let chunks = match state
        .fetcher
        .fetch_path_chunks(&view.manifest, path_hash, entry)
        .await
    {
        Ok(chunks) => chunks,
        Err(err) => {
            eprintln!("hestia substituter: cannot serve NAR for {path_hash}: {err}");
            return StatusCode::NOT_FOUND.into_response();
        }
    };

    // NAR assembly and the full-NAR hash are CPU-bound and run as single
    // non-yielding polls (the Vec sink never pends), so they go off the
    // runtime workers: with many NAR requests assembling at once, a
    // multi-hundred-MiB path would otherwise pin every worker thread and
    // starve the hook socket (whose client times out and silently drops
    // path registrations).
    let tree = entry.tree.clone();
    let nar_size = entry.nar_size;
    let expected_hash = entry.nar_hash;
    let nar = tokio::task::spawn_blocking(move || {
        use futures_util::FutureExt as _;
        let nar = nar_from_chunks(&tree, &chunks)
            .now_or_never()
            .expect("NAR synthesis into a Vec sink never pends")
            .map_err(|err| format!("NAR synthesis failed: {err}"))?;
        // Final integrity gate: the response must hash to exactly the NAR
        // hash the manifest (and the narinfo we served) promised.
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
    .expect("NAR synthesis task panicked");
    let nar = match nar {
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
                contents: ChunkList { chunks: vec![] },
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
