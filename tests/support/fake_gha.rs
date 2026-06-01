//! Behavioral fake of the GitHub Actions cache backend.
//!
//! Not a request stub: a stateful HTTP server (axum) that implements the
//! same three surfaces hestia talks to in production, backed by blobs on a
//! tempdir:
//!
//! * Twirp: CreateCacheEntry / FinalizeCacheEntryUpload /
//!   GetCacheEntryDownloadURL, with real reservation semantics
//!   (`already_exists` blocks reserved-but-unfinalized keys too).
//! * Azure blob: PUT BlockBlob / GET with Range, gated on signed URLs.
//! * GitHub REST: list (prefix + pagination) / delete by key.
//!
//! Test-only injection endpoints simulate the failure modes GitHub will
//! throw at us in production:
//!
//! * `POST /test/evict/{key}`: LRU eviction of an entry.
//! * `POST /test/expire-urls`: invalidate all previously issued signed URLs
//!   (subsequent transfers get 403, like an expired SAS URL).
//! * `POST /test/expire-token-after/{n}`: the next `n` Twirp calls succeed,
//!   every later one gets HTTP 401 (expired `ACTIONS_RUNTIME_TOKEN`).
//! * `POST /test/fail-blob-reads/{n}`: the next `n` blob downloads get their
//!   connection dropped mid-body (Azure timeout / connection reset).
//! * `POST /test/stale-lookups/{0|1}`: download lookups hide the newest
//!   matching entry (eventual consistency: a just-finalized entry is not
//!   visible yet).
//! * `POST /test/exhaust-quota-after/{n}`: the next `n` CreateCacheEntry
//!   calls succeed, every later one gets a `resource_exhausted` Twirp error
//!   (the 10 GB repository cache quota is full).

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post, put};
use serde::Deserialize;
use serde_json::json;

use hestia::gha::rest::{RestClient, format_timestamp};
use hestia::gha::twirp::{
    CreateCacheEntryRequest, FinalizeCacheEntryUploadRequest, GetCacheEntryDownloadUrlRequest,
    TwirpClient,
};

const TWIRP_PATH: &str = "/twirp/github.actions.results.api.v1.CacheService";

#[derive(Debug, Clone)]
struct Entry {
    id: u64,
    key: String,
    version: String,
    finalized: bool,
    size: u64,
    created_at: u64,
    last_accessed_at: u64,
}

/// One recorded blob download (used by tests asserting fetch behavior,
/// e.g. "prefetched chunks are not fetched twice").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRequest {
    /// Cache key of the entry the blob belongs to (e.g. `pack-<hex>`).
    pub key: String,
    /// Raw `Range` header value, if the request was a range read.
    pub range: Option<String>,
}

#[derive(Debug)]
struct Inner {
    dir: PathBuf,
    entries: Vec<Entry>,
    next_id: u64,
    next_sig: u64,
    valid_sigs: HashSet<String>,
    clock: u64,
    blob_requests: Vec<BlobRequest>,
    /// `Some(n)`: n more Twirp calls succeed, then everything gets 401.
    twirp_calls_until_401: Option<u64>,
    /// `Some(n)`: n more CreateCacheEntry calls succeed, then quota errors.
    creates_until_quota: Option<u64>,
    /// Number of upcoming blob GETs whose connection gets dropped mid-body.
    blob_read_failures: u64,
    /// When set, download lookups pretend the newest matching entry does
    /// not exist yet (simulates the real service's eventual consistency).
    stale_lookups: bool,
}

impl Inner {
    /// Advance the clock by one second and return it. The clock counts unix
    /// seconds; tests control its absolute value via [`FakeGha::set_clock`]
    /// to simulate days passing between operations.
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    fn new_sig(&mut self) -> String {
        self.next_sig += 1;
        let sig = format!("sig{}", self.next_sig);
        self.valid_sigs.insert(sig.clone());
        sig
    }

    fn blob_path(&self, id: u64) -> PathBuf {
        self.dir.join(format!("blob-{id}"))
    }

    fn find(&self, key: &str, version: &str) -> Option<&Entry> {
        self.entries
            .iter()
            .find(|e| e.key == key && e.version == version)
    }

    fn remove_by_key(&mut self, key: &str) -> Vec<Entry> {
        let (removed, kept): (Vec<Entry>, Vec<Entry>) =
            self.entries.drain(..).partition(|e| e.key == key);
        self.entries = kept;
        for entry in &removed {
            let _ = std::fs::remove_file(self.blob_path(entry.id));
        }
        removed
    }
}

#[derive(Clone)]
struct AppState {
    inner: Arc<Mutex<Inner>>,
    base_url: String,
}

fn twirp_error(status: StatusCode, code: &str, msg: &str) -> Response {
    (status, Json(json!({ "code": code, "msg": msg }))).into_response()
}

// ---------------------------------------------------------------------------
// Twirp handlers
// ---------------------------------------------------------------------------

async fn twirp_create(State(state): State<AppState>, body: Bytes) -> Response {
    let Ok(request) = serde_json::from_slice::<CreateCacheEntryRequest>(&body) else {
        return twirp_error(StatusCode::BAD_REQUEST, "malformed", "bad json");
    };
    let mut inner = state.inner.lock().unwrap();
    // Quota injection: reservations are what the real service rejects when
    // the repository cache is full.
    if let Some(remaining) = &mut inner.creates_until_quota {
        if *remaining == 0 {
            return twirp_error(
                StatusCode::TOO_MANY_REQUESTS,
                "resource_exhausted",
                "cache storage quota has been exceeded",
            );
        }
        *remaining -= 1;
    }
    if inner.find(&request.key, &request.version).is_some() {
        return twirp_error(
            StatusCode::CONFLICT,
            "already_exists",
            "cache entry with the same key, version, and scope already exists",
        );
    }
    inner.next_id += 1;
    let id = inner.next_id;
    let created_at = inner.tick();
    inner.entries.push(Entry {
        id,
        key: request.key,
        version: request.version,
        finalized: false,
        size: 0,
        created_at,
        last_accessed_at: created_at,
    });
    let sig = inner.new_sig();
    let url = format!("{}/blob/{id}?sig={sig}", state.base_url);
    Json(json!({ "ok": true, "signed_upload_url": url })).into_response()
}

async fn twirp_finalize(State(state): State<AppState>, body: Bytes) -> Response {
    let Ok(request) = serde_json::from_slice::<FinalizeCacheEntryUploadRequest>(&body) else {
        return twirp_error(StatusCode::BAD_REQUEST, "malformed", "bad json");
    };
    let mut inner = state.inner.lock().unwrap();
    let Some(position) = inner
        .entries
        .iter()
        .position(|e| e.key == request.key && e.version == request.version && !e.finalized)
    else {
        return twirp_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "no pending entry for this key",
        );
    };
    let blob_path = inner.blob_path(inner.entries[position].id);
    let actual_size = std::fs::metadata(&blob_path).map(|m| m.len()).ok();
    if actual_size != Some(request.size_bytes) {
        return twirp_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            &format!(
                "uploaded size {actual_size:?} does not match declared size {}",
                request.size_bytes
            ),
        );
    }
    let entry = &mut inner.entries[position];
    entry.finalized = true;
    entry.size = request.size_bytes;
    let id = entry.id;
    Json(json!({ "ok": true, "entry_id": id.to_string() })).into_response()
}

async fn twirp_download_url(State(state): State<AppState>, body: Bytes) -> Response {
    let Ok(request) = serde_json::from_slice::<GetCacheEntryDownloadUrlRequest>(&body) else {
        return twirp_error(StatusCode::BAD_REQUEST, "malformed", "bad json");
    };
    let mut inner = state.inner.lock().unwrap();

    // Fidelity note (verified against the production service): only
    // `restore_keys` are consulted, as ordered prefix matches with the
    // newest entry winning per prefix. The `key` field alone matches
    // nothing — a request with empty restore keys always misses, even for
    // entries that exist. Clients must send the key as a restore key
    // (go-actions-cache does the same).
    let matched = request.restore_keys.iter().find_map(|prefix| {
        let mut matching: Vec<&Entry> = inner
            .entries
            .iter()
            .filter(|e| e.finalized && e.version == request.version && e.key.starts_with(prefix))
            .collect();
        matching.sort_by_key(|e| std::cmp::Reverse(e.created_at));
        // Eventual-consistency injection: the newest entry is not visible
        // yet, so the lookup returns the previous one (or misses).
        let skip = usize::from(inner.stale_lookups);
        matching.get(skip).copied().cloned()
    });

    match matched {
        None => Json(json!({ "ok": false })).into_response(),
        Some(entry) => {
            let sig = inner.new_sig();
            let url = format!("{}/blob/{}?sig={sig}", state.base_url, entry.id);
            Json(json!({
                "ok": true,
                "signed_download_url": url,
                "matched_key": entry.key,
            }))
            .into_response()
        }
    }
}

async fn twirp_dispatch(
    State(state): State<AppState>,
    Path(method): Path<String>,
    body: Bytes,
) -> Response {
    // Token-expiry injection: the real service rejects every Twirp call with
    // HTTP 401 once the runtime JWT has expired (~6h lifetime).
    {
        let mut inner = state.inner.lock().unwrap();
        if let Some(remaining) = &mut inner.twirp_calls_until_401 {
            if *remaining == 0 {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({ "code": "unauthenticated", "msg": "token expired" })),
                )
                    .into_response();
            }
            *remaining -= 1;
        }
    }
    match method.as_str() {
        "CreateCacheEntry" => twirp_create(State(state), body).await,
        "FinalizeCacheEntryUpload" => twirp_finalize(State(state), body).await,
        "GetCacheEntryDownloadURL" => twirp_download_url(State(state), body).await,
        _ => twirp_error(StatusCode::NOT_FOUND, "bad_route", "unknown rpc"),
    }
}

// ---------------------------------------------------------------------------
// Azure blob handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct SigQuery {
    #[serde(default)]
    sig: String,
}

async fn blob_put(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Query(query): Query<SigQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let inner = state.inner.lock().unwrap();
    if !inner.valid_sigs.contains(&query.sig) {
        return (StatusCode::FORBIDDEN, "signature expired").into_response();
    }
    if headers.get("x-ms-blob-type").and_then(|v| v.to_str().ok()) != Some("BlockBlob") {
        return (StatusCode::BAD_REQUEST, "missing x-ms-blob-type").into_response();
    }
    if !inner.entries.iter().any(|e| e.id == id) {
        return (StatusCode::NOT_FOUND, "no such blob").into_response();
    }
    let path = inner.blob_path(id);
    std::fs::write(path, &body).unwrap();
    StatusCode::CREATED.into_response()
}

/// Build a blob response, optionally dropping the connection mid-body.
///
/// The injected failure advertises the full Content-Length but streams only
/// half the bytes before erroring out: clients see a reset/truncated
/// connection exactly like an Azure-side timeout.
fn blob_response(status: StatusCode, data: Vec<u8>, drop_mid_body: bool) -> Response {
    if !drop_mid_body {
        return (status, data).into_response();
    }
    let half = data.len() / 2;
    let body = axum::body::Body::from_stream(futures_util::stream::iter([
        Ok::<_, std::io::Error>(Bytes::from(data[..half].to_vec())),
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "injected connection drop",
        )),
    ]));
    Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, data.len())
        .body(body)
        .expect("static response parts are valid")
}

/// Parse `bytes=start-end` (both inclusive) / `bytes=start-`.
fn parse_range(value: &str, len: u64) -> Option<(u64, u64)> {
    let spec = value.strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: u64 = if end.is_empty() {
        len.saturating_sub(1)
    } else {
        end.parse().ok()?
    };
    (start <= end && start < len).then_some((start, end.min(len.saturating_sub(1))))
}

async fn blob_get(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    Query(query): Query<SigQuery>,
    headers: HeaderMap,
) -> Response {
    let mut inner = state.inner.lock().unwrap();
    if !inner.valid_sigs.contains(&query.sig) {
        return (StatusCode::FORBIDDEN, "signature expired").into_response();
    }
    let Some(position) = inner.entries.iter().position(|e| e.id == id) else {
        return (StatusCode::NOT_FOUND, "no such blob").into_response();
    };
    let path = inner.blob_path(id);
    let Ok(data) = std::fs::read(&path) else {
        return (StatusCode::NOT_FOUND, "blob not uploaded").into_response();
    };

    // Downloads bump the LRU clock (verified against the real service).
    let now = inner.tick();
    inner.entries[position].last_accessed_at = now;

    // Record the download for tests that assert fetch behavior.
    let request = BlobRequest {
        key: inner.entries[position].key.clone(),
        range: headers
            .get(header::RANGE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
    };
    inner.blob_requests.push(request);

    // Connection-drop injection (Azure timeout simulation).
    let drop_mid_body = if inner.blob_read_failures > 0 {
        inner.blob_read_failures -= 1;
        true
    } else {
        false
    };

    match headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(|v| parse_range(v, data.len() as u64))
    {
        // Range requested but unsatisfiable.
        Some(None) => (StatusCode::RANGE_NOT_SATISFIABLE, "bad range").into_response(),
        Some(Some((start, end))) => {
            let slice = data[start as usize..=end as usize].to_vec();
            blob_response(StatusCode::PARTIAL_CONTENT, slice, drop_mid_body)
        }
        None => blob_response(StatusCode::OK, data, drop_mid_body),
    }
}

// ---------------------------------------------------------------------------
// GitHub REST handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default)]
    key: String,
    #[serde(default)]
    page: Option<u64>,
    #[serde(default)]
    per_page: Option<u64>,
}

fn rest_entry_json(entry: &Entry) -> serde_json::Value {
    json!({
        "id": entry.id,
        "ref": "refs/heads/main",
        "key": entry.key,
        "version": entry.version,
        // The real REST API reports RFC 3339 UTC timestamps.
        "last_accessed_at": format_timestamp(entry.last_accessed_at),
        "created_at": format_timestamp(entry.created_at),
        "size_in_bytes": entry.size,
    })
}

async fn rest_list(State(state): State<AppState>, Query(query): Query<ListQuery>) -> Response {
    let inner = state.inner.lock().unwrap();
    let mut matching: Vec<&Entry> = inner
        .entries
        .iter()
        .filter(|e| e.finalized && e.key.starts_with(&query.key))
        .collect();
    matching.sort_by_key(|e| std::cmp::Reverse(e.last_accessed_at));

    let per_page = query.per_page.unwrap_or(30).max(1) as usize;
    let page = query.page.unwrap_or(1).max(1) as usize;
    let start = (page - 1) * per_page;
    let page_entries: Vec<serde_json::Value> = matching
        .iter()
        .skip(start)
        .take(per_page)
        .map(|e| rest_entry_json(e))
        .collect();

    Json(json!({
        "total_count": matching.len(),
        "actions_caches": page_entries,
    }))
    .into_response()
}

async fn rest_delete(State(state): State<AppState>, Query(query): Query<ListQuery>) -> Response {
    let mut inner = state.inner.lock().unwrap();
    let removed = inner.remove_by_key(&query.key);
    if removed.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Not Found" })),
        )
            .into_response();
    }
    let removed_json: Vec<serde_json::Value> = removed.iter().map(rest_entry_json).collect();
    Json(json!({
        "total_count": removed_json.len(),
        "actions_caches": removed_json,
    }))
    .into_response()
}

// ---------------------------------------------------------------------------
// Test-only injection endpoints
// ---------------------------------------------------------------------------

async fn test_evict(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    let mut inner = state.inner.lock().unwrap();
    let removed = inner.remove_by_key(&key);
    Json(json!({ "evicted": removed.len() })).into_response()
}

async fn test_expire_urls(State(state): State<AppState>) -> Response {
    let mut inner = state.inner.lock().unwrap();
    let count = inner.valid_sigs.len();
    inner.valid_sigs.clear();
    Json(json!({ "expired": count })).into_response()
}

async fn test_expire_token_after(
    State(state): State<AppState>,
    Path(calls): Path<u64>,
) -> Response {
    state.inner.lock().unwrap().twirp_calls_until_401 = Some(calls);
    Json(json!({ "calls_until_401": calls })).into_response()
}

async fn test_exhaust_quota_after(
    State(state): State<AppState>,
    Path(creates): Path<u64>,
) -> Response {
    state.inner.lock().unwrap().creates_until_quota = Some(creates);
    Json(json!({ "creates_until_quota": creates })).into_response()
}

async fn test_fail_blob_reads(State(state): State<AppState>, Path(reads): Path<u64>) -> Response {
    state.inner.lock().unwrap().blob_read_failures = reads;
    Json(json!({ "blob_read_failures": reads })).into_response()
}

async fn test_stale_lookups(State(state): State<AppState>, Path(on): Path<u8>) -> Response {
    state.inner.lock().unwrap().stale_lookups = on != 0;
    Json(json!({ "stale_lookups": on != 0 })).into_response()
}

// ---------------------------------------------------------------------------
// Server wiring
// ---------------------------------------------------------------------------

/// A running fake GHA cache backend.
pub struct FakeGha {
    /// Base URL, used both as `ACTIONS_RESULTS_URL` and as the GitHub API URL.
    pub base_url: String,
    /// Repository slug the REST routes are mounted under.
    pub repo: String,
    inner: Arc<Mutex<Inner>>,
    task: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl FakeGha {
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().expect("create tempdir");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake gha listener");
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let inner = Arc::new(Mutex::new(Inner {
            dir: dir.path().to_path_buf(),
            entries: Vec::new(),
            next_id: 0,
            next_sig: 0,
            valid_sigs: HashSet::new(),
            clock: 0,
            blob_requests: Vec::new(),
            twirp_calls_until_401: None,
            creates_until_quota: None,
            blob_read_failures: 0,
            stale_lookups: false,
        }));
        let state = AppState {
            inner: Arc::clone(&inner),
            base_url: base_url.clone(),
        };

        let router = Router::new()
            .route(&format!("{TWIRP_PATH}/{{method}}"), post(twirp_dispatch))
            .route("/blob/{id}", put(blob_put).get(blob_get))
            .route(
                "/repos/{owner}/{repo}/actions/caches",
                get(rest_list).delete(rest_delete),
            )
            .route("/test/evict/{key}", post(test_evict))
            .route("/test/expire-urls", post(test_expire_urls))
            .route(
                "/test/expire-token-after/{calls}",
                post(test_expire_token_after),
            )
            .route(
                "/test/exhaust-quota-after/{creates}",
                post(test_exhaust_quota_after),
            )
            .route("/test/fail-blob-reads/{reads}", post(test_fail_blob_reads))
            .route("/test/stale-lookups/{on}", post(test_stale_lookups))
            .with_state(state);

        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        Self {
            base_url,
            repo: "fake/repo".to_string(),
            inner,
            task,
            _dir: dir,
        }
    }

    /// All blob downloads served so far, in order.
    pub fn blob_requests(&self) -> Vec<BlobRequest> {
        self.inner.lock().unwrap().blob_requests.clone()
    }

    /// Set the fake's clock (unix seconds). Subsequent operations record
    /// `created_at` / `last_accessed_at` values just after this instant,
    /// which lets GC tests simulate days passing.
    pub fn set_clock(&self, unix_seconds: u64) {
        self.inner.lock().unwrap().clock = unix_seconds;
    }

    /// Twirp client pointed at this fake.
    pub fn twirp(&self, http: &reqwest::Client) -> TwirpClient {
        TwirpClient::new(http.clone(), &self.base_url, "fake-runtime-token")
    }

    /// REST client pointed at this fake.
    pub fn rest(&self, http: &reqwest::Client) -> RestClient {
        RestClient::new(
            http.clone(),
            &self.base_url,
            &self.repo,
            "fake-github-token",
        )
    }

    /// Simulate LRU eviction of `key` (entry and blob disappear).
    pub async fn evict(&self, http: &reqwest::Client, key: &str) {
        let url = format!("{}/test/evict/{key}", self.base_url);
        let response = http.post(&url).send().await.expect("evict request");
        assert!(response.status().is_success());
    }

    /// Invalidate all previously issued signed URLs (simulates SAS expiry).
    pub async fn expire_urls(&self, http: &reqwest::Client) {
        let url = format!("{}/test/expire-urls", self.base_url);
        let response = http.post(&url).send().await.expect("expire request");
        assert!(response.status().is_success());
    }

    /// Let `calls` more Twirp requests succeed, then reject everything with
    /// HTTP 401 (simulates the ~6h runtime-token expiry).
    pub async fn expire_token_after(&self, http: &reqwest::Client, calls: u64) {
        let url = format!("{}/test/expire-token-after/{calls}", self.base_url);
        let response = http.post(&url).send().await.expect("expire-token request");
        assert!(response.status().is_success());
    }

    /// Let `creates` more CreateCacheEntry calls succeed, then reject new
    /// reservations with `resource_exhausted` (simulates the full 10 GB
    /// repository quota).
    pub async fn exhaust_quota_after(&self, http: &reqwest::Client, creates: u64) {
        let url = format!("{}/test/exhaust-quota-after/{creates}", self.base_url);
        let response = http.post(&url).send().await.expect("exhaust-quota request");
        assert!(response.status().is_success());
    }

    /// Drop the connection mid-body on the next `reads` blob downloads
    /// (simulates Azure timeouts / connection resets). `0` clears the
    /// injection.
    pub async fn fail_blob_reads(&self, http: &reqwest::Client, reads: u64) {
        let url = format!("{}/test/fail-blob-reads/{reads}", self.base_url);
        let response = http
            .post(&url)
            .send()
            .await
            .expect("fail-blob-reads request");
        assert!(response.status().is_success());
    }

    /// Toggle eventual-consistency simulation: while on, download lookups
    /// hide the newest matching entry (a just-finalized entry is "not
    /// visible yet").
    pub async fn set_stale_lookups(&self, http: &reqwest::Client, on: bool) {
        let url = format!("{}/test/stale-lookups/{}", self.base_url, u8::from(on));
        let response = http.post(&url).send().await.expect("stale-lookups request");
        assert!(response.status().is_success());
    }
}

impl Drop for FakeGha {
    fn drop(&mut self) {
        self.task.abort();
    }
}
