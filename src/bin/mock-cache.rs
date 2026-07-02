//! Standalone mock of the GitHub Actions cache backend, for benchmarking a
//! real hestia daemon without a GitHub runner.
//!
//! Implements the three surfaces hestia talks to, reusing hestia's own wire
//! types so the encodings cannot drift:
//!
//! * Twirp: CreateCacheEntry / FinalizeCacheEntryUpload /
//!   GetCacheEntryDownloadURL, with real reservation semantics.
//! * Azure blob: PUT BlockBlob / GET with Range.
//! * GitHub REST: list (prefix + pagination + sort) / delete by key.
//!
//! Blobs are stored on disk under `--data-dir`. This is a plain, always-
//! healthy server: no token expiry, quota, eviction, or eventual-consistency
//! injection (that lives in the test-only `FakeGha`). Point a daemon at it:
//!
//! ```text
//! $ mock-cache --addr 127.0.0.1:8080
//! # exports its env; then in another shell:
//! $ eval "$(mock-cache --print-env --addr 127.0.0.1:8080)"
//! $ hestia serve ...
//! ```

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post, put};
use clap::Parser;
use serde::Deserialize;
use serde_json::json;

use hestia::gha::rest::format_timestamp;
use hestia::gha::twirp::{
    CreateCacheEntryRequest, FinalizeCacheEntryUploadRequest, GetCacheEntryDownloadUrlRequest,
};
use hestia::pipeline::now_unix;

const TWIRP_PATH: &str = "/twirp/github.actions.results.api.v1.CacheService";
const RUNTIME_TOKEN: &str = "mock-runtime-token";
const GITHUB_TOKEN: &str = "mock-github-token";
const REPO: &str = "mock/repo";

#[derive(Parser)]
#[command(about = "Local mock of the GitHub Actions cache backend")]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    /// Directory for blob storage (created if missing; default: a temp dir).
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Print the env vars a hestia process needs, then exit.
    #[arg(long)]
    print_env: bool,
}

#[derive(Clone)]
struct Entry {
    id: u64,
    key: String,
    version: String,
    finalized: bool,
    size: u64,
    created_at: u64,
    last_accessed_at: u64,
}

struct Inner {
    dir: PathBuf,
    entries: Vec<Entry>,
    next_id: u64,
}

impl Inner {
    fn blob_path(&self, id: u64) -> PathBuf {
        self.dir.join(format!("blob-{id}"))
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

// --- Twirp ----------------------------------------------------------------

async fn twirp_create(State(state): State<AppState>, body: Bytes) -> Response {
    let Ok(request) = serde_json::from_slice::<CreateCacheEntryRequest>(&body) else {
        return twirp_error(StatusCode::BAD_REQUEST, "malformed", "bad json");
    };
    let mut inner = state.inner.lock().unwrap();
    if inner
        .entries
        .iter()
        .any(|e| e.key == request.key && e.version == request.version)
    {
        return twirp_error(
            StatusCode::CONFLICT,
            "already_exists",
            "cache entry with the same key, version, and scope already exists",
        );
    }
    inner.next_id += 1;
    let id = inner.next_id;
    let created_at = now_unix();
    inner.entries.push(Entry {
        id,
        key: request.key,
        version: request.version,
        finalized: false,
        size: 0,
        created_at,
        last_accessed_at: created_at,
    });
    let url = format!("{}/blob/{id}?sig=ok", state.base_url);
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
    let inner = state.inner.lock().unwrap();
    // Production matches only restore_keys, as ordered prefixes, newest
    // entry winning.
    let matched = request.restore_keys.iter().find_map(|prefix| {
        inner
            .entries
            .iter()
            .filter(|e| e.finalized && e.version == request.version && e.key.starts_with(prefix))
            .max_by_key(|e| e.created_at)
            .cloned()
    });
    match matched {
        None => Json(json!({ "ok": false })).into_response(),
        Some(entry) => {
            let url = format!("{}/blob/{}?sig=ok", state.base_url, entry.id);
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
    match method.as_str() {
        "CreateCacheEntry" => twirp_create(State(state), body).await,
        "FinalizeCacheEntryUpload" => twirp_finalize(State(state), body).await,
        "GetCacheEntryDownloadURL" => twirp_download_url(State(state), body).await,
        _ => twirp_error(StatusCode::NOT_FOUND, "bad_route", "unknown rpc"),
    }
}

// --- Azure blob -----------------------------------------------------------

async fn blob_put(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if headers.get("x-ms-blob-type").and_then(|v| v.to_str().ok()) != Some("BlockBlob") {
        return (StatusCode::BAD_REQUEST, "missing x-ms-blob-type").into_response();
    }
    let path = {
        let inner = state.inner.lock().unwrap();
        if !inner.entries.iter().any(|e| e.id == id) {
            return (StatusCode::NOT_FOUND, "no such blob").into_response();
        }
        inner.blob_path(id)
    };
    // Write outside the lock: pack blobs are large and this must not block
    // concurrent transfers.
    if std::fs::write(&path, &body).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "write failed").into_response();
    }
    StatusCode::CREATED.into_response()
}

/// Parse `bytes=start-end` (inclusive) / `bytes=start-`.
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
    headers: HeaderMap,
) -> Response {
    let path = {
        let mut inner = state.inner.lock().unwrap();
        let Some(entry) = inner.entries.iter_mut().find(|e| e.id == id) else {
            return (StatusCode::NOT_FOUND, "no such blob").into_response();
        };
        entry.last_accessed_at = now_unix();
        inner.blob_path(id)
    };
    let Ok(data) = std::fs::read(&path) else {
        return (StatusCode::NOT_FOUND, "blob not uploaded").into_response();
    };
    match headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(|v| parse_range(v, data.len() as u64))
    {
        Some(None) => (StatusCode::RANGE_NOT_SATISFIABLE, "bad range").into_response(),
        Some(Some((start, end))) => (
            StatusCode::PARTIAL_CONTENT,
            data[start as usize..=end as usize].to_vec(),
        )
            .into_response(),
        None => (StatusCode::OK, data).into_response(),
    }
}

// --- GitHub REST ----------------------------------------------------------

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default)]
    key: String,
    #[serde(default)]
    page: Option<u64>,
    #[serde(default)]
    per_page: Option<u64>,
    #[serde(default)]
    sort: Option<String>,
    #[serde(default)]
    direction: Option<String>,
}

fn rest_entry_json(entry: &Entry) -> serde_json::Value {
    json!({
        "id": entry.id,
        "ref": "refs/heads/main",
        "key": entry.key,
        "version": entry.version,
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
    match (query.sort.as_deref(), query.direction.as_deref()) {
        (Some("created_at"), Some("asc")) => matching.sort_by_key(|e| e.created_at),
        _ => matching.sort_by_key(|e| std::cmp::Reverse(e.last_accessed_at)),
    }
    let per_page = query.per_page.unwrap_or(30).max(1) as usize;
    let page = query.page.unwrap_or(1).max(1) as usize;
    let start = (page - 1) * per_page;
    let page_entries: Vec<serde_json::Value> = matching
        .iter()
        .skip(start)
        .take(per_page)
        .map(|e| rest_entry_json(e))
        .collect();
    Json(json!({ "total_count": matching.len(), "actions_caches": page_entries })).into_response()
}

async fn rest_delete(State(state): State<AppState>, Query(query): Query<ListQuery>) -> Response {
    let mut inner = state.inner.lock().unwrap();
    let (removed, kept): (Vec<Entry>, Vec<Entry>) =
        inner.entries.drain(..).partition(|e| e.key == query.key);
    inner.entries = kept;
    for entry in &removed {
        let _ = std::fs::remove_file(inner.blob_path(entry.id));
    }
    if removed.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "message": "Not Found" })),
        )
            .into_response();
    }
    let removed_json: Vec<serde_json::Value> = removed.iter().map(rest_entry_json).collect();
    Json(json!({ "total_count": removed_json.len(), "actions_caches": removed_json }))
        .into_response()
}

fn env_exports(base_url: &str) -> String {
    [
        ("ACTIONS_RESULTS_URL", base_url),
        ("ACTIONS_RUNTIME_TOKEN", RUNTIME_TOKEN),
        ("GITHUB_API_URL", base_url),
        ("GITHUB_TOKEN", GITHUB_TOKEN),
        ("GITHUB_REPOSITORY", REPO),
    ]
    .map(|(k, v)| format!("export {k}={v}"))
    .join("\n")
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.print_env {
        println!("{}", env_exports(&format!("http://{}", args.addr)));
        return;
    }

    let dir = args.data_dir.unwrap_or_else(|| {
        std::env::temp_dir().join(format!("hestia-mock-cache-{}", std::process::id()))
    });
    std::fs::create_dir_all(&dir).expect("create data dir");

    let listener = tokio::net::TcpListener::bind(&args.addr)
        .await
        .unwrap_or_else(|e| panic!("bind {}: {e}", args.addr));
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");

    let state = AppState {
        inner: Arc::new(Mutex::new(Inner {
            dir: dir.clone(),
            entries: Vec::new(),
            next_id: 0,
        })),
        base_url: base_url.clone(),
    };

    let router = Router::new()
        .route(&format!("{TWIRP_PATH}/{{method}}"), post(twirp_dispatch))
        // Pack blobs run to tens of MiB; lift axum's 2 MB default cap.
        .route(
            "/blob/{id}",
            put(blob_put)
                .get(blob_get)
                .layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/repos/{owner}/{repo}/actions/caches",
            get(rest_list).delete(rest_delete),
        )
        .with_state(state);

    eprintln!(
        "mock-cache listening on {base_url}, blobs in {}",
        dir.display()
    );
    eprintln!(
        "point a hestia process at it with:\n{}",
        env_exports(&base_url)
    );

    axum::serve(listener, router).await.expect("serve");
}
