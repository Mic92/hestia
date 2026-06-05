//! Regression tests for the GitHub REST client (`gha::rest`) against
//! realistic server behavior that the friendly fake backend
//! (`tests/support/fake_gha.rs`) does not exhibit:
//!
//! * GitHub's default listing order is `last_accessed_at desc`, which is
//!   *mutable*: every cache download (by any concurrent CI job) bumps an
//!   entry's `last_accessed_at` and reorders the listing between page
//!   fetches. Page-numbered pagination over a reordering collection skips
//!   and duplicates entries.
//! * `total_count` is server-controlled data; pagination termination must
//!   not depend on it ever being consistent with the returned pages.
//! * Mutating requests trip GitHub's secondary rate limit when sent
//!   back-to-back (GC deletes dozens of entries in one sweep). The client
//!   must pace them and retry rate-limited responses.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use serde_json::json;

use hestia::gha::rest::{RestClient, format_timestamp};

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
struct Entry {
    key: String,
    created_at: u64,
    last_accessed_at: u64,
}

/// A GitHub-like cache listing endpoint: honors `key`, `page`, `per_page`,
/// `sort` and `direction` query parameters with GitHub's documented
/// defaults (`sort=last_accessed_at`, `direction=desc`).
struct GitHubLike {
    entries: Vec<Entry>,
    /// When true, a concurrent download bumps the least-recently-used
    /// entry's `last_accessed_at` after every list request (the LRU
    /// reordering a busy repository exhibits while GC paginates).
    concurrent_downloads: bool,
}

impl GitHubLike {
    fn bump_least_recently_used(&mut self) {
        let Some(max) = self.entries.iter().map(|e| e.last_accessed_at).max() else {
            return;
        };
        if let Some(entry) = self.entries.iter_mut().min_by_key(|e| e.last_accessed_at) {
            entry.last_accessed_at = max + 1;
        }
    }
}

async fn list_handler(
    State(state): State<Arc<Mutex<GitHubLike>>>,
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let mut inner = state.lock().unwrap();

    let key_prefix = params.get("key").cloned().unwrap_or_default();
    let per_page: usize = params
        .get("per_page")
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let page: usize = params.get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
    // GitHub's documented defaults for this endpoint.
    let sort = params
        .get("sort")
        .cloned()
        .unwrap_or_else(|| "last_accessed_at".to_string());
    let direction = params
        .get("direction")
        .cloned()
        .unwrap_or_else(|| "desc".to_string());

    let mut matching: Vec<Entry> = inner
        .entries
        .iter()
        .filter(|e| e.key.starts_with(key_prefix.as_str()))
        .cloned()
        .collect();
    matching.sort_by_key(|e| match sort.as_str() {
        "created_at" => e.created_at,
        _ => e.last_accessed_at,
    });
    if direction == "desc" {
        matching.reverse();
    }

    let page_entries: Vec<serde_json::Value> = matching
        .iter()
        .skip((page - 1) * per_page)
        .take(per_page)
        .enumerate()
        .map(|(i, e)| {
            json!({
                "id": i,
                "ref": "refs/heads/main",
                "key": e.key,
                "version": "v",
                "created_at": format_timestamp(e.created_at),
                "last_accessed_at": format_timestamp(e.last_accessed_at),
                "size_in_bytes": 1,
            })
        })
        .collect();

    // Simulate concurrent CI jobs downloading packs while we paginate:
    // every download bumps last_accessed_at, reordering the default
    // listing between this page fetch and the next one.
    if inner.concurrent_downloads {
        inner.bump_least_recently_used();
    }

    Json(json!({
        "total_count": matching.len(),
        "actions_caches": page_entries,
    }))
}

/// A pathological listing endpoint: claims entries exist (`total_count`)
/// but never returns any. Server-controlled data must not be able to make
/// the client loop forever.
async fn empty_pages_handler() -> Json<serde_json::Value> {
    Json(json!({
        "total_count": 5,
        "actions_caches": [],
    }))
}

/// A listing endpoint whose `total_count` underreports the entries it
/// actually serves (an eventually consistent counter, or one shrunk by
/// concurrent deletions). Pagination termination must come from an empty
/// page, never from trusting the counter.
async fn understated_total_count_handler(
    Query(params): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let per_page: usize = params
        .get("per_page")
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let page: usize = params.get("page").and_then(|v| v.parse().ok()).unwrap_or(1);
    let page_entries: Vec<serde_json::Value> = (0..150usize)
        .skip((page - 1) * per_page)
        .take(per_page)
        .map(|i| {
            json!({
                "id": i,
                "ref": "refs/heads/main",
                "key": format!("pack-{i:03}"),
                "version": "v",
                "created_at": format_timestamp(1_000 + i as u64),
                "last_accessed_at": format_timestamp(1_000 + i as u64),
                "size_in_bytes": 1,
            })
        })
        .collect();
    Json(json!({
        "total_count": 1,
        "actions_caches": page_entries,
    }))
}

async fn start_server(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}

async fn start_github_like(entries: Vec<Entry>, concurrent_downloads: bool) -> String {
    let state = Arc::new(Mutex::new(GitHubLike {
        entries,
        concurrent_downloads,
    }));
    let router = Router::new()
        .route("/repos/{owner}/{repo}/actions/caches", get(list_handler))
        .with_state(state);
    start_server(router).await
}

/// A cache endpoint that rate-limits the first `rate_limited_requests`
/// calls the way GitHub's secondary rate limit does (403 + Retry-After +
/// a "secondary rate limit" message), and records when each request
/// arrived.
struct RateLimitedServer {
    rate_limited_requests: usize,
    requests: Vec<Instant>,
}

impl RateLimitedServer {
    /// Returns the rate-limit response if this request should be limited.
    fn admit(&mut self) -> Option<Response> {
        self.requests.push(Instant::now());
        if self.requests.len() > self.rate_limited_requests {
            return None;
        }
        Some(
            (
                StatusCode::FORBIDDEN,
                [("retry-after", "1")],
                Json(json!({
                    "message": "You have exceeded a secondary rate limit. Please wait a few minutes before you try again.",
                    "documentation_url": "https://docs.github.com/rest/overview/rate-limits-for-the-rest-api#about-secondary-rate-limits",
                })),
            )
                .into_response(),
        )
    }
}

fn single_entry_listing(key: &str) -> Json<serde_json::Value> {
    Json(json!({
        "total_count": 1,
        "actions_caches": [{
            "id": 1,
            "ref": "refs/heads/main",
            "key": key,
            "version": "v",
            "created_at": format_timestamp(1_000),
            "last_accessed_at": format_timestamp(1_000),
            "size_in_bytes": 1,
        }],
    }))
}

async fn rate_limited_delete_handler(
    State(state): State<Arc<Mutex<RateLimitedServer>>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if let Some(limited) = state.lock().unwrap().admit() {
        return limited;
    }
    let key = params.get("key").cloned().unwrap_or_default();
    single_entry_listing(&key).into_response()
}

async fn rate_limited_list_handler(State(state): State<Arc<Mutex<RateLimitedServer>>>) -> Response {
    if let Some(limited) = state.lock().unwrap().admit() {
        return limited;
    }
    single_entry_listing("pack-listed").into_response()
}

async fn start_rate_limited_server(
    rate_limited_requests: usize,
) -> (String, Arc<Mutex<RateLimitedServer>>) {
    let state = Arc::new(Mutex::new(RateLimitedServer {
        rate_limited_requests,
        requests: Vec::new(),
    }));
    let router = Router::new()
        .route(
            "/repos/{owner}/{repo}/actions/caches",
            get(rate_limited_list_handler).delete(rate_limited_delete_handler),
        )
        .with_state(state.clone());
    (start_server(router).await, state)
}

/// A flaky endpoint: the first `failures` requests get a 502, then it
/// serves normally (the intermittent 5xx GitHub's results service throws
/// under load).
struct FlakyServer {
    failures: usize,
    requests: usize,
}

async fn flaky_list_handler(State(state): State<Arc<Mutex<FlakyServer>>>) -> Response {
    let mut inner = state.lock().unwrap();
    inner.requests += 1;
    if inner.requests <= inner.failures {
        return (StatusCode::BAD_GATEWAY, "upstream error").into_response();
    }
    single_entry_listing("pack-flaky").into_response()
}

#[tokio::test]
async fn list_retries_transient_server_errors() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let state = Arc::new(Mutex::new(FlakyServer {
            failures: 2,
            requests: 0,
        }));
        let router = Router::new()
            .route(
                "/repos/{owner}/{repo}/actions/caches",
                get(flaky_list_handler),
            )
            .with_state(state.clone());
        let url = start_server(router).await;
        let rest = RestClient::new(reqwest::Client::new(), &url, "fake/repo", "fake-token")
            .with_pacing(Duration::ZERO, Duration::from_millis(100));

        // A single transient 502 during pagination must not abort the whole
        // GC run; the data plane (blob.rs) already retries 5xx with backoff
        // and the control plane deserves the same.
        let listed = rest.list_caches("pack-").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(state.lock().unwrap().requests, 3);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn persistent_server_errors_still_fail() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let state = Arc::new(Mutex::new(FlakyServer {
            failures: usize::MAX,
            requests: 0,
        }));
        let router = Router::new()
            .route(
                "/repos/{owner}/{repo}/actions/caches",
                get(flaky_list_handler),
            )
            .with_state(state.clone());
        let url = start_server(router).await;
        let rest = RestClient::new(reqwest::Client::new(), &url, "fake/repo", "fake-token")
            .with_pacing(Duration::ZERO, Duration::from_millis(10));

        let err = rest.list_caches("pack-").await.unwrap_err();
        assert!(
            err.to_string().contains("502"),
            "error should carry the HTTP status: {err}"
        );
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn delete_retries_after_secondary_rate_limit() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (url, state) = start_rate_limited_server(1).await;
        let rest = RestClient::new(reqwest::Client::new(), &url, "fake/repo", "fake-token")
            .with_pacing(Duration::ZERO, Duration::from_millis(100));

        // GitHub answered the first DELETE with a secondary rate limit
        // error; the client must wait and retry instead of failing GC.
        let deleted = rest.delete_by_key("pack-rate-limited").await.unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(state.lock().unwrap().requests.len(), 2);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn list_retries_after_secondary_rate_limit() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (url, state) = start_rate_limited_server(1).await;
        let rest = RestClient::new(reqwest::Client::new(), &url, "fake/repo", "fake-token")
            .with_pacing(Duration::ZERO, Duration::from_millis(100));

        // Reads hit the same secondary rate limit as writes when GC pages
        // through a large cache; they must retry too.
        let listed = rest.list_caches("pack-").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(state.lock().unwrap().requests.len(), 2);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn rate_limiting_that_never_lifts_is_an_error_not_a_hang() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (url, _state) = start_rate_limited_server(usize::MAX).await;
        let rest = RestClient::new(reqwest::Client::new(), &url, "fake/repo", "fake-token")
            .with_pacing(Duration::ZERO, Duration::from_millis(10));

        let err = rest.delete_by_key("pack-never").await.unwrap_err();
        assert!(
            err.to_string().contains("403"),
            "error should carry the HTTP status: {err}"
        );
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn mutating_requests_are_paced() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (url, state) = start_rate_limited_server(0).await;
        let interval = Duration::from_millis(300);
        let rest = RestClient::new(reqwest::Client::new(), &url, "fake/repo", "fake-token")
            .with_pacing(interval, Duration::from_secs(1));

        for i in 0..3 {
            rest.delete_by_key(&format!("pack-{i}")).await.unwrap();
        }

        // Three deletes, two enforced gaps. Checking the gap between
        // consecutive server-side arrivals (not total elapsed time) so a
        // slow first request cannot mask missing pacing.
        let requests = state.lock().unwrap().requests.clone();
        assert_eq!(requests.len(), 3);
        for pair in requests.windows(2) {
            let gap = pair[1] - pair[0];
            assert!(
                gap >= interval - Duration::from_millis(50),
                "deletes arrived {gap:?} apart, expected at least {interval:?}"
            );
        }
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn pagination_returns_every_entry_despite_concurrent_lru_reordering() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        // 250 entries → 3 pages at the client's page size. Each entry has a
        // distinct creation and access time.
        let entries: Vec<Entry> = (0..250u64)
            .map(|i| Entry {
                key: format!("pack-{i:03}"),
                created_at: 1_000 + i,
                last_accessed_at: 1_000_000 + i,
            })
            .collect();
        let expected: BTreeSet<String> = entries.iter().map(|e| e.key.clone()).collect();

        let url = start_github_like(entries, true).await;
        let rest = RestClient::new(reqwest::Client::new(), &url, "fake/repo", "fake-token");

        let listed = rest.list_caches("pack-").await.unwrap();
        let unique: BTreeSet<String> = listed.iter().map(|e| e.key.clone()).collect();

        // Every entry must be listed exactly once: GC treats packs missing
        // from this listing as evicted and drops the paths that reference
        // them, so a skipped entry means losing live data.
        let missing: Vec<&String> = expected.difference(&unique).collect();
        assert!(
            missing.is_empty(),
            "pagination skipped {} entries: {missing:?}",
            missing.len()
        );
        assert_eq!(
            listed.len(),
            expected.len(),
            "pagination duplicated entries"
        );
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn pagination_does_not_trust_an_understated_total_count() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let router = Router::new().route(
            "/repos/{owner}/{repo}/actions/caches",
            get(understated_total_count_handler),
        );
        let url = start_server(router).await;
        let rest = RestClient::new(reqwest::Client::new(), &url, "fake/repo", "fake-token");

        // GC treats packs missing from this listing as evicted and drops
        // the paths referencing them, so ending the listing early on a
        // stale counter loses live data.
        let listed = rest.list_caches("pack-").await.unwrap();
        assert_eq!(
            listed.len(),
            150,
            "listing must continue past a lying total_count until an empty page"
        );
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn pagination_terminates_when_server_returns_empty_pages() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let router = Router::new().route(
            "/repos/{owner}/{repo}/actions/caches",
            get(empty_pages_handler),
        );
        let url = start_server(router).await;
        let rest = RestClient::new(reqwest::Client::new(), &url, "fake/repo", "fake-token");

        // The inner timeout is the actual assertion: a server that reports
        // total_count > 0 but returns no entries must not be able to make
        // list_caches spin forever (hammering the API in a tight loop).
        let listed = tokio::time::timeout(Duration::from_secs(10), rest.list_caches(""))
            .await
            .expect("list_caches must terminate on inconsistent server data")
            .unwrap();
        assert!(listed.is_empty());
    })
    .await
    .expect("test timed out");
}
