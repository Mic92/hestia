//! Regression tests for the Azure blob client (`gha::blob`) against
//! servers that mishandle `Range` requests or fail transiently.
//!
//! `blob::get(url, Some(range))` promises to return exactly the requested
//! bytes. Callers build chunk extraction on that promise:
//!
//! * the GC repack path slices the response at manifest-recorded offsets
//!   (`&data[from..to]`) — a short body panics there;
//! * the substituter extracts chunks at offsets relative to `range.start` —
//!   a full-body 200 response (a server/proxy that ignores `Range`) shifts
//!   every offset and yields garbage.
//!
//! Both failure modes must surface as clean errors from `blob::get`, never
//! as silently wrong data.
//!
//! Azure also fails transiently (503 ServerBusy, dropped connections). The
//! `*_with_refresh` transfer functions every production caller uses must
//! retry those instead of failing the drain or GC run.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use bytes::Bytes;

use hestia::gha::blob;

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

const BLOB: [u8; 1000] = {
    let mut data = [0u8; 1000];
    let mut i = 0;
    while i < data.len() {
        data[i] = (i % 251) as u8;
        i += 1;
    }
    data
};

/// A server that ignores the `Range` header entirely and always answers
/// `200 OK` with the full blob (misconfigured proxies and non-Azure
/// endpoints behave like this).
async fn ignores_range() -> impl IntoResponse {
    (StatusCode::OK, BLOB.to_vec())
}

/// A server that honors `Range` syntactically (206) but returns fewer bytes
/// than requested — what Azure does when the blob is shorter than the
/// manifest says it is (truncated upload, key re-used with different
/// content after eviction).
async fn truncates_range(headers: HeaderMap) -> impl IntoResponse {
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| v.split_once('-'))
        .and_then(|(start, _)| start.parse::<usize>().ok())
        .expect("test server only receives range requests");
    // Pretend the blob ends 25 bytes after the requested start.
    let end = (range + 25).min(BLOB.len());
    (StatusCode::PARTIAL_CONTENT, BLOB[range..end].to_vec())
}

async fn start_server() -> String {
    let router = Router::new()
        .route("/ignores-range", get(ignores_range))
        .route("/truncates-range", get(truncates_range));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}")
}

/// A server that answers 503 ServerBusy a fixed number of times before
/// succeeding, the way Azure behaves under load.
async fn busy_then_ok(
    State(remaining): State<Arc<AtomicUsize>>,
    method: axum::http::Method,
) -> impl IntoResponse {
    if remaining
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
    {
        return (StatusCode::SERVICE_UNAVAILABLE, b"ServerBusy".to_vec());
    }
    if method == axum::http::Method::PUT {
        (StatusCode::CREATED, Vec::new())
    } else {
        (StatusCode::OK, BLOB.to_vec())
    }
}

/// Start a server whose blob endpoint is busy for the first `busy` requests,
/// returning the request counter.
async fn start_busy_server(busy: usize) -> (String, Arc<AtomicUsize>) {
    let remaining = Arc::new(AtomicUsize::new(busy));
    let router = Router::new()
        .route("/blob", get(busy_then_ok).put(busy_then_ok))
        .with_state(remaining.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stub listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{addr}"), remaining)
}

/// Refresh callback that must not be called: 503 is not a URL expiry.
async fn no_refresh() -> Result<String, hestia::gha::Error> {
    panic!("refresh must only be called for 401/403, not for transient errors");
}

#[tokio::test]
async fn upload_retries_transient_server_errors() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (base, _) = start_busy_server(2).await;
        let http = reqwest::Client::new();
        let url = format!("{base}/blob");

        blob::put_with_refresh(&http, &url, Bytes::from_static(b"data"), no_refresh)
            .await
            .expect("two 503s then success must not fail the upload");
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn download_retries_transient_server_errors() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (base, _) = start_busy_server(2).await;
        let http = reqwest::Client::new();
        let url = format!("{base}/blob");

        let data = blob::get_with_refresh(&http, &url, None, no_refresh)
            .await
            .expect("two 503s then success must not fail the download");
        assert_eq!(data.as_ref(), &BLOB[..]);
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn permanent_server_errors_fail_after_bounded_retries() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let (base, remaining) = start_busy_server(usize::MAX).await;
        let http = reqwest::Client::new();
        let url = format!("{base}/blob");

        let err = blob::put_with_refresh(&http, &url, Bytes::from_static(b"data"), no_refresh)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("503"),
            "error should carry the HTTP status: {err}"
        );
        // Bounded: a permanently busy server must not be hammered forever.
        let requests = usize::MAX - remaining.load(Ordering::SeqCst);
        assert!(
            (2..=8).contains(&requests),
            "expected a small bounded number of attempts, got {requests}"
        );
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn full_body_response_to_range_request_is_an_error() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let base = start_server().await;
        let http = reqwest::Client::new();
        let url = format!("{base}/ignores-range");

        // Without a range the full body is exactly what we asked for.
        let full = blob::get(&http, &url, None).await.unwrap();
        assert_eq!(full.as_ref(), &BLOB[..]);

        // With a range, a 200 full-body response is NOT the requested
        // bytes 100..200; returning it as if it were corrupts every
        // offset the caller computes relative to range.start.
        let result = blob::get(&http, &url, Some(100..200)).await;
        match result {
            Err(_) => {}
            Ok(data) => panic!(
                "range request must not silently accept a full-body response \
                 (got {} bytes instead of an error)",
                data.len()
            ),
        }
    })
    .await
    .expect("test timed out");
}

#[tokio::test]
async fn truncated_range_response_is_an_error() {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let base = start_server().await;
        let http = reqwest::Client::new();
        let url = format!("{base}/truncates-range");

        // Ask for 100 bytes; the server only has 25 left at that offset.
        let result = blob::get(&http, &url, Some(900..1000)).await;
        match result {
            Err(_) => {}
            Ok(data) => panic!(
                "range request must not return fewer bytes than requested \
                 (got {} bytes instead of an error); callers slice this \
                 buffer at fixed offsets and would panic or read garbage",
                data.len()
            ),
        }
    })
    .await
    .expect("test timed out");
}
