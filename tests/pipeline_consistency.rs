//! Regression tests for write-pipeline correctness under eventually
//! consistent cache lookups (PLAN.md Decision 28).
//!
//! The real GHA cache service can return *non-monotonic* lookup results: a
//! prefix lookup may return manifest version N, and a later lookup (from
//! the same process, seconds apart) may return N-1 again, because reads can
//! be served by different replicas. The pipeline makes its chunk-dedup
//! decisions against one load of the manifest and commits against another:
//! everything the dedup decisions were based on must end up in the
//! committed manifest, no matter what the commit-time load returns.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::extract::State;
use axum::response::IntoResponse;

use hestia::gha::twirp::TwirpClient;
use hestia::pipeline::now_unix;

use support::common::{
    assert_all_chunks_locatable, committed_manifest, path_hash_of,
    pipeline_context_with as context, to_path_set,
};
use support::fake_gha::FakeGha;
use support::store::ScratchStore;

// ---------------------------------------------------------------------------
// Lookup-regression proxy
// ---------------------------------------------------------------------------

/// A pass-through proxy between a [`TwirpClient`] and the fake backend that
/// simulates non-monotonic eventually consistent lookups: the first manifest
/// (`m#`) lookup is forwarded as-is, every later one is answered as if the
/// newest manifest version did not exist (by toggling the fake's
/// stale-lookups injection around the forwarded call).
///
/// This reproduces the real service's behavior when a concurrent writer's
/// fresh commit is visible to one read replica but not to another.
#[derive(Clone)]
struct ProxyState {
    http: reqwest::Client,
    fake_base: String,
    manifest_lookups: Arc<AtomicU64>,
}

async fn proxy_handler(
    State(state): State<ProxyState>,
    request: axum::extract::Request,
) -> axum::response::Response {
    let path = request.uri().path().to_string();
    let body = axum::body::to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("reading proxied request body failed");

    let is_manifest_lookup = path.ends_with("/GetCacheEntryDownloadURL")
        && serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|request| {
                Some(
                    request
                        .get("restore_keys")?
                        .get(0)?
                        .as_str()?
                        .starts_with("m#"),
                )
            })
            .unwrap_or(false);

    // Manifest lookups after the first one see a regressed view.
    let regress = is_manifest_lookup && state.manifest_lookups.fetch_add(1, Ordering::SeqCst) >= 1;

    let toggle = async |on: bool| {
        let url = format!("{}/test/stale-lookups/{}", state.fake_base, u8::from(on));
        let response = state.http.post(url).send().await.expect("toggle request");
        assert!(response.status().is_success());
    };

    if regress {
        toggle(true).await;
    }
    let response = state
        .http
        .post(format!("{}{}", state.fake_base, path))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("forwarding proxied request failed");
    let status = response.status().as_u16();
    let bytes = response.bytes().await.expect("reading proxied response");
    if regress {
        toggle(false).await;
    }

    (
        axum::http::StatusCode::from_u16(status).expect("valid status"),
        bytes,
    )
        .into_response()
}

struct LookupRegressionProxy {
    base_url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for LookupRegressionProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl LookupRegressionProxy {
    async fn start(fake: &FakeGha, http: &reqwest::Client) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding proxy listener failed");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let state = ProxyState {
            http: http.clone(),
            fake_base: fake.base_url.clone(),
            manifest_lookups: Arc::new(AtomicU64::new(0)),
        };
        let router = axum::Router::new()
            .fallback(axum::routing::any(proxy_handler))
            .with_state(state);
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { base_url, task }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commit_includes_everything_the_dedup_decisions_were_based_on() {
    let test = async {
        let Some(store) = ScratchStore::create() else {
            return;
        };

        // Three paths:
        //  * `solo`              -> committed as m#1
        //  * `shared-original`   -> committed as m#2 (its blob chunks live here)
        //  * `shared-copy`       -> same blob content as shared-original, so
        //                           its blob chunks dedup against m#2
        let solo = store.add_fixture("solo", 211);
        let original = store.add_fixture("shared-original", 223);
        let copy = store.add_fixture("shared-copy", 223);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();

        // Set up two manifest versions directly against the fake (these
        // represent commits by other, concurrent CI jobs).
        let direct = context(fake.twirp(&http), &http, store.database());
        let stats = direct
            .run(to_path_set(&[&solo]), BTreeSet::new(), now_unix())
            .await
            .expect("first setup run failed");
        assert_eq!(stats.manifest_version, 1);
        let stats = direct
            .run(to_path_set(&[&original]), BTreeSet::new(), now_unix())
            .await
            .expect("second setup run failed");
        assert_eq!(stats.manifest_version, 2);

        // Push `copy` through a pipeline whose manifest lookups regress
        // after the first one: the dedup load sees m#2 (and skips the
        // shared blob chunks), but the commit-time load only returns m#1.
        let proxy = LookupRegressionProxy::start(&fake, &http).await;
        let lagging = context(
            TwirpClient::new(http.clone(), &proxy.base_url, "fake-runtime-token"),
            &http,
            store.database(),
        );
        let stats = lagging
            .run(to_path_set(&[&copy]), BTreeSet::new(), now_unix())
            .await
            .expect("pipeline run with regressed lookups failed");
        assert_eq!(stats.pushed, 1);

        // Whatever version the commit landed on, it must contain everything
        // the dedup decisions were based on.
        let (_, manifest) = committed_manifest(&fake, &http)
            .await
            .expect("a manifest must be committed");

        // 1. No dangling chunk references: `copy` was committed referencing
        //    chunks it dedup-skipped against m#2, so m#2's chunk locations
        //    must be part of the commit.
        assert_all_chunks_locatable(&manifest);

        // 2. No concurrent writer's paths are dropped: m#2's paths were
        //    visible to this run, so the commit must keep them.
        for path in [&solo, &original, &copy] {
            assert!(
                manifest.paths.contains_key(&path_hash_of(path)),
                "path {} was dropped by the commit",
                path.display()
            );
        }
    };
    // Generous timeout: with the bug present, the run first spins in the
    // SaveMutable conflict loop (~60s with production retry settings)
    // before committing the broken manifest.
    tokio::time::timeout(Duration::from_secs(150), test)
        .await
        .expect("test timed out: deadlock or hung pipeline");
}
