//! Serving from the segmented store through the fake GHA backend, with
//! nix as the oracle.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use hestia::manifest::Hash32;
use hestia::pipeline::AccessLog;
use hestia::protocol::DrainStats;
use hestia::store::Snapshot;
use hestia::substituter::{ManifestStore, Substituter};

use support::common::{TEST_ROOT_KEY, nar_body, pipeline_context, to_path_set};
use support::fake_gha::FakeGha;
use support::store::{ScratchStore, assert_trees_equal, nix_copy};

async fn timed<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(120), f)
        .await
        .expect("test timed out")
}

async fn push(
    fake: &FakeGha,
    http: &reqwest::Client,
    store: &ScratchStore,
    paths: &[&std::path::Path],
) -> DrainStats {
    let ctx = pipeline_context(fake, http, store.database());
    ctx.run(to_path_set(paths), BTreeSet::new())
        .await
        .expect("pipeline run")
}

/// Serves what the drains published.
async fn serve(
    fake: &FakeGha,
    http: &reqwest::Client,
    store: &ScratchStore,
) -> (String, AccessLog, tokio::task::JoinHandle<()>) {
    let backend = fake.backend(http);
    let snapshot = Snapshot::load(
        backend.clone(),
        hestia::trust::Trust::open(),
        &[TEST_ROOT_KEY.to_string()],
        None,
    )
    .await
    .unwrap();
    let manifest_store = ManifestStore::new();
    manifest_store.set_snapshot(Arc::new(snapshot));
    let access_log = AccessLog::new();
    let router = Substituter::new(
        store.database().store_dir().clone(),
        manifest_store,
        access_log.clone(),
        backend,
    )
    .into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{addr}"), access_log, task)
}

#[tokio::test]
async fn narinfo_and_nar_from_segments() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("segserve", 91);
        let (expected_hash, expected_size) = store.nar_hash_oracle(&fixture).expect("nix oracle");

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push(&fake, &http, &store, &[&fixture]).await;
        let (base, access_log, _task) = serve(&fake, &http, &store).await;

        let hash = &fixture.file_name().unwrap().to_str().unwrap()[..32];
        let narinfo = http
            .get(format!("{base}/{hash}.narinfo"))
            .send()
            .await
            .unwrap();
        assert_eq!(narinfo.status(), 200);
        let text = narinfo.text().await.unwrap();
        let url = text.lines().find_map(|l| l.strip_prefix("URL: ")).unwrap();
        assert!(
            text.contains(&format!("NarSize: {expected_size}")),
            "{text}"
        );

        let nar = http.get(format!("{base}/{url}")).send().await.unwrap();
        assert_eq!(nar.status(), 200);
        let body = nar_body(nar).await;
        assert_eq!(body.len() as u64, expected_size);
        assert_eq!(Hash32::digest(&body), expected_hash);
        assert!(access_log.snapshot().contains(&hash.parse().unwrap()));

        let miss = http
            .get(format!("{base}/00000000000000000000000000000000.narinfo"))
            .send()
            .await
            .unwrap();
        assert_eq!(miss.status(), 404);
    })
    .await;
}

#[tokio::test]
async fn nix_copy_closure_from_segments() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let (top, dep) = store.add_paths_with_reference("segcopy");

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push(&fake, &http, &store, &[&top, &dep]).await;
        let (base, _log, _task) = serve(&fake, &http, &store).await;
        let store_url = format!("{base}?store={}", store.store_dir_path().display());

        let destination = store.create_destination();
        let output = nix_copy(&store_url, &destination.uri, &top).await;
        assert!(
            output.status.success(),
            "nix copy failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_trees_equal(&top, &destination.physical_path(&top));
        assert_trees_equal(&dep, &destination.physical_path(&dep));
    })
    .await;
}

#[tokio::test]
async fn unserved_root_is_invisible() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("segroot", 92);
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push(&fake, &http, &store, &[&fixture]).await;

        let snapshot = Snapshot::load(
            fake.backend(&http),
            hestia::trust::Trust::open(),
            &["other-root".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(snapshot.path_count(), 0);
        let snapshot = Snapshot::load(
            fake.backend(&http),
            hestia::trust::Trust::open(),
            &[TEST_ROOT_KEY.to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(snapshot.path_count(), 1);
        assert!(snapshot.view.roots.contains_key(TEST_ROOT_KEY));
    })
    .await;
}

/// The second drain sees the first one's paths and chunks only through
/// segments (its legacy manifest family is empty) and must not store
/// them again.
#[tokio::test]
async fn drain_dedups_against_segments() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let (top, dep) = store.add_paths_with_reference("segdedup");
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let first = push(&fake, &http, &store, &[&dep]).await;
        assert_eq!(first.pushed, 1);

        let mut ctx = pipeline_context(&fake, &http, store.database());
        let publish = ManifestStore::new();
        let snapshot = Snapshot::load(
            fake.backend(&http),
            hestia::trust::Trust::open(),
            &[TEST_ROOT_KEY.to_string()],
            None,
        )
        .await
        .unwrap();
        publish.set_snapshot(Arc::new(snapshot));
        ctx.publish = Some(publish.clone());
        let second = ctx
            .run(to_path_set(&[&top, &dep]), BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(second.pushed, 1);
        assert_eq!(second.skipped_existing, 1);

        // Read-your-writes: the drain published its segment into `publish`.
        let served = publish.snapshot().unwrap();
        for p in [&top, &dep] {
            assert!(served.contains(&support::common::path_hash_of(p)));
        }
    })
    .await;
}
