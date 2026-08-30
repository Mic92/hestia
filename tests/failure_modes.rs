//! Failure-mode tests: what happens when the GHA cache misbehaves.
//!
//! Production failure modes simulated against the fake backend
//! (`tests/support/fake_gha.rs`):
//!
//! * Segment eviction or corruption: the daemon and the pipeline skip it
//!   instead of failing, its paths miss and get pushed again.
//! * Token expiry mid-upload: clear error, nothing published.
//! * Quota exhaustion: graceful pipeline failure; already-uploaded packs
//!   are cleaned up by the next GC run (orphan sweep).
//! * Concurrent serve daemons (matrix jobs): both heads count, no data lost.

mod support;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use hestia::gc::{Gc, GcPolicy};
use hestia::pipeline::{AccessLog, PipelineContext};
use hestia::store::meta_key;

use support::common::{
    assert_all_chunks_locatable, load_snapshot, path_hash_of, pipeline_context, to_path_set,
};
use support::fake_gha::FakeGha;
use support::store::ScratchStore;

fn context(fake: &FakeGha, http: &reqwest::Client, store: &ScratchStore) -> PipelineContext {
    pipeline_context(fake, http, store.database())
}

// ---------------------------------------------------------------------------
// Token expiry
// ---------------------------------------------------------------------------

#[tokio::test]
async fn token_expiry_mid_upload_fails_cleanly_without_publishing() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("token-expiry", 233);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, &store);

    // Twirp call budget: ① CreateCacheEntry (pack reserve). The second
    // call, the pack's FinalizeCacheEntryUpload, hits the expired token:
    // failure lands mid-upload, after the blob PUT already went through.
    fake.expire_token_after(&http, 1).await;

    let error = ctx
        .run(to_path_set(&[&fixture]), BTreeSet::new())
        .await
        .expect_err("pipeline must fail when the token expires mid-upload");

    // The error tells the workflow author what happened and what to do.
    let message = error.to_string();
    assert!(
        message.contains("token") && message.contains("expired"),
        "error must explain the token expiry, got: {message}"
    );
    assert!(
        message.contains("re-run"),
        "error must tell the user to re-run the job, got: {message}"
    );

    // Nothing published: a later job (fresh token) sees an empty root.
    fake.expire_token_after(&http, u64::MAX).await;
    assert_eq!(load_snapshot(&fake, &http).await.path_count(), 0);
}

// ---------------------------------------------------------------------------
// Quota exhaustion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn quota_exhaustion_fails_gracefully_and_gc_cleans_orphaned_packs() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("quota", 239);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, &store);

    // Reservation budget: ① the pack's CreateCacheEntry succeeds (the pack
    // uploads fine), ② its index hits the quota error. This is the worst
    // case: data uploaded, nothing referencing it.
    fake.exhaust_quota_after(&http, 1).await;

    let error = ctx
        .run(to_path_set(&[&fixture]), BTreeSet::new())
        .await
        .expect_err("pipeline must fail when the quota is exhausted");
    assert!(
        error.to_string().contains("resource_exhausted"),
        "error must surface the quota problem, got: {error}"
    );

    // Nothing published, but the pack blob is now an orphan in the cache.
    assert_eq!(load_snapshot(&fake, &http).await.path_count(), 0);
    let packs = fake.rest(&http).list_caches("pack-").await.unwrap();
    assert_eq!(packs.len(), 1, "the uploaded pack is orphaned");

    // The orphan is not stuck forever: the next GC run (with quota pressure
    // gone) deletes it once it is older than the safety age. The fake's
    // clock is in small tick units; pretend an hour+ passed since upload.
    fake.exhaust_quota_after(&http, u64::MAX).await;
    let gc = Gc {
        backend: fake.backend(&http),
        policy: GcPolicy::default(),
        dry_run: false,
    };
    let pack_created = packs[0].created_unix().unwrap_or(0);
    let gc_now = pack_created + 2 * 3600;

    let report = gc.run(gc_now).await.expect("GC must succeed");
    assert_eq!(report.deleted, 1, "GC must delete the orphaned pack");

    let packs = fake.rest(&http).list_caches("pack-").await.unwrap();
    assert!(packs.is_empty(), "orphaned pack must be gone after GC");
}

// ---------------------------------------------------------------------------
// Segment loss
// ---------------------------------------------------------------------------

#[tokio::test]
async fn evicted_segment_is_skipped_not_fatal() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let old = store.add_fixture("seg-evicted-old", 223);
    let new = store.add_fixture("seg-evicted-new", 227);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, &store);
    ctx.run(to_path_set(&[&old]), BTreeSet::new())
        .await
        .expect("first drain");

    let snapshot = load_snapshot(&fake, &http).await;
    let digest = snapshot.view.roots.values().next().unwrap()[0];
    fake.rest(&http)
        .delete_by_key(&meta_key(&digest))
        .await
        .unwrap();

    // Loading skips the lost segment instead of failing...
    let snapshot = load_snapshot(&fake, &http).await;
    assert_eq!(snapshot.path_count(), 0);

    // ...and a drain on top of it publishes normally.
    let stats = ctx
        .run(to_path_set(&[&new]), BTreeSet::new())
        .await
        .expect("drain must survive a lost segment");
    assert_eq!(stats.pushed, 1);
    let snapshot = load_snapshot(&fake, &http).await;
    assert!(snapshot.contains(&path_hash_of(&new)));
    assert!(!snapshot.contains(&path_hash_of(&old)));
}

// ---------------------------------------------------------------------------
// Concurrent serve daemons (matrix jobs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_serve_daemons_lose_no_paths() {
    // Matrix builds: two jobs run two independent hestia daemons against
    // the same repository cache and drain at the same time. Both heads
    // count. Neither job's paths or packs may be lost.
    let test = async {
        let Some(store_a) = ScratchStore::create() else {
            return;
        };
        let Some(store_b) = ScratchStore::create() else {
            return;
        };
        let path_a = store_a.add_fixture("matrix-a", 241);
        let path_b = store_b.add_fixture("matrix-b", 251);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();

        let start_daemon = |store: &ScratchStore, label: &str| {
            let socket: PathBuf = store
                .db_path()
                .parent()
                .unwrap()
                .join(format!("hook-{label}.sock"));
            let ctx = context(&fake, &http, store);
            let daemon = hestia::serve::Daemon::bind(
                &socket,
                None,
                ctx,
                AccessLog::new(),
                hestia::substituter::ManifestStore::new(),
            )
            .expect("daemon must bind");
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let handle = tokio::spawn(daemon.run(async {
                let _ = shutdown_rx.await;
            }));
            (socket, shutdown_tx, handle)
        };

        let (socket_a, shutdown_a, handle_a) = start_daemon(&store_a, "a");
        let (socket_b, shutdown_b, handle_b) = start_daemon(&store_b, "b");

        // Each job's post-build-hook registers its own path...
        for (socket, path) in [(&socket_a, &path_a), (&socket_b, &path_b)] {
            hestia::protocol::roundtrip(
                socket,
                &hestia::protocol::Request::Add {
                    paths: vec![path.to_string_lossy().into_owned()],
                },
            )
            .await
            .expect("add failed");
        }

        // ...and both post-steps drain at the same time.
        let (response_a, response_b) = tokio::join!(
            hestia::protocol::roundtrip(&socket_a, &hestia::protocol::Request::Drain),
            hestia::protocol::roundtrip(&socket_b, &hestia::protocol::Request::Drain),
        );
        let stats_a = response_a.expect("drain A failed").stats.unwrap();
        let stats_b = response_b.expect("drain B failed").stats.unwrap();
        assert_eq!(stats_a.pushed, 1);
        assert_eq!(stats_b.pushed, 1);
        assert_ne!(
            stats_a.head, stats_b.head,
            "each drain publishes its own head"
        );

        // Shut both daemons down (their final drains are no-ops).
        drop(shutdown_a);
        drop(shutdown_b);
        handle_a
            .await
            .unwrap()
            .expect("daemon A final drain failed");
        handle_b
            .await
            .unwrap()
            .expect("daemon B final drain failed");

        // The root holds both jobs' work.
        let snapshot = load_snapshot(&fake, &http).await;
        assert!(snapshot.contains(&path_hash_of(&path_a)), "path A lost");
        assert!(snapshot.contains(&path_hash_of(&path_b)), "path B lost");
        assert_eq!(snapshot.pack_hashes().len(), 2, "both packs referenced");
        assert_all_chunks_locatable(&snapshot).await;
    };
    tokio::time::timeout(Duration::from_secs(120), test)
        .await
        .expect("test timed out: deadlock or hung server");
}

// ---------------------------------------------------------------------------
// Eventual consistency (read-your-writes)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drained_paths_are_substitutable_despite_lookup_lag() {
    // The real cache service is eventually consistent: right after a
    // drain, lookups may not show what it wrote. Two guarantees under
    // that lag:
    //
    // 1. paths pushed by THIS daemon are substitutable immediately
    //    (read-your-writes: the daemon serves the segment it published
    //    instead of re-loading it from the cache);
    // 2. the second drain (the action's post step) still names the first
    //    one's paths, so GC keeps them.
    //
    // Regression test for the failure the action-test CI job hit: drain
    // succeeded, but the narinfo request that followed got a 404.
    let test = async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("lag", 257);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();

        // Start a daemon + substituter sharing a ManifestStore, exactly
        // like `hestia serve` wires them.
        let manifest_store = hestia::substituter::ManifestStore::new();
        let access_log = AccessLog::new();
        let socket: PathBuf = store.db_path().parent().unwrap().join("hestia-lag.sock");
        let daemon = hestia::serve::Daemon::bind(
            &socket,
            None,
            context(&fake, &http, &store),
            access_log.clone(),
            manifest_store.clone(),
        )
        .expect("daemon must bind");
        let substituter = hestia::substituter::Substituter::new(
            store.database().store_dir().clone(),
            manifest_store.clone(),
            access_log.clone(),
            fake.backend(&http),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, substituter.into_router())
                .await
                .unwrap();
        });
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let daemon_handle = tokio::spawn(daemon.run(async {
            let _ = shutdown_rx.await;
        }));

        // All lookups lag one version behind from here on (the real
        // service's observed behavior right after a commit).
        fake.set_stale_lookups(&http, true).await;

        hestia::protocol::roundtrip(
            &socket,
            &hestia::protocol::Request::Add {
                paths: vec![fixture.to_string_lossy().into_owned()],
            },
        )
        .await
        .expect("add failed");
        let response = hestia::protocol::roundtrip(&socket, &hestia::protocol::Request::Drain)
            .await
            .expect("drain failed");
        let stats = response.stats.expect("drain stats");
        assert_eq!(stats.pushed, 1);
        assert!(stats.head.is_some());

        // Guarantee 1: the just-pushed path is substitutable right away.
        let hash = path_hash_of(&fixture);
        let narinfo = http
            .get(format!("{base_url}/{hash}.narinfo"))
            .send()
            .await
            .expect("narinfo request failed");
        assert_eq!(
            narinfo.status(),
            200,
            "a path pushed by this daemon must be servable immediately \
             (read-your-writes), regardless of lookup propagation lag"
        );

        // Guarantee 2: the shutdown drain (nothing new pushed, one path
        // accessed) publishes a segment that names the path again.
        drop(shutdown_tx);
        let final_stats = daemon_handle.await.unwrap().expect("final drain failed");
        assert_eq!(final_stats.pushed, 0);
        let head = final_stats.head.expect("the post-step drain must publish");
        server.abort();
        fake.set_stale_lookups(&http, false).await;
        let snapshot = load_snapshot(&fake, &http).await;
        assert!(snapshot.view.heads.iter().any(|(h, _)| *h == head));
        assert!(snapshot.contains(&hash));
    };
    tokio::time::timeout(Duration::from_secs(120), test)
        .await
        .expect("test timed out: deadlock or hung server");
}
