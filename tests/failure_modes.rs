//! Failure-mode tests: what happens when the GHA cache misbehaves.
//!
//! Production failure modes simulated against the fake backend
//! (`tests/support/fake_gha.rs`):
//!
//! * Manifest corruption (truncated upload, garbage blob): the daemon and
//!   the pipeline must start from an empty manifest instead of failing —
//!   a corrupt manifest means cache misses and rebuilds, never broken CI.
//! * Token expiry mid-upload: clear error, no partial manifest commit.
//! * Quota exhaustion: graceful pipeline failure; already-uploaded packs
//!   are cleaned up by the next GC run (orphan sweep).
//! * Azure connection drops mid-Range-read: transparent retry, and a clean
//!   404 (never corrupt data) when the failure persists.
//! * Concurrent serve daemons (matrix jobs): manifests merge, no data lost.

mod support;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use hestia::chunker::pack_cache_key;
use hestia::gc::{GcContext, GcPolicy};
use hestia::gha::savemutable::SaveMutable;
use hestia::pipeline::{AccessLog, MANIFEST_PREFIX, PipelineContext, now_unix};

use support::common::{
    TEST_ROOT_KEY, committed_manifest, path_hash_of, pipeline_context, store_entry, to_path_set,
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
async fn token_expiry_mid_upload_fails_cleanly_without_partial_commit() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("token-expiry", 233);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, &store);

    // Twirp call budget: ① GetCacheEntryDownloadURL (manifest load),
    // ② CreateCacheEntry (pack reserve). The third call — the pack's
    // FinalizeCacheEntryUpload — hits the expired token: failure lands
    // mid-upload, after the blob PUT already went through.
    fake.expire_token_after(&http, 2).await;

    let error = ctx
        .run(to_path_set(&[&fixture]), BTreeSet::new(), now_unix())
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

    // No partial manifest commit: a later job (fresh token) sees no
    // manifest at all — the failed run left nothing half-finished behind.
    fake.expire_token_after(&http, u64::MAX).await;
    assert!(
        committed_manifest(&fake, &http).await.is_none(),
        "a failed upload must not leave a partial manifest behind"
    );
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
    // uploads fine), ② the manifest's CreateCacheEntry hits the quota error.
    // This is the worst case: data uploaded, nothing referencing it.
    fake.exhaust_quota_after(&http, 1).await;

    let error = ctx
        .run(to_path_set(&[&fixture]), BTreeSet::new(), now_unix())
        .await
        .expect_err("pipeline must fail when the quota is exhausted");
    assert!(
        error.to_string().contains("resource_exhausted"),
        "error must surface the quota problem, got: {error}"
    );

    // No manifest was committed, but the pack blob is now an orphan in the
    // cache.
    assert!(committed_manifest(&fake, &http).await.is_none());
    let packs = fake.rest(&http).list_caches("pack-").await.unwrap();
    assert_eq!(packs.len(), 1, "the uploaded pack is orphaned");

    // The orphan is not stuck forever: the next GC run (with quota pressure
    // gone) deletes it once it is older than the safety age. The fake's
    // clock is in small tick units; pretend an hour+ passed since upload.
    fake.exhaust_quota_after(&http, u64::MAX).await;
    let gc = GcContext {
        twirp: fake.twirp(&http),
        rest: fake.rest(&http),
        http: http.clone(),
        manifest_prefix: MANIFEST_PREFIX.to_string(),
        policy: GcPolicy::default(),
    };
    let pack_created = packs[0].created_unix().unwrap_or(0);
    let gc_now = pack_created + 2 * 3600;

    let report = gc.run(false, gc_now).await.expect("GC must succeed");
    assert_eq!(
        report.orphans_deleted, 1,
        "GC must delete the orphaned pack"
    );

    let packs = fake.rest(&http).list_caches("pack-").await.unwrap();
    assert!(packs.is_empty(), "orphaned pack must be gone after GC");
}

// ---------------------------------------------------------------------------
// Manifest corruption
// ---------------------------------------------------------------------------

#[tokio::test]
async fn garbage_manifest_blob_is_replaced_not_fatal() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("corrupt-garbage", 211);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    // Plant a manifest blob that is not even valid zstd.
    store_entry(&twirp, &http, "m#1", b"this is not a manifest at all").await;

    // Loading must degrade to an empty manifest, not fail.
    let ctx = context(&fake, &http, &store);
    let loaded = ctx.load_manifest().await.expect("load must not fail");
    assert!(loaded.paths.is_empty(), "corrupt manifest reads as empty");

    // A drain over the corrupt manifest must still succeed and commit a
    // fresh, decodable manifest version on top of it.
    let stats = ctx
        .run(to_path_set(&[&fixture]), BTreeSet::new(), now_unix())
        .await
        .expect("pipeline must recover from a corrupt manifest");
    assert_eq!(stats.pushed, 1);
    assert_eq!(
        stats.manifest_version, 2,
        "commits on top of the corrupt m#1"
    );

    let (version, manifest) = committed_manifest(&fake, &http).await.unwrap();
    assert_eq!(version, 2);
    assert!(manifest.paths.contains_key(&path_hash_of(&fixture)));
}

#[tokio::test]
async fn truncated_manifest_blob_is_replaced_not_fatal() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture_old = store.add_fixture("corrupt-truncated-old", 223);
    let fixture_new = store.add_fixture("corrupt-truncated-new", 227);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, &store);

    // Commit a real manifest first...
    let stats = ctx
        .run(to_path_set(&[&fixture_old]), BTreeSet::new(), now_unix())
        .await
        .expect("first pipeline run failed");
    assert_eq!(stats.manifest_version, 1);

    // ...then simulate a truncated upload of the next version: the first
    // half of a valid manifest encoding (cut mid-zstd-frame).
    let twirp = fake.twirp(&http);
    let save = SaveMutable::new(&twirp, &http, MANIFEST_PREFIX);
    let valid = save.load().await.unwrap().unwrap().data;
    let truncated = &valid[..valid.len() / 2];
    store_entry(&twirp, &http, "m#2", truncated).await;

    // The truncated newest version reads as empty (the older intact m#1 is
    // NOT consulted: SaveMutable always serves the newest version)...
    let loaded = ctx.load_manifest().await.expect("load must not fail");
    assert!(loaded.paths.is_empty());

    // ...and the next drain commits a valid m#3 containing the new path.
    let stats = ctx
        .run(to_path_set(&[&fixture_new]), BTreeSet::new(), now_unix())
        .await
        .expect("pipeline must recover from a truncated manifest");
    assert_eq!(stats.manifest_version, 3);

    let (version, manifest) = committed_manifest(&fake, &http).await.unwrap();
    assert_eq!(version, 3);
    assert!(manifest.paths.contains_key(&path_hash_of(&fixture_new)));
    // The path from the corrupt era is gone from the manifest (it will be
    // rebuilt and re-pushed next run); its pack lingers until GC's orphan
    // sweep removes it.
    assert!(!manifest.paths.contains_key(&path_hash_of(&fixture_old)));
}

#[tokio::test]
async fn gc_refuses_to_act_on_a_corrupt_manifest() {
    // GC is the only destructive consumer of the manifest: acting on a
    // corrupt (= unreadable = effectively empty) manifest would judge every
    // pack an orphan and delete real data. GC must fail loudly instead and
    // leave the cache untouched.
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    store_entry(&twirp, &http, "m#1", b"garbage manifest").await;
    // A hestia-shaped pack key, old enough to pass the min_age guard: if GC
    // misjudged the corrupt manifest as empty and ran its orphan sweep, this
    // is exactly what it would delete.
    let pack_key = pack_cache_key(&hestia::manifest::PackHash::digest(b"some pack contents"));
    store_entry(&twirp, &http, &pack_key, b"some pack contents").await;

    let gc = GcContext {
        twirp: fake.twirp(&http),
        rest: fake.rest(&http),
        http: http.clone(),
        manifest_prefix: MANIFEST_PREFIX.to_string(),
        policy: GcPolicy::default(),
    };

    // Run well past min_age so the planted pack is not shielded by the
    // too-young-to-delete guard.
    let gc_now = now_unix() + GcPolicy::default().min_age + 1;
    let result = gc.run(false, gc_now).await;
    assert!(result.is_err(), "GC must fail on a corrupt manifest");

    // Nothing was deleted.
    let entries = fake.rest(&http).list_caches("").await.unwrap();
    let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
    assert!(keys.contains(&"m#1"), "corrupt manifest left in place");
    assert!(keys.contains(&pack_key.as_str()), "packs left in place");
}

#[tokio::test]
async fn daemon_starts_and_drains_over_a_corrupt_manifest() {
    // The serve-level guarantee: a corrupt manifest must not prevent the
    // daemon from starting, serving (cache misses), or draining.
    let test = async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("corrupt-daemon", 229);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let twirp = fake.twirp(&http);
        store_entry(&twirp, &http, "m#1", b"garbage manifest").await;

        let ctx = context(&fake, &http, &store);

        // Startup: load the manifest exactly like serve::run does.
        let manifest_store = hestia::substituter::ManifestStore::new();
        manifest_store.set(ctx.load_manifest().await.expect("load must not fail"));
        assert_eq!(manifest_store.path_count(), 0, "daemon starts empty");

        // The daemon runs and a hook + drain cycle works.
        let socket: PathBuf = store.db_path().parent().unwrap().join("hestia-hook.sock");
        let daemon = hestia::serve::Daemon::bind(
            &socket,
            None,
            ctx,
            AccessLog::new(),
            manifest_store.clone(),
        )
        .expect("daemon must bind despite the corrupt manifest");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(daemon.run(async {
            let _ = shutdown_rx.await;
        }));

        hestia::protocol::roundtrip(
            &socket,
            &hestia::protocol::Request::Add {
                paths: vec![fixture.to_string_lossy().into_owned()],
            },
        )
        .await
        .expect("add failed");

        let response =
            hestia::protocol::roundtrip(&socket, &hestia::protocol::Request::Drain).await;
        let stats = response.expect("drain must succeed").stats.unwrap();
        assert_eq!(stats.pushed, 1);
        assert_eq!(stats.manifest_version, 2);

        drop(shutdown_tx);
        handle.await.unwrap().expect("final drain failed");

        let (_, manifest) = committed_manifest(&fake, &http).await.unwrap();
        assert!(manifest.paths.contains_key(&path_hash_of(&fixture)));
    };
    tokio::time::timeout(Duration::from_secs(120), test)
        .await
        .expect("test timed out: deadlock or hung server");
}

// ---------------------------------------------------------------------------
// Concurrent serve daemons (matrix jobs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_serve_daemons_merge_without_losing_paths() {
    // Matrix builds: two jobs run two independent hestia daemons against
    // the same repository cache and drain at the same time. SaveMutable
    // conflict handling must merge both manifests; neither job's paths,
    // packs, or root pins may be lost.
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

        // One daemon won version 1, the other re-merged onto version 2.
        let mut versions = [stats_a.manifest_version, stats_b.manifest_version];
        versions.sort();
        assert_eq!(versions, [1, 2], "drains must land on distinct versions");

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

        // The final manifest holds both jobs' work.
        let (version, manifest) = committed_manifest(&fake, &http).await.unwrap();
        assert!(version >= 2);
        let hash_a = path_hash_of(&path_a);
        let hash_b = path_hash_of(&path_b);
        assert!(manifest.paths.contains_key(&hash_a), "path A lost in merge");
        assert!(manifest.paths.contains_key(&hash_b), "path B lost in merge");
        assert_eq!(manifest.packs.len(), 2, "both packs referenced");

        // Every chunk of both paths is locatable in a known pack.
        for entry in manifest.paths.values() {
            for (_, node) in hestia::chunker::flatten_tree(&entry.tree) {
                if let hestia::manifest::FileSystemObject::Regular(regular) = node {
                    for chunk in &regular.contents.chunks {
                        let location = manifest.chunks.get(chunk).expect("chunk has a location");
                        assert!(manifest.packs.contains_key(&location.pack));
                    }
                }
            }
        }

        // The shared root pins both paths (concurrent updates union).
        let root = &manifest.roots[TEST_ROOT_KEY];
        assert!(root.paths.contains(&hash_a) && root.paths.contains(&hash_b));

        // Both paths remain substitutable from the merged manifest.
        let manifest_store = hestia::substituter::ManifestStore::new();
        manifest_store.set(manifest);
        assert_eq!(manifest_store.path_count(), 2);
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
    // drain commits manifest m#N, lookups may still
    // return m#N-1 (or nothing). Three guarantees under that lag:
    //
    // 1. paths pushed by THIS daemon are substitutable immediately
    //    (read-your-writes: the daemon publishes the manifest it
    //    committed instead of re-loading it from the cache);
    // 2. a second drain (the action's post step) commits the next version
    //    promptly instead of fighting its own previous commit in the
    //    SaveMutable conflict loop;
    // 3. the second commit still contains the first one's paths (the
    //    daemon's own manifest is part of every merge base).
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
            fake.twirp(&http),
            http.clone(),
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

        // Hook + drain the path through the daemon (commits m#1; lookups
        // now pretend m#1 does not exist yet).
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
        assert_eq!(stats.manifest_version, 1);

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

        // Guarantee 2: the shutdown drain commits m#2 promptly. Without
        // the reservation floor it would spin in the conflict loop against
        // its own m#1 until the stale-skip window (60s in production)
        // expired — the whole test would blow its timeout. The narinfo
        // hit alone would be a pure root-clock refresh (skipped, no
        // commit), so record a new accessed hash to give the shutdown
        // drain a real root delta.
        let extra_accessed: hestia::manifest::PathHash =
            "86yk8b7ny30zl1wsq2vd66j9vrcgrkah".parse().unwrap();
        access_log.record(extra_accessed);
        drop(shutdown_tx);
        let final_stats = daemon_handle.await.unwrap().expect("final drain failed");
        assert_eq!(
            final_stats.manifest_version, 2,
            "the post-step drain must commit the next version"
        );

        // Guarantee 3: m#2 still contains the path pushed in m#1 — the
        // stale merge base was healed with the daemon's own manifest.
        server.abort();
        fake.set_stale_lookups(&http, false).await;
        let (version, manifest) = committed_manifest(&fake, &http).await.unwrap();
        assert_eq!(version, 2);
        assert!(
            manifest.paths.contains_key(&hash),
            "the second commit must not lose the first commit's paths"
        );
        assert!(manifest.roots[TEST_ROOT_KEY].paths.contains(&hash));
        assert!(
            manifest.roots[TEST_ROOT_KEY]
                .paths
                .contains(&extra_accessed)
        );
    };
    tokio::time::timeout(Duration::from_secs(120), test)
        .await
        .expect("test timed out: deadlock or hung server");
}
