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
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;

use hestia::gc::{GcContext, GcPolicy};
use hestia::gha::blob;
use hestia::gha::savemutable::SaveMutable;
use hestia::gha::twirp::{Reservation, TwirpClient};
use hestia::manifest::{Manifest, PathHash};
use hestia::pipeline::{AccessLog, MANIFEST_PREFIX, PipelineContext, now_unix};
use hestia::upstream::UpstreamFilter;

use support::fake_gha::FakeGha;
use support::store::ScratchStore;

const TEST_ROOT_KEY: &str = "main-test-system";

fn context(fake: &FakeGha, http: &reqwest::Client, store: &ScratchStore) -> PipelineContext {
    PipelineContext {
        twirp: fake.twirp(http),
        http: http.clone(),
        store: store.database(),
        upstream: UpstreamFilter::default(),
        root_key: TEST_ROOT_KEY.to_string(),
        manifest_prefix: MANIFEST_PREFIX.to_string(),
    }
}

/// Reserve + upload + finalize one cache entry directly (bypassing hestia's
/// pipeline), e.g. to plant a corrupt manifest blob.
async fn store_entry(twirp: &TwirpClient, http: &reqwest::Client, key: &str, data: &[u8]) {
    let Reservation::Created { upload_url } = twirp.create_cache_entry(key).await.unwrap() else {
        panic!("entry {key} unexpectedly already exists");
    };
    blob::put(http, &upload_url, Bytes::copy_from_slice(data))
        .await
        .unwrap();
    twirp.finalize_upload(key, data.len() as u64).await.unwrap();
}

/// Load the committed manifest from the fake backend, or None.
async fn committed_manifest(fake: &FakeGha, http: &reqwest::Client) -> Option<(u64, Manifest)> {
    let twirp = fake.twirp(http);
    let save = SaveMutable::new(&twirp, http, MANIFEST_PREFIX);
    let entry = save.load().await.expect("loading manifest failed")?;
    Some((
        entry.index,
        Manifest::decode(&entry.data).expect("manifest must decode"),
    ))
}

fn path_hash_of(store_path: &Path) -> PathHash {
    let name = store_path.file_name().unwrap().to_str().unwrap();
    name[..32].parse().unwrap()
}

fn to_path_set(paths: &[&Path]) -> BTreeSet<String> {
    paths
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
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
    store_entry(&twirp, &http, "pack-data", b"some pack contents").await;

    let gc = GcContext {
        twirp: fake.twirp(&http),
        rest: fake.rest(&http),
        http: http.clone(),
        manifest_prefix: MANIFEST_PREFIX.to_string(),
        policy: GcPolicy::default(),
    };

    let result = gc.run(false, now_unix()).await;
    assert!(result.is_err(), "GC must fail on a corrupt manifest");

    // Nothing was deleted.
    let entries = fake.rest(&http).list_caches("").await.unwrap();
    let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
    assert!(keys.contains(&"m#1"), "corrupt manifest left in place");
    assert!(keys.contains(&"pack-data"), "packs left in place");
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
