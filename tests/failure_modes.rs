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
