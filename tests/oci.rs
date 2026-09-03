//! The OCI backend against the fake registry: the key/blob/tag mapping,
//! auth, and a full drain + substitution round trip.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hestia::gc::{Gc, GcPolicy};
use hestia::manifest::Hash32;
use hestia::pipeline::AccessLog;
use hestia::store::Snapshot;
use hestia::substituter::{ManifestStore, Substituter};
use support::common::{TEST_ROOT_KEY, pipeline_context_with, to_path_set};
use support::fake_oci::FakeOci;
use support::sim::{SimCache, SimPath};
use support::store::{ScratchStore, assert_trees_equal, nix_copy};

async fn timed<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(120), f)
        .await
        .expect("test timed out")
}

fn key(kind: &str, data: &[u8]) -> String {
    format!("{kind}-{}", Hash32::digest(data))
}

#[tokio::test]
async fn content_keys_are_blobs_and_heads_are_tags() {
    timed(async {
        let fake = FakeOci::start().await;
        let http = reqwest::Client::new();
        let b = fake.backend(&http);

        let pack = Bytes::from(vec![7u8; 1000]);
        let pack_key = key("pack", &pack);
        assert!(b.put(&pack_key, pack.clone()).await.unwrap());
        assert!(
            !b.put(&pack_key, pack.clone()).await.unwrap(),
            "second put dedups"
        );
        assert_eq!(b.get(&pack_key, None).await.unwrap().unwrap(), pack);
        assert_eq!(
            b.get(&pack_key, Some(10..20)).await.unwrap().unwrap(),
            pack.slice(10..20)
        );
        assert!(b.touch(&pack_key).await.unwrap());
        assert_eq!(b.get(&key("pack", b"nope"), None).await.unwrap(), None);

        assert_eq!(b.list("h-", None).await.unwrap().unwrap(), vec![]);
        let record = Bytes::from_static(b"record body");
        b.put(
            "c-0000000000000000-00000000075bcd15-0000000000000001-x",
            record.clone(),
        )
        .await
        .unwrap();
        b.put(
            "h-0000000000000000-00000000075bcd15-0000000000000001-y",
            Bytes::new(),
        )
        .await
        .unwrap();
        assert_eq!(
            b.get(
                "c-0000000000000000-00000000075bcd15-0000000000000001-x",
                None
            )
            .await
            .unwrap()
            .unwrap(),
            record
        );
        assert_eq!(
            b.get(
                "h-0000000000000000-00000000075bcd15-0000000000000001-y",
                None
            )
            .await
            .unwrap()
            .unwrap(),
            Bytes::new()
        );
        let listed: Vec<String> = b
            .list("c-", None)
            .await
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|l| l.key)
            .collect();
        assert_eq!(
            listed,
            ["c-0000000000000000-00000000075bcd15-0000000000000001-x"]
        );
        assert_eq!(
            b.list("pack-", None).await.unwrap(),
            None,
            "blobs cannot be listed"
        );
        assert_eq!(fake.tags().len(), 2, "content manifests stay untagged");

        // Every blob read went to the CDN without the bearer token and
        // partial reads were Range requests.
        let gets = fake.blob_gets();
        assert!(
            gets.iter()
                .any(|(_, r)| r.as_deref() == Some("bytes=10-19"))
        );
    })
    .await;
}

#[tokio::test]
async fn listing_pages_and_lags() {
    timed(async {
        let fake = FakeOci::start().await;
        let http = reqwest::Client::new();
        let b = fake.backend(&http);
        for i in 0..2100 {
            b.put(
                &format!("h-0000000000000000-00000000075bcd15-0000000000000001-{i:04}"),
                Bytes::new(),
            )
            .await
            .unwrap();
        }
        assert_eq!(b.list("h-", None).await.unwrap().unwrap().len(), 2100);
        assert_eq!(b.list("h-", Some(10)).await.unwrap(), None);

        fake.set_tag_lag(5);
        b.put(
            "g-0000000000000001-0000000000000001-z",
            Bytes::from_static(b"r"),
        )
        .await
        .unwrap();
        assert!(b.list("g-", None).await.unwrap().unwrap().is_empty());
        assert!(
            b.get("g-0000000000000001-0000000000000001-z", None)
                .await
                .unwrap()
                .is_some(),
            "readable by name before it is listed"
        );
        for _ in 0..5 {
            b.touch("g-0000000000000001-0000000000000001-z")
                .await
                .unwrap();
        }
        assert_eq!(b.list("g-", None).await.unwrap().unwrap().len(), 1);
    })
    .await;
}

#[tokio::test]
async fn pull_only_credentials_read_but_cannot_write() {
    timed(async {
        let fake = FakeOci::start().await;
        let http = reqwest::Client::new();
        let rw = fake.backend(&http);
        let data = Bytes::from_static(b"blob");
        rw.put(&key("seg", &data), data.clone()).await.unwrap();

        let ro = fake.anonymous(&http);
        assert!(!ro.probe_writable().await.unwrap());
        assert!(rw.probe_writable().await.unwrap());
        assert_eq!(
            ro.get(&key("seg", &data), None).await.unwrap().unwrap(),
            data
        );
        let err = ro
            .put(&key("tree", b"x"), Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("401"), "{err}");

        fake.deny_push();
        let fresh = fake.backend(&reqwest::Client::new());
        assert!(!fresh.probe_writable().await.unwrap());
    })
    .await;
}

#[tokio::test]
async fn drain_and_nix_copy_over_oci() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let (top, dep) = store.add_paths_with_reference("ocicopy");
        let fake = FakeOci::start().await;
        let http = reqwest::Client::new();

        let stats = pipeline_context_with(fake.backend(&http), store.database())
            .run(to_path_set(&[&top, &dep]), BTreeSet::new())
            .await
            .expect("pipeline run");
        assert_eq!(stats.pushed, 2);
        assert!(stats.head.as_deref().is_some_and(|h| h.starts_with("c-")));
        // A second drain of the same paths finds them and uploads nothing.
        let again = pipeline_context_with(fake.backend(&http), store.database())
            .run(to_path_set(&[&top, &dep]), BTreeSet::new())
            .await
            .unwrap();
        assert_eq!((again.pushed, again.packs_uploaded), (0, 0));

        // Substitution needs no credentials.
        let backend = fake.anonymous(&http);
        let snapshot = Snapshot::load(
            backend.clone(),
            hestia::trust::Trust::open(),
            &[TEST_ROOT_KEY.to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(snapshot.path_count(), 2);
        let manifest_store = ManifestStore::new();
        manifest_store.set_snapshot(Arc::new(snapshot));
        let router = Substituter::new(
            store.database().store_dir().clone(),
            manifest_store,
            AccessLog::new(),
            backend,
        )
        .into_router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let _server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
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

const T0: u64 = 1_750_000_000;
const HOUR: u64 = 3600;

/// Two roots drained over OCI, one abandoned. GC over the GHCR packages
/// API folds heads, and the run after sweeps the retired segments and the
/// expired root's packs plus an orphan by version id.
#[tokio::test]
async fn gc_over_ghcr_deletes_by_version_id() {
    timed(async {
        let fake = FakeOci::start().await;
        let http = reqwest::Client::new();
        fake.set_clock(T0);
        let sim = SimCache::with(fake.backend(&http), fake.clock());
        let a = SimPath::new("a", 1, 200_000);
        let b = SimPath::new("b", 3, 200_000);
        let gone = SimPath::new("gone", 5, 200_000);
        sim.push("main", &[&a], &[&a]).await;
        sim.push("main", &[&b], &[&a, &b]).await;
        sim.push("old", &[&gone], &[&gone]).await;
        sim.upload_orphan_pack(9).await;
        assert_eq!(fake.versions("pack-"), 4);
        assert_eq!(fake.versions("seg-"), 3);

        let policy = GcPolicy {
            root_ttl: 24 * HOUR,
            ..GcPolicy::default()
        };
        fake.set_clock(T0 + 2 * HOUR);
        let stats = sim.run_gc(policy.clone(), T0 + 2 * HOUR).await;
        assert_eq!((stats.roots, stats.deleted), (2, 4), "{stats:?}");
        assert_eq!(
            fake.versions("pack-"),
            3,
            "orphan swept via the ledger listing"
        );
        assert!(fake.tags().iter().any(|t| t == "x-ledger"));
        sim.assert_readable(&[&a, &b, &gone]).await;

        // A day on only main is drained: old expires, its pack goes, and
        // the segments the first run merged away are swept.
        fake.set_clock(T0 + 30 * HOUR);
        sim.push("main", &[], &[&a, &b]).await;
        let calls = fake.api_calls();
        let stats = sim.run_gc(policy, T0 + 30 * HOUR).await;
        assert_eq!(stats.roots_expired, 1, "{stats:?}");
        sim.assert_readable(&[&a, &b]).await;
        assert_eq!(fake.versions("pack-"), 3, "old's pack waits one epoch");
        assert_eq!(
            fake.versions("seg-"),
            2,
            "main's merged segment and old's, retired"
        );
        assert!(
            fake.api_calls() - calls < 20,
            "ledger resumes: one versions page plus deletes, got {}",
            fake.api_calls() - calls
        );
        fake.set_clock(T0 + 60 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 60 * HOUR).await;
        assert!(stats.deleted >= 2, "{stats:?}");
        assert_eq!(fake.versions("pack-"), 2);
        assert_eq!(fake.versions("seg-"), 1);
        sim.assert_readable(&[&a, &b]).await;
    })
    .await;
}

/// A plain registry cannot list blobs, so GC probes each referenced pack
/// to notice what a registry-side GC or retention policy removed.
#[tokio::test]
async fn gc_over_plain_registry_notices_evicted_packs() {
    timed(async {
        let fake = FakeOci::start().await;
        let http = reqwest::Client::new();
        fake.set_clock(T0);
        let sim = SimCache::with(fake.plain(&http), fake.clock());
        let a = SimPath::new("a", 1, 100_000);
        let b = SimPath::new("b", 3, 100_000);
        sim.push("main", &[&a], &[&a]).await;
        assert_eq!(fake.evict("pack-"), 1);
        sim.push("main", &[&b], &[&a, &b]).await;
        let gc = Gc {
            dry_run: true,
            ..sim.gc(GcPolicy::default())
        };
        let stats = gc.run(T0 + 2 * HOUR).await.expect("gc");
        assert_eq!(
            (stats.packs_evicted, stats.paths_dropped),
            (1, 1),
            "{stats:?}"
        );
    })
    .await;
}

/// Without a packages API GC deletes manifests over plain OCI (405 on the
/// fake, like GHCR) and cannot list, so it must still complete and serve.
#[tokio::test]
async fn gc_over_plain_registry_survives_refused_deletes() {
    timed(async {
        let fake = FakeOci::start().await;
        let http = reqwest::Client::new();
        fake.set_clock(T0);
        let sim = SimCache::with(fake.plain(&http), fake.clock());
        let a = SimPath::new("a", 1, 100_000);
        sim.push("main", &[&a], &[&a]).await;
        sim.push("main", &[], &[&a]).await;
        let err = sim
            .gc(GcPolicy::default())
            .run(T0 + 2 * HOUR)
            .await
            .expect_err("head delete is refused");
        assert!(err.to_string().contains("405"), "{err}");
        // The record landed before the sweep, so readers see the GC'd view.
        let snap = sim.snapshot().await;
        assert_eq!(snap.view.roots["main"].len(), 1);
        sim.assert_readable(&[&a]).await;
    })
    .await;
}
