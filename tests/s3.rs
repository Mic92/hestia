//! The S3 backend against the fake bucket: key layout, listing, auth, and
//! a full drain + GC + substitution round trip.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hestia::gc::GcPolicy;
use hestia::manifest::Hash32;
use hestia::pipeline::AccessLog;
use hestia::store::Snapshot;
use hestia::substituter::{ManifestStore, Substituter};
use support::common::{TEST_ROOT_KEY, pipeline_context_with, to_path_set};
use support::fake_s3::{FakeS3, PREFIX};
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

async fn listed(b: &hestia::backend::Backend, prefix: &str) -> Vec<String> {
    let l = b.list(prefix, None).await.unwrap().unwrap();
    l.into_iter().map(|l| l.key).collect()
}

const HEAD: &str = "h-0000000000000000-00000000075bcd15-0000000000000001-y";

#[tokio::test]
async fn keys_map_to_prefixed_objects() {
    timed(async {
        let fake = FakeS3::start().await;
        let b = fake.backend();

        let pack = Bytes::from(vec![7u8; 1000]);
        let pack_key = key("pack", &pack);
        b.put(&pack_key, pack.clone()).await.unwrap();
        assert_eq!(b.get(&pack_key, None).await.unwrap().unwrap(), pack);
        assert_eq!(
            b.get(&pack_key, Some(10..20)).await.unwrap().unwrap(),
            pack.slice(10..20)
        );
        assert_eq!(
            b.get(&pack_key, Some(5000..6000)).await.unwrap().unwrap(),
            Bytes::new(),
            "past the end of an existing object"
        );
        assert!(b.touch(&pack_key).await.unwrap());
        assert_eq!(b.get(&key("pack", b"nope"), None).await.unwrap(), None);

        b.put(HEAD, Bytes::new()).await.unwrap();
        assert_eq!(b.get(HEAD, None).await.unwrap().unwrap(), Bytes::new());
        assert_eq!(
            fake.keys(),
            [
                format!("{PREFIX}/heads/{HEAD}"),
                format!("{PREFIX}/pack/{}/{pack_key}", &pack_key[5..7]),
            ]
        );
        assert_eq!(listed(&b, "h-").await, [HEAD]);
        assert_eq!(listed(&b, "c-").await, Vec::<String>::new());
        assert_eq!(listed(&b, "pack-").await, [pack_key.as_str()]);
        assert!(b.delete(HEAD).await.unwrap());
        assert_eq!(listed(&b, "h-").await, Vec::<String>::new());
    })
    .await;
}

#[tokio::test]
async fn listing_pages_and_lags() {
    timed(async {
        let fake = FakeS3::start().await;
        let b = fake.backend();
        fake.set_clock(1_700_000_000);
        for i in 0..2100u32 {
            let data = i.to_le_bytes();
            b.put(&key("seg", &data), Bytes::copy_from_slice(&data))
                .await
                .unwrap();
        }
        let all = b.list("seg-", None).await.unwrap().unwrap();
        assert_eq!(all.len(), 2100);
        assert!(all.iter().all(|l| l.created == Some(1_700_000_000)));
        assert_eq!(b.list("seg-", Some(10)).await.unwrap(), None);
        assert_eq!(b.list("tree-", None).await.unwrap().unwrap().len(), 0);

        fake.set_list_lag(5);
        b.put(HEAD, Bytes::new()).await.unwrap();
        assert!(b.list("h-", None).await.unwrap().unwrap().is_empty());
        assert!(
            b.get(HEAD, None).await.unwrap().is_some(),
            "readable by name before it is listed"
        );
        for _ in 0..5 {
            b.touch(HEAD).await.unwrap();
        }
        assert_eq!(b.list("h-", None).await.unwrap().unwrap().len(), 1);
    })
    .await;
}

#[tokio::test]
async fn anonymous_and_read_only_credentials() {
    timed(async {
        let fake = FakeS3::start().await;
        let rw = fake.backend();
        let data = Bytes::from_static(b"blob");
        rw.put(&key("seg", &data), data.clone()).await.unwrap();
        assert!(rw.probe_writable().await.unwrap());

        let anon = fake.anonymous();
        assert!(!anon.probe_writable().await.unwrap());
        let err = anon.get(&key("seg", &data), None).await.unwrap_err();
        assert!(err.to_string().contains("403"), "{err}");
        fake.set_public(true);
        assert_eq!(
            anon.get(&key("seg", &data), None).await.unwrap().unwrap(),
            data
        );

        fake.set_read_only(true);
        assert!(!rw.probe_writable().await.unwrap());
        let err = rw
            .put(&key("tree", b"x"), Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("403"), "{err}");
    })
    .await;
}

#[tokio::test]
async fn drain_and_nix_copy_over_s3() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let (top, dep) = store.add_paths_with_reference("s3copy");
        let fake = FakeS3::start().await;

        let stats = pipeline_context_with(fake.backend(), store.database())
            .run(to_path_set(&[&top, &dep]), BTreeSet::new())
            .await
            .expect("pipeline run");
        assert_eq!(stats.pushed, 2);
        let again = pipeline_context_with(fake.backend(), store.database())
            .run(to_path_set(&[&top, &dep]), BTreeSet::new())
            .await
            .unwrap();
        assert_eq!((again.pushed, again.packs_uploaded), (0, 0));

        fake.set_public(true);
        let backend = fake.anonymous();
        let snapshot = Snapshot::load(backend.clone(), &[TEST_ROOT_KEY.to_string()], None)
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

/// GC over S3 lists objects directly: retired segments go the run after,
/// an orphan older than `min_age` goes at once, an expired root's pack
/// one epoch later, and a lagging listing loses nothing.
#[tokio::test]
async fn gc_over_s3_with_lagging_listing() {
    timed(async {
        let fake = FakeS3::start().await;
        fake.set_clock(T0);
        let sim = SimCache::with(fake.backend(), fake.clock());
        let a = SimPath::new("a", 1, 200_000);
        let b = SimPath::new("b", 3, 200_000);
        let gone = SimPath::new("gone", 5, 200_000);
        sim.push("main", &[&a], &[&a]).await;
        sim.push("main", &[&b], &[&a, &b]).await;
        sim.push("old", &[&gone], &[&gone]).await;
        sim.upload_orphan_pack(9).await;
        let count = |kind: &'static str| {
            let needle = format!("/{kind}-");
            fake.keys().iter().filter(|k| k.contains(&needle)).count()
        };
        assert_eq!((count("pack"), count("seg")), (4, 3));

        let policy = GcPolicy {
            root_ttl: 24 * HOUR,
            ..GcPolicy::default()
        };
        fake.set_clock(T0 + 2 * HOUR);
        fake.set_list_lag(3);
        let stats = sim.run_gc(policy.clone(), T0 + 2 * HOUR).await;
        assert_eq!((stats.roots, stats.deleted), (2, 4), "{stats:?}");
        assert_eq!(count("pack"), 3, "orphan swept");
        fake.set_list_lag(0);
        sim.assert_readable(&[&a, &b, &gone]).await;

        fake.set_clock(T0 + 30 * HOUR);
        sim.push("main", &[], &[&a, &b]).await;
        let stats = sim.run_gc(policy.clone(), T0 + 30 * HOUR).await;
        assert_eq!(stats.roots_expired, 1, "{stats:?}");
        assert_eq!(count("pack"), 3, "old's pack waits one epoch");
        assert_eq!(count("seg"), 2);
        sim.assert_readable(&[&a, &b]).await;

        fake.set_clock(T0 + 60 * HOUR);
        sim.push("main", &[], &[&a, &b]).await;
        sim.run_gc(policy, T0 + 60 * HOUR).await;
        assert_eq!((count("pack"), count("seg")), (2, 1));
        sim.assert_readable(&[&a, &b]).await;
        sim.assert_no_dangling_references().await;
    })
    .await;
}
