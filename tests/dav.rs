//! The WebDAV origin of the blobdir backend against the fake share: key
//! layout, MKCOL on demand, PROPFIND listings, auth, nginx quirks, the
//! index for plain-HTTP readers, and a drain + GC + substitution round trip.

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
use support::fake_dav::{FakeDav, PREFIX};
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
    let mut l: Vec<String> = b
        .list(prefix, None)
        .await
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|l| l.key)
        .collect();
    l.sort();
    l
}

const HEAD: &str = "h-0000000000000000-00000000075bcd15-0000000000000001-y";

#[tokio::test]
async fn keys_map_to_collections_made_on_demand() {
    timed(async {
        let fake = FakeDav::start().await;
        let b = fake.backend();
        assert!(b.probe_writable().await.unwrap());

        let pack = Bytes::from(vec![7u8; 1000]);
        let pack_key = key("pack", &pack);
        assert!(b.put(&pack_key, pack.clone()).await.unwrap());
        assert!(
            !b.put(&pack_key, pack.clone()).await.unwrap(),
            "create-once: the second PUT finds it there"
        );
        assert_eq!(b.get(&pack_key, None).await.unwrap().unwrap(), pack);
        assert_eq!(
            b.get(&pack_key, Some(10..20)).await.unwrap().unwrap(),
            pack.slice(10..20)
        );
        assert_eq!(
            b.get(&pack_key, Some(5000..6000)).await.unwrap().unwrap(),
            Bytes::new()
        );
        assert!(b.touch(&pack_key).await.unwrap());
        assert_eq!(b.get(&key("pack", b"nope"), None).await.unwrap(), None);
        assert!(!b.touch(&key("seg", b"nope")).await.unwrap());

        let seg = Bytes::from_static(b"segment");
        let seg_key = key("seg", &seg);
        b.put(&seg_key, seg).await.unwrap();
        b.put(HEAD, Bytes::new()).await.unwrap();
        b.put(HEAD, Bytes::from_static(b"v2")).await.unwrap();
        assert_eq!(
            b.get(HEAD, None).await.unwrap().unwrap(),
            Bytes::from_static(b"v2"),
            "heads are overwritten, not create-once"
        );
        assert_eq!(
            fake.files(),
            [
                format!("{PREFIX}/heads/{HEAD}"),
                format!("{PREFIX}/heads/x-probe"),
                format!("{PREFIX}/pack/{}/{pack_key}", &pack_key[5..7]),
                format!("{PREFIX}/seg/{}/{seg_key}", &seg_key[4..6]),
            ]
            .into_iter()
            .filter(|p| !p.ends_with("x-probe"))
            .collect::<Vec<_>>()
        );
        assert_eq!(listed(&b, "h-").await, [HEAD]);
        assert_eq!(listed(&b, "").await, [HEAD]);
        assert!(listed(&b, "c-").await.is_empty());
        assert_eq!(listed(&b, "pack-").await, [pack_key.as_str()]);
        assert_eq!(listed(&b, "seg-").await, [seg_key.as_str()]);
        assert!(listed(&b, "tree-").await.is_empty());
        let objects = b.list_objects().await.unwrap();
        assert_eq!(objects.len(), 2);
        assert!(b.delete(HEAD).await.unwrap());
        assert!(!b.delete(HEAD).await.unwrap());
        assert!(listed(&b, "h-").await.is_empty());
    })
    .await;
}

/// Four packs upload concurrently into fresh shard directories; each
/// directory is made once, not once per object or per racer.
#[tokio::test]
async fn mkcol_runs_once_per_directory() {
    timed(async {
        let fake = FakeDav::start().await;
        let b = fake.backend();
        fake.set_clock(1_700_000_000);
        let puts = (0..64u32).map(|i| {
            let b = b.clone();
            async move {
                let data = i.to_le_bytes();
                b.put(&key("seg", &data), Bytes::copy_from_slice(&data))
                    .await
                    .unwrap();
            }
        });
        futures_util::future::join_all(puts).await;
        let all = b.list("seg-", None).await.unwrap().unwrap();
        assert_eq!(all.len(), 64);
        assert!(all.iter().all(|l| l.created == Some(1_700_000_000)));
        assert_eq!(b.list("seg-", Some(10)).await.unwrap(), None);
        let shards: BTreeSet<_> = fake
            .files()
            .iter()
            .filter_map(|f| f.strip_prefix(&format!("{PREFIX}/seg/")))
            .map(|f| f[..2].to_owned())
            .collect();
        // seg/ and exactly one per shard despite the racers.
        assert_eq!(fake.mkcols(), 1 + shards.len() as u64);
        let before = fake.mkcols();
        b.put(
            &key("seg", &7u32.to_le_bytes()),
            Bytes::from_static(b"again"),
        )
        .await
        .unwrap();
        assert_eq!(fake.mkcols(), before, "known directories are not remade");
    })
    .await;
}

#[tokio::test]
async fn nginx_quirks() {
    timed(async {
        let fake = FakeDav::start().await;
        fake.set_nginx(true);
        let b = fake.backend();
        assert!(b.probe_writable().await.unwrap());
        let pack = Bytes::from_static(b"frames");
        assert!(b.put(&key("pack", &pack), pack.clone()).await.unwrap());
        assert!(
            b.put(&key("pack", &pack), pack.clone()).await.unwrap(),
            "nginx ignores If-None-Match, so an overwrite looks like a create"
        );
        b.put(HEAD, Bytes::new()).await.unwrap();
        b.flush().await.unwrap();
        fake.set_public(true);
        assert_eq!(listed(&fake.plain_http(), "h-").await, [HEAD]);
    })
    .await;
}

#[tokio::test]
async fn anonymous_and_read_only_credentials() {
    timed(async {
        let fake = FakeDav::start().await;
        let rw = fake.backend();
        let data = Bytes::from_static(b"blob");
        rw.put(&key("seg", &data), data.clone()).await.unwrap();

        let anon = fake.anonymous();
        let err = anon.get(&key("seg", &data), None).await.unwrap_err();
        assert!(err.to_string().contains("401"), "{err}");
        assert!(!anon.probe_writable().await.unwrap());
        fake.set_public(true);
        assert_eq!(
            anon.get(&key("seg", &data), None).await.unwrap().unwrap(),
            data
        );
        assert!(!anon.probe_writable().await.unwrap());

        // The same tree over plain HTTP: objects by name, heads from the
        // index the DAV writer keeps.
        let http = fake.plain_http();
        assert_eq!(
            http.get(&key("seg", &data), None).await.unwrap().unwrap(),
            data
        );
        assert_eq!(http.list_heads().await.unwrap(), vec![]);
        rw.put(HEAD, Bytes::from_static(b"head")).await.unwrap();
        rw.flush().await.unwrap();
        assert_eq!(listed(&http, "h-").await, [HEAD]);
        assert!(rw.delete(HEAD).await.unwrap());
        rw.flush().await.unwrap();
        assert!(listed(&http, "h-").await.is_empty());
        assert!(!http.probe_writable().await.unwrap());

        fake.set_read_only(true);
        assert!(!rw.probe_writable().await.unwrap());
        let err = rw
            .put(&key("tree", b"x"), Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("403"), "{err}");
        assert!(rw.read_only_hint().contains("credentials"));
    })
    .await;
}

#[tokio::test]
async fn concurrent_head_writers_keep_both_in_the_index() {
    timed(async {
        let fake = FakeDav::start().await;
        let (a, b) = (fake.backend(), fake.backend());
        let other = HEAD.replace("-y", "-z");
        a.put(HEAD, Bytes::from_static(b"a")).await.unwrap();
        b.put(&other, Bytes::from_static(b"b")).await.unwrap();
        fake.set_rtt(Duration::from_millis(50));
        fake.take_requests();
        tokio::try_join!(a.flush(), b.flush()).unwrap();
        let puts = fake
            .take_requests()
            .into_iter()
            .filter(|r| r.starts_with("PUT") && r.ends_with("/index"))
            .count();
        assert_eq!(puts, 3, "one writer loses the compare and swap and retries");
        fake.set_rtt(Duration::ZERO);
        fake.set_public(true);
        assert_eq!(
            listed(&fake.plain_http(), "h-").await,
            [HEAD.to_owned(), other]
        );
    })
    .await;
}

#[tokio::test]
async fn drain_and_nix_copy_over_dav() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let (top, dep) = store.add_paths_with_reference("davcopy");
        let fake = FakeDav::start().await;

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

        for backend in [fake.backend(), fake.plain_http()] {
            fake.set_public(true);
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
        }
    })
    .await;
}

const T0: u64 = 1_750_000_000;
const HOUR: u64 = 3600;

/// GC lists pack/ and seg/ shard by shard over PROPFIND.
#[tokio::test]
async fn gc_over_dav() {
    timed(async {
        let fake = FakeDav::start().await;
        fake.set_clock(T0);
        let sim = SimCache::with(fake.backend(), fake.clock());
        let a = SimPath::new("a", 1, 200_000);
        let b = SimPath::new("b", 3, 200_000);
        sim.push("main", &[&a], &[&a]).await;
        sim.push("main", &[&b], &[&a, &b]).await;
        let orphan = sim.upload_orphan_pack(9).await;
        let count = |kind: &'static str| {
            let needle = format!("/{kind}-");
            fake.files().iter().filter(|k| k.contains(&needle)).count()
        };
        assert_eq!((count("pack"), count("seg")), (3, 2));

        fake.set_clock(T0 + 2 * HOUR);
        let policy = GcPolicy::default();
        let stats = sim.run_gc(policy.clone(), T0 + 2 * HOUR).await;
        assert_eq!(stats.roots, 1, "{stats:?}");
        assert!(stats.deleted >= 1, "{stats:?}");
        assert_eq!(sim.backend.get(&orphan, None).await.unwrap(), None);
        sim.assert_readable(&[&a, &b]).await;

        fake.set_clock(T0 + 4 * HOUR);
        sim.push("main", &[], &[&a, &b]).await;
        let stats = sim.run_gc(policy, T0 + 4 * HOUR).await;
        assert!(stats.deleted >= 1, "retired segment: {stats:?}");
        assert_eq!((count("pack"), count("seg")), (2, 1));
        sim.assert_readable(&[&a, &b]).await;
        sim.assert_no_dangling_references().await;
    })
    .await;
}
