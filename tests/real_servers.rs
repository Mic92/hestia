//! The same backend contract and GC round trip against real servers:
//! rustfs for S3 and the distribution registry for OCI. Skipped when the
//! binaries are not on PATH.

mod support;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use hestia::backend::Backend;
use hestia::gc::GcPolicy;
use hestia::manifest::Hash32;
use support::real::Server;
use support::sim::{SimCache, SimPath};

const HEAD: &str = "h-0000000000000000-00000000075bcd15-0000000000000001-y";

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn timed<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(120), f)
        .await
        .expect("test timed out")
}

async fn keys(b: &Backend, prefix: &str) -> Vec<String> {
    let l = b.list(prefix, None).await.unwrap().unwrap();
    l.into_iter().map(|l| l.key).collect()
}

/// `enumerable`: plain registries cannot list blobs, so GC sweeps no orphans there.
async fn contract(b: &Backend, enumerable: bool) {
    assert!(b.probe_writable().await.unwrap());
    let pack = Bytes::from(vec![7u8; 1000]);
    let pack_key = format!("pack-{}", Hash32::digest(&pack));
    b.put(&pack_key, pack.clone()).await.unwrap();
    assert_eq!(b.get(&pack_key, None).await.unwrap().unwrap(), pack);
    assert_eq!(
        b.get(&pack_key, Some(10..20)).await.unwrap().unwrap(),
        pack.slice(10..20)
    );
    assert!(b.touch(&pack_key).await.unwrap());
    assert_eq!(
        b.get(&format!("pack-{}", Hash32::digest(b"nope")), None)
            .await
            .unwrap(),
        None
    );

    b.put(HEAD, Bytes::new()).await.unwrap();
    assert_eq!(b.get(HEAD, None).await.unwrap().unwrap(), Bytes::new());
    assert_eq!(keys(b, "h-").await, [HEAD]);
    assert!(keys(b, "c-").await.is_empty());
    let objects = b.list_objects().await.unwrap();
    assert_eq!(objects.is_some(), enumerable);
    if let Some(objects) = objects {
        assert!(objects.iter().any(|l| l.key == pack_key), "{objects:?}");
        assert!(
            objects
                .iter()
                .all(|l| l.created.is_some_and(|t| t + 600 > now())),
            "{objects:?}"
        );
    }
    assert!(b.delete(HEAD).await.unwrap());
    assert!(keys(b, "h-").await.is_empty());
    assert!(b.delete(&pack_key).await.unwrap());
    // A registry keeps the blob until its own GC runs.
    assert_eq!(b.get(&pack_key, None).await.unwrap().is_some(), !enumerable);
}

/// Drains, an orphan, GC runs in between (`now + 2` so `min_age: 0`
/// counts fresh objects as old), and every path still readable.
async fn gc_round_trip(backend: Backend, enumerable: bool) {
    let sim = SimCache::with(backend, Arc::new(now));
    let a = SimPath::new("a", 1, 200_000);
    let b = SimPath::new("b", 3, 200_000);
    sim.push("main", &[&a], &[&a]).await;
    sim.push("main", &[&b], &[&a, &b]).await;
    let orphan = sim.upload_orphan_pack(9).await;

    let policy = GcPolicy {
        min_age: 0,
        min_interval: 0,
        ..GcPolicy::default()
    };
    let stats = sim.run_gc(policy.clone(), now() + 2).await;
    assert_eq!(stats.roots, 1, "{stats:?}");
    if enumerable {
        assert_eq!(sim.backend.get(&orphan, None).await.unwrap(), None);
    }
    sim.assert_readable(&[&a, &b]).await;

    sim.push("main", &[], &[&a, &b]).await;
    let stats = sim.run_gc(policy, now() + 2).await;
    assert!(stats.deleted >= 1, "retired segment: {stats:?}");
    sim.assert_readable(&[&a, &b]).await;
    if enumerable {
        sim.assert_no_dangling_references().await;
    }
}

#[tokio::test]
async fn rustfs() {
    timed(async {
        let http = reqwest::Client::new();
        let Some(server) = Server::rustfs(&http).await else {
            eprintln!("rustfs not on PATH, skipping");
            return;
        };
        contract(&server.s3(&http), true).await;
        gc_round_trip(server.s3(&http), true).await;
    })
    .await;
}

#[tokio::test]
async fn distribution_registry() {
    timed(async {
        let http = reqwest::Client::new();
        let Some(server) = Server::registry(&http).await else {
            eprintln!("registry not on PATH, skipping");
            return;
        };
        contract(&server.oci(&http), false).await;
        gc_round_trip(server.oci(&http), false).await;
    })
    .await;
}
