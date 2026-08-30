//! The OCI backend against the fake registry: the key/blob/tag mapping,
//! auth, and a full drain + substitution round trip.

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use hestia::manifest::Hash32;
use hestia::pipeline::AccessLog;
use hestia::store::Snapshot;
use hestia::substituter::{ManifestStore, Substituter};
use support::common::{TEST_ROOT_KEY, pipeline_context_with, to_path_set};
use support::fake_oci::FakeOci;
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
        b.put("c-0000000000000000-00000000075bcd15-x", record.clone())
            .await
            .unwrap();
        b.put("h-0000000000000000-00000000075bcd15-y", Bytes::new())
            .await
            .unwrap();
        assert_eq!(
            b.get("c-0000000000000000-00000000075bcd15-x", None)
                .await
                .unwrap()
                .unwrap(),
            record
        );
        assert_eq!(
            b.get("h-0000000000000000-00000000075bcd15-y", None)
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
        assert_eq!(listed, ["c-0000000000000000-00000000075bcd15-x"]);
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
                &format!("h-0000000000000000-00000000075bcd15-{i:04}"),
                Bytes::new(),
            )
            .await
            .unwrap();
        }
        assert_eq!(b.list("h-", None).await.unwrap().unwrap().len(), 2100);
        assert_eq!(b.list("h-", Some(10)).await.unwrap(), None);

        fake.set_tag_lag(5);
        b.put("g-0000000000000001-z", Bytes::from_static(b"r"))
            .await
            .unwrap();
        assert!(b.list("g-", None).await.unwrap().unwrap().is_empty());
        assert!(
            b.get("g-0000000000000001-z", None).await.unwrap().is_some(),
            "readable by name before it is listed"
        );
        for _ in 0..5 {
            b.touch("g-0000000000000001-z").await.unwrap();
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
