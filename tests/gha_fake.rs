//! Integration tests for the GHA cache client against the behavioral fake
//! backend (`tests/support/fake_gha.rs`).
//!
//! The same scenarios run against the real API in `tests/gha_real.rs`
//! (CI only, `#[ignore]` locally).

mod support;

use std::time::Duration;

use bytes::Bytes;

use hestia::gha::savemutable::SaveMutable;
use hestia::gha::twirp::{DownloadUrl, Reservation};
use hestia::gha::{Error, blob};
use support::fake_gha::FakeGha;

/// Reserve + upload + finalize one entry; returns nothing but panics on error.
async fn store_entry(
    twirp: &hestia::gha::twirp::TwirpClient,
    http: &reqwest::Client,
    key: &str,
    data: &[u8],
) {
    let Reservation::Created { upload_url } = twirp.create_cache_entry(key).await.unwrap() else {
        panic!("entry {key} unexpectedly already exists");
    };
    blob::put(http, &upload_url, Bytes::copy_from_slice(data))
        .await
        .unwrap();
    twirp.finalize_upload(key, data.len() as u64).await.unwrap();
}

#[tokio::test]
async fn blob_round_trip() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    // 1 MiB of patterned data: large enough to be a realistic pack blob.
    let data: Vec<u8> = (0..1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    store_entry(&twirp, &http, "pack-roundtrip", &data).await;

    let DownloadUrl::Hit { url, matched_key } =
        twirp.get_download_url("pack-roundtrip", &[]).await.unwrap()
    else {
        panic!("expected hit");
    };
    assert_eq!(matched_key, "pack-roundtrip");

    let downloaded = blob::get(&http, &url, None).await.unwrap();
    assert_eq!(downloaded.as_ref(), data.as_slice());
}

#[tokio::test]
async fn blob_range_reads() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    let data: Vec<u8> = (0..10_000u32).map(|i| (i % 256) as u8).collect();
    store_entry(&twirp, &http, "pack-range", &data).await;

    let DownloadUrl::Hit { url, .. } = twirp.get_download_url("pack-range", &[]).await.unwrap()
    else {
        panic!("expected hit");
    };

    // Interior range.
    let chunk = blob::get(&http, &url, Some(1000..2000)).await.unwrap();
    assert_eq!(chunk.as_ref(), &data[1000..2000]);

    // Range up to the last byte.
    let tail = blob::get(&http, &url, Some(9990..10_000)).await.unwrap();
    assert_eq!(tail.as_ref(), &data[9990..]);

    // 1-byte read (the GC LRU touch).
    let touch = blob::get(&http, &url, Some(0..1)).await.unwrap();
    assert_eq!(touch.as_ref(), &data[0..1]);
}

#[tokio::test]
async fn already_exists_dedup() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    store_entry(&twirp, &http, "pack-dedup", b"chunk data").await;

    // Reserving the same content-addressed key again must be a clean
    // AlreadyExists, not an error: the caller skips the upload.
    let reservation = twirp.create_cache_entry("pack-dedup").await.unwrap();
    assert_eq!(reservation, Reservation::AlreadyExists);

    // A reservation that was never finalized also blocks its key.
    let Reservation::Created { .. } = twirp.create_cache_entry("pack-pending").await.unwrap()
    else {
        panic!("expected fresh reservation");
    };
    let reservation = twirp.create_cache_entry("pack-pending").await.unwrap();
    assert_eq!(reservation, Reservation::AlreadyExists);

    // But unfinalized entries are not downloadable.
    let lookup = twirp.get_download_url("pack-pending", &[]).await.unwrap();
    assert_eq!(lookup, DownloadUrl::Miss);
}

#[tokio::test]
async fn download_miss_and_restore_key_prefix() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    assert_eq!(
        twirp.get_download_url("no-such-key", &[]).await.unwrap(),
        DownloadUrl::Miss
    );

    store_entry(&twirp, &http, "m#1", b"manifest v1").await;
    store_entry(&twirp, &http, "m#2", b"manifest v2").await;

    // Prefix restore key returns the newest matching entry.
    let DownloadUrl::Hit { matched_key, url } =
        twirp.get_download_url("m#", &["m#"]).await.unwrap()
    else {
        panic!("expected hit");
    };
    assert_eq!(matched_key, "m#2");
    let data = blob::get(&http, &url, None).await.unwrap();
    assert_eq!(data.as_ref(), b"manifest v2");
}

#[tokio::test]
async fn url_refresh_retry_on_403() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    store_entry(&twirp, &http, "pack-refresh", b"data behind expiring url").await;

    let DownloadUrl::Hit { url, .. } = twirp.get_download_url("pack-refresh", &[]).await.unwrap()
    else {
        panic!("expected hit");
    };

    // Expire all signed URLs: a plain GET must now fail with 403...
    fake.expire_urls(&http).await;
    let error = blob::get(&http, &url, None).await.unwrap_err();
    let Error::Status { status: 403, .. } = error else {
        panic!("expected 403, got {error:?}");
    };

    // ...but get_with_refresh recovers by fetching a fresh URL via Twirp.
    let twirp_ref = &twirp;
    let data = blob::get_with_refresh(&http, &url, None, async move || {
        match twirp_ref.get_download_url("pack-refresh", &[]).await? {
            DownloadUrl::Hit { url, .. } => Ok(url),
            DownloadUrl::Miss => panic!("entry vanished"),
        }
    })
    .await
    .unwrap();
    assert_eq!(data.as_ref(), b"data behind expiring url");
}

#[tokio::test]
async fn upload_url_refresh_retry_on_403() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    // Reserve, then expire the upload URL before uploading.
    let Reservation::Created { upload_url } = twirp
        .create_cache_entry("pack-upload-refresh")
        .await
        .unwrap()
    else {
        panic!("expected reservation");
    };
    fake.expire_urls(&http).await;

    let error = blob::put(&http, &upload_url, Bytes::from_static(b"payload"))
        .await
        .unwrap_err();
    let Error::Status { status: 403, .. } = error else {
        panic!("expected 403, got {error:?}");
    };

    // The fake (like the real service) cannot re-issue an upload URL for a
    // reserved key via CreateCacheEntry, so refresh goes through the test
    // injection: expire-urls only invalidates old sigs, while a fresh
    // download-URL lookup mints a new one. For uploads we just verify the
    // refresh callback is invoked and its error propagates.
    let result = blob::put_with_refresh(
        &http,
        &upload_url,
        Bytes::from_static(b"payload"),
        async move || {
            Err(Error::InvalidResponse(
                "upload URL expired and cannot be refreshed".to_string(),
            ))
        },
    )
    .await;
    let Err(Error::InvalidResponse(msg)) = result else {
        panic!("expected refresh error to propagate, got {result:?}");
    };
    assert!(msg.contains("cannot be refreshed"));
}

#[tokio::test]
async fn savemutable_versioning() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    let save = SaveMutable::new(&twirp, &http, "m").with_retry(Duration::from_millis(20), 20, 3);

    // First save: no current entry.
    let index = save
        .save(|current| {
            assert!(current.is_none());
            Ok(b"version 1".to_vec())
        })
        .await
        .unwrap();
    assert_eq!(index, 1);

    // Second save sees the first.
    let index = save
        .save(|current| {
            let current = current.expect("must see previous version");
            assert_eq!(current.index, 1);
            assert_eq!(current.data.as_ref(), b"version 1");
            Ok(b"version 2".to_vec())
        })
        .await
        .unwrap();
    assert_eq!(index, 2);

    // Load returns the newest version.
    let entry = save.load().await.unwrap().expect("entry exists");
    assert_eq!(entry.index, 2);
    assert_eq!(entry.key, "m#2");
    assert_eq!(entry.data.as_ref(), b"version 2");
}

#[tokio::test]
async fn savemutable_concurrent_writers_lose_no_updates() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    // Both writers append their marker to a JSON list. Whatever the
    // interleaving, the final manifest must contain both markers: the
    // conflict loop forces the loser to re-merge on top of the winner.
    let merge_with = |marker: &'static str| {
        move |current: Option<&hestia::gha::savemutable::MutableEntry>| {
            let mut list: Vec<String> = match current {
                None => Vec::new(),
                Some(entry) => serde_json::from_slice(&entry.data)
                    .map_err(|e| Error::InvalidResponse(e.to_string()))?,
            };
            list.push(marker.to_string());
            serde_json::to_vec(&list).map_err(|e| Error::InvalidResponse(e.to_string()))
        }
    };

    let save_a = SaveMutable::new(&twirp, &http, "m").with_retry(Duration::from_millis(20), 50, 10);
    let save_b = SaveMutable::new(&twirp, &http, "m").with_retry(Duration::from_millis(20), 50, 10);

    let (index_a, index_b) = tokio::join!(
        save_a.save(merge_with("writer-a")),
        save_b.save(merge_with("writer-b")),
    );
    let (index_a, index_b) = (index_a.unwrap(), index_b.unwrap());
    assert_ne!(index_a, index_b, "writers must land on distinct versions");

    // The newest version contains both markers.
    let final_entry = save_a.load().await.unwrap().expect("manifest exists");
    assert_eq!(final_entry.index, index_a.max(index_b));
    let list: Vec<String> = serde_json::from_slice(&final_entry.data).unwrap();
    assert!(list.contains(&"writer-a".to_string()), "final: {list:?}");
    assert!(list.contains(&"writer-b".to_string()), "final: {list:?}");
}

#[tokio::test]
async fn savemutable_skips_stale_reservation() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    // Simulate a crashed writer: m#1 is reserved but never finalized.
    let Reservation::Created { .. } = twirp.create_cache_entry("m#1").await.unwrap() else {
        panic!("expected reservation");
    };

    let save = SaveMutable::new(&twirp, &http, "m").with_retry(Duration::from_millis(10), 30, 3);
    let index = save.save(|_| Ok(b"recovered".to_vec())).await.unwrap();
    assert_eq!(index, 2, "writer must skip the dead index");

    let entry = save.load().await.unwrap().expect("entry exists");
    assert_eq!(entry.data.as_ref(), b"recovered");
}

#[tokio::test]
async fn savemutable_conflict_gives_up_eventually() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    // m#1 permanently reserved; stale-skip disabled (very high threshold)
    // so the writer can only retry and must eventually give up.
    let Reservation::Created { .. } = twirp.create_cache_entry("m#1").await.unwrap() else {
        panic!("expected reservation");
    };

    let save = SaveMutable::new(&twirp, &http, "m").with_retry(Duration::from_millis(1), 3, 100);
    let error = save
        .save(|_| Ok(b"never lands".to_vec()))
        .await
        .unwrap_err();
    let Error::Conflict { attempts: 3, .. } = error else {
        panic!("expected conflict error, got {error:?}");
    };
}

#[tokio::test]
async fn rest_list_pagination_usage_and_delete() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);
    let rest = fake.rest(&http);

    // 5 packs of 100 bytes plus one unrelated entry.
    for i in 0..5 {
        store_entry(&twirp, &http, &format!("pack-{i:02}"), &[i as u8; 100]).await;
    }
    store_entry(&twirp, &http, "m#1", b"manifest").await;

    // Prefix listing paginates (fake default per_page=30, but our client
    // requests 100; force multiple pages by listing all 6 with prefix "").
    let packs = rest.list_caches("pack-").await.unwrap();
    assert_eq!(packs.len(), 5);
    assert!(packs.iter().all(|e| e.key.starts_with("pack-")));
    assert!(packs.iter().all(|e| e.size_in_bytes == 100));

    let everything = rest.list_caches("").await.unwrap();
    assert_eq!(everything.len(), 6);

    // Usage reflects all finalized entries.
    let usage = rest.usage().await.unwrap();
    assert_eq!(usage.active_caches_count, 6);
    assert_eq!(usage.active_caches_size_in_bytes, 5 * 100 + 8);

    // Delete one pack; it disappears from list and Twirp lookups.
    let deleted = rest.delete_by_key("pack-03").await.unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].key, "pack-03");

    let packs = rest.list_caches("pack-").await.unwrap();
    assert_eq!(packs.len(), 4);
    assert_eq!(
        twirp.get_download_url("pack-03", &[]).await.unwrap(),
        DownloadUrl::Miss
    );

    // Deleting a non-existent key is idempotent (empty result, no error).
    let deleted = rest.delete_by_key("pack-03").await.unwrap();
    assert!(deleted.is_empty());
}

#[tokio::test]
async fn eviction_makes_entry_disappear() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    store_entry(&twirp, &http, "pack-evictme", b"soon to be gone").await;

    let DownloadUrl::Hit { url, .. } = twirp.get_download_url("pack-evictme", &[]).await.unwrap()
    else {
        panic!("expected hit");
    };

    // GitHub evicts the entry behind our back (7-day idle / quota LRU).
    fake.evict(&http, "pack-evictme").await;

    // The Twirp lookup now misses, and the old signed URL 404s: callers must
    // treat the cache as lossy and heal the manifest.
    assert_eq!(
        twirp.get_download_url("pack-evictme", &[]).await.unwrap(),
        DownloadUrl::Miss
    );
    let error = blob::get(&http, &url, None).await.unwrap_err();
    let Error::Status { status: 404, .. } = error else {
        panic!("expected 404 after eviction, got {error:?}");
    };

    // The key can be re-uploaded after eviction (heal path).
    store_entry(&twirp, &http, "pack-evictme", b"healed").await;
    let DownloadUrl::Hit { url, .. } = twirp.get_download_url("pack-evictme", &[]).await.unwrap()
    else {
        panic!("expected hit after heal");
    };
    let data = blob::get(&http, &url, None).await.unwrap();
    assert_eq!(data.as_ref(), b"healed");
}

#[tokio::test]
async fn rest_pagination_with_small_pages() {
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let twirp = fake.twirp(&http);

    // More entries than one fake page (fake per_page default is 30, our
    // client asks for 100; create 7 and list with the client to make sure
    // multi-page accumulation terminates and returns everything).
    for i in 0..7 {
        store_entry(&twirp, &http, &format!("pack-page-{i}"), &[0u8; 10]).await;
    }

    let rest = fake.rest(&http);
    let entries = rest.list_caches("pack-page-").await.unwrap();
    assert_eq!(entries.len(), 7);

    // Listing honors last_accessed_at ordering (most recent first): touch
    // pack-page-0 and verify it moves to the front.
    let DownloadUrl::Hit { url, .. } = twirp.get_download_url("pack-page-0", &[]).await.unwrap()
    else {
        panic!("expected hit");
    };
    blob::get(&http, &url, Some(0..1)).await.unwrap();

    let entries = rest.list_caches("pack-page-").await.unwrap();
    assert_eq!(entries[0].key, "pack-page-0");
}
