//! Integration tests against the *real* GitHub Actions cache API.
//!
//! These can only run inside a GitHub Actions job with the runtime tokens
//! captured by the hestia action wrapper (`./action`), so they are
//! `#[ignore]` locally and run in CI via:
//!
//! ```text
//! cargo test --test gha_real -- --ignored
//! ```
//!
//! They exercise the same scenarios as `tests/gha_fake.rs` to catch drift
//! between the fake and the real service.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use hestia::gha::blob;
use hestia::gha::rest::RestClient;
use hestia::gha::twirp::{DownloadUrl, Reservation, TwirpClient};

/// The real service is eventually consistent: a just-finalized entry can
/// take a while to become visible to GetCacheEntryDownloadURL (observed in
/// CI: sometimes instant, sometimes several seconds). Hestia's design
/// tolerates this (a lagging read is a cache miss, never corruption), but
/// read-after-write assertions in these tests must poll.
const PROPAGATION_TIMEOUT: Duration = Duration::from_secs(120);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Poll `lookup` until `accept` returns true for its result or the
/// propagation timeout expires; returns the last result either way.
async fn wait_for<T, F, Fut>(mut lookup: F, accept: impl Fn(&T) -> bool) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = T>,
{
    let deadline = std::time::Instant::now() + PROPAGATION_TIMEOUT;
    loop {
        let result = lookup().await;
        if accept(&result) || std::time::Instant::now() >= deadline {
            return result;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Unique key per test run so re-runs never collide with old entries
/// (cache keys are write-once).
fn unique_key(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("hestia-test-{name}-{nanos}")
}

fn twirp_client(http: &reqwest::Client) -> TwirpClient {
    TwirpClient::from_env(http.clone()).expect(
        "ACTIONS_RESULTS_URL / ACTIONS_RUNTIME_TOKEN not set; \
         these tests only work inside a GitHub Actions job with the hestia action",
    )
}

fn rest_client(http: &reqwest::Client) -> RestClient {
    RestClient::from_env(http.clone())
        .expect("GITHUB_TOKEN / GITHUB_REPOSITORY not set; need workflow env")
}

#[tokio::test]
#[ignore = "requires real GHA cache tokens (run in CI)"]
async fn real_blob_round_trip_range_read_and_delete() {
    let http = reqwest::Client::new();
    let twirp = twirp_client(&http);
    let rest = rest_client(&http);

    let key = unique_key("roundtrip");
    // Patterned payload, large enough to make Range reads meaningful.
    let data: Vec<u8> = (0..256 * 1024u32).map(|i| (i % 251) as u8).collect();

    // Reserve + upload + finalize.
    let Reservation::Created { upload_url } = twirp.create_cache_entry(&key).await.unwrap() else {
        panic!("fresh unique key reported as already existing");
    };
    blob::put(&http, &upload_url, Bytes::from(data.clone()))
        .await
        .unwrap();
    twirp
        .finalize_upload(&key, data.len() as u64)
        .await
        .unwrap();

    // Reserving the same key again must report AlreadyExists (CAS dedup).
    // Unlike lookups, reservations are strongly consistent: this is what
    // SaveMutable's conflict detection relies on.
    let reservation = twirp.create_cache_entry(&key).await.unwrap();
    assert_eq!(reservation, Reservation::AlreadyExists);

    // Full download. Lookups are eventually consistent: poll until the
    // just-finalized entry becomes visible.
    let lookup = wait_for(
        || async { twirp.get_download_url(&key, &[]).await.unwrap() },
        |result| matches!(result, DownloadUrl::Hit { .. }),
    )
    .await;
    let DownloadUrl::Hit { url, matched_key } = lookup else {
        panic!("just-finalized entry not found after {PROPAGATION_TIMEOUT:?}");
    };
    assert_eq!(matched_key, key);
    let downloaded = blob::get(&http, &url, None).await.unwrap();
    assert_eq!(
        downloaded.as_ref(),
        data.as_slice(),
        "full download differs"
    );

    // Range read (chunk access pattern).
    let chunk = blob::get(&http, &url, Some(1000..2000)).await.unwrap();
    assert_eq!(chunk.as_ref(), &data[1000..2000], "range read differs");

    // 1-byte read (GC LRU touch).
    let touch = blob::get(&http, &url, Some(0..1)).await.unwrap();
    assert_eq!(touch.len(), 1);

    // REST: the entry shows up in a prefix list. The REST view lags behind
    // uploads just like Twirp lookups do, so this must poll too.
    let listed = wait_for(
        || async { rest.list_caches("hestia-test-roundtrip-").await.unwrap() },
        |entries| entries.iter().any(|entry| entry.key == key),
    )
    .await;
    assert!(
        listed.iter().any(|entry| entry.key == key),
        "uploaded entry missing from REST list after {PROPAGATION_TIMEOUT:?}: {listed:?}"
    );

    // ...and REST delete removes it for real (deletes propagate eventually,
    // like lookups).
    let deleted = rest.delete_by_key(&key).await.unwrap();
    assert!(
        deleted.iter().any(|entry| entry.key == key),
        "delete did not report the entry: {deleted:?}"
    );
    let lookup = wait_for(
        || async { twirp.get_download_url(&key, &[]).await.unwrap() },
        |result| matches!(result, DownloadUrl::Miss),
    )
    .await;
    assert_eq!(
        lookup,
        DownloadUrl::Miss,
        "entry still downloadable after REST delete + propagation timeout"
    );
}

#[tokio::test]
#[ignore = "requires real GHA cache tokens (run in CI)"]
async fn real_miss_and_restore_key_prefix() {
    let http = reqwest::Client::new();
    let twirp = twirp_client(&http);
    let rest = rest_client(&http);

    // Lookup of a key that cannot exist.
    let missing = unique_key("missing");
    assert_eq!(
        twirp.get_download_url(&missing, &[]).await.unwrap(),
        DownloadUrl::Miss
    );

    // SaveMutable-style versioned entries: v1 then v2; prefix lookup
    // must return v2 (the newest).
    let family = unique_key("family");
    for version in 1..=2u8 {
        let key = format!("{family}#{version}");
        let payload = format!("payload v{version}").into_bytes();
        let Reservation::Created { upload_url } = twirp.create_cache_entry(&key).await.unwrap()
        else {
            panic!("fresh key {key} already exists");
        };
        blob::put(&http, &upload_url, Bytes::from(payload.clone()))
            .await
            .unwrap();
        twirp
            .finalize_upload(&key, payload.len() as u64)
            .await
            .unwrap();
    }

    // Newest-wins is also subject to propagation lag: right after
    // finalizing #2, the lookup may still return #1 (or nothing). This is
    // exactly why SaveMutable detects conflicts at reservation time (which
    // is strongly consistent) instead of trusting load() to be fresh.
    let prefix = format!("{family}#");
    let expected_key = format!("{family}#2");
    let lookup = wait_for(
        || async {
            twirp
                .get_download_url(&prefix, &[prefix.as_str()])
                .await
                .unwrap()
        },
        |result| matches!(result, DownloadUrl::Hit { matched_key, .. } if *matched_key == expected_key),
    )
    .await;
    let DownloadUrl::Hit { url, matched_key } = lookup else {
        panic!("prefix restore lookup missed after {PROPAGATION_TIMEOUT:?}");
    };
    assert_eq!(matched_key, expected_key, "newest entry must win");
    let data = blob::get(&http, &url, None).await.unwrap();
    assert_eq!(data.as_ref(), b"payload v2");

    // Cleanup.
    for version in 1..=2u8 {
        let _ = rest.delete_by_key(&format!("{family}#{version}")).await;
    }
}
