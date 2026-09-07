//! Helpers shared by the integration tests that drive the write pipeline
//! against the fake GHA backend.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use bytes::Bytes;

use hestia::backend::Backend;
use hestia::gha::blob;
use hestia::gha::twirp::{Reservation, TwirpClient};
use hestia::manifest::PathHash;
use hestia::pathinfo::StoreDatabase;
use hestia::pipeline::{PACK_TARGET_SIZE, PipelineContext, system_clock};
use hestia::store::Snapshot;
use hestia::upstream::UpstreamFilter;

use super::fake_gha::FakeGha;

/// Root key (branch + system) used by all pipeline-driving tests.
pub const TEST_ROOT_KEY: &str = "main-test-system";

/// Pipeline context against the fake backend. Uses the default upstream
/// key set (production only enables filtering with
/// --upstream-cache-filter); scratch-store paths pass either way because
/// they are unsigned, just like locally built paths.
pub fn pipeline_context(
    fake: &FakeGha,
    http: &reqwest::Client,
    store: StoreDatabase,
) -> PipelineContext {
    PipelineContext {
        clock: fake.clock(),
        ..pipeline_context_with(fake.backend(http), store)
    }
}

/// Same, with an explicit backend for tests that put a proxy between
/// the pipeline and the fake.
pub fn pipeline_context_with(backend: Backend, store: StoreDatabase) -> PipelineContext {
    PipelineContext {
        backend,
        trust: hestia::trust::Trust::open(),
        store,
        upstream: UpstreamFilter::default(),
        expand_closure: true,
        filter_drv_closures: false,
        root_key: TEST_ROOT_KEY.to_string(),
        pack_target_size: PACK_TARGET_SIZE,
        read_only: Arc::new(AtomicBool::new(false)),
        publish: None,
        clock: system_clock(),
    }
}

/// What the test root currently publishes, freshly listed.
pub async fn load_snapshot(fake: &FakeGha, http: &reqwest::Client) -> Arc<Snapshot> {
    Arc::new(
        Snapshot::load(
            fake.backend(http),
            hestia::trust::Trust::open(),
            &[TEST_ROOT_KEY.to_string()],
            None,
        )
        .await
        .expect("loading heads failed"),
    )
}

/// Store path hash (`<hash>` of `<hash>-<name>`).
/// A served `.nar.zst` body back to the NAR.
pub async fn nar_body(response: reqwest::Response) -> Bytes {
    let body = response.bytes().await.expect("nar body");
    Bytes::from(zstd::decode_all(&body[..]).expect("nar.zst decodes"))
}

pub fn path_hash_of(store_path: &Path) -> PathHash {
    let name = store_path.file_name().unwrap().to_str().unwrap();
    name[..32]
        .parse()
        .expect("store path basename starts with its hash")
}

pub fn to_path_set(paths: &[&Path]) -> BTreeSet<String> {
    paths
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
}

/// Reserve + upload + finalize one cache entry directly, bypassing hestia's
/// pipeline (e.g. to plant a corrupt blob).
pub async fn store_entry(twirp: &TwirpClient, http: &reqwest::Client, key: &str, data: &[u8]) {
    let Reservation::Created { upload_url } = twirp.create_cache_entry(key).await.unwrap() else {
        panic!("entry {key} unexpectedly already exists");
    };
    blob::put(http, &upload_url, Bytes::copy_from_slice(data))
        .await
        .unwrap();
    twirp.finalize_upload(key, data.len() as u64).await.unwrap();
}

/// Every path the snapshot lists must resolve: each chunk found in a
/// loadable pack index. A violation means narinfo answers but the NAR
/// 404s, and no later drain heals it since the path dedup-skips.
pub async fn assert_all_chunks_locatable(snapshot: &Snapshot) {
    for hash in snapshot.path_hashes() {
        snapshot
            .resolve(&hash)
            .await
            .unwrap_or_else(|err| panic!("path {hash} does not resolve: {err}"));
    }
}

/// A GET answer honouring `Range: bytes=s-e`: 206 with the slice, 416
/// past the end, 200 with everything when no range was asked for.
pub fn serve_range(body: Bytes, range: Option<&str>) -> axum::response::Response {
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;
    let etag = format!("\"{}\"", hestia::manifest::Hash32::digest(&body));
    let Some(spec) = range else {
        return (StatusCode::OK, [(header::ETAG, etag)], body).into_response();
    };
    let parse = || -> Option<(usize, usize)> {
        let (s, e) = spec.strip_prefix("bytes=")?.split_once('-')?;
        let s: usize = s.parse().ok()?;
        let e = if e.is_empty() {
            body.len().checked_sub(1)?
        } else {
            e.parse::<usize>().ok()?.min(body.len().checked_sub(1)?)
        };
        (s <= e).then_some((s, e))
    };
    match parse() {
        Some((s, e)) => (
            StatusCode::PARTIAL_CONTENT,
            [(
                header::CONTENT_RANGE,
                format!("bytes {s}-{e}/{}", body.len()),
            )],
            body.slice(s..=e),
        )
            .into_response(),
        None => (
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(header::CONTENT_RANGE, format!("bytes */{}", body.len()))],
        )
            .into_response(),
    }
}
