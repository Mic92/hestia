//! Helpers shared by the integration tests that drive the write pipeline
//! against the fake GHA backend.

use std::collections::BTreeSet;
use std::path::Path;

use bytes::Bytes;

use hestia::gha::blob;
use hestia::gha::savemutable::SaveMutable;
use hestia::gha::twirp::{Reservation, TwirpClient};
use hestia::manifest::{FileSystemObject, Manifest, PathHash};
use hestia::pathinfo::StoreDatabase;
use hestia::pipeline::{MANIFEST_PREFIX, PACK_TARGET_SIZE, PipelineContext};
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
    pipeline_context_with(fake.twirp(http), http, store)
}

/// Same, with an explicit Twirp client for tests that put a proxy between
/// the pipeline and the fake backend.
pub fn pipeline_context_with(
    twirp: TwirpClient,
    http: &reqwest::Client,
    store: StoreDatabase,
) -> PipelineContext {
    PipelineContext {
        twirp,
        http: http.clone(),
        store,
        upstream: UpstreamFilter::default(),
        expand_closure: true,
        root_key: TEST_ROOT_KEY.to_string(),
        run_id: Some("test-run".to_string()),
        manifest_prefix: MANIFEST_PREFIX.to_string(),
        pack_target_size: PACK_TARGET_SIZE,
        publish: None,
    }
}

/// The manifest path key of a store path (`<hash>` of `<hash>-<name>`).
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

/// Load the newest committed manifest directly from the fake backend, or
/// `None` if no version was ever committed.
pub async fn committed_manifest(fake: &FakeGha, http: &reqwest::Client) -> Option<(u64, Manifest)> {
    let twirp = fake.twirp(http);
    let save = SaveMutable::new(&twirp, http, MANIFEST_PREFIX);
    let entry = save.load().await.expect("loading manifest failed")?;
    Some((
        entry.index,
        Manifest::decode(&entry.data).expect("manifest must decode"),
    ))
}

/// Reserve + upload + finalize one cache entry directly, bypassing hestia's
/// pipeline (e.g. to plant a corrupt manifest blob).
pub async fn store_entry(twirp: &TwirpClient, http: &reqwest::Client, key: &str, data: &[u8]) {
    let Reservation::Created { upload_url } = twirp.create_cache_entry(key).await.unwrap() else {
        panic!("entry {key} unexpectedly already exists");
    };
    blob::put(http, &upload_url, Bytes::copy_from_slice(data))
        .await
        .unwrap();
    twirp.finalize_upload(key, data.len() as u64).await.unwrap();
}

/// Every chunk referenced by every path in the manifest must have a
/// location pointing at a pack the manifest knows about. A violation means
/// the path is listed (narinfo answers) but can never be served (NAR 404),
/// and no future drain heals it: the path dedup-skips as "already stored".
pub fn assert_all_chunks_locatable(manifest: &Manifest) {
    for (path_hash, entry) in &manifest.paths {
        for (_, node) in hestia::chunker::flatten_tree(&entry.tree) {
            if let FileSystemObject::Regular(regular) = node {
                for chunk_hash in &regular.contents.chunks {
                    let location = manifest.chunks.get(chunk_hash).unwrap_or_else(|| {
                        panic!(
                            "path {path_hash}: chunk {chunk_hash} has no location in the \
                             committed manifest (dangling reference)"
                        )
                    });
                    assert!(
                        manifest.packs.contains_key(&location.pack),
                        "path {path_hash}: chunk location points at an unknown pack"
                    );
                }
            }
        }
    }
}
