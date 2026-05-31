//! End-to-end tests for the write pipeline: hermetic scratch Nix stores +
//! the fake GHA backend.
//!
//! Tests skip themselves (with a notice) when nix tooling is unavailable
//! (e.g. inside the Nix build sandbox).

mod support;

use std::collections::BTreeSet;
use std::path::Path;

use hestia::gha::savemutable::SaveMutable;
use hestia::manifest::{FileSystemObject, Manifest, PathHash};
use hestia::pathinfo::StoreDatabase;
use hestia::pipeline::{AccessLog, MANIFEST_PREFIX, PipelineContext, now_unix};
use hestia::upstream::UpstreamFilter;

use support::fake_gha::FakeGha;
use support::store::ScratchStore;

const TEST_ROOT_KEY: &str = "main-test-system";

fn context(fake: &FakeGha, http: &reqwest::Client, store: StoreDatabase) -> PipelineContext {
    PipelineContext {
        twirp: fake.twirp(http),
        http: http.clone(),
        store,
        // Scratch store paths are unsigned, so the default filter (which
        // would skip cache.nixos.org-signed paths) lets them through --
        // exactly like locally built paths in production.
        upstream: UpstreamFilter::default(),
        root_key: TEST_ROOT_KEY.to_string(),
        manifest_prefix: MANIFEST_PREFIX.to_string(),
    }
}

/// Load the committed manifest from the fake backend, or None if no version
/// was ever committed.
async fn committed_manifest(ctx: &PipelineContext) -> Option<(u64, Manifest)> {
    let save = SaveMutable::new(&ctx.twirp, &ctx.http, &ctx.manifest_prefix);
    let entry = save.load().await.expect("loading manifest failed")?;
    let manifest = Manifest::decode(&entry.data).expect("manifest must decode");
    Some((entry.index, manifest))
}

/// The store path basename (`<hash>-<name>`).
fn fixture_name(store_path: &Path) -> String {
    store_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

/// The manifest path key for a store path.
fn path_hash_of(store_path: &Path) -> PathHash {
    let name = store_path.file_name().unwrap().to_str().unwrap();
    name[..32]
        .parse()
        .expect("store path basename starts with its hash")
}

/// Number of `pack-*` entries in the fake backend.
async fn pack_count(fake: &FakeGha, http: &reqwest::Client) -> usize {
    fake.rest(http)
        .list_caches("pack-")
        .await
        .expect("listing packs failed")
        .len()
}

/// Every chunk referenced by every path in the manifest must have a
/// location pointing at a pack the manifest knows about.
fn assert_all_chunks_locatable(manifest: &Manifest) {
    for entry in manifest.paths.values() {
        for (_, node) in hestia::chunker::flatten_tree(&entry.tree) {
            if let FileSystemObject::Regular(regular) = node {
                for chunk_hash in &regular.contents.chunks {
                    let location = manifest
                        .chunks
                        .get(chunk_hash)
                        .expect("tree chunk must have a location");
                    assert!(
                        manifest.packs.contains_key(&location.pack),
                        "chunk location must point at a known pack"
                    );
                }
            }
        }
    }
}

fn to_path_set(paths: &[&Path]) -> BTreeSet<String> {
    paths
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
}

#[tokio::test]
async fn pushes_paths_end_to_end() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    // A multi-chunk fixture plus a pair of small paths with a reference
    // between them: covers chunking, packing, and reference recording.
    let fixture = store.add_fixture("e2e", 7);
    let (top, dep) = store.add_paths_with_reference("e2e");
    let (expected_hash, expected_size) = store
        .nar_hash_oracle(&fixture)
        .expect("nix path-info oracle unavailable");

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());

    let now = now_unix();
    let stats = ctx
        .run(to_path_set(&[&fixture, &top, &dep]), BTreeSet::new(), now)
        .await
        .expect("pipeline run failed");

    // Stats: three new paths, one new pack, nothing skipped.
    assert_eq!(stats.paths_received, 3);
    assert_eq!(stats.pushed, 3);
    assert_eq!(stats.skipped_existing, 0);
    assert_eq!(stats.skipped_upstream, 0);
    assert_eq!(stats.skipped_invalid, 0);
    assert_eq!(stats.failed_verification, 0);
    assert_eq!(stats.packs_uploaded, 1);
    assert!(stats.new_chunks > 0);
    assert!(stats.bytes_uploaded > 0);
    assert_eq!(stats.manifest_version, 1);

    // The committed manifest is correct.
    let (version, manifest) = committed_manifest(&ctx).await.expect("manifest committed");
    assert_eq!(version, 1);
    assert_eq!(manifest.paths.len(), 3);

    // The fixture entry's NAR hash/size match nix's record (this is what
    // narinfo responses will serve in Phase 4).
    let fixture_entry = &manifest.paths[&path_hash_of(&fixture)];
    assert_eq!(fixture_entry.nar_hash, expected_hash, "nar_hash mismatch");
    assert_eq!(fixture_entry.nar_size, expected_size, "nar_size mismatch");
    assert!(fixture_entry.ca.is_some(), "added paths are CA");
    assert_eq!(fixture_entry.last_pushed, now);

    // top's entry records its reference to dep (full basename, so the
    // substituter can put it on the narinfo References line).
    let top_entry = &manifest.paths[&path_hash_of(&top)];
    assert_eq!(
        top_entry.store_path.to_string(),
        fixture_name(&top),
        "entry must record its own full basename"
    );
    let reference_names: Vec<String> = top_entry
        .references
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(reference_names, vec![fixture_name(&dep)]);

    // All chunks of all paths are locatable in uploaded packs.
    assert_all_chunks_locatable(&manifest);

    // The root for this branch+system pins all three paths.
    let root = manifest.roots.get(TEST_ROOT_KEY).expect("root must exist");
    for path in [&fixture, &top, &dep] {
        assert!(root.paths.contains(&path_hash_of(path)));
    }
    assert_eq!(root.updated, now);

    // Exactly one pack blob landed in the (fake) GHA cache.
    assert_eq!(pack_count(&fake, &http).await, 1);
}

#[tokio::test]
async fn second_run_dedups_and_uploads_nothing() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("dedup", 11);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());
    let path_set = to_path_set(&[&fixture]);

    let first_now = now_unix();
    let first = ctx
        .run(path_set.clone(), BTreeSet::new(), first_now)
        .await
        .expect("first run failed");
    assert_eq!(first.pushed, 1);
    assert_eq!(first.packs_uploaded, 1);

    // Second run with the same path: dedup-skip, no uploads, but the
    // manifest gets a new version with a bumped last_pushed clock.
    let second_now = first_now + 100;
    let second = ctx
        .run(path_set, BTreeSet::new(), second_now)
        .await
        .expect("second run failed");
    assert_eq!(second.pushed, 0);
    assert_eq!(second.skipped_existing, 1);
    assert_eq!(second.packs_uploaded, 0);
    assert_eq!(second.new_chunks, 0);
    assert_eq!(second.bytes_uploaded, 0);
    assert_eq!(second.manifest_version, 2);

    // Still exactly one pack in the cache.
    assert_eq!(pack_count(&fake, &http).await, 1);

    // The path entry survived with its push clock bumped, and stays in
    // the root (dedup-skipped paths remain pinned).
    let (_, manifest) = committed_manifest(&ctx).await.unwrap();
    let hash = path_hash_of(&fixture);
    assert_eq!(manifest.paths.len(), 1);
    assert_eq!(manifest.paths[&hash].last_pushed, second_now);
    assert!(manifest.roots[TEST_ROOT_KEY].paths.contains(&hash));
}

#[tokio::test]
async fn upstream_signed_path_is_skipped() {
    // Hermetic upstream-filter test: a path signed with a key named like
    // cache.nixos.org's must be skipped; an unsigned path pushed alongside
    // it must still go through.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let signed = store.add_fixture("upstream", 13);
    let local = store.add_fixture("local", 17);
    store.sign_path(&signed, "cache.nixos.org-1");

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());

    let stats = ctx
        .run(to_path_set(&[&signed, &local]), BTreeSet::new(), now_unix())
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.skipped_upstream, 1);
    assert_eq!(stats.pushed, 1);
    assert_eq!(stats.manifest_version, 1);

    // Only the local path made it into the manifest and the root.
    let (_, manifest) = committed_manifest(&ctx).await.unwrap();
    assert!(manifest.paths.contains_key(&path_hash_of(&local)));
    assert!(!manifest.paths.contains_key(&path_hash_of(&signed)));
    let root = &manifest.roots[TEST_ROOT_KEY];
    assert!(root.paths.contains(&path_hash_of(&local)));
    assert!(
        !root.paths.contains(&path_hash_of(&signed)),
        "upstream paths must not be pinned by our roots"
    );
}

#[tokio::test]
async fn only_upstream_paths_means_nothing_is_committed() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let signed = store.add_fixture("only-upstream", 19);
    store.sign_path(&signed, "cache.nixos.org-1");

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());

    let stats = ctx
        .run(to_path_set(&[&signed]), BTreeSet::new(), now_unix())
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.skipped_upstream, 1);
    assert_eq!(stats.pushed, 0);
    assert_eq!(stats.manifest_version, 0, "nothing should be committed");
    assert!(committed_manifest(&ctx).await.is_none());
    assert_eq!(pack_count(&fake, &http).await, 0);
}

#[tokio::test]
async fn invalid_and_malformed_paths_are_skipped_without_failing_the_drain() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("good", 23);
    let database = store.database();
    let unknown = format!(
        "{}/00000000000000000000000000000000-does-not-exist",
        database.store_dir()
    );

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, database);

    // One real path mixed with one unknown and one malformed path: the bad
    // ones are skipped, the good one still gets pushed.
    let mut paths = to_path_set(&[&fixture]);
    paths.insert(unknown);
    paths.insert("/not/a/store/path".to_string());

    let stats = ctx
        .run(paths, BTreeSet::new(), now_unix())
        .await
        .expect("pipeline must not fail because of bad input paths");

    assert_eq!(stats.paths_received, 3);
    assert_eq!(stats.skipped_invalid, 2);
    assert_eq!(stats.pushed, 1);
    assert_eq!(stats.manifest_version, 1);

    let (_, manifest) = committed_manifest(&ctx).await.unwrap();
    assert_eq!(manifest.paths.len(), 1);
}

#[tokio::test]
async fn accessed_paths_join_the_root_without_store_queries() {
    // The AccessLog interface (substituter integration, Phase 4): accessed
    // paths must end up in the root even though they are never queried,
    // chunked, or uploaded. Needs no Nix store at all.
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(
        &fake,
        &http,
        // Never opened: no buffered paths means no queries.
        StoreDatabase::new("/nonexistent/db.sqlite"),
    );

    let access_log = AccessLog::new();
    let accessed_hash: PathHash = "76yk8b7ny30zl1wsq2vd66j9vrcgrkah".parse().unwrap();
    access_log.record(accessed_hash);

    let now = now_unix();
    let stats = ctx
        .run(BTreeSet::new(), access_log.snapshot(), now)
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.pushed, 0);
    assert_eq!(stats.packs_uploaded, 0);
    assert_eq!(stats.manifest_version, 1, "root-only update still commits");

    let (_, manifest) = committed_manifest(&ctx).await.unwrap();
    let root = &manifest.roots[TEST_ROOT_KEY];
    assert!(root.paths.contains(&accessed_hash));
    assert_eq!(root.updated, now);
    assert!(manifest.paths.is_empty());
    assert!(manifest.packs.is_empty());
}

#[tokio::test]
async fn concurrent_pipelines_keep_both_paths() {
    // SaveMutable conflict handling: two pipelines (e.g. matrix jobs)
    // committing concurrently against the same cache must not lose each
    // other's paths.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let path_a = store.add_fixture("concurrent-a", 29);
    let path_b = store.add_fixture("concurrent-b", 31);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    // Two independent contexts (separate clients), same backend.
    let ctx_a = context(&fake, &http, store.database());
    let ctx_b = context(&fake, &http, store.database());

    let now = now_unix();
    let (stats_a, stats_b) = tokio::join!(
        ctx_a.run(to_path_set(&[&path_a]), BTreeSet::new(), now),
        ctx_b.run(to_path_set(&[&path_b]), BTreeSet::new(), now),
    );
    let stats_a = stats_a.expect("pipeline A failed");
    let stats_b = stats_b.expect("pipeline B failed");

    assert_eq!(stats_a.pushed, 1);
    assert_eq!(stats_b.pushed, 1);

    // Exactly one of them got version 1; the other re-merged onto version 2.
    let mut versions = [stats_a.manifest_version, stats_b.manifest_version];
    versions.sort();
    assert_eq!(versions, [1, 2]);

    // The final manifest contains BOTH paths with all chunks locatable.
    let (version, manifest) = committed_manifest(&ctx_a).await.unwrap();
    assert_eq!(version, 2);
    let hash_a = path_hash_of(&path_a);
    let hash_b = path_hash_of(&path_b);
    assert!(
        manifest.paths.contains_key(&hash_a),
        "path A lost in the merge"
    );
    assert!(
        manifest.paths.contains_key(&hash_b),
        "path B lost in the merge"
    );
    assert_all_chunks_locatable(&manifest);

    // Both packs were uploaded (different content, different hashes).
    assert_eq!(pack_count(&fake, &http).await, 2);

    // Roots merged: concurrent updates within the union window keep both.
    let root = &manifest.roots[TEST_ROOT_KEY];
    assert!(root.paths.contains(&hash_a) && root.paths.contains(&hash_b));
}

#[tokio::test]
async fn identical_content_across_paths_shares_chunks() {
    // Chunk-level dedup across store paths: two different paths with the
    // same blob content must not store the blob twice.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    // Same seed -> same blob content, but different names -> different paths.
    let path_a = store.add_fixture("twin-a", 37);
    let path_b = store.add_fixture("twin-b", 37);
    assert_ne!(path_a, path_b, "paths must differ");

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());

    // Push A first, then B: B's blob chunks must all dedup against A's.
    let first = ctx
        .run(to_path_set(&[&path_a]), BTreeSet::new(), now_unix())
        .await
        .unwrap();
    let second = ctx
        .run(to_path_set(&[&path_b]), BTreeSet::new(), now_unix())
        .await
        .unwrap();

    assert_eq!(first.pushed, 1);
    assert_eq!(second.pushed, 1);
    assert!(
        second.new_chunks < first.new_chunks,
        "second path must reuse the first path's blob chunks \
         (first: {} chunks, second: {} chunks)",
        first.new_chunks,
        second.new_chunks
    );

    let (_, manifest) = committed_manifest(&ctx).await.unwrap();
    assert_eq!(manifest.paths.len(), 2);
    assert_all_chunks_locatable(&manifest);
}
