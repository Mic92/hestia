//! End-to-end tests for the write pipeline: hermetic scratch Nix stores +
//! the fake GHA backend.
//!
//! Tests skip themselves (with a notice) when nix tooling is unavailable
//! (e.g. inside the Nix build sandbox).

mod support;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use hestia::pipeline::PipelineContext;
use hestia::upstream::UpstreamFilter;

use support::common::{
    assert_all_chunks_locatable, load_snapshot, path_hash_of, pipeline_context as context,
    to_path_set,
};
use support::fake_gha::FakeGha;
use support::store::ScratchStore;

/// The store path basename (`<hash>-<name>`).
fn fixture_name(store_path: &Path) -> String {
    store_path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

/// Number of `pack-*` entries in the fake backend.
async fn pack_count(fake: &FakeGha, http: &reqwest::Client) -> usize {
    fake.rest(http)
        .list_caches("pack-")
        .await
        .expect("listing packs failed")
        .len()
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

    let stats = ctx
        .run(to_path_set(&[&fixture, &top, &dep]), BTreeSet::new())
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
    assert!(stats.head.is_some());

    let snapshot = load_snapshot(&fake, &http).await;
    assert_eq!(snapshot.path_count(), 3);

    // The fixture entry's NAR hash/size match nix's record (this is what
    // narinfo responses serve).
    let fixture_entry = snapshot.lookup(&path_hash_of(&fixture)).unwrap();
    assert_eq!(fixture_entry.nar_hash, expected_hash, "nar_hash mismatch");
    assert_eq!(fixture_entry.nar_size, expected_size, "nar_size mismatch");
    assert!(fixture_entry.ca.is_some(), "added paths are CA");

    // top's entry records its reference to dep (full basename, so the
    // substituter can put it on the narinfo References line).
    let top_entry = snapshot.lookup(&path_hash_of(&top)).unwrap();
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
    assert_all_chunks_locatable(&snapshot).await;

    // Exactly one pack blob landed in the (fake) GHA cache.
    assert_eq!(pack_count(&fake, &http).await, 1);
}

#[tokio::test]
async fn closure_expansion_pushes_dependencies() {
    // Hooking only `top` must push `dep` too: dependencies never trigger
    // the post-build-hook themselves.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let (top, dep) = store.add_paths_with_reference("closure");

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());

    let stats = ctx
        .run(to_path_set(&[&top]), BTreeSet::new())
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.paths_received, 1, "only top was hooked");
    assert_eq!(stats.pushed, 2, "top and its dependency must be pushed");

    let snapshot = load_snapshot(&fake, &http).await;
    assert!(snapshot.contains(&path_hash_of(&top)));
    assert!(
        snapshot.contains(&path_hash_of(&dep)),
        "dependency must be cached even though it was never hooked"
    );
    assert_all_chunks_locatable(&snapshot).await;
}

#[tokio::test]
async fn registered_drv_is_pushed_with_its_input_closure() {
    // Eval-only jobs register .drv paths from nix-eval-jobs via
    // `hestia hook`; the drain must cache the drv plus its input closure
    // so build jobs can run `nix build /nix/store/<hash>-x.drv^*`.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let drv = store.instantiate_drv("eval-only");

    // Oracle: the drv's registered references (input drv + source).
    let references = match store
        .database()
        .query(&drv.to_string_lossy())
        .expect("store database query failed")
    {
        hestia::pathinfo::Lookup::Found(info) => info.references,
        other => panic!("drv must be valid in the store database, got {other:?}"),
    };
    assert!(
        references.len() >= 2,
        "drv must reference its input drv and input source, got {references:?}"
    );

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());

    let stats = ctx
        .run(to_path_set(&[&drv]), BTreeSet::new())
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.paths_received, 1);
    assert_eq!(stats.pushed as usize, 1 + references.len());

    let snapshot = load_snapshot(&fake, &http).await;
    assert!(snapshot.contains(&path_hash_of(&drv)));
    for reference in &references {
        let path = store.store_dir_path().join(reference.to_string());
        assert!(
            snapshot.contains(&path_hash_of(&path)),
            "input {reference} must be cached"
        );
    }
    assert_all_chunks_locatable(&snapshot).await;
}

#[tokio::test]
async fn no_closure_pushes_only_hooked_paths() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let (top, dep) = store.add_paths_with_reference("no-closure");

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = PipelineContext {
        expand_closure: false,
        ..context(&fake, &http, store.database())
    };

    let stats = ctx
        .run(to_path_set(&[&top]), BTreeSet::new())
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.pushed, 1, "only the hooked path must be pushed");

    let snapshot = load_snapshot(&fake, &http).await;
    assert!(snapshot.contains(&path_hash_of(&top)));
    assert!(
        !snapshot.contains(&path_hash_of(&dep)),
        "dependency must not be pushed with --no-closure"
    );
}

#[tokio::test]
async fn disabled_upstream_filter_caches_signed_paths() {
    // Production default: no filter, upstream-signed paths are cached too.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let signed = store.add_fixture("signed-cached", 41);
    store.sign_path(&signed, "cache.nixos.org-1");

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = PipelineContext {
        upstream: UpstreamFilter::new(Vec::new()),
        ..context(&fake, &http, store.database())
    };

    let stats = ctx
        .run(to_path_set(&[&signed]), BTreeSet::new())
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.skipped_upstream, 0);
    assert_eq!(stats.pushed, 1, "signed path must be cached");

    let snapshot = load_snapshot(&fake, &http).await;
    assert!(snapshot.contains(&path_hash_of(&signed)));
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

    let first = ctx
        .run(path_set.clone(), BTreeSet::new())
        .await
        .expect("first run failed");
    assert_eq!(first.pushed, 1);
    assert_eq!(first.packs_uploaded, 1);

    // Second run with the same path: dedup-skip, no uploads, but a head
    // that names the path again so GC keeps it.
    let second = ctx
        .run(path_set, BTreeSet::new())
        .await
        .expect("second run failed");
    assert_eq!(second.pushed, 0);
    assert_eq!(second.skipped_existing, 1);
    assert_eq!(second.packs_uploaded, 0);
    assert_eq!(second.new_chunks, 0);
    assert_eq!(second.bytes_uploaded, 0);
    assert!(second.head.is_some());

    // Still exactly one pack in the cache.
    assert_eq!(pack_count(&fake, &http).await, 1);

    let snapshot = load_snapshot(&fake, &http).await;
    assert_eq!(snapshot.path_count(), 1);
    assert!(snapshot.contains(&path_hash_of(&fixture)));
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
        .run(to_path_set(&[&signed, &local]), BTreeSet::new())
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.skipped_upstream, 1);
    assert_eq!(stats.pushed, 1);
    assert!(stats.head.is_some());

    let snapshot = load_snapshot(&fake, &http).await;
    assert!(snapshot.contains(&path_hash_of(&local)));
    assert!(!snapshot.contains(&path_hash_of(&signed)));
}

#[tokio::test]
async fn only_upstream_paths_means_nothing_is_published() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let signed = store.add_fixture("only-upstream", 19);
    store.sign_path(&signed, "cache.nixos.org-1");

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());

    let stats = ctx
        .run(to_path_set(&[&signed]), BTreeSet::new())
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.skipped_upstream, 1);
    assert_eq!(stats.pushed, 0);
    assert!(stats.head.is_none(), "nothing should be published");
    assert_eq!(load_snapshot(&fake, &http).await.path_count(), 0);
    assert_eq!(pack_count(&fake, &http).await, 0);
}

#[tokio::test]
async fn invalid_and_malformed_paths_are_skipped_without_failing_the_drain() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("good", 23);
    let database = store.database();
    let missing = format!(
        "{}/00000000000000000000000000000000-does-not-exist",
        database.store_dir()
    );

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, database);

    // One real path mixed with one unknown and one malformed path: the bad
    // ones are skipped, the good one still gets pushed.
    let mut paths = to_path_set(&[&fixture]);
    paths.insert(missing);
    paths.insert("/not/a/store/path".to_string());

    let stats = ctx
        .run(paths, BTreeSet::new())
        .await
        .expect("pipeline must not fail because of bad input paths");

    assert_eq!(stats.paths_received, 3);
    assert_eq!(stats.skipped_invalid, 2);
    assert_eq!(stats.pushed, 1);
    assert!(stats.head.is_some());

    let snapshot = load_snapshot(&fake, &http).await;
    assert_eq!(snapshot.path_count(), 1);
}

#[tokio::test]
async fn unchunkable_path_is_skipped_without_failing_the_drain() {
    // A path that deterministically fails chunking (here: its contents
    // vanished from disk while staying registered in the store database)
    // must not abort the whole drain: the healthy path still gets cached
    // and the drain reports success.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let good = store.add_fixture("chunkable", 29);
    let bad = store.add_fixture("unchunkable", 31);
    // Make the bad path unreadable on disk; the DB still lists it.
    std::process::Command::new("chmod")
        .arg("-R")
        .arg("u+w")
        .arg(&bad)
        .status()
        .unwrap();
    std::fs::remove_dir_all(&bad).unwrap();

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());

    let stats = ctx
        .run(to_path_set(&[&good, &bad]), BTreeSet::new())
        .await
        .expect("a per-path chunking failure must not fail the drain");

    assert_eq!(stats.failed_chunking, 1);
    assert_eq!(stats.pushed, 1);
    assert!(stats.head.is_some());

    let snapshot = load_snapshot(&fake, &http).await;
    assert!(snapshot.contains(&path_hash_of(&good)));
    assert!(!snapshot.contains(&path_hash_of(&bad)));
}

#[tokio::test]
async fn identical_content_across_paths_shares_chunks() {
    // Chunk-level dedup across drains: a rebuild of a path (same name,
    // mostly the same content) must not store the shared chunks again.
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let path_a = store.add_fixture("twin", 37);
    let rebuilt = tempfile::tempdir().unwrap();
    let source = store.write_fixture(rebuilt.path(), "twin", 37);
    std::fs::write(source.join("extra"), b"changed").unwrap();
    let path_b = store.add_path(&source);
    assert_ne!(path_a, path_b, "paths must differ");

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = context(&fake, &http, store.database());

    // Push A first, then B: B's blob chunks must all dedup against A's.
    let first = ctx
        .run(to_path_set(&[&path_a]), BTreeSet::new())
        .await
        .unwrap();
    let second = ctx
        .run(to_path_set(&[&path_b]), BTreeSet::new())
        .await
        .unwrap();

    assert_eq!(first.pushed, 1);
    assert_eq!(second.pushed, 1);
    assert!(
        second.new_chunks < first.new_chunks,
        "the rebuild must reuse the first build's blob chunks \
         (first: {} chunks, second: {} chunks)",
        first.new_chunks,
        second.new_chunks
    );

    let snapshot = load_snapshot(&fake, &http).await;
    assert_eq!(snapshot.path_count(), 2);
    assert_all_chunks_locatable(&snapshot).await;
}

#[tokio::test]
async fn small_pack_target_splits_a_drain_into_multiple_packs() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    // ~600 KB of pseudo-random data -> several FastCDC chunks.
    let fixture = store.add_fixture("multipack", 7);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let ctx = PipelineContext {
        // Every chunk exceeds the target, so every chunk seals its own pack.
        pack_target_size: 1,
        ..context(&fake, &http, store.database())
    };

    let stats = ctx
        .run(to_path_set(&[&fixture]), BTreeSet::new())
        .await
        .expect("pipeline run failed");

    assert_eq!(stats.pushed, 1);
    assert!(
        stats.packs_uploaded >= 2,
        "expected multiple packs, got {}",
        stats.packs_uploaded
    );
    assert_eq!(stats.packs_uploaded, pack_count(&fake, &http).await);

    // Every chunk must remain locatable across the pack split.
    let snapshot = load_snapshot(&fake, &http).await;
    assert_eq!(snapshot.pack_hashes().len(), stats.packs_uploaded);
    assert_all_chunks_locatable(&snapshot).await;
}

/// A read-only runtime token skips the write pipeline entirely: nothing is
/// reserved, uploaded or published. The drain
/// still succeeds so the post-step never marks the job failed.
#[tokio::test]
async fn read_only_token_skips_the_write_pipeline() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("read-only", 3);

    let fake = FakeGha::start().await;
    fake.deny_writes();
    let http = reqwest::Client::new();
    let ctx = PipelineContext {
        read_only: Arc::new(AtomicBool::new(true)),
        ..context(&fake, &http, store.database())
    };

    let stats = ctx
        .run(to_path_set(&[&fixture]), BTreeSet::new())
        .await
        .expect("read-only drain must succeed");

    assert_eq!(stats.paths_received, 1);
    assert_eq!(stats.pushed, 0);
    assert_eq!(stats.packs_uploaded, 0);
    assert!(stats.head.is_none());
    assert_eq!(pack_count(&fake, &http).await, 0);
    assert_eq!(load_snapshot(&fake, &http).await.path_count(), 0);
}

/// The startup probe reports a read-only token against a backend that
/// denies writes, and a writable one otherwise.
#[tokio::test]
async fn write_probe_detects_a_read_only_token() {
    let http = reqwest::Client::new();

    let writable = FakeGha::start().await;
    assert!(
        writable.twirp(&http).probe_writable().await.unwrap(),
        "a normal backend must probe as writable"
    );

    let read_only = FakeGha::start().await;
    read_only.deny_writes();
    assert!(
        !read_only.twirp(&http).probe_writable().await.unwrap(),
        "a write-denying backend must probe as read-only"
    );
}
