//! End-to-end GC over the segmented store against the fake GHA backend.
//! Paths are fabricated (see `support::sim`), everything else is
//! production code, and the substituter verifies live paths stay readable.

mod support;

use std::future::Future;
use std::time::Duration;

use hestia::gc::{Error as GcError, GcPolicy, SECS_PER_DAY, SECS_PER_HOUR};
use hestia::store::Heads;

use support::fake_gha::FakeGha;
use support::sim::{SimCache, SimPath, last_accessed, one_byte_reads};

const T0: u64 = 1_750_000_000;
const DAY: u64 = SECS_PER_DAY;
const HOUR: u64 = SECS_PER_HOUR;
const ROOT: &str = "main-x86_64-linux";

async fn timed<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(120), future)
        .await
        .expect("test timed out")
}

async fn setup() -> (FakeGha, SimCache) {
    let fake = FakeGha::start().await;
    fake.set_clock(T0);
    let sim = SimCache::new(&fake, &reqwest::Client::new());
    (fake, sim)
}

#[tokio::test]
async fn compacts_each_root_to_one_segment_and_folds_heads() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 200_000);
        let b = SimPath::new("b", 2, 200_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        sim.push(ROOT, &[&b], &[&a, &b]).await;
        sim.push("other-x86_64-linux", &[&b], &[&b]).await;
        assert_eq!(
            sim.stored_keys("h-").await.len() + sim.stored_keys("c-").await.len(),
            3
        );

        fake.set_clock(T0 + 2 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;
        assert_eq!(
            (stats.epoch, stats.roots, stats.segments_written),
            (1, 2, 1)
        );
        assert_eq!(stats.deleted, 3, "just the heads");

        let heads = Heads::load(&sim.backend).await.unwrap();
        assert_eq!(heads.view.epoch, 1);
        assert!(heads.view.roots.values().all(|s| s.len() == 1));
        assert!(sim.stored_keys("h-").await.is_empty());
        sim.assert_readable(&[&a, &b]).await;
        // Inputs are retired, not deleted: a reader may still hold them.
        assert_eq!(sim.stored_keys("seg-").await.len(), 2 * 4);

        fake.set_clock(T0 + 4 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 4 * HOUR).await;
        assert_eq!(
            stats.deleted,
            2 * 2 + 1,
            "two segments and the previous g-*"
        );
        assert_eq!(sim.stored_keys("seg-").await.len(), 2 * 2);
        sim.assert_readable(&[&a, &b]).await;
        sim.assert_no_dangling_references().await;
    })
    .await;
}

#[tokio::test]
async fn refuses_to_run_right_after_the_previous_gc() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 50_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        fake.set_clock(T0 + HOUR);
        sim.run_gc(GcPolicy::default(), T0 + HOUR).await;
        let err = sim
            .gc(GcPolicy::default())
            .run(T0 + HOUR + 120)
            .await
            .unwrap_err();
        assert!(matches!(err, GcError::TooSoon(..)), "{err}");
    })
    .await;
}

#[tokio::test]
async fn dry_run_changes_nothing() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 200_000);
        let b = SimPath::new("b", 2, 200_000);
        sim.push(ROOT, &[&a, &b], &[&a, &b]).await;
        sim.push(ROOT, &[], &[&a]).await;
        let before = sim.stored_keys("").await;
        fake.set_clock(T0 + 2 * HOUR);
        let mut gc = sim.gc(GcPolicy::default());
        gc.dry_run = true;
        let stats = gc.run(T0 + 2 * HOUR).await.unwrap();
        assert_eq!(stats.segments_written, 1);
        assert_eq!(sim.stored_keys("").await, before);
    })
    .await;
}

/// The newest drains name what the root keeps. A path no drain since the
/// last GC mentioned is dropped, its pack repacked, and the old pack swept
/// by the run after.
#[tokio::test]
async fn dropped_path_gets_repacked_away() {
    timed(async {
        let (fake, sim) = setup().await;
        let keep = SimPath::new("keep", 1, 200_000);
        let drop = SimPath::new("drop", 2, 600_000);
        sim.push(ROOT, &[&keep, &drop], &[&keep, &drop]).await;
        assert_eq!(sim.stored_keys("pack-").await.len(), 1);
        let both = sim.stored_pack_bytes().await;

        sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;
        fake.set_clock(T0 + DAY);
        sim.push(ROOT, &[], &[&keep]).await;
        fake.set_clock(T0 + 2 * DAY);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 2 * DAY).await;
        assert_eq!((stats.paths_dropped, stats.packs_repacked), (0, 1));
        assert_eq!(
            sim.stored_keys("pack-").await.len(),
            2,
            "old pack stays one epoch"
        );
        sim.assert_readable(&[&keep]).await;
        sim.assert_unavailable(&[&drop]).await;

        fake.set_clock(T0 + 3 * DAY);
        sim.run_gc(GcPolicy::default(), T0 + 3 * DAY).await;
        assert_eq!(sim.stored_keys("pack-").await.len(), 1);
        assert!(sim.stored_pack_bytes().await < both / 2);
        assert_eq!(sim.stored_keys("idx-").await.len(), 1);
        sim.assert_readable(&[&keep]).await;
        sim.assert_no_dangling_references().await;
    })
    .await;
}

#[tokio::test]
async fn stale_root_expires_and_its_storage_is_swept() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 100_000);
        let f = SimPath::new("f", 2, 100_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        sim.push("feature-x86_64-linux", &[&f], &[&f]).await;
        let policy = GcPolicy {
            root_ttl: 3 * DAY,
            ..GcPolicy::default()
        };
        sim.run_gc(policy.clone(), T0 + 2 * HOUR).await;
        for day in 1..=5 {
            let now = T0 + day * DAY;
            fake.set_clock(now);
            sim.push(ROOT, &[], &[&a]).await;
            sim.run_gc(policy.clone(), now + HOUR).await;
        }
        let heads = Heads::load(&sim.backend).await.unwrap();
        assert_eq!(heads.view.roots.keys().collect::<Vec<_>>(), [ROOT]);
        assert_eq!(sim.stored_keys("pack-").await.len(), 1);
        sim.assert_readable(&[&a]).await;
        sim.assert_unavailable(&[&f]).await;
    })
    .await;
}

#[tokio::test]
async fn evicted_pack_drops_its_paths_and_a_repush_restores_them() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 100_000);
        let b = SimPath::new("b", 2, 100_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        sim.push(ROOT, &[&b], &[&a, &b]).await;
        let packs: Vec<String> = sim.stored_keys("pack-").await.into_iter().collect();
        assert_eq!(packs.len(), 2);
        // Evict a's pack behind hestia's back.
        let a_pack = {
            let snap = sim.snapshot().await;
            let r = snap.resolve(&a.path_hash()).await.unwrap().unwrap();
            hestia::chunker::pack_cache_key(&r.map.chunks.values().next().unwrap().pack)
        };
        sim.rest.delete_by_key(&a_pack).await.unwrap();

        let stats = sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;
        assert_eq!((stats.packs_evicted, stats.paths_dropped), (1, 1));
        sim.assert_unavailable(&[&a]).await;
        sim.assert_readable(&[&b]).await;

        fake.set_clock(T0 + 3 * HOUR);
        sim.push(ROOT, &[&a], &[&a, &b]).await;
        sim.assert_readable(&[&a, &b]).await;
    })
    .await;
}

#[tokio::test]
async fn orphan_pack_is_swept_only_once_old_enough() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 50_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        let orphan = sim.upload_orphan_pack(99);
        let orphan = orphan.await;

        let stats = sim.run_gc(GcPolicy::default(), T0 + 10).await;
        assert_eq!(
            stats.deleted, 1,
            "just the head: the orphan is younger than min_age"
        );
        assert!(sim.stored_keys("pack-").await.contains(&orphan));

        fake.set_clock(T0 + 3 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 3 * HOUR).await;
        assert_eq!(stats.deleted, 3, "orphan pack, its index, the previous g-*");
        assert!(!sim.stored_keys("pack-").await.contains(&orphan));
        sim.assert_readable(&[&a]).await;
    })
    .await;
}

#[tokio::test]
async fn idle_live_packs_are_touched() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 50_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        let pack = sim.stored_keys("pack-").await.into_iter().next().unwrap();
        let now = T0 + 5 * DAY;
        fake.set_clock(now);
        let stats = sim.run_gc(GcPolicy::default(), now).await;
        assert_eq!(stats.touched, 1);
        assert_eq!(one_byte_reads(&fake, &pack), 1);
        assert!(last_accessed(&sim.rest, &pack).await.unwrap() >= now);
    })
    .await;
}

/// A drain that read the view before GC published and publishes after it
/// (base_epoch = epoch-1) is still picked up by the next run.
#[tokio::test]
async fn drain_racing_gc_is_kept() {
    timed(async {
        let (fake, sim) = setup().await;
        let a = SimPath::new("a", 1, 50_000);
        let b = SimPath::new("b", 2, 50_000);
        sim.push(ROOT, &[&a], &[&a]).await;
        let stale_view = sim.snapshot().await;
        sim.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;

        // Publish b against the pre-GC view.
        fake.set_clock(T0 + 3 * HOUR);
        let mut writer = hestia::segment::SegmentWriter::default();
        let (tree, chunks) = b.chunked();
        let mut builder = hestia::chunker::PackBuilder::new();
        for c in &chunks {
            builder.add(c).unwrap();
        }
        let pack = builder.finish();
        hestia::pipeline::upload_pack(&sim.backend, &pack)
            .await
            .unwrap();
        let mut known = hestia::store::KnownChunks::default();
        known.add(pack.hash, &pack.index());
        let entry = hestia::manifest::PathEntry {
            store_path: b.store_path(),
            nar_hash: hestia::manifest::NarHash::digest(b"unused"),
            nar_size: 1,
            references: vec![],
            ca: None,
            deriver: None,
            tree,
        };
        hestia::store::push_entry(&mut writer, &entry, &known).unwrap();
        stale_view
            .copy_entry(&a.path_hash(), &mut writer)
            .await
            .unwrap();
        let sealed = writer.seal().unwrap();
        hestia::store::publish(&sim.backend, &stale_view.view, ROOT, &sealed)
            .await
            .unwrap();

        fake.set_clock(T0 + DAY);
        sim.run_gc(GcPolicy::default(), T0 + DAY).await;
        let snap = sim.snapshot().await;
        assert!(snap.contains(&a.path_hash()) && snap.contains(&b.path_hash()));
        fake.set_clock(T0 + 2 * DAY);
        sim.run_gc(GcPolicy::default(), T0 + 2 * DAY).await;
        sim.assert_no_dangling_references().await;
        sim.assert_readable(&[&a]).await;
    })
    .await;
}

#[tokio::test]
async fn thirty_day_history_converges_to_live_set_storage() {
    timed(async {
        let (fake, sim) = setup().await;
        let policy = GcPolicy::default();
        let base: Vec<SimPath> = (0..10)
            .map(|i| SimPath::new(&format!("base-{i}"), 1000 + i, 100_000))
            .collect();
        let mut generation = 0u64;
        let mut apps: Vec<SimPath> = (0..10)
            .map(|i| SimPath::new(&format!("app-gen0-{i}"), 2000 + i, 20_000))
            .collect();
        let mut dailies: Vec<SimPath> = Vec::new();
        let feature: Vec<SimPath> = (0..2)
            .map(|i| SimPath::new(&format!("feature-{i}"), 3000 + i, 20_000))
            .collect();

        for day in 0..30u64 {
            let now = T0 + day * DAY;
            fake.set_clock(now);
            if day > 0 && day % 7 == 0 {
                generation += 1;
                apps = (0..10)
                    .map(|i| {
                        SimPath::new(
                            &format!("app-gen{generation}-{i}"),
                            2000 + generation * 100 + i,
                            20_000,
                        )
                    })
                    .collect();
            }
            for i in 0..2u64 {
                dailies.push(SimPath::new(
                    &format!("daily-{day}-{i}"),
                    4000 + day * 10 + i,
                    10_000,
                ));
            }
            while dailies.len() > 6 {
                dailies.remove(0);
            }
            let closure: Vec<SimPath> = base
                .iter()
                .chain(apps.iter())
                .chain(dailies.iter())
                .cloned()
                .collect();
            let refs: Vec<&SimPath> = closure.iter().collect();
            sim.push(ROOT, &refs, &refs).await;
            if day == 3 {
                let refs: Vec<&SimPath> = feature.iter().collect();
                sim.push("feature-x86_64-linux", &refs, &refs).await;
            }

            fake.set_clock(now + 2 * HOUR);
            sim.run_gc(policy.clone(), now + 2 * HOUR).await;
            sim.assert_no_dangling_references().await;
        }

        let closure: Vec<&SimPath> = base
            .iter()
            .chain(apps.iter())
            .chain(dailies.iter())
            .collect();
        sim.assert_readable(&closure).await;
        sim.assert_unavailable(&feature.iter().collect::<Vec<_>>())
            .await;
        let live = sim.live_chunk_bytes().await;
        let stored = sim.stored_pack_bytes().await;
        assert!(
            stored <= live * 2,
            "storage {stored} should converge towards the live set {live}"
        );
        let heads = Heads::load(&sim.backend).await.unwrap();
        assert_eq!(heads.view.roots.len(), 1);
        assert_eq!(sim.stored_keys("g-").await.len(), 1);
    })
    .await;
}

#[tokio::test]
async fn drain_compaction_folds_pending_segments_and_gc_folds_it() {
    timed(async {
        let (fake, sim) = setup().await;
        let paths: Vec<SimPath> = (0..5u64)
            .map(|i| SimPath::new(&format!("p{i}"), 10 + i, 50_000))
            .collect();
        // The first drain of an unknown root is a c-* itself, the rest h-*.
        for p in &paths {
            sim.push(ROOT, &[p], &[p]).await;
        }
        let refs: Vec<&SimPath> = paths.iter().collect();

        let snapshot = sim.snapshot().await;
        assert_eq!(snapshot.view.roots[ROOT].len(), 5);
        assert_eq!(
            snapshot.maybe_compact(ROOT, T0, 0.0).await.unwrap(),
            None,
            "the root's own c-* is younger than the window"
        );
        assert_eq!(
            snapshot.maybe_compact(ROOT, T0 + 200, 0.9).await.unwrap(),
            None,
            "five drains in ~3 windows: a 0.9 coin loses"
        );
        let name = snapshot
            .maybe_compact(ROOT, T0 + 200, 0.0)
            .await
            .unwrap()
            .expect("elected");
        assert!(name.starts_with("c-"));

        let after = sim.snapshot().await;
        assert_eq!(after.view.roots[ROOT].len(), 1, "{:?}", after.view);
        assert_eq!(after.view.heads.len(), 1);
        sim.assert_readable(&refs).await;
        assert_eq!(sim.stored_keys("seg-").await.len(), 12, "nothing deleted");

        fake.set_clock(T0 + 3 * HOUR);
        let stats = sim.run_gc(GcPolicy::default(), T0 + 3 * HOUR).await;
        assert_eq!((stats.roots, stats.segments_written), (1, 0), "{stats:?}");
        let after = sim.snapshot().await;
        assert!(after.view.heads.is_empty());
        assert_eq!(after.view.roots[ROOT].len(), 1);
        sim.assert_readable(&refs).await;
        assert_eq!(sim.stored_keys("seg-").await.len(), 2, "inputs swept");
    })
    .await;
}
