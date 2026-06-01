//! End-to-end GC tests against the fake GHA backend.
//!
//! Paths are fabricated (no Nix store needed — see `support::sim`), but
//! everything else is production code: chunker, pack builder, uploads,
//! SaveMutable manifest commits, the GC planner/executor, and the
//! substituter used to verify that live paths stay readable.

mod support;

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use hestia::chunker::pack_cache_key;
use hestia::gc::{GcPolicy, RepackOutput, SECS_PER_DAY, SECS_PER_HOUR, TIER_STABLE};
use hestia::manifest::{FileSystemObject, Manifest, PackHash};

use support::fake_gha::FakeGha;
use support::sim::{SimCache, SimPath, last_accessed, one_byte_reads};

/// Reference start time for all simulated histories.
const T0: u64 = 1_750_000_000;
const DAY: u64 = SECS_PER_DAY;
const HOUR: u64 = SECS_PER_HOUR;

/// Hard timeout for every test body (PLAN.md Decision 16: a hung
/// server must fail the test, not the suite).
const TEST_TIMEOUT: Duration = Duration::from_secs(120);

async fn timed<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .expect("test timed out: deadlock or hung server")
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

/// Chunk locations of one path in the manifest.
fn chunk_locations_of(manifest: &Manifest, path: &SimPath) -> Vec<hestia::manifest::ChunkLocation> {
    let entry = &manifest.paths[&path.path_hash()];
    hestia::chunker::flatten_tree(&entry.tree)
        .into_iter()
        .filter_map(|(_, node)| match node {
            FileSystemObject::Regular(regular) => Some(regular.contents.chunks.clone()),
            _ => None,
        })
        .flatten()
        .map(|chunk| manifest.chunks[&chunk].clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Dry run
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dry_run_plans_but_changes_nothing() {
    timed(async {
        let fake = FakeGha::start().await;
        let http = client();
        let sim = SimCache::new(&fake, &http);

        let big = SimPath::new("big-app", 1, 200_000);
        let small = SimPath::new("small-lib", 2, 40_000);
        fake.set_clock(T0);
        sim.push(
            "main",
            &[big.clone(), small.clone()],
            &[big.clone(), small.clone()],
            T0,
        )
        .await;

        // 20 days later only `small` is still in the closure.
        let t1 = T0 + 20 * DAY;
        fake.set_clock(t1);
        sim.push("main", &[], std::slice::from_ref(&small), t1)
            .await;

        let manifest_before = sim.manifest().await;
        let packs_before = sim.stored_pack_keys().await;

        let gc = sim.gc(GcPolicy::default());
        let report = gc.run(true, t1).await.expect("dry run failed");

        // The plan sees the work...
        assert_eq!(report.plan.drop_paths, vec![big.path_hash()]);
        assert_eq!(report.plan.repack_jobs.len(), 1);
        assert!(!report.plan.delete_packs.is_empty());

        // ...but nothing changed.
        assert_eq!(sim.manifest().await, manifest_before);
        assert_eq!(sim.stored_pack_keys().await, packs_before);
        assert_eq!(report.packs_uploaded, 0);
        assert_eq!(report.packs_deleted, 0);
    })
    .await;
}

// ---------------------------------------------------------------------------
// Repack
// ---------------------------------------------------------------------------

#[tokio::test]
async fn repack_copies_live_chunks_and_deletes_old_pack_after_commit() {
    timed(async {
        let fake = FakeGha::start().await;
        let http = client();
        let sim = SimCache::new(&fake, &http);

        // One pack, ~83% of which belongs to `big`. When `big` dies the pack
        // is ~17% live -> repack.
        let big = SimPath::new("big-app", 1, 200_000);
        let small = SimPath::new("small-lib", 2, 40_000);
        fake.set_clock(T0);
        sim.push(
            "main",
            &[big.clone(), small.clone()],
            &[big.clone(), small.clone()],
            T0,
        )
        .await;

        let manifest = sim.manifest().await;
        assert_eq!(manifest.packs.len(), 1);
        let original_pack: PackHash = *manifest.packs.keys().next().unwrap();

        // 20 days later: `big` left the closure (out of grace AND PushTTL).
        let t1 = T0 + 20 * DAY;
        fake.set_clock(t1);
        sim.push("main", &[], std::slice::from_ref(&small), t1)
            .await;

        let gc = sim.gc(GcPolicy::default());
        let report = gc.run(false, t1).await.expect("gc run failed");

        // Repack happened: live chunks were downloaded (verified) and a new
        // pack was uploaded.
        assert_eq!(report.packs_uploaded, 1);
        assert!(report.bytes_downloaded > 0);
        assert!(report.bytes_uploaded > 0);
        assert!(
            report.bytes_uploaded < 100_000,
            "only small-lib's ~40 KB should be copied, not big-app's 200 KB \
             (uploaded {} bytes)",
            report.bytes_uploaded
        );
        assert_eq!(
            report.packs_deleted, 1,
            "the old pack is deleted after commit"
        );

        // Manifest: big gone, small alive with relocated chunks.
        let manifest = sim.manifest().await;
        assert!(!manifest.paths.contains_key(&big.path_hash()));
        assert!(manifest.paths.contains_key(&small.path_hash()));
        assert!(!manifest.packs.contains_key(&original_pack));
        for location in chunk_locations_of(&manifest, &small) {
            assert_ne!(location.pack, original_pack, "chunk must have moved");
            assert_eq!(location.repacks_survived, 1, "chunk survived one repack");
            assert!(manifest.packs.contains_key(&location.pack));
        }

        // Cache: old pack physically gone, new pack present.
        let stored = sim.stored_pack_keys().await;
        assert!(!stored.contains(&pack_cache_key(&original_pack)));
        sim.assert_no_dangling_pack_references().await;

        // The surviving path is still fully readable through the
        // substituter; the dead one is a clean 404.
        sim.assert_readable(&[&small]).await;
        sim.assert_unavailable(&[&big]).await;
    })
    .await;
}

// ---------------------------------------------------------------------------
// Stability tiers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chunks_surviving_two_repacks_land_in_a_stable_pack() {
    timed(async {
        let fake = FakeGha::start().await;
        let http = client();
        let sim = SimCache::new(&fake, &http);
        let gc_policy = GcPolicy::default;

        // `keeper` survives everything; `transient` survives one cycle;
        // `doomed` dies first. Sizes chosen so each death pushes the pack's
        // liveness below 0.5.
        let keeper = SimPath::new("keeper", 1, 30_000);
        let transient = SimPath::new("transient", 2, 100_000);
        let doomed = SimPath::new("doomed", 3, 300_000);

        fake.set_clock(T0);
        sim.push(
            "main",
            &[keeper.clone(), transient.clone(), doomed.clone()],
            &[keeper.clone(), transient.clone(), doomed.clone()],
            T0,
        )
        .await;

        // --- Cycle 1: doomed dies -> repack 1 -> survivors are volatile ---
        let t1 = T0 + 15 * DAY;
        fake.set_clock(t1);
        sim.push("main", &[], &[keeper.clone(), transient.clone()], t1)
            .await;
        let report = sim.gc(gc_policy()).run(false, t1).await.unwrap();
        assert_eq!(report.packs_uploaded, 1);

        let manifest = sim.manifest().await;
        for location in chunk_locations_of(&manifest, &keeper) {
            assert_eq!(location.repacks_survived, 1);
            assert_eq!(manifest.packs[&location.pack].tier, 0, "still volatile");
        }

        // --- Cycle 2: transient dies -> repack 2 -> keeper becomes stable ---
        let t2 = t1 + 15 * DAY;
        fake.set_clock(t2);
        sim.push("main", &[], std::slice::from_ref(&keeper), t2)
            .await;
        let report = sim.gc(gc_policy()).run(false, t2).await.unwrap();
        assert_eq!(report.packs_uploaded, 1);

        let manifest = sim.manifest().await;
        let mut stable_pack = None;
        for location in chunk_locations_of(&manifest, &keeper) {
            assert_eq!(location.repacks_survived, 2);
            assert_eq!(
                manifest.packs[&location.pack].tier, TIER_STABLE,
                "chunks that survived {} repacks go into a stable-tier pack",
                location.repacks_survived
            );
            stable_pack = Some(location.pack);
        }
        let stable_pack = stable_pack.expect("keeper has chunks");

        // --- Cycle 3: nothing dies -> the stable pack is only touched ---
        let t3 = t2 + 15 * DAY;
        fake.set_clock(t3);
        sim.push("main", &[], std::slice::from_ref(&keeper), t3)
            .await;
        let report = sim.gc(gc_policy()).run(false, t3).await.unwrap();

        assert!(
            report.plan.repack_jobs.is_empty(),
            "a fully live stable pack must never be repacked: {}",
            report.plan.summary()
        );
        assert_eq!(report.packs_uploaded, 0);
        assert_eq!(
            report.plan.touch_packs,
            vec![stable_pack],
            "the idle stable pack gets an LRU touch instead"
        );
        assert_eq!(report.packs_touched, 1);

        // The stable pack still exists under the same content-addressed key,
        // and keeper is still readable.
        let manifest = sim.manifest().await;
        assert!(manifest.packs.contains_key(&stable_pack));
        assert!(
            sim.stored_pack_keys()
                .await
                .contains(&pack_cache_key(&stable_pack))
        );
        sim.assert_readable(&[&keeper]).await;
    })
    .await;
}

// ---------------------------------------------------------------------------
// CAS no-op trap + touch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fully_live_pack_is_touched_and_never_repacked() {
    timed(async {
        let fake = FakeGha::start().await;
        let http = client();
        let sim = SimCache::new(&fake, &http);

        let app = SimPath::new("the-app", 1, 100_000);
        fake.set_clock(T0);
        sim.push(
            "main",
            std::slice::from_ref(&app),
            std::slice::from_ref(&app),
            T0,
        )
        .await;

        let manifest = sim.manifest().await;
        let pack = *manifest.packs.keys().next().unwrap();
        let pack_key = pack_cache_key(&pack);

        let accessed_before = last_accessed(&sim.rest, &pack_key)
            .await
            .expect("pack exists");
        assert!(accessed_before <= T0 + HOUR);

        // 5 days later (idle > TouchAge=4d), the path is still in the root.
        let t1 = T0 + 5 * DAY;
        fake.set_clock(t1);
        sim.push("main", &[], std::slice::from_ref(&app), t1).await;

        let gc = sim.gc(GcPolicy::default());
        let report = gc.run(false, t1).await.expect("gc run failed");

        // CAS no-op trap: never repacked, only touched.
        assert!(report.plan.repack_jobs.is_empty());
        assert_eq!(report.packs_uploaded, 0);
        assert_eq!(report.plan.touch_packs, vec![pack]);
        assert_eq!(report.packs_touched, 1);
        assert_eq!(report.packs_deleted, 0);

        // The touch was a 1-byte Range read and it reset the LRU clock.
        assert_eq!(one_byte_reads(&fake, &pack_key), 1);
        let accessed_after = last_accessed(&sim.rest, &pack_key)
            .await
            .expect("pack still exists");
        assert!(
            accessed_after >= t1,
            "LRU clock must be reset by the touch ({accessed_before} -> {accessed_after})"
        );

        // Same pack, same key, path still readable.
        let manifest = sim.manifest().await;
        assert!(manifest.packs.contains_key(&pack));
        sim.assert_readable(&[&app]).await;
    })
    .await;
}

// ---------------------------------------------------------------------------
// Eviction healing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn evicted_pack_heals_and_repush_restores_the_path() {
    timed(async {
        let fake = FakeGha::start().await;
        let http = client();
        let sim = SimCache::new(&fake, &http);

        // Two separate pushes -> two packs.
        let victim = SimPath::new("victim-app", 1, 60_000);
        let survivor = SimPath::new("survivor-lib", 2, 60_000);
        fake.set_clock(T0);
        sim.push(
            "main",
            std::slice::from_ref(&victim),
            std::slice::from_ref(&victim),
            T0,
        )
        .await;
        sim.push(
            "main",
            std::slice::from_ref(&survivor),
            &[victim.clone(), survivor.clone()],
            T0 + 60,
        )
        .await;

        let manifest = sim.manifest().await;
        assert_eq!(manifest.packs.len(), 2);
        let victim_pack = chunk_locations_of(&manifest, &victim)[0].pack;

        // GitHub evicts the victim's pack (quota pressure / 7-day idle).
        fake.evict(&http, &pack_cache_key(&victim_pack)).await;

        // GC two hours later reconciles the loss.
        let t1 = T0 + 2 * HOUR;
        fake.set_clock(t1);
        let report = sim.gc(GcPolicy::default()).run(false, t1).await.unwrap();

        assert_eq!(report.plan.evicted_packs, vec![victim_pack]);
        assert_eq!(report.plan.heal_paths, vec![victim.path_hash()]);

        // The healed path is gone (clean 404, no partial data); the other
        // path is untouched.
        let manifest = sim.manifest().await;
        assert!(!manifest.paths.contains_key(&victim.path_hash()));
        assert!(manifest.paths.contains_key(&survivor.path_hash()));
        sim.assert_unavailable(&[&victim]).await;
        sim.assert_readable(&[&survivor]).await;
        sim.assert_no_dangling_pack_references().await;

        // The next CI run rebuilds the path (cache miss) and re-pushes it.
        let t2 = t1 + HOUR;
        fake.set_clock(t2);
        sim.push(
            "main",
            std::slice::from_ref(&victim),
            &[victim.clone(), survivor.clone()],
            t2,
        )
        .await;

        let manifest = sim.manifest().await;
        assert!(manifest.paths.contains_key(&victim.path_hash()));
        sim.assert_readable(&[&victim, &survivor]).await;
        sim.assert_no_dangling_pack_references().await;
    })
    .await;
}

// ---------------------------------------------------------------------------
// GC vs concurrent push
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_push_during_gc_keeps_both_outcomes_correct() {
    timed(async {
        let fake = FakeGha::start().await;
        let http = client();
        let sim = SimCache::new(&fake, &http);

        let big = SimPath::new("big-app", 1, 200_000);
        let small = SimPath::new("small-lib", 2, 40_000);
        fake.set_clock(T0);
        sim.push(
            "main",
            &[big.clone(), small.clone()],
            &[big.clone(), small.clone()],
            T0,
        )
        .await;
        let original_pack = *sim.manifest().await.packs.keys().next().unwrap();

        // 20 days later `big` is dead; GC plans to drop it and repack.
        let t1 = T0 + 20 * DAY;
        fake.set_clock(t1);
        sim.push("main", &[], std::slice::from_ref(&small), t1)
            .await;

        let gc = sim.gc(GcPolicy::default());
        let (_, observations, plan) = gc.plan(t1).await.unwrap();
        assert!(plan.drop_paths.contains(&big.path_hash()));
        assert_eq!(plan.repack_jobs.len(), 1);

        let repacks = gc.execute_repacks(&plan).await.unwrap();
        gc.execute_touches(&plan).await.unwrap();

        // Concurrent push lands BEFORE the commit: another CI job rebuilt
        // `big` and put it back into the closure.
        sim.push(
            "main",
            std::slice::from_ref(&big),
            &[big.clone(), small.clone()],
            t1 + 60,
        )
        .await;

        // The commit re-plans against the new manifest.
        let outcome = gc.commit(&observations, &repacks, t1).await.unwrap();
        let deleted = gc.delete_packs(&outcome.deletable).await.unwrap();
        gc.delete_orphans(&outcome.orphan_keys).await.unwrap();

        // Both outcomes are correct: big survives (the push won), small's
        // chunks moved to the repacked pack, and the original pack was NOT
        // deleted because big still references it.
        let manifest = sim.manifest().await;
        assert!(manifest.paths.contains_key(&big.path_hash()));
        assert!(manifest.paths.contains_key(&small.path_hash()));
        for location in chunk_locations_of(&manifest, &big) {
            assert_eq!(location.pack, original_pack);
        }
        for location in chunk_locations_of(&manifest, &small) {
            assert_ne!(location.pack, original_pack);
        }
        assert!(
            manifest.packs.contains_key(&original_pack),
            "the pack big still lives in must stay referenced"
        );
        assert_eq!(deleted, 0, "no pack may be deleted while still referenced");
        assert!(
            sim.stored_pack_keys()
                .await
                .contains(&pack_cache_key(&original_pack))
        );

        sim.assert_readable(&[&big, &small]).await;
        sim.assert_no_dangling_pack_references().await;
    })
    .await;
}

// ---------------------------------------------------------------------------
// Crash safety
// ---------------------------------------------------------------------------

/// Build the standard crash-test scenario: a mostly-dead pack that needs
/// repacking, an idle pack that needs touching, an orphan pack, and stale
/// manifest versions.
async fn crash_scenario(
    fake: &FakeGha,
    http: &reqwest::Client,
) -> (SimCache, SimPath, SimPath, u64) {
    let sim = SimCache::new(fake, http);
    let dying = SimPath::new("dying-app", 11, 200_000);
    let surviving = SimPath::new("surviving-lib", 12, 40_000);

    fake.set_clock(T0);
    sim.push(
        "main",
        &[dying.clone(), surviving.clone()],
        &[dying.clone(), surviving.clone()],
        T0,
    )
    .await;
    // A pack that was uploaded but whose push never committed a manifest.
    sim.upload_orphan_pack(99).await;

    let t1 = T0 + 20 * DAY;
    fake.set_clock(t1);
    sim.push("main", &[], std::slice::from_ref(&surviving), t1)
        .await;

    (sim, dying, surviving, t1)
}

#[tokio::test]
async fn crash_between_any_two_execute_steps_never_loses_live_paths() {
    timed(async {
        // Execute steps: 1 repack, 2 touch, 3 commit, 4 delete packs,
        // 5 delete orphans, 6 cleanup old manifests. `stop_after = N` means
        // the process died after step N (0 = died right after planning).
        for stop_after in 0..=6 {
            let fake = FakeGha::start().await;
            let http = client();
            let (sim, dying, surviving, t1) = crash_scenario(&fake, &http).await;
            let gc = sim.gc(GcPolicy::default());

            let (_, observations, plan) = gc.plan(t1).await.unwrap();
            let mut repacks = RepackOutput::default();
            let mut outcome = None;

            if stop_after >= 1 {
                repacks = gc.execute_repacks(&plan).await.unwrap();
            }
            if stop_after >= 2 {
                gc.execute_touches(&plan).await.unwrap();
            }
            if stop_after >= 3 {
                outcome = Some(gc.commit(&observations, &repacks, t1).await.unwrap());
            }
            if stop_after >= 4 {
                let deletable = &outcome.as_ref().unwrap().deletable;
                gc.delete_packs(deletable).await.unwrap();
            }
            if stop_after >= 5 {
                let orphans = &outcome.as_ref().unwrap().orphan_keys;
                gc.delete_orphans(orphans).await.unwrap();
            }
            if stop_after >= 6 {
                gc.cleanup_manifests(t1).await.unwrap();
            }

            // INVARIANT after the crash: no live path references a deleted
            // pack, and every live path is still fully readable.
            sim.assert_no_dangling_pack_references().await;
            sim.assert_readable(&[&surviving]).await;

            // RECOVERY: the next scheduled GC run (2 hours later, so the
            // crashed run's uploads are old enough for the orphan sweep).
            let t2 = t1 + 2 * HOUR;
            fake.set_clock(t2);
            sim.gc(GcPolicy::default())
                .run(false, t2)
                .await
                .unwrap_or_else(|err| {
                    panic!("recovery GC failed (stop_after={stop_after}): {err}")
                });

            sim.assert_no_dangling_pack_references().await;
            sim.assert_readable(&[&surviving]).await;

            // Converged: the dead path is gone and storage holds exactly one
            // pack (the repacked one) — the old pack, the orphan, and any
            // intermediate uploads are all cleaned up.
            let manifest = sim.manifest().await;
            assert!(
                !manifest.paths.contains_key(&dying.path_hash()),
                "stop_after={stop_after}: dead path must be collected"
            );
            assert_eq!(
                manifest.packs.len(),
                1,
                "stop_after={stop_after}: exactly one pack must remain in the manifest"
            );
            let stored = sim.stored_pack_keys().await;
            assert_eq!(
                stored.len(),
                1,
                "stop_after={stop_after}: exactly one pack must remain in storage, got {stored:?}"
            );
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// 30-day simulation (the Phase 5 milestone)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn thirty_day_history_converges_to_live_set_storage() {
    timed(async {
        let fake = FakeGha::start().await;
        let http = client();
        let sim = SimCache::new(&fake, &http);
        let policy = GcPolicy::default();

        // The repository's "system" closure: stable across the whole month.
        let base: Vec<SimPath> = (0..10)
            .map(|i| SimPath::new(&format!("base-{i}"), 1000 + i, 100_000))
            .collect();
        // Application paths: replaced wholesale by weekly "nixpkgs bumps"
        // (50% of the closure's path count).
        let mut generation = 0u64;
        let mut apps: Vec<SimPath> = (0..10)
            .map(|i| SimPath::new(&format!("app-gen0-{i}"), 2000 + i, 20_000))
            .collect();
        // Daily artifacts: two new paths per day, kept in the closure for
        // three days.
        let mut dailies: Vec<SimPath> = Vec::new();
        // Feature-branch paths (the branch gets deleted).
        let feature: Vec<SimPath> = (0..2)
            .map(|i| SimPath::new(&format!("feature-{i}"), 3000 + i, 20_000))
            .collect();

        let mut healed_at_least_once = false;

        for day in 0..30u64 {
            let now = T0 + day * DAY;
            fake.set_clock(now);

            // Weekly nixpkgs bump: all app paths replaced with new content.
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

            // Daily artifacts rotate: two new, oldest leave after 3 days.
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

            // Today's closure.
            let closure: Vec<SimPath> = base
                .iter()
                .chain(apps.iter())
                .chain(dailies.iter())
                .cloned()
                .collect();

            // The CI run rebuilds (and therefore pushes) whatever the cache
            // does not serve: new paths and paths lost to eviction.
            let manifest = sim.manifest().await;
            let to_push: Vec<SimPath> = closure
                .iter()
                .filter(|path| !manifest.paths.contains_key(&path.path_hash()))
                .cloned()
                .collect();
            if day > 0 && to_push.iter().any(|path| path.name.starts_with("base-")) {
                // Base paths only need re-pushing after an eviction healed them.
                healed_at_least_once = true;
            }
            sim.push("main-x86_64-linux", &to_push, &closure, now).await;

            // Days 2-3: a feature branch exists, then is deleted (its root
            // is never updated again -> RootTTL drops it).
            if day == 2 || day == 3 {
                let feature_closure: Vec<SimPath> =
                    feature.iter().chain(base.iter()).cloned().collect();
                let feature_push: Vec<SimPath> = feature.to_vec();
                sim.push(
                    "feature-x86_64-linux",
                    &feature_push,
                    &feature_closure,
                    now + 60,
                )
                .await;
            }

            // Day 15: quota pressure evicts a referenced pack mid-history.
            if day == 15 {
                let manifest = sim.manifest().await;
                // Evict the pack holding the base paths (worst case: the
                // most valuable pack).
                let base_pack = chunk_locations_of(&manifest, &base[0])[0].pack;
                fake.evict(&http, &pack_cache_key(&base_pack)).await;
            }

            // Daily scheduled GC, one hour after the push.
            let gc_now = now + HOUR;
            fake.set_clock(gc_now);
            sim.gc(policy.clone())
                .run(false, gc_now)
                .await
                .unwrap_or_else(|err| panic!("GC failed on day {day}: {err}"));

            // Continuous invariant: GC never breaks a live path.
            sim.assert_no_dangling_pack_references().await;
        }

        let final_now = T0 + 29 * DAY + HOUR;

        // --- The eviction actually happened and healed -------------------
        assert!(
            healed_at_least_once,
            "the day-15 eviction must have forced a re-push of base paths"
        );

        // --- Milestone assertion 1: storage ≈ live-set size (within 20%) --
        let live_bytes = sim.live_chunk_bytes().await;
        let stored_bytes = sim.stored_pack_bytes().await;
        assert!(
            stored_bytes >= live_bytes,
            "storage ({stored_bytes}) cannot be smaller than the live set ({live_bytes})"
        );
        assert!(
            stored_bytes as f64 <= live_bytes as f64 * 1.2,
            "storage must converge to the live-set size within 20%: \
             stored {stored_bytes} bytes vs live {live_bytes} bytes \
             ({:.1}% overhead)",
            (stored_bytes as f64 / live_bytes as f64 - 1.0) * 100.0
        );

        // --- Milestone assertion 2: no referenced pack idle > TouchAge ----
        let manifest = sim.manifest().await;
        for pack in manifest.packs.keys() {
            let key = pack_cache_key(pack);
            let accessed = last_accessed(&sim.rest, &key)
                .await
                .unwrap_or_else(|| panic!("referenced pack {key} missing from storage"));
            assert!(
                final_now.saturating_sub(accessed) <= policy.touch_age,
                "referenced pack {key} has not been accessed for {} days (> TouchAge)",
                final_now.saturating_sub(accessed) / DAY
            );
        }

        // --- Milestone assertion 3: every live path fully readable --------
        let final_closure: Vec<&SimPath> = base
            .iter()
            .chain(apps.iter())
            .chain(dailies.iter())
            .collect();
        sim.assert_readable(&final_closure).await;

        // --- Branch deletion: feature paths were collected ----------------
        for path in &feature {
            assert!(
                !manifest.paths.contains_key(&path.path_hash()),
                "feature-branch path {} must be gone (root expired on day ~17)",
                path.name
            );
        }
        assert!(
            !manifest.roots.contains_key("feature-x86_64-linux"),
            "the deleted branch's root must be gone"
        );

        // --- Old generations were collected -------------------------------
        let path_names: BTreeSet<String> = manifest
            .paths
            .values()
            .map(|entry| entry.store_path.to_string())
            .collect();
        assert!(
            !path_names.iter().any(|name| name.contains("app-gen0-")),
            "generation-0 app paths must be long gone"
        );
        assert!(
            !path_names.iter().any(|name| name.contains("daily-0-")),
            "day-0 daily paths must be long gone"
        );

        // --- Manifest version cleanup keeps the entry count bounded -------
        let manifest_entries = sim.rest.list_caches("m#").await.unwrap();
        assert!(
            manifest_entries.len() <= 3,
            "old manifest versions must be cleaned up, found {}: {:?}",
            manifest_entries.len(),
            manifest_entries.iter().map(|e| &e.key).collect::<Vec<_>>()
        );
    })
    .await;
}
