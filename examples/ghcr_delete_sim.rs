//! GHCR deletes a manifest by "package version id", and the only way to
//! learn ids is paging the versions list (100 per page, newest first).
//! Listing and DELETE share one REST budget of 1000 calls/h. How does
//! daily GC find the ids of what it wants to delete?
//!
//!   cargo run --release --example ghcr_delete_sim [tb…]     default: 0.03 1 10 100
//!
//!   full     page through everything every run
//!   topdown  page newest-first until the oldest wanted version is reached
//!   cache    keep a digest→id map in the Actions cache, page only what is newer;
//!            the entry is lost now and then (eviction), then fall back to topdown
//!   ledger   same map as a blob referenced from the GC head: never lost,
//!            one extra read and write per run
//!
//! What GC deletes per day: last epoch's heads and segments (young),
//! packs it repacked (skewed old), a few orphans (young).

mod common;
use common::Rng;

const PAGE: f64 = 100.0;
const BUDGET: f64 = 1000.0;
const DAYS: usize = 120;

const PACK_BYTES: f64 = 64.0 * 1048576.0;
/// Versions per pack: its manifest plus a share of segment manifests.
const VERSIONS_PER_PACK: f64 = 1.15;
const GROWTH_PER_DAY: f64 = 0.02;
const REPACKED_PER_DAY: f64 = 0.004;
/// h-*/c-* tags and their segment manifests, all deleted next epoch.
const HEADS_PER_DAY: usize = 400;
const ORPHANS_PER_DAY: usize = 5;
const CACHE_LOSS_P: f64 = 0.05;

fn main() {
    println!("REST calls per daily GC run (list pages + deletes), budget {BUDGET}/h");
    for tb in common::tb_args(&[0.03, 1.0, 10.0, 100.0]) {
        simulate(tb);
    }
}

fn simulate(tb: f64) {
    let packs = (tb * (1u64 << 40) as f64 / PACK_BYTES) as usize;
    let initial = (packs as f64 * VERSIONS_PER_PACK) as usize;
    let new_per_day =
        HEADS_PER_DAY + ((packs as f64 * GROWTH_PER_DAY * VERSIONS_PER_PACK) as usize).max(20);
    let repacked_per_day = ((packs as f64 * REPACKED_PER_DAY) as usize).max(1);
    let capacity = initial + DAYS * (new_per_day + 10);

    let mut rng = Rng(0xD1B54A32D192ED03 ^ packs as u64);
    let mut versions = Versions::new(capacity);
    // Long-lived versions (packs), candidates for repack. Young ones are tracked per epoch.
    let mut old: Vec<usize> = Vec::new();
    for _ in 0..initial {
        old.push(versions.create());
    }
    let mut last_epoch_young: Vec<usize> = Vec::new();
    let mut cache_covers = versions.created; // map knows every id created before this index

    let [mut full, mut topdown, mut cache, mut ledger] = [(); 4].map(|_| Vec::<f64>::new());

    for _ in 0..DAYS {
        let mut young = Vec::new();
        for k in 0..new_per_day {
            let v = versions.create();
            if k < HEADS_PER_DAY {
                young.push(v)
            } else {
                old.push(v)
            }
        }

        let mut wanted = std::mem::replace(&mut last_epoch_young, young);
        for _ in 0..repacked_per_day.min(old.len()) {
            // min of two picks skews towards old packs
            let i = rng.below(old.len()).min(rng.below(old.len()));
            wanted.push(old.swap_remove(i));
        }
        for _ in 0..ORPHANS_PER_DAY {
            let v = versions.created - 1 - rng.below(new_per_day);
            if versions.is_live(v) && !wanted.contains(&v) {
                wanted.push(v);
            }
        }

        let deletes = wanted.len() as f64;
        let oldest_wanted = *wanted.iter().min().unwrap();
        let pages_all = (versions.live as f64 / PAGE).ceil();
        let pages_down_to_oldest =
            ((versions.live_newer_than(oldest_wanted) + 1) as f64 / PAGE).ceil();
        let pages_since_cache = ((versions.live_at_or_newer(cache_covers)) as f64 / PAGE)
            .ceil()
            .max(1.0);

        full.push(pages_all + deletes);
        topdown.push(pages_down_to_oldest + deletes);
        cache.push(
            deletes
                + if rng.uniform() < CACHE_LOSS_P {
                    pages_down_to_oldest
                } else {
                    pages_since_cache
                },
        );
        ledger.push(pages_since_cache + deletes + 2.0);
        cache_covers = versions.created;

        for v in wanted {
            versions.delete(v);
        }
    }

    println!(
        "\n== {tb} TB: {initial} versions, +{new_per_day}/day, GC deletes ≈ {HEADS_PER_DAY} young + {repacked_per_day} repacked per day"
    );
    println!(
        "{:8} {:>9} {:>9} {:>9} {:>12}",
        "strategy", "mean", "p99", "max", "runs >1000/h"
    );
    report("full", full, "");
    report("topdown", topdown, "");
    report(
        "cache",
        cache,
        &format!("(map lost in {:.0} % of runs)", CACHE_LOSS_P * 100.0),
    );
    report(
        "ledger",
        ledger,
        &format!("(ledger blob {:.1} MB)", versions.live as f64 * 48.0 / 1e6),
    );
}

fn report(name: &str, mut calls: Vec<f64>, note: &str) {
    calls.sort_by(f64::total_cmp);
    let mean = calls.iter().sum::<f64>() / calls.len() as f64;
    let p99 = calls[(calls.len() as f64 * 0.99) as usize];
    let max = calls.last().unwrap();
    let over = calls.iter().filter(|&&c| c > BUDGET).count();
    println!(
        "{name:8} {mean:>9.0} {p99:>9.0} {max:>9.0} {over:>7}/{:<4} {note}",
        calls.len()
    );
}

/// Versions in creation order with a Fenwick tree over "still live", so
/// "how many live versions are newer than i" (= its distance from the top
/// of the newest-first listing) is O(log n).
struct Versions {
    tree: Vec<u32>,
    live_flag: Vec<bool>,
    created: usize,
    live: usize,
}

impl Versions {
    fn new(capacity: usize) -> Self {
        Versions {
            tree: vec![0; capacity + 1],
            live_flag: vec![false; capacity],
            created: 0,
            live: 0,
        }
    }
    fn create(&mut self) -> usize {
        let i = self.created;
        self.created += 1;
        self.live += 1;
        self.live_flag[i] = true;
        self.adjust(i, 1);
        i
    }
    fn delete(&mut self, i: usize) {
        if std::mem::replace(&mut self.live_flag[i], false) {
            self.live -= 1;
            self.adjust(i, -1);
        }
    }
    fn is_live(&self, i: usize) -> bool {
        self.live_flag[i]
    }
    fn live_newer_than(&self, i: usize) -> usize {
        self.live - self.live_before(i + 1)
    }
    fn live_at_or_newer(&self, i: usize) -> usize {
        self.live - self.live_before(i)
    }

    fn adjust(&mut self, i: usize, d: i32) {
        let mut i = i + 1;
        while i < self.tree.len() {
            self.tree[i] = (self.tree[i] as i32 + d) as u32;
            i += i & i.wrapping_neg();
        }
    }
    /// live versions with index < i
    fn live_before(&self, mut i: usize) -> usize {
        let mut n = 0;
        while i > 0 {
            n += self.tree[i] as usize;
            i -= i & i.wrapping_neg();
        }
        n
    }
}
