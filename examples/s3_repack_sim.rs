//! How should GC repack sparse packs on S3?
//!
//!   cargo run --release --example s3_repack_sim
//!
//! A pack is a sequence of store paths. Paths die over time, mostly a
//! whole drain's worth at once (a dependency bump), and daily GC rewrites
//! every pack whose live share fell under a threshold. The question is
//! how the new pack gets built:
//!
//!   runner   download the live bytes, upload a new pack (the only option on gha/OCI)
//!   compose  UploadPartCopy each live run server-side. S3 parts must be ≥ 5 MiB,
//!            so shorter runs still go through the runner
//!   bridge   like compose, but neighbouring live runs are joined across small dead
//!            gaps until the copy range reaches 5 MiB. The gap bytes are copied too
//!   aligned  R2 demands all parts be the same size: pad every range to a multiple
//!            of 5 MiB with whatever bytes follow it
//!
//! Reported: GB that passed through the runner, S3 requests, and storage
//! overhead (bytes stored / bytes live − 1, mean over all days).

mod common;
use common::{MIB, Rng};

const MIN_PART: f64 = 5.0 * MIB;

const DAYS: usize = 90;
const DRAINS_PER_DAY: usize = 40;
const PATHS_PER_DRAIN: usize = 150;

/// Per day, each past drain has this chance of a dependency bump …
const BUMP_P: f64 = 0.04;
/// … which kills this share of its still-live paths.
const BUMP_KILLS: f64 = 0.6;
/// Independent per-path death rate per day on top.
const CHURN_P: f64 = 0.004;

#[derive(Clone, Copy, PartialEq)]
enum Strategy {
    Runner,
    Compose,
    Bridge { max_gap: f64, min_live_share: f64 },
    Aligned,
}

#[derive(Clone, Copy)]
struct Params {
    pack_size: f64,
    live_threshold: f64,
    strategy: Strategy,
}

// ---------------------------------------------------------------- store model

#[derive(Clone)]
struct Extent {
    len: f64,
    live: bool,
    /// Which drain wrote it. Deaths are correlated per drain. Dead filler uses NONE.
    drain: usize,
}
const NONE: usize = usize::MAX;

#[derive(Default)]
struct Pack {
    extents: Vec<Extent>,
}

/// A maximal stretch of adjacent live extents inside one pack.
struct Run {
    start: f64,
    len: f64,
    extents: std::ops::Range<usize>,
}

impl Pack {
    fn size(&self) -> f64 {
        self.extents.iter().map(|e| e.len).sum()
    }
    fn live(&self) -> f64 {
        self.extents.iter().filter(|e| e.live).map(|e| e.len).sum()
    }
    fn live_runs(&self) -> Vec<Run> {
        let mut runs: Vec<Run> = Vec::new();
        let mut offset = 0.0;
        for (i, e) in self.extents.iter().enumerate() {
            if e.live {
                match runs.last_mut() {
                    Some(r) if r.extents.end == i => {
                        r.len += e.len;
                        r.extents.end = i + 1;
                    }
                    _ => runs.push(Run {
                        start: offset,
                        len: e.len,
                        extents: i..i + 1,
                    }),
                }
            }
            offset += e.len;
        }
        runs
    }
}

// ---------------------------------------------------------------- counters

#[derive(Default)]
struct Totals {
    runner_bytes: f64,
    requests: f64,
    dead_bytes_copied: f64,
    overhead_sum: f64,
    overhead_days: f64,
}

impl Totals {
    fn overhead_pct(&self) -> f64 {
        100.0 * self.overhead_sum / self.overhead_days
    }
}

// ---------------------------------------------------------------- simulation

fn simulate(params: Params, seed: u64) -> Totals {
    let mut rng = Rng(seed);
    let mut packs: Vec<Pack> = Vec::new();
    let mut drains = 0usize;
    let mut totals = Totals::default();

    for _ in 0..DAYS {
        for _ in 0..DRAINS_PER_DAY {
            write_drain(&mut packs, drains, params, &mut rng, &mut totals);
            drains += 1;
        }
        kill_paths(&mut packs, drains, &mut rng);
        packs = gc(packs, params, &mut totals);

        let stored: f64 = packs.iter().map(Pack::size).sum();
        let live: f64 = packs.iter().map(Pack::live).sum();
        totals.overhead_sum += stored / live - 1.0;
        totals.overhead_days += 1.0;
    }
    totals
}

fn write_drain(
    packs: &mut Vec<Pack>,
    drain: usize,
    params: Params,
    rng: &mut Rng,
    totals: &mut Totals,
) {
    let mut pack = Pack::default();
    for _ in 0..PATHS_PER_DRAIN {
        pack.extents.push(Extent {
            len: path_size(rng),
            live: true,
            drain,
        });
        if pack.size() >= params.pack_size {
            totals.requests += upload_requests(pack.size());
            packs.push(std::mem::take(&mut pack));
        }
    }
    if !pack.extents.is_empty() {
        totals.requests += upload_requests(pack.size());
        packs.push(pack);
    }
}

/// Multipart upload in 64 MiB parts: create + parts + complete.
fn upload_requests(bytes: f64) -> f64 {
    2.0 + (bytes / (64.0 * MIB)).ceil()
}

fn kill_paths(packs: &mut [Pack], drains: usize, rng: &mut Rng) {
    let bumped: Vec<bool> = (0..drains).map(|_| rng.uniform() < BUMP_P).collect();
    for e in packs.iter_mut().flat_map(|p| p.extents.iter_mut()) {
        if e.live {
            let bumped = e.drain != NONE && bumped[e.drain] && rng.uniform() < BUMP_KILLS;
            if bumped || rng.uniform() < CHURN_P {
                e.live = false;
            }
        }
    }
}

/// Rewrites every pack under the live threshold into `output`, which is
/// cut into new packs of `pack_size`. Returns the surviving pack set.
fn gc(packs: Vec<Pack>, params: Params, totals: &mut Totals) -> Vec<Pack> {
    let mut kept: Vec<Pack> = Vec::new();
    let mut output = Pack::default();

    for pack in packs {
        let (size, live) = (pack.size(), pack.live());
        if live == 0.0 {
            continue; // plain delete, batched, ~free
        }
        if live / size >= params.live_threshold {
            kept.push(pack);
            continue;
        }
        match params.strategy {
            Strategy::Runner => {
                copy_through_runner(&pack, 0..pack.extents.len(), &mut output, totals)
            }
            Strategy::Compose => {
                for run in pack.live_runs() {
                    copy_range(&pack, run.start, run.len, run.extents, &mut output, totals);
                }
            }
            Strategy::Bridge {
                max_gap,
                min_live_share,
            } => {
                for (start, len, extents) in bridge(&pack, max_gap, min_live_share) {
                    copy_range(&pack, start, len, extents, &mut output, totals);
                }
            }
            Strategy::Aligned => {
                for (start, len, extents) in bridge(&pack, 8.0 * MIB, 0.5) {
                    let padded = (len / MIN_PART).ceil() * MIN_PART;
                    if start + padded <= size {
                        copy_range(&pack, start, padded, extents, &mut output, totals);
                    } else {
                        copy_through_runner(&pack, extents, &mut output, totals);
                    }
                }
            }
        }
        while output.size() >= params.pack_size {
            let mut cut = Pack::default();
            while cut.size() < params.pack_size {
                cut.extents.push(output.extents.remove(0));
            }
            totals.requests += 2.0; // create + complete; parts counted in copy_range
            kept.push(cut);
        }
    }
    if !output.extents.is_empty() {
        totals.requests += 2.0;
        kept.push(output);
    }
    kept
}

/// Joins neighbouring live runs into one copy range when the dead gap is
/// small and the joined range stays mostly live, but only while one side
/// is still under the minimum part size. Returns (start, len, extents).
fn bridge(
    pack: &Pack,
    max_gap: f64,
    min_live_share: f64,
) -> Vec<(f64, f64, std::ops::Range<usize>)> {
    let mut ranges: Vec<(f64, f64, std::ops::Range<usize>, f64)> = Vec::new(); // + live bytes
    for run in pack.live_runs() {
        if let Some((start, len, extents, live)) = ranges.last_mut() {
            let gap = run.start - (*start + *len);
            let joined_len = *len + gap + run.len;
            let joined_live = *live + run.len;
            let needs_it = *len < MIN_PART || run.len < MIN_PART;
            if needs_it && gap <= max_gap && joined_live / joined_len >= min_live_share {
                *len = joined_len;
                *live = joined_live;
                extents.end = run.extents.end;
                continue;
            }
        }
        ranges.push((run.start, run.len, run.extents, run.len));
    }
    ranges.into_iter().map(|(s, l, e, _)| (s, l, e)).collect()
}

/// Server-side copy of `pack[start .. start+len]` if it is long enough
/// for an S3 part, else through the runner. Dead bytes inside the range
/// land in the output as dead filler.
fn copy_range(
    pack: &Pack,
    start: f64,
    len: f64,
    extents: std::ops::Range<usize>,
    output: &mut Pack,
    totals: &mut Totals,
) {
    if len < MIN_PART {
        copy_through_runner(pack, extents, output, totals);
        return;
    }
    let live: f64 = pack.extents[extents.clone()]
        .iter()
        .filter(|e| e.live)
        .map(|e| e.len)
        .sum();
    for e in &pack.extents[extents] {
        if e.live {
            output.extents.push(e.clone());
        }
    }
    output.extents.push(Extent {
        len: len - live,
        live: false,
        drain: NONE,
    });
    totals.dead_bytes_copied += len - live;
    totals.requests += (len / (5.0 * 1024.0 * MIB)).ceil(); // one UploadPartCopy per 5 GiB
    let _ = start;
}

fn copy_through_runner(
    pack: &Pack,
    extents: std::ops::Range<usize>,
    output: &mut Pack,
    totals: &mut Totals,
) {
    for e in pack.extents[extents].iter().filter(|e| e.live) {
        totals.runner_bytes += e.len;
        output.extents.push(e.clone());
    }
    totals.requests += 2.0; // ranged GET + UploadPart
}

// ---------------------------------------------------------------- report

fn main() {
    let written_gb = (DAYS * DRAINS_PER_DAY * PATHS_PER_DRAIN) as f64 * 1.5 * MIB / 1e9;
    println!(
        "{DAYS} days × {DRAINS_PER_DAY} drains × {PATHS_PER_DRAIN} paths ≈ {written_gb:.0} GB written\n"
    );

    let bridge = Strategy::Bridge {
        max_gap: 8.0 * MIB,
        min_live_share: 0.5,
    };

    section("strategies (threshold 0.5)");
    for pack_mib in [64.0, 256.0] {
        for (name, strategy) in [
            ("runner", Strategy::Runner),
            ("compose", Strategy::Compose),
            ("bridge", bridge),
            ("aligned", Strategy::Aligned),
        ] {
            report(
                name,
                Params {
                    pack_size: pack_mib * MIB,
                    live_threshold: 0.5,
                    strategy,
                },
            );
        }
    }

    section("live threshold: runner cost grows with it, bridge cost does not (256 MiB packs)");
    for live_threshold in [0.3, 0.5, 0.7] {
        for (name, strategy) in [("runner", Strategy::Runner), ("bridge", bridge)] {
            report(
                name,
                Params {
                    pack_size: 256.0 * MIB,
                    live_threshold,
                    strategy,
                },
            );
        }
    }

    section(
        "bridge rule: gap limit and minimum live share of a joined range (256 MiB, threshold 0.7)",
    );
    for (max_gap, min_live_share) in [(2.0, 0.6), (8.0, 0.6), (8.0, 0.5), (8.0, 0.4), (32.0, 0.25)]
    {
        let strategy = Strategy::Bridge {
            max_gap: max_gap * MIB,
            min_live_share,
        };
        report(
            &format!("≤{max_gap} MiB ≥{:.0} %", min_live_share * 100.0),
            Params {
                pack_size: 256.0 * MIB,
                live_threshold: 0.7,
                strategy,
            },
        );
    }
}

fn section(title: &str) {
    println!("-- {title}");
    println!(
        "{:>16} {:>9} {:>5} {:>10} {:>9} {:>9} {:>13}",
        "strategy", "pack MiB", "thr", "runner GB", "requests", "overhead", "dead GB copied"
    );
}

fn report(name: &str, params: Params) {
    let t = simulate(params, 0x243F6A8885A308D3);
    println!(
        "{:>16} {:>9} {:>5} {:>10.1} {:>9.0} {:>8.1}% {:>13.1}",
        name,
        params.pack_size / MIB,
        params.live_threshold,
        t.runner_bytes / 1e9,
        t.requests,
        t.overhead_pct(),
        t.dead_bytes_copied / 1e9,
    );
}

/// Lognormal like a nixpkgs closure: median 200 KB, mean ~1.5 MB, capped at 2 GiB.
fn path_size(rng: &mut Rng) -> f64 {
    rng.lognormal(200.0 * 1024.0, 2.0)
        .clamp(4096.0, 2048.0 * MIB)
}
