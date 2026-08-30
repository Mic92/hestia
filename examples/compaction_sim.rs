//! One busy root: when should a drain also compact the pending segments,
//! given that other drains decide concurrently from a listing that lags?
//!
//!   cargo run --release --example compaction_sim [gha|hydra]
//!
//!   gha    nixpkgs merge queue on Actions: 200 pushes/day of ~150 paths,
//!          25 min jobs, 1000 read-only PR jobs/day, 20k-path root
//!   hydra  hydra.nixos.org: 143k build steps/day of ~1.3 paths, 4 min steps,
//!          200k-path root
//!
//!   never  drains never compact, GC does once a day
//!   elect  if no compaction head younger than `cooldown` is visible, compact with
//!          probability 1 / (drains expected in the window a compaction needs to
//!          become visible), rate estimated from the listing itself
//!   cron   drains never compact, `hestia compact` runs every `period`
//!
//! Fixed count thresholds (with and without jitter) were tried first: at
//! hydra rates every drain inside the lag window sees the same count,
//! all compact the same inputs, and the next one merges the copies.
//!
//! Reported per policy: what a cold load costs (requests, MB, seconds,
//! worst), peak segments and heads a reader sees, writer upload and
//! seconds, compactions/day and how many raced on the same inputs, base
//! rewrites/day, peak segment bytes held until GC.

mod common;
use common::Rng;
use std::collections::{BinaryHeap, HashSet};

// measured: manifest_bench, oci_probe
const META_BYTES_COMPACTED: f64 = 93.0; // per path, refs resolved to local indexes
const META_BYTES_PENDING: f64 = 350.0; // per path, refs still strings
const TREE_BYTES: f64 = 600.0;
const TAG_BYTES: f64 = 90.0;
const MERGE_CPU_S_PER_PATH: f64 = 33e-6;
const DOWN_BPS: f64 = 72e6;
const UP_BPS: f64 = 30e6;
const PARALLEL: f64 = 32.0;
const OBJECT_WRITE_S: f64 = 1.0;
const MANIFEST_GET_S: f64 = 0.28;
const BLOB_GET_S: f64 = 0.41;
const LIST_S: f64 = 0.3;
const LIST_PAGE: f64 = 10_000.0;

const SIM_DAYS: f64 = 5.0;
const DAY_S: f64 = 86400.0;
const LIST_LAG_MAX_S: f64 = 30.0;

struct Scenario {
    name: &'static str,
    root_paths: f64,
    pushes_per_day: f64,
    readers_per_day: f64,
    job_median_s: f64,
    job_sigma: f64,
    drain_paths_median: f64,
    drain_paths_sigma: f64,
}

#[derive(Clone, Copy)]
enum Policy {
    Never,
    Elect { cooldown_s: f64 },
    Cron { period_s: f64 },
}

fn main() {
    let which = std::env::args().nth(1).unwrap_or("gha".into());
    let (scenario, policies) = match which.as_str() {
        "hydra" => (
            Scenario {
                name: "hydra.nixos.org: 143k steps/day × 1.3 paths, 4 min steps, 200k-path root",
                root_paths: 200_000.0,
                pushes_per_day: 143_000.0,
                readers_per_day: 0.0,
                job_median_s: 240.0,
                job_sigma: 1.2,
                drain_paths_median: 1.3,
                drain_paths_sigma: 0.6,
            },
            vec![
                Policy::Elect { cooldown_s: 60.0 },
                Policy::Elect { cooldown_s: 300.0 },
                Policy::Elect { cooldown_s: 900.0 },
                Policy::Cron { period_s: 300.0 },
                Policy::Cron { period_s: 1800.0 },
            ],
        ),
        _ => (
            Scenario {
                name: "nixpkgs on Actions: 200 pushes/day × 150 paths, 25 min jobs, 20k-path root, 1000 PR jobs/day",
                root_paths: 20_000.0,
                pushes_per_day: 200.0,
                readers_per_day: 1000.0,
                job_median_s: 1500.0,
                job_sigma: 0.8,
                drain_paths_median: 150.0,
                drain_paths_sigma: 1.0,
            },
            vec![
                Policy::Never,
                Policy::Elect { cooldown_s: 60.0 },
                Policy::Elect { cooldown_s: 300.0 },
                Policy::Cron { period_s: 600.0 },
                Policy::Cron { period_s: 1800.0 },
            ],
        ),
    };

    println!(
        "{} — {SIM_DAYS} days, GC daily, list lag ≤ {LIST_LAG_MAX_S} s\n",
        scenario.name
    );
    println!(
        "{:<16} {:>7} {:>6} {:>6} {:>7} {:>5} {:>6} | {:>9} {:>5} {:>7} | {:>7} {:>5} {:>6} | {:>9}",
        "policy",
        "ld req",
        "ld MB",
        "ld s",
        "ld max",
        "segs↑",
        "heads↑",
        "wr MB/day",
        "wr s",
        "wr max",
        "comp/d",
        "dup%",
        "base/d",
        "store MB↑"
    );
    for policy in policies {
        let name = match policy {
            Policy::Never => "never".to_string(),
            Policy::Elect { cooldown_s } => format!("elect {cooldown_s}s"),
            Policy::Cron { period_s } => format!("cron {period_s}s"),
        };
        let s = simulate(&scenario, policy);
        println!(
            "{:<16} {:>7.1} {:>6.2} {:>6.2} {:>7.2} {:>5} {:>6} | {:>9.0} {:>5.1} {:>7.1} | {:>7.1} {:>5.1} {:>6.1} | {:>9.0}",
            name,
            s.load_requests / s.loads,
            s.load_bytes / s.loads / 1e6,
            s.load_s / s.loads,
            s.load_s_max,
            s.segments_max,
            s.heads_max,
            s.upload_bytes / SIM_DAYS / 1e6,
            s.writer_s / s.writers,
            s.writer_s_max,
            s.compactions / SIM_DAYS,
            100.0 * s.raced_compactions / s.compactions.max(1.0),
            s.base_rewrites / SIM_DAYS,
            s.stored_bytes_peak / 1e6,
        );
    }
}

// ---------------------------------------------------------------- model

/// A segment. `drains` records which drains since the last GC it covers,
/// so that merging two racing compactions dedups like the real PathHash
/// merge would instead of double counting paths.
#[derive(Clone)]
struct Segment {
    id: u64,
    includes_base: bool,
    drains: Bitset,
    paths: f64,
    compacted: bool,
}

impl Segment {
    fn meta_bytes(&self) -> f64 {
        self.paths
            * if self.compacted {
                META_BYTES_COMPACTED
            } else {
                META_BYTES_PENDING
            }
    }
    fn tree_bytes(&self) -> f64 {
        self.paths * TREE_BYTES
    }
    fn bytes(&self) -> f64 {
        self.meta_bytes() + self.tree_bytes()
    }
}

/// A drain or compaction head. Drain heads have empty `replaces`/`subsumes`.
struct Head {
    id: u64,
    published_at: f64,
    visible_at: f64,
    added: Segment,
    replaces: HashSet<u64>, // segment ids
    subsumes: HashSet<u64>, // head ids
}

impl Head {
    fn is_compaction(&self) -> bool {
        !self.replaces.is_empty()
    }
}

#[derive(Default)]
struct Stats {
    loads: f64,
    load_requests: f64,
    load_bytes: f64,
    load_s: f64,
    load_s_max: f64,
    segments_max: f64,
    heads_max: f64,
    writers: f64,
    upload_bytes: f64,
    writer_s: f64,
    writer_s_max: f64,
    compactions: f64,
    raced_compactions: f64,
    base_rewrites: f64,
    stored_bytes_peak: f64,
}

/// What a reader computes from the listing. Maintained incrementally so a
/// load is O(1) in the sim even at hydra rates.
#[derive(Default, Clone, Copy)]
struct View {
    segments: f64,
    meta_bytes: f64,
    pending: usize,
    pending_paths: f64,
    compaction_heads: f64,
}

impl View {
    fn count(&mut self, s: &Segment, sign: f64) {
        self.segments += sign;
        self.meta_bytes += sign * s.meta_bytes();
        if !s.includes_base {
            self.pending = (self.pending as f64 + sign) as usize;
            self.pending_paths += sign * s.paths;
        }
    }
}

struct Sim<'a> {
    scenario: &'a Scenario,
    policy: Policy,
    rng: Rng,
    next_id: u64,
    stats: Stats,

    /// GC's compacted segment for the root.
    base: Segment,
    base_replaced: bool,
    /// Paths per drain since last GC, indexed by the bit in `Segment::drains`.
    drain_paths: Vec<f64>,
    /// Published but still inside listing lag.
    unlisted: Vec<Head>,
    /// Listed and not subsumed: what a reader considers.
    listed: Vec<Head>,
    view: View,
    tags: f64,
    stored_bytes: f64,
    newest_compaction_seen: f64,
    last_gc: f64,
}

fn transfer_s(objects: f64, bytes: f64, per_object_s: f64, bps: f64) -> f64 {
    (objects / PARALLEL).ceil() * per_object_s + bytes / bps
}

impl Sim<'_> {
    fn new_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// Heads whose listing lag has elapsed become visible. Apply what they hide.
    fn advance_listing(&mut self, now: f64) {
        let (ready, still): (Vec<Head>, Vec<Head>) = std::mem::take(&mut self.unlisted)
            .into_iter()
            .partition(|h| h.visible_at <= now);
        self.unlisted = still;
        for head in ready {
            if !head.subsumes.is_empty() {
                let (gone, kept): (Vec<Head>, Vec<Head>) = std::mem::take(&mut self.listed)
                    .into_iter()
                    .partition(|l| head.subsumes.contains(&l.id));
                self.listed = kept;
                for g in &gone {
                    self.view.count(&g.added, -1.0);
                    if g.is_compaction() {
                        self.view.compaction_heads -= 1.0;
                    }
                }
            }
            if head.replaces.contains(&self.base.id) && !self.base_replaced {
                self.base_replaced = true;
                self.view.count(&self.base.clone(), -1.0);
            }
            if head.is_compaction() {
                self.view.compaction_heads += 1.0;
                self.newest_compaction_seen = self.newest_compaction_seen.max(head.published_at);
            }
            self.view.count(&head.added, 1.0);
            self.listed.push(head);
        }
    }

    /// tags/list, GET g-* and every c-* (manifest + config), GET every segment's .meta.
    fn load(&mut self) {
        let pages = (self.tags / LIST_PAGE).ceil().max(1.0);
        let list_bytes = self.tags * TAG_BYTES;
        let head_gets = 2.0 + 2.0 * self.view.compaction_heads;
        let secs = pages * LIST_S
            + list_bytes / DOWN_BPS
            + transfer_s(head_gets, 0.0, MANIFEST_GET_S, DOWN_BPS)
            + transfer_s(
                self.view.segments,
                self.view.meta_bytes,
                BLOB_GET_S,
                DOWN_BPS,
            );
        let s = &mut self.stats;
        s.loads += 1.0;
        s.load_requests += pages + head_gets + self.view.segments;
        s.load_bytes += list_bytes + self.view.meta_bytes;
        s.load_s += secs;
        s.load_s_max = s.load_s_max.max(secs);
        s.segments_max = s.segments_max.max(self.view.segments);
        s.heads_max = s.heads_max.max(self.listed.len() as f64);
    }

    /// Returns (compact?, include the base segment?).
    fn decide(&mut self, now: f64, scheduled: bool) -> (bool, bool) {
        let with_base = self.view.pending_paths > 0.5 * self.scenario.root_paths;
        if scheduled {
            return (self.view.pending > 0, with_base);
        }
        let Policy::Elect { cooldown_s } = self.policy else {
            return (false, false);
        };
        if now - self.newest_compaction_seen < cooldown_s || self.view.pending < 2 {
            return (false, false);
        }
        let since = self.newest_compaction_seen.max(self.last_gc);
        let drains_per_s = self.listed.len() as f64 / (now - since).max(1.0);
        let merge_s = if with_base {
            self.scenario.root_paths
                * (MERGE_CPU_S_PER_PATH + (TREE_BYTES + META_BYTES_COMPACTED) / UP_BPS)
        } else {
            0.0
        };
        let window_s = LIST_LAG_MAX_S + 10.0 + merge_s;
        let competitors = (drains_per_s * window_s).max(1.0);
        (self.rng.uniform() < 1.0 / competitors, with_base)
    }

    /// A drain publishing `new_paths` (0 for the scheduled compactor).
    fn drain(&mut self, now: f64, scheduled: bool) {
        self.load(); // re-list before deciding, the job-start view is stale
        let new_paths = if scheduled {
            0.0
        } else {
            self.rng
                .lognormal(
                    self.scenario.drain_paths_median,
                    self.scenario.drain_paths_sigma,
                )
                .round()
                .max(1.0)
        };
        let (compact, with_base) = self.decide(now, scheduled);
        if scheduled && !compact {
            return;
        }

        let mut drains = Bitset::default();
        drains.set(self.drain_paths.len());
        self.drain_paths.push(new_paths);
        let head_id = self.new_id();
        let mut secs = 0.0;
        let mut replaces = HashSet::new();
        let mut subsumes = HashSet::new();

        let added = if compact {
            let mut inputs: Vec<&Segment> = Vec::new();
            if with_base && !self.base_replaced {
                inputs.push(&self.base);
            }
            for h in &self.listed {
                if with_base || !h.added.includes_base {
                    inputs.push(&h.added);
                    subsumes.insert(h.id);
                }
            }
            let mut includes_base = false;
            let mut input_paths = 0.0;
            let mut fetch_bytes = 0.0;
            for s in &inputs {
                drains.union(&s.drains);
                includes_base |= s.includes_base;
                input_paths += s.paths;
                fetch_bytes += s.tree_bytes();
                replaces.insert(s.id);
            }
            secs += transfer_s(inputs.len() as f64, fetch_bytes, BLOB_GET_S, DOWN_BPS);
            secs += (input_paths + new_paths) * MERGE_CPU_S_PER_PATH;

            let raced = self
                .unlisted
                .iter()
                .any(|h| h.replaces.iter().any(|r| replaces.contains(r)));
            self.stats.compactions += 1.0;
            self.stats.raced_compactions += raced as u8 as f64;
            self.stats.base_rewrites += with_base as u8 as f64;

            let paths = drains.weighted_sum(&self.drain_paths)
                + if includes_base {
                    self.scenario.root_paths
                } else {
                    0.0
                };
            Segment {
                id: self.new_id(),
                includes_base,
                drains,
                paths,
                compacted: with_base,
            }
        } else {
            Segment {
                id: self.new_id(),
                includes_base: false,
                drains,
                paths: new_paths,
                compacted: false,
            }
        };

        // pack, pack manifest, .meta, .tree, segment manifest. A c-* head is one more object
        let objects = if compact { 6.0 } else { 5.0 };
        secs += objects * OBJECT_WRITE_S + added.bytes() / UP_BPS;
        self.stored_bytes += added.bytes();
        self.tags += 1.0;

        let s = &mut self.stats;
        s.writers += 1.0;
        s.upload_bytes += added.bytes();
        s.writer_s += secs;
        s.writer_s_max = s.writer_s_max.max(secs);
        s.stored_bytes_peak = s.stored_bytes_peak.max(self.stored_bytes);

        let published_at = now + secs;
        let visible_at = published_at + self.rng.uniform() * LIST_LAG_MAX_S;
        self.unlisted.push(Head {
            id: head_id,
            published_at,
            visible_at,
            added,
            replaces,
            subsumes,
        });
    }

    /// GC folds everything listed into a new base and deletes the old tags.
    /// Heads still inside listing lag survive and are re-indexed.
    fn gc(&mut self, now: f64) {
        self.last_gc = now;
        self.newest_compaction_seen = now;
        self.base = Segment {
            id: self.new_id(),
            includes_base: true,
            drains: Bitset::default(),
            paths: self.scenario.root_paths,
            compacted: true,
        };
        self.base_replaced = false;
        self.listed.clear();
        self.view = View::default();
        self.view.count(&self.base.clone(), 1.0);

        self.drain_paths.clear();
        for h in &mut self.unlisted {
            let own_paths = h.added.paths
                - if h.added.includes_base {
                    self.scenario.root_paths
                } else {
                    0.0
                };
            h.added.drains = Bitset::default();
            h.added.drains.set(self.drain_paths.len());
            self.drain_paths.push(own_paths);
        }
        self.tags = self.unlisted.len() as f64;
        self.stored_bytes =
            self.base.bytes() + self.unlisted.iter().map(|h| h.added.bytes()).sum::<f64>();
    }
}

// ---------------------------------------------------------------- event loop

#[derive(PartialEq)]
enum Event {
    JobStart,
    ReaderStart,
    Drain,
    ScheduledCompact,
    Gc,
}

fn simulate(scenario: &Scenario, policy: Policy) -> Stats {
    let base = Segment {
        id: 1,
        includes_base: true,
        drains: Bitset::default(),
        paths: scenario.root_paths,
        compacted: true,
    };
    let mut view = View::default();
    view.count(&base, 1.0);
    let mut sim = Sim {
        scenario,
        policy,
        rng: Rng(0x9E3779B97F4A7C15),
        next_id: 1,
        stats: Stats::default(),
        base,
        base_replaced: false,
        drain_paths: vec![],
        unlisted: vec![],
        listed: vec![],
        view,
        tags: 0.0,
        stored_bytes: 0.0,
        newest_compaction_seen: f64::NEG_INFINITY,
        last_gc: 0.0,
    };

    let mut queue = Queue::default();
    let end = SIM_DAYS * DAY_S;
    let mut arrivals = Rng(42);
    queue.poisson(&mut arrivals, scenario.pushes_per_day / DAY_S, end, || {
        Event::JobStart
    });
    queue.poisson(&mut arrivals, scenario.readers_per_day / DAY_S, end, || {
        Event::ReaderStart
    });
    queue.periodic(DAY_S, end, || Event::Gc);
    if let Policy::Cron { period_s } = policy {
        queue.periodic(period_s, end, || Event::ScheduledCompact);
    }

    while let Some((now, event)) = queue.pop() {
        sim.advance_listing(now);
        match event {
            Event::ReaderStart => sim.load(),
            Event::JobStart => {
                sim.load();
                let duration = sim
                    .rng
                    .lognormal(scenario.job_median_s, scenario.job_sigma)
                    .min(6.0 * 3600.0);
                queue.push(now + duration, Event::Drain);
            }
            Event::Drain => sim.drain(now, false),
            Event::ScheduledCompact => sim.drain(now, true),
            Event::Gc => sim.gc(now),
        }
    }
    sim.stats
}

/// Min-heap of (time, event) with FIFO order for equal times.
#[derive(Default)]
struct Queue {
    heap: BinaryHeap<std::cmp::Reverse<(OrdF64, u64)>>,
    events: Vec<Option<Event>>,
}
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct OrdF64(u64);
impl OrdF64 {
    fn new(f: f64) -> Self {
        OrdF64(f.to_bits()) // fine for non-negative times
    }
}
impl Queue {
    fn push(&mut self, at: f64, e: Event) {
        let seq = self.events.len() as u64;
        self.events.push(Some(e));
        self.heap.push(std::cmp::Reverse((OrdF64::new(at), seq)));
    }
    fn pop(&mut self) -> Option<(f64, Event)> {
        let std::cmp::Reverse((t, seq)) = self.heap.pop()?;
        Some((
            f64::from_bits(t.0),
            self.events[seq as usize].take().unwrap(),
        ))
    }
    fn poisson(&mut self, rng: &mut Rng, rate_per_s: f64, end: f64, e: impl Fn() -> Event) {
        if rate_per_s <= 0.0 {
            return;
        }
        let mut t = rng.exponential(rate_per_s);
        while t < end {
            self.push(t, e());
            t += rng.exponential(rate_per_s);
        }
    }
    fn periodic(&mut self, period: f64, end: f64, e: impl Fn() -> Event) {
        let mut t = period;
        while t < end {
            self.push(t, e());
            t += period;
        }
    }
}

#[derive(Clone, Default)]
struct Bitset(Vec<u64>);

impl Bitset {
    fn set(&mut self, i: usize) {
        if self.0.len() <= i / 64 {
            self.0.resize(i / 64 + 1, 0);
        }
        self.0[i / 64] |= 1 << (i % 64);
    }
    fn union(&mut self, o: &Bitset) {
        if self.0.len() < o.0.len() {
            self.0.resize(o.0.len(), 0);
        }
        for (a, b) in self.0.iter_mut().zip(&o.0) {
            *a |= b;
        }
    }
    fn weighted_sum(&self, weights: &[f64]) -> f64 {
        let mut total = 0.0;
        for (w, &word) in self.0.iter().enumerate() {
            let mut bits = word;
            while bits != 0 {
                total += weights[w * 64 + bits.trailing_zeros() as usize];
                bits &= bits - 1;
            }
        }
        total
    }
}
