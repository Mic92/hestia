//! At what store size does the single manifest stop working, and what
//! replaces it? Per-job manifest overhead of three layouts as the store
//! grows from 5 GB to 100 TB:
//!
//!   mono      one document with every path and a global chunk table (today)
//!   shards    256 path shards + 256 chunk shards by hash prefix, a Bloom
//!             filter over chunk hashes so drains fetch fewer chunk shards
//!   segments  per-root path segments + per-pack chunk indexes. Dedup only
//!             against what the job already has in memory, so some chunks
//!             are stored twice (LOCAL_MISS)
//!
//!   cargo run --release --example manifest_sim
//!
//! One timeline: CI jobs arrive Poisson, each serves a closure then drains
//! new paths. GC runs daily. The store only grows, so one run sweeps all
//! sizes and per-job cost is bucketed by store size at the time. Pack
//! traffic is the same in every layout and not counted. Byte constants
//! are calibrated on production (4.4k paths / 28 GiB ≈ 25 MB wire).

mod common;
use common::Rng;
use std::collections::BTreeMap;

// network
const RTT_S: f64 = 0.066;
const PARALLEL: f64 = 32.0;
const THROUGHPUT_BPS: f64 = 72e6;
/// CBOR+zstd decode (and encode on drain) of the monolithic manifest, per wire byte.
const MONO_CODEC_BPS: f64 = 60e6;

// content
const CHUNK_BYTES: f64 = 64.0 * 1024.0;
const CHUNKS_PER_PACK: f64 = 1024.0;
/// A closure's chunks are spread over this many times the minimum number of packs.
const PACK_SCATTER: f64 = 3.0;
/// zstd on 64 KiB chunks.
const CHUNK_STORED_BYTES: f64 = 0.4 * CHUNK_BYTES;

// wire bytes, compressed
const HEAD_FIXED: f64 = 4096.0;
const PACK_ENTRY: f64 = 80.0;
const PATH_ENTRY: f64 = 300.0;
const MONO_PER_CHUNK: f64 = 48.0 + 5.0; // chunk table row + reference from the file tree
const SHARD_PATH_CHUNK_REF: f64 = 7.0; // (pack idx, idx in pack) + rewrites
const SHARD_CHUNK_ENTRY: f64 = 38.0; // hash + (pack, idx)
const PACK_INDEX_ENTRY: f64 = 44.0; // hash + offset + sizes
const DIGEST: f64 = 48.0;
const PATH_SHARDS: f64 = 256.0;
const CHUNK_SHARDS: f64 = 256.0;
const BLOOM_BITS_PER_KEY: f64 = 10.0;
const BLOOM_FP: f64 = 0.01;
const BLOOM_PARTS: f64 = 16.0;

// workload
const JOBS_PER_DAY: f64 = 200.0;
const SIM_DAYS: f64 = 1500.0;
/// narinfo lookups per job (closure minus what the runner already has).
const SERVE_PATHS: f64 = 3000.0;
const SERVE_CHUNKS_PER_PATH: f64 = 30.0;
const DRAIN_PATHS_MEDIAN: f64 = 40.0;
const DRAIN_PATHS_MEAN: f64 = DRAIN_PATHS_MEDIAN * 1.65; // lognormal σ = 1
const DRAIN_CHUNKS_PER_PATH_MEDIAN: f64 = 100.0;
/// Share of a drain's chunks that are new: own-code rebuild vs dependency bump, 50/50.
const NEW_SHARE_OWN: f64 = 0.85;
const NEW_SHARE_DEP: f64 = 0.15;

// segments
const ROOTS: f64 = 20.0;
const ROOT_PATHS: f64 = 20_000.0;
/// Roots a job loads: its own branch + default branch.
const SERVE_ROOTS: f64 = 2.0;
const SEG_FILTER_BITS_PER_PATH: f64 = 10.0;
/// Would-be dedup hits that local dedup misses and stores again.
const LOCAL_MISS: f64 = 0.2;

const SIZE_BUCKETS_GB: &[f64] = &[
    5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 10_000.0, 100_000.0,
];

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Layout {
    Mono,
    Shards,
    Segments,
}
const LAYOUTS: [(Layout, &str); 3] = [
    (Layout::Mono, "mono"),
    (Layout::Shards, "shards+bloom"),
    (Layout::Segments, "segments"),
];

// ---------------------------------------------------------------- cost model

/// Seconds, bytes and requests of one phase. A "round" is n parallel
/// requests that must all finish before the next round starts.
#[derive(Default, Clone, Copy)]
struct Cost {
    secs: f64,
    bytes: f64,
    requests: f64,
}

fn round(requests: f64, bytes: f64) -> Cost {
    if requests <= 0.0 {
        return Cost::default();
    }
    let waves = (requests / PARALLEL).ceil();
    Cost {
        secs: waves * RTT_S + bytes / THROUGHPUT_BPS,
        bytes,
        requests,
    }
}
fn codec(bytes: f64) -> Cost {
    Cost {
        secs: bytes / MONO_CODEC_BPS,
        ..Cost::default()
    }
}

impl std::ops::Add for Cost {
    type Output = Cost;
    fn add(self, o: Cost) -> Cost {
        Cost {
            secs: self.secs + o.secs,
            bytes: self.bytes + o.bytes,
            requests: self.requests + o.requests,
        }
    }
}
impl std::ops::AddAssign for Cost {
    fn add_assign(&mut self, o: Cost) {
        *self = *self + o;
    }
}

/// Expected distinct bins hit by n balls.
fn distinct(bins: f64, n: f64) -> f64 {
    bins * (1.0 - (1.0 - 1.0 / bins).powf(n))
}

#[derive(Clone, Copy)]
struct Store {
    chunks: f64,
    paths: f64,
}

struct Drain {
    paths: f64,
    chunks: f64,
    new_share: f64,
}

impl Store {
    fn gb(&self) -> f64 {
        self.chunks * CHUNK_BYTES / 1e9
    }
    fn packs(&self) -> f64 {
        (self.chunks / CHUNKS_PER_PACK).ceil().max(1.0)
    }
    fn chunks_per_path(&self) -> f64 {
        self.chunks / self.paths
    }

    fn mono_bytes(&self) -> f64 {
        HEAD_FIXED
            + self.packs() * PACK_ENTRY
            + self.paths * PATH_ENTRY
            + self.chunks * MONO_PER_CHUNK
    }

    fn shard_head_bytes(&self) -> f64 {
        HEAD_FIXED + self.packs() * PACK_ENTRY + (PATH_SHARDS + CHUNK_SHARDS + BLOOM_PARTS) * DIGEST
    }
    fn path_shard_bytes(&self) -> f64 {
        self.paths * (PATH_ENTRY + self.chunks_per_path() * SHARD_PATH_CHUNK_REF) / PATH_SHARDS
    }
    fn chunk_shard_bytes(&self) -> f64 {
        self.chunks * SHARD_CHUNK_ENTRY / CHUNK_SHARDS
    }
    fn bloom_part_bytes(&self) -> f64 {
        self.chunks * BLOOM_BITS_PER_KEY / 8.0 / BLOOM_PARTS
    }

    fn pack_index_bytes() -> f64 {
        CHUNKS_PER_PACK * PACK_INDEX_ENTRY
    }
    /// Pack indexes for every pack a served closure touches (mono has locations inline).
    fn closure_pack_indexes(&self) -> Cost {
        let packs = (SERVE_PATHS * SERVE_CHUNKS_PER_PATH / CHUNKS_PER_PACK * PACK_SCATTER)
            .ceil()
            .min(self.packs());
        round(packs, packs * Self::pack_index_bytes())
    }
}

fn segment_bytes(paths: f64, chunks_per_path: f64) -> f64 {
    paths * (PATH_ENTRY + chunks_per_path * SHARD_PATH_CHUNK_REF)
}
/// Uncompacted drain segments per root, halfway between daily GCs.
fn pending_per_root() -> f64 {
    JOBS_PER_DAY / ROOTS / 2.0
}
fn segments_head_bytes() -> f64 {
    let segments = ROOTS * (1.0 + pending_per_root());
    let filter_bits =
        ROOTS * (ROOT_PATHS + pending_per_root() * DRAIN_PATHS_MEAN) * SEG_FILTER_BITS_PER_PATH;
    HEAD_FIXED + segments * DIGEST + filter_bits / 8.0
}
/// All segments of `roots` roots: one compacted + the pending drain segments each.
fn segments_of(roots: f64, chunks_per_path: f64) -> Cost {
    let n = roots * (1.0 + pending_per_root());
    let bytes = roots
        * (segment_bytes(ROOT_PATHS, chunks_per_path)
            + pending_per_root() * segment_bytes(DRAIN_PATHS_MEAN, chunks_per_path));
    round(n, bytes)
}

fn serve(layout: Layout, s: &Store) -> Cost {
    match layout {
        Layout::Mono => round(1.0, s.mono_bytes()) + codec(s.mono_bytes()),
        Layout::Shards => {
            let shards = distinct(PATH_SHARDS, SERVE_PATHS);
            round(1.0, s.shard_head_bytes())
                + round(shards, shards * s.path_shard_bytes())
                + s.closure_pack_indexes()
        }
        Layout::Segments => {
            round(1.0, segments_head_bytes())
                + segments_of(SERVE_ROOTS, s.chunks_per_path())
                + s.closure_pack_indexes()
        }
    }
}

fn drain(layout: Layout, s: &Store, d: &Drain) -> Cost {
    let new = d.chunks * d.new_share;
    let existing = d.chunks - new;
    match layout {
        // manifest is in memory from serve, dedup is free, commit re-uploads it whole
        Layout::Mono => {
            let bytes = s.mono_bytes() + new * MONO_PER_CHUNK;
            round(1.0, bytes) + codec(bytes)
        }
        Layout::Shards => {
            let after = Store {
                chunks: s.chunks + new,
                paths: s.paths + d.paths,
            };
            // read: bloom, then chunk shards for hashes the filter says may exist
            let lookups = existing + new * BLOOM_FP;
            let read_shards = distinct(CHUNK_SHARDS, lookups);
            // write: shards and bloom parts that received new chunks, path shards for new paths, head
            let w_chunk = distinct(CHUNK_SHARDS, new);
            let w_path = distinct(PATH_SHARDS, d.paths);
            let w_bloom = distinct(BLOOM_PARTS, new);
            round(BLOOM_PARTS, BLOOM_PARTS * s.bloom_part_bytes())
                + round(read_shards, read_shards * s.chunk_shard_bytes())
                + round(
                    w_chunk + w_path + w_bloom,
                    w_chunk * after.chunk_shard_bytes()
                        + w_path * after.path_shard_bytes()
                        + w_bloom * after.bloom_part_bytes(),
                )
                + round(1.0, after.shard_head_bytes())
        }
        // no lookups. The price is uploading LOCAL_MISS of the existing chunks again
        Layout::Segments => {
            round(1.0, existing * LOCAL_MISS * CHUNK_STORED_BYTES)
                + round(1.0, segment_bytes(d.paths, d.chunks / d.paths))
                + round(1.0, segments_head_bytes())
        }
    }
}

/// Upper bound: GC reads everything it owns and rewrites it.
fn gc(layout: Layout, s: &Store) -> Cost {
    match layout {
        Layout::Mono => round(1.0, s.mono_bytes()) + round(1.0, s.mono_bytes()),
        Layout::Shards => {
            let docs = PATH_SHARDS + CHUNK_SHARDS + BLOOM_PARTS;
            let doc_bytes = PATH_SHARDS * s.path_shard_bytes()
                + CHUNK_SHARDS * s.chunk_shard_bytes()
                + BLOOM_PARTS * s.bloom_part_bytes();
            let indexes = s.packs() * Store::pack_index_bytes();
            round(1.0, s.shard_head_bytes())
                + round(docs + s.packs(), doc_bytes + indexes)
                + round(docs, doc_bytes)
        }
        // O(live roots), not O(store): read all segments, write one per root
        Layout::Segments => {
            let cpp = s.chunks_per_path();
            round(1.0, segments_head_bytes())
                + segments_of(ROOTS, cpp)
                + round(ROOTS, ROOTS * segment_bytes(ROOT_PATHS, cpp))
                + round(1.0, segments_head_bytes())
        }
    }
}

// ---------------------------------------------------------------- timeline

#[derive(Default)]
struct Bucket {
    jobs: f64,
    serve: Cost,
    drain: Cost,
    gcs: f64,
    gc: Cost,
}

fn bucket_of(gb: f64) -> Option<usize> {
    SIZE_BUCKETS_GB
        .iter()
        .position(|&b| gb >= b / 1.5 && gb < b * 1.5)
}

fn main() {
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let initial_chunks = 2.5e9 / CHUNK_BYTES;
    let mut store = Store {
        chunks: initial_chunks,
        paths: initial_chunks / 30.0,
    };
    let mut buckets: BTreeMap<(usize, Layout), Bucket> = BTreeMap::new();
    let mut duplicated_chunks = 0.0;
    let mut inflation: BTreeMap<usize, (f64, f64)> = BTreeMap::new();

    let mut day = 0.0;
    let mut next_gc = 1.0;
    let mut next_job = rng.exponential(JOBS_PER_DAY);
    while day < SIM_DAYS {
        let bucket = bucket_of(store.gb());
        if next_gc <= next_job {
            day = next_gc;
            next_gc += 1.0;
            if let Some(b) = bucket {
                for (layout, _) in LAYOUTS {
                    let e = buckets.entry((b, layout)).or_default();
                    e.gc += gc(layout, &store);
                    e.gcs += 1.0;
                }
            }
            continue;
        }
        day = next_job;
        next_job += rng.exponential(JOBS_PER_DAY);

        let paths = rng.lognormal(DRAIN_PATHS_MEDIAN, 1.0).round().max(1.0);
        let d = Drain {
            paths,
            chunks: (paths * rng.lognormal(DRAIN_CHUNKS_PER_PATH_MEDIAN, 1.2))
                .round()
                .max(1.0),
            new_share: if rng.uniform() < 0.5 {
                NEW_SHARE_OWN
            } else {
                NEW_SHARE_DEP
            },
        };
        if let Some(b) = bucket {
            for (layout, _) in LAYOUTS {
                let e = buckets.entry((b, layout)).or_default();
                e.jobs += 1.0;
                e.serve += serve(layout, &store);
                e.drain += drain(layout, &store, &d);
            }
            let e = inflation.entry(b).or_default();
            e.0 += duplicated_chunks / store.chunks;
            e.1 += 1.0;
        }
        store.chunks += d.chunks * d.new_share;
        store.paths += d.paths;
        duplicated_chunks += d.chunks * (1.0 - d.new_share) * LOCAL_MISS;
    }

    println!(
        "per-job manifest overhead, pack traffic excluded; serve = {SERVE_PATHS} lookups, drain ≈ {DRAIN_PATHS_MEDIAN} paths × {DRAIN_CHUNKS_PER_PATH_MEDIAN} chunks\n"
    );
    println!(
        "{:>9} {:<13} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "store", "layout", "serve s", "drain s", "job s", "job MB", "gc s", "gc MB"
    );
    for (b, &gb) in SIZE_BUCKETS_GB.iter().enumerate() {
        for (layout, name) in LAYOUTS {
            let Some(e) = buckets.get(&(b, layout)) else {
                continue;
            };
            println!(
                "{:>6} GB {:<13} {:>8.2} {:>8.2} {:>8.2} {:>8.0} {:>8.1} {:>8.0}",
                gb,
                name,
                e.serve.secs / e.jobs,
                e.drain.secs / e.jobs,
                (e.serve.secs + e.drain.secs) / e.jobs,
                (e.serve.bytes + e.drain.bytes) / e.jobs / 1e6,
                e.gc.secs / e.gcs.max(1.0),
                e.gc.bytes / e.gcs.max(1.0) / 1e6,
            );
        }
        println!();
    }
    println!(
        "segments: chunks stored twice because of LOCAL_MISS = {LOCAL_MISS}, as share of unique chunks"
    );
    for (b, (sum, n)) in &inflation {
        println!("{:>6} GB  +{:.1} %", SIZE_BUCKETS_GB[*b], 100.0 * sum / n);
    }
}
