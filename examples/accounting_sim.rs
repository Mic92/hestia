//! GC repacks a pack when its live share falls under a threshold. Packs
//! are shared across roots, so "live" is the union of what every root's
//! segment references, and per-segment counts are not additive. What
//! should a segment's pack-table row carry so GC can decide cheaply and
//! exactly?
//!
//!   cargo run --release --example accounting_sim [tb…]     default: 1 10 100
//!
//!   bitmap  a bitset of referenced chunks per row, OR them per pack      exact, 128 B/row
//!   owner   only the row of the root that wrote the pack counts          4 B/row
//!   hybrid  a u32 count per row decides most packs (sum ≤ threshold → sparse,
//!           max ≥ threshold → dense). Bitsets are fetched only for the rest   exact
//!
//! Earlier rounds also tried walking chunk lists (3 GB of input at
//! 100 TB), clamped sums, sampled bitsets and HyperLogLog (all inexact)
//! and run-length lists (exact but 3× the bytes of hybrid).
//!
//! Errors are against the true union: "missed" = a sparse pack kept
//! (costs storage), "wrong" = a dense pack repacked (costs work).

mod common;
use common::Rng;
use std::time::Instant;

const CHUNK_BYTES: u64 = 64 << 10;
const PACK_CHUNKS: usize = 1024;
const PACK_BYTES: u64 = CHUNK_BYTES * PACK_CHUNKS as u64;
const THRESHOLD: f64 = 0.5;
/// Chance that a pack is referenced from further roots (up to 4) …
const SHARED_P: f64 = 0.35;
/// … and that such a root references mostly the owner's paths rather than its own block.
const SHARED_SAME_PATHS_P: f64 = 0.6;

const WORDS: usize = PACK_CHUNKS / 64;
type Bits = [u64; WORDS];

/// One pack-table row: root X's segment references these chunks of `pack`.
struct Row {
    pack: usize,
    is_owner: bool,
    bits: Bits,
}

fn main() {
    for tb in common::tb_args(&[1.0, 10.0, 100.0]) {
        simulate(tb);
    }
}

fn simulate(tb: f64) {
    let packs = (tb * (1u64 << 40) as f64 / PACK_BYTES as f64) as usize;
    let mut rng = Rng(0x9E3779B97F4A7C15 ^ packs as u64);
    let (rows, true_live) = generate(packs, &mut rng);
    let sparse_packs = true_live.iter().filter(|&&n| is_sparse(n)).count();
    println!(
        "\n== {tb} TB: {packs} packs, {} rows ({:.2} roots/pack), {sparse_packs} sparse ({:.0} %)",
        rows.len(),
        rows.len() as f64 / packs as f64,
        100.0 * sparse_packs as f64 / packs as f64
    );
    println!(
        "{:8} {:>7} {:>10} {:>9} {:>6} {:>16} {:>16}",
        "algo", "B/row", "input MB", "mem MB", "secs", "missed (%sparse)", "wrong (%dense)"
    );
    let report = |name: &str,
                  row_bytes: &str,
                  input: f64,
                  mem: f64,
                  t: Instant,
                  verdict: &[bool]| {
        let (mut missed, mut wrong) = (0, 0);
        for (&sparse, &n) in verdict.iter().zip(&true_live) {
            match (is_sparse(n), sparse) {
                (true, false) => missed += 1,
                (false, true) => wrong += 1,
                _ => {}
            }
        }
        println!(
            "{name:8} {row_bytes:>7} {:>10.1} {:>9.1} {:>6.2} {missed:>8} ({:>4.1} %) {wrong:>8} ({:>4.1} %)",
            input / 1e6,
            mem / 1e6,
            t.elapsed().as_secs_f64(),
            100.0 * missed as f64 / sparse_packs.max(1) as f64,
            100.0 * wrong as f64 / (packs - sparse_packs).max(1) as f64,
        );
    };

    // bitmap
    let t = Instant::now();
    let mut union = vec![[0u64; WORDS]; packs];
    for r in &rows {
        or_into(&mut union[r.pack], &r.bits);
    }
    let verdict: Vec<bool> = union.iter().map(|b| is_sparse(popcount(b))).collect();
    report(
        "bitmap",
        "128",
        (rows.len() * 128) as f64,
        (packs * 128) as f64,
        t,
        &verdict,
    );
    drop(union);

    // owner
    let t = Instant::now();
    let mut verdict = vec![false; packs];
    for r in rows.iter().filter(|r| r.is_owner) {
        verdict[r.pack] = is_sparse(popcount(&r.bits));
    }
    report(
        "owner",
        "4",
        (rows.len() * 4) as f64,
        (packs * 4) as f64,
        t,
        &verdict,
    );

    // hybrid
    let t = Instant::now();
    let mut sum = vec![0u32; packs];
    let mut max = vec![0u32; packs];
    for r in &rows {
        let n = popcount(&r.bits);
        sum[r.pack] = (sum[r.pack] + n).min(PACK_CHUNKS as u32);
        max[r.pack] = max[r.pack].max(n);
    }
    let undecided: Vec<bool> = (0..packs)
        .map(|p| !is_sparse(sum[p]) && is_sparse(max[p]))
        .collect();
    let n_undecided = undecided.iter().filter(|&&u| u).count();
    let mut union: std::collections::HashMap<usize, Bits> =
        std::collections::HashMap::with_capacity(n_undecided);
    let mut bitsets_fetched = 0;
    for r in rows.iter().filter(|r| undecided[r.pack]) {
        or_into(union.entry(r.pack).or_insert([0; WORDS]), &r.bits);
        bitsets_fetched += 1;
    }
    let verdict: Vec<bool> = (0..packs)
        .map(|p| {
            if undecided[p] {
                is_sparse(popcount(&union[&p]))
            } else {
                is_sparse(sum[p])
            }
        })
        .collect();
    report(
        "hybrid",
        "4+128",
        (rows.len() * 4 + bitsets_fetched * 128) as f64,
        (packs * 8 + n_undecided * 128) as f64,
        t,
        &verdict,
    );
    println!(
        "         hybrid fetched bitsets for {n_undecided} packs ({:.1} %)",
        100.0 * n_undecided as f64 / packs as f64
    );
}

/// A store where each pack has one owner row and sometimes rows from
/// other roots. Owners drop whole paths (runs of ~12 chunks), not single
/// chunks. Returns rows in random order and the true live count per pack.
fn generate(packs: usize, rng: &mut Rng) -> (Vec<Row>, Vec<u32>) {
    let mut rows = Vec::with_capacity(packs * 3 / 2);
    let mut true_live = Vec::with_capacity(packs);
    for pack in 0..packs {
        let live_share = if rng.uniform() < 0.5 {
            1.0
        } else {
            rng.uniform()
        };
        let owner = path_runs(rng, |rng, _| rng.uniform() < live_share);
        let mut union = owner;
        rows.push(Row {
            pack,
            is_owner: true,
            bits: owner,
        });

        for _ in 0..4 {
            if rng.uniform() >= SHARED_P {
                break;
            }
            let bits = if rng.uniform() < SHARED_SAME_PATHS_P {
                // another system or branch holding mostly the same paths
                path_runs(rng, |rng, i| bit(&owner, i) && rng.uniform() < 0.7)
            } else {
                // a foreign root dedup'd one block of paths into this pack
                let start = rng.below(PACK_CHUNKS);
                let end = (start + rng.below(PACK_CHUNKS / 4)).min(PACK_CHUNKS);
                let mut b = [0; WORDS];
                (start..end).for_each(|i| set(&mut b, i));
                b
            };
            or_into(&mut union, &bits);
            rows.push(Row {
                pack,
                is_owner: false,
                bits,
            });
        }
        true_live.push(popcount(&union));
    }
    for i in (1..rows.len()).rev() {
        rows.swap(i, rng.below(i + 1));
    }
    (rows, true_live)
}

/// Fills a bitset path by path (runs of 1–24 chunks). `keep(rng, first_chunk)` decides per path.
fn path_runs(rng: &mut Rng, mut keep: impl FnMut(&mut Rng, usize) -> bool) -> Bits {
    let mut b = [0; WORDS];
    let mut i = 0;
    while i < PACK_CHUNKS {
        let end = (i + 1 + rng.below(24)).min(PACK_CHUNKS);
        if keep(rng, i) {
            (i..end).for_each(|j| set(&mut b, j));
        }
        i = end;
    }
    b
}

fn is_sparse(live_chunks: u32) -> bool {
    (live_chunks as f64) < THRESHOLD * PACK_CHUNKS as f64
}
fn popcount(b: &Bits) -> u32 {
    b.iter().map(|w| w.count_ones()).sum()
}
fn or_into(a: &mut Bits, b: &Bits) {
    for w in 0..WORDS {
        a[w] |= b[w];
    }
}
fn bit(b: &Bits, i: usize) -> bool {
    b[i / 64] >> (i % 64) & 1 == 1
}
fn set(b: &mut Bits, i: usize) {
    b[i / 64] |= 1 << (i % 64);
}
