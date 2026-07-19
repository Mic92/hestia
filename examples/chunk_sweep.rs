//! Experiment: sweep FastCDC chunk sizes over local NixOS generations.
//!
//! Ingests the closures of the system profile generations on this machine
//! with several FastCDC parameter sets and reports, for each: unique chunk
//! count, packed (compressed) bytes, encoded manifest size, chunking and
//! compression time, and the incremental bytes each generation would have
//! uploaded. Used to decide whether the pinned 64 KiB average chunk size
//! should change.
//!
//! Usage (run with pueue, this chunks tens of GB):
//!   cargo run --release --example chunk_sweep -- \
//!     [--profiles /nix/var/nix/profiles] [--last N] [--avg-kib 32,64,128,256] \
//!     [--roots /nix/store/aaa-foo,/nix/store/bbb-foo]
//!
//! `--roots` replaces the profile generations with an explicit ordered list
//! of store paths (e.g. the same package at successive nixpkgs revisions).
//!
//! Requires read access to /nix/store and /nix/var/nix/db/db.sqlite.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hestia::chunker::{ChunkParams, PackBuilder, chunk_path_with};
use hestia::manifest::{
    ChunkHash, ChunkLocation, Manifest, PackHash, PackInfo, PathEntry, PathHash, Root,
};
use hestia::pathinfo::{DEFAULT_DB_PATH, Lookup, PathInfo, StoreDatabase};
use hestia::refnorm::RefTable;

const PACK_TARGET_SIZE: u64 = 64 * 1024 * 1024;

struct Args {
    profiles: String,
    last: usize,
    avg_kib: Vec<u32>,
    roots: Vec<String>,
    /// Dump the cumulative manifest after each generation to this directory
    /// (`manifest-avg<K>-gen<N>.bin`), for offline manifest-format experiments.
    dump_dir: Option<String>,
}

fn parse_args() -> Args {
    let mut args = Args {
        profiles: "/nix/var/nix/profiles".into(),
        last: 5,
        avg_kib: vec![32, 64, 128, 256],
        roots: Vec::new(),
        dump_dir: None,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let value = iter.next().unwrap_or_else(|| {
            eprintln!("missing value for {flag}");
            std::process::exit(2);
        });
        match flag.as_str() {
            "--profiles" => args.profiles = value,
            "--last" => args.last = value.parse().expect("--last takes a number"),
            "--avg-kib" => {
                args.avg_kib = value
                    .split(',')
                    .map(|s| s.trim().parse().expect("--avg-kib takes numbers"))
                    .collect();
            }
            "--roots" => {
                args.roots = value.split(',').map(|s| s.trim().to_string()).collect();
            }
            "--dump-dir" => args.dump_dir = Some(value),
            other => {
                eprintln!("unknown flag {other}");
                std::process::exit(2);
            }
        }
    }
    args
}

/// `system-<N>-link` entries under the profiles dir, sorted by generation
/// number, resolved to absolute store paths.
fn generation_roots(profiles: &str, last: usize) -> Vec<(u64, String)> {
    let mut generations: Vec<(u64, String)> = std::fs::read_dir(profiles)
        .expect("read profiles dir")
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().into_string().ok()?;
            let number: u64 = name
                .strip_prefix("system-")?
                .strip_suffix("-link")?
                .parse()
                .ok()?;
            let target = std::fs::canonicalize(entry.path()).ok()?;
            Some((number, target.to_string_lossy().into_owned()))
        })
        .collect();
    generations.sort_unstable();
    if generations.len() > last {
        generations.drain(..generations.len() - last);
    }
    generations
}

fn human_mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs()
}

struct SweepResult {
    params: ChunkParams,
    paths: usize,
    total_nar_bytes: u64,
    unique_chunks: usize,
    unique_chunk_bytes: u64,
    packed_bytes: u64,
    packs: usize,
    manifest_bytes: usize,
    chunk_time: Duration,
    pack_time: Duration,
    /// (generation number, new packed bytes contributed by that generation)
    generation_deltas: Vec<(u64, u64)>,
}

async fn sweep(
    params: ChunkParams,
    generations: &[(u64, Vec<PathInfo>)],
    store_dir: &str,
    dump_dir: Option<&str>,
) -> SweepResult {
    let mut paths: BTreeMap<PathHash, PathEntry> = BTreeMap::new();
    let mut chunks: BTreeMap<ChunkHash, ChunkLocation> = BTreeMap::new();
    let mut packs: BTreeMap<PackHash, PackInfo> = BTreeMap::new();
    let mut roots: BTreeMap<String, Root> = BTreeMap::new();

    let mut builder = PackBuilder::new();
    let mut total_nar_bytes = 0u64;
    let mut unique_chunk_bytes = 0u64;
    let mut packed_bytes = 0u64;
    let mut chunk_time = Duration::ZERO;
    let mut pack_time = Duration::ZERO;
    let mut generation_deltas = Vec::new();

    // Locations of chunks in the pack currently being built are only known
    // once the pack is finished (its hash names it), so finished packs flush
    // into the chunks map here.
    let flush_pack = |builder: &mut PackBuilder,
                      chunks: &mut BTreeMap<ChunkHash, ChunkLocation>,
                      packs: &mut BTreeMap<PackHash, PackInfo>,
                      packed_bytes: &mut u64| {
        if builder.is_empty() {
            return;
        }
        let pack = std::mem::take(builder).finish();
        *packed_bytes += pack.data.len() as u64;
        packs.insert(
            pack.hash,
            PackInfo {
                size: pack.data.len() as u64,
                created: now_unix(),
                tier: 0,
            },
        );
        for (hash, location) in pack.locations() {
            chunks.entry(hash).or_insert(location);
        }
    };

    for (generation, infos) in generations {
        let packed_before = packed_bytes + builder.compressed_size();
        let mut root_paths: BTreeSet<PathHash> = BTreeSet::new();

        for info in infos {
            let path_hash = info.path_hash();
            root_paths.insert(path_hash);
            if paths.contains_key(&path_hash) {
                continue;
            }
            total_nar_bytes += info.nar_size;

            let abs_path = format!("{store_dir}/{}", info.store_path);
            let refs = RefTable::new(&info.references);
            let started = Instant::now();
            let chunked = match chunk_path_with(&abs_path, &refs, params).await {
                Ok(chunked) => chunked,
                Err(err) => {
                    eprintln!("skipping {abs_path}: {err}");
                    continue;
                }
            };
            chunk_time += started.elapsed();

            let started = Instant::now();
            for chunk in &chunked.chunks {
                if chunks.contains_key(&chunk.hash) {
                    continue;
                }
                // PackBuilder skips duplicates within the open pack itself.
                if builder.add(chunk).expect("compress chunk") {
                    unique_chunk_bytes += chunk.data.len() as u64;
                }
                if builder.compressed_size() >= PACK_TARGET_SIZE {
                    flush_pack(&mut builder, &mut chunks, &mut packs, &mut packed_bytes);
                }
            }
            pack_time += started.elapsed();

            paths.insert(
                path_hash,
                PathEntry {
                    store_path: info.store_path.clone(),
                    nar_hash: info.nar_hash,
                    nar_size: info.nar_size,
                    references: info.references.clone(),
                    ca: info.ca.clone(),
                    deriver: info.deriver.clone(),
                    tree: chunked.tree,
                    last_reachable: 0,
                    last_pushed: 0,
                },
            );
        }

        // Delta this generation would have uploaded: everything packed since
        // the previous generation finished (current builder buffer included).
        let packed_after = packed_bytes + builder.compressed_size();
        generation_deltas.push((*generation, packed_after - packed_before));
        roots.insert(
            format!("gen-{generation}"),
            Root {
                paths: root_paths,
                updated: now_unix(),
                run_id: None,
            },
        );

        if let Some(dir) = dump_dir {
            // Snapshot of the cumulative manifest as of this generation. The
            // open pack's chunks are not yet located, which slightly
            // undercounts the last generation's chunk map — fine for
            // format experiments.
            let snapshot = Manifest {
                paths: paths.clone(),
                chunks: chunks.clone(),
                packs: packs.clone(),
                roots: roots.clone(),
            };
            std::fs::create_dir_all(dir).expect("create dump dir");
            let file = format!(
                "{dir}/manifest-avg{}-gen{generation}.bin",
                params.avg / 1024
            );
            std::fs::write(&file, snapshot.encode().expect("encode snapshot"))
                .expect("write manifest snapshot");
        }
    }
    flush_pack(&mut builder, &mut chunks, &mut packs, &mut packed_bytes);

    let manifest = Manifest {
        paths,
        chunks,
        packs,
        roots,
    };
    let manifest_bytes = manifest.encode().expect("encode manifest").len();

    SweepResult {
        params,
        paths: manifest.paths.len(),
        total_nar_bytes,
        unique_chunks: manifest.chunks.len(),
        unique_chunk_bytes,
        packed_bytes,
        packs: manifest.packs.len(),
        manifest_bytes,
        chunk_time,
        pack_time,
        generation_deltas,
    }
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let generations: Vec<(u64, String)> = if args.roots.is_empty() {
        generation_roots(&args.profiles, args.last)
    } else {
        args.roots
            .iter()
            .enumerate()
            .map(|(index, root)| (index as u64 + 1, root.clone()))
            .collect()
    };
    if generations.is_empty() {
        eprintln!("no system-*-link generations found in {}", args.profiles);
        std::process::exit(1);
    }
    eprintln!(
        "generations: {}",
        generations
            .iter()
            .map(|(n, _)| n.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let db = StoreDatabase::new(DEFAULT_DB_PATH);
    let store_dir = db.store_dir().to_string();
    let closures: Vec<(u64, Vec<PathInfo>)> = generations
        .iter()
        .map(|(number, root)| {
            let infos: Vec<PathInfo> = db
                .query_closure([root.clone()])
                .expect("query closure")
                .into_iter()
                .filter_map(|(path, lookup)| match lookup {
                    Lookup::Found(info) => Some(*info),
                    other => {
                        eprintln!("skipping {path}: {other:?}");
                        None
                    }
                })
                .collect();
            eprintln!("generation {number}: {} paths", infos.len());
            (*number, infos)
        })
        .collect();

    println!(
        "{:>8} {:>8} {:>10} {:>10} {:>10} {:>6} {:>10} {:>9} {:>9}",
        "avg KiB",
        "chunks",
        "uniq MiB",
        "packed MiB",
        "manif KiB",
        "packs",
        "paths",
        "chunk s",
        "zstd s"
    );
    for avg_kib in &args.avg_kib {
        let params = ChunkParams {
            min: avg_kib * 1024 / 4,
            avg: avg_kib * 1024,
            max: avg_kib * 1024 * 4,
        };
        let result = sweep(params, &closures, &store_dir, args.dump_dir.as_deref()).await;
        println!(
            "{:>8} {:>8} {:>10.1} {:>10.1} {:>10.1} {:>6} {:>10} {:>9.1} {:>9.1}",
            avg_kib,
            result.unique_chunks,
            human_mib(result.unique_chunk_bytes),
            human_mib(result.packed_bytes),
            result.manifest_bytes as f64 / 1024.0,
            result.packs,
            result.paths,
            result.chunk_time.as_secs_f64(),
            result.pack_time.as_secs_f64(),
        );
        for (generation, delta) in &result.generation_deltas {
            println!(
                "         gen {generation}: +{:.1} MiB packed",
                human_mib(*delta)
            );
        }
        eprintln!(
            "  (total NAR {:.1} MiB, params min/avg/max = {}/{}/{} KiB)",
            human_mib(result.total_nar_bytes),
            result.params.min / 1024,
            result.params.avg / 1024,
            result.params.max / 1024
        );
    }
}
