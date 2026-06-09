//! `hestia gc`: mark/sweep garbage collection over the GHA cache.
//!
//! Runs as a scheduled workflow on the default branch.
//! The flow is split into a read-only **plan** (which doubles as `--dry-run`
//! output) and an **execute** phase made of individually resumable steps:
//!
//! ```text
//! ① Reconcile   REST list "pack-*" → packs GitHub already evicted → drop
//!               their chunk locations; affected paths are healed (dropped;
//!               they re-upload from the local store on the next push)
//! ② Mark        paths reachable from live roots → last_reachable = now
//! ③ Sweep       roots not updated within RootTTL → drop
//!               paths unreachable AND out of PathGrace AND out of PushTTL → drop
//! ④ Plan        per pack: liveness = live chunk bytes / total bytes
//!               liveness < MinLiveness or too many volatile packs → repack
//!               fully live but idle packs → touch (1-byte Range read)
//! ⑤ Execute     Range-copy verified frames → upload new packs → commit
//!               manifest (SaveMutable; re-plan on conflict) → REST DELETE
//!               replaced/garbage packs and orphans
//! ```
//!
//! Crash-safe ordering: replaced packs stay referenced by the manifest until
//! the commit lands; only then are they deleted. A crash at any point leaves
//! either the old state or a state the next GC run converges from (orphaned
//! uploads are cleaned by the orphan sweep once they are old enough).

use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;

use crate::chunker::{
    self, PackBuilder, coalesce_adjacent, extract_chunk, flatten_tree, pack_cache_key,
};
use crate::cli::GcArgs;
use crate::drain::human_bytes;
use crate::gha::rest::{CacheEntry, RestClient};
use crate::gha::savemutable::{MutableEntry, SaveMutable};
use crate::gha::twirp::{DownloadUrl, TwirpClient};
use crate::gha::{Error as GhaError, blob};
use crate::manifest::{
    ChunkHash, ChunkLocation, FileSystemObject, Hash32, Manifest, PackHash, PackInfo, PathHash,
};
use crate::pipeline::{MANIFEST_PREFIX, now_unix, upload_pack};

pub const SECS_PER_HOUR: u64 = 3_600;
pub const SECS_PER_DAY: u64 = 86_400;

/// Pack tier for chunks that have not yet proven their stability.
pub const TIER_VOLATILE: u8 = 0;
/// Pack tier for chunks that survived [`GcPolicy::stable_threshold`] repacks.
pub const TIER_STABLE: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("GHA cache error: {0}")]
    Gha(#[from] GhaError),

    #[error("manifest error: {0}")]
    Manifest(#[from] crate::manifest::Error),

    #[error("chunk verification failed during repack: {0}")]
    Chunker(#[from] chunker::Error),

    #[error(
        "manifest lookup returned nothing, but {0} manifest version entries exist in the \
         REST listing; refusing to plan against a possibly stale empty view"
    )]
    ManifestLookupInconsistent(usize),
}

/// GC policy knobs.
#[derive(Debug, Clone)]
pub struct GcPolicy {
    /// Unreachable paths are kept this long after they were last reachable.
    /// Survives revert/flip-flop pushes.
    pub path_grace: u64,
    /// Paths are kept this long after their last push, reachable or not.
    pub push_ttl: u64,
    /// Roots not updated for this long are dropped (deleted branches).
    pub root_ttl: u64,
    /// Referenced packs not accessed for this long get a 1-byte LRU touch.
    pub touch_age: u64,
    /// Packs whose live-byte ratio falls below this get repacked.
    pub min_liveness: f64,
    /// Volatile (tier-0) packs above this count get consolidated.
    pub max_volatile_packs: usize,
    /// Chunks surviving this many repacks are promoted to the stable tier.
    pub stable_threshold: u32,
    /// Cache entries younger than this are never judged evicted or orphaned:
    /// a concurrent push may have uploaded them without having committed its
    /// manifest yet.
    pub min_age: u64,
    /// Repack output packs are sealed at this compressed size.
    pub pack_target_size: u64,
}

impl Default for GcPolicy {
    fn default() -> Self {
        Self {
            path_grace: 72 * SECS_PER_HOUR,
            push_ttl: 14 * SECS_PER_DAY,
            root_ttl: 14 * SECS_PER_DAY,
            touch_age: 4 * SECS_PER_DAY,
            min_liveness: 0.5,
            max_volatile_packs: 4,
            stable_threshold: 2,
            min_age: SECS_PER_HOUR,
            pack_target_size: crate::pipeline::PACK_TARGET_SIZE,
        }
    }
}

/// Parse a manifest version key (`<prefix><index>`) back into its index.
/// Only the canonical decimal form SaveMutable writes is accepted:
/// `str::parse` alone also accepts `m#+99` and `m#007`, which would let a
/// foreign same-prefix key inflate the newest-version computation.
fn parse_manifest_index(prefix: &str, key: &str) -> Option<u64> {
    let suffix = key.strip_prefix(prefix)?;
    let index: u64 = suffix.parse().ok()?;
    (suffix == index.to_string()).then_some(index)
}

/// Parse a `pack-<sha256 hex>` cache key back into the pack hash.
fn parse_pack_key(key: &str) -> Option<PackHash> {
    let hex = key.strip_prefix("pack-")?;
    if hex.len() != 64 {
        return None;
    }
    // Hestia only emits lowercase hex. `from_str_radix` would also accept
    // uppercase digits and a leading `+` — keys hestia never created, so
    // they belong to other workflows and must not parse.
    if !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(Hash32(bytes))
}

/// What the GitHub REST API reports about one `pack-*` cache entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackObservation {
    /// Full cache key.
    pub key: String,
    /// Pack hash parsed from the key (`None` for keys that merely share the
    /// prefix but are not hestia packs).
    pub pack: Option<PackHash>,
    /// Creation time (unix seconds, 0 if unparsable).
    pub created: u64,
    /// LRU clock (unix seconds, 0 if unparsable).
    pub last_accessed: u64,
}

impl PackObservation {
    fn from_entry(entry: &CacheEntry) -> Self {
        Self {
            pack: parse_pack_key(&entry.key),
            key: entry.key.clone(),
            created: entry.created_unix().unwrap_or(0),
            last_accessed: entry.last_accessed_unix().unwrap_or(0),
        }
    }
}

/// One chunk to copy out of a source pack during repack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkCopy {
    pub chunk: ChunkHash,
    /// Where the chunk currently lives.
    pub from: ChunkLocation,
}

/// One repack job: live chunks copied forward into a new pack of `tier`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepackJob {
    /// Output pack tier ([`TIER_VOLATILE`] or [`TIER_STABLE`]).
    pub tier: u8,
    /// Chunks to copy, ordered by (source pack, offset) for read locality
    /// and deterministic output pack content.
    pub copies: Vec<ChunkCopy>,
}

impl RepackJob {
    /// Compressed bytes that must be Range-read from source packs. Frames
    /// are copied without recompression, so this is also the total size of
    /// the output packs (the job seals and uploads a pack each time it
    /// reaches [`GcPolicy::pack_target_size`]). The actual output can be
    /// smaller when a source pack disappeared mid-run and its chunks were
    /// skipped.
    pub fn download_bytes(&self) -> u64 {
        self.copies
            .iter()
            .map(|copy| u64::from(copy.from.compressed_size))
            .sum()
    }
}

/// The complete read-only GC plan (also the `--dry-run` output).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcPlan {
    /// The clock all decisions were made against.
    pub now: u64,
    /// Packs the manifest references but GitHub no longer stores.
    pub evicted_packs: Vec<PackHash>,
    /// Paths dropped because their chunks were lost to eviction. They
    /// re-upload from the local store on the next push.
    pub heal_paths: Vec<PathHash>,
    /// Roots not updated within RootTTL (dead branches).
    pub drop_roots: Vec<String>,
    /// Paths that are unreachable and out of both grace and push TTL.
    pub drop_paths: Vec<PathHash>,
    /// Referenced packs that need an LRU touch, least valuable first
    /// (most valuable packs are touched last → evicted last under quota
    /// pressure).
    pub touch_packs: Vec<PackHash>,
    /// Repack jobs (at most one per output tier).
    pub repack_jobs: Vec<RepackJob>,
    /// Manifest packs that become garbage once the commit lands
    /// (fully dead packs + repack sources).
    pub delete_packs: Vec<PackHash>,
    /// Cache keys of hestia packs (`pack-<sha256>`) present in GitHub but
    /// referenced by no manifest, old enough to be sure no in-flight push
    /// still wants them. Keys that merely share the `pack-` prefix belong
    /// to other workflows and are never touched.
    pub orphan_keys: BTreeSet<String>,
}

impl GcPlan {
    pub fn download_bytes(&self) -> u64 {
        self.repack_jobs.iter().map(RepackJob::download_bytes).sum()
    }

    /// One-line human summary for logs and `--dry-run`.
    pub fn summary(&self) -> String {
        // Repacks copy compressed frames verbatim, so upload == download.
        format!(
            "evicted {} pack(s); heal {} path(s); drop {} root(s) + {} path(s); \
             repack {} job(s) ({} chunk(s), {} copied); \
             touch {} pack(s); delete {} pack(s) + {} orphan(s)",
            self.evicted_packs.len(),
            self.heal_paths.len(),
            self.drop_roots.len(),
            self.drop_paths.len(),
            self.repack_jobs.len(),
            self.repack_jobs
                .iter()
                .map(|job| job.copies.len())
                .sum::<usize>(),
            human_bytes(self.download_bytes()),
            self.touch_packs.len(),
            self.delete_packs.len(),
            self.orphan_keys.len(),
        )
    }
}

/// Chunk hashes referenced by any path tree in the manifest.
fn referenced_chunks(manifest: &Manifest) -> BTreeSet<ChunkHash> {
    manifest
        .paths
        .values()
        .flat_map(|entry| flatten_tree(&entry.tree))
        .filter_map(|(_, node)| match node {
            FileSystemObject::Regular(regular) => Some(&regular.contents.chunks),
            _ => None,
        })
        .flatten()
        .copied()
        .collect()
}

/// Paths whose trees reference chunks without a usable location
/// (chunk missing from the manifest, or located in a pack the manifest no
/// longer knows). These paths cannot be served and must be healed.
fn broken_paths(manifest: &Manifest) -> Vec<PathHash> {
    manifest
        .paths
        .iter()
        .filter(|(_, entry)| {
            flatten_tree(&entry.tree)
                .iter()
                .any(|(_, node)| match node {
                    FileSystemObject::Regular(regular) => {
                        regular.contents.chunks.iter().any(|chunk| {
                            manifest
                                .chunks
                                .get(chunk)
                                .is_none_or(|location| !manifest.packs.contains_key(&location.pack))
                        })
                    }
                    _ => false,
                })
        })
        .map(|(hash, _)| *hash)
        .collect()
}

/// Per-pack liveness statistics used by the planning phase.
struct PackStats {
    tier: u8,
    /// Total blob size (denominator of the liveness ratio).
    total_bytes: u64,
    /// Compressed bytes of chunks the manifest still locates in this pack.
    chunk_bytes: u64,
    /// Live chunks (referenced by surviving paths) and their bytes.
    live: Vec<ChunkCopy>,
    live_bytes: u64,
}

impl PackStats {
    fn liveness(&self) -> f64 {
        self.live_bytes as f64 / self.total_bytes.max(1) as f64
    }

    /// True when copying the live chunks would reproduce the pack
    /// byte-identically (all frames live, nothing pruned).
    fn fully_live(&self) -> bool {
        self.live_bytes == self.total_bytes && self.live_bytes == self.chunk_bytes
    }
}

/// Compute the GC plan. Pure function: read-only against `manifest` and
/// `observations`, all clock decisions taken against `now`.
pub fn plan(
    manifest: &Manifest,
    observations: &[PackObservation],
    now: u64,
    policy: &GcPolicy,
) -> GcPlan {
    let mut work = manifest.clone();
    let mut plan = GcPlan {
        now,
        ..GcPlan::default()
    };

    // The REST listing repeats a key once per ref, each entry with its own
    // clocks. Keep the *stalest* parsed LRU clock per pack: any copy going
    // idle warrants a touch, since each ref's entry is evicted
    // independently. 0 means no copy's clock parsed.
    let mut observed: BTreeMap<PackHash, u64> = BTreeMap::new();
    for observation in observations {
        let Some(pack) = observation.pack else {
            continue;
        };
        let last_accessed = observed.entry(pack).or_insert(observation.last_accessed);
        if *last_accessed == 0
            || (observation.last_accessed != 0 && observation.last_accessed < *last_accessed)
        {
            *last_accessed = observation.last_accessed;
        }
    }
    // Newest creation time per key across refs; 0 when any duplicate's
    // timestamp failed to parse. Deletion is by key across all refs, so a
    // key is only safe to judge by its newest entry.
    let mut newest_created: BTreeMap<&str, u64> = BTreeMap::new();
    for observation in observations.iter().filter(|o| o.pack.is_some()) {
        let created = newest_created
            .entry(observation.key.as_str())
            .or_insert(observation.created);
        *created = if *created == 0 || observation.created == 0 {
            0
        } else {
            (*created).max(observation.created)
        };
    }

    // ① Reconcile: packs GitHub already evicted
    // Packs younger than min_age are never judged evicted: they may have been
    // uploaded after the REST listing was taken (concurrent push).
    plan.evicted_packs = work
        .packs
        .iter()
        .filter(|(hash, info)| {
            !observed.contains_key(*hash) && now.saturating_sub(info.created) > policy.min_age
        })
        .map(|(hash, _)| *hash)
        .collect();
    for pack in &plan.evicted_packs {
        work.packs.remove(pack);
    }
    work.chunks
        .retain(|_, location| work.packs.contains_key(&location.pack));
    plan.heal_paths = broken_paths(&work);
    for path in &plan.heal_paths {
        work.paths.remove(path);
    }

    // ③a Sweep expired roots (before mark: dead roots must not mark)
    plan.drop_roots = work
        .roots
        .iter()
        .filter(|(_, root)| now.saturating_sub(root.updated) > policy.root_ttl)
        .map(|(key, _)| key.clone())
        .collect();
    for key in &plan.drop_roots {
        work.roots.remove(key);
    }

    // ② Mark + ③b sweep paths
    let reachable = work.reachable();
    plan.drop_paths = work
        .paths
        .iter()
        .filter(|(hash, entry)| {
            !reachable.contains(hash)
                && now.saturating_sub(entry.last_reachable) > policy.path_grace
                && now.saturating_sub(entry.last_pushed) > policy.push_ttl
        })
        .map(|(hash, _)| *hash)
        .collect();
    for path in &plan.drop_paths {
        work.paths.remove(path);
    }

    // ④ Pack liveness
    let live_chunks = referenced_chunks(&work);
    let mut stats: BTreeMap<PackHash, PackStats> = work
        .packs
        .iter()
        .map(|(hash, info)| {
            (
                *hash,
                PackStats {
                    tier: info.tier,
                    total_bytes: info.size,
                    chunk_bytes: 0,
                    live: Vec::new(),
                    live_bytes: 0,
                },
            )
        })
        .collect();
    for (chunk, location) in &work.chunks {
        let Some(pack_stats) = stats.get_mut(&location.pack) else {
            continue;
        };
        pack_stats.chunk_bytes += u64::from(location.compressed_size);
        if live_chunks.contains(chunk) {
            pack_stats.live_bytes += u64::from(location.compressed_size);
            pack_stats.live.push(ChunkCopy {
                chunk: *chunk,
                from: location.clone(),
            });
        }
    }

    // Fully dead packs are deleted outright; mostly dead packs are repacked.
    let mut dead_packs: Vec<PackHash> = Vec::new();
    let mut repack_sources: BTreeSet<PackHash> = BTreeSet::new();
    for (pack, pack_stats) in &stats {
        if pack_stats.live.is_empty() {
            dead_packs.push(*pack);
        } else if pack_stats.liveness() < policy.min_liveness {
            repack_sources.insert(*pack);
        }
    }

    // Consolidation: too many volatile packs → merge them all (this is what
    // keeps the daily-push pack count bounded).
    let volatile: Vec<PackHash> = stats
        .iter()
        .filter(|(_, pack_stats)| pack_stats.tier == TIER_VOLATILE && !pack_stats.live.is_empty())
        .map(|(pack, _)| *pack)
        .collect();
    if volatile.len() > policy.max_volatile_packs {
        repack_sources.extend(volatile);
    }

    // ④b Repack jobs, split by output tier
    let mut copies: Vec<ChunkCopy> = repack_sources
        .iter()
        .flat_map(|pack| stats[pack].live.iter().cloned())
        .collect();
    copies.sort_by_key(|copy| (copy.from.pack, copy.from.offset));
    let (stable, volatile): (Vec<ChunkCopy>, Vec<ChunkCopy>) = copies
        .into_iter()
        .partition(|copy| copy.from.repacks_survived + 1 >= policy.stable_threshold);
    if !stable.is_empty() {
        plan.repack_jobs.push(RepackJob {
            tier: TIER_STABLE,
            copies: stable,
        });
    }
    if !volatile.is_empty() {
        plan.repack_jobs.push(RepackJob {
            tier: TIER_VOLATILE,
            copies: volatile,
        });
    }

    // CAS no-op trap: a job whose copies reproduce a single fully-live source
    // pack byte-identically (frames are copied verbatim, in offset order)
    // would produce the same content-addressed key → already_exists → the
    // upload is skipped and the LRU clock is NOT reset. Since repack sources
    // are also excluded from touching, such a pack would idle into GitHub's
    // 7-day eviction while still referenced. Drop the job and let the pack
    // be touched instead. This covers both the trivial single-source case
    // and the tier-split case where consolidation isolates one pack's chunks
    // into their own job.
    plan.repack_jobs.retain(|job| {
        let sources: BTreeSet<PackHash> = job.copies.iter().map(|copy| copy.from.pack).collect();
        if sources.len() == 1 {
            let only = *sources.first().expect("len checked");
            if stats[&only].fully_live() && job.download_bytes() == stats[&only].total_bytes {
                repack_sources.remove(&only);
                return false;
            }
        }
        true
    });

    plan.delete_packs = dead_packs
        .iter()
        .chain(repack_sources.iter())
        .copied()
        .collect();
    plan.delete_packs.sort();

    // ④c Touch: referenced packs going idle
    // Anything we keep referencing must not fall victim to the 7-day-idle
    // eviction. Touch least valuable first so the most valuable packs end up
    // most recently used (evicted last under quota pressure).
    let mut touch: Vec<(u8, u64, PackHash)> = stats
        .iter()
        .filter(|(pack, pack_stats)| {
            !pack_stats.live.is_empty()
                && !repack_sources.contains(*pack)
                && observed.get(*pack).is_some_and(|last_accessed| {
                    // 0 means no REST timestamp parsed: the idle time is
                    // unknown, so do not force a touch (mirrors the
                    // `created == 0` orphan guard).
                    *last_accessed != 0 && now.saturating_sub(*last_accessed) > policy.touch_age
                })
        })
        .map(|(pack, pack_stats)| (pack_stats.tier, pack_stats.live_bytes, *pack))
        .collect();
    touch.sort();
    plan.touch_packs = touch.into_iter().map(|(_, _, pack)| pack).collect();

    // Orphans: in GitHub but in no manifest. The REST listing repeats a
    // key once per ref; deletion is by key, so the set collapses the
    // duplicates.
    plan.orphan_keys = observations
        .iter()
        .filter(|observation| {
            // Only keys hestia itself creates (pack-<64 hex>) can be hestia
            // orphans. The REST listing is prefix-based and ignores the cache
            // version namespace, so entries created by other workflows (e.g.
            // actions/cache with a "pack-..." key) show up here too -- those
            // are not ours to delete.
            let Some(pack) = observation.pack else {
                return false;
            };
            // Judge the key by its *newest* entry across refs: a fresh
            // duplicate on another ref is an in-flight push's upload (or a
            // retried push's dedup re-use), and deletion removes every
            // ref's copy. `newest == 0` means the age is unknown: treat it
            // as too young to judge.
            let newest = newest_created[observation.key.as_str()];
            !manifest.packs.contains_key(&pack)
                && newest != 0
                && now.saturating_sub(newest) > policy.min_age
        })
        .map(|observation| observation.key.clone())
        .collect();

    plan
}

/// Result of executing a plan's repack jobs.
#[derive(Debug, Clone, Default)]
pub struct RepackOutput {
    /// Newly uploaded packs.
    pub packs: BTreeMap<PackHash, PackInfo>,
    /// New locations for copied chunks (`repacks_survived` already
    /// incremented).
    pub locations: BTreeMap<ChunkHash, ChunkLocation>,
    /// Source packs whose live chunks were copied out.
    pub replaced: BTreeSet<PackHash>,
    /// Compressed bytes actually downloaded / uploaded.
    pub downloaded: u64,
    pub uploaded: u64,
}

/// Apply `plan` (which must have been computed against `manifest`) plus the
/// executed repack output.
///
/// Returns the manifest to commit and the packs that are safe to REST-delete
/// **after** the commit lands: exactly those no chunk location in the
/// committed manifest references. This is what guarantees the crash-safety
/// invariant — no live path ever references a deleted pack — even when the
/// repack output was produced against an older manifest version (commit
/// re-plans on conflict and calls this again).
pub fn apply(
    mut manifest: Manifest,
    plan: &GcPlan,
    repacks: &RepackOutput,
) -> (Manifest, Vec<PackHash>) {
    // Drops decided by the plan.
    for pack in &plan.evicted_packs {
        manifest.packs.remove(pack);
    }
    for path in &plan.heal_paths {
        manifest.paths.remove(path);
    }
    for key in &plan.drop_roots {
        manifest.roots.remove(key);
    }
    for path in &plan.drop_paths {
        manifest.paths.remove(path);
    }

    // Mark phase: record reachability clocks in the committed manifest.
    manifest.mark_reachable(plan.now);

    // Relocate chunks that were copied into new packs. Only chunks whose
    // current location still points at a replaced pack move; a chunk that a
    // concurrent push relocated elsewhere keeps its newer location.
    for (chunk, location) in &repacks.locations {
        if let Some(existing) = manifest.chunks.get_mut(chunk)
            && repacks.replaced.contains(&existing.pack)
        {
            *existing = location.clone();
        }
    }
    for (pack, info) in &repacks.packs {
        // A repack output can reproduce an existing pack byte-identically
        // (same content-addressed hash). Merge higher-tier-wins (like
        // PackInfo::merge) so a stable-tier promotion is never lost —
        // otherwise consolidation would re-plan the pack forever.
        manifest
            .packs
            .entry(*pack)
            .and_modify(|existing| existing.tier = existing.tier.max(info.tier))
            .or_insert_with(|| info.clone());
    }

    // Prune: chunks no surviving path references, then packs no surviving
    // chunk references. The pruned packs are the deletable set.
    let referenced = referenced_chunks(&manifest);
    manifest
        .chunks
        .retain(|chunk, _| referenced.contains(chunk));
    let live_packs: BTreeSet<PackHash> = manifest
        .chunks
        .values()
        .map(|location| location.pack)
        .collect();
    let deletable: Vec<PackHash> = manifest
        .packs
        .keys()
        .filter(|pack| !live_packs.contains(pack))
        .copied()
        .collect();
    for pack in &deletable {
        manifest.packs.remove(pack);
    }
    // Defense in depth: a chunk whose pack vanished is unusable.
    manifest
        .chunks
        .retain(|_, location| manifest.packs.contains_key(&location.pack));

    (manifest, deletable)
}

/// Filter orphan keys: anything the committed manifest references is not an
/// orphan, no matter what the pre-commit plan thought. Without this, a GC
/// run recovering from a crashed predecessor could commit a manifest that
/// references a previously-orphaned pack and then delete that very pack.
fn retained_orphans(orphan_keys: &BTreeSet<String>, committed: &Manifest) -> BTreeSet<String> {
    orphan_keys
        .iter()
        .filter(|key| parse_pack_key(key).is_none_or(|pack| !committed.packs.contains_key(&pack)))
        .cloned()
        .collect()
}

/// Outcome of the manifest commit step.
#[derive(Debug, Clone)]
pub struct CommitOutcome {
    /// Committed manifest version (`None` when nothing needed committing).
    pub version: Option<u64>,
    /// Packs no longer referenced by the committed manifest → safe to delete.
    pub deletable: Vec<PackHash>,
    /// Orphan keys re-derived against the committed manifest.
    pub orphan_keys: BTreeSet<String>,
}

/// What one full GC run did.
#[derive(Debug, Clone, Default)]
pub struct GcReport {
    pub plan: GcPlan,
    pub packs_uploaded: usize,
    pub packs_touched: usize,
    pub manifest_version: Option<u64>,
    pub packs_deleted: usize,
    pub orphans_deleted: usize,
    pub manifests_deleted: usize,
    pub bytes_downloaded: u64,
    pub bytes_uploaded: u64,
}

impl GcReport {
    pub fn summary(&self) -> String {
        format!(
            "uploaded {} pack(s) ({} down, {} up); touched {} pack(s); \
             deleted {} pack(s), {} orphan(s), {} old manifest version(s); \
             manifest version: {}",
            self.packs_uploaded,
            human_bytes(self.bytes_downloaded),
            human_bytes(self.bytes_uploaded),
            self.packs_touched,
            self.packs_deleted,
            self.orphans_deleted,
            self.manifests_deleted,
            self.manifest_version
                .map_or_else(|| "unchanged".to_string(), |version| version.to_string()),
        )
    }
}

/// Everything `hestia gc` needs to talk to the world.
pub struct GcContext {
    pub twirp: TwirpClient,
    pub rest: RestClient,
    pub http: reqwest::Client,
    /// SaveMutable family prefix (always [`MANIFEST_PREFIX`] in production;
    /// tests use distinct prefixes).
    pub manifest_prefix: String,
    pub policy: GcPolicy,
}

impl GcContext {
    fn save_mutable(&self) -> SaveMutable<'_> {
        SaveMutable::new(&self.twirp, &self.http, &self.manifest_prefix)
    }

    /// Load the current manifest, or an empty one if none exists yet.
    ///
    /// A lookup miss is cross-checked against the REST listing: the Twirp
    /// lookup is eventually consistent, and planning against a falsely
    /// empty manifest would orphan-delete the entire pack store. The miss
    /// is only trusted when the REST listing shows no manifest versions
    /// either.
    pub async fn load_manifest(&self) -> Result<Manifest, Error> {
        match self.load_manifest_entry().await? {
            Some(entry) => Ok(Manifest::decode(&entry.data)?),
            None => Ok(Manifest::new()),
        }
    }

    /// Like [`Self::load_manifest`], but keeps the raw entry (the commit
    /// step needs its index as a reservation floor).
    async fn load_manifest_entry(&self) -> Result<Option<MutableEntry>, Error> {
        match self.save_mutable().load().await? {
            Some(entry) => Ok(Some(entry)),
            None => {
                let prefix = format!("{}#", self.manifest_prefix);
                let versions = self
                    .rest
                    .list_caches(&prefix)
                    .await?
                    .iter()
                    .filter(|entry| entry.version == self.twirp.version())
                    .count();
                if versions > 0 {
                    return Err(Error::ManifestLookupInconsistent(versions));
                }
                Ok(None)
            }
        }
    }

    /// REST-list all `pack-*` cache entries in GC's own cache version
    /// namespace. The listing is prefix-based and version-blind, so it
    /// also returns same-key entries of other namespaces (salted runs,
    /// foreign workflows): not ours to delete as orphans, and they must
    /// not mask an eviction of our own entry during reconcile.
    pub async fn observe_packs(&self) -> Result<Vec<PackObservation>, Error> {
        Ok(self
            .rest
            .list_caches("pack-")
            .await?
            .iter()
            .filter(|entry| entry.version == self.twirp.version())
            .map(PackObservation::from_entry)
            .collect())
    }

    /// Read-only planning step (serves `--dry-run`).
    pub async fn plan(&self, now: u64) -> Result<(Manifest, Vec<PackObservation>, GcPlan), Error> {
        let manifest = self.load_manifest().await?;
        let observations = self.observe_packs().await?;
        let plan = plan(&manifest, &observations, now, &self.policy);
        Ok((manifest, observations, plan))
    }

    /// Get a signed download URL for a pack, or `None` if it vanished.
    async fn pack_url(&self, pack: &PackHash) -> Result<Option<String>, Error> {
        match self
            .twirp
            .get_download_url(&pack_cache_key(pack), &[])
            .await?
        {
            DownloadUrl::Hit { url, .. } => Ok(Some(url)),
            DownloadUrl::Miss => Ok(None),
        }
    }

    /// Range-read a slice of a pack, refreshing the signed URL on expiry.
    async fn read_pack_range(
        &self,
        pack: &PackHash,
        url: &str,
        range: std::ops::Range<u64>,
    ) -> Result<bytes::Bytes, Error> {
        let pack = *pack;
        let refresh = async move || match self
            .twirp
            .get_download_url(&pack_cache_key(&pack), &[])
            .await?
        {
            DownloadUrl::Hit { url, .. } => Ok(url),
            DownloadUrl::Miss => Err(GhaError::InvalidResponse(format!(
                "pack {pack} disappeared while repacking"
            ))),
        };
        Ok(blob::get_with_refresh(&self.http, url, Some(range), refresh).await?)
    }

    /// Execute step 1 of 6: run all repack jobs (download, verify, upload).
    ///
    /// Source packs that disappeared since planning are skipped: their
    /// chunks keep their old locations, the affected paths break, and the
    /// next GC run heals them (lossy-storage stance).
    pub async fn execute_repacks(&self, plan: &GcPlan) -> Result<RepackOutput, Error> {
        let mut output = RepackOutput::default();

        for job in &plan.repack_jobs {
            // Group copies by source pack, preserving the per-pack offset
            // order the plan established.
            let mut by_pack: BTreeMap<PackHash, Vec<&ChunkCopy>> = BTreeMap::new();
            for copy in &job.copies {
                by_pack.entry(copy.from.pack).or_default().push(copy);
            }

            let mut builder = PackBuilder::new();
            // repacks_survived value each copied chunk gets in its new home.
            let mut survived: BTreeMap<ChunkHash, u32> = BTreeMap::new();
            let mut sources_read: BTreeSet<PackHash> = BTreeSet::new();

            for (source, copies) in by_pack {
                let Some(url) = self.pack_url(&source).await? else {
                    eprintln!(
                        "hestia gc: source pack {source} disappeared since planning; \
                         skipping its chunks (healed next run)"
                    );
                    continue;
                };

                // Coalesce adjacent frames into single Range requests.
                let runs =
                    coalesce_adjacent(copies, |copy| (copy.from.offset, copy.from.compressed_size));

                for run in runs {
                    let start = run[0].from.offset;
                    let last = &run[run.len() - 1].from;
                    let end = last.offset + u64::from(last.compressed_size);
                    let data = self.read_pack_range(&source, &url, start..end).await?;
                    output.downloaded += data.len() as u64;

                    // The cache is lossy and its contents are untrusted: a
                    // pack shorter than the manifest claims (truncated blob,
                    // corrupt chunk location) returns a short Range read.
                    // That must surface as an error, not as a slice panic.
                    let expected = usize::try_from(end - start).expect("chunk run fits in memory");
                    if data.len() != expected {
                        return Err(Error::Gha(GhaError::InvalidResponse(format!(
                            "pack {source}: range read {start}..{end} returned {} bytes, \
                             expected {expected}; the pack is truncated or the manifest \
                             chunk locations are corrupt",
                            data.len()
                        ))));
                    }

                    for copy in run {
                        let from = (copy.from.offset - start) as usize;
                        let to = from + copy.from.compressed_size as usize;
                        let frame = &data[from..to];
                        // Mandatory integrity gate: the decompressed frame
                        // must hash to the chunk hash. Corrupt cache contents
                        // must never propagate into new packs.
                        let decompressed = extract_chunk(frame, &copy.chunk)?;
                        if builder.add_compressed(copy.chunk, frame, decompressed.len() as u32) {
                            survived.insert(copy.chunk, copy.from.repacks_survived + 1);
                        }
                        // A consolidation job can span many source packs;
                        // seal at the target size instead of producing one
                        // giant pack.
                        if builder.compressed_size() >= self.policy.pack_target_size {
                            let full = std::mem::replace(&mut builder, PackBuilder::new());
                            self.upload_repacked(full, job.tier, plan.now, &survived, &mut output)
                                .await?;
                        }
                    }
                }
                sources_read.insert(source);
            }

            if !builder.is_empty() {
                self.upload_repacked(builder, job.tier, plan.now, &survived, &mut output)
                    .await?;
            }
            output.replaced.extend(sources_read);
        }

        Ok(output)
    }

    async fn upload_repacked(
        &self,
        builder: PackBuilder,
        tier: u8,
        now: u64,
        survived: &BTreeMap<ChunkHash, u32>,
        output: &mut RepackOutput,
    ) -> Result<(), Error> {
        let pack = builder.finish();
        // A CAS no-op (pack already exists) is touched inside upload_pack.
        if upload_pack(&self.twirp, &self.http, &pack).await? {
            output.uploaded += pack.data.len() as u64;
        }
        output.packs.insert(
            pack.hash,
            PackInfo {
                size: pack.data.len() as u64,
                created: now,
                tier,
            },
        );
        for (chunk, location) in pack.locations() {
            output.locations.insert(
                chunk,
                ChunkLocation {
                    repacks_survived: survived[&chunk],
                    ..location
                },
            );
        }
        Ok(())
    }

    /// Execute step 2 of 6: 1-byte Range reads to reset the LRU clock of
    /// referenced packs that are going idle.
    pub async fn execute_touches(&self, plan: &GcPlan) -> Result<usize, Error> {
        let mut touched = 0;
        for pack in &plan.touch_packs {
            // A missing pack was evicted since planning; the next run
            // reconciles it.
            let Some(url) = self.pack_url(pack).await? else {
                continue;
            };
            let pack = *pack;
            let refresh = async move || match self
                .twirp
                .get_download_url(&pack_cache_key(&pack), &[])
                .await?
            {
                DownloadUrl::Hit { url, .. } => Ok(url),
                DownloadUrl::Miss => Err(GhaError::InvalidResponse(format!(
                    "pack {pack} disappeared while touching"
                ))),
            };
            // Downloads through the Twirp/Azure path bump last_accessed_at
            // (verified against the real service). A touch is a pure LRU
            // optimization that self-heals next run; a failure must not
            // abort the run and strand the commit and deletes behind it.
            match blob::get_with_refresh(&self.http, &url, Some(0..1), refresh).await {
                Ok(_) => touched += 1,
                Err(err) => eprintln!("hestia gc: touch of pack {pack} failed ({err}); skipping"),
            }
        }
        Ok(touched)
    }

    /// Execute step 3 of 6: commit the manifest via SaveMutable.
    ///
    /// On every (re)try the merge closure **re-plans against the manifest
    /// version it is shown**: if a concurrent push landed since planning,
    /// its paths/packs are respected — a path the stale plan wanted to drop
    /// stays if the push made it live again, and a pack only enters the
    /// deletable set if the *committed* manifest no longer references it.
    pub async fn commit(&self, repacks: &RepackOutput, now: u64) -> Result<CommitOutcome, Error> {
        // Re-list the pack observations: the plan-time listing predates the
        // potentially long repack and touch phases. A pack a concurrent
        // drain uploaded since then carries the drain's *start* time, which
        // can exceed min_age — the stale listing would judge it evicted and
        // heal away the drain's just-committed paths.
        let observations = self.observe_packs().await?;
        let observations = observations.as_slice();

        // Skip the commit when it would be a byte-identical no-op. Note
        // this only fires for an empty or fully-unreachable manifest:
        // mark_reachable bumps last_reachable on every reachable path, so
        // any cache with a live root always commits (the fresh clocks
        // preserve the full path_grace window for paths whose roots later
        // expire).
        let entry = self.load_manifest_entry().await?;
        // Never reserve at or below the version observed here, even when
        // the eventually consistent lookup inside save() regresses below it
        // (same hazard the drain commit guards against).
        let floor = entry.as_ref().map_or(0, |e| e.index);
        let base = match &entry {
            Some(entry) => Manifest::decode(&entry.data)?,
            None => Manifest::new(),
        };
        let base_plan = plan(&base, observations, now, &self.policy);
        let (transformed, deletable) = apply(base.clone(), &base_plan, repacks);
        if transformed == base {
            let orphan_keys = retained_orphans(&base_plan.orphan_keys, &base);
            return Ok(CommitOutcome {
                version: None,
                deletable,
                orphan_keys,
            });
        }

        let mut committed: Option<(Vec<PackHash>, BTreeSet<String>)> = None;
        let version = self
            .save_mutable()
            .save_with_floor(floor, |existing| {
                let latest = match existing {
                    Some(entry) => Manifest::decode(&entry.data)
                        .map_err(|err| GhaError::InvalidResponse(err.to_string()))?,
                    None => Manifest::new(),
                };
                // Re-plan against the latest manifest (concurrent pushes).
                let fresh = plan(&latest, observations, now, &self.policy);
                let (next, deletable) = apply(latest, &fresh, repacks);
                let encoded = next
                    .encode()
                    .map_err(|err| GhaError::InvalidResponse(err.to_string()))?;
                // Orphans were judged against the pre-commit manifest; a key
                // the *committed* manifest references (e.g. a repack output
                // recovered from a crashed earlier run) is not an orphan.
                let orphans = retained_orphans(&fresh.orphan_keys, &next);
                committed = Some((deletable, orphans));
                Ok(encoded)
            })
            .await?;

        let (deletable, orphan_keys) = committed.expect("save ran the merge closure at least once");
        Ok(CommitOutcome {
            version: Some(version),
            deletable,
            orphan_keys,
        })
    }

    /// Execute step 4 of 6: REST-delete packs the committed manifest no
    /// longer references. Must only run **after** [`Self::commit`] succeeded.
    pub async fn delete_packs(&self, packs: &[PackHash]) -> Result<usize, Error> {
        let mut deleted = 0;
        for pack in packs {
            if !self
                .rest
                .delete_by_key(&pack_cache_key(pack))
                .await?
                .is_empty()
            {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Execute step 5 of 6: REST-delete orphaned cache keys (uploaded but
    /// never referenced by any committed manifest, e.g. by a crashed GC or
    /// push).
    pub async fn delete_orphans(&self, keys: &BTreeSet<String>) -> Result<usize, Error> {
        let mut deleted = 0;
        for key in keys {
            if !self.rest.delete_by_key(key).await?.is_empty() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Execute step 6 of 6: delete superseded manifest versions (`m#K` for
    /// `K < newest`). Without this, every drain and GC commit leaves one
    /// dead entry behind forever.
    pub async fn cleanup_manifests(&self, now: u64) -> Result<usize, Error> {
        let prefix = format!("{}#", self.manifest_prefix);
        let entries = self.rest.list_caches(&prefix).await?;
        let indexed: Vec<(u64, &CacheEntry)> = entries
            .iter()
            // Another namespace's m#N sequence restarts at 1; letting its
            // (possibly higher) indices into `newest` would make GC delete
            // this namespace's live manifest head.
            .filter(|entry| entry.version == self.twirp.version())
            .filter_map(|entry| Some((parse_manifest_index(&prefix, &entry.key)?, entry)))
            .collect();
        let Some(newest) = indexed.iter().map(|(index, _)| *index).max() else {
            return Ok(0);
        };

        let mut deleted = 0;
        for (index, entry) in indexed {
            // Keep the newest version; older ones must also be old enough
            // that no in-flight reader still holds their download URL.
            // An unparsable timestamp means the age is unknown: skip the
            // entry rather than judging it infinitely old.
            let Some(created) = entry.created_unix() else {
                continue;
            };
            let age = now.saturating_sub(created);
            if index < newest
                && age > self.policy.min_age
                && !self.rest.delete_by_key(&entry.key).await?.is_empty()
            {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// The full GC flow: plan, then (unless `dry_run`) execute every step in
    /// crash-safe order.
    pub async fn run(&self, dry_run: bool, now: u64) -> Result<GcReport, Error> {
        let (_, _, gc_plan) = self.plan(now).await?;
        eprintln!("hestia gc: plan: {}", gc_plan.summary());

        let mut report = GcReport {
            plan: gc_plan.clone(),
            ..GcReport::default()
        };
        if dry_run {
            return Ok(report);
        }

        // 1. Repack (uploads new packs; old ones stay referenced).
        let repacks = self.execute_repacks(&gc_plan).await?;
        report.packs_uploaded = repacks.packs.len();
        report.bytes_downloaded = repacks.downloaded;
        report.bytes_uploaded = repacks.uploaded;

        // 2. Touch.
        report.packs_touched = self.execute_touches(&gc_plan).await?;

        // 3. Commit the manifest (re-plans on conflict, re-lists packs).
        let outcome = self.commit(&repacks, now).await?;
        report.manifest_version = outcome.version;

        // 4-6. Deletes happen strictly after the commit landed.
        report.packs_deleted = self.delete_packs(&outcome.deletable).await?;
        report.orphans_deleted = self.delete_orphans(&outcome.orphan_keys).await?;
        report.manifests_deleted = self.cleanup_manifests(now).await?;

        Ok(report)
    }
}

pub async fn run(args: &GcArgs) -> ExitCode {
    let http = reqwest::Client::new();
    let twirp = match TwirpClient::from_env(http.clone()) {
        Ok(twirp) => twirp,
        Err(err) => {
            eprintln!(
                "hestia gc: {err}\n\
                 hint: the GHA cache tokens are only visible to shell steps when the \
                 hestia action wrapper exported them"
            );
            return ExitCode::FAILURE;
        }
    };
    let rest = match RestClient::from_env(http.clone()) {
        Ok(rest) => rest,
        Err(err) => {
            eprintln!(
                "hestia gc: {err}\n\
                 hint: GC needs GITHUB_TOKEN with `actions: write` permission and \
                 GITHUB_REPOSITORY"
            );
            return ExitCode::FAILURE;
        }
    };

    let context = GcContext {
        twirp,
        rest,
        http,
        manifest_prefix: MANIFEST_PREFIX.to_string(),
        policy: GcPolicy {
            path_grace: args.grace * SECS_PER_DAY,
            push_ttl: args.push_ttl * SECS_PER_DAY,
            root_ttl: args.root_ttl * SECS_PER_DAY,
            touch_age: args.touch_age * SECS_PER_DAY,
            ..GcPolicy::default()
        },
    };

    match context.run(args.dry_run, now_unix()).await {
        Ok(report) => {
            if args.dry_run {
                eprintln!("hestia gc: dry run, nothing executed");
            } else {
                eprintln!("hestia gc: {}", report.summary());
            }
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("hestia gc: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        ChunkList, Directory, FileTree, PathEntry, Regular, Root, StorePath, StorePathHash,
    };

    /// Reference clock for all tests (an arbitrary fixed point in time).
    const NOW: u64 = 1_750_000_000;

    fn path_hash(seed: u8) -> PathHash {
        PathHash(StorePathHash::new([seed; 20]))
    }

    fn store_path(seed: u8) -> StorePath {
        format!("{}-path-{seed}", path_hash(seed)).parse().unwrap()
    }

    fn chunk_hash(seed: u8) -> ChunkHash {
        ChunkHash::digest([b'c', seed])
    }

    fn pack_hash(seed: u8) -> PackHash {
        Hash32::digest([b'p', seed])
    }

    /// Build a single-file tree referencing the given chunks.
    fn tree_of(chunks: &[u8]) -> FileTree<ChunkList> {
        FileTree(FileSystemObject::Directory(Directory {
            entries: std::collections::BTreeMap::from([(
                "blob".to_string(),
                Box::new(FileTree(FileSystemObject::Regular(Regular {
                    executable: false,
                    contents: ChunkList {
                        chunks: chunks.iter().map(|seed| chunk_hash(*seed)).collect(),
                    },
                }))),
            )]),
        }))
    }

    struct ManifestBuilder {
        manifest: Manifest,
        offsets: BTreeMap<PackHash, u64>,
    }

    impl ManifestBuilder {
        fn new() -> Self {
            Self {
                manifest: Manifest::new(),
                offsets: BTreeMap::new(),
            }
        }

        /// Register a pack; its size accumulates as chunks are added to it.
        fn pack(mut self, seed: u8, tier: u8, created: u64) -> Self {
            self.manifest.packs.insert(
                pack_hash(seed),
                PackInfo {
                    size: 0,
                    created,
                    tier,
                },
            );
            self
        }

        /// Place a chunk of `size` compressed bytes into a pack.
        fn chunk(mut self, chunk_seed: u8, pack_seed: u8, size: u32, survived: u32) -> Self {
            let pack = pack_hash(pack_seed);
            let offset = self.offsets.entry(pack).or_insert(0);
            self.manifest.chunks.insert(
                chunk_hash(chunk_seed),
                ChunkLocation {
                    pack,
                    offset: *offset,
                    compressed_size: size,
                    uncompressed_size: size * 2,
                    repacks_survived: survived,
                },
            );
            *offset += u64::from(size);
            let info = self.manifest.packs.get_mut(&pack).expect("pack registered");
            info.size += u64::from(size);
            self
        }

        /// Add a path whose tree references the given chunks.
        fn path(
            mut self,
            seed: u8,
            chunks: &[u8],
            references: &[u8],
            last_pushed: u64,
            last_reachable: u64,
        ) -> Self {
            self.manifest.paths.insert(
                path_hash(seed),
                PathEntry {
                    store_path: store_path(seed),
                    nar_hash: Hash32::digest([b'n', seed]),
                    nar_size: 1000,
                    references: references.iter().map(|r| store_path(*r)).collect(),
                    ca: None,
                    deriver: None,
                    tree: tree_of(chunks),
                    last_reachable,
                    last_pushed,
                },
            );
            self
        }

        /// Add a root pinning the given paths.
        fn root(mut self, key: &str, paths: &[u8], updated: u64) -> Self {
            self.manifest.roots.insert(
                key.to_string(),
                Root {
                    paths: paths.iter().map(|seed| path_hash(*seed)).collect(),
                    updated,
                    run_id: None,
                },
            );
            self
        }

        fn build(self) -> Manifest {
            self.manifest
        }
    }

    /// Observations matching every pack in the manifest (nothing evicted),
    /// all created long ago and accessed at `last_accessed`.
    fn observe_all(manifest: &Manifest, last_accessed: u64) -> Vec<PackObservation> {
        manifest
            .packs
            .iter()
            .map(|(hash, info)| PackObservation {
                key: pack_cache_key(hash),
                pack: Some(*hash),
                created: info.created,
                last_accessed,
            })
            .collect()
    }

    fn policy() -> GcPolicy {
        GcPolicy::default()
    }

    #[test]
    fn pack_key_round_trips() {
        let hash = Hash32::digest(b"some pack");
        assert_eq!(parse_pack_key(&pack_cache_key(&hash)), Some(hash));
        assert_eq!(parse_pack_key("pack-nothex"), None);
        assert_eq!(parse_pack_key("pack-"), None);
        assert_eq!(parse_pack_key("m#3"), None);
        assert_eq!(parse_pack_key(&format!("pack-{}", "0".repeat(63))), None);
        // Hestia only emits lowercase hex; an uppercase 64-hex key (e.g. a
        // Get-FileHash digest in an actions/cache key) is foreign.
        assert_eq!(parse_pack_key(&format!("pack-{}", "A".repeat(64))), None);
        assert_eq!(parse_pack_key(&format!("pack-{}", "+1".repeat(32))), None);
    }

    #[test]
    fn manifest_index_parses_only_canonical_decimal() {
        assert_eq!(parse_manifest_index("m#", "m#0"), Some(0));
        assert_eq!(parse_manifest_index("m#", "m#42"), Some(42));
        assert_eq!(parse_manifest_index("m#", "m#+99"), None);
        assert_eq!(parse_manifest_index("m#", "m#007"), None);
        assert_eq!(parse_manifest_index("m#", "m#"), None);
        assert_eq!(parse_manifest_index("m#", "m#1x"), None);
        assert_eq!(parse_manifest_index("m#", "other#1"), None);
    }

    #[test]
    fn expired_roots_are_dropped() {
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, NOW - 30 * SECS_PER_DAY)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], NOW - 30 * SECS_PER_DAY, NOW - SECS_PER_DAY)
            .root("dead-branch", &[1], NOW - 15 * SECS_PER_DAY)
            .root("live-branch", &[1], NOW - 13 * SECS_PER_DAY)
            .build();

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        assert_eq!(plan.drop_roots, vec!["dead-branch".to_string()]);
        // The path stays alive: the live root still pins it.
        assert!(plan.drop_paths.is_empty());
    }

    #[test]
    fn unreachable_paths_swept_only_after_grace_and_push_ttl() {
        let old = NOW - 30 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 0)
            .chunk(2, 1, 100, 0)
            .chunk(3, 1, 100, 0)
            .chunk(4, 1, 100, 0)
            // Path 1: reachable from the root → kept.
            .path(1, &[1], &[], old, old)
            // Path 2: unreachable but recently marked reachable → grace keeps it.
            .path(2, &[2], &[], old, NOW - SECS_PER_DAY)
            // Path 3: unreachable, grace expired, but pushed recently → PushTTL keeps it.
            .path(3, &[3], &[], NOW - SECS_PER_DAY, old)
            // Path 4: unreachable, both clocks expired → swept.
            .path(4, &[4], &[], old, old)
            .root("main", &[1], NOW)
            .build();

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        assert_eq!(plan.drop_paths, vec![path_hash(4)]);
    }

    #[test]
    fn sweep_follows_references_from_roots() {
        let old = NOW - 30 * SECS_PER_DAY;
        // Root pins path 1, which references path 2: both stay. Path 3 is
        // referenced by nothing → swept.
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 0)
            .chunk(2, 1, 100, 0)
            .chunk(3, 1, 100, 0)
            .path(1, &[1], &[2], old, old)
            .path(2, &[2], &[], old, old)
            .path(3, &[3], &[], old, old)
            .root("main", &[1], NOW)
            .build();

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        assert_eq!(plan.drop_paths, vec![path_hash(3)]);
    }

    #[test]
    fn root_expiry_makes_its_paths_sweepable() {
        let old = NOW - 30 * SECS_PER_DAY;
        // The only root expired → its paths become unreachable → swept
        // (their grace and TTL are also expired).
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], old, old)
            .root("deleted-branch", &[1], NOW - 20 * SECS_PER_DAY)
            .build();

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        assert_eq!(plan.drop_roots, vec!["deleted-branch".to_string()]);
        assert_eq!(plan.drop_paths, vec![path_hash(1)]);
        // Everything referenced only by the dead path becomes garbage.
        assert_eq!(plan.delete_packs, vec![pack_hash(1)]);
    }

    #[test]
    fn evicted_packs_drop_their_paths() {
        let old = NOW - 10 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .pack(2, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 0)
            .chunk(2, 2, 100, 0)
            .path(1, &[1], &[], NOW, NOW)
            .path(2, &[2], &[], NOW, NOW)
            .root("main", &[1, 2], NOW)
            .build();

        // Pack 2 is missing from the observations (GitHub evicted it).
        let observations: Vec<PackObservation> = observe_all(&manifest, NOW)
            .into_iter()
            .filter(|observation| observation.pack != Some(pack_hash(2)))
            .collect();

        let plan = plan(&manifest, &observations, NOW, &policy());
        assert_eq!(plan.evicted_packs, vec![pack_hash(2)]);
        assert_eq!(
            plan.heal_paths,
            vec![path_hash(2)],
            "path 2 lost its chunks"
        );
        assert!(plan.drop_paths.is_empty());
        assert!(
            !plan.delete_packs.contains(&pack_hash(2)),
            "evicted packs are already gone; nothing to delete"
        );
    }

    #[test]
    fn young_packs_are_never_judged_evicted() {
        // A pack created moments ago that is missing from the REST listing
        // was probably uploaded by a concurrent push after the listing was
        // taken. It must not be treated as evicted.
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, NOW - 60)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], NOW, NOW)
            .root("main", &[1], NOW)
            .build();

        let plan = plan(&manifest, &[], NOW, &policy());
        assert!(plan.evicted_packs.is_empty());
        assert!(plan.heal_paths.is_empty());
    }

    #[test]
    fn mostly_dead_pack_is_repacked() {
        let old = NOW - 30 * SECS_PER_DAY;
        // Pack 1: 600 bytes total, path 1 (live) holds 200, path 2 (dead)
        // holds 400 → liveness 0.33 < 0.5 → repack.
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 200, 0)
            .chunk(2, 1, 400, 0)
            .path(1, &[1], &[], old, NOW)
            .path(2, &[2], &[], old, old)
            .root("main", &[1], NOW)
            .build();

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        assert_eq!(plan.drop_paths, vec![path_hash(2)]);
        assert_eq!(plan.repack_jobs.len(), 1);
        let job = &plan.repack_jobs[0];
        assert_eq!(job.tier, TIER_VOLATILE);
        assert_eq!(job.copies.len(), 1);
        assert_eq!(job.copies[0].chunk, chunk_hash(1));
        assert_eq!(job.download_bytes(), 200);
        assert_eq!(plan.delete_packs, vec![pack_hash(1)]);
        assert!(
            plan.touch_packs.is_empty(),
            "repacked packs are not touched"
        );
    }

    #[test]
    fn dead_pack_is_deleted_without_repack() {
        let old = NOW - 30 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], old, old)
            .build(); // no roots: path 1 is unreachable and out of TTL

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        assert_eq!(plan.drop_paths, vec![path_hash(1)]);
        assert!(plan.repack_jobs.is_empty());
        assert_eq!(plan.delete_packs, vec![pack_hash(1)]);
    }

    #[test]
    fn fully_live_pack_is_touched_never_repacked() {
        // The CAS no-op trap: a fully live pack repacked alone would produce
        // identical content → already_exists → LRU clock not reset.
        let old = NOW - 30 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 0)
            .chunk(2, 1, 100, 0)
            .path(1, &[1, 2], &[], old, NOW)
            .root("main", &[1], NOW)
            .build();

        // Idle for 5 days → needs a touch.
        let observations = observe_all(&manifest, NOW - 5 * SECS_PER_DAY);
        let idle_plan = plan(&manifest, &observations, NOW, &policy());
        assert!(
            idle_plan.repack_jobs.is_empty(),
            "{:?}",
            idle_plan.repack_jobs
        );
        assert!(idle_plan.delete_packs.is_empty());
        assert_eq!(idle_plan.touch_packs, vec![pack_hash(1)]);

        // Recently accessed → nothing to do at all.
        let observations = observe_all(&manifest, NOW - SECS_PER_DAY);
        let fresh_plan = plan(&manifest, &observations, NOW, &policy());
        assert!(fresh_plan.touch_packs.is_empty());
    }

    #[test]
    fn stable_pack_with_dead_chunks_is_still_repacked() {
        // Stability protects against pointless copying, not against waste:
        // a stable pack that decays below MinLiveness is repacked like any
        // other.
        let old = NOW - 30 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_STABLE, old)
            .chunk(1, 1, 100, 2)
            .chunk(2, 1, 400, 2)
            .path(1, &[1], &[], old, NOW)
            .path(2, &[2], &[], old, old)
            .root("main", &[1], NOW)
            .build();

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        assert_eq!(plan.repack_jobs.len(), 1);
        assert_eq!(
            plan.repack_jobs[0].tier, TIER_STABLE,
            "chunks that already survived 2 repacks stay stable"
        );
    }

    #[test]
    fn volatile_pack_count_triggers_consolidation() {
        let old = NOW - 30 * SECS_PER_DAY;
        // 5 fully-live volatile packs (one path each) > MaxVolatilePacks=4
        // → all get consolidated even though each one is fully live.
        let mut builder = ManifestBuilder::new();
        for seed in 1..=5u8 {
            builder = builder
                .pack(seed, TIER_VOLATILE, old)
                .chunk(seed, seed, 100, 0)
                .path(seed, &[seed], &[], old, NOW);
        }
        let manifest = builder.root("main", &[1, 2, 3, 4, 5], NOW).build();

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        assert_eq!(plan.repack_jobs.len(), 1);
        assert_eq!(plan.repack_jobs[0].copies.len(), 5);
        assert_eq!(plan.delete_packs.len(), 5);
        // A multi-source consolidation can still reproduce a source pack
        // byte-identically (the builder seals at the same target size the
        // pipeline used); that case is handled at upload time, where the
        // already-exists result triggers an LRU touch instead.
    }

    #[test]
    fn surviving_chunks_split_into_stability_tiers() {
        let old = NOW - 30 * SECS_PER_DAY;
        // One mostly-dead pack with two live chunks: one already survived a
        // repack (next survival promotes it to stable), one is fresh.
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 1) // survived 1 → after this repack: 2 → stable
            .chunk(2, 1, 100, 0) // survived 0 → after this repack: 1 → volatile
            .chunk(3, 1, 400, 0) // dead
            .path(1, &[1, 2], &[], old, NOW)
            .path(2, &[3], &[], old, old)
            .root("main", &[1], NOW)
            .build();

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        assert_eq!(plan.repack_jobs.len(), 2);
        let stable = plan
            .repack_jobs
            .iter()
            .find(|job| job.tier == TIER_STABLE)
            .expect("stable job");
        let volatile = plan
            .repack_jobs
            .iter()
            .find(|job| job.tier == TIER_VOLATILE)
            .expect("volatile job");
        assert_eq!(stable.copies[0].chunk, chunk_hash(1));
        assert_eq!(volatile.copies[0].chunk, chunk_hash(2));
    }

    #[test]
    fn consolidation_never_repacks_a_pack_into_itself() {
        // CAS no-op trap, general form. Volatile consolidation pulls in five
        // fully-live packs; pack 1's chunk already survived one repack, so
        // the tier partition puts it alone into a stable-tier job. Copying
        // pack 1's frames in offset order reproduces pack 1 byte-identically:
        // the upload would hit `already_exists` (content-addressed key), the
        // LRU clock would NOT reset, and -- because repack sources are
        // excluded from touching -- the pack would never be touched either.
        // A live, referenced pack would idle its way into GitHub's 7-day
        // eviction. Such no-op jobs must be dropped and the pack touched
        // instead.
        let old = NOW - 30 * SECS_PER_DAY;
        let mut builder = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 1) // survived 1 -> stable tier on next repack
            .path(1, &[1], &[], old, NOW);
        for seed in 2..=5u8 {
            builder = builder
                .pack(seed, TIER_VOLATILE, old)
                .chunk(seed, seed, 100, 0)
                .path(seed, &[seed], &[], old, NOW);
        }
        let manifest = builder.root("main", &[1, 2, 3, 4, 5], NOW).build();

        // Every pack has been idle past TouchAge.
        let observations = observe_all(&manifest, NOW - 5 * SECS_PER_DAY);
        let plan = plan(&manifest, &observations, NOW, &policy());

        // The genuine consolidation (packs 2-5 -> one new pack) goes ahead.
        assert_eq!(plan.repack_jobs.len(), 1, "{}", plan.summary());
        assert_eq!(plan.repack_jobs[0].tier, TIER_VOLATILE);
        assert_eq!(plan.repack_jobs[0].copies.len(), 4);
        // Pack 1 is left alone: not deleted, not repacked, but touched so
        // its LRU clock keeps it safe from idle eviction.
        assert!(
            !plan.delete_packs.contains(&pack_hash(1)),
            "a byte-identical repack output must not schedule its source for deletion"
        );
        assert!(
            plan.touch_packs.contains(&pack_hash(1)),
            "the pack skipped by the CAS guard must be touched instead: {:?}",
            plan.touch_packs
        );
    }

    #[test]
    fn packs_with_unparsable_lru_clock_are_not_force_touched() {
        // `last_accessed == 0` means the REST timestamp did not parse.
        // Treating the unknown idle time as "idle since 1970" would
        // force-touch every referenced pack on every run.
        let old = NOW - 30 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], old, NOW)
            .root("main", &[1], NOW)
            .build();

        let plan = plan(&manifest, &observe_all(&manifest, 0), NOW, &policy());
        assert!(
            plan.touch_packs.is_empty(),
            "a pack of unknown idle time must not be force-touched: {:?}",
            plan.touch_packs
        );
    }

    #[test]
    fn orphan_keys_are_old_unreferenced_entries() {
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, NOW - SECS_PER_DAY)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], NOW, NOW)
            .root("main", &[1], NOW)
            .build();

        let mut observations = observe_all(&manifest, NOW);
        // An old pack nobody references → orphan.
        observations.push(PackObservation {
            key: pack_cache_key(&pack_hash(9)),
            pack: Some(pack_hash(9)),
            created: NOW - 2 * SECS_PER_HOUR,
            last_accessed: NOW,
        });
        // A young unreferenced pack → in-flight push, leave it alone.
        observations.push(PackObservation {
            key: pack_cache_key(&pack_hash(10)),
            pack: Some(pack_hash(10)),
            created: NOW - 60,
            last_accessed: NOW,
        });
        // A key that merely shares the prefix → foreign entry, never touched.
        observations.push(PackObservation {
            key: "pack-not-a-hash".to_string(),
            pack: None,
            created: NOW - SECS_PER_DAY,
            last_accessed: NOW,
        });

        let plan = plan(&manifest, &observations, NOW, &policy());
        assert_eq!(
            plan.orphan_keys,
            BTreeSet::from([pack_cache_key(&pack_hash(9))])
        );
    }

    #[test]
    fn orphan_keys_are_deduplicated_across_refs() {
        // The REST listing repeats a key once per ref (same pack pushed
        // from several branches); one delete removes all of them.
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, NOW - SECS_PER_DAY)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], NOW, NOW)
            .root("main", &[1], NOW)
            .build();

        let mut observations = observe_all(&manifest, NOW);
        for _ in 0..3 {
            observations.push(PackObservation {
                key: pack_cache_key(&pack_hash(9)),
                pack: Some(pack_hash(9)),
                created: NOW - 2 * SECS_PER_HOUR,
                last_accessed: NOW,
            });
        }

        let plan = plan(&manifest, &observations, NOW, &policy());
        assert_eq!(
            plan.orphan_keys,
            BTreeSet::from([pack_cache_key(&pack_hash(9))])
        );
    }

    #[test]
    fn orphan_keys_require_every_duplicate_entry_to_be_old() {
        // A week-old entry on one ref must not orphan the key while
        // another ref holds a minutes-old entry (an in-flight push's
        // upload): deletion is by key across all refs.
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, NOW - SECS_PER_DAY)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], NOW, NOW)
            .root("main", &[1], NOW)
            .build();

        let mut observations = observe_all(&manifest, NOW);
        for created in [NOW - 7 * SECS_PER_DAY, NOW - 60] {
            observations.push(PackObservation {
                key: pack_cache_key(&pack_hash(9)),
                pack: Some(pack_hash(9)),
                created,
                last_accessed: NOW,
            });
        }

        let plan = plan(&manifest, &observations, NOW, &policy());
        assert!(
            plan.orphan_keys.is_empty(),
            "a key with a fresh duplicate on another ref must not be deleted: {:?}",
            plan.orphan_keys
        );
    }

    #[test]
    fn touch_judges_staleness_by_the_stalest_duplicate_entry() {
        // Each ref-scoped entry has its own LRU clock and is evicted
        // independently. A fresh feature-branch copy must not mask a 6-day
        // idle default-branch copy of the same key.
        let old = NOW - 30 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], old, NOW)
            .root("main", &[1], NOW)
            .build();

        // Stale copy listed first, fresh copy second (last-wins collapse
        // would judge against the fresh clock).
        let mut observations = observe_all(&manifest, NOW - 6 * SECS_PER_DAY);
        observations.push(PackObservation {
            key: pack_cache_key(&pack_hash(1)),
            pack: Some(pack_hash(1)),
            created: old,
            last_accessed: NOW,
        });

        let plan = plan(&manifest, &observations, NOW, &policy());
        assert_eq!(
            plan.touch_packs,
            vec![pack_hash(1)],
            "the stalest per-ref copy decides whether a touch is needed"
        );
    }

    #[test]
    fn packs_with_unparsable_timestamps_are_never_orphans() {
        // PackObservation records `created: 0` when the REST API timestamp
        // does not parse (the parser is hand-rolled; an API format change
        // must not turn into data loss). An unknown age has to be judged
        // "too young to touch", not "infinitely old": the entry could be a
        // concurrent push's just-uploaded pack, and deleting it would leave
        // that push's committed manifest referencing a missing pack.
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, NOW - SECS_PER_DAY)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], NOW, NOW)
            .root("main", &[1], NOW)
            .build();

        let mut observations = observe_all(&manifest, NOW);
        observations.push(PackObservation {
            key: pack_cache_key(&pack_hash(9)),
            pack: Some(pack_hash(9)),
            created: 0,
            last_accessed: 0,
        });

        let plan = plan(&manifest, &observations, NOW, &policy());
        assert!(
            plan.orphan_keys.is_empty(),
            "a pack of unknown age must never be judged an orphan: {:?}",
            plan.orphan_keys
        );
    }

    #[test]
    fn foreign_keys_sharing_the_pack_prefix_are_never_orphans() {
        // Other workflows in the same repository may use actions/cache with
        // keys that happen to start with "pack-" (e.g. "pack-deps-v1"). The
        // REST listing is prefix-based and ignores the cache version
        // namespace, so those entries show up in GC's observations -- but
        // they are not hestia's to delete. Hestia itself only ever creates
        // pack-<64 hex> keys, so anything else is foreign by definition.
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, NOW - SECS_PER_DAY)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], NOW, NOW)
            .root("main", &[1], NOW)
            .build();

        let mut observations = observe_all(&manifest, NOW);
        observations.push(PackObservation {
            key: "pack-deps-v1-linux".to_string(),
            pack: None,
            created: NOW - 30 * SECS_PER_DAY,
            last_accessed: NOW - 30 * SECS_PER_DAY,
        });

        let plan = plan(&manifest, &observations, NOW, &policy());
        assert!(
            plan.orphan_keys.is_empty(),
            "foreign cache entries must never be deleted: {:?}",
            plan.orphan_keys
        );
    }

    #[test]
    fn noop_plan_for_clean_cache() {
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, NOW - SECS_PER_DAY)
            .chunk(1, 1, 100, 0)
            .path(1, &[1], &[], NOW, NOW)
            .root("main", &[1], NOW)
            .build();

        let plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        let noop = GcPlan {
            now: NOW,
            ..GcPlan::default()
        };
        assert_eq!(plan, noop, "{}", plan.summary());
    }

    /// Fake repack execution: pretend every copy landed in a new pack.
    fn fake_repack_output(plan: &GcPlan) -> RepackOutput {
        let mut output = RepackOutput::default();
        for (job_index, job) in plan.repack_jobs.iter().enumerate() {
            let new_pack = Hash32::digest([b'N', job_index as u8]);
            let mut offset = 0u64;
            for copy in &job.copies {
                output.locations.insert(
                    copy.chunk,
                    ChunkLocation {
                        pack: new_pack,
                        offset,
                        compressed_size: copy.from.compressed_size,
                        uncompressed_size: copy.from.uncompressed_size,
                        repacks_survived: copy.from.repacks_survived + 1,
                    },
                );
                offset += u64::from(copy.from.compressed_size);
                output.replaced.insert(copy.from.pack);
            }
            output.packs.insert(
                new_pack,
                PackInfo {
                    size: offset,
                    created: plan.now,
                    tier: job.tier,
                },
            );
        }
        output
    }

    #[test]
    fn apply_merges_repack_output_with_higher_tier_wins() {
        // A repack output reproducing an existing pack byte-identically
        // carries the same pack hash. Its tier promotion must not be
        // dropped, or the pack stays volatile and consolidation re-plans
        // it on every run.
        let old = NOW - 30 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 100, 1)
            .path(1, &[1], &[], old, NOW)
            .root("main", &[1], NOW)
            .build();

        let gc_plan = GcPlan {
            now: NOW,
            ..GcPlan::default()
        };
        let location = manifest.chunks[&chunk_hash(1)].clone();
        let repacks = RepackOutput {
            packs: BTreeMap::from([(
                pack_hash(1),
                PackInfo {
                    size: 100,
                    created: NOW,
                    tier: TIER_STABLE,
                },
            )]),
            locations: BTreeMap::from([(
                chunk_hash(1),
                ChunkLocation {
                    repacks_survived: location.repacks_survived + 1,
                    ..location
                },
            )]),
            replaced: BTreeSet::from([pack_hash(1)]),
            ..RepackOutput::default()
        };

        let (committed, deletable) = apply(manifest, &gc_plan, &repacks);
        assert_eq!(
            committed.packs[&pack_hash(1)].tier,
            TIER_STABLE,
            "a same-hash repack output must promote the existing pack's tier"
        );
        assert!(deletable.is_empty());
    }

    #[test]
    fn apply_executes_the_plan_and_keeps_the_manifest_consistent() {
        let old = NOW - 30 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 200, 0)
            .chunk(2, 1, 400, 0)
            .path(1, &[1], &[], old, NOW)
            .path(2, &[2], &[], old, old)
            .root("main", &[1], NOW)
            .root("dead", &[2], NOW - 20 * SECS_PER_DAY)
            .build();

        let gc_plan = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        let repacks = fake_repack_output(&gc_plan);
        let (committed, deletable) = apply(manifest, &gc_plan, &repacks);

        // Dead root and path gone.
        assert!(!committed.roots.contains_key("dead"));
        assert!(!committed.paths.contains_key(&path_hash(2)));
        // Live path kept and marked.
        assert_eq!(committed.paths[&path_hash(1)].last_reachable, NOW);
        // Its chunk relocated into the new pack.
        let location = &committed.chunks[&chunk_hash(1)];
        assert_ne!(location.pack, pack_hash(1));
        assert_eq!(location.repacks_survived, 1);
        assert!(committed.packs.contains_key(&location.pack));
        // Dead chunk pruned, old pack gone and deletable.
        assert!(!committed.chunks.contains_key(&chunk_hash(2)));
        assert!(!committed.packs.contains_key(&pack_hash(1)));
        assert_eq!(deletable, vec![pack_hash(1)]);

        // The committed manifest is internally consistent: every referenced
        // chunk has a location in a known pack.
        assert!(broken_paths(&committed).is_empty());
    }

    #[test]
    fn apply_after_concurrent_push_respects_the_new_manifest() {
        // GC planned and repacked against manifest V1, but a concurrent push
        // committed V2 in the meantime: a new path 3 references chunk 2,
        // which the stale plan considered dead. The commit step re-plans
        // against V2 and calls apply with the *old* repack output: chunk 2
        // must keep pointing at a pack that still exists, and that pack must
        // not be deleted.
        let old = NOW - 30 * SECS_PER_DAY;
        let v1 = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 200, 0)
            .chunk(2, 1, 400, 0)
            .path(1, &[1], &[], old, NOW)
            .path(2, &[2], &[], old, old)
            .root("main", &[1], NOW)
            .build();
        let observations = observe_all(&v1, NOW);

        // Stale plan + executed repack against V1: copies only chunk 1.
        let stale_plan = plan(&v1, &observations, NOW, &policy());
        let repacks = fake_repack_output(&stale_plan);
        assert!(repacks.locations.contains_key(&chunk_hash(1)));
        assert!(!repacks.locations.contains_key(&chunk_hash(2)));

        // Concurrent push lands V2: path 3 re-references chunk 2 and pins it.
        let mut v2 = v1.clone();
        v2.paths.insert(
            path_hash(3),
            PathEntry {
                store_path: store_path(3),
                nar_hash: Hash32::digest([b'n', 3]),
                nar_size: 1000,
                references: vec![],
                ca: None,
                deriver: None,
                tree: tree_of(&[2]),
                last_reachable: 0,
                last_pushed: NOW,
            },
        );
        v2.roots.insert(
            "main".to_string(),
            Root {
                paths: [path_hash(1), path_hash(3)].into_iter().collect(),
                updated: NOW,
                run_id: None,
            },
        );

        // Commit re-plans against V2 and applies the old repack output.
        let fresh_plan = plan(&v2, &observations, NOW, &policy());
        let (committed, deletable) = apply(v2, &fresh_plan, &repacks);

        // Path 3 and chunk 2 survive.
        assert!(committed.paths.contains_key(&path_hash(3)));
        let chunk2 = &committed.chunks[&chunk_hash(2)];
        assert!(
            committed.packs.contains_key(&chunk2.pack),
            "chunk 2's pack must still be in the manifest"
        );
        // Pack 1 still holds chunk 2 → it must NOT be deletable.
        assert!(
            !deletable.contains(&pack_hash(1)),
            "no live path may ever reference a deleted pack"
        );
        // Chunk 1 still got relocated to the repack output.
        assert_ne!(committed.chunks[&chunk_hash(1)].pack, pack_hash(1));
        assert!(broken_paths(&committed).is_empty());
    }

    #[test]
    fn apply_is_idempotent_once_clean() {
        // After applying a plan, re-planning against the result (with
        // up-to-date observations) finds nothing left to do.
        let old = NOW - 30 * SECS_PER_DAY;
        let manifest = ManifestBuilder::new()
            .pack(1, TIER_VOLATILE, old)
            .chunk(1, 1, 200, 0)
            .chunk(2, 1, 400, 0)
            .path(1, &[1], &[], old, NOW)
            .path(2, &[2], &[], old, old)
            .root("main", &[1], NOW)
            .build();

        let first = plan(&manifest, &observe_all(&manifest, NOW), NOW, &policy());
        let repacks = fake_repack_output(&first);
        let (committed, _) = apply(manifest, &first, &repacks);

        let second = plan(&committed, &observe_all(&committed, NOW), NOW, &policy());
        let noop = GcPlan {
            now: NOW,
            ..GcPlan::default()
        };
        assert_eq!(second, noop, "second plan: {}", second.summary());
    }
}
