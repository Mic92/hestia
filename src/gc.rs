//! Garbage collection over the segmented store: compact every root to one
//! segment, repack mostly-dead packs, publish `g-<epoch+1>`, then delete
//! what neither the new view nor this run's inputs name. GC is the only
//! deleter and the only clock (`docs/spec/segments.qnt`).

use std::collections::{BTreeSet, HashMap};
use std::process::ExitCode;

use crate::backend::{self, Backend, Listed};
use crate::chunker::{PackBuilder, coalesce_adjacent, extract_chunk, pack_cache_key};
use crate::cli::GcArgs;
use crate::heads::{GcRecord, HeadName, RootId, RootRow, root_id};
use crate::manifest::{ChunkHash, PackHash, SegDigest};
use crate::pipeline::{now_unix, upload_pack};
use crate::segment::{self, Meta, PackRow, Relocated, Tree};
use crate::store::{self, Heads, fetch_pack_index, meta_key, tree_key};
use crate::trust::Trust;

pub const SECS_PER_HOUR: u64 = 3_600;
pub const SECS_PER_DAY: u64 = 86_400;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Backend(#[from] backend::Error),
    #[error(transparent)]
    Store(#[from] store::Error),
    #[error(transparent)]
    Trust(#[from] crate::trust::Error),
    #[error(transparent)]
    Segment(#[from] segment::Error),
    #[error(transparent)]
    Chunker(#[from] crate::chunker::Error),
    #[error("previous GC ran {0}s ago, less than the minimum interval")]
    TooSoon(u64),
}

#[derive(Debug, Clone)]
pub struct GcPolicy {
    /// Roots without a drain for this long are dropped (deleted branches).
    pub root_ttl: u64,
    /// Live packs not accessed for this long get a 1-byte LRU touch.
    pub touch_age: u64,
    /// Packs whose live-chunk ratio falls below this get repacked.
    pub min_liveness: f64,
    /// Unreferenced objects younger than this are kept: a drain may have
    /// uploaded them without having published its head yet.
    pub min_age: u64,
    /// Two GC heads closer than this would let a reader's view fall two
    /// epochs behind.
    pub min_interval: u64,
    pub pack_target_size: u64,
}

impl Default for GcPolicy {
    fn default() -> Self {
        Self {
            root_ttl: 14 * SECS_PER_DAY,
            touch_age: 4 * SECS_PER_DAY,
            min_liveness: 0.5,
            min_age: SECS_PER_HOUR,
            min_interval: SECS_PER_HOUR,
            pack_target_size: crate::pipeline::PACK_TARGET_SIZE,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GcStats {
    pub epoch: u64,
    pub roots: usize,
    pub roots_expired: usize,
    pub segments_written: usize,
    pub paths_dropped: usize,
    pub packs_repacked: usize,
    pub packs_evicted: usize,
    pub deleted: usize,
    pub touched: usize,
}

pub struct Gc {
    pub backend: Backend,
    pub trust: Trust,
    pub policy: GcPolicy,
    pub dry_run: bool,
}

struct Root {
    name: String,
    stamp: u64,
    inputs: Vec<(SegDigest, Meta)>,
}

/// Live chunks of one pack, OR-ed over every input segment.
struct PackUse {
    row: PackRow,
    bits: Vec<u8>,
}

impl PackUse {
    fn add(&mut self, bits: &[u8]) {
        if self.bits.len() < bits.len() {
            self.bits.resize(bits.len(), 0);
        }
        for (a, b) in self.bits.iter_mut().zip(bits) {
            *a |= b;
        }
    }
    fn ratio(&self) -> f64 {
        let live: u32 = self.bits.iter().map(|b| b.count_ones()).sum();
        f64::from(live) / f64::from(self.row.chunks.max(1))
    }
    fn is_live(&self, i: usize) -> bool {
        self.bits
            .get(i / 8)
            .is_some_and(|b| b & (1 << (i % 8)) != 0)
    }
}

/// Copies live chunks out of packs into new ones and remembers where they went.
#[derive(Default)]
struct Repacker {
    builder: PackBuilder,
    staged: Vec<((PackHash, u16), ChunkHash)>,
    moved: HashMap<(PackHash, u16), Relocated>,
}

impl Repacker {
    async fn seal(&mut self, gc: &Gc) -> Result<(), Error> {
        if self.builder.is_empty() {
            return Ok(());
        }
        let pack = std::mem::take(&mut self.builder).finish();
        if !gc.dry_run {
            upload_pack(&gc.backend, &pack).await?;
        }
        let size = pack.index().size();
        let n = pack.chunks.len() as u32;
        let position: HashMap<ChunkHash, u16> = pack
            .chunks
            .iter()
            .enumerate()
            .map(|(j, (h, _))| (*h, j as u16))
            .collect();
        for (from, hash) in self.staged.drain(..) {
            self.moved
                .insert(from, (pack.hash, size, n, position[&hash]));
        }
        Ok(())
    }
}

/// Everything a live segment pins.
fn object_keys(seg: &SegDigest, meta: &Meta) -> impl Iterator<Item = String> {
    [meta_key(seg), tree_key(&meta.tree)]
        .into_iter()
        .chain(meta.packs.iter().map(|p| pack_cache_key(&p.hash)))
}

impl Gc {
    async fn put(&self, key: &str, body: Vec<u8>) -> Result<(), Error> {
        if !self.dry_run {
            self.backend.put(key, body.into()).await?;
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, Error> {
        if self.dry_run {
            eprintln!("hestia gc: would delete {key}");
            return Ok(true);
        }
        Ok(self.backend.delete(key).await?)
    }

    pub async fn run(&self, now: u64) -> Result<GcStats, Error> {
        let mut stats = GcStats::default();
        let heads = Heads::load(&self.backend, &self.trust).await?;
        let prev = heads.gc.as_ref();
        if let Some(age) = prev.map(|g| now.saturating_sub(g.time))
            && age < self.policy.min_interval
            && !self.dry_run
        {
            return Err(Error::TooSoon(age));
        }
        stats.epoch = prev.map_or(0, |g| g.epoch) + 1;

        let objects = self.backend.list_objects().await?;
        let stored_packs: HashMap<PackHash, &Listed> = objects
            .iter()
            .filter_map(|l| Some((PackHash::from_hex(l.key.strip_prefix("pack-")?)?, l)))
            .collect();

        // Roots: a drain's segment names every path it wants kept, so once
        // any drain published, the previous GC segment is not an input.
        let prev_rows: HashMap<&str, &RootRow> = prev
            .into_iter()
            .flat_map(|g| g.roots.iter().map(|r| (r.name.as_str(), r)))
            .collect();
        let active: BTreeSet<RootId> = heads
            .view
            .heads
            .iter()
            .filter_map(|(h, _)| match HeadName::parse(h)? {
                HeadName::Drain { root, .. } | HeadName::Compaction { root, .. } => Some(root),
                HeadName::Gc { .. } => None,
            })
            .collect();
        let mut roots = Vec::new();
        let mut retired: BTreeSet<SegDigest> = heads.view.segments();
        for (name, segs) in &heads.view.roots {
            let base = prev_rows.get(name.as_str());
            let stamp = match base {
                Some(b) if !active.contains(&root_id(name)) => b.stamp,
                _ => now,
            };
            if now.saturating_sub(stamp) > self.policy.root_ttl {
                stats.roots_expired += 1;
                continue;
            }
            let mut inputs = Vec::new();
            for d in segs {
                if segs.len() > 1 && base.is_some_and(|b| b.seg == *d) {
                    continue;
                }
                match self.backend.get(&meta_key(d), None).await? {
                    Some(body) => inputs.push((*d, Meta::open(&body)?)),
                    None => eprintln!("hestia gc: segment {d} of {name} is gone, so are its paths"),
                }
            }
            roots.push(Root {
                name: name.clone(),
                stamp,
                inputs,
            });
        }

        // Repack packs whose live ratio over all inputs is too low.
        let mut usage: HashMap<PackHash, PackUse> = HashMap::new();
        for (_, meta) in roots.iter().flat_map(|r| &r.inputs) {
            for row in &meta.packs {
                usage
                    .entry(row.hash)
                    .or_insert(PackUse {
                        row: PackRow::new(row.hash, row.size, row.chunks),
                        bits: Vec::new(),
                    })
                    .add(&row.live_bits);
            }
        }
        let mut lost: BTreeSet<PackHash> = usage
            .keys()
            .filter(|p| !stored_packs.contains_key(p))
            .copied()
            .collect();
        stats.packs_evicted = lost.len();
        for p in &lost {
            eprintln!("hestia gc: pack {p} was evicted, dropping the paths that need it");
        }
        let mut repacker = Repacker::default();
        for (source, used) in &usage {
            if lost.contains(source) || used.ratio() >= self.policy.min_liveness {
                continue;
            }
            if self.repack(*source, used, &mut repacker).await? {
                stats.packs_repacked += 1;
            } else {
                lost.insert(*source);
            }
        }
        repacker.seal(self).await?;
        let touched = |m: &Meta| {
            m.packs.iter().any(|p| {
                lost.contains(&p.hash) || usage[&p.hash].ratio() < self.policy.min_liveness
            })
        };

        // Compact each root to one segment.
        let mut rows = Vec::new();
        let mut live_packs: BTreeSet<PackHash> = BTreeSet::new();
        let mut keep: BTreeSet<String> = BTreeSet::new();
        for root in roots {
            let clean = root.inputs.len() == 1
                && heads.view.roots[&root.name].len() == 1
                && !touched(&root.inputs[0].1);
            let (seg, meta) = if clean {
                root.inputs.into_iter().next().unwrap()
            } else {
                self.merge(&root.inputs, &lost, &repacker, &mut stats)
                    .await?
            };
            live_packs.extend(meta.packs.iter().map(|p| p.hash));
            keep.extend(object_keys(&seg, &meta));
            retired.remove(&seg);
            rows.push(RootRow {
                name: root.name,
                seg,
                stamp: root.stamp,
            });
        }
        stats.roots = rows.len();

        // Every listed head is folded, including writer heads too old for
        // the view: once their segment is gone they must not come back.
        let record = GcRecord {
            epoch: stats.epoch,
            roots: rows,
            origin: vec![],
            retired: retired.iter().copied().collect(),
            folded: heads.listed.iter().map(|l| l.key.clone()).collect(),
            orphan_cursor: None,
            time: now,
        };
        let body = store::signed(&self.trust, record.encode()).await?;
        self.put(&record.head_name(&body).to_string(), body).await?;

        // Sweep. This run's inputs and their packs stay one more epoch: a
        // reader may hold the previous view. What the last run retired goes
        // now, and where the backend can list, so does anything else
        // unreferenced and older than `min_age` (drains that never published).
        for d in &retired {
            if let Some(body) = self.backend.get(&meta_key(d), None).await? {
                keep.extend(object_keys(d, &Meta::open(&body)?));
            }
        }
        let mut sweep: BTreeSet<String> = BTreeSet::new();
        for d in prev.into_iter().flat_map(|g| &g.retired) {
            if let Some(body) = self.backend.get(&meta_key(d), None).await? {
                sweep.extend(object_keys(d, &Meta::open(&body)?));
            }
        }
        for l in &objects {
            if l.created
                .is_some_and(|c| now.saturating_sub(c) > self.policy.min_age)
            {
                sweep.insert(l.key.clone());
            }
        }
        sweep.extend(record.folded.iter().cloned());
        for key in sweep.difference(&keep) {
            if self.delete(key).await? {
                stats.deleted += 1;
            }
        }

        if !self.dry_run {
            for hash in &live_packs {
                let idle = stored_packs
                    .get(hash)
                    .and_then(|l| l.last_accessed)
                    .map_or(0, |t| now.saturating_sub(t));
                if idle > self.policy.touch_age {
                    match self.backend.touch(&pack_cache_key(hash)).await {
                        Ok(touched) => stats.touched += usize::from(touched),
                        Err(err) => eprintln!("hestia gc: touch {hash} failed: {err}"),
                    }
                }
            }
        }
        if !self.dry_run {
            self.backend.flush().await?;
        }
        Ok(stats)
    }

    /// `false` if the pack or its index vanished meanwhile.
    async fn repack(
        &self,
        source: PackHash,
        used: &PackUse,
        out: &mut Repacker,
    ) -> Result<bool, Error> {
        let index = match fetch_pack_index(&self.backend, &used.row).await {
            Ok(index) => index,
            Err(store::Error::MissingPack(_)) => return Ok(false),
            Err(err) => return Err(err.into()),
        };
        let live = index
            .entries
            .iter()
            .enumerate()
            .filter(|(i, _)| used.is_live(*i));
        for run in coalesce_adjacent(live, |(_, e)| (e.offset, e.compressed_size)) {
            let start = run[0].1.offset;
            let last = run[run.len() - 1].1;
            let end = last.offset + u64::from(last.compressed_size);
            let Some(data) = self
                .backend
                .get(&pack_cache_key(&source), Some(start..end))
                .await?
                .filter(|d| d.len() as u64 == end - start)
            else {
                return Ok(false);
            };
            for (i, e) in run {
                let from = (e.offset - start) as usize;
                let frame = &data[from..from + e.compressed_size as usize];
                let raw = extract_chunk(frame, &e.hash)?;
                out.builder.add_compressed(e.hash, frame, raw.len() as u32);
                out.staged.push(((source, i as u16), e.hash));
                if out.builder.compressed_size() >= self.policy.pack_target_size {
                    out.seal(self).await?;
                }
            }
        }
        Ok(true)
    }

    async fn merge(
        &self,
        inputs: &[(SegDigest, Meta)],
        lost: &BTreeSet<PackHash>,
        repacker: &Repacker,
        stats: &mut GcStats,
    ) -> Result<(SegDigest, Meta), Error> {
        let mut trees = Vec::new();
        for (_, m) in inputs {
            let body = self
                .backend
                .get(&tree_key(&m.tree), None)
                .await?
                .ok_or_else(|| store::Error::Missing(tree_key(&m.tree)))?;
            trees.push(Tree::open(&body)?);
        }
        let pairs: Vec<(&Meta, &Tree)> = inputs.iter().map(|(_, m)| m).zip(&trees).collect();
        let (sealed, dropped) = segment::merge(&pairs, |row, i| {
            if lost.contains(&row.hash) {
                return None;
            }
            let here = (row.hash, row.size, row.chunks, i);
            Some(repacker.moved.get(&(row.hash, i)).copied().unwrap_or(here))
        })?;
        stats.paths_dropped += dropped;
        stats.segments_written += 1;
        let d = sealed.digest();
        let meta = Meta::open(&sealed.meta)?;
        self.put(&tree_key(&meta.tree), sealed.tree).await?;
        self.put(&meta_key(&d), sealed.meta).await?;
        Ok((d, meta))
    }
}

pub async fn run(args: &GcArgs) -> ExitCode {
    let http = reqwest::Client::new();
    let backend = match Backend::from_env(http.clone()) {
        Ok(b) => b,
        Err(err) => {
            eprintln!(
                "hestia gc: {err}\n\
                 hint: GC needs the cache tokens the hestia action exports and \
                 GITHUB_TOKEN with `actions: write`"
            );
            return ExitCode::FAILURE;
        }
    };
    let trust = match Trust::from_env() {
        Ok(t) => t,
        Err(err) => {
            eprintln!("hestia gc: {err}");
            return ExitCode::FAILURE;
        }
    };
    let gc = Gc {
        backend,
        trust,
        policy: GcPolicy {
            root_ttl: args.root_ttl * SECS_PER_DAY,
            touch_age: args.touch_age * SECS_PER_DAY,
            ..GcPolicy::default()
        },
        dry_run: args.dry_run,
    };
    match gc.run(now_unix()).await {
        Ok(stats) => {
            eprintln!("hestia gc: {stats:?}");
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

    #[test]
    fn pack_use_ors_bitsets() {
        let mut u = PackUse {
            row: PackRow::new(PackHash([0; 32]), 0, 16),
            bits: vec![],
        };
        u.add(&[0b0000_0101]);
        u.add(&[0b0000_0100, 0b1]);
        assert_eq!(u.ratio(), 3.0 / 16.0);
        assert!(u.is_live(0) && u.is_live(2) && u.is_live(8) && !u.is_live(1));
    }
}
