//! Latency benchmark for the manifest v4 segment format (PLAN.md).
//!
//! Measures real CPU cost, then feeds it into a small event simulation with
//! injected network latency (narinfo burst of a `nix build`).
//!
//!   cargo run --release -p hestia-core --example manifest_bench -- [paths_per_seg] [rtt_ms] [mbit] [roots]
//!   e.g. 100 TB store, big closures, many branches:  200000 30 320 2000
//!   manifest_bench profile {find,meta,narinfo,tree,open}         # 3 s hot loop for `perf record`
//!
//! seg-lean  v4: per segment two objects (.meta for narinfo, .tree for NAR), each
//!             n | u64 prefix[n] | 12 B tail[n] | u32 off[n+1] | bodies, zstd with content size
//!           .meta body = name_len u8 | name | minicbor{nar_hash, nar_size, local_refs [u32], foreign_refs, deriver_name, ca}
//!           .tree body = minicbor [ {name, exec, chunks: bytes (u16 pack_idx,u16 chunk_idx)*} ]
//!           lookups borrow from the decompressed buffer, nothing is decoded up front
//! head      {root -> segment digests}, ~100 B/root, no filters (roots are namespaces, PLAN.md)
//!
//! Dropped after measuring (see git history): whole-map CBOR segments, 20-byte
//! memcmp index, u32-prefix filters, framed Range bodies, ciborium/prost bodies.

use hestia::manifest::{
    ChunkList, Directory, FileSystemObject, FileTree, Hash32, PathEntry, PathHash, Regular,
    StorePath, StorePathHash,
};
use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::time::{Duration, Instant};

const REFS: usize = 12;
const FILES: usize = 24;
const CHUNKS_PER_FILE: usize = 4;
const ZSTD_LEVEL: i32 = 9;

// ---------------------------------------------------------------- data epoch

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn bytes<const N: usize>(&mut self) -> [u8; N] {
        let mut b = [0u8; N];
        for c in b.chunks_mut(8) {
            let n = c.len();
            c.copy_from_slice(&self.next().to_le_bytes()[..n]);
        }
        b
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn store_path(h: &PathHash, name: &str) -> StorePath {
    format!("{h}-{name}").parse().unwrap()
}

fn epoch_entries(n: usize, seed: u64) -> Vec<(PathHash, PathEntry)> {
    epoch_entries_with(n, seed, &[])
}

/// `pool`: hashes outside this set that refs may point to (a pending
/// segment referencing the root's compacted closure).
fn epoch_entries_with(n: usize, seed: u64, pool: &[PathHash]) -> Vec<(PathHash, PathEntry)> {
    let mut rng = Rng(seed);
    let mut hashes: Vec<PathHash> = pool.to_vec();
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let ph = PathHash(StorePathHash::new(rng.bytes::<20>()));
        let references = (0..REFS.min(hashes.len()))
            .map(|_| {
                store_path(
                    &hashes[rng.below(hashes.len())],
                    "python3.12-somepackage-1.2.3",
                )
            })
            .collect();
        let mut dir = BTreeMap::new();
        const WORDS: [&str; 16] = [
            "lib",
            "share",
            "bin",
            "include",
            "python3.12",
            "site-packages",
            "locale",
            "doc",
            "man",
            "__pycache__",
            "gtk-4.0",
            "icons",
            "hicolor",
            "src",
            "tests",
            "data",
        ];
        let nfiles = 1 + rng.below(2 * FILES);
        for i in 0..nfiles {
            let nchunks = 1 + rng.below(2 * CHUNKS_PER_FILE);
            let chunks = (0..nchunks)
                .map(|_| hestia::manifest::Blake3Chunk(rng.bytes::<16>()))
                .collect();
            dir.insert(
                format!(
                    "{}/{}/{}-{:x}.{}",
                    WORDS[rng.below(16)],
                    WORDS[rng.below(16)],
                    WORDS[rng.below(16)],
                    rng.next() % 0xfffff,
                    ["so", "py", "h", "mo", "png"][i % 5]
                ),
                Box::new(FileTree(FileSystemObject::Regular(Regular {
                    executable: i % 3 == 0,
                    contents: ChunkList {
                        chunks,
                        rewrites: vec![],
                    },
                }))),
            );
        }
        let e = PathEntry {
            store_path: store_path(&ph, "python3.12-somepackage-1.2.3"),
            nar_hash: Hash32(rng.bytes::<32>()),
            nar_size: rng.next() % (64 << 20),
            references,
            ca: None,
            deriver: Some(store_path(&ph, "python3.12-somepackage-1.2.3.drv")),
            tree: FileTree(FileSystemObject::Directory(Directory { entries: dir })),
            last_reachable: 1_700_000_000,
            last_pushed: 1_700_000_000,
        };
        hashes.push(ph);
        v.push((ph, e));
    }
    v.sort_by(|a, b| AsRef::<[u8]>::as_ref(&a.0.0).cmp(AsRef::<[u8]>::as_ref(&b.0.0)));
    v
}

fn timeit<T>(f: impl FnOnce() -> T) -> (Duration, T) {
    let t = Instant::now();
    let r = f();
    (t.elapsed(), r)
}

fn per_op(iters: usize, mut f: impl FnMut(usize)) -> Duration {
    let t = Instant::now();
    for i in 0..iters {
        f(i);
    }
    t.elapsed() / iters as u32
}

// --------------------------------------------------------------- seg-lean

fn prefix64(k: &PathHash) -> u64 {
    let a: &[u8] = k.0.as_ref();
    u64::from_be_bytes(a[0..8].try_into().unwrap())
}

/// n u32 | pad | u64 prefix[n] | 12 B tail[n] | u32 off[n+1] | bodies
fn build_segment(
    entries: &[(PathHash, PathEntry)],
    body: impl Fn(&PathEntry) -> Vec<u8>,
) -> Vec<u8> {
    let n = entries.len();
    let bodies: Vec<Vec<u8>> = entries.iter().map(|(_, e)| body(e)).collect();
    let mut out = Vec::new();
    out.extend((n as u32).to_le_bytes());
    out.extend([0u8; 4]);
    for (h, _) in entries {
        out.extend(prefix64(h).to_le_bytes());
    }
    for (h, _) in entries {
        let a: &[u8] = h.0.as_ref();
        out.extend(&a[8..20]);
    }
    let mut off = 0u32;
    for b in &bodies {
        out.extend(off.to_le_bytes());
        off += b.len() as u32;
    }
    out.extend(off.to_le_bytes());
    for b in &bodies {
        out.extend(b);
    }
    out
}

struct Segment<'a> {
    n: usize,
    prefix: &'a [u64],
    tail: &'a [u8],
    offsets: &'a [u8],
    bodies: &'a [u8],
}
impl<'a> Segment<'a> {
    fn open(buf: &'a [u8]) -> Self {
        let n = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let p_end = 8 + n * 8;
        let t_end = p_end + n * 12;
        let o_end = t_end + (n + 1) * 4;
        assert!(
            (buf.as_ptr() as usize).is_multiple_of(8),
            "segment buffer must be 8-aligned"
        );
        // SAFETY: aligned (asserted), in bounds, u64 has no invalid bit patterns.
        let prefix = unsafe { std::slice::from_raw_parts(buf[8..p_end].as_ptr().cast::<u64>(), n) };
        Segment {
            n,
            prefix,
            tail: &buf[p_end..t_end],
            offsets: &buf[t_end..o_end],
            bodies: &buf[o_end..],
        }
    }
    fn index_bytes(&self) -> usize {
        8 + self.n * 24 + 4
    }
    #[inline]
    fn off(&self, i: usize) -> usize {
        u32::from_le_bytes(self.offsets[i * 4..i * 4 + 4].try_into().unwrap()) as usize
    }
    #[inline]
    fn find(&self, k: &PathHash) -> Option<usize> {
        if self.n == 0 {
            return None;
        }
        let x = prefix64(k);
        let p = self.prefix;
        // Keys are uniform: the expected slot is x/2^64 * n, stddev ~sqrt(n)/4.
        // Count keys < x in a fixed window around it: branch-free, vectorizes.
        const W: usize = 32;
        let guess = ((x as u128 * self.n as u128) >> 64) as usize;
        let wlo = guess.saturating_sub(W / 2).min(self.n.saturating_sub(W));
        let win = &p[wlo..(wlo + W).min(self.n)];
        let mut lo = wlo
            + win
                .iter()
                .map(|&v| (u64::from_le(v) < x) as usize)
                .sum::<usize>();
        let in_window =
            (lo > wlo || wlo == 0) && (lo < wlo + win.len() || wlo + win.len() == self.n);
        if !in_window {
            lo = p.partition_point(|&v| u64::from_le(v) < x);
        }
        let a: &[u8] = k.0.as_ref();
        while lo < self.n && u64::from_le(p[lo]) == x {
            if self.tail[lo * 12..lo * 12 + 12] == a[8..20] {
                return Some(lo);
            }
            lo += 1;
        }
        None
    }
    #[inline]
    fn body(&self, i: usize) -> &'a [u8] {
        &self.bodies[self.off(i)..self.off(i + 1)]
    }
    /// Reassemble "<hash>" of entry i from the index columns (for refs).
    fn hash(&self, i: usize) -> [u8; 20] {
        let mut h = [0u8; 20];
        h[..8].copy_from_slice(&u64::from_le(self.prefix[i]).to_be_bytes());
        h[8..].copy_from_slice(&self.tail[i * 12..i * 12 + 12]);
        h
    }
}

fn zstd_open(z: &[u8]) -> Vec<u8> {
    let cap = zstd::zstd_safe::get_frame_content_size(z)
        .ok()
        .flatten()
        .expect("content size") as usize;
    zstd::bulk::decompress(z, cap).unwrap()
}

// -- framed container (.tree): index raw, bodies in independent zstd frames

fn frame_entries() -> usize {
    std::env::var("FRAME")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256)
}

/// wire: nf u32 | index (as Segment, bodies empty) | u32 frame_off[nf+1] | zstd frames…
/// Body offsets in the index stay global (uncompressed positions), so a
/// body is frames[i/frame_entries()] decompressed, sliced at off[i]-off[base].
struct FramedBytes {
    index: Vec<u8>,
    frame_off: Vec<u32>,
    frames: Vec<u8>,
}
impl FramedBytes {
    fn wire_len(&self) -> usize {
        4 + self.index.len() + self.frame_off.len() * 4 + self.frames.len()
    }
}

/// Split an unframed container into index + compressed frames.
fn frame_container(raw: &[u8], level: i32) -> FramedBytes {
    let s = Segment::open(raw);
    let index = raw[..s.index_bytes()].to_vec();
    let mut frame_off = vec![0u32];
    let mut frames = Vec::with_capacity(s.bodies.len() / 3);
    let mut cctx = zstd::bulk::Compressor::new(level).unwrap();
    for f in 0..s.n.div_ceil(frame_entries()) {
        let a = s.off(f * frame_entries());
        let b = s.off(((f + 1) * frame_entries()).min(s.n));
        frames.extend(cctx.compress(&s.bodies[a..b]).unwrap());
        frame_off.push(frames.len() as u32);
    }
    FramedBytes {
        index,
        frame_off,
        frames,
    }
}

/// Reader over FramedBytes with a tiny decoded-frame cache.
struct Framed<'a> {
    idx: Segment<'a>,
    src: &'a FramedBytes,
    cache: std::cell::RefCell<Vec<(usize, Vec<u8>)>>,
    cap: usize,
}
impl<'a> Framed<'a> {
    fn open(src: &'a FramedBytes, cap: usize) -> Self {
        Framed {
            idx: Segment::open(&src.index),
            src,
            cache: Default::default(),
            cap,
        }
    }
    fn frame_z(&self, f: usize) -> &'a [u8] {
        &self.src.frames[self.src.frame_off[f] as usize..self.src.frame_off[f + 1] as usize]
    }
    fn frame_raw_len(&self, f: usize) -> usize {
        self.idx.off(((f + 1) * frame_entries()).min(self.idx.n))
            - self.idx.off(f * frame_entries())
    }
    /// Calls `k` with body i. Decompresses its frame unless cached.
    fn with_body<R>(&self, i: usize, k: impl FnOnce(&[u8]) -> R) -> R {
        let f = i / frame_entries();
        let mut c = self.cache.borrow_mut();
        let pos = match c.iter().position(|(cf, _)| *cf == f) {
            Some(p) => p,
            None => {
                if c.len() == self.cap {
                    c.remove(0);
                }
                c.push((
                    f,
                    zstd::bulk::decompress(self.frame_z(f), self.frame_raw_len(f)).unwrap(),
                ));
                c.len() - 1
            }
        };
        let base = self.idx.off(f * frame_entries());
        let r = &c[pos].1[self.idx.off(i) - base..self.idx.off(i + 1) - base];
        k(r)
    }
    fn resident(&self) -> usize {
        self.src.index.len()
            + self
                .cache
                .borrow()
                .iter()
                .map(|(_, v)| v.len())
                .sum::<usize>()
    }
}

/// Sequential cursor for GC: inputs are consumed in key order, so one
/// decoded frame per input suffices.
struct FrameCursor<'a> {
    r: Framed<'a>,
}
impl<'a> FrameCursor<'a> {
    fn new(src: &'a FramedBytes) -> Self {
        FrameCursor {
            r: Framed::open(src, 1),
        }
    }
}

// -- .meta body

#[derive(minicbor::Encode)]
struct MetaOwned {
    #[cbor(n(0), with = "minicbor::bytes")]
    nar_hash: [u8; 32],
    #[n(1)]
    nar_size: u64,
    #[cbor(n(2), with = "minicbor::bytes")]
    local_refs: Vec<u8>,
    /// space-separated full store paths outside this segment (rare)
    #[n(3)]
    foreign_refs: String,
    #[n(4)]
    deriver_name: Option<String>,
    #[n(5)]
    ca: Option<String>,
}
#[allow(dead_code)]
#[derive(minicbor::Decode)]
struct Meta<'a> {
    #[cbor(b(0), with = "minicbor::bytes")]
    nar_hash: &'a [u8],
    #[n(1)]
    nar_size: u64,
    /// u32 LE indices into this segment
    #[cbor(b(2), with = "minicbor::bytes")]
    local_refs: &'a [u8],
    #[b(3)]
    foreign_refs: &'a str,
    #[b(4)]
    deriver_name: Option<&'a str>,
    #[b(5)]
    ca: Option<&'a str>,
}

fn meta_body(e: &PathEntry, index_of: &HashMap<PathHash, u32>) -> Vec<u8> {
    let name = e.store_path.name().to_string();
    let mut v = vec![name.len() as u8];
    v.extend(name.as_bytes());
    let mut local_refs = Vec::new();
    let mut foreign_refs = Vec::new();
    for r in &e.references {
        match index_of.get(&PathHash(*r.hash())) {
            Some(i) => local_refs.extend(i.to_le_bytes()),
            None => foreign_refs.push(r.to_string()),
        }
    }
    let foreign_refs = foreign_refs.join(" ");
    v.extend(
        minicbor::to_vec(MetaOwned {
            nar_hash: e.nar_hash.0,
            nar_size: e.nar_size,
            local_refs,
            foreign_refs,
            deriver_name: e.deriver.as_ref().map(|d| d.name().to_string()),
            ca: e.ca.clone(),
        })
        .unwrap(),
    );
    v
}
#[inline]
fn body_name(b: &[u8]) -> &str {
    // SAFETY: written from a &str. Real code validates once at open.
    unsafe { std::str::from_utf8_unchecked(&b[1..1 + b[0] as usize]) }
}
#[inline]
fn body_meta(b: &[u8]) -> Meta<'_> {
    minicbor::decode(&b[1 + b[0] as usize..]).unwrap()
}

/// What the substituter does per narinfo hit: decode meta, resolve refs to
/// (hash, name) pairs. Returns something so the optimizer keeps it.
#[inline]
fn narinfo(seg: &Segment, k: &PathHash) -> Option<(u64, usize)> {
    let j = seg.find(k)?;
    let b = seg.body(j);
    let m = body_meta(b);
    let mut acc = body_name(b).len() + m.foreign_refs.len();
    // Remaining cost: one L3 miss per neighbour body. OoO already overlaps
    // them, explicit prefetch measured no gain.
    for r in m
        .local_refs
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()) as usize)
    {
        acc += seg.hash(r)[0] as usize + body_name(seg.body(r)).len();
    }
    Some((m.nar_size, acc))
}

// -- .tree body

#[derive(minicbor::Encode)]
struct FileOwned {
    #[n(0)]
    name: String,
    #[n(1)]
    executable: bool,
    #[cbor(n(2), with = "minicbor::bytes")]
    chunks: Vec<u8>,
}
#[allow(dead_code)]
#[derive(minicbor::Decode)]
struct File<'a> {
    #[b(0)]
    name: &'a str,
    #[n(1)]
    executable: bool,
    /// (pack_idx u16, chunk_idx u16) little-endian pairs
    #[cbor(b(2), with = "minicbor::bytes")]
    chunks: &'a [u8],
}

const PACKS_PER_SEG: usize = 64;

fn tree_body(e: &PathEntry) -> Vec<u8> {
    let FileSystemObject::Directory(d) = &e.tree.0 else {
        return minicbor::to_vec(Vec::<FileOwned>::new()).unwrap();
    };
    let mut rng = Rng(e.nar_size | 1);
    let files: Vec<FileOwned> = d
        .entries
        .iter()
        .filter_map(|(name, node)| match &node.0 {
            FileSystemObject::Regular(r) => Some(FileOwned {
                name: name.clone(),
                executable: r.executable,
                // a path's chunks sit in 1-3 packs, chunk_idx arbitrary within a ~1000-chunk pack
                chunks: {
                    let packs = [
                        rng.below(PACKS_PER_SEG) as u32,
                        rng.below(PACKS_PER_SEG) as u32,
                        rng.below(PACKS_PER_SEG) as u32,
                    ];
                    (0..r.contents.chunks.len())
                        .flat_map(|_| {
                            (packs[rng.below(3)] | ((rng.below(1000) as u32) << 16)).to_le_bytes()
                        })
                        .collect()
                },
            }),
            _ => None,
        })
        .collect();
    minicbor::to_vec(files).unwrap()
}

// ------------------------------------------------------- writer and GC

/// One segment as stored. `.meta` wire = u32 npacks | pack_table (40 B each:
/// hash+size) | zstd(container). GC liveness Range-reads just the table.
/// `.tree` is framed. pack_table maps local u16 -> global pack id.
struct SegBytes {
    pack_table: Vec<u32>,
    meta_z: Vec<u8>,
    tree: FramedBytes,
}
impl SegBytes {
    fn meta_wire_len(&self) -> usize {
        4 + self.pack_table.len() * 40 + self.meta_z.len()
    }
    fn pack_table_wire_len(&self) -> usize {
        4 + self.pack_table.len() * 40
    }
    fn wire_len(&self) -> usize {
        self.meta_wire_len() + self.tree.wire_len()
    }
}

fn seal(pack_table: Vec<u32>, meta_raw: &[u8], tree_raw: &[u8]) -> SegBytes {
    SegBytes {
        pack_table,
        meta_z: zstd::bulk::compress(meta_raw, ZSTD_LEVEL).unwrap(),
        tree: frame_container(tree_raw, ZSTD_LEVEL),
    }
}

/// What a drain does after upload: encode + compress its segment.
fn writer(entries: &[(PathHash, PathEntry)], pack_base: u32) -> SegBytes {
    let index_of: HashMap<PathHash, u32> = entries
        .iter()
        .enumerate()
        .map(|(i, (k, _))| (*k, i as u32))
        .collect();
    seal(
        (0..PACKS_PER_SEG as u32).map(|i| pack_base + i).collect(),
        &build_segment(entries, |e| meta_body(e, &index_of)),
        &build_segment(entries, tree_body),
    )
}

fn parse_ref_hash(s: &str) -> Option<[u8; 20]> {
    let sp: StorePath = s.parse().ok()?;
    let h: &[u8] = sp.hash().as_ref();
    h.try_into().ok()
}

/// Index columns for `order`, then bodies produced by `emit` appended
/// straight into the output (no per-body allocation).
fn write_container(
    order: &[([u8; 20], usize, usize)],
    body_hint: usize,
    mut emit: impl FnMut(usize, &mut Vec<u8>),
) -> Vec<u8> {
    let n = order.len();
    let o_start = 8 + n * 20;
    let b_start = o_start + (n + 1) * 4;
    let mut out = Vec::with_capacity(b_start + body_hint);
    out.extend((n as u32).to_le_bytes());
    out.extend([0u8; 4]);
    for (h, _, _) in order {
        out.extend(u64::from_be_bytes(h[..8].try_into().unwrap()).to_le_bytes());
    }
    for (h, _, _) in order {
        out.extend(&h[8..]);
    }
    out.resize(b_start, 0);
    for i in 0..n {
        let off = (out.len() - b_start) as u32;
        out[o_start + i * 4..o_start + i * 4 + 4].copy_from_slice(&off.to_le_bytes());
        emit(i, &mut out);
    }
    let off = (out.len() - b_start) as u32;
    out[o_start + n * 4..o_start + n * 4 + 4].copy_from_slice(&off.to_le_bytes());
    out
}

/// PathHash bytes are already uniform: hash = first 8 bytes.
#[derive(Default)]
struct IdHasher(u64);
impl std::hash::Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        self.0 = u64::from_le_bytes(b[..8].try_into().unwrap());
    }
    fn write_usize(&mut self, _: usize) {}
}
type IdMap<V> = HashMap<[u8; 20], V, std::hash::BuildHasherDefault<IdHasher>>;

/// GC compaction of one root: k-way merge by hash (later segment wins),
/// drop `dead`, rewrite .meta bodies (ref indices move, foreign refs may
/// become local), patch pack_idx in .tree bodies against the merged table.
struct CompactStats {
    peak_resident: usize,
}

fn compact(inputs: &[SegBytes], dead: &dyn Fn(&[u8; 20]) -> bool) -> (SegBytes, CompactStats) {
    let meta_raws: Vec<Vec<u8>> = inputs.iter().map(|s| zstd_open(&s.meta_z)).collect();
    let metas: Vec<Segment> = meta_raws.iter().map(|r| Segment::open(r)).collect();
    let trees: Vec<FrameCursor> = inputs.iter().map(|s| FrameCursor::new(&s.tree)).collect();

    // merged order
    let mut all: Vec<([u8; 20], usize, usize)> = Vec::new();
    for (si, m) in metas.iter().enumerate() {
        for i in 0..m.n {
            all.push((m.hash(i), si, i));
        }
    }
    all.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut order: Vec<([u8; 20], usize, usize)> = Vec::with_capacity(all.len());
    for e in all {
        if dead(&e.0) {
            continue;
        }
        match order.last_mut() {
            Some(l) if l.0 == e.0 => *l = e,
            _ => order.push(e),
        }
    }
    let new_index: IdMap<u32> = order
        .iter()
        .enumerate()
        .map(|(i, (h, _, _))| (*h, i as u32))
        .collect();

    // merged pack table
    let mut packs: Vec<u32> = inputs
        .iter()
        .flat_map(|s| s.pack_table.iter().copied())
        .collect();
    packs.sort_unstable();
    packs.dedup();
    let remap: Vec<Vec<u16>> = inputs
        .iter()
        .map(|s| {
            s.pack_table
                .iter()
                .map(|p| packs.binary_search(p).unwrap() as u16)
                .collect()
        })
        .collect();

    // .meta bodies
    let meta_hint: usize = meta_raws.iter().map(|r| r.len()).sum();
    let mut local = Vec::with_capacity(REFS * 4);
    let mut foreign: Vec<String> = Vec::new();
    let meta = write_container(&order, meta_hint, |oi, out| {
        let (_, si, i) = order[oi];
        let seg = &metas[si];
        let b = seg.body(i);
        let name = body_name(b);
        let m = body_meta(b);
        local.clear();
        foreign.clear();
        for r in m
            .local_refs
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()) as usize)
        {
            match new_index.get(&seg.hash(r)) {
                Some(ni) => local.extend(ni.to_le_bytes()),
                None => foreign.push(format!(
                    "{}-{}",
                    PathHash(StorePathHash::new(seg.hash(r))),
                    body_name(seg.body(r))
                )),
            }
        }
        for s in m.foreign_refs.split(' ').filter(|s| !s.is_empty()) {
            match parse_ref_hash(s).and_then(|h| new_index.get(&h).copied()) {
                Some(ni) => local.extend(ni.to_le_bytes()),
                None => foreign.push(s.to_string()),
            }
        }
        out.push(name.len() as u8);
        out.extend(name.as_bytes());
        let joined = foreign.join(" ");
        minicbor::encode(
            &MetaRef {
                nar_hash: m.nar_hash,
                nar_size: m.nar_size,
                local_refs: &local,
                foreign_refs: &joined,
                deriver_name: m.deriver_name,
                ca: m.ca,
            },
            minicbor::encode::write::Writer::new(out),
        )
        .unwrap();
    });

    // .tree bodies: same key order per input as .meta, so index i carries
    // over. Copy bytes and patch pack_idx in place, no CBOR re-encode.
    // Inputs are read through one-frame cursors (monotone per input), so
    // resident .tree input is inputs × 1 frame, not the whole thing.
    let tree_hint: usize = inputs.iter().map(|s| s.tree.frames.len() * 3).sum();
    let meta_in: usize = meta_raws.iter().map(|r| r.len()).sum();
    drop(metas);
    drop(meta_raws);
    let tree = write_container(&order, tree_hint, |oi, out| {
        let (_, si, i) = order[oi];
        let rm = &remap[si];
        trees[si].r.with_body(i, |body| {
            let start = out.len();
            out.extend(body);
            let files: Vec<File> = minicbor::decode(body).unwrap();
            for f in &files {
                let rel = f.chunks.as_ptr() as usize - body.as_ptr() as usize;
                let c = &mut out[start + rel..start + rel + f.chunks.len()];
                for q in c.chunks_exact_mut(4) {
                    let p = u16::from_le_bytes([q[0], q[1]]);
                    q[..2].copy_from_slice(&rm[p as usize].to_le_bytes());
                }
            }
        });
    });
    let tree_cursor_res: usize = trees.iter().map(|c| c.r.resident()).sum();
    // meta pass: inputs raw + output raw. tree pass: cursors + output raw
    // (output could be framed on the fly, counted here as built whole).
    let aux = order.len() * (36 + 32);
    let peak_resident = (meta_in + meta.len() + aux).max(tree_cursor_res + tree.len() + aux);

    (seal(packs, &meta, &tree), CompactStats { peak_resident })
}

/// Borrowing encoder twin of MetaOwned (same wire layout).
#[derive(minicbor::Encode)]
struct MetaRef<'a> {
    #[cbor(n(0), with = "minicbor::bytes")]
    nar_hash: &'a [u8],
    #[n(1)]
    nar_size: u64,
    #[cbor(n(2), with = "minicbor::bytes")]
    local_refs: &'a [u8],
    #[n(3)]
    foreign_refs: &'a str,
    #[n(4)]
    deriver_name: Option<&'a str>,
    #[n(5)]
    ca: Option<&'a str>,
}

// -------------------------------------------------------------- net model

#[derive(Clone, Copy)]
struct Net {
    rtt: Duration,
    bytes_per_sec: f64,
}
impl Net {
    fn fetch(&self, bytes: usize) -> Duration {
        self.rtt + Duration::from_secs_f64(bytes as f64 / self.bytes_per_sec)
    }
}

struct Cand {
    head_bytes: usize,
    cold_bytes: usize,
    open: Duration,
    miss: Duration,
    hit: Duration,
}

struct SimOut {
    first: Duration,
    wall: Duration,
    cpu: Duration,
    bytes: usize,
}

fn simulate(c: &Cand, net: Net, queries: usize, hit_rate: f64, conc: usize, segs: usize) -> SimOut {
    let mut rng = Rng(99);
    let ready = net.fetch(c.head_bytes)
        + if c.cold_bytes > 0 {
            net.fetch(c.cold_bytes) + c.open * segs as u32
        } else {
            Duration::ZERO
        };
    let mut lanes = vec![Duration::ZERO; conc];
    let mut first = Duration::MAX;
    let mut cpu = Duration::ZERO;
    for q in 0..queries {
        let lane = q % conc;
        let mut t = lanes[lane].max(ready);
        let mut w = c.miss * segs as u32;
        if (rng.next() as f64 / u64::MAX as f64) < hit_rate {
            w += c.hit;
        }
        cpu += w;
        t += w;
        lanes[lane] = t;
        first = first.min(t);
    }
    SimOut {
        first,
        wall: lanes.iter().copied().fold(Duration::ZERO, Duration::max),
        cpu,
        bytes: c.head_bytes + c.cold_bytes * segs,
    }
}

fn ms(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1e3)
}

// ------------------------------------------------------------------- main

struct Fixture {
    /// kept alive: freeing it leaves glibc with millions of free chunks and the
    /// next large malloc pays ~100 ms in malloc_consolidate, skewing "open".
    _entries: Vec<(PathHash, PathEntry)>,
    keys: Vec<PathHash>,
    hits: Vec<PathHash>,
    misses: Vec<PathHash>,
    seg: SegBytes,
    /// same .tree as one zstd frame, for comparison
    tree_z_single: Vec<u8>,
}

fn fixture(n: usize) -> Fixture {
    let entries = epoch_entries(n, 1);
    let keys: Vec<PathHash> = entries.iter().map(|(h, _)| *h).collect();
    let mut rng = Rng(42);
    let misses = (0..200_000)
        .map(|_| PathHash(StorePathHash::new(rng.bytes::<20>())))
        .collect();
    let hits = (0..200_000).map(|_| keys[rng.below(n)]).collect();
    let packs = (n / 40).max(PACKS_PER_SEG) as u32;
    let index_of: HashMap<PathHash, u32> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (*k, i as u32))
        .collect();
    let tree_raw = build_segment(&entries, tree_body);
    let tree_z_single = zstd::bulk::compress(&tree_raw, ZSTD_LEVEL).unwrap();
    let seg = seal(
        (0..packs).collect(),
        &build_segment(&entries, |e| meta_body(e, &index_of)),
        &tree_raw,
    );
    Fixture {
        _entries: entries,
        keys,
        hits,
        misses,
        seg,
        tree_z_single,
    }
}

fn profile(phase: &str) {
    if phase == "gc" {
        let f = fixture(50_000);
        let mut segsv = vec![f.seg];
        for p in 0..10 {
            segsv.push(writer(
                &epoch_entries_with(500, 100 + p as u64, &f.keys),
                ((p + 1) * PACKS_PER_SEG) as u32,
            ));
        }
        let dead = |h: &[u8; 20]| h[19] < 13;
        eprintln!("profiling gc…");
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            black_box(compact(&segsv, &dead));
        }
        return;
    }
    let f = fixture(20_000);
    let n = f.keys.len();
    let meta_raw = zstd_open(&f.seg.meta_z);
    let meta = Segment::open(&meta_raw);
    let tree = Framed::open(&f.seg.tree, 8);
    let mut rng = Rng(5);
    let probes: Vec<PathHash> = (0..1 << 16)
        .map(|i| {
            if i % 2 == 0 {
                f.hits[rng.below(n)]
            } else {
                f.misses[i]
            }
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut it = 0u64;
    eprintln!("profiling {phase}…");
    while Instant::now() < deadline {
        for i in 0..4096 {
            let k = &probes[(it as usize * 4096 + i) & 0xffff];
            match phase {
                "find" => {
                    black_box(meta.find(k));
                }
                "meta" => {
                    black_box(body_meta(meta.body(i % n)));
                }
                "narinfo" => {
                    black_box(narinfo(&meta, k));
                }
                "tree" => {
                    tree.with_body(i % n, |b| {
                        black_box(minicbor::decode::<Vec<File>>(b).unwrap());
                    });
                }
                "open" => {
                    black_box(zstd_open(&f.seg.meta_z));
                    break;
                }
                _ => panic!("phases: find meta narinfo tree open gc"),
            }
        }
        it += 1;
    }
    eprintln!("{it} outer iterations");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s.as_str()) == Some("profile") {
        return profile(args.get(2).map(|s| s.as_str()).unwrap_or("narinfo"));
    }
    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(20_000);
    let rtt_ms: f64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(30.0);
    let mbps: f64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(320.0);
    let net = Net {
        rtt: Duration::from_secs_f64(rtt_ms / 1e3),
        bytes_per_sec: mbps * 1e6 / 8.0,
    };
    let segs = 2usize;
    let roots: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(20);
    let queries = 3000usize;
    let hit_rate = 0.35;
    let conc = 25usize;
    let nars = 300usize;

    println!(
        "paths/segment={n} segs_served={segs} roots={roots} refs={REFS} files={FILES} chunks/file={CHUNKS_PER_FILE}"
    );
    println!(
        "net: rtt={rtt_ms}ms bw={mbps}Mbit/s   workload: {queries} narinfo queries, hit={hit_rate}, conc={conc}, then {nars} NARs\n"
    );

    let f = fixture(n);
    let (hits, misses) = (&f.hits, &f.misses);
    let head_bytes = 4096 + 100 * roots;
    println!("== head: {roots} roots ≈ {} KB\n", head_bytes / 1024);

    println!("== segment ({n} paths)");
    println!(
        "{:<16} {:>9} {:>9} {:>9} {:>9} {:>11} {:>9}",
        "", "wire KB", "B/path", "open ms", "find ns", "lookup ns", "miss ns"
    );

    // ---- .meta
    let (meta_open, meta_raw) = timeit(|| zstd_open(&f.seg.meta_z));
    let meta = Segment::open(&meta_raw);
    assert!(hits.iter().take(1000).all(|h| meta.find(h).is_some()));
    assert!(misses.iter().take(1000).all(|h| meta.find(h).is_none()));
    let m_find = per_op(hits.len(), |i| {
        black_box(meta.find(&hits[i]));
    });
    let m_miss = per_op(misses.len(), |i| {
        black_box(meta.find(&misses[i]));
    });
    let m_narinfo = per_op(hits.len(), |i| {
        black_box(narinfo(&meta, &hits[i]));
    });
    println!(
        "{:<16} {:>9} {:>9} {:>9} {:>9} {:>11} {:>9}   (pack table {} KB + zstd. raw: index 24 + body {} B/path)",
        ".meta",
        f.seg.meta_wire_len() / 1024,
        f.seg.meta_wire_len() / n,
        ms(meta_open),
        m_find.as_nanos(),
        m_narinfo.as_nanos(),
        m_miss.as_nanos(),
        f.seg.pack_table_wire_len() / 1024,
        (meta_raw.len() - meta.index_bytes()) / n
    );

    // ---- .tree single frame vs framed
    let (ts_open, ts_raw) = timeit(|| zstd_open(&f.tree_z_single));
    let ts = Segment::open(&ts_raw);
    let ts_lookup = per_op(hits.len() / 4, |i| {
        black_box(
            ts.find(&hits[i])
                .map(|j| minicbor::decode::<Vec<File>>(ts.body(j)).unwrap()),
        );
    });
    println!(
        "{:<16} {:>9} {:>9} {:>9} {:>9} {:>11} {:>9}   (resident {} MB)",
        ".tree 1 frame",
        f.tree_z_single.len() / 1024,
        f.tree_z_single.len() / n,
        ms(ts_open),
        "",
        ts_lookup.as_nanos(),
        "",
        ts_raw.len() >> 20
    );
    drop(ts_raw);

    let fr = &f.seg.tree;
    let nframes = fr.frame_off.len() - 1;
    let avg_frame_z = fr.frames.len() / nframes;
    let tree = Framed::open(fr, 16);
    let t_cold = per_op(2000, |i| {
        let k = &hits[i * 37 % hits.len()];
        tree.with_body(tree.idx.find(k).unwrap(), |b| {
            black_box(minicbor::decode::<Vec<File>>(b).unwrap());
        });
    });
    // warm: same few frames
    let t_warm = per_op(20000, |i| {
        tree.with_body(i % (frame_entries() * 8), |b| {
            black_box(minicbor::decode::<Vec<File>>(b).unwrap());
        });
    });
    println!(
        "{:<16} {:>9} {:>9} {:>9} {:>9} {:>11} {:>9}   (index {} KB raw + {} frames × ~{} KB. lookup cold {} µs = 1 frame inflate, warm {} ns. resident {} MB with 16 frames)",
        ".tree framed",
        fr.wire_len() / 1024,
        fr.wire_len() / n,
        "0.0",
        "",
        t_warm.as_nanos(),
        "",
        fr.index.len() / 1024,
        nframes,
        avg_frame_z / 1024,
        t_cold.as_micros(),
        t_warm.as_nanos(),
        tree.resident() >> 20
    );
    println!();

    // ---- narinfo burst + NAR phase
    let c = Cand {
        head_bytes,
        cold_bytes: f.seg.meta_wire_len(),
        open: meta_open,
        miss: m_miss,
        hit: m_narinfo,
    };
    for (label, net) in [
        ("as configured", net),
        (
            "slow link (rtt×3, bw/10)",
            Net {
                rtt: net.rtt * 3,
                bytes_per_sec: net.bytes_per_sec / 10.0,
            },
        ),
    ] {
        let o = simulate(&c, net, queries, hit_rate, conc, segs);
        // NAR phase: `nars` paths, each needs its .tree body. single: fetch whole once. framed: index once + distinct frames.
        let mut rng = Rng(3);
        let mut touched = std::collections::BTreeSet::new();
        for _ in 0..nars {
            touched.insert(rng.below(n) / frame_entries());
        }
        let single = net.fetch(f.tree_z_single.len()) + ts_open;
        let lanes = conc.min(touched.len()).max(1);
        let framed = net.fetch(fr.index.len() + 4 * nframes)
            + (net.fetch(avg_frame_z) + t_cold) * (touched.len().div_ceil(lanes)) as u32;
        println!("== job, {label}");
        println!(
            "   narinfo: first answer {} ms, wall {} ms, cpu {} ms, fetched {} KB",
            ms(o.first),
            ms(o.wall),
            ms(o.cpu),
            o.bytes / 1024
        );
        println!(
            "   .tree for {nars} NARs: 1 frame {} ms / {} KB   framed {} ms / {} KB ({} of {} frames, {lanes} parallel)",
            ms(single),
            f.tree_z_single.len() / 1024,
            ms(framed),
            (fr.index.len() + touched.len() * avg_frame_z) / 1024,
            touched.len(),
            nframes
        );
    }
    println!();

    // ---- writers
    let pending = 10usize;
    let k = 500usize;
    println!("== writer: drain of {k} new paths onto a {n}-path root");
    let mut segsv: Vec<SegBytes> = Vec::new();
    let mut wsum = Duration::ZERO;
    for p in 0..pending {
        let e = epoch_entries_with(k, 100 + p as u64, &f.keys);
        let (t, s) = timeit(|| writer(&e, ((p + 1) * PACKS_PER_SEG) as u32 + 1_000_000));
        wsum += t;
        segsv.push(s);
    }
    let wbytes: usize = segsv.iter().map(|s| s.wire_len()).sum::<usize>() / pending;
    println!(
        "   encode + zstd + frame: {} ms, {} KB on the wire ({} B/path, refs into root are foreign strings until compaction)",
        ms(wsum / pending as u32),
        wbytes / 1024,
        wbytes / k
    );
    println!(
        "   reader with {pending} pending segments: narinfo miss = {} × find ≈ {} ns",
        pending + 1,
        (m_miss * (pending as u32 + 1)).as_nanos()
    );
    println!();

    let mem_reader = format!(
        "   reader: .meta raw {} MB × {segs} + .tree index {} MB × {segs} + 16 frames {} MB = {} MB   (+ 40 KB per cached pack index)",
        meta_raw.len() >> 20,
        fr.index.len() >> 20,
        (tree.resident() - fr.index.len()) >> 20,
        ((meta_raw.len() + fr.index.len()) * segs + tree.resident() - fr.index.len()) >> 20
    );
    drop(tree);
    drop(meta_raw);

    // ---- GC
    println!("== GC: compact root = 1 × {n} + {pending} × {k} pending, 5 % of base tombstoned");
    let dead = |h: &[u8; 20]| h[19] < 13;
    let Fixture { seg: fseg, .. } = f;
    let mut inputs = vec![fseg];
    inputs.append(&mut segsv);
    let in_wire: usize = inputs.iter().map(|s| s.wire_len()).sum();
    let (gc_t, (out, st)) = timeit(|| compact(&inputs, &dead));
    let out_n = Segment::open(&out.tree.index).n;
    let om_raw = zstd_open(&out.meta_z);
    let om = Segment::open(&om_raw);
    let (mut loc, mut forn) = (0usize, 0usize);
    for i in (0..om.n).step_by(97).take(200) {
        let m = body_meta(om.body(i));
        loc += m.local_refs.len() / 4;
        forn += m.foreign_refs.split(' ').filter(|s| !s.is_empty()).count();
    }
    println!(
        "   in {} paths / {} MB wire → out {} paths / .meta {} KB + .tree {} KB wire.  refs after: {:.0} % local",
        n + pending * k,
        in_wire >> 20,
        out_n,
        out.meta_wire_len() / 1024,
        out.tree.wire_len() / 1024,
        100.0 * loc as f64 / (loc + forn).max(1) as f64
    );
    println!(
        "   CPU incl. inflate inputs, merge, rewrite, zstd + frame outputs: {} ms ({:.1} µs/path).  peak resident ≈ {} MB (meta pass whole, tree pass 1 frame per input)",
        ms(gc_t),
        gc_t.as_secs_f64() * 1e6 / (n + pending * k) as f64,
        st.peak_resident >> 20
    );

    let packs_per_root = inputs[0].pack_table.len();
    let (live_t, live_n) = timeit(|| {
        let mut live: Vec<u32> = Vec::with_capacity(roots * packs_per_root);
        let mut rng = Rng(9);
        for _ in 0..roots {
            for _ in 0..packs_per_root {
                live.push((rng.next() % (roots as u64 * packs_per_root as u64 / 3)) as u32);
            }
        }
        live.sort_unstable();
        live.dedup();
        live.len()
    });
    println!(
        "   pack liveness over {roots} roots × {packs_per_root} packs: {} ms ({} live, {} MB Vec<u32>)",
        ms(live_t),
        live_n,
        (roots * packs_per_root * 4) >> 20
    );

    let dirty = (roots / 10).max(1);
    let pend_wire = wbytes * pending.min(3);
    let read_dirty = dirty * (inputs[0].wire_len() + pend_wire);
    let read_tables = (roots - dirty) * inputs[0].pack_table_wire_len();
    let read_all_tree = roots * (inputs[0].wire_len()) + dirty * pend_wire;
    let write = dirty * out.wire_len() + head_bytes;
    println!(
        "   I/O per run, {dirty} dirty of {roots} roots: read {} MB (dirty segments {} MB + clean roots' pack tables {} MB), write {} MB → {:.0} s at {mbps} Mbit/s",
        (read_dirty + read_tables) >> 20,
        read_dirty >> 20,
        read_tables >> 20,
        write >> 20,
        (read_dirty + read_tables + write) as f64 / net.bytes_per_sec
    );
    println!(
        "   (pack table inside .tree instead: read {} MB → {:.0} s)",
        read_all_tree >> 20,
        read_all_tree as f64 / net.bytes_per_sec
    );

    println!();
    println!("== memory");
    println!("{mem_reader}");
    println!(
        "   writer: reader + {} KB pending segment + one open 64 MB pack buffer",
        wbytes >> 10
    );
    println!(
        "   gc: {} MB per root in flight + {} MB liveness",
        st.peak_resident >> 20,
        (roots * packs_per_root * 4) >> 20
    );
}
