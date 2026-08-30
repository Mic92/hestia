//! Segment encoding: the per-root, immutable unit of the manifest.
//!
//! ```text
//! index   = u32 n | 20 B PathHash[n] sorted | u32 body_off[n+1]
//! .meta   = "HSM1" | u32 header_len | cbor Header (pack table) | zstd( index | bodies )
//!           body = u8 name_len | name | cbor MetaBody
//! .tree   = "HST1" | u32 index_len | index | u32 frames | u32 frame_end[frames] | zstd frames
//!           body = cbor Node, TREE_FRAME_ENTRIES bodies per frame
//! pack index = "HSP1" | u32 n | (16 B ChunkHash, u64 offset, u32 compressed, u32 raw)[n]
//! ```
//!
//! `.meta` is fetched whole and bodies are decoded on lookup. `.tree` keeps
//! bodies compressed and inflates one frame at a time. The pack table is in
//! front of the zstd stream so GC can Range-read it alone.

use std::collections::{BTreeMap, HashMap};
use std::io::Read as _;
use std::sync::{Arc, Mutex};

use minicbor::{Decode, Decoder, Encode, Encoder};

use crate::manifest::{
    ChunkHash, NarHash, PackHash, PathHash, Rewrite, SegDigest, StorePath, StorePathHash,
};

const MAGIC_META: &[u8; 4] = b"HSM1";
const MAGIC_TREE: &[u8; 4] = b"HST1";
const MAGIC_PACK_INDEX: &[u8; 4] = b"HSP1";
const ZSTD_LEVEL: i32 = 9;
pub const TREE_FRAME_ENTRIES: usize = 256;

// Storage is untrusted: lengths from the wire are capped before allocating.
const MAX_ENTRIES: usize = 4_000_000;
const MAX_PACKS: usize = 1 << 16;
const MAX_HEADER_BYTES: usize = 64 << 20;
const MAX_META_RAW_BYTES: u64 = 2 << 30;
const MAX_TREE_FRAME_RAW_BYTES: usize = 256 << 20;
const MAX_PACK_INDEX_ENTRIES: usize = 1 << 16;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("segment: {0}")]
    Format(String),
    #[error("segment cbor: {0}")]
    Cbor(#[from] minicbor::decode::Error),
    #[error("segment zstd: {0}")]
    Io(#[from] std::io::Error),
}

fn bad(msg: impl Into<String>) -> Error {
    Error::Format(msg.into())
}

fn read_u32(buf: &[u8], at: usize) -> Result<usize, Error> {
    buf.get(at..at + 4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
        .ok_or_else(|| bad("truncated"))
}

fn read_u32s(buf: &[u8], at: usize, n: usize) -> Result<Vec<u32>, Error> {
    Ok(buf
        .get(at..at + n * 4)
        .ok_or_else(|| bad("truncated"))?
        .chunks_exact(4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .collect())
}

fn push_u32(out: &mut Vec<u8>, v: usize) {
    out.extend((v as u32).to_le_bytes());
}

// ---------------------------------------------------------------- types

/// One row of a segment's pack table. `ChunkRef::pack` indexes it.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PackRow {
    #[n(0)]
    pub hash: PackHash,
    #[n(1)]
    pub size: u64,
    /// Chunks of this pack referenced by this segment, for repack accounting.
    #[n(2)]
    pub live_count: u32,
    #[cbor(n(3), with = "minicbor::bytes")]
    pub live_bits: Vec<u8>,
}

impl PackRow {
    pub fn new(hash: PackHash, size: u64) -> Self {
        PackRow {
            hash,
            size,
            live_count: 0,
            live_bits: Vec::new(),
        }
    }

    fn mark_live(&mut self, chunk: u16) {
        let (byte, bit) = (chunk as usize / 8, chunk % 8);
        if self.live_bits.len() <= byte {
            self.live_bits.resize(byte + 1, 0);
        }
        if self.live_bits[byte] & (1 << bit) == 0 {
            self.live_bits[byte] |= 1 << bit;
            self.live_count += 1;
        }
    }

    pub fn is_live(&self, chunk: u16) -> bool {
        let (byte, bit) = (chunk as usize / 8, chunk % 8);
        self.live_bits
            .get(byte)
            .is_some_and(|b| b & (1 << bit) != 0)
    }
}

#[derive(Encode, Decode)]
struct Header {
    #[n(0)]
    packs: Vec<PackRow>,
}

/// A chunk by location: row in the segment's pack table, entry in that pack's index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkRef {
    pub pack: u16,
    pub chunk: u16,
}

/// A file's chunks, CBOR-encoded as one byte string of `(u16 pack, u16 chunk)` LE pairs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Chunks(pub Vec<ChunkRef>);

impl<C> Encode<C> for Chunks {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut Encoder<W>,
        _: &mut C,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        let bytes: Vec<u8> = self
            .0
            .iter()
            .flat_map(|c| (c.pack as u32 | (c.chunk as u32) << 16).to_le_bytes())
            .collect();
        e.bytes(&bytes)?.ok()
    }
}

impl<'b, C> Decode<'b, C> for Chunks {
    fn decode(d: &mut Decoder<'b>, _: &mut C) -> Result<Self, minicbor::decode::Error> {
        let p = d.position();
        let bytes = d.bytes()?;
        if !bytes.len().is_multiple_of(4) {
            return Err(minicbor::decode::Error::message("chunk list length").at(p));
        }
        Ok(Chunks(
            bytes
                .chunks_exact(4)
                .map(|b| ChunkRef {
                    pack: u16::from_le_bytes([b[0], b[1]]),
                    chunk: u16::from_le_bytes([b[2], b[3]]),
                })
                .collect(),
        ))
    }
}

/// File tree of one store path as stored in `.tree`.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum Node {
    #[n(0)]
    Regular {
        #[n(0)]
        executable: bool,
        #[n(1)]
        chunks: Chunks,
        #[n(2)]
        rewrites: Vec<Rewrite>,
    },
    #[n(1)]
    Symlink {
        #[n(0)]
        target: String,
    },
    #[n(2)]
    Directory {
        #[n(0)]
        entries: BTreeMap<String, Node>,
    },
}

impl Node {
    /// Every chunk reference in NAR order.
    pub fn for_each_chunk(&self, f: &mut impl FnMut(ChunkRef)) {
        match self {
            Node::Regular { chunks, .. } => chunks.0.iter().copied().for_each(f),
            Node::Symlink { .. } => {}
            Node::Directory { entries } => entries.values().for_each(|n| n.for_each_chunk(f)),
        }
    }

    pub fn map_chunks(&mut self, f: &mut impl FnMut(ChunkRef) -> ChunkRef) {
        match self {
            Node::Regular { chunks, .. } => chunks.0.iter_mut().for_each(|c| *c = f(*c)),
            Node::Symlink { .. } => {}
            Node::Directory { entries } => entries.values_mut().for_each(|n| n.map_chunks(f)),
        }
    }
}

/// Everything a segment knows about one store path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub path: StorePath,
    pub nar_hash: NarHash,
    pub nar_size: u64,
    pub references: Vec<StorePath>,
    pub deriver: Option<StorePath>,
    pub ca: Option<String>,
    pub tree: Node,
}

impl Entry {
    pub fn key(&self) -> PathHash {
        PathHash(*self.path.hash())
    }
}

/// A `.meta` body, borrowed from the segment buffer.
#[derive(Encode, Decode)]
pub struct MetaBody<'a> {
    #[n(0)]
    pub nar_hash: NarHash,
    #[n(1)]
    pub nar_size: u64,
    /// u32 LE positions in this segment.
    #[cbor(b(2), with = "minicbor::bytes")]
    local_refs: &'a [u8],
    /// Space-joined base names of references outside this segment.
    #[b(3)]
    foreign_refs: &'a str,
    #[b(4)]
    pub deriver: Option<&'a str>,
    #[b(5)]
    pub ca: Option<&'a str>,
}

impl MetaBody<'_> {
    pub fn local_refs(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        self.local_refs
            .chunks_exact(4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
    }
    pub fn foreign_refs(&self) -> impl Iterator<Item = &str> {
        self.foreign_refs.split(' ').filter(|s| !s.is_empty())
    }
}

// ---------------------------------------------------------------- index

type Key = [u8; 20];

fn key_of(k: &PathHash) -> &Key {
    k.0.as_ref()
}

/// Sorted keys plus body offsets, shared by `.meta` and `.tree`.
#[derive(Debug, Clone)]
pub struct Index {
    keys: Vec<Key>,
    body_off: Vec<u32>,
}

impl Index {
    /// Parse from the front of `buf`. Returns the index and the bytes it used.
    fn parse(buf: &[u8]) -> Result<(Index, usize), Error> {
        let n = read_u32(buf, 0)?;
        if n > MAX_ENTRIES {
            return Err(bad(format!("{n} entries exceeds cap")));
        }
        let keys_end = 4 + n * 20;
        let keys: Vec<Key> = buf
            .get(4..keys_end)
            .ok_or_else(|| bad("index truncated"))?
            .chunks_exact(20)
            .map(|b| b.try_into().unwrap())
            .collect();
        let body_off = read_u32s(buf, keys_end, n + 1)?;
        if keys.windows(2).any(|w| w[0] >= w[1]) {
            return Err(bad("index keys not sorted/unique"));
        }
        if body_off[0] != 0 || body_off.windows(2).any(|w| w[0] > w[1]) {
            return Err(bad("body offsets not monotonic"));
        }
        Ok((Index { keys, body_off }, keys_end + (n + 1) * 4))
    }

    fn write(keys: &[Key], body_lens: &[usize], out: &mut Vec<u8>) {
        push_u32(out, keys.len());
        for k in keys {
            out.extend(k);
        }
        let mut off = 0;
        push_u32(out, off);
        for len in body_lens {
            off += len;
            push_u32(out, off);
        }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
    pub fn find(&self, key: &PathHash) -> Option<usize> {
        self.keys.binary_search(key_of(key)).ok()
    }
    pub fn hash(&self, i: usize) -> PathHash {
        PathHash(StorePathHash::new(self.keys[i]))
    }
    fn body(&self, i: usize) -> std::ops::Range<usize> {
        self.body_off[i] as usize..self.body_off[i + 1] as usize
    }
    fn bodies_len(&self) -> usize {
        *self.body_off.last().unwrap() as usize
    }
}

// ---------------------------------------------------------------- .meta reader

/// An opened `.meta` object: pack table, index, and undecoded bodies.
pub struct Meta {
    pub packs: Vec<PackRow>,
    index: Index,
    bodies: Vec<u8>,
}

impl Meta {
    /// Bytes to Range-read from the front of a `.meta` for [`Meta::packs_only`].
    pub fn header_len(prefix: &[u8]) -> Result<usize, Error> {
        if prefix.get(..4) != Some(MAGIC_META) {
            return Err(bad("not a .meta object"));
        }
        let len = read_u32(prefix, 4)?;
        if len > MAX_HEADER_BYTES {
            return Err(bad("header exceeds cap"));
        }
        Ok(8 + len)
    }

    pub fn packs_only(prefix: &[u8]) -> Result<Vec<PackRow>, Error> {
        let end = Self::header_len(prefix)?;
        let header: Header =
            minicbor::decode(prefix.get(8..end).ok_or_else(|| bad("header truncated"))?)?;
        if header.packs.len() > MAX_PACKS {
            return Err(bad("pack table exceeds cap"));
        }
        Ok(header.packs)
    }

    pub fn open(bytes: &[u8]) -> Result<Meta, Error> {
        let packs = Self::packs_only(bytes)?;
        let compressed = &bytes[Self::header_len(bytes)?..];
        let mut raw = Vec::new();
        zstd::Decoder::with_buffer(compressed)?
            .take(MAX_META_RAW_BYTES + 1)
            .read_to_end(&mut raw)?;
        if raw.len() as u64 > MAX_META_RAW_BYTES {
            return Err(bad(".meta decompresses past cap"));
        }
        let (index, index_len) = Index::parse(&raw)?;
        if index_len + index.bodies_len() != raw.len() {
            return Err(bad(".meta body length mismatch"));
        }
        raw.drain(..index_len);
        let meta = Meta {
            packs,
            index,
            bodies: raw,
        };
        for i in 0..meta.len() {
            meta.validate(i)?;
        }
        Ok(meta)
    }

    /// Checked once at open so accessors need no error paths.
    fn validate(&self, i: usize) -> Result<(), Error> {
        let b = &self.bodies[self.index.body(i)];
        let name_len = *b.first().ok_or_else(|| bad("empty body"))? as usize;
        let name = b
            .get(1..1 + name_len)
            .ok_or_else(|| bad("name truncated"))?;
        harmonia_store_path::into_name(&name).map_err(|e| bad(format!("entry {i}: {e}")))?;
        let body: MetaBody = minicbor::decode(&b[1 + name_len..])?;
        if !body.local_refs.len().is_multiple_of(4) || body.local_refs().any(|r| r >= self.len()) {
            return Err(bad("local ref out of range"));
        }
        for p in body.foreign_refs().chain(body.deriver) {
            StorePath::from_base_path(p).map_err(|e| bad(e.to_string()))?;
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
    pub fn find(&self, key: &PathHash) -> Option<usize> {
        self.index.find(key)
    }
    pub fn hash(&self, i: usize) -> PathHash {
        self.index.hash(i)
    }
    pub fn name(&self, i: usize) -> &str {
        let b = &self.bodies[self.index.body(i)];
        std::str::from_utf8(&b[1..1 + b[0] as usize]).expect("validated")
    }
    pub fn store_path(&self, i: usize) -> StorePath {
        StorePath::from((self.hash(i).0, self.name(i).parse().expect("validated")))
    }
    pub fn body(&self, i: usize) -> MetaBody<'_> {
        let b = &self.bodies[self.index.body(i)];
        minicbor::decode(&b[1 + b[0] as usize..]).expect("validated")
    }

    pub fn references(&self, i: usize) -> Vec<StorePath> {
        let body = self.body(i);
        let mut refs: Vec<StorePath> = body.local_refs().map(|r| self.store_path(r)).collect();
        refs.extend(
            body.foreign_refs()
                .map(|r| StorePath::from_base_path(r).expect("validated")),
        );
        refs.sort();
        refs
    }

    /// Owned entry. The tree comes from the matching `.tree`.
    pub fn entry(&self, i: usize, tree: Node) -> Entry {
        let body = self.body(i);
        Entry {
            path: self.store_path(i),
            nar_hash: body.nar_hash,
            nar_size: body.nar_size,
            references: self.references(i),
            deriver: body
                .deriver
                .map(|d| StorePath::from_base_path(d).expect("validated")),
            ca: body.ca.map(str::to_owned),
            tree,
        }
    }
}

// ---------------------------------------------------------------- .tree reader

/// An opened `.tree` object. Bodies stay compressed. [`Tree::node`] inflates
/// the frame holding the entry and keeps the last few frames.
pub struct Tree {
    index: Index,
    frame_end: Vec<u32>,
    frames: Vec<u8>,
    cache: Mutex<Vec<(usize, Arc<Vec<u8>>)>>,
}

const TREE_CACHE_FRAMES: usize = 16;

impl Tree {
    pub fn open(bytes: &[u8]) -> Result<Tree, Error> {
        if bytes.get(..4) != Some(MAGIC_TREE) {
            return Err(bad("not a .tree object"));
        }
        let index_len = read_u32(bytes, 4)?;
        let (index, used) = Index::parse(
            bytes
                .get(8..8 + index_len)
                .ok_or_else(|| bad(".tree index truncated"))?,
        )?;
        if used != index_len {
            return Err(bad(".tree index length mismatch"));
        }
        let mut pos = 8 + index_len;
        let frames_n = read_u32(bytes, pos)?;
        pos += 4;
        if frames_n != index.len().div_ceil(TREE_FRAME_ENTRIES) {
            return Err(bad(".tree frame count mismatch"));
        }
        let frame_end = read_u32s(bytes, pos, frames_n)?;
        pos += frames_n * 4;
        let frames = bytes[pos..].to_vec();
        if frame_end.windows(2).any(|w| w[0] > w[1])
            || frame_end
                .last()
                .is_some_and(|&e| e as usize != frames.len())
        {
            return Err(bad(".tree frame table inconsistent"));
        }
        let tree = Tree {
            index,
            frame_end,
            frames,
            cache: Default::default(),
        };
        for f in 0..frames_n {
            if tree.frame_raw_range(f).len() > MAX_TREE_FRAME_RAW_BYTES {
                return Err(bad(".tree frame exceeds cap"));
            }
        }
        Ok(tree)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
    pub fn find(&self, key: &PathHash) -> Option<usize> {
        self.index.find(key)
    }

    /// Uncompressed byte range of frame `f` within the concatenated bodies.
    fn frame_raw_range(&self, f: usize) -> std::ops::Range<usize> {
        let first = f * TREE_FRAME_ENTRIES;
        let last = ((f + 1) * TREE_FRAME_ENTRIES).min(self.len());
        self.index.body_off[first] as usize..self.index.body_off[last] as usize
    }

    fn frame(&self, f: usize) -> Result<Arc<Vec<u8>>, Error> {
        let mut cache = self.cache.lock().unwrap();
        if let Some((_, raw)) = cache.iter().find(|(cf, _)| *cf == f) {
            return Ok(raw.clone());
        }
        let start = if f == 0 {
            0
        } else {
            self.frame_end[f - 1] as usize
        };
        let compressed = &self.frames[start..self.frame_end[f] as usize];
        let want = self.frame_raw_range(f).len();
        let raw = Arc::new(zstd::bulk::decompress(compressed, want)?);
        if raw.len() != want {
            return Err(bad(".tree frame length mismatch"));
        }
        if cache.len() == TREE_CACHE_FRAMES {
            cache.remove(0);
        }
        cache.push((f, raw.clone()));
        Ok(raw)
    }

    pub fn node(&self, i: usize) -> Result<Node, Error> {
        let f = i / TREE_FRAME_ENTRIES;
        let raw = self.frame(f)?;
        let base = self.frame_raw_range(f).start;
        let r = self.index.body(i);
        Ok(minicbor::decode(&raw[r.start - base..r.end - base])?)
    }
}

// ---------------------------------------------------------------- writer

/// Encoded segment, uploaded as `seg-<digest>.meta` and `.tree`.
pub struct Sealed {
    pub meta: Vec<u8>,
    pub tree: Vec<u8>,
}

impl Sealed {
    pub fn digest(&self) -> SegDigest {
        let mut h = blake3::Hasher::new();
        h.update(&(self.meta.len() as u64).to_le_bytes());
        h.update(&self.meta);
        h.update(&self.tree);
        SegDigest(*h.finalize().as_bytes())
    }
}

/// Builds one segment. Entries may arrive in any order. The same key pushed
/// twice keeps the last.
#[derive(Default)]
pub struct SegmentWriter {
    packs: Vec<PackRow>,
    pack_pos: HashMap<PackHash, u16>,
    entries: BTreeMap<Key, Entry>,
}

impl SegmentWriter {
    pub fn new(packs: Vec<PackRow>) -> Self {
        let pack_pos = packs
            .iter()
            .enumerate()
            .map(|(i, p)| (p.hash, i as u16))
            .collect();
        SegmentWriter {
            packs,
            pack_pos,
            entries: BTreeMap::new(),
        }
    }

    /// Row of `hash` in the pack table, appending it if new.
    pub fn pack(&mut self, hash: PackHash, size: u64) -> u16 {
        let packs = &mut self.packs;
        *self.pack_pos.entry(hash).or_insert_with(|| {
            packs.push(PackRow::new(hash, size));
            (packs.len() - 1) as u16
        })
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn push(&mut self, entry: Entry) {
        self.entries.insert(*key_of(&entry.key()), entry);
    }

    pub fn seal(mut self) -> Result<Sealed, Error> {
        if self.packs.len() > MAX_PACKS {
            return Err(bad("too many packs for one segment"));
        }
        for p in &mut self.packs {
            p.live_count = 0;
            p.live_bits.clear();
        }
        for e in self.entries.values() {
            let mut out_of_range = None;
            e.tree
                .for_each_chunk(&mut |c| match self.packs.get_mut(c.pack as usize) {
                    Some(p) => p.mark_live(c.chunk),
                    None => out_of_range = Some(c.pack),
                });
            if let Some(p) = out_of_range {
                return Err(bad(format!("{}: pack index {p} out of range", e.path)));
            }
        }
        // Unreferenced rows would pin dead packs against GC.
        let mut renumber = vec![0u16; self.packs.len()];
        let mut kept = 0u16;
        for (i, p) in self.packs.iter().enumerate() {
            if p.live_count > 0 {
                renumber[i] = kept;
                kept += 1;
            }
        }
        if kept as usize != self.packs.len() {
            self.packs.retain(|p| p.live_count > 0);
            for e in self.entries.values_mut() {
                e.tree.map_chunks(&mut |c| ChunkRef {
                    pack: renumber[c.pack as usize],
                    ..c
                });
            }
        }

        let keys: Vec<Key> = self.entries.keys().copied().collect();
        let position: HashMap<&Key, u32> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k, i as u32))
            .collect();

        let mut bodies = Vec::with_capacity(keys.len() * 160);
        let mut lens = Vec::with_capacity(keys.len());
        for e in self.entries.values() {
            let start = bodies.len();
            let name: &str = e.path.name().as_ref();
            bodies.push(name.len() as u8);
            bodies.extend(name.as_bytes());
            let mut local = Vec::new();
            let mut foreign = Vec::new();
            for r in &e.references {
                match position.get(key_of(&PathHash(*r.hash()))) {
                    Some(i) => local.extend(i.to_le_bytes()),
                    None => foreign.push(r.to_base_path()),
                }
            }
            let deriver = e.deriver.as_ref().map(StorePath::to_base_path);
            let body = MetaBody {
                nar_hash: e.nar_hash,
                nar_size: e.nar_size,
                local_refs: &local,
                foreign_refs: &foreign.join(" "),
                deriver: deriver.as_deref(),
                ca: e.ca.as_deref(),
            };
            minicbor::encode(&body, &mut bodies).expect("Vec write");
            lens.push(bodies.len() - start);
        }
        let mut raw = Vec::new();
        Index::write(&keys, &lens, &mut raw);
        raw.extend(&bodies);
        let header = minicbor::to_vec(Header { packs: self.packs }).expect("Vec write");
        let mut meta = Vec::new();
        meta.extend(MAGIC_META);
        push_u32(&mut meta, header.len());
        meta.extend(&header);
        meta.extend(zstd::bulk::compress(&raw, ZSTD_LEVEL)?);

        let mut lens = Vec::with_capacity(keys.len());
        let mut frames = Vec::new();
        let mut frame_end = Vec::new();
        let mut compressor = zstd::bulk::Compressor::new(ZSTD_LEVEL)?;
        let entries: Vec<&Entry> = self.entries.values().collect();
        for group in entries.chunks(TREE_FRAME_ENTRIES) {
            let mut raw = Vec::new();
            for e in group {
                let start = raw.len();
                minicbor::encode(&e.tree, &mut raw).expect("Vec write");
                lens.push(raw.len() - start);
            }
            if raw.len() > MAX_TREE_FRAME_RAW_BYTES {
                return Err(bad("tree frame exceeds cap"));
            }
            frames.extend(compressor.compress(&raw)?);
            frame_end.push(frames.len());
        }
        let mut index = Vec::new();
        Index::write(&keys, &lens, &mut index);
        let mut tree = Vec::new();
        tree.extend(MAGIC_TREE);
        push_u32(&mut tree, index.len());
        tree.extend(&index);
        push_u32(&mut tree, frame_end.len());
        for e in frame_end {
            push_u32(&mut tree, e);
        }
        tree.extend(&frames);

        Ok(Sealed { meta, tree })
    }
}

/// Merge segments of one root: later inputs win on the same key, `keep`
/// filters paths, pack tables are unioned and unreferenced packs dropped.
pub fn merge(inputs: &[(&Meta, &Tree)], keep: impl Fn(&PathHash) -> bool) -> Result<Sealed, Error> {
    let mut writer = SegmentWriter::default();
    for (meta, tree) in inputs {
        if meta.len() != tree.len() {
            return Err(bad(".meta and .tree disagree on entry count"));
        }
        let remap: Vec<u16> = meta
            .packs
            .iter()
            .map(|p| writer.pack(p.hash, p.size))
            .collect();
        if writer.packs.len() > MAX_PACKS {
            return Err(bad("merged pack table exceeds u16"));
        }
        for i in 0..meta.len() {
            if !keep(&meta.hash(i)) {
                continue;
            }
            let mut node = tree.node(i)?;
            node.map_chunks(&mut |c| ChunkRef {
                pack: remap[c.pack as usize],
                ..c
            });
            writer.push(meta.entry(i, node));
        }
    }
    writer.seal()
}

// ---------------------------------------------------------------- pack index

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackIndexEntry {
    pub hash: ChunkHash,
    pub offset: u64,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
}

/// Where each chunk sits inside one pack, in offset order. `ChunkRef::chunk` indexes it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackIndex {
    pub entries: Vec<PackIndexEntry>,
}

impl PackIndex {
    const ROW: usize = 16 + 8 + 4 + 4;

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.entries.len() * Self::ROW);
        out.extend(MAGIC_PACK_INDEX);
        push_u32(&mut out, self.entries.len());
        for e in &self.entries {
            out.extend(e.hash.0);
            out.extend(e.offset.to_le_bytes());
            out.extend(e.compressed_size.to_le_bytes());
            out.extend(e.uncompressed_size.to_le_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<PackIndex, Error> {
        if bytes.get(..4) != Some(MAGIC_PACK_INDEX) {
            return Err(bad("not a pack index"));
        }
        let n = read_u32(bytes, 4)?;
        if n > MAX_PACK_INDEX_ENTRIES || bytes.len() != 8 + n * Self::ROW {
            return Err(bad("pack index length"));
        }
        let mut entries = Vec::with_capacity(n);
        let mut prev_end = 0u64;
        for row in bytes[8..].chunks_exact(Self::ROW) {
            let e = PackIndexEntry {
                hash: crate::manifest::Blake3Chunk(row[..16].try_into().unwrap()),
                offset: u64::from_le_bytes(row[16..24].try_into().unwrap()),
                compressed_size: u32::from_le_bytes(row[24..28].try_into().unwrap()),
                uncompressed_size: u32::from_le_bytes(row[28..32].try_into().unwrap()),
            };
            if e.offset < prev_end {
                return Err(bad("pack index rows overlap or are unsorted"));
            }
            prev_end = e.offset + e.compressed_size as u64;
            entries.push(e);
        }
        Ok(PackIndex { entries })
    }

    pub fn get(&self, chunk: u16) -> Option<&PackIndexEntry> {
        self.entries.get(chunk as usize)
    }

    /// For chunk dedup against packs already in memory.
    pub fn positions(&self) -> HashMap<ChunkHash, u16> {
        self.entries
            .iter()
            .enumerate()
            .map(|(i, e)| (e.hash, i as u16))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Blake3Chunk, Blake3Pack, Hash32};

    fn path(seed: u32, name: &str) -> StorePath {
        let h = blake3::hash(&seed.to_le_bytes());
        StorePath::from((
            StorePathHash::new(h.as_bytes()[..20].try_into().unwrap()),
            name.parse().unwrap(),
        ))
    }

    fn c(pack: u16, chunk: u16) -> ChunkRef {
        ChunkRef { pack, chunk }
    }

    fn entry(seed: u32, refs: &[&StorePath], chunks: &[ChunkRef]) -> Entry {
        let p = path(seed, &format!("pkg{seed}-1.0"));
        let file = Node::Regular {
            executable: true,
            chunks: Chunks(chunks.to_vec()),
            rewrites: vec![Rewrite {
                offset: 3,
                ref_index: 0,
            }],
        };
        let tree = Node::Directory {
            entries: BTreeMap::from([
                (
                    "bin".to_string(),
                    Node::Directory {
                        entries: BTreeMap::from([("prog".to_string(), file)]),
                    },
                ),
                (
                    "self".to_string(),
                    Node::Symlink {
                        target: "bin/prog".into(),
                    },
                ),
            ]),
        };
        Entry {
            references: refs
                .iter()
                .map(|r| (*r).clone())
                .chain([p.clone()])
                .collect(),
            path: p,
            nar_hash: Hash32([seed as u8; 32]),
            nar_size: 1000 + seed as u64,
            deriver: Some(path(seed + 10_000, &format!("pkg{seed}-1.0.drv"))),
            ca: seed
                .is_multiple_of(3)
                .then(|| "fixed:r:sha256:abc".to_string()),
            tree,
        }
    }

    fn packs(n: usize) -> Vec<PackRow> {
        (0..n)
            .map(|i| PackRow::new(Blake3Pack([i as u8; 32]), 64 << 20))
            .collect()
    }

    fn roundtrip(entries: Vec<Entry>, packs: Vec<PackRow>) -> (Meta, Tree, Vec<Entry>) {
        let mut w = SegmentWriter::new(packs);
        for e in &entries {
            w.push(e.clone());
        }
        let sealed = w.seal().unwrap();
        let meta = Meta::open(&sealed.meta).unwrap();
        let tree = Tree::open(&sealed.tree).unwrap();
        let mut sorted = entries;
        sorted.sort_by_key(|e| *key_of(&e.key()));
        for e in &mut sorted {
            e.references.sort();
        }
        (meta, tree, sorted)
    }

    fn first_chunk(m: &Meta, t: &Tree, e: &Entry) -> ChunkRef {
        let mut out = None;
        t.node(m.find(&e.key()).unwrap())
            .unwrap()
            .for_each_chunk(&mut |r| out = out.or(Some(r)));
        out.unwrap()
    }

    #[test]
    fn roundtrip_preserves_entries() {
        let foreign = path(999_999, "glibc-2.40");
        let a = entry(1, &[&foreign], &[c(0, 5), c(1, 0)]);
        let b = entry(2, &[&a.path], &[c(1, 0), c(1, 700)]);
        let (meta, tree, want) = roundtrip(vec![b.clone(), a.clone()], packs(3));
        let got: Vec<Entry> = (0..meta.len())
            .map(|i| meta.entry(i, tree.node(i).unwrap()))
            .collect();
        assert_eq!(got, want);

        let ib = meta.find(&b.key()).unwrap();
        assert_eq!(meta.body(ib).local_refs().count(), 2);
        let ia = meta.find(&a.key()).unwrap();
        assert_eq!(
            meta.body(ia).foreign_refs().collect::<Vec<_>>(),
            vec![foreign.to_base_path()]
        );

        // (1,0) is shared by a and b and counts once. Unreferenced pack 2 is dropped
        assert_eq!(
            meta.packs.iter().map(|p| p.live_count).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(
            meta.packs[0].is_live(5) && meta.packs[1].is_live(700) && !meta.packs[1].is_live(1)
        );
    }

    #[test]
    fn find_across_frames() {
        let entries: Vec<Entry> = (0..3000)
            .map(|i| entry(i, &[], &[c(0, (i % 1024) as u16)]))
            .collect();
        let (meta, tree, want) = roundtrip(entries, packs(1));
        for (i, e) in want.iter().enumerate() {
            assert_eq!(meta.find(&e.key()), Some(i));
            assert_eq!(tree.find(&e.key()), Some(i));
        }
        for i in 3000..3500 {
            assert_eq!(meta.find(&PathHash(*path(i, "x").hash())), None);
        }
        assert_eq!(tree.node(2999).unwrap(), want[2999].tree);
        assert_eq!(tree.node(0).unwrap(), want[0].tree);
    }

    #[test]
    fn packs_only_reads_just_the_header() {
        let mut w = SegmentWriter::new(packs(4));
        w.push(entry(1, &[], &[c(3, 1)]));
        let s = w.seal().unwrap();
        let n = Meta::header_len(&s.meta[..8]).unwrap();
        let rows = Meta::packs_only(&s.meta[..n]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].hash, rows[0].live_count), (Blake3Pack([3; 32]), 1));
    }

    #[test]
    fn seal_rejects_pack_index_out_of_range() {
        let mut w = SegmentWriter::new(packs(1));
        w.push(entry(1, &[], &[c(1, 0)]));
        assert!(matches!(w.seal(), Err(Error::Format(_))));
    }

    #[test]
    fn merge_later_wins_remaps_packs_and_drops_dead() {
        let pa = packs(2); // [0], [1]
        let pb = vec![pa[1].clone(), PackRow::new(Blake3Pack([9; 32]), 1)]; // [1], [9]
        let e1 = entry(1, &[], &[c(0, 0)]);
        let e2_old = entry(2, &[], &[c(1, 1)]);
        let mut e2_new = entry(2, &[], &[c(1, 2)]); // pack [9]
        e2_new.nar_size = 5;
        let e3 = entry(3, &[&e1.path], &[c(0, 3)]); // pack [1]
        let dead = entry(4, &[], &[c(0, 0)]);

        let (ma, ta, _) = roundtrip(vec![e1.clone(), e2_old, dead.clone()], pa);
        let (mb, tb, _) = roundtrip(vec![e2_new.clone(), e3.clone()], pb);
        let sealed = merge(&[(&ma, &ta), (&mb, &tb)], |k| *k != dead.key()).unwrap();
        let (m, t) = (
            Meta::open(&sealed.meta).unwrap(),
            Tree::open(&sealed.tree).unwrap(),
        );

        assert_eq!(m.len(), 3);
        assert_eq!(m.find(&dead.key()), None);
        assert_eq!(m.body(m.find(&e2_new.key()).unwrap()).nar_size, 5);
        let hashes: Vec<PackHash> = m.packs.iter().map(|p| p.hash).collect();
        assert_eq!(
            hashes,
            vec![
                Blake3Pack([0; 32]),
                Blake3Pack([1; 32]),
                Blake3Pack([9; 32])
            ]
        );
        assert_eq!(first_chunk(&m, &t, &e1), c(0, 0));
        assert_eq!(first_chunk(&m, &t, &e2_new), c(2, 2));
        assert_eq!(first_chunk(&m, &t, &e3), c(1, 3));
        // e3 → e1 became local through the merge
        assert_eq!(m.body(m.find(&e3.key()).unwrap()).foreign_refs().count(), 0);
        // only e3's chunk 3 is left in pack [1]
        assert_eq!(m.packs[1].live_count, 1);
    }

    #[test]
    fn merge_drops_unreferenced_packs() {
        let (m, t, _) = roundtrip(vec![entry(1, &[], &[c(2, 0)])], packs(3));
        let sealed = merge(&[(&m, &t)], |_| true).unwrap();
        let m2 = Meta::open(&sealed.meta).unwrap();
        assert_eq!(
            m2.packs.iter().map(|p| p.hash).collect::<Vec<_>>(),
            vec![Blake3Pack([2; 32])]
        );
    }

    #[test]
    fn decoders_reject_garbage() {
        let mut w = SegmentWriter::new(packs(1));
        w.push(entry(1, &[], &[]));
        let s = w.seal().unwrap();
        assert!(Meta::open(&s.meta[..s.meta.len() - 3]).is_err());
        assert!(Meta::open(&s.tree).is_err());
        assert!(Tree::open(&s.meta).is_err());
        assert!(Tree::open(&s.tree[..s.tree.len() - 1]).is_err());
        assert!(PackIndex::decode(b"HSP1\x01\0\0\0").is_err());
    }

    #[test]
    fn pack_index_roundtrip() {
        let idx = PackIndex {
            entries: vec![
                PackIndexEntry {
                    hash: Blake3Chunk([1; 16]),
                    offset: 0,
                    compressed_size: 10,
                    uncompressed_size: 20,
                },
                PackIndexEntry {
                    hash: Blake3Chunk([2; 16]),
                    offset: 10,
                    compressed_size: 5,
                    uncompressed_size: 9,
                },
            ],
        };
        let back = PackIndex::decode(&idx.encode()).unwrap();
        assert_eq!(back, idx);
        assert_eq!(back.positions()[&Blake3Chunk([2; 16])], 1);
        let mut overlapping = idx.clone();
        overlapping.entries[1].offset = 5;
        assert!(PackIndex::decode(&overlapping.encode()).is_err());
    }
}
