//! Read side of the segmented store: heads → view → `.meta` of the served
//! roots. `.tree` and pack indexes are fetched on first use.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use futures_util::future::{join_all, try_join_all};
use tokio::sync::OnceCell;

use crate::backend::{Backend, Listed};
use crate::chunker::pack_cache_key;
use crate::heads::{self, CompactionRecord, GcRecord, HeadName, Signed, View, root_id};
use crate::manifest::{
    ChunkHash, ChunkList, ChunkLocation, Directory, FileSystemObject, FileTree, Hash32, PackHash,
    PathEntry, PathHash, Regular, SegDigest, StorePath, Symlink,
};
use crate::segment::{
    self, ChunkRef, Chunks, Meta, Node, PackIndex, PackRow, Sealed, SegmentWriter, Tree,
};
use crate::trust::{GC_ROW, Trust};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Backend(#[from] crate::backend::Error),
    #[error(transparent)]
    Segment(#[from] segment::Error),
    #[error(transparent)]
    Trust(#[from] crate::trust::Error),
    #[error("{0} missing from the store")]
    Missing(String),
    #[error("pack {0} missing from the store")]
    MissingPack(PackHash),
}

/// Every key but a head's is `<kind>-<sha256 of the body>`.
pub fn meta_key(d: &SegDigest) -> String {
    format!("seg-{d}")
}
pub fn tree_key(d: &SegDigest) -> String {
    format!("tree-{d}")
}

pub struct Segment {
    pub digest: SegDigest,
    pub meta: Meta,
    tree: OnceCell<Tree>,
}

/// Where chunks live, plus the index of each pack involved (read-ahead).
#[derive(Default)]
pub struct ChunkMap {
    pub chunks: BTreeMap<ChunkHash, ChunkLocation>,
    pub packs: HashMap<PackHash, Arc<PackIndex>>,
}

pub struct Resolved {
    pub entry: PathEntry,
    pub map: ChunkMap,
}

/// The store as seen through a set of roots. Immutable. A refresh builds
/// a new one that shares loaded segments and pack indexes.
pub struct Snapshot {
    backend: Backend,
    trust: Trust,
    roots: Vec<String>,
    listed: Vec<Listed>,
    pub view: View,
    /// In lookup priority: served roots in order, newest segment first.
    segments: Vec<Arc<Segment>>,
    pack_indexes: Mutex<HashMap<PackHash, Arc<PackIndex>>>,
}

async fn fetch(backend: &Backend, key: &str) -> Result<bytes::Bytes, Error> {
    backend
        .get(key, None)
        .await?
        .ok_or_else(|| Error::Missing(key.to_owned()))
}

pub async fn fetch_pack_index(backend: &Backend, row: &PackRow) -> Result<PackIndex, Error> {
    let range = PackIndex::range(row.size, row.chunks);
    let bytes = backend
        .get(&pack_cache_key(&row.hash), Some(range))
        .await?
        .ok_or(Error::MissingPack(row.hash))?;
    Ok(PackIndex::decode(&bytes)?)
}

async fn signed_record(backend: &Backend, name: &str) -> Result<Option<(Signed, Vec<u8>)>, Error> {
    Ok(backend
        .get(name, None)
        .await?
        .and_then(|body| Some((Signed::decode(&body).ok()?, body.to_vec()))))
}

async fn gc_record(
    backend: &Backend,
    trust: &Trust,
    name: &str,
) -> Result<Option<GcRecord>, Error> {
    let Some((s, body)) = signed_record(backend, name).await? else {
        return Ok(None);
    };
    let Some(r) = GcRecord::decode(&s.record)
        .ok()
        .filter(|r| Some(r.head_name(&body)) == HeadName::parse(name))
    else {
        return Ok(None);
    };
    Ok(trust.verify(GC_ROW, &s.record, &s.proof).await.then_some(r))
}

async fn compaction_record(
    backend: &Backend,
    trust: &Trust,
    name: &str,
) -> Result<Option<CompactionRecord>, Error> {
    let (Some((s, body)), Some(HeadName::Compaction { base_epoch, .. })) =
        (signed_record(backend, name).await?, HeadName::parse(name))
    else {
        return Ok(None);
    };
    let Some(r) = CompactionRecord::decode(&s.record)
        .ok()
        .filter(|r| r.head_name(base_epoch, &body).to_string() == name)
    else {
        return Ok(None);
    };
    Ok(trust
        .verify(&r.root, &s.record, &s.proof)
        .await
        .then_some(r))
}

/// A drain head's proof signs its own name.
async fn drain_verified(
    backend: &Backend,
    trust: &Trust,
    name: &str,
    root: &str,
) -> Result<bool, Error> {
    if trust.is_open() {
        return Ok(true);
    }
    let Some(body) = backend.get(name, None).await? else {
        return Ok(false);
    };
    Ok(trust.verify(root, name.as_bytes(), &body).await)
}

/// The head listing and what it resolves to.
pub struct Heads {
    pub listed: Vec<Listed>,
    /// Newest GC record whose body matches its name.
    pub gc: Option<GcRecord>,
    pub view: View,
}

impl Heads {
    /// Heads whose proof `trust` rejects count as not listed.
    pub async fn load(backend: &Backend, trust: &Trust) -> Result<Heads, Error> {
        let listed = backend.list_heads().await?;
        let names = || listed.iter().map(|l| l.key.as_str());
        let mut gc = None;
        for name in heads::newest_gc(names()) {
            if let Some(record) = gc_record(backend, trust, name).await? {
                gc = Some(record);
                break;
            }
        }
        let pending = heads::pending_heads(names(), gc.as_ref());
        let compactions: HashMap<String, CompactionRecord> = join_all(
            pending
                .iter()
                .filter(|(_, h)| matches!(h, HeadName::Compaction { .. }))
                .map(|(n, _)| async move {
                    compaction_record(backend, trust, n)
                        .await
                        .map(|c| c.map(|c| ((*n).to_owned(), c)))
                }),
        )
        .await
        .into_iter()
        .filter_map(Result::transpose)
        .collect::<Result<_, _>>()?;
        let root_names: HashMap<_, &str> = gc
            .iter()
            .flat_map(|g| g.roots.iter().map(|r| r.name.as_str()))
            .chain(compactions.values().map(|c| c.root.as_str()))
            .map(|n| (root_id(n), n))
            .collect();
        let rejected: BTreeSet<&str> = join_all(
            pending
                .iter()
                .filter_map(|(n, h)| match h {
                    HeadName::Drain { root, .. } => Some((*n, *root_names.get(root)?)),
                    _ => None,
                })
                .map(|(n, root)| async move {
                    drain_verified(backend, trust, n, root)
                        .await
                        .map(|ok| (!ok).then_some(n))
                }),
        )
        .await
        .into_iter()
        .filter_map(Result::transpose)
        .collect::<Result<_, _>>()?;
        let accepted = || names().filter(|n| !rejected.contains(n));
        let view = View::compute(accepted(), gc.as_ref(), &compactions);
        Ok(Heads { listed, gc, view })
    }
}

impl Snapshot {
    pub async fn load(
        backend: Backend,
        trust: Trust,
        roots: &[String],
        previous: Option<&Snapshot>,
    ) -> Result<Snapshot, Error> {
        let pack_indexes = previous
            .map(|p| p.pack_indexes.lock().unwrap().clone())
            .unwrap_or_default();
        Self::load_with(
            backend,
            trust,
            roots,
            previous.into_iter().flat_map(|p| &p.segments),
            pack_indexes,
        )
        .await
    }

    /// [`Self::load`] reusing `known` segments and pack indexes.
    async fn load_with(
        backend: Backend,
        trust: Trust,
        roots: &[String],
        known: impl Iterator<Item = &Arc<Segment>> + Send,
        pack_indexes: HashMap<PackHash, Arc<PackIndex>>,
    ) -> Result<Snapshot, Error> {
        let Heads { listed, view, .. } = Heads::load(&backend, &trust).await?;
        let loaded: HashMap<SegDigest, Arc<Segment>> =
            known.map(|s| (s.digest, s.clone())).collect();
        let wanted = roots
            .iter()
            .filter_map(|r| view.roots.get(r))
            .flat_map(|d| d.iter().rev());
        let segments = join_all(wanted.map(|digest| {
            let (backend, loaded) = (&backend, &loaded);
            async move {
                if let Some(s) = loaded.get(digest) {
                    return Some(s.clone());
                }
                let meta = match fetch(backend, &meta_key(digest)).await {
                    Ok(bytes) => Meta::open(&bytes).map_err(Error::from),
                    Err(err) => Err(err),
                };
                match meta {
                    Ok(meta) => Some(Arc::new(Segment {
                        digest: *digest,
                        meta,
                        tree: OnceCell::new(),
                    })),
                    // Evicted or corrupt: its paths miss and get pushed again.
                    Err(err) => {
                        eprintln!("hestia: skipping segment {digest}: {err}");
                        None
                    }
                }
            }
        }))
        .await
        .into_iter()
        .flatten()
        .collect();
        Ok(Snapshot {
            backend,
            trust,
            roots: roots.to_vec(),
            listed,
            view,
            segments,
            pack_indexes: Mutex::new(pack_indexes),
        })
    }

    /// Reload, then make sure `sealed` (just published under the first
    /// root) is served even if the listing does not show its head yet.
    pub async fn refresh_with(&self, sealed: &Sealed) -> Result<Snapshot, Error> {
        let fresh = Arc::new(Segment {
            digest: sealed.digest(),
            meta: Meta::open(&sealed.meta)?,
            tree: OnceCell::new_with(Some(Tree::open(&sealed.tree)?)),
        });
        let pack_indexes = self.pack_indexes.lock().unwrap().clone();
        let mut next = Snapshot::load_with(
            self.backend.clone(),
            self.trust.clone(),
            &self.roots,
            self.segments.iter().chain([&fresh]),
            pack_indexes,
        )
        .await?;
        if !next.segments.iter().any(|s| s.digest == fresh.digest) {
            next.segments.insert(0, fresh);
        }
        Ok(next)
    }

    /// Fold `root`'s pending segments into one `c-*` when this drain
    /// elects itself: enough are pending, no compaction for the root is in
    /// flight, and `coin` (uniform in 0..1) beats odds that let about one
    /// of the drains expected within the window win. Frees nothing, only
    /// shortens the next reader's load.
    pub async fn maybe_compact(
        &self,
        root: &str,
        now: u64,
        coin: f64,
    ) -> Result<Option<String>, Error> {
        let Some(live) = self.view.roots.get(root) else {
            return Ok(None);
        };
        let id = root_id(root);
        let created = |key: &str| HeadName::parse(key).map_or(now, |h| h.time());
        let in_flight = self.listed.iter().any(|l| {
            matches!(HeadName::parse(&l.key), Some(HeadName::Compaction { root, time, .. })
                if root == id && time + COMPACT_WINDOW > now)
        });
        // Head and segment stay paired so `subsumes` never names a head
        // whose segment `replaces` lacks.
        let pending: Vec<(&str, &Arc<Segment>)> = self
            .view
            .heads
            .iter()
            .filter(|(_, d)| live.contains(d))
            .filter_map(|(n, d)| Some((n.as_str(), self.segments.iter().find(|s| s.digest == *d)?)))
            .collect();
        if in_flight || pending.len() < COMPACT_MIN {
            return Ok(None);
        }
        let oldest = pending.iter().map(|(n, _)| created(n)).min().unwrap_or(now);
        let expected =
            pending.len() as f64 * COMPACT_WINDOW as f64 / now.saturating_sub(oldest).max(1) as f64;
        if coin * expected.max(1.0) >= 1.0 {
            return Ok(None);
        }
        let trees = try_join_all(pending.iter().map(|(_, seg)| self.tree(seg))).await?;
        let inputs: Vec<_> = pending
            .iter()
            .zip(trees)
            .map(|((_, s), t)| (&s.meta, t))
            .collect();
        let (sealed, _) = segment::merge(&inputs, segment::in_place)?;
        let record = CompactionRecord {
            root: root.to_owned(),
            added: put_segment(&self.backend, &sealed).await?,
            replaces: pending.iter().map(|(_, s)| s.digest).collect(),
            subsumes: pending.iter().map(|(n, _)| (*n).to_owned()).collect(),
            time: now,
        };
        Ok(Some(
            put_compaction(&self.backend, &self.trust, record, self.view.epoch).await?,
        ))
    }

    /// Copy a stored entry into `writer`. `false` if no served segment has it.
    pub async fn copy_entry(
        &self,
        hash: &PathHash,
        writer: &mut SegmentWriter,
    ) -> Result<bool, Error> {
        let Some((seg, i)) = self.find(hash) else {
            return Ok(false);
        };
        let mut node = self.tree(seg).await?.node(i)?;
        node.map_chunks(&mut |c| {
            let row = &seg.meta.packs[c.pack as usize];
            ChunkRef {
                pack: writer.pack(row.hash, row.size, row.chunks),
                ..c
            }
        });
        writer.push(seg.meta.entry(i, node));
        Ok(true)
    }

    /// Load the pack indexes behind stored entries with these names.
    pub async fn load_indexes_for(&self, names: &BTreeSet<&str>) -> Result<(), Error> {
        for seg in &self.segments {
            let hits: Vec<usize> = (0..seg.meta.len())
                .filter(|&i| names.contains(seg.meta.name(i)))
                .collect();
            if hits.is_empty() {
                continue;
            }
            let tree = self.tree(seg).await?;
            let mut packs = BTreeSet::new();
            for i in hits {
                tree.node(i)?
                    .for_each_chunk(&mut |c| _ = packs.insert(c.pack));
            }
            for p in packs {
                self.pack_index(&seg.meta.packs[p as usize]).await?;
            }
        }
        Ok(())
    }

    /// Chunks locatable without a fetch: every pack index loaded so far.
    pub fn known_chunks(&self) -> KnownChunks {
        let mut known = KnownChunks::default();
        for (pack, index) in self.pack_indexes.lock().unwrap().iter() {
            known.add(*pack, index);
        }
        known
    }

    pub fn path_count(&self) -> usize {
        self.segments.iter().map(|s| s.meta.len()).sum()
    }

    pub fn path_hashes(&self) -> BTreeSet<PathHash> {
        self.segments
            .iter()
            .flat_map(|s| (0..s.meta.len()).map(|i| s.meta.hash(i)))
            .collect()
    }

    pub fn pack_hashes(&self) -> BTreeSet<PackHash> {
        self.segments
            .iter()
            .flat_map(|s| s.meta.packs.iter().map(|p| p.hash))
            .collect()
    }

    pub fn by_nar_hash(&self, nar_hash: &Hash32) -> Option<PathHash> {
        self.segments.iter().find_map(|s| {
            (0..s.meta.len())
                .find_map(|i| (s.meta.body(i).nar_hash == *nar_hash).then(|| s.meta.hash(i)))
        })
    }

    /// Packs holding chunks of `hash` (empty if unknown).
    pub async fn packs_of(&self, hash: &PathHash) -> Result<BTreeSet<PackHash>, Error> {
        let mut packs = BTreeSet::new();
        if let Some((seg, i)) = self.find(hash) {
            self.tree(seg)
                .await?
                .node(i)?
                .for_each_chunk(&mut |c| _ = packs.insert(seg.meta.packs[c.pack as usize].hash));
        }
        Ok(packs)
    }

    fn find(&self, hash: &PathHash) -> Option<(&Segment, usize)> {
        self.segments
            .iter()
            .find_map(|s| s.meta.find(hash).map(|i| (&**s, i)))
    }

    pub fn contains(&self, hash: &PathHash) -> bool {
        self.find(hash).is_some()
    }

    /// The path realising CA output `id` (`drv^output`).
    pub fn realisation(&self, id: &str) -> Option<StorePath> {
        self.segments
            .iter()
            .find_map(|s| s.meta.realisation(id).map(|i| s.meta.store_path(i)))
    }

    /// Without the file tree: enough for narinfo.
    pub fn lookup(&self, hash: &PathHash) -> Option<PathEntry> {
        let (seg, i) = self.find(hash)?;
        let empty = Node::Directory {
            entries: BTreeMap::new(),
        };
        Some(path_entry(
            seg.meta.entry(i, empty.clone()),
            to_file_tree(&empty, &mut |_| unreachable!()),
        ))
    }

    async fn tree<'a>(&self, seg: &'a Segment) -> Result<&'a Tree, Error> {
        seg.tree
            .get_or_try_init(|| async {
                Ok(Tree::open(
                    &fetch(&self.backend, &tree_key(&seg.meta.tree)).await?,
                )?)
            })
            .await
    }

    async fn pack_index(&self, row: &PackRow) -> Result<Arc<PackIndex>, Error> {
        if let Some(idx) = self.pack_indexes.lock().unwrap().get(&row.hash) {
            return Ok(idx.clone());
        }
        let idx = Arc::new(fetch_pack_index(&self.backend, row).await?);
        self.pack_indexes
            .lock()
            .unwrap()
            .insert(row.hash, idx.clone());
        Ok(idx)
    }

    pub async fn resolve(&self, hash: &PathHash) -> Result<Option<Resolved>, Error> {
        let Some((seg, i)) = self.find(hash) else {
            return Ok(None);
        };
        let node = self.tree(seg).await?.node(i)?;

        let mut rows = BTreeSet::new();
        node.for_each_chunk(&mut |c| _ = rows.insert(c));
        let mut map = ChunkMap::default();
        let mut indexes: HashMap<u16, (PackHash, Arc<PackIndex>)> = HashMap::new();
        for c in rows {
            let (pack, index) = match indexes.get(&c.pack) {
                Some(x) => x,
                None => {
                    let row = &seg.meta.packs[c.pack as usize];
                    let index = self.pack_index(row).await?;
                    map.packs.insert(row.hash, index.clone());
                    indexes.entry(c.pack).or_insert((row.hash, index))
                }
            };
            let e = index
                .get(c.chunk)
                .ok_or_else(|| Error::Missing(format!("chunk {} of pack {pack}", c.chunk)))?;
            map.chunks.entry(e.hash).or_insert(ChunkLocation {
                pack: *pack,
                offset: e.offset,
                compressed_size: e.compressed_size,
                uncompressed_size: e.uncompressed_size,
            });
        }
        let tree = to_file_tree(&node, &mut |c| {
            indexes[&c.pack].1.entries[c.chunk as usize].hash
        });
        Ok(Some(Resolved {
            entry: path_entry(seg.meta.entry(i, node), tree),
            map,
        }))
    }
}

fn path_entry(e: segment::Entry, tree: FileTree<ChunkList>) -> PathEntry {
    PathEntry {
        store_path: e.path,
        nar_hash: e.nar_hash,
        nar_size: e.nar_size,
        references: e.references,
        ca: e.ca,
        deriver: e.deriver,
        realises: e.realises,
        tree,
    }
}

fn to_file_tree(
    node: &Node,
    hash_of: &mut impl FnMut(ChunkRef) -> ChunkHash,
) -> FileTree<ChunkList> {
    FileTree(match node {
        Node::Regular {
            executable,
            chunks,
            rewrites,
        } => FileSystemObject::Regular(Regular {
            executable: *executable,
            contents: ChunkList {
                chunks: chunks.0.iter().map(|c| hash_of(*c)).collect(),
                rewrites: rewrites.clone(),
            },
        }),
        Node::Symlink { target } => FileSystemObject::Symlink(Symlink {
            target: target.clone(),
        }),
        Node::Directory { entries } => FileSystemObject::Directory(Directory {
            entries: entries
                .iter()
                .map(|(n, c)| (n.clone(), Box::new(to_file_tree(c, hash_of))))
                .collect(),
        }),
    })
}

/// Chunks locatable through a loaded pack index.
#[derive(Default)]
pub struct KnownChunks {
    chunks: HashMap<ChunkHash, (PackHash, u16)>,
    /// `(size, entries)` per pack.
    packs: HashMap<PackHash, (u64, u32)>,
}

impl KnownChunks {
    pub fn add(&mut self, pack: PackHash, index: &PackIndex) {
        self.packs
            .insert(pack, (index.size(), index.entries.len() as u32));
        for (i, e) in index.entries.iter().enumerate() {
            self.chunks.entry(e.hash).or_insert((pack, i as u16));
        }
    }

    pub fn contains(&self, hash: &ChunkHash) -> bool {
        self.chunks.contains_key(hash)
    }
}

fn from_file_tree(
    tree: &FileTree<ChunkList>,
    writer: &mut SegmentWriter,
    known: &KnownChunks,
) -> Option<Node> {
    Some(match &tree.0 {
        FileSystemObject::Regular(r) => {
            let mut chunks = Vec::with_capacity(r.contents.chunks.len());
            for h in &r.contents.chunks {
                let &(pack, chunk) = known.chunks.get(h)?;
                let (size, n) = known.packs[&pack];
                chunks.push(ChunkRef {
                    pack: writer.pack(pack, size, n),
                    chunk,
                });
            }
            Node::Regular {
                executable: r.executable,
                chunks: Chunks(chunks),
                rewrites: r.contents.rewrites.clone(),
            }
        }
        FileSystemObject::Symlink(l) => Node::Symlink {
            target: l.target.clone(),
        },
        FileSystemObject::Directory(d) => {
            let mut entries = BTreeMap::new();
            for (name, child) in &d.entries {
                entries.insert(name.clone(), from_file_tree(child, writer, known)?);
            }
            Node::Directory { entries }
        }
    })
}

/// Add a legacy-shaped entry to `writer`. `None` if a chunk cannot be located.
pub fn push_entry(
    writer: &mut SegmentWriter,
    entry: &PathEntry,
    known: &KnownChunks,
) -> Option<()> {
    let tree = from_file_tree(&entry.tree, writer, known)?;
    writer.push(segment::Entry {
        path: entry.store_path.clone(),
        nar_hash: entry.nar_hash,
        nar_size: entry.nar_size,
        references: entry.references.clone(),
        deriver: entry.deriver.clone(),
        ca: entry.ca.clone(),
        realises: entry.realises.clone(),
        tree,
    });
    Some(())
}

/// Pending segments per root before a drain considers compacting them,
/// and how long a published `c-*` holds off the next attempt.
const COMPACT_MIN: usize = 4;
const COMPACT_WINDOW: u64 = 60;

async fn put_segment(backend: &Backend, sealed: &Sealed) -> Result<SegDigest, Error> {
    let (tree, meta) = (tree_key(&sealed.tree_digest()), meta_key(&sealed.digest()));
    tokio::try_join!(
        backend.put(&tree, sealed.tree.clone().into()),
        backend.put(&meta, sealed.meta.clone().into()),
    )?;
    Ok(sealed.digest())
}

pub async fn signed(trust: &Trust, record: Vec<u8>) -> Result<Vec<u8>, crate::trust::Error> {
    let proof = trust.sign(&record).await?;
    Ok(Signed { record, proof }.encode())
}

async fn put_compaction(
    backend: &Backend,
    trust: &Trust,
    record: CompactionRecord,
    base_epoch: u64,
) -> Result<String, Error> {
    let body = signed(trust, record.encode()).await?;
    let name = record.head_name(base_epoch, &body).to_string();
    backend.put(&name, body.into()).await?;
    Ok(name)
}

/// Upload a sealed segment and a head for it under `root`. A root the
/// view cannot name yet gets a `c-*` (which carries the name), else `h-*`.
pub async fn publish(
    backend: &Backend,
    trust: &Trust,
    view: &View,
    root: &str,
    sealed: &Sealed,
    now: u64,
) -> Result<String, Error> {
    let digest = put_segment(backend, sealed).await?;
    if !view.roots.contains_key(root) {
        let record = CompactionRecord {
            root: root.to_owned(),
            added: digest,
            replaces: vec![],
            subsumes: vec![],
            time: now,
        };
        return put_compaction(backend, trust, record, view.epoch).await;
    }
    let name = HeadName::Drain {
        base_epoch: view.epoch,
        root: root_id(root),
        time: now,
        seg: digest,
    }
    .to_string();
    // Never empty: the Actions cache refuses zero-byte entries.
    let mut body = trust.sign(name.as_bytes()).await?;
    if body.is_empty() {
        body.push(0);
    }
    backend.put(&name, body.into()).await?;
    Ok(name)
}
