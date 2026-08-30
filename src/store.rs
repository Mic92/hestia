//! Read side of the segmented store: heads → view → `.meta` of the served
//! roots. `.tree` and pack indexes are fetched on first use.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use tokio::sync::OnceCell;

use crate::backend::{Backend, Listed};
use crate::heads::{self, CompactionRecord, GcRecord, HeadName, View, root_id};
use crate::manifest::{
    ChunkHash, ChunkList, ChunkLocation, Directory, FileSystemObject, FileTree, Hash32, PackHash,
    PathEntry, PathHash, Regular, SegDigest, Symlink,
};
use crate::segment::{self, ChunkRef, Chunks, Meta, Node, PackIndex, Sealed, SegmentWriter, Tree};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Backend(#[from] crate::backend::Error),
    #[error(transparent)]
    Segment(#[from] segment::Error),
    #[error("{0} missing from the store")]
    Missing(String),
}

pub fn meta_key(d: &SegDigest) -> String {
    format!("seg-{d}.meta")
}
pub fn tree_key(d: &SegDigest) -> String {
    format!("seg-{d}.tree")
}
/// Not `pack-<h>.idx`: gha lookups are prefix matches.
pub fn pack_index_key(p: &PackHash) -> String {
    format!("idx-{p}")
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
    roots: Vec<String>,
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

/// A `g-*` body that decodes and hashes back to its name.
async fn gc_record(backend: &Backend, name: &str) -> Result<Option<GcRecord>, Error> {
    let (Some(body), Some(parsed)) = (backend.get(name, None).await?, HeadName::parse(name)) else {
        return Ok(None);
    };
    Ok(GcRecord::decode(&body)
        .ok()
        .filter(|r| r.head_name() == parsed))
}

async fn compaction_record(
    backend: &Backend,
    name: &str,
) -> Result<Option<CompactionRecord>, Error> {
    let (Some(body), Some(HeadName::Compaction { base_epoch, .. })) =
        (backend.get(name, None).await?, HeadName::parse(name))
    else {
        return Ok(None);
    };
    Ok(CompactionRecord::decode(&body)
        .ok()
        .filter(|r| r.head_name(base_epoch).to_string() == name))
}

/// The head listing and what it resolves to.
pub struct Heads {
    pub listed: Vec<Listed>,
    /// Newest GC record whose body matches its name, and when it was written.
    pub gc: Option<(GcRecord, Listed)>,
    pub view: View,
}

impl Heads {
    pub async fn load(backend: &Backend) -> Result<Heads, Error> {
        let mut listed = Vec::new();
        for prefix in ["g-", "h-", "c-"] {
            listed.extend(backend.list(prefix, None).await?.expect("unbounded"));
        }
        let names = || listed.iter().map(|l| l.key.as_str());
        let mut gc = None;
        for name in heads::newest_gc(names()) {
            if let Some(record) = gc_record(backend, name).await? {
                let entry = listed.iter().find(|l| l.key == name).unwrap().clone();
                gc = Some((record, entry));
                break;
            }
        }
        let record = gc.as_ref().map(|(r, _)| r);
        let mut compactions = HashMap::new();
        for name in heads::compactions_to_fetch(names(), record) {
            if let Some(c) = compaction_record(backend, name).await? {
                compactions.insert(name.to_owned(), c);
            }
        }
        let view = View::compute(names(), record, &compactions);
        Ok(Heads { listed, gc, view })
    }
}

impl Snapshot {
    pub async fn load(
        backend: Backend,
        roots: &[String],
        previous: Option<&Snapshot>,
    ) -> Result<Snapshot, Error> {
        let view = Heads::load(&backend).await?.view;
        let loaded: HashMap<SegDigest, Arc<Segment>> = previous
            .into_iter()
            .flat_map(|p| p.segments.iter().map(|s| (s.digest, s.clone())))
            .collect();
        let mut segments = Vec::new();
        for digest in roots
            .iter()
            .filter_map(|r| view.roots.get(r))
            .flat_map(|d| d.iter().rev())
        {
            if let Some(s) = loaded.get(digest) {
                segments.push(s.clone());
                continue;
            }
            // Evicted or corrupt: its paths miss and get pushed again.
            let meta =
                async { Ok::<_, Error>(Meta::open(&fetch(&backend, &meta_key(digest)).await?)?) };
            match meta.await {
                Ok(meta) => segments.push(Arc::new(Segment {
                    digest: *digest,
                    meta,
                    tree: OnceCell::new(),
                })),
                Err(err) => eprintln!("hestia: skipping segment {digest}: {err}"),
            }
        }
        let pack_indexes = previous
            .map(|p| p.pack_indexes.lock().unwrap().clone())
            .unwrap_or_default();
        Ok(Snapshot {
            backend,
            roots: roots.to_vec(),
            view,
            segments,
            pack_indexes: Mutex::new(pack_indexes),
        })
    }

    /// Reload, then make sure `sealed` (just published under the first
    /// root) is served even if the listing does not show its head yet.
    pub async fn refresh_with(&self, sealed: &Sealed) -> Result<Snapshot, Error> {
        let mut next = Snapshot::load(self.backend.clone(), &self.roots, Some(self)).await?;
        let digest = sealed.digest();
        if !next.segments.iter().any(|s| s.digest == digest) {
            let segment = Segment {
                digest,
                meta: Meta::open(&sealed.meta)?,
                tree: OnceCell::new_with(Some(Tree::open(&sealed.tree)?)),
            };
            next.segments.insert(0, Arc::new(segment));
        }
        Ok(next)
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
                self.pack_index(seg.meta.packs[p as usize].hash).await?;
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
                    &fetch(&self.backend, &tree_key(&seg.digest)).await?,
                )?)
            })
            .await
    }

    async fn pack_index(&self, pack: PackHash) -> Result<Arc<PackIndex>, Error> {
        if let Some(idx) = self.pack_indexes.lock().unwrap().get(&pack) {
            return Ok(idx.clone());
        }
        let idx = Arc::new(PackIndex::decode(
            &fetch(&self.backend, &pack_index_key(&pack)).await?,
        )?);
        self.pack_indexes.lock().unwrap().insert(pack, idx.clone());
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
                    let hash = seg.meta.packs[c.pack as usize].hash;
                    let index = self.pack_index(hash).await?;
                    map.packs.insert(hash, index.clone());
                    indexes.entry(c.pack).or_insert((hash, index))
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
        tree,
    });
    Some(())
}

/// Upload a sealed segment and a head for it under `root`. A root the
/// view cannot name yet gets a `c-*` (which carries the name), else `h-*`.
pub async fn publish(
    backend: &Backend,
    view: &View,
    root: &str,
    sealed: &Sealed,
) -> Result<String, Error> {
    let digest = sealed.digest();
    backend
        .put(&meta_key(&digest), sealed.meta.clone().into())
        .await?;
    backend
        .put(&tree_key(&digest), sealed.tree.clone().into())
        .await?;
    let (name, body) = if view.roots.contains_key(root) {
        (
            HeadName::Drain {
                base_epoch: view.epoch,
                root: root_id(root),
                seg: digest,
            }
            .to_string(),
            vec![0u8],
        )
    } else {
        let record = CompactionRecord {
            root: root.to_owned(),
            added: digest,
            replaces: vec![],
            subsumes: vec![],
        };
        (record.head_name(view.epoch).to_string(), record.encode())
    };
    backend.put(&name, body.into()).await?;
    Ok(name)
}
