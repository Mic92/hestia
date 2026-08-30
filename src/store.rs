//! Read side of the segmented store: heads → view → `.meta` of the served
//! roots. `.tree` and pack indexes are fetched on first use.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use tokio::sync::OnceCell;

use crate::backend::Backend;
use crate::heads::{self, CompactionRecord, GcRecord, HeadName, View, root_id};
use crate::manifest::{
    ChunkHash, ChunkList, ChunkLocation, Directory, FileSystemObject, FileTree, Manifest, PackHash,
    PathEntry, PathHash, Regular, SegDigest, Symlink,
};
use crate::segment::{
    self, ChunkRef, Chunks, Meta, Node, PackIndex, PackIndexEntry, Sealed, SegmentWriter, Tree,
};

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

/// Bodies of `names` that decode and hash back to their name.
async fn records<T>(
    backend: &Backend,
    names: Vec<&str>,
    verify: impl Fn(&[u8], &HeadName) -> Option<T>,
) -> Result<Vec<(String, T)>, Error> {
    let mut out = Vec::new();
    for name in names {
        let (Some(body), Some(parsed)) = (backend.get(name, None).await?, HeadName::parse(name))
        else {
            continue;
        };
        if let Some(record) = verify(&body, &parsed) {
            out.push((name.to_owned(), record));
        }
    }
    Ok(out)
}

impl Snapshot {
    pub async fn load(
        backend: Backend,
        roots: &[String],
        previous: Option<&Snapshot>,
    ) -> Result<Snapshot, Error> {
        let mut names = Vec::new();
        for prefix in ["g-", "h-", "c-"] {
            names.extend(
                backend
                    .list(prefix, None)
                    .await?
                    .expect("unbounded")
                    .into_iter()
                    .map(|l| l.key),
            );
        }
        let names = || names.iter().map(String::as_str);
        let gc = records(&backend, heads::newest_gc(names()), |body, name| {
            GcRecord::decode(body)
                .ok()
                .filter(|r| r.head_name() == *name)
        })
        .await?
        .into_iter()
        .next()
        .map(|(_, r)| r);
        let compactions: HashMap<_, _> = records(
            &backend,
            heads::compactions_to_fetch(names(), gc.as_ref()),
            |body, name| {
                let HeadName::Compaction { base_epoch, .. } = name else {
                    return None;
                };
                CompactionRecord::decode(body)
                    .ok()
                    .filter(|r| r.head_name(*base_epoch) == *name)
            },
        )
        .await?
        .into_iter()
        .collect();
        let view = View::compute(names(), gc.as_ref(), &compactions);

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
            let segment = match loaded.get(digest) {
                Some(s) => s.clone(),
                None => Arc::new(Segment {
                    digest: *digest,
                    meta: Meta::open(&fetch(&backend, &meta_key(digest)).await?)?,
                    tree: OnceCell::new(),
                }),
            };
            segments.push(segment);
        }
        let pack_indexes = previous
            .map(|p| p.pack_indexes.lock().unwrap().clone())
            .unwrap_or_default();
        Ok(Snapshot {
            backend,
            view,
            segments,
            pack_indexes: Mutex::new(pack_indexes),
        })
    }

    pub fn path_count(&self) -> usize {
        self.segments.iter().map(|s| s.meta.len()).sum()
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
        let tree = seg
            .tree
            .get_or_try_init(|| async {
                Ok::<_, Error>(Tree::open(
                    &fetch(&self.backend, &tree_key(&seg.digest)).await?,
                )?)
            })
            .await?;
        let node = tree.node(i)?;

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
                repacks_survived: 0,
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
        last_reachable: 0,
        last_pushed: 0,
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

/// `(pack, pack size, position in that pack's index)` of a chunk, `None` if unknown.
pub trait Locate: Fn(&ChunkHash) -> Option<(PackHash, u64, u16)> {}
impl<F: Fn(&ChunkHash) -> Option<(PackHash, u64, u16)>> Locate for F {}

fn from_file_tree(
    tree: &FileTree<ChunkList>,
    writer: &mut SegmentWriter,
    locate: &impl Locate,
) -> Option<Node> {
    Some(match &tree.0 {
        FileSystemObject::Regular(r) => {
            let mut chunks = Vec::with_capacity(r.contents.chunks.len());
            for h in &r.contents.chunks {
                let (pack, size, chunk) = locate(h)?;
                chunks.push(ChunkRef {
                    pack: writer.pack(pack, size),
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
                entries.insert(name.clone(), from_file_tree(child, writer, locate)?);
            }
            Node::Directory { entries }
        }
    })
}

/// Add a legacy-shaped entry to `writer`. `None` if a chunk cannot be located.
pub fn push_entry(
    writer: &mut SegmentWriter,
    entry: &PathEntry,
    locate: &impl Locate,
) -> Option<()> {
    let tree = from_file_tree(&entry.tree, writer, locate)?;
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

/// Per-pack indexes of a legacy manifest, chunks in offset order.
pub fn pack_indexes(manifest: &Manifest) -> BTreeMap<PackHash, PackIndex> {
    let mut out: BTreeMap<PackHash, PackIndex> = BTreeMap::new();
    for (hash, loc) in &manifest.chunks {
        out.entry(loc.pack)
            .or_default()
            .entries
            .push(PackIndexEntry {
                hash: *hash,
                offset: loc.offset,
                compressed_size: loc.compressed_size,
                uncompressed_size: loc.uncompressed_size,
            });
    }
    for index in out.values_mut() {
        index.entries.sort_by_key(|e| e.offset);
    }
    out
}

/// A legacy manifest as pack indexes plus one segment per root.
pub fn convert_manifest(
    manifest: &Manifest,
) -> (BTreeMap<PackHash, PackIndex>, Vec<(String, Sealed)>) {
    let indexes = pack_indexes(manifest);
    let mut position: HashMap<ChunkHash, (PackHash, u64, u16)> = HashMap::new();
    for (pack, index) in &indexes {
        let size = manifest.packs.get(pack).map_or(0, |p| p.size);
        for (i, e) in index.entries.iter().enumerate() {
            position.insert(e.hash, (*pack, size, i as u16));
        }
    }
    let locate = |h: &ChunkHash| position.get(h).copied();
    let mut segments = Vec::new();
    for (root, members) in &manifest.roots {
        let mut writer = SegmentWriter::default();
        for entry in members.paths.iter().filter_map(|h| manifest.paths.get(h)) {
            push_entry(&mut writer, entry, &locate);
        }
        if !writer.is_empty() {
            segments.push((
                root.clone(),
                writer.seal().expect("located chunks are in range"),
            ));
        }
    }
    (indexes, segments)
}
