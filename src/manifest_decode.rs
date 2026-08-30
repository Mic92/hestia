//! minicbor decoder for the legacy manifest as serde/ciborium wrote it:
//! structs are string-keyed maps, `FileTree` is internally tagged by
//! `"type"` with `Regular` contents flattened into the same map, hashes
//! are byte strings, store paths are base-name strings. Unknown keys are
//! skipped, missing `#[serde(default)]` fields default.

use std::collections::BTreeMap;

use minicbor::Decoder;
use minicbor::data::Type;
use minicbor::decode::Error;

use crate::manifest::{
    Blob, Directory, FileSystemObject, FileTree, Hash32, PackInfo, PathEntry, PathHash, Regular,
    Rewrite, Root, StorePath, Symlink, WireChunkList, WireManifest,
};

type R<T> = Result<T, Error>;

pub(crate) fn wire_manifest(bytes: &[u8]) -> R<WireManifest> {
    let mut d = Decoder::new(bytes);
    let mut m = WireManifest::default();
    each_key(&mut d, |d, k| {
        match k {
            "chunk_hashes" => m.chunk_hashes = Blob(bytes_owned(d)?),
            "pack_hashes" => m.pack_hashes = Blob(bytes_owned(d)?),
            "location_chunks" => m.location_chunks = array(d, |d| d.u64())?,
            "location_packs" => m.location_packs = array(d, |d| d.u64())?,
            "location_offsets" => m.location_offsets = array(d, |d| d.u64())?,
            "location_compressed_sizes" => m.location_compressed_sizes = array(d, |d| d.u32())?,
            "location_uncompressed_sizes" => m.location_uncompressed_sizes = array(d, |d| d.u32())?,
            "location_repacks_survived" => m.location_repacks_survived = array(d, |d| d.u32())?,
            "pack_infos" => m.pack_infos = map(d, |d| d.u64(), pack_info)?,
            "paths" => m.paths = map(d, path_hash, path_entry)?,
            "roots" => m.roots = map(d, |d| Ok(d.str()?.to_owned()), root)?,
            _ => d.skip()?,
        }
        Ok(())
    })?;
    Ok(m)
}

fn err(d: &Decoder<'_>, msg: impl Into<String>) -> Error {
    Error::message(msg.into()).at(d.position())
}

/// Run `item` for each element of a definite (`Some(n)`) or indefinite container.
fn repeat<'b>(
    d: &mut Decoder<'b>,
    len: Option<u64>,
    mut item: impl FnMut(&mut Decoder<'b>) -> R<()>,
) -> R<()> {
    match len {
        Some(n) => (0..n).try_for_each(|_| item(d)),
        None => {
            while d.datatype()? != Type::Break {
                item(d)?;
            }
            d.skip()
        }
    }
}

fn each_key<'b>(d: &mut Decoder<'b>, mut f: impl FnMut(&mut Decoder<'b>, &str) -> R<()>) -> R<()> {
    let len = d.map()?;
    repeat(d, len, |d| {
        let k = d.str()?;
        f(d, k)
    })
}

fn array<'b, T>(d: &mut Decoder<'b>, mut item: impl FnMut(&mut Decoder<'b>) -> R<T>) -> R<Vec<T>> {
    let len = d.array()?;
    let mut out = Vec::with_capacity(len.unwrap_or(0).min(1 << 20) as usize);
    repeat(d, len, |d| {
        out.push(item(d)?);
        Ok(())
    })?;
    Ok(out)
}

fn map<'b, K: Ord, V>(
    d: &mut Decoder<'b>,
    mut key: impl FnMut(&mut Decoder<'b>) -> R<K>,
    mut value: impl FnMut(&mut Decoder<'b>) -> R<V>,
) -> R<BTreeMap<K, V>> {
    let len = d.map()?;
    let mut out = BTreeMap::new();
    repeat(d, len, |d| {
        let k = key(d)?;
        out.insert(k, value(d)?);
        Ok(())
    })?;
    Ok(out)
}

/// ciborium never splits byte strings, but accept indefinite ones anyway.
fn bytes_owned(d: &mut Decoder<'_>) -> R<Vec<u8>> {
    let mut out = Vec::new();
    for part in d.bytes_iter()? {
        out.extend_from_slice(part?);
    }
    Ok(out)
}

fn optional<'b, T>(d: &mut Decoder<'b>, f: impl FnOnce(&mut Decoder<'b>) -> R<T>) -> R<Option<T>> {
    if d.datatype()? == Type::Null {
        d.skip()?;
        return Ok(None);
    }
    f(d).map(Some)
}

fn hash32(d: &mut Decoder<'_>) -> R<Hash32> {
    let p = d.position();
    bytes_owned(d)?
        .try_into()
        .map(Hash32)
        .map_err(|_| Error::message("expected 32 bytes").at(p))
}

fn store_path(d: &mut Decoder<'_>) -> R<StorePath> {
    let s = d.str()?;
    StorePath::from_base_path(s).map_err(|e| err(d, e.to_string()))
}

fn path_hash(d: &mut Decoder<'_>) -> R<PathHash> {
    let s = d.str()?;
    s.parse().map_err(|e| err(d, format!("{e}")))
}

fn pack_info(d: &mut Decoder<'_>) -> R<PackInfo> {
    let (mut size, mut created, mut tier) = (None, None, 0);
    each_key(d, |d, k| {
        match k {
            "size" => size = Some(d.u64()?),
            "created" => created = Some(d.u64()?),
            "tier" => tier = d.u8()?,
            _ => d.skip()?,
        }
        Ok(())
    })?;
    match (size, created) {
        (Some(size), Some(created)) => Ok(PackInfo {
            size,
            created,
            tier,
        }),
        _ => Err(err(d, "PackInfo missing size/created")),
    }
}

fn root(d: &mut Decoder<'_>) -> R<Root> {
    let mut v = Root::default();
    each_key(d, |d, k| {
        match k {
            "paths" => v.paths = array(d, path_hash)?.into_iter().collect(),
            "updated" => v.updated = d.u64()?,
            "run_id" => v.run_id = optional(d, |d| Ok(d.str()?.to_owned()))?,
            _ => d.skip()?,
        }
        Ok(())
    })?;
    Ok(v)
}

fn rewrite(d: &mut Decoder<'_>) -> R<Rewrite> {
    let (mut offset, mut ref_index) = (None, None);
    each_key(d, |d, k| {
        match k {
            "offset" => offset = Some(d.u64()?),
            "ref_index" => ref_index = Some(d.u32()?),
            _ => d.skip()?,
        }
        Ok(())
    })?;
    match (offset, ref_index) {
        (Some(offset), Some(ref_index)) => Ok(Rewrite { offset, ref_index }),
        _ => Err(err(d, "Rewrite missing offset/ref_index")),
    }
}

fn path_entry(d: &mut Decoder<'_>) -> R<PathEntry<WireChunkList>> {
    let mut store_path_ = None;
    let mut nar_hash = None;
    let mut nar_size = None;
    let mut tree = None;
    let mut references = Vec::new();
    let mut ca = None;
    let mut deriver = None;
    let mut last_reachable = 0;
    let mut last_pushed = 0;
    each_key(d, |d, k| {
        match k {
            "store_path" => store_path_ = Some(store_path(d)?),
            "nar_hash" => nar_hash = Some(hash32(d)?),
            "nar_size" => nar_size = Some(d.u64()?),
            "references" => references = array(d, store_path)?,
            "ca" => ca = optional(d, |d| Ok(d.str()?.to_owned()))?,
            "deriver" => deriver = optional(d, store_path)?,
            "tree" => tree = Some(file_tree(d)?),
            "last_reachable" => last_reachable = d.u64()?,
            "last_pushed" => last_pushed = d.u64()?,
            _ => d.skip()?,
        }
        Ok(())
    })?;
    let missing = |f| err(d, format!("PathEntry missing {f}"));
    Ok(PathEntry {
        store_path: store_path_.ok_or_else(|| missing("store_path"))?,
        nar_hash: nar_hash.ok_or_else(|| missing("nar_hash"))?,
        nar_size: nar_size.ok_or_else(|| missing("nar_size"))?,
        references,
        ca,
        deriver,
        tree: tree.ok_or_else(|| missing("tree"))?,
        last_reachable,
        last_pushed,
    })
}

const MAX_TREE_DEPTH: usize = 256;

fn file_tree(d: &mut Decoder<'_>) -> R<FileTree<WireChunkList>> {
    file_tree_at(d, 0)
}

fn file_tree_at(d: &mut Decoder<'_>, depth: usize) -> R<FileTree<WireChunkList>> {
    if depth > MAX_TREE_DEPTH {
        return Err(err(d, "file tree too deep"));
    }
    let mut kind: Option<String> = None;
    let mut executable = false;
    let mut contents = WireChunkList::default();
    let mut entries = None;
    let mut target = None;
    each_key(d, |d, k| {
        match k {
            "type" => kind = Some(d.str()?.to_owned()),
            "executable" => executable = d.bool()?,
            "chunks" => contents.chunks = array(d, |d| d.u64())?,
            "rewrites" => contents.rewrites = array(d, rewrite)?,
            "entries" => {
                entries = Some(map(
                    d,
                    |d| Ok(d.str()?.to_owned()),
                    |d| file_tree_at(d, depth + 1).map(Box::new),
                )?)
            }
            "target" => target = Some(d.str()?.to_owned()),
            _ => d.skip()?,
        }
        Ok(())
    })?;
    let obj = match kind.as_deref() {
        Some("regular") => FileSystemObject::Regular(Regular {
            executable,
            contents,
        }),
        Some("directory") => FileSystemObject::Directory(Directory {
            entries: entries.unwrap_or_default(),
        }),
        Some("symlink") => FileSystemObject::Symlink(Symlink {
            target: target.ok_or_else(|| err(d, "symlink missing target"))?,
        }),
        Some(other) => return Err(err(d, format!("unknown file type {other:?}"))),
        None => return Err(err(d, "file tree missing type")),
    };
    Ok(FileTree(obj))
}
