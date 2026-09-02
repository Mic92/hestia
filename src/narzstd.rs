//! A path's `.nar.zst` without recompressing: stored chunk frames are
//! spliced verbatim between raw (stored-block) zstd frames that carry
//! the NAR framing. Decoders accept concatenated frames, so the stream
//! decompresses to the canonical NAR and nix's `NarHash` check holds.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::chunker::{Error, TreeNode, compress_chunk_fast, extract_chunk};
use crate::manifest::{ChunkHash, ChunkList, FileSystemObject, FileTree};
use crate::refnorm::{HASH_LEN, RefTable};

/// One stored chunk: its zstd frame and decompressed length.
#[derive(Debug, Clone)]
pub struct Frame {
    pub zstd: Bytes,
    pub size: u32,
}

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const RAW_BLOCK_MAX: usize = 128 << 10;

/// `data` as one zstd frame of raw blocks (no entropy coding).
fn raw_frame(out: &mut Vec<u8>, data: &[u8]) {
    out.extend(ZSTD_MAGIC);
    // Single segment, 8-byte frame content size.
    out.push(0xE0);
    out.extend((data.len() as u64).to_le_bytes());
    let mut rest = data;
    loop {
        let n = rest.len().min(RAW_BLOCK_MAX);
        let last = n == rest.len();
        let header = (n as u32) << 3 | u32::from(last);
        out.extend(&header.to_le_bytes()[..3]);
        out.extend(&rest[..n]);
        rest = &rest[n..];
        if last {
            break;
        }
    }
}

struct Stitcher<'a> {
    frames: &'a BTreeMap<ChunkHash, Frame>,
    /// Re-encoded frames of reference-touched chunks, in NAR order.
    reencoded: std::vec::IntoIter<Vec<u8>>,
    out: Vec<u8>,
    /// NAR framing not yet wrapped into a raw frame.
    pending: Vec<u8>,
}

impl Stitcher<'_> {
    fn str(&mut self, s: &[u8]) {
        self.pending.extend((s.len() as u64).to_le_bytes());
        self.pending.extend(s);
        self.pad(s.len() as u64);
    }

    fn pad(&mut self, len: u64) {
        let n = (8 - len % 8) % 8;
        self.pending.extend(&[0u8; 7][..n as usize]);
    }

    fn flush(&mut self) {
        if !self.pending.is_empty() {
            raw_frame(&mut self.out, &self.pending);
            self.pending.clear();
        }
    }

    fn node(&mut self, node: &TreeNode) -> Result<(), Error> {
        self.str(b"(");
        self.str(b"type");
        match node {
            FileSystemObject::Regular(f) => {
                self.str(b"regular");
                if f.executable {
                    self.str(b"executable");
                    self.str(b"");
                }
                self.str(b"contents");
                let size = self.contents(&f.contents)?;
                self.pad(size);
            }
            FileSystemObject::Symlink(l) => {
                self.str(b"symlink");
                self.str(b"target");
                self.str(l.target.as_bytes());
            }
            FileSystemObject::Directory(d) => {
                self.str(b"directory");
                for (name, child) in &d.entries {
                    self.str(b"entry");
                    self.str(b"(");
                    self.str(b"name");
                    self.str(name.as_bytes());
                    self.str(b"node");
                    self.node(&child.0)?;
                    self.str(b")");
                }
            }
        }
        self.str(b")");
        Ok(())
    }

    /// Emits the length word and the file bytes, returns the length.
    fn contents(&mut self, c: &ChunkList) -> Result<u64, Error> {
        let frames = self.frames;
        let frame = |h: &ChunkHash| frames.get(h).ok_or(Error::MissingChunk(*h));
        let mut size = 0u64;
        for h in &c.chunks {
            size += u64::from(frame(h)?.size);
        }
        self.pending.extend(size.to_le_bytes());
        self.flush();
        let mut start = 0u64;
        for h in &c.chunks {
            let f = frame(h)?;
            let end = start + u64::from(f.size);
            if touched(c, start, end) {
                let z = self.reencoded.next().expect("one per touched chunk");
                self.out.extend(z);
            } else {
                self.out.extend_from_slice(&f.zstd);
            }
            start = end;
        }
        Ok(size)
    }
}

fn touched(c: &ChunkList, start: u64, end: u64) -> bool {
    c.rewrites
        .iter()
        .any(|r| r.offset < end && r.offset + HASH_LEN as u64 > start)
}

/// Chunks a restored reference touches, re-encoded with the references
/// put back, in NAR order. This is the only CPU-heavy part, so it runs
/// across cores.
fn reencode(
    tree: &FileTree<ChunkList>,
    frames: &BTreeMap<ChunkHash, Frame>,
    refs: &RefTable,
) -> Result<Vec<Vec<u8>>, Error> {
    let mut jobs: Vec<(&ChunkList, &ChunkHash, &Frame, u64)> = Vec::new();
    for (_, node) in crate::chunker::flatten_tree(tree) {
        let FileSystemObject::Regular(f) = node else {
            continue;
        };
        let c = &f.contents;
        if c.rewrites.is_empty() {
            continue;
        }
        let mut start = 0u64;
        for h in &c.chunks {
            let frame = frames.get(h).ok_or(Error::MissingChunk(*h))?;
            let end = start + u64::from(frame.size);
            if touched(c, start, end) {
                jobs.push((c, h, frame, start));
            }
            start = end;
        }
    }
    let one = |&(c, h, f, start): &(&ChunkList, &ChunkHash, &Frame, u64)| {
        let mut data = extract_chunk(&f.zstd, h)?;
        refs.restore_window(&mut data, start, &c.rewrites)?;
        compress_chunk_fast(&data)
    };
    let workers = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(jobs.len() / 8)
        .max(1);
    if workers == 1 {
        return jobs.iter().map(one).collect();
    }
    std::thread::scope(|scope| {
        let handles: Vec<_> = jobs
            .chunks(jobs.len().div_ceil(workers))
            .map(|slice| scope.spawn(move || slice.iter().map(one).collect::<Result<Vec<_>, _>>()))
            .collect();
        let mut out = Vec::with_capacity(jobs.len());
        for h in handles {
            out.extend(h.join().expect("re-encode worker panicked")?);
        }
        Ok(out)
    })
}

/// The `.nar.zst` body for `tree`.
pub fn stitch(
    tree: &FileTree<ChunkList>,
    frames: &BTreeMap<ChunkHash, Frame>,
    refs: &RefTable,
) -> Result<Vec<u8>, Error> {
    let zsize: usize = frames.values().map(|f| f.zstd.len()).sum();
    let mut s = Stitcher {
        frames,
        reencoded: reencode(tree, frames, refs)?.into_iter(),
        out: Vec::with_capacity(zsize + 4096),
        pending: Vec::new(),
    };
    s.str(b"nix-archive-1");
    s.node(&tree.0)?;
    s.flush();
    Ok(s.out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunker::{chunk_path, nar_from_chunks};

    fn frames_of(chunked: &crate::chunker::ChunkedPath) -> BTreeMap<ChunkHash, Frame> {
        chunked
            .chunks
            .iter()
            .map(|c| {
                let frame = Frame {
                    zstd: Bytes::from(crate::chunker::compress_chunk(&c.data).unwrap()),
                    size: c.data.len() as u32,
                };
                (c.hash, frame)
            })
            .collect()
    }

    #[test]
    fn raw_frame_round_trips_past_one_block() {
        for len in [0, 1, RAW_BLOCK_MAX, RAW_BLOCK_MAX + 1, 3 * RAW_BLOCK_MAX] {
            let data: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let mut out = Vec::new();
            raw_frame(&mut out, &data);
            assert_eq!(zstd::decode_all(&out[..]).unwrap(), data);
        }
    }

    /// The stitched stream decodes to what the NAR writer produces, for a
    /// tree with every node kind, an empty file, and normalized references.
    #[tokio::test]
    async fn stitched_stream_decodes_to_the_nar() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("p");
        std::fs::create_dir_all(root.join("sub")).unwrap();
        let dep = "0d71ygfwbmy1xjlbj1v027dfmy9cjm9c";
        // References all over a multi-chunk file so some straddle chunk borders.
        let mut big = Vec::new();
        for i in 0..400_000u32 {
            big.extend(i.to_le_bytes());
            if i % 10 == 0 {
                big.extend(format!("/nix/store/{dep}-sh").bytes());
            }
        }
        std::fs::write(root.join("big"), &big).unwrap();
        std::fs::write(
            root.join("sub/ref"),
            format!("#!/nix/store/{dep}-sh/bin/sh\n"),
        )
        .unwrap();
        std::fs::write(root.join("empty"), b"").unwrap();
        std::os::unix::fs::symlink("big", root.join("link")).unwrap();
        let mut perms = std::fs::metadata(root.join("sub/ref"))
            .unwrap()
            .permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut perms, 0o755);
        std::fs::set_permissions(root.join("sub/ref"), perms).unwrap();

        let refs = RefTable::new(&[format!("{dep}-sh").parse().unwrap()]);
        let chunked = chunk_path(&root, &refs).await.unwrap();
        let frames = frames_of(&chunked);
        let straddles = crate::chunker::flatten_tree(&chunked.tree)
            .iter()
            .any(|(_, n)| match n {
                FileSystemObject::Regular(r) => {
                    let mut borders = Vec::new();
                    let mut at = 0u64;
                    for h in &r.contents.chunks {
                        at += u64::from(frames[h].size);
                        borders.push(at);
                    }
                    borders.pop();
                    r.contents
                        .rewrites
                        .iter()
                        .any(|w| borders.iter().any(|b| w.offset < *b && w.offset + 32 > *b))
                }
                _ => false,
            });
        assert!(straddles, "fixture has a reference across a chunk border");
        let want = nar_from_chunks(&chunked.tree, &chunked.chunk_map(), &refs)
            .await
            .unwrap();
        let got = zstd::decode_all(&stitch(&chunked.tree, &frames, &refs).unwrap()[..]).unwrap();
        assert_eq!(got, want);
    }
}
