//! nix's `<hash>.ls`: the file tree with sizes and NAR offsets, computed
//! from the stored tree and chunk sizes without touching pack data.

use serde_json::{Map, Value, json};

use crate::chunker::{Error, TreeNode};
use crate::manifest::{ChunkHash, ChunkList, FileSystemObject, FileTree};

struct Walker<F> {
    size_of: F,
    pos: u64,
}

fn padded(len: u64) -> u64 {
    len + (8 - len % 8) % 8
}

impl<F: Fn(&ChunkHash) -> Option<u64>> Walker<F> {
    /// Advance past NAR string tokens.
    fn skip(&mut self, tokens: &[&str]) {
        for t in tokens {
            self.pos += 8 + padded(t.len() as u64);
        }
    }

    fn node(&mut self, node: &TreeNode) -> Result<Value, Error> {
        self.skip(&["(", "type"]);
        let v = match node {
            FileSystemObject::Regular(f) => {
                self.skip(&["regular", "contents"]);
                if f.executable {
                    self.skip(&["executable", ""]);
                }
                let mut size = 0u64;
                for h in &f.contents.chunks {
                    size += (self.size_of)(h).ok_or(Error::MissingChunk(*h))?;
                }
                let offset = self.pos + 8;
                self.pos += 8 + padded(size);
                json!({"type": "regular", "size": size, "executable": f.executable, "narOffset": offset})
            }
            FileSystemObject::Symlink(l) => {
                self.skip(&["symlink", "target", &l.target]);
                json!({"type": "symlink", "target": l.target})
            }
            FileSystemObject::Directory(d) => {
                self.skip(&["directory"]);
                let mut entries = Map::new();
                for (name, child) in &d.entries {
                    self.skip(&["entry", "(", "name", name, "node"]);
                    entries.insert(name.clone(), self.node(&child.0)?);
                    self.skip(&[")"]);
                }
                json!({"type": "directory", "entries": entries})
            }
        };
        self.skip(&[")"]);
        Ok(v)
    }
}

pub fn listing(
    tree: &FileTree<ChunkList>,
    size_of: impl Fn(&ChunkHash) -> Option<u64>,
) -> Result<Value, Error> {
    let mut w = Walker { size_of, pos: 0 };
    w.skip(&["nix-archive-1"]);
    let root = w.node(&tree.0)?;
    Ok(json!({"version": 1, "root": root}))
}
