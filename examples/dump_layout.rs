//! Throwaway helper for planning fetch heuristics: dump a manifest's
//! per-path chunk layout as JSON lines.
//!
//! Usage: dump_layout <blob-dir-or-manifest-file>
//! Scans the given directory (e.g. mock-cache --data-dir) for a decodable
//! manifest and prints one JSON object per store path:
//!   {"hash": "...", "nar_size": N, "references": [...],
//!    "chunks": [{"pack": "...", "offset": N, "size": N}, ...]}

use std::collections::BTreeSet;

use hestia::chunker::flatten_tree;
use hestia::manifest::{FileSystemObject, Manifest, PathHash};

fn main() {
    let arg = std::env::args()
        .nth(1)
        .expect("usage: dump_layout <dir|file>");
    let meta = std::fs::metadata(&arg).expect("stat argument");
    let candidates: Vec<std::path::PathBuf> = if meta.is_dir() {
        std::fs::read_dir(&arg)
            .expect("read dir")
            .map(|entry| entry.expect("dir entry").path())
            .collect()
    } else {
        vec![arg.into()]
    };
    let manifest = candidates
        .iter()
        .filter_map(|path| Manifest::decode(&std::fs::read(path).ok()?).ok())
        .max_by_key(|manifest| manifest.paths.len())
        .expect("no decodable manifest found");

    for (hash, entry) in &manifest.paths {
        let chunks: Vec<String> = flatten_tree(&entry.tree)
            .into_iter()
            .filter_map(|(_, node)| match node {
                FileSystemObject::Regular(regular) => Some(&regular.contents.chunks),
                _ => None,
            })
            .flatten()
            .filter_map(|chunk| {
                let location = manifest.chunks.get(chunk)?;
                Some(format!(
                    r#"{{"pack":"{}","offset":{},"size":{}}}"#,
                    location.pack, location.offset, location.compressed_size
                ))
            })
            .collect();
        let references: BTreeSet<String> = entry
            .references
            .iter()
            .map(PathHash::from_store_path)
            .filter(|reference| *reference != *hash && manifest.paths.contains_key(reference))
            .map(|reference| format!(r#""{reference}""#))
            .collect();
        println!(
            r#"{{"hash":"{hash}","nar_size":{},"references":[{}],"chunks":[{}]}}"#,
            entry.nar_size,
            references.into_iter().collect::<Vec<_>>().join(","),
            chunks.join(",")
        );
    }
}
