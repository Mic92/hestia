//! End-to-end tests for the chunker against real filesystem trees and real
//! store paths (Phase 2 milestone):
//!
//! 1. Walk a path with harmonia's NAR dumper, chunk all files, pack them,
//!    and rebuild every file byte-identical from the pack buffer.
//! 2. The NAR hash computed by replaying through harmonia's NarByteStream
//!    matches what Nix itself reports (`nix-store --dump` / `nix path-info`).
//!
//! Tests that need Nix tooling or /nix/store skip themselves (with a notice)
//! when running where those are unavailable (e.g. the Nix build sandbox).

mod support;

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use bytes::Bytes;

use hestia::chunker::{self, PackBuilder, chunk_path, extract_chunk, flatten_tree};
use hestia::manifest::{ChunkHash, ChunkLocation, FileSystemObject};
use support::store::{find_real_store_path, nix_path_info_hash, nix_store_dump_hash};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a realistic store-path-like tree: nested dirs, executables,
/// symlinks, empty files, small and large (multi-chunk) files.
fn create_fixture(root: &Path) {
    std::fs::create_dir_all(root.join("bin")).unwrap();
    std::fs::create_dir_all(root.join("lib")).unwrap();
    std::fs::create_dir_all(root.join("share/doc")).unwrap();
    std::fs::create_dir_all(root.join("empty-dir")).unwrap();

    // Executable with shebang.
    let exe = root.join("bin/hello");
    std::fs::write(&exe, b"#!/bin/sh\necho hello world\n").unwrap();
    std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();

    // Large pseudo-random file: spans many chunks.
    let mut large = Vec::with_capacity(3 * 1024 * 1024);
    let mut state: u64 = 0x123456789;
    while large.len() < 3 * 1024 * 1024 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        large.extend_from_slice(&state.to_le_bytes());
    }
    std::fs::write(root.join("lib/libbig.so"), &large).unwrap();

    // A copy of the same data under another name: must share all chunks.
    std::fs::write(root.join("lib/libbig-copy.so"), &large).unwrap();

    // Small text files.
    std::fs::write(root.join("share/doc/README"), b"docs go here\n").unwrap();
    std::fs::write(root.join("share/doc/empty"), b"").unwrap();

    // Symlinks: relative and dangling.
    std::os::unix::fs::symlink("../bin/hello", root.join("lib/hello-link")).unwrap();
    std::os::unix::fs::symlink("/nix/store/nonexistent", root.join("dangling")).unwrap();
}

/// Chunk a path, pack all chunks, then rebuild every regular file purely
/// from the pack buffer + manifest-style locations and compare against the
/// filesystem.
async fn assert_reconstruction_from_pack(path: &Path) {
    let chunked = chunk_path(path).await.unwrap();

    // Build the pack the way the write pipeline will.
    let mut builder = PackBuilder::new();
    for chunk in &chunked.chunks {
        builder.add(chunk).unwrap();
    }
    let pack = builder.finish();
    let locations: BTreeMap<ChunkHash, ChunkLocation> = pack.locations().collect();

    // Rebuild every file from the pack buffer alone.
    let mut files_checked = 0;
    let mut symlinks_checked = 0;
    for (relative, node) in flatten_tree(&chunked.tree) {
        let fs_path = if relative.is_empty() {
            path.to_path_buf()
        } else {
            path.join(&relative)
        };
        match node {
            FileSystemObject::Regular(regular) => {
                let mut rebuilt = Vec::new();
                for chunk_hash in &regular.contents.chunks {
                    let location = &locations[chunk_hash];
                    let start = location.offset as usize;
                    let end = start + location.compressed_size as usize;
                    let data = extract_chunk(&pack.data[start..end], chunk_hash).unwrap();
                    rebuilt.extend_from_slice(&data);
                }
                let original = std::fs::read(&fs_path).unwrap();
                assert_eq!(
                    rebuilt, original,
                    "file {relative:?} not byte-identical after reconstruction"
                );

                let executable =
                    std::fs::metadata(&fs_path).unwrap().permissions().mode() & 0o100 != 0;
                assert_eq!(
                    regular.executable, executable,
                    "{relative:?} executable bit"
                );
                files_checked += 1;
            }
            FileSystemObject::Symlink(symlink) => {
                let target = std::fs::read_link(&fs_path).unwrap();
                assert_eq!(
                    symlink.target,
                    target.to_string_lossy(),
                    "{relative:?} symlink target"
                );
                symlinks_checked += 1;
            }
            FileSystemObject::Directory(_) => {
                assert!(fs_path.is_dir(), "{relative:?} should be a directory");
            }
        }
    }
    assert!(files_checked > 0, "fixture must contain regular files");
    eprintln!(
        "reconstructed {files_checked} files and verified {symlinks_checked} symlinks from a \
         {} byte pack ({} chunks) for {}",
        pack.data.len(),
        pack.chunks.len(),
        path.display()
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fixture_tree_reconstructs_byte_identical_from_pack() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("fixture-0.1.0");
    create_fixture(&root);
    assert_reconstruction_from_pack(&root).await;

    // Dedup check: the two identical large files must share every chunk, so
    // unique chunks must be far fewer than total chunk references.
    let chunked = chunk_path(&root).await.unwrap();
    let total_refs: usize = flatten_tree(&chunked.tree)
        .iter()
        .filter_map(|(_, node)| match node {
            FileSystemObject::Regular(regular) => Some(regular.contents.chunks.len()),
            _ => None,
        })
        .sum();
    assert!(
        chunked.chunks.len() < total_refs,
        "identical files must dedup: {} unique chunks vs {} references",
        chunked.chunks.len(),
        total_refs
    );
}

#[tokio::test]
async fn real_store_path_reconstructs_byte_identical_from_pack() {
    let Some(store_path) = find_real_store_path() else {
        eprintln!("skipping: no real /nix/store path available");
        return;
    };
    assert_reconstruction_from_pack(&store_path).await;
}

#[tokio::test]
async fn single_file_path_chunks_and_reconstructs() {
    // NARs of bare files (not directories) are a special case: the root node
    // is the file itself.
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("single");
    std::fs::write(&file, vec![7u8; 100_000]).unwrap();

    let chunked = chunk_path(&file).await.unwrap();
    let FileSystemObject::Regular(regular) = &chunked.tree.0 else {
        panic!("root node must be a regular file");
    };
    let rebuilt: Vec<u8> = {
        let by_hash: BTreeMap<ChunkHash, Bytes> = chunked
            .chunks
            .iter()
            .map(|c| (c.hash, c.data.clone()))
            .collect();
        regular
            .contents
            .chunks
            .iter()
            .flat_map(|hash| by_hash[hash].to_vec())
            .collect()
    };
    assert_eq!(rebuilt, std::fs::read(&file).unwrap());
}

#[tokio::test]
async fn nar_hash_matches_nix_store_dump_for_fixture() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("fixture-0.1.0");
    create_fixture(&root);

    let Some((expected_hash, expected_size)) = nix_store_dump_hash(&root) else {
        eprintln!("skipping: nix-store not available");
        return;
    };

    let (hash, size) = chunker::nar_hash_and_size(&root).await.unwrap();
    assert_eq!(size, expected_size, "NAR size mismatch");
    assert_eq!(hash, expected_hash, "NAR hash mismatch");
}

#[tokio::test]
async fn nar_hash_matches_nix_path_info_for_real_store_path() {
    // The Phase 2 milestone check: our NAR serialization must agree with
    // Nix's own database record for a real store path.
    let Some(store_path) = find_real_store_path() else {
        eprintln!("skipping: no real /nix/store path available");
        return;
    };
    let Some((expected_hash, expected_size)) = nix_path_info_hash(&store_path) else {
        eprintln!("skipping: nix path-info not available or path not in Nix database");
        return;
    };

    let (hash, size) = chunker::nar_hash_and_size(&store_path).await.unwrap();
    assert_eq!(
        size,
        expected_size,
        "NAR size mismatch for {}",
        store_path.display()
    );
    assert_eq!(
        hash,
        expected_hash,
        "NAR hash mismatch for {}",
        store_path.display()
    );
}

#[tokio::test]
async fn nar_hash_from_chunks_matches_nix_for_real_store_path() {
    // The write-pipeline integrity check: the NAR hash computed from the
    // chunked representation (tree + chunk data, no second disk walk) must
    // equal what Nix records for the path. This is what proves a path's
    // stored form is correct before it gets uploaded.
    let Some(store_path) = find_real_store_path() else {
        eprintln!("skipping: no real /nix/store path available");
        return;
    };
    let Some((expected_hash, expected_size)) =
        nix_path_info_hash(&store_path).or_else(|| nix_store_dump_hash(&store_path))
    else {
        eprintln!("skipping: no nix oracle available");
        return;
    };

    let chunked = chunk_path(&store_path).await.unwrap();
    let (hash, size) = chunker::nar_hash_from_chunks(&chunked.tree, &chunked.chunk_map())
        .await
        .unwrap();
    assert_eq!(size, expected_size, "NAR size mismatch");
    assert_eq!(hash, expected_hash, "NAR hash mismatch");
}
