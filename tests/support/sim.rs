//! Synthetic push driver for GC tests.
//!
//! GC logic does not depend on a Nix store: it operates on manifests and
//! pack blobs. This module fabricates store paths with deterministic
//! contents and pushes them with the real chunker, pack builder, pack
//! upload, and SaveMutable manifest commit against the fake GHA backend.
//! (Pack assembly is simplified versus the drain pipeline: one pack per
//! push via PackBuilder::add, no target-size splitting.) No nix tooling
//! required, so these tests run everywhere
//! (including the Nix build sandbox) and a 30-day history simulates in
//! seconds.
//!
//! Readability checks also use the real substituter: a path counts as
//! "fully readable" only if its narinfo is served and its NAR downloads
//! and hashes correctly.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use hestia::chunker::{Chunk, PackBuilder, chunk_data, nar_hash_from_chunks};
use hestia::gc::{GcContext, GcPolicy};
use hestia::gha::Error as GhaError;
use hestia::gha::rest::RestClient;
use hestia::gha::savemutable::SaveMutable;
use hestia::gha::twirp::TwirpClient;
use hestia::manifest::{
    ChunkHash, ChunkList, Directory, FileSystemObject, FileTree, Hash32, Manifest, PackInfo,
    PathEntry, PathHash, Regular, Root, StorePath, StorePathHash,
};
use hestia::pathinfo::StoreDir;
use hestia::pipeline::{AccessLog, MANIFEST_PREFIX, upload_pack};
use hestia::substituter::{ManifestStore, Substituter};

use super::fake_gha::FakeGha;

/// Deterministic pseudo-random data (xorshift).
pub fn test_data(len: usize, seed: u64) -> Bytes {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    Bytes::from(out)
}

// ---------------------------------------------------------------------------
// SimPath: a fabricated store path
// ---------------------------------------------------------------------------

/// A fabricated store path. Its hash is derived from its name, its contents
/// from the seed: the same (name, seed, size) is always the same path with
/// the same chunks.
#[derive(Debug, Clone)]
pub struct SimPath {
    pub name: String,
    pub files: Vec<(String, Bytes)>,
}

impl SimPath {
    /// One path with a single pseudo-random blob.
    pub fn new(name: &str, seed: u64, size: usize) -> Self {
        Self {
            name: name.to_string(),
            files: vec![("blob".to_string(), test_data(size, seed))],
        }
    }

    pub fn path_hash(&self) -> PathHash {
        let digest = Hash32::digest(self.name.as_bytes());
        let bytes: [u8; 20] = digest.0[..20].try_into().expect("20 bytes");
        PathHash(StorePathHash::new(bytes))
    }

    pub fn store_path(&self) -> StorePath {
        format!("{}-{}", self.path_hash(), self.name)
            .parse()
            .expect("sim path names are valid store path names")
    }

    /// File tree + chunks of this path.
    pub fn chunked(&self) -> (FileTree<ChunkList>, Vec<Chunk>) {
        let mut entries: BTreeMap<String, Box<FileTree<ChunkList>>> = BTreeMap::new();
        let mut chunks: Vec<Chunk> = Vec::new();
        let mut seen: BTreeSet<ChunkHash> = BTreeSet::new();
        for (file_name, data) in &self.files {
            let file_chunks = chunk_data(data);
            entries.insert(
                file_name.clone(),
                Box::new(FileTree(FileSystemObject::Regular(Regular {
                    executable: false,
                    contents: ChunkList {
                        chunks: file_chunks.iter().map(|chunk| chunk.hash).collect(),
                    },
                }))),
            );
            for chunk in file_chunks {
                if seen.insert(chunk.hash) {
                    chunks.push(chunk);
                }
            }
        }
        (
            FileTree(FileSystemObject::Directory(Directory { entries })),
            chunks,
        )
    }
}

// ---------------------------------------------------------------------------
// SimCache: pushes + GC against a fake GHA backend
// ---------------------------------------------------------------------------

/// Drives pushes (and builds GC contexts) against one fake GHA backend.
pub struct SimCache {
    pub http: reqwest::Client,
    pub twirp: TwirpClient,
    pub rest: RestClient,
}

impl SimCache {
    pub fn new(fake: &FakeGha, http: &reqwest::Client) -> Self {
        Self {
            http: http.clone(),
            twirp: fake.twirp(http),
            rest: fake.rest(http),
        }
    }

    fn save_mutable(&self) -> SaveMutable<'_> {
        SaveMutable::new(&self.twirp, &self.http, MANIFEST_PREFIX)
    }

    /// A GC context sharing this cache's clients.
    pub fn gc(&self, policy: GcPolicy) -> GcContext {
        GcContext {
            twirp: self.twirp.clone(),
            rest: self.rest.clone(),
            http: self.http.clone(),
            manifest_prefix: MANIFEST_PREFIX.to_string(),
            policy,
        }
    }

    /// The currently committed manifest (empty if none).
    pub async fn manifest(&self) -> Manifest {
        match self.save_mutable().load().await.expect("manifest load") {
            Some(entry) => Manifest::decode(&entry.data).expect("manifest decode"),
            None => Manifest::new(),
        }
    }

    /// Simulate one CI run's drain:
    ///
    /// * `pushed`: paths the run built (uploaded if new, push clock bumped
    ///   if already stored) — mirrors the hook → pipeline flow;
    /// * `closure`: paths the run needed; the root for `root_key` is
    ///   replaced with exactly this set — mirrors root = pushed ∪ accessed.
    pub async fn push(&self, root_key: &str, pushed: &[SimPath], closure: &[SimPath], now: u64) {
        let current = self.manifest().await;
        let mut delta = Manifest::new();
        let mut builder = PackBuilder::new();
        let mut batch_chunks: BTreeSet<ChunkHash> = BTreeSet::new();

        for path in pushed {
            let hash = path.path_hash();
            if let Some(existing) = current.paths.get(&hash) {
                // Dedup-skip: bump the push clock (PushTTL liveness).
                let mut entry = existing.clone();
                entry.last_pushed = now;
                delta.paths.insert(hash, entry);
                continue;
            }

            let (tree, chunks) = path.chunked();
            let chunk_map: BTreeMap<ChunkHash, Bytes> = chunks
                .iter()
                .map(|chunk| (chunk.hash, chunk.data.clone()))
                .collect();
            let (nar_hash, nar_size) = nar_hash_from_chunks(&tree, &chunk_map)
                .await
                .expect("nar hash from chunks");

            for chunk in &chunks {
                if !current.chunks.contains_key(&chunk.hash) && batch_chunks.insert(chunk.hash) {
                    builder.add(chunk).expect("pack add");
                }
            }

            delta.paths.insert(
                hash,
                PathEntry {
                    store_path: path.store_path(),
                    nar_hash,
                    nar_size,
                    references: vec![],
                    ca: None,
                    deriver: None,
                    tree,
                    last_reachable: 0,
                    last_pushed: now,
                },
            );
        }

        if !builder.is_empty() {
            let pack = builder.finish();
            upload_pack(&self.twirp, &self.http, &pack)
                .await
                .expect("pack upload");
            for (chunk, location) in pack.locations() {
                delta.chunks.insert(chunk, location);
            }
            delta.packs.insert(
                pack.hash,
                PackInfo {
                    size: pack.data.len() as u64,
                    created: now,
                    tier: 0,
                },
            );
        }

        delta.roots.insert(
            root_key.to_string(),
            Root {
                paths: closure.iter().map(SimPath::path_hash).collect(),
                updated: now,
                run_id: None,
            },
        );

        self.save_mutable()
            .save(|existing| {
                let base = match existing {
                    Some(entry) => Manifest::decode(&entry.data)
                        .map_err(|err| GhaError::InvalidResponse(err.to_string()))?,
                    None => Manifest::new(),
                };
                // Mirror production's drain commit (pipeline.rs): fold the
                // drain-start snapshot in as well, because the dedup
                // decisions above were based on it and `existing` can be a
                // stale read. This merge is also how a drain racing GC can
                // resurrect GC-dropped paths, so the GC suite's dangling-
                // reference checks depend on the sim performing it too.
                base.merge(current.clone())
                    .merge(delta.clone())
                    .encode()
                    .map_err(|err| GhaError::InvalidResponse(err.to_string()))
            })
            .await
            .expect("manifest commit");
    }

    /// Commit a drain whose manifest snapshot was taken earlier (passed in
    /// instead of loaded): mirrors a long drain racing GC, whose
    /// merge-commit resurrects everything GC dropped since the snapshot.
    pub async fn commit_stale_drain(
        &self,
        snapshot: &Manifest,
        root_key: &str,
        closure: &[SimPath],
        now: u64,
    ) {
        let mut delta = Manifest::new();
        delta.roots.insert(
            root_key.to_string(),
            Root {
                paths: closure.iter().map(SimPath::path_hash).collect(),
                updated: now,
                run_id: None,
            },
        );
        let snapshot = snapshot.clone();
        self.save_mutable()
            .save(move |existing| {
                let base = match existing {
                    Some(entry) => Manifest::decode(&entry.data)
                        .map_err(|err| GhaError::InvalidResponse(err.to_string()))?,
                    None => Manifest::new(),
                };
                base.merge(snapshot.clone())
                    .merge(delta.clone())
                    .encode()
                    .map_err(|err| GhaError::InvalidResponse(err.to_string()))
            })
            .await
            .expect("stale drain commit");
    }

    /// Upload a pack blob without ever referencing it from a manifest
    /// (simulates a push that crashed before its manifest commit).
    pub async fn upload_orphan_pack(&self, seed: u64) -> String {
        let chunks = chunk_data(&test_data(50_000, seed));
        let mut builder = PackBuilder::new();
        for chunk in &chunks {
            builder.add(chunk).expect("pack add");
        }
        let pack = builder.finish();
        upload_pack(&self.twirp, &self.http, &pack)
            .await
            .expect("orphan pack upload");
        pack.cache_key()
    }

    /// Total bytes of `pack-*` entries currently stored in the (fake) cache.
    pub async fn stored_pack_bytes(&self) -> u64 {
        self.rest
            .list_caches("pack-")
            .await
            .expect("pack listing")
            .iter()
            .map(|entry| entry.size_in_bytes)
            .sum()
    }

    /// Cache keys of all stored `pack-*` entries.
    pub async fn stored_pack_keys(&self) -> BTreeSet<String> {
        self.rest
            .list_caches("pack-")
            .await
            .expect("pack listing")
            .iter()
            .map(|entry| entry.key.clone())
            .collect()
    }

    /// Compressed bytes of all chunks the manifest references (the live-set
    /// size GC storage should converge towards).
    pub async fn live_chunk_bytes(&self) -> u64 {
        self.manifest()
            .await
            .chunks
            .values()
            .map(|location| u64::from(location.compressed_size))
            .sum()
    }

    /// Assert that every given path is fully readable through the
    /// substituter (narinfo served, NAR downloads, hash matches).
    pub async fn assert_readable(&self, paths: &[&SimPath]) {
        let manifest = self.manifest().await;
        let substituter = self.start_substituter(manifest.clone()).await;

        for path in paths {
            let hash = path.path_hash();
            let entry = manifest
                .paths
                .get(&hash)
                .unwrap_or_else(|| panic!("path {} must be in the manifest", path.name));

            // narinfo
            let response = self
                .http
                .get(format!("{}/{hash}.narinfo", substituter.base_url))
                .send()
                .await
                .expect("narinfo request");
            assert_eq!(response.status(), 200, "narinfo for {}", path.name);
            let narinfo = response.text().await.expect("narinfo body");

            // The NAR URL comes from the narinfo (exactly what Nix would do).
            let nar_url = narinfo
                .lines()
                .find_map(|line| line.strip_prefix("URL: "))
                .expect("narinfo has a URL line");
            let response = self
                .http
                .get(format!("{}/{nar_url}", substituter.base_url))
                .send()
                .await
                .expect("nar request");
            assert_eq!(response.status(), 200, "NAR for {}", path.name);
            let nar = response.bytes().await.expect("nar body");

            assert_eq!(
                nar.len() as u64,
                entry.nar_size,
                "NAR size of {}",
                path.name
            );
            assert_eq!(
                Hash32::digest(&nar),
                entry.nar_hash,
                "NAR hash of {}",
                path.name
            );
        }
    }

    /// Assert that the given paths are NOT served (dropped/healed paths must
    /// 404 so Nix rebuilds them instead of receiving partial data).
    pub async fn assert_unavailable(&self, paths: &[&SimPath]) {
        let manifest = self.manifest().await;
        let substituter = self.start_substituter(manifest).await;
        for path in paths {
            let response = self
                .http
                .get(format!(
                    "{}/{}.narinfo",
                    substituter.base_url,
                    path.path_hash()
                ))
                .send()
                .await
                .expect("narinfo request");
            assert_eq!(response.status(), 404, "{} must not be served", path.name);
        }
    }

    /// Crash-safety invariant: every chunk referenced by any path in the
    /// committed manifest must live in a pack that still exists in the
    /// (fake) GHA cache. "No live path ever references a deleted pack."
    pub async fn assert_no_dangling_pack_references(&self) {
        let manifest = self.manifest().await;
        let stored = self.stored_pack_keys().await;
        for (path_hash, entry) in &manifest.paths {
            for (_, node) in hestia::chunker::flatten_tree(&entry.tree) {
                let FileSystemObject::Regular(regular) = node else {
                    continue;
                };
                for chunk in &regular.contents.chunks {
                    let location = manifest
                        .chunks
                        .get(chunk)
                        .unwrap_or_else(|| panic!("chunk of {path_hash} has no location"));
                    let key = hestia::chunker::pack_cache_key(&location.pack);
                    assert!(
                        stored.contains(&key),
                        "path {path_hash} references chunk {chunk} in pack {key}, \
                         but that pack does not exist in the cache"
                    );
                }
            }
        }
    }

    async fn start_substituter(&self, manifest: Manifest) -> RunningSubstituter {
        let manifest_store = ManifestStore::new();
        manifest_store.set(manifest);
        let substituter = Substituter::new(
            StoreDir::default(),
            manifest_store,
            AccessLog::new(),
            self.twirp.clone(),
            self.http.clone(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind substituter listener");
        let addr = listener.local_addr().expect("local addr");
        let task = tokio::spawn(async move {
            axum::serve(listener, substituter.into_router())
                .await
                .expect("substituter serve");
        });
        RunningSubstituter {
            base_url: format!("http://{addr}"),
            task,
        }
    }
}

/// A substituter HTTP server running for the duration of one check.
pub struct RunningSubstituter {
    pub base_url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for RunningSubstituter {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Touch detection helper: the `last_accessed_at` of a cache key as unix
/// seconds, or `None` if the key does not exist.
pub async fn last_accessed(rest: &RestClient, key: &str) -> Option<u64> {
    rest.list_caches(key)
        .await
        .expect("cache listing")
        .iter()
        .find(|entry| entry.key == key)
        .and_then(hestia::gha::rest::CacheEntry::last_accessed_unix)
}

/// 1-byte blob reads recorded by the fake (GC touches use `bytes=0-0`).
pub fn one_byte_reads(fake: &FakeGha, key: &str) -> usize {
    fake.blob_requests()
        .iter()
        .filter(|request| request.key == key && request.range.as_deref() == Some("bytes=0-0"))
        .count()
}
