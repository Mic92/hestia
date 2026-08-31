//! Synthetic push driver for GC tests.
//!
//! GC operates on segments and pack blobs, not on a Nix store. This
//! module fabricates store paths with deterministic contents and pushes
//! them with the real chunker, pack builder, segment writer and head
//! publish against the fake GHA backend (one pack per push, no size
//! splitting). No nix tooling required, so a 30-day history simulates
//! in seconds. Readability checks go through the real substituter.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;

use hestia::backend::Backend;
use hestia::chunker::{Chunk, PackBuilder, chunk_data, nar_hash_from_chunks};
use hestia::gc::{Gc, GcPolicy, GcStats};
use hestia::manifest::{
    ChunkHash, ChunkList, Directory, FileSystemObject, FileTree, Hash32, PathEntry, PathHash,
    Regular, StorePath, StorePathHash,
};
use hestia::pathinfo::StoreDir;
use hestia::pipeline::{AccessLog, Clock, upload_pack};
use hestia::refnorm::RefTable;
use hestia::segment::SegmentWriter;
use hestia::store::{self, Snapshot};
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

/// A fabricated store path. Its hash is derived from its name, its contents
/// from the seed: the same (name, seed, size) is always the same path with
/// the same chunks.
#[derive(Debug, Clone)]
pub struct SimPath {
    pub name: String,
    pub files: Vec<(String, Bytes)>,
}

impl SimPath {
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
                        ..Default::default()
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

/// Drives pushes and GC against one fake backend.
pub struct SimCache {
    pub http: reqwest::Client,
    pub backend: Backend,
    pub trust: hestia::trust::Trust,
    /// The fake's clock, so head names age with `set_clock`.
    pub clock: Clock,
}

impl SimCache {
    pub fn new(fake: &FakeGha, http: &reqwest::Client) -> Self {
        Self::with(fake.backend(http), fake.clock())
    }

    pub fn with(backend: Backend, clock: Clock) -> Self {
        Self {
            http: reqwest::Client::new(),
            backend,
            trust: hestia::trust::Trust::open(),
            clock,
        }
    }

    pub fn gc(&self, policy: GcPolicy) -> Gc {
        Gc {
            backend: self.backend.clone(),
            trust: self.trust.clone(),
            policy,
            dry_run: false,
        }
    }

    pub async fn run_gc(&self, policy: GcPolicy, now: u64) -> GcStats {
        self.gc(policy).run(now).await.expect("gc run")
    }

    /// What a reader subscribed to every root sees.
    pub async fn snapshot(&self) -> Snapshot {
        let roots: Vec<String> = store::Heads::load(&self.backend, &self.trust)
            .await
            .expect("heads")
            .view
            .roots
            .into_keys()
            .collect();
        Snapshot::load(self.backend.clone(), self.trust.clone(), &roots, None)
            .await
            .expect("snapshot")
    }

    /// Simulate one CI run's drain: `pushed` are uploaded unless a served
    /// segment already has them, `closure` is everything the run used. The
    /// published segment names pushed ∪ closure, like the pipeline does.
    pub async fn push(&self, root_key: &str, pushed: &[&SimPath], closure: &[&SimPath]) {
        let snapshot = self.snapshot().await;
        let mut known = snapshot.known_chunks();
        let mut builder = PackBuilder::new();
        let mut fresh: Vec<PathEntry> = Vec::new();

        for path in pushed {
            if snapshot.contains(&path.path_hash()) {
                continue;
            }
            let (tree, chunks) = path.chunked();
            let chunk_map: BTreeMap<ChunkHash, Bytes> = chunks
                .iter()
                .map(|chunk| (chunk.hash, chunk.data.clone()))
                .collect();
            let (nar_hash, nar_size) = nar_hash_from_chunks(&tree, &chunk_map, &RefTable::new(&[]))
                .await
                .expect("nar hash from chunks");
            for chunk in &chunks {
                if !known.contains(&chunk.hash) {
                    builder.add(chunk).expect("pack add");
                }
            }
            fresh.push(PathEntry {
                store_path: path.store_path(),
                nar_hash,
                nar_size,
                references: vec![],
                ca: None,
                deriver: None,
                tree,
            });
        }
        if !builder.is_empty() {
            let pack = builder.finish();
            upload_pack(&self.backend, &pack)
                .await
                .expect("pack upload");
            known.add(pack.hash, &pack.index());
        }
        let mut writer = SegmentWriter::default();
        for entry in &fresh {
            store::push_entry(&mut writer, entry, &known).expect("chunks located");
        }
        for path in pushed.iter().chain(closure) {
            let hash = path.path_hash();
            if !writer.contains(&hash) {
                snapshot
                    .copy_entry(&hash, &mut writer)
                    .await
                    .expect("copy entry");
            }
        }
        if writer.is_empty() {
            return;
        }
        let sealed = writer.seal().expect("seal");
        store::publish(
            &self.backend,
            &self.trust,
            &snapshot.view,
            root_key,
            &sealed,
            (self.clock)(),
        )
        .await
        .expect("publish");
    }

    /// Upload a pack no segment references (a drain that crashed before
    /// publishing its head).
    pub async fn upload_orphan_pack(&self, seed: u64) -> String {
        let chunks = chunk_data(&test_data(50_000, seed));
        let mut builder = PackBuilder::new();
        for chunk in &chunks {
            builder.add(chunk).expect("pack add");
        }
        let pack = builder.finish();
        upload_pack(&self.backend, &pack)
            .await
            .expect("orphan pack upload");
        pack.cache_key()
    }

    pub async fn stored_keys(&self, prefix: &str) -> BTreeSet<String> {
        let listed = match self.backend.list(prefix, None).await.expect("listing") {
            Some(heads) => heads,
            None => self
                .backend
                .list_objects()
                .await
                .expect("objects")
                .expect("listable"),
        };
        listed
            .into_iter()
            .map(|l| l.key)
            .filter(|k| k.starts_with(prefix))
            .collect()
    }

    /// Compressed bytes of every chunk a served path references (what GC
    /// storage should converge towards).
    pub async fn live_chunk_bytes(&self) -> u64 {
        let snapshot = self.snapshot().await;
        let mut chunks = BTreeMap::new();
        for hash in snapshot.path_hashes() {
            let r = snapshot.resolve(&hash).await.expect("resolve").unwrap();
            chunks.extend(r.map.chunks);
        }
        chunks.values().map(|l| u64::from(l.compressed_size)).sum()
    }

    /// Every given path is fully readable through the substituter
    /// (narinfo served, NAR downloads, hash matches).
    pub async fn assert_readable(&self, paths: &[&SimPath]) {
        let snapshot = Arc::new(self.snapshot().await);
        let substituter = self.start_substituter(snapshot.clone()).await;
        for path in paths {
            let hash = path.path_hash();
            let entry = snapshot
                .lookup(&hash)
                .unwrap_or_else(|| panic!("path {} must be served", path.name));
            let response = self
                .http
                .get(format!("{}/{hash}.narinfo", substituter.base_url))
                .send()
                .await
                .expect("narinfo request");
            assert_eq!(response.status(), 200, "narinfo for {}", path.name);
            let narinfo = response.text().await.expect("narinfo body");
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

    /// Dropped paths must 404 so Nix rebuilds them.
    pub async fn assert_unavailable(&self, paths: &[&SimPath]) {
        let snapshot = Arc::new(self.snapshot().await);
        let substituter = self.start_substituter(snapshot).await;
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

    /// Crash-safety invariant: every pack and segment the view names exists.
    pub async fn assert_no_dangling_references(&self) {
        let snapshot = self.snapshot().await;
        let packs = self.stored_keys("pack-").await;
        let segs = self.stored_keys("seg-").await;
        for d in snapshot.view.segments() {
            assert!(
                segs.contains(&store::meta_key(&d)),
                "view names missing segment {d}"
            );
        }
        for pack in snapshot.pack_hashes() {
            assert!(
                packs.contains(&hestia::chunker::pack_cache_key(&pack)),
                "a served segment references missing pack {pack}"
            );
        }
    }

    async fn start_substituter(&self, snapshot: Arc<Snapshot>) -> RunningSubstituter {
        let manifest_store = ManifestStore::new();
        manifest_store.set_snapshot(snapshot);
        let substituter = Substituter::new(
            StoreDir::default(),
            manifest_store,
            AccessLog::new(),
            self.backend.clone(),
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

pub struct RunningSubstituter {
    pub base_url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for RunningSubstituter {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// 1-byte blob reads recorded by the fake (GC touches use `bytes=0-0`).
pub fn one_byte_reads(fake: &FakeGha, key: &str) -> usize {
    fake.blob_requests()
        .iter()
        .filter(|request| request.key == key && request.range.as_deref() == Some("bytes=0-0"))
        .count()
}
