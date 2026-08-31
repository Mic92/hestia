//! End-to-end tests for the substituter: hermetic scratch Nix stores + the
//! fake GHA backend, with Nix itself as the correctness oracle.
//!
//! Tests skip themselves (with a notice) when nix tooling is unavailable
//! (e.g. inside the Nix build sandbox).

mod support;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hestia::chunker::pack_cache_key;
use hestia::gc::{Gc, GcPolicy};
use hestia::manifest::{Hash32, PathHash};
use hestia::pipeline::{AccessLog, now_unix};
use hestia::store::Snapshot;
use hestia::substituter::{ManifestStore, Substituter, verify_packs};

use support::common::{TEST_ROOT_KEY, load_snapshot, pipeline_context, to_path_set};
use support::fake_gha::FakeGha;
use support::store::{ScratchStore, assert_trees_equal, nix_copy};

/// Hard timeout for every test body: a hung server or a deadlocked await
/// turns into a test failure instead of a stuck test binary.
const TEST_TIMEOUT: Duration = Duration::from_secs(120);

async fn timed<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .expect("test timed out: deadlock or hung server")
}

/// A substituter HTTP server running in the background of the test.
struct RunningSubstituter {
    base_url: String,
    /// Store URL for nix commands: includes `?store=<dir>` because the
    /// scratch stores live outside /nix/store and nix's http binary cache
    /// client checks the advertised StoreDir against its own prefix.
    store_url: String,
    manifest: ManifestStore,
    access_log: AccessLog,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for RunningSubstituter {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RunningSubstituter {
    /// Start a substituter for `store`, serving what `fake` publishes.
    async fn start(fake: &FakeGha, http: &reqwest::Client, store: &ScratchStore) -> Self {
        let manifest_store = ManifestStore::new();
        manifest_store.set_snapshot(load_snapshot(fake, http).await);
        let access_log = AccessLog::new();

        let substituter = Substituter::new(
            store.database().store_dir().clone(),
            manifest_store.clone(),
            access_log.clone(),
            fake.backend(http),
        );
        let router = substituter.into_router();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding substituter listener failed");
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let base_url = format!("http://{addr}");
        Self {
            store_url: format!("{base_url}?store={}", store.store_dir_path().display()),
            base_url,
            manifest: manifest_store,
            access_log,
            task,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{path}", self.base_url)
    }

    async fn get(&self, http: &reqwest::Client, path: &str) -> reqwest::Response {
        http.get(self.url(path))
            .send()
            .await
            .expect("substituter request failed")
    }

    async fn narinfo(&self, http: &reqwest::Client, store_path: &Path) -> reqwest::Response {
        self.get(http, &format!("{}.narinfo", path_hash_str(store_path)))
            .await
    }
}

/// The 32-character hash part of a store path basename.
fn path_hash_str(store_path: &Path) -> String {
    store_path.file_name().unwrap().to_str().unwrap()[..32].to_string()
}

fn path_hash_of(store_path: &Path) -> PathHash {
    path_hash_str(store_path).parse().unwrap()
}

/// Push paths through the write pipeline and return what is published.
async fn push_paths(
    fake: &FakeGha,
    http: &reqwest::Client,
    store: &ScratchStore,
    paths: &[&Path],
) -> Arc<Snapshot> {
    let ctx = pipeline_context(fake, http, store.database());
    let stats = ctx
        .run(to_path_set(paths), BTreeSet::new())
        .await
        .expect("pipeline run failed");
    assert_eq!(stats.pushed, paths.len(), "all paths must be pushed");
    load_snapshot(fake, http).await
}

/// Parse narinfo text into a key -> value map (References kept as one line).
fn parse_narinfo(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

/// Number of pack blob downloads the fake backend has served so far.
fn pack_download_count(fake: &FakeGha) -> usize {
    fake.blob_requests()
        .iter()
        .filter(|request| request.key.starts_with("pack-"))
        .count()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nix_cache_info_advertises_store_dir_and_priority() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let response = substituter.get(&http, "nix-cache-info").await;
        assert_eq!(response.status(), 200);
        let body = response.text().await.unwrap();

        assert!(
            body.contains(&format!("StoreDir: {}\n", store.store_dir_path().display())),
            "nix-cache-info:\n{body}"
        );
        assert!(body.contains("WantMassQuery: 1\n"), "{body}");
        assert!(body.contains("Priority: 30\n"), "{body}");
    })
    .await;
}

#[tokio::test]
async fn narinfo_miss_is_404() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // Well-formed hash that is in no manifest.
        let response = substituter
            .get(&http, "00000000000000000000000000000000.narinfo")
            .await;
        assert_eq!(response.status(), 404);
        // Misses are not liveness signals.
        assert!(substituter.access_log.snapshot().is_empty());

        // Malformed requests are 404 too (never 400/500) — including a
        // query string the NarQuery extractor cannot deserialize: every
        // NAR failure must let Nix fall through to the next substituter.
        for path in [
            "zzz.narinfo",
            "x",
            "nar/zzz.nar",
            "nar/x",
            "nar/zzz.nar?hash=a&hash=b",
        ] {
            let response = substituter.get(&http, path).await;
            assert_eq!(response.status(), 404, "GET /{path}");
        }
    })
    .await;
}

#[tokio::test]
async fn narinfo_matches_nix_path_info_oracle() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("narinfo", 71);
        let (top, dep) = store.add_paths_with_reference("narinfo");

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push_paths(&fake, &http, &store, &[&fixture, &top, &dep]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // ---- fixture: no references, CA path ---------------------------------
        let oracle = store
            .path_info_json(&fixture)
            .expect("nix path-info oracle unavailable");
        let response = substituter.narinfo(&http, &fixture).await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/x-nix-narinfo")
        );
        let narinfo = parse_narinfo(&response.text().await.unwrap());

        assert_eq!(narinfo["StorePath"], fixture.display().to_string());
        assert_eq!(narinfo["Compression"], "none");
        assert_eq!(
            narinfo["NarSize"],
            oracle["narSize"].as_u64().unwrap().to_string()
        );
        // NarHash formats differ (sha256:base32 vs SRI); compare parsed digests.
        assert_eq!(
            Hash32::parse_sha256(&narinfo["NarHash"]).expect("NarHash must parse"),
            Hash32::parse_sha256(oracle["narHash"].as_str().unwrap()).unwrap(),
        );
        assert!(
            narinfo["URL"].starts_with("nar/") && narinfo["URL"].contains(".nar"),
            "URL: {}",
            narinfo["URL"]
        );
        assert!(!narinfo.contains_key("References"), "fixture has no refs");
        // nix-store --add produces a content-addressed path; CA must round-trip.
        assert_eq!(
            narinfo.get("CA").map(String::as_str),
            oracle["ca"].as_str(),
            "CA mismatch"
        );
        assert!(!narinfo.contains_key("Sig"), "hestia serves unsigned");

        // ---- top: one reference (dep), full basenames ------------------------
        let response = substituter.narinfo(&http, &top).await;
        assert_eq!(response.status(), 200);
        let narinfo = parse_narinfo(&response.text().await.unwrap());
        let oracle = store.path_info_json(&top).unwrap();

        let mut oracle_refs: Vec<String> = oracle["references"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| {
                // Absolute path -> basename.
                Path::new(value.as_str().unwrap())
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        oracle_refs.sort();
        let mut actual_refs: Vec<String> = narinfo["References"]
            .split_whitespace()
            .map(str::to_string)
            .collect();
        actual_refs.sort();
        assert_eq!(actual_refs, oracle_refs, "references mismatch");
    })
    .await;
}

#[tokio::test]
async fn nar_round_trips_with_matching_hash() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("narbytes", 73);
        let (expected_hash, expected_size) = store
            .nar_hash_oracle(&fixture)
            .expect("nix path-info oracle unavailable");

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let snapshot = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // Follow the URL from the narinfo response, like Nix does.
        let narinfo_text = substituter
            .narinfo(&http, &fixture)
            .await
            .text()
            .await
            .unwrap();
        let narinfo = parse_narinfo(&narinfo_text);
        let response = substituter.get(&http, &narinfo["URL"]).await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.content_length(),
            Some(expected_size),
            "Content-Length must equal the NAR size"
        );

        let nar = response.bytes().await.unwrap();
        assert_eq!(nar.len() as u64, expected_size);

        // Body hashes to the value in the manifest AND the value Nix recorded.
        let body_hash = Hash32::digest(&nar);
        assert_eq!(body_hash, expected_hash, "NAR hash mismatch vs nix oracle");
        assert_eq!(
            body_hash,
            snapshot.lookup(&path_hash_of(&fixture)).unwrap().nar_hash,
            "NAR hash mismatch vs narinfo"
        );

        // The same NAR is also reachable without the ?hash= parameter (pure
        // nar_hash lookup).
        let bare_url = narinfo["URL"].split('?').next().unwrap();
        let response = substituter.get(&http, bare_url).await;
        assert_eq!(response.status(), 200);
        assert_eq!(Hash32::digest(response.bytes().await.unwrap()), body_hash);
    })
    .await;
}

#[tokio::test]
async fn drv_closure_bypasses_upstream_filter_and_imports_into_a_fresh_store() {
    timed(async {
        // The prefetch endpoint: one request streams the whole closure in
        // `nix-store --export` format; `nix-store --import` is the format
        // oracle.
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let drv = store.instantiate_drv("closure-upstream-filter");
        let references = match store
            .database()
            .query(&drv.to_string_lossy())
            .expect("store database query failed")
        {
            hestia::pathinfo::Lookup::Found(info) => info.references,
            other => panic!("drv must be valid in the store database, got {other:?}"),
        };
        let signed_reference = store.store_dir_path().join(
            references
                .iter()
                .find(|reference| reference.to_string().ends_with(".drv"))
                .expect("drv must have an input derivation")
                .to_string(),
        );
        store.sign_path(&signed_reference, "cache.nixos.org-1");
        let unrelated_upstream = store.add_fixture("unrelated-upstream", 13);
        store.sign_path(&unrelated_upstream, "cache.nixos.org-1");

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let ctx = pipeline_context(&fake, &http, store.database());
        let stats = ctx
            .run(
                to_path_set(&[&drv, &unrelated_upstream]),
                BTreeSet::new(),
            )
            .await
            .expect("pipeline run failed");
        assert_eq!(stats.paths_received, 2);
        assert_eq!(stats.skipped_upstream, 1);

        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let response = substituter
            .get(&http, &format!("closure/{}", path_hash_str(&drv)))
            .await;
        assert_eq!(response.status(), 200);
        let body = response.bytes().await.expect("reading export stream");

        let destination = store.create_destination();
        let mut import = tokio::process::Command::new("nix-store")
            .args([
                "--store",
                &destination.uri,
                "--option",
                "require-sigs",
                "false",
                "--import",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawning nix-store --import failed");
        {
            use tokio::io::AsyncWriteExt as _;
            let mut stdin = import.stdin.take().unwrap();
            stdin.write_all(&body).await.unwrap();
        }
        let output = import.wait_with_output().await.unwrap();
        assert!(
            output.status.success(),
            "nix-store --import failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        // The whole drv closure landed, including the upstream-signed input.
        assert_trees_equal(&drv, &destination.physical_path(&drv));
        assert_trees_equal(
            &signed_reference,
            &destination.physical_path(&signed_reference),
        );

        // Prefetched paths count as accessed (GC liveness).
        let accessed = substituter.access_log.snapshot();
        assert!(accessed.contains(&path_hash_of(&drv)));
        assert!(accessed.contains(&path_hash_of(&signed_reference)));

        // Unknown roots are a 404, not a partial stream.
        let miss = substituter
            .get(&http, "closure/00000000000000000000000000000000")
            .await;
        assert_eq!(miss.status(), 404);

        let filtered_fake = FakeGha::start().await;
        let filtered_ctx = hestia::pipeline::PipelineContext {
            filter_drv_closures: true,
            ..pipeline_context(&filtered_fake, &http, store.database())
        };
        let stats = filtered_ctx
            .run(
                to_path_set(&[&drv, &unrelated_upstream]),
                BTreeSet::new(),
            )
            .await
            .expect("pipeline run failed");
        assert_eq!(stats.skipped_upstream, 2);

        let filtered_substituter = RunningSubstituter::start(&filtered_fake, &http, &store).await;
        let external_references = filtered_substituter
            .get(
                &http,
                &format!("closure/{}/external-references", path_hash_str(&drv)),
            )
            .await;
        assert_eq!(external_references.status(), 200);
        assert_eq!(
            external_references.text().await.unwrap(),
            signed_reference.display().to_string()
        );

        let filtered_destination = store.create_destination();
        let installable = format!(
            "/nix/store/{}^*",
            drv.file_name().unwrap().to_string_lossy()
        );
        let nix_config = format!(
            "experimental-features = nix-command\nsubstitute = true\nsubstituters = {}\nrequire-sigs = false\n",
            substituter.store_url
        );
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_hestia"))
            .arg("prefetch")
            .arg("--listen")
            .arg(
                filtered_substituter
                    .base_url
                    .strip_prefix("http://")
                    .unwrap(),
            )
            .arg(installable)
            .env("NIX_REMOTE", &filtered_destination.uri)
            .env("NIX_CONFIG", nix_config)
            .output()
            .await
            .unwrap();
        assert!(
            output.status.success(),
            "hestia prefetch failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_trees_equal(&drv, &filtered_destination.physical_path(&drv));
        assert_trees_equal(
            &signed_reference,
            &filtered_destination.physical_path(&signed_reference),
        );
    })
    .await;
}

#[tokio::test]
async fn nix_copy_substitutes_into_fresh_store() {
    timed(async {
        // The key end-to-end test: nix itself copies a closure out of
        // hestia into an empty store and the contents come out
        // byte-identical.
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("nixcopy", 79);
        let (top, dep) = store.add_paths_with_reference("nixcopy");

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push_paths(&fake, &http, &store, &[&fixture, &top, &dep]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let destination = store.create_destination();

        // Copy the fixture (multi-chunk files, symlink, executable).
        let output = nix_copy(&substituter.store_url, &destination.uri, &fixture).await;
        assert!(
            output.status.success(),
            "nix copy failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_trees_equal(&fixture, &destination.physical_path(&fixture));

        // Copy `top`, whose closure includes `dep`: nix must follow the
        // References line and fetch both.
        let output = nix_copy(&substituter.store_url, &destination.uri, &top).await;
        assert!(
            output.status.success(),
            "nix copy of a closure failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_trees_equal(&top, &destination.physical_path(&top));
        assert_trees_equal(&dep, &destination.physical_path(&dep));

        // Substituted paths were recorded as accessed (liveness signal).
        let accessed = substituter.access_log.snapshot();
        for path in [&fixture, &top, &dep] {
            assert!(
                accessed.contains(&path_hash_of(path)),
                "{} must be in the access log",
                path.display()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn evicted_pack_turns_nar_requests_into_404() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("evicted", 83);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let snapshot = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // Simulate quota-pressure LRU eviction of the pack blob.
        let pack_hash = *snapshot.pack_hashes().first().expect("one pack uploaded");
        fake.evict(&http, &pack_cache_key(&pack_hash)).await;

        // narinfo still answers (the manifest survived) ...
        let response = substituter.narinfo(&http, &fixture).await;
        assert_eq!(response.status(), 200);
        let narinfo = parse_narinfo(&response.text().await.unwrap());

        // ... but the NAR request must be a clean 404, not corrupt data.
        let response = substituter.get(&http, &narinfo["URL"]).await;
        assert_eq!(response.status(), 404);

        // And nix copy fails cleanly: no partial path lands in the destination.
        let destination = store.create_destination();
        let output = nix_copy(&substituter.store_url, &destination.uri, &fixture).await;
        assert!(
            !output.status.success(),
            "nix copy must fail when the pack is gone"
        );
        assert!(
            !destination.physical_path(&fixture).exists(),
            "no partial path may be registered after a failed substitution"
        );
    })
    .await;
}

#[tokio::test]
async fn evicted_pack_negative_caches_narinfo() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("neg-cache", 87);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let snapshot = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let pack_hash = *snapshot.pack_hashes().first().expect("one pack uploaded");
        fake.evict(&http, &pack_cache_key(&pack_hash)).await;

        // The eviction is not known yet.
        let response = substituter.narinfo(&http, &fixture).await;
        assert_eq!(response.status(), 200);
        let narinfo = parse_narinfo(&response.text().await.unwrap());

        // The NAR request discovers the evicted pack ...
        let response = substituter.get(&http, &narinfo["URL"]).await;
        assert_eq!(response.status(), 404);

        // ... and from now on the narinfo misses too.
        let response = substituter.narinfo(&http, &fixture).await;
        assert_eq!(
            response.status(),
            404,
            "narinfo must miss once the pack is known to be evicted"
        );

        // A snapshot replacement resets the negative cache.
        substituter.manifest.set_snapshot(snapshot.clone());
        let response = substituter.narinfo(&http, &fixture).await;
        assert_eq!(
            response.status(),
            200,
            "a fresh snapshot must clear the negative cache"
        );
    })
    .await;
}

#[tokio::test]
async fn eager_pack_verification_marks_evicted_packs_upfront() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("eager-verify", 91);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let snapshot = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // One pack listed, zero threshold: verification must bail out
        // without marking anything.
        verify_packs(&fake.backend(&http), &substituter.manifest, 0).await;
        assert_eq!(
            substituter.narinfo(&http, &fixture).await.status(),
            200,
            "a bailed-out verification must not mark packs missing"
        );

        let pack_hash = *snapshot.pack_hashes().first().expect("one pack uploaded");
        fake.evict(&http, &pack_cache_key(&pack_hash)).await;

        // The listing reveals the eviction before Nix ever asks.
        verify_packs(&fake.backend(&http), &substituter.manifest, 1000).await;
        assert_eq!(
            substituter.narinfo(&http, &fixture).await.status(),
            404,
            "eager verification must turn the narinfo into a miss"
        );
        assert_eq!(
            pack_download_count(&fake),
            0,
            "no pack download may have been attempted"
        );
    })
    .await;
}

#[tokio::test]
async fn narinfo_hits_join_the_root_at_next_drain() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture_a = store.add_fixture("accessed-a", 89);
        let fixture_b = store.add_fixture("accessed-b", 97);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push_paths(&fake, &http, &store, &[&fixture_a, &fixture_b]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // Only A gets a narinfo hit; B is never asked for.
        let response = substituter.narinfo(&http, &fixture_a).await;
        assert_eq!(response.status(), 200);

        assert_eq!(
            substituter.access_log.snapshot(),
            BTreeSet::from([path_hash_of(&fixture_a)]),
            "only the served path is recorded"
        );

        // A later drain (no new pushed paths) publishes a segment naming
        // the accessed path only: that is what GC keeps of this root.
        let ctx = pipeline_context(&fake, &http, store.database());
        let stats = ctx
            .run(BTreeSet::new(), substituter.access_log.snapshot())
            .await
            .expect("drain failed");
        assert!(stats.head.is_some());
        let snapshot = load_snapshot(&fake, &http).await;
        let newest = &snapshot.view.roots[TEST_ROOT_KEY][1];
        let body = fake
            .backend(&http)
            .get(&hestia::store::meta_key(newest), None)
            .await
            .unwrap()
            .unwrap();
        let meta = hestia::segment::Meta::open(&body).unwrap();
        assert!(meta.find(&path_hash_of(&fixture_a)).is_some());
        assert!(
            meta.find(&path_hash_of(&fixture_b)).is_none(),
            "never-accessed, never-pushed path is not named again"
        );
    })
    .await;
}

#[tokio::test]
async fn second_nar_request_reuses_cached_chunks() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("cache-reuse", 101);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let response = substituter.narinfo(&http, &fixture).await;
        assert_eq!(response.status(), 200);
        let narinfo = parse_narinfo(&response.text().await.unwrap());

        // First NAR request fetches chunks from packs.
        let response = substituter.get(&http, &narinfo["URL"]).await;
        assert_eq!(response.status(), 200);
        let nar = response.bytes().await.unwrap();
        let (expected_hash, _) = store.nar_hash_oracle(&fixture).unwrap();
        assert_eq!(Hash32::digest(&nar), expected_hash);

        let downloads_after_first = pack_download_count(&fake);
        assert!(
            downloads_after_first > 0,
            "first NAR request must fetch from packs"
        );

        // Second NAR request for the same path must be served entirely
        // from the in-memory chunk cache — no additional pack reads.
        let response = substituter.get(&http, &narinfo["URL"]).await;
        assert_eq!(response.status(), 200);
        let nar = response.bytes().await.unwrap();
        assert_eq!(Hash32::digest(&nar), expected_hash);

        assert_eq!(
            pack_download_count(&fake),
            downloads_after_first,
            "second NAR request must reuse cached chunks, no new pack reads"
        );
    })
    .await;
}

#[tokio::test]
async fn expired_download_url_is_refreshed_mid_serving() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        // Two paths pushed in one run -> their chunks share a single pack, so
        // serving B reuses the pack URL cached while serving A.
        let fixture_a = store.add_fixture("expiry-a", 103);
        let fixture_b = store.add_fixture("expiry-b", 107);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let snapshot = push_paths(&fake, &http, &store, &[&fixture_a, &fixture_b]).await;
        assert_eq!(snapshot.pack_hashes().len(), 1, "one shared pack expected");
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // Serve A: caches the signed pack URL.
        let narinfo_a = parse_narinfo(
            &substituter
                .narinfo(&http, &fixture_a)
                .await
                .text()
                .await
                .unwrap(),
        );
        let response = substituter.get(&http, &narinfo_a["URL"]).await;
        assert_eq!(response.status(), 200);

        // All previously issued signed URLs expire (SAS expiry).
        fake.expire_urls(&http).await;

        // Serve B: chunk cache has only A's chunks, so packs must be read again
        // through the now-stale cached URL -> 403 -> refresh -> success.
        let narinfo_b = parse_narinfo(
            &substituter
                .narinfo(&http, &fixture_b)
                .await
                .text()
                .await
                .unwrap(),
        );
        let response = substituter.get(&http, &narinfo_b["URL"]).await;
        assert_eq!(
            response.status(),
            200,
            "expired URLs must be refreshed transparently"
        );
        let (expected_hash, _) = store.nar_hash_oracle(&fixture_b).unwrap();
        assert_eq!(
            Hash32::digest(response.bytes().await.unwrap()),
            expected_hash
        );
    })
    .await;
}

#[tokio::test]
async fn drains_become_visible_without_restart() {
    timed(async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture_a = store.add_fixture("refresh-a", 109);
        let fixture_b = store.add_fixture("refresh-b", 113);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();

        // Only A is pushed before the substituter starts.
        push_paths(&fake, &http, &store, &[&fixture_a]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        assert_eq!(substituter.narinfo(&http, &fixture_a).await.status(), 200);
        assert_eq!(
            substituter.narinfo(&http, &fixture_b).await.status(),
            404,
            "B is not published yet"
        );

        // B gets pushed by a later drain; the daemon hands the refreshed
        // snapshot to the substituter.
        let updated = push_paths(&fake, &http, &store, &[&fixture_b]).await;
        substituter.manifest.set_snapshot(updated);

        // Both paths are servable now, including their NARs.
        for fixture in [&fixture_a, &fixture_b] {
            let response = substituter.narinfo(&http, fixture).await;
            assert_eq!(response.status(), 200);
            let narinfo = parse_narinfo(&response.text().await.unwrap());
            let response = substituter.get(&http, &narinfo["URL"]).await;
            assert_eq!(response.status(), 200);
            let (expected_hash, _) = store.nar_hash_oracle(fixture).unwrap();
            assert_eq!(
                Hash32::digest(response.bytes().await.unwrap()),
                expected_hash
            );
        }

        // The destination of a real substitution sees both paths too.
        let destination = store.create_destination();
        for fixture in [&fixture_a, &fixture_b] {
            let output = nix_copy(&substituter.store_url, &destination.uri, fixture).await;
            assert!(
                output.status.success(),
                "nix copy failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_trees_equal(fixture, &destination.physical_path(fixture));
        }
    })
    .await;
}

#[tokio::test]
async fn substitution_with_trusted_store_url_realises_paths() {
    timed(async {
        // Production flow: Nix substitutes through `substituters =
        // http://...?trusted=true` while realising a path into a store, instead
        // of an explicit `nix copy --no-check-sigs`.
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("realise", 127);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let destination = store.create_destination();
        let output = tokio::process::Command::new("nix-store")
            .arg("--store")
            .arg(&destination.uri)
            .arg("--option")
            .arg("substituters")
            .arg(format!("{}&trusted=true", substituter.store_url))
            .arg("--realise")
            .arg(&fixture)
            .output()
            .await
            .expect("running nix-store --realise failed");
        assert!(
            output.status.success(),
            "substitution via trusted store URL failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_trees_equal(&fixture, &destination.physical_path(&fixture));
    })
    .await;
}

#[tokio::test]
async fn transient_blob_failure_is_retried_transparently() {
    timed(async {
        // An Azure-side connection drop mid-Range-read must not surface to
        // Nix at all: the substituter retries the read and serves the NAR.
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("transient-drop", 137);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let snapshot = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // Request the NAR directly: the narinfo round-trip is irrelevant
        // here, the injected failure targets the pack Range read.
        let entry = snapshot.lookup(&path_hash_of(&fixture)).unwrap();
        let nar_url = format!("nar/{}.nar", entry.nar_hash.to_hex());

        // Exactly one connection drop: within the retry budget.
        fake.fail_blob_reads(&http, 1).await;

        let response = substituter.get(&http, &nar_url).await;
        assert_eq!(
            response.status(),
            200,
            "a single transient failure must be absorbed by the retry"
        );
        let nar = response.bytes().await.unwrap();
        assert_eq!(
            Hash32::digest(&nar),
            entry.nar_hash,
            "retried NAR must still be byte-correct"
        );
    })
    .await;
}

#[tokio::test]
async fn nar_downloads_record_access_without_a_narinfo_hit() {
    timed(async {
        // Nix caches positive narinfo lookups on disk
        // (~/.cache/nix/binary-cache-v6.sqlite) and may fetch a NAR without
        // re-requesting the narinfo. A NAR download is the strongest
        // possible evidence a path is in use, so it must be recorded in the
        // access log (the GC liveness signal) just like a narinfo hit --
        // otherwise an actively substituted path never joins the root and
        // can be garbage collected out from under its users.
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("nar-access", 149);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let snapshot = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let path_hash = path_hash_of(&fixture);
        let entry = snapshot.lookup(&path_hash).unwrap();

        // Fetch the NAR directly, both with and without the ?hash= parameter
        // a narinfo URL would carry. No narinfo request is ever made.
        for nar_url in [
            format!("nar/{}.nar?hash={}", entry.nar_hash.to_hex(), path_hash),
            format!("nar/{}.nar", entry.nar_hash.to_hex()),
        ] {
            let response = substituter.get(&http, &nar_url).await;
            assert_eq!(response.status(), 200);
            assert_eq!(
                Hash32::digest(response.bytes().await.unwrap()),
                entry.nar_hash
            );
        }

        assert!(
            substituter.access_log.snapshot().contains(&path_hash),
            "a served NAR must be recorded as an access (GC liveness signal)"
        );
    })
    .await;
}

#[tokio::test]
async fn concurrent_gc_repack_triggers_reload() {
    timed(async {
        // A nightly `hestia gc` repack moves live chunks into a new pack
        // and the old pack disappears while a running daemon still serves
        // the pre-repack view. The NAR handler must reload on the pack
        // miss and serve from the new pack instead of 404ing every
        // affected path for the rest of the job.
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("repack-reload", 157);
        let dropped = store.add_fixture("repack-dropped", 163);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let snapshot = push_paths(&fake, &http, &store, &[&fixture, &dropped]).await;

        let manifest_store = ManifestStore::new();
        manifest_store.set_snapshot(snapshot.clone());
        let reload_store = manifest_store.clone();
        let reload_backend = fake.backend(&http);
        let substituter = Substituter::new(
            store.database().store_dir().clone(),
            manifest_store.clone(),
            AccessLog::new(),
            fake.backend(&http),
        )
        .with_manifest_reload(std::sync::Arc::new(move || {
            let backend = reload_backend.clone();
            let manifest_store = reload_store.clone();
            Box::pin(async move {
                let roots = [TEST_ROOT_KEY.to_string()];
                if let Ok(s) =
                    Snapshot::load(backend, hestia::trust::Trust::open(), &roots, None).await
                {
                    manifest_store.set_snapshot(Arc::new(s));
                }
            })
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, substituter.into_router())
                .await
                .unwrap();
        });

        // A later job uses only `fixture`, so after the next GC half the
        // pack is dead: GC copies the live half into a new pack and the
        // old one goes away.
        let old_pack = *snapshot.pack_hashes().first().expect("one pack uploaded");
        let gc = Gc {
            backend: fake.backend(&http),
            trust: hestia::trust::Trust::open(),
            policy: GcPolicy {
                min_liveness: 0.9,
                ..GcPolicy::default()
            },
            dry_run: false,
        };
        let t = now_unix() + 2 * 3600;
        gc.run(t).await.unwrap();
        pipeline_context(&fake, &http, store.database())
            .run(BTreeSet::new(), BTreeSet::from([path_hash_of(&fixture)]))
            .await
            .unwrap();
        let stats = gc.run(t + 2 * 3600).await.unwrap();
        assert_eq!(stats.packs_repacked, 1);
        fake.evict(&http, &pack_cache_key(&old_pack)).await;

        // The served view is stale (still points at the deleted pack), but
        // the NAR request must succeed via the reload.
        let entry = snapshot.lookup(&path_hash_of(&fixture)).unwrap();
        let nar_url = format!("{base_url}/nar/{}.nar", entry.nar_hash.to_hex());
        let response = http.get(&nar_url).send().await.unwrap();
        assert_eq!(
            response.status(),
            200,
            "a pack deleted by a concurrent gc repack must trigger a reload, not a 404"
        );
        assert_eq!(
            Hash32::digest(response.bytes().await.unwrap()),
            entry.nar_hash
        );
        server.abort();
    })
    .await;
}

#[tokio::test]
async fn persistent_blob_failures_yield_404_then_recover() {
    timed(async {
        // When Azure keeps dropping connections (outage), the NAR request
        // must fail with a clean 404 — Nix rebuilds — and never with corrupt
        // data. Once the outage is over, the same URL works again.
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("persistent-drop", 139);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let snapshot = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let entry = snapshot.lookup(&path_hash_of(&fixture)).unwrap();
        let nar_url = format!("nar/{}.nar", entry.nar_hash.to_hex());

        // More failures than the retry budget can absorb.
        fake.fail_blob_reads(&http, 1_000).await;
        let response = substituter.get(&http, &nar_url).await;
        assert_eq!(
            response.status(),
            404,
            "persistent failures must produce a clean 404, never partial data"
        );

        // Nix copy against the broken cache fails without leaving partial
        // paths behind.
        let destination = store.create_destination();
        let output = nix_copy(&substituter.store_url, &destination.uri, &fixture).await;
        assert!(
            !output.status.success(),
            "nix copy must fail during the outage"
        );
        assert!(
            !destination.physical_path(&fixture).exists(),
            "no partial path may be registered after a failed substitution"
        );

        // Outage over: the substituter recovers without a restart.
        fake.fail_blob_reads(&http, 0).await;
        let response = substituter.get(&http, &nar_url).await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            Hash32::digest(response.bytes().await.unwrap()),
            entry.nar_hash
        );
    })
    .await;
}
