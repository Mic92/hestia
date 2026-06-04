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
use std::time::Duration;

use hestia::chunker::pack_cache_key;
use hestia::manifest::{Hash32, Manifest, PathHash};
use hestia::pipeline::{AccessLog, now_unix};
use hestia::substituter::{ManifestStore, Substituter};

use support::common::{TEST_ROOT_KEY, pipeline_context, to_path_set};
use support::fake_gha::FakeGha;
use support::store::ScratchStore;

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
    /// Start a substituter for `store`, serving the manifest currently
    /// committed in `fake`.
    async fn start(fake: &FakeGha, http: &reqwest::Client, store: &ScratchStore) -> Self {
        let ctx = pipeline_context(fake, http, store.database());
        let manifest = ctx.load_manifest().await.expect("loading manifest failed");

        let manifest_store = ManifestStore::new();
        manifest_store.set(manifest);
        let access_log = AccessLog::new();

        let substituter = Substituter::new(
            store.database().store_dir().clone(),
            manifest_store.clone(),
            access_log.clone(),
            fake.twirp(http),
            http.clone(),
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

/// Push paths through the write pipeline and return the committed manifest.
async fn push_paths(
    fake: &FakeGha,
    http: &reqwest::Client,
    store: &ScratchStore,
    paths: &[&Path],
) -> Manifest {
    let ctx = pipeline_context(fake, http, store.database());
    let stats = ctx
        .run(to_path_set(paths), BTreeSet::new(), now_unix())
        .await
        .expect("pipeline run failed");
    assert_eq!(stats.pushed, paths.len(), "all paths must be pushed");
    ctx.load_manifest().await.expect("loading manifest failed")
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

/// Recursively compare two directory trees (types, contents, symlink
/// targets, executable bits).
fn assert_trees_equal(expected: &Path, actual: &Path) {
    let expected_meta = std::fs::symlink_metadata(expected)
        .unwrap_or_else(|err| panic!("missing expected path {}: {err}", expected.display()));
    let actual_meta = std::fs::symlink_metadata(actual)
        .unwrap_or_else(|err| panic!("missing actual path {}: {err}", actual.display()));

    if expected_meta.file_type().is_symlink() {
        assert!(actual_meta.file_type().is_symlink());
        assert_eq!(
            std::fs::read_link(expected).unwrap(),
            std::fs::read_link(actual).unwrap(),
            "symlink target mismatch at {}",
            actual.display()
        );
    } else if expected_meta.is_dir() {
        assert!(actual_meta.is_dir(), "{} must be a dir", actual.display());
        let list = |dir: &Path| -> Vec<String> {
            let mut names: Vec<String> = std::fs::read_dir(dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            names
        };
        let entries = list(expected);
        assert_eq!(entries, list(actual), "dir entries at {}", actual.display());
        for name in entries {
            assert_trees_equal(&expected.join(&name), &actual.join(&name));
        }
    } else {
        assert_eq!(
            std::fs::read(expected).unwrap(),
            std::fs::read(actual).unwrap(),
            "file contents mismatch at {}",
            actual.display()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                expected_meta.permissions().mode() & 0o111 != 0,
                actual_meta.permissions().mode() & 0o111 != 0,
                "executable bit mismatch at {}",
                actual.display()
            );
        }
    }
}

/// Run `nix copy` from a substituter URL into a destination store.
///
/// Async (tokio::process): the substituter being copied from runs as a task
/// on this test's runtime, so the test must not block the runtime thread
/// while waiting for the subprocess.
async fn nix_copy(from_url: &str, to_uri: &str, store_path: &Path) -> std::process::Output {
    tokio::process::Command::new("nix")
        .args([
            "--extra-experimental-features",
            "nix-command",
            "copy",
            "--no-check-sigs",
            "--from",
            from_url,
            "--to",
            to_uri,
        ])
        .arg(store_path)
        .output()
        .await
        .expect("running nix copy failed")
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

        // Malformed requests are 404 too (never 500).
        for path in ["zzz.narinfo", "x", "nar/zzz.nar", "nar/x"] {
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
            .filter(|name| {
                // The oracle includes the self-reference for toFile paths;
                // narinfo convention is to list it too, but hestia stores
                // references without self (the manifest drops self-edges).
                *name != top.file_name().unwrap().to_string_lossy()
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
        let manifest = push_paths(&fake, &http, &store, &[&fixture]).await;
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
            manifest.paths[&path_hash_of(&fixture)].nar_hash,
            "NAR hash mismatch vs manifest"
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
        let manifest = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // Simulate quota-pressure LRU eviction of the pack blob.
        let pack_hash = *manifest.packs.keys().next().expect("one pack uploaded");
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

        // A later drain (no new pushed paths) replaces the root with
        // pushed ∪ accessed. Use a timestamp outside the root union window so
        // the new root *replaces* the old one instead of merging with it.
        let ctx = pipeline_context(&fake, &http, store.database());
        let later = now_unix() + 3600;
        let stats = ctx
            .run(BTreeSet::new(), substituter.access_log.snapshot(), later)
            .await
            .expect("drain failed");
        assert!(stats.manifest_version > 0);

        let manifest = ctx.load_manifest().await.unwrap();
        let root = &manifest.roots[TEST_ROOT_KEY];
        assert!(
            root.paths.contains(&path_hash_of(&fixture_a)),
            "accessed path must be pinned by the new root"
        );
        assert!(
            !root.paths.contains(&path_hash_of(&fixture_b)),
            "never-accessed, never-pushed path must drop out of the root"
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
        let manifest = push_paths(&fake, &http, &store, &[&fixture_a, &fixture_b]).await;
        assert_eq!(manifest.packs.len(), 1, "one shared pack expected");
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
async fn manifest_updates_become_visible_without_restart() {
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
            "B is not in the manifest yet"
        );

        // B gets pushed by a later drain; the daemon hands the refreshed
        // manifest to the substituter (here: ManifestStore::set).
        let updated = push_paths(&fake, &http, &store, &[&fixture_b]).await;
        substituter.manifest.set(updated);

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
        let manifest = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        // Request the NAR directly (not via narinfo) so the background
        // prefetch cannot race with the injected failure.
        let entry = &manifest.paths[&path_hash_of(&fixture)];
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
        let manifest = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let path_hash = path_hash_of(&fixture);
        let entry = &manifest.paths[&path_hash];

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
        let manifest = push_paths(&fake, &http, &store, &[&fixture]).await;
        let substituter = RunningSubstituter::start(&fake, &http, &store).await;

        let entry = &manifest.paths[&path_hash_of(&fixture)];
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
