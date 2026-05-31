//! Integration tests for the serve daemon: hook listener, drain lifecycle,
//! idle-exit, and shutdown behavior.
//!
//! Uses in-process daemons against hermetic scratch stores and the fake GHA
//! backend; one test drives the real `hestia drain` binary end to end.

mod support;

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use hestia::gha::savemutable::SaveMutable;
use hestia::manifest::Manifest;
use hestia::pathinfo::StoreDatabase;
use hestia::pipeline::{self, AccessLog, MANIFEST_PREFIX, PipelineContext};
use hestia::protocol::{self, DrainStats, Request};
use hestia::serve::Daemon;
use hestia::upstream::UpstreamFilter;

use support::fake_gha::FakeGha;
use support::store::ScratchStore;

const TEST_ROOT_KEY: &str = "main-test-system";

fn pipeline_context(
    fake: &FakeGha,
    http: &reqwest::Client,
    store: StoreDatabase,
) -> PipelineContext {
    PipelineContext {
        twirp: fake.twirp(http),
        http: http.clone(),
        store,
        upstream: UpstreamFilter::default(),
        root_key: TEST_ROOT_KEY.to_string(),
        manifest_prefix: MANIFEST_PREFIX.to_string(),
    }
}

/// A daemon running in the background of the test.
struct RunningDaemon {
    socket: PathBuf,
    handle: JoinHandle<Result<DrainStats, pipeline::Error>>,
    shutdown: oneshot::Sender<()>,
}

impl RunningDaemon {
    async fn start(socket: PathBuf, idle_exit: Option<Duration>, ctx: PipelineContext) -> Self {
        let daemon =
            Daemon::bind(&socket, idle_exit, ctx, AccessLog::new()).expect("binding daemon failed");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(daemon.run(async {
            let _ = shutdown_rx.await;
        }));
        Self {
            socket,
            handle,
            shutdown,
        }
    }

    /// Trigger shutdown and wait for the final drain's stats.
    async fn stop(self) -> Result<DrainStats, pipeline::Error> {
        let _ = self.shutdown.send(());
        self.handle.await.expect("daemon task panicked")
    }

    async fn request(&self, request: &Request) -> protocol::Response {
        protocol::roundtrip(&self.socket, request)
            .await
            .expect("request to daemon failed")
    }

    async fn add(&self, paths: &[&Path]) -> protocol::Response {
        self.request(&Request::Add {
            paths: paths
                .iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect(),
        })
        .await
    }
}

async fn committed_manifest(fake: &FakeGha, http: &reqwest::Client) -> Option<(u64, Manifest)> {
    let twirp = fake.twirp(http);
    let save = SaveMutable::new(&twirp, http, MANIFEST_PREFIX);
    let entry = save.load().await.expect("loading manifest failed")?;
    Some((
        entry.index,
        Manifest::decode(&entry.data).expect("manifest must decode"),
    ))
}

fn path_hash_of(store_path: &Path) -> hestia::manifest::PathHash {
    let name = store_path.file_name().unwrap().to_str().unwrap();
    name[..32].parse().unwrap()
}

#[tokio::test]
async fn hook_drain_status_lifecycle() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture_a = store.add_fixture("lifecycle-a", 41);
    let fixture_b = store.add_fixture("lifecycle-b", 43);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let socket = store_socket_path(&store);
    let daemon = RunningDaemon::start(
        socket,
        None,
        pipeline_context(&fake, &http, store.database()),
    )
    .await;

    // Initially: nothing buffered.
    let status = daemon.request(&Request::Status).await;
    assert_eq!(status.buffered, Some(0));

    // Hook registers two paths (one per request, like two nix builds).
    let response = daemon.add(&[&fixture_a]).await;
    assert_eq!(response.buffered, Some(1));
    let response = daemon.add(&[&fixture_b]).await;
    assert_eq!(response.buffered, Some(2));

    // Re-registering the same path does not double-count.
    let response = daemon.add(&[&fixture_a]).await;
    assert_eq!(response.buffered, Some(2));

    // Drain uploads both.
    let response = daemon.request(&Request::Drain).await;
    let stats = response.stats.expect("drain response carries stats");
    assert_eq!(stats.paths_received, 2);
    assert_eq!(stats.pushed, 2);
    assert_eq!(stats.packs_uploaded, 1);
    assert!(stats.manifest_version > 0);

    // Buffer is empty afterwards.
    let status = daemon.request(&Request::Status).await;
    assert_eq!(status.buffered, Some(0));

    // The manifest contains both paths.
    let (_, manifest) = committed_manifest(&fake, &http).await.unwrap();
    assert!(manifest.paths.contains_key(&path_hash_of(&fixture_a)));
    assert!(manifest.paths.contains_key(&path_hash_of(&fixture_b)));

    // Shutdown: final drain has nothing to do.
    let final_stats = daemon.stop().await.expect("final drain failed");
    assert_eq!(final_stats.pushed, 0);
    assert_eq!(final_stats.paths_received, 0);
}

#[tokio::test]
async fn drain_under_concurrent_hook_sends_loses_no_paths() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    // Several distinct paths, registered from concurrent connections while
    // drains run in between.
    let fixtures: Vec<PathBuf> = (0..4)
        .map(|i| store.add_fixture(&format!("concurrent-{i}"), 100 + i))
        .collect();

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let socket = store_socket_path(&store);
    let daemon = RunningDaemon::start(
        socket.clone(),
        None,
        pipeline_context(&fake, &http, store.database()),
    )
    .await;

    // Concurrently: every fixture registered from its own connection, and
    // two drain requests racing with the adds.
    let mut tasks = Vec::new();
    for fixture in &fixtures {
        let socket = socket.clone();
        let path = fixture.to_string_lossy().into_owned();
        tasks.push(tokio::spawn(async move {
            protocol::roundtrip(&socket, &Request::Add { paths: vec![path] })
                .await
                .expect("add failed");
        }));
    }
    for _ in 0..2 {
        let socket = socket.clone();
        tasks.push(tokio::spawn(async move {
            // Drains may interleave with adds in any order; both outcomes
            // (paths drained now or at shutdown) are valid.
            protocol::roundtrip(&socket, &Request::Drain)
                .await
                .expect("drain failed");
        }));
    }
    for task in tasks {
        task.await.expect("task panicked");
    }

    // Shutdown drains whatever the racing drains did not catch.
    daemon.stop().await.expect("final drain failed");

    // No path lost: all fixtures are in the manifest and pinned by the root.
    let (_, manifest) = committed_manifest(&fake, &http).await.unwrap();
    for fixture in &fixtures {
        let hash = path_hash_of(fixture);
        assert!(
            manifest.paths.contains_key(&hash),
            "path {} lost during concurrent hook/drain",
            fixture.display()
        );
        assert!(manifest.roots[TEST_ROOT_KEY].paths.contains(&hash));
    }
}

#[tokio::test]
async fn shutdown_drains_buffered_paths() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("shutdown", 53);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let daemon = RunningDaemon::start(
        store_socket_path(&store),
        None,
        pipeline_context(&fake, &http, store.database()),
    )
    .await;

    // Register but never drain explicitly.
    daemon.add(&[&fixture]).await;

    // Shutdown must flush the buffer (the action post-step relies on this).
    let stats = daemon.stop().await.expect("final drain failed");
    assert_eq!(stats.pushed, 1);
    assert_eq!(stats.packs_uploaded, 1);

    let (_, manifest) = committed_manifest(&fake, &http).await.unwrap();
    assert!(manifest.paths.contains_key(&path_hash_of(&fixture)));
}

#[tokio::test]
async fn idle_exit_drains_and_returns() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("idle", 59);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let socket = store_socket_path(&store);

    let daemon = Daemon::bind(
        &socket,
        Some(Duration::from_millis(300)),
        pipeline_context(&fake, &http, store.database()),
        AccessLog::new(),
    )
    .expect("binding daemon failed");

    // Run with a shutdown future that never resolves: only idle-exit can
    // end this daemon.
    let handle = tokio::spawn(daemon.run(std::future::pending()));

    // Register a path, then go quiet.
    protocol::roundtrip(
        &socket,
        &Request::Add {
            paths: vec![fixture.to_string_lossy().into_owned()],
        },
    )
    .await
    .expect("add failed");

    // The daemon must exit by itself and push the path on the way out.
    let stats = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("daemon did not idle-exit")
        .expect("daemon task panicked")
        .expect("final drain failed");
    assert_eq!(stats.pushed, 1);

    let (_, manifest) = committed_manifest(&fake, &http).await.unwrap();
    assert!(manifest.paths.contains_key(&path_hash_of(&fixture)));
}

#[tokio::test]
async fn failed_drain_keeps_paths_buffered_for_retry() {
    // A drain that cannot reach the store database must not lose the
    // buffered paths: they stay queued for a later retry.
    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("hook.sock");

    let broken_store = StoreDatabase::new("/nonexistent/db.sqlite");
    let daemon = RunningDaemon::start(
        socket.clone(),
        None,
        pipeline_context(&fake, &http, broken_store),
    )
    .await;

    daemon
        .request(&Request::Add {
            paths: vec!["/nix/store/00000000000000000000000000000000-some-path".to_string()],
        })
        .await;

    // Drain fails (database unreadable) and reports an error.
    let result = protocol::roundtrip(&socket, &Request::Drain).await;
    assert!(
        matches!(result, Err(protocol::Error::Daemon(_))),
        "drain against a broken store must fail, got {result:?}"
    );

    // The path is still buffered.
    let status = daemon.request(&Request::Status).await;
    assert_eq!(status.buffered, Some(1));

    // Shutdown: the final drain fails too (still broken), and the daemon
    // surfaces that error.
    assert!(daemon.stop().await.is_err());
}

#[tokio::test]
async fn drain_cli_binary_reports_stats_and_exits_zero() {
    let Some(store) = ScratchStore::create() else {
        return;
    };
    let fixture = store.add_fixture("cli-drain", 61);

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    let socket = store_socket_path(&store);
    let daemon = RunningDaemon::start(
        socket.clone(),
        None,
        pipeline_context(&fake, &http, store.database()),
    )
    .await;

    daemon.add(&[&fixture]).await;

    // Drive the real `hestia drain` binary against the daemon socket.
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_hestia"))
        .args(["drain", "--timeout", "60", "--socket"])
        .arg(&socket)
        .output()
        .await
        .expect("spawning hestia drain failed");

    assert!(
        output.status.success(),
        "drain must exit 0 on success, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("1 path(s) pushed"),
        "summary must mention the pushed path, got: {stderr}"
    );

    daemon.stop().await.expect("final drain failed");
}

#[tokio::test]
async fn drain_cli_binary_fails_against_dead_socket() {
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_hestia"))
        .args([
            "drain",
            "--timeout",
            "1",
            "--socket",
            "/nonexistent/hestia/hook.sock",
        ])
        .output()
        .await
        .expect("spawning hestia drain failed");

    assert!(
        !output.status.success(),
        "drain must report failure when the daemon is unreachable"
    );
}

/// Socket path inside the scratch store's tempdir (cleaned up with it).
fn store_socket_path(store: &ScratchStore) -> PathBuf {
    store.db_path().parent().unwrap().join("hestia-hook.sock")
}
