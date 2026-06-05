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

use hestia::pathinfo::StoreDatabase;
use hestia::pipeline::{self, AccessLog, PipelineContext};
use hestia::protocol::{self, DrainStats, Request};
use hestia::serve::Daemon;
use hestia::substituter::{ManifestStore, Substituter};

use support::common::{TEST_ROOT_KEY, committed_manifest, path_hash_of, pipeline_context};
use support::fake_gha::FakeGha;
use support::store::ScratchStore;

/// A daemon running in the background of the test.
struct RunningDaemon {
    socket: PathBuf,
    manifest_store: ManifestStore,
    access_log: AccessLog,
    handle: JoinHandle<Result<DrainStats, pipeline::Error>>,
    shutdown: oneshot::Sender<()>,
}

impl RunningDaemon {
    async fn start(socket: PathBuf, idle_exit: Option<Duration>, ctx: PipelineContext) -> Self {
        let manifest_store = ManifestStore::new();
        let access_log = AccessLog::new();
        let daemon = Daemon::bind(
            &socket,
            idle_exit,
            ctx,
            access_log.clone(),
            manifest_store.clone(),
        )
        .expect("binding daemon failed");
        let (shutdown, shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(daemon.run(async {
            let _ = shutdown_rx.await;
        }));
        Self {
            socket,
            manifest_store,
            access_log,
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
        ManifestStore::new(),
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
async fn oversized_hook_request_is_rejected_with_bounded_memory() {
    // The hook socket is reachable by anything that can open the socket
    // file (and clients are not necessarily hestia: a stray process can
    // connect and write garbage). A request line must not be buffered
    // without bound: the daemon has to stop reading after a fixed cap and
    // drop the connection, instead of accumulating an endless line in
    // memory until the runner OOMs.
    let test = async {
        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hook.sock");

        // No store database needed: the malformed request never reaches the
        // pipeline.
        let daemon = RunningDaemon::start(
            socket.clone(),
            None,
            pipeline_context(&fake, &http, StoreDatabase::new("/nonexistent/db.sqlite")),
        )
        .await;

        // Stream a single line far larger than any legitimate request
        // (every Add request carries one build's $OUT_PATHS; even thousands
        // of paths are well under a megabyte). 64 MiB, never a newline.
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let mut stream = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("connecting to hook socket failed");
        let chunk = vec![b'a'; 1024 * 1024];
        let mut write_failed = false;
        for _ in 0..64 {
            if stream.write_all(&chunk).await.is_err() {
                // The daemon cut the connection: exactly what we want.
                write_failed = true;
                break;
            }
        }

        if !write_failed {
            // The daemon accepted everything written so far. It must now
            // either respond with an error or close the connection -- not
            // keep reading (and buffering) forever.
            let mut response = Vec::new();
            let outcome =
                tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut response))
                    .await;
            assert!(
                outcome.is_ok(),
                "daemon must reject an oversized request instead of \
                 buffering it in memory indefinitely"
            );
            if let Ok(Ok(read)) = outcome
                && read > 0
            {
                let text = String::from_utf8_lossy(&response);
                assert!(
                    text.contains("\"ok\":false"),
                    "oversized requests must be answered with an error, got: {text}"
                );
            }
        }
        drop(stream);

        // The daemon survived and still serves well-formed requests.
        let status = daemon.request(&Request::Status).await;
        assert_eq!(status.buffered, Some(0));
        let _ = daemon.stop().await;
    };
    tokio::time::timeout(Duration::from_secs(60), test)
        .await
        .expect("test timed out: daemon hung on an oversized request");
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
        stderr.contains("pushed 1 path ("),
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

#[tokio::test]
async fn substituter_serves_paths_pushed_by_daemon_drains() {
    // The serve-level wiring (what `hestia serve` assembles): the daemon
    // and the substituter share a ManifestStore and an AccessLog. Paths
    // pushed through the hook socket become substitutable after a drain,
    // without restarting anything; narinfo hits show up in the daemon's
    // access log so the next drain pins them.
    let test = async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        let fixture = store.add_fixture("serve-substituter", 131);

        let fake = FakeGha::start().await;
        let http = reqwest::Client::new();
        let socket = store_socket_path(&store);
        let daemon = RunningDaemon::start(
            socket,
            None,
            pipeline_context(&fake, &http, store.database()),
        )
        .await;

        // Mount the substituter on the daemon's shared state, exactly like
        // serve::run does.
        let substituter = Substituter::new(
            store.database().store_dir().clone(),
            daemon.manifest_store.clone(),
            daemon.access_log.clone(),
            fake.twirp(&http),
            http.clone(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, substituter.into_router())
                .await
                .unwrap();
        });

        let hash = path_hash_of(&fixture);
        let narinfo_url = format!("{base_url}/{hash}.narinfo");

        // Nothing pushed yet: miss.
        let response = http.get(&narinfo_url).send().await.unwrap();
        assert_eq!(response.status(), 404);

        // Hook + drain through the socket.
        daemon.add(&[&fixture]).await;
        let response = daemon.request(&Request::Drain).await;
        assert_eq!(response.stats.expect("drain stats").pushed, 1);

        // The drain refreshed the shared manifest: the path is servable now.
        let response = http.get(&narinfo_url).send().await.unwrap();
        assert_eq!(
            response.status(),
            200,
            "path pushed by a drain must be substitutable without a restart"
        );
        let narinfo = response.text().await.unwrap();
        let nar_url = narinfo
            .lines()
            .find_map(|line| line.strip_prefix("URL: "))
            .expect("narinfo has a URL line");

        let response = http
            .get(format!("{base_url}/{nar_url}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let (expected_hash, expected_size) = store.nar_hash_oracle(&fixture).unwrap();
        let nar = response.bytes().await.unwrap();
        assert_eq!(nar.len() as u64, expected_size);
        assert_eq!(hestia::manifest::Hash32::digest(nar), expected_hash);

        // The narinfo hit landed in the daemon's access log...
        assert!(daemon.access_log.snapshot().contains(&hash));

        // ...so the final drain pins it in the root even though nothing new
        // was pushed.
        daemon.stop().await.expect("final drain failed");
        server.abort();
        let (_, manifest) = committed_manifest(&fake, &http).await.unwrap();
        assert!(manifest.roots[TEST_ROOT_KEY].paths.contains(&hash));
    };
    tokio::time::timeout(Duration::from_secs(120), test)
        .await
        .expect("test timed out: deadlock or hung server");
}

#[tokio::test]
async fn serve_becomes_ready_while_cache_api_stalls() {
    // A stalled GHA cache API at startup must not block the daemon's
    // listeners: the action polls /nix-cache-info for only 30s and then
    // hard-fails the job, even though cache unavailability is designed to
    // degrade to "substitute nothing". The manifest load must therefore
    // happen concurrently with (not before) binding the listeners.
    let test = async {
        let Some(store) = ScratchStore::create() else {
            return;
        };
        // The store database only exists once something was added.
        store.add_fixture("stall-ready", 151);

        // A cache API that accepts connections but never responds.
        let stall = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stall_addr = stall.local_addr().unwrap();
        let stall_task = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let Ok((stream, _)) = stall.accept().await else {
                    return;
                };
                held.push(stream);
            }
        });

        // A free port for the substituter (bind-and-release).
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen = probe.local_addr().unwrap().to_string();
        drop(probe);

        let socket = store_socket_path(&store);
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_hestia"))
            .args(["serve", "--listen", &listen, "--socket"])
            .arg(&socket)
            .arg("--db-path")
            .arg(store.db_path())
            .env("ACTIONS_RESULTS_URL", format!("http://{stall_addr}"))
            .env("ACTIONS_RUNTIME_TOKEN", "test-token")
            .kill_on_drop(true)
            .spawn()
            .expect("spawning hestia serve failed");

        // The substituter must answer readiness probes well within the
        // action's 30s budget despite the stalled manifest load.
        let http = reqwest::Client::new();
        let url = format!("http://{listen}/nix-cache-info");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut ready = false;
        while std::time::Instant::now() < deadline {
            if let Ok(response) = http.get(&url).send().await
                && response.status() == 200
            {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            ready,
            "substituter must become ready while the cache API stalls"
        );

        child.kill().await.expect("killing hestia serve failed");
        stall_task.abort();
    };
    tokio::time::timeout(Duration::from_secs(60), test)
        .await
        .expect("test timed out");
}

/// Socket path inside the scratch store's tempdir (cleaned up with it).
fn store_socket_path(store: &ScratchStore) -> PathBuf {
    store.db_path().parent().unwrap().join("hestia-hook.sock")
}
