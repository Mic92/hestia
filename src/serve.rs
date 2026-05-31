//! `hestia serve`: the per-job daemon.
//!
//! Phase 3 scope: the post-build-hook listener (unix socket) and the drain
//! lifecycle. The substituter HTTP server (`--listen`) is Phase 4; its
//! integration point — the [`AccessLog`] — already exists.
//!
//! Lifecycle:
//!
//! ```text
//! bind socket -> accept hook/drain/status requests
//!   add    -> buffer paths in memory
//!   drain  -> run the write pipeline over buffered + accessed paths
//!   status -> report buffered count
//! exit on: shutdown signal (SIGTERM/SIGINT) or idle timeout
//!   -> one final drain before returning
//! ```
//!
//! Buffered paths live in memory only (PLAN.md "Hook: keep it minimal"):
//! on ephemeral CI runners, a persistent queue would not survive the job
//! either, and lost registrations self-correct (the path is rebuilt and
//! re-registered next run).

use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::cli::ServeArgs;
use crate::gha::twirp::TwirpClient;
use crate::pathinfo::StoreDatabase;
use crate::pipeline::{self, AccessLog, MANIFEST_PREFIX, PipelineContext, now_unix};
use crate::protocol::{DrainStats, Request, Response, encode_line};
use crate::upstream::UpstreamFilter;

/// How often the idle-exit timer checks for inactivity.
const IDLE_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Shared state of a running daemon.
struct DaemonState {
    /// Store paths registered by hooks, waiting for the next drain.
    buffered: Mutex<BTreeSet<String>>,
    /// Paths served by the substituter (Phase 4 fills this).
    access_log: AccessLog,
    /// The write pipeline.
    pipeline: PipelineContext,
    /// Serializes drains: concurrent drain requests run one at a time.
    drain_lock: tokio::sync::Mutex<()>,
    /// Last time anything happened (for idle-exit).
    last_activity: Mutex<Instant>,
}

impl DaemonState {
    fn touch(&self) {
        *self.last_activity.lock().expect("activity lock poisoned") = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_activity
            .lock()
            .expect("activity lock poisoned")
            .elapsed()
    }

    fn buffered_count(&self) -> usize {
        self.buffered.lock().expect("buffer lock poisoned").len()
    }

    /// Run the pipeline over everything buffered + accessed.
    ///
    /// On failure the paths go back into the buffer so a later drain (or
    /// the final drain at shutdown) can retry them.
    async fn drain(&self) -> Result<DrainStats, pipeline::Error> {
        let _guard = self.drain_lock.lock().await;
        self.touch();

        let paths = std::mem::take(&mut *self.buffered.lock().expect("buffer lock poisoned"));
        let accessed = self.access_log.snapshot();

        match self.pipeline.run(paths.clone(), accessed, now_unix()).await {
            Ok(stats) => {
                self.touch();
                Ok(stats)
            }
            Err(err) => {
                // Paths added during the drain are kept too (extend, not replace).
                self.buffered
                    .lock()
                    .expect("buffer lock poisoned")
                    .extend(paths);
                Err(err)
            }
        }
    }

    async fn handle_request(&self, request: Request) -> Response {
        self.touch();
        match request {
            Request::Add { paths } => {
                let count = {
                    let mut buffered = self.buffered.lock().expect("buffer lock poisoned");
                    buffered.extend(paths);
                    buffered.len()
                };
                Response::ok().with_buffered(count)
            }
            Request::Status => Response::ok().with_buffered(self.buffered_count()),
            Request::Drain => match self.drain().await {
                Ok(stats) => Response::ok().with_stats(stats),
                Err(err) => Response::error(format!("drain failed: {err}")),
            },
        }
    }
}

/// A bound (but not yet running) daemon.
pub struct Daemon {
    state: Arc<DaemonState>,
    listener: UnixListener,
    idle_exit: Option<Duration>,
}

impl Daemon {
    /// Bind the hook socket and assemble the daemon.
    ///
    /// The socket's parent directory is created if missing. An existing
    /// socket file is removed first (leftover from a previous daemon that
    /// did not shut down cleanly).
    pub fn bind(
        socket: &Path,
        idle_exit: Option<Duration>,
        pipeline: PipelineContext,
        access_log: AccessLog,
    ) -> std::io::Result<Self> {
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::remove_file(socket) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        let listener = harmonia_utils_io::unix_socket::bind_unix_long(socket)?;

        Ok(Self {
            state: Arc::new(DaemonState {
                buffered: Mutex::new(BTreeSet::new()),
                access_log,
                pipeline,
                drain_lock: tokio::sync::Mutex::new(()),
                last_activity: Mutex::new(Instant::now()),
            }),
            listener,
            idle_exit,
        })
    }

    /// The daemon's access log (handed to the Phase 4 substituter).
    pub fn access_log(&self) -> AccessLog {
        self.state.access_log.clone()
    }

    /// Serve until `shutdown` resolves or the idle timeout expires, then
    /// run one final drain and return its stats.
    pub async fn run(
        self,
        shutdown: impl Future<Output = ()>,
    ) -> Result<DrainStats, pipeline::Error> {
        let Daemon {
            state,
            listener,
            idle_exit,
        } = self;

        // Accept loop: one task per connection.
        let accept_state = Arc::clone(&state);
        let accept = async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let state = Arc::clone(&accept_state);
                        tokio::spawn(async move {
                            if let Err(err) = handle_connection(&state, stream).await {
                                eprintln!("hestia serve: connection error: {err}");
                            }
                        });
                    }
                    Err(err) => {
                        eprintln!("hestia serve: accept failed: {err}");
                        // Socket is gone; nothing left to serve.
                        break;
                    }
                }
            }
        };

        // Idle-exit timer.
        let idle_state = Arc::clone(&state);
        let idle = async move {
            match idle_exit {
                None => std::future::pending::<()>().await,
                Some(timeout) => loop {
                    tokio::time::sleep(IDLE_CHECK_INTERVAL.min(timeout)).await;
                    if idle_state.idle_for() >= timeout {
                        break;
                    }
                },
            }
        };

        tokio::select! {
            () = shutdown => {
                eprintln!("hestia serve: shutdown requested, draining");
            }
            () = idle => {
                eprintln!("hestia serve: idle timeout reached, draining and exiting");
            }
            () = accept => {
                eprintln!("hestia serve: listener closed, draining and exiting");
            }
        }

        // Final drain: whatever is still buffered must be uploaded before
        // the runner disappears.
        state.drain().await
    }
}

/// Serve one client connection: JSON request lines, JSON response lines.
async fn handle_connection(state: &DaemonState, stream: UnixStream) -> std::io::Result<()> {
    let mut stream = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        let read = stream.read_line(&mut line).await?;
        if read == 0 {
            return Ok(()); // client hung up
        }
        let response = match serde_json::from_str::<Request>(&line) {
            Ok(request) => state.handle_request(request).await,
            Err(err) => Response::error(format!("malformed request: {err}")),
        };
        let encoded = encode_line(&response).map_err(std::io::Error::other)?;
        stream.get_mut().write_all(&encoded).await?;
        stream.get_mut().flush().await?;
    }
}

/// CLI entry point: assemble the pipeline from args + environment and run
/// until SIGTERM/SIGINT.
pub async fn run(args: &ServeArgs) -> ExitCode {
    // GHA cache credentials (injected by the hestia action wrapper).
    let http = reqwest::Client::new();
    let twirp = match TwirpClient::from_env(http.clone()) {
        Ok(twirp) => twirp,
        Err(err) => {
            eprintln!(
                "hestia serve: {err}\n\
                 hint: the GHA cache tokens are only visible to shell steps when the \
                 hestia action wrapper exported them (see PLAN.md, Critical Constraint 1)"
            );
            return ExitCode::FAILURE;
        }
    };

    // Store database: fail fast if unreadable; a daemon that can never
    // drain is worse than a failed step.
    let store = StoreDatabase::new(&args.db_path);
    if let Err(err) = store.ping() {
        eprintln!("hestia serve: cannot read the Nix store database: {err}");
        return ExitCode::FAILURE;
    }

    let upstream = if args.upstream_keys.is_empty() {
        UpstreamFilter::default()
    } else {
        UpstreamFilter::new(args.upstream_keys.iter().cloned())
    };

    let branch = args
        .branch
        .clone()
        .or_else(|| std::env::var("GITHUB_REF_NAME").ok())
        .filter(|branch| !branch.is_empty())
        .unwrap_or_else(|| "local".to_string());
    let system = args.system.clone().unwrap_or_else(pipeline::current_system);

    let pipeline = PipelineContext {
        twirp,
        http,
        store,
        upstream,
        root_key: pipeline::root_key(&branch, &system),
        manifest_prefix: MANIFEST_PREFIX.to_string(),
    };

    let idle_exit = args.idle_exit.map(Duration::from_secs);
    let daemon = match Daemon::bind(&args.socket, idle_exit, pipeline, AccessLog::new()) {
        Ok(daemon) => daemon,
        Err(err) => {
            eprintln!(
                "hestia serve: cannot bind hook socket {}: {err}",
                args.socket.display()
            );
            return ExitCode::FAILURE;
        }
    };

    eprintln!(
        "hestia serve: listening on {} (root key: {}-{})",
        args.socket.display(),
        branch,
        system
    );

    // SIGTERM (runner shutdown) and SIGINT (^C) both trigger drain + exit.
    let shutdown = async {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing SIGTERM handler failed");
        tokio::select! {
            _ = sigterm.recv() => {},
            result = tokio::signal::ctrl_c() => {
                result.expect("installing SIGINT handler failed");
            },
        }
    };

    match daemon.run(shutdown).await {
        Ok(stats) => {
            eprintln!(
                "hestia serve: final drain pushed {} path(s), {} pack(s), {} bytes \
                 (manifest version {})",
                stats.pushed, stats.packs_uploaded, stats.bytes_uploaded, stats.manifest_version
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("hestia serve: final drain failed: {err}");
            ExitCode::FAILURE
        }
    }
}
