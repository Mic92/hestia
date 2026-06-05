//! `hestia serve`: the per-job daemon.
//!
//! Two servers share one process:
//!
//! * the post-build-hook listener (unix socket): buffers locally-built
//!   paths and uploads them on drain;
//! * the substituter (HTTP): serves previously cached paths back to Nix.
//!
//! They are coupled through two pieces of shared state: the [`AccessLog`]
//! (substituter records narinfo hits, drains turn them into GC roots) and
//! the [`ManifestStore`] (drains publish newly pushed paths, the
//! substituter serves them without a restart).
//!
//! Lifecycle:
//!
//! ```text
//! bind hook socket + substituter listener (manifest loads concurrently)
//!   add     -> buffer paths in memory
//!   drain   -> run the write pipeline over buffered + accessed paths
//!              -> refresh the served manifest
//!   status  -> report buffered count
//!   narinfo -> record access
//!   nar     -> serve chunks fetched from packs
//! exit on: shutdown signal (SIGTERM/SIGINT) or idle timeout
//!   -> one final drain before returning
//! ```
//!
//! Buffered paths live in memory only: on ephemeral CI runners, a
//! persistent queue would not survive the job either, and lost
//! registrations self-correct (the path is rebuilt and re-registered next
//! run).

use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::cli::ServeArgs;
use crate::gha::twirp::TwirpClient;
use crate::pathinfo::StoreDatabase;
use crate::pipeline::{self, AccessLog, MANIFEST_PREFIX, PipelineContext, now_unix};
use crate::protocol::{DrainStats, Request, Response, encode_line};
use crate::substituter::{ManifestStore, Substituter};
use crate::upstream::UpstreamFilter;

/// How often the idle-exit timer checks for inactivity.
const IDLE_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Upper bound for one request line on the hook socket.
///
/// The largest legitimate request is an Add carrying one build's
/// `$OUT_PATHS`; even thousands of paths fit in well under a megabyte.
/// The cap exists so a misbehaving client (or something that is not a
/// hestia client at all) connecting to the socket cannot make the daemon
/// buffer an unbounded line in memory.
const MAX_REQUEST_BYTES: u64 = 16 * 1024 * 1024;

/// Shared state of a running daemon.
struct DaemonState {
    /// Store paths registered by hooks, waiting for the next drain.
    buffered: Mutex<BTreeSet<String>>,
    /// Paths served by the substituter (narinfo hits).
    access_log: AccessLog,
    /// The write pipeline.
    pipeline: PipelineContext,
    /// Serializes drains: concurrent drain requests run one at a time.
    drain_lock: tokio::sync::Mutex<()>,
    /// Last time anything happened (for idle-exit).
    last_activity: Arc<Mutex<Instant>>,
    /// Number of operations currently in progress (drains, substituter
    /// requests). Idle-exit only measures time since the *start* of work,
    /// so without this count any operation longer than the idle timeout
    /// (a long drain, a large NAR fetch) would trip the timer mid-flight
    /// and get severed by the shutdown.
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    /// Set once shutdown begins: Add requests are rejected from then on
    /// (an accepted-and-ACKed path that misses the final drain would be
    /// silently dropped at process exit).
    shutting_down: std::sync::atomic::AtomicBool,
    /// Number of connection tasks currently alive. The shutdown path
    /// waits for this to reach zero so a response in flight (e.g. the
    /// drain client's stats) is flushed before the process exits.
    connections: Arc<std::sync::atomic::AtomicUsize>,
}

/// Keeps the daemon alive while held: counts as in-flight work for the
/// idle-exit timer and restarts the idle clock when dropped.
pub struct WorkGuard {
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    last_activity: Arc<Mutex<Instant>>,
}

impl Drop for WorkGuard {
    fn drop(&mut self) {
        // Touch on completion: the idle window starts after the work
        // finished, not when it began.
        *self.last_activity.lock().expect("activity lock poisoned") = Instant::now();
        self.in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl DaemonState {
    fn touch(&self) {
        *self.last_activity.lock().expect("activity lock poisoned") = Instant::now();
    }

    fn begin_work(&self) -> WorkGuard {
        self.touch();
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        WorkGuard {
            in_flight: Arc::clone(&self.in_flight),
            last_activity: Arc::clone(&self.last_activity),
        }
    }

    fn has_in_flight(&self) -> bool {
        self.in_flight.load(std::sync::atomic::Ordering::SeqCst) > 0
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
        let _work = self.begin_work();
        let _guard = self.drain_lock.lock().await;

        let paths = std::mem::take(&mut *self.buffered.lock().expect("buffer lock poisoned"));
        let accessed = self.access_log.snapshot();

        let started = Instant::now();
        match self.pipeline.run(paths.clone(), accessed, now_unix()).await {
            Ok(mut stats) => {
                stats.elapsed_ms = started.elapsed().as_millis() as u64;
                if stats.pushed > 0 {
                    eprintln!(
                        "hestia serve: drain stages: {}",
                        crate::drain::stage_breakdown(&stats)
                    );
                }
                self.touch();
                // The pipeline publishes the committed manifest into the
                // shared ManifestStore itself; reloading from the cache here
                // could return a stale version (lookups are eventually
                // consistent).
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
                if self.shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
                    return Response::error(
                        "daemon is shutting down; path not registered".to_string(),
                    );
                }
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
    /// socket file is removed first — but only after probing it: if a
    /// daemon still answers on it, binding would silently sever that
    /// daemon from all of its clients, so the bind is refused instead.
    pub fn bind(
        socket: &Path,
        idle_exit: Option<Duration>,
        mut pipeline: PipelineContext,
        access_log: AccessLog,
        manifest_store: ManifestStore,
    ) -> std::io::Result<Self> {
        // Committed manifests go straight into the substituter's store:
        // re-loading from the cache after a drain could return a stale
        // version (lookups are eventually consistent) and make just-pushed
        // paths unsubstitutable.
        pipeline.publish = Some(manifest_store);

        if let Some(parent) = socket.parent() {
            // The default path lives under world-writable /tmp and the
            // line protocol has no authentication: create the directory
            // 0700 so other local users cannot connect, and refuse a
            // pre-existing directory owned by someone else (its owner
            // could unlink our socket and bind their own in its place).
            use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _};
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true).mode(0o700);
            builder.create(parent)?;
            let metadata = std::fs::metadata(parent)?;
            let uid = unsafe { libc::getuid() };
            if metadata.uid() != uid {
                return Err(std::io::Error::other(format!(
                    "socket directory {} is owned by uid {} (we are uid {uid}); \
                     a foreign owner could replace the socket underneath us",
                    parent.display(),
                    metadata.uid(),
                )));
            }
        }
        if std::os::unix::net::UnixStream::connect(socket).is_ok() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!(
                    "another daemon is already serving {}; refusing to take over its socket",
                    socket.display()
                ),
            ));
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
                last_activity: Arc::new(Mutex::new(Instant::now())),
                in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                shutting_down: std::sync::atomic::AtomicBool::new(false),
                connections: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }),
            listener,
            idle_exit,
        })
    }

    /// The daemon's access log (shared with the substituter).
    pub fn access_log(&self) -> AccessLog {
        self.state.access_log.clone()
    }

    /// An activity callback for the substituter: requests served over HTTP
    /// reset the idle-exit timer just like hook traffic does (a Nix that is
    /// actively substituting must not be cut off).
    pub fn activity_hook(&self) -> crate::substituter::ActivityHook {
        let state = Arc::clone(&self.state);
        Arc::new(move || Box::new(state.begin_work()))
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

        // Closing this channel tells connection tasks to stop at their
        // next read instead of waiting for the client to hang up.
        let (conn_shutdown_tx, conn_shutdown_rx) = tokio::sync::watch::channel(false);

        // Accept loop: one task per connection.
        let accept_state = Arc::clone(&state);
        let accept = async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let state = Arc::clone(&accept_state);
                        let mut shutdown_rx = conn_shutdown_rx.clone();
                        state
                            .connections
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        tokio::spawn(async move {
                            if let Err(err) =
                                handle_connection(&state, stream, &mut shutdown_rx).await
                            {
                                eprintln!("hestia serve: connection error: {err}");
                            }
                            state
                                .connections
                                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        });
                    }
                    Err(err) if is_transient_accept_error(&err) => {
                        // The short sleep lets fds free up instead of
                        // spinning.
                        eprintln!("hestia serve: accept failed (transient), retrying: {err}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
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
                    // In-flight work (a drain, an active NAR download)
                    // counts as activity: exiting now would sever it.
                    if !idle_state.has_in_flight() && idle_state.idle_for() >= timeout {
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

        // No new registrations from here on: an Add accepted after the
        // final drain snapshots the buffer would be ACKed and then
        // silently dropped at process exit.
        state
            .shutting_down
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Final drain: whatever is still buffered must be uploaded before
        // the runner disappears. Adds that raced in before the flag
        // landed are caught by re-draining until the buffer stays empty.
        let mut stats = state.drain().await?;
        while state.buffered_count() > 0 {
            let more = state.drain().await?;
            stats.paths_received += more.paths_received;
            stats.pushed += more.pushed;
            stats.new_chunks += more.new_chunks;
            stats.packs_uploaded += more.packs_uploaded;
            stats.bytes_uploaded += more.bytes_uploaded;
            if more.manifest_version > 0 {
                stats.manifest_version = more.manifest_version;
            }
        }

        // Let connection tasks flush their responses (e.g. the drain
        // client's stats line) before the runtime is torn down. Tasks
        // idle at a read return promptly via the watch channel; the
        // timeout bounds a client that never reads its response.
        let _ = conn_shutdown_tx.send(true);
        let flush_deadline = Instant::now() + Duration::from_secs(5);
        while state.connections.load(std::sync::atomic::Ordering::SeqCst) > 0
            && Instant::now() < flush_deadline
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        Ok(stats)
    }
}

/// Load the newest committed manifest and publish it into the served
/// store if it is newer than the current view. A drain may have published
/// a newer manifest while the load was in flight (or the load may return a
/// stale version: lookups are eventually consistent); that version must
/// win. Recording the version makes drains start their reservations above
/// it even when cache lookups lag.
async fn load_published_manifest(
    twirp: &TwirpClient,
    http: &reqwest::Client,
    manifest_store: &ManifestStore,
) {
    let save = crate::gha::savemutable::SaveMutable::new(twirp, http, MANIFEST_PREFIX);
    match save.load().await {
        Ok(Some(entry)) => manifest_store
            .set_version_if_newer(pipeline::decode_manifest_or_empty(&entry.data), entry.index),
        Ok(None) => {}
        Err(err) => {
            eprintln!("hestia serve: cannot load the manifest, substituting nothing: {err}");
        }
    }
}

/// Accept errors that do not mean the listener is dead: a queued client
/// that disconnected before being accepted, or momentary fd/buffer
/// exhaustion. Treating these as fatal would kill caching for the rest of
/// the job over a single transient failure.
fn is_transient_accept_error(err: &std::io::Error) -> bool {
    if err.kind() == std::io::ErrorKind::ConnectionAborted {
        return true;
    }
    matches!(
        err.raw_os_error(),
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM)
    )
}

/// Serve one client connection: JSON request lines, JSON response lines.
/// Returns when the client hangs up or `shutdown` flips to true.
async fn handle_connection(
    state: &DaemonState,
    stream: UnixStream,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    let mut stream = BufReader::new(stream);
    let mut line = Vec::new();
    loop {
        line.clear();
        // Bound how much one request line may buffer: `take` makes
        // `read_until` stop at the cap as if the stream had ended there.
        // Raw bytes, not `read_line`: that validates UTF-8 over the
        // accumulated buffer and would error out before the oversize and
        // malformed-request responses below get a chance to be sent (e.g.
        // when the cap lands inside a multi-byte character).
        let mut bounded = (&mut stream).take(MAX_REQUEST_BYTES);
        let read = tokio::select! {
            read = bounded.read_until(b'\n', &mut line) => read?,
            _ = shutdown.wait_for(|down| *down) => return Ok(()),
        };
        if read == 0 {
            return Ok(()); // client hung up
        }
        if read as u64 == MAX_REQUEST_BYTES && line.last() != Some(&b'\n') {
            // The cap was hit before a newline: oversized request. Answer
            // with an error and drop the connection (the rest of the line
            // is still in flight, so there is no way to resync to the next
            // request on this stream).
            let response = Response::error(format!(
                "request exceeds {MAX_REQUEST_BYTES} bytes; rejected"
            ));
            let encoded = encode_line(&response).map_err(std::io::Error::other)?;
            stream.get_mut().write_all(&encoded).await?;
            stream.get_mut().flush().await?;
            return Ok(());
        }
        // Lossy conversion: invalid UTF-8 falls into the malformed-request
        // arm via the serde parse error instead of killing the connection.
        let line = String::from_utf8_lossy(&line);
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
    // Connect timeout only: a total request timeout would break large
    // pack uploads/downloads, but a connection that cannot even be
    // established should fail fast instead of hanging a drain or fetch.
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("building the HTTP client failed");
    let twirp = match TwirpClient::from_env(http.clone()) {
        Ok(twirp) => twirp,
        Err(err) => {
            eprintln!(
                "hestia serve: {err}\n\
                 hint: the GHA cache tokens are only visible to shell steps when the \
                 hestia action wrapper exported them"
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

    // The filter is opt-in: by default everything is cached, upstream-served
    // paths included. An empty filter skips nothing.
    let upstream = if args.upstream_cache_filter {
        UpstreamFilter::new(args.upstream_cache_key_names.iter().cloned())
    } else {
        UpstreamFilter::new(Vec::new())
    };

    let branch = args
        .branch
        .clone()
        .or_else(|| std::env::var("GITHUB_REF_NAME").ok())
        .filter(|branch| !branch.is_empty())
        .unwrap_or_else(|| "local".to_string());
    let system = args.system.clone().unwrap_or_else(pipeline::current_system);

    let store_dir = store.store_dir().clone();
    let root_key = pipeline::root_key(&branch, &system);
    let pipeline = PipelineContext {
        twirp: twirp.clone(),
        http: http.clone(),
        store,
        upstream,
        expand_closure: !args.no_closure,
        root_key: root_key.clone(),
        manifest_prefix: MANIFEST_PREFIX.to_string(),
        pack_target_size: pipeline::PACK_TARGET_SIZE,
        // Replaced by Daemon::bind with the daemon's shared ManifestStore.
        publish: None,
    };

    let manifest_store = ManifestStore::new();

    // Bind the substituter port before touching the hook socket: if the
    // port is taken (most likely by another hestia serve with default
    // flags), failing here must leave that daemon's socket alone.
    let listener = match tokio::net::TcpListener::bind(&args.listen).await {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!(
                "hestia serve: cannot bind substituter address {}: {err}",
                args.listen
            );
            return ExitCode::FAILURE;
        }
    };

    let idle_exit = args.idle_exit.map(Duration::from_secs);
    let daemon = match Daemon::bind(
        &args.socket,
        idle_exit,
        pipeline,
        AccessLog::new(),
        manifest_store.clone(),
    ) {
        Ok(daemon) => daemon,
        Err(err) => {
            eprintln!(
                "hestia serve: cannot bind hook socket {}: {err}",
                args.socket.display()
            );
            return ExitCode::FAILURE;
        }
    };

    // The substituter HTTP server shares the manifest and access log with
    // the daemon and runs until the daemon exits.
    let substituter = Substituter::new(
        store_dir,
        manifest_store.clone(),
        daemon.access_log(),
        twirp.clone(),
        http.clone(),
    )
    .with_activity_hook(daemon.activity_hook())
    .with_manifest_reload({
        let twirp = twirp.clone();
        let http = http.clone();
        let manifest_store = manifest_store.clone();
        Arc::new(move || {
            let twirp = twirp.clone();
            let http = http.clone();
            let manifest_store = manifest_store.clone();
            Box::pin(async move {
                load_published_manifest(&twirp, &http, &manifest_store).await;
            })
        })
    });
    let substituter_task = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, substituter.into_router()).await {
            eprintln!("hestia serve: substituter server failed: {err}");
        }
    });

    // Load the manifest committed by previous runs so the substituter can
    // serve those paths. Loaded concurrently: the listeners are already
    // bound, so a slow or stalled cache API cannot delay the action's
    // readiness probe (which gives up after 30s and fails the job). No
    // manifest yet (first run) or a load failure both mean "serve nothing
    // until the first drain".
    let load_task = {
        let twirp = twirp.clone();
        let http = http.clone();
        let manifest_store = manifest_store.clone();
        tokio::spawn(async move {
            load_published_manifest(&twirp, &http, &manifest_store).await;
        })
    };

    eprintln!(
        "hestia serve: hook socket {}, substituter http://{} (root key: {root_key})",
        args.socket.display(),
        args.listen,
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
        // Tokio's signal registration disables the default disposition
        // for the rest of the process, so without a live listener a
        // second SIGTERM/^C during a hung final drain would be silently
        // swallowed and only SIGKILL would work. Keep listening and
        // force-exit on the second signal.
        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => {},
                _ = tokio::signal::ctrl_c() => {},
            }
            eprintln!("hestia serve: second signal received, exiting without finishing the drain");
            std::process::exit(1);
        });
    };

    let result = daemon.run(shutdown).await;
    load_task.abort();
    substituter_task.abort();
    // Remove the socket file: a leftover socket at the fixed default path
    // makes later hook invocations connect to a dead endpoint and makes
    // the next daemon's takeover look like a crash recovery.
    let _ = std::fs::remove_file(&args.socket);
    match result {
        Ok(stats) => {
            eprintln!(
                "hestia serve: final drain: {}",
                crate::drain::summarize(&stats)
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("hestia serve: final drain failed: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_accept_errors_are_classified() {
        for errno in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
            assert!(is_transient_accept_error(
                &std::io::Error::from_raw_os_error(errno)
            ));
        }
        assert!(is_transient_accept_error(&std::io::Error::from(
            std::io::ErrorKind::ConnectionAborted
        )));
        // A listener that is actually gone must still end the loop.
        assert!(!is_transient_accept_error(&std::io::Error::from(
            std::io::ErrorKind::NotFound
        )));
        assert!(!is_transient_accept_error(
            &std::io::Error::from_raw_os_error(libc::EBADF)
        ));
    }
}
