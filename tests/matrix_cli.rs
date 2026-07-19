//! Integration tests for `hestia matrix`: the whole flow from a (fake)
//! nix-eval-jobs invocation to the emitted matrix and the drv registration
//! on the daemon socket.

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;

use hestia::protocol::{DrainStats, Request, Response, encode_line};

mod support;
use support::store::ScratchStore;

const HESTIA_BIN: &str = env!("CARGO_BIN_EXE_hestia");

/// Write a fake nix-eval-jobs that dumps its arguments and prints fixture
/// JSON lines.
fn write_fake_nix_eval_jobs(dir: &Path, json_lines: &str) -> std::path::PathBuf {
    let script = dir.join("fake-nix-eval-jobs");
    let output = dir.join("nix-eval-jobs.args");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\ncat <<'EOF'\n{json_lines}\nEOF\n",
            output.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    script
}

/// Serve the daemon protocol for one connection per request: Add responds
/// ok, Drain responds with `manifest_version`. Returns the received
/// requests.
async fn fake_daemon(listener: UnixListener, manifest_version: u64) -> Vec<Request> {
    let mut requests = Vec::new();
    // hestia matrix opens one connection per roundtrip: Add, then Drain.
    for _ in 0..2 {
        let (stream, _) = listener.accept().await.expect("accept failed");
        let mut stream = BufReader::new(stream);
        let mut line = String::new();
        stream.read_line(&mut line).await.unwrap();
        let request: Request = serde_json::from_str(&line).unwrap();
        let response = match &request {
            Request::Add { paths } => Response::ok().with_buffered(paths.len()),
            Request::Drain => Response::ok().with_stats(DrainStats {
                manifest_version,
                ..DrainStats::default()
            }),
            Request::Status => Response::ok().with_buffered(0),
        };
        requests.push(request);
        stream
            .get_mut()
            .write_all(&encode_line(&response).unwrap())
            .await
            .unwrap();
    }
    requests
}

const FIXTURE: &str = concat!(
    r#"{"attr":"x86_64-linux.a","drvPath":"/nix/store/aaa-a.drv","system":"x86_64-linux","isCached":false}"#,
    "\n",
    r#"{"attr":"x86_64-linux.b","drvPath":"/nix/store/bbb-b.drv","system":"x86_64-linux","isCached":true}"#,
);

#[tokio::test]
async fn matrix_registers_drvs_and_emits_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let script = write_fake_nix_eval_jobs(dir.path(), FIXTURE);
    let socket = dir.path().join("hook.sock");
    let github_output = dir.path().join("github-output");
    let daemon = tokio::spawn(fake_daemon(UnixListener::bind(&socket).unwrap(), 42));

    let output = Command::new(HESTIA_BIN)
        .arg("matrix")
        .arg("--nix-eval-jobs")
        .arg(format!("{} --workers 4", script.display()))
        .arg("--socket")
        .arg(&socket)
        .args(["--flake", ".#hydraJobs"])
        .env("GITHUB_OUTPUT", &github_output)
        .stdin(Stdio::null())
        .output()
        .await
        .expect("failed to spawn hestia binary");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "stderr: {stderr}");

    // nix-eval-jobs got the extra args from the command string, the flake,
    // and the standard flags.
    let args = std::fs::read_to_string(dir.path().join("nix-eval-jobs.args")).unwrap();
    let args: Vec<&str> = args.lines().collect();
    assert_eq!(
        args,
        [
            "--workers",
            "4",
            "--flake",
            ".#hydraJobs",
            "--check-cache-status",
            "--meta",
            "--force-recurse"
        ]
    );

    // Both drvs (cached one included) were registered, then drained.
    let requests = daemon.await.unwrap();
    assert_eq!(
        requests[0],
        Request::Add {
            paths: vec![
                "/nix/store/aaa-a.drv".to_string(),
                "/nix/store/bbb-b.drv".to_string(),
            ]
        }
    );
    assert_eq!(requests[1], Request::Drain);

    // Outputs land in $GITHUB_OUTPUT: only the uncached job in the matrix,
    // and the drain's manifest version.
    let outputs = std::fs::read_to_string(&github_output).unwrap();
    assert!(outputs.contains("any-jobs=true"), "{outputs}");
    assert!(outputs.contains("manifest-version=42"), "{outputs}");
    let matrix_line = outputs
        .lines()
        .find_map(|line| line.strip_prefix("matrix="))
        .expect("matrix output present");
    let matrix: serde_json::Value = serde_json::from_str(matrix_line).unwrap();
    let include = matrix["include"].as_array().unwrap();
    assert_eq!(include.len(), 1);
    assert_eq!(include[0]["attr"], "x86_64-linux.a");
    assert_eq!(include[0]["drvPath"], "/nix/store/aaa-a.drv");
    assert_eq!(include[0]["os"][0], "ubuntu-24.04");
    assert_eq!(include[0]["installables"], "/nix/store/aaa-a.drv^*");
}

#[tokio::test]
async fn missing_daemon_still_emits_the_matrix() {
    let dir = tempfile::tempdir().unwrap();
    let script = write_fake_nix_eval_jobs(dir.path(), FIXTURE);

    let output = Command::new(HESTIA_BIN)
        .arg("matrix")
        .arg("--nix-eval-jobs")
        .arg(&script)
        .args(["--socket", "/nonexistent/hestia/hook.sock"])
        .env_remove("GITHUB_OUTPUT")
        .output()
        .await
        .expect("failed to spawn hestia binary");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "stderr: {stderr}");
    assert!(stderr.contains("cannot register"), "{stderr}");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("manifest-version=0"), "{stdout}");
    assert!(stdout.contains("any-jobs=true"), "{stdout}");
    assert!(
        stdout.contains(r#""drvPath":"/nix/store/aaa-a.drv""#),
        "{stdout}"
    );
}

/// End-to-end against the real nix-eval-jobs on a tiny dependency-free
/// flake: catches drift in the nix-eval-jobs CLI/JSON interface that the
/// fake-script tests above cannot see.
#[tokio::test]
async fn real_nix_eval_jobs_produces_the_matrix() {
    // No --version flag; --help is the cheapest liveness probe.
    let eval_jobs_works = std::process::Command::new("nix-eval-jobs")
        .arg("--help")
        .output()
        .is_ok_and(|output| output.status.success());
    if !eval_jobs_works {
        eprintln!("skipping: nix-eval-jobs not available");
        return;
    }
    let Some(store) = ScratchStore::create() else {
        return;
    };

    // Checked-in subflake with dependency-free checks (own directory: a
    // `path:` flake copies its whole directory into the store).
    let flake_dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/matrix-checks");

    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("hook.sock");
    let daemon = tokio::spawn(fake_daemon(UnixListener::bind(&socket).unwrap(), 3));

    let output = Command::new(HESTIA_BIN)
        .arg("matrix")
        .args(["--nix-eval-jobs", "nix-eval-jobs"])
        .arg("--flake")
        .arg(format!("path:{}#checks", flake_dir.display()))
        .arg("--socket")
        .arg(&socket)
        // Hermetic: scratch store, no substituters (so --check-cache-status
        // stays offline), flakes enabled regardless of the host config.
        .env("NIX_REMOTE", store.store_uri())
        .env(
            "NIX_CONFIG",
            "experimental-features = nix-command flakes\nsubstituters =\n",
        )
        .env_remove("GITHUB_OUTPUT")
        .output()
        .await
        .expect("failed to spawn hestia binary");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "stderr: {stderr}\nstdout: {stdout}"
    );

    // Three distinct drvs registered (the alias collapses).
    let requests = daemon.await.unwrap();
    let Request::Add { paths } = &requests[0] else {
        panic!("expected Add, got {:?}", requests[0]);
    };
    assert_eq!(paths.len(), 3, "paths: {paths:?}");
    assert!(paths.iter().all(|path| path.ends_with(".drv")));

    // Two matrix rows: the plain check and the group.
    let matrix_line = stdout
        .lines()
        .find_map(|line| line.strip_prefix("matrix="))
        .expect("matrix output present");
    let matrix: serde_json::Value = serde_json::from_str(matrix_line).unwrap();
    let include = matrix["include"].as_array().unwrap();
    let names: Vec<&str> = include
        .iter()
        .map(|row| row["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["x86_64-linux.fast", "small"], "matrix: {matrix}");
    let group = &include[1];
    assert_eq!(group["system"], "x86_64-linux");
    assert_eq!(
        group["installables"].as_str().unwrap().split(' ').count(),
        2,
        "group builds both members: {group}"
    );
    assert!(stdout.contains("manifest-version=3"), "{stdout}");
}

#[tokio::test]
async fn eval_errors_fail_the_command() {
    let dir = tempfile::tempdir().unwrap();
    let script = write_fake_nix_eval_jobs(
        dir.path(),
        r#"{"attr":"x86_64-linux.broken","error":"assertion failed"}"#,
    );

    let output = Command::new(HESTIA_BIN)
        .arg("matrix")
        .arg("--nix-eval-jobs")
        .arg(&script)
        .args(["--socket", "/nonexistent/hestia/hook.sock"])
        .output()
        .await
        .expect("failed to spawn hestia binary");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("x86_64-linux.broken"), "{stderr}");
    assert!(stderr.contains("assertion failed"), "{stderr}");
}
