//! Integration tests for the `hestia hook` binary.
//!
//! These spawn the real binary (not library calls) because the property
//! under test is the process exit code: a post-build-hook that exits
//! non-zero fails the nix build, so `hestia hook` must exit 0 no matter
//! what goes wrong.

use std::process::Stdio;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;

use hestia::protocol::{Request, Response, encode_line};

/// Path to the compiled hestia binary (provided by cargo for integration tests).
const HESTIA_BIN: &str = env!("CARGO_BIN_EXE_hestia");

#[tokio::test]
async fn hook_exits_zero_when_daemon_is_unreachable() {
    let output = Command::new(HESTIA_BIN)
        .args([
            "hook",
            "--socket",
            "/nonexistent/hestia/hook.sock",
            "/nix/store/00000000000000000000000000000000-some-path",
        ])
        .env_remove("OUT_PATHS")
        .output()
        .await
        .expect("failed to spawn hestia binary");

    assert!(
        output.status.success(),
        "hook must exit 0 even when the daemon is unreachable, got {:?}",
        output.status
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to reach daemon"),
        "error must be reported on stderr, got: {stderr}"
    );
}

#[tokio::test]
async fn hook_exits_zero_with_empty_out_paths() {
    let output = Command::new(HESTIA_BIN)
        .args(["hook", "--socket", "/nonexistent/hestia/hook.sock"])
        .env("OUT_PATHS", "")
        .output()
        .await
        .expect("failed to spawn hestia binary");

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("nothing to do"), "got: {stderr}");
}

/// Accept one connection, read one request line, send `response`, and
/// return the parsed request.
async fn accept_one(listener: &UnixListener, response: &Response) -> Request {
    let (stream, _) = listener.accept().await.expect("accept failed");
    let mut stream = BufReader::new(stream);

    let mut line = String::new();
    stream
        .read_line(&mut line)
        .await
        .expect("reading request line failed");
    let request: Request = serde_json::from_str(&line).expect("request line must be valid JSON");

    stream
        .get_mut()
        .write_all(&encode_line(response).unwrap())
        .await
        .expect("writing response failed");
    request
}

#[tokio::test]
async fn hook_sends_out_paths_from_environment() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("hook.sock");
    let listener = UnixListener::bind(&socket).unwrap();

    let server = tokio::spawn({
        let response = Response::ok().with_buffered(2);
        async move { accept_one(&listener, &response).await }
    });

    let output = Command::new(HESTIA_BIN)
        .args(["hook", "--socket"])
        .arg(&socket)
        .env(
            "OUT_PATHS",
            "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo /nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-bar",
        )
        .stdout(Stdio::null())
        .output()
        .await
        .expect("failed to spawn hestia binary");

    assert!(output.status.success());
    let request = server.await.unwrap();
    assert_eq!(
        request,
        Request::Add {
            paths: vec![
                "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo".to_string(),
                "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-bar".to_string(),
            ]
        }
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("registered 2 path(s), 2 buffered"),
        "got: {stderr}"
    );
}

#[tokio::test]
async fn hook_prefers_explicit_arguments_over_out_paths() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("hook.sock");
    let listener = UnixListener::bind(&socket).unwrap();

    let server = tokio::spawn({
        let response = Response::ok().with_buffered(1);
        async move { accept_one(&listener, &response).await }
    });

    let status = Command::new(HESTIA_BIN)
        .args(["hook", "--socket"])
        .arg(&socket)
        .arg("/nix/store/cccccccccccccccccccccccccccccccc-explicit")
        .env(
            "OUT_PATHS",
            "/nix/store/dddddddddddddddddddddddddddddddd-env",
        )
        .status()
        .await
        .expect("failed to spawn hestia binary");

    assert!(status.success());
    let request = server.await.unwrap();
    assert_eq!(
        request,
        Request::Add {
            paths: vec!["/nix/store/cccccccccccccccccccccccccccccccc-explicit".to_string()]
        }
    );
}

#[tokio::test]
async fn hook_exits_zero_when_daemon_rejects_the_request() {
    // Even a daemon-side error must not fail the build.
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("hook.sock");
    let listener = UnixListener::bind(&socket).unwrap();

    let server = tokio::spawn({
        let response = Response::error("daemon is draining, try again");
        async move { accept_one(&listener, &response).await }
    });

    let output = Command::new(HESTIA_BIN)
        .args(["hook", "--socket"])
        .arg(&socket)
        .arg("/nix/store/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee-foo")
        .output()
        .await
        .expect("failed to spawn hestia binary");

    assert!(
        output.status.success(),
        "hook must exit 0 even on daemon errors"
    );
    server.await.unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("daemon is draining"), "got: {stderr}");
}

#[tokio::test]
async fn hook_exits_zero_when_daemon_hangs_up_without_responding() {
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("hook.sock");
    let listener = UnixListener::bind(&socket).unwrap();

    // Server accepts and immediately closes the connection.
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(stream);
    });

    let output = Command::new(HESTIA_BIN)
        .args(["hook", "--socket"])
        .arg(&socket)
        .arg("/nix/store/ffffffffffffffffffffffffffffffff-foo")
        .output()
        .await
        .expect("failed to spawn hestia binary");

    assert!(output.status.success());
    server.await.unwrap();
}
