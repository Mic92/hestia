//! `hestia hook`: the post-build-hook client.
//!
//! Nix runs this after every successful build with the built outputs in
//! `$OUT_PATHS` (space-separated). The hook forwards them to the daemon
//! over the unix socket and exits.
//!
//! **This command must always exit 0.** A failing post-build-hook fails
//! the nix build that triggered it. Losing a path registration is harmless
//! (the path gets rebuilt and re-registered on a future run); failing a
//! build over it is not. All errors go to stderr only.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use crate::cli::HookArgs;
use crate::protocol::{self, Request};

/// Environment variable Nix sets for post-build-hooks.
pub const OUT_PATHS_ENV: &str = "OUT_PATHS";

/// How long the hook waits for the daemon before giving up.
///
/// The daemon only buffers paths in memory, so the round-trip is
/// milliseconds; the timeout exists so a wedged daemon can never stall
/// nix builds indefinitely.
pub const HOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// Collect the store paths to register: explicit CLI arguments win,
/// otherwise fall back to `$OUT_PATHS` (space-separated, as Nix sets it).
pub fn collect_paths(arg_paths: &[PathBuf], out_paths_env: Option<&str>) -> Vec<String> {
    if !arg_paths.is_empty() {
        return arg_paths
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
    }
    out_paths_env
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

/// Send paths to the daemon. Returns the number of paths the daemon has
/// buffered after the add.
pub async fn send_paths(args: &HookArgs, paths: Vec<String>) -> Result<usize, protocol::Error> {
    let request = Request::Add { paths };
    let response = protocol::roundtrip(&args.socket, &request).await?;
    Ok(response.buffered.unwrap_or(0))
}

pub async fn run(args: &HookArgs) -> ExitCode {
    let out_paths = std::env::var(OUT_PATHS_ENV).ok();
    let paths = collect_paths(&args.paths, out_paths.as_deref());

    if paths.is_empty() {
        eprintln!("hestia hook: no paths given and ${OUT_PATHS_ENV} is empty; nothing to do");
        return ExitCode::SUCCESS;
    }

    let count = paths.len();
    match tokio::time::timeout(HOOK_TIMEOUT, send_paths(args, paths)).await {
        Ok(Ok(buffered)) => {
            eprintln!("hestia hook: registered {count} path(s), {buffered} buffered for upload");
        }
        Ok(Err(err)) => {
            eprintln!(
                "hestia hook: failed to reach daemon at {}: {err} \
                 (build continues; paths will be re-pushed on a future run)",
                args.socket.display()
            );
        }
        Err(_) => {
            eprintln!(
                "hestia hook: daemon at {} did not respond within {}s \
                 (build continues; paths will be re-pushed on a future run)",
                args.socket.display(),
                HOOK_TIMEOUT.as_secs()
            );
        }
    }

    // Never fail the build (see module docs).
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_arguments_win_over_out_paths() {
        let args = vec![PathBuf::from("/nix/store/aaa-explicit")];
        let paths = collect_paths(&args, Some("/nix/store/bbb-from-env"));
        assert_eq!(paths, vec!["/nix/store/aaa-explicit".to_string()]);
    }

    #[test]
    fn out_paths_is_split_on_whitespace() {
        // Nix separates multiple outputs with single spaces; be liberal and
        // accept any whitespace (newlines, multiple spaces).
        let paths = collect_paths(
            &[],
            Some("/nix/store/aaa-foo /nix/store/bbb-bar\n/nix/store/ccc-baz"),
        );
        assert_eq!(
            paths,
            vec![
                "/nix/store/aaa-foo".to_string(),
                "/nix/store/bbb-bar".to_string(),
                "/nix/store/ccc-baz".to_string(),
            ]
        );
    }

    #[test]
    fn empty_inputs_produce_no_paths() {
        assert!(collect_paths(&[], None).is_empty());
        assert!(collect_paths(&[], Some("")).is_empty());
        assert!(collect_paths(&[], Some("   \n  ")).is_empty());
    }

    #[tokio::test]
    async fn run_exits_success_when_daemon_is_unreachable() {
        // The core guarantee: a missing daemon must never fail the build.
        // ExitCode has no PartialEq, so compare through Debug formatting.
        let args = HookArgs {
            socket: PathBuf::from("/nonexistent/hestia/hook.sock"),
            paths: vec![PathBuf::from("/nix/store/aaa-foo")],
        };
        let code = run(&args).await;
        assert_eq!(format!("{code:?}"), format!("{:?}", ExitCode::SUCCESS));
    }

    #[tokio::test]
    async fn run_exits_success_with_nothing_to_do() {
        let args = HookArgs {
            socket: PathBuf::from("/nonexistent/hestia/hook.sock"),
            paths: vec![],
        };
        // OUT_PATHS is not set in the test environment (and even if it were,
        // the daemon is unreachable) -- either way the exit code is success.
        let code = run(&args).await;
        assert_eq!(format!("{code:?}"), format!("{:?}", ExitCode::SUCCESS));
    }
}
