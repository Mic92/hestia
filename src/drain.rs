//! `hestia drain`: tell the daemon to upload pending paths and commit.
//!
//! Run by the action's post-step. Unlike `hestia hook`, this command DOES
//! report failure through its exit code: a failed drain means built paths
//! were not cached, and the workflow author should see that (the step is
//! marked failed, but it does not fail the build itself — post-steps run
//! after the job's outcome is already decided).

use std::process::ExitCode;
use std::time::Duration;

use crate::cli::DrainArgs;
use crate::protocol::{self, DrainStats, Request};

/// Human-readable one-line summary of what a drain accomplished.
pub fn summarize(stats: &DrainStats) -> String {
    let mut parts = vec![format!("{} path(s) pushed", stats.pushed)];
    if stats.skipped_existing > 0 {
        parts.push(format!("{} already cached", stats.skipped_existing));
    }
    if stats.skipped_upstream > 0 {
        parts.push(format!("{} upstream-signed", stats.skipped_upstream));
    }
    if stats.skipped_invalid > 0 {
        parts.push(format!("{} invalid", stats.skipped_invalid));
    }
    if stats.failed_verification > 0 {
        parts.push(format!("{} FAILED VERIFICATION", stats.failed_verification));
    }
    let mut summary = parts.join(", ");
    if stats.packs_uploaded > 0 {
        summary.push_str(&format!(
            "; uploaded {} pack(s), {} chunk(s), {} bytes",
            stats.packs_uploaded, stats.new_chunks, stats.bytes_uploaded
        ));
    }
    if stats.manifest_version > 0 {
        summary.push_str(&format!("; manifest version m#{}", stats.manifest_version));
    } else {
        summary.push_str("; nothing to commit");
    }
    summary
}

pub async fn run(args: &DrainArgs) -> ExitCode {
    let timeout = Duration::from_secs(args.timeout);
    let result =
        tokio::time::timeout(timeout, protocol::roundtrip(&args.socket, &Request::Drain)).await;

    match result {
        Ok(Ok(response)) => {
            let stats = response.stats.unwrap_or_default();
            eprintln!("hestia drain: {}", summarize(&stats));
            ExitCode::SUCCESS
        }
        Ok(Err(err)) => {
            eprintln!(
                "hestia drain: failed to drain daemon at {}: {err}",
                args.socket.display()
            );
            ExitCode::FAILURE
        }
        Err(_) => {
            eprintln!(
                "hestia drain: daemon at {} did not finish within {}s",
                args.socket.display(),
                args.timeout
            );
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_mentions_everything_that_happened() {
        let stats = DrainStats {
            paths_received: 10,
            pushed: 4,
            skipped_existing: 3,
            skipped_upstream: 2,
            skipped_invalid: 1,
            failed_verification: 0,
            new_chunks: 123,
            packs_uploaded: 1,
            bytes_uploaded: 456789,
            manifest_version: 7,
        };
        let summary = summarize(&stats);
        assert!(summary.contains("4 path(s) pushed"), "{summary}");
        assert!(summary.contains("3 already cached"), "{summary}");
        assert!(summary.contains("2 upstream-signed"), "{summary}");
        assert!(summary.contains("1 invalid"), "{summary}");
        assert!(summary.contains("1 pack(s)"), "{summary}");
        assert!(summary.contains("m#7"), "{summary}");
        assert!(!summary.contains("FAILED VERIFICATION"), "{summary}");
    }

    #[test]
    fn empty_drain_summary_says_nothing_to_commit() {
        let summary = summarize(&DrainStats::default());
        assert!(summary.contains("0 path(s) pushed"), "{summary}");
        assert!(summary.contains("nothing to commit"), "{summary}");
    }

    #[test]
    fn verification_failures_are_loud() {
        let stats = DrainStats {
            failed_verification: 2,
            ..DrainStats::default()
        };
        assert!(summarize(&stats).contains("2 FAILED VERIFICATION"));
    }

    #[tokio::test]
    async fn unreachable_daemon_is_a_failure_exit() {
        let args = crate::cli::DrainArgs {
            socket: std::path::PathBuf::from("/nonexistent/hestia/hook.sock"),
            timeout: 1,
        };
        let code = run(&args).await;
        assert_eq!(format!("{code:?}"), format!("{:?}", ExitCode::FAILURE));
    }
}
