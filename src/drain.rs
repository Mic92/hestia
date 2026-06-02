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

/// `1 path`, `5 paths`.
fn count(n: usize, noun: &str) -> String {
    if n == 1 {
        format!("1 {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

/// `512 B`, `1.5 KiB`, `64.6 MiB`, `2.1 GiB`.
fn human_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = "B";
    for next in ["KiB", "MiB", "GiB", "TiB"] {
        value /= 1024.0;
        unit = next;
        if value < 1024.0 {
            break;
        }
    }
    format!("{value:.1} {unit}")
}

/// `0.5s`, `57.3s`.
fn seconds(ms: u64) -> String {
    format!("{:.1}s", ms as f64 / 1000.0)
}

/// Human-readable one-line summary of what a drain accomplished.
pub fn summarize(stats: &DrainStats) -> String {
    let mut parts = vec![format!("pushed {}", count(stats.pushed, "path"))];
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
        let mut inner = human_bytes(stats.bytes_uploaded);
        if stats.elapsed_ms > 0 {
            inner.push_str(&format!(
                " in {}, {}/s",
                seconds(stats.elapsed_ms),
                human_bytes(throughput(stats.bytes_uploaded, stats.elapsed_ms))
            ));
        }
        summary.push_str(&format!(" ({inner})"));
    }
    if stats.manifest_version > 0 {
        summary.push_str(&format!("; manifest m#{}", stats.manifest_version));
    } else {
        summary.push_str("; nothing to commit");
    }
    summary
}

/// Bytes per second. `ms` is clamped to 1 to avoid dividing by zero.
fn throughput(bytes: u64, ms: u64) -> u64 {
    (bytes as f64 / (ms.max(1) as f64 / 1000.0)) as u64
}

/// Per-stage timing breakdown of a drain, for the daemon log.
pub fn stage_breakdown(stats: &DrainStats) -> String {
    let mut stages = vec![
        format!("load {}", seconds(stats.load_ms)),
        format!("chunk {}", seconds(stats.chunk_ms)),
    ];
    if stats.packs_uploaded > 0 {
        stages.push(format!(
            "pack {} ({}, {})",
            seconds(stats.pack_ms),
            count(stats.new_chunks, "chunk"),
            count(stats.packs_uploaded, "pack"),
        ));
        stages.push(format!(
            "upload {} ({}/s)",
            seconds(stats.upload_ms),
            human_bytes(throughput(stats.bytes_uploaded, stats.upload_ms))
        ));
    }
    stages.push(format!("commit {}", seconds(stats.commit_ms)));
    stages.push(format!("total {}", seconds(stats.elapsed_ms)));
    stages.join(", ")
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
            bytes_uploaded: 456_789,
            manifest_version: 7,
            load_ms: 200,
            chunk_ms: 1_000,
            pack_ms: 500,
            upload_ms: 200,
            commit_ms: 100,
            elapsed_ms: 2_000,
        };
        assert_eq!(
            summarize(&stats),
            "pushed 4 paths, 3 already cached, 2 upstream-signed, 1 invalid \
             (446.1 KiB in 2.0s, 223.0 KiB/s); manifest m#7"
        );
        assert_eq!(
            stage_breakdown(&stats),
            "load 0.2s, chunk 1.0s, pack 0.5s (123 chunks, 1 pack), \
             upload 0.2s (2.2 MiB/s), commit 0.1s, total 2.0s"
        );
    }

    #[test]
    fn deduplicated_drain_has_no_upload_in_summary_or_breakdown() {
        let stats = DrainStats {
            pushed: 2,
            manifest_version: 3,
            load_ms: 100,
            chunk_ms: 400,
            commit_ms: 100,
            elapsed_ms: 600,
            ..DrainStats::default()
        };
        assert_eq!(summarize(&stats), "pushed 2 paths; manifest m#3");
        assert_eq!(
            stage_breakdown(&stats),
            "load 0.1s, chunk 0.4s, commit 0.1s, total 0.6s"
        );
    }

    #[test]
    fn singular_counts_have_no_plural_s() {
        let stats = DrainStats {
            pushed: 1,
            new_chunks: 1,
            packs_uploaded: 1,
            bytes_uploaded: 1_048_576,
            manifest_version: 1,
            chunk_ms: 300,
            pack_ms: 100,
            upload_ms: 500,
            commit_ms: 100,
            elapsed_ms: 1_000,
            ..DrainStats::default()
        };
        assert_eq!(
            summarize(&stats),
            "pushed 1 path (1.0 MiB in 1.0s, 1.0 MiB/s); manifest m#1"
        );
        assert_eq!(
            stage_breakdown(&stats),
            "load 0.0s, chunk 0.3s, pack 0.1s (1 chunk, 1 pack), upload 0.5s (2.0 MiB/s), \
             commit 0.1s, total 1.0s"
        );
    }

    #[test]
    fn empty_drain_summary_says_nothing_to_commit() {
        assert_eq!(
            summarize(&DrainStats::default()),
            "pushed 0 paths; nothing to commit"
        );
    }

    #[test]
    fn human_bytes_units() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(456_789), "446.1 KiB");
        assert_eq!(human_bytes(67_694_023), "64.6 MiB");
        assert_eq!(human_bytes(3_000_000_000), "2.8 GiB");
        assert_eq!(human_bytes(5_000_000_000_000), "4.5 TiB");
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
