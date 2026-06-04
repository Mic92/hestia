//! Local drain benchmark: pipeline stage breakdown without a CI
//! round-trip.
//!
//! Blob transfers are throttled to Azure-like characteristics (60 ms
//! latency, 48 MiB/s per stream), and fixtures are incompressible random
//! data — the compression ratio measured for a NixOS ISO closure.
//!
//!     cargo test --release --test drain_bench -- --ignored --nocapture
//!
//! Workload size via env: BENCH_MIB (default 512), BENCH_PATHS (default 64).

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use hestia::drain::stage_breakdown;
use hestia::pipeline::now_unix;

use support::common::pipeline_context;
use support::fake_gha::FakeGha;
use support::store::ScratchStore;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::test]
#[ignore = "benchmark; run explicitly with --ignored --nocapture"]
async fn drain_bench() {
    let total_mib = env_usize("BENCH_MIB", 512);
    let n_paths = env_usize("BENCH_PATHS", 64);
    let bytes_per_path = total_mib * 1024 * 1024 / n_paths;

    let Some(store) = ScratchStore::create() else {
        return;
    };

    eprintln!(
        "building workload: {n_paths} paths x {} MiB",
        bytes_per_path / (1024 * 1024)
    );
    let paths: BTreeSet<String> = (0..n_paths)
        .map(|i| {
            store
                .add_fixture_sized(&format!("bench-{i}"), i as u64 + 1, bytes_per_path)
                .to_string_lossy()
                .into_owned()
        })
        .collect();

    let fake = FakeGha::start().await;
    fake.set_blob_throttle(Duration::from_millis(60), 48 * 1024 * 1024);
    let http = reqwest::Client::new();
    let pipeline = pipeline_context(&fake, &http, store.database());

    let started = std::time::Instant::now();
    let mut stats = pipeline
        .run(paths, BTreeSet::new(), now_unix())
        .await
        .expect("drain failed");
    stats.elapsed_ms = started.elapsed().as_millis() as u64;

    eprintln!("drain stages: {}", stage_breakdown(&stats));
}
