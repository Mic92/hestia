//! One CI run against each store backend, over the in-process fakes with
//! an injected round-trip time: drain, a reader's cold load, and the
//! narinfo + NAR burst of a `nix build`. Prints backend requests per phase
//! after the timings.
//!
//!   cargo bench --bench backend
//!   HESTIA_BENCH_RTT_MS=30 HESTIA_BENCH_PATHS=200 cargo bench --bench backend

#[path = "../tests/support/mod.rs"]
mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use divan::Bencher;
use futures_util::{StreamExt, stream};
use hestia::backend::Backend;
use hestia::pipeline::AccessLog;
use hestia::store::Snapshot;
use hestia::substituter::{ManifestStore, Substituter};
use hestia::trust::Trust;
use support::fake_gha::FakeGha;
use support::fake_oci::FakeOci;
use support::fake_s3::FakeS3;
use support::net::Net;
use support::sim::{SimCache, SimPath};

const BACKENDS: [&str; 3] = ["gha", "oci", "s3"];
const ROOT: &str = "main";
/// Like nix's default `http-connections`.
const PARALLEL: usize = 25;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

static RTT: LazyLock<Duration> =
    LazyLock::new(|| Duration::from_millis(env("HESTIA_BENCH_RTT_MS", 20)));
static PATHS: LazyLock<Vec<SimPath>> = LazyLock::new(|| {
    (0..env("HESTIA_BENCH_PATHS", 50))
        .map(|i| SimPath::new(&format!("p{i}"), i, 64 << 10))
        .collect()
});
static RT: LazyLock<tokio::runtime::Runtime> =
    LazyLock::new(|| tokio::runtime::Runtime::new().unwrap());
static REQUESTS: Mutex<BTreeMap<(&'static str, &'static str), u64>> = Mutex::new(BTreeMap::new());

enum Fake {
    Gha(FakeGha),
    Oci(FakeOci),
    S3(FakeS3),
}

impl Fake {
    async fn start(kind: &str) -> Self {
        match kind {
            "gha" => Self::Gha(FakeGha::start().await),
            "oci" => Self::Oci(FakeOci::start().await),
            _ => Self::S3(FakeS3::start().await),
        }
    }

    fn net(&self) -> &Arc<Net> {
        match self {
            Self::Gha(f) => &f.net,
            Self::Oci(f) => &f.net,
            Self::S3(f) => &f.net,
        }
    }

    fn backend(&self, http: &reqwest::Client) -> Backend {
        match self {
            Self::Gha(f) => f.backend(http),
            Self::Oci(f) => f.backend(http),
            Self::S3(f) => f.backend(),
        }
    }
}

struct Setup {
    fake: Fake,
    sim: SimCache,
}

/// A store holding one earlier run, so drains and loads meet existing
/// heads and segments. RTT is off until setup is done.
fn setup(kind: &str, drained: bool) -> Setup {
    RT.block_on(async {
        let fake = Fake::start(kind).await;
        let sim = SimCache::with(
            fake.backend(&reqwest::Client::new()),
            hestia::pipeline::system_clock(),
        );
        let old = SimPath::new("old", 9999, 64 << 10);
        sim.push(ROOT, &[&old], &[&old]).await;
        if drained {
            let paths: Vec<&SimPath> = PATHS.iter().collect();
            sim.push(ROOT, &paths, &paths).await;
        }
        fake.net().take();
        fake.net().set_rtt(*RTT);
        Setup { fake, sim }
    })
}

fn record(phase: &'static str, kind: &'static str, net: &Net) {
    let log = net.take();
    *REQUESTS.lock().unwrap().entry((phase, kind)).or_default() = log.len() as u64;
    if std::env::var_os("HESTIA_BENCH_TRACE").is_some() {
        let mut by: BTreeMap<String, usize> = BTreeMap::new();
        for l in log {
            // Collapse digests and ids so routes group.
            let route = l
                .split('/')
                .map(|seg| {
                    if seg.len() > 30 || seg.parse::<u64>().is_ok() {
                        "*"
                    } else {
                        seg
                    }
                })
                .collect::<Vec<_>>()
                .join("/");
            *by.entry(route).or_default() += 1;
        }
        eprintln!("-- {phase} {kind}");
        for (r, n) in by {
            eprintln!("   {n:>4} {r}");
        }
    }
}

fn kind(name: &str) -> &'static str {
    BACKENDS.iter().find(|k| **k == name).unwrap()
}

#[divan::bench(args = BACKENDS, sample_count = 3, sample_size = 1)]
fn drain(bencher: Bencher, name: &str) {
    let kind = kind(name);
    bencher
        .with_inputs(|| setup(kind, false))
        .bench_local_values(|s| {
            RT.block_on(async {
                let paths: Vec<&SimPath> = PATHS.iter().collect();
                s.sim.push(ROOT, &paths, &paths).await;
            });
            record("drain", kind, s.fake.net());
        });
}

#[divan::bench(args = BACKENDS, sample_count = 3, sample_size = 1)]
fn cold_load(bencher: Bencher, name: &str) {
    let kind = kind(name);
    bencher
        .with_inputs(|| setup(kind, true))
        .bench_local_values(|s| {
            let snapshot = RT.block_on(async {
                Snapshot::load(s.sim.backend.clone(), Trust::open(), &[ROOT.into()], None)
                    .await
                    .unwrap()
            });
            assert_eq!(snapshot.path_count(), PATHS.len() + 1);
            record("cold_load", kind, s.fake.net());
        });
}

#[divan::bench(args = BACKENDS, sample_count = 3, sample_size = 1)]
fn fetch_burst(bencher: Bencher, name: &str) {
    let kind = kind(name);
    bencher
        .with_inputs(|| {
            let s = setup(kind, true);
            let base = RT.block_on(async {
                let snapshot =
                    Snapshot::load(s.sim.backend.clone(), Trust::open(), &[ROOT.into()], None)
                        .await
                        .unwrap();
                let store = ManifestStore::new();
                store.set_snapshot(Arc::new(snapshot));
                let router = Substituter::new(
                    Default::default(),
                    store,
                    AccessLog::new(),
                    s.sim.backend.clone(),
                )
                .into_router();
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let base = format!("http://{}", listener.local_addr().unwrap());
                tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
                base
            });
            s.fake.net().take();
            (s, base, reqwest::Client::new())
        })
        .bench_local_values(|(s, base, http)| {
            RT.block_on(async {
                let get = |url: String| {
                    let http = &http;
                    async move {
                        let r = http.get(&url).send().await.unwrap();
                        assert_eq!(r.status(), 200, "{url}");
                        r.bytes().await.unwrap()
                    }
                };
                let narinfos: Vec<String> = stream::iter(PATHS.iter())
                    .map(|p| get(format!("{base}/{}.narinfo", p.path_hash())))
                    .buffered(PARALLEL)
                    .map(|b| String::from_utf8(b.to_vec()).unwrap())
                    .collect()
                    .await;
                stream::iter(narinfos)
                    .map(|n| {
                        let url = n.lines().find_map(|l| l.strip_prefix("URL: ")).unwrap();
                        get(format!("{base}/{url}"))
                    })
                    .buffer_unordered(PARALLEL)
                    .for_each(|_| async {})
                    .await;
            });
            record("fetch_burst", kind, s.fake.net());
        });
}

fn main() {
    eprintln!(
        "rtt {} ms, {} paths of 64 KiB, {} parallel fetches",
        RTT.as_millis(),
        PATHS.len(),
        PARALLEL
    );
    divan::main();
    let requests = REQUESTS.lock().unwrap();
    eprintln!(
        "\nbackend requests   {:>6} {:>6} {:>6}",
        BACKENDS[0], BACKENDS[1], BACKENDS[2]
    );
    for phase in ["drain", "cold_load", "fetch_burst"] {
        eprint!("{phase:<18}");
        for kind in BACKENDS {
            eprint!(" {:>6}", requests.get(&(phase, kind)).copied().unwrap_or(0));
        }
        eprintln!();
    }
}
