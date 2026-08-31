//! Head provenance with real cosign (keyed, offline): a reader with a
//! policy ignores heads that are unsigned, signed by an untrusted key, or
//! forged `g-*` records, and GC folds only what the policy accepts.

mod support;

use std::path::Path;
use std::process::Command;

use hestia::gc::GcPolicy;
use hestia::heads::{GcRecord, Signed};
use hestia::trust::Trust;
use support::fake_gha::FakeGha;
use support::sim::{SimCache, SimPath};

const T0: u64 = 1_750_000_000;
const HOUR: u64 = 3600;

/// `None` without cosign on PATH.
fn keypair(dir: &Path, name: &str) -> Option<(String, String)> {
    let ok = Command::new("cosign")
        .args(["generate-key-pair", "--output-key-prefix", name])
        .current_dir(dir)
        .env("COSIGN_PASSWORD", "")
        .output()
        .ok()?
        .status
        .success();
    assert!(ok, "cosign generate-key-pair failed");
    let p = |ext| dir.join(format!("{name}.{ext}")).display().to_string();
    Some((p("key"), p("pub")))
}

fn signing_config(dir: &Path) -> String {
    let out = dir.join("signing.json");
    let ok = Command::new("cosign")
        .args(["signing-config", "create", "--out"])
        .arg(&out)
        .status()
        .is_ok_and(|s| s.success());
    assert!(ok, "cosign signing-config failed");
    out.display().to_string()
}

fn verify(pubkey: &str) -> String {
    format!("cosign --key {pubkey} --insecure-ignore-tlog=true")
}

fn sign(key: &str, config: &str) -> String {
    format!("--key {key} --signing-config {config}")
}

#[tokio::test]
async fn policy_rejects_unsigned_untrusted_and_forged_heads() {
    let dir = tempfile::tempdir().unwrap();
    let Some((writer_key, writer_pub)) = keypair(dir.path(), "writer") else {
        eprintln!("cosign not found, skipping");
        return;
    };
    let (gc_key, gc_pub) = keypair(dir.path(), "gc").unwrap();
    let (rogue_key, _) = keypair(dir.path(), "rogue").unwrap();
    let config = signing_config(dir.path());
    // SAFETY: single value, set before any thread reads it.
    unsafe { std::env::set_var("COSIGN_PASSWORD", "") };
    let rows = format!(
        "main {}\nfeature {}\n@gc {}",
        verify(&writer_pub),
        verify(&writer_pub),
        verify(&gc_pub)
    );
    let trust = |signer: Option<String>| Trust::new(&rows, signer.as_deref()).unwrap();

    let fake = FakeGha::start().await;
    let http = reqwest::Client::new();
    fake.set_clock(T0);
    let mut honest = SimCache::new(&fake, &http);
    honest.trust = trust(Some(sign(&writer_key, &config)));
    let mut rogue = SimCache::new(&fake, &http);
    rogue.trust = Trust::new("", Some(&sign(&rogue_key, &config))).unwrap();
    let unsigned = SimCache::new(&fake, &http);

    let a = SimPath::new("a", 1, 100_000);
    let evil = SimPath::new("evil", 2, 100_000);
    let evil2 = SimPath::new("evil2", 3, 100_000);
    honest.push("main", &[&a], &[&a]).await;
    unsigned.push("main", &[&evil], &[&evil]).await;
    rogue.push("main", &[&evil2], &[&evil2]).await;

    let seen = honest.snapshot().await;
    assert!(seen.contains(&a.path_hash()));
    assert!(!seen.contains(&evil.path_hash()), "unsigned head ignored");
    assert!(!seen.contains(&evil2.path_hash()), "untrusted key ignored");
    let open = unsigned.snapshot().await;
    assert!(open.contains(&evil.path_hash()) && open.contains(&a.path_hash()));

    fake.set_clock(T0 + 2 * HOUR);
    let mut gc = SimCache::new(&fake, &http);
    gc.trust = trust(Some(sign(&gc_key, &config)));
    let stats = gc.run_gc(GcPolicy::default(), T0 + 2 * HOUR).await;
    assert_eq!(stats.roots, 1, "{stats:?}");
    let seen = honest.snapshot().await;
    assert_eq!(seen.view.epoch, 1);
    assert!(seen.contains(&a.path_hash()) && !seen.contains(&evil.path_hash()));

    // Forged g-* at a higher epoch.
    let forged = GcRecord {
        epoch: 2,
        time: T0 + 3 * HOUR,
        ..GcRecord::default()
    };
    let body = Signed::unsigned(forged.encode());
    unsigned
        .backend
        .put(&forged.head_name(&body).to_string(), body.into())
        .await
        .unwrap();
    let seen = honest.snapshot().await;
    assert_eq!(seen.view.epoch, 1, "reader stays on the signed g-*");
    assert!(seen.contains(&a.path_hash()));
    assert_eq!(unsigned.snapshot().await.view.epoch, 2);

    // After GC drains publish h-*, signed over the name.
    let b = SimPath::new("b", 4, 100_000);
    honest.push("main", &[&b], &[&a, &b]).await;
    let seen = honest.snapshot().await;
    assert!(seen.view.heads.iter().any(|(n, _)| n.starts_with("h-")));
    assert!(seen.contains(&b.path_hash()));
}
