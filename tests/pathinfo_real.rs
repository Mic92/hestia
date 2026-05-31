//! Integration tests for the nix-daemon path info client against the real
//! daemon and real store paths.
//!
//! These tests skip themselves (with a notice) when no nix-daemon is
//! reachable — e.g. inside the Nix build sandbox, which has no store access.

mod support;

use hestia::manifest::Hash32;
use hestia::upstream::UpstreamFilter;
use support::store::{daemon_or_skip, find_real_store_path, nix_path_info_json};

#[tokio::test]
async fn daemon_path_info_matches_nix_path_info_json() {
    let Some(daemon) = daemon_or_skip().await else {
        return;
    };
    let Some(store_path) = find_real_store_path() else {
        eprintln!("skipping: no real /nix/store path available");
        return;
    };
    let Some(expected) = nix_path_info_json(&store_path) else {
        eprintln!("skipping: nix path-info not available");
        return;
    };

    let info = daemon
        .query(store_path.to_str().unwrap())
        .await
        .expect("daemon query failed")
        .expect("path queried from nix path-info must be valid in the daemon too");

    // nar hash and size must agree with nix's own database record.
    let expected_hash = Hash32::parse_sha256(expected["narHash"].as_str().unwrap()).unwrap();
    assert_eq!(info.nar_hash, expected_hash, "narHash mismatch");
    assert_eq!(
        info.nar_size,
        expected["narSize"].as_u64().unwrap(),
        "narSize mismatch"
    );

    // References must agree (both sides sorted for comparison).
    let mut expected_refs: Vec<String> = expected["references"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_string())
        .collect();
    expected_refs.sort();
    let mut actual_refs: Vec<String> = info
        .references
        .iter()
        .map(|reference| format!("{}", daemon.store_dir().display(reference)))
        .collect();
    actual_refs.sort();
    assert_eq!(actual_refs, expected_refs, "references mismatch");

    // Signature key names must agree.
    let expected_sigs: Vec<&str> = expected["signatures"]
        .as_array()
        .map(|sigs| {
            sigs.iter()
                .map(|sig| {
                    sig.as_str()
                        .unwrap()
                        .split_once(':')
                        .expect("signature has key:sig form")
                        .0
                })
                .collect()
        })
        .unwrap_or_default();
    let actual_sigs: Vec<&str> = info.signatures.iter().map(|sig| sig.name()).collect();
    assert_eq!(actual_sigs, expected_sigs, "signature key names mismatch");
}

#[tokio::test]
async fn upstream_filter_rejects_cache_nixos_org_signed_path() {
    // The Phase 3 upstream-filter requirement: a real path signed by
    // cache.nixos.org must be detected as upstream so the pipeline skips it.
    let Some(daemon) = daemon_or_skip().await else {
        return;
    };
    let Some(store_path) = find_real_store_path() else {
        eprintln!("skipping: no real /nix/store path available");
        return;
    };

    let info = daemon
        .query(store_path.to_str().unwrap())
        .await
        .expect("daemon query failed")
        .expect("resolved sh path must be valid");

    let upstream_signed = info
        .signatures
        .iter()
        .any(|sig| sig.name() == "cache.nixos.org-1");
    if !upstream_signed {
        eprintln!(
            "skipping: {} is not signed by cache.nixos.org-1 \
             (locally built or substituted from another cache)",
            store_path.display()
        );
        return;
    }

    let filter = UpstreamFilter::default();
    assert!(
        filter.is_upstream_signed(&info.signatures),
        "default filter must flag a cache.nixos.org-signed path as upstream"
    );

    // And a filter with no trusted keys must not flag it.
    assert!(!UpstreamFilter::none().is_upstream_signed(&info.signatures));
}

#[tokio::test]
async fn nar_hash_from_daemon_matches_local_nar_serialization() {
    // Cross-check: the daemon's recorded NAR hash equals what hestia's own
    // NAR serialization (chunker) produces for the same path. This is the
    // property that lets the pipeline verify chunked data integrity.
    let Some(daemon) = daemon_or_skip().await else {
        return;
    };
    let Some(store_path) = find_real_store_path() else {
        eprintln!("skipping: no real /nix/store path available");
        return;
    };

    let info = daemon
        .query(store_path.to_str().unwrap())
        .await
        .expect("daemon query failed")
        .expect("resolved sh path must be valid");

    let (nar_hash, nar_size) = hestia::chunker::nar_hash_and_size(&store_path)
        .await
        .expect("local NAR serialization failed");
    assert_eq!(nar_hash, info.nar_hash, "NAR hash mismatch");
    assert_eq!(nar_size, info.nar_size, "NAR size mismatch");
}

#[tokio::test]
async fn nonexistent_path_returns_none() {
    let Some(daemon) = daemon_or_skip().await else {
        return;
    };

    let result = daemon
        .query("/nix/store/00000000000000000000000000000000-nonexistent-0.0.0")
        .await
        .expect("query for a nonexistent path must not error");
    assert_eq!(result, None);
}
