//! Real Nix store and nix-daemon helpers for integration tests.
//!
//! Tests that need a real store or a reachable nix-daemon call these and
//! skip themselves (with a notice) when the environment lacks them — e.g.
//! the Nix build sandbox, which has neither store database nor daemon.

use std::path::{Path, PathBuf};
use std::process::Command;

use hestia::manifest::Hash32;
use hestia::pathinfo::{DEFAULT_DAEMON_SOCKET, NixDaemon};

/// Connect to the local nix-daemon, or return `None` (test should skip).
///
/// Probes in two steps: socket existence, then an actual protocol
/// handshake. Both can fail independently (no daemon installed vs. daemon
/// not accepting connections).
pub async fn daemon_or_skip() -> Option<NixDaemon> {
    let socket = Path::new(DEFAULT_DAEMON_SOCKET);
    if !socket.exists() {
        eprintln!("skipping: no nix-daemon socket at {DEFAULT_DAEMON_SOCKET}");
        return None;
    }
    let daemon = NixDaemon::new(socket);
    match daemon.ping().await {
        Ok(()) => Some(daemon),
        Err(err) => {
            eprintln!("skipping: nix-daemon not reachable: {err}");
            None
        }
    }
}

/// Find a real store path by resolving the `sh` binary through symlinks.
pub fn find_real_store_path() -> Option<PathBuf> {
    let output = Command::new("sh")
        .args(["-c", "command -v sh"])
        .output()
        .ok()?;
    let sh = String::from_utf8(output.stdout).ok()?;
    let resolved = std::fs::canonicalize(sh.trim()).ok()?;
    // /nix/store/<hash>-<name>/bin/bash -> /nix/store/<hash>-<name>
    let mut components = resolved.components();
    let prefix: PathBuf = components.by_ref().take(4).collect();
    if !prefix.starts_with("/nix/store") || prefix == Path::new("/nix/store") {
        return None;
    }
    prefix.is_dir().then_some(prefix)
}

/// Full `nix path-info --json` record for a path (the test oracle).
/// `None` if nix is unavailable or the path is not in the Nix database.
pub fn nix_path_info_json(path: &Path) -> Option<serde_json::Value> {
    let output = Command::new("nix")
        .args([
            "--extra-experimental-features",
            "nix-command",
            "path-info",
            "--json",
        ])
        .arg(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    // nix >= 2.19: {"/nix/store/...": {...}}; older nix: [{"path": ..., ...}]
    let info = match value {
        serde_json::Value::Object(map) => map.into_iter().next().map(|(_, info)| info),
        serde_json::Value::Array(array) => array.into_iter().next(),
        _ => None,
    }?;
    // Unknown paths come back as null entries on modern nix.
    (!info.is_null()).then_some(info)
}

/// NAR hash + size from `nix path-info --json`.
pub fn nix_path_info_hash(path: &Path) -> Option<(Hash32, u64)> {
    let info = nix_path_info_json(path)?;
    let nar_hash = Hash32::parse_sha256(info.get("narHash")?.as_str()?)?;
    let nar_size = info.get("narSize")?.as_u64()?;
    Some((nar_hash, nar_size))
}

/// Reference NAR hash + size via `nix-store --dump` (works on arbitrary
/// paths, no Nix database needed). `None` if nix-store is unavailable.
pub fn nix_store_dump_hash(path: &Path) -> Option<(Hash32, u64)> {
    let output = Command::new("nix-store")
        .arg("--dump")
        .arg(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some((Hash32::digest(&output.stdout), output.stdout.len() as u64))
}
