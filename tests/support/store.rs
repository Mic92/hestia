//! Nix store helpers for integration tests.
//!
//! Two kinds of stores are used:
//!
//! * [`ScratchStore`]: a throwaway store created per test with
//!   `nix-store --store 'local?store=…' --add`. Fully hermetic: contents,
//!   references, and signatures are all controlled by the test. Needs the
//!   nix tooling on PATH (tests skip with a notice when it is missing,
//!   e.g. inside the Nix build sandbox).
//! * The system store (`/nix/store`): used to cross-check hestia against
//!   real-world paths and the `nix path-info` oracle.

use std::path::{Path, PathBuf};
use std::process::Command;

use hestia::manifest::Hash32;
use hestia::pathinfo::{DEFAULT_DB_PATH, StoreDatabase, StoreDir};

// ---------------------------------------------------------------------------
// Scratch store
// ---------------------------------------------------------------------------

/// A hermetic Nix store in a tempdir, populated via nix-store/nix eval.
pub struct ScratchStore {
    dir: tempfile::TempDir,
}

impl Drop for ScratchStore {
    fn drop(&mut self) {
        // Nix strips write permission from store contents; restore it so
        // the TempDir can actually delete them afterwards.
        let _ = Command::new("chmod")
            .arg("-R")
            .arg("u+w")
            .arg(self.dir.path())
            .status();
    }
}

impl ScratchStore {
    /// Create an empty scratch store. Returns `None` (test should skip)
    /// when the nix tooling is unavailable.
    pub fn create() -> Option<Self> {
        let nix_works = Command::new("nix-store")
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success());
        if !nix_works {
            eprintln!("skipping: nix-store not available");
            return None;
        }
        let dir = tempfile::tempdir().expect("creating tempdir failed");
        Some(Self { dir })
    }

    /// The `--store` URI for nix commands operating on this store.
    pub fn store_uri(&self) -> String {
        let root = self.dir.path().display();
        format!("local?store={root}/store&state={root}/state&log={root}/log")
    }

    /// The store directory (both the logical prefix of store paths and
    /// their physical location).
    pub fn store_dir_path(&self) -> PathBuf {
        self.dir.path().join("store")
    }

    /// Location of this store's SQLite database.
    pub fn db_path(&self) -> PathBuf {
        self.dir.path().join("state/db/db.sqlite")
    }

    /// A hestia [`StoreDatabase`] client for this store.
    pub fn database(&self) -> StoreDatabase {
        let store_dir =
            StoreDir::new(self.store_dir_path()).expect("scratch store dir is a valid StoreDir");
        StoreDatabase::with_store_dir(self.db_path(), store_dir)
    }

    /// Create a second, empty store that shares this store's *logical*
    /// store directory but lives at a different physical location
    /// (`local?store=…&real=…`). This is the destination of substitution
    /// tests: `nix copy --to <dest>` must be able to register paths whose
    /// logical prefix is this store's directory.
    pub fn create_destination(&self) -> DestinationStore {
        let dir = tempfile::tempdir().expect("creating tempdir failed");
        let root = dir.path().display().to_string();
        let logical = self.store_dir_path().display().to_string();
        DestinationStore {
            uri: format!(
                "local?store={logical}&real={root}/store&state={root}/state&log={root}/log"
            ),
            physical_store_dir: dir.path().join("store"),
            dir,
        }
    }

    fn nix_store_cmd(&self) -> Command {
        let mut cmd = Command::new("nix-store");
        cmd.arg("--store").arg(self.store_uri());
        cmd
    }

    /// A `nix` command with experimental features enabled and `--store`
    /// pointing at this scratch store.
    fn nix_cmd(&self) -> Command {
        let mut cmd = Command::new("nix");
        cmd.args(["--extra-experimental-features", "nix-command"])
            .arg("--store")
            .arg(self.store_uri());
        cmd
    }

    /// Add a filesystem tree to the store (`nix-store --add`).
    /// Returns the registered store path.
    pub fn add_path(&self, source: &Path) -> PathBuf {
        let output = self
            .nix_store_cmd()
            .arg("--add")
            .arg(source)
            .output()
            .expect("running nix-store --add failed");
        assert!(
            output.status.success(),
            "nix-store --add failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
    }

    /// Create a deterministic fixture tree (multi-chunk blob, executable,
    /// symlink, empty file) and add it to the store.
    ///
    /// Same `name` + `seed` always produces the same store path.
    /// Like [`Self::add_fixture`], but with a blob of `bytes` pseudo-random
    /// bytes; benchmarks use this to build workloads of arbitrary size.
    pub fn add_fixture_sized(&self, name: &str, seed: u64, bytes: usize) -> PathBuf {
        let fixture = self.dir.path().join(format!("fixture-{name}"));
        std::fs::create_dir_all(&fixture).unwrap();
        let mut blob = Vec::with_capacity(bytes);
        let mut state = seed | 1;
        while blob.len() < bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            blob.extend_from_slice(&state.to_le_bytes());
        }
        blob.truncate(bytes);
        std::fs::write(fixture.join("blob"), &blob).unwrap();
        self.add_path(&fixture)
    }

    pub fn add_fixture(&self, name: &str, seed: u64) -> PathBuf {
        let fixture = self.dir.path().join(format!("fixture-{name}"));
        std::fs::create_dir_all(fixture.join("bin")).unwrap();

        // Executable script.
        let exe = fixture.join("bin").join(name);
        std::fs::write(&exe, format!("#!/bin/sh\necho {name}\n")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        // Pseudo-random blob spanning multiple FastCDC chunks (~600 KB).
        let mut blob = Vec::with_capacity(600_000);
        let mut state = seed | 1;
        while blob.len() < 600_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            blob.extend_from_slice(&state.to_le_bytes());
        }
        std::fs::write(fixture.join("blob"), &blob).unwrap();

        std::fs::write(fixture.join("empty"), b"").unwrap();
        std::os::unix::fs::symlink(format!("bin/{name}"), fixture.join("link")).unwrap();

        self.add_path(&fixture)
    }

    /// Register two text paths where `top` references `dep`
    /// (via `builtins.toFile` interpolation). Returns `(top, dep)`.
    pub fn add_paths_with_reference(&self, label: &str) -> (PathBuf, PathBuf) {
        let expr = format!(
            r#"let dep = builtins.toFile "{label}-dep" "dependency contents for {label}";
               in builtins.toFile "{label}-top" "depends on ${{dep}}""#
        );
        let output = self
            .nix_cmd()
            .args(["eval", "--raw", "--expr", &expr])
            .output()
            .expect("running nix eval failed");
        assert!(
            output.status.success(),
            "nix eval failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let top = PathBuf::from(String::from_utf8(output.stdout).unwrap().trim());

        // The dep path is top's only reference; read it from the store
        // contents (the interpolated path is the file's payload).
        let content = std::fs::read_to_string(&top).expect("reading top path failed");
        let dep = PathBuf::from(
            content
                .split_whitespace()
                .last()
                .expect("top content ends with the dep path"),
        );
        (top, dep)
    }

    /// Sign a store path with a freshly generated key of the given name.
    /// Used to simulate upstream (cache.nixos.org) signatures hermetically.
    pub fn sign_path(&self, path: &Path, key_name: &str) {
        let key_file = self.dir.path().join(format!("{key_name}.sec"));
        let keygen = Command::new("nix")
            .args([
                "--extra-experimental-features",
                "nix-command",
                "key",
                "generate-secret",
                "--key-name",
                key_name,
            ])
            .output()
            .expect("running nix key generate-secret failed");
        assert!(
            keygen.status.success(),
            "nix key generate-secret failed: {}",
            String::from_utf8_lossy(&keygen.stderr)
        );
        std::fs::write(&key_file, &keygen.stdout).unwrap();

        // `nix store sign` (subcommand) inherits --store from nix_cmd.
        let sign = self
            .nix_cmd()
            .args(["store", "sign", "--key-file"])
            .arg(&key_file)
            .arg(path)
            .output()
            .expect("running nix store sign failed");
        assert!(
            sign.status.success(),
            "nix store sign failed: {}",
            String::from_utf8_lossy(&sign.stderr)
        );
    }

    /// `nix path-info --json` against this store (the test oracle).
    pub fn path_info_json(&self, path: &Path) -> Option<serde_json::Value> {
        let output = self
            .nix_cmd()
            .args(["path-info", "--json"])
            .arg(path)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        parse_path_info_output(&output.stdout)
    }

    /// NAR hash + size oracle for a path in this store.
    pub fn nar_hash_oracle(&self, path: &Path) -> Option<(Hash32, u64)> {
        let info = self.path_info_json(path)?;
        let nar_hash = Hash32::parse_sha256(info.get("narHash")?.as_str()?)?;
        let nar_size = info.get("narSize")?.as_u64()?;
        Some((nar_hash, nar_size))
    }
}

/// A destination store for substitution tests (see
/// [`ScratchStore::create_destination`]).
pub struct DestinationStore {
    /// `--store` URI for nix commands targeting this store.
    pub uri: String,
    /// Where store path contents physically end up.
    pub physical_store_dir: PathBuf,
    dir: tempfile::TempDir,
}

impl Drop for DestinationStore {
    fn drop(&mut self) {
        // Nix strips write permission from store contents; restore it so
        // the TempDir can actually delete them afterwards.
        let _ = Command::new("chmod")
            .arg("-R")
            .arg("u+w")
            .arg(self.dir.path())
            .status();
    }
}

impl DestinationStore {
    /// Physical location of one store path in this store.
    pub fn physical_path(&self, store_path: &Path) -> PathBuf {
        self.physical_store_dir
            .join(store_path.file_name().expect("store path has a basename"))
    }
}

// ---------------------------------------------------------------------------
// System store
// ---------------------------------------------------------------------------

/// Open the system store database, or return `None` (test should skip)
/// when it does not exist or is not readable.
pub fn system_db_or_skip() -> Option<StoreDatabase> {
    let store = StoreDatabase::new(DEFAULT_DB_PATH);
    if !Path::new(DEFAULT_DB_PATH).exists() {
        eprintln!("skipping: no system store database at {DEFAULT_DB_PATH}");
        return None;
    }
    match store.ping() {
        Ok(()) => Some(store),
        Err(err) => {
            eprintln!("skipping: system store database not readable: {err}");
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

/// Full `nix path-info --json` record for a system store path.
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
    parse_path_info_output(&output.stdout)
}

/// NAR hash + size from `nix path-info --json` (system store).
pub fn nix_path_info_hash(path: &Path) -> Option<(Hash32, u64)> {
    let info = nix_path_info_json(path)?;
    let nar_hash = Hash32::parse_sha256(info.get("narHash")?.as_str()?)?;
    let nar_size = info.get("narSize")?.as_u64()?;
    Some((nar_hash, nar_size))
}

/// Reference NAR hash + size via `nix-store --dump` (works on arbitrary
/// paths, no store database needed). `None` if nix-store is unavailable.
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

/// Parse `nix path-info --json` output (handles both the keyed-object
/// format of nix >= 2.19 and the array format of older versions).
fn parse_path_info_output(stdout: &[u8]) -> Option<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_slice(stdout).ok()?;
    let info = match value {
        serde_json::Value::Object(map) => map.into_iter().next().map(|(_, info)| info),
        serde_json::Value::Array(array) => array.into_iter().next(),
        _ => None,
    }?;
    // Unknown paths come back as null entries on modern nix.
    (!info.is_null()).then_some(info)
}
