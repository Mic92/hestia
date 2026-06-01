//! Path metadata from the Nix store database (direct SQLite reads).
//!
//! The write pipeline needs `PathInfo` (nar hash/size, references,
//! signatures, …) for every path it considers pushing. It reads the store's
//! SQLite database directly via harmonia-store-db — the same access path
//! harmonia-cache uses in production.
//!
//! Why not the nix-daemon protocol: a daemon only exists on multi-user
//! installs, while the database exists wherever paths were built — and a
//! post-build-hook by definition runs on the machine that built the paths.
//! Direct reads also make tests hermetic: a scratch store created with
//! `nix-store --store 'local?store=…' --add` is queryable without spawning
//! a daemon process.

use std::path::{Path, PathBuf};

use harmonia_store_db::StoreDb;
use harmonia_store_path::StorePath;
use harmonia_store_path_info::UnkeyedValidPathInfo;
use harmonia_utils_signature::Signature;

// Re-export so callers can construct custom store dirs without depending on
// harmonia crates directly.
pub use harmonia_store_path::StoreDir;

use crate::manifest::{Hash32, PathHash};

/// Default location of the Nix store database.
pub const DEFAULT_DB_PATH: &str = "/nix/var/nix/db/db.sqlite";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("store database error: {0}")]
    Database(#[from] harmonia_store_db::Error),
}

/// Everything the write pipeline needs to know about one valid store path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathInfo {
    pub store_path: StorePath,
    /// SHA-256 of the path's NAR serialization.
    pub nar_hash: Hash32,
    pub nar_size: u64,
    /// Store paths this path references (may include itself).
    pub references: Vec<StorePath>,
    /// Deriver store path, if known.
    pub deriver: Option<StorePath>,
    /// Content address (nix text form, e.g. `fixed:r:sha256:…`), if the
    /// path is content-addressed.
    pub ca: Option<String>,
    pub signatures: Vec<Signature>,
}

impl PathInfo {
    /// Manifest key for this path.
    pub fn path_hash(&self) -> PathHash {
        PathHash::from_store_path(&self.store_path)
    }

    /// All referenced store paths, excluding the self-reference (the
    /// manifest reachability walk treats self-edges as no-ops anyway, but
    /// dropping them keeps entries smaller).
    pub fn references_without_self(&self) -> Vec<StorePath> {
        self.references
            .iter()
            .filter(|reference| **reference != self.store_path)
            .cloned()
            .collect()
    }
}

/// Result of looking up one path string.
#[derive(Debug)]
pub enum Lookup {
    /// The path is valid; here is its metadata.
    Found(Box<PathInfo>),
    /// The path is well-formed but not registered in the database.
    Unknown,
    /// The path string is not a valid store path for this store.
    Malformed { reason: String },
}

/// Convert harmonia's database record into hestia's [`PathInfo`].
fn convert(path: StorePath, info: UnkeyedValidPathInfo) -> PathInfo {
    PathInfo {
        nar_hash: Hash32(
            info.nar_hash
                .digest_bytes()
                .try_into()
                .expect("NarHash is always 32 bytes"),
        ),
        nar_size: info.nar_size,
        references: info.references.into_iter().collect(),
        deriver: info.deriver,
        ca: info.ca.map(|ca| ca.to_string()),
        signatures: info.signatures.into_iter().collect(),
        store_path: path,
    }
}

/// Read-only client for a Nix store database.
///
/// Holds no open connection: each query batch opens the database read-only
/// (without SQLite's immutable flag), so concurrent registrations by nix
/// builds running in the same job are visible. All methods are synchronous
/// (rusqlite); callers in async context should wrap batches in
/// `spawn_blocking`.
#[derive(Debug, Clone)]
pub struct StoreDatabase {
    db_path: PathBuf,
    store_dir: StoreDir,
}

impl StoreDatabase {
    /// The system store: `/nix/store` backed by [`DEFAULT_DB_PATH`].
    pub fn system() -> Self {
        Self::new(DEFAULT_DB_PATH)
    }

    /// A database for the default store dir (`/nix/store`) at a custom
    /// database location.
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
            store_dir: StoreDir::default(),
        }
    }

    /// A database for a non-default store dir (scratch stores in tests,
    /// `local?store=…` setups).
    pub fn with_store_dir(db_path: impl Into<PathBuf>, store_dir: StoreDir) -> Self {
        Self {
            db_path: db_path.into(),
            store_dir,
        }
    }

    pub fn store_dir(&self) -> &StoreDir {
        &self.store_dir
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    fn open(&self) -> Result<StoreDb, Error> {
        Ok(StoreDb::open_readonly(&self.db_path)?)
    }

    /// Verify the database exists and has a valid schema.
    pub fn ping(&self) -> Result<(), Error> {
        let db = self.open()?;
        db.count_valid_paths()?;
        Ok(())
    }

    /// Look up one absolute store path.
    pub fn query(&self, store_path: &str) -> Result<Lookup, Error> {
        let db = self.open()?;
        self.lookup_one(&db, store_path)
    }

    /// Look up many paths over a single database connection.
    ///
    /// Per-path problems (malformed, unknown) are reported as [`Lookup`]
    /// variants; only database-level failures are errors.
    pub fn query_batch<I, S>(&self, store_paths: I) -> Result<Vec<(String, Lookup)>, Error>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let store_paths: Vec<String> = store_paths.into_iter().map(Into::into).collect();
        if store_paths.is_empty() {
            // No queries -> no reason to require a database (callers may
            // legitimately have nothing buffered).
            return Ok(Vec::new());
        }
        let db = self.open()?;
        store_paths
            .into_iter()
            .map(|path| {
                let lookup = self.lookup_one(&db, &path)?;
                Ok((path, lookup))
            })
            .collect()
    }

    fn lookup_one(&self, db: &StoreDb, store_path: &str) -> Result<Lookup, Error> {
        let parsed = match self.store_dir.parse::<StorePath>(store_path) {
            Ok(parsed) => parsed,
            Err(err) => {
                return Ok(Lookup::Malformed {
                    reason: err.to_string(),
                });
            }
        };
        match db.query_path_info(&self.store_dir, &parsed)? {
            Some(record) => Ok(Lookup::Found(Box::new(convert(record.path, record.info)))),
            None => Ok(Lookup::Unknown),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_database_is_an_error() {
        let store = StoreDatabase::new("/nonexistent/db.sqlite");
        assert!(matches!(store.ping(), Err(Error::Database(_))));
        assert!(matches!(
            store.query("/nix/store/00000000000000000000000000000000-x"),
            Err(Error::Database(_))
        ));
    }

    #[test]
    fn system_database_uses_default_locations() {
        let store = StoreDatabase::system();
        assert_eq!(store.db_path(), Path::new(DEFAULT_DB_PATH));
        assert_eq!(store.store_dir().to_str(), "/nix/store");
    }

    #[test]
    fn custom_store_dir_rejects_foreign_paths_as_malformed() {
        // A store rooted somewhere else must not accept /nix/store paths.
        // Use an in-memory database so this works without any store.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("db.sqlite");
        {
            // Create an empty database with the Nix schema.
            let db =
                harmonia_store_db::StoreDb::open(&db_path, harmonia_store_db::OpenMode::Create)
                    .unwrap();
            db.create_schema().unwrap();
        }
        let store_dir = StoreDir::new(dir.path().join("store")).unwrap();
        let store = StoreDatabase::with_store_dir(&db_path, store_dir);

        store.ping().expect("schema-initialized database pings");

        let lookup = store
            .query("/nix/store/00000000000000000000000000000000-foreign")
            .unwrap();
        assert!(matches!(lookup, Lookup::Malformed { .. }));

        // A well-formed path for *this* store dir that simply is not
        // registered comes back as Unknown.
        let own_path = format!(
            "{}/00000000000000000000000000000000-not-there",
            store.store_dir()
        );
        let lookup = store.query(&own_path).unwrap();
        assert!(matches!(lookup, Lookup::Unknown));
    }
}
