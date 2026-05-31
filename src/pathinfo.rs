//! Path metadata via the nix-daemon protocol (harmonia-store-remote).
//!
//! The write pipeline needs `PathInfo` (nar hash/size, references,
//! signatures, …) for every path it considers pushing. It comes from the
//! nix-daemon over pooled unix-socket connections — the daemon is the
//! authoritative store database, and every environment hestia serves
//! (CI runners, self-hosted boxes) runs one.
//!
//! Environments without a reachable daemon (e.g. the Nix build sandbox)
//! cannot provide path info at all; tests detect that and skip.

use std::path::Path;

use harmonia_store_path::{StoreDir, StorePath};
use harmonia_store_remote::pool::{ConnectionPool, PoolConfig};
use harmonia_store_remote::{DaemonStore as _, UnkeyedValidPathInfo};
use harmonia_utils_signature::Signature;

use crate::manifest::{Hash32, PathHash};

/// Default location of the nix-daemon socket.
pub const DEFAULT_DAEMON_SOCKET: &str = "/nix/var/nix/daemon-socket/socket";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid store path {path:?}: {reason}")]
    InvalidStorePath { path: String, reason: String },

    #[error("nix-daemon request failed: {0}")]
    Daemon(#[from] harmonia_store_remote::DaemonError),
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
    /// Absolute deriver path, if known.
    pub deriver: Option<String>,
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

    /// Manifest keys of all referenced paths, excluding the self-reference
    /// (the manifest reachability walk treats self-edges as no-ops anyway,
    /// but dropping them keeps entries smaller).
    pub fn reference_hashes(&self) -> Vec<PathHash> {
        let own = self.path_hash();
        self.references
            .iter()
            .map(PathHash::from_store_path)
            .filter(|hash| *hash != own)
            .collect()
    }
}

/// Convert harmonia's daemon answer into hestia's [`PathInfo`].
fn from_daemon(store_dir: &StoreDir, path: StorePath, info: UnkeyedValidPathInfo) -> PathInfo {
    PathInfo {
        nar_hash: Hash32(
            info.nar_hash
                .digest_bytes()
                .try_into()
                .expect("NarHash is always 32 bytes"),
        ),
        nar_size: info.nar_size,
        references: info.references.into_iter().collect(),
        deriver: info
            .deriver
            .map(|deriver| store_dir.display(&deriver).to_string()),
        ca: info.ca.map(|ca| ca.to_string()),
        signatures: info.signatures.into_iter().collect(),
        store_path: path,
    }
}

/// Client for the nix-daemon (pooled connections, lazy connect).
///
/// Construction never touches the socket; the first query (or [`ping`])
/// does. [`NixDaemon::ping`] is how callers find out whether a daemon is
/// actually reachable.
///
/// [`ping`]: NixDaemon::ping
pub struct NixDaemon {
    pool: ConnectionPool,
}

impl NixDaemon {
    pub fn new(socket_path: impl AsRef<Path>) -> Self {
        Self {
            pool: ConnectionPool::new(socket_path, PoolConfig::default()),
        }
    }

    /// The store directory the daemon serves (normally `/nix/store`).
    pub fn store_dir(&self) -> &StoreDir {
        self.pool.store_dir()
    }

    /// Verify the daemon is reachable by performing a connection handshake.
    pub async fn ping(&self) -> Result<(), Error> {
        let _guard = self.pool.acquire().await?;
        Ok(())
    }

    /// Query metadata for an absolute store path (`/nix/store/…`).
    ///
    /// Returns `Ok(None)` if the path is not a valid path in the store.
    pub async fn query(&self, store_path: &str) -> Result<Option<PathInfo>, Error> {
        let parsed = self
            .store_dir()
            .parse::<StorePath>(store_path)
            .map_err(|err| Error::InvalidStorePath {
                path: store_path.to_string(),
                reason: err.to_string(),
            })?;
        let store_dir = self.store_dir().clone();
        let mut guard = self.pool.acquire().await?;
        let info = guard.client().query_path_info(&parsed).await?;
        Ok(info.map(|info| from_daemon(&store_dir, parsed, info)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn malformed_store_path_is_an_error_not_a_query() {
        // The path never reaches the socket, so this works even where no
        // daemon is running (e.g. the Nix build sandbox).
        let daemon = NixDaemon::new("/nonexistent/socket");
        let result = daemon.query("/not/a/store/path").await;
        assert!(matches!(result, Err(Error::InvalidStorePath { .. })));

        let result = daemon.query("relative/path").await;
        assert!(matches!(result, Err(Error::InvalidStorePath { .. })));
    }

    #[tokio::test]
    async fn unreachable_daemon_is_a_daemon_error() {
        let daemon = NixDaemon::new("/nonexistent/socket");
        let result = daemon
            .query("/nix/store/4bwbk4an4bx7cb8xwffghvjjyfyl7m2i-bash-interactive-5.3p9")
            .await;
        assert!(matches!(result, Err(Error::Daemon(_))));

        let result = daemon.ping().await;
        assert!(matches!(result, Err(Error::Daemon(_))));
    }
}
