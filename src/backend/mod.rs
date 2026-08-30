//! Blob storage by string key. Every key but a head's is
//! `<kind>-<sha256 of the body>`, which the OCI backend relies on.

use std::ops::Range;

use bytes::Bytes;

pub use crate::gha::Error;

pub mod gha;
pub mod ghcr;
pub mod oci;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub created: Option<u64>,
    pub last_accessed: Option<u64>,
}

#[derive(Clone)]
pub enum Backend {
    Gha(gha::Gha),
    Oci(oci::Oci),
}

pub const ENV_OCI: &str = "HESTIA_OCI";

impl Backend {
    /// `HESTIA_OCI=<registry>/<repository>` selects a registry, else the
    /// Actions cache.
    pub fn from_env(http: reqwest::Client) -> Result<Self, Error> {
        match std::env::var(ENV_OCI) {
            Ok(repo) if !repo.is_empty() => Ok(Backend::Oci(oci::Oci::from_env(&repo, http)?)),
            _ => Ok(Backend::Gha(gha::Gha::from_env(http)?)),
        }
    }

    /// `false` if the key already existed (same content for content-addressed keys).
    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.put(key, data).await,
            Backend::Oci(b) => b.put(key, data).await,
        }
    }

    /// `None` if the key does not exist.
    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<Bytes>, Error> {
        match self {
            Backend::Gha(b) => b.get(key, range).await,
            Backend::Oci(b) => b.get(key, range).await,
        }
    }

    /// Reset the key's LRU clock where there is one. `false` if the key is gone.
    pub async fn touch(&self, key: &str) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.touch(key).await,
            Backend::Oci(b) => b.exists(key).await,
        }
    }

    /// `Ok(None)` if there are more than `limit` entries: a partial listing
    /// proves presence but never absence.
    pub async fn list(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Option<Vec<Listed>>, Error> {
        match self {
            Backend::Gha(b) => b.list(prefix, limit).await,
            Backend::Oci(b) => b.list(prefix, limit).await,
        }
    }

    /// GC only: all `pack-`/`seg-`/`tree-` objects, `None` where the
    /// backend cannot enumerate them.
    pub async fn list_objects(&self) -> Result<Option<Vec<Listed>>, Error> {
        match self {
            Backend::Gha(b) => {
                let mut out = Vec::new();
                for prefix in ["pack-", "seg-", "tree-"] {
                    out.extend(b.list(prefix, None).await?.expect("unbounded"));
                }
                Ok(Some(out))
            }
            Backend::Oci(b) => b.list_objects().await,
        }
    }

    pub async fn delete(&self, key: &str) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.delete(key).await,
            Backend::Oci(b) => b.delete(key).await,
        }
    }

    pub async fn probe_writable(&self) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.probe_writable().await,
            Backend::Oci(b) => b.probe_writable().await,
        }
    }

    /// Persist backend bookkeeping (the GHCR ledger) at the end of a GC run.
    pub async fn flush(&self) -> Result<(), Error> {
        match self {
            Backend::Gha(_) => Ok(()),
            Backend::Oci(b) => b.flush().await,
        }
    }
}
