//! Blob storage by string key. Every key but a head's is
//! `<kind>-<sha256 of the body>`, which the OCI backend relies on.

use std::ops::Range;

use bytes::Bytes;

pub use crate::gha::Error;

pub mod gha;
pub mod ghcr;
pub mod oci;
pub mod s3;
mod urls;

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
    S3(s3::S3),
}

pub const ENV_OCI: &str = "HESTIA_OCI";
pub const ENV_S3: &str = "HESTIA_S3";

impl Backend {
    /// `HESTIA_S3=s3://<bucket>/<prefix>` selects a bucket,
    /// `HESTIA_OCI=<registry>/<repository>` a registry, else the Actions cache.
    pub fn from_env(http: reqwest::Client) -> Result<Self, Error> {
        let var = |k| std::env::var(k).ok().filter(|v: &String| !v.is_empty());
        if let Some(url) = var(ENV_S3) {
            return Ok(Backend::S3(s3::S3::from_env(&url, http)?));
        }
        if let Some(repo) = var(ENV_OCI) {
            return Ok(Backend::Oci(oci::Oci::from_env(&repo, http)?));
        }
        Ok(Backend::Gha(gha::Gha::from_env(http)?))
    }

    /// `false` if the key already existed (same content for content-addressed keys).
    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.put(key, data).await,
            Backend::Oci(b) => b.put(key, data).await,
            Backend::S3(b) => b.put(key, data).await,
        }
    }

    /// `None` if the key does not exist.
    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<Bytes>, Error> {
        match self {
            Backend::Gha(b) => b.get(key, range).await,
            Backend::Oci(b) => b.get(key, range).await,
            Backend::S3(b) => b.get(key, range).await,
        }
    }

    /// Reset the key's LRU clock where there is one. `false` if the key is gone.
    pub async fn touch(&self, key: &str) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.touch(key).await,
            Backend::Oci(b) => b.exists(key).await,
            Backend::S3(b) => b.exists(key).await,
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
            Backend::S3(b) => b.list(prefix, limit).await,
        }
    }

    /// Every `g-`/`h-`/`c-` key. One request where the backend can.
    pub async fn list_heads(&self) -> Result<Vec<Listed>, Error> {
        if let Backend::Gha(b) = self {
            let (g, h, c) = futures_util::future::try_join3(
                b.list("g-", None),
                b.list("h-", None),
                b.list("c-", None),
            )
            .await?;
            return Ok([g, h, c].into_iter().flatten().flatten().collect());
        }
        Ok(self.list("", None).await?.expect("unbounded"))
    }

    /// GC only: all `pack-`/`seg-`/`tree-` objects with creation times.
    pub async fn list_objects(&self) -> Result<Vec<Listed>, Error> {
        if let Backend::Oci(b) = self {
            return b.list_objects().await;
        }
        let mut out = Vec::new();
        for prefix in ["pack-", "seg-", "tree-"] {
            let listed = self.list(prefix, None).await?.ok_or(Error::InvalidEnv {
                name: ENV_S3,
                reason: "an http(s):// store is read-only and cannot be collected".into(),
            })?;
            out.extend(listed);
        }
        Ok(out)
    }

    pub async fn delete(&self, key: &str) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.delete(key).await,
            Backend::Oci(b) => b.delete(key).await,
            Backend::S3(b) => b.delete(key).await,
        }
    }

    pub async fn probe_writable(&self) -> Result<bool, Error> {
        match self {
            Backend::Gha(b) => b.probe_writable().await,
            Backend::Oci(b) => b.probe_writable().await,
            Backend::S3(b) => b.probe_writable().await,
        }
    }

    /// Persist backend bookkeeping: the GHCR ledger, the S3 head index.
    pub async fn flush(&self) -> Result<(), Error> {
        match self {
            Backend::Gha(_) => Ok(()),
            Backend::S3(b) => b.flush().await,
            Backend::Oci(b) => b.flush().await,
        }
    }
}
