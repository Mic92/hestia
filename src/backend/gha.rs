//! The GitHub Actions cache. Reads go through signed URLs (cached until
//! they expire). Listing and deletes use the REST API and need `GITHUB_TOKEN`.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::urls::UrlCache;
use super::{Error, Listed};
use crate::gha::blob;
use crate::gha::rest::{ENV_GITHUB_TOKEN, RestClient};
use crate::gha::twirp::{DownloadUrl, Reservation, TwirpClient};

const URL_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Clone)]
pub struct Gha {
    twirp: TwirpClient,
    rest: Option<RestClient>,
    http: reqwest::Client,
    urls: Arc<UrlCache>,
}

impl Gha {
    pub fn new(twirp: TwirpClient, rest: Option<RestClient>, http: reqwest::Client) -> Self {
        Gha {
            twirp,
            rest,
            http,
            urls: Arc::new(UrlCache::new(URL_TTL)),
        }
    }

    pub fn from_env(http: reqwest::Client) -> Result<Self, Error> {
        let twirp = TwirpClient::from_env(http.clone())?;
        let rest = RestClient::from_env(http.clone()).ok();
        Ok(Self::new(twirp, rest, http))
    }

    fn rest(&self) -> Result<&RestClient, Error> {
        self.rest
            .as_ref()
            .ok_or(Error::MissingEnv(ENV_GITHUB_TOKEN))
    }

    /// `false` if the key already existed (same content for content-addressed keys).
    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        match self.twirp.create_cache_entry(key).await? {
            Reservation::AlreadyExists => Ok(false),
            Reservation::Created { upload_url } => {
                self.twirp
                    .upload_and_finalize(&self.http, key, upload_url, data)
                    .await?;
                Ok(true)
            }
        }
    }

    async fn url(&self, key: &str, force: bool) -> Result<Option<String>, Error> {
        self.urls
            .get(key, force, async {
                Ok(match self.twirp.get_download_url(key, &[]).await? {
                    DownloadUrl::Hit { url, .. } => Some(url),
                    DownloadUrl::Miss => None,
                })
            })
            .await
    }

    /// `None` if the key does not exist.
    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<Bytes>, Error> {
        let gone = || Error::Status {
            status: 404,
            url: key.to_owned(),
            body: String::new(),
        };
        // A cached URL outlives its entry when the key was deleted and put
        // again, so a 404 on it is retried with a fresh lookup.
        for force in [false, true] {
            let Some(url) = self.url(key, force).await? else {
                return Ok(None);
            };
            let refresh = async || self.url(key, true).await?.ok_or_else(gone);
            match blob::get_with_refresh(&self.http, &url, range.clone(), refresh).await {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(Error::Status { status: 404, .. }) => self.urls.evict(key),
                Err(err) => return Err(err),
            }
        }
        Ok(None)
    }

    /// 1-byte read to reset the LRU clock. `false` if the key is gone.
    pub async fn touch(&self, key: &str) -> Result<bool, Error> {
        Ok(self.get(key, Some(0..1)).await?.is_some())
    }

    /// `Ok(None)` if there are more than `limit` entries: a partial listing
    /// proves presence but never absence.
    pub async fn list(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Option<Vec<Listed>>, Error> {
        let entries = self
            .rest()?
            .list_caches_bounded(prefix, limit.unwrap_or(u64::MAX))
            .await?;
        // The listing spans all `version` namespaces. Only ours count.
        Ok(entries.map(|entries| {
            entries
                .into_iter()
                .filter(|e| e.version == self.twirp.version())
                .map(|e| Listed {
                    created: e.created_unix(),
                    last_accessed: e.last_accessed_unix(),
                    key: e.key,
                })
                .collect()
        }))
    }

    pub async fn delete(&self, key: &str) -> Result<bool, Error> {
        self.urls.evict(key);
        Ok(!self.rest()?.delete_by_key(key).await?.is_empty())
    }

    pub async fn probe_writable(&self) -> Result<bool, Error> {
        self.twirp.probe_writable().await
    }
}
