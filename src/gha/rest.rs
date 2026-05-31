//! GitHub REST API client for Actions cache management.
//!
//! Used by `hestia gc`: unlike the Twirp API (runtime token, job-scoped),
//! the REST API is authenticated with `GITHUB_TOKEN` and can list, inspect
//! and **delete** cache entries:
//!
//! ```text
//! GET    /repos/{repo}/actions/caches?key={prefix}&per_page=&page=
//! GET    /repos/{repo}/actions/cache/usage
//! DELETE /repos/{repo}/actions/caches?key={key}
//! ```
//!
//! The workflow needs `permissions: actions: write` for deletion.

use serde::{Deserialize, Serialize};

use crate::gha::Error;

/// Default GitHub API endpoint; overridable for tests and GHES.
pub const DEFAULT_API_URL: &str = "https://api.github.com";

pub const ENV_GITHUB_TOKEN: &str = "GITHUB_TOKEN";
pub const ENV_GITHUB_REPOSITORY: &str = "GITHUB_REPOSITORY";
pub const ENV_GITHUB_API_URL: &str = "GITHUB_API_URL";

const PER_PAGE: u32 = 100;

/// Build the caches collection URL for a repository.
fn caches_url(api_url: &str, repo: &str) -> String {
    format!(
        "{}/repos/{repo}/actions/caches",
        api_url.trim_end_matches('/')
    )
}

/// One cache entry as reported by the REST API.
///
/// `last_accessed_at` is the LRU clock: downloads through the Twirp/Azure
/// path bump it, which is what makes 1-byte Range reads work as GC touches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub id: u64,
    #[serde(rename = "ref", default)]
    pub git_ref: String,
    pub key: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub last_accessed_at: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub size_in_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheList {
    pub total_count: u64,
    #[serde(default)]
    pub actions_caches: Vec<CacheEntry>,
}

/// Repository cache usage (quota pressure signal for GC).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheUsage {
    #[serde(default)]
    pub full_name: String,
    pub active_caches_count: u64,
    pub active_caches_size_in_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct RestClient {
    http: reqwest::Client,
    api_url: String,
    repo: String,
    token: String,
}

impl RestClient {
    pub fn new(
        http: reqwest::Client,
        api_url: impl Into<String>,
        repo: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            http,
            api_url: api_url.into(),
            repo: repo.into(),
            token: token.into(),
        }
    }

    /// Build a client from `GITHUB_TOKEN` / `GITHUB_REPOSITORY`
    /// (and `GITHUB_API_URL` if set, for GHES).
    pub fn from_env(http: reqwest::Client) -> Result<Self, Error> {
        let token =
            std::env::var(ENV_GITHUB_TOKEN).map_err(|_| Error::MissingEnv(ENV_GITHUB_TOKEN))?;
        let repo = std::env::var(ENV_GITHUB_REPOSITORY)
            .map_err(|_| Error::MissingEnv(ENV_GITHUB_REPOSITORY))?;
        let api_url =
            std::env::var(ENV_GITHUB_API_URL).unwrap_or_else(|_| DEFAULT_API_URL.to_string());
        if token.is_empty() {
            return Err(Error::MissingEnv(ENV_GITHUB_TOKEN));
        }
        if !repo.contains('/') {
            return Err(Error::InvalidEnv {
                name: ENV_GITHUB_REPOSITORY,
                reason: format!("expected owner/repo, got {repo:?}"),
            });
        }
        Ok(Self::new(http, api_url, repo, token))
    }

    fn caches_url(&self) -> String {
        caches_url(&self.api_url, &self.repo)
    }

    fn request(&self, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, url)
            .bearer_auth(&self.token)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header(reqwest::header::USER_AGENT, "hestia")
    }

    async fn check<T: serde::de::DeserializeOwned>(
        url: &str,
        response: reqwest::Response,
    ) -> Result<T, Error> {
        let status = response.status();
        if status.is_success() {
            Ok(response.json().await?)
        } else {
            Err(Error::Status {
                status: status.as_u16(),
                url: url.to_string(),
                body: response.text().await.unwrap_or_default(),
            })
        }
    }

    /// List all cache entries whose key starts with `key_prefix`
    /// (empty prefix lists everything). Follows pagination.
    pub async fn list_caches(&self, key_prefix: &str) -> Result<Vec<CacheEntry>, Error> {
        let url = self.caches_url();
        let mut entries = Vec::new();
        let mut page: u32 = 1;
        loop {
            let mut request = self.request(reqwest::Method::GET, &url).query(&[
                ("per_page", PER_PAGE.to_string()),
                ("page", page.to_string()),
            ]);
            if !key_prefix.is_empty() {
                request = request.query(&[("key", key_prefix)]);
            }
            let response = request.send().await?;
            let list: CacheList = Self::check(&url, response).await?;
            entries.extend(list.actions_caches);
            if entries.len() as u64 >= list.total_count {
                return Ok(entries);
            }
            page += 1;
        }
    }

    /// Repository-wide cache usage (quota pressure).
    pub async fn usage(&self) -> Result<CacheUsage, Error> {
        let url = format!(
            "{}/repos/{}/actions/cache/usage",
            self.api_url.trim_end_matches('/'),
            self.repo
        );
        let response = self.request(reqwest::Method::GET, &url).send().await?;
        Self::check(&url, response).await
    }

    /// Delete all cache entries with exactly this key (across versions/refs).
    /// Returns the deleted entries. Deleting a non-existent key is not an
    /// error and returns an empty list (GC idempotence).
    pub async fn delete_by_key(&self, key: &str) -> Result<Vec<CacheEntry>, Error> {
        let url = self.caches_url();
        let response = self
            .request(reqwest::Method::DELETE, &url)
            .query(&[("key", key)])
            .send()
            .await?;
        // GitHub returns 404 when nothing matched the key.
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        let list: CacheList = Self::check(&url, response).await?;
        Ok(list.actions_caches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caches_url_layout() {
        // No reqwest::Client here: TLS client construction requires system
        // CA certs, which do not exist in the Nix build sandbox.
        assert_eq!(
            caches_url("https://api.github.com/", "nix-community/hestia"),
            "https://api.github.com/repos/nix-community/hestia/actions/caches"
        );
    }

    #[test]
    fn cache_list_deserializes_github_response() {
        let json = r#"{
            "total_count": 1,
            "actions_caches": [{
                "id": 505,
                "ref": "refs/heads/main",
                "key": "pack-abc123",
                "version": "73885106f58cc52a7df9ec4d4a5622a5614813162cb516c759a30af6bf56e6f0",
                "last_accessed_at": "2019-01-24T22:45:36.000Z",
                "created_at": "2019-01-24T22:45:36.000Z",
                "size_in_bytes": 1024
            }]
        }"#;
        let list: CacheList = serde_json::from_str(json).unwrap();
        assert_eq!(list.total_count, 1);
        assert_eq!(list.actions_caches[0].key, "pack-abc123");
        assert_eq!(list.actions_caches[0].git_ref, "refs/heads/main");
        assert_eq!(list.actions_caches[0].size_in_bytes, 1024);
    }

    #[test]
    fn usage_deserializes_github_response() {
        let json = r#"{
            "full_name": "nix-community/hestia",
            "active_caches_size_in_bytes": 312329,
            "active_caches_count": 5
        }"#;
        let usage: CacheUsage = serde_json::from_str(json).unwrap();
        assert_eq!(usage.active_caches_count, 5);
        assert_eq!(usage.active_caches_size_in_bytes, 312329);
    }
}
