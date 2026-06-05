//! Twirp client for the GitHub Actions cache v2 ("results") service.
//!
//! Endpoint layout and request/response shapes are ported from
//! tonistiigi/go-actions-cache (`cache_v2.go`):
//!
//! ```text
//! POST {ACTIONS_RESULTS_URL}/twirp/github.actions.results.api.v1.CacheService/<RPC>
//! Authorization: Bearer {ACTIONS_RUNTIME_TOKEN}
//! Content-Type: application/json
//! ```
//!
//! Twirp errors come back as non-2xx responses with a JSON body
//! `{"code": "...", "msg": "..."}`. The `already_exists` code is not an
//! error for us: cache keys are content-addressed, so it means the data is
//! already there (CAS semantics).

use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::gha::{Error, blob};

/// Cache `version` namespace: sha256 of "hestia-2".
///
/// A namespace, not a format version. Changing it orphans every existing
/// cache entry, so it is only bumped on incompatible storage format changes.
pub const CACHE_VERSION: &str = "aa3f0c68abc7983158c10a1be8be9bbd7014211eee928dc266f9f0bb37e7be7a";

const SERVICE_PATH: &str = "twirp/github.actions.results.api.v1.CacheService";

fn rpc_url(base_url: &str, method: &str) -> String {
    format!("{}/{SERVICE_PATH}/{method}", base_url.trim_end_matches('/'))
}

/// Environment variables a real Actions job provides (via the hestia action
/// wrapper; shell steps cannot see them otherwise).
pub const ENV_RESULTS_URL: &str = "ACTIONS_RESULTS_URL";
pub const ENV_RUNTIME_TOKEN: &str = "ACTIONS_RUNTIME_TOKEN";

/// Optional cache namespace salt (benchmarking): a salted daemon sees an
/// empty cache and shares no entries with unsalted daemons. The perf
/// workflow sets this to the run id.
pub const ENV_VERSION_SALT: &str = "HESTIA_CACHE_VERSION_SALT";

/// [`CACHE_VERSION`] when `salt` is empty, sha256("hestia-2:<salt>")
/// otherwise.
fn cache_version(salt: &str) -> String {
    use sha2::{Digest, Sha256};
    if salt.is_empty() {
        return CACHE_VERSION.to_string();
    }
    let digest = Sha256::digest(format!("hestia-2:{salt}"));
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateCacheEntryRequest {
    pub key: String,
    pub version: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CreateCacheEntryResponse {
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub signed_upload_url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FinalizeCacheEntryUploadRequest {
    pub key: String,
    pub size_bytes: u64,
    pub version: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct FinalizeCacheEntryUploadResponse {
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub entry_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetCacheEntryDownloadUrlRequest {
    pub key: String,
    pub restore_keys: Vec<String>,
    pub version: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GetCacheEntryDownloadUrlResponse {
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub signed_download_url: String,
    #[serde(default)]
    pub matched_key: String,
}

/// Twirp wire error body.
///
/// The Twirp spec uses `msg`, but go-actions-cache parses `message`, so
/// accept both.
#[derive(Debug, Serialize, Deserialize)]
pub struct TwirpErrorBody {
    pub code: String,
    #[serde(default, alias = "message")]
    pub msg: String,
}

/// Result of reserving a cache entry for upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reservation {
    /// Key reserved; upload the blob to this pre-signed Azure URL, then call
    /// [`TwirpClient::finalize_upload`].
    Created { upload_url: String },
    /// The key+version already exists (reserved or finalized). For
    /// content-addressed keys this means the data is already present.
    AlreadyExists,
}

/// Result of looking up a cache entry for download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadUrl {
    /// Entry found; `matched_key` is the full key (relevant for prefix
    /// restore-key matches).
    Hit { url: String, matched_key: String },
    /// No entry matches.
    Miss,
}

#[derive(Debug, Clone)]
pub struct TwirpClient {
    http: reqwest::Client,
    base_url: String,
    token: String,
    /// Cache entry `version` sent with every request ([`CACHE_VERSION`]
    /// unless a namespace salt is configured).
    version: String,
}

impl TwirpClient {
    pub fn new(
        http: reqwest::Client,
        results_url: impl Into<String>,
        runtime_token: impl Into<String>,
    ) -> Self {
        Self {
            http,
            base_url: results_url.into(),
            token: runtime_token.into(),
            version: CACHE_VERSION.to_string(),
        }
    }

    /// Build a client from `ACTIONS_RESULTS_URL` / `ACTIONS_RUNTIME_TOKEN`,
    /// honoring the optional `HESTIA_CACHE_VERSION_SALT` namespace salt.
    pub fn from_env(http: reqwest::Client) -> Result<Self, Error> {
        let url = std::env::var(ENV_RESULTS_URL).map_err(|_| Error::MissingEnv(ENV_RESULTS_URL))?;
        let token =
            std::env::var(ENV_RUNTIME_TOKEN).map_err(|_| Error::MissingEnv(ENV_RUNTIME_TOKEN))?;
        if url.is_empty() {
            return Err(Error::MissingEnv(ENV_RESULTS_URL));
        }
        if token.is_empty() {
            return Err(Error::MissingEnv(ENV_RUNTIME_TOKEN));
        }
        let salt = std::env::var(ENV_VERSION_SALT).unwrap_or_default();
        Ok(Self::new(http, url, token).with_version_salt(&salt))
    }

    /// The cache `version` namespace this client writes and reads.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Switch to the cache namespace derived from `salt` (no-op for an
    /// empty salt). See [`ENV_VERSION_SALT`].
    pub fn with_version_salt(mut self, salt: &str) -> Self {
        self.version = cache_version(salt);
        self
    }

    async fn call<Req, Resp>(&self, method: &str, request: &Req) -> Result<Resp, Error>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let url = rpc_url(&self.base_url, method);
        let response = self
            .http
            .post(&url)
            .bearer_auth(&self.token)
            .json(request)
            .send()
            .await?;

        let status = response.status();
        if status.is_success() {
            return Ok(response.json().await?);
        }

        // 401 means the runtime token was rejected (JWTs expire after ~6h).
        // This deserves a dedicated, actionable error: it is the one failure
        // a workflow author can do nothing about except re-run the job.
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(Error::TokenExpired {
                method: method.to_string(),
            });
        }

        let body = response.text().await.unwrap_or_default();
        // Twirp errors are JSON {"code", "msg"}; anything else is unexpected.
        match serde_json::from_str::<TwirpErrorBody>(&body) {
            Ok(twirp_err) if !twirp_err.code.is_empty() => Err(Error::Twirp {
                method: method.to_string(),
                code: twirp_err.code,
                msg: twirp_err.msg,
            }),
            _ => Err(Error::Status {
                status: status.as_u16(),
                url,
                body,
            }),
        }
    }

    /// Reserve `key` for upload (Twirp `CreateCacheEntry`).
    pub async fn create_cache_entry(&self, key: &str) -> Result<Reservation, Error> {
        let request = CreateCacheEntryRequest {
            key: key.to_string(),
            version: self.version.clone(),
        };
        match self
            .call::<_, CreateCacheEntryResponse>("CreateCacheEntry", &request)
            .await
        {
            Ok(response) if response.ok => Ok(Reservation::Created {
                upload_url: response.signed_upload_url,
            }),
            // Some backends signal "exists" with ok=false instead of a Twirp
            // error; treat both the same.
            Ok(_) => Ok(Reservation::AlreadyExists),
            Err(err) if err.is_already_exists() => Ok(Reservation::AlreadyExists),
            Err(err) => Err(err),
        }
    }

    /// Commit an uploaded blob (Twirp `FinalizeCacheEntryUpload`). Returns
    /// the entry id assigned by the service.
    pub async fn finalize_upload(&self, key: &str, size_bytes: u64) -> Result<String, Error> {
        let request = FinalizeCacheEntryUploadRequest {
            key: key.to_string(),
            size_bytes,
            version: self.version.clone(),
        };
        let response: FinalizeCacheEntryUploadResponse =
            self.call("FinalizeCacheEntryUpload", &request).await?;
        if !response.ok {
            return Err(Error::InvalidResponse(format!(
                "FinalizeCacheEntryUpload for {key} returned ok=false"
            )));
        }
        Ok(response.entry_id)
    }

    /// PUT `data` to a reserved entry's upload URL, then finalize it.
    ///
    /// If the SAS URL expires mid-upload, the key is re-reserved once for
    /// a fresh URL. An `AlreadyExists` answer means no fresh URL is coming
    /// and the upload fails.
    pub async fn upload_and_finalize(
        &self,
        http: &reqwest::Client,
        key: &str,
        upload_url: String,
        data: Bytes,
    ) -> Result<(), Error> {
        let size = data.len() as u64;
        blob::put_with_refresh(http, &upload_url, data, async move || {
            match self.create_cache_entry(key).await? {
                Reservation::Created { upload_url } => Ok(upload_url),
                Reservation::AlreadyExists => Err(Error::InvalidResponse(format!(
                    "upload URL for {key:?} expired and cannot be refreshed"
                ))),
            }
        })
        .await?;
        self.finalize_upload(key, size).await?;
        Ok(())
    }

    /// Look up a download URL (Twirp `GetCacheEntryDownloadURL`).
    ///
    /// `restore_keys` are prefix-matched in order; the newest entry matching
    /// a prefix wins (this is how `m#` finds the highest manifest version).
    ///
    /// The key itself is always sent as the first restore key: the real
    /// service ignores the `key` field for matching and only consults
    /// `restore_keys` (verified against the production API — an exact-key
    /// request with empty restore keys misses even for entries that exist;
    /// go-actions-cache sends `RestoreKeys: keys` for the same reason).
    pub async fn get_download_url(
        &self,
        key: &str,
        restore_keys: &[&str],
    ) -> Result<DownloadUrl, Error> {
        let request = GetCacheEntryDownloadUrlRequest {
            key: key.to_string(),
            restore_keys: std::iter::once(key)
                .chain(restore_keys.iter().copied().filter(|&k| k != key))
                .map(String::from)
                .collect(),
            version: self.version.clone(),
        };
        match self
            .call::<_, GetCacheEntryDownloadUrlResponse>("GetCacheEntryDownloadURL", &request)
            .await
        {
            Ok(response) if response.ok => Ok(DownloadUrl::Hit {
                url: response.signed_download_url,
                matched_key: response.matched_key,
            }),
            Ok(_) => Ok(DownloadUrl::Miss),
            // not_found is a miss, not an error.
            Err(Error::Twirp { code, .. }) if code == "not_found" => Ok(DownloadUrl::Miss),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_version_is_sha256_of_hestia_2() {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(b"hestia-2");
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(CACHE_VERSION, hex);
    }

    #[test]
    fn salted_version_differs_per_salt_and_defaults_to_the_constant() {
        assert_eq!(cache_version(""), CACHE_VERSION);

        let a = cache_version("run-1");
        let b = cache_version("run-2");
        assert_ne!(a, CACHE_VERSION);
        assert_ne!(a, b);
        assert_eq!(a, cache_version("run-1"));
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn rpc_url_layout_matches_go_actions_cache() {
        // No reqwest::Client here: TLS client construction requires system
        // CA certs, which do not exist in the Nix build sandbox.
        assert_eq!(
            rpc_url("https://results.example.com/abc/", "CreateCacheEntry"),
            "https://results.example.com/abc/twirp/github.actions.results.api.v1.CacheService/CreateCacheEntry"
        );
    }

    #[test]
    fn request_shapes_serialize_with_snake_case_fields() {
        let request = CreateCacheEntryRequest {
            key: "pack-abc".into(),
            version: CACHE_VERSION.into(),
        };
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["key"], "pack-abc");
        assert_eq!(json["version"], CACHE_VERSION);

        let request = FinalizeCacheEntryUploadRequest {
            key: "pack-abc".into(),
            size_bytes: 42,
            version: CACHE_VERSION.into(),
        };
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["size_bytes"], 42);

        let request = GetCacheEntryDownloadUrlRequest {
            key: "m#".into(),
            restore_keys: vec!["m#".to_string()],
            version: CACHE_VERSION.into(),
        };
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["restore_keys"][0], "m#");
    }

    #[test]
    fn response_shapes_deserialize_from_service_json() {
        let response: CreateCacheEntryResponse =
            serde_json::from_str(r#"{"ok": true, "signed_upload_url": "https://blob/x?sig=1"}"#)
                .unwrap();
        assert!(response.ok);
        assert_eq!(response.signed_upload_url, "https://blob/x?sig=1");

        let response: GetCacheEntryDownloadUrlResponse = serde_json::from_str(
            r#"{"ok": true, "signed_download_url": "https://blob/y", "matched_key": "m#3"}"#,
        )
        .unwrap();
        assert_eq!(response.matched_key, "m#3");

        // Unknown fields must be ignored (forward compatibility).
        let response: FinalizeCacheEntryUploadResponse =
            serde_json::from_str(r#"{"ok": true, "entry_id": "7", "future_field": [1,2]}"#)
                .unwrap();
        assert_eq!(response.entry_id, "7");
    }

    #[test]
    fn twirp_error_body_accepts_msg_and_message() {
        let error: TwirpErrorBody =
            serde_json::from_str(r#"{"code": "already_exists", "msg": "exists"}"#).unwrap();
        assert_eq!(error.code, "already_exists");
        assert_eq!(error.msg, "exists");

        let error: TwirpErrorBody =
            serde_json::from_str(r#"{"code": "already_exists", "message": "exists"}"#).unwrap();
        assert_eq!(error.msg, "exists");

        let parsed = Error::Twirp {
            method: "CreateCacheEntry".into(),
            code: error.code,
            msg: error.msg,
        };
        assert!(parsed.is_already_exists());
    }
}
