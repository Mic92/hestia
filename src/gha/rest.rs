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

// ---------------------------------------------------------------------------
// Timestamps
//
// The REST API reports `created_at` / `last_accessed_at` as RFC 3339 UTC
// strings ("2019-01-24T22:45:36.000Z"). GC compares them against unix-second
// clocks, so both directions of the conversion live here (the fake GHA
// backend uses the formatter so tests exercise the same parser as
// production). Calendar math follows Howard Hinnant's civil-date algorithms;
// no chrono/time dependency needed for one fixed format.
// ---------------------------------------------------------------------------

/// Days since 1970-01-01 for a proleptic Gregorian calendar date.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = (year - era * 400) as u64;
    let month_shifted = (month + 9) % 12;
    let day_of_year = (153 * month_shifted + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year as u64;
    era * 146_097 + day_of_era as i64 - 719_468
}

/// Inverse of [`days_from_civil`].
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = (days - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_shifted = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_shifted + 2) / 5 + 1) as u32;
    let month = if month_shifted < 10 {
        month_shifted + 3
    } else {
        month_shifted - 9
    } as u32;
    let year = year_of_era as i64 + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Parse an RFC 3339 UTC timestamp (the format the GitHub REST API emits)
/// into unix seconds. Fractional seconds are accepted and ignored. Returns
/// `None` for non-UTC offsets or malformed input.
pub fn parse_timestamp(s: &str) -> Option<u64> {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || (bytes[10] != b'T' && bytes[10] != b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    let hour: u64 = s.get(11..13)?.parse().ok()?;
    let minute: u64 = s.get(14..16)?.parse().ok()?;
    let second: u64 = s.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 {
        return None;
    }
    // Optional fractional seconds, then a UTC marker.
    let mut rest = &s[19..];
    if let Some(stripped) = rest.strip_prefix('.') {
        rest = stripped.trim_start_matches(|c: char| c.is_ascii_digit());
    }
    if rest != "Z" && rest != "z" && rest != "+00:00" {
        return None;
    }
    // Leap seconds (second == 60) clamp to 59.
    let second = second.min(59);
    let days = days_from_civil(year, month, day);
    let seconds = days.checked_mul(86_400)? + (hour * 3600 + minute * 60 + second) as i64;
    u64::try_from(seconds).ok()
}

/// Format unix seconds as an RFC 3339 UTC timestamp
/// (`YYYY-MM-DDTHH:MM:SSZ`), the inverse of [`parse_timestamp`].
pub fn format_timestamp(seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let in_day = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        in_day / 3600,
        (in_day % 3600) / 60,
        in_day % 60
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

impl CacheEntry {
    /// `created_at` as unix seconds (`None` if unparsable).
    pub fn created_unix(&self) -> Option<u64> {
        parse_timestamp(&self.created_at)
    }

    /// `last_accessed_at` as unix seconds (`None` if unparsable).
    pub fn last_accessed_unix(&self) -> Option<u64> {
        parse_timestamp(&self.last_accessed_at)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheList {
    pub total_count: u64,
    #[serde(default)]
    pub actions_caches: Vec<CacheEntry>,
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
    fn timestamp_parses_github_rest_format() {
        // Example straight from the GitHub REST API documentation.
        assert_eq!(
            parse_timestamp("2019-01-24T22:45:36.000Z"),
            Some(1_548_369_936)
        );
        // Without fractional seconds.
        assert_eq!(parse_timestamp("2019-01-24T22:45:36Z"), Some(1_548_369_936));
        // Explicit UTC offset.
        assert_eq!(
            parse_timestamp("2019-01-24T22:45:36+00:00"),
            Some(1_548_369_936)
        );
        // Epoch and a leap-year date.
        assert_eq!(parse_timestamp("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_timestamp("2024-02-29T12:00:00Z"), Some(1_709_208_000));
        // Garbage.
        assert_eq!(parse_timestamp(""), None);
        assert_eq!(parse_timestamp("not a timestamp"), None);
        assert_eq!(parse_timestamp("2019-13-24T22:45:36Z"), None);
        assert_eq!(parse_timestamp("2019-01-24T22:45:36+02:00"), None);
    }

    #[test]
    fn timestamp_format_parse_round_trip() {
        for seconds in [
            0u64,
            1,
            86_399,
            86_400,
            1_548_369_936,
            1_709_208_000,
            4_102_444_799,
        ] {
            let formatted = format_timestamp(seconds);
            assert_eq!(
                parse_timestamp(&formatted),
                Some(seconds),
                "round trip failed for {seconds} ({formatted})"
            );
        }
        assert_eq!(format_timestamp(1_548_369_936), "2019-01-24T22:45:36Z");
    }

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
}
