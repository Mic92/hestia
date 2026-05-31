//! Azure blob transfers over pre-signed SAS URLs.
//!
//! The Twirp API hands out pre-signed upload/download URLs pointing at Azure
//! Blob Storage. No Azure SDK is needed (PLAN.md, Critical Constraint 5):
//!
//! * Upload: single `PUT` with `x-ms-blob-type: BlockBlob` (works for blobs
//!   up to 5000 MiB).
//! * Download: `GET`, optionally with a `Range` header for chunk reads.
//!
//! SAS URLs expire. On 401/403 the caller-provided refresh callback is asked
//! for a fresh URL (a new Twirp round-trip) and the transfer is retried once.

use std::ops::Range;

use bytes::Bytes;

use crate::gha::Error;

/// `x-ms-blob-type` header value for single-shot uploads.
pub const BLOB_TYPE: &str = "BlockBlob";

/// Azure storage API version header (matches what actions/toolkit sends).
pub const API_VERSION: &str = "2020-04-08";

/// Format a half-open byte range as an HTTP `Range` header value
/// (inclusive on both ends per RFC 9110).
fn format_range(range: &Range<u64>) -> String {
    format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
}

fn url_expired(status: u16) -> bool {
    status == 403 || status == 401
}

async fn status_error(url: &str, response: reqwest::Response) -> Error {
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    Error::Status {
        status,
        url: url.to_string(),
        body,
    }
}

/// Upload `data` to a pre-signed Azure URL with a single PUT.
pub async fn put(http: &reqwest::Client, url: &str, data: Bytes) -> Result<(), Error> {
    let response = http
        .put(url)
        .header("x-ms-blob-type", BLOB_TYPE)
        .header("x-ms-version", API_VERSION)
        .header(reqwest::header::CONTENT_LENGTH, data.len())
        .body(data)
        .send()
        .await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(status_error(url, response).await)
    }
}

/// Download a blob (or a byte range of it) from a pre-signed Azure URL.
pub async fn get(
    http: &reqwest::Client,
    url: &str,
    range: Option<Range<u64>>,
) -> Result<Bytes, Error> {
    let mut request = http.get(url).header("x-ms-version", API_VERSION);
    if let Some(range) = &range {
        request = request.header(reqwest::header::RANGE, format_range(range));
    }
    let response = request.send().await?;
    if response.status().is_success() {
        Ok(response.bytes().await?)
    } else {
        Err(status_error(url, response).await)
    }
}

/// Like [`put`], but when the SAS URL has expired (401/403), ask `refresh`
/// for a fresh URL and retry once.
pub async fn put_with_refresh<F>(
    http: &reqwest::Client,
    url: &str,
    data: Bytes,
    refresh: F,
) -> Result<(), Error>
where
    F: AsyncFnOnce() -> Result<String, Error>,
{
    match put(http, url, data.clone()).await {
        Err(Error::Status { status, .. }) if url_expired(status) => {
            let fresh_url = refresh().await?;
            put(http, &fresh_url, data).await
        }
        result => result,
    }
}

/// Like [`get`], but when the SAS URL has expired (401/403), ask `refresh`
/// for a fresh URL and retry once.
pub async fn get_with_refresh<F>(
    http: &reqwest::Client,
    url: &str,
    range: Option<Range<u64>>,
    refresh: F,
) -> Result<Bytes, Error>
where
    F: AsyncFnOnce() -> Result<String, Error>,
{
    match get(http, url, range.clone()).await {
        Err(Error::Status { status, .. }) if url_expired(status) => {
            let fresh_url = refresh().await?;
            get(http, &fresh_url, range).await
        }
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_header_is_inclusive() {
        assert_eq!(format_range(&(0..1)), "bytes=0-0");
        assert_eq!(format_range(&(100..200)), "bytes=100-199");
        assert_eq!(format_range(&(0..0)), "bytes=0-0"); // degenerate, never sent
    }

    #[test]
    fn expiry_detection_only_matches_auth_failures() {
        assert!(url_expired(403));
        assert!(url_expired(401));
        assert!(!url_expired(404));
        assert!(!url_expired(500));
    }
}
