//! Any S3-compatible bucket. Keys map to `<prefix>/pack/<xx>/pack-…`
//! (first hash byte spreads request rate across key prefixes),
//! `<prefix>/seg/{seg,tree}-…` and `<prefix>/heads/<head>`. Nothing is
//! evicted, listings are complete but may lag, deletes are plain.

use std::ops::Range;
use std::time::Duration;

use bytes::Bytes;
use reqwest::{Method, Response, StatusCode, Url, header};
use rusty_s3::actions::{ListObjectsV2, S3Action};
use rusty_s3::{Bucket, Credentials, UrlStyle};

use super::{Error, Listed};
use crate::gha::blob::{is_transient, status_error};
use crate::gha::rest::parse_timestamp;

pub const ENV_S3_ENDPOINT: &str = "HESTIA_S3_ENDPOINT";
pub const ENV_S3_REGION: &str = "AWS_REGION";
const SIGNATURE_TTL: Duration = Duration::from_secs(3600);
const TRANSIENT_RETRIES: u32 = 4;

#[derive(Clone)]
pub struct S3 {
    http: reqwest::Client,
    bucket: Bucket,
    prefix: String,
    credentials: Option<Credentials>,
}

/// Object path under the store prefix for a hestia key or listing prefix.
fn object(key: &str) -> String {
    match key.split_once('-') {
        Some(("pack", h)) if h.len() >= 2 => format!("pack/{}/{key}", &h[..2]),
        Some(("pack", _)) => "pack/".to_owned(),
        Some(("seg" | "tree", _)) => format!("seg/{key}"),
        _ => format!("heads/{key}"),
    }
}

fn key_of(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

impl S3 {
    /// `url` is `s3://<bucket>/<prefix>`. Without `endpoint` it is AWS
    /// virtual-hosted style, with one path style (MinIO, Garage, R2, ...).
    pub fn new(
        url: &str,
        endpoint: Option<&str>,
        region: &str,
        credentials: Option<Credentials>,
        http: reqwest::Client,
    ) -> Result<Self, Error> {
        let invalid = |reason: String| Error::InvalidEnv {
            name: super::ENV_S3,
            reason,
        };
        let rest = url
            .strip_prefix("s3://")
            .filter(|r| !r.is_empty() && !r.starts_with('/'))
            .ok_or_else(|| invalid("want s3://<bucket>/<prefix>".into()))?;
        let (name, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let (endpoint, style) = match endpoint {
            Some(e) => (e.to_owned(), UrlStyle::Path),
            None => (
                format!("https://s3.{region}.amazonaws.com"),
                UrlStyle::VirtualHost,
            ),
        };
        let endpoint = Url::parse(&endpoint).map_err(|e| invalid(e.to_string()))?;
        let bucket = Bucket::new(endpoint, style, name.to_owned(), region.to_owned())
            .map_err(|e| invalid(e.to_string()))?;
        Ok(S3 {
            http,
            bucket,
            prefix: prefix.trim_matches('/').to_owned(),
            credentials,
        })
    }

    pub fn from_env(url: &str, http: reqwest::Client) -> Result<Self, Error> {
        let var = |k| std::env::var(k).ok().filter(|v: &String| !v.is_empty());
        Self::new(
            url,
            var(ENV_S3_ENDPOINT).as_deref(),
            &var(ENV_S3_REGION).unwrap_or_else(|| "us-east-1".to_owned()),
            Credentials::from_env(),
            http,
        )
    }

    fn path(&self, key: &str) -> String {
        match self.prefix.as_str() {
            "" => object(key),
            p => format!("{p}/{}", object(key)),
        }
    }

    /// Sends a presigned request, retrying transient failures. Any status
    /// not in `ok` is an error.
    async fn send(
        &self,
        url: Url,
        build: impl Fn(&Url) -> reqwest::RequestBuilder,
        ok: &[StatusCode],
    ) -> Result<Response, Error> {
        let mut attempt = 0;
        loop {
            let result = build(&url).send().await.map_err(Error::Http);
            let transient = match &result {
                Ok(r) => {
                    r.status().is_server_error() || r.status() == StatusCode::TOO_MANY_REQUESTS
                }
                Err(e) => is_transient(e),
            };
            if !transient || attempt == TRANSIENT_RETRIES {
                let r = result?;
                if !ok.contains(&r.status()) {
                    return Err(status_error(url.as_str(), r).await);
                }
                return Ok(r);
            }
            tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
            attempt += 1;
        }
    }

    /// One object request: GET/HEAD/PUT/DELETE on the key's path.
    async fn object(
        &self,
        method: Method,
        key: &str,
        body: Bytes,
        range: Option<&Range<u64>>,
        ok: &[StatusCode],
    ) -> Result<Response, Error> {
        let (c, p) = (self.credentials.as_ref(), self.path(key));
        let url = match method {
            Method::PUT => self.bucket.put_object(c, &p).sign(SIGNATURE_TTL),
            Method::HEAD => self.bucket.head_object(c, &p).sign(SIGNATURE_TTL),
            Method::DELETE => self.bucket.delete_object(c, &p).sign(SIGNATURE_TTL),
            _ => self.bucket.get_object(c, &p).sign(SIGNATURE_TTL),
        };
        self.send(
            url,
            |u| {
                let mut b = self.http.request(method.clone(), u.clone());
                // reqwest omits Content-Length for an empty body and rustfs
                // then rejects the PUT.
                if method == Method::PUT {
                    b = b
                        .header(header::CONTENT_LENGTH, body.len())
                        .body(body.clone());
                }
                if let Some(r) = range {
                    b = b.header(header::RANGE, format!("bytes={}-{}", r.start, r.end - 1));
                }
                b
            },
            ok,
        )
        .await
    }

    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        self.object(Method::PUT, key, data, None, &[StatusCode::OK])
            .await?;
        Ok(true)
    }

    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<Bytes>, Error> {
        if range.as_ref().is_some_and(|r| r.is_empty()) {
            return Ok(self.exists(key).await?.then(Bytes::new));
        }
        let ok = [
            StatusCode::OK,
            StatusCode::PARTIAL_CONTENT,
            StatusCode::NOT_FOUND,
            // A range starting at or past the end of an existing object.
            StatusCode::RANGE_NOT_SATISFIABLE,
        ];
        let r = self
            .object(Method::GET, key, Bytes::new(), range.as_ref(), &ok)
            .await?;
        match r.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(Some(Bytes::new())),
            _ => Ok(Some(r.bytes().await?)),
        }
    }

    pub async fn exists(&self, key: &str) -> Result<bool, Error> {
        let ok = [StatusCode::OK, StatusCode::NOT_FOUND];
        let r = self
            .object(Method::HEAD, key, Bytes::new(), None, &ok)
            .await?;
        Ok(r.status() == StatusCode::OK)
    }

    /// S3 answers 204 whether or not the key existed.
    pub async fn delete(&self, key: &str) -> Result<bool, Error> {
        let ok = [
            StatusCode::NO_CONTENT,
            StatusCode::OK,
            StatusCode::NOT_FOUND,
        ];
        let r = self
            .object(Method::DELETE, key, Bytes::new(), None, &ok)
            .await?;
        Ok(r.status() != StatusCode::NOT_FOUND)
    }

    /// The empty prefix lists `heads/`.
    pub async fn list(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Option<Vec<Listed>>, Error> {
        let full = self.path(prefix);
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut action = ListObjectsV2::new(&self.bucket, self.credentials.as_ref());
            action.with_prefix(full.as_str());
            if let Some(t) = &token {
                action.with_continuation_token(t.as_str());
            }
            let r = self
                .send(
                    action.sign(SIGNATURE_TTL),
                    |u| self.http.get(u.clone()),
                    &[StatusCode::OK],
                )
                .await?;
            let page = ListObjectsV2::parse_response(&r.text().await?)
                .map_err(|e| Error::InvalidResponse(format!("ListObjectsV2: {e}")))?;
            out.extend(page.contents.into_iter().filter_map(|o| {
                let key = key_of(&o.key);
                key.starts_with(prefix).then(|| Listed {
                    key: key.to_owned(),
                    created: parse_timestamp(&o.last_modified),
                    last_accessed: None,
                })
            }));
            if limit.is_some_and(|l| out.len() as u64 > l) {
                return Ok(None);
            }
            match page.next_continuation_token {
                Some(t) => token = Some(t),
                None => return Ok(Some(out)),
            }
        }
    }

    pub async fn probe_writable(&self) -> Result<bool, Error> {
        if self.credentials.is_none() {
            return Ok(false);
        }
        let ok = [
            StatusCode::OK,
            StatusCode::FORBIDDEN,
            StatusCode::UNAUTHORIZED,
        ];
        let r = self
            .object(Method::PUT, "x-probe", Bytes::new(), None, &ok)
            .await?;
        if r.status() != StatusCode::OK {
            return Ok(false);
        }
        self.delete("x-probe").await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_map_to_sharded_paths() {
        assert_eq!(object("pack-abcdef"), "pack/ab/pack-abcdef");
        assert_eq!(object("pack-"), "pack/");
        assert_eq!(object("seg-0123"), "seg/seg-0123");
        assert_eq!(object("tree-0123"), "seg/tree-0123");
        assert_eq!(
            object("h-0000000000000001-x-0-y"),
            "heads/h-0000000000000001-x-0-y"
        );
        let s3 = S3::new(
            "s3://b/store/",
            Some("http://127.0.0.1:9000"),
            "r",
            None,
            reqwest::Client::new(),
        )
        .unwrap();
        assert_eq!(s3.path("g-1"), "store/heads/g-1");
        assert_eq!(key_of("store/pack/ab/pack-abcdef"), "pack-abcdef");
    }
}
