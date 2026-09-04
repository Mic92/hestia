//! Any S3-compatible bucket. Keys map to `<prefix>/pack/<xx>/pack-…`
//! (first hash byte spreads request rate across key prefixes),
//! `<prefix>/seg/{seg,tree}-…` and `<prefix>/heads/<head>`. Nothing is
//! evicted, listings are complete but may lag, deletes are plain.
//!
//! Given an `https://` URL instead, the bucket is read anonymously through
//! whatever serves it there (a CDN, the website endpoint). Only plain GETs:
//! heads come from `<prefix>/index`, which writers keep up to date, so a
//! public bucket never has to allow anonymous listing.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::HeaderName;
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

/// Head names for readers that cannot list, one per line.
const INDEX: &str = "index";
const INDEX_ATTEMPTS: u32 = 3;

#[derive(Clone)]
pub struct S3 {
    http: reqwest::Client,
    origin: Origin,
    prefix: String,
    /// Heads this backend wrote or deleted since the last `flush`, and
    /// whether they still exist.
    own_heads: Arc<Mutex<BTreeMap<String, bool>>>,
}

#[derive(Clone)]
enum Origin {
    Bucket(Box<Bucket>, Option<Credentials>),
    /// Bucket root over plain HTTP, read-only.
    Http(Url),
}

/// Object path under the store prefix for a hestia key or listing prefix.
fn object(key: &str) -> String {
    match key.split_once('-') {
        _ if key == INDEX => INDEX.to_owned(),
        Some(("pack", h)) if h.len() >= 2 => format!("pack/{}/{key}", &h[..2]),
        Some(("pack", _)) => "pack/".to_owned(),
        Some(("seg" | "tree", _)) => format!("seg/{key}"),
        _ => format!("heads/{key}"),
    }
}

fn key_of(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// A GC record, a drain head or a compaction head.
fn is_head(key: &str) -> bool {
    matches!(key.split_once('-'), Some(("g" | "h" | "c", rest)) if !rest.is_empty())
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
        if url.starts_with("http://") || url.starts_with("https://") {
            let mut root = Url::parse(url).map_err(|e| invalid(e.to_string()))?;
            let prefix = root.path().trim_matches('/').to_owned();
            root.set_path("/");
            root.set_query(None);
            return Ok(S3 {
                http,
                origin: Origin::Http(root),
                prefix,
                own_heads: Arc::default(),
            });
        }
        let rest = url
            .strip_prefix("s3://")
            .filter(|r| !r.is_empty() && !r.starts_with('/'))
            .ok_or_else(|| {
                invalid("want s3://<bucket>/<prefix> or https://<host>/<prefix>".into())
            })?;
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
            origin: Origin::Bucket(Box::new(bucket), credentials),
            prefix: prefix.trim_matches('/').to_owned(),
            own_heads: Arc::default(),
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
        let url = self.url(&method, key);
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

    fn url(&self, method: &Method, key: &str) -> Url {
        let p = self.path(key);
        match &self.origin {
            Origin::Http(root) => root.join(&p).expect("object path is a valid URL path"),
            Origin::Bucket(bucket, c) => {
                let c = c.as_ref();
                match *method {
                    Method::PUT => bucket.put_object(c, &p).sign(SIGNATURE_TTL),
                    Method::HEAD => bucket.head_object(c, &p).sign(SIGNATURE_TTL),
                    Method::DELETE => bucket.delete_object(c, &p).sign(SIGNATURE_TTL),
                    _ => bucket.get_object(c, &p).sign(SIGNATURE_TTL),
                }
            }
        }
    }

    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        self.writable()?;
        self.object(Method::PUT, key, data, None, &[StatusCode::OK])
            .await?;
        self.remember_head(key, true);
        Ok(true)
    }

    fn remember_head(&self, key: &str, exists: bool) {
        if is_head(key) {
            self.own_heads
                .lock()
                .unwrap()
                .insert(key.to_owned(), exists);
        }
    }

    pub fn is_http(&self) -> bool {
        matches!(self.origin, Origin::Http(_))
    }

    fn writable(&self) -> Result<(), Error> {
        match self.origin {
            Origin::Bucket(..) => Ok(()),
            Origin::Http(_) => Err(Error::InvalidEnv {
                name: super::ENV_S3,
                reason: "an http(s):// store is read-only, writing needs s3://".into(),
            }),
        }
    }

    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<Bytes>, Error> {
        if range.as_ref().is_some_and(|r| r.is_empty()) {
            return Ok(self.exists(key).await?.then(Bytes::new));
        }
        let mut ok = self.absent_statuses();
        ok.extend([
            StatusCode::OK,
            StatusCode::PARTIAL_CONTENT,
            // A range starting at or past the end of an existing object.
            StatusCode::RANGE_NOT_SATISFIABLE,
        ]);
        let r = self
            .object(Method::GET, key, Bytes::new(), range.as_ref(), &ok)
            .await?;
        match r.status() {
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(Some(Bytes::new())),
            // A proxy that drops the Range header hands out the whole
            // object, which callers would read as the range they asked for.
            StatusCode::OK if range.is_some() => Err(Error::InvalidResponse(format!(
                "{key}: a ranged GET was answered with the whole object"
            ))),
            s if self.absent_statuses().contains(&s) => Ok(None),
            _ => Ok(Some(r.bytes().await?)),
        }
    }

    /// A bucket that grants only GetObject answers 403 for keys that are
    /// not there: telling the two apart would leak whether they exist.
    fn absent_statuses(&self) -> Vec<StatusCode> {
        match self.origin {
            Origin::Bucket(..) => vec![StatusCode::NOT_FOUND],
            Origin::Http(_) => vec![StatusCode::NOT_FOUND, StatusCode::FORBIDDEN],
        }
    }

    pub async fn exists(&self, key: &str) -> Result<bool, Error> {
        let mut ok = self.absent_statuses();
        ok.push(StatusCode::OK);
        let r = self
            .object(Method::HEAD, key, Bytes::new(), None, &ok)
            .await?;
        Ok(r.status() == StatusCode::OK)
    }

    /// S3 answers 204 whether or not the key existed.
    pub async fn delete(&self, key: &str) -> Result<bool, Error> {
        self.writable()?;
        let ok = [
            StatusCode::NO_CONTENT,
            StatusCode::OK,
            StatusCode::NOT_FOUND,
        ];
        let r = self
            .object(Method::DELETE, key, Bytes::new(), None, &ok)
            .await?;
        self.remember_head(key, false);
        Ok(r.status() != StatusCode::NOT_FOUND)
    }

    /// Rewrite the head index if this backend changed a head. The body is
    /// a fresh listing, so a lost update only costs the writers a retry:
    /// whoever wins last has listed everything the losers wrote. Listings
    /// lag on some stores, so our own writes are merged over it.
    pub async fn flush(&self) -> Result<(), Error> {
        let own = self.own_heads.lock().unwrap().clone();
        if own.is_empty() {
            return Ok(());
        }
        for _ in 0..INDEX_ATTEMPTS {
            let precondition = self.index_precondition().await?;
            let listed = self.list("", None).await?.expect("unbounded");
            let mut heads: BTreeMap<&str, bool> =
                listed.iter().map(|l| (l.key.as_str(), true)).collect();
            heads.extend(own.iter().map(|(key, exists)| (key.as_str(), *exists)));
            let body: String = heads
                .iter()
                .filter(|(_, exists)| **exists)
                .map(|(key, _)| format!("{key}\n"))
                .collect();
            let url = self.url(&Method::PUT, INDEX);
            let r = self
                .send(
                    url,
                    |u| {
                        let mut b = self
                            .http
                            .put(u.clone())
                            .header(header::CONTENT_LENGTH, body.len())
                            .body(body.clone());
                        if let Some((name, value)) = &precondition {
                            b = b.header(name.clone(), value.clone());
                        }
                        b
                    },
                    &[StatusCode::OK, StatusCode::PRECONDITION_FAILED],
                )
                .await?;
            if r.status() == StatusCode::OK {
                // Whatever a concurrent writer added meanwhile stays.
                let mut pending = self.own_heads.lock().unwrap();
                pending.retain(|key, exists| own.get(key) != Some(exists));
                break;
            }
        }
        Ok(())
    }

    /// What the index must still look like for our rewrite of it to count.
    /// A store that answers without an `ETag` cannot compare and swap, so
    /// there the last writer simply wins.
    async fn index_precondition(&self) -> Result<Option<(HeaderName, String)>, Error> {
        let ok = [StatusCode::OK, StatusCode::NOT_FOUND];
        let current = self
            .object(Method::HEAD, INDEX, Bytes::new(), None, &ok)
            .await?;
        if current.status() == StatusCode::NOT_FOUND {
            return Ok(Some((header::IF_NONE_MATCH, "*".to_owned())));
        }
        Ok(current
            .headers()
            .get(header::ETAG)
            .and_then(|e| e.to_str().ok())
            .map(|etag| (header::IF_MATCH, etag.to_owned())))
    }

    /// The empty prefix lists `heads/`.
    pub async fn list(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Option<Vec<Listed>>, Error> {
        match &self.origin {
            Origin::Http(_) => self.list_index(prefix).await,
            Origin::Bucket(bucket, c) => self.list_bucket(bucket, c.as_ref(), prefix, limit).await,
        }
    }

    async fn list_bucket(
        &self,
        bucket: &Bucket,
        credentials: Option<&Credentials>,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Option<Vec<Listed>>, Error> {
        let full = self.path(prefix);
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut action = ListObjectsV2::new(bucket, credentials);
            action.with_prefix(full.as_str());
            if let Some(t) = &token {
                action.with_continuation_token(t.as_str());
            }
            let url = action.sign(SIGNATURE_TTL);
            let r = self
                .send(url, |u| self.http.get(u.clone()), &[StatusCode::OK])
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

    /// Heads from the index. Other kinds are unknown to it, which is what
    /// `None` says: a listing that cannot prove absence.
    async fn list_index(&self, prefix: &str) -> Result<Option<Vec<Listed>>, Error> {
        if !prefix.is_empty() && !matches!(prefix, "g-" | "h-" | "c-") {
            return Ok(None);
        }
        let Some(body) = self.get(INDEX, None).await? else {
            return Ok(Some(Vec::new()));
        };
        let body = String::from_utf8(body.to_vec())
            .map_err(|e| Error::InvalidResponse(format!("head index: {e}")))?;
        Ok(Some(
            body.lines()
                .filter(|key| key.starts_with(prefix) && is_head(key))
                .map(|key| Listed {
                    key: key.to_owned(),
                    created: None,
                    last_accessed: None,
                })
                .collect(),
        ))
    }

    pub async fn probe_writable(&self) -> Result<bool, Error> {
        if !matches!(self.origin, Origin::Bucket(_, Some(_))) {
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
