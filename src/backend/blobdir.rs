//! Any S3-compatible bucket. Content-addressed keys are sharded by their
//! first hash byte, `<prefix>/pack/<xx>/pack-…` and
//! `<prefix>/seg/<xx>/{seg,tree}-…`: on AWS that spreads request rate
//! across prefixes, on stores where a prefix is a directory (MinIO,
//! POSIX gateways, WebDAV) it keeps directories small. Heads are named,
//! not hashed, and every job lists all of them, so `<prefix>/heads/` is
//! flat. Nothing is evicted, listings are complete but may lag, deletes
//! are plain.
//!
//! Given an `https://` URL instead, the bucket is read anonymously through
//! whatever serves it there (a CDN, the website endpoint). Only plain GETs:
//! heads come from `<prefix>/index`, which writers keep up to date, so a
//! public bucket never has to allow anonymous listing.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
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
/// What a CDN in front of the bucket may cache, and for how long. Only
/// heads and the index ever change.
const IMMUTABLE: &str = "public, max-age=31536000, immutable";
const MUTABLE: &str = "public, max-age=30";
const INDEX_ATTEMPTS: u32 = 3;

#[derive(Clone)]
pub struct BlobDir {
    http: reqwest::Client,
    origin: Origin,
    prefix: String,
    /// Heads this backend wrote or deleted since the last `flush`, and
    /// whether they still exist.
    own_heads: Arc<Mutex<BTreeMap<String, bool>>>,
    warned_index: Arc<AtomicBool>,
}

#[derive(Clone)]
enum Origin {
    Bucket(Box<Bucket>, Option<Credentials>),
    /// Bucket root over plain HTTP, read-only.
    Http(Url),
}

/// Object path under the store prefix for a hestia key or listing prefix.
fn object(key: &str) -> String {
    if key == INDEX {
        return INDEX.to_owned();
    }
    let Some((kind, hash)) = key.split_once('-') else {
        return format!("heads/{key}");
    };
    let dir = match kind {
        "pack" => "pack",
        "seg" | "tree" => "seg",
        _ => return format!("heads/{key}"),
    };
    match hash.get(..2) {
        Some(shard) => format!("{dir}/{shard}/{key}"),
        None => format!("{dir}/"),
    }
}

fn key_of(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Content-addressed objects never change, so a CDN may keep them; the
/// index and the heads are the only things a reader must see move.
fn cache_control(key: &str) -> &'static str {
    match is_head(key) || key == INDEX {
        true => MUTABLE,
        false => IMMUTABLE,
    }
}

/// A GC record, a drain head or a compaction head.
fn is_head(key: &str) -> bool {
    matches!(key.split_once('-'), Some(("g" | "h" | "c", rest)) if !rest.is_empty())
}

impl BlobDir {
    /// `url` is `s3://<bucket>/<prefix>`. Without `endpoint` it is AWS
    /// virtual-hosted style, with one path style (MinIO, Garage, R2, ...).
    pub fn s3(
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
        let (origin, prefix) = if url.starts_with("http://") || url.starts_with("https://") {
            let mut root = Url::parse(url).map_err(|e| invalid(e.to_string()))?;
            let prefix = root.path().trim_matches('/').to_owned();
            root.set_path("/");
            root.set_query(None);
            (Origin::Http(root), prefix)
        } else {
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
            let origin = Origin::Bucket(Box::new(bucket), credentials);
            (origin, prefix.trim_matches('/').to_owned())
        };
        Ok(BlobDir {
            http,
            origin,
            prefix,
            own_heads: Arc::default(),
            warned_index: Arc::default(),
        })
    }

    pub fn s3_from_env(url: &str, http: reqwest::Client) -> Result<Self, Error> {
        let var = |k| std::env::var(k).ok().filter(|v: &String| !v.is_empty());
        Self::s3(
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

    /// One request, retried on transient failures. `absent` statuses and
    /// `ok` pass, anything else is an error.
    async fn request(
        &self,
        method: Method,
        url: Url,
        body: Option<(Bytes, &'static str)>,
        headers: &[(HeaderName, String)],
        ok: &[StatusCode],
    ) -> Result<Response, Error> {
        let mut attempt = 0;
        loop {
            let mut b = self.http.request(method.clone(), url.clone());
            if let Some((data, cache)) = &body {
                // reqwest omits Content-Length for an empty body and rustfs
                // then rejects the PUT.
                b = b
                    .header(header::CONTENT_LENGTH, data.len())
                    .header(header::CACHE_CONTROL, *cache)
                    .body(data.clone());
            }
            for (name, value) in headers {
                b = b.header(name, value);
            }
            let result = b.send().await.map_err(Error::Http);
            let transient = match &result {
                Ok(r) => {
                    r.status().is_server_error() || r.status() == StatusCode::TOO_MANY_REQUESTS
                }
                Err(e) => is_transient(e),
            };
            if !transient || attempt == TRANSIENT_RETRIES {
                let r = result?;
                if !ok.contains(&r.status()) && !self.absent().contains(&r.status()) {
                    return Err(status_error(url.as_str(), r).await);
                }
                return Ok(r);
            }
            tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
            attempt += 1;
        }
    }

    /// GET/HEAD/PUT/DELETE on the key's object.
    async fn object(
        &self,
        method: Method,
        key: &str,
        body: Option<Bytes>,
        headers: &[(HeaderName, String)],
        ok: &[StatusCode],
    ) -> Result<Response, Error> {
        let p = self.path(key);
        let url = match &self.origin {
            Origin::Http(root) => root.join(&p).expect("object path is a valid URL path"),
            Origin::Bucket(bucket, c) => {
                let c = c.as_ref();
                match method {
                    Method::PUT => bucket.put_object(c, &p).sign(SIGNATURE_TTL),
                    Method::HEAD => bucket.head_object(c, &p).sign(SIGNATURE_TTL),
                    Method::DELETE => bucket.delete_object(c, &p).sign(SIGNATURE_TTL),
                    _ => bucket.get_object(c, &p).sign(SIGNATURE_TTL),
                }
            }
        };
        let body = body.map(|b| (b, cache_control(key)));
        self.request(method, url, body, headers, ok).await
    }

    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        self.check_writable()?;
        self.object(Method::PUT, key, Some(data), &[], &[StatusCode::OK])
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

    /// An `https://` origin is somebody else's bucket seen through a CDN.
    pub fn writable(&self) -> bool {
        matches!(self.origin, Origin::Bucket(..))
    }

    fn check_writable(&self) -> Result<(), Error> {
        self.writable().then_some(()).ok_or(Error::InvalidEnv {
            name: super::ENV_S3,
            reason: "an http(s):// store is read-only, writing needs s3://".into(),
        })
    }

    /// What a missing key answers. A bucket that grants only GetObject
    /// says 403: telling the two apart would leak whether keys exist.
    fn absent(&self) -> &'static [StatusCode] {
        match self.origin {
            Origin::Bucket(..) => &[StatusCode::NOT_FOUND],
            Origin::Http(_) => &[StatusCode::NOT_FOUND, StatusCode::FORBIDDEN],
        }
    }

    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<Bytes>, Error> {
        debug_assert!(range.as_ref().is_none_or(|r| !r.is_empty()));
        let range_header: Vec<_> = range
            .iter()
            .map(|r| (header::RANGE, format!("bytes={}-{}", r.start, r.end - 1)))
            .collect();
        // 416: a range starting at or past the end of an existing object.
        let ok = [
            StatusCode::OK,
            StatusCode::PARTIAL_CONTENT,
            StatusCode::RANGE_NOT_SATISFIABLE,
        ];
        let r = self
            .object(Method::GET, key, None, &range_header, &ok)
            .await?;
        match r.status() {
            StatusCode::RANGE_NOT_SATISFIABLE => Ok(Some(Bytes::new())),
            // A proxy that drops the Range header hands out the whole
            // object, which callers would read as the range they asked for.
            StatusCode::OK if range.is_some() => Err(Error::InvalidResponse(format!(
                "{key}: a ranged GET was answered with the whole object"
            ))),
            s if self.absent().contains(&s) => Ok(None),
            _ => Ok(Some(r.bytes().await?)),
        }
    }

    pub async fn exists(&self, key: &str) -> Result<bool, Error> {
        let r = self
            .object(Method::HEAD, key, None, &[], &[StatusCode::OK])
            .await?;
        Ok(r.status() == StatusCode::OK)
    }

    /// S3 answers 204 whether or not the key existed.
    pub async fn delete(&self, key: &str) -> Result<bool, Error> {
        self.check_writable()?;
        let ok = [StatusCode::NO_CONTENT, StatusCode::OK];
        let r = self.object(Method::DELETE, key, None, &[], &ok).await?;
        self.remember_head(key, false);
        Ok(!self.absent().contains(&r.status()))
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
        for attempt in 1..=INDEX_ATTEMPTS {
            let precondition: Vec<_> = self.index_precondition().await?.into_iter().collect();
            let listed = self.list("", None).await?.expect("unbounded");
            let mut heads: BTreeMap<&str, bool> =
                listed.iter().map(|l| (l.key.as_str(), true)).collect();
            heads.extend(own.iter().map(|(key, exists)| (key.as_str(), *exists)));
            let body: String = heads
                .iter()
                .filter(|(_, exists)| **exists)
                .map(|(key, _)| format!("{key}\n"))
                .collect();
            let ok = [StatusCode::OK, StatusCode::PRECONDITION_FAILED];
            let r = self
                .object(Method::PUT, INDEX, Some(body.into()), &precondition, &ok)
                .await?;
            if r.status() == StatusCode::OK {
                // Whatever a concurrent writer added meanwhile stays.
                let mut pending = self.own_heads.lock().unwrap();
                pending.retain(|key, exists| own.get(key) != Some(exists));
                return Ok(());
            }
            if attempt == INDEX_ATTEMPTS {
                eprintln!(
                    "hestia: head index at {} changed under us {INDEX_ATTEMPTS} times, leaving it \
                     to the other writer",
                    self.path(INDEX)
                );
            }
        }
        Ok(())
    }

    /// What the index must still look like for our rewrite of it to count.
    /// A store that answers without an `ETag` cannot compare and swap, so
    /// there the last writer simply wins.
    async fn index_precondition(&self) -> Result<Option<(HeaderName, String)>, Error> {
        let current = self
            .object(Method::HEAD, INDEX, None, &[], &[StatusCode::OK])
            .await?;
        if current.status() != StatusCode::OK {
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
                .request(Method::GET, url, None, &[], &[StatusCode::OK])
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
            // The likeliest misconfiguration is a URL that misses the
            // prefix, which would otherwise just look like an empty store.
            if !self.warned_index.swap(true, Ordering::Relaxed) {
                eprintln!(
                    "hestia: no head index at {}, so this store has nothing to serve; writers \
                     publishing to the bucket keep it up to date",
                    self.path(INDEX)
                );
            }
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
            // Anonymous S3 cannot sign a PUT at all.
            return Ok(false);
        }
        let ok = [
            StatusCode::OK,
            StatusCode::FORBIDDEN,
            StatusCode::UNAUTHORIZED,
        ];
        let r = self
            .object(Method::PUT, "x-probe", Some(Bytes::new()), &[], &ok)
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
        assert_eq!(object("seg-0123"), "seg/01/seg-0123");
        assert_eq!(object("tree-0123"), "seg/01/tree-0123");
        assert_eq!(object("seg-"), "seg/");
        assert_eq!(
            object("h-0000000000000001-x-0-y"),
            "heads/h-0000000000000001-x-0-y"
        );
        let s3 = BlobDir::s3(
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
