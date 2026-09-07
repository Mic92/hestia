//! Objects as files under a prefix, over three transports: an S3 bucket
//! (presigned requests, ListObjectsV2), a WebDAV share (Basic auth,
//! PROPFIND, MKCOL), or read-only plain HTTP through whatever serves the
//! same tree (a CDN, a bucket website endpoint, the share minus auth).
//!
//! Content-addressed keys are sharded by their first hash byte,
//! `<prefix>/pack/<xx>/pack-…` and `<prefix>/seg/<xx>/{seg,tree}-…`: on
//! AWS that spreads request rate across prefixes, where a prefix is a
//! real directory (MinIO, POSIX gateways, WebDAV) it keeps listings
//! small. Heads are named, not hashed, and every job lists all of them,
//! so `<prefix>/heads/` is flat. Writers also keep `<prefix>/index`, the
//! head names one per line, so plain-HTTP readers need no listing.
//! Nothing is evicted, listings are complete but may lag.

use std::collections::{BTreeMap, HashSet};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use reqwest::header::HeaderName;
use reqwest::{Method, Response, StatusCode, Url, header};
use rusty_s3::actions::{ListObjectsV2, S3Action};
use rusty_s3::{Bucket, Credentials, UrlStyle};

use super::{Error, Listed};
use crate::gha::blob::{is_transient, status_error};
use crate::gha::rest::parse_timestamp;

pub const ENV_S3_ENDPOINT: &str = "HESTIA_S3_ENDPOINT";
pub const ENV_S3_REGION: &str = "AWS_REGION";
pub const ENV_DAV_USER: &str = "HESTIA_DAV_USER";
pub const ENV_DAV_PASSWORD: &str = "HESTIA_DAV_PASSWORD";
const SIGNATURE_TTL: Duration = Duration::from_secs(3600);
const TRANSIENT_RETRIES: u32 = 4;
/// Shard directories listed in flight during a WebDAV GC listing.
const PROPFIND_CONCURRENCY: usize = 32;

/// Head names for readers that cannot list, one per line.
const INDEX: &str = "index";
/// What a CDN in front of the store may cache, and for how long. Only
/// heads and the index ever change.
const IMMUTABLE: &str = "public, max-age=31536000, immutable";
const MUTABLE: &str = "public, max-age=30";
const INDEX_ATTEMPTS: u32 = 3;

#[derive(Clone)]
pub struct BlobDir {
    http: reqwest::Client,
    origin: Origin,
    env: &'static str,
    prefix: String,
    /// Heads this backend wrote or deleted since the last `flush`, and
    /// whether they still exist.
    own_heads: Arc<Mutex<BTreeMap<String, bool>>>,
    warned_index: Arc<AtomicBool>,
}

#[derive(Clone)]
enum Origin {
    S3(Box<Bucket>, Option<Credentials>),
    /// Store root over plain HTTP, read-only.
    Http(Url),
    Dav {
        root: Url,
        auth: Option<(String, String)>,
        /// Collections known to exist, so MKCOL runs once per directory.
        /// Async so concurrent uploads into one new shard wait for the
        /// first MKCOL instead of each issuing their own.
        dirs: Arc<tokio::sync::Mutex<HashSet<String>>>,
    },
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

/// Splits `scheme://[user:pass@]host/prefix` into a root URL and the prefix.
fn split_url(url: &str, invalid: impl Fn(String) -> Error) -> Result<(Url, String), Error> {
    let mut root = Url::parse(url).map_err(|e| invalid(e.to_string()))?;
    let prefix = root.path().trim_matches('/').to_owned();
    root.set_path("/");
    root.set_query(None);
    Ok((root, prefix))
}

impl BlobDir {
    /// `s3://<bucket>/<prefix>`: without `endpoint` AWS virtual-hosted
    /// style, with one path style (MinIO, Garage, R2, ...). `https://…`:
    /// the same tree read-only over plain HTTP.
    pub fn s3(
        url: &str,
        endpoint: Option<&str>,
        region: &str,
        credentials: Option<Credentials>,
        http: reqwest::Client,
    ) -> Result<Self, Error> {
        let env = super::ENV_S3;
        let invalid = |reason: String| Error::InvalidEnv { name: env, reason };
        if url.starts_with("http://") || url.starts_with("https://") {
            let (root, prefix) = split_url(url, invalid)?;
            return Ok(Self::with(http, Origin::Http(root), env, prefix));
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
        let origin = Origin::S3(Box::new(bucket), credentials);
        Ok(Self::with(
            http,
            origin,
            env,
            prefix.trim_matches('/').to_owned(),
        ))
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

    /// `http[s]://[user:password@]host/<prefix>`, a WebDAV collection.
    pub fn dav(
        url: &str,
        auth: Option<(String, String)>,
        http: reqwest::Client,
    ) -> Result<Self, Error> {
        let env = super::ENV_DAV;
        let invalid = |reason: String| Error::InvalidEnv { name: env, reason };
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(invalid("want http(s)://<host>/<prefix>".into()));
        }
        let (mut root, prefix) = split_url(url, invalid)?;
        let from_url = match (root.username(), root.password()) {
            ("", _) => None,
            (u, p) => Some((percent_decode(u), percent_decode(p.unwrap_or_default()))),
        };
        let _ = root.set_username("");
        let _ = root.set_password(None);
        let origin = Origin::Dav {
            root,
            auth: auth.or(from_url),
            dirs: Arc::default(),
        };
        Ok(Self::with(http, origin, env, prefix))
    }

    pub fn dav_from_env(url: &str, http: reqwest::Client) -> Result<Self, Error> {
        let var = |k| std::env::var(k).ok().filter(|v: &String| !v.is_empty());
        let auth = var(ENV_DAV_USER).map(|u| (u, var(ENV_DAV_PASSWORD).unwrap_or_default()));
        Self::dav(url, auth, http)
    }

    fn with(http: reqwest::Client, origin: Origin, env: &'static str, prefix: String) -> Self {
        BlobDir {
            http,
            origin,
            env,
            prefix,
            own_heads: Arc::default(),
            warned_index: Arc::default(),
        }
    }

    fn path(&self, rel: &str) -> String {
        match self.prefix.as_str() {
            "" => rel.to_owned(),
            p => format!("{p}/{rel}"),
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
            if let Origin::Dav {
                auth: Some((u, p)), ..
            } = &self.origin
            {
                b = b.basic_auth(u, Some(p));
            }
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
                    !ok.contains(&r.status())
                        && (r.status().is_server_error()
                            || r.status() == StatusCode::TOO_MANY_REQUESTS)
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

    fn url(&self, method: &Method, rel: &str) -> Url {
        let p = self.path(rel);
        match &self.origin {
            Origin::Http(root) | Origin::Dav { root, .. } => {
                root.join(&p).expect("object path is a valid URL path")
            }
            Origin::S3(bucket, c) => {
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

    /// GET/HEAD/PUT/DELETE on the key's object.
    async fn object(
        &self,
        method: Method,
        key: &str,
        body: Option<Bytes>,
        headers: &[(HeaderName, String)],
        ok: &[StatusCode],
    ) -> Result<Response, Error> {
        let url = self.url(&method, &object(key));
        let body = body.map(|b| (b, cache_control(key)));
        self.request(method, url, body, headers, ok).await
    }

    /// `false` if a create-once PUT found the key already there.
    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        // Hardening only: S3 stores without conditional PUT and nginx
        // ignore it and overwrite with the same bytes.
        let once: Vec<_> = (!is_head(key) && key != INDEX)
            .then(|| (header::IF_NONE_MATCH, "*".to_owned()))
            .into_iter()
            .collect();
        let r = self.put_object(key, data, &once).await?;
        self.remember_head(key, true);
        Ok(r.status() != StatusCode::PRECONDITION_FAILED)
    }

    async fn put_object(
        &self,
        key: &str,
        data: Bytes,
        headers: &[(HeaderName, String)],
    ) -> Result<Response, Error> {
        self.check_writable()?;
        self.make_parents(&object(key)).await?;
        let ok = [
            StatusCode::OK,
            StatusCode::CREATED,
            StatusCode::NO_CONTENT,
            StatusCode::PRECONDITION_FAILED,
        ];
        let r = self
            .object(Method::PUT, key, Some(data), headers, &ok)
            .await?;
        match self.absent().contains(&r.status()) {
            true => Err(status_error(self.url(&Method::PUT, &object(key)).as_str(), r).await),
            false => Ok(r),
        }
    }

    /// WebDAV refuses a PUT below a missing collection (409, nginx: 500),
    /// so MKCOL every directory between the share and `rel` once per
    /// process. The collection HESTIA_DAV names is the operator's.
    async fn make_parents(&self, rel: &str) -> Result<(), Error> {
        let Origin::Dav { root, dirs, .. } = &self.origin else {
            return Ok(());
        };
        let Some((dir_path, _)) = rel.rsplit_once('/') else {
            return Ok(());
        };
        let mut known = dirs.lock().await;
        let mut dir = self.path("");
        for part in dir_path.split('/') {
            dir.push_str(part);
            dir.push('/');
            if known.contains(&dir) {
                continue;
            }
            // nginx wants the trailing slash, 405 means it exists.
            let url = root
                .join(&dir)
                .expect("collection path is a valid URL path");
            let ok = [
                StatusCode::CREATED,
                StatusCode::METHOD_NOT_ALLOWED,
                StatusCode::MOVED_PERMANENTLY,
            ];
            self.request(Method::from_bytes(b"MKCOL").unwrap(), url, None, &[], &ok)
                .await?;
            known.insert(dir.clone());
        }
        Ok(())
    }

    fn remember_head(&self, key: &str, exists: bool) {
        if is_head(key) {
            self.own_heads
                .lock()
                .unwrap()
                .insert(key.to_owned(), exists);
        }
    }

    /// An `https://` S3 origin is somebody else's bucket seen through a CDN.
    pub fn writable(&self) -> bool {
        !matches!(self.origin, Origin::Http(_))
    }

    fn check_writable(&self) -> Result<(), Error> {
        self.writable().then_some(()).ok_or(Error::InvalidEnv {
            name: self.env,
            reason: "an http(s):// store is read-only, writing needs s3:// or HESTIA_DAV".into(),
        })
    }

    /// What a missing key answers. A bucket that grants only GetObject
    /// says 403: telling the two apart would leak whether keys exist.
    fn absent(&self) -> &'static [StatusCode] {
        match self.origin {
            Origin::Http(_) => &[StatusCode::NOT_FOUND, StatusCode::FORBIDDEN],
            _ => &[StatusCode::NOT_FOUND],
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
            let r = self.put_object(INDEX, body.into(), &precondition).await?;
            if r.status() != StatusCode::PRECONDITION_FAILED {
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
    /// A store that answers without an `ETag`, with a weak one (Apache,
    /// for a file modified this second; `If-Match` compares strongly so
    /// it could never pass), or that ignores `If-Match` (nginx) cannot
    /// compare and swap, so there the last writer wins.
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
            .filter(|etag| !etag.starts_with("W/"))
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
            Origin::S3(bucket, c) => self.list_bucket(bucket, c.as_ref(), prefix, limit).await,
            Origin::Dav { .. } => self.list_dav(prefix, limit).await,
        }
    }

    async fn list_bucket(
        &self,
        bucket: &Bucket,
        credentials: Option<&Credentials>,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Option<Vec<Listed>>, Error> {
        let full = self.path(&object(prefix));
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

    /// PROPFIND has neither paging nor a prefix filter: list the key's
    /// directory, for sharded kinds every shard below it that exists.
    async fn list_dav(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Option<Vec<Listed>>, Error> {
        let dir = object(prefix);
        let dir = dir.rsplit_once('/').map_or("", |(d, _)| d);
        let sharded = !dir.starts_with("heads");
        let dirs: Vec<String> = if sharded {
            let (shards, _) = self.propfind(&format!("{dir}/")).await?;
            shards.into_iter().map(|s| format!("{dir}/{s}/")).collect()
        } else {
            vec![format!("{dir}/")]
        };
        let pages: Vec<Vec<Listed>> = stream::iter(dirs)
            .map(|d| async move { Ok::<_, Error>(self.propfind(&d).await?.1) })
            .buffer_unordered(PROPFIND_CONCURRENCY)
            .try_collect()
            .await?;
        let out: Vec<Listed> = pages
            .into_iter()
            .flatten()
            .filter(|l| l.key.starts_with(prefix))
            .collect();
        if limit.is_some_and(|l| out.len() as u64 > l) {
            return Ok(None);
        }
        Ok(Some(out))
    }

    /// `Depth: 1` listing of one collection: (sub-collections, files).
    /// A missing collection is empty, nothing was written there yet.
    async fn propfind(&self, rel_dir: &str) -> Result<(Vec<String>, Vec<Listed>), Error> {
        let url = self.url(&Method::GET, rel_dir);
        let headers = [
            (HeaderName::from_static("depth"), "1".to_owned()),
            (header::CONTENT_TYPE, "application/xml".to_owned()),
        ];
        let body = Bytes::from_static(PROPFIND_BODY.as_bytes());
        let r = self
            .request(
                Method::from_bytes(b"PROPFIND").unwrap(),
                url.clone(),
                Some((body, MUTABLE)),
                &headers,
                &[StatusCode::MULTI_STATUS],
            )
            .await?;
        if r.status() != StatusCode::MULTI_STATUS {
            return Ok((Vec::new(), Vec::new()));
        }
        parse_multistatus(&r.text().await?, url.path())
            .map_err(|e| Error::InvalidResponse(format!("PROPFIND {url}: {e}")))
    }

    pub async fn probe_writable(&self) -> Result<bool, Error> {
        if matches!(self.origin, Origin::S3(_, None) | Origin::Http(_)) {
            // Anonymous S3 cannot sign a PUT at all.
            return Ok(false);
        }
        match self.put_object("x-probe", Bytes::new(), &[]).await {
            Ok(_) => self.delete("x-probe").await,
            Err(Error::Status {
                status: 401 | 403, ..
            }) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

const PROPFIND_BODY: &str = r#"<?xml version="1.0" encoding="utf-8"?><propfind xmlns="DAV:"><prop><resourcetype/><getlastmodified/></prop></propfind>"#;

/// The entries of a `207 Multi-Status` below the collection at
/// `base_path`: names of sub-collections, and files as `Listed`.
/// Servers differ in namespace prefixes, absolute vs. relative hrefs,
/// trailing slashes and percent-encoding; only local names are matched.
fn parse_multistatus(xml: &str, base_path: &str) -> Result<(Vec<String>, Vec<Listed>), String> {
    use xmlparser::{ElementEnd, Token};
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    let (mut href, mut modified, mut collection) = (String::new(), None, false);
    let mut text_of: Option<&str> = None;
    let base = base_path.trim_end_matches('/');
    for token in xmlparser::Tokenizer::from(xml) {
        match token.map_err(|e| e.to_string())? {
            Token::ElementStart { local, .. } => match local.as_str() {
                "response" => (href, modified, collection) = (String::new(), None, false),
                "href" => text_of = Some("href"),
                "getlastmodified" => text_of = Some("getlastmodified"),
                "collection" => collection = true,
                _ => {}
            },
            Token::Text { text } | Token::Cdata { text, .. } => match text_of {
                Some("href") => href.push_str(text.as_str().trim()),
                Some("getlastmodified") => modified = parse_http_date(text.as_str().trim()),
                _ => {}
            },
            Token::ElementEnd {
                end: ElementEnd::Close(_, local),
                ..
            } => {
                text_of = None;
                if local.as_str() != "response" {
                    continue;
                }
                // Absolute URL, absolute path, or relative: reduce to a path.
                let path = match Url::parse(&href) {
                    Ok(u) => u.path().to_owned(),
                    Err(_) => href.clone(),
                };
                let path = percent_decode(path.trim_end_matches('/'));
                if path == base || !path.starts_with(base) {
                    continue;
                }
                let name = key_of(&path).to_owned();
                if collection {
                    dirs.push(name);
                } else {
                    files.push(Listed {
                        key: name,
                        created: modified,
                        last_accessed: None,
                    });
                }
            }
            Token::ElementEnd {
                end: ElementEnd::Empty,
                ..
            } => text_of = None,
            _ => {}
        }
    }
    Ok((dirs, files))
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

/// RFC 7231 date, `Sun, 06 Nov 1994 08:49:37 GMT`, as unix seconds.
fn parse_http_date(s: &str) -> Option<u64> {
    let t = httpdate::parse_http_date(s).ok()?;
    Some(t.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs())
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
        assert_eq!(s3.path(&object("g-1")), "store/heads/g-1");
        assert_eq!(key_of("store/pack/ab/pack-abcdef"), "pack-abcdef");
    }

    #[test]
    fn dav_url_credentials() {
        let d = BlobDir::dav(
            "https://u:p%40ss@host/dav/ci/",
            None,
            reqwest::Client::new(),
        )
        .unwrap();
        let Origin::Dav { root, auth, .. } = &d.origin else {
            panic!()
        };
        assert_eq!(root.as_str(), "https://host/");
        assert_eq!(d.prefix, "dav/ci");
        assert_eq!(auth.as_ref().unwrap(), &("u".to_owned(), "p@ss".to_owned()));
    }

    #[test]
    fn http_dates() {
        assert_eq!(
            parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"),
            parse_timestamp("1994-11-06T08:49:37Z")
        );
        assert_eq!(parse_http_date("garbage"), None);
    }

    /// nginx: `D:` prefix, absolute hrefs, the collection itself first.
    const NGINX: &str = r#"<?xml version="1.0" encoding="utf-8" ?>
<D:multistatus xmlns:D="DAV:">
<D:response><D:href>/store/pack/ab/</D:href><D:propstat><D:prop>
<D:getlastmodified>Mon, 07 Sep 2026 19:15:58 GMT</D:getlastmodified>
<D:resourcetype><D:collection/></D:resourcetype></D:prop>
<D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
<D:response><D:href>/store/pack/ab/pack-abc</D:href><D:propstat><D:prop>
<D:getcontentlength>100</D:getcontentlength>
<D:getlastmodified>Mon, 07 Sep 2026 19:15:58 GMT</D:getlastmodified>
<D:resourcetype></D:resourcetype></D:prop>
<D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>
</D:multistatus>"#;

    /// SabreDAV: `d:` prefix, hrefs percent-encoded with trailing
    /// slash on collections, full URL on some setups.
    const SABRE: &str = r#"<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
 <d:response><d:href>/remote.php/dav/files/u/ci/seg/</d:href>
  <d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
 <d:response><d:href>/remote.php/dav/files/u/ci/seg/0a/</d:href>
  <d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
 <d:response><d:href>https://cloud.example/remote.php/dav/files/u/ci/seg/seg%2Dff</d:href>
  <d:propstat><d:prop><d:getlastmodified>Tue, 01 Jan 2030 00:00:00 GMT</d:getlastmodified><d:resourcetype/></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>
</d:multistatus>"#;

    #[test]
    fn multistatus_from_nginx() {
        let (dirs, files) = parse_multistatus(NGINX, "/store/pack/ab/").unwrap();
        assert!(dirs.is_empty());
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].key, "pack-abc");
        assert!(files[0].created.is_some());
    }

    #[test]
    fn multistatus_from_sabre() {
        let (dirs, files) = parse_multistatus(SABRE, "/remote.php/dav/files/u/ci/seg/").unwrap();
        assert_eq!(dirs, ["0a"]);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].key, "seg-ff");
        assert_eq!(files[0].created, parse_timestamp("2030-01-01T00:00:00Z"));
    }
}
