//! An OCI distribution registry (GHCR first). Content keys are blobs at
//! `sha256:<key suffix>`, each with a one-layer manifest so registries
//! that hide unreferenced blobs serve them. Heads are tags on a manifest
//! whose config blob is the record, and every manifest names its key in
//! an annotation. Listing is `tags/list`. On GHCR content manifests stay
//! untagged and the [`ghcr`](super::ghcr) ledger knows every object;
//! elsewhere they are tagged with their key, so untagged-manifest
//! retention (Harbor, Quay, ECR, Hub, `garbage-collect --delete-untagged`)
//! leaves them alone and GC can enumerate them. Content tags (`pack-`,
//! `seg-`, `tree-`) sort after every head kind, so listing heads stops
//! at the first one. Delete is `DELETE /manifests/<digest>`, or GHCR's
//! packages API.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use reqwest::{RequestBuilder, Response, StatusCode, header};
use serde::Deserialize;
use tokio::sync::OnceCell;

use super::ghcr::{Ledger, Packages};
use super::{Error, Listed};
use crate::gha::blob::{is_transient, status_error};
use crate::gha::rest::{
    DEFAULT_API_URL, ENV_GITHUB_API_URL, ENV_GITHUB_TOKEN, format_timestamp, parse_timestamp,
};
use crate::manifest::Hash32;
use crate::pipeline::now_unix;

const MANIFEST_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
const EMPTY_TYPE: &str = "application/vnd.oci.empty.v1+json";
const EMPTY_DIGEST: &str =
    "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";
const ARTIFACT_PREFIX: &str = "application/vnd.hestia.";
const KEY_ANNOTATION: &str = "org.opencontainers.image.ref.name";
const CREATED_ANNOTATION: &str = "org.opencontainers.image.created";
/// GETs in flight when GC reads creation times off content manifests.
const LIST_CONCURRENCY: usize = 16;
const TAGS_PAGE: usize = 1000;
const TRANSIENT_RETRIES: u32 = 4;
/// Blobs up to this size are sent with the opening POST.
const MONOLITHIC_MAX: usize = 4 << 20;
/// GHCR's signed blob URLs carry a longer expiry, this stays below it.
const REDIRECT_TTL: Duration = Duration::from_secs(60);

pub const ENV_OCI_USER: &str = "HESTIA_OCI_USER";
pub const ENV_OCI_PASSWORD: &str = "HESTIA_OCI_PASSWORD";

#[derive(Clone)]
pub struct Oci {
    http: reqwest::Client,
    /// Same, but sees blob redirects instead of following them.
    no_redirect: reqwest::Client,
    /// Blob digest -> CDN URL the registry redirected to.
    redirects: Arc<Mutex<HashMap<String, (String, Instant)>>>,
    /// `https://ghcr.io`
    registry: String,
    /// `owner/repo/hestia`
    name: String,
    basic: Option<(String, String)>,
    /// What answered the last challenge: a bearer token, or the basic
    /// credentials themselves for registries that challenge with Basic.
    auth: Arc<Mutex<Option<Auth>>>,
    empty_pushed: Arc<OnceCell<bool>>,
    ghcr: Option<Packages>,
    /// Loaded and synced on first use, written back by [`Oci::flush`].
    ledger: Arc<OnceCell<tokio::sync::Mutex<Ledger>>>,
}

fn content_digest(key: &str) -> Option<String> {
    let (kind, hex) = key.split_once('-')?;
    (matches!(kind, "pack" | "seg" | "tree") && Hash32::from_hex(hex).is_some())
        .then(|| format!("sha256:{hex}"))
}

fn kind(key: &str) -> &str {
    key.split_once('-').map_or(key, |(k, _)| k)
}

fn sha256(data: &[u8]) -> String {
    format!("sha256:{}", Hash32::digest(data))
}

fn descriptor(media_type: &str, digest: &str, size: usize) -> serde_json::Value {
    serde_json::json!({"mediaType": media_type, "digest": digest, "size": size})
}

/// The key annotation makes every head its own manifest, so deleting one
/// never takes another tag with it, and tells GC which key a manifest is.
fn manifest(
    key: &str,
    config: Option<(&str, usize)>,
    layer: Option<(&str, usize)>,
    created: Option<u64>,
) -> Vec<u8> {
    let empty = || descriptor(EMPTY_TYPE, EMPTY_DIGEST, 2);
    let mut annotations = serde_json::json!({KEY_ANNOTATION: key});
    if let Some(t) = created {
        annotations[CREATED_ANNOTATION] = format_timestamp(t).into();
    }
    let m = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": MANIFEST_TYPE,
        "artifactType": format!("{ARTIFACT_PREFIX}{}", kind(key)),
        "config": config.map_or_else(empty, |(d, n)| descriptor("application/cbor", d, n)),
        "layers": [layer.map_or_else(empty, |(d, n)| descriptor("application/octet-stream", d, n))],
        "annotations": annotations,
    });
    serde_json::to_vec(&m).expect("json")
}

#[derive(Deserialize)]
struct Manifest {
    config: Descriptor,
    #[serde(default)]
    annotations: std::collections::BTreeMap<String, String>,
}
#[derive(Deserialize)]
struct Descriptor {
    digest: String,
}
#[derive(Deserialize)]
struct Token {
    token: Option<String>,
    access_token: Option<String>,
}
#[derive(Deserialize)]
struct Tags {
    tags: Option<Vec<String>>,
}

/// `Bearer realm="…",service="…",scope="…"` → those three.
#[derive(Clone)]
enum Auth {
    Bearer(String),
    Basic,
}

fn parse_challenge(h: &str) -> Option<(String, Option<String>)> {
    let params = h.strip_prefix("Bearer ")?;
    let mut realm = None;
    let mut service = None;
    for kv in params.split(',') {
        let (k, v) = kv.trim().split_once('=')?;
        let v = v.trim_matches('"').to_owned();
        match k {
            "realm" => realm = Some(v),
            "service" => service = Some(v),
            _ => {}
        }
    }
    Some((realm?, service))
}

impl Oci {
    /// `repo` is `<registry host>/<name>` or a full `http(s)://host/<name>`.
    /// `github_api` (URL, token) marks the registry as GHCR.
    pub fn new(
        repo: &str,
        basic: Option<(String, String)>,
        github_api: Option<(&str, String)>,
        http: reqwest::Client,
    ) -> Result<Self, Error> {
        let invalid = |reason: &str| Error::InvalidEnv {
            name: super::ENV_OCI,
            reason: reason.into(),
        };
        let (scheme, rest) = match repo.split_once("://") {
            Some((s, r)) => (s, r),
            None => ("https", repo),
        };
        let (host, name) = rest
            .split_once('/')
            .ok_or_else(|| invalid("want <registry>/<repository>"))?;
        if name.is_empty() || host.is_empty() {
            return Err(invalid("want <registry>/<repository>"));
        }
        let name = name.trim_end_matches('/');
        Ok(Oci {
            ghcr: github_api.and_then(|(api, token)| Packages::new(http.clone(), api, name, token)),
            http,
            no_redirect: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            redirects: Default::default(),
            registry: format!("{scheme}://{host}"),
            name: name.to_owned(),
            basic,
            auth: Default::default(),
            empty_pushed: Default::default(),
            ledger: Default::default(),
        })
    }

    pub fn from_env(repo: &str, http: reqwest::Client) -> Result<Self, Error> {
        let var = |k| std::env::var(k).ok().filter(|v: &String| !v.is_empty());
        let github_token = var(ENV_GITHUB_TOKEN).filter(|_| repo.starts_with("ghcr.io/"));
        let basic = match (var(ENV_OCI_USER), var(ENV_OCI_PASSWORD)) {
            (Some(u), Some(p)) => Some((u, p)),
            // GHCR takes any user name with a token.
            _ => github_token.clone().map(|t| ("token".into(), t)),
        };
        let api = var(ENV_GITHUB_API_URL).unwrap_or_else(|| DEFAULT_API_URL.to_owned());
        Self::new(repo, basic, github_token.map(|t| (api.as_str(), t)), http)
    }

    fn v2(&self, path: &str) -> String {
        format!("{}/v2/{}/{path}", self.registry, self.name)
    }

    fn absolute(&self, url: &str) -> String {
        if url.starts_with('/') {
            format!("{}{url}", self.registry)
        } else {
            url.to_owned()
        }
    }

    async fn answer(&self, challenge: &str) -> Result<Auth, Error> {
        if challenge.starts_with("Basic ") && self.basic.is_some() {
            return Ok(Auth::Basic);
        }
        let (realm, service) = parse_challenge(challenge)
            .ok_or_else(|| Error::InvalidResponse(format!("WWW-Authenticate: {challenge}")))?;
        let mut q = vec![("scope", format!("repository:{}:pull,push", self.name))];
        if let Some(s) = service {
            q.push(("service", s));
        }
        let mut req = self.http.get(&realm).query(&q);
        if let Some((u, p)) = &self.basic {
            req = req.basic_auth(u, Some(p));
        }
        let r = req.send().await?;
        if !r.status().is_success() {
            return Err(status_error(&realm, r).await);
        }
        let t: Token = r.json().await?;
        t.token
            .or(t.access_token)
            .map(Auth::Bearer)
            .ok_or_else(|| Error::InvalidResponse("token response without token".into()))
    }

    /// Send with the bearer token, fetching one on 401, retrying transient failures.
    async fn send(&self, build: impl Fn() -> RequestBuilder) -> Result<Response, Error> {
        let mut authed = false;
        let mut transient_left = TRANSIENT_RETRIES;
        let mut delay = Duration::from_millis(200);
        loop {
            let mut req = build();
            let auth = self.auth.lock().unwrap().clone();
            req = match (auth, &self.basic) {
                (Some(Auth::Bearer(t)), _) => req.bearer_auth(t),
                (Some(Auth::Basic), Some((u, p))) => req.basic_auth(u, Some(p)),
                _ => req,
            };
            let result = req.send().await.map_err(Error::Http);
            let transient = match &result {
                Ok(r) if r.status() == StatusCode::UNAUTHORIZED && !authed => {
                    let challenge = r
                        .headers()
                        .get(header::WWW_AUTHENTICATE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_owned();
                    authed = true;
                    *self.auth.lock().unwrap() = Some(self.answer(&challenge).await?);
                    continue;
                }
                Ok(r) => {
                    r.status().is_server_error() || r.status() == StatusCode::TOO_MANY_REQUESTS
                }
                Err(e) => is_transient(e),
            };
            if transient && transient_left > 0 {
                transient_left -= 1;
                tokio::time::sleep(delay).await;
                delay *= 2;
                continue;
            }
            return result;
        }
    }

    async fn blob_exists(&self, digest: &str) -> Result<bool, Error> {
        Ok(self.head_size(&format!("blobs/{digest}")).await?.is_some())
    }

    /// POST then PUT, the one upload flow every registry has. `false` if
    /// `may_exist` and the blob was there already.
    async fn upload_blob(&self, digest: &str, data: Bytes, may_exist: bool) -> Result<bool, Error> {
        if may_exist && self.blob_exists(digest).await? {
            return Ok(false);
        }
        // Small blobs go in the opening POST, saving a round trip where the
        // registry takes single-request uploads. Others answer 202 as for an
        // empty POST, so packs (too big to send twice) never try.
        let url = self.v2("blobs/uploads/");
        let r = if data.len() <= MONOLITHIC_MAX {
            let mono = format!("{url}?digest={digest}");
            self.send(|| {
                self.http
                    .post(&mono)
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .body(data.clone())
            })
            .await?
        } else {
            self.send(|| self.http.post(&url).header(header::CONTENT_LENGTH, 0))
                .await?
        };
        match r.status() {
            StatusCode::CREATED => return Ok(true),
            StatusCode::ACCEPTED => {}
            _ => return Err(status_error(&url, r).await),
        }
        let location = r
            .headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(|l| self.absolute(l))
            .ok_or_else(|| Error::InvalidResponse("upload without Location".into()))?;
        let sep = if location.contains('?') { '&' } else { '?' };
        let put_url = format!("{location}{sep}digest={digest}");
        let r = self
            .send(|| {
                self.http
                    .put(&put_url)
                    .header(header::CONTENT_TYPE, "application/octet-stream")
                    .body(data.clone())
            })
            .await?;
        if r.status() != StatusCode::CREATED {
            return Err(status_error(&put_url, r).await);
        }
        Ok(true)
    }

    /// Every manifest here references the empty descriptor.
    async fn put_manifest(&self, reference: &str, body: Vec<u8>) -> Result<(), Error> {
        self.empty_pushed
            .get_or_try_init(|| self.upload_blob(EMPTY_DIGEST, Bytes::from_static(b"{}"), true))
            .await?;
        let url = self.v2(&format!("manifests/{reference}"));
        let body = Bytes::from(body);
        let r = self
            .send(|| {
                self.http
                    .put(&url)
                    .header(header::CONTENT_TYPE, MANIFEST_TYPE)
                    .body(body.clone())
            })
            .await?;
        if r.status() != StatusCode::CREATED {
            return Err(status_error(&url, r).await);
        }
        Ok(())
    }

    pub async fn put(&self, key: &str, data: Bytes) -> Result<bool, Error> {
        let digest = sha256(&data);
        let blob = Some((digest.as_str(), data.len()));
        if let Some(expected) = content_digest(key) {
            assert_eq!(digest, expected, "{key} does not name its content");
            // Packs may come from a concurrent writer, segments never do.
            let created = self
                .upload_blob(&digest, data.clone(), kind(key) == "pack")
                .await?;
            // Also when the blob existed: heals one whose manifest never landed.
            if self.ghcr.is_some() {
                let m = manifest(key, None, blob, None);
                self.put_manifest(&sha256(&m), m).await?;
            } else {
                self.put_manifest(key, manifest(key, None, blob, Some(now_unix())))
                    .await?;
            }
            return Ok(created);
        }
        if !data.is_empty() {
            self.upload_blob(&digest, data.clone(), false).await?;
        }
        let m = manifest(key, blob.filter(|_| !data.is_empty()), None, None);
        self.put_manifest(key, m).await?;
        Ok(true)
    }

    async fn manifest_bytes(&self, reference: &str) -> Result<Option<Bytes>, Error> {
        let url = self.v2(&format!("manifests/{reference}"));
        let r = self
            .send(|| self.http.get(&url).header(header::ACCEPT, MANIFEST_TYPE))
            .await?;
        match r.status() {
            StatusCode::NOT_FOUND => Ok(None),
            StatusCode::OK => Ok(Some(r.bytes().await?)),
            _ => Err(status_error(&url, r).await),
        }
    }

    /// A manifest by tag or digest.
    async fn get_manifest(&self, reference: &str) -> Result<Option<Manifest>, Error> {
        let Some(body) = self.manifest_bytes(reference).await? else {
            return Ok(None);
        };
        serde_json::from_slice(&body)
            .map(Some)
            .map_err(|e| Error::InvalidResponse(format!("manifest {reference}: {e}")))
    }

    /// The hestia key a manifest was written for.
    pub(super) async fn key_of(&self, digest: &str) -> Result<Option<String>, Error> {
        Ok(self
            .get_manifest(digest)
            .await?
            .and_then(|m| m.annotations.get(KEY_ANNOTATION).cloned()))
    }

    /// `Content-Length` of a HEAD, `None` on 404.
    async fn head_size(&self, path: &str) -> Result<Option<usize>, Error> {
        let url = self.v2(path);
        let r = self
            .send(|| self.http.head(&url).header(header::ACCEPT, MANIFEST_TYPE))
            .await?;
        match r.status() {
            StatusCode::OK => Ok(r
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok()?.parse().ok())),
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(status_error(&url, r).await),
        }
    }

    /// Through a remembered CDN URL, unless there is none or it expired.
    async fn fetch_redirected(
        &self,
        digest: &str,
        fetch: &impl Fn(&str, &reqwest::Client) -> RequestBuilder,
    ) -> Result<Option<Response>, Error> {
        let cdn = {
            let mut redirects = self.redirects.lock().unwrap();
            redirects.retain(|_, (_, t)| t.elapsed() < REDIRECT_TTL);
            redirects.get(digest).map(|(u, _)| u.clone())
        };
        let Some(cdn) = cdn else { return Ok(None) };
        let r = fetch(&cdn, &self.http).send().await?;
        Ok((!matches!(r.status(), StatusCode::FORBIDDEN | StatusCode::NOT_FOUND)).then_some(r))
    }

    fn remember_redirect(
        &self,
        digest: &str,
        url: &str,
        r: &Response,
    ) -> Result<Option<String>, Error> {
        if !r.status().is_redirection() {
            return Ok(None);
        }
        let cdn = r
            .headers()
            .get(header::LOCATION)
            .and_then(|l| l.to_str().ok())
            .map(|l| self.absolute(l))
            .ok_or_else(|| Error::InvalidResponse(format!("{url}: redirect without Location")))?;
        self.redirects
            .lock()
            .unwrap()
            .insert(digest.to_owned(), (cdn.clone(), Instant::now()));
        Ok(Some(cdn))
    }

    /// Registries like GHCR redirect blob reads to signed CDN URLs. Those
    /// are remembered so a burst of range reads on one pack skips the
    /// registry round trip.
    async fn get_blob(
        &self,
        digest: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<Bytes>, Error> {
        let url = self.v2(&format!("blobs/{digest}"));
        let fetch = |u: &str, http: &reqwest::Client| match &range {
            Some(r) => http
                .get(u)
                .header(header::RANGE, format!("bytes={}-{}", r.start, r.end - 1)),
            None => http.get(u),
        };
        let r = match self.fetch_redirected(digest, &fetch).await? {
            Some(r) => r,
            None => {
                let r = self.send(|| fetch(&url, &self.no_redirect)).await?;
                match self.remember_redirect(digest, &url, &r)? {
                    Some(cdn) => fetch(&cdn, &self.http).send().await?,
                    None => r,
                }
            }
        };
        match (r.status(), &range) {
            (StatusCode::NOT_FOUND, _) => Ok(None),
            (StatusCode::OK, None) => Ok(Some(r.bytes().await?)),
            (StatusCode::PARTIAL_CONTENT, Some(want)) => {
                let body = r.bytes().await?;
                if body.len() as u64 != want.end - want.start {
                    return Err(Error::InvalidResponse(format!(
                        "range {}..{} of {url} returned {} bytes",
                        want.start,
                        want.end,
                        body.len()
                    )));
                }
                Ok(Some(body))
            }
            _ => Err(status_error(&url, r).await),
        }
    }

    pub async fn get(&self, key: &str, range: Option<Range<u64>>) -> Result<Option<Bytes>, Error> {
        if let Some(digest) = content_digest(key) {
            return self.get_blob(&digest, range).await;
        }
        let Some(m) = self.get_manifest(key).await? else {
            return Ok(None);
        };
        if m.config.digest == EMPTY_DIGEST {
            return Ok(Some(Bytes::new()));
        }
        self.get_blob(&m.config.digest, range).await
    }

    pub async fn exists(&self, key: &str) -> Result<bool, Error> {
        match content_digest(key) {
            Some(d) => self.blob_exists(&d).await,
            None => Ok(self.get(key, None).await?.is_some()),
        }
    }

    async fn ledger(&self) -> Result<Option<tokio::sync::MutexGuard<'_, Ledger>>, Error> {
        let Some(ghcr) = &self.ghcr else {
            return Ok(None);
        };
        let cell = self
            .ledger
            .get_or_try_init(|| async {
                let stored = self.get(super::ghcr::LEDGER_TAG, None).await?;
                let mut ledger = Ledger::decode(&stored.unwrap_or_default());
                ghcr.sync(self, &mut ledger).await?;
                Ok::<_, Error>(tokio::sync::Mutex::new(ledger))
            })
            .await?;
        Ok(Some(cell.lock().await))
    }

    pub async fn flush(&self) -> Result<(), Error> {
        if let Some(cell) = self.ledger.get() {
            let body = cell.lock().await.encode();
            self.put(super::ghcr::LEDGER_TAG, body.into()).await?;
        }
        Ok(())
    }

    /// GC only: every content object with its creation time, through the
    /// GHCR ledger or off the content tags' manifests.
    pub async fn list_objects(&self) -> Result<Vec<Listed>, Error> {
        if let Some(l) = self.ledger().await? {
            return Ok(l.objects());
        }
        let mut tags = Vec::new();
        for prefix in ["pack-", "seg-", "tree-"] {
            tags.extend(self.tags(prefix, None).await?.expect("unbounded"));
        }
        let listed = futures_util::stream::iter(tags)
            .map(|key| async move {
                let created = self
                    .get_manifest(&key)
                    .await?
                    .and_then(|m| parse_timestamp(m.annotations.get(CREATED_ANNOTATION)?));
                Ok::<_, Error>(Listed {
                    key,
                    created,
                    last_accessed: None,
                })
            })
            .buffer_unordered(LIST_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(listed)
    }

    /// The empty prefix lists every head kind. Content prefixes give
    /// `None` on GHCR, where content manifests carry no tag.
    pub async fn list(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Option<Vec<Listed>>, Error> {
        if self.ghcr.is_some() && !matches!(kind(prefix), "" | "g" | "h" | "c") {
            return Ok(None);
        }
        Ok(self.tags(prefix, limit).await?.map(|tags| {
            tags.into_iter()
                .map(|key| Listed {
                    key,
                    created: None,
                    last_accessed: None,
                })
                .collect()
        }))
    }

    async fn tags(&self, prefix: &str, limit: Option<u64>) -> Result<Option<Vec<String>>, Error> {
        let heads = matches!(kind(prefix), "" | "g" | "h" | "c");
        let wanted = |t: &str| match prefix {
            "" => matches!(kind(t), "g" | "h" | "c"),
            p => t.starts_with(p),
        };
        let mut out = Vec::new();
        let mut url = self.v2(&format!("tags/list?n={TAGS_PAGE}"));
        loop {
            let r = self.send(|| self.http.get(&url)).await?;
            match r.status() {
                // No push yet: the repository does not exist.
                StatusCode::NOT_FOUND => return Ok(Some(out)),
                StatusCode::OK => {}
                _ => return Err(status_error(&url, r).await),
            }
            let next = r
                .headers()
                .get(header::LINK)
                .and_then(|v| v.to_str().ok())
                .and_then(|l| l.split(';').next())
                .map(|u| self.absolute(u.trim().trim_start_matches('<').trim_end_matches('>')));
            let tags: Tags = r.json().await?;
            let mut past_heads = false;
            for t in tags.tags.unwrap_or_default() {
                past_heads |= t.as_str() > "i";
                if wanted(&t) {
                    out.push(t);
                }
            }
            if limit.is_some_and(|l| out.len() as u64 > l) {
                return Ok(None);
            }
            match next {
                Some(n) if !(heads && past_heads) => url = n,
                _ => return Ok(Some(out)),
            }
        }
    }

    /// Deletes the key's manifest. The registry's own GC reclaims the blobs.
    pub async fn delete(&self, key: &str) -> Result<bool, Error> {
        if let Some(d) = content_digest(key) {
            self.redirects.lock().unwrap().remove(&d);
        }
        if let (Some(ghcr), Some(mut ledger)) = (&self.ghcr, self.ledger().await?) {
            return ghcr.delete(&mut ledger, key).await;
        }
        // Every key is a tag here. Deleting by digest takes the tag along,
        // deleting by tag is an API registries may not offer.
        let Some(reference) = self.manifest_bytes(key).await?.map(|b| sha256(&b)) else {
            return Ok(false);
        };
        let url = self.v2(&format!("manifests/{reference}"));
        let r = self.send(|| self.http.delete(&url)).await?;
        match r.status() {
            StatusCode::ACCEPTED | StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(status_error(&url, r).await),
        }
    }

    pub async fn probe_writable(&self) -> Result<bool, Error> {
        let url = self.v2("blobs/uploads/");
        let r = self
            .send(|| self.http.post(&url).header(header::CONTENT_LENGTH, 0))
            .await?;
        match r.status() {
            StatusCode::ACCEPTED => Ok(true),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Ok(false),
            _ => Err(status_error(&url, r).await),
        }
    }
}
