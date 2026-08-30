//! Behavioral fake of an OCI distribution registry, modelling the
//! unfriendliest union of GHCR, Docker Hub and `distribution`:
//!
//! * bearer token dance (`/v2/` → 401 + `WWW-Authenticate`, `/token`),
//!   push needs basic credentials, pull is anonymous
//! * blobs: POST+PUT upload, HEAD, GET answers 307 to a CDN URL that
//!   honours `Range`, and GET is **404 until some manifest references the
//!   blob** (GHCR)
//! * manifests by tag or digest, referenced blobs must exist, tags are
//!   last-writer-wins
//! * `tags/list` lexically ordered, paged with `n`/`last` + `Link`, and
//!   new tags invisible for `tag_lag` further requests (GHCR ≤ 30 s)
//! * DELETE is 405, deletes go through GitHub's packages REST API on a
//!   third origin: versions paged newest-first, delete by version id (GHCR)

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Query, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;
use serde_json::json;

use hestia::backend::Backend;
use hestia::backend::oci::Oci;
use hestia::gha::rest::format_timestamp;
use hestia::manifest::Hash32;

pub const REPO: &str = "owner/repo/hestia";
pub const USER: &str = "ci";
pub const PASSWORD: &str = "secret";
pub const API_TOKEN: &str = "gh-api-token";
const PULL_TOKEN: &str = "pull-token";
const PUSH_TOKEN: &str = "push-token";

#[derive(Default)]
struct Inner {
    blobs: HashMap<String, Bytes>,
    next_upload: u64,
    manifests: HashMap<String, Bytes>,
    /// manifest digest → (version id, created), GHCR's package versions
    versions: BTreeMap<String, (u64, u64)>,
    next_version: u64,
    clock: u64,
    api_calls: u64,
    /// Blob digests some manifest lists (config or layer).
    referenced: HashSet<String>,
    /// tag → (manifest digest, request count when written)
    tags: BTreeMap<String, (String, u64)>,
    requests: u64,
    tag_lag: u64,
    deny_push: bool,
    /// Every blob GET as (digest, Range header).
    blob_gets: Vec<(String, Option<String>)>,
}

fn api_authed(headers: &HeaderMap) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == format!("Bearer {API_TOKEN}"))
}

/// `/orgs/{owner}/packages/container/{name}/versions[/{id}]`
async fn api(State(state): State<AppState>, req: Request) -> Response {
    if !api_authed(req.headers()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let (owner, pkg) = REPO.split_once('/').unwrap();
    let prefix = format!(
        "/orgs/{owner}/packages/container/{}/versions",
        pkg.replace('/', "%2F")
    );
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let Some(rest) = path.strip_prefix(&prefix) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let param = |k: &str| -> Option<usize> {
        query
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(key, _)| *key == k)
            .and_then(|(_, v)| v.parse().ok())
    };
    let mut inner = state.inner.lock().unwrap();
    inner.api_calls += 1;
    match (req.method().clone(), rest) {
        (Method::GET, "") => {
            let per_page = param("per_page").unwrap_or(30).min(100);
            let page = param("page").unwrap_or(1).max(1);
            let mut all: Vec<(&String, &(u64, u64))> = inner.versions.iter().collect();
            all.sort_by_key(|(_, (id, _))| std::cmp::Reverse(*id));
            let body: Vec<_> = all
                .iter()
                .skip((page - 1) * per_page)
                .take(per_page)
                .map(|(digest, (id, created))| {
                    let tags: Vec<&String> = inner
                        .tags
                        .iter()
                        .filter(|(_, (d, _))| d == *digest)
                        .map(|(t, _)| t)
                        .collect();
                    json!({
                        "id": id,
                        "name": digest,
                        "created_at": format_timestamp(*created),
                        "metadata": {"package_type": "container", "container": {"tags": tags}},
                    })
                })
                .collect();
            Json(body).into_response()
        }
        (Method::DELETE, id) => {
            let Ok(id) = id.trim_start_matches('/').parse::<u64>() else {
                return StatusCode::NOT_FOUND.into_response();
            };
            let Some(digest) = inner
                .versions
                .iter()
                .find(|(_, (i, _))| *i == id)
                .map(|(d, _)| d.clone())
            else {
                return StatusCode::NOT_FOUND.into_response();
            };
            inner.versions.remove(&digest);
            inner.manifests.remove(&digest);
            inner.tags.retain(|_, (d, _)| *d != digest);
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Clone)]
struct AppState {
    inner: Arc<Mutex<Inner>>,
    base_url: String,
    /// Separate origin so HTTP clients drop the bearer token on redirect.
    cdn_url: String,
}

pub struct FakeOci {
    inner: Arc<Mutex<Inner>>,
    pub base_url: String,
    pub api_url: String,
    servers: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for FakeOci {
    fn drop(&mut self) {
        for s in &self.servers {
            s.abort();
        }
    }
}

fn sha256(data: &[u8]) -> String {
    format!("sha256:{}", Hash32::digest(data))
}

fn error(status: StatusCode, code: &str) -> Response {
    (
        status,
        Json(json!({"errors": [{"code": code, "message": code}]})),
    )
        .into_response()
}

fn challenge(base: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(
            header::WWW_AUTHENTICATE,
            format!(r#"Bearer realm="{base}/token",service="fake",scope="repository:{REPO}:pull""#),
        )],
        Json(json!({"errors": [{"code": "UNAUTHORIZED"}]})),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
struct TokenQuery {
    scope: Option<String>,
}

async fn token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<TokenQuery>,
) -> Response {
    let wants_push = q.scope.as_deref().is_some_and(|s| s.contains("push"));
    let authed = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            use base64::Engine;
            let want =
                base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{PASSWORD}"));
            v == format!("Basic {want}")
        });
    let deny = state.inner.lock().unwrap().deny_push;
    // Like registries do: grant the subset the caller is entitled to.
    let t = if wants_push && authed && !deny {
        PUSH_TOKEN
    } else {
        PULL_TOKEN
    };
    Json(json!({"token": t})).into_response()
}

enum Access {
    None,
    Pull,
    Push,
}

fn access(headers: &HeaderMap) -> Access {
    match headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        Some(v) if v == format!("Bearer {PUSH_TOKEN}") => Access::Push,
        Some(v) if v == format!("Bearer {PULL_TOKEN}") => Access::Pull,
        _ => Access::None,
    }
}

fn parse_range(v: &str, len: u64) -> Option<(u64, u64)> {
    let (start, end) = v.strip_prefix("bytes=")?.split_once('-')?;
    let start: u64 = start.parse().ok()?;
    let end: u64 = if end.is_empty() {
        len - 1
    } else {
        end.parse().ok()?
    };
    (start <= end && end < len).then_some((start, end))
}

async fn cdn(State(state): State<AppState>, req: Request) -> Response {
    let digest = req.uri().path().trim_start_matches("/cdn/").to_owned();
    let range = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    if req.headers().contains_key(header::AUTHORIZATION) {
        // Real CDNs reject the registry's bearer token.
        return error(StatusCode::BAD_REQUEST, "auth header leaked to cdn");
    }
    let mut inner = state.inner.lock().unwrap();
    inner.blob_gets.push((digest.clone(), range.clone()));
    let Some(data) = inner.blobs.get(&digest).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match range {
        None => data.into_response(),
        Some(r) => match parse_range(&r, data.len() as u64) {
            Some((s, e)) => (
                StatusCode::PARTIAL_CONTENT,
                [(
                    header::CONTENT_RANGE,
                    format!("bytes {s}-{e}/{}", data.len()),
                )],
                data.slice(s as usize..=e as usize),
            )
                .into_response(),
            None => StatusCode::RANGE_NOT_SATISFIABLE.into_response(),
        },
    }
}

async fn v2(State(state): State<AppState>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    let method = req.method().clone();
    let headers = req.headers().clone();
    let access = access(&headers);
    if matches!(access, Access::None) {
        return challenge(&state.base_url);
    }
    let write = matches!(
        method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    );
    if write && !matches!(access, Access::Push) {
        return challenge(&state.base_url);
    }
    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    let Some(rest) = path.strip_prefix(&format!("/v2/{REPO}/")) else {
        return if path == "/v2/" {
            Json(json!({})).into_response()
        } else {
            error(StatusCode::NOT_FOUND, "NAME_UNKNOWN")
        };
    };
    let param = |k: &str| {
        query
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .find(|(key, _)| *key == k)
            .map(|(_, v)| v.replace("%3A", ":"))
    };
    let mut inner = state.inner.lock().unwrap();
    inner.requests += 1;

    if let Some(digest) = rest.strip_prefix("blobs/uploads/") {
        return match method {
            Method::POST if digest.is_empty() => {
                inner.next_upload += 1;
                let location = format!("/v2/{REPO}/blobs/uploads/{}?state=x", inner.next_upload);
                (StatusCode::ACCEPTED, [(header::LOCATION, location)]).into_response()
            }
            Method::PUT => {
                let Some(want) = param("digest") else {
                    return error(StatusCode::BAD_REQUEST, "DIGEST_INVALID");
                };
                if sha256(&body) != want {
                    return error(StatusCode::BAD_REQUEST, "DIGEST_INVALID");
                }
                inner.blobs.insert(want.clone(), body);
                (
                    StatusCode::CREATED,
                    [(header::LOCATION, format!("/v2/{REPO}/blobs/{want}"))],
                )
                    .into_response()
            }
            _ => error(StatusCode::NOT_FOUND, "BLOB_UPLOAD_UNKNOWN"),
        };
    }
    if let Some(digest) = rest.strip_prefix("blobs/") {
        let known = inner.blobs.contains_key(digest);
        return match method {
            Method::HEAD if known => (
                StatusCode::OK,
                [(
                    header::CONTENT_LENGTH,
                    inner.blobs[digest].len().to_string(),
                )],
            )
                .into_response(),
            Method::GET if known && inner.referenced.contains(digest) => (
                StatusCode::TEMPORARY_REDIRECT,
                [(header::LOCATION, format!("{}/cdn/{digest}", state.cdn_url))],
            )
                .into_response(),
            Method::DELETE => StatusCode::METHOD_NOT_ALLOWED.into_response(),
            _ => error(StatusCode::NOT_FOUND, "BLOB_UNKNOWN"),
        };
    }
    if let Some(reference) = rest.strip_prefix("manifests/") {
        let by_digest = reference.starts_with("sha256:");
        let visible_at = inner.requests;
        return match method {
            Method::PUT => {
                let Ok(m) = serde_json::from_slice::<serde_json::Value>(&body) else {
                    return error(StatusCode::BAD_REQUEST, "MANIFEST_INVALID");
                };
                let refs: Vec<String> = std::iter::once(&m["config"])
                    .chain(m["layers"].as_array().into_iter().flatten())
                    .filter_map(|d| d["digest"].as_str().map(str::to_owned))
                    .collect();
                if refs.iter().any(|d| !inner.blobs.contains_key(d)) {
                    return error(StatusCode::BAD_REQUEST, "MANIFEST_BLOB_UNKNOWN");
                }
                let digest = sha256(&body);
                if by_digest && reference != digest {
                    return error(StatusCode::BAD_REQUEST, "DIGEST_INVALID");
                }
                inner.referenced.extend(refs);
                inner.manifests.insert(digest.clone(), body);
                if !inner.versions.contains_key(&digest) {
                    inner.next_version += 1;
                    let v = (inner.next_version, inner.clock);
                    inner.versions.insert(digest.clone(), v);
                }
                if !by_digest {
                    inner
                        .tags
                        .insert(reference.to_owned(), (digest.clone(), visible_at));
                }
                (
                    StatusCode::CREATED,
                    [
                        (header::LOCATION, format!("/v2/{REPO}/manifests/{digest}")),
                        (
                            header::HeaderName::from_static("docker-content-digest"),
                            digest,
                        ),
                    ],
                )
                    .into_response()
            }
            Method::GET | Method::HEAD => {
                let digest = if by_digest {
                    Some(reference.to_owned())
                } else {
                    inner.tags.get(reference).map(|(d, _)| d.clone())
                };
                match digest.and_then(|d| inner.manifests.get(&d).cloned()) {
                    Some(m) => (
                        [(
                            header::CONTENT_TYPE,
                            "application/vnd.oci.image.manifest.v1+json",
                        )],
                        m,
                    )
                        .into_response(),
                    None => error(StatusCode::NOT_FOUND, "MANIFEST_UNKNOWN"),
                }
            }
            _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
        };
    }
    if rest == "tags/list" && method == Method::GET {
        if inner.manifests.is_empty() {
            return error(StatusCode::NOT_FOUND, "NAME_UNKNOWN");
        }
        let n: usize = param("n")
            .and_then(|v| v.parse().ok())
            .unwrap_or(usize::MAX);
        let last = param("last").unwrap_or_default();
        let now = inner.requests;
        let lag = inner.tag_lag;
        let visible: Vec<&String> = inner
            .tags
            .iter()
            .filter(|(t, (_, at))| at + lag < now && t.as_str() > last.as_str())
            .map(|(t, _)| t)
            .collect();
        let page: Vec<&String> = visible.iter().take(n).copied().collect();
        let mut resp = Json(json!({"name": REPO, "tags": page})).into_response();
        if visible.len() > n {
            let link = format!(
                "</v2/{REPO}/tags/list?n={n}&last={}>; rel=\"next\"",
                page.last().unwrap()
            );
            resp.headers_mut()
                .insert(header::LINK, link.parse().unwrap());
        }
        return resp;
    }
    error(StatusCode::NOT_FOUND, "unknown route")
}

impl FakeOci {
    pub async fn start() -> Self {
        let bind = || async {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind fake oci listener");
            let url = format!("http://{}", l.local_addr().unwrap());
            (l, url)
        };
        let (registry, base_url) = bind().await;
        let (cdn_listener, cdn_url) = bind().await;
        let (api_listener, api_url) = bind().await;
        let inner = Arc::new(Mutex::new(Inner::default()));
        let state = AppState {
            inner: inner.clone(),
            base_url: base_url.clone(),
            cdn_url,
        };
        let router = Router::new()
            .route("/token", get(token))
            .fallback(v2)
            .with_state(state.clone());
        let cdn_router = Router::new()
            .route("/cdn/{*digest}", get(cdn))
            .with_state(state.clone());
        let api_router = Router::new().fallback(api).with_state(state);
        let servers = vec![
            tokio::spawn(async move { axum::serve(registry, router).await.unwrap() }),
            tokio::spawn(async move { axum::serve(cdn_listener, cdn_router).await.unwrap() }),
            tokio::spawn(async move { axum::serve(api_listener, api_router).await.unwrap() }),
        ];
        FakeOci {
            inner,
            base_url,
            api_url,
            servers,
        }
    }

    pub fn repo(&self) -> String {
        format!("{}/{REPO}", self.base_url)
    }

    /// Push credentials plus the packages API, like a GHCR job token.
    pub fn backend(&self, http: &reqwest::Client) -> Backend {
        Backend::Oci(
            Oci::new(
                &self.repo(),
                Some((USER.into(), PASSWORD.into())),
                Some((&self.api_url, API_TOKEN.into())),
                http.clone(),
            )
            .unwrap(),
        )
    }

    /// Push credentials on a registry without a delete side channel.
    pub fn plain(&self, http: &reqwest::Client) -> Backend {
        Backend::Oci(
            Oci::new(
                &self.repo(),
                Some((USER.into(), PASSWORD.into())),
                None,
                http.clone(),
            )
            .unwrap(),
        )
    }

    pub fn anonymous(&self, http: &reqwest::Client) -> Backend {
        Backend::Oci(Oci::new(&self.repo(), None, None, http.clone()).unwrap())
    }

    pub fn set_clock(&self, unix_seconds: u64) {
        self.inner.lock().unwrap().clock = unix_seconds;
    }

    pub fn clock(&self) -> hestia::pipeline::Clock {
        let inner = self.inner.clone();
        Arc::new(move || inner.lock().unwrap().clock)
    }

    /// Package versions (manifests) whose hestia key starts with `prefix`.
    pub fn versions(&self, prefix: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        let needle = format!("\"org.opencontainers.image.ref.name\":\"{prefix}");
        inner
            .versions
            .keys()
            .filter(|d| {
                std::str::from_utf8(&inner.manifests[*d]).is_ok_and(|m| m.contains(&needle))
            })
            .count()
    }

    pub fn api_calls(&self) -> u64 {
        self.inner.lock().unwrap().api_calls
    }

    /// New tags stay out of `tags/list` for this many further requests.
    pub fn set_tag_lag(&self, requests: u64) {
        self.inner.lock().unwrap().tag_lag = requests;
    }

    /// Token endpoint hands out pull-only tokens whatever the credentials.
    pub fn deny_push(&self) {
        self.inner.lock().unwrap().deny_push = true;
    }

    pub fn tags(&self) -> Vec<String> {
        self.inner.lock().unwrap().tags.keys().cloned().collect()
    }

    pub fn blob_gets(&self) -> Vec<(String, Option<String>)> {
        self.inner.lock().unwrap().blob_gets.clone()
    }
}
