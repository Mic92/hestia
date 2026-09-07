//! Behavioral fake of a WebDAV share over a directory tree in memory,
//! modelling what hestia relies on and the quirks of real servers:
//!
//! * PUT (201 new, 204 overwrite), GET with `Range` (206, 416 past the
//!   end), HEAD with a strong ETag, DELETE (204, 404), MKCOL (201, 405
//!   when it exists, 409 for a missing parent or no trailing slash),
//!   PROPFIND `Depth: 1` answering a `D:`-prefixed 207 with absolute
//!   hrefs and RFC 1123 dates
//! * PUT below a missing collection is 409, or 500 in `nginx` mode,
//!   which also ignores `If-Match`/`If-None-Match` like nginx does
//! * Basic auth on everything unless `public` (then reads are open);
//!   `read_only` answers writes with 403

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;

use hestia::backend::Backend;
use hestia::backend::blobdir::BlobDir;
use hestia::pipeline::Clock;

pub const PREFIX: &str = "dav/ci";
pub const USER: &str = "hestia";
pub const PASSWORD: &str = "s3cret";

#[derive(Default)]
struct Inner {
    /// path (no leading slash) → (body, written at clock)
    files: BTreeMap<String, (Bytes, u64)>,
    /// collections, each with trailing slash; the root always exists
    dirs: BTreeSet<String>,
    clock: u64,
    public: bool,
    read_only: bool,
    nginx: bool,
    mkcols: u64,
}

#[derive(Clone)]
struct AppState {
    inner: Arc<Mutex<Inner>>,
}

pub struct FakeDav {
    inner: Arc<Mutex<Inner>>,
    pub net: Arc<super::net::Net>,
    base_url: String,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for FakeDav {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn etag(body: &Bytes) -> String {
    format!("\"{}\"", hestia::manifest::Hash32::digest(body))
}

fn parent(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(i) => trimmed[..=i].to_owned(),
        None => String::new(),
    }
}

fn authorized(req: &Request) -> bool {
    let want = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{USER}:{PASSWORD}"))
    );
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        == Some(want.as_str())
}

async fn handle(State(state): State<AppState>, req: Request) -> Response {
    let path = req.uri().path().trim_start_matches('/').to_owned();
    let method = req.method().clone();
    let [range, if_match, if_none_match] = [header::RANGE, header::IF_MATCH, header::IF_NONE_MATCH]
        .map(|name| {
            req.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        });
    let authed = authorized(&req);
    {
        let inner = state.inner.lock().unwrap();
        let read = matches!(method, Method::GET | Method::HEAD) || method.as_str() == "PROPFIND";
        if !authed && !(inner.public && read) {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Basic realm=\"dav\"")],
            )
                .into_response();
        }
        if !read && inner.read_only {
            return StatusCode::FORBIDDEN.into_response();
        }
    }
    let body = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap();
    let mut inner = state.inner.lock().unwrap();
    match method.as_str() {
        "PUT" => {
            if !inner.dirs.contains(&parent(&path)) {
                return match inner.nginx {
                    true => StatusCode::INTERNAL_SERVER_ERROR,
                    false => StatusCode::CONFLICT,
                }
                .into_response();
            }
            let current = inner.files.get(&path).map(|(b, _)| etag(b));
            if !inner.nginx {
                if if_none_match.as_deref() == Some("*") && current.is_some() {
                    return StatusCode::PRECONDITION_FAILED.into_response();
                }
                if let Some(want) = &if_match
                    && current.as_deref() != Some(want.as_str())
                {
                    return StatusCode::PRECONDITION_FAILED.into_response();
                }
            }
            let clock = inner.clock;
            let existed = inner.files.insert(path, (body, clock)).is_some();
            match existed {
                true => StatusCode::NO_CONTENT,
                false => StatusCode::CREATED,
            }
            .into_response()
        }
        "MKCOL" => {
            inner.mkcols += 1;
            if !path.ends_with('/') || !inner.dirs.contains(&parent(&path)) {
                return StatusCode::CONFLICT.into_response();
            }
            match inner.dirs.insert(path) {
                true => StatusCode::CREATED,
                false => StatusCode::METHOD_NOT_ALLOWED,
            }
            .into_response()
        }
        "GET" | "HEAD" => {
            let Some((body, _)) = inner.files.get(&path).cloned() else {
                return StatusCode::NOT_FOUND.into_response();
            };
            if method == Method::HEAD {
                let headers = [
                    (header::CONTENT_LENGTH, body.len().to_string()),
                    (header::ETAG, etag(&body)),
                ];
                return (StatusCode::OK, headers).into_response();
            }
            super::common::serve_range(body, range.as_deref())
        }
        "DELETE" => match inner.files.remove(&path).is_some() {
            true => StatusCode::NO_CONTENT.into_response(),
            false => StatusCode::NOT_FOUND.into_response(),
        },
        "PROPFIND" => {
            let dir = format!("{}/", path.trim_end_matches('/'));
            if !inner.dirs.contains(&dir) {
                return StatusCode::NOT_FOUND.into_response();
            }
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n<D:multistatus xmlns:D=\"DAV:\">\n",
            );
            let entry = |href: &str, dir: bool, t: u64| {
                let rt = if dir { "<D:collection/>" } else { "" };
                format!(
                    "<D:response><D:href>/{href}</D:href><D:propstat><D:prop>\
                     <D:getlastmodified>{}</D:getlastmodified>\
                     <D:resourcetype>{rt}</D:resourcetype></D:prop>\
                     <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>\n",
                    httpdate::fmt_http_date(
                        std::time::UNIX_EPOCH + std::time::Duration::from_secs(t)
                    )
                )
            };
            xml.push_str(&entry(&dir, true, 0));
            for d in inner.dirs.iter().filter(|d| parent(d) == dir) {
                xml.push_str(&entry(d, true, 0));
            }
            for (f, (_, t)) in inner.files.iter().filter(|(f, _)| parent(f) == dir) {
                xml.push_str(&entry(&f.replace('-', "%2D"), false, *t));
            }
            xml.push_str("</D:multistatus>\n");
            (
                StatusCode::MULTI_STATUS,
                [(header::CONTENT_TYPE, "application/xml; charset=utf-8")],
                xml,
            )
                .into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

impl FakeDav {
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake dav listener");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let mut inner = Inner::default();
        // The share itself exists; everything below it hestia creates.
        inner.dirs.insert(String::new());
        inner.dirs.insert("dav/".to_owned());
        inner.dirs.insert(format!("{PREFIX}/"));
        let inner = Arc::new(Mutex::new(inner));
        let net = Arc::new(super::net::Net::default());
        let router = net.layer(
            Router::new()
                .fallback(axum::routing::any(handle))
                .with_state(AppState {
                    inner: inner.clone(),
                }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        FakeDav {
            inner,
            net,
            base_url,
            server,
        }
    }

    pub fn url(&self) -> String {
        format!("{}/{PREFIX}", self.base_url)
    }

    fn dav(&self, auth: Option<(String, String)>) -> Backend {
        Backend::Dir(BlobDir::dav(&self.url(), auth, reqwest::Client::new()).expect("dav backend"))
    }

    pub fn backend(&self) -> Backend {
        self.dav(Some((USER.to_owned(), PASSWORD.to_owned())))
    }

    pub fn anonymous(&self) -> Backend {
        self.dav(None)
    }

    /// The same tree over plain HTTP, as `HESTIA_S3=https://…` reads it.
    pub fn plain_http(&self) -> Backend {
        Backend::Dir(
            BlobDir::s3(&self.url(), None, "unused", None, reqwest::Client::new())
                .expect("http backend"),
        )
    }

    pub fn set_clock(&self, t: u64) {
        self.inner.lock().unwrap().clock = t;
    }

    pub fn clock(&self) -> Clock {
        let inner = self.inner.clone();
        Arc::new(move || inner.lock().unwrap().clock)
    }

    pub fn set_public(&self, public: bool) {
        self.inner.lock().unwrap().public = public;
    }

    pub fn set_read_only(&self, read_only: bool) {
        self.inner.lock().unwrap().read_only = read_only;
    }

    /// 500 for a missing parent and no conditional requests.
    pub fn set_nginx(&self, nginx: bool) {
        self.inner.lock().unwrap().nginx = nginx;
    }

    pub fn mkcols(&self) -> u64 {
        self.inner.lock().unwrap().mkcols
    }

    pub fn set_rtt(&self, rtt: std::time::Duration) {
        self.net.set_rtt(rtt);
    }

    pub fn take_requests(&self) -> Vec<String> {
        self.net.take()
    }

    /// Raw file paths on the share.
    pub fn files(&self) -> Vec<String> {
        self.inner.lock().unwrap().files.keys().cloned().collect()
    }
}
