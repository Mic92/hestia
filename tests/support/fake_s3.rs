//! Behavioral fake of an S3-compatible bucket (path style), modelling the
//! portable subset hestia relies on and the quirks of the less friendly
//! stores:
//!
//! * PUT, GET with `Range` (206, 416 past the end), HEAD, DELETE (204
//!   whether or not the key existed)
//! * ListObjectsV2 with `prefix`, `max-keys`, continuation tokens,
//!   URL-encoded keys, and new keys invisible to listings for `list_lag`
//!   further requests (B2, Wasabi, Garage are eventually consistent)
//! * requests must carry a SigV4 query signature for the access key,
//!   anonymous GET/HEAD allowed when the bucket is public
//! * no conditional PUT, last writer wins

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};

use hestia::backend::Backend;
use hestia::backend::s3::S3;
use hestia::gha::rest::format_timestamp;
use hestia::pipeline::Clock;
use rusty_s3::Credentials;

pub const BUCKET: &str = "ci-cache";
pub const PREFIX: &str = "hestia";
pub const ACCESS_KEY: &str = "AKIAFAKE";
pub const SECRET_KEY: &str = "fake-secret";
pub const REGION: &str = "garage";

#[derive(Default)]
struct Inner {
    /// key → (body, written at clock, written at request count)
    objects: BTreeMap<String, (Bytes, u64, u64)>,
    clock: u64,
    requests: u64,
    list_lag: u64,
    public: bool,
    read_only: bool,
}

#[derive(Clone)]
struct AppState {
    inner: Arc<Mutex<Inner>>,
}

pub struct FakeS3 {
    inner: Arc<Mutex<Inner>>,
    base_url: String,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for FakeS3 {
    fn drop(&mut self) {
        self.server.abort();
    }
}

fn xml(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

fn error(status: StatusCode, code: &str) -> Response {
    xml(
        status,
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code></Error>"),
    )
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn handle(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(q): Query<BTreeMap<String, String>>,
    req: Request,
) -> Response {
    let Some(key) = path
        .strip_prefix(BUCKET)
        .and_then(|k| k.strip_prefix('/').or(k.is_empty().then_some("")))
    else {
        return error(StatusCode::NOT_FOUND, "NoSuchBucket");
    };
    let key = key.to_owned();
    let method = req.method().clone();
    let range = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let signed = q
        .get("X-Amz-Credential")
        .is_some_and(|c| c.starts_with(&format!("{ACCESS_KEY}/")))
        && q.contains_key("X-Amz-Signature");
    {
        let mut inner = state.inner.lock().unwrap();
        inner.requests += 1;
        let allowed = match method {
            Method::GET | Method::HEAD => signed || inner.public,
            _ => signed && !inner.read_only,
        };
        if !allowed {
            return error(StatusCode::FORBIDDEN, "AccessDenied");
        }
    }
    if key.is_empty() {
        return match (method, q.get("list-type").map(String::as_str)) {
            (Method::GET, Some("2")) => list(&state, &q),
            _ => error(StatusCode::BAD_REQUEST, "unsupported bucket operation"),
        };
    }
    match method {
        Method::PUT => {
            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .unwrap();
            let mut inner = state.inner.lock().unwrap();
            let (clock, requests) = (inner.clock, inner.requests);
            inner.objects.insert(key, (body, clock, requests));
            StatusCode::OK.into_response()
        }
        Method::GET | Method::HEAD => {
            let Some((body, _, _)) = state.inner.lock().unwrap().objects.get(&key).cloned() else {
                return error(StatusCode::NOT_FOUND, "NoSuchKey");
            };
            if method == Method::HEAD {
                let len = [(header::CONTENT_LENGTH, body.len().to_string())];
                return (StatusCode::OK, len).into_response();
            }
            let Some(spec) = range else {
                return (StatusCode::OK, body).into_response();
            };
            let parse = || -> Option<(usize, usize)> {
                let (s, e) = spec.strip_prefix("bytes=")?.split_once('-')?;
                let s: usize = s.parse().ok()?;
                let e = if e.is_empty() {
                    body.len().checked_sub(1)?
                } else {
                    e.parse::<usize>().ok()?.min(body.len().checked_sub(1)?)
                };
                (s <= e).then_some((s, e))
            };
            match parse() {
                Some((s, e)) => (
                    StatusCode::PARTIAL_CONTENT,
                    [(
                        header::CONTENT_RANGE,
                        format!("bytes {s}-{e}/{}", body.len()),
                    )],
                    body.slice(s..=e),
                )
                    .into_response(),
                None => (
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    [(header::CONTENT_RANGE, format!("bytes */{}", body.len()))],
                )
                    .into_response(),
            }
        }
        Method::DELETE => {
            state.inner.lock().unwrap().objects.remove(&key);
            StatusCode::NO_CONTENT.into_response()
        }
        _ => error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
    }
}

fn list(state: &AppState, q: &BTreeMap<String, String>) -> Response {
    let inner = state.inner.lock().unwrap();
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let max: usize = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000)
        .min(1000);
    let after = q
        .get("continuation-token")
        .or(q.get("start-after"))
        .cloned()
        .unwrap_or_default();
    let visible_before = inner.requests.saturating_sub(inner.list_lag);
    let mut keys = inner
        .objects
        .iter()
        .filter(|(k, (_, _, at))| {
            k.starts_with(&prefix) && k.as_str() > after.as_str() && *at < visible_before
        })
        .map(|(k, (b, created, _))| (k.clone(), b.len(), *created));
    let page: Vec<_> = keys.by_ref().take(max).collect();
    let truncated = keys.next().is_some();
    let mut body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>{BUCKET}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{max}</MaxKeys>\
         <EncodingType>url</EncodingType><IsTruncated>{truncated}</IsTruncated>",
        escape(&prefix),
        page.len()
    );
    if truncated {
        body.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape(&page.last().unwrap().0)
        ));
    }
    for (key, size, created) in &page {
        let encoded = percent_encode(key);
        body.push_str(&format!(
            "<Contents><Key>{encoded}</Key><LastModified>{}</LastModified>\
             <ETag>&quot;0&quot;</ETag><Size>{size}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            format_timestamp(*created)
        ));
    }
    body.push_str("</ListBucketResult>");
    xml(StatusCode::OK, body)
}

fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

impl FakeS3 {
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake s3 listener");
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let inner = Arc::new(Mutex::new(Inner::default()));
        let router = Router::new()
            .route("/{*path}", axum::routing::any(handle))
            .with_state(AppState {
                inner: inner.clone(),
            });
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        FakeS3 {
            inner,
            base_url,
            server,
        }
    }

    pub fn url(&self) -> String {
        format!("s3://{BUCKET}/{PREFIX}")
    }

    fn s3(&self, credentials: Option<Credentials>) -> Backend {
        Backend::S3(
            S3::new(
                &self.url(),
                Some(&self.base_url),
                REGION,
                credentials,
                reqwest::Client::new(),
            )
            .expect("s3 backend"),
        )
    }

    pub fn backend(&self) -> Backend {
        self.s3(Some(Credentials::new(ACCESS_KEY, SECRET_KEY)))
    }

    /// No credentials: reads work once the bucket is `set_public`.
    pub fn anonymous(&self) -> Backend {
        self.s3(None)
    }

    pub fn set_clock(&self, t: u64) {
        self.inner.lock().unwrap().clock = t;
    }

    pub fn clock(&self) -> Clock {
        let inner = self.inner.clone();
        Arc::new(move || inner.lock().unwrap().clock)
    }

    /// New objects stay out of listings for this many further requests.
    pub fn set_list_lag(&self, requests: u64) {
        self.inner.lock().unwrap().list_lag = requests;
    }

    pub fn set_public(&self, public: bool) {
        self.inner.lock().unwrap().public = public;
    }

    /// Writes answer 403, like credentials scoped to `s3:GetObject`.
    pub fn set_read_only(&self, read_only: bool) {
        self.inner.lock().unwrap().read_only = read_only;
    }

    /// Raw object paths under the bucket.
    pub fn keys(&self) -> Vec<String> {
        self.inner.lock().unwrap().objects.keys().cloned().collect()
    }
}
