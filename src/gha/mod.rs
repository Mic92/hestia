//! Client for the GitHub Actions cache (v2 "results" API).
//!
//! Three separate HTTP surfaces are involved:
//!
//! * **Twirp** ([`twirp`]): reserve / finalize / look up cache entries.
//!   Authenticated with `ACTIONS_RUNTIME_TOKEN`, base URL from
//!   `ACTIONS_RESULTS_URL`. Only available inside Actions jobs.
//! * **Azure blob** ([`blob`]): the actual data transfer. The Twirp API hands
//!   out pre-signed SAS URLs; uploads/downloads are plain `PUT`/`GET`.
//! * **GitHub REST** ([`rest`]): list / delete, authenticated with
//!   `GITHUB_TOKEN`. Used by `hestia gc`.

pub mod blob;
pub mod rest;
pub mod savemutable;
pub mod twirp;

/// Errors shared by all GHA cache client modules.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("missing environment variable {0}")]
    MissingEnv(&'static str),

    #[error("invalid environment variable {name}: {reason}")]
    InvalidEnv { name: &'static str, reason: String },

    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error(
        "GitHub Actions rejected the runtime token during {method} (HTTP 401): the token has \
         expired (runtime tokens are only valid for ~6 hours) or is invalid; re-run the job \
         to get a fresh token"
    )]
    TokenExpired { method: String },

    #[error("twirp call {method} failed: {code}: {msg}")]
    Twirp {
        method: String,
        code: String,
        msg: String,
    },

    #[error("unexpected HTTP status {status} from {url}: {body}")]
    Status {
        status: u16,
        url: String,
        body: String,
    },

    #[error("invalid response: {0}")]
    InvalidResponse(String),

    #[error("gave up after {attempts} conflicting attempts to save {key}")]
    Conflict { key: String, attempts: u32 },

    #[error(
        "cache write denied ({reason}): the runtime token is read-only, so nothing can be \
         cached. Expected for events whose token has no writable cache scopes (check_run, \
         fork pull_request). The build is unaffected; its paths are rebuilt next time."
    )]
    WriteDenied { reason: String },
}

impl Error {
    /// True if this error is a Twirp `already_exists` response
    /// (CAS semantics: somebody else already stored this key+version).
    pub fn is_already_exists(&self) -> bool {
        matches!(self, Error::Twirp { code, .. } if code == "already_exists")
    }
}
