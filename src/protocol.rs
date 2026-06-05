//! The daemon's unix-socket protocol: one JSON object per line.
//!
//! Clients (`hestia hook`, `hestia drain`) connect, send a single request
//! line, and read a single response line. The protocol is internal to
//! hestia — both ends ship in the same binary — so there is no versioning
//! beyond "unknown fields are ignored".

use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};

/// Responses are a single small JSON object (at most a [`DrainStats`]), so
/// cap the line read: the default socket lives under world-writable /tmp,
/// and a hostile or buggy listener squatting there must not be able to make
/// the client buffer an unbounded line in memory (mirrors the daemon-side
/// MAX_REQUEST_BYTES cap in serve.rs).
const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error talking to the daemon: {0}")]
    Io(#[from] std::io::Error),

    #[error("malformed protocol message: {0}")]
    Json(#[from] serde_json::Error),

    #[error("daemon closed the connection without responding")]
    ConnectionClosed,

    #[error("daemon reported an error: {0}")]
    Daemon(String),
}

/// One request line from a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Register store paths for upload at the next drain.
    Add { paths: Vec<String> },
    /// Upload all buffered paths, commit the manifest, and report stats.
    /// The response is sent only after the pipeline finishes.
    Drain,
    /// Liveness check: reports how many paths are currently buffered.
    Status,
}

/// What one drain accomplished. Also the daemon's status payload.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainStats {
    /// Paths received from hooks since the daemon started.
    #[serde(default)]
    pub paths_received: usize,
    /// Paths skipped because an upstream cache already serves them.
    #[serde(default)]
    pub skipped_upstream: usize,
    /// Paths skipped because the manifest already has them
    /// (their `last_pushed` clock was bumped instead).
    #[serde(default)]
    pub skipped_existing: usize,
    /// Paths skipped because the local store does not know them.
    #[serde(default)]
    pub skipped_invalid: usize,
    /// Paths skipped because their chunked representation failed to
    /// reproduce the NAR hash recorded in the Nix database (indicates a
    /// chunker bug or store corruption; never uploaded).
    #[serde(default)]
    pub failed_verification: usize,
    /// Paths newly added to the manifest.
    #[serde(default)]
    pub pushed: usize,
    /// Chunks newly uploaded.
    #[serde(default)]
    pub new_chunks: usize,
    /// Pack blobs uploaded.
    #[serde(default)]
    pub packs_uploaded: usize,
    /// Compressed bytes uploaded (packs only, not the manifest).
    #[serde(default)]
    pub bytes_uploaded: u64,
    /// Manifest version this drain committed (`m#N`), 0 if nothing
    /// needed committing.
    #[serde(default)]
    pub manifest_version: u64,
    /// Time spent loading the manifest and querying the local store
    /// (everything before chunking starts), in milliseconds.
    #[serde(default)]
    pub load_ms: u64,
    /// Producer time spent chunking and verifying new paths, in
    /// milliseconds. Chunk, pack, and upload run pipelined, so these
    /// stage times overlap and do not sum to the drain duration.
    #[serde(default)]
    pub chunk_ms: u64,
    /// Producer time spent compressing chunks into packs, in milliseconds
    /// (excludes waiting for upload backpressure).
    #[serde(default)]
    pub pack_ms: u64,
    /// Wall time of the pipelined chunk/pack/upload section, in
    /// milliseconds. Uploads overlap the CPU stages, so this is an upper
    /// bound on upload time (throughput derived from it is a lower bound).
    #[serde(default)]
    pub upload_ms: u64,
    /// Time spent committing the manifest, in milliseconds.
    #[serde(default)]
    pub commit_ms: u64,
    /// Wall-clock duration of the whole drain, in milliseconds.
    #[serde(default)]
    pub elapsed_ms: u64,
}

/// One response line from the daemon.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    /// Error description when `ok` is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Paths currently buffered (Add/Status responses).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffered: Option<usize>,
    /// Drain results (Drain responses).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<DrainStats>,
}

impl Response {
    pub fn ok() -> Self {
        Self {
            ok: true,
            ..Self::default()
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(message.into()),
            ..Self::default()
        }
    }

    pub fn with_buffered(mut self, buffered: usize) -> Self {
        self.buffered = Some(buffered);
        self
    }

    pub fn with_stats(mut self, stats: DrainStats) -> Self {
        self.stats = Some(stats);
        self
    }

    /// Turn an error response into a Rust error (for clients).
    pub fn into_result(self) -> Result<Self, Error> {
        if self.ok {
            Ok(self)
        } else {
            Err(Error::Daemon(
                self.error.unwrap_or_else(|| "unspecified error".into()),
            ))
        }
    }
}

/// Serialize a message as one JSON line (newline-terminated).
pub fn encode_line<T: Serialize>(message: &T) -> Result<Vec<u8>, Error> {
    let mut line = serde_json::to_vec(message)?;
    line.push(b'\n');
    Ok(line)
}

/// Connect to the daemon socket, send one request, and wait for the
/// response line. Error responses are surfaced as [`Error::Daemon`].
pub async fn roundtrip(socket: &Path, request: &Request) -> Result<Response, Error> {
    let stream = harmonia_utils_io::unix_socket::connect_unix_long(socket).await?;
    let mut stream = BufReader::new(stream);

    stream.get_mut().write_all(&encode_line(request)?).await?;
    stream.get_mut().flush().await?;

    let mut response_line = String::new();
    let read = (&mut stream)
        .take(MAX_RESPONSE_BYTES)
        .read_line(&mut response_line)
        .await?;
    if read == 0 {
        return Err(Error::ConnectionClosed);
    }
    let response: Response = serde_json::from_str(&response_line)?;
    response.into_result()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_wire_format_is_stable() {
        // The hook and the daemon may come from different hestia builds
        // (e.g. a cached binary vs a fresh one), so the wire format must
        // stay stable. These assertions pin it.
        let request = Request::Add {
            paths: vec!["/nix/store/aaa-foo".into(), "/nix/store/bbb-bar".into()],
        };
        assert_eq!(
            serde_json::to_string(&request).unwrap(),
            r#"{"op":"add","paths":["/nix/store/aaa-foo","/nix/store/bbb-bar"]}"#
        );

        assert_eq!(
            serde_json::to_string(&Request::Drain).unwrap(),
            r#"{"op":"drain"}"#
        );
        assert_eq!(
            serde_json::to_string(&Request::Status).unwrap(),
            r#"{"op":"status"}"#
        );
    }

    #[test]
    fn request_round_trips() {
        for request in [
            Request::Add {
                paths: vec!["/nix/store/aaa-foo".into()],
            },
            Request::Add { paths: vec![] },
            Request::Drain,
            Request::Status,
        ] {
            let line = encode_line(&request).unwrap();
            assert!(line.ends_with(b"\n"));
            let decoded: Request = serde_json::from_slice(&line).unwrap();
            assert_eq!(decoded, request);
        }
    }

    #[test]
    fn response_round_trips_and_omits_empty_fields() {
        let response = Response::ok().with_buffered(3);
        let json = serde_json::to_string(&response).unwrap();
        assert_eq!(json, r#"{"ok":true,"buffered":3}"#);

        let stats = DrainStats {
            paths_received: 5,
            pushed: 2,
            skipped_upstream: 3,
            packs_uploaded: 1,
            bytes_uploaded: 12345,
            manifest_version: 7,
            ..DrainStats::default()
        };
        let response = Response::ok().with_stats(stats.clone());
        let decoded: Response =
            serde_json::from_str(&serde_json::to_string(&response).unwrap()).unwrap();
        assert_eq!(decoded.stats, Some(stats));

        // Error responses survive the trip through into_result.
        let error = Response::error("manifest upload failed");
        let decoded: Response =
            serde_json::from_str(&serde_json::to_string(&error).unwrap()).unwrap();
        match decoded.into_result() {
            Err(Error::Daemon(message)) => assert_eq!(message, "manifest upload failed"),
            other => panic!("expected daemon error, got {other:?}"),
        }
    }

    #[test]
    fn unknown_fields_are_ignored() {
        // Forward compatibility between hestia versions sharing a socket.
        let request: Request =
            serde_json::from_str(r#"{"op":"add","paths":["/nix/store/x"],"future_field":42}"#)
                .unwrap();
        assert_eq!(
            request,
            Request::Add {
                paths: vec!["/nix/store/x".into()]
            }
        );

        let response: Response =
            serde_json::from_str(r#"{"ok":true,"buffered":1,"future_field":[1,2,3]}"#).unwrap();
        assert!(response.ok);
        assert_eq!(response.buffered, Some(1));
    }

    #[tokio::test]
    async fn roundtrip_caps_response_size() {
        // A squatter on the socket path streaming newline-free bytes must
        // produce a bounded error, not an unbounded allocation.
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hook.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            use tokio::io::AsyncWriteExt as _;
            let garbage = vec![b'x'; 64 * 1024];
            // Well past MAX_RESPONSE_BYTES, never a newline.
            for _ in 0..64 {
                if stream.write_all(&garbage).await.is_err() {
                    break;
                }
            }
        });

        let result = roundtrip(&socket, &Request::Status).await;
        assert!(
            matches!(result, Err(Error::Json(_))),
            "capped read must surface as a parse error, got {result:?}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn roundtrip_against_unreachable_socket_is_io_error() {
        let result = roundtrip(Path::new("/nonexistent/hestia/hook.sock"), &Request::Status).await;
        assert!(matches!(result, Err(Error::Io(_))));
    }
}
