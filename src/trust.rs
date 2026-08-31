//! Who may publish heads into which root. A head's proof is a sigstore
//! bundle holding an in-toto attestation over the payload, made with
//! `cosign attest-blob`. Per root glob the policy names a verifier
//! (`cosign verify-blob-attestation` or `gh attestation verify`) and its
//! arguments, first matching glob wins.

use std::path::Path;
use std::process::{Output, Stdio};
use std::sync::Arc;

use tokio::process::Command;

pub const ENV_TRUST: &str = "HESTIA_TRUST";
pub const ENV_SIGN: &str = "HESTIA_SIGN";
/// Row for `g-*`.
pub const GC_ROW: &str = "@gc";
pub const PREDICATE_TYPE: &str = "https://github.com/Mic92/hestia/head/v1";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{name}: {reason}")]
    InvalidEnv { name: &'static str, reason: String },
    #[error("signing: {0}")]
    Io(#[from] std::io::Error),
    #[error("cosign attest-blob failed: {0}")]
    Sign(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tool {
    Cosign,
    Gh,
}

#[derive(Debug)]
struct Row {
    glob: String,
    tool: Tool,
    args: Vec<String>,
}

#[derive(Debug, Default)]
struct Inner {
    /// `None` publishes unsigned.
    sign: Option<Vec<String>>,
    /// Empty accepts anything.
    rows: Vec<Row>,
}

#[derive(Debug, Clone, Default)]
pub struct Trust(Arc<Inner>);

fn glob(pattern: &str, s: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == s,
        Some((head, tail)) => {
            s.len() >= head.len() + tail.len()
                && s.starts_with(head)
                && (tail.is_empty()
                    || (0..=s.len() - head.len() - tail.len())
                        .any(|i| glob(tail, &s[head.len() + i..])))
        }
    }
}

fn words(s: &str) -> Vec<String> {
    s.split_whitespace().map(str::to_owned).collect()
}

fn parse_row(line: &str) -> Result<Row, Error> {
    let invalid = |reason: String| Error::InvalidEnv {
        name: ENV_TRUST,
        reason,
    };
    let mut it = line.split_whitespace();
    let (Some(pat), Some(tool)) = (it.next(), it.next()) else {
        return Err(invalid(format!(
            "row `{line}`: want <glob> <cosign|gh> <args>"
        )));
    };
    let tool = match tool {
        "cosign" => Tool::Cosign,
        "gh" => Tool::Gh,
        t => return Err(invalid(format!("row `{line}`: unknown verifier {t}"))),
    };
    let args: Vec<String> = it.map(str::to_owned).collect();
    if args.is_empty() {
        return Err(invalid(format!("row `{line}` has no verify arguments")));
    }
    Ok(Row {
        glob: pat.to_owned(),
        tool,
        args,
    })
}

async fn run(cmd: &str, args: &[&str], more: &[String], last: &Path) -> std::io::Result<Output> {
    Command::new(cmd)
        .args(args)
        .args(more)
        .arg(last)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await
}

impl Trust {
    pub fn open() -> Trust {
        Trust::default()
    }

    /// `rows`: one `<root glob | @gc> <cosign|gh> <verify args>` per line.
    /// `sign`: `cosign attest-blob` args, empty for keyless.
    pub fn new(rows: &str, sign: Option<&str>) -> Result<Trust, Error> {
        let rows = rows
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(parse_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Trust(Arc::new(Inner {
            sign: sign.map(words),
            rows,
        })))
    }

    pub fn from_env() -> Result<Trust, Error> {
        let var = |k| std::env::var(k).ok();
        Trust::new(
            &var(ENV_TRUST).unwrap_or_default(),
            var(ENV_SIGN).as_deref(),
        )
    }

    pub fn is_open(&self) -> bool {
        self.0.rows.is_empty()
    }

    /// Empty without a signer.
    pub async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let Some(args) = &self.0.sign else {
            return Ok(vec![]);
        };
        let dir = tempfile::tempdir()?;
        let [blob, bundle, predicate] =
            ["blob", "bundle.json", "predicate.json"].map(|f| dir.path().join(f));
        tokio::fs::write(&blob, payload).await?;
        tokio::fs::write(&predicate, b"{}").await?;
        let fixed = [
            "attest-blob",
            "--yes",
            "--type",
            PREDICATE_TYPE,
            "--predicate",
            predicate.to_str().expect("utf8"),
            "--bundle",
            bundle.to_str().expect("utf8"),
        ];
        let out = run("cosign", &fixed, args, &blob).await?;
        if !out.status.success() {
            return Err(Error::Sign(
                String::from_utf8_lossy(&out.stderr).trim().to_owned(),
            ));
        }
        Ok(tokio::fs::read(&bundle).await?)
    }

    /// Only rows sharing the first matching glob count, so a catch-all
    /// below `main-*` does not widen main.
    pub async fn verify(&self, root: &str, payload: &[u8], bundle: &[u8]) -> bool {
        if self.is_open() {
            return true;
        }
        let matches = |p: &str| match root {
            GC_ROW => p == GC_ROW,
            _ => p != GC_ROW && glob(p, root),
        };
        let Some(first) = self.0.rows.iter().find(|r| matches(&r.glob)) else {
            return false;
        };
        if bundle.is_empty() {
            return false;
        }
        let Ok(dir) = tempfile::tempdir() else {
            return false;
        };
        let [blob, file] = ["blob", "bundle.json"].map(|f| dir.path().join(f));
        if tokio::fs::write(&blob, payload).await.is_err()
            || tokio::fs::write(&file, bundle).await.is_err()
        {
            return false;
        }
        let file = file.to_str().expect("utf8");
        for row in self.0.rows.iter().filter(|r| r.glob == first.glob) {
            let out = match row.tool {
                Tool::Cosign => {
                    let fixed = [
                        "verify-blob-attestation",
                        "--type",
                        PREDICATE_TYPE,
                        "--bundle",
                        file,
                    ];
                    run("cosign", &fixed, &row.args, &blob).await
                }
                Tool::Gh => {
                    let fixed = [
                        "attestation",
                        "verify",
                        "--predicate-type",
                        PREDICATE_TYPE,
                        "--bundle",
                        file,
                    ];
                    run("gh", &fixed, &row.args, &blob).await
                }
            };
            match out {
                Ok(o) if o.status.success() => return true,
                Ok(o) => eprintln!(
                    "hestia: {root}: proof rejected by {:?}: {}",
                    row.tool,
                    String::from_utf8_lossy(&o.stderr).trim()
                ),
                Err(e) => eprintln!("hestia: {root}: cannot run {:?}: {e}", row.tool),
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn globs() {
        assert!(glob("main-*", "main-x86_64-linux"));
        assert!(!glob("main-*", "pr-1-main-x"));
        assert!(glob("*", "anything"));
        assert!(glob("pr-*-linux", "pr-12-x86_64-linux"));
        assert!(!glob("pr-*-linux", "pr-12-darwin"));
        assert!(glob("exact", "exact") && !glob("exact", "exactly"));
    }

    #[test]
    fn rows_parse() {
        let t = Trust::new(
            "main-* cosign --key k.pub\n# c\n\n@gc gh --repo o/r --source-ref refs/heads/main",
            Some(""),
        )
        .unwrap();
        assert_eq!(t.0.rows.len(), 2);
        assert_eq!(t.0.rows[1].tool, Tool::Gh);
        assert!(!t.is_open());
        assert!(Trust::new("main-* cosign", None).is_err());
        assert!(Trust::new("main-* openssl x", None).is_err());
        assert!(Trust::open().is_open());
    }
}
