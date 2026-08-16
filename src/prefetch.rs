//! `hestia prefetch`: prepare external references, then bulk-import a
//! closure from the local Hestia daemon.

use std::process::{ExitCode, ExitStatus, Stdio};

use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use crate::cli::{DEFAULT_LISTEN, PrefetchArgs};
use crate::manifest::{PathHash, StorePath};
use crate::pathinfo::StoreDir;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("invalid store path or drv installable: {0}")]
    InvalidPath(String),
    #[error("request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cannot run {command}: {source}")]
    Spawn {
        command: &'static str,
        source: std::io::Error,
    },
    #[error("{operation} failed with {status}")]
    Nix {
        operation: &'static str,
        status: ExitStatus,
    },
    #[error("writing paths to nix build failed: {0}")]
    PrepareWrite(#[source] std::io::Error),
    #[error("writing the closure to nix-store --import failed: {0}")]
    ImportWrite(#[source] std::io::Error),
}

fn root_hashes(paths: &[String]) -> Result<String, Error> {
    let store_dir = StoreDir::default();
    let mut hashes = Vec::with_capacity(paths.len());
    for installable in paths {
        let path = installable.strip_suffix("^*").unwrap_or(installable);
        let path = store_dir
            .parse::<StorePath>(path)
            .map_err(|_| Error::InvalidPath(installable.clone()))?;
        hashes.push(PathHash::from_store_path(&path).to_string());
    }
    Ok(hashes.join(","))
}

async fn prepare_external_paths(paths: &str) -> Result<(), Error> {
    if paths.is_empty() {
        return Ok(());
    }
    // Literal store paths (without `^` output selectors) are opaque Nix
    // installables: this substitutes even `.drv` paths without building
    // their outputs. Stdin keeps large closures clear of ARG_MAX.
    let mut child = Command::new("nix")
        .args(["build", "--no-link", "--stdin"])
        .stdin(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|source| Error::Spawn {
            command: "nix build",
            source,
        })?;
    let mut stdin = child.stdin.take().expect("nix stdin was piped");
    stdin
        .write_all(paths.as_bytes())
        .await
        .map_err(Error::PrepareWrite)?;
    stdin.write_all(b"\n").await.map_err(Error::PrepareWrite)?;
    drop(stdin);
    let status = child.wait().await.map_err(|source| Error::Spawn {
        command: "nix build",
        source,
    })?;
    if !status.success() {
        return Err(Error::Nix {
            operation: "nix build --no-link",
            status,
        });
    }
    Ok(())
}

async fn import_closure(mut response: reqwest::Response) -> Result<(), Error> {
    let mut child = Command::new("nix-store")
        .arg("--import")
        .stdin(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|source| Error::Spawn {
            command: "nix-store --import",
            source,
        })?;
    let mut stdin = child.stdin.take().expect("nix-store stdin was piped");
    while let Some(chunk) = response.chunk().await? {
        stdin.write_all(&chunk).await.map_err(Error::ImportWrite)?;
    }
    drop(stdin);
    let status = child.wait().await.map_err(|source| Error::Spawn {
        command: "nix-store --import",
        source,
    })?;
    if !status.success() {
        return Err(Error::Nix {
            operation: "nix-store --import",
            status,
        });
    }
    Ok(())
}

async fn run_inner(args: &PrefetchArgs) -> Result<(), Error> {
    let hashes = root_hashes(&args.paths)?;
    let listen = args
        .listen
        .clone()
        .or_else(|| std::env::var("HESTIA_LISTEN").ok())
        .unwrap_or_else(|| DEFAULT_LISTEN.to_string());
    let base = format!("http://{listen}");
    let http = reqwest::Client::new();

    let external_references = http
        .get(format!("{base}/closure/{hashes}/external-references"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    prepare_external_paths(&external_references).await?;

    let closure = http
        .get(format!("{base}/closure/{hashes}"))
        .send()
        .await?
        .error_for_status()?;
    import_closure(closure).await
}

pub async fn run(args: &PrefetchArgs) -> ExitCode {
    match run_inner(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("hestia prefetch: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_are_extracted_from_matrix_installables() {
        let paths = vec![
            "/nix/store/00000000000000000000000000000000-a.drv^*".to_string(),
            "/nix/store/11111111111111111111111111111111-b.drv".to_string(),
        ];
        assert_eq!(
            root_hashes(&paths).unwrap(),
            "00000000000000000000000000000000,11111111111111111111111111111111"
        );
    }

    #[test]
    fn malformed_installable_is_rejected() {
        let err = root_hashes(&[".#check".to_string()]).unwrap_err();
        assert!(matches!(err, Error::InvalidPath(_)));
    }
}
