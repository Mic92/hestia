//! Real third-party servers spawned per test: rustfs (S3) and the CNCF
//! distribution registry (OCI). Each `start` returns `None` when the
//! binary is not on PATH so the suite still runs without them.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use hestia::backend::Backend;
use hestia::backend::oci::Oci;
use hestia::backend::s3::S3;
use rusty_s3::actions::{CreateBucket, S3Action};
use rusty_s3::{Bucket, Credentials, UrlStyle};
use tempfile::TempDir;

const ACCESS_KEY: &str = "hestiatest";
const SECRET_KEY: &str = "hestiatestsecret";
const REGION: &str = "us-east-1";
const BUCKET: &str = "hestia-test";

pub struct Server {
    child: Child,
    _dir: TempDir,
    pub base_url: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn which(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|d| d.join(name))
        .find(|c| c.is_file())
}

impl Server {
    /// Runs `cmd` (with `{port}` and `{dir}` to fill in) and polls `ready`
    /// with a request builder until it answers 2xx.
    async fn spawn(
        cmd: impl FnOnce(u16, &std::path::Path) -> Command,
        ready: impl Fn(&str) -> reqwest::RequestBuilder,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child = cmd(port, dir.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut server = Self {
            child,
            _dir: dir,
            base_url: format!("http://127.0.0.1:{port}"),
        };
        for _ in 0..300 {
            if let Some(status) = server.child.try_wait().unwrap() {
                panic!("{}: exited early with {status}", server.base_url);
            }
            if let Ok(r) = ready(&server.base_url).send().await
                && r.status().is_success()
            {
                return server;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{}: did not come up", server.base_url);
    }

    pub async fn rustfs(http: &reqwest::Client) -> Option<Self> {
        let bin = which("rustfs")?;
        let credentials = Credentials::new(ACCESS_KEY, SECRET_KEY);
        let server = Self::spawn(
            |port, dir| {
                let mut c = Command::new(bin);
                c.args(["server", "--address", &format!("127.0.0.1:{port}")])
                    .arg(dir)
                    .env("RUSTFS_ACCESS_KEY", ACCESS_KEY)
                    .env("RUSTFS_SECRET_KEY", SECRET_KEY)
                    .env("RUSTFS_CONSOLE_ENABLE", "false")
                    .env("RUSTFS_REGION", REGION);
                c
            },
            // Ready once the bucket can be created: the port answers 503
            // before the storage layer is up.
            |base| {
                let bucket =
                    Bucket::new(base.parse().unwrap(), UrlStyle::Path, BUCKET, REGION).unwrap();
                http.put(CreateBucket::new(&bucket, &credentials).sign(Duration::from_secs(60)))
            },
        )
        .await;
        Some(server)
    }

    pub fn s3(&self, http: &reqwest::Client) -> Backend {
        Backend::S3(
            S3::new(
                &format!("s3://{BUCKET}/ci"),
                Some(&self.base_url),
                REGION,
                Some(Credentials::new(ACCESS_KEY, SECRET_KEY)),
                http.clone(),
            )
            .unwrap(),
        )
    }

    pub async fn registry(http: &reqwest::Client) -> Option<Self> {
        let bin = which("registry")?;
        let server = Self::spawn(
            |port, dir| {
                let config = dir.join("config.yml");
                std::fs::write(
                    &config,
                    format!(
                        "version: 0.1\n\
                         storage:\n  filesystem:\n    rootdirectory: {root}\n  delete:\n    enabled: true\n\
                         http:\n  addr: 127.0.0.1:{port}\n\
                         log:\n  level: error\n  accesslog:\n    disabled: true\n",
                        root = dir.join("data").display(),
                    ),
                )
                .unwrap();
                let mut c = Command::new(bin);
                c.arg("serve").arg(config);
                c
            },
            |base| http.get(format!("{base}/v2/")),
        )
        .await;
        Some(server)
    }

    pub fn oci(&self, http: &reqwest::Client) -> Backend {
        Backend::Oci(
            Oci::new(
                &format!("{}/hestia/cache", self.base_url),
                None,
                None,
                http.clone(),
            )
            .unwrap(),
        )
    }
}
