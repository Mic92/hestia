//! Integration test for `hestia prefetch`: HTTP planning followed by the
//! batched Nix substitution and import operations.

use std::os::unix::fs::PermissionsExt as _;

use axum::Router;
use axum::routing::get;

const HESTIA_BIN: &str = env!("CARGO_BIN_EXE_hestia");

async fn external_references() -> &'static str {
    concat!(
        "/nix/store/00000000000000000000000000000002-valid\n",
        "/nix/store/00000000000000000000000000000003-missing",
    )
}

async fn closure() -> &'static [u8] {
    b"nix-export-stream"
}

#[tokio::test]
async fn prepares_and_imports_in_order() {
    let temp = tempfile::tempdir().unwrap();
    let fake_nix = temp.path().join("nix");
    let fake_nix_store = temp.path().join("nix-store");
    std::fs::write(
        &fake_nix,
        r#"#!/bin/sh
printf 'nix %s\n' "$*" >> "$PREFETCH_LOG"
cat > "$PREFETCH_PATHS"
"#,
    )
    .unwrap();
    std::fs::write(
        &fake_nix_store,
        r#"#!/bin/sh
printf 'nix-store %s\n' "$*" >> "$PREFETCH_LOG"
cat > "$PREFETCH_IMPORT"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&fake_nix, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&fake_nix_store, std::fs::Permissions::from_mode(0o755)).unwrap();

    let router = Router::new()
        .route(
            "/closure/{hashes}/external-references",
            get(external_references),
        )
        .route("/closure/{hashes}", get(closure));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let log = temp.path().join("nix-store.log");
    let prepared = temp.path().join("prepared");
    let imported = temp.path().join("imported");
    let path = std::env::join_paths(std::iter::once(temp.path().to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))
    .unwrap();
    let output = tokio::process::Command::new(HESTIA_BIN)
        .arg("prefetch")
        .args([
            "/nix/store/00000000000000000000000000000000-a.drv^*",
            "/nix/store/11111111111111111111111111111111-b.drv^*",
        ])
        .env("HESTIA_LISTEN", addr.to_string())
        .env("PATH", path)
        .env("PREFETCH_LOG", &log)
        .env("PREFETCH_PATHS", &prepared)
        .env("PREFETCH_IMPORT", &imported)
        .output()
        .await
        .unwrap();
    server.abort();

    assert!(
        output.status.success(),
        "prefetch failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(&log).unwrap(),
        concat!("nix build --no-link --stdin\n", "nix-store --import\n",)
    );
    assert_eq!(
        std::fs::read_to_string(&prepared).unwrap(),
        concat!(
            "/nix/store/00000000000000000000000000000002-valid\n",
            "/nix/store/00000000000000000000000000000003-missing\n",
        )
    );
    assert_eq!(std::fs::read(&imported).unwrap(), b"nix-export-stream");
}
