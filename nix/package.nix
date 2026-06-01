# Static (musl) release binaries via pkgsStatic. Everything else builds
# with crane (nix/crane.nix).
{
  lib,
  rustPlatform,
  cacert,
}:
rustPlatform.buildRustPackage {
  pname = "hestia";
  # Single source of truth for the version; crane.nix reads it implicitly.
  version = (lib.importTOML ../Cargo.toml).package.version;

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.toml
      ../Cargo.lock
      ../src
      ../tests
    ];
  };

  cargoLock = {
    lockFile = ../Cargo.lock;
    # Harmonia crates are git dependencies; builtins.fetchGit avoids having
    # to maintain an outputHashes entry per crate.
    allowBuiltinFetchGit = true;
  };

  # reqwest's rustls-platform-verifier loads system CA certificates when a
  # client is constructed -- even for plain-HTTP test servers. The build
  # sandbox has none, so point it at the nixpkgs CA bundle for the tests.
  env.SSL_CERT_FILE = "${cacert}/etc/ssl/certs/ca-bundle.crt";

  # The release artifact only needs to link; the full test suite already
  # gates every merge in CI. Running it again here doubles build time and
  # peak memory, which OOM-kills the 16 GB GitHub arm64 runners.
  doCheck = false;

  meta = {
    description = "Nix binary cache backed by the GitHub Actions cache (v2 API)";
    homepage = "https://github.com/Mic92/hestia";
    license = lib.licenses.mit;
    mainProgram = "hestia";
  };
}
