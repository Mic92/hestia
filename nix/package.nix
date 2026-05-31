{
  lib,
  rustPlatform,
  cacert,
}:
rustPlatform.buildRustPackage {
  pname = "hestia";
  version = "0.1.0";

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

  meta = {
    description = "Nix binary cache backed by the GitHub Actions cache";
    homepage = "https://github.com/nix-community/hestia";
    license = lib.licenses.mit;
    mainProgram = "hestia";
  };
}
