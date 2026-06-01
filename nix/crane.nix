# Crane builds: dependencies live in a separate derivation (cargoArtifacts)
# keyed on Cargo.toml/Cargo.lock, so source-only changes do not recompile
# them. The static release binary uses nix/package.nix instead.
{
  pkgs,
  lib,
  craneLib,
}:
let
  src = craneLib.cleanCargoSource ../.;

  commonArgs = {
    inherit src;
    pname = "hestia";
    strictDeps = true;
    # reqwest's rustls-platform-verifier needs CA certs to construct any
    # client, even for plain-HTTP localhost use; the sandbox has none.
    env.SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
  };

  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
in
{
  # Tests run as the separate `tests` check.
  package = craneLib.buildPackage (
    commonArgs
    // {
      inherit cargoArtifacts;
      doCheck = false;
      meta = {
        description = "Nix binary cache backed by the GitHub Actions cache (v2 API)";
        homepage = "https://github.com/Mic92/hestia";
        license = lib.licenses.mit;
        mainProgram = "hestia";
      };
    }
  );

  clippy = craneLib.cargoClippy (
    commonArgs
    // {
      inherit cargoArtifacts;
      cargoClippyExtraArgs = "--all-targets -- --deny warnings";
    }
  );

  tests = craneLib.cargoTest (
    commonArgs
    // {
      inherit cargoArtifacts;
      # The integration tests drive real nix tooling (scratch stores,
      # signing, nix copy) inside the sandbox.
      nativeBuildInputs = [ pkgs.nix ];
      # nix needs a writable HOME.
      preBuild = ''
        export HOME="$(mktemp -d)"
      '';
    }
  );
}
