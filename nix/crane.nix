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

  # The gha_real integration test binary as an installable package, so CI
  # can substitute it instead of recompiling the workspace. Build only:
  # the tests need real GHA credentials.
  ghaRealTests = craneLib.mkCargoDerivation (
    commonArgs
    // {
      inherit cargoArtifacts;
      pname = "gha-real-tests";
      doInstallCargoArtifacts = false;
      nativeBuildInputs = [
        pkgs.jq
        pkgs.makeWrapper
      ];
      # The test executable lands in target/.../deps/ with a hash suffix;
      # --message-format json reports its exact path.
      buildPhaseCargoCommand = ''
        cargoWithProfile test --test gha_real --no-run --message-format json > cargo-test-build.json
      '';
      installPhaseCommand = ''
        bin=$(jq -r 'select(.reason == "compiler-artifact" and .target.name == "gha_real" and .executable != null) | .executable' cargo-test-build.json | tail -n1)
        if [ -z "$bin" ]; then
          echo "error: could not locate the gha_real test executable" >&2
          exit 1
        fi
        install -D -m755 "$bin" "$out/bin/gha-real-tests"
        # rustls-platform-verifier needs CA certs to construct any reqwest
        # client; set-default keeps the runner's own SSL_CERT_FILE if set.
        wrapProgram "$out/bin/gha-real-tests" \
          --set-default SSL_CERT_FILE ${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt
      '';
      meta.mainProgram = "gha-real-tests";
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
