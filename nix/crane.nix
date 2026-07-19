# Crane builds: dependencies live in a separate derivation (cargoArtifacts)
# keyed on Cargo.toml/Cargo.lock, so source-only changes do not recompile
# them.
#
# With `staticTarget` set (Linux), cargo cross-compiles to that stdenv's
# musl target and links statically; build scripts and proc macros stay
# glibc-dynamic.
{
  pkgs,
  lib,
  craneLib,
  # stdenv of the static target platform, or null for a native build.
  staticTarget ? null,
}:
let
  # Cargo sources plus the nix fixtures the integration tests evaluate.
  src = lib.cleanSourceWith {
    src = ../.;
    filter =
      path: type: (lib.hasInfix "/tests/fixtures" path) || (craneLib.filterCargoSources path type);
    name = "source";
  };

  staticArgs = lib.optionalAttrs (staticTarget != null) (
    let
      triple = staticTarget.hostPlatform.rust.rustcTarget;
      cc = "${staticTarget.cc}/bin/${staticTarget.cc.targetPrefix}cc";
      envTriple = lib.toUpper (lib.replaceStrings [ "-" ] [ "_" ] triple);
    in
    {
      CARGO_BUILD_TARGET = triple;
      "CARGO_TARGET_${envTriple}_LINKER" = cc;
      # For the cc crate (zstd-sys, aws-lc-sys C code).
      TARGET_CC = cc;
      TARGET_AR = "${staticTarget.cc.bintools}/bin/${staticTarget.cc.targetPrefix}ar";
    }
  );

  commonArgs = {
    inherit src;
    pname = "hestia";
    strictDeps = true;
    # reqwest's rustls-platform-verifier needs CA certs to construct any
    # client, even for plain-HTTP localhost use; the sandbox has none.
    env.SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
  }
  // staticArgs;

  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
in
{
  # Tests run as the separate `tests` check.
  package = craneLib.buildPackage (
    commonArgs
    // {
      inherit cargoArtifacts;
      doCheck = false;
      # Release binaries must run outside the nix store. Linux is static
      # (musl); darwin must only link system libraries.
      postInstall = lib.optionalString pkgs.stdenv.hostPlatform.isDarwin ''
        # Rust std links libiconv, and nixpkgs' rustc resolves it to the
        # nix store copy. Swap in the system one (shipped with macOS).
        # The fixup phase re-signs the binary afterwards.
        nix_libiconv=$(otool -L "$out/bin/hestia" | awk '/\/nix\/store\/.*libiconv/{print $1}')
        if [ -n "$nix_libiconv" ]; then
          install_name_tool -change "$nix_libiconv" /usr/lib/libiconv.2.dylib "$out/bin/hestia"
        fi

        if otool -L "$out/bin/hestia" | tail -n +2 | grep /nix/store; then
          echo "error: hestia links against nix store paths" >&2
          exit 1
        fi
      '';
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
      # signing, nix copy, nix-eval-jobs) inside the sandbox.
      nativeBuildInputs = [
        pkgs.nix
        pkgs.nix-eval-jobs
      ];
      # nix needs a writable HOME.
      preBuild = ''
        export HOME="$(mktemp -d)"
      '';
    }
  );
}
