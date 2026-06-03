{
  description = "Nix binary cache backed by the GitHub Actions cache";

  inputs.nixpkgs.url = "git+https://github.com/NixOS/nixpkgs?shallow=1&ref=nixpkgs-unstable";
  inputs.treefmt-nix.url = "github:numtide/treefmt-nix";
  inputs.treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
  inputs.crane.url = "github:ipetkov/crane";

  outputs =
    {
      self,
      nixpkgs,
      treefmt-nix,
      crane,
    }:
    let
      inherit (nixpkgs) lib;

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      eachSystem =
        f:
        lib.genAttrs systems (
          system:
          f {
            inherit system;
            pkgs = nixpkgs.legacyPackages.${system};
          }
        );

      treefmt = eachSystem ({ pkgs, ... }: treefmt-nix.lib.evalModule pkgs ./nix/treefmt.nix);

      # Crane builds (package, clippy, tests) with shared dependency artifacts.
      # Linux targets musl, so CI tests the same static binaries releases
      # ship; darwin stays dynamic. The cross rustc runs on glibc and ships
      # std for both glibc and musl. Not pkgsStatic: its stdenv forces
      # -static onto build scripts and proc macros, which must link
      # dynamically.
      craneFor =
        pkgs:
        let
          isLinux = pkgs.stdenv.hostPlatform.isLinux;
          craneLib = (crane.mkLib pkgs).overrideScope (
            _final: _prev:
            lib.optionalAttrs isLinux {
              inherit (pkgs.pkgsStatic.buildPackages) rustc clippy;
            }
          );
        in
        import ./nix/crane.nix {
          inherit pkgs lib craneLib;
          staticTarget = if isLinux then pkgs.pkgsStatic.stdenv else null;
        };
    in
    {
      devShells = eachSystem (
        { pkgs, ... }:
        {
          default = pkgs.callPackage ./nix/devShell.nix { };
        }
      );

      packages = eachSystem (
        { pkgs, ... }:
        {
          default = (craneFor pkgs).package;
          # Real-API test binary; CI's token-probe job substitutes it.
          gha-real-tests = (craneFor pkgs).ghaRealTests;
        }
      );

      formatter = eachSystem ({ system, ... }: treefmt.${system}.config.build.wrapper);

      # Everything CI verifies: `nix flake check` runs the formatter check,
      # clippy, the test suite, and builds the package.
      checks = eachSystem (
        { pkgs, system, ... }:
        {
          treefmt = treefmt.${system}.config.build.check self;
          package = self.packages.${system}.default;
          clippy = (craneFor pkgs).clippy;
          tests = (craneFor pkgs).tests;
          gha-real-tests = self.packages.${system}.gha-real-tests;
        }
      );
    };
}
