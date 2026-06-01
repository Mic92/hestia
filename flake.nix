{
  description = "Nix binary cache backed by the GitHub Actions cache";

  inputs.nixpkgs.url = "git+https://github.com/NixOS/nixpkgs?shallow=1&ref=nixpkgs-unstable";
  inputs.treefmt-nix.url = "github:numtide/treefmt-nix";
  inputs.treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";

  outputs =
    {
      self,
      nixpkgs,
      treefmt-nix,
    }:
    let
      inherit (nixpkgs) lib;

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
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
          default = pkgs.callPackage ./nix/package.nix { };
        }
        // lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          # Statically linked (musl) build for release binaries: nix-built
          # dynamic binaries reference /nix/store library paths and cannot
          # run outside the store they were built in.
          static = pkgs.pkgsStatic.callPackage ./nix/package.nix { };
        }
      );

      formatter = eachSystem ({ system, ... }: treefmt.${system}.config.build.wrapper);

      checks = eachSystem (
        { system, ... }:
        {
          treefmt = treefmt.${system}.config.build.check self;
        }
      );
    };
}
