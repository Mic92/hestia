{ pkgs }:
(pkgs.mkShell.override {
  stdenv =
    if pkgs.stdenv.hostPlatform.isElf then
      pkgs.stdenvAdapters.useMoldLinker pkgs.stdenv
    else
      pkgs.stdenv;
})
  {
    nativeBuildInputs = with pkgs; [
      rustc
      cargo
      cargo-watch
      cargo-nextest
      nix-eval-jobs
      cosign
      rustfs
      distribution
      # WebDAV real-server test; stock nginx lacks dav_ext (PROPFIND)
      (nginx.override { modules = [ nginxModules.dav ]; })
    ];

    buildInputs = with pkgs; [
      rust-analyzer
      rustfmt
      clippy
    ];

    RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
  }
