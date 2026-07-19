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
    ];

    buildInputs = with pkgs; [
      rust-analyzer
      rustfmt
      clippy
    ];

    RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
  }
