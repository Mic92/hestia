//! Hestia: a Nix binary cache backed by the GitHub Actions cache (v2 API).
//!
//! The library half of the crate holds everything that integration tests
//! need to reach; the `hestia` binary in `main.rs` is a thin CLI on top.

pub mod chunker;
pub mod cli;
pub mod drain;
pub mod gc;
pub mod gha;
pub mod hook;
pub mod manifest;
pub mod pathinfo;
pub mod pipeline;
pub mod protocol;
pub mod refnorm;
pub mod serve;
pub mod substituter;
pub mod upstream;
