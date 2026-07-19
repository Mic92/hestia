//! Hestia: a Nix binary cache backed by the GitHub Actions cache (v2 API).
//!
//! The library half of the crate holds everything that integration tests
//! need to reach; the `hestia` binary in `main.rs` is a thin CLI on top.

pub use hestia_core::{chunker, manifest, pathinfo, protocol, refnorm};

pub mod cli;
pub mod drain;
pub mod gc;
pub mod gha;
pub mod hook;
pub mod matrix;
pub mod pipeline;
pub mod serve;
pub mod substituter;
pub mod upstream;
