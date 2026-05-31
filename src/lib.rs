//! Hestia: a Nix binary cache backed by the GitHub Actions cache (v2 API).
//!
//! The library half of the crate holds everything that integration tests
//! need to reach; the `hestia` binary in `main.rs` is a thin CLI on top.

pub mod gha;
pub mod manifest;
