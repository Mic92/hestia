//! Shared storage core for hestia and pheme.
//!
//! Everything here is backend-agnostic: content-defined chunking into
//! zstd-compressed packs, the mergeable manifest, store path info lookup,
//! reference normalization, and the daemon's unix-socket protocol. The
//! hestia crate layers the GitHub Actions cache backend on top; pheme
//! layers iroh-based peer-to-peer transfer on top.

pub mod chunker;
pub mod manifest;
pub mod pathinfo;
pub mod protocol;
pub mod refnorm;
