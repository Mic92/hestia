//! Shared support code for integration tests.
//!
//! Every integration-test binary compiles its own copy of this module and
//! uses a different subset of the helpers, so unused-item warnings are
//! meaningless here.
#![allow(dead_code)]

pub mod common;
pub mod fake_gha;
pub mod fake_oci;
pub mod fake_s3;
pub mod real;
pub mod sim;
pub mod store;
