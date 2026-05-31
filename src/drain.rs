//! `hestia drain`: tell the daemon to upload pending paths and commit.

use std::process::ExitCode;

use crate::cli::DrainArgs;

pub async fn run(args: &DrainArgs) -> ExitCode {
    eprintln!(
        "hestia drain: not implemented yet \
         (would ask daemon at {} to upload and commit, timeout {}s)",
        args.socket.display(),
        args.timeout,
    );
    ExitCode::FAILURE
}
