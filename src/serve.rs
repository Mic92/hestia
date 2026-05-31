//! `hestia serve`: the per-job daemon (hook listener + write pipeline).

use std::process::ExitCode;

use crate::cli::ServeArgs;

pub async fn run(args: &ServeArgs) -> ExitCode {
    eprintln!(
        "hestia serve: not implemented yet \
         (would listen on {} for substitution requests, \
         accept hook connections on {}, idle-exit: {:?})",
        args.listen,
        args.socket.display(),
        args.idle_exit,
    );
    ExitCode::FAILURE
}
