//! `hestia gc`: mark/sweep garbage collection over the GHA cache (Phase 5).

use std::process::ExitCode;

use crate::cli::GcArgs;

pub fn run(args: &GcArgs) -> ExitCode {
    eprintln!(
        "hestia gc: not implemented yet \
         (would mark/sweep the GHA cache; dry-run: {}, grace: {}d, root-ttl: {}d)",
        args.dry_run, args.grace, args.root_ttl,
    );
    ExitCode::SUCCESS
}
