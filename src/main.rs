use std::process::ExitCode;

use clap::Parser;

use hestia::cli::{Cli, Command};
use hestia::{drain, gc, hook, matrix, serve};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => serve::run(&args).await,
        Command::Matrix(args) => matrix::run(&args).await,
        Command::Hook(args) => hook::run(&args).await,
        Command::Drain(args) => drain::run(&args).await,
        Command::Gc(args) => gc::run(&args).await,
    }
}
