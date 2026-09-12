use crate::app::App;
use crate::cli::Args;
use clap::Parser;

mod app;
mod cli;
mod config;
mod error;
mod logging;
mod recovery;
mod storage;
mod wal;

fn main() {
    let args = Args::parse();
    if let Err(err) = App::run(args) {
        eprintln!("application can not run: {err}");
        std::process::exit(-1);
    }
}
