use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub(super) struct Args {
    #[arg(short, long)]
    pub(super) config: Option<PathBuf>,
    #[command(subcommand)]
    pub(super) commands: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub(super) enum Commands {
    Set {
        #[arg(short, long)]
        key: String,
        #[arg(short, long)]
        value: String,
    },
    Get {
        #[arg(short, long)]
        key: String,
    },
}
