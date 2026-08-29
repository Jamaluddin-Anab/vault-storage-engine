use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub(crate) struct Args {
    #[arg(short, long)]
    pub(crate) config: Option<PathBuf>,
}
