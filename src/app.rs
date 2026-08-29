use crate::cli::Args;
use crate::config::Config;
use crate::error::AppError;
use crate::logging::Logging;
use std::path::{Path, PathBuf};
use tracing::info;

pub(crate) struct App;

impl App {
    pub(crate) fn run(args: Args) -> Result<(), AppError> {
        let config = Self::load_config(&args.config)?;
        Self::initialize_logging(&config)?;

        info!("App run successfully");

        Ok(())
    }

    fn load_config(config: &Option<PathBuf>) -> Result<Config, AppError> {
        let path = config.as_deref().unwrap_or(Path::new("config.toml"));
        Config::load_config(path)
    }

    fn initialize_logging(config: &Config) -> Result<(), AppError> {
        Logging::initialize_logging(config)?;
        Ok(())
    }
}
