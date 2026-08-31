use crate::cli::{Args, Commands};
use crate::config::Config;
use crate::error::AppError;
use crate::logging::Logging;
use crate::storage::storage_engine::StorageEngine;
use std::path::{Path, PathBuf};
use tracing::info;

pub(crate) struct App;

impl App {
    pub(crate) fn run(args: Args) -> Result<(), AppError> {
        let config = Self::load_config(&args.config)?;
        Self::initialize_logging(&config)?;

        if let Some(commands) = args.commands {
            let mut engine = StorageEngine::start()?;
            match commands {
                Commands::Set { key, value } => {
                    engine.put(key, value)?;
                }
                Commands::Get { key } => match engine.get(key)? {
                    Some(value) => info!("value: {value}"),
                    None => info!("value not found"),
                },
            }
        }

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
