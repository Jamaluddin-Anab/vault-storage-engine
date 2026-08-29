use crate::config::Config;
use crate::error::AppError;
use serde::Deserialize;
use tracing::Level;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogLevel {
    Info,
    Warning,
    Error,
}

#[derive(Deserialize, Debug)]
pub(crate) struct Logging {
    level: LogLevel,
}

impl From<&LogLevel> for Level {
    fn from(value: &LogLevel) -> Self {
        match value {
            LogLevel::Info => Level::INFO,
            LogLevel::Warning => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

impl Logging {
    pub(crate) fn initialize_logging(config: &Config) -> Result<(), AppError> {
        tracing_subscriber::fmt()
            .without_time()
            .with_level(true)
            .with_target(false)
            .with_max_level(Level::from(&config.logging.level))
            .try_init()
            .map_err(AppError::InitializeLogging)?;

        Ok(())
    }
}
