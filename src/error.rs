use std::io::Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum AppError {
    #[error("read config file error: {0}")]
    ReadConfigFile(Error),
    #[error("can not convert string to config struct: {0}")]
    ConvertToConfigStruct(toml::de::Error),
    #[error("can not initialize logging: {0}")]
    InitializeLogging(Box<dyn std::error::Error + Send + Sync>),
}
