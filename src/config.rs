use crate::error::AppError;
use crate::logging::Logging;
use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize, Debug)]
pub(crate) struct Config {
    pub(crate) logging: Logging,
}

impl Config {
    pub(crate) fn load_config(path: &Path) -> Result<Config, AppError> {
        let file = std::fs::read_to_string(path).map_err(AppError::ReadConfigFile)?;
        let config =
            toml::from_str::<Config>(file.as_str()).map_err(AppError::ConvertToConfigStruct)?;
        Ok(config)
    }
}
