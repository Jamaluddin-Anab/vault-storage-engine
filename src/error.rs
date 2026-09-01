use std::io::Error;
use std::string::FromUtf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum AppError {
    #[error("read config file error: {0}")]
    ReadConfigFile(Error),
    #[error("can not convert string to config struct: {0}")]
    ConvertToConfigStruct(toml::de::Error),
    #[error("can not initialize logging: {0}")]
    InitializeLogging(Box<dyn std::error::Error + Send + Sync>),
    #[error("can not load .db file: {0}")]
    LoadDbFile(Error),
    #[error("can not seek inside .db file: {0}")]
    SeekInDb(Error),
    #[error("can not read key, value len from .db file: {0}")]
    ReadHeaderLen(Error),
    #[error("can not read key from .db file: {0}")]
    ReadKey(Error),
    #[error("can not convert utf-8 to string: {0}")]
    ConvertUtf8ToString(FromUtf8Error),
    #[error("can not read value from .db file: {0}")]
    ReadValue(Error),
    #[error("invalid key or value len key <= 256 value <= 1024 * 1024 ===> key :{0} value: {1}")]
    InvalidKeyValueLen(usize, usize),
    #[error("can not write data to db file. {0}")]
    WriteToDb(Error),
    #[error("key len should be 256 : {0}")]
    InvalidKey(usize),
    #[error("storage corrupted.")]
    CorruptedDb,
}
