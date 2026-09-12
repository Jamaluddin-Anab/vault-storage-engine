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
    #[error("can not load index.db file. {0}")]
    LoadIndexFile(Error),
    #[error("can not write data to index.db file. {0}")]
    WriteToIndex(Error),
    #[error("index corrupted")]
    CorruptedIndex,
    #[error("can not seek inside index.db file: {0}")]
    SeekInIndex(Error),
    #[error("can not load temp.db file: {0}")]
    LoadTempDbFile(Error),
    #[error("can not write data to data.db.tmp file. {0}")]
    WriteToTempDb(Error),
    #[error("can not seek inside data.temp.db file: {0}")]
    SeekInTempDb(Error),
    #[error("can not replace .tmp file to .db file: {0}")]
    ReplaceDbFile(Error),
    #[error("can not write to TempIndex file: {0}")]
    WriteToTempIndex(Error),
    #[error("can not replace .tmp file to .db file: {0}")]
    ReplaceIndexFile(Error),
    #[error("can not load index.temp.db file: {0}")]
    LoadTempIndexFile(Error),
    #[error("can not create wal file: {0}")]
    CreateWalFile(Error),
    #[error("can not write to wal file: {0}")]
    WriteToWal(Error),
    #[error("can not clean wal file: {0}")]
    CleanWalFile(Error),
    #[error("can not seek in wal file: {0}")]
    SeekInWal(Error),
    #[error("can not Read wal file: {0}")]
    ReadWalFile(Error),
    #[error("wal file is corrupted.")]
    CorruptedWal,
    #[error("can not truncate index.db file: {0}")]
    TruncateIndex(Error),
    #[error("unknown compact operation detected.")]
    UnknownCompactOperation,
    #[error("key length not found.")]
    KeyLenNotFound,
    #[error("value length not found.")]
    ValueLenNotFound,
    #[error("offset can not read: {0}")]
    ReadOffset(Error),
}
