use crate::error::AppError;
use crate::storage::recovery::ReadOperationStatus::CompleteRead;
use crate::storage::wal::Operation;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::Path;
use crate::storage::recovery::ReadPutWalStatus::{EndOfFile, Record};

#[derive(Debug)]
pub(super) struct WalRecord {
    pub(super) operation: Operation,
    pub(super) key: String,
    pub(super) value: String,
    pub(super) offset: u64,
}

enum ReadOperationStatus {
    Eof,
    CompleteRead(Operation),
    InterruptedFile
}

#[derive(Debug)]
pub(super) enum  ReadPutWalStatus {
    EndOfFile,
    Record(WalRecord)
}

pub(super) struct Recovery {
    pub(super) put_file: File,
}

impl Recovery {
    pub(super) fn start() -> Result<Recovery, AppError> {
        let put_file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(Path::new("put.wal"))
            .map_err(AppError::ReadWalFile)?;
        Ok(Recovery{put_file})
    }

    pub(super) fn read_put_wal(&mut self) -> Result<ReadPutWalStatus, AppError> {
        let operation = match self.read_operation()? {
                ReadOperationStatus::Eof => { return Ok(EndOfFile) },
                CompleteRead(operation) => operation,
                ReadOperationStatus::InterruptedFile => { return Err(AppError::CorruptedWal) }
        };

        let key_len = self.read_len()?;
        let value_len = self.read_len()?;

        let mut key_buf = vec![0u8; key_len as usize];
        self.put_file.read_exact(&mut key_buf).map_err(|_| AppError::CorruptedWal)?;
        let key = String::from_utf8(key_buf).map_err(AppError::ConvertUtf8ToString)?;


        let mut value_buf = vec![0u8; value_len as usize];
        self.put_file.read_exact(&mut value_buf).map_err(|_| AppError::CorruptedWal)?;
        let value= String::from_utf8(value_buf).map_err(AppError::ConvertUtf8ToString)?;

        let mut offset_buf = [0u8; 8];
        self.put_file.read_exact(&mut offset_buf).map_err(|_| AppError::CorruptedWal)?;
        let offset = u64::from_le_bytes(offset_buf);

        Ok(Record(WalRecord{operation, key, value, offset}))
    }

    fn read_operation(&mut self) -> Result<ReadOperationStatus, AppError> {
        let mut operation_buf = [0u8; 1];
        match self.put_file.read(&mut operation_buf) {
            Ok(0) => Ok(ReadOperationStatus::Eof),
            Ok(1) => {
                match operation_buf[0] {
                    1 => Ok(CompleteRead(Operation::WriteInDb)),
                    2 => Ok(CompleteRead(Operation::WriteInIndex)),
                    _ => Err(AppError::CorruptedWal)
                }
            },
            Ok(_) => Ok(ReadOperationStatus::InterruptedFile),
            Err(_) => Err(AppError::CorruptedWal)
        }
    }

    fn read_len(&mut self) -> Result<u64, AppError> {
        let mut len_buf = [0u8; 4];
        match self.put_file.read(&mut len_buf) {
            Ok(4) => Ok(u32::from_le_bytes(len_buf) as u64),
            Ok(_) => Err(AppError::CorruptedWal),
            Err(_) => Err(AppError::CorruptedWal)
        }
    }
}

