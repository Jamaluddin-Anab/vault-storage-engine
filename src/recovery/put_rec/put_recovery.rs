use crate::error::AppError;
use crate::recovery::put_rec::db_recover::DbRecovery;
use crate::recovery::put_rec::index_recovery::IndexRecovery;
use crate::recovery::put_rec::put_recovery::Operation::{WriteInDb, WriteInIndex};
use crate::recovery::put_rec::put_recovery::ReadFileStatus::{EndOfFile, Record};
use crate::storage::storage_engine::StorageEngine;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
enum Operation {
    WriteInDb = 1,
    WriteInIndex = 2,
}
enum ReadOperationStatus {
    Eof,
    CompleteRead(Operation),
    InterruptedFile,
}
impl Operation {
    pub(crate) fn to_bytes(self) -> [u8; 1] {
        [self as u8]
    }
}

#[derive(Debug)]
pub(super) struct WalRecord {
    operation: Operation,
    pub(super) key: String,
    pub(super) value: String,
    pub(super) offset: u64,
}
#[derive(Debug)]
enum ReadFileStatus {
    EndOfFile,
    Record(WalRecord),
}

pub(crate) struct PutRecovery {
    put_file: File,
}
impl PutRecovery {
    pub(crate) fn start() -> Result<PutRecovery, AppError> {
        let put_file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(Path::new("put.wal"))
            .map_err(AppError::ReadWalFile)?;
        Ok(PutRecovery { put_file })
    }

    pub(crate) fn recovery(&mut self) -> Result<(), AppError> {
        let wal_record = match self.read_put_wal()? {
            EndOfFile => return Ok(()),
            Record(wal_record) => wal_record,
        };

        match wal_record.operation {
            WriteInDb => DbRecovery::start(self).db_recovery(wal_record),
            WriteInIndex => IndexRecovery::start(self).index_recovery(wal_record),
        }
    }

    fn read_put_wal(&mut self) -> Result<ReadFileStatus, AppError> {
        let operation = match self.read_operation()? {
            ReadOperationStatus::Eof => return Ok(EndOfFile),
            ReadOperationStatus::CompleteRead(operation) => operation,
            ReadOperationStatus::InterruptedFile => return Err(AppError::CorruptedWal),
        };
        let (key_len, value_len) = self.read_key_value_len()?;
        let (key, value, offset) = self.read_key_value_offset(key_len, value_len)?;

        Ok(Record(WalRecord {
            operation,
            key,
            value,
            offset,
        }))
    }

    fn read_operation(&mut self) -> Result<ReadOperationStatus, AppError> {
        let mut operation_buf = [0u8; 1];
        match self.put_file.read(&mut operation_buf) {
            Ok(0) => Ok(ReadOperationStatus::Eof),
            Ok(1) => match operation_buf[0] {
                1 => Ok(ReadOperationStatus::CompleteRead(WriteInDb)),
                2 => Ok(ReadOperationStatus::CompleteRead(WriteInIndex)),
                _ => Err(AppError::CorruptedWal),
            },
            Ok(_) => Ok(ReadOperationStatus::InterruptedFile),
            Err(_) => Err(AppError::CorruptedWal),
        }
    }

    fn read_key_value_len(&mut self) -> Result<(u64, u64), AppError> {
        let key_len = self.read_len()?;
        let value_len = self.read_len()?;

        if key_len == 0
            || value_len == 0
            || key_len > StorageEngine::KEY_LEN
            || value_len > StorageEngine::VALUE_LEN
        {
            return Err(AppError::CorruptedWal);
        }
        Ok((key_len, value_len))
    }

    fn read_len(&mut self) -> Result<u64, AppError> {
        let mut len_buf = [0u8; 4];
        match self.put_file.read(&mut len_buf) {
            Ok(4) => Ok(u32::from_le_bytes(len_buf) as u64),
            Ok(_) => Err(AppError::CorruptedWal),
            Err(_) => Err(AppError::CorruptedWal),
        }
    }

    fn read_key_value_offset(
        &mut self,
        key_len: u64,
        value_len: u64,
    ) -> Result<(String, String, u64), AppError> {
        let key = self.read_key(key_len)?;
        let value = self.read_value(value_len)?;
        let offset = self.read_offset()?;
        Ok((key, value, offset))
    }

    fn read_key(&mut self, key_len: u64) -> Result<String, AppError> {
        let mut key_buf = vec![0u8; key_len as usize];
        self.put_file
            .read_exact(&mut key_buf)
            .map_err(|_| AppError::CorruptedWal)?;
        let key = String::from_utf8(key_buf).map_err(|_| AppError::CorruptedWal)?;
        Ok(key)
    }

    fn read_value(&mut self, value_len: u64) -> Result<String, AppError> {
        let mut value_buf = vec![0u8; value_len as usize];
        self.put_file
            .read_exact(&mut value_buf)
            .map_err(|_| AppError::CorruptedWal)?;
        let value = String::from_utf8(value_buf).map_err(|_| AppError::CorruptedWal)?;
        Ok(value)
    }

    fn read_offset(&mut self) -> Result<u64, AppError> {
        let mut offset_buf = [0u8; 8];
        self.put_file
            .read_exact(&mut offset_buf)
            .map_err(|_| AppError::CorruptedWal)?;
        let offset = u64::from_le_bytes(offset_buf);
        Ok(offset)
    }

    pub(super) fn mark_write_db(&mut self, wal_record: WalRecord) -> Result<(), AppError> {
        self.put_file
            .seek(SeekFrom::Start(0))
            .map_err(AppError::SeekInWal)?;
        self.put_file
            .write_all(WriteInIndex.to_bytes().as_slice())
            .map_err(AppError::WriteToWal)?;
        self.put_file.sync_all().map_err(AppError::WriteToWal)?;

        IndexRecovery::start(self).index_recovery(wal_record)
    }

    pub(super) fn clear_put_wal_file(&mut self) -> Result<(), AppError> {
        self.put_file.set_len(0).map_err(AppError::CleanWalFile)?;
        self.put_file.sync_all().map_err(AppError::CleanWalFile)?;
        Ok(())
    }
}
