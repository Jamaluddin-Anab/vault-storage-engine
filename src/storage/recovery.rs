use crate::error::AppError;
use crate::storage::recovery::ReadOperationStatus::CompleteRead;
use crate::storage::recovery::ReadPutWalStatus::{EndOfFile, Record};
use crate::storage::storage_engine::ReadStatus::{CorruptTail, Eof};
use crate::storage::storage_engine::{ReadStatus, StorageEngine};
use crate::storage::wal::Operation;
use crate::storage::wal::Operation::{WriteInDb, WriteInIndex};
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::path::Path;

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
    InterruptedFile,
}

#[derive(Debug)]
pub(super) enum ReadPutWalStatus {
    EndOfFile,
    Record(WalRecord),
}

pub(crate) struct Recovery {
    pub(super) put_file: File,
}

impl Recovery {
    pub(crate) fn start() -> Result<Recovery, AppError> {
        let put_file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .create(true)
            .open(Path::new("put.wal"))
            .map_err(AppError::ReadWalFile)?;
        Ok(Recovery { put_file })
    }

    pub(super) fn read_put_wal(&mut self) -> Result<ReadPutWalStatus, AppError> {
        let operation = match self.read_operation()? {
            ReadOperationStatus::Eof => return Ok(EndOfFile),
            CompleteRead(operation) => operation,
            ReadOperationStatus::InterruptedFile => return Err(AppError::CorruptedWal),
        };

        let key_len = self.read_len()?;
        let value_len = self.read_len()?;

        if key_len == 0
            || value_len == 0
            || key_len > StorageEngine::KEY_LEN
            || value_len > StorageEngine::VALUE_LEN
        {
            return Err(AppError::CorruptedWal);
        }

        let mut key_buf = vec![0u8; key_len as usize];
        self.put_file
            .read_exact(&mut key_buf)
            .map_err(|_| AppError::CorruptedWal)?;
        let key = String::from_utf8(key_buf).map_err(|_| AppError::CorruptedWal)?;

        let mut value_buf = vec![0u8; value_len as usize];
        self.put_file
            .read_exact(&mut value_buf)
            .map_err(|_| AppError::CorruptedWal)?;
        let value = String::from_utf8(value_buf).map_err(|_| AppError::CorruptedWal)?;

        let mut offset_buf = [0u8; 8];
        self.put_file
            .read_exact(&mut offset_buf)
            .map_err(|_| AppError::CorruptedWal)?;
        let offset = u64::from_le_bytes(offset_buf);

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
                1 => Ok(CompleteRead(WriteInDb)),
                2 => Ok(CompleteRead(WriteInIndex)),
                _ => Err(AppError::CorruptedWal),
            },
            Ok(_) => Ok(ReadOperationStatus::InterruptedFile),
            Err(_) => Err(AppError::CorruptedWal),
        }
    }

    fn read_len(&mut self) -> Result<u64, AppError> {
        let mut len_buf = [0u8; 4];
        match self.put_file.read(&mut len_buf) {
            Ok(4) => Ok(u32::from_le_bytes(len_buf) as u64),
            Ok(_) => Err(AppError::CorruptedWal),
            Err(_) => Err(AppError::CorruptedWal),
        }
    }

    pub(crate) fn recovery(&mut self) -> Result<(), AppError> {
        let wal_record = match self.read_put_wal()? {
            EndOfFile => return Ok(()),
            Record(wal_record) => wal_record,
        };

        match wal_record.operation {
            WriteInDb => self.db_recovery(wal_record),
            WriteInIndex => self.index_recovery(wal_record),
        }
    }

    fn get_file(file_name: &str) -> Result<File, Error> {
        OpenOptions::new()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(Path::new(file_name))
    }

    pub(super) fn db_recovery(&mut self, wal_record: WalRecord) -> Result<(), AppError> {
        let mut db_file = Self::get_file("data.db").map_err(AppError::LoadDbFile)?;
        db_file
            .seek(SeekFrom::Start(wal_record.offset))
            .map_err(AppError::SeekInDb)?;

        let key_len =
            if let ReadStatus::CompleteRead(key_len) = StorageEngine::rebuild_len(&mut db_file)? {
                key_len
            } else {
                return self.write_in_db(wal_record, db_file);
            };
        if key_len != wal_record.key.len() as u64 {
            return self.write_in_db(wal_record, db_file);
        }

        let value_len = if let ReadStatus::CompleteRead(value_len) =
            StorageEngine::rebuild_len(&mut db_file)?
        {
            value_len
        } else {
            return self.write_in_db(wal_record, db_file);
        };
        if value_len != wal_record.value.len() as u64 {
            return self.write_in_db(wal_record, db_file);
        }

        let mut key_buf = vec![0u8; key_len as usize];
        if db_file.read_exact(&mut key_buf).is_err() {
            return self.write_in_db(wal_record, db_file);
        }
        let key = if let Ok(key) = String::from_utf8(key_buf) {
            key
        } else {
            return self.write_in_db(wal_record, db_file);
        };
        if !key.eq(&wal_record.key) {
            return self.write_in_db(wal_record, db_file);
        }

        let mut value_buf = vec![0u8; value_len as usize];
        if db_file.read_exact(&mut value_buf).is_err() {
            return self.write_in_db(wal_record, db_file);
        }
        let value = if let Ok(value) = String::from_utf8(value_buf) {
            value
        } else {
            return self.write_in_db(wal_record, db_file);
        };
        if !value.eq(&wal_record.value) {
            return self.write_in_db(wal_record, db_file);
        }

        self.mark_write_db(wal_record)
    }

    fn write_in_db(&mut self, wal_record: WalRecord, mut db_file: File) -> Result<(), AppError> {
        db_file
            .seek(SeekFrom::Start(wal_record.offset))
            .map_err(AppError::SeekInDb)?;
        let key_len = wal_record.key.len() as u32;
        let value_len = wal_record.value.len() as u32;
        let record = [
            key_len.to_le_bytes().as_slice(),
            value_len.to_le_bytes().as_slice(),
            wal_record.key.as_bytes(),
            wal_record.value.as_bytes(),
        ]
        .concat();
        db_file.write_all(&record).map_err(AppError::WriteToDb)?;
        db_file.sync_all().map_err(AppError::WriteToDb)?;

        self.mark_write_db(wal_record)
    }

    fn mark_write_db(&mut self, wal_record: WalRecord) -> Result<(), AppError> {
        self.put_file
            .seek(SeekFrom::Start(0))
            .map_err(AppError::SeekInWal)?;
        self.put_file
            .write_all(WriteInIndex.to_bytes().as_slice())
            .map_err(AppError::WriteToWal)?;
        self.put_file.sync_all().map_err(AppError::WriteToWal)?;

        self.index_recovery(wal_record)
    }

    pub(super) fn index_recovery(&mut self, wal_record: WalRecord) -> Result<(), AppError> {
        let mut index_file = Self::get_file("index.db").map_err(AppError::LoadIndexFile)?;
        loop {
            let record_start = index_file
                .stream_position()
                .map_err(AppError::SeekInIndex)?;

            let key_len = match StorageEngine::rebuild_len(&mut index_file)? {
                Eof => return self.write_in_index(wal_record, index_file),
                ReadStatus::CompleteRead(key_len) => key_len,
                CorruptTail => {
                    self.truncate_index_file(&mut index_file, record_start)?;
                    return self.write_in_index(wal_record, index_file);
                }
            };

            let mut key_buf = vec![0u8; key_len as usize];
            if index_file.read_exact(&mut key_buf).is_err() {
                self.truncate_index_file(&mut index_file, record_start)?;
                return self.write_in_index(wal_record, index_file);
            }
            let key = if let Ok(key) = String::from_utf8(key_buf) {
                key
            } else {
                self.truncate_index_file(&mut index_file, record_start)?;
                return self.write_in_index(wal_record, index_file);
            };
            let mut offset_buf = [0u8; 8];
            if index_file.read_exact(&mut offset_buf).is_err() {
                self.truncate_index_file(&mut index_file, record_start)?;
                return self.write_in_index(wal_record, index_file);
            }
            let offset = u64::from_le_bytes(offset_buf);

            if key.eq(&wal_record.key) {
                return if offset == wal_record.offset {
                    self.clear_put_wal_file()?;
                    Ok(())
                } else {
                    let current_position = index_file
                        .stream_position()
                        .map_err(AppError::SeekInIndex)?;
                    index_file
                        .seek(SeekFrom::Start(current_position - 8))
                        .map_err(AppError::SeekInIndex)?;
                    index_file
                        .write_all(wal_record.offset.to_le_bytes().as_slice())
                        .map_err(AppError::WriteToIndex)?;
                    index_file.sync_all().map_err(AppError::WriteToIndex)?;
                    self.clear_put_wal_file()?;
                    Ok(())
                };
            }
        }
    }

    fn truncate_index_file(
        &mut self,
        index_file: &mut File,
        position: u64,
    ) -> Result<(), AppError> {
        index_file
            .set_len(position)
            .map_err(AppError::TruncateIndex)?;
        index_file.sync_all().map_err(AppError::TruncateIndex)?;
        Ok(())
    }

    pub(super) fn clear_put_wal_file(&mut self) -> Result<(), AppError> {
        self.put_file.set_len(0).map_err(AppError::CleanWalFile)?;
        self.put_file.sync_all().map_err(AppError::CleanWalFile)?;
        Ok(())
    }

    fn write_in_index(
        &mut self,
        wal_record: WalRecord,
        mut index_file: File,
    ) -> Result<(), AppError> {
        let key_len = wal_record.key.len() as u32;
        let record = [
            key_len.to_le_bytes().as_slice(),
            wal_record.key.as_bytes(),
            &wal_record.offset.to_le_bytes(),
        ]
        .concat();

        index_file
            .seek(SeekFrom::End(0))
            .map_err(AppError::WriteToIndex)?;
        index_file
            .write_all(&record)
            .map_err(AppError::WriteToIndex)?;
        index_file.sync_all().map_err(AppError::WriteToIndex)?;

        self.clear_put_wal_file()?;

        Ok(())
    }
}
