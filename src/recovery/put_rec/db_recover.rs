use crate::error::AppError;
use crate::recovery::put_rec::put_recovery::{PutRecovery, WalRecord};
use crate::storage::storage_engine::{ReadStatus, StorageEngine};
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(super) struct DbRecovery<'a> {
    put_recovery: &'a mut PutRecovery,
}

impl<'a> DbRecovery<'a> {
    pub(super) fn start(put_recovery: &'a mut PutRecovery) -> Self {
        Self { put_recovery }
    }

    pub(super) fn db_recovery(&mut self, wal_record: WalRecord) -> Result<(), AppError> {
        let mut db_file = Self::get_db_file().map_err(AppError::LoadDbFile)?;
        db_file
            .seek(SeekFrom::Start(wal_record.offset))
            .map_err(AppError::SeekInDb)?;

        let key_len = match self.get_key_len(&mut db_file) {
            Ok(key_len) => key_len,
            Err(_) => return self.write_in_db(wal_record, &mut db_file),
        };
        if key_len != wal_record.key.len() as u64 {
            return self.write_in_db(wal_record, &mut db_file);
        }

        let value_len = match self.get_value_len(&mut db_file) {
            Ok(value_len) => value_len,
            Err(_) => return self.write_in_db(wal_record, &mut db_file),
        };
        if value_len != wal_record.value.len() as u64 {
            return self.write_in_db(wal_record, &mut db_file);
        }

        let key = match self.read_key(&mut db_file, key_len) {
            Ok(key) => key,
            Err(_) => return self.write_in_db(wal_record, &mut db_file),
        };
        if !key.eq(&wal_record.key) {
            return self.write_in_db(wal_record, &mut db_file);
        }

        let value = match self.read_value(&mut db_file, value_len) {
            Ok(value) => value,
            Err(_) => return self.write_in_db(wal_record, &mut db_file),
        };
        if !value.eq(&wal_record.value) {
            return self.write_in_db(wal_record, &mut db_file);
        }

        self.put_recovery.mark_write_db(wal_record)
    }

    fn get_db_file() -> Result<File, Error> {
        OpenOptions::new()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(Path::new("data.db"))
    }

    fn read_key(&mut self, db_file: &mut File, key_len: u64) -> Result<String, AppError> {
        let mut key_buf = vec![0u8; key_len as usize];
        db_file
            .read_exact(&mut key_buf)
            .map_err(AppError::ReadKey)?;
        let key = String::from_utf8(key_buf).map_err(AppError::ConvertUtf8ToString)?;
        Ok(key)
    }

    fn read_value(&mut self, db_file: &mut File, value_len: u64) -> Result<String, AppError> {
        let mut value_buf = vec![0u8; value_len as usize];
        db_file
            .read_exact(&mut value_buf)
            .map_err(AppError::ReadValue)?;
        let value = String::from_utf8(value_buf).map_err(AppError::ConvertUtf8ToString)?;
        Ok(value)
    }

    fn get_value_len(&mut self, db_file: &mut File) -> Result<u64, AppError> {
        let value_len =
            if let ReadStatus::CompleteRead(value_len) = StorageEngine::rebuild_len(db_file)? {
                value_len
            } else {
                return Err(AppError::ValueLenNotFound);
            };
        Ok(value_len)
    }

    fn get_key_len(&mut self, db_file: &mut File) -> Result<u64, AppError> {
        let key_len =
            if let ReadStatus::CompleteRead(key_len) = StorageEngine::rebuild_len(db_file)? {
                key_len
            } else {
                return Err(AppError::KeyLenNotFound);
            };
        Ok(key_len)
    }

    fn write_in_db(&mut self, wal_record: WalRecord, db_file: &mut File) -> Result<(), AppError> {
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

        self.put_recovery.mark_write_db(wal_record)
    }
}

