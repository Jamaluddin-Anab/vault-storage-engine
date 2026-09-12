use crate::error::AppError;
use crate::recovery::put_rec::put_recovery::{PutRecovery, WalRecord};
use crate::storage::storage_engine::ReadStatus::{CorruptTail, Eof};
use crate::storage::storage_engine::{ReadStatus, StorageEngine};
use std::fs::{File, OpenOptions};
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(super) struct IndexRecovery<'a> {
    put_recovery: &'a mut PutRecovery,
}

impl<'a> IndexRecovery<'a> {
    pub(super) fn start(put_recovery: &'a mut PutRecovery) -> Self {
        Self { put_recovery }
    }

    fn get_index_file() -> Result<File, Error> {
        OpenOptions::new()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(Path::new("index.db"))
    }

    pub(super) fn index_recovery(&mut self, wal_record: WalRecord) -> Result<(), AppError> {
        let mut index_file = Self::get_index_file().map_err(AppError::LoadIndexFile)?;
        loop {
            let record_start = index_file
                .stream_position()
                .map_err(AppError::SeekInIndex)?;

            let key_len = match self.get_key_len(&mut index_file) {
                Ok(key_len) => key_len,
                Err(_) => {
                    self.truncate_index_file(&mut index_file, record_start)?;
                    return self.write_in_index(wal_record, index_file);
                }
            };

            let key = match self.get_key(&mut index_file, key_len) {
                Ok(key) => key,
                Err(_) => {
                    self.truncate_index_file(&mut index_file, record_start)?;
                    return self.write_in_index(wal_record, index_file);
                }
            };

            let offset = match self.read_offset(&mut index_file) {
                Ok(offset) => offset,
                Err(_) => {
                    self.truncate_index_file(&mut index_file, record_start)?;
                    return self.write_in_index(wal_record, index_file);
                }
            };

            if key.eq(&wal_record.key) {
                return if offset == wal_record.offset {
                    self.put_recovery.clear_put_wal_file()?;
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
                    self.put_recovery.clear_put_wal_file()?;
                    Ok(())
                };
            }
        }
    }

    fn read_offset(&mut self, index_file: &mut File) -> Result<u64, AppError> {
        let mut offset_buf = [0u8; 8];
        index_file
            .read_exact(&mut offset_buf)
            .map_err(AppError::ReadOffset)?;
        Ok(u64::from_le_bytes(offset_buf))
    }

    fn get_key(&mut self, index_file: &mut File, key_len: u64) -> Result<String, AppError> {
        let mut key_buf = vec![0u8; key_len as usize];
        index_file
            .read_exact(&mut key_buf)
            .map_err(AppError::ReadKey)?;
        let key = String::from_utf8(key_buf).map_err(AppError::ConvertUtf8ToString)?;
        Ok(key)
    }

    fn get_key_len(&mut self, index_file: &mut File) -> Result<u64, AppError> {
        let key_len = match StorageEngine::rebuild_len(index_file)? {
            Eof => return Err(AppError::KeyLenNotFound),
            ReadStatus::CompleteRead(key_len) => key_len,
            CorruptTail => return Err(AppError::KeyLenNotFound),
        };
        Ok(key_len)
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

        self.put_recovery.clear_put_wal_file()?;

        Ok(())
    }
}
