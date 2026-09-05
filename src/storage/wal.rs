use crate::error::AppError;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

#[repr(u8)]
#[derive(Copy, Clone)]
pub(super) enum Operation {
    WriteInDb = 1,
    WriteInIndex = 2,
}
impl Operation {
    pub(super) fn to_bytes(self) -> [u8; 1] {
        [self as u8]
    }
}

#[allow(dead_code)]
pub(super) struct Wal {
    pub(super) put_file: File,
    pub(super) compact_file: File,
}

impl Wal {
    pub(super) fn new() -> Result<Wal, AppError> {
        let put_file = Self::create_file("put.wal")?;
        let compact_file = Self::create_file("compact.wal")?;
        Ok(Wal {
            put_file,
            compact_file,
        })
    }

    fn create_file(file_name: &str) -> Result<File, AppError> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(Path::new(file_name))
            .map_err(AppError::CreateWalFile)
    }

    pub(super) fn put(
        &mut self,
        operation: Operation,
        key: &str,
        value: &str,
        offset: u64,
    ) -> Result<(), AppError> {
        let key_len = key.len() as u32;
        let value_len = value.len() as u32;
        let record = [
            operation.to_bytes().as_slice(),
            key_len.to_le_bytes().as_slice(),
            value_len.to_le_bytes().as_slice(),
            key.as_bytes(),
            value.as_bytes(),
            &offset.to_le_bytes(),
        ]
        .concat();

        self.put_file
            .write_all(&record)
            .map_err(AppError::WriteToWal)?;
        self.put_file.sync_all().map_err(AppError::WriteToWal)?;
        Ok(())
    }

    pub(crate) fn mark_write_index(&mut self) -> Result<(), AppError> {
        self.put_file
            .seek(SeekFrom::Start(0))
            .map_err(AppError::SeekInWal)?;
        self.put_file
            .write_all(Operation::WriteInIndex.to_bytes().as_slice())
            .map_err(AppError::WriteToWal)?;
        self.put_file.sync_all().map_err(AppError::WriteToWal)?;
        Ok(())
    }

    pub(super) fn clear_wal_put(&mut self) -> Result<(), AppError> {
        self.put_file.set_len(0).map_err(AppError::CleanWalFile)?;
        self.put_file
            .seek(SeekFrom::Start(0))
            .map_err(AppError::SeekInWal)?;
        Ok(())
    }
}
