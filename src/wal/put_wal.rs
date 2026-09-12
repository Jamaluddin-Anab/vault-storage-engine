use crate::error::AppError;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub(crate) enum Operation {
    WriteInDb = 1,
    WriteInIndex = 2,
}
impl Operation {
    pub(crate) fn to_bytes(self) -> [u8; 1] {
        [self as u8]
    }
}

pub(crate) struct PutWal {
    pub(crate) file: File,
}

impl PutWal {
    pub(crate) fn new() -> Result<PutWal, AppError> {
        let file = Self::create_put_file()?;
        Ok(PutWal { file })
    }

    fn create_put_file() -> Result<File, AppError> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(Path::new("put.wal"))
            .map_err(AppError::CreateWalFile)
    }

    pub(crate) fn write(
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

        self.file.write_all(&record).map_err(AppError::WriteToWal)?;
        self.file.sync_all().map_err(AppError::WriteToWal)?;

        Ok(())
    }

    pub(crate) fn mark_write_index(&mut self) -> Result<(), AppError> {
        self.file
            .seek(SeekFrom::Start(0))
            .map_err(AppError::SeekInWal)?;

        self.file
            .write_all(Operation::WriteInIndex.to_bytes().as_slice())
            .map_err(AppError::WriteToWal)?;

        self.file.sync_all().map_err(AppError::WriteToWal)?;

        Ok(())
    }

    pub(crate) fn clear(&mut self) -> Result<(), AppError> {
        self.file.set_len(0).map_err(AppError::CleanWalFile)?;

        self.file.sync_all().map_err(AppError::CleanWalFile)?;

        self.file
            .seek(SeekFrom::Start(0))
            .map_err(AppError::SeekInWal)?;

        Ok(())
    }
}
