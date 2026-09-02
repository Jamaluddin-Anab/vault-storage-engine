use crate::error::AppError;
use crate::storage::index::Index;
use crate::storage::storage_engine::ReadStatus::{CompleteRead, CorruptTail, Eof};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) struct StorageEngine {
    pub(super) file: File,
    pub(super) index: Index,
}

pub(super) enum ReadStatus {
    Eof,
    CompleteRead(u64),
    CorruptTail,
}

impl StorageEngine {
    const KEY_LEN: u64 = 256;
    const VALUE_LEN: u64 = 1024 * 1024;

    pub(crate) fn start() -> Result<StorageEngine, AppError> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(Path::new("data.db"))
            .map_err(AppError::LoadDbFile)?;

        let index = Index::load_index()?;

        Ok(StorageEngine { file, index })
    }

    pub(super) fn rebuild_len(file: &mut File) -> Result<ReadStatus, AppError> {
        let mut buf = [0u8; 4];
        match file.read(&mut buf) {
            Ok(0) => Ok(Eof), // clean end of file
            Ok(4) => Ok(CompleteRead(u32::from_le_bytes(buf) as u64)),
            Ok(_) => Ok(CorruptTail), // partially read at the very end of file (corrupt tail) EOF
            Err(err) => Err(AppError::ReadHeaderLen(err)),
        }
    }

    pub(crate) fn put(&mut self, key: String, value: String) -> Result<(), AppError> {
        if key.is_empty()
            || key.len() > Self::KEY_LEN as usize
            || value.len() > Self::VALUE_LEN as usize
        {
            return Err(AppError::InvalidKeyValueLen(key.len(), value.len()));
        }

        let offset = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(AppError::SeekInDb)?;

        let key_len = key.len() as u32;
        let value_len = value.len() as u32;

        let record = [
            key_len.to_le_bytes().as_slice(),
            value_len.to_le_bytes().as_slice(),
            key.as_bytes(),
            value.as_bytes(),
        ]
        .concat();

        self.file.write_all(&record).map_err(AppError::WriteToDb)?;
        self.file.flush().map_err(AppError::WriteToDb)?;

        self.index.put(key, offset)?;

        Ok(())
    }

    pub(crate) fn get(&mut self, key: &str) -> Result<Option<String>, AppError> {
        if key.is_empty() || key.len() > Self::KEY_LEN as usize {
            return Err(AppError::InvalidKey(key.len()));
        }

        if let Some(offset) = self.index.get(key) {
            let file = &mut self.file;

            file.seek(SeekFrom::Start(*offset))
                .map_err(AppError::SeekInDb)?;

            let key_len = match Self::rebuild_len(file)? {
                Eof => return Ok(None),
                CompleteRead(key_len) => key_len,
                CorruptTail => return Err(AppError::CorruptedDb),
            };
            let value_len = match Self::rebuild_len(file)? {
                Eof => return Ok(None),
                CompleteRead(value_len) => value_len,
                CorruptTail => return Err(AppError::CorruptedDb),
            };

            let mut buf = vec![0u8; key_len as usize];
            file.read_exact(&mut buf).map_err(AppError::ReadKey)?;
            let disk_key = String::from_utf8(buf).map_err(AppError::ConvertUtf8ToString)?;

            if !disk_key.eq(&key) {
                return Ok(None);
            }

            let mut buf = vec![0u8; value_len as usize];
            file.read_exact(&mut buf).map_err(AppError::ReadValue)?;
            let value = String::from_utf8(buf).map_err(AppError::ConvertUtf8ToString)?;

            return Ok(Some(value));
        }

        Ok(None)
    }
}
