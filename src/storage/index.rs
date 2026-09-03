use crate::error::AppError;
use crate::storage::storage_engine::ReadStatus::{CompleteRead, CorruptTail, Eof};
use crate::storage::storage_engine::StorageEngine;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(super) struct Index {
    pub(super) file: File,
    pub(super) index: HashMap<String, u64>,
}

impl Index {
    const MAX_LEN: usize = 256;

    pub(super) fn load_index() -> Result<Index, AppError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(Path::new("index.db"))
            .map_err(AppError::LoadIndexFile)?;

        let index = Self::build_index(&mut file)?;

        Ok(Index { file, index })
    }

    pub(super) fn build_index(file: &mut File) -> Result<HashMap<String, u64>, AppError> {
        let mut index = HashMap::<String, u64>::new();
        file.seek(SeekFrom::Start(0))
            .map_err(AppError::SeekInIndex)?;

        loop {
            let key_len = match StorageEngine::rebuild_len(file)? {
                Eof => break,
                CompleteRead(key_len) => key_len,
                CorruptTail => return Err(AppError::CorruptedIndex),
            };
            if key_len > Self::MAX_LEN as u64 {
                return Err(AppError::CorruptedIndex);
            }

            let mut buf = vec![0u8; key_len as usize];
            file.read_exact(&mut buf)
                .map_err(|_| AppError::CorruptedIndex)?;
            let key = String::from_utf8(buf).map_err(AppError::ConvertUtf8ToString)?;

            let mut buf = [0u8; 8];
            file.read_exact(&mut buf)
                .map_err(|_| AppError::CorruptedIndex)?;
            let offset = u64::from_le_bytes(buf);

            index.insert(key, offset);
        }

        Ok(index)
    }

    pub(super) fn put(&mut self, key: String, offset: u64) -> Result<(), AppError> {
        if key.is_empty() || key.len() > Self::MAX_LEN {
            return Err(AppError::InvalidKey(key.len()));
        }

        self.file
            .seek(SeekFrom::End(0))
            .map_err(AppError::SeekInIndex)?;
        let key_len = key.len() as u32;

        let record = [
            key_len.to_le_bytes().as_slice(),
            key.as_bytes(),
            &offset.to_le_bytes(),
        ]
        .concat();

        self.file
            .write_all(&record)
            .map_err(AppError::WriteToIndex)?;
        self.file.flush().map_err(AppError::WriteToIndex)?;
        // update in Memory
        self.index.insert(key, offset);

        Ok(())
    }

    pub(super) fn get(&self, key: &str) -> Option<&u64> {
        self.index.get(key)
    }

    pub(super) fn update_memory_map(&mut self, new_map: HashMap<String, u64>, new_file: File) {
        self.index = new_map;
        self.file = new_file;
    }
}
