use crate::error::AppError;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) struct StorageEngine {
    file: File,
    index: HashMap<String, usize>,
}

impl StorageEngine {
    const KEY_LEN: u64 = 256;
    const VALUE_LEN: u64 = 1024 * 1024;

    pub(crate) fn start() -> Result<StorageEngine, AppError> {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(Path::new("data.db"))
            .map_err(AppError::LoadDbFile)?;

        let index = Self::rebuild_index(&mut file)?;

        Ok(StorageEngine { file, index })
    }

    fn rebuild_index(file: &mut File) -> Result<HashMap<String, usize>, AppError> {
        let mut index = HashMap::<String, usize>::new();
        file.seek(SeekFrom::Start(0)).map_err(AppError::SeekInDb)?;

        loop {
            let offset = file.stream_position().map_err(AppError::SeekInDb)?;

            let key_len = match Self::rebuild_len(file)? {
                Some(len) => len,
                None => break,
            };
            let value_len = match Self::rebuild_len(file)? {
                Some(len) => len,
                None => break,
            };

            if key_len == 0 || key_len > Self::KEY_LEN || value_len > Self::VALUE_LEN {
                break;
            }

            let mut key_buf = vec![0u8; key_len as usize];
            file.read_exact(&mut key_buf).map_err(AppError::ReadKey)?;
            let key = String::from_utf8(key_buf).map_err(AppError::ConvertUtf8ToString)?;

            let mut value_buf = vec![0u8; value_len as usize];
            file.read_exact(&mut value_buf)
                .map_err(AppError::ReadValue)?;

            index.insert(key, offset as usize);
        }

        Ok(index)
    }

    fn rebuild_len(file: &mut File) -> Result<Option<u64>, AppError> {
        let mut buf = [0u8; 4];
        match file.read(&mut buf) {
            Ok(0) => Ok(None), // clean end of file
            Ok(4) => Ok(Some(u32::from_le_bytes(buf) as u64)),
            Ok(_) => Ok(None), // partially read at the very end of file (corrupt tail) EOF
            Err(err) => Err(AppError::ReadHeaderLen(err)),
        }
    }

    pub(crate) fn put(&mut self, key: &str, value: &str) -> Result<(), AppError> {
        if key.len() == 0
            || key.len() > Self::KEY_LEN as usize
            || value.len() > Self::VALUE_LEN as usize
        {
            return Err(AppError::InvalidKeyValueLen(key.len(), value.len()));
        }
        let offset = self.file.stream_position().map_err(AppError::SeekInDb)?;
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

        self.index.insert(key.to_string(), offset as usize);

        Ok(())
    }

    // pub(crate) fn get(&mut self, key: &str) -> Result<Option<String>, AppError> {
    //
    //     if key.len() == 0 || key.len() > Self::KEY_LEN as usize {
    //         return Err(AppError::InvalidKey(key.len()));
    //     }
    //
    //     if let Some(offset) = self.index.get(key) {
    //         self.file.seek(SeekFrom::Start(*offset as u64)).map_err(AppError::SeekInDb)?;
    //         let key_len = match Self::rebuild_len(&mut self.file)? {
    //             Some(key_len) => key_len,
    //             None => return Ok(None)
    //         };
    //         let value_len = match Self::rebuild_len(&mut self.file)? {
    //             Some(key_len) => key_len,
    //             None => return Ok(None)
    //         };
    //
    //     }
    //     Ok(None)
    // }
}
