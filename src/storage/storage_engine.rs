use crate::error::AppError;
use crate::storage::storage_engine::ReadStatus::{CompleteRead, CorruptTail, Eof};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) struct StorageEngine {
    file: File,
    index: HashMap<String, u64>,
}

enum ReadStatus {
    Eof,
    CompleteRead(u64),
    CorruptTail,
}

impl StorageEngine {
    const KEY_LEN: u64 = 256;
    const VALUE_LEN: u64 = 1024 * 1024;

    pub(crate) fn start() -> Result<StorageEngine, AppError> {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(Path::new("data.db"))
            .map_err(AppError::LoadDbFile)?;

        let index = Self::rebuild_index(&mut file)?;

        Ok(StorageEngine { file, index })
    }

    fn rebuild_index(file: &mut File) -> Result<HashMap<String, u64>, AppError> {
        let mut index = HashMap::<String, u64>::new();
        file.seek(SeekFrom::Start(0)).map_err(AppError::SeekInDb)?;

        loop {
            let offset = file.stream_position().map_err(AppError::SeekInDb)?;

            let key_len = match Self::rebuild_len(file)? {
                Eof => break,
                CompleteRead(key_len) => key_len,
                CorruptTail => return Err(AppError::CorruptedDb),
            };
            let value_len = match Self::rebuild_len(file)? {
                Eof => break,
                CompleteRead(value_len) => value_len,
                CorruptTail => return Err(AppError::CorruptedDb),
            };

            if key_len == 0 || key_len > Self::KEY_LEN || value_len > Self::VALUE_LEN {
                return Err(AppError::CorruptedDb);
            }

            let mut key_buf = vec![0u8; key_len as usize];
            file.read_exact(&mut key_buf).map_err(|_| AppError::CorruptedDb)?;
            let key = String::from_utf8(key_buf).map_err(AppError::ConvertUtf8ToString)?;

            let mut value_buf = vec![0u8; value_len as usize];
            file.read_exact(&mut value_buf)
                .map_err(|_| AppError::CorruptedDb)?;

            index.insert(key, offset);
        }

        Ok(index)
    }

    fn rebuild_len(file: &mut File) -> Result<ReadStatus, AppError> {
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

        self.index.insert(key, offset);

        Ok(())
    }

    pub(crate) fn get(&mut self, key: String) -> Result<Option<String>, AppError> {
        if key.is_empty() || key.len() > Self::KEY_LEN as usize {
            return Err(AppError::InvalidKey(key.len()));
        }

        if let Some(offset) = self.index.get(&key) {
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
            let _key = String::from_utf8(buf).map_err(AppError::ConvertUtf8ToString)?;

            if !_key.eq(&key) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn get_temp_path() -> PathBuf {
        std::env::temp_dir().join("test_db.db")
    }

    fn cleanup_file(path: PathBuf) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_empty_db() {
        let path = get_temp_path();
        File::create(&path).unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        let index = StorageEngine::rebuild_index(&mut file);
        assert!(index.is_ok());
        let index = index.unwrap();
        assert!(index.is_empty(), "Index should be Empty for 0 byte file");
        cleanup_file(path);
    }

    #[test]
    fn test_valid_db() {
        let path = get_temp_path();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        let key = "hello";
        let val = "world";

        file.write_all(&(key.len() as u32).to_le_bytes()).unwrap();
        file.write_all(&(val.len() as u32).to_le_bytes()).unwrap();
        file.write_all(key.as_bytes()).unwrap();
        file.write_all(val.as_bytes()).unwrap();

        file.flush().unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();

        let index_res = StorageEngine::rebuild_index(&mut file);

        if let Err(ref e) = index_res {
            panic!("rebuild_index failed with error: {:?}", e);
        }

        assert!(index_res.is_ok());
        let index = index_res.unwrap();
        assert_eq!(index.len(), 1);
        assert!(index.contains_key("hello"));
        assert_eq!(*index.get("hello").unwrap(), 0); // starts at offset 0

        cleanup_file(path);
    }

    #[test]
    fn test_partial_header() {
        let path = get_temp_path();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        // Write a valid first record
        let key = "k";
        let val = "v";
        file.write_all(&(key.len() as u32).to_le_bytes()).unwrap();
        file.write_all(&(val.len() as u32).to_le_bytes()).unwrap();
        file.write_all(key.as_bytes()).unwrap();
        file.write_all(val.as_bytes()).unwrap();

        // Write an incomplete header (only 6 bytes total out of the required 8 bytes for len prefixes)
        // 4 bytes for next key_len, but only 2 bytes for next value_len
        let partial_key_len = 10u32;
        file.write_all(&partial_key_len.to_le_bytes()).unwrap();
        file.write_all(&[0u8, 0u8]).unwrap(); // Broken partial value length payload
        file.flush().unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        let index_res = StorageEngine::rebuild_index(&mut file);

        // Your code triggers CorruptTail on rebuild_len mismatch, which bubbles up CorruptedDb error
        assert!(index_res.is_err());
        match index_res.unwrap_err() {
            AppError::CorruptedDb => {} // Expected outcome
            _ => panic!("Expected AppError::CorruptedDb"),
        }

        cleanup_file(path);
    }

    #[test]
    fn test_partial_key() {
        let path = get_temp_path();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        // Header claims key is 10 bytes, value is 5 bytes
        let target_key_len = 10u32;
        let target_val_len = 5u32;
        file.write_all(&target_key_len.to_le_bytes()).unwrap();
        file.write_all(&target_val_len.to_le_bytes()).unwrap();

        // Write only 4 bytes of actual key data instead of 10
        file.write_all(b"half").unwrap();
        file.flush().unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        let index_res = StorageEngine::rebuild_index(&mut file);

        // read_exact will fail with UnexpectedEof, wrapped by your code into ReadKey
        assert!(index_res.is_err());
        match index_res.unwrap_err() {
            AppError::CorruptedDb => {} // Expected outcome
            _ => panic!("Expected AppError::CorruptedDb"),
        }

        cleanup_file(path);
    }

    #[test]
    fn test_partial_value() {
        let path = get_temp_path();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        // Header claims key is 4 bytes, value is 100 bytes
        let target_key_len = 4u32;
        let target_val_len = 100u32;
        file.write_all(&target_key_len.to_le_bytes()).unwrap();
        file.write_all(&target_val_len.to_le_bytes()).unwrap();

        // Write complete key payload (4 bytes)
        file.write_all(b"test").unwrap();
        // Write incomplete value payload (only 10 bytes instead of 100)
        file.write_all(b"incomplete").unwrap();
        file.flush().unwrap();

        file.seek(SeekFrom::Start(0)).unwrap();
        let index_res = StorageEngine::rebuild_index(&mut file);

        // read_exact on value buffer fails with UnexpectedEof, wrapped into ReadValue
        assert!(index_res.is_err());
        match index_res.unwrap_err() {
            AppError::CorruptedDb => {} // Expected outcome
            _ => panic!("Expected AppError::CorruptedDb"),
        }

        cleanup_file(path);
    }

    #[test]
    fn test_valid_records_before_partial_corruption() {
        let path = get_temp_path();

        // Open with read and write permissions
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();

        // ---- Record 1 (Valid) ----
        let key1 = "first_key";
        let val1 = "first_value";
        file.write_all(&(key1.len() as u32).to_le_bytes()).unwrap();
        file.write_all(&(val1.len() as u32).to_le_bytes()).unwrap();
        file.write_all(key1.as_bytes()).unwrap();
        file.write_all(val1.as_bytes()).unwrap();

        let key2 = "second_key";
        let val2 = "second_value";
        file.write_all(&(key2.len() as u32).to_le_bytes()).unwrap();
        file.write_all(&(val2.len() as u32).to_le_bytes()).unwrap();
        file.write_all(key2.as_bytes()).unwrap();
        file.write_all(val2.as_bytes()).unwrap();

        // ---- Record 3 (Corrupt Tail / Partial Header) ----
        // Write only 2 bytes of garbage out of a 4-byte length header
        file.write_all(&[42u8, 42u8]).unwrap();
        file.flush().unwrap();

        // Rewind file pointer to the beginning for the engine to read
        file.seek(SeekFrom::Start(0)).unwrap();

        // Run rebuild_index
        let index_res = StorageEngine::rebuild_index(&mut file);

        // ---- Verifications ----

        // 1. The function must ultimately return an Error because of the corrupt tail
        assert!(index_res.is_err(), "Engine should fail due to the final corrupt record");

        match index_res.unwrap_err() {
            AppError::CorruptedDb => {
                // This is the expected error type matching your `CorruptTail => return Err(AppError::CorruptedDb)` logic
            }
            other => panic!("Expected AppError::CorruptedDb, but got: {:?}", other),
        }

        cleanup_file(path);
    }

}
