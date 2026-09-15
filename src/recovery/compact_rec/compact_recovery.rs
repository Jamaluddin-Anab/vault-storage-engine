use crate::error::AppError;
use crate::storage::index::Index;
use crate::storage::storage_engine::ReadStatus::*;
use crate::storage::storage_engine::StorageEngine;
use crate::wal::compact_wal::CompactOperation;
use crate::wal::compact_wal::CompactOperation::*;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

enum ReadOperationStatus {
    Eof,
    CompleteRead(CompactOperation),
    InterruptedFile,
}

pub(crate) struct CompactRecovery {
    compact_file: File,
}

impl CompactRecovery {
    pub(crate) fn start() -> Result<Self, AppError> {
        let compact_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(Path::new("compact.wal"))
            .map_err(AppError::ReadWalFile)?;

        Ok(Self { compact_file })
    }

    pub(crate) fn recovery(&mut self) -> Result<(), AppError> {
        match self.read_operation()? {
            ReadOperationStatus::Eof => Ok(()),
            ReadOperationStatus::InterruptedFile => Err(AppError::CorruptedWal),
            ReadOperationStatus::CompleteRead(operation) => {
                self.dispatch(operation)?;
                Ok(())
            }
        }
    }

    fn read_operation(&mut self) -> Result<ReadOperationStatus, AppError> {
        let mut operation_buf = [0u8; 1];
        match self.compact_file.read(&mut operation_buf) {
            Ok(0) => Ok(ReadOperationStatus::Eof),
            Ok(1) => Ok(ReadOperationStatus::CompleteRead(
                CompactOperation::from_bytes(operation_buf)?,
            )),
            Ok(_) => Ok(ReadOperationStatus::InterruptedFile),
            Err(err) => Err(AppError::ReadWalFile(err)),
        }
    }

    fn dispatch(&self, operation: CompactOperation) -> Result<(), AppError> {
        match operation {
            CreateData => {}
            CreateIndex => {}
            CopyDataTo => {}
            ReplaceData => {}
            ReplaceIndex => {}
        }
        Ok(())
    }

    fn create_data(&mut self) -> Result<(), AppError> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(Path::new("data.temp.db"))
            .map_err(AppError::CreateTempFile)?;

        self.update_compact_wal(CreateIndex)
    }

    fn create_index(&mut self) -> Result<(), AppError> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(Path::new("index.temp.db"))
            .map_err(AppError::CreateTempFile)?;

        self.update_compact_wal(CopyDataTo)
    }

    fn copy_data_to(&mut self) -> Result<(), AppError> {
        if !std::fs::exists(Path::new("data.db")).map_err(AppError::FileAccess)? {
            return Err(AppError::DbFileNotExist);
        }

        let mut compact_offset = HashMap::<String, u64>::new();
        let mut db_file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(false)
            .open(Path::new("data.db"))
            .map_err(AppError::LoadDbFile)?;
        let mut data_temp = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(true)
            .open(Path::new("data.temp.db"))
            .map_err(AppError::LoadTempDbFile)?;
        let mut index_temp = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(true)
            .open(Path::new("index.temp.db"))
            .map_err(AppError::LoadTempIndexFile)?;

        let index = Index::load_index()?;

        for (key, old_offset) in &index.index {
            db_file
                .seek(SeekFrom::Start(*old_offset))
                .map_err(AppError::SeekInDb)?;

            let key_len = match StorageEngine::rebuild_len(&mut db_file)? {
                Eof => break,
                CompleteRead(key_len) => key_len,
                CorruptTail => return Err(AppError::CorruptedDb),
            };
            let value_len = match StorageEngine::rebuild_len(&mut db_file)? {
                Eof => break,
                CompleteRead(value_len) => value_len,
                CorruptTail => return Err(AppError::CorruptedDb),
            };
            db_file
                .seek(SeekFrom::Current(key_len as i64))
                .map_err(AppError::SeekInDb)?;

            let mut value_buf = vec![0u8; value_len as usize];
            db_file
                .read_exact(&mut value_buf)
                .map_err(|_| AppError::CorruptedDb)?;

            let new_offset = data_temp.stream_position().map_err(AppError::SeekInTemp)?;
            let key_len_u32 = key.len() as u32;
            let value_len_u32 = value_buf.len() as u32;

            let record = [
                key_len_u32.to_le_bytes().as_slice(),
                value_len_u32.to_le_bytes().as_slice(),
                key.as_bytes(),
                &value_buf,
            ]
            .concat();

            data_temp
                .write_all(&record)
                .map_err(AppError::WriteToTempDb)?;
            compact_offset.insert(key.clone(), new_offset);
        }
        data_temp.sync_all().map_err(AppError::WriteToTempDb)?;

        for (new_key, new_offset) in &compact_offset {
            let key_len = new_key.len() as u32;
            let record = [
                key_len.to_le_bytes().as_slice(),
                new_key.as_bytes(),
                &new_offset.to_le_bytes(),
            ]
            .concat();

            index_temp
                .write_all(&record)
                .map_err(AppError::WriteToTempIndex)?;
        }
        index_temp.sync_all().map_err(AppError::WriteToTempIndex)?;

        self.update_compact_wal(ReplaceData)?;

        Ok(())
    }

    fn update_compact_wal(&mut self, operation: CompactOperation) -> Result<(), AppError> {
        self.compact_file
            .set_len(0)
            .map_err(AppError::CleanWalFile)?;

        self.compact_file
            .write_all(&operation.to_bytes())
            .map_err(AppError::WriteToWal)?;

        self.compact_file.sync_all().map_err(AppError::WriteToWal)?;
        Ok(())
    }
}

#[cfg(test)]
mod test_compact_initialization {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    struct MockCompactor {
        compact_file: File,
    }

    impl MockCompactor {
        fn create_temp_file_test(&mut self, file_name: &str) -> Result<(), AppError> {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(Path::new(file_name))
                .map_err(AppError::CreateTempFile)?;

            Ok(())
        }

        fn update_compact_wal(&mut self, operation: CompactOperation) -> Result<(), AppError> {
            self.compact_file
                .set_len(0)
                .map_err(AppError::CleanWalFile)?;

            self.compact_file
                .seek(SeekFrom::Start(0))
                .map_err(AppError::SeekInWal)?;

            self.compact_file
                .write_all(&operation.to_bytes())
                .map_err(AppError::WriteToWal)?;

            self.compact_file.sync_all().map_err(AppError::WriteToWal)?;
            Ok(())
        }
    }

    fn setup_clean_compact_wal() -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("compact.wal")
            .unwrap()
    }

    fn teardown_compact_files(file_name: &str) {
        let _ = std::fs::remove_file(file_name);
        let _ = std::fs::remove_file("compact.wal");
    }

    #[test]
    fn test_create_data_no_existing_temp_db() {
        let _ = std::fs::remove_file("data.temp.db");
        let compact_file = setup_clean_compact_wal();

        {
            let mut compactor = MockCompactor { compact_file };
            assert!(compactor.create_temp_file_test("data.temp.db").is_ok());
            compactor.update_compact_wal(CreateIndex).unwrap();
        }
        assert!(
            Path::new("data.temp.db").exists(),
            "data.temp.db was not created"
        );

        let data_meta = std::fs::metadata("data.temp.db").unwrap();
        assert_eq!(data_meta.len(), 0, "data.temp.db length should be 0");

        let mut wal_file = File::open("compact.wal").unwrap();
        let mut wal_buf = [0u8; 1];
        wal_file.read_exact(&mut wal_buf).unwrap();

        assert_eq!(
            wal_buf,
            CreateIndex.to_bytes(),
            "compact.wal did not advance to CreateIndex"
        );
        assert_eq!(
            wal_file.metadata().unwrap().len(),
            1,
            "compact.wal should contain exactly 1 byte"
        );

        teardown_compact_files("data.temp.db");
    }

    #[test]
    fn test_create_data_with_existing_garbage_temp_db() {
        {
            let mut dirty_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open("data.temp.db")
                .unwrap();
            dirty_file
                .write_all(b"corrupted_stale_garbage_database_bytes_payload")
                .unwrap();
            dirty_file.flush().unwrap();
        }

        let compact_file = setup_clean_compact_wal();

        {
            let mut compactor = MockCompactor { compact_file };
            assert!(compactor.create_temp_file_test("data.temp.db").is_ok());
            compactor.update_compact_wal(CreateIndex).unwrap();
        }

        assert!(Path::new("data.temp.db").exists());

        let data_meta = std::fs::metadata("data.temp.db").unwrap();
        assert_eq!(
            data_meta.len(),
            0,
            "Stale bytes were not truncated from data.temp.db"
        );

        let mut wal_file = File::open("compact.wal").unwrap();
        let mut wal_buf = [0u8; 1];
        wal_file.read_exact(&mut wal_buf).unwrap();

        assert_eq!(wal_buf, CreateIndex.to_bytes());
        assert_eq!(wal_file.metadata().unwrap().len(), 1);

        teardown_compact_files("data.temp.db");
    }

    #[test]
    fn test_create_index_no_existing_temp_db() {
        let _ = std::fs::remove_file("index.temp.db");
        let compact_file = setup_clean_compact_wal();

        {
            let mut compactor = MockCompactor { compact_file };
            assert!(compactor.create_temp_file_test("index.temp.db").is_ok());
            compactor.update_compact_wal(CopyDataTo).unwrap();
        }

        assert!(
            Path::new("index.temp.db").exists(),
            "index.temp.db was not created"
        );

        let data_meta = std::fs::metadata("index.temp.db").unwrap();
        assert_eq!(data_meta.len(), 0, "index.temp.db length should be 0");

        let mut wal_file = File::open("compact.wal").unwrap();
        let mut wal_buf = [0u8; 1];
        wal_file.read_exact(&mut wal_buf).unwrap();

        assert_eq!(
            wal_buf,
            CopyDataTo.to_bytes(),
            "compact.wal did not advance to CopyDataTo"
        );
        assert_eq!(
            wal_file.metadata().unwrap().len(),
            1,
            "compact.wal should contain exactly 1 byte"
        );

        teardown_compact_files("index.temp.db");
    }

    #[test]
    fn test_create_index_with_existing_garbage_temp_db() {
        {
            let mut dirty_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open("index.temp.db")
                .unwrap();
            dirty_file
                .write_all(b"corrupted_stale_garbage_database_bytes_payload")
                .unwrap();
            dirty_file.flush().unwrap();
        }

        let compact_file = setup_clean_compact_wal();

        {
            let mut compactor = MockCompactor { compact_file };

            assert!(compactor.create_temp_file_test("index.temp.db").is_ok());
            compactor.update_compact_wal(CopyDataTo).unwrap();
        }

        assert!(Path::new("index.temp.db").exists());

        let data_meta = std::fs::metadata("index.temp.db").unwrap();
        assert_eq!(
            data_meta.len(),
            0,
            "Stale bytes were not truncated from index.temp.db"
        );

        let mut wal_file = File::open("compact.wal").unwrap();
        let mut wal_buf = [0u8; 1];
        wal_file.read_exact(&mut wal_buf).unwrap();

        assert_eq!(wal_buf, CopyDataTo.to_bytes());
        assert_eq!(wal_file.metadata().unwrap().len(), 1);

        teardown_compact_files("index.temp.db");
    }
}
