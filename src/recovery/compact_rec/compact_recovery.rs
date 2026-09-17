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

    fn replace_data(&mut self) -> Result<(), AppError> {
        if !std::fs::exists(Path::new("data.db")).map_err(AppError::FileAccess)? {
            return Err(AppError::DbFileNotExist);
        }
        if !std::fs::exists(Path::new("data.temp.db")).map_err(AppError::FileAccess)? {
            self.copy_data_to()?;
        }
        std::fs::rename("data.temp.db", "data.db").map_err(AppError::ReplaceDbFile)?;
        self.update_compact_wal(ReplaceIndex)
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

#[cfg(test)]
mod test_copy_data_to {
    use crate::recovery::compact_rec::compact_recovery::CompactRecovery;
    use crate::wal::compact_wal::CompactOperation;
    use std::collections::HashMap;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    fn teardown_all_files() {
        let _ = std::fs::remove_file("data.db");
        let _ = std::fs::remove_file("index.db");
        let _ = std::fs::remove_file("data.temp.db");
        let _ = std::fs::remove_file("index.temp.db");
        let _ = std::fs::remove_file("compact.wal");
    }

    #[test]
    fn test_copy_data_to_drops_dead_records_and_updates_temp_index() {
        // Clear any old artifacts to establish a pristine single-threaded baseline
        teardown_all_files();

        // 1. Arrange: Create a data.db file with live and dead records
        let mut db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("data.db")
            .unwrap();

        // Track live keys and values to query against at the end of the test
        let mut live_expectations = HashMap::new();
        live_expectations.insert("user_1".to_string(), "active_session_new".to_string());
        live_expectations.insert("user_2".to_string(), "profile_data".to_string());

        // Write Dead Record: "user_1" -> "stale_session_old"
        let d_key = "user_1";
        let d_val = "stale_session_old";
        db_file
            .write_all(&(d_key.len() as u32).to_le_bytes())
            .unwrap();
        db_file
            .write_all(&(d_val.len() as u32).to_le_bytes())
            .unwrap();
        db_file.write_all(d_key.as_bytes()).unwrap();
        db_file.write_all(d_val.as_bytes()).unwrap();

        // Write Live Record 1 (Overwrite for user_1): "user_1" -> "active_session_new"
        let live_offset_1 = db_file.stream_position().unwrap();
        let l_key_1 = "user_1";
        let l_val_1 = "active_session_new";
        db_file
            .write_all(&(l_key_1.len() as u32).to_le_bytes())
            .unwrap();
        db_file
            .write_all(&(l_val_1.len() as u32).to_le_bytes())
            .unwrap();
        db_file.write_all(l_key_1.as_bytes()).unwrap();
        db_file.write_all(l_val_1.as_bytes()).unwrap();

        // Write Live Record 2: "user_2" -> "profile_data"
        let live_offset_2 = db_file.stream_position().unwrap();
        let l_key_2 = "user_2";
        let l_val_2 = "profile_data";
        db_file
            .write_all(&(l_key_2.len() as u32).to_le_bytes())
            .unwrap();
        db_file
            .write_all(&(l_val_2.len() as u32).to_le_bytes())
            .unwrap();
        db_file.write_all(l_key_2.as_bytes()).unwrap();
        db_file.write_all(l_val_2.as_bytes()).unwrap();
        db_file.flush().unwrap();
        drop(db_file); // Close handle so copy_data_to can safely open it

        // 2. Arrange: Create the active index.db containing points only to the live records
        let mut idx_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("index.db")
            .unwrap();

        // Index for Live Record 1
        idx_file
            .write_all(&(l_key_1.len() as u32).to_le_bytes())
            .unwrap();
        idx_file.write_all(l_key_1.as_bytes()).unwrap();
        idx_file.write_all(&live_offset_1.to_le_bytes()).unwrap();

        // Index for Live Record 2
        idx_file
            .write_all(&(l_key_2.len() as u32).to_le_bytes())
            .unwrap();
        idx_file.write_all(l_key_2.as_bytes()).unwrap();
        idx_file.write_all(&live_offset_2.to_le_bytes()).unwrap();
        idx_file.flush().unwrap();
        drop(idx_file);

        // Setup a blank compact.wal log file
        let compact_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("compact.wal")
            .unwrap();

        // 3. Act: Run your copy_data_to() function inside a scoped block
        {
            let mut compactor = CompactRecovery { compact_file };

            // Invoke the production implementation you provided
            // (Assumes copy_data_to is exposed or built in scope here)
            let result = compactor.copy_data_to();
            assert!(
                result.is_ok(),
                "copy_data_to failed: {:?}",
                result.unwrap_err()
            );
        }

        // 4. Assert: Verify the content invariants like a real storage engine
        assert!(Path::new("data.temp.db").exists());
        assert!(Path::new("index.temp.db").exists());

        let mut data_temp = OpenOptions::new().read(true).open("data.temp.db").unwrap();
        let mut index_temp = OpenOptions::new().read(true).open("index.temp.db").unwrap();

        let mut parsed_keys_count = 0;

        // Traverse index.temp.db sequentially to pull out offsets and keys
        loop {
            let mut len_buf = [0u8; 4];
            if index_temp.read_exact(&mut len_buf).is_err() {
                break; // EOF reached safely
            }
            let key_len = u32::from_le_bytes(len_buf) as usize;

            let mut key_buf = vec![0u8; key_len];
            index_temp.read_exact(&mut key_buf).unwrap();
            let index_key = String::from_utf8(key_buf).unwrap();

            let mut offset_buf = [0u8; 8];
            index_temp.read_exact(&mut offset_buf).unwrap();
            let index_offset = u64::from_le_bytes(offset_buf);

            // CRITICAL ENGINE CHECK: Seek into data.temp.db using the index's offset pointer
            data_temp.seek(SeekFrom::Start(index_offset)).unwrap();

            let mut d_key_len_buf = [0u8; 4];
            data_temp.read_exact(&mut d_key_len_buf).unwrap();
            let data_key_len = u32::from_le_bytes(d_key_len_buf) as usize;

            let mut d_val_len_buf = [0u8; 4];
            data_temp.read_exact(&mut d_val_len_buf).unwrap();
            let data_val_len = u32::from_le_bytes(d_val_len_buf) as usize;

            let mut d_key_buf = vec![0u8; data_key_len];
            data_temp.read_exact(&mut d_key_buf).unwrap();
            let data_key = String::from_utf8(d_key_buf).unwrap();

            let mut d_val_buf = vec![0u8; data_val_len];
            data_temp.read_exact(&mut d_val_buf).unwrap();
            let data_val = String::from_utf8(d_val_buf).unwrap();

            // Assert that the index entry matches the exact structural layout on data.temp.db
            assert_eq!(
                index_key, data_key,
                "Index key does not align with data.temp.db record key"
            );

            // Assert that the value on disk matches our live expectation map (removes dead records)
            let expected_value = live_expectations
                .get(&index_key)
                .expect("Compacted data contains an unexpected key!");
            assert_eq!(
                &data_val, expected_value,
                "Compacted data contains incorrect or stale value mappings!"
            );

            parsed_keys_count += 1;
        }

        // Verify that exactly 2 records were processed (excluding the dead record)
        assert_eq!(
            parsed_keys_count, 2,
            "Compacted index size should contain exactly 2 live entries"
        );

        // 5. Assert: Verify compact.wal contains exactly ReplaceData (byte value 9)
        let mut wal_file = File::open("compact.wal").unwrap();
        let mut wal_buf = [0u8; 1];
        wal_file.read_exact(&mut wal_buf).unwrap();
        assert_eq!(
            wal_buf,
            CompactOperation::ReplaceData.to_bytes(),
            "compact.wal was not updated to ReplaceData"
        );

        // Clean up workspace files
        teardown_all_files();
    }

    #[test]
    fn test_copy_data_to_discards_and_rebuilds_garbage_temp_files() {
        teardown_all_files();

        // 1. Arrange: Create a valid data.db and index.db with 1 live record
        let mut db_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("data.db")
            .unwrap();

        let k = "recovery_key";
        let v = "fresh_clean_value";
        db_file.write_all(&(k.len() as u32).to_le_bytes()).unwrap();
        db_file.write_all(&(v.len() as u32).to_le_bytes()).unwrap();
        db_file.write_all(k.as_bytes()).unwrap();
        db_file.write_all(v.as_bytes()).unwrap();
        db_file.flush().unwrap();
        drop(db_file);

        let mut idx_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("index.db")
            .unwrap();
        idx_file.write_all(&(k.len() as u32).to_le_bytes()).unwrap();
        idx_file.write_all(k.as_bytes()).unwrap();
        idx_file.write_all(&0u64.to_le_bytes()).unwrap(); // offset 0
        idx_file.flush().unwrap();
        drop(idx_file);

        // 2. Arrange: PRE-SEED THE TEMP FILES WITH STALE GARBAGE BYTES
        // This simulates a previous compaction crash mid-execution
        {
            let mut dirty_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open("data.temp.db")
                .unwrap();
            dirty_data
                .write_all(b"stale_garbage_from_crashed_compaction_run")
                .unwrap();
            dirty_data.flush().unwrap();

            let mut dirty_index = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open("index.temp.db")
                .unwrap();
            dirty_index
                .write_all(b"corrupted_stale_index_bytes")
                .unwrap();
            dirty_index.flush().unwrap();
        }

        let compact_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("compact.wal")
            .unwrap();

        // 3. Act: Execute copy_data_to()
        {
            let mut compactor = CompactRecovery { compact_file };
            let result = compactor.copy_data_to();
            assert!(
                result.is_ok(),
                "copy_data_to failed: {:?}",
                result.unwrap_err()
            );
        }

        // 4. Assert: Verify recovery properties like a real storage engine
        let mut data_temp = OpenOptions::new().read(true).open("data.temp.db").unwrap();
        let mut index_temp = OpenOptions::new().read(true).open("index.temp.db").unwrap();

        // A. Parse the rebuilt index to verify the garbage was dropped
        let mut len_buf = [0u8; 4];
        index_temp.read_exact(&mut len_buf).unwrap();
        let key_len = u32::from_le_bytes(len_buf) as usize;
        assert_eq!(
            key_len,
            k.len(),
            "Index was not truncated; still contains garbage data length!"
        );

        let mut key_buf = vec![0u8; key_len];
        index_temp.read_exact(&mut key_buf).unwrap();
        let index_key = String::from_utf8(key_buf).unwrap();
        assert_eq!(index_key, k);

        let mut offset_buf = [0u8; 8];
        index_temp.read_exact(&mut offset_buf).unwrap();
        let index_offset = u64::from_le_bytes(offset_buf);

        // B. Seek data.temp.db using the newly tracked offset to verify record contents
        data_temp.seek(SeekFrom::Start(index_offset)).unwrap();

        let mut d_key_len_buf = [0u8; 4];
        data_temp.read_exact(&mut d_key_len_buf).unwrap();
        let data_key_len = u32::from_le_bytes(d_key_len_buf) as usize;

        let mut d_val_len_buf = [0u8; 4];
        data_temp.read_exact(&mut d_val_len_buf).unwrap();
        let data_val_len = u32::from_le_bytes(d_val_len_buf) as usize;

        let mut d_key_buf = vec![0u8; data_key_len];
        data_temp.read_exact(&mut d_key_buf).unwrap();
        let data_key = String::from_utf8(d_key_buf).unwrap();

        let mut d_val_buf = vec![0u8; data_val_len];
        data_temp.read_exact(&mut d_val_buf).unwrap();
        let data_val = String::from_utf8(d_val_buf).unwrap();

        // Ensure everything matches the clean live values instead of stale pre-seeded garbage
        assert_eq!(data_key, k);
        assert_eq!(data_val, v);

        let mut trailing_check = [0u8; 4];
        assert!(
            index_temp.read_exact(&mut trailing_check).is_err(),
            "Index contains extra trailing garbage data!"
        );

        teardown_all_files();
    }
}

#[cfg(test)]
mod test_replace_data {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    fn teardown_replace_files() {
        let _ = std::fs::remove_file("data.db");
        let _ = std::fs::remove_file("data.temp.db");
        let _ = std::fs::remove_file("compact.wal");
    }

    fn setup_clean_wal() -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("compact.wal")
            .unwrap()
    }

    // ========================================================================== --
    // TESTS SCENARIOS                                                            --
    // ========================================================================== --

    #[test]
    fn test_replace_data_db_missing_returns_error_and_leaves_wal_unchanged() {
        teardown_replace_files();

        // 1. Arrange: Ensure data.db is missing, set initial WAL value to something else (e.g., 5)
        let mut compact_file = setup_clean_wal();
        compact_file.write_all(&[5u8]).unwrap();
        compact_file.flush().unwrap();

        {
            let mut compactor = CompactRecovery {
                compact_file,
            };

            // 2. Act: Call replace_data() -> Must error out
            let res = compactor.replace_data();
            assert!(matches!(res, Err(AppError::DbFileNotExist)));
        }

        // 3. Assert: Verify WAL operation remains untouched (still 5, not changed to ReplaceIndex)
        let mut wal_file = File::open("compact.wal").unwrap();
        let mut wal_buf = [0u8; 1];
        wal_file.read_exact(&mut wal_buf).unwrap();
        assert_eq!(wal_buf[0], 5, "WAL should not change when primary data.db is missing");

        teardown_replace_files();
    }

    #[test]
    fn test_replace_data_temp_exists_replaces_successfully_and_updates_wal() {
        teardown_replace_files();

        // 1. Arrange: Create both files explicitly ahead of time
        File::create("data.db").unwrap();

        let mut temp_file = File::create("data.temp.db").unwrap();
        temp_file.write_all(b"compacted_clean_data_payload").unwrap();
        temp_file.flush().unwrap();
        drop(temp_file);

        let compact_file = setup_clean_wal();

        {
            let mut compactor = CompactRecovery {
                compact_file,
            };

            // 2. Act: Run replacement
            let res = compactor.replace_data();
            assert!(res.is_ok());
        }

        // 3. Assert: Verify swap complete and temp file moved to permanent spot
        assert!(!Path::new("data.temp.db").exists());
        assert!(Path::new("data.db").exists());

        let mut db_content = Vec::new();
        File::open("data.db").unwrap().read_to_end(&mut db_content).unwrap();
        assert_eq!(db_content, b"compacted_clean_data_payload");

        // 4. Assert: WAL advanced to ReplaceIndex (byte code 11)
        let mut wal_buf = [0u8; 1];
        File::open("compact.wal").unwrap().read_exact(&mut wal_buf).unwrap();
        assert_eq!(wal_buf, ReplaceIndex.to_bytes());

        teardown_replace_files();
    }

    #[test]
    fn test_replace_data_temp_missing_triggers_copy_data_to_and_succeeds() {
        teardown_replace_files();
        let _ = std::fs::remove_file("index.db"); // Clean old index artifacts

        // 1. Arrange: Create a valid data.db with exactly 1 live record
        let mut db_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("data.db")
            .unwrap();

        let k = "flow_key";
        let v = "live_value";
        db_file.write_all(&(k.len() as u32).to_le_bytes()).unwrap();
        db_file.write_all(&(v.len() as u32).to_le_bytes()).unwrap();
        db_file.write_all(k.as_bytes()).unwrap();
        db_file.write_all(v.as_bytes()).unwrap();
        db_file.flush().unwrap();
        drop(db_file);

        // 2. Arrange: Create matching valid index.db file tracking that record
        let mut idx_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("index.db")
            .unwrap();
        idx_file.write_all(&(k.len() as u32).to_le_bytes()).unwrap();
        idx_file.write_all(k.as_bytes()).unwrap();
        idx_file.write_all(&0u64.to_le_bytes()).unwrap(); // offset 0
        idx_file.flush().unwrap();
        drop(idx_file);

        // 3. Arrange: Ensure data.temp.db is ABSENT before starting
        let _ = std::fs::remove_file("data.temp.db");
        assert!(!Path::new("data.temp.db").exists(), "Setup error: data.temp.db should not exist here");

        let compact_file = setup_clean_wal();

        {
            let mut compactor = CompactRecovery {
                compact_file,
            };

            // 4. Act: Execute replacement loop (this internally calls copy_data_to and renames it)
            let res = compactor.replace_data();
            assert!(res.is_ok(), "replace_data failed: {:?}", res.unwrap_err());
        } // <--- Compactor scope drops here, releasing all active descriptors

        // 5. Assert: Final atomic parameters must succeed cleanly
        assert!(
            !Path::new("data.temp.db").exists(),
            "Temporary file should have been cleanly moved/consumed by std::fs::rename"
        );
        assert!(Path::new("data.db").exists(), "data.db should be repaired and present");

        let mut wal_buf = [0u8; 1];
        File::open("compact.wal").unwrap().read_exact(&mut wal_buf).unwrap();
        
        teardown_replace_files();
        let _ = std::fs::remove_file("index.db");
    }


}

