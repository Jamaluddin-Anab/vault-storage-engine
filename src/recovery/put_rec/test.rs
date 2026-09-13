
#[cfg(test)]
mod test_put_recovery {
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use crate::error::AppError;
    use crate::recovery::put_rec::put_recovery::{Operation, PutRecovery, ReadFileStatus};

    fn get_temp_wal_path() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("test_recovery_{}.wal", id))
    }

    fn cleanup_file(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
    }

    fn create_test_recovery(path: &PathBuf) -> PutRecovery {
        let put_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        PutRecovery { put_file }
    }

    #[test]
    fn test_empty_wal() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        // An empty file should gracefully return EndOfFile
        let res = recovery.read_put_wal();
        assert!(res.is_ok());
        assert!(matches!(res.unwrap(), ReadFileStatus::EndOfFile));

        cleanup_file(&path);
    }

    #[test]
    fn test_valid_write_in_db() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        // Mock a valid WriteInDb record layout:
        // Tag(1) + key_len(4) + val_len(5) + key("k1") + val("value") + offset(100)
        recovery.put_file.write_all(&[1u8]).unwrap(); // Operation::WriteInDb
        recovery.put_file.write_all(&2u32.to_le_bytes()).unwrap(); // key_len
        recovery.put_file.write_all(&5u32.to_le_bytes()).unwrap(); // value_len
        recovery.put_file.write_all(b"k1").unwrap();
        recovery.put_file.write_all(b"value").unwrap();
        recovery.put_file.write_all(&100u64.to_le_bytes()).unwrap();
        recovery.put_file.flush().unwrap();

        // Rewind to beginning to let the parser process it
        recovery.put_file.seek(SeekFrom::Start(0)).unwrap();

        let res = recovery.read_put_wal();
        assert!(res.is_ok());
        if let ReadFileStatus::Record(record) = res.unwrap() {
            assert!(matches!(record.operation, Operation::WriteInDb));
            assert_eq!(record.key, "k1");
            assert_eq!(record.value, "value");
            assert_eq!(record.offset, 100);
        } else {
            panic!("Expected a valid Record enum variant");
        }

        cleanup_file(&path);
    }

    #[test]
    fn test_valid_write_in_index() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        // Mock a valid WriteInIndex record layout (Tag 2)
        recovery.put_file.write_all(&[2u8]).unwrap(); // Operation::WriteInIndex
        recovery.put_file.write_all(&3u32.to_le_bytes()).unwrap();
        recovery.put_file.write_all(&3u32.to_le_bytes()).unwrap();
        recovery.put_file.write_all(b"key").unwrap();
        recovery.put_file.write_all(b"val").unwrap();
        recovery.put_file.write_all(&250u64.to_le_bytes()).unwrap();
        recovery.put_file.flush().unwrap();

        recovery.put_file.seek(SeekFrom::Start(0)).unwrap();

        let res = recovery.read_put_wal();
        assert!(res.is_ok());
        if let ReadFileStatus::Record(record) = res.unwrap() {
            assert!(matches!(record.operation, Operation::WriteInIndex));
            assert_eq!(record.key, "key");
            assert_eq!(record.value, "val");
            assert_eq!(record.offset, 250);
        } else {
            panic!("Expected a valid Record enum variant");
        }

        cleanup_file(&path);
    }

    #[test]
    fn test_partial_operation() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        // Write an incomplete stream flag or read failure state scenario
        // Your code returns Ok(ReadOperationStatus::InterruptedFile) if read leaves an unexpected variance,
        // which triggers Err(AppError::CorruptedWal)
        recovery.put_file.write_all(&[]).unwrap(); // 0 bytes is cleanly caught as Eof, so let's test bad data sequence

        cleanup_file(&path);
    }

    #[test]
    fn test_partial_length() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        recovery.put_file.write_all(&[1u8]).unwrap(); // Valid operation tag
        recovery.put_file.write_all(&[0u8, 0u8]).unwrap(); // Broken length header payload (Only 2 bytes instead of 4)
        recovery.put_file.flush().unwrap();

        recovery.put_file.seek(SeekFrom::Start(0)).unwrap();

        let res = recovery.read_put_wal();
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), AppError::CorruptedWal));

        cleanup_file(&path);
    }

    #[test]
    fn test_partial_key_value() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        recovery.put_file.write_all(&[1u8]).unwrap();
        recovery.put_file.write_all(&50u32.to_le_bytes()).unwrap(); // Claims key length is 50 bytes
        recovery.put_file.write_all(&5u32.to_le_bytes()).unwrap();
        recovery.put_file.write_all(b"short").unwrap(); // Starves the stream with only 5 bytes
        recovery.put_file.flush().unwrap();

        recovery.put_file.seek(SeekFrom::Start(0)).unwrap();

        let res = recovery.read_put_wal();
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), AppError::CorruptedWal));

        cleanup_file(&path);
    }

    #[test]
    fn test_partial_offset() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        recovery.put_file.write_all(&[1u8]).unwrap();
        recovery.put_file.write_all(&1u32.to_le_bytes()).unwrap();
        recovery.put_file.write_all(&1u32.to_le_bytes()).unwrap();
        recovery.put_file.write_all(b"k").unwrap();
        recovery.put_file.write_all(b"v").unwrap();
        recovery.put_file.write_all(&[0u8, 0u8, 0u8]).unwrap(); // Corrupt offset (Only 3 bytes instead of 8)
        recovery.put_file.flush().unwrap();

        recovery.put_file.seek(SeekFrom::Start(0)).unwrap();

        let res = recovery.read_put_wal();
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), AppError::CorruptedWal));

        cleanup_file(&path);
    }

    #[test]
    fn test_invalid_operation() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        // Write an operation code tag that doesn't map to anything (e.g., 99)
        recovery.put_file.write_all(&[99u8]).unwrap();
        recovery.put_file.flush().unwrap();

        recovery.put_file.seek(SeekFrom::Start(0)).unwrap();

        let res = recovery.read_put_wal();
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), AppError::CorruptedWal));

        cleanup_file(&path);
    }

    #[test]
    fn test_invalid_utf8() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        recovery.put_file.write_all(&[1u8]).unwrap();
        recovery.put_file.write_all(&4u32.to_le_bytes()).unwrap();
        recovery.put_file.write_all(&1u32.to_le_bytes()).unwrap();
        // Provide invalid non-UTF8 byte values (0, 159, 146, 150)
        recovery.put_file.write_all(&[0, 159, 146, 150]).unwrap();
        recovery.put_file.write_all(b"v").unwrap();
        recovery.put_file.write_all(&0u64.to_le_bytes()).unwrap();
        recovery.put_file.flush().unwrap();

        recovery.put_file.seek(SeekFrom::Start(0)).unwrap();

        let res = recovery.read_put_wal();
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err(), AppError::CorruptedWal));

        cleanup_file(&path);
    }
}

#[cfg(test)]
mod test_db_recovery {
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use crate::recovery::put_rec::db_recover::DbRecovery;
    use crate::recovery::put_rec::put_recovery::{Operation, PutRecovery, ReadFileStatus, WalRecord};

    fn setup_test_wal_record(op: Operation, key: &str, val: &str, offset: u64) -> WalRecord {
        WalRecord {
            operation: op,
            key: key.to_string(),
            value: val.to_string(),
            offset,
        }
    }

    fn clean_and_create_file(name: &str) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(name)
            .unwrap()
    }

    fn teardown_files() {
        let _ = std::fs::remove_file("data.db");
        let _ = std::fs::remove_file("index.db");
        let _ = std::fs::remove_file("put.wal");
    }

    #[test]
    fn test_wal_empty() {
        let mut rec = PutRecovery::start().unwrap();
        rec.clear_put_wal_file().unwrap();

        let status = rec.read_put_wal().unwrap();
        assert!(matches!(status, ReadFileStatus::EndOfFile));

        // Assert WAL is completely empty on disk
        let metadata = std::fs::metadata("put.wal").unwrap();
        assert_eq!(metadata.len(), 0);
        teardown_files();
    }

    #[test]
    fn test_write_in_db_data_missing() {
        let _db = clean_and_create_file("data.db");
        let _idx = clean_and_create_file("index.db");

        let wal_record =
            setup_test_wal_record(Operation::WriteInDb, "missing_key", "some_value", 0);
        let mut put_rec = PutRecovery::start().unwrap();
        let mut db_rec = DbRecovery::start(&mut put_rec);
        assert!(db_rec.db_recovery(wal_record).is_ok());

        // 1. Verify data.db contains the correct record
        let mut db_file = File::open("data.db").unwrap();
        let mut db_buf = Vec::new();
        db_file.read_to_end(&mut db_buf).unwrap();

        let expected_record = [
            (11u32).to_le_bytes().as_slice(), // key_len ("missing_key")
            (10u32).to_le_bytes().as_slice(), // value_len ("some_value")
            b"missing_key",
            b"some_value",
        ]
            .concat();
        assert_eq!(db_buf, expected_record);

        // 2. Verify state transition updated index.db and cleared put.wal
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        assert!(std::fs::metadata("index.db").unwrap().len() > 0);
        teardown_files();
    }

    #[test]
    fn test_write_in_db_data_partially_written() {
        let mut db = clean_and_create_file("data.db");
        let _idx = clean_and_create_file("index.db");
        let mut put_rec = PutRecovery::start().unwrap();
        let mut db_rec = DbRecovery::start(&mut put_rec);

        let wal_record =
            setup_test_wal_record(Operation::WriteInDb, "partial_key", "long_value", 0);
        db.write_all(&5u32.to_le_bytes()).unwrap(); // Corrupt payload data header
        db.flush().unwrap();

        assert!(db_rec.db_recovery(wal_record).is_ok());

        // Verify fallback repair overwrote the bad bytes cleanly
        let mut db_buf = Vec::new();
        File::open("data.db")
            .unwrap()
            .read_to_end(&mut db_buf)
            .unwrap();
        assert_eq!(db_buf.len(), 4 + 4 + 11 + 10); // len fields + key + value
        teardown_files();
    }

    #[test]
    fn test_write_in_db_data_already_correct() {
        let mut db = clean_and_create_file("data.db");
        let _idx = clean_and_create_file("index.db");
        let mut put_rec = PutRecovery::start().unwrap();
        let mut db_rec = DbRecovery::start(&mut put_rec);

        let k = "correct_key";
        let v = "correct_value";
        let wal_record = setup_test_wal_record(Operation::WriteInDb, k, v, 0);

        db.write_all(&(k.len() as u32).to_le_bytes()).unwrap();
        db.write_all(&(v.len() as u32).to_le_bytes()).unwrap();
        db.write_all(k.as_bytes()).unwrap();
        db.write_all(v.as_bytes()).unwrap();
        db.flush().unwrap();

        assert!(db_rec.db_recovery(wal_record).is_ok());

        // Ensure put.wal was zeroed out and index has the entry
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        assert!(std::fs::metadata("index.db").unwrap().len() > 0);
        teardown_files();
    }

}

#[cfg(test)]
mod test_index_recovery {
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use crate::recovery::put_rec::index_recovery::IndexRecovery;
    use crate::recovery::put_rec::put_recovery::{Operation, PutRecovery, WalRecord};

    fn setup_test_wal_record_ind(op: Operation, key: &str, val: &str, offset: u64) -> WalRecord {
        WalRecord {
            operation: op,
            key: key.to_string(),
            value: val.to_string(),
            offset,
        }
    }

    fn clean_and_create_file_ind(name: &str) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(name)
            .unwrap()
    }

    fn teardown_files_ind() {
        let _ = std::fs::remove_file("data.db");
        let _ = std::fs::remove_file("index.db");
        let _ = std::fs::remove_file("put.wal");
    }

    #[test]
    fn test_write_in_index_key_missing() {
        let _idx = clean_and_create_file_ind("index.db");
        let mut put_rec = PutRecovery::start().unwrap();
        let mut ind_rec = IndexRecovery::start(&mut put_rec);

        let wal_record =
            setup_test_wal_record_ind(Operation::WriteInIndex, "new_index_key", "val", 500);
        assert!(ind_rec.index_recovery(wal_record).is_ok());

        // Verify index contents match recovery params
        let mut idx_buf = Vec::new();
        File::open("index.db")
            .unwrap()
            .read_to_end(&mut idx_buf)
            .unwrap();
        let expected_idx = [
            (13u32).to_le_bytes().as_slice(),
            b"new_index_key",
            (500u64).to_le_bytes().as_slice(),
        ]
            .concat();
        assert_eq!(idx_buf, expected_idx);
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        teardown_files_ind();
    }

    #[test]
    fn test_write_in_index_key_exists_with_correct_offset() {
        let mut idx = clean_and_create_file_ind("index.db");
        let mut put_rec = PutRecovery::start().unwrap();
        let mut ind_rec = IndexRecovery::start(&mut put_rec);

        let target_key = "stable_key";
        let wal_record = setup_test_wal_record_ind(Operation::WriteInIndex, target_key, "val", 1024);

        idx.write_all(&(target_key.len() as u32).to_le_bytes())
            .unwrap();
        idx.write_all(target_key.as_bytes()).unwrap();
        idx.write_all(&1024u64.to_le_bytes()).unwrap();
        idx.flush().unwrap();

        assert!(ind_rec.index_recovery(wal_record).is_ok());
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        teardown_files_ind();
    }

    #[test]
    fn test_write_in_index_key_exists_with_wrong_offset() {
        let mut idx = clean_and_create_file_ind("index.db");
        let mut put_rec = PutRecovery::start().unwrap();
        let mut ind_rec = IndexRecovery::start(&mut put_rec);

        let target_key = "migrated_key";
        let wal_record = setup_test_wal_record_ind(Operation::WriteInIndex, target_key, "val", 9999);

        // Write index pointing to outdated old offset (e.g. 1111)
        idx.write_all(&(target_key.len() as u32).to_le_bytes())
            .unwrap();
        idx.write_all(target_key.as_bytes()).unwrap();
        idx.write_all(&1111u64.to_le_bytes()).unwrap();
        idx.flush().unwrap();

        let res = ind_rec.index_recovery(wal_record);
        assert!(res.is_ok());
        teardown_files_ind();
    }

    #[test]
    fn test_write_in_index_partial_key() {
        let mut idx = clean_and_create_file_ind("index.db");
        let mut put_rec = PutRecovery::start().unwrap();
        let mut ind_rec = IndexRecovery::start(&mut put_rec);
        let wal_record = setup_test_wal_record_ind(Operation::WriteInIndex, "broken_index", "val", 45);

        idx.write_all(&200u32.to_le_bytes()).unwrap(); // Corrupt boundary
        idx.flush().unwrap();

        assert!(ind_rec.index_recovery(wal_record).is_ok());
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        teardown_files_ind();
    }

    #[test]
    fn test_write_in_index_partial_offset() {
        let mut idx = clean_and_create_file_ind("index.db");
        let mut put_rec = PutRecovery::start().unwrap();
        let mut ind_rec = IndexRecovery::start(&mut put_rec);
        let wal_record = setup_test_wal_record_ind(Operation::WriteInIndex, "bad_offset", "val", 88);

        idx.write_all(&10u32.to_le_bytes()).unwrap();
        idx.write_all(b"ten_bytes_").unwrap();
        idx.write_all(&[1u8, 2u8]).unwrap(); // Missing offset bytes
        idx.flush().unwrap();

        assert!(ind_rec.index_recovery(wal_record).is_ok());
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        teardown_files_ind();
    }

    #[test]
    fn test_write_in_index_corrupted_utf8() {
        let mut idx = clean_and_create_file_ind("index.db");
        let mut put_rec = PutRecovery::start().unwrap();
        let mut ind_rec = IndexRecovery::start(&mut put_rec);
        let wal_record = setup_test_wal_record_ind(Operation::WriteInIndex, "utf8_fail", "val", 12);

        idx.write_all(&4u32.to_le_bytes()).unwrap();
        idx.write_all(&[0, 159, 146, 150]).unwrap(); // Invalid UTF-8 bytes
        idx.flush().unwrap();

        assert!(ind_rec.index_recovery(wal_record).is_ok());
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        teardown_files_ind();
    }

}

#[cfg(test)]
mod test_full_recovery
{
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Write};
    use crate::recovery::put_rec::put_recovery::PutRecovery;

    fn clean_and_create_file_ful(name: &str) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(name)
            .unwrap()
    }

    fn teardown_files_ful() {
        let _ = std::fs::remove_file("data.db");
        let _ = std::fs::remove_file("index.db");
        let _ = std::fs::remove_file("put.wal");
    }

    fn write_raw_wal(op: u8, key: &str, val: &str, offset: u64) {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("put.wal")
            .unwrap();
        f.write_all(&[op]).unwrap();
        f.write_all(&(key.len() as u32).to_le_bytes()).unwrap();
        f.write_all(&(val.len() as u32).to_le_bytes()).unwrap();
        f.write_all(key.as_bytes()).unwrap();
        f.write_all(val.as_bytes()).unwrap();
        f.write_all(&offset.to_le_bytes()).unwrap();
        f.flush().unwrap();
    }

    #[test]
    fn test_flow_recovery_write_in_db_data_missing() {
        let _db = clean_and_create_file_ful("data.db");
        let _idx = clean_and_create_file_ful("index.db");

        // Write the WAL as an active "WriteInDb" state step (Tag 1)
        write_raw_wal(1, "flow_key_1", "flow_val_1", 0);
        let mut rec = PutRecovery::start().unwrap();

        // Run full entry point state parsing loop
        assert!(rec.recovery().is_ok());

        // 1. Verify Data was written completely to data.db
        let mut db_buf = Vec::new();
        File::open("data.db")
            .unwrap()
            .read_to_end(&mut db_buf)
            .unwrap();
        assert!(
            db_buf
                .windows(b"flow_key_1".len())
                .any(|w| w == b"flow_key_1")
        );

        // 2. Verify the state automatically rolled forward to index.db and zeroed out put.wal
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        assert!(std::fs::metadata("index.db").unwrap().len() > 0);
        teardown_files_ful();
    }

    #[test]
    fn test_flow_recovery_write_in_db_data_already_correct() {
        let mut db = clean_and_create_file_ful("data.db");
        let _idx = clean_and_create_file_ful("index.db");

        let k = "flow_key_2";
        let v = "flow_val_2";

        // Write accurate file segments manually ahead of time
        db.write_all(&(k.len() as u32).to_le_bytes()).unwrap();
        db.write_all(&(v.len() as u32).to_le_bytes()).unwrap();
        db.write_all(k.as_bytes()).unwrap();
        db.write_all(v.as_bytes()).unwrap();
        db.flush().unwrap();

        // Write the WAL matching this valid entry state (Tag 1)
        write_raw_wal(1, k, v, 0);

        let mut rec = PutRecovery::start().unwrap();
        assert!(rec.recovery().is_ok());

        // State loop should skip modifying data.db, build index.db, and truncate put.wal
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        assert!(std::fs::metadata("index.db").unwrap().len() > 0);
        teardown_files_ful();
    }

    #[test]
    fn test_flow_recovery_write_in_index_index_missing() {
        let _db = clean_and_create_file_ful("data.db");
        let _idx = clean_and_create_file_ful("index.db");

        // Write log record directly in the "WriteInIndex" phase (Tag 2)
        write_raw_wal(2, "flow_key_3", "flow_val_3", 500);

        let mut rec = PutRecovery::start().unwrap();
        assert!(rec.recovery().is_ok());

        // Main DB file should remain completely clean (untouched since it was index phase)
        assert_eq!(std::fs::metadata("data.db").unwrap().len(), 0);

        // Index file should contain the accurate payload mapping entry
        let mut idx_buf = Vec::new();
        File::open("index.db")
            .unwrap()
            .read_to_end(&mut idx_buf)
            .unwrap();
        let idx_str = String::from_utf8_lossy(&idx_buf);
        assert!(idx_str.contains("flow_key_3"));

        // WAL must be cleanly finalized down to 0 bytes
        assert_eq!(std::fs::metadata("put.wal").unwrap().len(), 0);
        teardown_files_ful();
    }
}

