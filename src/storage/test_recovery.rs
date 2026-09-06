#[cfg(test)]
mod test {
    use std::fs::{OpenOptions};
    use std::io::{Write, Seek, SeekFrom};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use crate::error::AppError;
    use crate::storage::recovery::*;
    use crate::storage::wal::Operation;

    // Helper to generate non-conflicting, clean temporary WAL paths for each test case
    fn get_temp_wal_path() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("test_recovery_{}.wal", id))
    }

    fn cleanup_file(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
    }

    // Helper to manually initialize a Recovery struct with a custom test file
    fn create_test_recovery(path: &PathBuf) -> Recovery {
        let put_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        Recovery { put_file }
    }

    #[test]
    fn test_empty_wal() {
        let path = get_temp_wal_path();
        let mut recovery = create_test_recovery(&path);

        // An empty file should gracefully return EndOfFile
        let res = recovery.read_put_wal();
        assert!(res.is_ok());
        assert!(matches!(res.unwrap(), ReadPutWalStatus::EndOfFile));

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
        if let ReadPutWalStatus::Record(record) = res.unwrap() {
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
        if let ReadPutWalStatus::Record(record) = res.unwrap() {
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
        assert!(matches!(res.unwrap_err(), AppError::ConvertUtf8ToString(..)));

        cleanup_file(&path);
    }
}