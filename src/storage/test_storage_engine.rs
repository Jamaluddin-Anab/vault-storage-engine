#[cfg(test)]
mod test {
    use crate::error::AppError;
    use crate::storage::index::Index;
    use crate::storage::storage_engine::StorageEngine;
    use crate::storage::wal::Wal;
    use std::collections::HashMap;
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn get_temp_db_paths() -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        let data_path = std::env::temp_dir().join(format!("test_data_{}.db", id));
        let index_path = std::env::temp_dir().join(format!("test_index_{}.db", id));
        let put_file = std::env::temp_dir().join(format!("test_put_{}.wal", id));
        let compact_file = std::env::temp_dir().join(format!("test_compact_{}.wal", id));
        (data_path, index_path, put_file, compact_file)
    }

    fn cleanup_files(paths: &(PathBuf, PathBuf, PathBuf, PathBuf)) {
        let _ = std::fs::remove_file(&paths.0);
        let _ = std::fs::remove_file(&paths.1);
        let _ = std::fs::remove_file(&paths.2);
        let _ = std::fs::remove_file(&paths.3);
    }

    // Helper to build a completely initialized StorageEngine targeting specific custom files
    fn build_test_engine(
        data_path: &PathBuf,
        index_path: &PathBuf,
        put_file: &PathBuf,
        compact_file: &PathBuf,
    ) -> StorageEngine {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(data_path)
            .unwrap();

        // Create a blank tracking file index manually
        let index_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(index_path)
            .unwrap();

        // Create a blank tracking file put_file manually
        let put_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(put_file)
            .unwrap();

        // Create a blank tracking file put_file manually
        let compact_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(true)
            .open(compact_file)
            .unwrap();

        let index = Index {
            file: index_file,
            index: HashMap::<String, u64>::new(),
        };
        let wal = Wal {
            put_file,
            compact_file,
        };

        StorageEngine { file, index, wal }
    }

    #[test]
    fn test_empty_db() {
        let paths = get_temp_db_paths();
        let mut engine = build_test_engine(&paths.0, &paths.1, &paths.2, &paths.3);

        // Fetching from a completely pristine setup should yield Ok(None) safely
        let res = engine.get("any_key");
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), None);

        cleanup_files(&paths);
    }

    #[test]
    fn test_valid_db() {
        let paths = get_temp_db_paths();
        let mut engine = build_test_engine(&paths.0, &paths.1, &paths.2, &paths.3);

        // Put a regular entry to assert full storage round-trip capabilities
        let put_res = engine.put("hello".to_string(), "world".to_string());
        assert!(put_res.is_ok());

        // Fetch back and assert equivalence values
        let get_res = engine.get("hello");
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), Some("world".to_string()));

        cleanup_files(&paths);
    }

    #[test]
    fn test_partial_header() {
        let paths = get_temp_db_paths();
        let mut engine = build_test_engine(&paths.0, &paths.1, &paths.2, &paths.3);

        // Pretend key exists at offset 0 inside the index cache map
        engine.index.index.insert("target_key".to_string(), 0);

        // Write an invalid 2-byte fragment into data payload file instead of the 8-byte layout headers
        engine.file.write_all(&[1u8, 2u8]).unwrap();
        engine.file.flush().unwrap();

        let get_res = engine.get("target_key");
        assert!(get_res.is_err());
        match get_res.unwrap_err() {
            AppError::CorruptedDb => {} // Expected outcome
            other => panic!("Expected AppError::CorruptedDb, got: {:?}", other),
        }

        cleanup_files(&paths);
    }

    #[test]
    fn test_partial_key() {
        let paths = get_temp_db_paths();
        let mut engine = build_test_engine(&paths.0, &paths.1, &paths.2, &paths.3);

        engine
            .index
            .index
            .insert("missing_key_bytes".to_string(), 0);

        // Header definitions: key_len = 10, value_len = 5
        engine.file.write_all(&10u32.to_le_bytes()).unwrap();
        engine.file.write_all(&5u32.to_le_bytes()).unwrap();
        // Provide only a 3-byte layout payload instead of 10
        engine.file.write_all(b"abc").unwrap();
        engine.file.flush().unwrap();

        let get_res = engine.get("missing_key_bytes");
        assert!(get_res.is_err());
        match get_res.unwrap_err() {
            AppError::ReadKey(_) => {} // Expected outcome from read_exact failure triggers
            other => panic!("Expected AppError::ReadKey, got: {:?}", other),
        }

        cleanup_files(&paths);
    }

    #[test]
    fn test_partial_value() {
        let paths = get_temp_db_paths();
        let mut engine = build_test_engine(&paths.0, &paths.1, &paths.2, &paths.3);

        let target_key = "my_key";
        engine.index.index.insert(target_key.to_string(), 0);

        // Header definitions: key_len = 6, value_len = 500
        engine
            .file
            .write_all(&(target_key.len() as u32).to_le_bytes())
            .unwrap();
        engine.file.write_all(&500u32.to_le_bytes()).unwrap();
        // Write out correct full key content length payload
        engine.file.write_all(target_key.as_bytes()).unwrap();
        // Starve value payload data to create an incomplete entry fragment
        engine.file.write_all(b"short_value").unwrap();
        engine.file.flush().unwrap();

        let get_res = engine.get(target_key);
        assert!(get_res.is_err());
        match get_res.unwrap_err() {
            AppError::ReadValue(_) => {} // Expected value buffer underflow error outcome
            other => panic!("Expected AppError::ReadValue, got: {:?}", other),
        }

        cleanup_files(&paths);
    }

    #[test]
    fn test_valid_records_before_partial_corruption() {
        let paths = get_temp_db_paths();

        // Formulate standard files manually to mimic an indexing recovery loop
        let mut index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&paths.1)
            .unwrap();

        // 1. Write Valid Record Index Entry 1: key = "k1", offset = 100
        index_file.write_all(&2u32.to_le_bytes()).unwrap();
        index_file.write_all(b"k1").unwrap();
        index_file.write_all(&100u64.to_le_bytes()).unwrap();

        // 2. Write Valid Record Index Entry 2: key = "k2", offset = 200
        index_file.write_all(&2u32.to_le_bytes()).unwrap();
        index_file.write_all(b"k2").unwrap();
        index_file.write_all(&200u64.to_le_bytes()).unwrap();

        // 3. Write Corrupt Index Tail (Only a partial 2-byte fragment instead of a 4-byte string length header)
        index_file.write_all(&[9u8, 9u8]).unwrap();
        index_file.flush().unwrap();

        // Seek back to zero position boundaries
        index_file.seek(SeekFrom::Start(0)).unwrap();

        // Rebuild execution must bubble up an index compilation issue error variant
        let load_res = Index::build_index(&mut index_file);
        assert!(load_res.is_err());
        match load_res.unwrap_err() {
            AppError::CorruptedIndex => {} // Confirms complete verification boundaries
            other => panic!("Expected AppError::CorruptedIndex, got: {:?}", other),
        }

        cleanup_files(&paths);
    }
}
