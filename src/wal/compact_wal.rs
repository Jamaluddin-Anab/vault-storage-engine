use crate::error::AppError;
use crate::wal::compact_wal::CompactOperation::*;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum CompactOperation {
    ReplaceData = 1,
}
impl CompactOperation {
    pub(crate) fn to_bytes(self) -> [u8; 1] {
        [self as u8]
    }

    pub(crate) fn from_bytes(bytes: [u8; 1]) -> Result<Self, AppError> {
        match bytes[0] {
            1 => Ok(ReplaceData),
            _ => Err(AppError::UnknownCompactOperation),
        }
    }
}

pub(crate) struct CompactWal {
    pub(crate) file: File,
}
impl CompactWal {
    pub(crate) fn new() -> Result<Self, AppError> {
        let file = Self::create_compact_file()?;
        Ok(CompactWal { file })
    }

    fn create_compact_file() -> Result<File, AppError> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(Path::new("compact.wal"))
            .map_err(AppError::CreateWalFile)
    }

    pub(crate) fn write_operation(
        &mut self,
        compact_operation: CompactOperation,
    ) -> Result<(), AppError> {
        self.file.set_len(0).map_err(AppError::CleanWalFile)?;

        self.file
            .seek(SeekFrom::Start(0))
            .map_err(AppError::SeekInWal)?;

        self.file
            .write_all(&compact_operation.to_bytes())
            .map_err(AppError::WriteToWal)?;

        self.file.sync_all().map_err(AppError::WriteToWal)?;

        Ok(())
    }

    pub(crate) fn clear_wal_compact(&mut self) -> Result<(), AppError> {
        self.file.set_len(0).map_err(AppError::CleanWalFile)?;

        self.file.sync_all().map_err(AppError::CleanWalFile)?;

        self.file
            .seek(SeekFrom::Start(0))
            .map_err(AppError::SeekInWal)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Import variants to prevent repetitive prefix nesting

    // Generates an isolated temporary file path unique to each thread execution
    fn get_temp_wal_path() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("test_compact_{}.wal", id))
    }

    // Helper to create an active Wal engine wrapper with a custom file handler stream
    fn create_test_wal(path: &PathBuf) -> File {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap()
    }

    // ========================================================================== --
    // 1. CONVERSION TESTS                                                        --
    // ========================================================================== --

    #[test]
    fn test_every_enum_to_and_from_bytes() {
        let all_operations = vec![(ReplaceData, 1)];

        for (op, expected_byte) in all_operations {
            // Test Forward Conversion: Enum -> Bytes
            let bytes = op.to_bytes();
            assert_eq!(
                bytes[0], expected_byte,
                "Failed mapping serialization for {:?}",
                op
            );

            // Test Reverse Conversion: Bytes -> Enum
            let decoded = CompactOperation::from_bytes(bytes).unwrap();
            assert_eq!(decoded, op);
        }
    }

    #[test]
    fn test_unknown_operation_byte() {
        // Evaluate outside valid 1-12 range (e.g. 0 or 99)
        let invalid_bytes_1 = [0u8];
        let invalid_bytes_2 = [99u8];

        assert!(matches!(
            CompactOperation::from_bytes(invalid_bytes_1).unwrap_err(),
            AppError::UnknownCompactOperation
        ));
        assert!(matches!(
            CompactOperation::from_bytes(invalid_bytes_2).unwrap_err(),
            AppError::UnknownCompactOperation
        ));
    }

    // ========================================================================== --
    // 2. DISK WRITING AND SIZING TESTS                                           --
    // ========================================================================== --

    #[test]
    fn test_writing_each_operation_produces_exactly_1_byte() {
        let path = get_temp_wal_path();
        let file = create_test_wal(&path);
        let mut wal = CompactWal { file };

        let operations = vec![ReplaceData];

        for op in operations {
            wal.write_operation(op).unwrap();

            // Check metadata sizing directly from disk
            let metadata = std::fs::metadata(&path).unwrap();
            assert_eq!(
                metadata.len(),
                1,
                "Operation {:?} must write exactly 1 byte to file",
                op
            );

            // Read back and check inner byte alignment values
            let mut read_buf = [0u8; 1];
            wal.file.seek(SeekFrom::Start(0)).unwrap();
            wal.file.read_exact(&mut read_buf).unwrap();
            assert_eq!(read_buf, op.to_bytes());
        }

        let _ = std::fs::remove_file(&path);
    }

    // ========================================================================== --
    // 3. STATE MACHINE ADVANCEMENT TESTS                                         --
    // ========================================================================== --

    #[test]
    fn test_replace_data_clears_the_wal() {
        let path = get_temp_wal_path();
        let file = create_test_wal(&path);
        let mut wal = CompactWal { file };

        // Put down initial boilerplate layout data into file beforehand
        wal.write_operation(ReplaceData).unwrap();
        let initial_meta = std::fs::metadata(&path).unwrap();
        assert_eq!(initial_meta.len(), 1);

        wal.clear_wal_compact().unwrap();
        // Verify the file footprint was truncated down to 0 bytes completely
        let final_meta = std::fs::metadata(&path).unwrap();
        assert_eq!(
            final_meta.len(),
            0,
            "WAL payload footprint must be completely zeroed out"
        );

        // Verify seek pointer position resets safely to zero boundary constraints
        let pointer_pos = wal.file.stream_position().unwrap();
        assert_eq!(pointer_pos, 0);

        let _ = std::fs::remove_file(&path);
    }
}
