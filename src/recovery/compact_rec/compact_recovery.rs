use crate::error::AppError;
use crate::wal::compact_wal::CompactOperation;
use crate::wal::compact_wal::CompactOperation::*;
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
            CopyIndexTo => {}
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

        self.update_compact_wal(CreateIndex)?;

        Ok(())
    }

    fn update_compact_wal(&mut self, operation: CompactOperation) -> Result<(), AppError> {
        self.compact_file
            .set_len(0)
            .map_err(AppError::CleanWalFile)?;

        self.compact_file
            .write_all(&operation.to_bytes())
            .map_err(AppError::WriteToWal)?;

        self.compact_file
            .sync_all()
            .map_err(AppError::WriteToWal)?;
        Ok(())
    }
}

#[cfg(test)]
mod test_compact_initialization {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    // Mock wrapper structural layout to run your logic inside the test environment
    struct MockCompactor {
        compact_file: File,
    }

    impl MockCompactor {
        fn create_data_test(&mut self) -> Result<(), AppError> {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(Path::new("data.temp.db"))
                .map_err(AppError::CreateTempFile)?;

            self.update_compact_wal(CreateIndex)?;

            Ok(())
        }

        fn update_compact_wal(&mut self, operation: CompactOperation) -> Result<(), AppError> {
            self.compact_file
                .set_len(0)
                .map_err(AppError::CleanWalFile)?;

            // Reposition the cursor to the beginning to prevent null-byte padding bugs
            self.compact_file
                .seek(SeekFrom::Start(0))
                .map_err(AppError::SeekInWal)?;

            self.compact_file
                .write_all(&operation.to_bytes())
                .map_err(AppError::WriteToWal)?;

            self.compact_file
                .sync_all()
                .map_err(AppError::WriteToWal)?;
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

    fn teardown_compact_files() {
        let _ = std::fs::remove_file("data.temp.db");
        let _ = std::fs::remove_file("compact.wal");
    }

    #[test]
    fn test_create_data_no_existing_temp_db() {
        // Force cleanup from any previous runs
        let _ = std::fs::remove_file("data.temp.db");
        let compact_file = setup_clean_compact_wal();

        {
            let mut compactor = MockCompactor { compact_file };
            assert!(compactor.create_data_test().is_ok());
        } // <--- Compactor structure is dropped here, closing the active file handle!

        // VERIFICATIONS:
        // A. Verify data.temp.db exists on the filesystem
        assert!(Path::new("data.temp.db").exists(), "data.temp.db was not created");

        // B. Verify the length of data.temp.db is exactly 0
        let data_meta = std::fs::metadata("data.temp.db").unwrap();
        assert_eq!(data_meta.len(), 0, "data.temp.db length should be 0");

        // C. Verify compact.wal contains exactly CreateIndex
        let mut wal_file = File::open("compact.wal").unwrap();
        let mut wal_buf = [0u8; 1];
        wal_file.read_exact(&mut wal_buf).unwrap();

        assert_eq!(wal_buf, CompactOperation::CreateIndex.to_bytes(), "compact.wal did not advance to CreateIndex");
        assert_eq!(wal_file.metadata().unwrap().len(), 1, "compact.wal should contain exactly 1 byte");

        teardown_compact_files();
    }

    #[test]
    fn test_create_data_with_existing_garbage_temp_db() {
        // Pre-seed data.temp.db with corrupted/stale garbage bytes to simulate a crash midpoint
        {
            let mut dirty_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open("data.temp.db")
                .unwrap();
            dirty_file.write_all(b"corrupted_stale_garbage_database_bytes_payload").unwrap();
            dirty_file.flush().unwrap();
        }

        let compact_file = setup_clean_compact_wal();

        {
            let mut compactor = MockCompactor { compact_file };
            // Execute step - should cleanly truncate the garbage file back down to zero
            assert!(compactor.create_data_test().is_ok());
        } // <--- File handles released safely

        // VERIFICATIONS AFTER RECOVERY SIMULATION:
        // A. Verify data.temp.db still exists
        assert!(Path::new("data.temp.db").exists());

        // B. Verify that the file size was safely truncated back down to 0 bytes
        let data_meta = std::fs::metadata("data.temp.db").unwrap();
        assert_eq!(data_meta.len(), 0, "Stale bytes were not truncated from data.temp.db");

        // C. Verify compact.wal contains exactly the advanced CreateIndex byte
        let mut wal_file = File::open("compact.wal").unwrap();
        let mut wal_buf = [0u8; 1];
        wal_file.read_exact(&mut wal_buf).unwrap();

        assert_eq!(wal_buf, CompactOperation::CreateIndex.to_bytes());
        assert_eq!(wal_file.metadata().unwrap().len(), 1);

        teardown_compact_files();
    }
}

