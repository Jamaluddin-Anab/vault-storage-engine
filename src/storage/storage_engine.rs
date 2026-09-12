use crate::error::AppError;
use crate::recovery::rec::Recovery;
use crate::storage::index::Index;
use crate::storage::storage_engine::ReadStatus::{CompleteRead, CorruptTail, Eof};
use crate::wal::compact_wal::CompactOperation::*;
use crate::wal::compact_wal::CompactWal;
use crate::wal::put_wal::{Operation, PutWal};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(crate) struct StorageEngine {
    pub(super) file: File,
    pub(super) index: Index,
    pub(super) put_wal: PutWal,
    pub(super) compact_wal: CompactWal,
}

pub(crate) enum ReadStatus {
    Eof,
    CompleteRead(u64),
    CorruptTail,
}

impl StorageEngine {
    pub(crate) const KEY_LEN: u64 = 256;
    pub(crate) const VALUE_LEN: u64 = 1024 * 1024;

    pub(crate) fn start() -> Result<StorageEngine, AppError> {
        Recovery::start()?;

        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .truncate(false)
            .open(Path::new("data.db"))
            .map_err(AppError::LoadDbFile)?;

        let index = Index::load_index()?;
        let put_wal = PutWal::new()?;
        let compact_wal = CompactWal::new()?;

        Ok(StorageEngine {
            file,
            index,
            put_wal,
            compact_wal,
        })
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

        self.put_wal
            .write(Operation::WriteInDb, key.as_str(), value.as_str(), offset)?;

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
        self.file.sync_all().map_err(AppError::WriteToDb)?;

        self.put_wal.mark_write_index()?;

        self.index.put(key, offset)?;

        self.put_wal.clear()?;

        Ok(())
    }

    pub(crate) fn get(&mut self, key: &str) -> Result<Option<String>, AppError> {
        if key.is_empty() || key.len() > Self::KEY_LEN as usize {
            return Err(AppError::InvalidKey(key.len()));
        }

        if let Some(offset) = self.index.get(key) {
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
            let disk_key = String::from_utf8(buf).map_err(AppError::ConvertUtf8ToString)?;

            if !disk_key.eq(&key) {
                return Ok(None);
            }

            let mut buf = vec![0u8; value_len as usize];
            file.read_exact(&mut buf).map_err(AppError::ReadValue)?;
            let value = String::from_utf8(buf).map_err(AppError::ConvertUtf8ToString)?;

            return Ok(Some(value));
        }

        Ok(None)
    }

    pub(crate) fn rebuild_len(file: &mut File) -> Result<ReadStatus, AppError> {
        let mut buf = [0u8; 4];
        match file.read(&mut buf) {
            Ok(0) => Ok(Eof), // clean end of file
            Ok(4) => Ok(CompleteRead(u32::from_le_bytes(buf) as u64)),
            Ok(_) => Ok(CorruptTail), // partially read at the very end of file (corrupt tail) EOF
            Err(err) => Err(AppError::ReadHeaderLen(err)),
        }
    }

    pub(crate) fn compact(&mut self) -> Result<(), AppError> {
        self.compact_wal.write_operation(CreateData)?;
        let mut data_temp = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("data.temp.db")
            .map_err(AppError::LoadTempDbFile)?;

        self.compact_wal.write_operation(CreateIndex)?;
        let mut index_temp = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("index.temp.db")
            .map_err(AppError::LoadTempIndexFile)?;

        let mut compact_offset = HashMap::<String, u64>::new();

        self.compact_wal.write_operation(CopyDataTo)?;
        for (key, old_offset) in &self.index.index {
            self.file
                .seek(SeekFrom::Start(*old_offset))
                .map_err(AppError::SeekInDb)?;

            let key_len = match Self::rebuild_len(&mut self.file)? {
                Eof => break,
                CompleteRead(key_len) => key_len,
                CorruptTail => return Err(AppError::CorruptedDb),
            };
            let value_len = match Self::rebuild_len(&mut self.file)? {
                Eof => break,
                CompleteRead(value_len) => value_len,
                CorruptTail => return Err(AppError::CorruptedDb),
            };
            self.file
                .seek(SeekFrom::Current(key_len as i64))
                .map_err(AppError::SeekInDb)?;

            let mut value_buf = vec![0u8; value_len as usize];
            self.file
                .read_exact(&mut value_buf)
                .map_err(|_| AppError::CorruptedDb)?;

            let new_offset = data_temp
                .stream_position()
                .map_err(AppError::SeekInTempDb)?;
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

        self.compact_wal.write_operation(CopyIndexTo)?;
        for (key, offset) in &compact_offset {
            let key_len = key.len() as u32;
            let record = [
                key_len.to_le_bytes().as_slice(),
                key.as_bytes(),
                &offset.to_le_bytes(),
            ]
            .concat();
            index_temp
                .write_all(&record)
                .map_err(AppError::WriteToTempIndex)?;
        }
        index_temp.sync_all().map_err(AppError::WriteToTempIndex)?;

        drop(index_temp);
        drop(data_temp);

        self.compact_wal.write_operation(ReplaceData)?;
        std::fs::rename("data.temp.db", "data.db").map_err(AppError::ReplaceDbFile)?;
        self.compact_wal.write_operation(ReplaceIndex)?;
        std::fs::rename("index.temp.db", "index.db").map_err(AppError::ReplaceIndexFile)?;
        self.compact_wal.clear_wal_compact()?;

        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open("data.db")
            .map_err(AppError::LoadDbFile)?;

        let new_index = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open("index.db")
            .map_err(AppError::LoadIndexFile)?;

        self.index.update_memory_map(compact_offset, new_index);

        Ok(())
    }
}
