# Vault

A small persistent key-value storage engine written in Rust.

Vault was built as a systems-programming project to explore persistent storage, append-only data files, indexing, write-ahead logging, crash recovery, and database compaction.

The project focuses on correctness and recovery behavior rather than providing a production-ready database.

## Features

- Persistent key-value storage
- Append-only data file
- In-memory key-to-offset index
- `set` and `get` operations
- Updating an existing key by appending a new record
- Database corruption detection
- Database compaction
- Temporary files during compaction
- Write-ahead logging (WAL) for writes
- WAL-based write recovery
- WAL-based compaction recovery
- Crash-oriented recovery testing
- Idempotent recovery
- Unit tests for storage, WAL, and recovery logic

## Storage Format

Vault stores data in an append-only `data.db` file.

Each record is stored as:

```text
[key_len][value_len][key][value]
```

The index stores the latest offset for each key:

```text
[key_len][key][offset]
```

The in-memory index is represented as:

```text
HashMap<String, u64>
```

where the `String` is the key and the `u64` is the offset of its latest record in `data.db`.

When a key is updated, Vault appends a new record instead of modifying the existing record in place.

## Write-Ahead Logging

Vault uses a WAL to protect writes against interruptions during the transition between `data.db` and `index.db`.

A write progresses through these stages:

```text
Write WAL
   ↓
Write data.db
   ↓
Write index.db
   ↓
Clear WAL
```

If the application stops during the operation, the WAL is used during startup recovery to determine what must be completed.

## Compaction

Because updates are appended, old records remain in `data.db`.

Compaction removes obsolete records and creates new compacted database files:

```text
data.db
   ↓
copy live records
   ↓
data.temp.db
index.temp.db
   ↓
replace database files
```

Compaction is protected by its own WAL.

The recovery implementation is designed so that it does not depend on filesystem timestamps or assume that a previous rename completed successfully.

When necessary, the current `data.db` is treated as the authoritative source for rebuilding the index.

## Crash Recovery

Recovery was tested at multiple points during database operations, including crashes:

- immediately after WAL creation
- after temporary data synchronization
- after temporary index synchronization
- after replacing `data.db`
- after replacing both database files but before clearing the WAL

These tests exposed and helped fix a real recovery issue where the old `index.db` could no longer be trusted after `data.db` had already been replaced.

The final recovery process can safely repeat the compaction operation when the WAL indicates that compaction was interrupted.

## Project Structure

```text
src/
├── app.rs
├── cli.rs
├── config.rs
├── error.rs
├── logging.rs
├── main.rs
├── storage/
│   ├── mod.rs
│   ├── storage_engine.rs
│   ├── index.rs
│   └── test.rs
├── wal/
│   ├── mod.rs
│   ├── put_wal.rs
│   └── compact_wal.rs
└── recovery/
    ├── mod.rs
    ├── rec.rs
    ├── compact_rec/
    │   ├── mod.rs
    │   └── compact_recovery.rs
    └── put_rec/
        ├── mod.rs
        ├── put_recovery.rs
        ├── db_recovery.rs
        └── index_recovery.rs
```

## CLI

### Set a value

```bash
cargo run -- set -k name -v jamal
```

### Get a value

```bash
cargo run -- get -k name
```

### Compact the database

```bash
cargo run -- compact
```

### Run recovery manually

```bash
cargo run -- recovery
```

Recovery is also integrated into application startup so interrupted operations can be recovered before normal database access.

## Testing

The project contains tests covering:

- valid database records
- empty databases
- partial headers
- partial keys
- partial values
- corruption detection
- PUT WAL recovery
- partial WAL records
- invalid WAL operations
- corrupted WAL data
- compact recovery
- temporary-file rebuilding
- database replacement
- idempotent recovery

Current test status:

```text
37 passed
0 failed
```

The project also passes:

```bash
cargo fmt
cargo check
cargo clippy
cargo test -- --test-threads=1
```

## Technology

- Rust
- Cargo
- Standard library filesystem and I/O APIs
- `HashMap`
- Write-ahead logging
- Append-only storage

## Project Status

**v1.0.0**

Vault v1.0 represents the completed learning scope of this project.

The goal was to build a small but meaningful storage engine while gaining practical experience with:

- file formats
- binary serialization
- offsets
- persistent indexes
- filesystem operations
- WAL design
- crash recovery
- idempotency
- compaction
- failure testing

This project is primarily an educational systems-programming project and is not intended to be a production database.

## License

This project is part of a personal Rust systems-programming learning journey.