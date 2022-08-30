# Speedb Change Log

## Unreleased

Based on RocksDB 7.2.2

### New Features

### Bug Fixes

### Behavior Changes

### Performance Improvements

### Public API changes

### Java API Changes

## 2.0.0

Based on RocksDB 7.2.2

### New Features

 * Added a new experimental hash-based memtable with more granular locking (#30)
 * Added ability to create `MemTableFactory` from URI/string to tools (#72)

### Bug Fixes

 * Avoid comparing Status using == as it compares only status codes but not subcodes
 * examples: fix snapshots leak in `optimistic_transaction_example`
 * ldb: fix the `get` command to print the entire value
 * db_bench: fix a bug in handling multiple DBs that caused a `last_ref` assertion
 * db_bench: fix `SeekRandom` and `ReadRandomWriteRandom` to work on all CFs instead of only the default
 * db_bench: report accurate read response time when using a rate limit

### Miscellaneous

 * Add test for forwarding the incomplete status on `no_io` (an addition to facebook/rocksdb#8485)
 * CMake: fix the plugin infra and add support for `*_FUNC` registration
