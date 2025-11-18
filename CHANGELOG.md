# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2025-11-18

### Fixed
- **Critical Fix**: Resolved StarRocks 128-byte PRIMARY KEY size limit issue for `ops.backup_partitions` table
  - Changed `backup_partitions` table to use hash-based PRIMARY KEY (`key_hash`) instead of composite key
  - Composite keys (label + database_name + table_name + partition_name) can now exceed 128 bytes without errors
  - Implemented MD5 hashing for partition tracking to bypass size restrictions
  - Updated `record_backup_partitions()` to automatically compute MD5 hash of composite keys

### Changed
- **Schema Migration**: `ops.table_inventory` now uses UNIQUE KEY instead of PRIMARY KEY
  - Prevents potential size limit issues with long database/table names
  - Maintains backward-compatible INSERT behavior for users
  - No manual hash computation required for table inventory operations

### Technical Details
- Hash-based approach allows unlimited composite key sizes (248+ bytes tested)
- Old limit: 128 bytes for composite PRIMARY KEY
- New approach: 32-byte MD5 hash as PRIMARY KEY
- Distribution strategy: `DISTRIBUTED BY HASH(key_hash)` for backup_partitions
- Distribution strategy: `DISTRIBUTED BY HASH(inventory_group)` for table_inventory

### Migration Notes
⚠️ **Breaking Change**: This is a schema-breaking change for existing deployments.

Existing users must:
1. Drop and recreate `ops.backup_partitions` table (or add `key_hash` column and populate)
2. Drop and recreate `ops.table_inventory` table with UNIQUE KEY schema
3. Re-run `starrocks-br init` to apply the new schema

Alternatively, run:
```sql
DROP DATABASE ops;
```
Then re-initialize with `starrocks-br init --config config.yaml`

### Testing
- All 328 unit tests pass
- Integration test suite added for primary key size limit validation
- Test files: `test_pk_limit_fix.sh`, `MANUAL_TEST_GUIDE.md`, `RUN_INTEGRATION_TEST.md`

## [0.2.0] - 2025-11-17

### Added
- Initial public release
- Full and incremental backup support
- Intelligent point-in-time restore
- Inventory group management
- Automatic backup chain resolution
- Concurrency control via job slots
- Comprehensive error handling and logging

### Features
- Multi-table backup with wildcard support
- Partition-level incremental backups
- Atomic restore with temporary table renaming
- TLS/SSL connection support
- Cluster health validation
- Repository verification

## [0.1.0] - 2025-10-30

### Added
- Initial development version
- Core backup and restore functionality
- Schema initialization
- Basic CLI interface