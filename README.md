# StarRocks Backup & Restore - CLI Usage Guide

## Overview

The StarRocks Backup & Restore tool provides production-grade automation for backup and restore operations following the playbook.md specifications.

## Installation

```bash
# Activate virtual environment
source .venv/bin/activate

# The CLI is already installed as: starrocks-br
```

## Configuration

Create a `config.yaml` file with your StarRocks connection details:

```yaml
host: "127.0.0.1"
port: 9030
user: "root"
password: ""
database: "your_database"
repository: "your_repo_name"
```

**Note:** The repository must be created in StarRocks using the `CREATE REPOSITORY` command before running backups. For example:

```sql
CREATE REPOSITORY `your_repo_name`
WITH S3
ON LOCATION "s3://your-backup-bucket/backups/"
PROPERTIES (
    "aws.s3.access_key" = "your-access-key",
    "aws.s3.secret_key" = "your-secret-key",
    "aws.s3.endpoint" = "https://s3.amazonaws.com"
);
```

## Commands

### Initialize Schema

Before running backups, initialize the ops database and control tables:

```bash
starrocks-br init --config config.yaml
```

**What it does:**
- Creates `ops` database
- Creates `ops.table_inventory`: Table backup eligibility configuration
- Creates `ops.backup_history`: Backup operation history
- Creates `ops.restore_history`: Restore operation history
- Creates `ops.run_status`: Job concurrency control

**Next step:** Populate `ops.table_inventory` with your tables and their backup eligibility flags.

**Note:** If you skip this step, the ops schema will be auto-created on your first backup/restore operation (with a warning).

### Backup Commands

#### 1. Incremental Backup (Daily)

Backs up partitions that have been updated in the last N days.

```bash
starrocks-br backup incremental --config config.yaml --days 7
```

**Flow:**
1. Load config → verify cluster health
2. Ensure repository exists
3. Reserve job slot (prevent concurrent backups)
4. Query `ops.table_inventory` for tables where `incremental_eligible = TRUE`
5. Find recent partitions from `information_schema.partitions` (filtered by eligible tables)
6. Generate unique label (format: `{db}_{yyyymmdd}_inc`)
7. Build and execute `BACKUP SNAPSHOT ... PARTITION (...)` command
8. Poll `SHOW BACKUP` until completion
9. Log to `ops.backup_history` and release job slot

#### 2. Weekly Full Backup

Backs up dimension and non-partitioned tables from `ops.table_inventory`.

```bash
starrocks-br backup weekly --config config.yaml
```

**Flow:**
1. Load config → verify cluster health
2. Ensure repository exists
3. Reserve job slot
4. Query `ops.table_inventory` for tables where `weekly_eligible = TRUE`
5. Generate unique label (format: `{db}_{yyyymmdd}_weekly`)
6. Build and execute `BACKUP SNAPSHOT ... ON (TABLE ...)` command
7. Poll until completion and log results

#### 3. Monthly Full Database Backup

Backs up the entire database.

```bash
starrocks-br backup monthly --config config.yaml
```

**Flow:**
1. Load config → verify cluster health
2. Ensure repository exists
3. Reserve job slot
4. Generate unique label (format: `{db}_{yyyymmdd}_monthly`)
5. Build and execute `BACKUP DATABASE ... SNAPSHOT` command
6. Poll until completion and log results

### Restore Commands

#### Restore Single Partition

Restores a specific partition from a backup snapshot.

```bash
starrocks-br restore-partition \
  --config config.yaml \
  --backup-label my_db_20251016_inc \
  --table my_db.fact_sales \
  --partition p20251016
```

**Parameters:**
- `--backup-label`: The snapshot label to restore from
- `--table`: Fully qualified table name (database.table)
- `--partition`: Partition name to restore

**Flow:**
1. Load config
2. Build `RESTORE SNAPSHOT ... ON (TABLE ... PARTITION (...))` command
3. Execute restore
4. Poll `SHOW RESTORE` until completion
5. Log to `ops.restore_history`

## Example Usage Scenarios

### Initial Setup

```bash
# 1. Initialize ops schema (run once)
starrocks-br init --config config.yaml

# 2. Populate table inventory (in StarRocks)
# INSERT INTO ops.table_inventory VALUES (...);
```

### Daily Production Backup (Mon-Sat)

```bash
# Run via cron at 01:00
0 1 * * 1-6 cd /path/to/starrocks-br && source .venv/bin/activate && starrocks-br backup incremental --config config.yaml --days 1
```

### Weekly Full Backup (Sunday)

```bash
# Run via cron at 01:00 on Sundays
0 1 * * 0 cd /path/to/starrocks-br && source .venv/bin/activate && starrocks-br backup weekly --config config.yaml
```

### Monthly Baseline (First Sunday)

```bash
# Run via cron at 01:00 on the first Sunday of each month
0 1 1-7 * 0 cd /path/to/starrocks-br && source .venv/bin/activate && starrocks-br backup monthly --config config.yaml
```

### Disaster Recovery - Restore Recent Partition

```bash
# Restore yesterday's partition from incremental backup
starrocks-br restore-partition \
  --config config.yaml \
  --backup-label sales_db_20251015_inc \
  --table sales_db.fact_sales \
  --partition p20251015
```

## Error Handling

The CLI automatically handles:

- **Job slot conflicts**: Prevents overlapping backups/restores via `ops.run_status`
- **Label collisions**: Automatically appends `_r#` suffix if label exists
- **Cluster health**: Verifies FE/BE status before starting operations
- **Repository validation**: Ensures repository exists and is accessible
- **Graceful failures**: All errors are logged to history tables with proper status

## Monitoring

All operations are logged to:
- `ops.backup_history`: Tracks all backup attempts with status, timestamps, and error messages
- `ops.restore_history`: Tracks all restore operations with verification checksums
- `ops.run_status`: Tracks active jobs to prevent conflicts

Query examples:

```sql
-- Check recent backup status
SELECT label, backup_type, status, started_at, finished_at
FROM ops.backup_history
ORDER BY started_at DESC
LIMIT 10;

-- Check for failed backups
SELECT label, backup_type, error_message, started_at
FROM ops.backup_history
WHERE status = 'FAILED'
ORDER BY started_at DESC;

-- Check active jobs
SELECT scope, label, state, started_at
FROM ops.run_status
WHERE state = 'ACTIVE';
```

## Testing

The project includes comprehensive tests (142 tests, 90% coverage):

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src/starrocks_br --cov-report=term-missing

# Run specific test file
pytest tests/test_cli.py -v
```

## Project Status

✅ **Completed:**
- Config loader & validation
- Database connection wrapper
- StarRocks repository management
- Cluster health checks
- Job slot reservation (concurrency control)
- Label generation with collision handling
- Incremental/weekly/monthly backup planners with table eligibility filtering
- Schema initialization (ops tables) with auto-creation
- Backup & restore history logging
- Backup executor with polling
- Restore operations with polling
- **CLI commands (5 commands: init, 3 backup types, restore)**

📋 **Optional (deferred):**
- Exponential backoff retry for job conflicts
- Disk space precheck (requires external monitoring)
- Formal runbooks and DR drill procedures
- Monitoring dashboards and alerting integration