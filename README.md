# StarRocks Backup & Restore - CLI Usage Guide

## Overview

The StarRocks Backup & Restore tool provides production-grade automation for backup and restore operations.

## Installation

### Option 1: Install from PyPI (Recommended for Production)

We recommend using a virtual environment to ensure proper script availability and dependency isolation:

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Linux/Mac
# .venv\Scripts\activate    # On Windows

# Install the package from PyPI
pip install starrocks-br==0.1.0

# Verify the installation
starrocks-br --help
```

**Note:** Always activate the virtual environment before using the tool. The `starrocks-br` command will only be available when the virtual environment is activated.

### Option 2: Using Devbox (Recommended for Development)

Devbox provides a reproducible development environment with all required tools.

```bash
# Install devbox (if not already installed)
curl -fsSL https://get.jetpack.io/devbox | bash

# Start devbox shell - this automatically:
# - Installs Python 3.11 and dependencies
# - Creates a virtual environment (.venv)
# - Installs the package in editable mode
# - Installs development dependencies
devbox shell

# Once inside the devbox shell, you're ready to go:
starrocks-br --help
pytest
```

### Option 3: Manual Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd starrocks-br

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install in editable mode with development dependencies
pip install -e ".[dev]"

# The CLI is now available as: starrocks-br
```

## Quick Start

After installing from PyPI, follow these steps:

1. **Activate your virtual environment** (if not already active):
   ```bash
   source .venv/bin/activate  # On Linux/Mac
   # .venv\Scripts\activate    # On Windows
   ```

2. **Verify installation:**
   ```bash
   starrocks-br --help
   ```

3. **Create your `config.yaml` file** (see Configuration section below)

4. **Set your password as an environment variable:**
   ```bash
   export STARROCKS_PASSWORD="your_password"
   ```
   
   On Windows (PowerShell):
   ```powershell
   $env:STARROCKS_PASSWORD="your_password"
   ```
   
   On Windows (Command Prompt):
   ```cmd
   set STARROCKS_PASSWORD=your_password
   ```

5. **Initialize the ops schema:**
   ```bash
   starrocks-br init --config config.yaml
   ```

6. **Start using the tool** - see Commands section below for details

## Configuration

**Important:** After installing the package, you need to create your own `config.yaml` file. This file is **not included in the package** - each user creates it with their own StarRocks connection details. You can place it anywhere and reference it using the `--config` parameter.

Create a `config.yaml` file in your working directory (or any location you prefer) with your StarRocks connection details:

```yaml
host: "127.0.0.1"
port: 9030
user: "root"
database: "your_database"
repository: "your_repo_name"
```

**Password Management**

The database password must be provided via the `STARROCKS_PASSWORD` environment variable. This is a security measure to prevent storing credentials in configuration files.

```bash
export STARROCKS_PASSWORD="your_password"
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
- Creates `ops.table_inventory`: Inventory groups mapping to databases/tables
- Creates `ops.backup_history`: Backup operation history
- Creates `ops.restore_history`: Restore operation history
- Creates `ops.run_status`: Job concurrency control
- Creates `ops.backup_partitions`: Partition manifest for each backup (enables intelligent restore)

**Next step:** Populate `ops.table_inventory` with your backup groups. For example:
```sql
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('daily_facts', 'your_db', 'fact_sales'),
  ('weekly_dims', 'your_db', 'dim_users'),
  ('weekly_dims', 'your_db', 'dim_products'),
  ('full_db_backup', 'your_db', '*'); -- Wildcard for all tables
```

**Note:** If you skip this step, the ops schema will be auto-created on your first backup/restore operation (with a warning).

### Backup Commands

Backups are managed through "inventory groups" defined in `ops.table_inventory`. This provides a flexible way to schedule different backup strategies for different sets of tables.

#### 1. Full Backup

Runs a full backup for all tables within a specified inventory group.

```bash
starrocks-br backup full --config config.yaml --group <group_name>
```

**Parameters:**
- `--group`: The inventory group to back up.

**Flow:**
1. Load config → verify cluster health → ensure repository exists
2. Reserve job slot (prevent concurrent backups)
3. Query `ops.table_inventory` for all tables in the specified group.
4. Generate a unique backup label.
5. Build and execute the `BACKUP` command for the resolved tables.
6. Poll `SHOW BACKUP` until completion and log results.

#### 2. Incremental Backup

Backs up only the partitions that have changed since the last successful full backup for a given inventory group.

```bash
starrocks-br backup incremental --config config.yaml --group <group_name>
```

**Parameters:**
- `--group`: The inventory group to back up.
- `--baseline-backup` (Optional): Specify a backup label to use as the baseline instead of the latest full backup.

**Flow:**
1. Load config → verify cluster health → ensure repository exists
2. Reserve job slot
3. Find the latest successful full backup for the group to use as a baseline.
4. Find recent partitions from `information_schema.partitions` for tables in the group.
5. Generate a unique backup label.
6. Build and execute the `BACKUP` command for the new partitions.
7. Poll `SHOW BACKUP` until completion and log results.

### Restore Commands

#### Intelligent Point-in-Time Restore

Restores data to a specific point in time using intelligent backup chain resolution. This command automatically determines the correct sequence of backups needed for restore.

```bash
starrocks-br restore \
  --config config.yaml \
  --target-label my_db_20251016_inc \
  --group daily_facts \
  --rename-suffix _restored
```

**Parameters:**
- `--config`: Path to config YAML file (required)
- `--target-label`: Backup label to restore to (required)
- `--group`: Optional inventory group to filter tables to restore
- `--rename-suffix`: Suffix for temporary tables during restore (default: `_restored`)

**How it works:**
- **For full backups**: Restores directly from the target backup
- **For incremental backups**: Automatically restores the base full backup first, then applies the incremental
- **Safety mechanism**: Uses temporary tables with the specified suffix, then performs atomic rename to make restored data live

**Two Restore Modes:**
- **Disaster Recovery**: Restore all tables from a backup (omit `--group` parameter)
- **Surgical Restore**: Restore only specific table groups (use `--group` parameter)

**Purpose of `--rename-suffix`:**
The restore process creates temporary tables with the specified suffix (e.g., `table_restored`) to avoid conflicts with existing tables. Once the restore is complete and verified, the tool performs atomic renames to swap the original tables with the restored data. This ensures data safety and allows for rollback if needed.

**Flow:**
1. Load config → verify cluster health → ensure repository exists
2. Find the correct restore sequence (full backup + optional incremental)
3. Get tables from backup manifest (optionally filtered by group)
4. Execute restore flow with atomic renames
5. Log to `ops.restore_history`

## Example Usage Scenarios

### Initial Setup

```bash
# 1. Initialize ops schema (run once)
starrocks-br init --config config.yaml

# 2. Populate table inventory with your groups (in StarRocks)
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('daily_incrementals', 'sales_db', 'fact_orders'),
  ('weekly_full', 'sales_db', 'dim_customers'),
  ('weekly_full', 'sales_db', 'dim_products');
```

### Daily Incremental Backup (Mon-Sat)

```bash
# Run via cron at 01:00
0 1 * * 1-6 cd /path/to/starrocks-br && source .venv/bin/activate && starrocks-br backup incremental --config config.yaml --group daily_incrementals
```

### Weekly Full Backup (Sunday)

```bash
# Run via cron at 01:00 on Sundays
0 1 * * 0 cd /path/to/starrocks-br && source .venv/bin/activate && starrocks-br backup full --config config.yaml --group weekly_full
```

### Disaster Recovery - Point-in-Time Restore

```bash
# Restore to a specific backup point (automatically handles full + incremental chain)
starrocks-br restore \
  --config config.yaml \
  --target-label sales_db_20251015_inc \
  --group daily_facts

# Restore all tables from a full backup
starrocks-br restore \
  --config config.yaml \
  --target-label sales_db_20251014_full
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

The project includes comprehensive tests (150+ tests, 90%+ coverage).

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
- Group-based backup planners for full and incremental backups
- Schema initialization (ops tables) with auto-creation
- Backup & restore history logging
- Backup executor with polling
- Intelligent point-in-time restore with automatic backup chain resolution
- Partition metadata tracking for backup manifests
- Atomic table rename for safe restore operations
- CLI commands (init, backup full, backup incremental, restore)

📋 **Optional (deferred):**
- Exponential backoff retry for job conflicts
- Disk space precheck (requires external monitoring)
- Formal runbooks and DR drill procedures
- Monitoring dashboards and alerting integration