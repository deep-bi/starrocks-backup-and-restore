# Getting Started

This guide walks you through setting up and running your first backup with StarRocks Backup & Restore.

## Prerequisites

Before you begin, ensure you have:

1. **StarRocks 3.5 or later** running in **shared-nothing mode**
   - Shared-data architecture is not currently supported
   - Earlier versions (< 3.5) are not supported due to differences in `SHOW FRONTENDS` and `SHOW BACKENDS` output formats
2. **A backup repository** - You need to create this in StarRocks first (see [Repository Setup](#repository-setup) below)
3. **Database access** - User account with backup/restore privileges
4. **Python 3.8+** (if installing via PyPI) or download the standalone executable

## Repository Setup

First, create a backup repository in StarRocks. This defines where your backup data will be stored.

Connect to your StarRocks cluster and run:

```sql
CREATE REPOSITORY `my_backup_repo`
WITH S3
ON LOCATION "s3://your-backup-bucket/backups/"
PROPERTIES (
    "aws.s3.access_key" = "your-access-key",
    "aws.s3.secret_key" = "your-secret-key",
    "aws.s3.endpoint" = "https://s3.amazonaws.com"
);
```

Verify it was created:

```sql
SHOW REPOSITORIES;
```

For other storage backends (HDFS, Azure Blob, etc.), see the [StarRocks documentation](https://docs.starrocks.io/docs/administration/management/Backup_and_restore/).

## Installation

Choose one of the following installation methods:

### Option 1: Install from PyPI (Recommended)

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Linux/Mac
# .venv\Scripts\activate    # On Windows

# Install the package
pip install starrocks-br

# Verify installation
starrocks-br --help
```

**Note:** Always activate the virtual environment before using the tool.

### Option 2: Download Standalone Executable

Download the pre-built executable for your platform from the [latest release](https://github.com/deep-bi/starrocks-backup-and-restore/releases/latest):

- `starrocks-br-linux-x86_64` → Linux (Intel/AMD)
- `starrocks-br-windows-x86_64.exe` → Windows (Intel/AMD)
- `starrocks-br-macos-arm64` → macOS Apple Silicon
- `starrocks-br-macos-x86_64` → macOS Intel

Make it executable (Linux/macOS):
```bash
chmod +x starrocks-br-*
```

See [Installation Guide](installation.md) for more installation options.

## Configuration

Create a `config.yaml` file with your StarRocks connection details:

```yaml
host: "127.0.0.1"
port: 9030
user: "root"
database: "your_database"       # The database you want to backup
repository: "my_backup_repo"    # The repository you created above

# Optional: Define table inventory groups in the config file
table_inventory:
  - group: "important_tables"
    tables:
      - database: "your_database"
        table: "users"
      - database: "your_database"
        table: "orders"

  - group: "full_backup"
    tables:
      - database: "your_database"
        table: "*"  # Wildcard for all tables
```

Set your database password as an environment variable (never store passwords in config files):

```bash
export STARROCKS_PASSWORD="your_password"
```

On Windows (PowerShell):
```powershell
$env:STARROCKS_PASSWORD="your_password"
```

## Initialize the Tool

Initialize the ops database and control tables:

```bash
starrocks-br init --config config.yaml
```

This creates:
- `ops` database for storing metadata
- `ops.table_inventory` - where you'll define your backup groups
- `ops.backup_history` - tracks all backup operations
- `ops.restore_history` - tracks restore operations
- `ops.run_status` - prevents concurrent operations
- `ops.backup_partitions` - partition-level backup details

## Define Your Backup Groups

Now decide which tables you want to back up and how to group them.

### Option 1: Define in Config File (Recommended)

If you included `table_inventory` in your `config.yaml`, the init command automatically populated your groups. You can verify:

```sql
SELECT * FROM ops.table_inventory;
```

**Important:** If you add or modify tables in the `table_inventory` section of your config file later, rerun the init command to update the database:

```bash
starrocks-br init --config config.yaml
```

### Option 2: Manual SQL Insert

Alternatively, connect to your StarRocks cluster and populate the inventory manually:

```sql
-- Example: Create a group for important tables
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('important_tables', 'your_database', 'users'),
  ('important_tables', 'your_database', 'orders'),
  ('important_tables', 'your_database', 'payments');

-- You can create multiple groups
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('analytics_tables', 'your_database', 'events'),
  ('analytics_tables', 'your_database', 'metrics');

-- Or use a wildcard to backup all tables
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES ('full_backup', 'your_database', '*');
```

**Tip:** Think about how you'll schedule backups when creating groups. Tables with similar backup needs should be in the same group.

Not sure how to group your tables? See [Core Concepts: Inventory Groups](core-concepts.md#inventory-groups) for guidance.

## Run Your First Backup

Now you're ready to run a backup!

### Full Backup

Run a full backup of a group:

```bash
starrocks-br backup full --config config.yaml --group important_tables
```

The tool will:
1. Verify cluster health
2. Find all tables in the `important_tables` group
3. Execute the backup
4. Poll until completion
5. Log results to `ops.backup_history`

### Monitor the Backup

While the backup runs, you can check its status in StarRocks:

```sql
-- Check active backup jobs
SHOW BACKUP;

-- Check backup history (after completion)
SELECT label, backup_type, status, started_at, finished_at, error_message
FROM ops.backup_history
ORDER BY started_at DESC
LIMIT 10;
```

## Run an Incremental Backup

After you have a full backup, you can run incremental backups to capture only changed partitions:

```bash
starrocks-br backup incremental --config config.yaml --group important_tables
```

The tool automatically:
1. Finds the most recent full backup for the group
2. Compares current partitions with the baseline
3. Backs up only new or modified partitions

**Note:** Incremental backups require partitioned tables. If your tables aren't partitioned, use full backups.

## Restore from a Backup

To restore data from a backup, you need the backup label (found in `ops.backup_history`).

### Find Available Backups

```sql
SELECT label, backup_type, status, finished_at
FROM ops.backup_history
WHERE status = 'SUCCESS'
ORDER BY finished_at DESC;
```

### Restore All Tables

```bash
starrocks-br restore \
  --config config.yaml \
  --target-label your_backup_label_here
```

### Restore Specific Group

```bash
starrocks-br restore \
  --config config.yaml \
  --target-label your_backup_label_here \
  --group important_tables
```

### Restore Single Table

```bash
starrocks-br restore \
  --config config.yaml \
  --target-label your_backup_label_here \
  --table users
```

The tool automatically handles backup chains - if you specify an incremental backup, it will restore the base full backup first, then apply the incremental.

## Verify the Restore

The restore process uses temporary tables with a `_restored` suffix. After restore completes, you can verify the data before it's made live:

```sql
-- Check the restored data
SELECT COUNT(*) FROM users_restored;
SELECT * FROM users_restored LIMIT 10;

-- Compare with current data if needed
SELECT COUNT(*) FROM users;
```

The tool performs atomic renames to swap the temp tables with the live tables only after the restore succeeds.

## Next Steps

Now that you've completed your first backup and restore:

- **Understand the concepts**: Read [Core Concepts](core-concepts.md) to deepen your understanding
- **Explore all commands**: See [Command Reference](commands.md) for detailed options
- **Automate backups**: Learn about scheduling in [Scheduling and Monitoring](scheduling.md)
- **Advanced configuration**: Check [Configuration Reference](configuration.md) for TLS, custom settings, etc.

## Quick Reference

```bash
# Initialize (run once)
starrocks-br init --config config.yaml

# Full backup
starrocks-br backup full --config config.yaml --group my_group

# Incremental backup
starrocks-br backup incremental --config config.yaml --group my_group

# Restore
starrocks-br restore --config config.yaml --target-label backup_label

# Restore with options
starrocks-br restore \
  --config config.yaml \
  --target-label backup_label \
  --group my_group \
  --rename-suffix _verified
```

## Troubleshooting

**"Repository not found"**
- Verify the repository exists: `SHOW REPOSITORIES;`
- Check the repository name matches your config file

**"No full backup found for incremental"**
- Run a full backup first: `starrocks-br backup full --group my_group`

**"Connection refused"**
- Verify host and port in config.yaml
- Check that STARROCKS_PASSWORD is set: `echo $STARROCKS_PASSWORD`
- Ensure StarRocks FE is running

**"Table not found in inventory"**
- Check your inventory: `SELECT * FROM ops.table_inventory WHERE inventory_group = 'your_group';`
- Add missing tables to the inventory

For more help, see the [GitHub Issues](https://github.com/deep-bi/starrocks-backup-and-restore/issues).
