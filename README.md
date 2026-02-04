# StarRocks Backup & Restore

Full and incremental backup automation for StarRocks shared-nothing clusters.

**Requirements:** StarRocks 3.5+ (shared-nothing mode)

📋 **[Release Notes & Changelog](CHANGELOG.md)**

## Documentation

- [Why This Tool?](#why-this-tool) (this page)
- [Installation](#installation) (this page)
- [Configuration](#configuration) (this page)
- [Basic Usage](#basic-usage) (this page)
- [How It Works](#how-it-works) (this page)
- **[Getting Started](docs/getting-started.md)** - Step-by-step tutorial
- **[Core Concepts](docs/core-concepts.md)** - Understand inventory groups, backup types, and restore chains
- **[Installation Guide](docs/installation.md)** - All installation methods
- **[Configuration Reference](docs/configuration.md)** - Config file reference and TLS setup
- **[Commands Reference](docs/commands.md)** - Detailed command reference
- **[Scheduling & Monitoring](docs/scheduling.md)** - Automate backups and monitor status

## Why This Tool?

StarRocks provides native `BACKUP` and `RESTORE` commands, but they only support full backups. For large-scale deployments hosting data at petabyte scale, full backups are not feasible due to time, storage, and network constraints.

This tool adds **incremental backup capabilities** to StarRocks by leveraging native partition-based backup features.

**What StarRocks doesn't provide:**
- ❌ **No incremental backups** - You must manually identify changed partitions and build complex backup commands
- ❌ **No backup history** - No built-in way to track what was backed up, when, or which backups succeeded/failed
- ❌ **No restore intelligence** - You manually determine which backups are needed for point-in-time recovery
- ❌ **No organization** - No way to group tables or manage different backup strategies
- ❌ **No concurrency control** - Multiple backup operations can conflict

**What this tool provides:**
- ✅ **Automatic incremental backups** - Tool detects changed partitions since the last full backup automatically
- ✅ **Complete operation tracking** - Every backup and restore is logged with status, timestamps, and error details
- ✅ **Intelligent restore** - Automatically resolves backup chains (full + incremental) for you
- ✅ **Inventory groups** - Organize tables into groups with different backup strategies
- ✅ **Backup lifecycle management** - Prune old backups with flexible retention policies (keep-last, older-than, specific snapshots)
- ✅ **Job concurrency control** - Prevents conflicting operations
- ✅ **Safe restores** - Atomic rename mechanism prevents data loss during restore
- ✅ **Metadata management** - Dedicated `ops` database tracks all backup metadata and partition manifests

In short: this tool transforms StarRocks's basic backup/restore commands into a **production-ready incremental backup solution**.

## Installation

### Option 1: PyPI

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install starrocks-br
```

### Option 2: Standalone Executable

Download from [releases](https://github.com/deep-bi/starrocks-backup-and-restore/releases/latest):

```bash
# Linux
chmod +x starrocks-br-linux-x86_64
mv starrocks-br-linux-x86_64 starrocks-br
./starrocks-br --help
```

See [Installation Guide](docs/installation.md) for all options.

## Configuration

Create a `config.yaml` file pointing to your StarRocks cluster:

```yaml
host: "127.0.0.1"       # StarRocks FE node address
port: 9030              # MySQL protocol port
user: "root"            # Database user with backup/restore privileges
database: "your_database"   # Database containing tables to backup
repository: "your_repo_name"  # Repository created via CREATE REPOSITORY in StarRocks

# Optional: Define table inventory groups directly in config
table_inventory:
  - group: "production"
    tables:
      - database: "mydb"
        table: "users"
      - database: "mydb"
        table: "orders"
```

Set password:
```bash
export STARROCKS_PASSWORD="your_password"
```

See [Configuration Reference](docs/configuration.md) for TLS and advanced options.

## Basic Usage

**Initialize:**
```bash
starrocks-br init --config config.yaml
```

This creates the `ops` database and automatically populates table inventory from your config (if defined).

**Note:** If you modify the `table_inventory` in your config file, rerun `starrocks-br init --config config.yaml` to update the database.

**Alternative: Define inventory groups manually** (in StarRocks):
```sql
INSERT INTO ops.table_inventory (inventory_group, database_name, table_name)
VALUES
  ('production', 'mydb', 'users'),
  ('production', 'mydb', 'orders');
```

**Backup:**
```bash
# Full backup
starrocks-br backup full --config config.yaml --group production

# Incremental backup (tool detects changed partitions automatically)
starrocks-br backup incremental --config config.yaml --group production
```

**Restore:**
```bash
# Tool automatically resolves backup chains
starrocks-br restore --config config.yaml --target-label mydb_20251118_full
```

**Prune old backups:**
```bash
# Keep only last 5 backups
starrocks-br prune --config config.yaml --keep-last 5

# Delete backups older than a date
starrocks-br prune --config config.yaml --older-than "2024-01-01 00:00:00"
```

See [Commands Reference](docs/commands.md) for all options.

## How It Works

1. **Inventory Groups**: Define collections of tables that share the same backup strategy
2. **ops Database**: Tool creates an `ops` database to track all operations and metadata
3. **Automatic Incrementals**: Tool queries partition metadata and compares with the baseline to detect changes
4. **Intelligent Restore**: Automatically resolves backup chains (full + incremental) for point-in-time recovery
5. **Safe Operations**: All restores use temporary tables with atomic rename for safety

Read [Core Concepts](docs/core-concepts.md) for detailed explanations.

## Contributing

We welcome contributions! See issues for areas that need help or create a new issue to report a bug or request a feature.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.