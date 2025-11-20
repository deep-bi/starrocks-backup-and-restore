# StarRocks Backup & Restore

Full and incremental backup automation for StarRocks shared-nothing clusters.

**Requirements:** StarRocks 3.5+ (shared-nothing mode)

📋 **[Release Notes & Changelog](CHANGELOG.md)**

## Table of Contents

- [Why This Tool?](#why-this-tool)
- [Documentation](#documentation)
- [Installation](#installation)
- [Configuration](#configuration)
- [Basic Usage](#basic-usage)
- [How It Works](#how-it-works)

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
- ✅ **Job concurrency control** - Prevents conflicting operations
- ✅ **Safe restores** - Atomic rename mechanism prevents data loss during restore
- ✅ **Metadata management** - Dedicated `ops` database tracks all backup metadata and partition manifests

In short: this tool transforms StarRocks's basic backup/restore commands into a **production-ready incremental backup solution**.

## Documentation

- **[Getting Started](docs/getting-started.md)** - Step-by-step tutorial
- **[Core Concepts](docs/core-concepts.md)** - Understand inventory groups, backup types, and restore chains
- **[Installation](docs/installation.md)** - All installation methods
- **[Configuration](docs/configuration.md)** - Config file reference and TLS setup
- **[Commands](docs/commands.md)** - Detailed command reference
- **[Scheduling & Monitoring](docs/scheduling.md)** - Automate backups and monitor status

## Installation

### Option 1: PyPI

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install starrocks-br
```

### Option 2: Standalone Executable

Download from [releases](https://github.com/deep-bi/starrocks-br/releases/latest):

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

**Define inventory groups** (in StarRocks):
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