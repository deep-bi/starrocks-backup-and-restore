# StarRocks Backup and Restore (starrocks-br | MVP)

Minimal Python CLI to orchestrate StarRocks backups and single-table point-in-time restores. The tool is stateless; all state lives in StarRocks metadata tables.

## Features
- Init: creates metadata (`ops.backup_history`).
- Backup:
  - Decides full vs incremental based on last FINISHED backup.
  - Incremental backs up only changed partitions since the last backup (via `information_schema.partitions`).
  - Each snapshot also includes `ops.backup_history` (for disaster recovery bootstrap).
  - Polls `SHOW BACKUP` until completion and records `FINISHED`/`FAILED` with `backup_timestamp`.
  - On any error, marks the running record as `FAILED` (no stuck RUNNING).
  - Stores partition metadata in `partitions_json` column.
- List: prints backup history.
- Restore:
  - Validates the target table does not exist.
  - Builds chain: latest full + incrementals strictly after that full, up to target timestamp.
  - Restores full first, then partitions for incrementals, using StarRocks `PROPERTIES` syntax.

## Requirements
- Python 3.9+
- mysql-connector-python, PyYAML, click, pytest (+ pytest-cov for coverage)

## Install
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Config
```yaml
host: localhost
port: 9030
user: root
password: secret
database: ops
tables:
  - db1.tableA
  - db2.tableB
repository: my_backup_repo
```

## Usage
```bash
# Help
starrocks-br --help

# Create metadata
starrocks-br init --config config.yaml

# Run backup (auto full/incremental)
starrocks-br backup --config config.yaml

# List history
starrocks-br list --config config.yaml

# Restore one table at timestamp
starrocks-br restore --config config.yaml --table db1.tableA --timestamp "2025-10-06 12:00:00"
```

## DR note (cold start)
Because every snapshot includes `ops.backup_history`, you can restore it first on a new cluster, then use `restore` to rebuild tables using the recovered history.

## Testing
```bash
pytest --cov=src/starrocks_br --cov-report=term-missing
```
Unit tests mock all DB interactions. Coverage emphasizes decision paths (backup/restore flows).

## Security
- Values (labels, timestamps) use parameterized queries.
- Identifiers (table/partition names) cannot be parameterized in SQL; validate or whitelist if taking user input.

## Roadmap / Checklist
- [✅] CLI scaffold and friendly error handling (invalid options)
- [✅] YAML config loader with validation
- [✅] DB wrapper (mysql-connector) with unit tests (mocks)
- [✅] init: create `ops.backup_history`
- [✅] backup: full/incremental decision, polling, final status, includes metadata table
- [✅] backup: mark FAILED on errors (no stuck RUNNING)
- [✅] list: print history
- [✅] restore: chain resolution (full + incrementals after full), ordered execution
- [✅] High decision coverage with pytest/pytest-cov
- [✅] Real partition detection via `information_schema.partitions`
- [✅] Partition metadata storage in `partitions_json` column
- [✅] StarRocks-compliant BACKUP/RESTORE SQL syntax with PROPERTIES
- [✅] Repository configuration parameter
- [ ] Identifier validation (table/partition whitelist)
- [ ] Optional single-connection context per command (`with Database(...) as db`)
- [ ] Integration/E2E tests with a real StarRocks environment
