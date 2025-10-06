from __future__ import annotations

import sys
import logging
from pathlib import Path
from typing import List, Optional

import click

from .config import load_config
from .db import Database
from .backup import run_backup


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@click.group(no_args_is_help=False)
@click.version_option(package_name="starrocks-bbr", prog_name="starrocks-bbr")
def cli() -> None:
    """StarRocks backup and restore CLI (MVP)."""


def _config_option(f):
    return click.option("--config", "config_path", type=click.Path(exists=True, path_type=Path), required=True)(f)


@cli.command()
@_config_option
def init(config_path: Path) -> None:
    """Create metadata table."""
    cfg = load_config(config_path)
    db = Database(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, database=cfg.database)

    create_db_sql = "CREATE DATABASE IF NOT EXISTS ops"
    create_table_sql = (
        "CREATE TABLE IF NOT EXISTS ops.backup_history ("
        " id BIGINT AUTO_INCREMENT PRIMARY KEY,"
        " backup_type VARCHAR(16) NOT NULL,"
        " status VARCHAR(16) NOT NULL,"
        " start_time DATETIME NOT NULL,"
        " end_time DATETIME NULL,"
        " snapshot_label VARCHAR(255) NOT NULL,"
        " backup_timestamp DATETIME NULL,"
        " database_name VARCHAR(128) NOT NULL,"
        " table_name VARCHAR(128) NULL,"
        " partitions_json TEXT NULL,"
        " error_message TEXT NULL"
        ") ENGINE=OLAP"
    )

    db.execute(create_db_sql)
    db.execute(create_table_sql)
    click.echo("init: metadata structures ensured")


@cli.command()
@_config_option
def backup(config_path: Path) -> None:
    """Run backup workflow automatically."""
    cfg = load_config(config_path)
    db = Database(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, database=cfg.database)
    run_backup(db, cfg.tables)
    click.echo("backup: completed")


@cli.command()
@_config_option
def list(config_path: Path) -> None:
    """Show backup history."""
    cfg = load_config(config_path)
    db = Database(host=cfg.host, port=cfg.port, user=cfg.user, password=cfg.password, database=cfg.database)
    rows = db.query(
        "SELECT id, backup_type, status, start_time, end_time, snapshot_label, backup_timestamp, database_name, table_name FROM ops.backup_history ORDER BY id"
    )

    headers = ["ID", "TYPE", "STATUS", "START", "END", "LABEL", "TS", "DB", "TABLE"]
    click.echo("\t".join(headers))
    for r in rows:
        click.echo("\t".join(str(x) if x is not None else "" for x in r))


@cli.command()
@_config_option
@click.option("--table", "table_name", required=True, type=str)
@click.option("--timestamp", "timestamp_str", required=True, type=str)
def restore(config_path: Path, table_name: str, timestamp_str: str) -> None:
    """Perform point-in-time recovery of a single table."""
    _ = load_config(config_path)
    click.echo(f"restore: not implemented yet for table={table_name} ts={timestamp_str}")


def main(argv: Optional[List[str]] = None) -> int:
    argv = [] if argv is None else argv
    try:
        if not argv:
            # Print help and exit 0 when no args are provided in library mode
            with click.Context(cli) as ctx:
                click.echo(cli.get_help(ctx))
            return 0
        cli(standalone_mode=False, args=argv)
        return 0
    except SystemExit as exc:
        return int(exc.code)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv[1:]))
