from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from .db import Database

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 0.1


@dataclass(frozen=True)
class BackupPlan:
    backup_type: str  # 'full' | 'incremental'
    tables: List[str]
    partitions_by_table: dict[str, List[str]]  # only for incremental; empty for full


def get_last_successful_backup(db: Database) -> Optional[str]:
    rows = db.query(
        "SELECT DATE_FORMAT(MAX(backup_timestamp), '%Y-%m-%d %H:%i:%s') FROM ops.backup_history WHERE status='FINISHED'"
    )
    if not rows:
        return None
    ts = rows[0][0]
    return ts if ts else None


def get_changed_partitions_since(db: Database, table: str, since_ts: str) -> List[str]:
    """Query information_schema.partitions to find partitions modified since given timestamp.
    
    Note: UPDATE_TIME may be stale for external catalogs (e.g., Hive) and is subject to 
    session time_zone. For real-time accuracy, refresh metadata or query 
    information_schema.partitions_meta (or the source metastore) instead.
    """
    rows = db.query(
        "SELECT PARTITION_NAME FROM information_schema.partitions "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND UPDATE_TIME > %s",
        (table, since_ts)
    )
    return [row[0] for row in rows if row[0] is not None]


def decide_backup_plan(db: Database, tables: List[str]) -> BackupPlan:
    logger.info("determining backup strategy...")
    last_ts = get_last_successful_backup(db)
    if not last_ts:
        logger.info("no previous successful backup found, performing FULL backup")
        return BackupPlan(backup_type="full", tables=tables, partitions_by_table={})

    logger.info(f"last successful backup: {last_ts}")
    logger.info("checking for changed partitions since last backup...")
    
    partitions_by_table: dict[str, List[str]] = {}
    for t in tables:
        parts = get_changed_partitions_since(db, t, last_ts)
        partitions_by_table[t] = parts
        if parts:
            logger.info(f"  {t}: {len(parts)} changed partitions")
        else:
            logger.info(f"  {t}: no changes detected")

    has_any_changes = any(partitions_by_table.get(t) for t in tables)
    if not has_any_changes:
        logger.info("no changes detected, performing lightweight INCREMENTAL snapshot")
        return BackupPlan(backup_type="incremental", tables=[], partitions_by_table={})

    total_partitions = sum(len(p) for p in partitions_by_table.values())
    logger.info(f"performing INCREMENTAL backup with {total_partitions} changed partitions")
    return BackupPlan(backup_type="incremental", tables=tables, partitions_by_table=partitions_by_table)


def insert_running_history(db: Database, plan: BackupPlan, snapshot_label: str) -> None:
    """Insert backup record with partitions_json for incremental backups."""
    logger.info(f"recording backup job in ops.backup_history (label: {snapshot_label})")
    partitions_json = None
    if plan.backup_type == "incremental" and plan.partitions_by_table:
        partitions_data = {table: parts for table, parts in plan.partitions_by_table.items() if parts}
        if partitions_data:
            partitions_json = json.dumps(partitions_data)
            logger.info(f"storing partition metadata: {len(partitions_data)} tables")
    
    db.execute(
        "INSERT INTO ops.backup_history (backup_type, status, start_time, snapshot_label, database_name, partitions_json) "
        "VALUES (%s, 'RUNNING', NOW(), %s, %s, %s)",
        (plan.backup_type, snapshot_label, "ops", partitions_json),
    )


def issue_backup_commands(db: Database, plan: BackupPlan, repository: str = "repo") -> None:
    """Issue StarRocks BACKUP commands with proper syntax."""
    if plan.backup_type == "full":
        tables_list = ", ".join(plan.tables + ["ops.backup_history"])
        logger.info(f"issuing FULL backup command to repository '{repository}'")
        logger.info(f"  tables: {', '.join(plan.tables + ['ops.backup_history'])}")
        sql = f"BACKUP DATABASE ops ON ({tables_list}) TO {repository}"
        db.execute(sql)
    else:
        logger.info(f"issuing INCREMENTAL backup commands to repository '{repository}'")
        for table in plan.tables:
            partitions = plan.partitions_by_table.get(table) or []
            if partitions:
                parts = ", ".join(partitions)
                logger.info(f"  {table}: backing up {len(partitions)} partitions")
                objects = f"{table} PARTITION ({parts}), ops.backup_history"
            else:
                logger.info(f"  {table}: backing up entire table (no partition filter)")
                objects = f"{table}, ops.backup_history"
            sql = f"BACKUP DATABASE ops ON ({objects}) TO {repository}"
            db.execute(sql)


def poll_backup_until_done(db: Database) -> Tuple[str, Optional[str]]:
    logger.info("polling backup job status...")
    poll_count = 0
    while True:
        rows = db.query("SHOW BACKUP")
        if not rows:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        status, ts = rows[0][0], rows[0][1]
        poll_count += 1
        if poll_count % 10 == 0:
            logger.info(f"  job status: {status} (polling...)")
        if status in ("FINISHED", "FAILED"):
            logger.info(f"  job status: {status}")
            return status, ts
        time.sleep(POLL_INTERVAL_SECONDS)


def update_history_final(db: Database, status: str, backup_timestamp: Optional[str]) -> None:
    if status == "FINISHED":
        logger.info(f"updating backup history with status FINISHED (timestamp: {backup_timestamp})")
        db.execute(
            "UPDATE ops.backup_history SET status='FINISHED', end_time=NOW(), backup_timestamp=%s WHERE status='RUNNING' ORDER BY id DESC LIMIT 1",
            (backup_timestamp,),
        )
    else:
        logger.error(f"backup job FAILED, updating history")
        db.execute(
            "UPDATE ops.backup_history SET status='FAILED', end_time=NOW() WHERE status='RUNNING' ORDER BY id DESC LIMIT 1"
        )


def run_backup(db: Database, tables: List[str], repository: str = "repo") -> None:
    plan = decide_backup_plan(db, tables)
    snapshot_label = f"bbr_{int(time.time())}"
    logger.info(f"snapshot label: {snapshot_label}")
    
    insert_running_history(db, plan, snapshot_label)
    try:
        issue_backup_commands(db, plan, repository)
        status, ts = poll_backup_until_done(db)
        update_history_final(db, status, ts)
        if status == "FAILED":
            raise RuntimeError(
                f"backup job failed in StarRocks\n"
                f"  help: check StarRocks logs for details\n"
                f"  note: run 'SHOW BACKUP' in StarRocks to see error details"
            )
    except Exception as e:
        logger.error(f"backup workflow failed: {e}")
        update_history_final(db, "FAILED", None)
        raise
