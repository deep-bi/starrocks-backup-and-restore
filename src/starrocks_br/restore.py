from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple, Literal

from .db import Database

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RestoreStep:
    kind: Literal["full", "incremental"]
    snapshot_label: str
    backup_timestamp: str
    partitions: Optional[List[str]] = None


def table_exists(db: Database, table_name: str) -> bool:
    rows = db.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=%s",
        (table_name,)
    )
    return bool(rows)


def find_full_before(db: Database, target_ts: str) -> Optional[Tuple[str, str]]:
    rows = db.query(
        "SELECT DATE_FORMAT(backup_timestamp, '%Y-%m-%d %H:%i:%s'), snapshot_label"
        " FROM ops.backup_history WHERE status='FINISHED' AND backup_type='full' AND table_name IS NULL AND backup_timestamp <= %s"
        " ORDER BY backup_timestamp DESC LIMIT 1",
        (target_ts,),
    )
    if rows and rows[0][0]:
        return rows[0][0], rows[0][1]
    return None


def find_incrementals_before(db: Database, table_name: str, target_ts: str) -> List[Tuple[str, str]]:
    rows = db.query(
        "SELECT DATE_FORMAT(backup_timestamp, '%Y-%m-%d %H:%i:%s'), snapshot_label FROM ops.backup_history"
        " WHERE status='FINISHED' AND backup_type='incremental' AND table_name=%s AND backup_timestamp <= %s"
        " ORDER BY backup_timestamp",
        (table_name, target_ts),
    )
    return [(r[0], r[1]) for r in rows]


def get_partitions_for_incremental(db: Database, snapshot_label: str) -> List[str]:
    """Retrieve partition list from partitions_json column in backup_history."""
    rows = db.query(
        "SELECT partitions_json FROM ops.backup_history WHERE snapshot_label=%s",
        (snapshot_label,),
    )
    if not rows or not rows[0][0]:
        return []
    
    import json
    try:
        partitions_data = json.loads(rows[0][0])
        # Flatten all partitions from all tables
        all_partitions = []
        for table_partitions in partitions_data.values():
            all_partitions.extend(table_partitions)
        return sorted(all_partitions)
    except (json.JSONDecodeError, TypeError):
        return []


def build_restore_chain(db: Database, table_name: str, target_ts: str) -> List[RestoreStep]:
    logger.info(f"searching for full backup before {target_ts}")
    full = find_full_before(db, target_ts)
    if not full:
        raise RuntimeError(
            f"no full backup found before target timestamp '{target_ts}'\n"
            f"  help: run a full backup first with:\n"
            f"        starrocks-br backup --config config.yaml\n"
            f"  note: check available backups with:\n"
            f"        starrocks-br list --config config.yaml"
        )
    full_ts, full_label = full
    logger.info(f"found full backup: {full_label} at {full_ts}")
    steps: List[RestoreStep] = [RestoreStep(kind="full", snapshot_label=full_label, backup_timestamp=full_ts)]

    logger.info(f"searching for incremental backups for {table_name} after {full_ts}")
    incremental_count = 0
    for inc_ts, inc_label in find_incrementals_before(db, table_name, target_ts):
        if inc_ts <= full_ts:
            continue
        parts = get_partitions_for_incremental(db, inc_label)
        logger.info(f"found incremental backup: {inc_label} at {inc_ts} with {len(parts)} partitions")
        steps.append(
            RestoreStep(kind="incremental", snapshot_label=inc_label, backup_timestamp=inc_ts, partitions=parts)
        )
        incremental_count += 1
    
    logger.info(f"restore chain built: 1 full + {incremental_count} incremental backups")
    return steps


def execute_restore(db: Database, table_name: str, steps: List[RestoreStep], repository: str = "repo") -> None:
    """Execute restore commands with StarRocks syntax including PROPERTIES."""
    for i, step in enumerate(steps, 1):
        if step.kind == "full":
            logger.info(f"[{i}/{len(steps)}] restoring full backup: {step.snapshot_label}")
            sql = f'RESTORE DATABASE ops FROM {repository} PROPERTIES ("backup_timestamp" = "{step.backup_timestamp}")'
            db.execute(sql)
        else:
            if not step.partitions:
                logger.info(f"[{i}/{len(steps)}] skipping incremental {step.snapshot_label} (no partitions)")
                continue
            logger.info(f"[{i}/{len(steps)}] restoring incremental backup: {step.snapshot_label} ({len(step.partitions)} partitions)")
            parts = ", ".join(step.partitions)
            sql = f'RESTORE DATABASE ops ON ({table_name} PARTITION ({parts})) FROM {repository} PROPERTIES ("backup_timestamp" = "{step.backup_timestamp}")'
            db.execute(sql)


def run_restore(db: Database, table_name: str, target_ts: str, repository: str = "repo") -> None:
    logger.info(f"checking if target table '{table_name}' exists")
    if table_exists(db, table_name):
        raise RuntimeError(
            f"target table '{table_name}' already exists\n"
            f"  help: drop the table first or restore to a different table name\n"
            f"        DROP TABLE {table_name};"
        )
    steps = build_restore_chain(db, table_name, target_ts)
    execute_restore(db, table_name, steps, repository)
