from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from .db import Database


@dataclass(frozen=True)
class RestoreStep:
    kind: str  # 'full' or 'incremental'
    snapshot_label: str
    backup_timestamp: str
    partitions: Optional[List[str]] = None


def table_exists(db: Database, table_name: str) -> bool:
    rows = db.query("SELECT table_name FROM information_schema.tables WHERE table_name=%s", (table_name,))
    return bool(rows)


def find_full_before(db: Database, table_name: str, target_ts: str) -> Optional[Tuple[str, str]]:
    rows = db.query(
        "SELECT DATE_FORMAT(MAX(backup_timestamp), '%Y-%m-%d %H:%i:%s'), MAX(snapshot_label)"
        " FROM ops.backup_history WHERE status='FINISHED' AND backup_type='full' AND table_name IS NULL AND backup_timestamp <= %s",
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
    rows = db.query(
        "SELECT partition_name FROM ops.incremental_partitions WHERE snapshot_label=%s ORDER BY partition_name",
        (snapshot_label,),
    )
    return [r[0] for r in rows]


def build_restore_chain(db: Database, table_name: str, target_ts: str) -> List[RestoreStep]:
    full = find_full_before(db, table_name, target_ts)
    if not full:
        raise RuntimeError("No full backup found before target timestamp")
    full_ts, full_label = full
    steps: List[RestoreStep] = [RestoreStep(kind="full", snapshot_label=full_label, backup_timestamp=full_ts)]

    for inc_ts, inc_label in find_incrementals_before(db, table_name, target_ts):
        parts = get_partitions_for_incremental(db, inc_label)
        steps.append(
            RestoreStep(kind="incremental", snapshot_label=inc_label, backup_timestamp=inc_ts, partitions=parts)
        )

    return steps


def execute_restore(db: Database, table_name: str, steps: List[RestoreStep]) -> None:
    for step in steps:
        if step.kind == "full":
            sql = f"RESTORE TABLE {table_name} FROM {step.snapshot_label} AT '{step.backup_timestamp}'"
            db.execute(sql)
        else:
            parts = ", ".join(step.partitions or [])
            sql = f"RESTORE PARTITIONS ({parts}) FOR TABLE {table_name} FROM {step.snapshot_label} AT '{step.backup_timestamp}'"
            db.execute(sql)


def run_restore(db: Database, table_name: str, target_ts: str) -> None:
    if table_exists(db, table_name):
        raise RuntimeError(f"Target table '{table_name}' already exists")
    steps = build_restore_chain(db, table_name, target_ts)
    execute_restore(db, table_name, steps)
