# Copyright 2025 deep-bi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Literal

from . import exceptions, logger, utils


def reserve_job_slot(db, scope: str, label: str, ops_database: str = "ops") -> None:
    """Reserve a job slot in the run_status table to prevent overlapping jobs.

    We consider any row with state='ACTIVE' for the same scope as a conflict.
    However, we implement self-healing logic to automatically clean up stale locks.
    """
    active_jobs = _get_active_jobs_for_scope(db, scope, ops_database)

    if not active_jobs:
        _insert_new_job(db, scope, label, ops_database)
        return

    _handle_active_job_conflicts(db, scope, active_jobs, ops_database)

    _insert_new_job(db, scope, label, ops_database)


def _get_active_jobs_for_scope(
    db, scope: str, ops_database: str = "ops"
) -> list[tuple[str, str, str]]:
    """Get all active jobs for the given scope."""
    rows = db.query(
        f"SELECT scope, label, state FROM {ops_database}.run_status WHERE state = 'ACTIVE'"
    )
    return [row for row in rows if row[0] == scope]


def _handle_active_job_conflicts(
    db, scope: str, active_jobs: list[tuple[str, str, str]], ops_database: str = "ops"
) -> None:
    """Handle conflicts with active jobs, cleaning up stale ones where possible."""
    for active_scope, active_label, _ in active_jobs:
        if _can_heal_stale_job(active_scope, active_label, db, ops_database):
            _cleanup_stale_job(db, active_scope, active_label, ops_database)
            logger.success(f"Cleaned up stale backup job: {active_label}")
        else:
            _raise_concurrency_conflict(scope, active_jobs)


def _can_heal_stale_job(scope: str, label: str, db, ops_database: str = "ops") -> bool:
    """Check if a stale job can be healed (only for backup jobs)."""
    if scope != "backup":
        return False

    return _is_backup_job_stale(db, label, ops_database)


def _raise_concurrency_conflict(scope: str, active_jobs: list[tuple[str, str, str]]) -> None:
    """Raise a concurrency conflict error with helpful message."""
    raise exceptions.ConcurrencyConflictError(scope, active_jobs)


def _insert_new_job(db, scope: str, label: str, ops_database: str = "ops") -> None:
    """Insert a new active job record."""
    sql = f"""
        INSERT INTO {ops_database}.run_status (scope, label, state, started_at)
        VALUES ({utils.quote_value(scope)}, {utils.quote_value(label)}, 'ACTIVE', NOW())
    """
    db.execute(sql)


def _is_backup_job_stale(db, label: str, ops_database: str = "ops") -> bool:
    """Check if a backup job is stale by querying StarRocks SHOW BACKUP.

    Returns True if the job is stale (not actually running), False if it's still active.
    """
    try:
        user_databases = _get_user_databases(db, ops_database)

        for database_name in user_databases:
            job_status = _check_backup_job_in_database(db, database_name, label)

            if job_status is None:
                continue

            if job_status == "active":
                return False
            elif job_status == "stale":
                return True

        return True

    except Exception as e:
        logger.error(f"Error checking backup job status: {e}")
        return False


def _get_user_databases(db, ops_database: str = "ops") -> list[str]:
    """Get list of user databases (excluding system databases)."""
    system_databases = {"information_schema", "mysql", "sys", ops_database}

    databases = db.query("SHOW DATABASES")
    return [
        _extract_database_name(db_row)
        for db_row in databases
        if _extract_database_name(db_row) not in system_databases
    ]


def _extract_database_name(db_row) -> str:
    """Extract database name from database query result."""
    if isinstance(db_row, (list, tuple)):
        return db_row[0]
    return db_row.get("Database", "")


def _check_backup_job_in_database(db, database_name: str, label: str) -> str:
    """Check if backup job exists in specific database and return its status.

    Returns:
        'active' if job is still running
        'stale' if job is in terminal state
        None if job not found in this database
    """
    try:
        show_backup_query = f"SHOW BACKUP FROM {utils.quote_identifier(database_name)}"
        backup_rows = db.query(show_backup_query)

        if not backup_rows:
            return None

        result = backup_rows[0]
        snapshot_name, state = _extract_backup_info(result)

        if snapshot_name != label:
            return None

        if state in ["FINISHED", "CANCELLED", "FAILED"]:
            return "stale"
        else:
            return "active"

    except Exception:
        return None


def _extract_backup_info(result) -> tuple[str, str]:
    """Extract snapshot name and state from SHOW BACKUP result."""
    if isinstance(result, dict):
        snapshot_name = result.get("SnapshotName", "")
        state = result.get("State", "UNKNOWN")
    else:
        snapshot_name = result[1] if len(result) > 1 else ""
        state = result[3] if len(result) > 3 else "UNKNOWN"

    return snapshot_name, state


def _cleanup_stale_job(db, scope: str, label: str, ops_database: str = "ops") -> None:
    """Clean up a stale job by updating its state to CANCELLED."""
    sql = f"""
        UPDATE {ops_database}.run_status
        SET state='CANCELLED', finished_at=NOW()
        WHERE scope={utils.quote_value(scope)} AND label={utils.quote_value(label)} AND state='ACTIVE'
    """
    db.execute(sql)


def complete_job_slot(
    db,
    scope: str,
    label: str,
    final_state: Literal["FINISHED", "FAILED", "CANCELLED"],
    ops_database: str = "ops",
) -> None:
    """Complete job slot and persist final state.

    Simple approach: update the same row by scope/label.
    """
    sql = f"""
        UPDATE {ops_database}.run_status
        SET state={utils.quote_value(final_state)}, finished_at=NOW()
        WHERE scope={utils.quote_value(scope)} AND label={utils.quote_value(label)}
    """
    db.execute(sql)
