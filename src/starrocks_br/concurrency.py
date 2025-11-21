from typing import Literal

from . import exceptions, logger, utils


def reserve_job_slot(db, scope: str, label: str) -> None:
    """Reserve a job slot in ops.run_status to prevent overlapping jobs.

    We consider any row with state='ACTIVE' for the same scope as a conflict.
    However, we implement self-healing logic to automatically clean up stale locks.
    """
    active_jobs = _get_active_jobs_for_scope(db, scope)

    if not active_jobs:
        _insert_new_job(db, scope, label)
        return

    _handle_active_job_conflicts(db, scope, active_jobs)

    _insert_new_job(db, scope, label)


def _get_active_jobs_for_scope(db, scope: str) -> list[tuple[str, str, str]]:
    """Get all active jobs for the given scope."""
    rows = db.query("SELECT scope, label, state FROM ops.run_status WHERE state = 'ACTIVE'")
    return [row for row in rows if row[0] == scope]


def _handle_active_job_conflicts(db, scope: str, active_jobs: list[tuple[str, str, str]]) -> None:
    """Handle conflicts with active jobs, cleaning up stale ones where possible."""
    for active_scope, active_label, _ in active_jobs:
        if _can_heal_stale_job(active_scope, active_label, db):
            _cleanup_stale_job(db, active_scope, active_label)
            logger.success(f"Cleaned up stale backup job: {active_label}")
        else:
            _raise_concurrency_conflict(scope, active_jobs)


def _can_heal_stale_job(scope: str, label: str, db) -> bool:
    """Check if a stale job can be healed (only for backup jobs)."""
    if scope != "backup":
        return False

    return _is_backup_job_stale(db, label)


def _raise_concurrency_conflict(scope: str, active_jobs: list[tuple[str, str, str]]) -> None:
    """Raise a concurrency conflict error with helpful message."""
    raise exceptions.ConcurrencyConflictError(scope, active_jobs)


def _insert_new_job(db, scope: str, label: str) -> None:
    """Insert a new active job record."""
    sql = f"""
        INSERT INTO ops.run_status (scope, label, state, started_at)
        VALUES ({utils.quote_value(scope)}, {utils.quote_value(label)}, 'ACTIVE', NOW())
    """
    db.execute(sql)


def _is_backup_job_stale(db, label: str) -> bool:
    """Check if a backup job is stale by querying StarRocks SHOW BACKUP.

    Returns True if the job is stale (not actually running), False if it's still active.
    """
    try:
        user_databases = _get_user_databases(db)

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


def _get_user_databases(db) -> list[str]:
    """Get list of user databases (excluding system databases)."""
    system_databases = {"information_schema", "mysql", "sys", "ops"}

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


def _cleanup_stale_job(db, scope: str, label: str) -> None:
    """Clean up a stale job by updating its state to CANCELLED."""
    sql = f"""
        UPDATE ops.run_status
        SET state='CANCELLED', finished_at=NOW()
        WHERE scope={utils.quote_value(scope)} AND label={utils.quote_value(label)} AND state='ACTIVE'
    """
    db.execute(sql)


def complete_job_slot(
    db, scope: str, label: str, final_state: Literal["FINISHED", "FAILED", "CANCELLED"]
) -> None:
    """Complete job slot and persist final state.

    Simple approach: update the same row by scope/label.
    """
    sql = f"""
        UPDATE ops.run_status
        SET state={utils.quote_value(final_state)}, finished_at=NOW()
        WHERE scope={utils.quote_value(scope)} AND label={utils.quote_value(label)}
    """
    db.execute(sql)
