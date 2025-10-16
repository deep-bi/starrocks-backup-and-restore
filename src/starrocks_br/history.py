from typing import Dict, Optional


def log_backup(db, entry: Dict[str, Optional[str]]) -> None:
    """Write a backup history entry to ops.backup_history.

    Expected keys in entry:
      - job_id (optional; auto-generated if missing)
      - label
      - backup_type (incremental|weekly|monthly)
      - status (FINISHED|FAILED|CANCELLED)
      - repository
      - started_at (YYYY-MM-DD HH:MM:SS)
      - finished_at (YYYY-MM-DD HH:MM:SS)
      - error_message (nullable)
    """
    label = entry.get("label", "")
    backup_type = entry.get("backup_type", "")
    status = entry.get("status", "")
    repository = entry.get("repository", "")
    started_at = entry.get("started_at", "NULL")
    finished_at = entry.get("finished_at", "NULL")
    error_message = entry.get("error_message")

    # Build SQL with simple escaping for single quotes
    def esc(val: Optional[str]) -> str:
        if val is None:
            return "NULL"
        return "'" + str(val).replace("'", "''") + "'"

    sql = f"""
    INSERT INTO ops.backup_history (
        label, backup_type, status, repository, started_at, finished_at, error_message
    ) VALUES (
        {esc(label)}, {esc(backup_type)}, {esc(status)}, {esc(repository)},
        {esc(started_at)}, {esc(finished_at)}, {esc(error_message)}
    )
    """
    db.execute(sql)


def log_restore(db, entry: Dict[str, Optional[str]]) -> None:
    """Write a restore history entry to ops.restore_history.

    Expected keys in entry:
      - job_id
      - backup_label
      - restore_type (partition|table|database)
      - status (FINISHED|FAILED|CANCELLED)
      - repository
      - started_at (YYYY-MM-DD HH:MM:SS)
      - finished_at (YYYY-MM-DD HH:MM:SS)
      - error_message (nullable)
      - verification_checksum (optional)
    """
    job_id = entry.get("job_id", "")
    backup_label = entry.get("backup_label", "")
    restore_type = entry.get("restore_type", "")
    status = entry.get("status", "")
    repository = entry.get("repository", "")
    started_at = entry.get("started_at", "NULL")
    finished_at = entry.get("finished_at", "NULL")
    error_message = entry.get("error_message")
    verification_checksum = entry.get("verification_checksum")

    def esc(val: Optional[str]) -> str:
        if val is None:
            return "NULL"
        return "'" + str(val).replace("'", "''") + "'"

    sql = f"""
    INSERT INTO ops.restore_history (
        job_id, backup_label, restore_type, status, repository, 
        started_at, finished_at, error_message, verification_checksum
    ) VALUES (
        {esc(job_id)}, {esc(backup_label)}, {esc(restore_type)}, {esc(status)}, 
        {esc(repository)}, {esc(started_at)}, {esc(finished_at)}, 
        {esc(error_message)}, {esc(verification_checksum)}
    )
    """
    db.execute(sql)


