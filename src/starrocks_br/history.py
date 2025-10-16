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


