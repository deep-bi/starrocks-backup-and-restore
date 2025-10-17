import time
from typing import Dict, Optional
from . import history, concurrency

MAX_POLLS = 21600 # 6 hours

def submit_backup_command(db, backup_command: str) -> tuple[bool, Optional[str]]:
    """Submit a backup command to StarRocks.
    
    Returns (success, error_message).
    """
    try:
        db.execute(backup_command.strip())
        return True, None
    except Exception as e:
        error_msg = f"Failed to submit backup command: {type(e).__name__}: {str(e)}"
        print(f"error: {error_msg}")
        print(f"backup_command: {backup_command}")
        return False, error_msg


def poll_backup_status(db, label: str, database: str, max_polls: int = MAX_POLLS, poll_interval: float = 1.0) -> Dict[str, str]:
    """Poll backup status until completion or timeout.
    
    Note: SHOW BACKUP only returns the LAST backup in a database.
    We verify that the SnapshotName matches our expected label.
    
    Important: If we see a different snapshot name, it means another backup
    operation overwrote ours and we've lost tracking (race condition).
    
    Args:
        db: Database connection
        label: Expected snapshot name (label) to monitor
        database: Database name where backup was submitted
        max_polls: Maximum number of polling attempts
        poll_interval: Seconds to wait between polls
    
    Returns dictionary with keys: state, label
    Possible states: FINISHED, CANCELLED, TIMEOUT, ERROR, LOST
    """
    query = f"SHOW BACKUP FROM {database}"
    first_poll = True
    last_state = None
    poll_count = 0
    
    for _ in range(max_polls):
        poll_count += 1
        try:
            rows = db.query(query)
            
            if not rows:
                time.sleep(poll_interval)
                continue
            
            result = rows[0]
            
            if isinstance(result, dict):
                snapshot_name = result.get("SnapshotName", "")
                state = result.get("State", "UNKNOWN")
            else:
                snapshot_name = result[1] if len(result) > 1 else ""
                state = result[3] if len(result) > 3 else "UNKNOWN"
            
            if snapshot_name != label:
                if first_poll:
                    first_poll = False
                    time.sleep(poll_interval)
                    continue
                else:
                    return {"state": "LOST", "label": label}
            
            first_poll = False
            
            if state != last_state or poll_count % 10 == 0:
                print(f"⏳ Backup status: {state} (poll {poll_count}/{max_polls})")
                last_state = state
            
            if state in ["FINISHED", "CANCELLED"]:
                return {"state": state, "label": label}
            
            time.sleep(poll_interval)
            
        except Exception:
            return {"state": "ERROR", "label": label}
    
    return {"state": "TIMEOUT", "label": label}


def execute_backup(
    db,
    backup_command: str,
    max_polls: int = MAX_POLLS,
    poll_interval: float = 1.0,
    *,
    repository: Optional[str] = None,
    backup_type: Optional[str] = None,
    scope: str = "backup",
    database: Optional[str] = None,
) -> Dict:
    """Execute a complete backup workflow: submit command and monitor progress.
    
    Args:
        db: Database connection
        backup_command: Backup SQL command to execute
        max_polls: Maximum polling attempts
        poll_interval: Seconds between polls
        repository: Repository name (for logging)
        backup_type: Type of backup (for logging)
        scope: Job scope (for concurrency control)
        database: Database name (required for SHOW BACKUP)
    
    Returns dictionary with keys: success, final_status, error_message
    """
    label = _extract_label_from_command(backup_command)
    
    if not database:
        database = _extract_database_from_command(backup_command)
    
    success, submit_error = submit_backup_command(db, backup_command)
    if not success:
        return {
            "success": False,
            "final_status": None,
            "error_message": submit_error or "Failed to submit backup command (unknown error)"
        }
    
    try:
        final_status = poll_backup_status(db, label, database, max_polls, poll_interval)
        
        success = final_status["state"] == "FINISHED"

        try:
            history.log_backup(
                db,
                {
                    "label": label,
                    "backup_type": backup_type or "unknown",
                    "status": final_status["state"],
                    "repository": repository or "unknown",
                    "started_at": None,
                    "finished_at": None,
                    "error_message": None if success else (final_status["state"] or ""),
                },
            )
        except Exception:
            pass

        try:
            concurrency.complete_job_slot(db, scope=scope, label=label, final_state=final_status["state"])
        except Exception:
            pass
        
        return {
            "success": success,
            "final_status": final_status,
            "error_message": None if success else _build_error_message(final_status, label, database)
        }
        
    except Exception as e:
        error_msg = f"Unexpected error during backup execution: {type(e).__name__}: {str(e)}"
        print(f"error: {error_msg}")
        return {
            "success": False,
            "final_status": {"state": "ERROR", "label": label},
            "error_message": error_msg
        }


def _build_error_message(final_status: Dict, label: str, database: str) -> str:
    """Build a descriptive error message based on backup final status."""
    state = final_status.get('state', 'UNKNOWN')
    
    if state == "LOST":
        return (
            f"Backup tracking lost for '{label}' in database '{database}'. "
            f"Another backup operation overwrote the last backup status visible in SHOW BACKUP. "
            f"This indicates a concurrency issue - only one backup per database should run at a time. "
            f"Recommendation: Use ops.run_status concurrency control to prevent simultaneous backups, "
            f"or verify if another tool/user is running backups on this database."
        )
    elif state == "CANCELLED":
        return (
            f"Backup '{label}' was cancelled by StarRocks. "
            f"Check StarRocks logs for the reason (common causes: insufficient resources, storage issues, or manual cancellation)."
        )
    elif state == "TIMEOUT":
        return (
            f"Backup '{label}' monitoring timed out after {MAX_POLLS} polls. "
            f"The backup may still be running in the background. "
            f"Check SHOW BACKUP FROM {database} manually to see current status."
        )
    elif state == "ERROR":
        return (
            f"Error occurred while monitoring backup '{label}' status. "
            f"The backup may have been submitted but monitoring failed. "
            f"Check SHOW BACKUP FROM {database} and StarRocks logs for details."
        )
    else:
        return f"Backup '{label}' failed with unexpected state: {state}"


def _extract_label_from_command(backup_command: str) -> str:
    """Extract the snapshot label from a backup command.
    
    This is a simple parser for StarRocks backup commands.
    Handles both formats:
    - BACKUP DATABASE db SNAPSHOT label TO repo
    - BACKUP SNAPSHOT label TO repo (legacy)
    """
    lines = backup_command.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if line.startswith('BACKUP DATABASE'):
            parts = line.split()
            for i, part in enumerate(parts):
                if part == 'SNAPSHOT' and i + 1 < len(parts):
                    return parts[i + 1]
        elif line.startswith('BACKUP SNAPSHOT'):
            # Legacy syntax
            parts = line.split()
            if len(parts) >= 3:
                return parts[2]
    
    return "unknown_backup"


def _extract_database_from_command(backup_command: str) -> str:
    """Extract the database name from a backup command.
    
    Parses: BACKUP DATABASE db_name SNAPSHOT label ...
    """
    lines = backup_command.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if line.startswith('BACKUP DATABASE'):
            parts = line.split()
            if len(parts) >= 3:
                return parts[2]
    
    return "unknown_database"
