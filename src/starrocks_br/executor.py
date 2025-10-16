import time
from typing import Dict, Optional
from . import history, concurrency

MAX_POLLS = 21600 # 6 hours

def submit_backup_command(db, backup_command: str) -> bool:
    """Submit a backup command to StarRocks.
    
    Returns True if successful, False if failed.
    """
    try:
        db.execute(backup_command.strip())
        return True
    except Exception:
        return False


def poll_backup_status(db, label: str, max_polls: int = MAX_POLLS, poll_interval: float = 1.0) -> Dict[str, str]:
    """Poll backup status until completion or timeout.
    
    Returns dictionary with keys: state, label
    """
    query = f"SHOW BACKUP WHERE label = '{label}'"
    
    for _ in range(max_polls):
        try:
            rows = db.query(query)
            
            if not rows:
                return {"state": "UNKNOWN", "label": label}
            
            result = rows[0]
            
            if isinstance(result, dict):
                state = result.get("state", "UNKNOWN")
            else:
                state = result[1] if len(result) > 1 else "UNKNOWN"
            
            if state in ["FINISHED", "FAILED", "UNKNOWN"]:
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
) -> Dict:
    """Execute a complete backup workflow: submit command and monitor progress.
    
    Returns dictionary with keys: success, final_status, error_message
    """
    label = _extract_label_from_command(backup_command)
    
    if not submit_backup_command(db, backup_command):
        return {
            "success": False,
            "final_status": None,
            "error_message": "Failed to submit backup command"
        }
    
    try:
        final_status = poll_backup_status(db, label, max_polls, poll_interval)
        
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
            "error_message": None if success else f"Backup failed with state: {final_status['state']}"
        }
        
    except Exception as e:
        return {
            "success": False,
            "final_status": None,
            "error_message": str(e)
        }


def _extract_label_from_command(backup_command: str) -> str:
    """Extract the snapshot label from a backup command.
    
    This is a simple parser for StarRocks backup commands.
    """
    lines = backup_command.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if line.startswith('BACKUP SNAPSHOT'):
            parts = line.split()
            if len(parts) >= 3:
                return parts[2]
        elif line.startswith('BACKUP DATABASE'):
            parts = line.split()
            for i, part in enumerate(parts):
                if part == 'SNAPSHOT' and i + 1 < len(parts):
                    return parts[i + 1]
    
    return "unknown_backup"
