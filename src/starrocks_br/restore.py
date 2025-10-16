import time
from typing import Dict
from . import history, concurrency

MAX_POLLS = 21600 # 6 hours

def build_partition_restore_command(
    database: str,
    table: str,
    partition: str,
    backup_label: str,
    repository: str,
) -> str:
    """Build RESTORE command for single partition recovery."""
    return f"""
    RESTORE SNAPSHOT {backup_label}
    FROM {repository}
    ON (TABLE {database}.{table} PARTITION ({partition}))"""


def build_table_restore_command(
    database: str,
    table: str,
    backup_label: str,
    repository: str,
) -> str:
    """Build RESTORE command for full table recovery."""
    return f"""
    RESTORE SNAPSHOT {backup_label}
    FROM {repository}
    ON (TABLE {database}.{table})"""


def build_database_restore_command(
    database: str,
    backup_label: str,
    repository: str,
) -> str:
    """Build RESTORE command for full database recovery."""
    return f"""
    RESTORE DATABASE {database}
    FROM {repository}
    SNAPSHOT {backup_label}"""


def poll_restore_status(db, label: str, max_polls: int = MAX_POLLS, poll_interval: float = 1.0) -> Dict[str, str]:
    """Poll restore status until completion or timeout.
    
    Returns dictionary with keys: state, label
    """
    query = f"SHOW RESTORE WHERE label = '{label}'"
    
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
            
            if state in ["FINISHED", "FAILED", "CANCELLED"]:
                return {"state": state, "label": label}
            
            time.sleep(poll_interval)
            
        except Exception:
            return {"state": "ERROR", "label": label}
    
    return {"state": "TIMEOUT", "label": label}


def execute_restore(
    db,
    restore_command: str,
    backup_label: str,
    restore_type: str,
    repository: str,
    max_polls: int = MAX_POLLS,
    poll_interval: float = 1.0,
    scope: str = "restore",
) -> Dict:
    """Execute a complete restore workflow: submit command and monitor progress.
    
    Returns dictionary with keys: success, final_status, error_message
    """
    try:
        db.execute(restore_command.strip())
    except Exception as e:
        return {
            "success": False,
            "final_status": None,
            "error_message": f"Failed to submit restore command: {str(e)}"
        }
    
    label = backup_label
    
    try:
        final_status = poll_restore_status(db, label, max_polls, poll_interval)
        
        success = final_status["state"] == "FINISHED"
        
        try:
            history.log_restore(
                db,
                {
                    "job_id": label,
                    "backup_label": backup_label,
                    "restore_type": restore_type,
                    "status": final_status["state"],
                    "repository": repository,
                    "started_at": None,
                    "finished_at": None,
                    "error_message": None if success else final_status["state"],
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
            "error_message": None if success else f"Restore failed with state: {final_status['state']}"
        }
        
    except Exception as e:
        return {
            "success": False,
            "final_status": None,
            "error_message": str(e)
        }
