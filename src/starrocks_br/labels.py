from datetime import datetime
from typing import Literal


def determine_backup_label(
    db,
    backup_type: Literal["incremental", "full"],
    database_name: str,
    custom_name: str | None = None,
) -> str:
    """Determine a unique backup label for the given parameters.

    This is the single entry point for all backup label generation. It handles both
    custom names and auto-generated date-based labels, ensuring uniqueness by checking
    the ops.backup_history table.

    Args:
        db: Database connection
        backup_type: Type of backup (incremental, full)
        database_name: Name of the database being backed up
        custom_name: Optional custom name for the backup. If provided, this becomes
                    the base label. If None, generates a date-based label.

    Returns:
        Unique label string that doesn't conflict with existing backups
    """
    if custom_name:
        base_label = custom_name
    else:
        today = datetime.now().strftime("%Y%m%d")
        base_label = f"{database_name}_{today}_{backup_type}"

    query = """
    SELECT label
    FROM ops.backup_history
    WHERE label LIKE %s
    ORDER BY label
    """

    pattern = f"{base_label}%"

    try:
        rows = db.query(query, (pattern,))
        existing_labels = [row[0] for row in rows] if rows else []
    except Exception:
        existing_labels = []

    if base_label not in existing_labels:
        return base_label

    retry_count = 1
    while True:
        candidate_label = f"{base_label}_r{retry_count}"
        if candidate_label not in existing_labels:
            return candidate_label
        retry_count += 1
