from typing import List, Optional, Literal
import re


def generate_label(database: str, date: str, backup_type: Literal['incremental', 'full'], existing_labels: Optional[List[str]] = None) -> str:
    """Generate a unique snapshot label for backup operations.
    
    Format: {database}_{yyyymmdd}_{backup_type}
    On collision, adds _r# suffix (e.g., _r1, _r2, etc.)
    
    Args:
        database: Database name
        date: Date in YYYY-MM-DD format
        backup_type: Type of backup (incremental, full)
        existing_labels: List of existing labels to avoid collisions with
        
    Returns:
        Unique label string
    """
    if existing_labels is None:
        existing_labels = []
    
    date_formatted = date.replace("-", "")
    
    base_label = f"{database}_{date_formatted}_{backup_type}"
    
    label = base_label
    retry_count = 0
    
    while label in existing_labels:
        retry_count += 1
        label = f"{base_label}_r{retry_count}"
    
    return label


def resolve_label(db, provided_name: Optional[str]) -> Optional[str]:
    """Resolve a user-provided logical label name with optional versioning.

    Versioning placeholder: '-v#r'.
    Example: 'my-backup-v#r' -> queries existing labels like 'my-backup-v%'
    and assigns the next integer version suffix, starting at 1.

    If provided_name is None or empty, returns None.
    If provided_name does not contain the placeholder, returns provided_name unchanged.
    """
    if not provided_name:
        return None

    placeholder = "-v#r"
    if placeholder not in provided_name:
        return provided_name

    prefix = provided_name.replace(placeholder, "-v")

    like_pattern = prefix + "%"
    query = f"""
    SELECT label
    FROM ops.backup_history
    WHERE label LIKE '{like_pattern}'
    """

    try:
        rows = db.query(query)
    except Exception:
        rows = []

    max_version = 0
    suffix_regex = re.compile(re.escape(prefix) + r"(\d+)$")
    for row in rows or []:
        label_value = row[0]
        match = suffix_regex.match(label_value)
        if match:
            try:
                version_num = int(match.group(1))
                if version_num > max_version:
                    max_version = version_num
            except ValueError:
                continue

    next_version = max_version + 1
    return f"{prefix}{next_version}"
