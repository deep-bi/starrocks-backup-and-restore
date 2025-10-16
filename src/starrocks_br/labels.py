from typing import List, Optional


def generate_label(database: str, date: str, backup_type: str, existing_labels: Optional[List[str]] = None) -> str:
    """Generate a unique snapshot label for backup operations.
    
    Format: {database}_{yyyymmdd}_{backup_type}
    On collision, adds _r# suffix (e.g., _r1, _r2, etc.)
    
    Args:
        database: Database name
        date: Date in YYYY-MM-DD format
        backup_type: Type of backup (inc, weekly, monthly)
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
