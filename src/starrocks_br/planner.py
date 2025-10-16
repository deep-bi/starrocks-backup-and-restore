from datetime import datetime, timedelta
from typing import List, Dict


def find_recent_partitions(db, days: int) -> List[Dict[str, str]]:
    """Find partitions updated in the last N days.
    
    Returns list of dictionaries with keys: database, table, partition_name.
    """
    threshold_date = datetime.now() - timedelta(days=days)
    threshold_str = threshold_date.strftime("%Y-%m-%d")
    
    query = """
    SELECT table_schema, table_name, partition_name, update_time
    FROM information_schema.partitions 
    WHERE partition_name IS NOT NULL 
    AND update_time >= '%s'
    ORDER BY update_time DESC
    """ % threshold_str
    
    rows = db.query(query)
    
    return [
        {
            "database": row[0],
            "table": row[1], 
            "partition_name": row[2]
        }
        for row in rows
    ]


def build_incremental_backup_command(partitions: List[Dict[str, str]], repository: str, label: str) -> str:
    """Build BACKUP command for incremental backup of specific partitions."""
    if not partitions:
        return ""
    
    table_partitions = {}
    for partition in partitions:
        full_table = f"{partition['database']}.{partition['table']}"
        if full_table not in table_partitions:
            table_partitions[full_table] = []
        table_partitions[full_table].append(partition['partition_name'])
    
    on_clauses = []
    for table, parts in table_partitions.items():
        partitions_str = ", ".join(parts)
        on_clauses.append(f"TABLE {table} PARTITION ({partitions_str})")
    
    on_clause = ",\n    ".join(on_clauses)
    
    command = f"""
    BACKUP SNAPSHOT {label}
    TO {repository}
    ON ({on_clause})"""
    
    return command


def build_monthly_backup_command(database: str, repository: str, label: str) -> str:
    """Build BACKUP command for monthly full database backup."""
    return f"""
    BACKUP DATABASE {database} SNAPSHOT {label}
    TO {repository}"""


def find_weekly_eligible_tables(db) -> List[Dict[str, str]]:
    """Find tables eligible for weekly backup from table_inventory.
    
    Returns list of dictionaries with keys: database, table.
    """
    query = """
    SELECT database_name, table_name
    FROM ops.table_inventory
    WHERE weekly_eligible = TRUE
    ORDER BY database_name, table_name
    """
    
    rows = db.query(query)
    
    return [
        {
            "database": row[0],
            "table": row[1]
        }
        for row in rows
    ]


def build_weekly_backup_command(tables: List[Dict[str, str]], repository: str, label: str) -> str:
    """Build BACKUP command for weekly backup of specific tables."""
    if not tables:
        return ""
    
    on_clauses = []
    for table in tables:
        full_table = f"{table['database']}.{table['table']}"
        on_clauses.append(f"TABLE {full_table}")
    
    on_clause = ",\n        ".join(on_clauses)
    
    command = f"""
    BACKUP SNAPSHOT {label}
    TO {repository}
    ON ({on_clause})"""
    
    return command
