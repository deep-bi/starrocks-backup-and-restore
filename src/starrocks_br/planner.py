from typing import List, Dict, Optional


def find_latest_full_backup(db, database: str) -> Optional[Dict[str, str]]:
    """Find the latest successful full backup for a database.
    
    Args:
        db: Database connection
        database: Database name to search for
        
    Returns:
        Dictionary with keys: label, backup_type, finished_at, or None if no full backup found
    """
    query = f"""
    SELECT label, backup_type, finished_at
    FROM ops.backup_history
    WHERE backup_type = 'full'
    AND status = 'FINISHED'
    AND label LIKE '{database}_%'
    ORDER BY finished_at DESC
    LIMIT 1
    """
    
    rows = db.query(query)
    
    if not rows:
        return None
    
    row = rows[0]
    return {
        "label": row[0],
        "backup_type": row[1],
        "finished_at": row[2]
    }


def find_tables_by_group(db, group_name: str) -> List[Dict[str, str]]:
    """Find tables belonging to a specific inventory group.
    
    Returns list of dictionaries with keys: database, table.
    Supports '*' table wildcard which signifies all tables in a database.
    """
    query = f"""
    SELECT database_name, table_name
    FROM ops.table_inventory
    WHERE inventory_group = '{group_name}'
    ORDER BY database_name, table_name
    """
    rows = db.query(query)
    return [
        {"database": row[0], "table": row[1]} for row in rows
    ]


def find_recent_partitions(db, database: str, baseline_backup_label: Optional[str] = None, *, group_name: str) -> List[Dict[str, str]]:
    """Find partitions updated since baseline for tables in the given inventory group.
    
    Args:
        db: Database connection
        database: Database name (StarRocks database scope for backup)
        baseline_backup_label: Optional specific backup label to use as baseline.
        group_name: Inventory group whose tables will be considered
    
    Returns list of dictionaries with keys: database, table, partition_name.
    Only partitions of tables within the specified database are returned.
    """
    if baseline_backup_label:
        baseline_query = f"""
        SELECT finished_at
        FROM ops.backup_history
        WHERE label = '{baseline_backup_label}'
        AND status = 'FINISHED'
        """
        baseline_rows = db.query(baseline_query)
        if not baseline_rows:
            raise ValueError(f"Baseline backup '{baseline_backup_label}' not found or not successful")
        baseline_time = baseline_rows[0][0]
    else:
        latest_backup = find_latest_full_backup(db, database)
        if not latest_backup:
            raise ValueError(f"No successful full backup found for database '{database}'. Run a full database backup first.")
        baseline_time = latest_backup['finished_at']
    
    if isinstance(baseline_time, str):
        threshold_str = baseline_time
    else:
        threshold_str = baseline_time.strftime("%Y-%m-%d %H:%M:%S")
    
    group_tables = find_tables_by_group(db, group_name)

    if not group_tables:
        return []

    db_group_tables = [t for t in group_tables if t['database'] == database]

    if not db_group_tables:
        return []
    
    table_conditions = []
    for table in db_group_tables:
        table_conditions.append(f"(DB_NAME = '{table['database']}' AND TABLE_NAME = '{table['table']}')")
    
    table_filter = " AND (" + " OR ".join(table_conditions) + ")"
    
    query = f"""
    SELECT DB_NAME, TABLE_NAME, PARTITION_NAME, VISIBLE_VERSION_TIME
    FROM information_schema.partitions_meta 
    WHERE PARTITION_NAME IS NOT NULL 
    AND VISIBLE_VERSION_TIME > '{threshold_str}'
    {table_filter}
    ORDER BY VISIBLE_VERSION_TIME DESC
    """
    
    rows = db.query(query)
    
    return [
        {
            "database": row[0],
            "table": row[1], 
            "partition_name": row[2]
        }
        for row in rows
    ]


def build_incremental_backup_command(partitions: List[Dict[str, str]], repository: str, label: str, database: str) -> str:
    """Build BACKUP command for incremental backup of specific partitions.
    
    Args:
        partitions: List of partitions to backup
        repository: Repository name
        label: Backup label
        database: Database name (StarRocks requires BACKUP to be database-specific)
    
    Note: Filters partitions to only include those from the specified database.
    """
    if not partitions:
        return ""
    
    db_partitions = [p for p in partitions if p['database'] == database]
    
    if not db_partitions:
        return ""
    
    table_partitions = {}
    for partition in db_partitions:
        table_name = partition['table']
        if table_name not in table_partitions:
            table_partitions[table_name] = []
        table_partitions[table_name].append(partition['partition_name'])
    
    on_clauses = []
    for table, parts in table_partitions.items():
        partitions_str = ", ".join(parts)
        on_clauses.append(f"TABLE {table} PARTITION ({partitions_str})")
    
    on_clause = ",\n    ".join(on_clauses)
    
    command = f"""BACKUP DATABASE {database} SNAPSHOT {label}
    TO {repository}
    ON ({on_clause})"""
    
    return command


def build_full_backup_command(db, group_name: str, repository: str, label: str, database: str) -> str:
    """Build BACKUP command for an inventory group.
    
    If the group contains '*' for any entry in the target database, generate a
    simple BACKUP DATABASE command. Otherwise, generate ON (TABLE ...) list for
    the specific tables within the database.
    """
    tables = find_tables_by_group(db, group_name)

    db_entries = [t for t in tables if t['database'] == database]
    if not db_entries:
        return ""

    if any(t['table'] == '*' for t in db_entries):
        return f"""BACKUP DATABASE {database} SNAPSHOT {label}
    TO {repository}"""

    on_clauses = []
    for t in db_entries:
        on_clauses.append(f"TABLE {t['table']}")
    on_clause = ",\n        ".join(on_clauses)
    return f"""BACKUP DATABASE {database} SNAPSHOT {label}
    TO {repository}
    ON ({on_clause})"""