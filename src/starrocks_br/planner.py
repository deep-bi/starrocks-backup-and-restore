import datetime
import hashlib

from starrocks_br import logger, timezone, utils


def find_latest_full_backup(db, database: str) -> dict[str, str] | None:
    """Find the latest successful full backup for a database.

    Args:
        db: Database connection
        database: Database name to search for

    Returns:
        Dictionary with keys: label, backup_type, finished_at, or None if no full backup found.
        The finished_at value is returned as a string in the cluster timezone format.
    """
    query = f"""
    SELECT label, backup_type, finished_at
    FROM ops.backup_history
    WHERE backup_type = 'full'
    AND status = 'FINISHED'
    AND label LIKE {utils.quote_value(f"{database}_%")}
    ORDER BY finished_at DESC
    LIMIT 1
    """

    rows = db.query(query)

    if not rows:
        return None

    row = rows[0]
    finished_at = row[2]

    if isinstance(finished_at, datetime.datetime):
        finished_at_normalized = timezone.normalize_datetime_to_tz(finished_at, db.timezone)
        finished_at = finished_at_normalized.strftime("%Y-%m-%d %H:%M:%S")
    elif not isinstance(finished_at, str):
        finished_at = str(finished_at)

    return {"label": row[0], "backup_type": row[1], "finished_at": finished_at}


def find_tables_by_group(db, group_name: str) -> list[dict[str, str]]:
    """Find tables belonging to a specific inventory group.

    Returns list of dictionaries with keys: database, table.
    Supports '*' table wildcard which signifies all tables in a database.
    """
    query = f"""
    SELECT database_name, table_name
    FROM ops.table_inventory
    WHERE inventory_group = {utils.quote_value(group_name)}
    ORDER BY database_name, table_name
    """
    rows = db.query(query)
    return [{"database": row[0], "table": row[1]} for row in rows]


def find_recent_partitions(
    db, database: str, baseline_backup_label: str | None = None, *, group_name: str
) -> list[dict[str, str]]:
    """Find partitions updated since baseline for tables in the given inventory group.

    Args:
        db: Database connection
        database: Database name (StarRocks database scope for backup)
        baseline_backup_label: Optional specific backup label to use as baseline.
        group_name: Inventory group whose tables will be considered

    Returns list of dictionaries with keys: database, table, partition_name.
    Only partitions of tables within the specified database are returned.
    """
    cluster_tz = db.timezone

    if baseline_backup_label:
        baseline_query = f"""
        SELECT finished_at
        FROM ops.backup_history
        WHERE label = {utils.quote_value(baseline_backup_label)}
        AND status = 'FINISHED'
        """
        baseline_rows = db.query(baseline_query)
        if not baseline_rows:
            raise ValueError(
                f"Baseline backup '{baseline_backup_label}' not found or not successful"
            )
        baseline_time_raw = baseline_rows[0][0]
    else:
        latest_backup = find_latest_full_backup(db, database)
        if not latest_backup:
            raise ValueError(
                f"No successful full backup found for database '{database}'. Run a full database backup first."
            )
        baseline_time_raw = latest_backup["finished_at"]

    if isinstance(baseline_time_raw, datetime.datetime):
        baseline_time_str = baseline_time_raw.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(baseline_time_raw, str):
        baseline_time_str = baseline_time_raw
    else:
        baseline_time_str = str(baseline_time_raw)

    baseline_dt = timezone.parse_datetime_with_tz(baseline_time_str, cluster_tz)

    group_tables = find_tables_by_group(db, group_name)

    if not group_tables:
        return []

    db_group_tables = [t for t in group_tables if t["database"] == database]

    if not db_group_tables:
        return []

    concrete_tables = []
    for table_entry in db_group_tables:
        if table_entry["table"] == "*":
            show_tables_query = (
                f"SHOW TABLES FROM {utils.quote_identifier(table_entry['database'])}"
            )
            tables_rows = db.query(show_tables_query)
            for row in tables_rows:
                concrete_tables.append({"database": table_entry["database"], "table": row[0]})
        else:
            concrete_tables.append(table_entry)

    recent_partitions = []
    for table_entry in concrete_tables:
        db_name = table_entry["database"]
        table_name = table_entry["table"]

        show_partitions_query = (
            f"SHOW PARTITIONS FROM {utils.build_qualified_table_name(db_name, table_name)}"
        )
        try:
            partition_rows = db.query(show_partitions_query)
        except Exception as e:
            logger.error(f"Error showing partitions for table {db_name}.{table_name}: {e}")
            continue

        for row in partition_rows:
            # FOR SHARED NOTHING CLUSTER:
            # PartitionId, PartitionName, VisibleVersion, VisibleVersionTime, VisibleVersionHash, State, PartitionKey, Range, DistributionKey, Buckets, ReplicationNum, StorageMedium, CooldownTime, LastConsistencyCheckTime, DataSize, StorageSize, IsInMemory, RowCount, DataVersion, VersionEpoch, VersionTxnType
            partition_name = row[1]
            visible_version_time = row[3]

            if isinstance(visible_version_time, datetime.datetime):
                visible_version_time_str = visible_version_time.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(visible_version_time, str):
                visible_version_time_str = visible_version_time
            else:
                visible_version_time_str = str(visible_version_time)

            visible_version_dt = timezone.parse_datetime_with_tz(
                visible_version_time_str, cluster_tz
            )

            if visible_version_dt > baseline_dt:
                recent_partitions.append(
                    {"database": db_name, "table": table_name, "partition_name": partition_name}
                )

    return recent_partitions


def build_incremental_backup_command(
    partitions: list[dict[str, str]], repository: str, label: str, database: str
) -> str:
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

    db_partitions = [p for p in partitions if p["database"] == database]

    if not db_partitions:
        return ""

    table_partitions = {}
    for partition in db_partitions:
        table_name = partition["table"]
        if table_name not in table_partitions:
            table_partitions[table_name] = []
        table_partitions[table_name].append(partition["partition_name"])

    on_clauses = []
    for table, parts in table_partitions.items():
        partitions_str = ", ".join(utils.quote_identifier(p) for p in parts)
        on_clauses.append(f"TABLE {utils.quote_identifier(table)} PARTITION ({partitions_str})")

    on_clause = ",\n    ".join(on_clauses)

    command = f"""BACKUP DATABASE {utils.quote_identifier(database)} SNAPSHOT {utils.quote_identifier(label)}
    TO {utils.quote_identifier(repository)}
    ON ({on_clause})"""

    return command


def build_full_backup_command(
    db, group_name: str, repository: str, label: str, database: str
) -> str:
    """Build BACKUP command for an inventory group.

    If the group contains '*' for any entry in the target database, generate a
    simple BACKUP DATABASE command. Otherwise, generate ON (TABLE ...) list for
    the specific tables within the database.
    """
    tables = find_tables_by_group(db, group_name)

    db_entries = [t for t in tables if t["database"] == database]
    if not db_entries:
        return ""

    if any(t["table"] == "*" for t in db_entries):
        return f"""BACKUP DATABASE {utils.quote_identifier(database)} SNAPSHOT {utils.quote_identifier(label)}
    TO {utils.quote_identifier(repository)}"""

    on_clauses = []
    for t in db_entries:
        on_clauses.append(f"TABLE {utils.quote_identifier(t['table'])}")
    on_clause = ",\n        ".join(on_clauses)
    return f"""BACKUP DATABASE {utils.quote_identifier(database)} SNAPSHOT {utils.quote_identifier(label)}
    TO {utils.quote_identifier(repository)}
    ON ({on_clause})"""


def record_backup_partitions(db, label: str, partitions: list[dict[str, str]]) -> None:
    """Record partition metadata for a backup in ops.backup_partitions table.

    Args:
        db: Database connection
        label: Backup label
        partitions: List of partitions with keys: database, table, partition_name
    """
    if not partitions:
        return

    for partition in partitions:
        composite_key = (
            f"{label}|{partition['database']}|{partition['table']}|{partition['partition_name']}"
        )
        key_hash = hashlib.md5(composite_key.encode("utf-8")).hexdigest()

        db.execute(f"""
            INSERT INTO ops.backup_partitions
            (key_hash, label, database_name, table_name, partition_name)
            VALUES ({utils.quote_value(key_hash)}, {utils.quote_value(label)}, {utils.quote_value(partition["database"])}, {utils.quote_value(partition["table"])}, {utils.quote_value(partition["partition_name"])})
        """)


def get_all_partitions_for_tables(
    db, database: str, tables: list[dict[str, str]]
) -> list[dict[str, str]]:
    """Get all existing partitions for the specified tables.

    Args:
        db: Database connection
        database: Database name
        tables: List of tables with keys: database, table

    Returns:
        List of partitions with keys: database, table, partition_name
    """
    if not tables:
        return []

    db_tables = [t for t in tables if t["database"] == database]
    if not db_tables:
        return []

    where_conditions = [f"DB_NAME = {utils.quote_value(database)}", "PARTITION_NAME IS NOT NULL"]

    table_conditions = []
    for table in db_tables:
        if table["table"] == "*":
            pass
        else:
            table_conditions.append(f"TABLE_NAME = {utils.quote_value(table['table'])}")

    if table_conditions:
        where_conditions.append("(" + " OR ".join(table_conditions) + ")")

    where_clause = " AND ".join(where_conditions)

    query = f"""
    SELECT DB_NAME, TABLE_NAME, PARTITION_NAME
    FROM information_schema.partitions_meta 
    WHERE {where_clause}
    ORDER BY TABLE_NAME, PARTITION_NAME
    """

    rows = db.query(query)

    return [{"database": row[0], "table": row[1], "partition_name": row[2]} for row in rows]
