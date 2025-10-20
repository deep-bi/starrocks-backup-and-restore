def initialize_ops_schema(db) -> None:
    """Initialize the ops database and all required control tables.
    
    Creates empty ops tables. Does NOT populate with sample data.
    Users must manually insert their table inventory records.
    """
    db.execute("CREATE DATABASE IF NOT EXISTS ops")
    
    db.execute(get_table_inventory_schema())
    db.execute(get_backup_history_schema())
    db.execute(get_restore_history_schema())
    db.execute(get_run_status_schema())


def ensure_ops_schema(db) -> bool:
    """Ensure ops schema exists, creating it if necessary.
    
    Returns True if schema was created, False if it already existed.
    This is called automatically before backup/restore operations.
    """
    try:
        result = db.query("SHOW DATABASES LIKE 'ops'")
        
        if not result:
            initialize_ops_schema(db)
            return True
        
        db.execute("USE ops")
        tables_result = db.query("SHOW TABLES")
        
        if not tables_result or len(tables_result) < 4:
            initialize_ops_schema(db)
            return True
        
        return False
        
    except Exception:
        initialize_ops_schema(db)
        return True


def get_table_inventory_schema() -> str:
    """Get CREATE TABLE statement for table_inventory."""
    return """
    CREATE TABLE IF NOT EXISTS ops.table_inventory (
        inventory_group STRING NOT NULL COMMENT "Group name for a set of tables",
        database_name STRING NOT NULL COMMENT "Database name",
        table_name STRING NOT NULL COMMENT "Table name, or '*' for all tables in database",
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    PRIMARY KEY (inventory_group, database_name, table_name)
    COMMENT "Inventory groups mapping to databases/tables (supports '*' wildcard)"
    """


def get_backup_history_schema() -> str:
    """Get CREATE TABLE statement for backup_history."""
    return """
    CREATE TABLE IF NOT EXISTS ops.backup_history (
        label STRING NOT NULL COMMENT "Unique backup snapshot label",
        backup_type STRING NOT NULL COMMENT "Type of backup: full or incremental",
        status STRING NOT NULL COMMENT "Final backup status: FINISHED, FAILED, CANCELLED, TIMEOUT",
        repository STRING NOT NULL COMMENT "Repository name where backup was stored",
        started_at DATETIME NOT NULL COMMENT "Backup start timestamp",
        finished_at DATETIME COMMENT "Backup completion timestamp",
        error_message STRING COMMENT "Error message if backup failed",
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT "History record creation timestamp"
    )
    PRIMARY KEY (label)
    COMMENT "History log of all backup operations"
    """


def get_restore_history_schema() -> str:
    """Get CREATE TABLE statement for restore_history."""
    return """
    CREATE TABLE IF NOT EXISTS ops.restore_history (
        job_id STRING NOT NULL COMMENT "Unique restore job identifier",
        backup_label STRING NOT NULL COMMENT "Source backup snapshot label",
        restore_type STRING NOT NULL COMMENT "Type of restore: partition, table, or database",
        status STRING NOT NULL COMMENT "Final restore status: FINISHED, FAILED, CANCELLED",
        repository STRING NOT NULL COMMENT "Repository name where backup was retrieved from",
        started_at DATETIME NOT NULL COMMENT "Restore start timestamp",
        finished_at DATETIME COMMENT "Restore completion timestamp",
        error_message STRING COMMENT "Error message if restore failed",
        verification_checksum STRING COMMENT "Checksum for data verification",
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT "History record creation timestamp"
    )
    PRIMARY KEY (job_id)
    COMMENT "History log of all restore operations"
    """


def get_run_status_schema() -> str:
    """Get CREATE TABLE statement for run_status."""
    return """
    CREATE TABLE IF NOT EXISTS ops.run_status (
        scope STRING NOT NULL COMMENT "Job scope: backup or restore",
        label STRING NOT NULL COMMENT "Job label or identifier",
        state STRING NOT NULL DEFAULT "ACTIVE" COMMENT "Job state: ACTIVE or COMPLETED",
        started_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT "Job start timestamp",
        finished_at DATETIME COMMENT "Job completion timestamp"
    )
    PRIMARY KEY (scope, label)
    COMMENT "Tracks active and recently completed jobs for concurrency control"
    """