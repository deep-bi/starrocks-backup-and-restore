from typing import Dict


def initialize_ops_schema(db) -> None:
    """Initialize the ops database and all required control tables."""
    db.execute("CREATE DATABASE IF NOT EXISTS ops")
    
    db.execute(get_table_inventory_schema())
    db.execute(get_backup_history_schema())
    db.execute(get_restore_history_schema())
    db.execute(get_run_status_schema())
    
    _populate_table_inventory(db)


def get_table_inventory_schema() -> str:
    """Get CREATE TABLE statement for table_inventory."""
    return """
    CREATE TABLE IF NOT EXISTS ops.table_inventory (
        database_name VARCHAR(255) NOT NULL,
        table_name VARCHAR(255) NOT NULL,
        table_type VARCHAR(50) NOT NULL,
        backup_eligible BOOLEAN DEFAULT TRUE,
        incremental_eligible BOOLEAN DEFAULT FALSE,
        weekly_eligible BOOLEAN DEFAULT FALSE,
        monthly_eligible BOOLEAN DEFAULT TRUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (database_name, table_name)
    )
    """


def get_backup_history_schema() -> str:
    """Get CREATE TABLE statement for backup_history."""
    return """
    CREATE TABLE IF NOT EXISTS ops.backup_history (
        job_id VARCHAR(255) NOT NULL PRIMARY KEY,
        label VARCHAR(255) NOT NULL,
        backup_type VARCHAR(50) NOT NULL,
        status VARCHAR(50) NOT NULL,
        repository VARCHAR(255) NOT NULL,
        bytes_backed_up BIGINT DEFAULT 0,
        objects_count INT DEFAULT 0,
        started_at DATETIME NOT NULL,
        finished_at DATETIME,
        error_message TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """


def get_restore_history_schema() -> str:
    """Get CREATE TABLE statement for restore_history."""
    return """
    CREATE TABLE IF NOT EXISTS ops.restore_history (
        job_id VARCHAR(255) NOT NULL PRIMARY KEY,
        backup_label VARCHAR(255) NOT NULL,
        restore_type VARCHAR(50) NOT NULL,
        status VARCHAR(50) NOT NULL,
        repository VARCHAR(255) NOT NULL,
        started_at DATETIME NOT NULL,
        finished_at DATETIME,
        error_message TEXT,
        verification_checksum VARCHAR(255),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """


def get_run_status_schema() -> str:
    """Get CREATE TABLE statement for run_status."""
    return """
    CREATE TABLE IF NOT EXISTS ops.run_status (
        scope VARCHAR(50) NOT NULL,
        label VARCHAR(255) NOT NULL,
        state VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
        started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        finished_at DATETIME,
        PRIMARY KEY (scope, label)
    )
    """


def _populate_table_inventory(db) -> None:
    """Populate table_inventory with sample data for common table types."""
    sample_data = [
        ("sales_db", "fact_sales", "fact", True, True, False, True),
        ("orders_db", "fact_orders", "fact", True, True, False, True),
        
        ("sales_db", "dim_customers", "dimension", True, False, True, True),
        ("sales_db", "dim_products", "dimension", True, False, True, True),
        ("orders_db", "dim_regions", "dimension", True, False, True, True),
        
        ("config_db", "ref_countries", "reference", True, False, False, True),
        ("config_db", "ref_currencies", "reference", True, False, False, True),
    ]
    
    for data in sample_data:
        db.execute("""
            INSERT INTO ops.table_inventory 
            (database_name, table_name, table_type, backup_eligible, incremental_eligible, weekly_eligible, monthly_eligible)
            VALUES ('%s', '%s', '%s', %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            table_type = VALUES(table_type),
            backup_eligible = VALUES(backup_eligible),
            incremental_eligible = VALUES(incremental_eligible),
            weekly_eligible = VALUES(weekly_eligible),
            monthly_eligible = VALUES(monthly_eligible)
        """ % data)
