# Copyright 2025 deep-bi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from . import logger


def initialize_ops_schema(
    db, ops_database: str = "ops", table_inventory_entries: list[tuple[str, str, str]] | None = None
) -> None:
    """Initialize the ops database and all required control tables.

    Creates empty ops tables. Optionally populates table_inventory from entries.

    Args:
        db: Database connection
        ops_database: Name of the ops database (defaults to "ops")
        table_inventory_entries: Optional list of (group, database, table) tuples to bootstrap
    """

    logger.info(f"Creating {ops_database} database...")
    db.execute(f"CREATE DATABASE IF NOT EXISTS {ops_database}")
    logger.success(f"{ops_database} database created")

    logger.info(f"Creating {ops_database}.table_inventory...")
    db.execute(get_table_inventory_schema(ops_database=ops_database))
    logger.success(f"{ops_database}.table_inventory created")

    logger.info(f"Creating {ops_database}.backup_history...")
    db.execute(get_backup_history_schema(ops_database=ops_database))
    logger.success(f"{ops_database}.backup_history created")

    logger.info(f"Creating {ops_database}.restore_history...")
    db.execute(get_restore_history_schema(ops_database=ops_database))
    logger.success(f"{ops_database}.restore_history created")

    logger.info(f"Creating {ops_database}.run_status...")
    db.execute(get_run_status_schema(ops_database=ops_database))
    logger.success(f"{ops_database}.run_status created")

    logger.info(f"Creating {ops_database}.backup_partitions...")
    db.execute(get_backup_partitions_schema(ops_database=ops_database))
    logger.success(f"{ops_database}.backup_partitions created")

    if table_inventory_entries:
        logger.info(f"Bootstrapping {ops_database}.table_inventory from configuration...")
        bootstrap_table_inventory(db, table_inventory_entries, ops_database=ops_database)
        logger.success(
            f"{ops_database}.table_inventory bootstrapped with {len(table_inventory_entries)} entries"
        )

    logger.info("")
    logger.success("Schema initialized successfully!")


def ensure_ops_schema(db, ops_database: str = "ops") -> bool:
    """Ensure ops schema exists, creating it if necessary.

    Returns True if schema was created, False if it already existed.
    This is called automatically before backup/restore operations.
    """
    try:
        result = db.query(f"SHOW DATABASES LIKE '{ops_database}'")

        if not result:
            initialize_ops_schema(db, ops_database=ops_database)
            return True

        db.execute(f"USE {ops_database}")
        tables_result = db.query("SHOW TABLES")

        if not tables_result or len(tables_result) < 5:
            initialize_ops_schema(db, ops_database=ops_database)
            return True

        return False

    except Exception:
        initialize_ops_schema(db, ops_database=ops_database)
        return True


def bootstrap_table_inventory(
    db, entries: list[tuple[str, str, str]], ops_database: str = "ops"
) -> None:
    """Bootstrap table_inventory table with entries from configuration.

    Args:
        db: Database connection
        entries: List of (group, database, table) tuples
        ops_database: Name of the ops database (defaults to "ops")
    """
    if not entries:
        return

    for group, database, table in entries:
        sql = f"""
            INSERT INTO {ops_database}.table_inventory
            (inventory_group, database_name, table_name)
            VALUES ('{group}', '{database}', '{table}')
        """
        db.execute(sql)


def get_table_inventory_schema(ops_database: str = "ops") -> str:
    """Get CREATE TABLE statement for table_inventory."""
    return f"""
    CREATE TABLE IF NOT EXISTS {ops_database}.table_inventory (
        inventory_group STRING NOT NULL COMMENT "Group name for a set of tables",
        database_name STRING NOT NULL COMMENT "Database name",
        table_name STRING NOT NULL COMMENT "Table name, or '*' for all tables in database",
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    UNIQUE KEY (inventory_group, database_name, table_name)
    COMMENT "Inventory groups mapping to databases/tables (supports '*' wildcard)"
    DISTRIBUTED BY HASH(inventory_group)
    """


def get_backup_history_schema(ops_database: str = "ops") -> str:
    """Get CREATE TABLE statement for backup_history."""
    return f"""
    CREATE TABLE IF NOT EXISTS {ops_database}.backup_history (
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


def get_restore_history_schema(ops_database: str = "ops") -> str:
    """Get CREATE TABLE statement for restore_history."""
    return f"""
    CREATE TABLE IF NOT EXISTS {ops_database}.restore_history (
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


def get_run_status_schema(ops_database: str = "ops") -> str:
    """Get CREATE TABLE statement for run_status."""
    return f"""
    CREATE TABLE IF NOT EXISTS {ops_database}.run_status (
        scope STRING NOT NULL COMMENT "Job scope: backup or restore",
        label STRING NOT NULL COMMENT "Job label or identifier",
        state STRING NOT NULL DEFAULT "ACTIVE" COMMENT "Job state: ACTIVE, FINISHED, FAILED, or CANCELLED",
        started_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT "Job start timestamp",
        finished_at DATETIME COMMENT "Job completion timestamp"
    )
    PRIMARY KEY (scope, label)
    COMMENT "Tracks active and recently completed jobs for concurrency control"
    """


def get_backup_partitions_schema(ops_database: str = "ops") -> str:
    """Get CREATE TABLE statement for backup_partitions."""
    return f"""
    CREATE TABLE IF NOT EXISTS {ops_database}.backup_partitions (
        key_hash STRING NOT NULL COMMENT "MD5 hash of composite key (label, database_name, table_name, partition_name)",
        label STRING NOT NULL COMMENT "The backup label this partition belongs to. FK to {ops_database}.backup_history.label.",
        database_name STRING NOT NULL COMMENT "The name of the database the partition belongs to.",
        table_name STRING NOT NULL COMMENT "The name of the table the partition belongs to.",
        partition_name STRING NOT NULL COMMENT "The name of the specific partition.",
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT "Timestamp when this record was created."
    )
    PRIMARY KEY (key_hash)
    COMMENT "Tracks every partition included in a backup snapshot."
    DISTRIBUTED BY HASH(key_hash)
    """
