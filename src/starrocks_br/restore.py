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

import datetime
import time

from . import concurrency, exceptions, history, logger, timezone, utils

MAX_POLLS = 86400  # 1 day


def _calculate_next_interval(current_interval: float, max_interval: float) -> float:
    """Calculate the next polling interval using exponential backoff.

    Args:
        current_interval: Current polling interval in seconds
        max_interval: Maximum allowed interval in seconds

    Returns:
        Next interval (min of doubled current interval and max_interval)
    """
    return min(current_interval * 2, max_interval)


def get_snapshot_timestamp(db, repo_name: str, snapshot_name: str) -> str:
    """Get the backup timestamp for a specific snapshot from the repository.

    Args:
        db: Database connection
        repo_name: Repository name
        snapshot_name: Snapshot name to look up

    Returns:
        The backup timestamp string

    Raises:
        ValueError: If snapshot is not found in the repository
    """
    query = f"SHOW SNAPSHOT ON {utils.quote_identifier(repo_name)} WHERE Snapshot = {utils.quote_value(snapshot_name)}"

    rows = db.query(query)
    if not rows:
        raise exceptions.SnapshotNotFoundError(snapshot_name, repo_name)

    # The result should be a single row with columns: Snapshot, Timestamp, Status
    result = rows[0]

    if isinstance(result, dict):
        timestamp = result.get("Timestamp")
    else:
        timestamp = result[1] if len(result) > 1 else None

    if not timestamp:
        raise ValueError(f"Could not extract timestamp for snapshot '{snapshot_name}'")

    return timestamp


def build_partition_restore_command(
    database: str,
    table: str,
    partition: str,
    backup_label: str,
    repository: str,
    backup_timestamp: str,
) -> str:
    """Build RESTORE command for single partition recovery."""
    return f"""RESTORE SNAPSHOT {utils.quote_identifier(backup_label)}
    FROM {utils.quote_identifier(repository)}
    DATABASE {utils.quote_identifier(database)}
    ON (TABLE {utils.quote_identifier(table)} PARTITION ({utils.quote_identifier(partition)}))
    PROPERTIES ("backup_timestamp" = "{backup_timestamp}")"""


def build_table_restore_command(
    database: str,
    table: str,
    backup_label: str,
    repository: str,
    backup_timestamp: str,
) -> str:
    """Build RESTORE command for full table recovery."""
    return f"""RESTORE SNAPSHOT {utils.quote_identifier(backup_label)}
    FROM {utils.quote_identifier(repository)}
    DATABASE {utils.quote_identifier(database)}
    ON (TABLE {utils.quote_identifier(table)})
    PROPERTIES ("backup_timestamp" = "{backup_timestamp}")"""


def build_database_restore_command(
    database: str,
    backup_label: str,
    repository: str,
    backup_timestamp: str,
) -> str:
    """Build RESTORE command for full database recovery."""
    return f"""RESTORE SNAPSHOT {utils.quote_identifier(backup_label)}
    FROM {utils.quote_identifier(repository)}
    DATABASE {utils.quote_identifier(database)}
    PROPERTIES ("backup_timestamp" = "{backup_timestamp}")"""


def poll_restore_status(
    db,
    label: str,
    database: str,
    max_polls: int = MAX_POLLS,
    poll_interval: float = 1.0,
    max_poll_interval: float = 60.0,
) -> dict[str, str]:
    """Poll restore status until completion or timeout.

    Note: SHOW RESTORE only returns the LAST restore in a database.
    We verify that the Label matches our expected label.

    Important: If we see a different label, it means another restore
    operation overwrote ours and we've lost tracking (race condition).

    Args:
        db: Database connection
        label: Expected snapshot label to monitor
        database: Database name where restore was submitted
        max_polls: Maximum number of polling attempts
        poll_interval: Initial seconds to wait between polls (exponentially increases)
        max_poll_interval: Maximum interval between polls (default 60 seconds)

    Returns dictionary with keys: state, label
    Possible states: FINISHED, CANCELLED, TIMEOUT, ERROR, LOST
    """
    query = f"SHOW RESTORE FROM {utils.quote_identifier(database)}"
    first_poll = True
    last_state = None
    poll_count = 0
    current_interval = poll_interval

    for _ in range(max_polls):
        poll_count += 1
        try:
            rows = db.query(query)

            if not rows:
                time.sleep(current_interval)
                current_interval = _calculate_next_interval(current_interval, max_poll_interval)
                continue

            result = rows[0]

            if isinstance(result, dict):
                snapshot_label = result.get("Label", "")
                state = result.get("State", "UNKNOWN")
            else:
                # Tuple format: JobId, Label, Timestamp, DbName, State, ...
                snapshot_label = result[1] if len(result) > 1 else ""
                state = result[4] if len(result) > 4 else "UNKNOWN"

            if snapshot_label != label and snapshot_label:
                if first_poll:
                    first_poll = False
                    time.sleep(current_interval)
                    current_interval = _calculate_next_interval(current_interval, max_poll_interval)
                    continue
                else:
                    return {"state": "LOST", "label": label}

            first_poll = False

            if state != last_state or poll_count % 10 == 0:
                logger.progress(f"Restore status: {state} (poll {poll_count}/{max_polls})")
                last_state = state

            if state in ["FINISHED", "CANCELLED", "UNKNOWN"]:
                return {"state": state, "label": label}

            time.sleep(current_interval)
            current_interval = _calculate_next_interval(current_interval, max_poll_interval)

        except Exception:
            return {"state": "ERROR", "label": label}

    return {"state": "TIMEOUT", "label": label}


def execute_restore(
    db,
    restore_command: str,
    backup_label: str,
    restore_type: str,
    repository: str,
    database: str,
    max_polls: int = MAX_POLLS,
    poll_interval: float = 1.0,
    scope: str = "restore",
    ops_database: str = "ops",
) -> dict:
    """Execute a complete restore workflow: submit command and monitor progress.

    Args:
        db: Database connection
        restore_command: Restore SQL command to execute
        backup_label: Label of the backup being restored
        restore_type: Type of restore operation
        repository: Repository name
        database: Database name
        max_polls: Maximum polling attempts
        poll_interval: Seconds between polls
        scope: Job scope (for concurrency control)
        ops_database: Name of ops database (default: "ops")

    Returns dictionary with keys: success, final_status, error_message
    """
    cluster_tz = db.timezone
    started_at = timezone.get_current_time_in_cluster_tz(cluster_tz)

    try:
        db.execute(restore_command.strip())
    except Exception as e:
        logger.error(f"Failed to submit restore command: {str(e)}")
        return {
            "success": False,
            "final_status": None,
            "error_message": f"Failed to submit restore command: {str(e)}",
        }

    label = backup_label

    try:
        final_status = poll_restore_status(db, label, database, max_polls, poll_interval)

        success = final_status["state"] == "FINISHED"
        finished_at = timezone.get_current_time_in_cluster_tz(cluster_tz)

        try:
            history.log_restore(
                db,
                {
                    "job_id": label,
                    "backup_label": backup_label,
                    "restore_type": restore_type,
                    "status": final_status["state"],
                    "repository": repository,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "error_message": None if success else final_status["state"],
                },
                ops_database=ops_database,
            )
        except Exception as e:
            logger.error(f"Failed to log restore history: {str(e)}")

        try:
            concurrency.complete_job_slot(
                db,
                scope=scope,
                label=label,
                final_state=final_status["state"],
                ops_database=ops_database,
            )
        except Exception as e:
            logger.error(f"Failed to complete job slot: {str(e)}")

        return {
            "success": success,
            "final_status": final_status,
            "error_message": None
            if success
            else f"Restore failed with state: {final_status['state']}",
        }

    except Exception as e:
        logger.error(f"Restore execution failed: {str(e)}")
        return {"success": False, "final_status": None, "error_message": str(e)}


def find_restore_pair(db, target_label: str, ops_database: str = "ops") -> list[str]:
    """Find the correct sequence of backups needed for restore.

    Args:
        db: Database connection
        target_label: The backup label to restore to

    Returns:
        List of backup labels in restore order [base_full_backup, target_label]
        or [target_label] if target is a full backup

    Raises:
        ValueError: If target label not found or incremental has no preceding full backup
    """
    query = f"""
    SELECT label, backup_type, finished_at
    FROM {ops_database}.backup_history
    WHERE label = {utils.quote_value(target_label)}
    AND status = 'FINISHED'
    """

    rows = db.query(query)
    if not rows:
        raise exceptions.BackupLabelNotFoundError(target_label)

    target_info = {"label": rows[0][0], "backup_type": rows[0][1], "finished_at": rows[0][2]}

    if target_info["backup_type"] == "full":
        return [target_label]

    if target_info["backup_type"] == "incremental":
        database_name = target_label.split("_")[0]

        full_backup_query = f"""
        SELECT label, backup_type, finished_at
        FROM {ops_database}.backup_history
        WHERE backup_type = 'full'
        AND status = 'FINISHED'
        AND label LIKE {utils.quote_value(f"{database_name}_%")}
        AND finished_at < {utils.quote_value(target_info["finished_at"])}
        ORDER BY finished_at DESC
        LIMIT 1
        """

        full_rows = db.query(full_backup_query)
        if not full_rows:
            raise exceptions.NoSuccessfulFullBackupFoundError(target_label)

        base_full_backup = full_rows[0][0]
        return [base_full_backup, target_label]

    raise ValueError(
        f"Unknown backup type '{target_info['backup_type']}' for label '{target_label}'"
    )


def get_tables_from_backup(
    db,
    label: str,
    group: str | None = None,
    table: str | None = None,
    database: str | None = None,
    ops_database: str = "ops",
) -> list[str]:
    """Get list of tables to restore from backup manifest.

    Args:
        db: Database connection
        label: Backup label
        group: Optional inventory group to filter tables
        table: Optional table name to filter (single table, database comes from database parameter)
        database: Database name (required if table is specified)

    Returns:
        List of table names to restore (format: database.table)

    Raises:
        ValueError: If both group and table are specified
        ValueError: If table is specified but database is not provided
        ValueError: If table is specified but not found in backup
    """
    if group and table:
        raise exceptions.InvalidTableNameError(table, "Cannot specify both --group and --table")

    if table and not database:
        raise exceptions.InvalidTableNameError(
            table, "database parameter is required when table is specified"
        )

    query = f"""
    SELECT DISTINCT database_name, table_name
    FROM {ops_database}.backup_partitions
    WHERE label = {utils.quote_value(label)}
    ORDER BY database_name, table_name
    """

    rows = db.query(query)
    if not rows:
        return []

    tables = [f"{row[0]}.{row[1]}" for row in rows]

    if table:
        target_table = f"{database}.{table}"
        filtered_tables = [t for t in tables if t == target_table]

        if not filtered_tables:
            raise exceptions.TableNotFoundInBackupError(table, label, database)

        return filtered_tables

    if group:
        group_query = f"""
        SELECT database_name, table_name
        FROM {ops_database}.table_inventory
        WHERE inventory_group = {utils.quote_value(group)}
        """

        group_rows = db.query(group_query)
        if not group_rows:
            return []

        group_tables = set()
        for row in group_rows:
            database_name, table_name = row[0], row[1]
            if table_name == "*":
                show_tables_query = f"SHOW TABLES FROM {utils.quote_identifier(database_name)}"
                try:
                    tables_rows = db.query(show_tables_query)
                    for table_row in tables_rows:
                        group_tables.add(f"{database_name}.{table_row[0]}")
                except Exception:
                    continue
            else:
                group_tables.add(f"{database_name}.{table_name}")

        tables = [table for table in tables if table in group_tables]

    return tables


def get_partitions_from_backup(db, label: str, table: str, ops_database: str = "ops") -> list[str]:
    """Get list of partitions for a specific table from backup manifest.

    Args:
        db: Database connection
        label: Backup label
        table: Table name in format 'database.table'
        ops_database: Operations database name

    Returns:
        List of partition names for the table in this backup
    """
    database_name, table_name = table.split(".", 1)

    query = f"""
    SELECT partition_name
    FROM {ops_database}.backup_partitions
    WHERE label = {utils.quote_value(label)}
      AND database_name = {utils.quote_value(database_name)}
      AND table_name = {utils.quote_value(table_name)}
    ORDER BY partition_name
    """

    rows = db.query(query)
    return [row[0] for row in rows]


def execute_restore_flow(
    db,
    repo_name: str,
    restore_pair: list[str],
    tables_to_restore: list[str],
    rename_suffix: str = "_restored",
    skip_confirmation: bool = False,
    ops_database: str = "ops",
) -> dict:
    """Execute the complete restore flow with safety measures.

    Args:
        db: Database connection
        repo_name: Repository name
        restore_pair: List of backup labels in restore order
        tables_to_restore: List of tables to restore (format: database.table)
        rename_suffix: Suffix for temporary tables
        skip_confirmation: If True, skip interactive confirmation prompt
        ops_database: Name of ops database (default: "ops")

    Returns:
        Dictionary with success status and details
    """
    if not restore_pair:
        return {"success": False, "error_message": "No restore pair provided"}

    if not tables_to_restore:
        return {"success": False, "error_message": "No tables to restore"}

    logger.info("")
    logger.info("=== RESTORE PLAN ===")
    logger.info(f"Repository: {repo_name}")
    logger.info(f"Restore sequence: {' -> '.join(restore_pair)}")
    logger.info(f"Tables to restore: {', '.join(tables_to_restore)}")
    logger.info(f"Temporary table suffix: {rename_suffix}")
    logger.info("")
    logger.info("This will restore data to temporary tables and then perform atomic rename.")
    logger.warning("WARNING: This operation will replace existing tables!")

    if not skip_confirmation:
        confirmation = input("\nDo you want to proceed? [Y/n]: ").strip()
        if confirmation.lower() != "y":
            raise exceptions.RestoreOperationCancelledError()
    else:
        logger.info("Proceeding automatically (--yes flag provided)")

    try:
        database_name = tables_to_restore[0].split(".")[0]

        base_label = restore_pair[0]

        tables_in_base = get_tables_from_backup(db, base_label, ops_database=ops_database)
        tables_to_restore_from_base = [t for t in tables_to_restore if t in tables_in_base]

        if tables_to_restore_from_base:
            logger.info("")
            logger.info(f"Step 1: Restoring base backup '{base_label}'...")

            base_timestamp = get_snapshot_timestamp(db, repo_name, base_label)

            base_restore_command = _build_restore_command_with_rename(
                base_label,
                repo_name,
                tables_to_restore_from_base,
                rename_suffix,
                database_name,
                base_timestamp,
            )

            base_result = execute_restore(
                db,
                base_restore_command,
                base_label,
                "full",
                repo_name,
                database_name,
                scope="restore",
                ops_database=ops_database,
            )

            if not base_result["success"]:
                return {
                    "success": False,
                    "error_message": f"Base restore failed: {base_result['error_message']}",
                }

            logger.success("Base restore completed successfully")
        else:
            logger.info("")
            logger.info(
                f"Step 1: Skipping base backup '{base_label}' (no requested tables in this backup)"
            )

        if len(restore_pair) > 1:
            incremental_label = restore_pair[1]

            tables_in_incremental = get_tables_from_backup(
                db, incremental_label, ops_database=ops_database
            )
            tables_to_restore_from_incremental = [
                t for t in tables_to_restore if t in tables_in_incremental
            ]

            if not tables_to_restore_from_incremental:
                logger.info("")
                logger.info(
                    f"Step 2: Skipping incremental backup '{incremental_label}' (no requested tables in this backup)"
                )
            else:
                logger.info("")
                logger.info(f"Step 2: Applying incremental backup '{incremental_label}'...")

                incremental_timestamp = get_snapshot_timestamp(db, repo_name, incremental_label)

                for table in tables_to_restore_from_incremental:
                    partitions = get_partitions_from_backup(
                        db, incremental_label, table, ops_database=ops_database
                    )

                    if not partitions:
                        logger.warning(
                            f"No partitions found for {table} in {incremental_label}, skipping"
                        )
                        continue

                    table_was_in_base = table in tables_to_restore_from_base

                    if table_was_in_base:
                        _, table_name = table.split(".", 1)
                        target_table_name = f"{table_name}{rename_suffix}"
                        incremental_restore_command = _build_partition_restore_command(
                            incremental_label,
                            repo_name,
                            f"{database_name}.{target_table_name}",
                            partitions,
                            database_name,
                            incremental_timestamp,
                            rename_suffix=None,
                        )
                    else:
                        incremental_restore_command = _build_partition_restore_command(
                            incremental_label,
                            repo_name,
                            table,
                            partitions,
                            database_name,
                            incremental_timestamp,
                            rename_suffix=rename_suffix,
                        )

                    incremental_result = execute_restore(
                        db,
                        incremental_restore_command,
                        incremental_label,
                        "incremental",
                        repo_name,
                        database_name,
                        scope="restore",
                        ops_database=ops_database,
                    )

                    if not incremental_result["success"]:
                        return {
                            "success": False,
                            "error_message": f"Incremental restore failed for {table}: {incremental_result['error_message']}",
                        }

                logger.success("Incremental restore completed successfully")

        logger.info("")
        logger.info("Step 3: Performing atomic rename...")
        rename_result = _perform_atomic_rename(db, tables_to_restore, rename_suffix)

        if not rename_result["success"]:
            return {
                "success": False,
                "error_message": f"Atomic rename failed: {rename_result['error_message']}",
            }

        logger.success("Atomic rename completed successfully")

        return {
            "success": True,
            "message": f"Restore completed successfully. Restored {len(tables_to_restore)} tables.",
        }

    except Exception as e:
        return {"success": False, "error_message": f"Restore flow failed: {str(e)}"}


def _build_restore_command_with_rename(
    backup_label: str,
    repo_name: str,
    tables: list[str],
    rename_suffix: str,
    database: str,
    backup_timestamp: str,
) -> str:
    """Build restore command with AS clause for temporary table names."""
    table_clauses = []
    for table in tables:
        _, table_name = table.split(".", 1)
        temp_table_name = f"{table_name}{rename_suffix}"
        table_clauses.append(
            f"TABLE {utils.quote_identifier(table_name)} AS {utils.quote_identifier(temp_table_name)}"
        )

    on_clause = ",\n    ".join(table_clauses)

    return f"""RESTORE SNAPSHOT {utils.quote_identifier(backup_label)}
    FROM {utils.quote_identifier(repo_name)}
    DATABASE {utils.quote_identifier(database)}
    ON ({on_clause})
    PROPERTIES ("backup_timestamp" = "{backup_timestamp}")"""


def _build_restore_command_without_rename(
    backup_label: str, repo_name: str, tables: list[str], database: str, backup_timestamp: str
) -> str:
    """Build restore command without AS clause (for incremental restores to existing temp tables)."""
    table_clauses = []
    for table in tables:
        _, table_name = table.split(".", 1)
        table_clauses.append(f"TABLE {utils.quote_identifier(table_name)}")

    on_clause = ",\n    ".join(table_clauses)

    return f"""RESTORE SNAPSHOT {utils.quote_identifier(backup_label)}
    FROM {utils.quote_identifier(repo_name)}
    DATABASE {utils.quote_identifier(database)}
    ON ({on_clause})
    PROPERTIES ("backup_timestamp" = "{backup_timestamp}")"""


def _build_partition_restore_command(
    backup_label: str,
    repo_name: str,
    table: str,
    partitions: list[str],
    database: str,
    backup_timestamp: str,
    rename_suffix: str | None = None,
) -> str:
    """Build partition-level restore command with optional AS clause.

    Args:
        backup_label: Backup snapshot label
        repo_name: Repository name
        table: Table name in format 'database.table'
        partitions: List of partition names to restore
        database: Database name
        backup_timestamp: Backup timestamp
        rename_suffix: Optional suffix for AS clause (e.g., '_restored')

    Returns:
        SQL RESTORE command string
    """
    _, table_name = table.split(".", 1)

    # Build partition list
    partition_list = ", ".join([utils.quote_identifier(p) for p in partitions])

    # Build table clause
    if rename_suffix:
        # Table only in incremental: use AS clause
        temp_table_name = f"{table_name}{rename_suffix}"
        table_clause = f"TABLE {utils.quote_identifier(table_name)} PARTITION ({partition_list}) AS {utils.quote_identifier(temp_table_name)}"
    else:
        # Table in base: target the _restored table directly (no AS)
        table_clause = f"TABLE {utils.quote_identifier(table_name)} PARTITION ({partition_list})"

    return f"""RESTORE SNAPSHOT {utils.quote_identifier(backup_label)}
    FROM {utils.quote_identifier(repo_name)}
    DATABASE {utils.quote_identifier(database)}
    ON ({table_clause})
    PROPERTIES ("backup_timestamp" = "{backup_timestamp}")"""


def _generate_timestamped_backup_name(table_name: str) -> str:
    """Generate a timestamped backup table name.

    Args:
        table_name: Original table name

    Returns:
        Timestamped backup name in format: {table_name}_backup_YYYYMMDD_HHMMSS
    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{table_name}_backup_{timestamp}"


def _perform_atomic_rename(db, tables: list[str], rename_suffix: str) -> dict:
    """Perform atomic rename of temporary tables to make them live."""
    try:
        rename_statements = []
        for table in tables:
            database, table_name = table.split(".", 1)
            temp_table_name = f"{table_name}{rename_suffix}"
            backup_table_name = _generate_timestamped_backup_name(table_name)

            rename_statements.append(
                f"ALTER TABLE {utils.build_qualified_table_name(database, table_name)} RENAME {utils.quote_identifier(backup_table_name)}"
            )
            rename_statements.append(
                f"ALTER TABLE {utils.build_qualified_table_name(database, temp_table_name)} RENAME {utils.quote_identifier(table_name)}"
            )

        for statement in rename_statements:
            db.execute(statement)

        return {"success": True}

    except Exception as e:
        return {"success": False, "error_message": f"Failed to perform atomic rename: {str(e)}"}
