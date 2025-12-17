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

from datetime import datetime

from . import logger


def get_successful_backups(
    db, repository: str, group: str = None, ops_database: str = "ops"
) -> list[dict]:
    """Get all successful backups from backup_history, optionally filtered by group.

    Args:
        db: Database connection
        repository: Repository name to filter by
        group: Optional inventory group to filter by
        ops_database: Name of the ops database (defaults to "ops")

    Returns:
        List of backup records as dicts with keys: label, finished_at, inventory_group (if group filtering is used)
    """
    if group:
        sql = f"""
        SELECT DISTINCT
            bh.label,
            bh.finished_at,
            ti.inventory_group
        FROM {ops_database}.backup_history bh
        INNER JOIN {ops_database}.backup_partitions bp ON bh.label = bp.label
        INNER JOIN {ops_database}.table_inventory ti
            ON bp.database_name = ti.database_name
            AND (bp.table_name = ti.table_name OR ti.table_name = '*')
        WHERE bh.repository = '{repository}'
            AND bh.status = 'FINISHED'
            AND ti.inventory_group = '{group}'
        ORDER BY bh.finished_at ASC
        """
    else:
        sql = f"""
        SELECT
            label,
            finished_at
        FROM {ops_database}.backup_history
        WHERE repository = '{repository}'
            AND status = 'FINISHED'
        ORDER BY finished_at ASC
        """

    rows = db.query(sql)
    results = []

    for row in rows:
        if group:
            results.append({"label": row[0], "finished_at": str(row[1]), "inventory_group": row[2]})
        else:
            results.append({"label": row[0], "finished_at": str(row[1])})

    return results


def filter_snapshots_to_delete(
    all_snapshots: list[dict], strategy: str, **kwargs
) -> list[dict]:
    """Filter snapshots based on pruning strategy.

    Args:
        all_snapshots: List of snapshot dicts (must be sorted by finished_at ASC)
        strategy: Pruning strategy - 'keep_last', 'older_than', 'specific', or 'multiple'
        **kwargs: Strategy-specific parameters:
            - keep_last: 'count' (int) - number of backups to keep
            - older_than: 'timestamp' (str) - timestamp in 'YYYY-MM-DD HH:MM:SS' format
            - specific: 'snapshot' (str) - specific snapshot name
            - multiple: 'snapshots' (list) - list of snapshot names

    Returns:
        List of snapshots to delete
    """
    if strategy == "keep_last":
        count = kwargs.get("count")
        if count is None or count <= 0:
            raise ValueError("keep_last strategy requires a positive count")

        # Keep the last N, delete the rest
        if len(all_snapshots) <= count:
            return []
        return all_snapshots[: -count]  # Delete all except last N

    elif strategy == "older_than":
        timestamp_str = kwargs.get("timestamp")
        if not timestamp_str:
            raise ValueError("older_than strategy requires a timestamp")

        try:
            cutoff = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            raise ValueError(
                f"Invalid timestamp format '{timestamp_str}'. Expected 'YYYY-MM-DD HH:MM:SS'"
            ) from e

        to_delete = []
        for snapshot in all_snapshots:
            snapshot_time = datetime.strptime(snapshot["finished_at"], "%Y-%m-%d %H:%M:%S")
            if snapshot_time < cutoff:
                to_delete.append(snapshot)

        return to_delete

    elif strategy == "specific":
        snapshot_name = kwargs.get("snapshot")
        if not snapshot_name:
            raise ValueError("specific strategy requires a snapshot name")

        for snapshot in all_snapshots:
            if snapshot["label"] == snapshot_name:
                return [snapshot]

        return []

    elif strategy == "multiple":
        snapshot_names = kwargs.get("snapshots")
        if not snapshot_names:
            raise ValueError("multiple strategy requires a list of snapshot names")

        to_delete = []
        for snapshot in all_snapshots:
            if snapshot["label"] in snapshot_names:
                to_delete.append(snapshot)

        return to_delete

    else:
        raise ValueError(f"Unknown pruning strategy: {strategy}")


def verify_snapshot_exists(db, repository: str, snapshot_name: str) -> bool:
    """Verify that a snapshot exists in the repository.

    Args:
        db: Database connection
        repository: Repository name
        snapshot_name: Snapshot name to verify

    Returns:
        True if snapshot exists, False otherwise

    Raises:
        Exception if snapshot is not found
    """
    sql = f"SHOW SNAPSHOT ON {repository} WHERE SNAPSHOT = '{snapshot_name}'"

    try:
        rows = db.query(sql)
        if not rows:
            raise Exception(f"Snapshot '{snapshot_name}' not found in repository '{repository}'")
        return True
    except Exception as e:
        logger.error(f"Failed to verify snapshot '{snapshot_name}': {e}")
        raise


def execute_drop_snapshot(db, repository: str, snapshot_name: str) -> None:
    """Execute DROP SNAPSHOT command for a single snapshot.

    Args:
        db: Database connection
        repository: Repository name
        snapshot_name: Snapshot name to delete

    Raises:
        Exception if deletion fails
    """
    sql = f"DROP SNAPSHOT ON {repository} WHERE SNAPSHOT = '{snapshot_name}'"

    try:
        logger.info(f"Deleting snapshot: {snapshot_name}")
        db.execute(sql)
        logger.success(f"Successfully deleted snapshot: {snapshot_name}")
    except Exception as e:
        logger.error(f"Failed to delete snapshot '{snapshot_name}': {e}")
        raise


def cleanup_backup_history(db, snapshot_label: str, ops_database: str = "ops") -> None:
    """Remove backup history entry after snapshot deletion.

    Args:
        db: Database connection
        snapshot_label: Snapshot label to remove from history
        ops_database: Name of the ops database (defaults to "ops")
    """
    try:
        db.execute(f"DELETE FROM {ops_database}.backup_partitions WHERE label = '{snapshot_label}'")
        db.execute(f"DELETE FROM {ops_database}.backup_history WHERE label = '{snapshot_label}'")
        logger.debug(f"Cleaned up backup history for: {snapshot_label}")
    except Exception as e:
        logger.warning(f"Failed to cleanup backup history for '{snapshot_label}': {e}")
