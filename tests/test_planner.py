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

from unittest.mock import Mock

import pytest

from starrocks_br import exceptions, planner


@pytest.fixture
def db_with_timezone():
    db = Mock()
    db.timezone = "UTC"
    return db


def test_should_find_latest_full_backup(db_with_timezone):
    """Test finding the latest successful full backup."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.return_value = [("test_db_20251015_full", "full", "2025-10-15 10:00:00")]

    result = planner.find_latest_full_backup(db_with_timezone, "test_db")

    assert result is not None
    assert result["label"] == "test_db_20251015_full"
    assert result["backup_type"] == "full"
    assert result["finished_at"] == "2025-10-15 10:00:00"

    query = db_with_timezone.query.call_args[0][0]
    assert "ops.backup_history" in query
    assert "backup_type = 'full'" in query
    assert "status = 'FINISHED'" in query
    assert "label LIKE 'test_db_%'" in query


def test_should_return_none_when_no_full_backup_found(db_with_timezone):
    """Test that find_latest_full_backup returns None when no backup found."""
    db_with_timezone.query.return_value = []

    result = planner.find_latest_full_backup(db_with_timezone, "test_db")

    assert result is None


def test_should_find_partitions_with_specific_baseline_backup(db_with_timezone):
    """Test finding partitions with a specific baseline backup."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.side_effect = [
        [("2025-10-10 10:00:00",)],
        [("sales_db", "fact_sales")],
        [
            (
                "PartitionId",
                "p20251015",
                "VisibleVersion",
                "2025-10-15 12:00:00",
                "VisibleVersionHash",
            )
        ],  # SHOW PARTITIONS result
    ]

    partitions = planner.find_recent_partitions(
        db_with_timezone, "sales_db", "sales_db_20251010_full", group_name="daily_incremental"
    )

    assert len(partitions) == 1
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251015",
    } in partitions

    baseline_query = db_with_timezone.query.call_args_list[0][0][0]
    assert "ops.backup_history" in baseline_query
    assert "label = 'sales_db_20251010_full'" in baseline_query

    show_partitions_query = db_with_timezone.query.call_args_list[2][0][0]
    assert "SHOW PARTITIONS FROM `sales_db`.`fact_sales`" in show_partitions_query


def test_should_fail_when_no_full_backup_found(mocker, db_with_timezone):
    """Test that find_recent_partitions fails when no full backup is found."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.return_value = []

    mocker.patch("starrocks_br.planner.find_latest_full_backup", return_value=None)

    with pytest.raises(exceptions.NoFullBackupFoundError) as exc_info:
        planner.find_recent_partitions(db_with_timezone, "test_db", group_name="daily_incremental")

    assert exc_info.value.database == "test_db"


def test_should_fail_when_invalid_baseline_backup(db_with_timezone):
    """Test that find_recent_partitions fails when baseline backup is invalid."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.return_value = []

    with pytest.raises(exceptions.BackupLabelNotFoundError):
        planner.find_recent_partitions(
            db_with_timezone, "test_db", "invalid_backup", group_name="daily_incremental"
        )


def test_should_find_partitions_updated_since_latest_full_backup(mocker, db_with_timezone):
    """Test finding partitions updated since the latest full backup."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.side_effect = [
        [("sales_db", "fact_sales"), ("orders_db", "fact_orders")],
        [
            (
                "PartitionId",
                "p20251015",
                "VisibleVersion",
                "2025-10-15 12:00:00",
                "VisibleVersionHash",
            ),
            (
                "PartitionId",
                "p20251014",
                "VisibleVersion",
                "2025-10-14 12:00:00",
                "VisibleVersionHash",
            ),
        ],
    ]

    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "sales_db_20251010_full",
            "backup_type": "full",
            "finished_at": "2025-10-10 10:00:00",
        },
    )

    partitions = planner.find_recent_partitions(
        db_with_timezone, "sales_db", group_name="daily_incremental"
    )

    assert len(partitions) == 2
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251015",
    } in partitions
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251014",
    } in partitions
    assert db_with_timezone.query.call_count == 2


def test_should_build_incremental_backup_command():
    partitions = [
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"},
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251014"},
    ]
    repository = "my_repo"
    label = "sales_db_20251015_incremental"
    database = "sales_db"

    command = planner.build_incremental_backup_command(partitions, repository, label, database)

    expected = """BACKUP DATABASE `sales_db` SNAPSHOT `sales_db_20251015_incremental`
    TO `my_repo`
    ON (TABLE `fact_sales` PARTITION (`p20251015`, `p20251014`))"""

    assert command == expected


def test_should_handle_empty_partitions_list():
    command = planner.build_incremental_backup_command([], "my_repo", "label", "test_db")
    assert command == ""


def test_should_handle_single_partition():
    partitions = [{"database": "db1", "table": "table1", "partition_name": "p1"}]
    command = planner.build_incremental_backup_command(partitions, "repo", "label", "db1")

    assert "TABLE `table1` PARTITION (`p1`)" in command
    assert "BACKUP DATABASE `db1` SNAPSHOT `label`" in command
    assert "TO `repo`" in command


def test_should_format_date_correctly_in_query(mocker, db_with_timezone):
    """Test that the query uses the correct baseline time format and SHOW PARTITIONS command."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.side_effect = [
        [("sales_db", "fact_sales")],
        [],
    ]

    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "sales_db_20251010_full",
            "backup_type": "full",
            "finished_at": "2025-10-10 10:00:00",
        },
    )

    planner.find_recent_partitions(db_with_timezone, "sales_db", group_name="daily_incremental")

    partitions_query = db_with_timezone.query.call_args_list[1][0][0]
    assert "SHOW PARTITIONS FROM `sales_db`.`fact_sales`" in partitions_query


def test_should_build_full_backup_command_with_wildcard(db_with_timezone):
    """Test building full backup command when group contains wildcard."""
    db_with_timezone.query.return_value = [
        ("sales_db", "*"),
        ("sales_db", "dim_customers"),
    ]

    command = planner.build_full_backup_command(
        db_with_timezone, "monthly_full", "my_repo", "sales_db_20251015_full", "sales_db"
    )

    expected = """BACKUP DATABASE `sales_db` SNAPSHOT `sales_db_20251015_full`
    TO `my_repo`"""
    assert command == expected


def test_should_build_full_backup_command_with_specific_tables(db_with_timezone):
    """Test building full backup command with specific tables."""
    db_with_timezone.query.return_value = [
        ("sales_db", "dim_customers"),
        ("sales_db", "dim_products"),
    ]

    command = planner.build_full_backup_command(
        db_with_timezone, "weekly_dimensions", "my_repo", "weekly_backup_20251015", "sales_db"
    )

    expected = """BACKUP DATABASE `sales_db` SNAPSHOT `weekly_backup_20251015`
    TO `my_repo`
    ON (TABLE `dim_customers`,
        TABLE `dim_products`)"""
    assert command == expected


def test_should_return_empty_command_when_no_tables_in_group(db_with_timezone):
    """Test that build_full_backup_command returns empty when no tables in group."""
    db_with_timezone.query.return_value = []

    command = planner.build_full_backup_command(
        db_with_timezone, "empty_group", "repo", "label", "test_db"
    )

    assert command == ""


def test_should_return_empty_command_when_no_tables_for_database(db_with_timezone):
    """Test that build_full_backup_command returns empty when no tables for specific database."""
    db_with_timezone.query.return_value = [
        ("other_db", "table1"),
    ]

    command = planner.build_full_backup_command(
        db_with_timezone, "group", "repo", "label", "test_db"
    )

    assert command == ""


def test_should_find_tables_by_group(db_with_timezone):
    """Test finding tables by inventory group."""
    db_with_timezone.query.return_value = [
        ("sales_db", "fact_sales"),
        ("sales_db", "dim_customers"),
        ("orders_db", "fact_orders"),
    ]

    tables = planner.find_tables_by_group(db_with_timezone, "daily_incremental")

    assert len(tables) == 3
    assert {"database": "sales_db", "table": "fact_sales"} in tables
    assert {"database": "sales_db", "table": "dim_customers"} in tables
    assert {"database": "orders_db", "table": "fact_orders"} in tables

    query = db_with_timezone.query.call_args[0][0]
    assert "ops.table_inventory" in query
    assert "inventory_group = 'daily_incremental'" in query


def test_should_find_tables_by_group_with_wildcard(db_with_timezone):
    """Test finding tables by group including wildcard entries."""
    db_with_timezone.query.return_value = [
        ("sales_db", "*"),  # Wildcard
        ("orders_db", "fact_orders"),  # Specific table
    ]

    tables = planner.find_tables_by_group(db_with_timezone, "monthly_full")

    assert len(tables) == 2
    assert {"database": "sales_db", "table": "*"} in tables
    assert {"database": "orders_db", "table": "fact_orders"} in tables


def test_should_return_empty_list_when_group_not_found(db_with_timezone):
    """Test that find_tables_by_group returns empty list when group not found."""
    db_with_timezone.query.return_value = []

    tables = planner.find_tables_by_group(db_with_timezone, "nonexistent_group")

    assert len(tables) == 0


def test_should_find_recent_partitions_with_group_filtering(mocker, db_with_timezone):
    """Test finding recent partitions filtered by inventory group."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.side_effect = [
        [("sales_db", "fact_sales"), ("orders_db", "fact_orders")],
        [
            (
                "PartitionId",
                "p20251015",
                "VisibleVersion",
                "2025-10-15 12:00:00",
                "VisibleVersionHash",
            )
        ],
    ]

    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "sales_db_20251010_full",
            "backup_type": "full",
            "finished_at": "2025-10-10 10:00:00",
        },
    )

    partitions = planner.find_recent_partitions(
        db_with_timezone, "sales_db", group_name="daily_incremental"
    )

    assert len(partitions) == 1
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251015",
    } in partitions
    assert db_with_timezone.query.call_count == 2


def test_should_handle_no_recent_partitions_with_group_filtering(mocker, db_with_timezone):
    """Test handling when no recent partitions exist for group tables."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.side_effect = [
        [("sales_db", "fact_sales")],
        [
            (
                "PartitionId",
                "p20251005",
                "VisibleVersion",
                "2025-10-05 12:00:00",
                "VisibleVersionHash",
            )
        ],  # Old partition (before baseline)
    ]

    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "sales_db_20251010_full",
            "backup_type": "full",
            "finished_at": "2025-10-10 10:00:00",
        },
    )

    partitions = planner.find_recent_partitions(
        db_with_timezone, "sales_db", group_name="daily_incremental"
    )

    assert len(partitions) == 0
    assert db_with_timezone.query.call_count == 2


def test_should_return_empty_partitions_when_no_group_tables(mocker, db_with_timezone):
    """Test that find_recent_partitions returns empty when no tables in group."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.return_value = []

    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "test_db_20251010_full",
            "backup_type": "full",
            "finished_at": "2025-10-10 10:00:00",
        },
    )

    partitions = planner.find_recent_partitions(
        db_with_timezone, "test_db", group_name="empty_group"
    )

    assert len(partitions) == 0
    assert db_with_timezone.query.call_count == 1


def test_should_record_backup_partitions(db_with_timezone):
    """Test recording partition metadata for a backup."""
    import hashlib

    partitions = [
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"},
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251014"},
        {"database": "orders_db", "table": "fact_orders", "partition_name": "p20251015"},
    ]
    label = "sales_db_20251015_incremental"

    planner.record_backup_partitions(db_with_timezone, label, partitions)

    assert db_with_timezone.execute.call_count == 3

    first_call = db_with_timezone.execute.call_args_list[0][0][0]
    assert "INSERT INTO ops.backup_partitions" in first_call
    assert "key_hash, label, database_name, table_name, partition_name" in first_call

    # Verify the hash is computed correctly for the first partition
    expected_composite_key = f"{label}|sales_db|fact_sales|p20251015"
    expected_hash = hashlib.md5(expected_composite_key.encode("utf-8")).hexdigest()
    assert (
        f"VALUES ('{expected_hash}', 'sales_db_20251015_incremental', 'sales_db', 'fact_sales', 'p20251015')"
        in first_call
    )


def test_should_handle_empty_partitions_list_in_record_backup_partitions(db_with_timezone):
    """Test that record_backup_partitions handles empty partitions list gracefully."""

    planner.record_backup_partitions(db_with_timezone, "test_label", [])

    db_with_timezone.execute.assert_not_called()


def test_should_get_all_partitions_for_tables(db_with_timezone):
    """Test getting all partitions for specified tables."""
    db_with_timezone.query.return_value = [
        ("sales_db", "fact_sales", "p20251015"),
        ("sales_db", "fact_sales", "p20251014"),
        ("sales_db", "dim_customers", "p20251015"),
    ]

    tables = [
        {"database": "sales_db", "table": "fact_sales"},
        {"database": "sales_db", "table": "dim_customers"},
        {"database": "orders_db", "table": "fact_orders"},
    ]

    partitions = planner.get_all_partitions_for_tables(db_with_timezone, "sales_db", tables)

    assert len(partitions) == 3
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251015",
    } in partitions
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251014",
    } in partitions
    assert {
        "database": "sales_db",
        "table": "dim_customers",
        "partition_name": "p20251015",
    } in partitions

    query = db_with_timezone.query.call_args[0][0]
    assert "information_schema.partitions_meta" in query
    assert "PARTITION_NAME IS NOT NULL" in query


def test_should_handle_wildcard_tables_in_get_all_partitions(db_with_timezone):
    """Test getting all partitions when tables include wildcard entries."""
    db_with_timezone.query.return_value = [
        ("sales_db", "fact_sales", "p20251015"),
        ("sales_db", "dim_customers", "p20251015"),
        ("sales_db", "any_other_table", "p20251015"),
    ]

    tables = [
        {"database": "sales_db", "table": "*"},  # Wildcard
        {"database": "orders_db", "table": "fact_orders"},  # Specific table
    ]

    partitions = planner.get_all_partitions_for_tables(db_with_timezone, "sales_db", tables)

    # Should return all partitions for sales_db (due to wildcard)
    assert len(partitions) == 3
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251015",
    } in partitions
    assert {
        "database": "sales_db",
        "table": "dim_customers",
        "partition_name": "p20251015",
    } in partitions
    assert {
        "database": "sales_db",
        "table": "any_other_table",
        "partition_name": "p20251015",
    } in partitions


def test_should_return_empty_list_when_no_tables_in_get_all_partitions(db_with_timezone):
    """Test that get_all_partitions_for_tables returns empty list when no tables provided."""

    partitions = planner.get_all_partitions_for_tables(db_with_timezone, "test_db", [])

    assert len(partitions) == 0
    db_with_timezone.query.assert_not_called()


def test_should_return_empty_list_when_no_tables_for_database_in_get_all_partitions(
    db_with_timezone,
):
    """Test that get_all_partitions_for_tables returns empty when no tables for specified database."""

    tables = [
        {"database": "other_db", "table": "table1"},
        {"database": "another_db", "table": "table2"},
    ]

    partitions = planner.get_all_partitions_for_tables(db_with_timezone, "test_db", tables)

    assert len(partitions) == 0
    db_with_timezone.query.assert_not_called()


def test_find_recent_partitions_handles_wildcard_group(mocker, db_with_timezone):
    """Test that find_recent_partitions correctly handles wildcard table groups."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.side_effect = [
        [("sales_db", "*")],
        [("fact_sales",), ("dim_customers",)],
        [
            (
                "PartitionId",
                "p20251015",
                "VisibleVersion",
                "2025-10-15 12:00:00",
                "VisibleVersionHash",
            )
        ],
        [
            (
                "PartitionId",
                "p20251014",
                "VisibleVersion",
                "2025-10-14 12:00:00",
                "VisibleVersionHash",
            )
        ],
    ]

    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "sales_db_20251010_full",
            "backup_type": "full",
            "finished_at": "2025-10-10 10:00:00",
        },
    )

    partitions = planner.find_recent_partitions(
        db_with_timezone, "sales_db", group_name="monthly_full"
    )

    assert len(partitions) == 2
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251015",
    } in partitions
    assert {
        "database": "sales_db",
        "table": "dim_customers",
        "partition_name": "p20251014",
    } in partitions

    show_tables_query = db_with_timezone.query.call_args_list[1][0][0]
    assert "SHOW TABLES FROM `sales_db`" in show_tables_query

    show_partitions_query_1 = db_with_timezone.query.call_args_list[2][0][0]
    assert "SHOW PARTITIONS FROM `sales_db`.`fact_sales`" in show_partitions_query_1

    show_partitions_query_2 = db_with_timezone.query.call_args_list[3][0][0]
    assert "SHOW PARTITIONS FROM `sales_db`.`dim_customers`" in show_partitions_query_2


def test_find_recent_partitions_with_multiple_tables_mixed_timestamps(mocker, db_with_timezone):
    """Test finding recent partitions across multiple tables with mixed old and new partitions."""
    db_with_timezone.timezone = "UTC"
    db_with_timezone.query.side_effect = [
        [("sales_db", "fact_sales"), ("sales_db", "fact_orders"), ("sales_db", "dim_products")],
        # SHOW PARTITIONS for fact_sales - mix of old and new partitions
        [
            (
                "PartitionId",
                "p20251005",
                "VisibleVersion",
                "2025-10-05 08:00:00",
                "VisibleVersionHash",
            ),  # Old (before baseline)
            (
                "PartitionId",
                "p20251015",
                "VisibleVersion",
                "2025-10-15 12:00:00",
                "VisibleVersionHash",
            ),  # New
            (
                "PartitionId",
                "p20251016",
                "VisibleVersion",
                "2025-10-16 14:00:00",
                "VisibleVersionHash",
            ),
        ],  # New
        # SHOW PARTITIONS for fact_orders - only old partitions
        [
            (
                "PartitionId",
                "p20251001",
                "VisibleVersion",
                "2025-10-01 10:00:00",
                "VisibleVersionHash",
            ),  # Old
            (
                "PartitionId",
                "p20251008",
                "VisibleVersion",
                "2025-10-08 11:00:00",
                "VisibleVersionHash",
            ),
        ],  # Old
        # SHOW PARTITIONS for dim_products - only new partitions
        [
            (
                "PartitionId",
                "p20251020",
                "VisibleVersion",
                "2025-10-20 09:00:00",
                "VisibleVersionHash",
            ),  # New
            (
                "PartitionId",
                "p20251021",
                "VisibleVersion",
                "2025-10-21 10:00:00",
                "VisibleVersionHash",
            ),
        ],  # New
    ]

    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "sales_db_20251010_full",
            "backup_type": "full",
            "finished_at": "2025-10-10 10:00:00",
        },
    )

    partitions = planner.find_recent_partitions(
        db_with_timezone, "sales_db", group_name="daily_incremental"
    )

    # Should only include partitions with timestamps after 2025-10-10 10:00:00
    assert len(partitions) == 4

    # From fact_sales (2 new partitions)
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251015",
    } in partitions
    assert {
        "database": "sales_db",
        "table": "fact_sales",
        "partition_name": "p20251016",
    } in partitions

    # From fact_orders (0 new partitions - all are old)
    assert {
        "database": "sales_db",
        "table": "fact_orders",
        "partition_name": "p20251001",
    } not in partitions
    assert {
        "database": "sales_db",
        "table": "fact_orders",
        "partition_name": "p20251008",
    } not in partitions

    # From dim_products (2 new partitions)
    assert {
        "database": "sales_db",
        "table": "dim_products",
        "partition_name": "p20251020",
    } in partitions
    assert {
        "database": "sales_db",
        "table": "dim_products",
        "partition_name": "p20251021",
    } in partitions

    assert db_with_timezone.query.call_count == 4

    show_partitions_query_1 = db_with_timezone.query.call_args_list[1][0][0]
    assert "SHOW PARTITIONS FROM `sales_db`.`fact_sales`" in show_partitions_query_1

    show_partitions_query_2 = db_with_timezone.query.call_args_list[2][0][0]
    assert "SHOW PARTITIONS FROM `sales_db`.`fact_orders`" in show_partitions_query_2

    show_partitions_query_3 = db_with_timezone.query.call_args_list[3][0][0]
    assert "SHOW PARTITIONS FROM `sales_db`.`dim_products`" in show_partitions_query_3


def test_should_validate_tables_exist_in_database(db_with_timezone):
    """Test validating that tables in inventory exist in database."""
    db_with_timezone.query.return_value = [
        ("fact_sales",),
        ("dim_customers",),
    ]

    tables = [
        {"database": "sales_db", "table": "fact_sales"},
        {"database": "sales_db", "table": "dim_customers"},
    ]

    planner.validate_tables_exist(db_with_timezone, "sales_db", tables)

    query = db_with_timezone.query.call_args[0][0]
    assert "SHOW TABLES FROM `sales_db`" in query


def test_should_raise_error_when_tables_do_not_exist(db_with_timezone):
    """Test that validate_tables_exist raises error when tables don't exist in database."""
    db_with_timezone.query.return_value = [
        ("fact_sales",),
    ]

    tables = [
        {"database": "sales_db", "table": "fact_sales"},
        {"database": "sales_db", "table": "invalid_table"},
        {"database": "sales_db", "table": "another_invalid"},
    ]

    with pytest.raises(exceptions.InvalidTablesInInventoryError) as exc_info:
        planner.validate_tables_exist(db_with_timezone, "sales_db", tables)

    assert exc_info.value.database == "sales_db"
    assert exc_info.value.group is None
    assert "invalid_table" in exc_info.value.invalid_tables
    assert "another_invalid" in exc_info.value.invalid_tables
    assert "fact_sales" not in exc_info.value.invalid_tables


def test_should_skip_validation_for_wildcard_tables(db_with_timezone):
    """Test that wildcard tables skip validation."""
    tables = [
        {"database": "sales_db", "table": "*"},
    ]

    planner.validate_tables_exist(db_with_timezone, "sales_db", tables)

    db_with_timezone.query.assert_not_called()


def test_should_validate_only_non_wildcard_tables(db_with_timezone):
    """Test that validation only checks non-wildcard tables."""
    db_with_timezone.query.return_value = [
        ("fact_sales",),
    ]

    tables = [
        {"database": "sales_db", "table": "*"},
        {"database": "sales_db", "table": "fact_sales"},
    ]

    planner.validate_tables_exist(db_with_timezone, "sales_db", tables)

    db_with_timezone.query.assert_called_once()


def test_should_validate_only_tables_for_target_database(db_with_timezone):
    """Test that validation only checks tables for the target database."""
    db_with_timezone.query.return_value = [
        ("fact_sales",),
    ]

    tables = [
        {"database": "sales_db", "table": "fact_sales"},
        {"database": "other_db", "table": "other_table"},
    ]

    planner.validate_tables_exist(db_with_timezone, "sales_db", tables)

    query = db_with_timezone.query.call_args[0][0]
    assert "SHOW TABLES FROM `sales_db`" in query


def test_should_handle_empty_tables_list_in_validation(db_with_timezone):
    """Test that validation handles empty tables list gracefully."""
    tables = []

    planner.validate_tables_exist(db_with_timezone, "sales_db", tables)

    db_with_timezone.query.assert_not_called()


def test_should_handle_query_error_during_validation(db_with_timezone):
    """Test that validation handles query errors appropriately."""
    db_with_timezone.query.side_effect = Exception("Database connection error")

    tables = [
        {"database": "sales_db", "table": "fact_sales"},
    ]

    with pytest.raises(Exception) as exc_info:
        planner.validate_tables_exist(db_with_timezone, "sales_db", tables)

    assert "Database connection error" in str(exc_info.value)
