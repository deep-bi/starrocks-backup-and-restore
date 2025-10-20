import pytest
from starrocks_br import planner


def test_should_find_latest_full_backup(mocker):
    """Test finding the latest successful full backup."""
    db = mocker.Mock()
    db.query.return_value = [
        ("test_db_20251015_full", "full", "2025-10-15 10:00:00")
    ]
    
    result = planner.find_latest_full_backup(db, "test_db")
    
    assert result is not None
    assert result["label"] == "test_db_20251015_full"
    assert result["backup_type"] == "full"
    assert result["finished_at"] == "2025-10-15 10:00:00"
    
    query = db.query.call_args[0][0]
    assert "ops.backup_history" in query
    assert "backup_type = 'full'" in query
    assert "status = 'FINISHED'" in query
    assert "label LIKE 'test_db_%'" in query


def test_should_return_none_when_no_full_backup_found(mocker):
    """Test that find_latest_full_backup returns None when no backup found."""
    db = mocker.Mock()
    db.query.return_value = []
    
    result = planner.find_latest_full_backup(db, "test_db")
    
    assert result is None


def test_should_find_partitions_with_specific_baseline_backup(mocker):
    """Test finding partitions with a specific baseline backup."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("2025-10-10 10:00:00",)],
        [("sales_db", "fact_sales")],
        [("sales_db", "fact_sales", "p20251015", "2025-10-15")],
    ]
    
    partitions = planner.find_recent_partitions(db, "sales_db", "sales_db_20251010_full", group_name="daily_incremental")
    
    assert len(partitions) == 1
    assert {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"} in partitions
    
    baseline_query = db.query.call_args_list[0][0][0]
    assert "ops.backup_history" in baseline_query
    assert "label = 'sales_db_20251010_full'" in baseline_query


def test_should_fail_when_no_full_backup_found(mocker):
    """Test that find_recent_partitions fails when no full backup is found."""
    db = mocker.Mock()
    db.query.return_value = []
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value=None)
    
    with pytest.raises(ValueError, match="No successful full backup found"):
        planner.find_recent_partitions(db, "test_db", group_name="daily_incremental")


def test_should_fail_when_invalid_baseline_backup(mocker):
    """Test that find_recent_partitions fails when baseline backup is invalid."""
    db = mocker.Mock()
    db.query.return_value = []
    
    with pytest.raises(ValueError, match="Baseline backup 'invalid_backup' not found"):
        planner.find_recent_partitions(db, "test_db", "invalid_backup", group_name="daily_incremental")


def test_should_find_partitions_updated_since_latest_full_backup(mocker):
    """Test finding partitions updated since the latest full backup."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("sales_db", "fact_sales"), ("orders_db", "fact_orders")],  # group tables
        [("sales_db", "fact_sales", "p20251015", "2025-10-15"),
         ("sales_db", "fact_sales", "p20251014", "2025-10-14")],  # recent partitions
    ]
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'sales_db_20251010_full',
        'backup_type': 'full',
        'finished_at': '2025-10-10 10:00:00'
    })
    
    partitions = planner.find_recent_partitions(db, "sales_db", group_name="daily_incremental")
    
    assert len(partitions) == 2
    assert {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"} in partitions
    assert {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251014"} in partitions
    assert db.query.call_count == 2


def test_should_build_incremental_backup_command():
    partitions = [
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"},
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251014"},
    ]
    repository = "my_repo"
    label = "sales_db_20251015_incremental"
    database = "sales_db"
    
    command = planner.build_incremental_backup_command(partitions, repository, label, database)
    
    expected = """BACKUP DATABASE sales_db SNAPSHOT sales_db_20251015_incremental
    TO my_repo
    ON (TABLE fact_sales PARTITION (p20251015, p20251014))"""
    
    assert command == expected


def test_should_handle_empty_partitions_list():
    command = planner.build_incremental_backup_command([], "my_repo", "label", "test_db")
    assert command == ""


def test_should_handle_single_partition():
    partitions = [{"database": "db1", "table": "table1", "partition_name": "p1"}]
    command = planner.build_incremental_backup_command(partitions, "repo", "label", "db1")
    
    assert "TABLE table1 PARTITION (p1)" in command
    assert "BACKUP DATABASE db1 SNAPSHOT label" in command
    assert "TO repo" in command


def test_should_format_date_correctly_in_query(mocker):
    """Test that the query uses the correct baseline time format."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("sales_db", "fact_sales")],  # group tables
        [],  # no recent partitions
    ]
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'sales_db_20251010_full',
        'backup_type': 'full',
        'finished_at': '2025-10-10 10:00:00'
    })
    
    planner.find_recent_partitions(db, "sales_db", group_name="daily_incremental")
    
    # Check the second query (the partitions query)
    partitions_query = db.query.call_args_list[1][0][0]
    assert "information_schema.partitions_meta" in partitions_query
    assert "WHERE" in partitions_query
    assert "VISIBLE_VERSION_TIME > '2025-10-10 10:00:00'" in partitions_query


def test_should_build_full_backup_command_with_wildcard(mocker):
    """Test building full backup command when group contains wildcard."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "*"),  # Wildcard entry
        ("sales_db", "dim_customers"),  # Specific table
    ]
    
    command = planner.build_full_backup_command(db, "monthly_full", "my_repo", "sales_db_20251015_full", "sales_db")
    
    expected = """BACKUP DATABASE sales_db SNAPSHOT sales_db_20251015_full
    TO my_repo"""
    assert command == expected


def test_should_build_full_backup_command_with_specific_tables(mocker):
    """Test building full backup command with specific tables."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "dim_customers"),
        ("sales_db", "dim_products"),
    ]
    
    command = planner.build_full_backup_command(db, "weekly_dimensions", "my_repo", "weekly_backup_20251015", "sales_db")
    
    expected = """BACKUP DATABASE sales_db SNAPSHOT weekly_backup_20251015
    TO my_repo
    ON (TABLE dim_customers,
        TABLE dim_products)"""
    assert command == expected


def test_should_return_empty_command_when_no_tables_in_group(mocker):
    """Test that build_full_backup_command returns empty when no tables in group."""
    db = mocker.Mock()
    db.query.return_value = []
    
    command = planner.build_full_backup_command(db, "empty_group", "repo", "label", "test_db")
    
    assert command == ""


def test_should_return_empty_command_when_no_tables_for_database(mocker):
    """Test that build_full_backup_command returns empty when no tables for specific database."""
    db = mocker.Mock()
    db.query.return_value = [
        ("other_db", "table1"),
    ]
    
    command = planner.build_full_backup_command(db, "group", "repo", "label", "test_db")
    
    assert command == ""


def test_should_find_tables_by_group(mocker):
    """Test finding tables by inventory group."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "fact_sales"),
        ("sales_db", "dim_customers"),
        ("orders_db", "fact_orders"),
    ]
    
    tables = planner.find_tables_by_group(db, "daily_incremental")
    
    assert len(tables) == 3
    assert {"database": "sales_db", "table": "fact_sales"} in tables
    assert {"database": "sales_db", "table": "dim_customers"} in tables
    assert {"database": "orders_db", "table": "fact_orders"} in tables
    
    query = db.query.call_args[0][0]
    assert "ops.table_inventory" in query
    assert "inventory_group = 'daily_incremental'" in query


def test_should_find_tables_by_group_with_wildcard(mocker):
    """Test finding tables by group including wildcard entries."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "*"),  # Wildcard
        ("orders_db", "fact_orders"),  # Specific table
    ]
    
    tables = planner.find_tables_by_group(db, "monthly_full")
    
    assert len(tables) == 2
    assert {"database": "sales_db", "table": "*"} in tables
    assert {"database": "orders_db", "table": "fact_orders"} in tables


def test_should_return_empty_list_when_group_not_found(mocker):
    """Test that find_tables_by_group returns empty list when group not found."""
    db = mocker.Mock()
    db.query.return_value = []
    
    tables = planner.find_tables_by_group(db, "nonexistent_group")
    
    assert len(tables) == 0


def test_should_find_recent_partitions_with_group_filtering(mocker):
    """Test finding recent partitions filtered by inventory group."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("sales_db", "fact_sales"), ("orders_db", "fact_orders")],
        [("sales_db", "fact_sales", "p20251015", "2025-10-15")],
    ]
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'sales_db_20251010_full',
        'backup_type': 'full',
        'finished_at': '2025-10-10 10:00:00'
    })
    
    partitions = planner.find_recent_partitions(db, "sales_db", group_name="daily_incremental")
    
    assert len(partitions) == 1
    assert {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"} in partitions
    assert db.query.call_count == 2


def test_should_handle_no_recent_partitions_with_group_filtering(mocker):
    """Test handling when no recent partitions exist for group tables."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("sales_db", "fact_sales")],  # Group tables
        [],  # No recent partitions
    ]
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'sales_db_20251010_full',
        'backup_type': 'full',
        'finished_at': '2025-10-10 10:00:00'
    })
    
    partitions = planner.find_recent_partitions(db, "sales_db", group_name="daily_incremental")
    
    assert len(partitions) == 0
    assert db.query.call_count == 2


def test_should_return_empty_partitions_when_no_group_tables(mocker):
    """Test that find_recent_partitions returns empty when no tables in group."""
    db = mocker.Mock()
    db.query.return_value = []
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'test_db_20251010_full',
        'backup_type': 'full',
        'finished_at': '2025-10-10 10:00:00'
    })
    
    partitions = planner.find_recent_partitions(db, "test_db", group_name="empty_group")
    
    assert len(partitions) == 0
    assert db.query.call_count == 1
