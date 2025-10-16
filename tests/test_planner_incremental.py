from starrocks_br import planner


def test_should_find_partitions_updated_in_last_n_days(mocker):
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "fact_sales", "p20251015", "2025-10-15"),
        ("sales_db", "fact_sales", "p20251014", "2025-10-14"),
    ]
    
    partitions = planner.find_recent_partitions(db, days=7)
    
    assert len(partitions) == 2
    assert {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"} in partitions
    assert {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251014"} in partitions
    assert db.query.call_count == 1


def test_should_build_incremental_backup_command():
    partitions = [
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251015"},
        {"database": "sales_db", "table": "fact_sales", "partition_name": "p20251014"},
    ]
    repository = "my_repo"
    label = "sales_db_20251015_inc"
    
    command = planner.build_incremental_backup_command(partitions, repository, label)
    
    expected = """
    BACKUP SNAPSHOT sales_db_20251015_inc
    TO my_repo
    ON (TABLE sales_db.fact_sales PARTITION (p20251015, p20251014))"""
    
    assert command == expected


def test_should_handle_empty_partitions_list():
    command = planner.build_incremental_backup_command([], "my_repo", "label")
    assert command == "" or "no partitions" in command.lower()


def test_should_handle_single_partition():
    partitions = [{"database": "db1", "table": "table1", "partition_name": "p1"}]
    command = planner.build_incremental_backup_command(partitions, "repo", "label")
    
    assert "TABLE db1.table1 PARTITION (p1)" in command
    assert "BACKUP SNAPSHOT label" in command
    assert "TO repo" in command


def test_should_format_date_correctly_in_query(mocker):
    db = mocker.Mock()
    db.query.return_value = []
    
    planner.find_recent_partitions(db, days=3)
    
    query = db.query.call_args[0][0]
    assert "information_schema.partitions" in query
    assert "WHERE" in query
