from starrocks_br import schema

def populate_table_inventory_for_testing(db) -> None:
    sample_data = [
        ("daily_incremental", "sales_db", "fact_sales"),
        ("daily_incremental", "orders_db", "fact_orders"),
        
        ("weekly_dimensions", "sales_db", "dim_customers"),
        ("weekly_dimensions", "sales_db", "dim_products"),
        ("weekly_dimensions", "orders_db", "dim_regions"),
        
        ("monthly_full", "config_db", "*"),
        ("monthly_full", "sales_db", "*"),
    ]
    
    for data in sample_data:
        db.execute("""
            INSERT INTO ops.table_inventory 
            (inventory_group, database_name, table_name)
            VALUES ('%s', '%s', '%s');
        """ % data)

def test_should_create_ops_database(mocker):
    db = mocker.Mock()
    
    schema.initialize_ops_schema(db)
    
    create_db_calls = [call for call in db.execute.call_args_list if "CREATE DATABASE" in call[0][0]]
    assert len(create_db_calls) >= 1
    assert "CREATE DATABASE IF NOT EXISTS ops" in create_db_calls[0][0][0]


def test_should_create_all_required_tables(mocker):
    db = mocker.Mock()
    
    schema.initialize_ops_schema(db)
    
    executed_sqls = [call[0][0] for call in db.execute.call_args_list]
    
    assert any("ops.table_inventory" in sql for sql in executed_sqls)
    assert any("ops.backup_history" in sql for sql in executed_sqls)
    assert any("ops.restore_history" in sql for sql in executed_sqls)
    assert any("ops.run_status" in sql for sql in executed_sqls)
    assert any("ops.backup_partitions" in sql for sql in executed_sqls)


def test_should_populate_table_inventory_with_sample_data(mocker):
    db = mocker.Mock()
    
    populate_table_inventory_for_testing(db)
    
    insert_calls = [call for call in db.execute.call_args_list if "INSERT INTO ops.table_inventory" in call[0][0]]
    assert len(insert_calls) > 0


def test_should_handle_existing_database_gracefully(mocker):
    db = mocker.Mock()
    
    schema.initialize_ops_schema(db)
    
    executed_sqls = [call[0][0] for call in db.execute.call_args_list]
    assert any("ops.table_inventory" in sql for sql in executed_sqls)


def test_should_define_proper_table_structures():
    table_inventory_schema = schema.get_table_inventory_schema()
    backup_history_schema = schema.get_backup_history_schema()
    restore_history_schema = schema.get_restore_history_schema()
    run_status_schema = schema.get_run_status_schema()
    backup_partitions_schema = schema.get_backup_partitions_schema()
    
    assert "inventory_group" in table_inventory_schema
    assert "database_name" in table_inventory_schema
    assert "table_name" in table_inventory_schema
    
    assert "label" in backup_history_schema
    assert "backup_type" in backup_history_schema
    assert "status" in backup_history_schema
    
    assert "job_id" in restore_history_schema
    assert "status" in restore_history_schema
    
    assert "scope" in run_status_schema
    assert "label" in run_status_schema
    assert "state" in run_status_schema
    
    assert "label" in backup_partitions_schema
    assert "database_name" in backup_partitions_schema
    assert "table_name" in backup_partitions_schema
    assert "partition_name" in backup_partitions_schema


def test_should_create_table_inventory_with_unique_key():
    """Test that table_inventory table uses UNIQUE KEY instead of PRIMARY KEY"""
    schema_sql = schema.get_table_inventory_schema()

    # Check for required fields
    assert "inventory_group STRING NOT NULL" in schema_sql
    assert "database_name STRING NOT NULL" in schema_sql
    assert "table_name STRING NOT NULL" in schema_sql

    # Check for unique key (not primary key)
    assert "UNIQUE KEY (inventory_group, database_name, table_name)" in schema_sql

    # Check for distribution
    assert "DISTRIBUTED BY HASH(inventory_group)" in schema_sql

    # Verify PRIMARY KEY is not present
    assert "PRIMARY KEY" not in schema_sql


def test_ensure_ops_schema_when_database_does_not_exist(mocker):
    """Test ensure_ops_schema creates schema when ops database doesn't exist"""
    db = mocker.Mock()
    db.query.return_value = []
    mock_init = mocker.patch('starrocks_br.schema.initialize_ops_schema')
    
    result = schema.ensure_ops_schema(db)
    
    assert result is True
    mock_init.assert_called_once_with(db)
    db.query.assert_called_once()


def test_ensure_ops_schema_when_tables_are_missing(mocker):
    """Test ensure_ops_schema reinitializes when some tables are missing"""
    db = mocker.Mock()
    db.query.side_effect = [
        [("ops",)],
        [("table1",), ("table2",)]
    ]
    mock_init = mocker.patch('starrocks_br.schema.initialize_ops_schema')
    
    result = schema.ensure_ops_schema(db)
    
    assert result is True
    mock_init.assert_called_once_with(db)
    assert db.query.call_count == 2


def test_ensure_ops_schema_when_all_tables_exist(mocker):
    """Test ensure_ops_schema returns False when everything exists"""
    db = mocker.Mock()
    db.query.side_effect = [
        [("ops",)],
        [("table1",), ("table2",), ("table3",), ("table4",), ("table5",)]
    ]
    mock_init = mocker.patch('starrocks_br.schema.initialize_ops_schema')
    
    result = schema.ensure_ops_schema(db)
    
    assert result is False
    mock_init.assert_not_called()
    assert db.query.call_count == 2


def test_ensure_ops_schema_handles_exceptions_gracefully(mocker):
    """Test ensure_ops_schema handles exceptions by attempting initialization"""
    db = mocker.Mock()
    db.query.side_effect = Exception("Database error")
    mock_init = mocker.patch('starrocks_br.schema.initialize_ops_schema')
    
    result = schema.ensure_ops_schema(db)
    
    assert result is True
    mock_init.assert_called_once_with(db)


def test_ensure_ops_schema_when_tables_result_is_none(mocker):
    """Test ensure_ops_schema handles None result from SHOW TABLES"""
    db = mocker.Mock()
    db.query.side_effect = [
        [("ops",)],
        None
    ]
    mock_init = mocker.patch('starrocks_br.schema.initialize_ops_schema')
    
    result = schema.ensure_ops_schema(db)
    
    assert result is True
    mock_init.assert_called_once_with(db)


def test_should_create_backup_partitions_table_with_correct_structure():
    """Test that backup_partitions table has the correct schema structure"""
    schema_sql = schema.get_backup_partitions_schema()

    # Check for required fields
    assert "key_hash STRING NOT NULL" in schema_sql
    assert "label STRING NOT NULL" in schema_sql
    assert "database_name STRING NOT NULL" in schema_sql
    assert "table_name STRING NOT NULL" in schema_sql
    assert "partition_name STRING NOT NULL" in schema_sql
    assert "created_at DATETIME DEFAULT CURRENT_TIMESTAMP" in schema_sql

    # Check for primary key (hash-based)
    assert "PRIMARY KEY (key_hash)" in schema_sql

    # Check for distribution
    assert "DISTRIBUTED BY HASH(key_hash)" in schema_sql

    # Check for comments
    assert "MD5 hash of composite key" in schema_sql
    assert "FK to ops.backup_history.label" in schema_sql
    assert "Tracks every partition included in a backup snapshot" in schema_sql


def test_should_include_backup_partitions_in_initialization(mocker):
    """Test that backup_partitions table is created during schema initialization"""
    db = mocker.Mock()
    
    schema.initialize_ops_schema(db)
    
    executed_sqls = [call[0][0] for call in db.execute.call_args_list]
    backup_partitions_calls = [sql for sql in executed_sqls if "ops.backup_partitions" in sql]
    
    assert len(backup_partitions_calls) == 1
    assert "CREATE TABLE IF NOT EXISTS ops.backup_partitions" in backup_partitions_calls[0]
