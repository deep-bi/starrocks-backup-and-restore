from starrocks_br import schema


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


def test_should_populate_table_inventory_with_sample_data(mocker):
    db = mocker.Mock()
    
    schema.initialize_ops_schema(db)
    
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
    
    assert "database_name" in table_inventory_schema
    assert "table_name" in table_inventory_schema
    assert "table_type" in table_inventory_schema
    assert "backup_eligible" in table_inventory_schema
    
    assert "job_id" in backup_history_schema
    assert "status" in backup_history_schema
    assert "bytes_backed_up" in backup_history_schema
    
    assert "job_id" in restore_history_schema
    assert "status" in restore_history_schema
    
    assert "scope" in run_status_schema
    assert "label" in run_status_schema
    assert "state" in run_status_schema
