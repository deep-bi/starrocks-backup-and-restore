from starrocks_br import restore
from starrocks_br import history


def test_should_build_partition_restore_command():
    command = restore.build_partition_restore_command(
        database="sales_db",
        table="fact_sales",
        partition="p20251015",
        backup_label="sales_db_20251015_inc",
        repository="my_repo",
    )
    
    expected = """
    RESTORE SNAPSHOT sales_db_20251015_inc
    FROM my_repo
    ON (TABLE sales_db.fact_sales PARTITION (p20251015))"""
    
    assert command == expected


def test_should_build_table_restore_command():
    command = restore.build_table_restore_command(
        database="sales_db",
        table="dim_customers",
        backup_label="weekly_backup_20251015",
        repository="my_repo",
    )
    
    expected = """
    RESTORE SNAPSHOT weekly_backup_20251015
    FROM my_repo
    ON (TABLE sales_db.dim_customers)"""
    
    assert command == expected


def test_should_build_database_restore_command():
    command = restore.build_database_restore_command(
        database="sales_db",
        backup_label="sales_db_20251015_monthly",
        repository="my_repo",
    )
    
    expected = """
    RESTORE DATABASE sales_db
    FROM my_repo
    SNAPSHOT sales_db_20251015_monthly"""
    
    assert command == expected


def test_should_poll_restore_status_until_finished(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [{"label": "restore_job", "state": "PENDING"}],
        [{"label": "restore_job", "state": "RUNNING"}],
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", max_polls=5, poll_interval=0.1)
    
    assert status["state"] == "FINISHED"
    assert db.query.call_count == 3


def test_should_poll_restore_status_until_failed(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [{"label": "restore_job", "state": "PENDING"}],
        [{"label": "restore_job", "state": "FAILED"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", max_polls=5, poll_interval=0.1)
    
    assert status["state"] == "FAILED"


def test_should_query_correct_show_restore_syntax(mocker):
    db = mocker.Mock()
    db.query.return_value = [{"label": "restore_job", "state": "FINISHED"}]
    
    restore.poll_restore_status(db, "restore_job", max_polls=1, poll_interval=0.1)
    
    query = db.query.call_args[0][0]
    assert "SHOW RESTORE" in query
    assert "restore_job" in query


def test_should_log_restore_history(mocker):
    db = mocker.Mock()
    
    entry = {
        "job_id": "restore-1",
        "backup_label": "sales_db_20251015_inc",
        "restore_type": "partition",
        "status": "FINISHED",
        "repository": "my_repo",
        "started_at": "2025-10-15 02:00:00",
        "finished_at": "2025-10-15 02:10:00",
        "error_message": None,
    }
    
    history.log_restore(db, entry)
    
    assert db.execute.call_count == 1
    sql = db.execute.call_args[0][0]
    assert "INSERT INTO ops.restore_history" in sql
    assert "sales_db_20251015_inc" in sql


def test_should_execute_restore_workflow(mocker):
    db = mocker.Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore_job", "state": "PENDING"}],
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    log_restore = mocker.patch("starrocks_br.history.log_restore")
    complete_slot = mocker.patch("starrocks_br.concurrency.complete_job_slot")
    
    restore_command = """
    RESTORE SNAPSHOT sales_db_20251015_inc
    FROM my_repo
    ON (TABLE sales_db.fact_sales PARTITION (p20251015))"""
    
    result = restore.execute_restore(
        db,
        restore_command,
        backup_label="sales_db_20251015_inc",
        restore_type="partition",
        repository="my_repo",
        max_polls=5,
        poll_interval=0.1,
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
