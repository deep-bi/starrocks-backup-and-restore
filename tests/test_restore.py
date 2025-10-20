from unittest.mock import Mock, patch
from starrocks_br import restore
from starrocks_br import history


def test_should_build_partition_restore_command():
    command = restore.build_partition_restore_command(
        database="sales_db",
        table="fact_sales",
        partition="p20251015",
        backup_label="sales_db_20251015_incremental",
        repository="my_repo",
    )
    
    expected = """
    RESTORE SNAPSHOT sales_db_20251015_incremental
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
    
    status = restore.poll_restore_status(db, "restore_job", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"
    assert db.query.call_count == 3


def test_should_poll_restore_status_until_failed(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [{"label": "restore_job", "state": "PENDING"}],
        [{"label": "restore_job", "state": "FAILED"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "FAILED"


def test_should_query_correct_show_restore_syntax(mocker):
    db = mocker.Mock()
    db.query.return_value = [{"label": "restore_job", "state": "FINISHED"}]
    
    restore.poll_restore_status(db, "restore_job", max_polls=1, poll_interval=0.001)
    
    query = db.query.call_args[0][0]
    assert "SHOW RESTORE" in query
    assert "restore_job" in query


def test_should_log_restore_history(mocker):
    db = mocker.Mock()
    
    entry = {
        "job_id": "restore-1",
        "backup_label": "sales_db_20251015_incremental",
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
    assert "sales_db_20251015_incremental" in sql


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
        backup_label="sales_db_20251015_incremental",
        restore_type="partition",
        repository="my_repo",
        max_polls=5,
        poll_interval=0.001,
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_build_partition_restore_command_with_special_characters():
    """Test partition restore command building with special characters."""
    command = restore.build_partition_restore_command(
        database="test-db_2025",
        table="fact-table.sales",
        partition="p2025-01-15",
        backup_label="backup_2025.01.15",
        repository="repo-with-special.chars"
    )
    
    assert "RESTORE SNAPSHOT backup_2025.01.15" in command
    assert "FROM repo-with-special.chars" in command
    assert "TABLE test-db_2025.fact-table.sales" in command
    assert "PARTITION (p2025-01-15)" in command


def test_should_build_table_restore_command_with_special_characters():
    """Test table restore command building with special characters."""
    command = restore.build_table_restore_command(
        database="test-db_2025",
        table="fact-table.sales",
        backup_label="backup_2025.01.15",
        repository="repo-with-special.chars"
    )
    
    assert "RESTORE SNAPSHOT backup_2025.01.15" in command
    assert "FROM repo-with-special.chars" in command
    assert "TABLE test-db_2025.fact-table.sales" in command


def test_should_build_database_restore_command_with_special_characters():
    """Test database restore command building with special characters."""
    command = restore.build_database_restore_command(
        database="test-db_2025",
        backup_label="backup_2025.01.15",
        repository="repo-with-special.chars"
    )
    
    assert "RESTORE DATABASE test-db_2025" in command
    assert "FROM repo-with-special.chars" in command
    assert "SNAPSHOT backup_2025.01.15" in command


def test_should_poll_restore_status_with_tuple_results():
    """Test polling restore status when database returns tuple results."""
    db = Mock()
    db.query.side_effect = [
        [("restore_job", "PENDING", "2025-01-15 10:00:00")],
        [("restore_job", "RUNNING", "2025-01-15 10:01:00")],
        [("restore_job", "FINISHED", "2025-01-15 10:05:00")],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"
    assert status["label"] == "restore_job"
    assert db.query.call_count == 3


def test_should_poll_restore_status_with_malformed_results():
    """Test polling restore status with malformed database results."""
    db = Mock()
    db.query.side_effect = [
        [{"label": "restore_job"}],  # Missing state field
        [{"state": "RUNNING"}],      # Missing label field
        [("restore_job",)],          # Missing state in tuple
        [{"label": "restore_job", "state": "UNKNOWN"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "UNKNOWN"
    assert status["label"] == "restore_job"


def test_should_handle_restore_status_query_exceptions():
    """Test handling of exceptions during restore status queries."""
    db = Mock()
    db.query.side_effect = [
        Exception("Query timeout"),
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "ERROR"
    assert status["label"] == "restore_job"


def test_should_handle_restore_status_with_cancelled_state():
    """Test handling of CANCELLED restore state."""
    db = Mock()
    db.query.side_effect = [
        [{"label": "restore_job", "state": "RUNNING"}],
        [{"label": "restore_job", "state": "CANCELLED"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "CANCELLED"
    assert status["label"] == "restore_job"


def test_should_timeout_when_max_polls_reached_for_restore():
    """Test restore status polling timeout."""
    db = Mock()
    db.query.return_value = [{"label": "restore_job", "state": "RUNNING"}]
    
    status = restore.poll_restore_status(db, "restore_job", max_polls=2, poll_interval=0.001)
    
    assert status["state"] == "TIMEOUT"
    assert db.query.call_count == 2


def test_should_query_correct_show_restore_syntax_with_label():
    """Test that correct SHOW RESTORE query syntax is used."""
    db = Mock()
    db.query.return_value = [{"label": "restore_job", "state": "FINISHED"}]
    
    restore.poll_restore_status(db, "restore_job", max_polls=1, poll_interval=0.001)
    
    query = db.query.call_args[0][0]
    assert "SHOW RESTORE" in query
    assert "restore_job" in query


def test_should_handle_empty_restore_status_result():
    """Test handling of empty restore status results."""
    db = Mock()
    db.query.return_value = []
    
    status = restore.poll_restore_status(db, "nonexistent_restore", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "UNKNOWN"


def test_should_execute_restore_with_custom_polling_parameters():
    """Test restore execution with custom polling parameters."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore_job", "state": "PENDING"}],
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    result = restore.execute_restore(
        db, 
        restore_command, 
        backup_label="restore_job",
        restore_type="partition",
        repository="custom_repo",
        max_polls=10, 
        poll_interval=0.5
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_execute_restore_with_history_logging_failure():
    """Test restore execution when history logging fails."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore_job", "state": "RUNNING"}],
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    log_restore = Mock(side_effect=Exception("Logging failed"))
    complete_slot = Mock()
    
    with patch("starrocks_br.restore.history.log_restore", log_restore):
        with patch("starrocks_br.restore.concurrency.complete_job_slot", complete_slot):
            result = restore.execute_restore(
                db, 
                "RESTORE SNAPSHOT restore_job FROM repo",
                backup_label="restore_job",
                restore_type="partition",
                repository="test_repo",
                max_polls=3,
                poll_interval=0.001
            )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_restore.call_count == 1
    assert complete_slot.call_count == 1


def test_should_execute_restore_with_job_slot_completion_failure():
    """Test restore execution when job slot completion fails."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore_job", "state": "RUNNING"}],
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    log_restore = Mock()
    complete_slot = Mock(side_effect=Exception("Slot completion failed"))
    
    with patch("starrocks_br.restore.history.log_restore", log_restore):
        with patch("starrocks_br.restore.concurrency.complete_job_slot", complete_slot):
                result = restore.execute_restore(
                    db, 
                    "RESTORE SNAPSHOT restore_job FROM repo",
                    backup_label="restore_job",
                    restore_type="partition",
                    repository="test_repo",
                    max_polls=3,
                    poll_interval=0.001
                )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_restore.call_count == 1
    assert complete_slot.call_count == 1


def test_should_execute_restore_with_both_logging_and_slot_failures():
    """Test restore execution when both history logging and job slot completion fail."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore_job", "state": "RUNNING"}],
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    log_restore = Mock(side_effect=Exception("Logging failed"))
    complete_slot = Mock(side_effect=Exception("Slot completion failed"))
    
    with patch("starrocks_br.restore.history.log_restore", log_restore):
        with patch("starrocks_br.restore.concurrency.complete_job_slot", complete_slot):
                result = restore.execute_restore(
                    db, 
                    "RESTORE SNAPSHOT restore_job FROM repo",
                    backup_label="restore_job",
                    restore_type="partition",
                    repository="test_repo",
                    max_polls=3,
                    poll_interval=0.001
                )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_restore.call_count == 1
    assert complete_slot.call_count == 1


def test_should_handle_restore_execution_with_very_long_polling():
    """Test restore execution with very long polling duration."""
    db = Mock()
    db.execute.return_value = None
    db.query.return_value = [{"label": "restore_job", "state": "RUNNING"}]
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    result = restore.execute_restore(
        db, 
        restore_command, 
        backup_label="restore_job",
        restore_type="partition",
        repository="test_repo",
        max_polls=3, 
        poll_interval=0.001
    )
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "TIMEOUT"
    assert db.query.call_count == 3


def test_should_handle_restore_execution_with_zero_polls():
    """Test restore execution with zero max polls."""
    db = Mock()
    db.execute.return_value = None
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    result = restore.execute_restore(
        db, 
        restore_command, 
        backup_label="restore_job",
        restore_type="partition",
        repository="test_repo",
        max_polls=0, 
        poll_interval=0.001
    )
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "TIMEOUT"
    assert db.query.call_count == 0


def test_should_execute_restore_with_different_scope_values():
    """Test restore execution with different scope values."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore_job", "state": "RUNNING"}],
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    complete_slot = Mock()
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    scope = "restore"
    
    with patch("starrocks_br.restore.concurrency.complete_job_slot", complete_slot):
        result = restore.execute_restore(
            db, 
            restore_command, 
            backup_label="restore_job",
            restore_type="partition",
            repository="test_repo",
            scope=scope,
            max_polls=3,
            poll_interval=0.001
        )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    
    _, kwargs = complete_slot.call_args
    assert kwargs.get("scope") == scope


def test_should_handle_restore_execution_with_intermittent_query_failures():
    """Test restore execution with intermittent query failures."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        Exception("Temporary network error"),
        [{"label": "restore_job", "state": "RUNNING"}],
        Exception("Another temporary error"),
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    result = restore.execute_restore(
        db, 
        restore_command, 
        backup_label="restore_job",
        restore_type="partition",
        repository="test_repo",
        max_polls=5, 
        poll_interval=0.001
    )
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "ERROR"


def test_should_handle_restore_command_submission_failure():
    """Test restore execution when command submission fails."""
    db = Mock()
    db.execute.side_effect = Exception("Permission denied")
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    result = restore.execute_restore(
        db, 
        restore_command, 
        backup_label="restore_job",
        restore_type="partition",
        repository="test_repo"
    )
    
    assert result["success"] is False
    assert result["final_status"] is None
    assert "Failed to submit restore command" in result["error_message"]


def test_should_execute_restore_with_multiline_command():
    """Test restore execution with multiline restore commands."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore_job", "state": "RUNNING"}],
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    multiline_command = """
    RESTORE SNAPSHOT complex_backup_2025-01-15
    FROM my_repo
    ON (TABLE sales_db.fact_sales PARTITION (p20250115), TABLE sales_db.dim_customers)
    """
    
    result = restore.execute_restore(
        db,
        multiline_command,
        backup_label="complex_backup_2025-01-15",
        restore_type="partition",
        repository="my_repo",
        max_polls=3,
        poll_interval=0.001
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    
    restore_call = db.execute.call_args_list[0][0][0]
    assert "RESTORE SNAPSHOT complex_backup_2025-01-15" in restore_call
    assert "FROM my_repo" in restore_call


def test_should_execute_restore_with_special_characters_in_names():
    """Test restore execution with special characters in backup names."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore-job_2025.01.15", "state": "RUNNING"}],
        [{"label": "restore-job_2025.01.15", "state": "FINISHED"}],
    ]
    
    restore_command = "RESTORE SNAPSHOT restore-job_2025.01.15 FROM repo-with-special.chars"
    
    result = restore.execute_restore(
        db,
        restore_command,
        backup_label="restore-job_2025.01.15",
        restore_type="table",
        repository="repo-with-special.chars",
        max_polls=3,
        poll_interval=0.001
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_log_restore_history_with_correct_parameters():
    """Test that restore history is logged with correct parameters."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore_job", "state": "RUNNING"}],
        [{"label": "restore_job", "state": "FINISHED"}],
    ]
    
    log_restore = Mock()
    complete_slot = Mock()
    
    with patch("starrocks_br.restore.history.log_restore", log_restore):
        with patch("starrocks_br.restore.concurrency.complete_job_slot", complete_slot):
            result = restore.execute_restore(
                db,
                "RESTORE SNAPSHOT restore_job FROM repo",
                backup_label="restore_job",
                restore_type="partition",
                repository="test_repo",
                scope="restore",
                max_polls=3,
                poll_interval=0.001
            )
    
    assert result["success"] is True
    
    entry = log_restore.call_args[0][1]
    assert entry["job_id"] == "restore_job"
    assert entry["status"] == "FINISHED"
    assert entry["repository"] == "test_repo"
    assert entry["restore_type"] == "partition"


def test_should_log_restore_history_with_failure_state():
    """Test that restore history is logged correctly for failed restores."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "restore_job", "state": "RUNNING"}],
        [{"label": "restore_job", "state": "FAILED"}],
    ]
    
    log_restore = Mock()
    complete_slot = Mock()
    
    with patch("starrocks_br.restore.history.log_restore", log_restore):
        with patch("starrocks_br.restore.concurrency.complete_job_slot", complete_slot):
            result = restore.execute_restore(
                db,
                "RESTORE SNAPSHOT restore_job FROM repo",
                backup_label="restore_job",
                restore_type="partition",
                repository="test_repo",
                max_polls=3,
                poll_interval=0.001
            )
    
    assert result["success"] is False
    
    entry = log_restore.call_args[0][1]
    assert entry["job_id"] == "restore_job"
    assert entry["status"] == "FAILED"
    assert entry["repository"] == "test_repo"
    assert entry["restore_type"] == "partition"
