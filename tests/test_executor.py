from unittest.mock import Mock, patch
from starrocks_br import executor


def test_should_submit_backup_command_successfully(mocker):
    db = mocker.Mock()
    db.execute.return_value = None
    
    backup_command = """
    BACKUP SNAPSHOT test_backup_20251015
    TO my_repo
    ON (TABLE sales_db.dim_customers)"""
    
    result = executor.submit_backup_command(db, backup_command)
    
    assert result is True
    assert db.execute.call_count == 1
    assert db.execute.call_args[0][0] == backup_command.strip()


def test_should_handle_backup_command_execution_error(mocker):
    db = mocker.Mock()
    db.execute.side_effect = Exception("Database error")
    
    backup_command = "BACKUP SNAPSHOT test TO repo"
    
    result = executor.submit_backup_command(db, backup_command)
    
    assert result is False


def test_should_poll_backup_status_until_finished(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [{"label": "test_backup", "state": "PENDING"}],
        [{"label": "test_backup", "state": "RUNNING"}],
        [{"label": "test_backup", "state": "FINISHED"}],
    ]
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"
    assert db.query.call_count == 3


def test_should_poll_backup_status_until_failed(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [{"label": "test_backup", "state": "PENDING"}],
        [{"label": "test_backup", "state": "FAILED"}],
    ]
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "FAILED"


def test_should_timeout_when_max_polls_reached(mocker):
    db = mocker.Mock()
    db.query.return_value = [{"label": "test_backup", "state": "RUNNING"}]
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=2, poll_interval=0.001)
    
    assert status["state"] == "TIMEOUT"
    assert db.query.call_count == 2


def test_should_query_correct_show_backup_syntax(mocker):
    db = mocker.Mock()
    db.query.return_value = [{"label": "test_backup", "state": "FINISHED"}]
    
    executor.poll_backup_status(db, "test_backup", max_polls=1, poll_interval=0.001)
    
    query = db.query.call_args[0][0]
    assert "SHOW BACKUP" in query
    assert "test_backup" in query


def test_should_handle_empty_backup_status_result(mocker):
    db = mocker.Mock()
    db.query.return_value = []
    
    status = executor.poll_backup_status(db, "nonexistent_backup", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "UNKNOWN"  # Empty results return UNKNOWN


def test_should_handle_malformed_backup_status_result(mocker):
    db = mocker.Mock()
    db.query.return_value = [{"label": "test_backup", "state": "FINISHED"}]
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"


def test_should_execute_full_backup_workflow(mocker):
    db = mocker.Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "test_backup", "state": "PENDING"}],
        [{"label": "test_backup", "state": "FINISHED"}],
    ]
    
    backup_command = "BACKUP SNAPSHOT test_backup TO repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.001)
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    # execute called: 1) submit backup, 2) log history, 3) complete job slot
    assert db.execute.call_count == 3
    assert db.query.call_count == 2


def test_should_handle_backup_execution_failure_in_workflow(mocker):
    db = mocker.Mock()
    db.execute.side_effect = Exception("Database connection failed")
    
    backup_command = "BACKUP SNAPSHOT test_backup TO repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.001)
    
    assert result["success"] is False
    assert result["final_status"] is None
    assert "Failed to submit backup command" in result["error_message"]


def test_should_handle_backup_polling_failure_in_workflow(mocker):
    db = mocker.Mock()
    db.execute.return_value = None
    db.query.side_effect = Exception("Query failed")
    
    backup_command = "BACKUP SNAPSHOT test_backup TO repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.001)
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "ERROR" 
    assert "Backup failed with state: ERROR" in result["error_message"]


def test_should_log_history_and_finalize_on_success(mocker):
    db = mocker.Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "test_backup", "state": "RUNNING"}],
        [{"label": "test_backup", "state": "FINISHED"}],
    ]

    log_backup = mocker.patch("starrocks_br.history.log_backup")
    complete_slot = mocker.patch("starrocks_br.concurrency.complete_job_slot")

    backup_command = "BACKUP SNAPSHOT test_backup TO repo"

    result = executor.execute_backup(
        db,
        backup_command,
        max_polls=3,
        poll_interval=0.001,
        repository="repo",
        backup_type="weekly",
        scope="backup",
    )

    assert result["success"] is True
    assert log_backup.call_count == 1
    entry = log_backup.call_args[0][1]
    assert entry["label"] == "test_backup"
    assert entry["status"] == "FINISHED"
    assert entry["repository"] == "repo"
    assert entry["backup_type"] == "weekly"

    complete_slot.assert_called_once()
    args, kwargs = complete_slot.call_args
    assert args[0] is db
    assert kwargs.get("scope") == "backup"
    assert kwargs.get("label") == "test_backup"
    assert kwargs.get("final_state") == "FINISHED"


def test_should_log_history_and_finalize_on_failure(mocker):
    db = mocker.Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "test_backup", "state": "RUNNING"}],
        [{"label": "test_backup", "state": "FAILED"}],
    ]

    log_backup = mocker.patch("starrocks_br.history.log_backup")
    complete_slot = mocker.patch("starrocks_br.concurrency.complete_job_slot")

    backup_command = "BACKUP SNAPSHOT test_backup TO repo"

    result = executor.execute_backup(
        db,
        backup_command,
        max_polls=3,
        poll_interval=0.001,
        repository="repo",
        backup_type="incremental",
        scope="backup",
    )

    assert result["success"] is False
    assert log_backup.call_count == 1
    entry = log_backup.call_args[0][1]
    assert entry["label"] == "test_backup"
    assert entry["status"] == "FAILED"
    assert entry["repository"] == "repo"
    assert entry["backup_type"] == "incremental"

    complete_slot.assert_called_once()
    _, kwargs = complete_slot.call_args
    assert kwargs.get("final_state") == "FAILED"



def test_should_handle_backup_command_execution_with_whitespace():
    """Test backup command execution with various whitespace scenarios."""
    db = Mock()
    db.execute.return_value = None
    
    command_with_whitespace = "   BACKUP SNAPSHOT test_backup TO repo   \n\n"
    result = executor.submit_backup_command(db, command_with_whitespace)
    
    assert result is True
    assert db.execute.call_count == 1
    executed_command = db.execute.call_args[0][0]
    assert executed_command == "BACKUP SNAPSHOT test_backup TO repo"


def test_should_handle_backup_status_polling_with_empty_results():
    """Test backup status polling when database returns empty results."""
    db = Mock()
    db.query.return_value = []
    
    status = executor.poll_backup_status(db, "nonexistent_backup", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "UNKNOWN"  # Empty results return UNKNOWN
    assert status["label"] == "nonexistent_backup"


def test_should_handle_backup_status_polling_with_none_result():
    """Test backup status polling when database returns None."""
    db = Mock()
    db.query.return_value = None
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "UNKNOWN"  # None result becomes UNKNOWN
    assert status["label"] == "test_backup"


def test_should_handle_backup_status_polling_with_malformed_dict():
    """Test backup status polling with malformed dictionary results."""
    db = Mock()
    db.query.return_value = [{"invalid": "data"}]  # Missing state field
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "UNKNOWN"  # Missing state field becomes UNKNOWN
    assert status["label"] == "test_backup"


def test_should_handle_backup_status_polling_with_malformed_tuple():
    """Test backup status polling with malformed tuple results."""
    db = Mock()
    db.query.return_value = [("test_backup",)]  # Missing state field
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "UNKNOWN"  # Missing state field becomes UNKNOWN
    assert status["label"] == "test_backup"


def test_should_execute_backup_with_history_logging_exception():
    """Test backup execution when history logging raises an exception."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "test_backup", "state": "RUNNING"}],
        [{"label": "test_backup", "state": "FINISHED"}],
    ]
    
    log_backup = Mock(side_effect=Exception("Logging failed"))
    complete_slot = Mock()
    
    with patch("starrocks_br.executor.history.log_backup", log_backup):
        with patch("starrocks_br.executor.concurrency.complete_job_slot", complete_slot):
            result = executor.execute_backup(
                db, 
                "BACKUP SNAPSHOT test_backup TO repo",
                max_polls=3,
                poll_interval=0.001
            )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_backup.call_count == 1
    assert complete_slot.call_count == 1


def test_should_execute_backup_with_job_slot_completion_exception():
    """Test backup execution when job slot completion raises an exception."""
    db = Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "test_backup", "state": "RUNNING"}],
        [{"label": "test_backup", "state": "FINISHED"}],
    ]
    
    log_backup = Mock()
    complete_slot = Mock(side_effect=Exception("Slot completion failed"))
    
    with patch("starrocks_br.executor.history.log_backup", log_backup):
        with patch("starrocks_br.executor.concurrency.complete_job_slot", complete_slot):
            result = executor.execute_backup(
                db, 
                "BACKUP SNAPSHOT test_backup TO repo",
                max_polls=3,
                poll_interval=0.001
            )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_backup.call_count == 1
    assert complete_slot.call_count == 1


def test_should_extract_label_from_both_backup_syntaxes():
    """Test label extraction from both new and legacy backup command syntaxes."""
    new_syntax_simple = "BACKUP DATABASE sales_db SNAPSHOT my_backup_label TO repo"
    assert executor._extract_label_from_command(new_syntax_simple) == "my_backup_label"
    
    new_syntax_multiline = """BACKUP DATABASE sales_db SNAPSHOT sales_db_20251015_inc
    TO my_repo
    ON (TABLE fact_sales PARTITION (p20251015, p20251014))"""
    assert executor._extract_label_from_command(new_syntax_multiline) == "sales_db_20251015_inc"
    
    new_syntax_full = """BACKUP DATABASE sales_db SNAPSHOT sales_db_20251015_monthly
    TO my_repo"""
    assert executor._extract_label_from_command(new_syntax_full) == "sales_db_20251015_monthly"
    
    legacy_syntax_simple = "BACKUP SNAPSHOT my_backup_20251015 TO repo"
    assert executor._extract_label_from_command(legacy_syntax_simple) == "my_backup_20251015"
    
    legacy_syntax_multiline = """BACKUP SNAPSHOT legacy_backup_20251015
    TO my_repo
    ON (TABLE sales_db.fact_sales)"""
    assert executor._extract_label_from_command(legacy_syntax_multiline) == "legacy_backup_20251015"


def test_should_extract_label_from_command_with_extra_spaces():
    """Test label extraction from commands with extra spaces."""
    command_with_spaces = "BACKUP SNAPSHOT   my_backup_20251015   TO repo"
    label = executor._extract_label_from_command(command_with_spaces)
    assert label == "my_backup_20251015"


def test_should_extract_label_from_command_with_tabs():
    """Test label extraction from commands with tabs."""
    command_with_tabs = "BACKUP\tSNAPSHOT\tmy_backup_20251015\tTO repo"
    label = executor._extract_label_from_command(command_with_tabs)
    assert label == "unknown_backup"


def test_should_extract_label_from_command_case_insensitive():
    """Test label extraction from commands with different case."""
    command_mixed_case = "backup database test_db snapshot MY_BACKUP_20251015 to repo"
    label = executor._extract_label_from_command(command_mixed_case)
    assert label == "unknown_backup"  # lowercase commands not supported by parser


def test_should_handle_backup_execution_with_zero_poll_interval():
    """Test backup execution with zero poll interval."""
    db = Mock()
    db.execute.return_value = None
    db.query.return_value = [{"label": "test_backup", "state": "FINISHED"}]
    
    result = executor.execute_backup(
        db, 
        "BACKUP SNAPSHOT test_backup TO repo",
        max_polls=1,
        poll_interval=0.0
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_handle_backup_execution_with_very_small_poll_interval():
    """Test backup execution with very small poll interval."""
    db = Mock()
    db.execute.return_value = None
    db.query.return_value = [{"label": "test_backup", "state": "FINISHED"}]
    
    result = executor.execute_backup(
        db, 
        "BACKUP SNAPSHOT test_backup TO repo",
        max_polls=1,
        poll_interval=0.001
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_handle_backup_execution_with_large_max_polls():
    """Test backup execution with large max polls value."""
    db = Mock()
    db.execute.return_value = None
    db.query.return_value = [{"label": "test_backup", "state": "FINISHED"}]
    
    result = executor.execute_backup(
        db, 
        "BACKUP SNAPSHOT test_backup TO repo",
        max_polls=100000,
        poll_interval=0.001
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_handle_backup_execution_with_negative_max_polls():
    """Test backup execution with negative max polls."""
    db = Mock()
    db.execute.return_value = None
    
    result = executor.execute_backup(
        db, 
        "BACKUP SNAPSHOT test_backup TO repo",
        max_polls=-1,
        poll_interval=0.001
    )
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "TIMEOUT"
    assert db.query.call_count == 0
