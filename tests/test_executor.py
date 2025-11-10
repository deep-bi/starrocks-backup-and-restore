from unittest.mock import Mock, patch
import pytest
from starrocks_br import executor


@pytest.fixture
def db_with_timezone():
    """Fixture that provides a mock database object with timezone property set to UTC."""
    db = Mock()
    db.timezone = "UTC"
    return db


def test_should_submit_backup_command_successfully(mocker):
    db = mocker.Mock()
    db.execute.return_value = None
    
    backup_command = """
    BACKUP SNAPSHOT test_backup_20251015
    TO my_repo
    ON (TABLE sales_db.dim_customers)"""
    
    success, error = executor.submit_backup_command(db, backup_command)
    
    assert success is True
    assert error is None
    assert db.execute.call_count == 1
    assert db.execute.call_args[0][0] == backup_command.strip()


def test_should_handle_backup_command_execution_error(mocker):
    db = mocker.Mock()
    db.execute.side_effect = Exception("Database error")
    
    backup_command = "BACKUP SNAPSHOT test TO repo"
    
    success, error = executor.submit_backup_command(db, backup_command)
    
    assert success is False
    assert error is not None
    assert "Failed to submit backup command" in error
    assert "Exception" in error
    assert "Database error" in error


def test_should_poll_backup_status_until_finished(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [("job1", "test_backup", "test_db", "PENDING")],
        [("job1", "test_backup", "test_db", "SNAPSHOTING")],
        [("job1", "test_backup", "test_db", "FINISHED")],
    ]
    
    status = executor.poll_backup_status(db, "test_backup", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"
    assert db.query.call_count == 3


def test_should_poll_backup_status_until_failed(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [("job1", "test_backup", "test_db", "PENDING")],
        [("job1", "test_backup", "test_db", "CANCELLED")],
    ]
    
    status = executor.poll_backup_status(db, "test_backup", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "CANCELLED"


def test_should_timeout_when_max_polls_reached(mocker):
    db = mocker.Mock()
    db.query.return_value = [("job1", "test_backup", "test_db", "UPLOADING")]
    
    status = executor.poll_backup_status(db, "test_backup", "test_db", max_polls=2, poll_interval=0.001)
    
    assert status["state"] == "TIMEOUT"
    assert db.query.call_count == 2


def test_should_query_correct_show_backup_syntax(mocker):
    db = mocker.Mock()
    db.query.return_value = [("job1", "test_backup", "test_db", "FINISHED")]
    
    executor.poll_backup_status(db, "test_backup", "test_db", max_polls=1, poll_interval=0.001)
    
    query = db.query.call_args[0][0]
    assert "SHOW BACKUP FROM test_db" in query


def test_should_handle_empty_backup_status_result(mocker):
    db = mocker.Mock()
    db.query.return_value = []
    
    status = executor.poll_backup_status(db, "nonexistent_backup", "test_db", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "TIMEOUT"  # Empty results eventually timeout


def test_should_handle_dict_format_backup_status(mocker):
    """Test handling StarRocks dict-format results."""
    db = mocker.Mock()
    db.query.return_value = [{"JobId": "1", "SnapshotName": "test_backup", "DbName": "test_db", "State": "FINISHED"}]
    
    status = executor.poll_backup_status(db, "test_backup", "test_db", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"


def test_should_execute_full_backup_workflow(mocker, db_with_timezone):
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [("job1", "test_backup", "test_db", "PENDING")],
        [("job1", "test_backup", "test_db", "FINISHED")],
    ]
    
    backup_command = "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.001, repository="repo", backup_type="full", scope="backup", database="test_db")
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    # execute called: 1) submit backup, 2) log history, 3) complete job slot
    assert db.execute.call_count == 3
    assert db.query.call_count == 2


def test_should_handle_backup_execution_failure_in_workflow(mocker, db_with_timezone):
    db = db_with_timezone
    db.execute.side_effect = Exception("Database connection failed")
    
    backup_command = "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.001, repository="repo", backup_type="full", scope="backup", database="test_db")
    
    assert result["success"] is False
    assert result["final_status"] is None
    assert "Failed to submit backup command" in result["error_message"]
    assert "Database connection failed" in result["error_message"]


def test_should_handle_backup_polling_failure_in_workflow(mocker, db_with_timezone):
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = Exception("Query failed")
    
    backup_command = "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.001, repository="repo", backup_type="full", scope="backup", database="test_db")
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "ERROR" 
    assert "Error occurred while monitoring backup" in result["error_message"]
    assert "test_backup" in result["error_message"]


def test_should_handle_lost_backup_in_workflow(mocker, db_with_timezone):
    """Test that execute_backup handles LOST state correctly (race condition detected)."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [("job1", "other_backup", "test_db", "FINISHED")],  # Wrong backup on first poll
        [("job1", "other_backup", "test_db", "FINISHED")],  # Still wrong - LOST!
    ]
    
    log_backup = mocker.patch("starrocks_br.history.log_backup")
    complete_slot = mocker.patch("starrocks_br.concurrency.complete_job_slot")
    
    backup_command = "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo"
    
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
    assert result["final_status"]["state"] == "LOST"
    assert "Backup tracking lost" in result["error_message"]
    assert "test_backup" in result["error_message"]
    assert "test_db" in result["error_message"]
    assert "concurrency issue" in result["error_message"]
    
    assert log_backup.call_count == 1
    entry = log_backup.call_args[0][1]
    assert entry["status"] == "LOST"
    
    complete_slot.assert_called_once()
    _, kwargs = complete_slot.call_args
    assert kwargs.get("final_state") == "LOST"


def test_should_log_history_and_finalize_on_success(mocker, db_with_timezone):
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [("job1", "test_backup", "test_db", "UPLOADING")],
        [("job1", "test_backup", "test_db", "FINISHED")],
    ]

    log_backup = mocker.patch("starrocks_br.history.log_backup")
    complete_slot = mocker.patch("starrocks_br.concurrency.complete_job_slot")

    backup_command = "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo"

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


def test_should_log_history_and_finalize_on_failure(mocker, db_with_timezone):
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [("job1", "test_backup", "test_db", "UPLOADING")],
        [("job1", "test_backup", "test_db", "CANCELLED")],
    ]

    log_backup = mocker.patch("starrocks_br.history.log_backup")
    complete_slot = mocker.patch("starrocks_br.concurrency.complete_job_slot")

    backup_command = "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo"

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
    assert entry["status"] == "CANCELLED"
    assert entry["repository"] == "repo"
    assert entry["backup_type"] == "incremental"

    complete_slot.assert_called_once()
    _, kwargs = complete_slot.call_args
    assert kwargs.get("final_state") == "CANCELLED"



def test_should_handle_backup_command_execution_with_whitespace():
    """Test backup command execution with various whitespace scenarios."""
    db = Mock()
    db.execute.return_value = None
    
    command_with_whitespace = "   BACKUP SNAPSHOT test_backup TO repo   \n\n"
    success, error = executor.submit_backup_command(db, command_with_whitespace)
    
    assert success is True
    assert error is None
    assert db.execute.call_count == 1
    executed_command = db.execute.call_args[0][0]
    assert executed_command == "BACKUP SNAPSHOT test_backup TO repo"


def test_should_handle_backup_status_polling_with_empty_results():
    """Test backup status polling when database returns empty results."""
    db = Mock()
    db.query.return_value = []
    
    status = executor.poll_backup_status(db, "nonexistent_backup", "test_db", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "TIMEOUT"  # Empty results eventually timeout
    assert status["label"] == "nonexistent_backup"


def test_should_detect_lost_backup_when_overwritten():
    """Test that we detect when another backup overwrites ours (race condition)."""
    db = Mock()
    # First poll: sees different backup (gives one chance)
    # Second poll: still different backup - ours was overwritten!
    db.query.side_effect = [
        [("job1", "other_backup", "test_db", "FINISHED")],  # Wrong backup on first poll
        [("job1", "other_backup", "test_db", "FINISHED")],  # Still wrong on second poll - LOST!
    ]
    
    status = executor.poll_backup_status(db, "test_backup", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "LOST"
    assert status["label"] == "test_backup"


def test_should_recover_from_timing_issue_on_first_poll():
    """Test that we give one chance if wrong backup appears on first poll only."""
    db = Mock()
    db.query.side_effect = [
        [("job1", "other_backup", "test_db", "UPLOADING")],
        [("job2", "test_backup", "test_db", "FINISHED")],
    ]
    
    status = executor.poll_backup_status(db, "test_backup", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"
    assert status["label"] == "test_backup"


def test_should_handle_backup_status_polling_with_malformed_tuple():
    """Test backup status polling with malformed tuple results."""
    db = Mock()
    db.query.return_value = [("job1",)]  # Missing required fields
    
    status = executor.poll_backup_status(db, "test_backup", "test_db", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "TIMEOUT"  # Malformed data eventually times out
    assert status["label"] == "test_backup"


def test_should_execute_backup_with_history_logging_exception(db_with_timezone):
    """Test backup execution when history logging raises an exception."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [("job1", "test_backup", "test_db", "UPLOADING")],
        [("job1", "test_backup", "test_db", "FINISHED")],
    ]
    
    log_backup = Mock(side_effect=Exception("Logging failed"))
    complete_slot = Mock()
    
    with patch("starrocks_br.executor.history.log_backup", log_backup):
        with patch("starrocks_br.executor.concurrency.complete_job_slot", complete_slot):
            result = executor.execute_backup(
                db, 
                "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo",
                max_polls=3,
                poll_interval=0.001,
                repository="repo",
                backup_type="incremental",
                scope="backup",
                database="test_db"
            )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_backup.call_count == 1
    assert complete_slot.call_count == 1


def test_should_execute_backup_with_job_slot_completion_exception(db_with_timezone):
    """Test backup execution when job slot completion raises an exception."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [("job1", "test_backup", "test_db", "UPLOADING")],
        [("job1", "test_backup", "test_db", "FINISHED")],
    ]
    
    log_backup = Mock()
    complete_slot = Mock(side_effect=Exception("Slot completion failed"))
    
    with patch("starrocks_br.executor.history.log_backup", log_backup):
        with patch("starrocks_br.executor.concurrency.complete_job_slot", complete_slot):
            result = executor.execute_backup(
                db, 
                "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo",
                max_polls=3,
                poll_interval=0.001,
                repository="repo",
                backup_type="incremental",
                scope="backup",
                database="test_db"
            )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_backup.call_count == 1
    assert complete_slot.call_count == 1


def test_should_extract_label_from_both_backup_syntaxes():
    """Test label extraction from both new and legacy backup command syntaxes."""
    new_syntax_simple = "BACKUP DATABASE sales_db SNAPSHOT my_backup_label TO repo"
    assert executor._extract_label_from_command(new_syntax_simple) == "my_backup_label"
    
    new_syntax_multiline = """BACKUP DATABASE sales_db SNAPSHOT sales_db_20251015_incremental
    TO my_repo
    ON (TABLE fact_sales PARTITION (p20251015, p20251014))"""
    assert executor._extract_label_from_command(new_syntax_multiline) == "sales_db_20251015_incremental"
    
    new_syntax_full = """BACKUP DATABASE sales_db SNAPSHOT sales_db_20251015_full
    TO my_repo"""
    assert executor._extract_label_from_command(new_syntax_full) == "sales_db_20251015_full"
    
    legacy_syntax_simple = "BACKUP SNAPSHOT my_backup_20251015 TO repo"
    assert executor._extract_label_from_command(legacy_syntax_simple) == "my_backup_20251015"
    
    legacy_syntax_multiline = """BACKUP SNAPSHOT legacy_backup_20251015
    TO my_repo
    ON (TABLE sales_db.fact_sales)"""
    assert executor._extract_label_from_command(legacy_syntax_multiline) == "legacy_backup_20251015"


def test_should_extract_database_from_backup_command():
    """Test database extraction from backup commands."""
    simple_command = "BACKUP DATABASE sales_db SNAPSHOT my_label TO repo"
    assert executor._extract_database_from_command(simple_command) == "sales_db"
    
    multiline_command = """BACKUP DATABASE orders_db SNAPSHOT backup_20251016
    TO my_repo
    ON (TABLE fact_orders PARTITION (p20251015))"""
    assert executor._extract_database_from_command(multiline_command) == "orders_db"
    
    # Legacy syntax should return unknown_database
    legacy_command = "BACKUP SNAPSHOT my_backup TO repo"
    assert executor._extract_database_from_command(legacy_command) == "unknown_database"


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


def test_should_handle_backup_execution_with_zero_poll_interval(db_with_timezone):
    """Test backup execution with zero poll interval."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.return_value = [("job1", "test_backup", "test_db", "FINISHED")]
    
    result = executor.execute_backup(
        db, 
        "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo",
        max_polls=1,
        poll_interval=0.0,
        repository="repo",
        backup_type="incremental",
        scope="backup",
        database="test_db"
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_handle_backup_execution_with_very_small_poll_interval(db_with_timezone):
    """Test backup execution with very small poll interval."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.return_value = [("job1", "test_backup", "test_db", "FINISHED")]
    
    result = executor.execute_backup(
        db, 
        "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo",
        max_polls=1,
        poll_interval=0.001,
        repository="repo",
        backup_type="incremental",
        scope="backup",
        database="test_db"
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_handle_backup_execution_with_large_max_polls(db_with_timezone):
    """Test backup execution with large max polls value."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.return_value = [("job1", "test_backup", "test_db", "FINISHED")]
    
    result = executor.execute_backup(
        db, 
        "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo",
        max_polls=100000,
        poll_interval=0.001,
        repository="repo",
        backup_type="incremental",
        scope="backup",
        database="test_db"
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_handle_backup_execution_with_negative_max_polls(db_with_timezone):
    """Test backup execution with negative max polls."""
    db = db_with_timezone
    db.execute.return_value = None
    
    result = executor.execute_backup(
        db, 
        "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo",
        max_polls=-1,
        poll_interval=0.001,
        repository="repo",
        backup_type="incremental",
        scope="backup",
        database="test_db"
    )
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "TIMEOUT"
    assert db.query.call_count == 0


def test_should_log_status_changes_during_polling(mocker, capsys):
    """Test that polling logs status changes to stdout."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("job1", "test_backup", "test_db", "PENDING")],
        [("job1", "test_backup", "test_db", "SNAPSHOTING")],
        [("job1", "test_backup", "test_db", "UPLOADING")],
        [("job1", "test_backup", "test_db", "FINISHED")],
    ]
    
    status = executor.poll_backup_status(db, "test_backup", "test_db", max_polls=10, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"
    
    captured = capsys.readouterr()
    output = captured.out
    
    assert "PENDING" in output
    assert "SNAPSHOTING" in output
    assert "UPLOADING" in output
    assert "FINISHED" in output
    assert "poll" in output.lower()


def test_should_log_progress_every_10_polls(mocker, capsys):
    """Test that polling logs progress every 10 polls even without state change."""
    db = mocker.Mock()
    uploading_responses = [[("job1", "test_backup", "test_db", "UPLOADING")]] * 15
    finished_response = [[("job1", "test_backup", "test_db", "FINISHED")]]
    db.query.side_effect = uploading_responses + finished_response
    
    status = executor.poll_backup_status(db, "test_backup", "test_db", max_polls=20, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"
    
    captured = capsys.readouterr()
    output = captured.out
    
    assert "poll 1/" in output
    assert "poll 10/" in output
    assert "UPLOADING" in output


def test_should_include_max_polls_in_status_logging(mocker, capsys):
    """Test that status logs include max_polls information."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("job1", "test_backup", "test_db", "PENDING")],
        [("job1", "test_backup", "test_db", "FINISHED")],
    ]
    
    executor.poll_backup_status(db, "test_backup", "test_db", max_polls=100, poll_interval=0.001)
    
    captured = capsys.readouterr()
    output = captured.out
    
    assert "/100)" in output


def test_should_return_detailed_error_on_submit_failure(mocker):
    """Test that submit_backup_command returns detailed error information."""
    db = mocker.Mock()
    db.execute.side_effect = RuntimeError("Connection timeout after 30 seconds")
    
    backup_command = "BACKUP DATABASE test_db SNAPSHOT test_backup TO repo"
    
    success, error = executor.submit_backup_command(db, backup_command)
    
    assert success is False
    assert error is not None
    assert "RuntimeError" in error
    assert "Connection timeout after 30 seconds" in error
    assert "Failed to submit backup command" in error


def test_should_propagate_submit_error_to_execute_backup(mocker, db_with_timezone):
    """Test that execute_backup includes detailed submit error in result."""
    db = db_with_timezone
    db.execute.side_effect = ValueError("Invalid backup repository name")
    
    backup_command = "BACKUP DATABASE test_db SNAPSHOT test_backup TO invalid_repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.001, repository="repo", backup_type="incremental", scope="backup", database="test_db")
    
    assert result["success"] is False
    assert result["final_status"] is None
    assert "ValueError" in result["error_message"]
    assert "Invalid backup repository name" in result["error_message"]


def test_should_build_descriptive_error_message_for_lost_state():
    """Test that _build_error_message creates helpful message for LOST state."""
    final_status = {"state": "LOST"}
    error_msg = executor._build_error_message(final_status, "my_backup", "sales_db")
    
    assert "Backup tracking lost" in error_msg
    assert "my_backup" in error_msg
    assert "sales_db" in error_msg
    assert "concurrency issue" in error_msg
    assert "ops.run_status" in error_msg


def test_should_build_descriptive_error_message_for_cancelled_state():
    """Test that _build_error_message creates helpful message for CANCELLED state."""
    final_status = {"state": "CANCELLED"}
    error_msg = executor._build_error_message(final_status, "my_backup", "sales_db")
    
    assert "cancelled by StarRocks" in error_msg
    assert "my_backup" in error_msg
    assert "Check StarRocks logs" in error_msg


def test_should_build_descriptive_error_message_for_timeout_state():
    """Test that _build_error_message creates helpful message for TIMEOUT state."""
    final_status = {"state": "TIMEOUT"}
    error_msg = executor._build_error_message(final_status, "my_backup", "sales_db")
    
    assert "timed out" in error_msg
    assert "my_backup" in error_msg
    assert "SHOW BACKUP FROM sales_db" in error_msg
    assert "may still be running" in error_msg


def test_should_build_descriptive_error_message_for_error_state():
    """Test that _build_error_message creates helpful message for ERROR state."""
    final_status = {"state": "ERROR"}
    error_msg = executor._build_error_message(final_status, "my_backup", "sales_db")
    
    assert "Error occurred" in error_msg
    assert "my_backup" in error_msg
    assert "SHOW BACKUP FROM sales_db" in error_msg
    assert "monitoring failed" in error_msg
