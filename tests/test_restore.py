import pytest
import re
from unittest.mock import Mock, patch
from starrocks_br import restore
from starrocks_br import history


@pytest.fixture
def db_with_timezone():
    """Fixture that provides a mock database object with timezone property set to UTC."""
    db = Mock()
    db.timezone = "UTC"
    return db


def test_should_get_snapshot_timestamp_from_repository(mocker):
    """Test getting snapshot timestamp from repository."""
    db = mocker.Mock()
    db.query.return_value = [
        {"Snapshot": "sales_db_20251015_full", "Timestamp": "2025-10-21-13-51-17-465", "Status": "OK"}
    ]
    
    timestamp = restore.get_snapshot_timestamp(db, "my_repo", "sales_db_20251015_full")
    
    assert timestamp == "2025-10-21-13-51-17-465"
    
    query = db.query.call_args[0][0]
    assert "SHOW SNAPSHOT ON `my_repo`" in query
    assert "Snapshot = 'sales_db_20251015_full'" in query


def test_should_get_snapshot_timestamp_with_tuple_result(mocker):
    """Test getting snapshot timestamp when database returns tuple result."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db_20251015_full", "2025-10-21-13-51-17-465", "OK")
    ]
    
    timestamp = restore.get_snapshot_timestamp(db, "my_repo", "sales_db_20251015_full")
    
    assert timestamp == "2025-10-21-13-51-17-465"


def test_should_raise_error_when_snapshot_not_found(mocker):
    """Test that get_snapshot_timestamp raises error when snapshot not found."""
    db = mocker.Mock()
    db.query.return_value = []
    
    with pytest.raises(ValueError, match="Snapshot 'nonexistent' not found"):
        restore.get_snapshot_timestamp(db, "my_repo", "nonexistent")


def test_should_raise_error_when_timestamp_missing(mocker):
    """Test that get_snapshot_timestamp raises error when timestamp is missing."""
    db = mocker.Mock()
    db.query.return_value = [
        {"Snapshot": "sales_db_20251015_full", "Status": "OK"}  # Missing Timestamp
    ]
    
    with pytest.raises(ValueError, match="Could not extract timestamp"):
        restore.get_snapshot_timestamp(db, "my_repo", "sales_db_20251015_full")


def test_should_raise_error_when_tuple_timestamp_missing(mocker):
    """Test that get_snapshot_timestamp raises error when tuple result has missing timestamp."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db_20251015_full",)  # Only one element, missing timestamp
    ]
    
    with pytest.raises(ValueError, match="Could not extract timestamp"):
        restore.get_snapshot_timestamp(db, "my_repo", "sales_db_20251015_full")


def test_should_build_partition_restore_command():
    command = restore.build_partition_restore_command(
        database="sales_db",
        table="fact_sales",
        partition="p20251015",
        backup_label="sales_db_20251015_incremental",
        repository="my_repo",
        backup_timestamp="2025-10-21-13-51-17-465",
    )
    
    expected = """RESTORE SNAPSHOT `sales_db_20251015_incremental`
    FROM `my_repo`
    DATABASE `sales_db`
    ON (TABLE `fact_sales` PARTITION (`p20251015`))
    PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")"""

    assert command == expected


def test_should_build_table_restore_command():
    command = restore.build_table_restore_command(
        database="sales_db",
        table="dim_customers",
        backup_label="weekly_backup_20251015",
        repository="my_repo",
        backup_timestamp="2025-10-21-13-51-17-465",
    )
    
    expected = """RESTORE SNAPSHOT `weekly_backup_20251015`
    FROM `my_repo`
    DATABASE `sales_db`
    ON (TABLE `dim_customers`)
    PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")"""

    assert command == expected


def test_should_build_database_restore_command():
    command = restore.build_database_restore_command(
        database="sales_db",
        backup_label="sales_db_20251015_monthly",
        repository="my_repo",
        backup_timestamp="2025-10-21-13-51-17-465",
    )
    
    expected = """RESTORE SNAPSHOT `sales_db_20251015_monthly`
    FROM `my_repo`
    DATABASE `sales_db`
    PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")"""

    assert command == expected


def test_should_poll_restore_status_until_finished(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "PENDING"}],
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "FINISHED"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"
    assert db.query.call_count == 3


def test_should_poll_restore_status_until_failed(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "PENDING"}],
        [{"Label": "restore_job", "State": "CANCELLED"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "CANCELLED"


def test_should_query_correct_show_restore_syntax(mocker):
    db = mocker.Mock()
    db.query.return_value = [{"Label": "restore_job", "State": "FINISHED"}]
    
    restore.poll_restore_status(db, "restore_job", "test_db", max_polls=1, poll_interval=0.001)
    
    query = db.query.call_args[0][0]
    assert "SHOW RESTORE FROM `test_db`" in query


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


def test_should_execute_restore_workflow(mocker, db_with_timezone):
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "sales_db_20251015_incremental", "State": "PENDING"}],
        [{"Label": "sales_db_20251015_incremental", "State": "FINISHED"}],
    ]
    
    log_restore = mocker.patch("starrocks_br.history.log_restore")
    complete_slot = mocker.patch("starrocks_br.concurrency.complete_job_slot")
    
    restore_command = """
    RESTORE SNAPSHOT sales_db_20251015_inc
    FROM my_repo
    DATABASE sales_db
    ON (TABLE fact_sales PARTITION (p20251015))
    PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")"""
    
    result = restore.execute_restore(
        db,
        restore_command,
        backup_label="sales_db_20251015_incremental",
        restore_type="partition",
        repository="my_repo",
        database="sales_db",
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
        repository="repo-with-special.chars",
        backup_timestamp="2025-10-21-13-51-17-465",
    )
    
    assert "RESTORE SNAPSHOT `backup_2025.01.15`" in command
    assert "FROM `repo-with-special.chars`" in command
    assert "DATABASE `test-db_2025`" in command
    assert "TABLE `fact-table.sales`" in command
    assert "PARTITION (`p2025-01-15`)" in command
    assert 'PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")' in command


def test_should_build_table_restore_command_with_special_characters():
    """Test table restore command building with special characters."""
    command = restore.build_table_restore_command(
        database="test-db_2025",
        table="fact-table.sales",
        backup_label="backup_2025.01.15",
        repository="repo-with-special.chars",
        backup_timestamp="2025-10-21-13-51-17-465",
    )
    
    assert "RESTORE SNAPSHOT `backup_2025.01.15`" in command
    assert "FROM `repo-with-special.chars`" in command
    assert "DATABASE `test-db_2025`" in command
    assert "TABLE `fact-table.sales`" in command
    assert 'PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")' in command


def test_should_build_database_restore_command_with_special_characters():
    """Test database restore command building with special characters."""
    command = restore.build_database_restore_command(
        database="test-db_2025",
        backup_label="backup_2025.01.15",
        repository="repo-with-special.chars",
        backup_timestamp="2025-10-21-13-51-17-465",
    )
    
    assert "RESTORE SNAPSHOT `backup_2025.01.15`" in command
    assert "FROM `repo-with-special.chars`" in command
    assert "DATABASE `test-db_2025`" in command
    assert 'PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")' in command


def test_should_poll_restore_status_with_tuple_results():
    """Test polling restore status when database returns tuple results."""
    db = Mock()
    db.query.side_effect = [
        [("job1", "restore_job", "2025-01-15 10:00:00", "test_db", "PENDING", "2025-01-15 10:00:00")],
        [("job1", "restore_job", "2025-01-15 10:01:00", "test_db", "RUNNING", "2025-01-15 10:01:00")],
        [("job1", "restore_job", "2025-01-15 10:05:00", "test_db", "FINISHED", "2025-01-15 10:05:00")],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "FINISHED"
    assert status["label"] == "restore_job"
    assert db.query.call_count == 3


def test_should_poll_restore_status_with_malformed_results():
    """Test polling restore status with malformed database results."""
    db = Mock()
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "UNKNOWN"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "UNKNOWN"
    assert status["label"] == "restore_job"


def test_should_handle_restore_status_query_exceptions():
    """Test handling of exceptions during restore status queries."""
    db = Mock()
    db.query.side_effect = [
        Exception("Query timeout"),
        [{"Label": "restore_job", "State": "FINISHED"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "ERROR"
    assert status["label"] == "restore_job"


def test_should_handle_restore_status_with_cancelled_state():
    """Test handling of CANCELLED restore state."""
    db = Mock()
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "CANCELLED"}],
    ]
    
    status = restore.poll_restore_status(db, "restore_job", "test_db", max_polls=5, poll_interval=0.001)
    
    assert status["state"] == "CANCELLED"
    assert status["label"] == "restore_job"


def test_should_timeout_when_max_polls_reached_for_restore():
    """Test restore status polling timeout."""
    db = Mock()
    db.query.return_value = [{"Label": "restore_job", "State": "RUNNING"}]
    
    status = restore.poll_restore_status(db, "restore_job", "test_db", max_polls=2, poll_interval=0.001)
    
    assert status["state"] == "TIMEOUT"
    assert db.query.call_count == 2


def test_should_query_correct_show_restore_syntax_with_label():
    """Test that correct SHOW RESTORE query syntax is used."""
    db = Mock()
    db.query.return_value = [{"Label": "restore_job", "State": "FINISHED"}]
    
    restore.poll_restore_status(db, "restore_job", "test_db", max_polls=1, poll_interval=0.001)
    
    query = db.query.call_args[0][0]
    assert "SHOW RESTORE FROM `test_db`" in query


def test_should_handle_empty_restore_status_result():
    """Test handling of empty restore status results."""
    db = Mock()
    db.query.return_value = []
    
    status = restore.poll_restore_status(db, "nonexistent_restore", "test_db", max_polls=1, poll_interval=0.001)
    
    assert status["state"] == "TIMEOUT"


def test_should_execute_restore_with_custom_polling_parameters(db_with_timezone):
    """Test restore execution with custom polling parameters."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "PENDING"}],
        [{"Label": "restore_job", "State": "FINISHED"}],
    ]

    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"

    result = restore.execute_restore(
        db,
        restore_command,
        backup_label="restore_job",
        restore_type="partition",
        repository="custom_repo",
        database="test_db",
        max_polls=10,
        poll_interval=0.001
    )

    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_execute_restore_with_history_logging_failure(db_with_timezone):
    """Test restore execution when history logging fails."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "FINISHED"}],
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
                database="test_db",
                max_polls=3,
                poll_interval=0.001
            )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_restore.call_count == 1
    assert complete_slot.call_count == 1


def test_should_return_lost_when_label_mismatch():
    """Test that poll_restore_status returns LOST when label doesn't match."""
    db = Mock()
    db.query.side_effect = [
        [{"Label": "different_job", "State": "RUNNING"}],  # Different label
        [{"Label": "different_job", "State": "RUNNING"}],  # Still different
    ]
    
    status = restore.poll_restore_status(db, "expected_job", "test_db", max_polls=3, poll_interval=0.001)
    
    assert status["state"] == "LOST"
    assert status["label"] == "expected_job"
    assert db.query.call_count == 2


def test_should_execute_restore_with_job_slot_completion_failure(db_with_timezone):
    """Test restore execution when job slot completion fails."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "FINISHED"}],
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
                    database="test_db",
                    max_polls=3,
                    poll_interval=0.001
                )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_restore.call_count == 1
    assert complete_slot.call_count == 1


def test_should_execute_restore_with_both_logging_and_slot_failures(db_with_timezone):
    """Test restore execution when both history logging and job slot completion fail."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "FINISHED"}],
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
                    database="test_db",
                    max_polls=3,
                    poll_interval=0.001
                )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    assert log_restore.call_count == 1
    assert complete_slot.call_count == 1


def test_should_handle_restore_execution_with_very_long_polling(db_with_timezone):
    """Test restore execution with very long polling duration."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.return_value = [{"Label": "restore_job", "State": "RUNNING"}]
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    result = restore.execute_restore(
        db, 
        restore_command, 
        backup_label="restore_job",
        restore_type="partition",
        repository="test_repo",
        database="test_db",
        max_polls=3, 
        poll_interval=0.001
    )
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "TIMEOUT"
    assert db.query.call_count == 3


def test_should_handle_restore_execution_with_zero_polls(db_with_timezone):
    """Test restore execution with zero max polls."""
    db = db_with_timezone
    db.execute.return_value = None
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    result = restore.execute_restore(
        db, 
        restore_command, 
        backup_label="restore_job",
        restore_type="partition",
        repository="test_repo",
        database="test_db",
        max_polls=0, 
        poll_interval=0.001
    )
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "TIMEOUT"
    assert db.query.call_count == 0


def test_should_execute_restore_with_different_scope_values(db_with_timezone):
    """Test restore execution with different scope values."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "FINISHED"}],
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
            database="test_db",
            scope=scope,
            max_polls=3,
            poll_interval=0.001
        )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    
    _, kwargs = complete_slot.call_args
    assert kwargs.get("scope") == scope


def test_should_handle_restore_execution_with_intermittent_query_failures(db_with_timezone):
    """Test restore execution with intermittent query failures."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        Exception("Temporary network error"),
        [{"Label": "restore_job", "State": "RUNNING"}],
        Exception("Another temporary error"),
        [{"Label": "restore_job", "State": "FINISHED"}],
    ]
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    result = restore.execute_restore(
        db, 
        restore_command, 
        backup_label="restore_job",
        restore_type="partition",
        repository="test_repo",
        database="test_db",
        max_polls=5, 
        poll_interval=0.001
    )
    
    assert result["success"] is False
    assert result["final_status"]["state"] == "ERROR"


def test_should_handle_restore_command_submission_failure(db_with_timezone):
    """Test restore execution when command submission fails."""
    db = db_with_timezone
    db.execute.side_effect = Exception("Permission denied")
    
    restore_command = "RESTORE SNAPSHOT restore_job FROM repo"
    
    result = restore.execute_restore(
        db, 
        restore_command, 
        backup_label="restore_job",
        restore_type="partition",
        repository="test_repo",
        database="test_db"
    )
    
    assert result["success"] is False
    assert result["final_status"] is None
    assert "Failed to submit restore command" in result["error_message"]


def test_should_execute_restore_with_multiline_command(db_with_timezone):
    """Test restore execution with multiline restore commands."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "complex_backup_2025-01-15", "State": "RUNNING"}],
        [{"Label": "complex_backup_2025-01-15", "State": "FINISHED"}],
    ]
    
    multiline_command = """
    RESTORE SNAPSHOT complex_backup_2025-01-15
    FROM my_repo
    DATABASE sales_db
    ON (TABLE fact_sales PARTITION (p20250115), TABLE dim_customers)
    PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")
    """
    
    result = restore.execute_restore(
        db,
        multiline_command,
        backup_label="complex_backup_2025-01-15",
        restore_type="partition",
        repository="my_repo",
        database="sales_db",
        max_polls=3,
        poll_interval=0.001
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    
    restore_call = db.execute.call_args_list[0][0][0]
    assert "RESTORE SNAPSHOT complex_backup_2025-01-15" in restore_call
    assert "FROM my_repo" in restore_call


def test_should_execute_restore_with_special_characters_in_names(db_with_timezone):
    """Test restore execution with special characters in backup names."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "restore-job_2025.01.15", "State": "RUNNING"}],
        [{"Label": "restore-job_2025.01.15", "State": "FINISHED"}],
    ]
    
    restore_command = "RESTORE SNAPSHOT restore-job_2025.01.15 FROM repo-with-special.chars"
    
    result = restore.execute_restore(
        db,
        restore_command,
        backup_label="restore-job_2025.01.15",
        restore_type="table",
        repository="repo-with-special.chars",
        database="test_db",
        max_polls=3,
        poll_interval=0.001
    )
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"


def test_should_log_restore_history_with_correct_parameters(db_with_timezone):
    """Test that restore history is logged with correct parameters."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "FINISHED"}],
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
                database="test_db",
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


def test_should_log_restore_history_with_failure_state(db_with_timezone):
    """Test that restore history is logged correctly for failed restores."""
    db = db_with_timezone
    db.execute.return_value = None
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "CANCELLED"}],
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
                database="test_db",
                max_polls=3,
                poll_interval=0.001
            )
    
    assert result["success"] is False
    
    entry = log_restore.call_args[0][1]
    assert entry["job_id"] == "restore_job"
    assert entry["status"] == "CANCELLED"
    assert entry["repository"] == "test_repo"
    assert entry["restore_type"] == "partition"


def test_should_find_restore_pair_for_full_backup(mocker):
    """Test finding restore pair for a full backup (returns single label)."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db_20251015_full", "full", "2025-10-15 10:00:00")
    ]
    
    result = restore.find_restore_pair(db, "sales_db_20251015_full")
    
    assert result == ["sales_db_20251015_full"]
    
    query = db.query.call_args[0][0]
    assert "ops.backup_history" in query
    assert "label = 'sales_db_20251015_full'" in query
    assert "status = 'FINISHED'" in query


def test_should_find_restore_pair_for_incremental_backup(mocker):
    """Test finding restore pair for an incremental backup (returns full + incremental)."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("sales_db_20251016_inc", "incremental", "2025-10-16 10:00:00")],  # Target backup
        [("sales_db_20251015_full", "full", "2025-10-15 10:00:00")]  # Base full backup
    ]
    
    result = restore.find_restore_pair(db, "sales_db_20251016_inc")
    
    assert result == ["sales_db_20251015_full", "sales_db_20251016_inc"]
    assert db.query.call_count == 2


def test_should_raise_error_when_target_label_not_found(mocker):
    """Test that find_restore_pair raises error when target label not found."""
    db = mocker.Mock()
    db.query.return_value = []
    
    with pytest.raises(ValueError, match="Backup label 'nonexistent' not found"):
        restore.find_restore_pair(db, "nonexistent")


def test_should_raise_error_when_incremental_has_no_full_backup(mocker):
    """Test that find_restore_pair raises error when incremental has no preceding full backup."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("sales_db_20251016_inc", "incremental", "2025-10-16 10:00:00")],  # Target backup
        []  # No full backup found
    ]
    
    with pytest.raises(ValueError, match="No successful full backup found before incremental"):
        restore.find_restore_pair(db, "sales_db_20251016_inc")


def test_should_raise_error_for_unknown_backup_type(mocker):
    """Test that find_restore_pair raises error for unknown backup type."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db_20251015_unknown", "unknown", "2025-10-15 10:00:00")
    ]
    
    with pytest.raises(ValueError, match="Unknown backup type 'unknown'"):
        restore.find_restore_pair(db, "sales_db_20251015_unknown")


def test_should_get_tables_from_backup_without_group_filter(mocker):
    """Test getting tables from backup without group filtering."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "fact_sales"),
        ("sales_db", "dim_customers"),
        ("orders_db", "fact_orders"),
    ]
    
    result = restore.get_tables_from_backup(db, "sales_db_20251015_full")
    
    assert result == ["sales_db.fact_sales", "sales_db.dim_customers", "orders_db.fact_orders"]
    
    query = db.query.call_args[0][0]
    assert "ops.backup_partitions" in query
    assert "label = 'sales_db_20251015_full'" in query


def test_should_get_tables_from_backup_with_group_filter(mocker):
    """Test getting tables from backup with group filtering."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("sales_db", "fact_sales"), ("sales_db", "dim_customers"), ("orders_db", "fact_orders")],  # Backup tables
        [("sales_db", "fact_sales"), ("sales_db", "dim_customers")]  # Group tables
    ]
    
    result = restore.get_tables_from_backup(db, "sales_db_20251015_full", group="daily_incremental")
    
    assert result == ["sales_db.fact_sales", "sales_db.dim_customers"]
    assert db.query.call_count == 2


def test_should_return_empty_list_when_no_tables_in_backup(mocker):
    """Test that get_tables_from_backup returns empty list when no tables in backup."""
    db = mocker.Mock()
    db.query.return_value = []
    
    result = restore.get_tables_from_backup(db, "empty_backup")
    
    assert result == []


def test_should_return_empty_list_when_group_has_no_tables(mocker):
    """Test that get_tables_from_backup returns empty list when group has no tables."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("sales_db", "fact_sales")],  # Backup tables
        []  # No group tables
    ]
    
    result = restore.get_tables_from_backup(db, "sales_db_20251015_full", group="empty_group")
    
    assert result == []


def test_should_get_tables_from_backup_with_wildcard_group_filter(mocker):
    """Test getting tables from backup with group filtering that includes wildcard entries."""
    db = mocker.Mock()
    db.query.side_effect = [
        [("sales_db", "fact_sales"), ("sales_db", "dim_customers"), ("orders_db", "fact_orders")],  # Backup tables
        [("sales_db", "*")],  # Group inventory with wildcard
        [("fact_sales",), ("dim_customers",)]  # SHOW TABLES FROM sales_db result
    ]
    
    result = restore.get_tables_from_backup(db, "sales_db_20251015_full", group="full_database")
    
    assert result == ["sales_db.fact_sales", "sales_db.dim_customers"]
    assert db.query.call_count == 3
    
    calls = [call[0][0] for call in db.query.call_args_list]
    assert "SHOW TABLES FROM `sales_db`" in calls


def test_should_get_tables_from_backup_with_table_filter(mocker):
    """Test getting tables from backup with table filtering."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "fact_sales"),
        ("sales_db", "dim_customers"),
        ("orders_db", "fact_orders"),
    ]
    
    result = restore.get_tables_from_backup(
        db, 
        "sales_db_20251015_full", 
        table="fact_sales", 
        database="sales_db"
    )
    
    assert result == ["sales_db.fact_sales"]
    assert db.query.call_count == 1


def test_should_raise_value_error_when_table_not_found_in_backup(mocker):
    """Test that get_tables_from_backup raises ValueError when table is not found in backup."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "fact_sales"),
        ("sales_db", "dim_customers"),
    ]
    
    with pytest.raises(ValueError, match="Table 'nonexistent_table' not found in backup"):
        restore.get_tables_from_backup(
            db, 
            "sales_db_20251015_full", 
            table="nonexistent_table", 
            database="sales_db"
        )


def test_should_raise_value_error_when_table_and_group_both_specified(mocker):
    """Test that get_tables_from_backup raises ValueError when both table and group are specified."""
    db = mocker.Mock()
    
    with pytest.raises(ValueError, match="Cannot specify both --group and --table"):
        restore.get_tables_from_backup(
            db, 
            "sales_db_20251015_full", 
            group="daily_incremental",
            table="fact_sales",
            database="sales_db"
        )


def test_should_raise_value_error_when_table_specified_without_database(mocker):
    """Test that get_tables_from_backup raises ValueError when table is specified without database."""
    db = mocker.Mock()
    
    with pytest.raises(ValueError, match="database parameter is required when table is specified"):
        restore.get_tables_from_backup(
            db, 
            "sales_db_20251015_full", 
            table="fact_sales"
        )


def test_should_filter_table_by_database_when_multiple_databases_in_backup(mocker):
    """Test that table filtering correctly filters by database when backup contains multiple databases."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "users"),
        ("orders_db", "users"),
        ("sales_db", "products"),
    ]
    
    result = restore.get_tables_from_backup(
        db, 
        "multi_db_backup", 
        table="users", 
        database="sales_db"
    )
    
    assert result == ["sales_db.users"]
    assert len(result) == 1


def test_should_return_empty_list_when_table_not_in_specified_database(mocker):
    """Test that get_tables_from_backup returns empty list when table exists but in different database."""
    db = mocker.Mock()
    db.query.return_value = [
        ("sales_db", "fact_sales"),
        ("orders_db", "fact_orders"),
    ]
    
    with pytest.raises(ValueError, match="Table 'fact_orders' not found in backup"):
        restore.get_tables_from_backup(
            db, 
            "multi_db_backup", 
            table="fact_orders", 
            database="sales_db"
        )


def test_should_build_restore_command_with_rename():
    """Test building restore command with AS clause for temporary tables."""
    backup_label = "sales_db_20251015_full"
    repo_name = "my_repo"
    tables = ["sales_db.fact_sales", "sales_db.dim_customers"]
    rename_suffix = "_restored"
    database = "sales_db"
    backup_timestamp = "2025-10-21-13-51-17-465"
    
    command = restore._build_restore_command_with_rename(backup_label, repo_name, tables, rename_suffix, database, backup_timestamp)
    
    assert "RESTORE SNAPSHOT `sales_db_20251015_full`" in command
    assert "FROM `my_repo`" in command
    assert "DATABASE `sales_db`" in command
    assert "TABLE `fact_sales` AS `fact_sales_restored`" in command
    assert "TABLE `dim_customers` AS `dim_customers_restored`" in command
    assert 'PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")' in command


def test_should_build_restore_command_without_rename():
    """Test building restore command without AS clause."""
    backup_label = "sales_db_20251016_inc"
    repo_name = "my_repo"
    tables = ["sales_db.fact_sales", "sales_db.dim_customers"]
    database = "sales_db"
    backup_timestamp = "2025-10-21-13-51-17-465"
    
    command = restore._build_restore_command_without_rename(backup_label, repo_name, tables, database, backup_timestamp)

    print("command: ", command)
    
    assert "RESTORE SNAPSHOT `sales_db_20251016_inc`" in command
    assert "FROM `my_repo`" in command
    assert "DATABASE `sales_db`" in command
    assert "TABLE `fact_sales`" in command
    assert "TABLE `dim_customers`" in command
    assert "AS " not in command
    assert 'PROPERTIES ("backup_timestamp" = "2025-10-21-13-51-17-465")' in command


def test_should_perform_atomic_rename(mocker):
    """Test performing atomic rename of temporary tables."""
    db = mocker.Mock()
    tables = ["sales_db.fact_sales", "sales_db.dim_customers"]
    rename_suffix = "_restored"
    
    result = restore._perform_atomic_rename(db, tables, rename_suffix)
    
    assert result["success"] is True
    assert db.execute.call_count == 4  # 2 tables * 2 rename statements each
    
    calls = [call[0][0] for call in db.execute.call_args_list]
    
    assert "ALTER TABLE `sales_db`.`fact_sales_restored` RENAME `fact_sales`" in calls
    assert "ALTER TABLE `sales_db`.`dim_customers_restored` RENAME `dim_customers`" in calls

    backup_rename_pattern = re.compile(r"ALTER TABLE `sales_db`\.`fact_sales` RENAME `fact_sales_backup_\d{8}_\d{6}`")
    assert any(backup_rename_pattern.match(call) for call in calls), "Expected timestamped backup rename for fact_sales"

    backup_rename_pattern = re.compile(r"ALTER TABLE `sales_db`\.`dim_customers` RENAME `dim_customers_backup_\d{8}_\d{6}`")
    assert any(backup_rename_pattern.match(call) for call in calls), "Expected timestamped backup rename for dim_customers"


def test_should_handle_atomic_rename_failure(mocker):
    """Test handling of atomic rename failure."""
    db = mocker.Mock()
    db.execute.side_effect = Exception("Rename failed")
    tables = ["sales_db.fact_sales"]
    rename_suffix = "_restored"
    
    result = restore._perform_atomic_rename(db, tables, rename_suffix)
    
    assert result["success"] is False
    assert "Failed to perform atomic rename" in result["error_message"]


def test_should_execute_restore_flow_with_full_backup(mocker):
    """Test executing restore flow with full backup only."""
    db = mocker.Mock()
    repo_name = "my_repo"
    restore_pair = ["sales_db_20251015_full"]
    tables_to_restore = ["sales_db.fact_sales", "sales_db.dim_customers"]
    rename_suffix = "_restored"
    
    mocker.patch('starrocks_br.restore.get_snapshot_timestamp', return_value='2025-10-21-13-51-17-465')
    mocker.patch('starrocks_br.restore.execute_restore', return_value={"success": True})
    mocker.patch('starrocks_br.restore._perform_atomic_rename', return_value={"success": True})
    
    mocker.patch('builtins.input', return_value='y')
    
    result = restore.execute_restore_flow(db, repo_name, restore_pair, tables_to_restore, rename_suffix)
    
    assert result["success"] is True
    assert "Restore completed successfully" in result["message"]
    
    from starrocks_br.restore import get_snapshot_timestamp
    get_snapshot_timestamp.assert_called_once_with(db, repo_name, "sales_db_20251015_full")


def test_should_execute_restore_flow_with_incremental_backup(mocker):
    """Test executing restore flow with full + incremental backup."""
    db = mocker.Mock()
    repo_name = "my_repo"
    restore_pair = ["sales_db_20251015_full", "sales_db_20251016_inc"]
    tables_to_restore = ["sales_db.fact_sales"]
    rename_suffix = "_restored"
    
    mocker.patch('starrocks_br.restore.get_snapshot_timestamp', return_value='2025-10-21-13-51-17-465')
    mocker.patch('starrocks_br.restore.execute_restore', return_value={"success": True})
    mocker.patch('starrocks_br.restore._perform_atomic_rename', return_value={"success": True})
    
    mocker.patch('builtins.input', return_value='y')
    
    result = restore.execute_restore_flow(db, repo_name, restore_pair, tables_to_restore, rename_suffix)
    
    assert result["success"] is True
    assert "Restore completed successfully" in result["message"]
    
    from starrocks_br.restore import get_snapshot_timestamp
    assert get_snapshot_timestamp.call_count == 2
    get_snapshot_timestamp.assert_any_call(db, repo_name, "sales_db_20251015_full")
    get_snapshot_timestamp.assert_any_call(db, repo_name, "sales_db_20251016_inc")


def test_should_cancel_restore_flow_when_user_says_no(mocker):
    """Test that restore flow is cancelled when user says no."""
    db = mocker.Mock()
    repo_name = "my_repo"
    restore_pair = ["sales_db_20251015_full"]
    tables_to_restore = ["sales_db.fact_sales"]
    
    mocker.patch('builtins.input', return_value='n')
    
    result = restore.execute_restore_flow(db, repo_name, restore_pair, tables_to_restore)
    
    assert result["success"] is False
    assert "cancelled by user" in result["error_message"]


def test_should_skip_confirmation_when_skip_confirmation_is_true(mocker):
    """Test that restore flow skips input prompt when skip_confirmation is True."""
    db = mocker.Mock()
    repo_name = "my_repo"
    restore_pair = ["sales_db_20251015_full"]
    tables_to_restore = ["sales_db.fact_sales"]
    
    mocker.patch('starrocks_br.restore.get_snapshot_timestamp', return_value='2025-10-21-13-51-17-465')
    mocker.patch('starrocks_br.restore.execute_restore', return_value={"success": True})
    mocker.patch('starrocks_br.restore._perform_atomic_rename', return_value={"success": True})
    
    input_mock = mocker.patch('builtins.input')
    
    result = restore.execute_restore_flow(db, repo_name, restore_pair, tables_to_restore, skip_confirmation=True)
    
    assert result["success"] is True
    assert "Restore completed successfully" in result["message"]
    input_mock.assert_not_called()


def test_should_fail_restore_flow_when_base_restore_fails(mocker):
    """Test that restore flow fails when base restore fails."""
    db = mocker.Mock()
    repo_name = "my_repo"
    restore_pair = ["sales_db_20251015_full"]
    tables_to_restore = ["sales_db.fact_sales"]
    
    mocker.patch('starrocks_br.restore.get_snapshot_timestamp', return_value='2025-10-21-13-51-17-465')
    mocker.patch('starrocks_br.restore.execute_restore', return_value={
        "success": False, 
        "error_message": "Base restore failed"
    })
    
    mocker.patch('builtins.input', return_value='y')
    
    result = restore.execute_restore_flow(db, repo_name, restore_pair, tables_to_restore)
    
    assert result["success"] is False
    assert "Base restore failed" in result["error_message"]


def test_should_fail_restore_flow_when_incremental_restore_fails(mocker):
    """Test that restore flow fails when incremental restore fails."""
    db = mocker.Mock()
    repo_name = "my_repo"
    restore_pair = ["sales_db_20251015_full", "sales_db_20251016_inc"]
    tables_to_restore = ["sales_db.fact_sales"]
    
    mocker.patch('starrocks_br.restore.get_snapshot_timestamp', return_value='2025-10-21-13-51-17-465')
    
    def mock_execute_restore(db, command, backup_label, restore_type, repo, database, scope="restore"):
        if "full" in backup_label:
            return {"success": True}
        else:
            return {"success": False, "error_message": "Incremental restore failed"}
    
    mocker.patch('starrocks_br.restore.execute_restore', side_effect=mock_execute_restore)
    
    mocker.patch('builtins.input', return_value='y')
    
    result = restore.execute_restore_flow(db, repo_name, restore_pair, tables_to_restore)
    
    assert result["success"] is False
    assert "Incremental restore failed" in result["error_message"]


def test_should_fail_restore_flow_when_atomic_rename_fails(mocker):
    """Test that restore flow fails when atomic rename fails."""
    db = mocker.Mock()
    repo_name = "my_repo"
    restore_pair = ["sales_db_20251015_full"]
    tables_to_restore = ["sales_db.fact_sales"]
    
    mocker.patch('starrocks_br.restore.get_snapshot_timestamp', return_value='2025-10-21-13-51-17-465')
    mocker.patch('starrocks_br.restore.execute_restore', return_value={"success": True})
    mocker.patch('starrocks_br.restore._perform_atomic_rename', return_value={
        "success": False, 
        "error_message": "Atomic rename failed"
    })
    
    mocker.patch('builtins.input', return_value='y')
    
    result = restore.execute_restore_flow(db, repo_name, restore_pair, tables_to_restore)
    
    assert result["success"] is False
    assert "Atomic rename failed" in result["error_message"]


def test_should_validate_restore_flow_inputs(mocker):
    """Test that restore flow validates inputs properly."""
    db = mocker.Mock()
    repo_name = "my_repo"
    
    # Test empty restore pair
    result = restore.execute_restore_flow(db, repo_name, [], ["sales_db.fact_sales"])
    assert result["success"] is False
    assert "No restore pair provided" in result["error_message"]
    
    # Test empty tables list
    result = restore.execute_restore_flow(db, repo_name, ["sales_db_20251015_full"], [])
    assert result["success"] is False
    assert "No tables to restore" in result["error_message"]


def test_should_include_correct_timestamp_in_restore_commands(mocker):
    """Test that restore commands include the correct timestamp from repository."""
    db = mocker.Mock()
    repo_name = "my_repo"
    restore_pair = ["sales_db_20251015_full"]
    tables_to_restore = ["sales_db.fact_sales"]
    
    mock_timestamp = "2025-10-21-13-51-17-465"
    mocker.patch('starrocks_br.restore.get_snapshot_timestamp', return_value=mock_timestamp)
    
    execute_restore_mock = mocker.patch('starrocks_br.restore.execute_restore', return_value={"success": True})
    mocker.patch('starrocks_br.restore._perform_atomic_rename', return_value={"success": True})
    
    mocker.patch('builtins.input', return_value='y')
    
    result = restore.execute_restore_flow(db, repo_name, restore_pair, tables_to_restore)
    
    assert result["success"] is True
    
    execute_restore_mock.assert_called_once()
    restore_command = execute_restore_mock.call_args[0][1]
    
    assert f'PROPERTIES ("backup_timestamp" = "{mock_timestamp}")' in restore_command
    assert "DATABASE `sales_db`" in restore_command


def test_should_use_cluster_timezone_for_restore_timestamps(mocker):
    """Test that execute_restore uses cluster timezone for timestamps, not local time."""
    db = mocker.Mock()
    db.timezone = "Asia/Shanghai"
    db.execute.return_value = None
    db.query.return_value = [{"Label": "test_label", "State": "FINISHED"}]

    log_restore = mocker.patch("starrocks_br.history.log_restore")
    complete_slot = mocker.patch("starrocks_br.concurrency.complete_job_slot")
    mock_get_time = mocker.patch("starrocks_br.timezone.get_current_time_in_cluster_tz")
    mock_get_time.return_value = "2025-11-20 15:30:00"

    restore.execute_restore(
        db,
        "RESTORE SNAPSHOT test FROM repo",
        backup_label="test_label",
        restore_type="full",
        repository="repo",
        database="test_db",
        max_polls=5,
        poll_interval=0.001,
    )

    assert mock_get_time.call_count == 2
    assert mock_get_time.call_args_list[0][0][0] == "Asia/Shanghai"
    assert mock_get_time.call_args_list[1][0][0] == "Asia/Shanghai"

    log_restore_call = log_restore.call_args[0][1]
    assert log_restore_call["started_at"] == "2025-11-20 15:30:00"
    assert log_restore_call["finished_at"] == "2025-11-20 15:30:00"


# Exponential backoff tests

def test_should_calculate_next_interval_exponentially():
    """Test that _calculate_next_interval doubles the current interval."""
    assert restore._calculate_next_interval(0.001, 0.06) == 0.002
    assert restore._calculate_next_interval(0.002, 0.06) == 0.004
    assert restore._calculate_next_interval(0.004, 0.06) == 0.008
    assert restore._calculate_next_interval(0.008, 0.06) == 0.016
    assert restore._calculate_next_interval(0.016, 0.06) == 0.032


def test_should_cap_interval_at_maximum():
    """Test that _calculate_next_interval caps at max_interval."""
    assert restore._calculate_next_interval(0.032, 0.06) == 0.06
    assert restore._calculate_next_interval(0.06, 0.06) == 0.06
    assert restore._calculate_next_interval(0.1, 0.06) == 0.06


def test_should_use_exponential_backoff_during_restore_polling(mocker):
    """Test that poll_restore_status uses exponential backoff intervals."""
    db = mocker.Mock()
    db.query.side_effect = [
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "RUNNING"}],
        [{"Label": "restore_job", "State": "FINISHED"}],
    ]

    sleep_mock = mocker.patch('time.sleep')

    status = restore.poll_restore_status(
        db, "restore_job", "test_db",
        max_polls=10,
        poll_interval=0.001,
        max_poll_interval=0.06
    )

    assert status["state"] == "FINISHED"

    # Verify exponential backoff: 1ms, 2ms, 4ms, 8ms, 16ms
    sleep_calls = [call[0][0] for call in sleep_mock.call_args_list]
    assert sleep_calls[0] == 0.001
    assert sleep_calls[1] == 0.002
    assert sleep_calls[2] == 0.004
    assert sleep_calls[3] == 0.008
    assert sleep_calls[4] == 0.016


def test_should_cap_restore_backoff_at_max_interval(mocker):
    """Test that restore exponential backoff caps at max_poll_interval."""
    db = mocker.Mock()
    # Create enough responses to test capping
    responses = [[{"Label": "restore_job", "State": "RUNNING"}]] * 10
    responses.append([{"Label": "restore_job", "State": "FINISHED"}])
    db.query.side_effect = responses

    sleep_mock = mocker.patch('time.sleep')

    status = restore.poll_restore_status(
        db, "restore_job", "test_db",
        max_polls=15,
        poll_interval=0.001,
        max_poll_interval=0.01
    )

    assert status["state"] == "FINISHED"

    sleep_calls = [call[0][0] for call in sleep_mock.call_args_list]
    # 1ms, 2ms, 4ms, 8ms, then capped at 10ms
    assert sleep_calls[0] == 0.001
    assert sleep_calls[1] == 0.002
    assert sleep_calls[2] == 0.004
    assert sleep_calls[3] == 0.008
    assert sleep_calls[4] == 0.01  # Capped
    assert sleep_calls[5] == 0.01  # Still capped
    assert all(interval <= 0.01 for interval in sleep_calls)


def test_should_use_default_max_poll_interval_for_restore(mocker):
    """Test that poll_restore_status uses default max_poll_interval when not specified."""
    db = mocker.Mock()
    responses = [[{"Label": "restore_job", "State": "RUNNING"}]] * 8
    responses.append([{"Label": "restore_job", "State": "FINISHED"}])
    db.query.side_effect = responses

    sleep_mock = mocker.patch('time.sleep')

    status = restore.poll_restore_status(
        db, "restore_job", "test_db",
        max_polls=10,
        poll_interval=0.001
        # max_poll_interval not specified, should default to 60
    )

    assert status["state"] == "FINISHED"

    sleep_calls = [call[0][0] for call in sleep_mock.call_args_list]
    # 1ms, 2ms, 4ms, 8ms, 16ms, 32ms, 64ms, 128ms
    assert sleep_calls[0] == 0.001
    assert sleep_calls[1] == 0.002
    assert sleep_calls[2] == 0.004
    assert sleep_calls[3] == 0.008
    assert sleep_calls[4] == 0.016
    assert sleep_calls[5] == 0.032
    assert sleep_calls[6] == 0.064
    assert sleep_calls[7] == 0.128


def test_should_handle_restore_backoff_with_immediate_completion(mocker):
    """Test that exponential backoff works when restore completes immediately."""
    db = mocker.Mock()
    db.query.return_value = [{"Label": "restore_job", "State": "FINISHED"}]

    sleep_mock = mocker.patch('time.sleep')

    status = restore.poll_restore_status(
        db, "restore_job", "test_db",
        max_polls=10,
        poll_interval=0.001,
        max_poll_interval=0.06
    )

    assert status["state"] == "FINISHED"
    # Should not sleep if already finished
    assert sleep_mock.call_count == 0
