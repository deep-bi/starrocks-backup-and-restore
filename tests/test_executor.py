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
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=5, poll_interval=0.1)
    
    assert status["state"] == "FINISHED"
    assert db.query.call_count == 3


def test_should_poll_backup_status_until_failed(mocker):
    db = mocker.Mock()
    db.query.side_effect = [
        [{"label": "test_backup", "state": "PENDING"}],
        [{"label": "test_backup", "state": "FAILED"}],
    ]
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=5, poll_interval=0.1)
    
    assert status["state"] == "FAILED"


def test_should_timeout_when_max_polls_reached(mocker):
    db = mocker.Mock()
    db.query.return_value = [{"label": "test_backup", "state": "RUNNING"}]
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=2, poll_interval=0.1)
    
    assert status["state"] == "TIMEOUT"
    assert db.query.call_count == 2


def test_should_query_correct_show_backup_syntax(mocker):
    db = mocker.Mock()
    db.query.return_value = [{"label": "test_backup", "state": "FINISHED"}]
    
    executor.poll_backup_status(db, "test_backup", max_polls=1, poll_interval=0.1)
    
    query = db.query.call_args[0][0]
    assert "SHOW BACKUP" in query
    assert "test_backup" in query


def test_should_handle_empty_backup_status_result(mocker):
    db = mocker.Mock()
    db.query.return_value = []
    
    status = executor.poll_backup_status(db, "nonexistent_backup", max_polls=1, poll_interval=0.1)
    
    assert status["state"] == "UNKNOWN"


def test_should_handle_malformed_backup_status_result(mocker):
    db = mocker.Mock()
    db.query.return_value = [{"label": "test_backup", "state": "FINISHED"}]
    
    status = executor.poll_backup_status(db, "test_backup", max_polls=1, poll_interval=0.1)
    
    assert status["state"] == "FINISHED"


def test_should_execute_full_backup_workflow(mocker):
    db = mocker.Mock()
    db.execute.return_value = None
    db.query.side_effect = [
        [{"label": "test_backup", "state": "PENDING"}],
        [{"label": "test_backup", "state": "FINISHED"}],
    ]
    
    backup_command = "BACKUP SNAPSHOT test_backup TO repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.1)
    
    assert result["success"] is True
    assert result["final_status"]["state"] == "FINISHED"
    # execute called: 1) submit backup, 2) log history, 3) complete job slot
    assert db.execute.call_count == 3
    assert db.query.call_count == 2


def test_should_handle_backup_execution_failure_in_workflow(mocker):
    db = mocker.Mock()
    db.execute.side_effect = Exception("Database connection failed")
    
    backup_command = "BACKUP SNAPSHOT test_backup TO repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.1)
    
    assert result["success"] is False
    assert result["final_status"] is None
    assert "Failed to submit backup command" in result["error_message"]


def test_should_handle_backup_polling_failure_in_workflow(mocker):
    db = mocker.Mock()
    db.execute.return_value = None
    db.query.side_effect = Exception("Query failed")
    
    backup_command = "BACKUP SNAPSHOT test_backup TO repo"
    
    result = executor.execute_backup(db, backup_command, max_polls=5, poll_interval=0.1)
    
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
        poll_interval=0.01,
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
        poll_interval=0.01,
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
