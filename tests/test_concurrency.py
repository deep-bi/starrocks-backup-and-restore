from starrocks_br import concurrency


def test_should_reserve_job_slot_when_no_active_conflict(mocker):
    db = mocker.Mock()
    db.query.return_value = []

    concurrency.reserve_job_slot(db, scope="backup", label="db_20251015_incremental")

    assert db.query.call_count == 1
    assert "FROM ops.run_status" in db.query.call_args[0][0]
    assert db.execute.call_count == 1
    sql = db.execute.call_args[0][0]
    assert "INSERT INTO ops.run_status" in sql or "UPSERT INTO ops.run_status" in sql
    assert "ACTIVE" in sql


def test_should_raise_when_active_conflict_exists(mocker):
    db = mocker.Mock()
    db.query.return_value = [("backup", "db_20251015_incremental", "ACTIVE")]

    try:
        concurrency.reserve_job_slot(db, scope="backup", label="db_20251015_incremental")
        raise AssertionError("expected conflict")
    except RuntimeError as e:
        error_msg = str(e)
        assert "Concurrency conflict" in error_msg
        assert "Another 'backup' job is already ACTIVE: backup:db_20251015_incremental" in error_msg
        assert "Wait for it to complete or cancel it via" in error_msg
        assert "UPDATE ops.run_status SET state='CANCELLED'" in error_msg
        assert "WHERE label='db_20251015_incremental' AND state='ACTIVE'" in error_msg
    assert db.execute.call_count == 0


def test_should_update_state_when_completing_job_slot(mocker):
    db = mocker.Mock()

    concurrency.complete_job_slot(
        db, scope="backup", label="db_20251015_incremental", final_state="FINISHED"
    )

    assert db.execute.call_count == 1
    sql = db.execute.call_args[0][0]
    assert "UPDATE ops.run_status" in sql or "DELETE FROM ops.run_status" in sql
    assert "FINISHED" in sql or "state='IDLE'" in sql


def test_should_not_conflict_on_different_scope(mocker):
    db = mocker.Mock()
    db.query.return_value = [("restore", "some", "ACTIVE")]

    concurrency.reserve_job_slot(db, scope="backup", label="L1")
    assert db.execute.call_count == 1


def test_should_cleanup_stale_backup_job_and_proceed(mocker):
    """Test that stale backup jobs are automatically cleaned up and new job can proceed."""
    db = mocker.Mock()

    db.query.side_effect = [
        [("backup", "stale_backup_label", "ACTIVE")],
        [("test_db",), ("ops",)],
        [("test_db", "stale_backup_label", "2024-01-01", "FINISHED")],
    ]

    concurrency.reserve_job_slot(db, scope="backup", label="new_backup_label")

    assert db.query.call_count == 3

    assert db.execute.call_count == 2

    cleanup_sql = db.execute.call_args_list[0][0][0]
    assert "UPDATE ops.run_status" in cleanup_sql
    assert "state='CANCELLED'" in cleanup_sql
    assert "stale_backup_label" in cleanup_sql

    insert_sql = db.execute.call_args_list[1][0][0]
    assert "INSERT INTO ops.run_status" in insert_sql
    assert "new_backup_label" in insert_sql
    assert "ACTIVE" in insert_sql


def test_should_raise_conflict_when_backup_job_is_still_active(mocker):
    """Test that real conflicts are still detected when backup job is actually running."""
    db = mocker.Mock()

    db.query.side_effect = [
        [("backup", "active_backup_label", "ACTIVE")],
        [("test_db",), ("ops",)],
        [("test_db", "active_backup_label", "2024-01-01", "UPLOADING")],
    ]

    try:
        concurrency.reserve_job_slot(db, scope="backup", label="new_backup_label")
        raise AssertionError("expected conflict")
    except RuntimeError as e:
        error_msg = str(e)
        assert "Concurrency conflict" in error_msg
        assert "Another 'backup' job is already ACTIVE" in error_msg
        assert "active_backup_label" in error_msg

    assert db.query.call_count == 3
    assert db.execute.call_count == 0


def test_should_cleanup_stale_job_when_not_found_in_show_backup(mocker):
    """Test that jobs not found in SHOW BACKUP are considered stale."""
    db = mocker.Mock()

    db.query.side_effect = [
        [("backup", "missing_backup_label", "ACTIVE")],
        [("test_db",), ("ops",)],
        [],
    ]

    concurrency.reserve_job_slot(db, scope="backup", label="new_backup_label")

    assert db.query.call_count == 3

    assert db.execute.call_count == 2

    cleanup_sql = db.execute.call_args_list[0][0][0]
    assert "UPDATE ops.run_status" in cleanup_sql
    assert "state='CANCELLED'" in cleanup_sql
    assert "missing_backup_label" in cleanup_sql


def test_should_handle_multiple_databases_in_stale_check(mocker):
    """Test that stale check works across multiple databases."""
    db = mocker.Mock()

    db.query.side_effect = [
        [("backup", "stale_backup_label", "ACTIVE")],
        [("db1",), ("db2",), ("ops",)],
        [],
        [("db2", "stale_backup_label", "2024-01-01", "FINISHED")],
    ]

    concurrency.reserve_job_slot(db, scope="backup", label="new_backup_label")

    assert db.query.call_count == 4

    assert db.execute.call_count == 2


def test_should_skip_system_databases_in_stale_check(mocker):
    """Test that system databases are skipped during stale check."""
    db = mocker.Mock()

    db.query.side_effect = [
        [("backup", "stale_backup_label", "ACTIVE")],
        [("information_schema",), ("mysql",), ("sys",), ("ops",), ("user_db",)],
        [("user_db", "stale_backup_label", "2024-01-01", "FINISHED")],
    ]

    concurrency.reserve_job_slot(db, scope="backup", label="new_backup_label")

    assert db.query.call_count == 3

    assert db.execute.call_count == 2


def test_should_handle_non_backup_scope_conflicts(mocker):
    """Test that non-backup scopes still raise conflicts (no self-healing for them)."""
    db = mocker.Mock()
    db.query.return_value = [("restore", "active_restore", "ACTIVE")]

    try:
        concurrency.reserve_job_slot(db, scope="restore", label="new_restore")
        raise AssertionError("expected conflict")
    except RuntimeError as e:
        error_msg = str(e)
        assert "Concurrency conflict" in error_msg
        assert "Another 'restore' job is already ACTIVE" in error_msg

    assert db.query.call_count == 1
    assert db.execute.call_count == 0


def test_should_handle_exception_during_stale_check(mocker):
    """Test that exceptions during stale check are handled gracefully."""
    db = mocker.Mock()

    db.query.side_effect = [
        [("backup", "stale_backup_label", "ACTIVE")],
        Exception("Database connection error"),
    ]

    try:
        concurrency.reserve_job_slot(db, scope="backup", label="new_backup_label")
        raise AssertionError("expected conflict")
    except RuntimeError as e:
        error_msg = str(e)
        assert "Concurrency conflict" in error_msg
        assert "Another 'backup' job is already ACTIVE" in error_msg

    assert db.query.call_count == 2
    assert db.execute.call_count == 0
