from starrocks_br import history


def test_should_write_backup_history_success(mocker):
    db = mocker.Mock()

    entry = {
        "label": "sales_db_20251015_inc",
        "backup_type": "incremental",
        "status": "FINISHED",
        "repository": "my_repo",
        "started_at": "2025-10-15 01:00:00",
        "finished_at": "2025-10-15 01:10:00",
        "error_message": None,
    }

    history.log_backup(db, entry)

    assert db.execute.call_count == 1
    sql = db.execute.call_args[0][0]
    assert "INSERT INTO ops.backup_history" in sql
    assert "sales_db_20251015_inc" in sql


def test_should_escape_null_error_message(mocker):
    db = mocker.Mock()

    entry = {
        "label": "weekly_backup_20251019",
        "backup_type": "weekly",
        "status": "FAILED",
        "repository": "my_repo",
        "started_at": "2025-10-19 01:00:00",
        "finished_at": "2025-10-19 01:10:00",
        "error_message": "Something went wrong",
    }

    history.log_backup(db, entry)
    sql = db.execute.call_args[0][0]
    assert "Something went wrong" in sql


def test_should_generate_job_id_when_missing(mocker):
    db = mocker.Mock()

    entry = {
        "label": "sales_db_20251015_monthly",
        "backup_type": "monthly",
        "status": "FINISHED",
        "repository": "repo",
        "started_at": "2025-10-15 01:00:00",
        "finished_at": "2025-10-15 02:00:00",
        "error_message": None,
    }

    history.log_backup(db, entry)
    sql = db.execute.call_args[0][0]
    assert "INSERT INTO ops.backup_history" in sql
    assert "sales_db_20251015_monthly" in sql


