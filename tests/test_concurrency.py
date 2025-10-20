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
        assert False, "expected conflict"
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

    concurrency.complete_job_slot(db, scope="backup", label="db_20251015_incremental", final_state="FINISHED")

    assert db.execute.call_count == 1
    sql = db.execute.call_args[0][0]
    assert "UPDATE ops.run_status" in sql or "DELETE FROM ops.run_status" in sql
    assert "FINISHED" in sql or "state='IDLE'" in sql


def test_should_not_conflict_on_different_scope(mocker):
    db = mocker.Mock()
    db.query.return_value = [("restore", "some", "ACTIVE")]

    concurrency.reserve_job_slot(db, scope="backup", label="L1")
    assert db.execute.call_count == 1
