import types
from pathlib import Path

import pytest

from starrocks_bbr.backup import (
    BackupPlan,
    decide_backup_plan,
    get_last_successful_backup,
    issue_backup_commands,
    poll_backup_until_done,
    update_history_final,
)


@pytest.fixture()
def fake_db(mocker):
    class FakeDB:
        def __init__(self):
            self._exec_sql = []
            self._queries = []
            self._query_side_effect = []

        def execute(self, sql, params=()):
            self._exec_sql.append((sql, params))

        def query(self, sql, params=()):
            self._queries.append((sql, params))
            if self._query_side_effect:
                return self._query_side_effect.pop(0)
            return []

    return FakeDB()


def test_should_return_none_when_no_finished_backup_in_history(fake_db):
    """get_last_successful_backup returns None when history has no FINISHED entries (README §3.b.2)."""
    assert get_last_successful_backup(fake_db) is None


def test_should_choose_incremental_with_empty_tables_when_no_partitions_changed(fake_db, mocker):
    """decide_backup_plan picks incremental with no tables when nothing changed since last backup (README §3.b.4)."""
    mocker.patch("starrocks_bbr.backup.get_last_successful_backup", return_value="2025-10-05 12:00:00")
    mocker.patch("starrocks_bbr.backup.get_changed_partitions_since", return_value=[])

    plan = decide_backup_plan(fake_db, ["db1.t1", "db2.t2"])
    assert plan.backup_type == "incremental" and plan.tables == []


def test_should_issue_incremental_without_partitions_clause_when_parts_empty(fake_db):
    """issue_backup_commands should omit PARTITIONS clause if list is empty (incremental)."""
    plan = BackupPlan(backup_type="incremental", tables=["db1.t1"], partitions_by_table={"db1.t1": []})
    issue_backup_commands(fake_db, plan)
    assert any("BACKUP SNAPSHOT repo.db1.t1" in sql for sql, _ in fake_db._exec_sql)
    assert not any("PARTITIONS" in sql for sql, _ in fake_db._exec_sql)


def test_should_poll_until_finished_when_show_backup_initially_empty(fake_db, mocker):
    """poll_backup_until_done must sleep and retry on empty result, then return FINISHED."""
    fake_db._query_side_effect = [[], [("FINISHED", "2025-10-06 10:00:00")]]
    sleep_spy = mocker.patch("starrocks_bbr.backup.time.sleep")

    status, ts = poll_backup_until_done(fake_db)
    assert status == "FINISHED" and ts == "2025-10-06 10:00:00"
    sleep_spy.assert_called()


def test_should_update_history_to_failed_on_failure(fake_db):
    """update_history_final must mark FAILED with end_time when status is not FINISHED."""
    update_history_final(fake_db, "FAILED", None)
    executed = [sql for sql, _ in fake_db._exec_sql]
    assert any("SET status='FAILED'" in s for s in executed)
