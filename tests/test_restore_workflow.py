from pathlib import Path
import tempfile

import pytest
from click.testing import CliRunner

from starrocks_bbr.cli import cli
from tests.utils import write_cfg


@pytest.fixture()
def db_mock(mocker):
    db_cls = mocker.patch("starrocks_bbr.cli.Database")
    return db_cls.return_value


def test_should_fail_when_target_table_already_exists(db_mock):
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as td:
        cfg = write_cfg(Path(td))
        # Simulate table exists
        db_mock.query.side_effect = [
            [("db1.t1",)],  # table exists check
        ]
        result = runner.invoke(
            cli,
            [
                "restore",
                "--config",
                str(cfg),
                "--table",
                "db1.t1",
                "--timestamp",
                "2025-10-06 12:00:00",
            ],
        )
        assert result.exit_code != 0
        assert "already exists" in result.output


def test_should_restore_full_then_incremental_partitions_in_order(db_mock):
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as td:
        cfg = write_cfg(Path(td))
        # Simulate: table does not exist, chain resolution queries
        # Side effects for db.query in restore flow:
        # 1) table exists check -> []
        # 2) find last full backup before target -> [("2025-10-05 12:00:00", "snap_full")]
        # 3) find incrementals before target -> [("2025-10-06 10:00:00", "snap_inc")]
        # 4) partitions for incrementals -> [("p1",), ("p2",)]
        db_mock.query.side_effect = [
            [],
            [("2025-10-05 12:00:00", "snap_full")],
            [("2025-10-06 10:00:00", "snap_inc")],
            [("p1",), ("p2",)],
        ]

        result = runner.invoke(
            cli,
            [
                "restore",
                "--config",
                str(cfg),
                "--table",
                "db1.t1",
                "--timestamp",
                "2025-10-06 12:00:00",
            ],
        )
        assert result.exit_code == 0

        # Ensure we executed RESTORE commands in the correct order: full first, then partitions
        executed_sqls = [call.args[0] for call in db_mock.execute.call_args_list]
        assert any("RESTORE TABLE db1.t1 FROM snap_full AT '2025-10-05 12:00:00'" in s for s in executed_sqls)
        assert any("RESTORE PARTITIONS (p1, p2) FOR TABLE db1.t1 FROM snap_inc AT '2025-10-06 10:00:00'" in s for s in executed_sqls)
