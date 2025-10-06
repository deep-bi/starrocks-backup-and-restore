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
        assert db_mock.query.call_count == 4, "Expected 4 query calls for restore workflow"

        # Ensure we executed RESTORE commands in the correct order: full first, then partitions
        executed_sqls = [call.args[0] for call in db_mock.execute.call_args_list]
        
        full_restore_idx = next(
            (i for i, s in enumerate(executed_sqls) if "RESTORE TABLE db1.t1 FROM snap_full AT '2025-10-05 12:00:00'" in s),
            None
        )
        partition_restore_idx = next(
            (i for i, s in enumerate(executed_sqls) if "RESTORE PARTITIONS (p1, p2) FOR TABLE db1.t1 FROM snap_inc AT '2025-10-06 10:00:00'" in s),
            None
        )
        
        # Verify both operations were executed and in correct order
        assert full_restore_idx is not None, "Full restore command not found"
        assert partition_restore_idx is not None, "Partition restore command not found"
        assert full_restore_idx < partition_restore_idx, "Full restore must execute before partition restore"


def test_should_exclude_incrementals_older_than_full_backup(db_mock):
    """Ensure that incrementals older/equal to the chosen full are excluded from the chain."""
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as td:
        cfg = write_cfg(Path(td))
        # Scenario:
        # Full_A: 2025-09-01 (ignored, older)
        # Inc_A:  2025-09-15 (belongs to Full_A; must be excluded)
        # Full_B: 2025-09-28 (chosen)
        # Inc_B:  2025-09-30 (belongs to Full_B; must be included)
        db_mock.query.side_effect = [
            [],  # table exists
            [("2025-09-28 00:00:00", "snap_full_B")],  # chosen full
            [
                ("2025-09-15 00:00:00", "snap_inc_A"),  # older than full_B -> exclude
                ("2025-09-30 00:00:00", "snap_inc_B"),  # after full_B -> include
            ],
            [("pB1",), ("pB2",)],  # partitions for inc_B
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
                "2025-10-01 00:00:00",
            ],
        )

        assert result.exit_code == 0
        executed_sqls = [call.args[0] for call in db_mock.execute.call_args_list]
        # Must include full_B and inc_B only; not inc_A
        assert any("RESTORE TABLE db1.t1 FROM snap_full_B AT '2025-09-28 00:00:00'" in s for s in executed_sqls)
        assert any("RESTORE PARTITIONS (pB1, pB2) FOR TABLE db1.t1 FROM snap_inc_B AT '2025-09-30 00:00:00'" in s for s in executed_sqls)
        assert not any("snap_inc_A" in s for s in executed_sqls)
