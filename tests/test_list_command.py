from pathlib import Path

from click.testing import CliRunner

from starrocks_bbr.cli import cli
from .utils import write_cfg


def test_list_prints_history(tmp_path, mocker):
    cfg = write_cfg(tmp_path)

    # Mock DB used by CLI
    db_cls = mocker.patch("starrocks_bbr.cli.Database")
    db = db_cls.return_value
    db.query.return_value = [
        (1, "full", "FINISHED", "2025-10-05 12:00:00", "2025-10-05 12:10:00", "snap1", "2025-10-05 12:10:00", "ops", None),
        (2, "incremental", "FINISHED", "2025-10-06 11:00:00", "2025-10-06 11:05:00", "snap2", "2025-10-06 11:05:00", "ops", "db1.t1"),
    ]

    runner = CliRunner()
    result = runner.invoke(cli, ["list", "--config", str(cfg)])

    assert result.exit_code == 0
    out = result.output
    assert "ID" in out and "TYPE" in out and "STATUS" in out
    assert "1" in out and "full" in out and "snap1" in out
    assert "2" in out and "incremental" in out and "db1.t1" in out
