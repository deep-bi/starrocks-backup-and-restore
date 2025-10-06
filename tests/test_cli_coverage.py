from pathlib import Path

import tempfile
from click.testing import CliRunner
from starrocks_bbr.cli import cli, main
from tests.utils import write_cfg


def test_should_invoke_cli_version_via_runner_and_exit_zero():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"]) 
    assert result.exit_code == 0


def test_should_handle_invalid_option_gracefully_with_friendly_message():
    runner = CliRunner()
    result = runner.invoke(cli, ["--does-not-exist"])
    assert result.exit_code == 2
    assert "Error:" in result.output or result.stderr


def test_should_execute_restore_command_and_echo_message(mocker):
    runner = CliRunner()
    mock_run_restore = mocker.patch("starrocks_bbr.cli.run_restore", return_value=None)

    with tempfile.TemporaryDirectory() as td:
        cfg = write_cfg(Path(td))
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
        assert "restore: completed" in result.output
        # Verify run_restore called with (db, table, timestamp)
        from unittest.mock import ANY

        mock_run_restore.assert_called_once()
        args, _ = mock_run_restore.call_args
        assert len(args) == 3
        assert args[0] is not None  # db instance
        assert args[1] == "db1.t1"
        assert args[2] == "2025-10-06 12:00:00"
