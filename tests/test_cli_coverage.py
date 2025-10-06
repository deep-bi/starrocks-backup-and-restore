from pathlib import Path

import tempfile
from click.testing import CliRunner

from starrocks_bbr.cli import cli, main
from tests.utils import write_cfg


def test_should_invoke_cli_version_via_runner_and_exit_zero():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])  # avoids importlib.metadata lookup path issues
    assert result.exit_code == 0


def test_should_handle_invalid_option_gracefully_with_friendly_message():
    runner = CliRunner()
    result = runner.invoke(cli, ["--does-not-exist"])  # returns non-zero
    assert result.exit_code == 2
    assert "Error:" in result.output or result.stderr


def test_should_execute_restore_command_and_echo_message(mocker):
    runner = CliRunner()
    # Mock run_restore to simulate a successful restore without DB logic
    mocker.patch("starrocks_bbr.cli.run_restore", return_value=None)

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
