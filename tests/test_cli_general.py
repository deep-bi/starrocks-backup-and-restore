from click.testing import CliRunner

from starrocks_br import cli


def test_cli_verbose_flag_enables_debug_logging(config_file, mocker):
    """Test that --verbose flag enables debug logging."""
    runner = CliRunner()

    setup_logging_mock = mocker.patch("starrocks_br.logger.setup_logging")

    runner.invoke(cli.cli, ["--verbose", "restore", "--help"])

    import logging

    setup_logging_mock.assert_called_once_with(level=logging.DEBUG)


def test_cli_without_verbose_uses_info_logging(config_file, mocker):
    """Test that CLI without --verbose uses INFO level logging."""
    runner = CliRunner()

    setup_logging_mock = mocker.patch("starrocks_br.logger.setup_logging")

    runner.invoke(cli.cli, ["restore", "--help"])

    setup_logging_mock.assert_called_once_with()


def test_cli_main_group_requires_subcommand():
    """Test the main CLI group command requires a subcommand."""
    runner = CliRunner()
    result = runner.invoke(cli.cli, [])
    assert result.exit_code in (0, 2)
    assert "Usage:" in result.output


def test_backup_group_requires_subcommand():
    """Test the backup group command requires a subcommand."""
    runner = CliRunner()
    result = runner.invoke(cli.backup, [])
    assert result.exit_code in (0, 2)
    assert "Usage:" in result.output
