from click.testing import CliRunner

from starrocks_br import cli


def test_init_command_success(config_file, mock_db, setup_password_env, mocker):
    """Test successful init command."""
    runner = CliRunner()

    mocker.patch("starrocks_br.schema.initialize_ops_schema")

    result = runner.invoke(cli.init, ["--config", config_file])

    assert result.exit_code == 0
    assert "Next steps:" in result.output
    assert "INSERT INTO ops.table_inventory" in result.output
