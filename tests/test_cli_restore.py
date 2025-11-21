from click.testing import CliRunner

from starrocks_br import cli


def test_restore_success(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test successful restore operation."""
    runner = CliRunner()

    mocker.patch("starrocks_br.restore.find_restore_pair", return_value=["test_backup"])
    mocker.patch("starrocks_br.restore.get_tables_from_backup", return_value=["test_db.fact_table"])
    mocker.patch(
        "starrocks_br.restore.execute_restore_flow",
        return_value={
            "success": True,
            "message": "Restore completed successfully. Restored 1 tables.",
        },
    )
    mocker.patch("builtins.input", return_value="y")

    result = runner.invoke(
        cli.cli, ["restore", "--config", config_file, "--target-label", "test_backup"]
    )

    assert result.exit_code == 0
    assert "Restore completed successfully" in result.output


def test_restore_with_yes_flag_skips_confirmation(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that restore --yes flag skips confirmation prompt."""
    runner = CliRunner()

    mocker.patch("starrocks_br.restore.find_restore_pair", return_value=["test_backup"])
    mocker.patch("starrocks_br.restore.get_tables_from_backup", return_value=["test_db.fact_table"])

    execute_restore_flow_mock = mocker.patch(
        "starrocks_br.restore.execute_restore_flow",
        return_value={
            "success": True,
            "message": "Restore completed successfully. Restored 1 tables.",
        },
    )
    input_mock = mocker.patch("builtins.input")

    result = runner.invoke(
        cli.cli, ["restore", "--config", config_file, "--target-label", "test_backup", "--yes"]
    )

    assert result.exit_code == 0
    assert "Restore completed successfully" in result.output
    execute_restore_flow_mock.assert_called_once()
    assert execute_restore_flow_mock.call_args[1]["skip_confirmation"] is True
    input_mock.assert_not_called()


def test_restore_with_group_filter(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test restore with --group filter."""
    runner = CliRunner()

    mocker.patch("starrocks_br.restore.find_restore_pair", return_value=["test_backup"])
    get_tables_mock = mocker.patch(
        "starrocks_br.restore.get_tables_from_backup", return_value=["test_db.fact_table"]
    )
    mocker.patch(
        "starrocks_br.restore.execute_restore_flow",
        return_value={
            "success": True,
            "message": "Restore completed successfully. Restored 1 tables.",
        },
    )
    mocker.patch("builtins.input", return_value="y")

    result = runner.invoke(
        cli.cli,
        [
            "restore",
            "--config",
            config_file,
            "--target-label",
            "test_backup",
            "--group",
            "daily_incremental",
        ],
    )

    assert result.exit_code == 0
    assert "Restore completed successfully" in result.output
    assert get_tables_mock.call_args[1]["group"] == "daily_incremental"


def test_restore_with_table_filter(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test restore with --table filter."""
    runner = CliRunner()

    mocker.patch("starrocks_br.restore.find_restore_pair", return_value=["test_backup"])
    get_tables_mock = mocker.patch(
        "starrocks_br.restore.get_tables_from_backup", return_value=["test_db.fact_table"]
    )
    mocker.patch(
        "starrocks_br.restore.execute_restore_flow",
        return_value={
            "success": True,
            "message": "Restore completed successfully. Restored 1 tables.",
        },
    )
    mocker.patch("builtins.input", return_value="y")

    result = runner.invoke(
        cli.cli,
        [
            "restore",
            "--config",
            config_file,
            "--target-label",
            "test_backup",
            "--table",
            "fact_table",
        ],
    )

    assert result.exit_code == 0
    assert "Restore completed successfully" in result.output
    assert get_tables_mock.call_args[1]["table"] == "fact_table"
    assert get_tables_mock.call_args[1]["database"] == "test_db"
