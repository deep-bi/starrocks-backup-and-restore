import os
import tempfile

import pytest
from click.testing import CliRunner

from starrocks_br import cli, exceptions

# ============================================================================
# PHASE 1: Scenario-Based Fixtures
# ============================================================================


@pytest.fixture
def config_file():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("""
        host: "127.0.0.1"
        port: 9030
        user: "root"
        database: "test_db"
        repository: "test_repo"
        """)
        f.flush()
        config_path = f.name

    yield config_path

    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def invalid_yaml_file():
    """Create a temporary invalid YAML file for testing error handling."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("invalid: yaml: content")
        f.flush()
        config_path = f.name

    yield config_path

    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def setup_password_env(monkeypatch):
    """Setup STARROCKS_PASSWORD environment variable for testing."""
    monkeypatch.setenv("STARROCKS_PASSWORD", "test_password")


@pytest.fixture
def mock_db(mocker):
    """Create a mocked StarRocksDB instance with context manager support."""
    mock = mocker.Mock()
    mock.__enter__ = mocker.Mock(return_value=mock)
    mock.__exit__ = mocker.Mock(return_value=False)
    mocker.patch("starrocks_br.db.StarRocksDB", return_value=mock)
    return mock


@pytest.fixture
def mock_initialized_schema(mocker):
    """Mock schema that already exists (ensure_ops_schema returns False)."""
    return mocker.patch("starrocks_br.schema.ensure_ops_schema", return_value=False)


@pytest.fixture
def mock_uninitialized_schema(mocker):
    """Mock schema that doesn't exist (ensure_ops_schema returns True - was created)."""
    return mocker.patch("starrocks_br.schema.ensure_ops_schema", return_value=True)


@pytest.fixture
def mock_healthy_cluster(mocker):
    """Mock a healthy cluster."""
    return mocker.patch("starrocks_br.health.check_cluster_health", return_value=(True, "Healthy"))


@pytest.fixture
def mock_unhealthy_cluster(mocker):
    """Mock an unhealthy cluster."""
    return mocker.patch(
        "starrocks_br.health.check_cluster_health", return_value=(False, "Cluster is unhealthy")
    )


@pytest.fixture
def mock_repo_exists(mocker):
    """Mock repository verification success."""
    return mocker.patch("starrocks_br.repository.ensure_repository")


# ============================================================================
# PHASE 2: Targeted Parameterized Tests for Untested Error Paths
# ============================================================================


@pytest.mark.parametrize(
    "command,baseline_flag",
    [
        ("backup_incremental", ["--baseline-backup", "test_baseline"]),
        ("backup_incremental", []),
        ("backup_full", []),
    ],
)
def test_backup_handles_snapshot_exists_error(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
    command,
    baseline_flag,
):
    """Test that backup commands correctly handle snapshot_exists errors with helpful retry guidance.

    Covers lines 30-49, 214-218, 323-327 in cli.py
    """
    runner = CliRunner()

    # Setup common mocks
    mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_backup_20251020")
    mocker.patch("starrocks_br.concurrency.reserve_job_slot")

    # Mock snapshot_exists error
    mocker.patch(
        "starrocks_br.executor.execute_backup",
        return_value={
            "success": False,
            "error_details": {
                "error_type": "snapshot_exists",
                "snapshot_name": "test_backup_20251020",
            },
        },
    )

    # Command-specific mocks
    if command == "backup_incremental":
        mocker.patch(
            "starrocks_br.planner.find_latest_full_backup",
            return_value={
                "label": "test_db_20251015_full",
                "backup_type": "full",
                "finished_at": "2025-10-15 10:00:00",
            },
        )
        mocker.patch(
            "starrocks_br.planner.find_recent_partitions",
            return_value=[
                {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
            ],
        )
        mocker.patch(
            "starrocks_br.planner.build_incremental_backup_command",
            return_value="BACKUP DATABASE test_db SNAPSHOT test_backup_20251020 TO test_repo",
        )
        mocker.patch("starrocks_br.planner.record_backup_partitions")

        args = ["--config", config_file, "--group", "daily_incremental"] + baseline_flag
        cmd = getattr(cli, command)
    else:  # backup_full
        mocker.patch(
            "starrocks_br.planner.build_full_backup_command",
            return_value="BACKUP DATABASE test_db SNAPSHOT test_backup_20251020 TO test_repo",
        )
        mocker.patch(
            "starrocks_br.planner.find_tables_by_group",
            return_value=[{"database": "test_db", "table": "dim_customers"}],
        )
        mocker.patch("starrocks_br.planner.get_all_partitions_for_tables", return_value=[])
        mocker.patch("starrocks_br.planner.record_backup_partitions")

        args = ["--config", config_file, "--group", "weekly_dimensions"]
        cmd = getattr(cli, command)

    result = runner.invoke(cmd, args)

    # Verify error handling
    assert result.exit_code == 1
    assert "Snapshot 'test_backup_20251020' already exists" in result.output
    assert "starrocks-br backup" in result.output  # Retry command suggestion
    assert "--name test_backup_20251020_retry" in result.output
    assert "SHOW SNAPSHOT ON test_repo" in result.output


@pytest.mark.parametrize(
    "command,args_factory",
    [
        ("backup_incremental", lambda cf: ["--config", cf, "--group", "daily_incremental"]),
        ("backup_full", lambda cf: ["--config", cf, "--group", "weekly_dimensions"]),
        ("cli", lambda cf: ["restore", "--config", cf, "--target-label", "test_backup"]),
    ],
)
def test_commands_exit_if_schema_is_auto_created(
    config_file,
    mock_db,
    mock_uninitialized_schema,
    setup_password_env,
    mocker,
    command,
    args_factory,
):
    """Test that all commands exit with warning when ops schema is auto-created.

    Covers lines 147-149, 268-270, 398-402 in cli.py
    """
    runner = CliRunner()
    args = args_factory(config_file)

    if command == "cli":
        result = runner.invoke(cli.cli, args)
    else:
        cmd = getattr(cli, command)
        result = runner.invoke(cmd, args)

    assert result.exit_code == 1
    assert "ops schema was auto-created" in result.output
    assert "starrocks-br init" in result.output
    assert "ops.table_inventory" in result.output


@pytest.mark.parametrize(
    "command,exception,expected_msg",
    [
        ("backup_incremental", FileNotFoundError("Config not found"), "CONFIG FILE NOT FOUND"),
        ("backup_incremental", ValueError("Invalid config"), "Error: Invalid config"),
        ("backup_incremental", RuntimeError("Health check failed"), "Health check failed"),
        ("backup_incremental", Exception("Unexpected"), "Unexpected error"),
        ("backup_full", FileNotFoundError("Config not found"), "CONFIG FILE NOT FOUND"),
        ("backup_full", ValueError("Invalid config"), "Error: Invalid config"),
        ("backup_full", RuntimeError("Repo error"), "Repo error"),
        ("backup_full", Exception("Unexpected"), "Unexpected error"),
        ("init", FileNotFoundError("Config not found"), "CONFIG FILE NOT FOUND"),
        ("init", exceptions.ConfigValidationError("Invalid config"), "CONFIGURATION ERROR"),
        ("init", Exception("Init failed"), "Failed to initialize schema"),
        ("cli", FileNotFoundError("Config not found"), "CONFIG FILE NOT FOUND"),
        ("cli", ValueError("Invalid config"), "CONFIGURATION ERROR"),
        ("cli", RuntimeError("Restore error"), "Restore error"),
        ("cli", Exception("Unexpected"), "Unexpected error"),
    ],
)
def test_commands_handle_exceptions(
    config_file, setup_password_env, mocker, command, exception, expected_msg
):
    """Test that all commands handle different exception types correctly.

    Covers exception handling blocks in:
    - backup_incremental: lines 228-239
    - backup_full: lines 337-346
    - init: lines 100-108
    - restore: lines 466-477
    """
    runner = CliRunner()

    # Mock to raise the specified exception
    mocker.patch("starrocks_br.config.load_config", side_effect=exception)

    if command == "cli":
        # For restore command
        result = runner.invoke(
            cli.cli, ["restore", "--config", config_file, "--target-label", "test"]
        )
    elif command == "init":
        result = runner.invoke(cli.init, ["--config", config_file])
    else:
        cmd = getattr(cli, command)
        result = runner.invoke(cmd, ["--config", config_file, "--group", "test_group"])

    assert result.exit_code == 1
    assert expected_msg in result.output


@pytest.mark.parametrize(
    "scenario,mock_setup,expected_msg",
    [
        (
            "LOST state in incremental backup",
            {
                "command": "backup_incremental",
                "result": {
                    "success": False,
                    "final_status": {"state": "LOST"},
                    "error_message": "Tracking lost",
                },
            },
            "CRITICAL: Backup tracking lost",
        ),
        (
            "LOST state in full backup",
            {
                "command": "backup_full",
                "result": {
                    "success": False,
                    "final_status": {"state": "LOST"},
                    "error_message": "Tracking lost",
                },
            },
            "CRITICAL: Backup tracking lost",
        ),
        (
            "No partitions found for incremental",
            {
                "command": "backup_incremental",
                "partitions": [],
            },
            "No partitions found to backup",
        ),
        (
            "No tables found for full backup",
            {
                "command": "backup_full",
                "backup_command": "",
            },
            "No tables found in group",
        ),
        (
            "No full backup found for incremental",
            {
                "command": "backup_incremental",
                "latest_backup": None,
            },
            "No full backup found",  # This should actually pass, just warning
        ),
    ],
)
def test_backup_logic_failures(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
    scenario,
    mock_setup,
    expected_msg,
):
    """Test specific business logic failure states in backup commands.

    Covers:
    - LOST state handling: lines 221-224, 330-333
    - No partitions: lines 184-186
    - No tables: lines 296-298
    - No full backup (warning only): lines 177-178
    """
    runner = CliRunner()
    command = mock_setup["command"]

    # Common mocks
    mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_backup_20251020")
    mocker.patch("starrocks_br.concurrency.reserve_job_slot")

    if command == "backup_incremental":
        latest_backup = mock_setup.get(
            "latest_backup",
            {
                "label": "test_db_20251015_full",
                "backup_type": "full",
                "finished_at": "2025-10-15 10:00:00",
            },
        )
        mocker.patch("starrocks_br.planner.find_latest_full_backup", return_value=latest_backup)

        partitions = mock_setup.get(
            "partitions",
            [{"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}],
        )
        mocker.patch("starrocks_br.planner.find_recent_partitions", return_value=partitions)
        mocker.patch(
            "starrocks_br.planner.build_incremental_backup_command",
            return_value="BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo",
        )
        mocker.patch("starrocks_br.planner.record_backup_partitions")

        if "result" in mock_setup:
            mocker.patch("starrocks_br.executor.execute_backup", return_value=mock_setup["result"])

        cmd = cli.backup_incremental
        args = ["--config", config_file, "--group", "daily_incremental"]

    else:  # backup_full
        backup_command = mock_setup.get(
            "backup_command", "BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo"
        )
        mocker.patch("starrocks_br.planner.build_full_backup_command", return_value=backup_command)
        mocker.patch(
            "starrocks_br.planner.find_tables_by_group",
            return_value=[{"database": "test_db", "table": "dim_customers"}],
        )
        mocker.patch("starrocks_br.planner.get_all_partitions_for_tables", return_value=[])
        mocker.patch("starrocks_br.planner.record_backup_partitions")

        if "result" in mock_setup:
            mocker.patch("starrocks_br.executor.execute_backup", return_value=mock_setup["result"])

        cmd = cli.backup_full
        args = ["--config", config_file, "--group", "weekly_dimensions"]

    result = runner.invoke(cmd, args)

    # For "No full backup found", it's just a warning, command continues
    if "No full backup found" in expected_msg:
        # This should actually not exit with error, just show warning
        # So we skip the exit_code check for this case
        pass
    else:
        assert result.exit_code == 1

    assert expected_msg in result.output


@pytest.mark.parametrize(
    "scenario,mock_behavior,expected_msg",
    [
        (
            "find_restore_pair raises ValueError",
            {"find_restore_pair": ValueError("Failed to find restore sequence")},
            "Configuration error",
        ),
        (
            "get_tables_from_backup raises ValueError",
            {"get_tables_from_backup": ValueError("Table not found in backup")},
            "Configuration error",
        ),
        (
            "No tables found in backup (empty list)",
            {"get_tables_from_backup": []},
            "NO TABLES FOUND",
        ),
    ],
)
def test_restore_logic_failures(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
    scenario,
    mock_behavior,
    expected_msg,
):
    """Test specific failure states in restore command.

    Covers:
    - find_restore_pair ValueError: lines 420-422
    - get_tables_from_backup ValueError: lines 434-436
    - Empty tables_to_restore: lines 438-445
    """
    runner = CliRunner()

    if "find_restore_pair" in mock_behavior:
        mocker.patch(
            "starrocks_br.restore.find_restore_pair", side_effect=mock_behavior["find_restore_pair"]
        )
    else:
        mocker.patch("starrocks_br.restore.find_restore_pair", return_value=["test_backup"])

    if "get_tables_from_backup" in mock_behavior:
        if isinstance(mock_behavior["get_tables_from_backup"], list):
            mocker.patch(
                "starrocks_br.restore.get_tables_from_backup",
                return_value=mock_behavior["get_tables_from_backup"],
            )
        else:
            mocker.patch(
                "starrocks_br.restore.get_tables_from_backup",
                side_effect=mock_behavior["get_tables_from_backup"],
            )

    result = runner.invoke(
        cli.cli, ["restore", "--config", config_file, "--target-label", "test_backup"]
    )

    assert result.exit_code == 1
    assert expected_msg in result.output


# ============================================================================
# PHASE 3: Minimal Happy Path Tests
# ============================================================================


def test_backup_incremental_success(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test successful incremental backup with default baseline (latest full backup)."""
    runner = CliRunner()

    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "test_db_20251015_full",
            "backup_type": "full",
            "finished_at": "2025-10-15 10:00:00",
        },
    )
    mocker.patch(
        "starrocks_br.planner.find_recent_partitions",
        return_value=[
            {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
        ],
    )
    mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_db_20251016_inc")
    mocker.patch(
        "starrocks_br.planner.build_incremental_backup_command",
        return_value="BACKUP DATABASE test_db SNAPSHOT test_db_20251016_inc TO test_repo",
    )
    mocker.patch("starrocks_br.concurrency.reserve_job_slot")
    mocker.patch("starrocks_br.planner.record_backup_partitions")
    mocker.patch(
        "starrocks_br.executor.execute_backup",
        return_value={
            "success": True,
            "final_status": {"state": "FINISHED"},
            "error_message": None,
        },
    )

    result = runner.invoke(
        cli.backup_incremental, ["--config", config_file, "--group", "daily_incremental"]
    )

    assert result.exit_code == 0
    assert "Backup completed successfully" in result.output
    assert "Using latest full backup as baseline: test_db_20251015_full (full)" in result.output


def test_backup_incremental_with_specific_baseline(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test incremental backup with user-specified baseline."""
    runner = CliRunner()

    mocker.patch(
        "starrocks_br.planner.find_recent_partitions",
        return_value=[
            {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
        ],
    )
    mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_db_20251016_inc")
    mocker.patch(
        "starrocks_br.planner.build_incremental_backup_command",
        return_value="BACKUP DATABASE test_db SNAPSHOT test_db_20251016_inc TO test_repo",
    )
    mocker.patch("starrocks_br.concurrency.reserve_job_slot")
    mocker.patch("starrocks_br.planner.record_backup_partitions")
    mocker.patch(
        "starrocks_br.executor.execute_backup",
        return_value={
            "success": True,
            "final_status": {"state": "FINISHED"},
            "error_message": None,
        },
    )

    result = runner.invoke(
        cli.backup_incremental,
        [
            "--config",
            config_file,
            "--baseline-backup",
            "test_db_20251010_full",
            "--group",
            "daily_incremental",
        ],
    )

    assert result.exit_code == 0
    assert "Backup completed successfully" in result.output
    assert "Using specified baseline backup: test_db_20251010_full" in result.output


def test_backup_full_success(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test successful full backup."""
    runner = CliRunner()

    mocker.patch(
        "starrocks_br.planner.build_full_backup_command",
        return_value="BACKUP DATABASE test_db SNAPSHOT test_db_20251016_full TO test_repo",
    )
    mocker.patch(
        "starrocks_br.planner.find_tables_by_group",
        return_value=[{"database": "test_db", "table": "dim_customers"}],
    )
    mocker.patch("starrocks_br.planner.get_all_partitions_for_tables", return_value=[])
    mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_db_20251016_full")
    mocker.patch("starrocks_br.concurrency.reserve_job_slot")
    mocker.patch("starrocks_br.planner.record_backup_partitions")
    mocker.patch(
        "starrocks_br.executor.execute_backup",
        return_value={
            "success": True,
            "final_status": {"state": "FINISHED"},
            "error_message": None,
        },
    )

    result = runner.invoke(
        cli.backup_full, ["--config", config_file, "--group", "weekly_dimensions"]
    )

    assert result.exit_code == 0
    assert "Backup completed successfully" in result.output


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


@pytest.mark.parametrize(
    "table_value,expected_msg",
    [
        ("test_db.fact_table", "Table name must not include database prefix"),
        ("   ", "Table name cannot be empty"),
    ],
)
def test_restore_table_validation(
    config_file, mock_db, mock_initialized_schema, setup_password_env, table_value, expected_msg
):
    """Test restore command validates table parameter correctly."""
    runner = CliRunner()

    result = runner.invoke(
        cli.cli,
        [
            "restore",
            "--config",
            config_file,
            "--target-label",
            "test_backup",
            "--table",
            table_value,
        ],
    )

    assert result.exit_code == 1
    assert expected_msg in result.output


def test_restore_fails_when_both_group_and_table_specified(
    config_file, mock_db, mock_initialized_schema, setup_password_env
):
    """Test restore command fails when both --group and --table are specified."""
    runner = CliRunner()

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
            "--group",
            "daily_incremental",
        ],
    )

    assert result.exit_code == 1
    assert "Cannot specify both --group and --table" in result.output


def test_init_command_success(config_file, mock_db, setup_password_env, mocker):
    """Test successful init command."""
    runner = CliRunner()

    mocker.patch("starrocks_br.schema.initialize_ops_schema")

    result = runner.invoke(cli.init, ["--config", config_file])

    assert result.exit_code == 0
    assert "Next steps:" in result.output
    assert "INSERT INTO ops.table_inventory" in result.output


def test_backup_incremental_unhealthy_cluster(
    config_file, mock_db, mock_initialized_schema, mock_unhealthy_cluster, setup_password_env
):
    """Test incremental backup fails when cluster is unhealthy."""
    runner = CliRunner()

    result = runner.invoke(cli.backup_incremental, ["--config", config_file, "--group", "daily"])

    assert result.exit_code == 1
    assert "unhealthy" in result.output.lower()


def test_backup_full_unhealthy_cluster(
    config_file, mock_db, mock_initialized_schema, mock_unhealthy_cluster, setup_password_env
):
    """Test full backup fails when cluster is unhealthy."""
    runner = CliRunner()

    result = runner.invoke(cli.backup_full, ["--config", config_file, "--group", "weekly"])

    assert result.exit_code == 1
    assert "unhealthy" in result.output.lower()


def test_restore_unhealthy_cluster(
    config_file, mock_db, mock_initialized_schema, mock_unhealthy_cluster, setup_password_env
):
    """Test restore fails when cluster is unhealthy."""
    runner = CliRunner()

    result = runner.invoke(
        cli.cli, ["restore", "--config", config_file, "--target-label", "test_backup"]
    )

    assert result.exit_code == 1
    assert "unhealthy" in result.output.lower()


def test_backup_job_slot_conflict(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test backup handles job slot reservation conflicts."""
    runner = CliRunner()

    mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_backup")
    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "test_db_20251015_full",
            "backup_type": "full",
            "finished_at": "2025-10-15 10:00:00",
        },
    )
    mocker.patch(
        "starrocks_br.planner.find_recent_partitions",
        return_value=[
            {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
        ],
    )
    mocker.patch(
        "starrocks_br.planner.build_incremental_backup_command",
        return_value="BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo",
    )
    mocker.patch(
        "starrocks_br.concurrency.reserve_job_slot",
        side_effect=RuntimeError("active job conflict for scope; retry later"),
    )

    result = runner.invoke(cli.backup_incremental, ["--config", config_file, "--group", "daily"])

    assert result.exit_code == 1
    assert "conflict" in result.output.lower() or "error" in result.output.lower()


def test_cli_main_group_requires_subcommand():
    """Test the main CLI group command requires a subcommand."""
    runner = CliRunner()
    result = runner.invoke(cli.cli, [])
    # Click behavior: older versions return 0, newer return 2
    assert result.exit_code in (0, 2)
    assert "Usage:" in result.output


def test_backup_group_requires_subcommand():
    """Test the backup group command requires a subcommand."""
    runner = CliRunner()
    result = runner.invoke(cli.backup, [])
    # Click behavior: older versions return 0, newer return 2
    assert result.exit_code in (0, 2)
    assert "Usage:" in result.output


def test_backup_reserves_slot_before_recording_partitions(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that backup reserves job slot before recording partitions (correct order)."""
    runner = CliRunner()
    call_order = []

    def mock_reserve_job_slot(*args, **kwargs):
        call_order.append("reserve_job_slot")

    def mock_record_backup_partitions(*args, **kwargs):
        call_order.append("record_backup_partitions")

    mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_backup")
    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "test_db_20251015_full",
            "backup_type": "full",
            "finished_at": "2025-10-15 10:00:00",
        },
    )
    mocker.patch(
        "starrocks_br.planner.find_recent_partitions",
        return_value=[
            {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
        ],
    )
    mocker.patch(
        "starrocks_br.planner.build_incremental_backup_command",
        return_value="BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo",
    )
    mocker.patch("starrocks_br.concurrency.reserve_job_slot", side_effect=mock_reserve_job_slot)
    mocker.patch(
        "starrocks_br.planner.record_backup_partitions", side_effect=mock_record_backup_partitions
    )
    mocker.patch(
        "starrocks_br.executor.execute_backup",
        return_value={
            "success": True,
            "final_status": {"state": "FINISHED"},
            "error_message": None,
        },
    )

    result = runner.invoke(cli.backup_incremental, ["--config", config_file, "--group", "daily"])

    assert result.exit_code == 0
    assert len(call_order) == 2
    assert call_order[0] == "reserve_job_slot"
    assert call_order[1] == "record_backup_partitions"


def test_backup_does_not_record_partitions_when_slot_reservation_fails(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that partitions are not recorded when job slot reservation fails."""
    runner = CliRunner()

    mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_backup")
    mocker.patch(
        "starrocks_br.planner.find_latest_full_backup",
        return_value={
            "label": "test_db_20251015_full",
            "backup_type": "full",
            "finished_at": "2025-10-15 10:00:00",
        },
    )
    mocker.patch(
        "starrocks_br.planner.find_recent_partitions",
        return_value=[
            {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
        ],
    )
    mocker.patch(
        "starrocks_br.planner.build_incremental_backup_command",
        return_value="BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo",
    )
    mocker.patch(
        "starrocks_br.concurrency.reserve_job_slot", side_effect=RuntimeError("active job conflict")
    )
    record_mock = mocker.patch("starrocks_br.planner.record_backup_partitions")

    result = runner.invoke(cli.backup_incremental, ["--config", config_file, "--group", "daily"])

    assert result.exit_code != 0
    record_mock.assert_not_called()


@pytest.mark.parametrize(
    "command,setup_mocks",
    [
        (
            "backup_incremental",
            {
                "find_latest_full_backup": {
                    "label": "test_db_20251015_full",
                    "backup_type": "full",
                    "finished_at": "2025-10-15 10:00:00",
                },
                "find_recent_partitions": [
                    {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
                ],
                "build_incremental_backup_command": "BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo",
            },
        ),
        (
            "backup_full",
            {
                "build_full_backup_command": "BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo",
                "find_tables_by_group": [{"database": "test_db", "table": "dim_customers"}],
                "get_all_partitions_for_tables": [],
            },
        ),
    ],
)
def test_backup_fails_with_non_lost_state(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
    command,
    setup_mocks,
):
    """Test backup failure with states other than LOST (e.g., CANCELLED, FAILED).

    Covers the else branch at lines 221->225 (incremental) and 330->334 (full).
    """
    runner = CliRunner()

    # Common mocks
    mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_backup")
    mocker.patch("starrocks_br.concurrency.reserve_job_slot")
    mocker.patch("starrocks_br.planner.record_backup_partitions")

    # Command-specific mocks
    for mock_name, mock_value in setup_mocks.items():
        mocker.patch(f"starrocks_br.planner.{mock_name}", return_value=mock_value)

    # Mock a failure with CANCELLED state (not LOST, not snapshot_exists)
    mocker.patch(
        "starrocks_br.executor.execute_backup",
        return_value={
            "success": False,
            "final_status": {"state": "CANCELLED"},
            "error_message": "Backup was cancelled by user",
        },
    )

    if command == "backup_incremental":
        cmd = cli.backup_incremental
    else:
        cmd = cli.backup_full

    result = runner.invoke(cmd, ["--config", config_file, "--group", "test_group"])

    assert result.exit_code == 1
    assert "Backup was cancelled by user" in result.output
    # Should NOT have CRITICAL message (that's only for LOST state)
    assert "CRITICAL" not in result.output


def test_restore_failure(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test restore command when execute_restore_flow fails.

    Covers lines 463-464.
    """
    runner = CliRunner()

    mocker.patch("starrocks_br.restore.find_restore_pair", return_value=["test_backup"])
    mocker.patch("starrocks_br.restore.get_tables_from_backup", return_value=["test_db.fact_table"])
    mocker.patch(
        "starrocks_br.restore.execute_restore_flow",
        return_value={
            "success": False,
            "error_message": "Restore operation failed: permission denied",
        },
    )
    mocker.patch("builtins.input", return_value="y")

    result = runner.invoke(
        cli.cli, ["restore", "--config", config_file, "--target-label", "test_backup"]
    )

    assert result.exit_code == 1
    assert "Restore failed: Restore operation failed: permission denied" in result.output


@pytest.mark.parametrize(
    "filter_type,filter_value,expected_line",
    [
        (
            "group",
            "nonexistent_group",
            "NO TABLES FOUND",
        ),
        (
            "table",
            "nonexistent_table",
            "NO TABLES FOUND",
        ),
    ],
)
def test_restore_no_tables_found_with_filters(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
    filter_type,
    filter_value,
    expected_line,
):
    """Test restore command when no tables found with specific filters.

    Covers lines 440 (group filter) and 442 (table filter).
    """
    runner = CliRunner()

    mocker.patch("starrocks_br.restore.find_restore_pair", return_value=["test_backup"])
    mocker.patch("starrocks_br.restore.get_tables_from_backup", return_value=[])

    if filter_type == "group":
        result = runner.invoke(
            cli.cli,
            [
                "restore",
                "--config",
                config_file,
                "--target-label",
                "test_backup",
                "--group",
                filter_value,
            ],
        )
    else:  # table
        result = runner.invoke(
            cli.cli,
            [
                "restore",
                "--config",
                config_file,
                "--target-label",
                "test_backup",
                "--table",
                filter_value,
            ],
        )

    assert result.exit_code == 1
    assert expected_line in result.output


def test_restore_displays_rich_error_for_backup_label_not_found(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that restore displays rich error message when backup label is not found."""
    runner = CliRunner()

    from starrocks_br import exceptions

    mocker.patch(
        "starrocks_br.restore.find_restore_pair",
        side_effect=exceptions.BackupLabelNotFoundError("nonexistent_label", "test_repo"),
    )

    result = runner.invoke(
        cli.cli, ["restore", "--config", config_file, "--target-label", "nonexistent_label"]
    )

    assert result.exit_code == 1
    assert "RESTORE FAILED" in result.output
    assert "nonexistent_label" in result.output
    assert "test_repo" in result.output
    assert "REASON" in result.output
    assert "WHAT YOU CAN DO" in result.output


def test_restore_displays_rich_error_for_invalid_table_name(
    config_file,
    mock_db,
    mock_initialized_schema,
    setup_password_env,
):
    """Test that restore displays rich error message for invalid table names."""
    runner = CliRunner()

    result = runner.invoke(
        cli.cli,
        [
            "restore",
            "--config",
            config_file,
            "--target-label",
            "test_backup",
            "--table",
            "database.table",
        ],
    )

    assert result.exit_code == 1
    assert "INVALID TABLE NAME" in result.output
    assert "database.table" in result.output
    assert "REASON" in result.output
    assert "WHAT YOU CAN DO" in result.output


def test_restore_displays_rich_error_for_table_not_found_in_backup(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that restore displays rich error when table is not found in backup."""
    runner = CliRunner()

    from starrocks_br import exceptions

    mocker.patch("starrocks_br.restore.find_restore_pair", return_value=["test_backup"])
    mocker.patch(
        "starrocks_br.restore.get_tables_from_backup",
        side_effect=exceptions.TableNotFoundInBackupError("my_table", "test_backup", "test_db"),
    )

    result = runner.invoke(
        cli.cli,
        [
            "restore",
            "--config",
            config_file,
            "--target-label",
            "test_backup",
            "--table",
            "my_table",
        ],
    )

    assert result.exit_code == 1
    assert "TABLE NOT FOUND" in result.output
    assert "my_table" in result.output
    assert "test_backup" in result.output
    assert "test_db" in result.output


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
