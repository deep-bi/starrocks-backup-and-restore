import pytest
from click.testing import CliRunner

from starrocks_br import cli, exceptions


class TestBackupIncrementalExceptionHandling:
    """Test exception handling in backup incremental command."""

    def test_handles_config_file_not_found_error(self, mocker):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.config.load_config", side_effect=FileNotFoundError("Config not found")
        )

        result = runner.invoke(
            cli.backup_incremental,
            ["--config", "nonexistent.yaml", "--group", "test_group"],
        )

        assert result.exit_code == 1
        assert "CONFIG FILE NOT FOUND" in result.output

    def test_handles_config_validation_error(self, config_file, mocker):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.config.load_config",
            side_effect=exceptions.ConfigValidationError("Missing required field: host"),
        )

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "test_group"]
        )

        assert result.exit_code == 1
        assert "CONFIGURATION ERROR" in result.output

    def test_handles_no_full_backup_found_error(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.planner.find_latest_full_backup",
            return_value=None,
        )
        mocker.patch(
            "starrocks_br.planner.find_recent_partitions",
            side_effect=exceptions.NoFullBackupFoundError("test_db"),
        )

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "test_group"]
        )

        assert result.exit_code == 1
        assert "NO FULL BACKUP FOUND" in result.output
        assert "test_db" in result.output

    def test_handles_backup_label_not_found_error(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.planner.find_recent_partitions",
            side_effect=exceptions.BackupLabelNotFoundError("invalid_baseline", "test_repo"),
        )

        result = runner.invoke(
            cli.backup_incremental,
            [
                "--config",
                config_file,
                "--group",
                "test_group",
                "--baseline-backup",
                "invalid_baseline",
            ],
        )

        assert result.exit_code == 1
        assert "RESTORE FAILED" in result.output
        assert "invalid_baseline" in result.output

    def test_handles_concurrency_conflict_error(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()
        active_jobs = [("backup", "existing_backup", "ACTIVE")]
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
            return_value=[{"database": "test_db", "table": "test_table", "partition_name": "p1"}],
        )
        mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_label")
        mocker.patch(
            "starrocks_br.planner.build_incremental_backup_command", return_value="BACKUP ..."
        )
        mocker.patch(
            "starrocks_br.concurrency.reserve_job_slot",
            side_effect=exceptions.ConcurrencyConflictError("backup", active_jobs),
        )

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "test_group"]
        )

        assert result.exit_code == 1
        assert "CONCURRENCY CONFLICT" in result.output
        assert "existing_backup" in result.output

    @pytest.mark.parametrize(
        "baseline_flag",
        [
            ["--baseline-backup", "test_baseline"],
            [],
        ],
    )
    def test_handles_snapshot_exists_error(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
        baseline_flag,
    ):
        runner = CliRunner()

        mocker.patch(
            "starrocks_br.labels.determine_backup_label", return_value="test_backup_20251020"
        )
        mocker.patch("starrocks_br.concurrency.reserve_job_slot")
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
        result = runner.invoke(cli.backup_incremental, args)

        assert result.exit_code == 1
        assert "Snapshot 'test_backup_20251020' already exists" in result.output
        assert "starrocks-br backup" in result.output
        assert "--name test_backup_20251020_retry" in result.output
        assert "SHOW SNAPSHOT ON test_repo" in result.output

    def test_exits_if_schema_is_auto_created(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_uninitialized_schema,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
    ):
        runner = CliRunner()

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "daily_incremental"]
        )

        assert result.exit_code == 1
        assert "ops schema was auto-created" in result.output
        assert "starrocks-br init" in result.output
        assert "ops.table_inventory" in result.output

    def test_handles_lost_state(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

        mocker.patch(
            "starrocks_br.labels.determine_backup_label", return_value="test_backup_20251020"
        )
        mocker.patch("starrocks_br.concurrency.reserve_job_slot")
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
        mocker.patch("starrocks_br.planner.record_backup_partitions")
        mocker.patch(
            "starrocks_br.executor.execute_backup",
            return_value={
                "success": False,
                "final_status": {"state": "LOST"},
                "error_message": "Tracking lost",
            },
        )

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "daily_incremental"]
        )

        assert result.exit_code == 1
        assert "CRITICAL: Backup tracking lost" in result.output

    def test_handles_no_partitions_found(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

        mocker.patch(
            "starrocks_br.labels.determine_backup_label", return_value="test_backup_20251020"
        )
        mocker.patch("starrocks_br.concurrency.reserve_job_slot")
        mocker.patch(
            "starrocks_br.planner.find_latest_full_backup",
            return_value={
                "label": "test_db_20251015_full",
                "backup_type": "full",
                "finished_at": "2025-10-15 10:00:00",
            },
        )
        mocker.patch("starrocks_br.planner.find_recent_partitions", return_value=[])
        mocker.patch(
            "starrocks_br.planner.build_incremental_backup_command",
            return_value="BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo",
        )
        mocker.patch("starrocks_br.planner.record_backup_partitions")

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "daily_incremental"]
        )

        assert result.exit_code == 1
        assert "No partitions found to backup" in result.output

    def test_unhealthy_cluster(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_unhealthy_cluster,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
    ):
        runner = CliRunner()

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "daily"]
        )

        assert result.exit_code == 1
        assert "unhealthy" in result.output.lower()

    def test_handles_job_slot_conflict(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
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

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "daily"]
        )

        assert result.exit_code == 1
        assert "conflict" in result.output.lower() or "error" in result.output.lower()

    def test_does_not_record_partitions_when_slot_reservation_fails(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
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
            side_effect=RuntimeError("active job conflict"),
        )
        record_mock = mocker.patch("starrocks_br.planner.record_backup_partitions")

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "daily"]
        )

        assert result.exit_code != 0
        record_mock.assert_not_called()

    def test_fails_with_non_lost_state(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

        mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_backup")
        mocker.patch("starrocks_br.concurrency.reserve_job_slot")
        mocker.patch("starrocks_br.planner.record_backup_partitions")
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
            "starrocks_br.executor.execute_backup",
            return_value={
                "success": False,
                "final_status": {"state": "CANCELLED"},
                "error_message": "Backup was cancelled by user",
            },
        )

        result = runner.invoke(
            cli.backup_incremental, ["--config", config_file, "--group", "test_group"]
        )

        assert result.exit_code == 1
        assert "Backup was cancelled by user" in result.output
        assert "CRITICAL" not in result.output


class TestBackupFullExceptionHandling:
    """Test exception handling in backup full command."""

    def test_handles_config_file_not_found_error(self, mocker):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.config.load_config", side_effect=FileNotFoundError("Config not found")
        )

        result = runner.invoke(
            cli.backup_full, ["--config", "nonexistent.yaml", "--group", "test_group"]
        )

        assert result.exit_code == 1
        assert "CONFIG FILE NOT FOUND" in result.output

    def test_handles_config_validation_error(self, config_file, mocker):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.config.load_config",
            side_effect=exceptions.ConfigValidationError("Missing required field: repository"),
        )

        result = runner.invoke(cli.backup_full, ["--config", config_file, "--group", "test_group"])

        assert result.exit_code == 1
        assert "CONFIGURATION ERROR" in result.output

    def test_handles_concurrency_conflict_error(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()
        active_jobs = [("backup", "existing_full_backup", "ACTIVE")]
        mocker.patch(
            "starrocks_br.planner.find_tables_by_group",
            return_value=[{"database": "test_db", "table": "test_table"}],
        )
        mocker.patch(
            "starrocks_br.planner.build_full_backup_command", return_value="BACKUP DATABASE ..."
        )
        mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_label")
        mocker.patch("starrocks_br.planner.get_all_partitions_for_tables", return_value=[])
        mocker.patch(
            "starrocks_br.concurrency.reserve_job_slot",
            side_effect=exceptions.ConcurrencyConflictError("backup", active_jobs),
        )

        result = runner.invoke(cli.backup_full, ["--config", config_file, "--group", "test_group"])

        assert result.exit_code == 1
        assert "CONCURRENCY CONFLICT" in result.output
        assert "existing_full_backup" in result.output

    def test_handles_snapshot_exists_error(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

        mocker.patch(
            "starrocks_br.labels.determine_backup_label", return_value="test_backup_20251020"
        )
        mocker.patch("starrocks_br.concurrency.reserve_job_slot")
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
        result = runner.invoke(cli.backup_full, args)

        assert result.exit_code == 1
        assert "Snapshot 'test_backup_20251020' already exists" in result.output
        assert "starrocks-br backup" in result.output
        assert "--name test_backup_20251020_retry" in result.output
        assert "SHOW SNAPSHOT ON test_repo" in result.output

    def test_exits_if_schema_is_auto_created(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_uninitialized_schema,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
    ):
        runner = CliRunner()

        result = runner.invoke(
            cli.backup_full, ["--config", config_file, "--group", "weekly_dimensions"]
        )

        assert result.exit_code == 1
        assert "ops schema was auto-created" in result.output
        assert "starrocks-br init" in result.output
        assert "ops.table_inventory" in result.output

    def test_handles_lost_state(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

        mocker.patch(
            "starrocks_br.labels.determine_backup_label", return_value="test_backup_20251020"
        )
        mocker.patch("starrocks_br.concurrency.reserve_job_slot")
        mocker.patch(
            "starrocks_br.planner.build_full_backup_command",
            return_value="BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo",
        )
        mocker.patch(
            "starrocks_br.planner.find_tables_by_group",
            return_value=[{"database": "test_db", "table": "dim_customers"}],
        )
        mocker.patch("starrocks_br.planner.get_all_partitions_for_tables", return_value=[])
        mocker.patch("starrocks_br.planner.record_backup_partitions")
        mocker.patch(
            "starrocks_br.executor.execute_backup",
            return_value={
                "success": False,
                "final_status": {"state": "LOST"},
                "error_message": "Tracking lost",
            },
        )

        result = runner.invoke(
            cli.backup_full, ["--config", config_file, "--group", "weekly_dimensions"]
        )

        assert result.exit_code == 1
        assert "CRITICAL: Backup tracking lost" in result.output

    def test_handles_no_tables_found(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

        mocker.patch(
            "starrocks_br.labels.determine_backup_label", return_value="test_backup_20251020"
        )
        mocker.patch("starrocks_br.concurrency.reserve_job_slot")
        mocker.patch("starrocks_br.planner.build_full_backup_command", return_value="")
        mocker.patch(
            "starrocks_br.planner.find_tables_by_group",
            return_value=[{"database": "test_db", "table": "dim_customers"}],
        )
        mocker.patch("starrocks_br.planner.get_all_partitions_for_tables", return_value=[])
        mocker.patch("starrocks_br.planner.record_backup_partitions")

        result = runner.invoke(
            cli.backup_full, ["--config", config_file, "--group", "weekly_dimensions"]
        )

        assert result.exit_code == 1
        assert "No tables found in group" in result.output

    def test_unhealthy_cluster(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_unhealthy_cluster,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
    ):
        runner = CliRunner()

        result = runner.invoke(cli.backup_full, ["--config", config_file, "--group", "weekly"])

        assert result.exit_code == 1
        assert "unhealthy" in result.output.lower()

    def test_fails_with_non_lost_state(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

        mocker.patch("starrocks_br.labels.determine_backup_label", return_value="test_backup")
        mocker.patch("starrocks_br.concurrency.reserve_job_slot")
        mocker.patch("starrocks_br.planner.record_backup_partitions")
        mocker.patch(
            "starrocks_br.planner.build_full_backup_command",
            return_value="BACKUP DATABASE test_db SNAPSHOT test_backup TO test_repo",
        )
        mocker.patch(
            "starrocks_br.planner.find_tables_by_group",
            return_value=[{"database": "test_db", "table": "dim_customers"}],
        )
        mocker.patch("starrocks_br.planner.get_all_partitions_for_tables", return_value=[])
        mocker.patch(
            "starrocks_br.executor.execute_backup",
            return_value={
                "success": False,
                "final_status": {"state": "CANCELLED"},
                "error_message": "Backup was cancelled by user",
            },
        )

        result = runner.invoke(cli.backup_full, ["--config", config_file, "--group", "test_group"])

        assert result.exit_code == 1
        assert "Backup was cancelled by user" in result.output
        assert "CRITICAL" not in result.output


class TestInitExceptionHandling:
    """Test exception handling in init command."""

    def test_handles_config_file_not_found_error(self, mocker):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.config.load_config", side_effect=FileNotFoundError("Config not found")
        )

        result = runner.invoke(cli.init, ["--config", "nonexistent.yaml"])

        assert result.exit_code == 1
        assert "CONFIG FILE NOT FOUND" in result.output

    def test_handles_config_validation_error(self, config_file, mocker):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.config.load_config",
            side_effect=exceptions.ConfigValidationError("Missing required field: database"),
        )

        result = runner.invoke(cli.init, ["--config", config_file])

        assert result.exit_code == 1
        assert "CONFIGURATION ERROR" in result.output


class TestRestoreExceptionHandling:
    """Test exception handling in restore command."""

    def test_handles_config_file_not_found_error(self, mocker):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.config.load_config", side_effect=FileNotFoundError("Config not found")
        )

        result = runner.invoke(
            cli.cli, ["restore", "--config", "nonexistent.yaml", "--target-label", "test_label"]
        )

        assert result.exit_code == 1
        assert "CONFIG FILE NOT FOUND" in result.output

    def test_handles_config_validation_error(self, config_file, mocker):
        runner = CliRunner()
        mocker.patch(
            "starrocks_br.config.validate_config",
            side_effect=exceptions.ConfigValidationError("Invalid TLS config"),
        )

        result = runner.invoke(
            cli.cli, ["restore", "--config", config_file, "--target-label", "test_label"]
        )

        assert result.exit_code == 1
        assert "CONFIGURATION ERROR" in result.output

    def test_exits_if_schema_is_auto_created(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_uninitialized_schema,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
    ):
        runner = CliRunner()

        result = runner.invoke(
            cli.cli, ["restore", "--config", config_file, "--target-label", "test_backup"]
        )

        assert result.exit_code == 1
        assert "ops schema was auto-created" in result.output
        assert "starrocks-br init" in result.output
        assert "ops.table_inventory" in result.output

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
    def test_logic_failures(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
        scenario,  # noqa: ARG002
        mock_behavior,
        expected_msg,
    ):
        runner = CliRunner()

        if "find_restore_pair" in mock_behavior:
            mocker.patch(
                "starrocks_br.restore.find_restore_pair",
                side_effect=mock_behavior["find_restore_pair"],
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

    @pytest.mark.parametrize(
        "table_value,expected_msg",
        [
            ("test_db.fact_table", "Table name must not include database prefix"),
            ("   ", "Table name cannot be empty"),
        ],
    )
    def test_table_validation(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        table_value,
        expected_msg,
    ):
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

    def test_fails_when_both_group_and_table_specified(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
    ):
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

    def test_unhealthy_cluster(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_unhealthy_cluster,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
    ):
        runner = CliRunner()

        result = runner.invoke(
            cli.cli, ["restore", "--config", config_file, "--target-label", "test_backup"]
        )

        assert result.exit_code == 1
        assert "unhealthy" in result.output.lower()

    def test_restore_failure(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

        mocker.patch("starrocks_br.restore.find_restore_pair", return_value=["test_backup"])
        mocker.patch(
            "starrocks_br.restore.get_tables_from_backup", return_value=["test_db.fact_table"]
        )
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
    def test_no_tables_found_with_filters(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
        filter_type,
        filter_value,
        expected_line,
    ):
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
        else:
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

    def test_displays_rich_error_for_backup_label_not_found(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

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

    def test_displays_rich_error_for_invalid_table_name(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
    ):
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

    def test_displays_rich_error_for_table_not_found_in_backup(
        self,
        config_file,
        mock_db,  # noqa: ARG002
        mock_initialized_schema,  # noqa: ARG002
        mock_healthy_cluster,  # noqa: ARG002
        mock_repo_exists,  # noqa: ARG002
        setup_password_env,  # noqa: ARG002
        mocker,
    ):
        runner = CliRunner()

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
