from unittest.mock import patch

from src.starrocks_br import error_handler, exceptions


class TestErrorHandler:
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_display_structured_error_with_all_sections(self, mock_echo):
        error_handler.display_structured_error(
            title="TEST FAILED",
            reason="This is the reason",
            what_to_do=["Action 1", "Action 2"],
            inputs={"--config": "test.yaml", "--label": "test_label"},
            help_links=["Run --help"],
        )

        assert mock_echo.call_count > 0
        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "TEST FAILED" in output
        assert "REASON" in output
        assert "WHAT YOU CAN DO" in output
        assert "INPUT YOU PROVIDED" in output
        assert "NEED HELP" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_missing_option_error(self, mock_echo):
        exc = exceptions.MissingOptionError("--target-label")
        error_handler.handle_missing_option_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "OPERATION FAILED" in output
        assert "--target-label" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_backup_label_not_found_error(self, mock_echo):
        exc = exceptions.BackupLabelNotFoundError("my_label", "my_repo")
        error_handler.handle_backup_label_not_found_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "RESTORE FAILED" in output
        assert "my_label" in output
        assert "my_repo" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_no_successful_full_backup_found_error(self, mock_echo):
        exc = exceptions.NoSuccessfulFullBackupFoundError("incremental_label")
        error_handler.handle_no_successful_full_backup_found_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "RESTORE FAILED" in output
        assert "incremental_label" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_table_not_found_in_backup_error(self, mock_echo):
        exc = exceptions.TableNotFoundInBackupError("my_table", "my_label", "my_db")
        error_handler.handle_table_not_found_in_backup_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "TABLE NOT FOUND" in output
        assert "my_table" in output
        assert "my_label" in output
        assert "my_db" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_invalid_table_name_error(self, mock_echo):
        exc = exceptions.InvalidTableNameError("db.table", "Must not include database prefix")
        error_handler.handle_invalid_table_name_error(exc)

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "INVALID TABLE NAME" in output
        assert "db.table" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_config_file_not_found_error(self, mock_echo):
        exc = exceptions.ConfigFileNotFoundError("/path/to/config.yaml")
        error_handler.handle_config_file_not_found_error(exc)

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "CONFIG FILE NOT FOUND" in output
        assert "/path/to/config.yaml" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_config_validation_error(self, mock_echo):
        exc = exceptions.ConfigValidationError("Missing required field: host")
        error_handler.handle_config_validation_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "CONFIGURATION ERROR" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_cluster_health_check_failed_error(self, mock_echo):
        exc = exceptions.ClusterHealthCheckFailedError("Not enough alive backends")
        error_handler.handle_cluster_health_check_failed_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "CLUSTER HEALTH CHECK FAILED" in output
        assert "Not enough alive backends" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_snapshot_not_found_error(self, mock_echo):
        exc = exceptions.SnapshotNotFoundError("my_snapshot", "my_repo")
        error_handler.handle_snapshot_not_found_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "SNAPSHOT NOT FOUND" in output
        assert "my_snapshot" in output
        assert "my_repo" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_no_partitions_found_error(self, mock_echo):
        exc = exceptions.NoPartitionsFoundError("my_group")
        error_handler.handle_no_partitions_found_error(exc, config="test.yaml", group="my_group")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "NO PARTITIONS FOUND" in output
        assert "my_group" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_no_tables_found_error(self, mock_echo):
        exc = exceptions.NoTablesFoundError(group="my_group", label="my_label")
        error_handler.handle_no_tables_found_error(exc, config="test.yaml", target_label="my_label")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "NO TABLES FOUND" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_restore_operation_cancelled_error(self, mock_echo):
        error_handler.handle_restore_operation_cancelled_error()

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "OPERATION CANCELLED" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_handle_concurrency_conflict_error(self, mock_echo):
        active_jobs = [("backup", "active_backup_label", "ACTIVE")]
        exc = exceptions.ConcurrencyConflictError("backup", active_jobs)
        error_handler.handle_concurrency_conflict_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)

        assert "CONCURRENCY CONFLICT" in output
        assert "backup" in output
        assert "active_backup_label" in output

    @patch("src.starrocks_br.error_handler.click.echo")
    def test_display_structured_error_uses_stderr(self, mock_echo):
        error_handler.display_structured_error(
            title="TEST",
            reason="reason",
            what_to_do=["action"],
        )

        for call in mock_echo.call_args_list:
            if call[0]:
                assert call[1].get("err")
