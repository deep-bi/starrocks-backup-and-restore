# Copyright 2025 deep-bi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest.mock import patch

from src.starrocks_br import error_handler, exceptions


class TestGetOpsDatabaseName:
    @patch("src.starrocks_br.error_handler.config_module.load_config")
    @patch("src.starrocks_br.error_handler.config_module.get_ops_database")
    def test_returns_custom_ops_database_from_config(self, mock_get_ops, mock_load):
        mock_cfg = {"ops_database": "custom_ops"}
        mock_load.return_value = mock_cfg
        mock_get_ops.return_value = "custom_ops"

        result = error_handler._get_ops_database_name("/path/to/config.yaml")

        assert result == "custom_ops"
        mock_load.assert_called_once_with("/path/to/config.yaml")
        mock_get_ops.assert_called_once_with(mock_cfg)

    def test_returns_default_when_config_path_is_none(self):
        result = error_handler._get_ops_database_name(None)
        assert result == "ops"

    @patch("src.starrocks_br.error_handler.config_module.load_config")
    def test_returns_default_when_load_config_raises_exception(self, mock_load):
        mock_load.side_effect = Exception("Config error")

        result = error_handler._get_ops_database_name("/path/to/config.yaml")

        assert result == "ops"

    @patch("src.starrocks_br.error_handler.config_module.load_config")
    @patch("src.starrocks_br.error_handler.config_module.get_ops_database")
    def test_returns_default_when_get_ops_database_raises_exception(self, mock_get_ops, mock_load):
        mock_load.return_value = {"some": "config"}
        mock_get_ops.side_effect = Exception("Get ops error")

        result = error_handler._get_ops_database_name("/path/to/config.yaml")

        assert result == "ops"


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


class TestErrorHandlerWithCustomOpsDatabase:
    @patch("src.starrocks_br.error_handler._get_ops_database_name")
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_backup_label_not_found_uses_custom_ops_database(self, mock_echo, mock_get_ops):
        mock_get_ops.return_value = "custom_ops"
        exc = exceptions.BackupLabelNotFoundError("my_label", "my_repo")

        error_handler.handle_backup_label_not_found_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)
        assert "custom_ops.backup_history" in output
        assert " ops.backup_history" not in output
        mock_get_ops.assert_called_once_with("test.yaml")

    @patch("src.starrocks_br.error_handler._get_ops_database_name")
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_backup_label_not_found_defaults_to_ops_when_no_config(self, mock_echo, mock_get_ops):
        mock_get_ops.return_value = "ops"
        exc = exceptions.BackupLabelNotFoundError("my_label", "my_repo")

        error_handler.handle_backup_label_not_found_error(exc, config=None)

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)
        assert "ops.backup_history" in output
        mock_get_ops.assert_called_once_with(None)

    @patch("src.starrocks_br.error_handler._get_ops_database_name")
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_no_successful_full_backup_uses_custom_ops_database(self, mock_echo, mock_get_ops):
        mock_get_ops.return_value = "my_ops_db"
        exc = exceptions.NoSuccessfulFullBackupFoundError("inc_label")

        error_handler.handle_no_successful_full_backup_found_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)
        assert "my_ops_db.backup_history" in output
        assert " ops.backup_history" not in output

    @patch("src.starrocks_br.error_handler._get_ops_database_name")
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_table_not_found_in_backup_uses_custom_ops_database(self, mock_echo, mock_get_ops):
        mock_get_ops.return_value = "custom_ops"
        exc = exceptions.TableNotFoundInBackupError("my_table", "my_label", "my_db")

        error_handler.handle_table_not_found_in_backup_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)
        assert "custom_ops.backup_partitions" in output
        assert " ops.backup_partitions" not in output

    @patch("src.starrocks_br.error_handler._get_ops_database_name")
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_snapshot_not_found_uses_custom_ops_database(self, mock_echo, mock_get_ops):
        mock_get_ops.return_value = "custom_ops"
        exc = exceptions.SnapshotNotFoundError("my_snapshot", "my_repo")

        error_handler.handle_snapshot_not_found_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)
        assert "custom_ops.backup_history" in output
        assert " ops.backup_history" not in output

    @patch("src.starrocks_br.error_handler._get_ops_database_name")
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_no_partitions_found_uses_custom_ops_database(self, mock_echo, mock_get_ops):
        mock_get_ops.return_value = "custom_ops"
        exc = exceptions.NoPartitionsFoundError("my_group")

        error_handler.handle_no_partitions_found_error(exc, config="test.yaml", group="my_group")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)
        assert "custom_ops.table_inventory" in output
        assert " ops.table_inventory" not in output

    @patch("src.starrocks_br.error_handler._get_ops_database_name")
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_no_tables_found_uses_custom_ops_database(self, mock_echo, mock_get_ops):
        mock_get_ops.return_value = "custom_ops"
        exc = exceptions.NoTablesFoundError(group="my_group", label="my_label")

        error_handler.handle_no_tables_found_error(exc, config="test.yaml", target_label="my_label")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)
        assert "custom_ops.backup_partitions" in output
        assert "custom_ops.table_inventory" in output
        assert "custom_ops.backup_history" in output
        assert " ops.backup_partitions" not in output
        assert " ops.table_inventory" not in output
        assert " ops.backup_history" not in output

    @patch("src.starrocks_br.error_handler._get_ops_database_name")
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_concurrency_conflict_uses_custom_ops_database(self, mock_echo, mock_get_ops):
        mock_get_ops.return_value = "custom_ops"
        active_jobs = [("backup", "active_label", "ACTIVE")]
        exc = exceptions.ConcurrencyConflictError("backup", active_jobs)

        error_handler.handle_concurrency_conflict_error(exc, config="test.yaml")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)
        assert "custom_ops.run_status" in output
        assert " ops.run_status" not in output

    @patch("src.starrocks_br.error_handler._get_ops_database_name")
    @patch("src.starrocks_br.error_handler.click.echo")
    def test_no_full_backup_found_uses_custom_ops_database(self, mock_echo, mock_get_ops):
        mock_get_ops.return_value = "custom_ops"
        exc = exceptions.NoFullBackupFoundError("my_database")

        error_handler.handle_no_full_backup_found_error(exc, config="test.yaml", group="my_group")

        calls = [str(call) for call in mock_echo.call_args_list]
        output = " ".join(calls)
        assert "custom_ops.backup_history" in output
        assert " ops.backup_history" not in output
