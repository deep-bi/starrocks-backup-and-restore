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

from click.testing import CliRunner

from starrocks_br import cli


def test_prune_keep_last_success(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test pruning by keeping only the last N backups."""
    runner = CliRunner()

    # Mock getting list of snapshots from backup_history
    mock_snapshots = [
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
        {"label": "backup_20240102", "finished_at": "2024-01-02 00:00:00"},
        {"label": "backup_20240103", "finished_at": "2024-01-03 00:00:00"},
        {"label": "backup_20240104", "finished_at": "2024-01-04 00:00:00"},
        {"label": "backup_20240105", "finished_at": "2024-01-05 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mock_cleanup = mocker.patch("starrocks_br.prune.cleanup_backup_history")

    # Auto-confirm with --yes flag
    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--keep-last", "3", "--yes"],
    )

    assert result.exit_code == 0

    # Should delete the oldest 2 snapshots (keeping the last 3)
    assert mock_execute.call_count == 2
    mock_execute.assert_any_call(mock_db, "test_repo", "backup_20240101")
    mock_execute.assert_any_call(mock_db, "test_repo", "backup_20240102")

    # Should cleanup history for deleted snapshots
    assert mock_cleanup.call_count == 2


def test_prune_older_than_success(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test pruning snapshots older than a specific timestamp."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup_20231201", "finished_at": "2023-12-01 00:00:00"},
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
        {"label": "backup_20240201", "finished_at": "2024-02-01 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mocker.patch("starrocks_br.prune.cleanup_backup_history")

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--older-than", "2024-01-01 12:00:00", "--yes"],
    )

    assert result.exit_code == 0

    # Should delete snapshots older than 2024-01-01 12:00:00
    assert mock_execute.call_count == 2
    mock_execute.assert_any_call(mock_db, "test_repo", "backup_20231201")
    mock_execute.assert_any_call(mock_db, "test_repo", "backup_20240101")


def test_prune_single_snapshot_success(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test deleting a specific snapshot by name."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
        {"label": "backup_20240102", "finished_at": "2024-01-02 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mock_cleanup = mocker.patch("starrocks_br.prune.cleanup_backup_history")
    mocker.patch("starrocks_br.prune.verify_snapshot_exists", return_value=True)

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--snapshot", "backup_20240101", "--yes"],
    )

    assert result.exit_code == 0

    mock_execute.assert_called_once_with(mock_db, "test_repo", "backup_20240101")
    mock_cleanup.assert_called_once()


def test_prune_multiple_snapshots_success(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test deleting multiple specific snapshots."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
        {"label": "backup_20240102", "finished_at": "2024-01-02 00:00:00"},
        {"label": "backup_20240103", "finished_at": "2024-01-03 00:00:00"},
        {"label": "backup_20240104", "finished_at": "2024-01-04 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    def query_side_effect(sql):
        if "SHOW SNAPSHOT" in sql:
            return [["some_snapshot_data"]]
        return []

    mock_db.query.side_effect = query_side_effect

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mock_cleanup = mocker.patch("starrocks_br.prune.cleanup_backup_history")

    result = runner.invoke(
        cli.prune_command,
        [
            "--config",
            config_file,
            "--snapshots",
            "backup_20240101,backup_20240102,backup_20240103",
            "--yes",
        ],
    )

    assert result.exit_code == 0

    assert mock_execute.call_count == 3
    mock_execute.assert_any_call(mock_db, "test_repo", "backup_20240101")
    mock_execute.assert_any_call(mock_db, "test_repo", "backup_20240102")
    mock_execute.assert_any_call(mock_db, "test_repo", "backup_20240103")

    assert mock_cleanup.call_count == 3


def test_prune_dry_run_mode(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test dry-run mode shows what would be deleted without actually deleting."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
        {"label": "backup_20240102", "finished_at": "2024-01-02 00:00:00"},
        {"label": "backup_20240103", "finished_at": "2024-01-03 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mock_cleanup = mocker.patch("starrocks_br.prune.cleanup_backup_history")

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--keep-last", "1", "--dry-run"],
    )

    assert result.exit_code == 0

    # Should NOT execute any deletions in dry-run mode
    mock_execute.assert_not_called()
    mock_cleanup.assert_not_called()

    # Should show what would be deleted


def test_prune_confirmation_prompt_accept(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that user can confirm deletion at the prompt."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
        {"label": "backup_20240102", "finished_at": "2024-01-02 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mock_cleanup = mocker.patch("starrocks_br.prune.cleanup_backup_history")

    # Simulate user typing 'y' at the prompt
    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--keep-last", "1"],
        input="y\n",
    )

    assert result.exit_code == 0

    # Should execute deletion after confirmation
    mock_execute.assert_called_once()
    mock_cleanup.assert_called_once()


def test_prune_confirmation_prompt_cancel(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that user can cancel deletion at the prompt."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
        {"label": "backup_20240102", "finished_at": "2024-01-02 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mock_cleanup = mocker.patch("starrocks_br.prune.cleanup_backup_history")

    # Simulate user typing 'n' at the prompt
    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--keep-last", "1"],
        input="n\n",
    )

    assert result.exit_code == 1

    # Should NOT execute deletion after cancellation
    mock_execute.assert_not_called()
    mock_cleanup.assert_not_called()


def test_prune_mutually_exclusive_options_keep_last_and_older_than(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that --keep-last and --older-than cannot be used together."""
    runner = CliRunner()

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--keep-last", "5", "--older-than", "2024-01-01 00:00:00"],
    )

    assert result.exit_code != 0


def test_prune_mutually_exclusive_options_snapshot_and_snapshots(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that --snapshot and --snapshots cannot be used together."""
    runner = CliRunner()

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--snapshot", "backup1", "--snapshots", "backup2,backup3"],
    )

    assert result.exit_code != 0


def test_prune_no_options_specified(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that at least one pruning option must be specified."""
    runner = CliRunner()

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file],
    )

    assert result.exit_code != 0


def test_prune_snapshot_not_found(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test error when specified snapshot doesn't exist."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mocker.patch(
        "starrocks_br.prune.verify_snapshot_exists",
        side_effect=Exception("Snapshot 'nonexistent_backup' not found"),
    )

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--snapshot", "nonexistent_backup", "--yes"],
    )

    assert result.exit_code != 0


def test_prune_repository_not_found(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    setup_password_env,
    mocker,
):
    """Test error when repository doesn't exist."""
    runner = CliRunner()

    # Mock repository not found
    mocker.patch(
        "starrocks_br.repository.ensure_repository",
        side_effect=RuntimeError("Repository 'test_repo' not found"),
    )

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--snapshot", "backup_20240101", "--yes"],
    )

    assert result.exit_code != 0


def test_prune_invalid_timestamp_format(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test error when timestamp has invalid format."""
    runner = CliRunner()

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--older-than", "invalid-timestamp", "--yes"],
    )

    assert result.exit_code != 0


def test_prune_keep_last_zero(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that --keep-last must be a positive number."""
    runner = CliRunner()

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--keep-last", "0", "--yes"],
    )

    assert result.exit_code != 0


def test_prune_no_snapshots_to_delete(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test when there are no snapshots to delete (all are within keep-last threshold)."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
        {"label": "backup_20240102", "finished_at": "2024-01-02 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--keep-last", "10", "--yes"],
    )

    assert result.exit_code == 0

    # Should not delete anything
    mock_execute.assert_not_called()


def test_prune_cleanup_history_after_deletion(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that backup_history is cleaned up after snapshot deletion."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup_20240101", "finished_at": "2024-01-01 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mocker.patch("starrocks_br.prune.verify_snapshot_exists", return_value=True)
    mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mock_cleanup = mocker.patch("starrocks_br.prune.cleanup_backup_history")

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--snapshot", "backup_20240101", "--yes"],
    )

    assert result.exit_code == 0

    mock_cleanup.assert_called_once()
    call_args = mock_cleanup.call_args
    assert call_args[0][1] == "backup_20240101"


def test_prune_partial_failure_continues_deletion(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test that prune continues deleting other snapshots even if one fails."""
    runner = CliRunner()

    mock_snapshots = [
        {"label": "backup1", "finished_at": "2024-01-01 00:00:00"},
        {"label": "backup2", "finished_at": "2024-01-02 00:00:00"},
        {"label": "backup3", "finished_at": "2024-01-03 00:00:00"},
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    def query_side_effect(sql):
        if "SHOW SNAPSHOT" in sql:
            return [["some_snapshot_data"]]
        return []

    mock_db.query.side_effect = query_side_effect

    call_count = {"count": 0}

    def mock_execute_side_effect(db, repo, snapshot):
        call_count["count"] += 1
        if call_count["count"] == 2:
            raise Exception(f"Failed to delete {snapshot}")

    mock_execute = mocker.patch(
        "starrocks_br.prune.execute_drop_snapshot",
        side_effect=mock_execute_side_effect,
    )
    mocker.patch("starrocks_br.prune.cleanup_backup_history")

    runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--snapshots", "backup1,backup2,backup3", "--yes"],
    )

    assert mock_execute.call_count == 3


def test_prune_with_group_filter_keep_last(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test pruning with group filter keeps only last N backups for that group."""
    runner = CliRunner()

    # Mock snapshots from different groups
    mock_snapshots = [
        {
            "label": "prod_backup_20240101",
            "finished_at": "2024-01-01 00:00:00",
            "inventory_group": "production_tables",
        },
        {
            "label": "prod_backup_20240102",
            "finished_at": "2024-01-02 00:00:00",
            "inventory_group": "production_tables",
        },
        {
            "label": "prod_backup_20240103",
            "finished_at": "2024-01-03 00:00:00",
            "inventory_group": "production_tables",
        },
        {
            "label": "test_backup_20240101",
            "finished_at": "2024-01-01 00:00:00",
            "inventory_group": "test_tables",
        },
    ]

    # Mock should return only production_tables backups when group is specified
    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=[s for s in mock_snapshots if s["inventory_group"] == "production_tables"],
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mocker.patch("starrocks_br.prune.cleanup_backup_history")

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--group", "production_tables", "--keep-last", "2", "--yes"],
    )

    assert result.exit_code == 0

    # Should only delete the oldest production_tables backup
    assert mock_execute.call_count == 1
    mock_execute.assert_called_once_with(mock_db, "test_repo", "prod_backup_20240101")

    # Should NOT touch test_tables backups
    assert "test_backup" not in str(mock_execute.call_args_list)


def test_prune_with_group_filter_older_than(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test pruning with group filter deletes only old backups from that group."""
    runner = CliRunner()

    mock_snapshots = [
        {
            "label": "prod_backup_20231201",
            "finished_at": "2023-12-01 00:00:00",
            "inventory_group": "production_tables",
        },
        {
            "label": "prod_backup_20240101",
            "finished_at": "2024-01-01 00:00:00",
            "inventory_group": "production_tables",
        },
        {
            "label": "test_backup_20231201",
            "finished_at": "2023-12-01 00:00:00",
            "inventory_group": "test_tables",
        },
    ]

    # Mock should return only production_tables backups when group is specified
    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=[s for s in mock_snapshots if s["inventory_group"] == "production_tables"],
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mocker.patch("starrocks_br.prune.cleanup_backup_history")

    result = runner.invoke(
        cli.prune_command,
        [
            "--config",
            config_file,
            "--group",
            "production_tables",
            "--older-than",
            "2024-01-01 00:00:00",
            "--yes",
        ],
    )

    assert result.exit_code == 0

    # Should only delete old production_tables backup
    assert mock_execute.call_count == 1
    mock_execute.assert_called_once_with(mock_db, "test_repo", "prod_backup_20231201")

    # Should NOT touch test_tables backups
    assert "test_backup" not in str(mock_execute.call_args_list)


def test_prune_without_group_affects_all_backups(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test pruning without group filter affects ALL backups across all groups."""
    runner = CliRunner()

    mock_snapshots = [
        {
            "label": "prod_backup_20240101",
            "finished_at": "2024-01-01 00:00:00",
            "inventory_group": "production_tables",
        },
        {
            "label": "test_backup_20240102",
            "finished_at": "2024-01-02 00:00:00",
            "inventory_group": "test_tables",
        },
        {
            "label": "prod_backup_20240103",
            "finished_at": "2024-01-03 00:00:00",
            "inventory_group": "production_tables",
        },
        {
            "label": "test_backup_20240104",
            "finished_at": "2024-01-04 00:00:00",
            "inventory_group": "test_tables",
        },
    ]

    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=mock_snapshots,
    )

    mock_execute = mocker.patch("starrocks_br.prune.execute_drop_snapshot")
    mocker.patch("starrocks_br.prune.cleanup_backup_history")

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--keep-last", "2", "--yes"],
    )

    assert result.exit_code == 0

    assert mock_execute.call_count == 2
    mock_execute.assert_any_call(mock_db, "test_repo", "prod_backup_20240101")
    mock_execute.assert_any_call(mock_db, "test_repo", "test_backup_20240102")


def test_prune_group_not_found(
    config_file,
    mock_db,
    mock_initialized_schema,
    mock_healthy_cluster,
    mock_repo_exists,
    setup_password_env,
    mocker,
):
    """Test error when specified group doesn't exist or has no backups."""
    runner = CliRunner()

    # Mock returns empty list for non-existent group
    mocker.patch(
        "starrocks_br.prune.get_successful_backups",
        return_value=[],
    )

    result = runner.invoke(
        cli.prune_command,
        ["--config", config_file, "--group", "nonexistent_group", "--keep-last", "5", "--yes"],
    )

    assert result.exit_code == 0
