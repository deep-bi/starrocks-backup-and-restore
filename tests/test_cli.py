import tempfile
import os
import pytest
from click.testing import CliRunner
from starrocks_br import cli
from starrocks_br.labels import datetime


@pytest.fixture
def config_file():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
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
    
    # Cleanup
    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def mock_db(mocker):
    """Create a mocked StarRocksDB instance with context manager support."""
    mock = mocker.Mock()
    mock.__enter__ = mocker.Mock(return_value=mock)
    mock.__exit__ = mocker.Mock(return_value=False)
    mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock)
    return mock


@pytest.fixture
def setup_common_mocks(mocker):
    """Setup commonly used mocks for backup operations."""
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
    mocker.patch('starrocks_br.repository.ensure_repository')
    mocker.patch('starrocks_br.concurrency.reserve_job_slot')


@pytest.fixture
def invalid_yaml_file():
    """Create a temporary invalid YAML file for testing error handling."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("invalid: yaml: content")
        f.flush()
        config_path = f.name
    
    yield config_path
    
    # Cleanup
    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def setup_password_env(monkeypatch):
    """Setup STARROCKS_PASSWORD environment variable for testing."""
    monkeypatch.setenv('STARROCKS_PASSWORD', 'test_password')


def test_should_run_incremental_backup_with_specific_baseline(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test backup incremental command with specific baseline backup."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[
        {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
    ])
    mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_inc')
    mocker.patch('starrocks_br.planner.build_incremental_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_inc TO test_repo')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': True,
        'final_status': {'state': 'FINISHED'},
        'error_message': None
    })
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--baseline-backup', 'test_db_20251010_full', '--group', 'daily_incremental'])
    
    assert result.exit_code == 0
    assert 'Backup completed successfully' in result.output
    assert 'Using specified baseline backup: test_db_20251010_full' in result.output


def test_should_run_incremental_backup_with_valid_config(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test backup incremental command with default baseline (latest full backup)."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'test_db_20251015_full',
        'backup_type': 'full',
        'finished_at': '2025-10-15 10:00:00'
    })
    mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[
        {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
    ])
    mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_inc')
    mocker.patch('starrocks_br.planner.build_incremental_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_inc TO test_repo')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': True,
        'final_status': {'state': 'FINISHED'},
        'error_message': None
    })
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code == 0
    assert 'Backup completed successfully' in result.output
    assert 'Using latest full backup as baseline: test_db_20251015_full (full)' in result.output


def test_should_fail_when_config_file_not_found():
    """Test that backup incremental fails when config file doesn't exist."""
    runner = CliRunner()
    
    result = runner.invoke(cli.backup_incremental, ['--config', '/nonexistent/config.yaml'])
    
    assert result.exit_code != 0
    assert 'Error' in result.output or 'not found' in result.output.lower()


def test_should_fail_when_cluster_is_unhealthy(config_file, mock_db, setup_password_env, mocker):
    """Test that backup incremental fails when cluster health check fails."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(False, "Cluster unhealthy"))
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file])
    
    assert result.exit_code != 0
    assert 'unhealthy' in result.output.lower() or 'error' in result.output.lower()


def test_should_run_full_backup_with_valid_config(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test backup full command."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.build_full_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_full TO test_repo')
    mocker.patch('starrocks_br.planner.find_tables_by_group', return_value=[{'database': 'test_db', 'table': 'dim_customers'}])
    mocker.patch('starrocks_br.planner.get_all_partitions_for_tables', return_value=[])
    mocker.patch('starrocks_br.planner.record_backup_partitions')
    mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_full')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': True,
        'final_status': {'state': 'FINISHED'},
        'error_message': None
    })
    
    result = runner.invoke(cli.backup_full, ['--config', config_file, '--group', 'weekly_dimensions'])
    
    assert result.exit_code == 0
    assert 'Backup completed successfully' in result.output


def test_should_run_full_backup_with_wildcard_group(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test backup full command with wildcard group."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.build_full_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_full TO test_repo')
    mocker.patch('starrocks_br.planner.find_tables_by_group', return_value=[{'database': 'test_db', 'table': '*'}])
    mocker.patch('starrocks_br.planner.get_all_partitions_for_tables', return_value=[])
    mocker.patch('starrocks_br.planner.record_backup_partitions')
    mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_full')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': True,
        'final_status': {'state': 'FINISHED'},
        'error_message': None
    })
    
    result = runner.invoke(cli.backup_full, ['--config', config_file, '--group', 'monthly_full'])
    
    assert result.exit_code == 0
    assert 'Backup completed successfully' in result.output


def test_should_run_restore_with_valid_parameters(config_file, mock_db, setup_password_env, mocker):
    """Test restore command."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Cluster healthy"))
    mocker.patch('starrocks_br.repository.ensure_repository')
    mocker.patch('starrocks_br.restore.find_restore_pair', return_value=['test_backup'])
    mocker.patch('starrocks_br.restore.get_tables_from_backup', return_value=['test_db.fact_table'])
    mocker.patch('starrocks_br.restore.execute_restore_flow', return_value={
        'success': True,
        'message': 'Restore completed successfully. Restored 1 tables.'
    })
    mocker.patch('builtins.input', return_value='y')
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup'
    ])
    
    assert result.exit_code == 0
    assert 'Restore completed successfully' in result.output


def test_should_skip_confirmation_when_yes_flag_provided(config_file, mock_db, setup_password_env, mocker):
    """Test that restore command skips confirmation when --yes flag is provided."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Cluster healthy"))
    mocker.patch('starrocks_br.repository.ensure_repository')
    mocker.patch('starrocks_br.restore.find_restore_pair', return_value=['test_backup'])
    mocker.patch('starrocks_br.restore.get_tables_from_backup', return_value=['test_db.fact_table'])
    
    execute_restore_flow_mock = mocker.patch('starrocks_br.restore.execute_restore_flow', return_value={
        'success': True,
        'message': 'Restore completed successfully. Restored 1 tables.'
    })
    input_mock = mocker.patch('builtins.input')
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup',
        '--yes'
    ])
    
    assert result.exit_code == 0
    assert 'Restore completed successfully' in result.output
    execute_restore_flow_mock.assert_called_once()
    call_kwargs = execute_restore_flow_mock.call_args[1]
    assert call_kwargs['skip_confirmation'] is True
    input_mock.assert_not_called()


def test_should_fail_restore_when_cluster_is_unhealthy(config_file, mock_db, setup_password_env, mocker):
    """Test that restore fails when cluster health check fails."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(False, "Cluster unhealthy"))
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup'
    ])
    
    assert result.exit_code != 0
    assert 'unhealthy' in result.output.lower() or 'error' in result.output.lower()


def test_should_fail_restore_when_repository_invalid(config_file, mock_db, setup_password_env, mocker):
    """Test that restore fails when repository validation fails."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Cluster healthy"))
    mocker.patch('starrocks_br.repository.ensure_repository', side_effect=RuntimeError("Repository 'test_repo' not found"))
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup'
    ])
    
    assert result.exit_code != 0
    assert 'repository' in result.output.lower() or 'error' in result.output.lower()


def test_should_fail_restore_when_missing_required_parameters(config_file, setup_password_env):
    """Test that restore fails when required parameters are missing."""
    runner = CliRunner()
    
    result = runner.invoke(cli.cli, ['restore', '--config', config_file])
    
    assert result.exit_code != 0


def test_should_handle_backup_failure_gracefully(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test that backup incremental handles failures gracefully."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'test_db_20251015_full',
        'backup_type': 'full',
        'finished_at': '2025-10-15 10:00:00'
    })
    mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[
        {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
    ])
    mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_inc')
    mocker.patch('starrocks_br.planner.build_incremental_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_inc TO test_repo')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': False,
        'final_status': {'state': 'FAILED'},
        'error_message': 'Backup failed'
    })
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code != 0
    assert 'failed' in result.output.lower()


def test_should_handle_job_slot_conflict(config_file, mock_db, setup_password_env, mocker):
    """Test that backup handles job slot conflicts appropriately."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
    mocker.patch('starrocks_br.repository.ensure_repository')
    mocker.patch('starrocks_br.concurrency.reserve_job_slot', side_effect=RuntimeError("active job conflict for scope; retry later"))
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file])
    
    assert result.exit_code != 0
    assert 'conflict' in result.output.lower() or 'error' in result.output.lower()


def test_cli_main_group_command():
    """Test the main CLI group command."""
    runner = CliRunner()
    result = runner.invoke(cli.cli, [])
    assert result.exit_code == 2  # Click expects a subcommand
    assert "Usage:" in result.output


def test_backup_group_command():
    """Test the backup group command."""
    runner = CliRunner()
    result = runner.invoke(cli.backup, [])
    assert result.exit_code == 2  # Click expects a subcommand
    assert "Usage:" in result.output


def test_incremental_backup_with_no_partitions_warning(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test incremental backup when no partitions are found"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'test_db_20251015_full',
        'backup_type': 'full',
        'finished_at': '2025-10-15 10:00:00'
    })
    mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_inc')
    mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[])
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code == 1
    assert 'No partitions found to backup' in result.output


def test_full_backup_with_no_tables_warning(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test full backup when no tables are found"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_full')
    mocker.patch('starrocks_br.planner.build_full_backup_command', return_value='')
    
    result = runner.invoke(cli.backup_full, ['--config', config_file, '--group', 'empty_group'])
    
    assert result.exit_code == 1
    assert 'No tables found in group' in result.output


def test_restore_with_group_filter(config_file, mock_db, setup_password_env, mocker):
    """Test restore command with group filter."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Cluster healthy"))
    mocker.patch('starrocks_br.repository.ensure_repository')
    mocker.patch('starrocks_br.restore.find_restore_pair', return_value=['test_backup'])
    mocker.patch('starrocks_br.restore.get_tables_from_backup', return_value=['test_db.fact_table'])
    mocker.patch('starrocks_br.restore.execute_restore_flow', return_value={
        'success': True,
        'message': 'Restore completed successfully. Restored 1 tables.'
    })
    mocker.patch('builtins.input', return_value='y')
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup',
        '--group', 'daily_incremental'
    ])
    
    assert result.exit_code == 0
    assert 'Restore completed successfully' in result.output


def test_restore_with_table_filter(config_file, mock_db, setup_password_env, mocker):
    """Test restore command with table filter."""
    runner = CliRunner()
    
    get_tables_mock = mocker.patch('starrocks_br.restore.get_tables_from_backup', return_value=['test_db.fact_table'])
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Cluster healthy"))
    mocker.patch('starrocks_br.repository.ensure_repository')
    mocker.patch('starrocks_br.restore.find_restore_pair', return_value=['test_backup'])
    mocker.patch('starrocks_br.restore.execute_restore_flow', return_value={
        'success': True,
        'message': 'Restore completed successfully. Restored 1 tables.'
    })
    mocker.patch('builtins.input', return_value='y')
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup',
        '--table', 'fact_table'
    ])
    
    assert result.exit_code == 0
    assert 'Restore completed successfully' in result.output
    get_tables_mock.assert_called_once()
    call_args = get_tables_mock.call_args
    assert call_args[1]['table'] == 'fact_table'
    assert call_args[1]['database'] == 'test_db'


def test_restore_with_table_and_group_should_fail(config_file, mock_db, setup_password_env, mocker):
    """Test that restore command fails when both --table and --group are specified."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup',
        '--table', 'fact_table',
        '--group', 'daily_incremental'
    ])
    
    assert result.exit_code == 1
    assert 'Cannot specify both --group and --table' in result.output


def test_restore_with_table_containing_dot_should_fail(config_file, mock_db, setup_password_env, mocker):
    """Test that restore command fails when table name contains a dot."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup',
        '--table', 'db.fact_table'
    ])
    
    assert result.exit_code == 1
    assert 'Table name must not include database prefix' in result.output


def test_restore_with_empty_table_name_should_fail(config_file, mock_db, setup_password_env, mocker):
    """Test that restore command fails when table name is empty."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup',
        '--table', '   '
    ])
    
    assert result.exit_code == 1
    assert 'Table name cannot be empty' in result.output


def test_restore_with_table_not_found_in_backup(config_file, mock_db, setup_password_env, mocker):
    """Test that restore command fails when table is not found in backup."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Cluster healthy"))
    mocker.patch('starrocks_br.repository.ensure_repository')
    mocker.patch('starrocks_br.restore.find_restore_pair', return_value=['test_backup'])
    mocker.patch('starrocks_br.restore.get_tables_from_backup', side_effect=ValueError("Table 'nonexistent_table' not found in backup 'test_backup' for database 'test_db'"))
    
    result = runner.invoke(cli.cli, [
        'restore',
        '--config', config_file,
        '--target-label', 'test_backup',
        '--table', 'nonexistent_table'
    ])
    
    assert result.exit_code == 1
    assert "Table 'nonexistent_table' not found in backup" in result.output


def test_cli_exception_handling_file_not_found():
    """Test CLI exception handling for FileNotFoundError"""
    runner = CliRunner()
    
    result = runner.invoke(cli.backup_incremental, [
        '--config', '/nonexistent/file.yaml', '--group', 'daily_incremental'
    ])
    
    assert result.exit_code == 1
    assert 'Error: Config file not found' in result.output


def test_cli_exception_handling_value_error(invalid_yaml_file, setup_password_env):
    """Test CLI exception handling for ValueError"""
    runner = CliRunner()
    
    result = runner.invoke(cli.backup_incremental, [
        '--config', invalid_yaml_file, '--group', 'daily_incremental'
    ])
    
    assert result.exit_code == 1
    assert 'Error: Unexpected error' in result.output


def test_cli_exception_handling_runtime_error(config_file, mock_db, setup_password_env, mocker):
    """Test CLI exception handling for RuntimeError"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', 
                side_effect=RuntimeError("Health check failed"))
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code == 1
    assert 'Error: Health check failed' in result.output


def test_cli_exception_handling_generic_exception(config_file, setup_password_env, mocker):
    """Test CLI exception handling for generic Exception"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.db.StarRocksDB', side_effect=Exception("Unexpected error"))
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code == 1
    assert 'Error: Unexpected error' in result.output


def test_init_command_successfully_creates_schema(config_file, mock_db, setup_password_env, mocker):
    """Test init command creates ops schema successfully"""
    runner = CliRunner()
    
    def mock_initialize_ops_schema(db):
        from starrocks_br import logger
        logger.info("Creating ops database...")
        logger.success("ops database created")
        logger.info("Creating ops.table_inventory...")
        logger.success("ops.table_inventory created")
        logger.info("Creating ops.backup_history...")
        logger.success("ops.backup_history created")
        logger.info("Creating ops.restore_history...")
        logger.success("ops.restore_history created")
        logger.info("Creating ops.run_status...")
        logger.success("ops.run_status created")
        logger.info("")
        logger.success("Schema initialized successfully!")
    
    mocker.patch('starrocks_br.schema.initialize_ops_schema', side_effect=mock_initialize_ops_schema)
    
    result = runner.invoke(cli.init, ['--config', config_file])
    
    assert result.exit_code == 0
    assert 'Schema initialized successfully!' in result.output
    assert 'ops database created' in result.output
    assert 'ops.table_inventory created' in result.output
    assert 'ops.backup_history created' in result.output
    assert 'ops.restore_history created' in result.output
    assert 'ops.run_status created' in result.output


def test_init_command_fails_with_invalid_config():
    """Test init command fails gracefully with invalid config"""
    runner = CliRunner()
    
    result = runner.invoke(cli.init, ['--config', '/nonexistent/config.yaml'])
    
    assert result.exit_code == 1
    assert 'Error: Config file not found' in result.output


def test_init_command_shows_next_steps(config_file, mock_db, setup_password_env, mocker):
    """Test init command shows helpful next steps"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.initialize_ops_schema')
    
    result = runner.invoke(cli.init, ['--config', config_file])
    
    assert result.exit_code == 0
    assert 'Next steps:' in result.output
    assert 'INSERT INTO ops.table_inventory' in result.output
    assert 'inventory_group' in result.output
    assert 'starrocks-br backup incremental --group' in result.output


def test_should_fail_when_invalid_baseline_backup(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test that incremental backup fails when specified baseline backup is invalid."""
    runner = CliRunner()
    
    mock_db.query.return_value = []
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--baseline-backup', 'invalid_backup', '--group', 'daily_incremental'])
    
    assert result.exit_code != 0
    assert 'Baseline backup' in result.output and 'not found' in result.output


def test_should_fail_when_no_full_backup_found(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test that incremental backup fails when no full backup is found."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value=None)
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code != 0
    assert 'No successful full backup found' in result.output


def test_should_show_critical_warning_on_lost_backup_state(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test that CLI shows critical warning when backup state is LOST."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'test_db_20251015_full',
        'backup_type': 'full',
        'finished_at': '2025-10-15 10:00:00'
    })
    mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[
        {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
    ])
    mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_inc')
    mocker.patch('starrocks_br.planner.build_incremental_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_inc TO test_repo')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': False,
        'final_status': {'state': 'LOST'},
        'error_message': 'Backup tracking lost for test_db_20251016_inc in database test_db'
    })
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code != 0
    assert 'CRITICAL: Backup tracking lost' in result.output
    assert 'Another backup operation started during ours' in result.output
    assert 'Enable ops.run_status concurrency checks' in result.output
    assert 'Backup tracking lost' in result.output


def test_should_show_critical_warning_on_lost_full_backup(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    """Test that CLI shows critical warning for LOST state in full backup."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.build_full_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_full TO test_repo')
    mocker.patch('starrocks_br.planner.find_tables_by_group', return_value=[{'database': 'test_db', 'table': 'dim_customers'}])
    mocker.patch('starrocks_br.planner.get_all_partitions_for_tables', return_value=[])
    mocker.patch('starrocks_br.planner.record_backup_partitions')
    mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_full')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': False,
        'final_status': {'state': 'LOST'},
        'error_message': 'Backup tracking lost - race condition detected'
    })
    
    result = runner.invoke(cli.backup_full, ['--config', config_file, '--group', 'weekly_dimensions'])
    
    assert result.exit_code != 0
    assert 'CRITICAL: Backup tracking lost' in result.output


def test_should_use_password_from_environment_variable(config_file, mocker):
    """Test that database connection uses password from STARROCKS_PASSWORD environment variable."""
    import os
    
    mock_db_class = mocker.patch('starrocks_br.db.StarRocksDB')
    mock_db_instance = mocker.Mock()
    mock_db_instance.__enter__ = mocker.Mock(return_value=mock_db_instance)
    mock_db_instance.__exit__ = mocker.Mock(return_value=False)
    mock_db_class.return_value = mock_db_instance
    
    test_password = 'test_env_password'
    os.environ['STARROCKS_PASSWORD'] = test_password
    
    try:
        runner = CliRunner()
        
        mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
        mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
        mocker.patch('starrocks_br.repository.ensure_repository')
        mocker.patch('starrocks_br.concurrency.reserve_job_slot')
        mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
            'label': 'test_db_20251015_full',
            'backup_type': 'full',
            'finished_at': '2025-10-15 10:00:00'
        })
        mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[
            {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
        ])
        mocker.patch('starrocks_br.labels.determine_backup_label', return_value='test_db_20251016_inc')
        mocker.patch('starrocks_br.planner.build_incremental_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_inc TO test_repo')
        mocker.patch('starrocks_br.executor.execute_backup', return_value={
            'success': True,
            'final_status': {'state': 'FINISHED'},
            'error_message': None
        })
        
        runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
        
        mock_db_class.assert_called_once()
        call_args = mock_db_class.call_args
        assert call_args[1]['password'] == test_password
        
    finally:
        if 'STARROCKS_PASSWORD' in os.environ:
            del os.environ['STARROCKS_PASSWORD']


def test_should_prevent_incremental_backup_label_collision(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    runner = CliRunner()
    
    def mock_query(query, params=None):
        if "ops.backup_history" in query and params:
            if params[0] == "test_db_20251020_incremental%":
                return [("test_db_20251020_incremental",)]
        return []
    
    mock_db.query.side_effect = mock_query
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'sales_db_20251019_full',
        'backup_type': 'full',
        'finished_at': '2025-10-19 10:00:00'
    })
    mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[
        {"database": "sales_db", "table": "fact_table", "partition_name": "p20251020"}
    ])
    mocker.patch('starrocks_br.planner.build_incremental_backup_command', return_value='BACKUP DATABASE sales_db SNAPSHOT sales_db_20251020_incremental_r1 TO test_repo')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': True,
        'final_status': {'state': 'FINISHED'},
        'error_message': None
    })
    
    mock_datetime = mocker.patch('starrocks_br.labels.datetime') 
    mock_datetime.now.return_value.strftime.return_value = "20251020"
    
    result = runner.invoke(cli.backup_incremental, [
        '--config', config_file, 
        '--group', 'daily_incremental'
    ])
    
    assert result.exit_code == 0
    assert 'Backup completed successfully' in result.output
    assert 'Generated label:' in result.output
    
    output_lines = result.output.split('\n')
    label_line = [line for line in output_lines if 'Generated label:' in line][0]
    assert '_r1' in label_line


def test_should_prevent_full_backup_label_collision(config_file, mock_db, setup_common_mocks, setup_password_env, mocker):
    runner = CliRunner()
    
    def mock_query(query, params=None):
        if "ops.backup_history" in query and params:
            if params[0] == "test_db_20251020_full%":
                return [("test_db_20251020_full",)]
        return []
    
    mock_db.query.side_effect = mock_query
    
    mocker.patch('starrocks_br.planner.build_full_backup_command', return_value='BACKUP DATABASE sales_db SNAPSHOT sales_db_20251020_full_r1 TO test_repo')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': True,
        'final_status': {'state': 'FINISHED'},
        'error_message': None
    })
    
    mock_datetime = mocker.patch('starrocks_br.labels.datetime') 
    mock_datetime.now.return_value.strftime.return_value = "20251020"
    
    result = runner.invoke(cli.backup_full, [
        '--config', config_file, 
        '--group', 'weekly_full'
    ])
    
    assert result.exit_code == 0
    assert 'Backup completed successfully' in result.output
    assert 'Generated label:' in result.output
    
    output_lines = result.output.split('\n')
    label_line = [line for line in output_lines if 'Generated label:' in line][0]
    assert '_r1' in label_line

