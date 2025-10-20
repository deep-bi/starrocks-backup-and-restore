import tempfile
import os
import pytest
from click.testing import CliRunner
from starrocks_br import cli


@pytest.fixture
def config_file():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
        host: "127.0.0.1"
        port: 9030
        user: "root"
        password: ""
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


def test_should_run_incremental_backup_with_specific_baseline(config_file, mock_db, setup_common_mocks, mocker):
    """Test backup incremental command with specific baseline backup."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[
        {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
    ])
    mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_inc')
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


def test_should_run_incremental_backup_with_valid_config(config_file, mock_db, setup_common_mocks, mocker):
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
    mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_inc')
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


def test_should_fail_when_cluster_is_unhealthy(config_file, mock_db, mocker):
    """Test that backup incremental fails when cluster health check fails."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.health.check_cluster_health', return_value=(False, "Cluster unhealthy"))
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file])
    
    assert result.exit_code != 0
    assert 'unhealthy' in result.output.lower() or 'error' in result.output.lower()


def test_should_run_full_backup_with_valid_config(config_file, mock_db, setup_common_mocks, mocker):
    """Test backup full command."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.build_full_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_full TO test_repo')
    mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_full')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': True,
        'final_status': {'state': 'FINISHED'},
        'error_message': None
    })
    
    result = runner.invoke(cli.backup_full, ['--config', config_file, '--group', 'weekly_dimensions'])
    
    assert result.exit_code == 0
    assert 'Backup completed successfully' in result.output


def test_should_run_full_backup_with_wildcard_group(config_file, mock_db, setup_common_mocks, mocker):
    """Test backup full command with wildcard group."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.build_full_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_full TO test_repo')
    mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_full')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': True,
        'final_status': {'state': 'FINISHED'},
        'error_message': None
    })
    
    result = runner.invoke(cli.backup_full, ['--config', config_file, '--group', 'monthly_full'])
    
    assert result.exit_code == 0
    assert 'Backup completed successfully' in result.output


def test_should_run_restore_partition_with_valid_parameters(config_file, mock_db, mocker):
    """Test restore partition command."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.restore.build_partition_restore_command', return_value='RESTORE SNAPSHOT test_backup FROM test_repo')
    mocker.patch('starrocks_br.restore.execute_restore', return_value={
        'success': True,
        'final_status': {'state': 'FINISHED'},
        'error_message': None
    })
    
    result = runner.invoke(cli.restore_partition, [
        '--config', config_file,
        '--backup-label', 'test_backup',
        '--table', 'test_db.fact_table',
        '--partition', 'p20251016'
    ])
    
    assert result.exit_code == 0
    assert 'Restore completed successfully' in result.output


def test_should_fail_restore_when_missing_required_parameters(config_file):
    """Test that restore partition fails when required parameters are missing."""
    runner = CliRunner()
    
    result = runner.invoke(cli.restore_partition, ['--config', config_file])
    
    assert result.exit_code != 0


def test_should_handle_backup_failure_gracefully(config_file, mock_db, setup_common_mocks, mocker):
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
    mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_inc')
    mocker.patch('starrocks_br.planner.build_incremental_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_inc TO test_repo')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': False,
        'final_status': {'state': 'FAILED'},
        'error_message': 'Backup failed'
    })
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code != 0
    assert 'failed' in result.output.lower()


def test_should_handle_job_slot_conflict(config_file, mock_db, mocker):
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


def test_incremental_backup_with_no_partitions_warning(config_file, mock_db, setup_common_mocks, mocker):
    """Test incremental backup when no partitions are found"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value={
        'label': 'test_db_20251015_full',
        'backup_type': 'full',
        'finished_at': '2025-10-15 10:00:00'
    })
    mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_inc')
    mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[])
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code == 1
    assert 'Warning: No partitions found to backup' in result.output


def test_full_backup_with_no_tables_warning(config_file, mock_db, setup_common_mocks, mocker):
    """Test full backup when no tables are found"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_full')
    mocker.patch('starrocks_br.planner.build_full_backup_command', return_value='')
    
    result = runner.invoke(cli.backup_full, ['--config', config_file, '--group', 'empty_group'])
    
    assert result.exit_code == 1
    assert 'Warning: No tables found in group' in result.output


def test_restore_partition_invalid_table_format_error(config_file):
    """Test restore partition with invalid table format"""
    runner = CliRunner()
    
    result = runner.invoke(cli.restore_partition, [
        '--config', config_file,
        '--backup-label', 'test_backup',
        '--table', 'invalid_table_format',  # Missing database prefix
        '--partition', 'p20251016'
    ])
    
    assert result.exit_code == 1
    assert 'Table must be in format database.table' in result.output


def test_cli_exception_handling_file_not_found():
    """Test CLI exception handling for FileNotFoundError"""
    runner = CliRunner()
    
    result = runner.invoke(cli.backup_incremental, [
        '--config', '/nonexistent/file.yaml', '--group', 'daily_incremental'
    ])
    
    assert result.exit_code == 1
    assert 'Error: Config file not found' in result.output


def test_cli_exception_handling_value_error(invalid_yaml_file):
    """Test CLI exception handling for ValueError"""
    runner = CliRunner()
    
    result = runner.invoke(cli.backup_incremental, [
        '--config', invalid_yaml_file, '--group', 'daily_incremental'
    ])
    
    assert result.exit_code == 1
    assert 'Error: Unexpected error' in result.output


def test_cli_exception_handling_runtime_error(config_file, mock_db, mocker):
    """Test CLI exception handling for RuntimeError"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.ensure_ops_schema', return_value=False)
    mocker.patch('starrocks_br.health.check_cluster_health', 
                side_effect=RuntimeError("Health check failed"))
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code == 1
    assert 'Error: Health check failed' in result.output


def test_cli_exception_handling_generic_exception(config_file, mocker):
    """Test CLI exception handling for generic Exception"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.db.StarRocksDB', side_effect=Exception("Unexpected error"))
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code == 1
    assert 'Error: Unexpected error' in result.output


def test_init_command_successfully_creates_schema(config_file, mock_db, mocker):
    """Test init command creates ops schema successfully"""
    runner = CliRunner()
    
    mock_init = mocker.patch('starrocks_br.schema.initialize_ops_schema')
    
    result = runner.invoke(cli.init, ['--config', config_file])
    
    assert result.exit_code == 0
    assert 'Schema initialized successfully!' in result.output
    assert 'ops.table_inventory created' in result.output
    assert 'ops.backup_history created' in result.output
    assert 'ops.restore_history created' in result.output
    assert 'ops.run_status created' in result.output
    mock_init.assert_called_once()


def test_init_command_fails_with_invalid_config():
    """Test init command fails gracefully with invalid config"""
    runner = CliRunner()
    
    result = runner.invoke(cli.init, ['--config', '/nonexistent/config.yaml'])
    
    assert result.exit_code == 1
    assert 'Error: Config file not found' in result.output


def test_init_command_shows_next_steps(config_file, mock_db, mocker):
    """Test init command shows helpful next steps"""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.schema.initialize_ops_schema')
    
    result = runner.invoke(cli.init, ['--config', config_file])
    
    assert result.exit_code == 0
    assert 'Next steps:' in result.output
    assert 'INSERT INTO ops.table_inventory' in result.output
    assert 'inventory_group' in result.output
    assert 'starrocks-br backup incremental --group' in result.output


def test_should_fail_when_invalid_baseline_backup(config_file, mock_db, setup_common_mocks, mocker):
    """Test that incremental backup fails when specified baseline backup is invalid."""
    runner = CliRunner()
    
    mock_db.query.return_value = []
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--baseline-backup', 'invalid_backup', '--group', 'daily_incremental'])
    
    assert result.exit_code != 0
    assert 'Baseline backup' in result.output and 'not found' in result.output


def test_should_fail_when_no_full_backup_found(config_file, mock_db, setup_common_mocks, mocker):
    """Test that incremental backup fails when no full backup is found."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.find_latest_full_backup', return_value=None)
    
    result = runner.invoke(cli.backup_incremental, ['--config', config_file, '--group', 'daily_incremental'])
    
    assert result.exit_code != 0
    assert 'No successful full backup found' in result.output


def test_should_show_critical_warning_on_lost_backup_state(config_file, mock_db, setup_common_mocks, mocker):
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
    mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_inc')
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


def test_should_show_critical_warning_on_lost_full_backup(config_file, mock_db, setup_common_mocks, mocker):
    """Test that CLI shows critical warning for LOST state in full backup."""
    runner = CliRunner()
    
    mocker.patch('starrocks_br.planner.build_full_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_full TO test_repo')
    mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_full')
    mocker.patch('starrocks_br.executor.execute_backup', return_value={
        'success': False,
        'final_status': {'state': 'LOST'},
        'error_message': 'Backup tracking lost - race condition detected'
    })
    
    result = runner.invoke(cli.backup_full, ['--config', config_file, '--group', 'weekly_dimensions'])
    
    assert result.exit_code != 0
    assert 'CRITICAL: Backup tracking lost' in result.output

