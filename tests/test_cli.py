import tempfile
import os
from click.testing import CliRunner
from starrocks_br import cli


def test_should_run_incremental_backup_with_valid_config(mocker):
    """Test backup incremental command with all required parameters."""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
        mocker.patch('starrocks_br.repository.ensure_repository')
        mocker.patch('starrocks_br.concurrency.reserve_job_slot')
        mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[
            {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
        ])
        mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_inc')
        mocker.patch('starrocks_br.planner.build_incremental_backup_command', return_value='BACKUP SNAPSHOT test_db_20251016_inc TO test_repo')
        mocker.patch('starrocks_br.executor.execute_backup', return_value={
            'success': True,
            'final_status': {'state': 'FINISHED'},
            'error_message': None
        })
        
        result = runner.invoke(cli.backup_incremental, ['--config', config_path, '--days', '7'])
        
        assert result.exit_code == 0
        assert 'Backup completed successfully' in result.output
    finally:
        os.unlink(config_path)


def test_should_fail_when_config_file_not_found():
    """Test that backup incremental fails when config file doesn't exist."""
    runner = CliRunner()
    
    result = runner.invoke(cli.backup_incremental, ['--config', '/nonexistent/config.yaml', '--days', '7'])
    
    assert result.exit_code != 0
    assert 'Error' in result.output or 'not found' in result.output.lower()


def test_should_fail_when_cluster_is_unhealthy(mocker):
    """Test that backup incremental fails when cluster health check fails."""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.health.check_cluster_health', return_value=(False, "Cluster unhealthy"))
        
        result = runner.invoke(cli.backup_incremental, ['--config', config_path, '--days', '7'])
        
        assert result.exit_code != 0
        assert 'unhealthy' in result.output.lower() or 'error' in result.output.lower()
    finally:
        os.unlink(config_path)


def test_should_run_weekly_backup_with_valid_config(mocker):
    """Test backup weekly command."""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
        mocker.patch('starrocks_br.repository.ensure_repository')
        mocker.patch('starrocks_br.concurrency.reserve_job_slot')
        mocker.patch('starrocks_br.planner.find_weekly_eligible_tables', return_value=[
            {"database": "test_db", "table": "dim_table"}
        ])
        mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_weekly')
        mocker.patch('starrocks_br.planner.build_weekly_backup_command', return_value='BACKUP SNAPSHOT test_db_20251016_weekly TO test_repo')
        mocker.patch('starrocks_br.executor.execute_backup', return_value={
            'success': True,
            'final_status': {'state': 'FINISHED'},
            'error_message': None
        })
        
        result = runner.invoke(cli.backup_weekly, ['--config', config_path])
        
        assert result.exit_code == 0
        assert 'Backup completed successfully' in result.output
    finally:
        os.unlink(config_path)


def test_should_run_monthly_backup_with_valid_config(mocker):
    """Test backup monthly command."""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
        mocker.patch('starrocks_br.repository.ensure_repository')
        mocker.patch('starrocks_br.concurrency.reserve_job_slot')
        mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_monthly')
        mocker.patch('starrocks_br.planner.build_monthly_backup_command', return_value='BACKUP DATABASE test_db SNAPSHOT test_db_20251016_monthly TO test_repo')
        mocker.patch('starrocks_br.executor.execute_backup', return_value={
            'success': True,
            'final_status': {'state': 'FINISHED'},
            'error_message': None
        })
        
        result = runner.invoke(cli.backup_monthly, ['--config', config_path])
        
        assert result.exit_code == 0
        assert 'Backup completed successfully' in result.output
    finally:
        os.unlink(config_path)


def test_should_run_restore_partition_with_valid_parameters(mocker):
    """Test restore partition command."""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.restore.build_partition_restore_command', return_value='RESTORE SNAPSHOT test_backup FROM test_repo')
        mocker.patch('starrocks_br.restore.execute_restore', return_value={
            'success': True,
            'final_status': {'state': 'FINISHED'},
            'error_message': None
        })
        
        result = runner.invoke(cli.restore_partition, [
            '--config', config_path,
            '--backup-label', 'test_backup',
            '--table', 'test_db.fact_table',
            '--partition', 'p20251016'
        ])
        
        assert result.exit_code == 0
        assert 'Restore completed successfully' in result.output
    finally:
        os.unlink(config_path)


def test_should_fail_restore_when_missing_required_parameters():
    """Test that restore partition fails when required parameters are missing."""
    runner = CliRunner()
    
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
    
    try:
        result = runner.invoke(cli.restore_partition, ['--config', config_path])
        
        assert result.exit_code != 0
    finally:
        os.unlink(config_path)


def test_should_handle_backup_failure_gracefully(mocker):
    """Test that backup incremental handles failures gracefully."""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
        mocker.patch('starrocks_br.repository.ensure_repository')
        mocker.patch('starrocks_br.concurrency.reserve_job_slot')
        mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[
            {"database": "test_db", "table": "fact_table", "partition_name": "p20251016"}
        ])
        mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_inc')
        mocker.patch('starrocks_br.planner.build_incremental_backup_command', return_value='BACKUP SNAPSHOT test_db_20251016_inc TO test_repo')
        mocker.patch('starrocks_br.executor.execute_backup', return_value={
            'success': False,
            'final_status': {'state': 'FAILED'},
            'error_message': 'Backup failed'
        })
        
        result = runner.invoke(cli.backup_incremental, ['--config', config_path, '--days', '7'])
        
        assert result.exit_code != 0
        assert 'failed' in result.output.lower()
    finally:
        os.unlink(config_path)


def test_should_handle_job_slot_conflict(mocker):
    """Test that backup handles job slot conflicts appropriately."""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
        mocker.patch('starrocks_br.repository.ensure_repository')
        mocker.patch('starrocks_br.concurrency.reserve_job_slot', side_effect=RuntimeError("active job conflict for scope; retry later"))
        
        result = runner.invoke(cli.backup_incremental, ['--config', config_path, '--days', '7'])
        
        assert result.exit_code != 0
        assert 'conflict' in result.output.lower() or 'error' in result.output.lower()
    finally:
        os.unlink(config_path)


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


def test_incremental_backup_with_no_partitions_warning(mocker):
    """Test incremental backup when no partitions are found"""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
        mocker.patch('starrocks_br.repository.ensure_repository')
        mocker.patch('starrocks_br.concurrency.reserve_job_slot')
        mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_inc')
        
        mocker.patch('starrocks_br.planner.find_recent_partitions', return_value=[])
        
        result = runner.invoke(cli.backup_incremental, ['--config', config_path, '--days', '7'])
        
        assert result.exit_code == 1
        assert 'Warning: No partitions found to backup' in result.output
        
    finally:
        os.unlink(config_path)


def test_weekly_backup_with_no_tables_warning(mocker):
    """Test weekly backup when no tables are found"""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.health.check_cluster_health', return_value=(True, "Healthy"))
        mocker.patch('starrocks_br.repository.ensure_repository')
        mocker.patch('starrocks_br.concurrency.reserve_job_slot')
        mocker.patch('starrocks_br.labels.generate_label', return_value='test_db_20251016_weekly')
        
        mocker.patch('starrocks_br.planner.find_weekly_eligible_tables', return_value=[])
        
        result = runner.invoke(cli.backup_weekly, ['--config', config_path])
        
        assert result.exit_code == 1
        assert 'Warning: No tables found to backup' in result.output
        
    finally:
        os.unlink(config_path)


def test_restore_partition_invalid_table_format_error():
    """Test restore partition with invalid table format"""
    runner = CliRunner()
    
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
    
    try:
        result = runner.invoke(cli.restore_partition, [
            '--config', config_path,
            '--backup-label', 'test_backup',
            '--table', 'invalid_table_format',  # Missing database prefix
            '--partition', 'p20251016'
        ])
        
        assert result.exit_code == 1
        assert 'Table must be in format database.table' in result.output
        
    finally:
        os.unlink(config_path)


def test_cli_exception_handling_file_not_found():
    """Test CLI exception handling for FileNotFoundError"""
    runner = CliRunner()
    
    result = runner.invoke(cli.backup_incremental, [
        '--config', '/nonexistent/file.yaml',
        '--days', '7'
    ])
    
    assert result.exit_code == 1
    assert 'Error: Config file not found' in result.output


def test_cli_exception_handling_value_error():
    """Test CLI exception handling for ValueError"""
    runner = CliRunner()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("invalid: yaml: content")
        f.flush()
        config_path = f.name
    
    try:
        result = runner.invoke(cli.backup_incremental, [
            '--config', config_path,
            '--days', '7'
        ])
        
        assert result.exit_code == 1
        assert 'Error: Unexpected error' in result.output
        
    finally:
        os.unlink(config_path)


def test_cli_exception_handling_runtime_error(mocker):
    """Test CLI exception handling for RuntimeError"""
    runner = CliRunner()
    
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
    
    try:
        mock_db = mocker.Mock()
        mock_db.__enter__ = mocker.Mock(return_value=mock_db)
        mock_db.__exit__ = mocker.Mock(return_value=False)
        mocker.patch('starrocks_br.db.StarRocksDB', return_value=mock_db)
        mocker.patch('starrocks_br.health.check_cluster_health', 
                    side_effect=RuntimeError("Health check failed"))
        
        result = runner.invoke(cli.backup_incremental, ['--config', config_path, '--days', '7'])
        
        assert result.exit_code == 1
        assert 'Error: Health check failed' in result.output
        
    finally:
        os.unlink(config_path)


def test_cli_exception_handling_generic_exception(mocker):
    """Test CLI exception handling for generic Exception"""
    runner = CliRunner()
    
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
    
    try:
        mocker.patch('starrocks_br.db.StarRocksDB', side_effect=Exception("Unexpected error"))
        
        result = runner.invoke(cli.backup_incremental, ['--config', config_path, '--days', '7'])
        
        assert result.exit_code == 1
        assert 'Error: Unexpected error' in result.output
        
    finally:
        os.unlink(config_path)

