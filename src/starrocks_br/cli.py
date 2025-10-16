import click
import sys
from datetime import datetime
from . import config as config_module
from . import db
from . import health
from . import repository
from . import concurrency
from . import planner
from . import labels
from . import executor
from . import restore
from . import schema


@click.group()
def cli():
    """StarRocks Backup & Restore automation tool."""
    pass


@cli.command('init')
@click.option('--config', required=True, help='Path to config YAML file')
def init(config):
    """Initialize ops database and control tables.
    
    Creates the ops database with required tables:
    - ops.table_inventory: Table backup eligibility configuration
    - ops.backup_history: Backup operation history
    - ops.restore_history: Restore operation history
    - ops.run_status: Job concurrency control
    
    Run this once before using backup/restore commands.
    """
    try:
        cfg = config_module.load_config(config)
        config_module.validate_config(cfg)
        
        database = db.StarRocksDB(
            host=cfg['host'],
            port=cfg['port'],
            user=cfg['user'],
            password=cfg.get('password', ''),
            database=cfg['database']
        )
        
        with database:
            click.echo("Initializing ops schema...")
            schema.initialize_ops_schema(database)
            click.echo("✓ ops database created")
            click.echo("✓ ops.table_inventory created")
            click.echo("✓ ops.backup_history created")
            click.echo("✓ ops.restore_history created")
            click.echo("✓ ops.run_status created")
            click.echo("")
            click.echo("Schema initialized successfully!")
            click.echo("")
            click.echo("Next steps:")
            click.echo("1. Insert your table inventory records:")
            click.echo("   INSERT INTO ops.table_inventory")
            click.echo("   (database_name, table_name, table_type, backup_eligible,")
            click.echo("    incremental_eligible, weekly_eligible, monthly_eligible)")
            click.echo("   VALUES ('your_db', 'your_table', 'fact', true, true, false, true);")
            click.echo("")
            click.echo("2. Run your first backup:")
            click.echo("   starrocks-br backup incremental --config config.yaml --days 7")
            
    except FileNotFoundError as e:
        click.echo(f"Error: Config file not found: {e}", err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(f"Error: Configuration error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: Failed to initialize schema: {e}", err=True)
        sys.exit(1)


@cli.group()
def backup():
    """Backup commands."""
    pass


@backup.command('incremental')
@click.option('--config', required=True, help='Path to config YAML file')
@click.option('--days', type=int, required=True, help='Number of days to look back for changed partitions')
def backup_incremental(config, days):
    """Run incremental backup of recently changed partitions.
    
    Flow: load config → check health → ensure repository → reserve job slot →
    find recent partitions → generate label → build backup command → execute backup
    """
    try:
        cfg = config_module.load_config(config)
        config_module.validate_config(cfg)
        
        database = db.StarRocksDB(
            host=cfg['host'],
            port=cfg['port'],
            user=cfg['user'],
            password=cfg.get('password', ''),
            database=cfg['database']
        )
        
        with database:
            was_created = schema.ensure_ops_schema(database)
            if was_created:
                click.echo("⚠ ops schema was auto-created (run 'starrocks-br init' next time)")
                click.echo("⚠ Remember to populate ops.table_inventory with your tables!")
            
            healthy, message = health.check_cluster_health(database)
            if not healthy:
                click.echo(f"Error: Cluster health check failed: {message}", err=True)
                sys.exit(1)
            
            click.echo(f"✓ Cluster health: {message}")
            
            repository.ensure_repository(database, {
                'name': cfg['repository'],
                'type': 's3',
                'endpoint': cfg.get('endpoint', ''),
                'bucket': cfg.get('bucket', ''),
                'prefix': cfg.get('prefix', '/'),
                'access_key': cfg.get('access_key', ''),
                'secret_key': cfg.get('secret_key', ''),
                'force_https': cfg.get('force_https', True)
            })
            
            click.echo(f"✓ Repository '{cfg['repository']}' verified")
            
            today = datetime.now().strftime("%Y-%m-%d")
            label = labels.generate_label(cfg['database'], today, 'inc')
            
            click.echo(f"✓ Generated label: {label}")
            
            concurrency.reserve_job_slot(database, scope='backup', label=label)
            
            click.echo(f"✓ Job slot reserved")
            
            partitions = planner.find_recent_partitions(database, days)
            
            if not partitions:
                click.echo("Warning: No partitions found to backup", err=True)
                sys.exit(1)
            
            click.echo(f"✓ Found {len(partitions)} partition(s) to backup")
            
            backup_command = planner.build_incremental_backup_command(
                partitions, cfg['repository'], label
            )
            
            click.echo("Starting incremental backup...")
            result = executor.execute_backup(
                database,
                backup_command,
                repository=cfg['repository'],
                backup_type='incremental',
                scope='backup'
            )
            
            if result['success']:
                click.echo(f"✓ Backup completed successfully: {result['final_status']['state']}")
                sys.exit(0)
            else:
                click.echo(f"Error: Backup failed: {result['error_message']}", err=True)
                sys.exit(1)
                
    except FileNotFoundError as e:
        click.echo(f"Error: Config file not found: {e}", err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(f"Error: Configuration error: {e}", err=True)
        sys.exit(1)
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: Unexpected error: {e}", err=True)
        sys.exit(1)


@backup.command('weekly')
@click.option('--config', required=True, help='Path to config YAML file')
def backup_weekly(config):
    """Run weekly full backup of dimension and non-partitioned tables.
    
    Flow: load config → check health → ensure repository → reserve job slot →
    find weekly tables → generate label → build backup command → execute backup
    """
    try:
        cfg = config_module.load_config(config)
        config_module.validate_config(cfg)
        
        database = db.StarRocksDB(
            host=cfg['host'],
            port=cfg['port'],
            user=cfg['user'],
            password=cfg.get('password', ''),
            database=cfg['database']
        )
        
        with database:
            was_created = schema.ensure_ops_schema(database)
            if was_created:
                click.echo("⚠ ops schema was auto-created (run 'starrocks-br init' next time)")
                click.echo("⚠ Remember to populate ops.table_inventory with your tables!")
            
            healthy, message = health.check_cluster_health(database)
            if not healthy:
                click.echo(f"Error: Cluster health check failed: {message}", err=True)
                sys.exit(1)
            
            click.echo(f"✓ Cluster health: {message}")
            
            repository.ensure_repository(database, {
                'name': cfg['repository'],
                'type': 's3',
                'endpoint': cfg.get('endpoint', ''),
                'bucket': cfg.get('bucket', ''),
                'prefix': cfg.get('prefix', '/'),
                'access_key': cfg.get('access_key', ''),
                'secret_key': cfg.get('secret_key', ''),
                'force_https': cfg.get('force_https', True)
            })
            
            click.echo(f"✓ Repository '{cfg['repository']}' verified")
            
            today = datetime.now().strftime("%Y-%m-%d")
            label = labels.generate_label(cfg['database'], today, 'weekly')
            
            click.echo(f"✓ Generated label: {label}")
            
            concurrency.reserve_job_slot(database, scope='backup', label=label)
            
            click.echo(f"✓ Job slot reserved")
            
            tables = planner.find_weekly_eligible_tables(database)
            
            if not tables:
                click.echo("Warning: No tables found to backup", err=True)
                sys.exit(1)
            
            click.echo(f"✓ Found {len(tables)} table(s) to backup")
            
            backup_command = planner.build_weekly_backup_command(
                tables, cfg['repository'], label
            )
            
            click.echo("Starting weekly backup...")
            result = executor.execute_backup(
                database,
                backup_command,
                repository=cfg['repository'],
                backup_type='weekly',
                scope='backup'
            )
            
            if result['success']:
                click.echo(f"✓ Backup completed successfully: {result['final_status']['state']}")
                sys.exit(0)
            else:
                click.echo(f"Error: Backup failed: {result['error_message']}", err=True)
                sys.exit(1)
                
    except FileNotFoundError as e:
        click.echo(f"Error: Config file not found: {e}", err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(f"Error: Configuration error: {e}", err=True)
        sys.exit(1)
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: Unexpected error: {e}", err=True)
        sys.exit(1)


@backup.command('monthly')
@click.option('--config', required=True, help='Path to config YAML file')
def backup_monthly(config):
    """Run monthly full database backup.
    
    Flow: load config → check health → ensure repository → reserve job slot →
    generate label → build database backup command → execute backup
    """
    try:
        cfg = config_module.load_config(config)
        config_module.validate_config(cfg)
        
        database = db.StarRocksDB(
            host=cfg['host'],
            port=cfg['port'],
            user=cfg['user'],
            password=cfg.get('password', ''),
            database=cfg['database']
        )
        
        with database:
            was_created = schema.ensure_ops_schema(database)
            if was_created:
                click.echo("⚠ ops schema was auto-created (run 'starrocks-br init' next time)")
                click.echo("⚠ Remember to populate ops.table_inventory with your tables!")
            
            healthy, message = health.check_cluster_health(database)
            if not healthy:
                click.echo(f"Error: Cluster health check failed: {message}", err=True)
                sys.exit(1)
            
            click.echo(f"✓ Cluster health: {message}")
            
            repository.ensure_repository(database, {
                'name': cfg['repository'],
                'type': 's3',
                'endpoint': cfg.get('endpoint', ''),
                'bucket': cfg.get('bucket', ''),
                'prefix': cfg.get('prefix', '/'),
                'access_key': cfg.get('access_key', ''),
                'secret_key': cfg.get('secret_key', ''),
                'force_https': cfg.get('force_https', True)
            })
            
            click.echo(f"✓ Repository '{cfg['repository']}' verified")
            
            today = datetime.now().strftime("%Y-%m-%d")
            label = labels.generate_label(cfg['database'], today, 'monthly')
            
            click.echo(f"✓ Generated label: {label}")
            
            concurrency.reserve_job_slot(database, scope='backup', label=label)
            
            click.echo(f"✓ Job slot reserved")
            
            backup_command = planner.build_monthly_backup_command(
                cfg['database'], cfg['repository'], label
            )
            
            click.echo("Starting monthly backup...")
            result = executor.execute_backup(
                database,
                backup_command,
                repository=cfg['repository'],
                backup_type='monthly',
                scope='backup'
            )
            
            if result['success']:
                click.echo(f"✓ Backup completed successfully: {result['final_status']['state']}")
                sys.exit(0)
            else:
                click.echo(f"Error: Backup failed: {result['error_message']}", err=True)
                sys.exit(1)
                
    except FileNotFoundError as e:
        click.echo(f"Error: Config file not found: {e}", err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(f"Error: Configuration error: {e}", err=True)
        sys.exit(1)
    except RuntimeError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: Unexpected error: {e}", err=True)
        sys.exit(1)


@cli.command('restore-partition')
@click.option('--config', required=True, help='Path to config YAML file')
@click.option('--backup-label', required=True, help='Backup label to restore from')
@click.option('--table', required=True, help='Table name in format database.table')
@click.option('--partition', required=True, help='Partition name to restore')
def restore_partition(config, backup_label, table, partition):
    """Restore a single partition from a backup.
    
    Flow: load config → build restore command → execute restore → log history
    """
    try:
        cfg = config_module.load_config(config)
        config_module.validate_config(cfg)
        
        if '.' not in table:
            click.echo(f"Error: Table must be in format database.table", err=True)
            sys.exit(1)
        
        database_name, table_name = table.split('.', 1)
        
        database = db.StarRocksDB(
            host=cfg['host'],
            port=cfg['port'],
            user=cfg['user'],
            password=cfg.get('password', ''),
            database=cfg['database']
        )
        
        with database:
            was_created = schema.ensure_ops_schema(database)
            if was_created:
                click.echo("⚠ ops schema was auto-created (run 'starrocks-br init' next time)")
            
            click.echo(f"Restoring partition {partition} of {table} from backup {backup_label}...")
            
            restore_command = restore.build_partition_restore_command(
                database=database_name,
                table=table_name,
                partition=partition,
                backup_label=backup_label,
                repository=cfg['repository']
            )
            
            result = restore.execute_restore(
                database,
                restore_command,
                backup_label=backup_label,
                restore_type='partition',
                repository=cfg['repository'],
                scope='restore'
            )
            
            if result['success']:
                click.echo(f"✓ Restore completed successfully: {result['final_status']['state']}")
                sys.exit(0)
            else:
                click.echo(f"Error: Restore failed: {result['error_message']}", err=True)
                sys.exit(1)
                
    except FileNotFoundError as e:
        click.echo(f"Error: Config file not found: {e}", err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(f"Error: Configuration error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: Unexpected error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()

