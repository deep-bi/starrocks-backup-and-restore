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
    - ops.table_inventory: Inventory groups mapping to databases/tables
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
            click.echo("   (inventory_group, database_name, table_name)")
            click.echo("   VALUES ('my_daily_incremental', 'your_db', 'your_fact_table');")
            click.echo("   VALUES ('my_full_database_backup', 'your_db', '*');")
            click.echo("   VALUES ('my_full_dimension_tables', 'your_db', 'dim_customers');")
            click.echo("   VALUES ('my_full_dimension_tables', 'your_db', 'dim_products');")
            click.echo("")
            click.echo("2. Run your first backup:")
            click.echo("   starrocks-br backup incremental --group my_daily_incremental --config config.yaml")
            
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
@click.option('--baseline-backup', help='Specific backup label to use as baseline (optional). If not provided, uses the latest successful full backup.')
@click.option('--group', required=True, help='Inventory group to backup from table_inventory. Supports wildcard \'*\'.')
@click.option('--name', help='Optional logical name (label) for the backup. Supports -v#r placeholder for auto-versioning.')
def backup_incremental(config, baseline_backup, group, name):
    """Run incremental backup of partitions changed since the latest full backup.
    
    By default, uses the latest successful full backup as baseline.
    Optionally specify a specific backup label to use as baseline.
    
    Flow: load config → check health → ensure repository → reserve job slot →
    find baseline backup → find recent partitions → generate label → build backup command → execute backup
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
                click.echo("⚠ ops schema was auto-created. Please run 'starrocks-br init' after populating config.", err=True)
                click.echo("⚠ Remember to populate ops.table_inventory with your backup groups!", err=True)
                sys.exit(1) # Exit if schema was just created, requires user action
            
            healthy, message = health.check_cluster_health(database)
            if not healthy:
                click.echo(f"Error: Cluster health check failed: {message}", err=True)
                sys.exit(1)
            
            click.echo(f"✓ Cluster health: {message}")
            
            repository.ensure_repository(database, cfg['repository'])
            
            click.echo(f"✓ Repository '{cfg['repository']}' verified")
            
            resolved = labels.resolve_label(database, name)
            today = datetime.now().strftime("%Y-%m-%d")
            label = resolved or labels.generate_label(cfg['database'], today, 'incremental')
            
            click.echo(f"✓ Generated label: {label}")
            
            concurrency.reserve_job_slot(database, scope='backup', label=label)
            
            click.echo(f"✓ Job slot reserved")
            
            if baseline_backup:
                click.echo(f"✓ Using specified baseline backup: {baseline_backup}")
            else:
                latest_backup = planner.find_latest_full_backup(database, cfg['database'])
                if latest_backup:
                    click.echo(f"✓ Using latest full backup as baseline: {latest_backup['label']} ({latest_backup['backup_type']})")
                else:
                    click.echo("⚠ No full backup found - this will be the first incremental backup")
            
            partitions = planner.find_recent_partitions(
                database, cfg['database'], baseline_backup_label=baseline_backup, group_name=group
            )
            
            if not partitions:
                click.echo("Warning: No partitions found to backup", err=True)
                sys.exit(1)
            
            click.echo(f"✓ Found {len(partitions)} partition(s) to backup")
            
            backup_command = planner.build_incremental_backup_command(
                partitions, cfg['repository'], label, cfg['database']
            )
            
            click.echo(f"Starting incremental backup for group '{group}'...")
            result = executor.execute_backup(
                database,
                backup_command,
                repository=cfg['repository'],
                backup_type='incremental',
                scope='backup',
                database=cfg['database']
            )
            
            if result['success']:
                click.echo(f"✓ Backup completed successfully: {result['final_status']['state']}")
                sys.exit(0)
            else:
                state = result.get('final_status', {}).get('state', 'UNKNOWN')
                if state == "LOST":
                    click.echo("❌ CRITICAL: Backup tracking lost!", err=True)
                    click.echo("⚠️  Another backup operation started during ours.", err=True)
                    click.echo("💡 Enable ops.run_status concurrency checks to prevent this.", err=True)
                click.echo(f"Error: {result['error_message']}", err=True)
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


@backup.command('full')
@click.option('--config', required=True, help='Path to config YAML file')
@click.option('--group', required=True, help='Inventory group to backup from table_inventory. Supports wildcard \'*\'.')
@click.option('--name', help='Optional logical name (label) for the backup. Supports -v#r placeholder for auto-versioning.')
def backup_full(config, group, name):
    """Run a full backup for a specified inventory group.
    
    Flow: load config → check health → ensure repository → reserve job slot →
    find tables by group → generate label → build backup command → execute backup
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
                click.echo("⚠ ops schema was auto-created. Please run 'starrocks-br init' after populating config.", err=True)
                click.echo("⚠ Remember to populate ops.table_inventory with your backup groups!", err=True)
                sys.exit(1) # Exit if schema was just created, requires user action
            
            healthy, message = health.check_cluster_health(database)
            if not healthy:
                click.echo(f"Error: Cluster health check failed: {message}", err=True)
                sys.exit(1)
            
            click.echo(f"✓ Cluster health: {message}")
            
            repository.ensure_repository(database, cfg['repository'])
            
            click.echo(f"✓ Repository '{cfg['repository']}' verified")
            
            resolved = labels.resolve_label(database, name)
            today = datetime.now().strftime("%Y-%m-%d")
            label = resolved or labels.generate_label(cfg['database'], today, 'full')
            
            click.echo(f"✓ Generated label: {label}")
            
            concurrency.reserve_job_slot(database, scope='backup', label=label)
            
            click.echo(f"✓ Job slot reserved")
            
            backup_command = planner.build_full_backup_command(
                database, group, cfg['repository'], label, cfg['database']
            )
            
            if not backup_command:
                click.echo(f"Warning: No tables found in group '{group}' for database '{cfg['database']}' to backup", err=True)
                sys.exit(1)
            
            click.echo(f"Starting full backup for group '{group}'...")
            result = executor.execute_backup(
                database,
                backup_command,
                repository=cfg['repository'],
                backup_type='full',
                scope='backup',
                database=cfg['database']
            )
            
            if result['success']:
                click.echo(f"✓ Backup completed successfully: {result['final_status']['state']}")
                sys.exit(0)
            else:
                state = result.get('final_status', {}).get('state', 'UNKNOWN')
                if state == "LOST":
                    click.echo("❌ CRITICAL: Backup tracking lost!", err=True)
                    click.echo("⚠️  Another backup operation started during ours.", err=True)
                    click.echo("💡 Enable ops.run_status concurrency checks to prevent this.", err=True)
                click.echo(f"Error: {result['error_message']}", err=True)
                sys.exit(1)
                
    except (FileNotFoundError, ValueError, RuntimeError, Exception) as e:
        if isinstance(e, FileNotFoundError):
            click.echo(f"Error: Config file not found: {e}", err=True)
        elif isinstance(e, ValueError):
            click.echo(f"Error: Configuration error: {e}", err=True)
        elif isinstance(e, RuntimeError):
            click.echo(f"Error: {e}", err=True)
        else:
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
                click.echo("⚠ ops schema was auto-created. Please run 'starrocks-br init' after populating config.", err=True)
                click.echo("⚠ Remember to populate ops.table_inventory with your backup groups!", err=True)
                sys.exit(1) # Exit if schema was just created, requires user action
            
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

