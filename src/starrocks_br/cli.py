import click
import os
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
from . import logger


def _handle_snapshot_exists_error(error_details: dict, label: str, config: str, repository: str, backup_type: str, group: str, baseline_backup: str = None) -> None:
    """Handle snapshot_exists error by providing helpful guidance to the user.
    
    Args:
        error_details: Error details dict containing error_type and snapshot_name
        label: The backup label that was generated
        config: Path to config file
        repository: Repository name
        backup_type: Type of backup ('incremental' or 'full')
        group: Inventory group name
        baseline_backup: Optional baseline backup label (for incremental backups)
    """
    snapshot_name = error_details.get('snapshot_name', label)
    logger.error(f"Snapshot '{snapshot_name}' already exists in the repository.")
    logger.info("")
    logger.info("This typically happens when:")
    logger.info("  • The CLI lost connectivity during a previous backup operation")
    logger.info("  • The backup completed on the server, but backup_history wasn't updated")
    logger.info("")
    logger.info("To resolve this, retry the backup with a custom label using --name:")
    
    if backup_type == 'incremental':
        retry_cmd = f"  starrocks-br backup incremental --config {config} --group {group} --name {snapshot_name}_retry"
        if baseline_backup:
            retry_cmd += f" --baseline-backup {baseline_backup}"
        logger.info(retry_cmd)
    else:
        logger.info(f"  starrocks-br backup full --config {config} --group {group} --name {snapshot_name}_retry")
    
    logger.info("")
    logger.tip("You can verify the existing backup by checking the repository or running:")
    logger.tip(f"  SHOW SNAPSHOT ON {repository} WHERE Snapshot = '{snapshot_name}'")


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
            password=os.getenv('STARROCKS_PASSWORD'),
            database=cfg['database'],
            tls_config=cfg.get('tls'),
        )
        
        with database:
            logger.info("Initializing ops schema...")
            schema.initialize_ops_schema(database)
            logger.info("")
            logger.info("Next steps:")
            logger.info("1. Insert your table inventory records:")
            logger.info("   INSERT INTO ops.table_inventory")
            logger.info("   (inventory_group, database_name, table_name)")
            logger.info("   VALUES ('my_daily_incremental', 'your_db', 'your_fact_table');")
            logger.info("   VALUES ('my_full_database_backup', 'your_db', '*');")
            logger.info("   VALUES ('my_full_dimension_tables', 'your_db', 'dim_customers');")
            logger.info("   VALUES ('my_full_dimension_tables', 'your_db', 'dim_products');")
            logger.info("")
            logger.info("2. Run your first backup:")
            logger.info("   starrocks-br backup incremental --group my_daily_incremental --config config.yaml")
            
    except FileNotFoundError as e:
        logger.error(f"Config file not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to initialize schema: {e}")
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
            password=os.getenv('STARROCKS_PASSWORD'),
            database=cfg['database'],
            tls_config=cfg.get('tls'),
        )
        
        with database:
            was_created = schema.ensure_ops_schema(database)
            if was_created:
                logger.warning("ops schema was auto-created. Please run 'starrocks-br init' after populating config.")
                logger.warning("Remember to populate ops.table_inventory with your backup groups!")
                sys.exit(1) # Exit if schema was just created, requires user action
            
            healthy, message = health.check_cluster_health(database)
            if not healthy:
                logger.error(f"Cluster health check failed: {message}")
                sys.exit(1)
            
            logger.success(f"Cluster health: {message}")
            
            repository.ensure_repository(database, cfg['repository'])
            
            logger.success(f"Repository '{cfg['repository']}' verified")
            
            label = labels.determine_backup_label(
                db=database,
                backup_type='incremental',
                database_name=cfg['database'],
                custom_name=name
            )
            
            logger.success(f"Generated label: {label}")
            
            if baseline_backup:
                logger.success(f"Using specified baseline backup: {baseline_backup}")
            else:
                latest_backup = planner.find_latest_full_backup(database, cfg['database'])
                if latest_backup:
                    logger.success(f"Using latest full backup as baseline: {latest_backup['label']} ({latest_backup['backup_type']})")
                else:
                    logger.warning("No full backup found - this will be the first incremental backup")
            
            partitions = planner.find_recent_partitions(
                database, cfg['database'], baseline_backup_label=baseline_backup, group_name=group
            )
            
            if not partitions:
                logger.warning("No partitions found to backup")
                sys.exit(1)
            
            logger.success(f"Found {len(partitions)} partition(s) to backup")
            
            backup_command = planner.build_incremental_backup_command(
                partitions, cfg['repository'], label, cfg['database']
            )

            concurrency.reserve_job_slot(database, scope='backup', label=label)

            planner.record_backup_partitions(database, label, partitions)

            logger.success(f"Job slot reserved")
            logger.info(f"Starting incremental backup for group '{group}'...")
            result = executor.execute_backup(
                database,
                backup_command,
                repository=cfg['repository'],
                backup_type='incremental',
                scope='backup',
                database=cfg['database']
            )
            
            if result['success']:
                logger.success(f"Backup completed successfully: {result['final_status']['state']}")
                sys.exit(0)
            else:
                error_details = result.get('error_details')
                if error_details and error_details.get('error_type') == 'snapshot_exists':
                    _handle_snapshot_exists_error(
                        error_details, label, config, cfg['repository'], 'incremental', group, baseline_backup
                    )
                    sys.exit(1)
                
                state = result.get('final_status', {}).get('state', 'UNKNOWN')
                if state == "LOST":
                    logger.critical("Backup tracking lost!")
                    logger.warning("Another backup operation started during ours.")
                    logger.tip("Enable ops.run_status concurrency checks to prevent this.")
                logger.error(f"{result['error_message']}")
                sys.exit(1)
                
    except FileNotFoundError as e:
        logger.error(f"Config file not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except RuntimeError as e:
        logger.error(f"{e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
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
            password=os.getenv('STARROCKS_PASSWORD'),
            database=cfg['database'],
            tls_config=cfg.get('tls'),
        )
        
        with database:
            was_created = schema.ensure_ops_schema(database)
            if was_created:
                logger.warning("ops schema was auto-created. Please run 'starrocks-br init' after populating config.")
                logger.warning("Remember to populate ops.table_inventory with your backup groups!")
                sys.exit(1) # Exit if schema was just created, requires user action
            
            healthy, message = health.check_cluster_health(database)
            if not healthy:
                logger.error(f"Cluster health check failed: {message}")
                sys.exit(1)
            
            logger.success(f"Cluster health: {message}")
            
            repository.ensure_repository(database, cfg['repository'])
            
            logger.success(f"Repository '{cfg['repository']}' verified")
            
            label = labels.determine_backup_label(
                db=database,
                backup_type='full',
                database_name=cfg['database'],
                custom_name=name
            )
            
            logger.success(f"Generated label: {label}")
            
            backup_command = planner.build_full_backup_command(
                database, group, cfg['repository'], label, cfg['database']
            )
            
            if not backup_command:
                logger.warning(f"No tables found in group '{group}' for database '{cfg['database']}' to backup")
                sys.exit(1)
            
            tables = planner.find_tables_by_group(database, group)
            all_partitions = planner.get_all_partitions_for_tables(database, cfg['database'], tables)

            concurrency.reserve_job_slot(database, scope='backup', label=label)

            planner.record_backup_partitions(database, label, all_partitions)

            logger.success(f"Job slot reserved")
            logger.info(f"Starting full backup for group '{group}'...")
            result = executor.execute_backup(
                database,
                backup_command,
                repository=cfg['repository'],
                backup_type='full',
                scope='backup',
                database=cfg['database']
            )
            
            if result['success']:
                logger.success(f"Backup completed successfully: {result['final_status']['state']}")
                sys.exit(0)
            else:
                error_details = result.get('error_details')
                if error_details and error_details.get('error_type') == 'snapshot_exists':
                    _handle_snapshot_exists_error(
                        error_details, label, config, cfg['repository'], 'full', group
                    )
                    sys.exit(1)
                
                state = result.get('final_status', {}).get('state', 'UNKNOWN')
                if state == "LOST":
                    logger.critical("Backup tracking lost!")
                    logger.warning("Another backup operation started during ours.")
                    logger.tip("Enable ops.run_status concurrency checks to prevent this.")
                logger.error(f"{result['error_message']}")
                sys.exit(1)
                
    except (FileNotFoundError, ValueError, RuntimeError, Exception) as e:
        if isinstance(e, FileNotFoundError):
            logger.error(f"Config file not found: {e}")
        elif isinstance(e, ValueError):
            logger.error(f"Configuration error: {e}")
        elif isinstance(e, RuntimeError):
            logger.error(f"{e}")
        else:
            logger.error(f"Unexpected error: {e}")
        sys.exit(1)




@cli.command('restore')
@click.option('--config', required=True, help='Path to config YAML file')
@click.option('--target-label', required=True, help='Backup label to restore to')
@click.option('--group', help='Optional inventory group to filter tables to restore')
@click.option('--table', help='Optional table name to restore (table name only, database comes from config). Cannot be used with --group.')
@click.option('--rename-suffix', default='_restored', help='Suffix for temporary tables during restore (default: _restored)')
@click.option('--yes', is_flag=True, help='Skip confirmation prompt and proceed automatically')
def restore_command(config, target_label, group, table, rename_suffix, yes):
    """Restore data to a specific point in time using intelligent backup chain resolution.
    
    This command automatically determines the correct sequence of backups needed for restore:
    - For full backups: restores directly from the target backup
    - For incremental backups: restores the base full backup first, then applies the incremental
    
    The restore process uses temporary tables with the specified suffix for safety, then performs
    an atomic rename to make the restored data live.
    
    Flow: load config → check health → ensure repository → find restore pair → get tables from backup → execute restore flow
    """
    try:
        if group and table:
            logger.error("Cannot specify both --group and --table. Use --table for single table restore or --group for inventory group restore.")
            sys.exit(1)
        
        if table:
            table = table.strip()
            if not table:
                logger.error("Table name cannot be empty")
                sys.exit(1)
            
            if '.' in table:
                logger.error("Table name must not include database prefix. Use 'table_name' not 'database.table_name'. Database comes from config file.")
                sys.exit(1)
        
        cfg = config_module.load_config(config)
        config_module.validate_config(cfg)
        
        database = db.StarRocksDB(
            host=cfg['host'],
            port=cfg['port'],
            user=cfg['user'],
            password=os.getenv('STARROCKS_PASSWORD'),
            database=cfg['database'],
            tls_config=cfg.get('tls'),
        )
        
        with database:
            was_created = schema.ensure_ops_schema(database)
            if was_created:
                logger.warning("ops schema was auto-created. Please run 'starrocks-br init' after populating config.")
                logger.warning("Remember to populate ops.table_inventory with your backup groups!")
                sys.exit(1) # Exit if schema was just created, requires user action
            
            healthy, message = health.check_cluster_health(database)
            if not healthy:
                logger.error(f"Cluster health check failed: {message}")
                sys.exit(1)
            
            logger.success(f"Cluster health: {message}")
            
            repository.ensure_repository(database, cfg['repository'])
            
            logger.success(f"Repository '{cfg['repository']}' verified")
            
            logger.info(f"Finding restore sequence for target backup: {target_label}")
            
            try:
                restore_pair = restore.find_restore_pair(database, target_label)
                logger.success(f"Found restore sequence: {' -> '.join(restore_pair)}")
            except ValueError as e:
                logger.error(f"Failed to find restore sequence: {e}")
                sys.exit(1)
            
            logger.info("Determining tables to restore from backup manifest...")
            
            try:
                tables_to_restore = restore.get_tables_from_backup(
                    database, 
                    target_label, 
                    group=group, 
                    table=table, 
                    database=cfg['database'] if table else None
                )
            except ValueError as e:
                logger.error(str(e))
                sys.exit(1)
            
            if not tables_to_restore:
                if group:
                    logger.warning(f"No tables found in backup '{target_label}' for group '{group}'")
                elif table:
                    logger.warning(f"No tables found in backup '{target_label}' for table '{table}'")
                else:
                    logger.warning(f"No tables found in backup '{target_label}'")
                sys.exit(1)
            
            logger.success(f"Found {len(tables_to_restore)} table(s) to restore: {', '.join(tables_to_restore)}")
            
            logger.info("Starting restore flow...")
            result = restore.execute_restore_flow(
                database,
                cfg['repository'],
                restore_pair,
                tables_to_restore,
                rename_suffix,
                skip_confirmation=yes
            )
            
            if result['success']:
                logger.success(result['message'])
                sys.exit(0)
            else:
                logger.error(f"Restore failed: {result['error_message']}")
                sys.exit(1)
                
    except FileNotFoundError as e:
        logger.error(f"Config file not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except RuntimeError as e:
        logger.error(f"{e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    cli()

