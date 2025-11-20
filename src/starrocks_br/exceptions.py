class StarRocksBRError(Exception):
    pass


class MissingOptionError(StarRocksBRError):
    def __init__(self, missing_option: str):
        self.missing_option = missing_option
        super().__init__(f"Missing required option: {missing_option}")


class BackupLabelNotFoundError(StarRocksBRError):
    def __init__(self, label: str, repository: str = None):
        self.label = label
        self.repository = repository
        if repository:
            super().__init__(f"Backup label '{label}' not found in repository '{repository}'")
        else:
            super().__init__(f"Backup label '{label}' not found")


class NoSuccessfulFullBackupFoundError(StarRocksBRError):
    def __init__(self, incremental_label: str):
        self.incremental_label = incremental_label
        super().__init__(
            f"No successful full backup found before incremental '{incremental_label}'"
        )


class TableNotFoundInBackupError(StarRocksBRError):
    def __init__(self, table: str, label: str, database: str):
        self.table = table
        self.label = label
        self.database = database
        super().__init__(f"Table '{table}' not found in backup '{label}' for database '{database}'")


class InvalidTableNameError(StarRocksBRError):
    def __init__(self, table_name: str, reason: str):
        self.table_name = table_name
        self.reason = reason
        super().__init__(f"Invalid table name '{table_name}': {reason}")


class ConfigFileNotFoundError(StarRocksBRError):
    def __init__(self, config_path: str):
        self.config_path = config_path
        super().__init__(f"Config file not found: {config_path}")


class ConfigValidationError(StarRocksBRError):
    def __init__(self, message: str):
        super().__init__(f"Configuration error: {message}")


class ClusterHealthCheckFailedError(StarRocksBRError):
    def __init__(self, message: str):
        self.health_message = message
        super().__init__(f"Cluster health check failed: {message}")


class SnapshotNotFoundError(StarRocksBRError):
    def __init__(self, snapshot_name: str, repository: str):
        self.snapshot_name = snapshot_name
        self.repository = repository
        super().__init__(f"Snapshot '{snapshot_name}' not found in repository '{repository}'")


class NoPartitionsFoundError(StarRocksBRError):
    def __init__(self, group_name: str = None):
        self.group_name = group_name
        if group_name:
            super().__init__(f"No partitions found to backup for group '{group_name}'")
        else:
            super().__init__("No partitions found to backup")


class NoTablesFoundError(StarRocksBRError):
    def __init__(self, group: str = None, label: str = None):
        self.group = group
        self.label = label
        if group and label:
            super().__init__(f"No tables found in backup '{label}' for group '{group}'")
        elif group:
            super().__init__(f"No tables found for group '{group}'")
        elif label:
            super().__init__(f"No tables found in backup '{label}'")
        else:
            super().__init__("No tables found")


class RestoreOperationCancelledError(StarRocksBRError):
    def __init__(self):
        super().__init__("Restore operation cancelled by user")
