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

from typing import Any

import yaml

from . import exceptions


def load_config(config_path: str) -> dict[str, Any]:
    """Load and parse YAML configuration file.

    Args:
        config_path: Path to the YAML config file

    Returns:
        Dictionary containing configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is not valid YAML
    """
    with open(config_path) as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise exceptions.ConfigValidationError("Config must be a dictionary")

    return config


def validate_config(config: dict[str, Any]) -> None:
    """Validate that config contains required fields.

    Args:
        config: Configuration dictionary

    Raises:
        ConfigValidationError: If required fields are missing
    """
    required_fields = ["host", "port", "user", "database", "repository"]

    for field in required_fields:
        if field not in config:
            raise exceptions.ConfigValidationError(f"Missing required config field: {field}")

    _validate_tls_section(config.get("tls"))
    _validate_table_inventory_section(config.get("table_inventory"))


def get_ops_database(config: dict[str, Any]) -> str:
    """Get the ops database name from config, defaulting to 'ops'."""
    return config.get("ops_database", "ops")


def get_table_inventory_entries(config: dict[str, Any]) -> list[tuple[str, str, str]]:
    """Extract table inventory entries from config.

    Args:
        config: Configuration dictionary

    Returns:
        List of tuples (group, database, table)
    """
    table_inventory = config.get("table_inventory")
    if not table_inventory:
        return []

    entries = []
    for group_entry in table_inventory:
        group = group_entry["group"]
        for table_entry in group_entry["tables"]:
            entries.append((group, table_entry["database"], table_entry["table"]))

    return entries


def _validate_tls_section(tls_config) -> None:
    if tls_config is None:
        return

    if not isinstance(tls_config, dict):
        raise exceptions.ConfigValidationError("TLS configuration must be a dictionary")

    enabled = bool(tls_config.get("enabled", False))

    if enabled and not tls_config.get("ca_cert"):
        raise exceptions.ConfigValidationError(
            "TLS configuration requires 'ca_cert' when 'enabled' is true"
        )

    if "verify_server_cert" in tls_config and not isinstance(
        tls_config["verify_server_cert"], bool
    ):
        raise exceptions.ConfigValidationError(
            "TLS configuration field 'verify_server_cert' must be a boolean if provided"
        )

    if "tls_versions" in tls_config:
        tls_versions = tls_config["tls_versions"]
        if not isinstance(tls_versions, list) or not all(
            isinstance(version, str) for version in tls_versions
        ):
            raise exceptions.ConfigValidationError(
                "TLS configuration field 'tls_versions' must be a list of strings if provided"
            )


def _validate_table_inventory_section(table_inventory) -> None:
    if table_inventory is None:
        return

    if not isinstance(table_inventory, list):
        raise exceptions.ConfigValidationError("'table_inventory' must be a list")

    for entry in table_inventory:
        if not isinstance(entry, dict):
            raise exceptions.ConfigValidationError(
                "Each entry in 'table_inventory' must be a dictionary"
            )

        if "group" not in entry:
            raise exceptions.ConfigValidationError(
                "Each entry in 'table_inventory' must have a 'group' field"
            )

        if "tables" not in entry:
            raise exceptions.ConfigValidationError(
                "Each entry in 'table_inventory' must have a 'tables' field"
            )

        if not isinstance(entry["group"], str):
            raise exceptions.ConfigValidationError("'group' field must be a string")

        tables = entry["tables"]
        if not isinstance(tables, list):
            raise exceptions.ConfigValidationError("'tables' field must be a list")

        for table_entry in tables:
            if not isinstance(table_entry, dict):
                raise exceptions.ConfigValidationError("Each table entry must be a dictionary")

            if "database" not in table_entry or "table" not in table_entry:
                raise exceptions.ConfigValidationError(
                    "Each table entry must have 'database' and 'table' fields"
                )

            if not isinstance(table_entry["database"], str) or not isinstance(
                table_entry["table"], str
            ):
                raise exceptions.ConfigValidationError(
                    "'database' and 'table' fields must be strings"
                )
