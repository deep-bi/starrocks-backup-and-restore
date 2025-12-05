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


def get_ops_database(config: dict[str, Any]) -> str:
    """Get the ops database name from config, defaulting to 'ops'."""
    return config.get("ops_database", "ops")


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
