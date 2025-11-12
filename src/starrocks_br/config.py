import yaml
from typing import Any, Dict, Optional


def load_config(config_path: str) -> Dict[str, Any]:
    """Load and parse YAML configuration file.
    
    Args:
        config_path: Path to the YAML config file
        
    Returns:
        Dictionary containing configuration
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is not valid YAML
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    if not isinstance(config, dict):
        raise ValueError("Config must be a dictionary")
    
    return config


def validate_config(config: Dict[str, Any]) -> None:
    """Validate that config contains required fields.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        ValueError: If required fields are missing
    """
    required_fields = ['host', 'port', 'user', 'database', 'repository']
    
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing required config field: {field}")

    _validate_tls_section(config.get('tls'))


def _validate_tls_section(tls_config) -> None:
    if tls_config is None:
        return

    if not isinstance(tls_config, dict):
        raise ValueError("TLS configuration must be a dictionary")

    enabled = bool(tls_config.get('enabled', False))

    if enabled and not tls_config.get('ca_cert'):
        raise ValueError("TLS configuration requires 'ca_cert' when 'enabled' is true")

    if 'verify_server_cert' in tls_config and not isinstance(tls_config['verify_server_cert'], bool):
        raise ValueError("TLS configuration field 'verify_server_cert' must be a boolean if provided")

    if 'tls_versions' in tls_config:
        tls_versions = tls_config['tls_versions']
        if not isinstance(tls_versions, list) or not all(isinstance(version, str) for version in tls_versions):
            raise ValueError("TLS configuration field 'tls_versions' must be a list of strings if provided")

