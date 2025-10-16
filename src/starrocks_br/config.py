import yaml
from typing import Dict


def load_config(config_path: str) -> Dict:
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


def validate_config(config: Dict) -> None:
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

