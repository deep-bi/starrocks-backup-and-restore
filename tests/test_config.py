import os
import tempfile

import pytest
import yaml

from starrocks_br import config, exceptions


def test_should_load_valid_yaml_config():
    """Test loading a valid YAML configuration file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
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
        cfg = config.load_config(config_path)
        assert cfg["host"] == "127.0.0.1"
        assert cfg["port"] == 9030
        assert cfg["user"] == "root"
        assert cfg["database"] == "test_db"
        assert cfg["repository"] == "test_repo"
    finally:
        os.unlink(config_path)


def test_should_raise_error_when_config_file_not_found():
    """Test that FileNotFoundError is raised for non-existent config files."""
    with pytest.raises(FileNotFoundError):
        config.load_config("/nonexistent/config.yaml")


def test_should_raise_error_when_yaml_is_invalid():
    """Test that YAMLError is raised for invalid YAML syntax."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("invalid: yaml: content: [")
        f.flush()
        config_path = f.name

    try:
        with pytest.raises(yaml.YAMLError):
            config.load_config(config_path)
    finally:
        os.unlink(config_path)


def test_should_raise_error_when_yaml_root_is_not_dict():
    """Test that ConfigValidationError is raised when YAML root element is not a dictionary."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("- item1\n- item2")
        f.flush()
        config_path = f.name

    try:
        with pytest.raises(exceptions.ConfigValidationError, match="Config must be a dictionary"):
            config.load_config(config_path)
    finally:
        os.unlink(config_path)


def test_should_validate_config_with_all_required_fields():
    """Test that validation passes when all required fields are present."""
    cfg = {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "password": "",
        "database": "test_db",
        "repository": "test_repo",
    }

    config.validate_config(cfg)


@pytest.mark.parametrize("missing_field", ["host", "port", "user", "database", "repository"])
def test_should_raise_error_when_required_field_missing(missing_field):
    """Test that ConfigValidationError is raised when any required field is missing."""
    cfg = {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "database": "test_db",
        "repository": "test_repo",
    }

    # Remove the field being tested
    del cfg[missing_field]

    with pytest.raises(
        exceptions.ConfigValidationError, match=f"Missing required config field: {missing_field}"
    ):
        config.validate_config(cfg)


@pytest.mark.parametrize(
    "tls_config,expected_error",
    [
        # Wrong type: tls config is a string instead of a dictionary
        ("not-a-dict", "TLS configuration must be a dictionary"),
        # ca_cert missing when enabled is True
        ({"enabled": True}, "requires 'ca_cert' when 'enabled' is true"),
        # verify_server_cert wrong type
        (
            {"enabled": True, "ca_cert": "/path/to/cert", "verify_server_cert": "false"},
            "'verify_server_cert' must be a boolean",
        ),
        # tls_versions wrong type (not a list)
        (
            {"enabled": True, "ca_cert": "/path/to/cert", "tls_versions": "TLSv1.2"},
            "'tls_versions' must be a list of strings",
        ),
        # tls_versions list contains non-string elements
        (
            {"enabled": True, "ca_cert": "/path/to/cert", "tls_versions": [1.2, 1.3]},
            "'tls_versions' must be a list of strings",
        ),
    ],
)
def test_validate_config_with_invalid_tls_should_fail(tls_config, expected_error):
    """Test that validation fails for invalid TLS configurations."""
    cfg = {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "database": "test_db",
        "repository": "test_repo",
        "tls": tls_config,
    }

    with pytest.raises(exceptions.ConfigValidationError, match=expected_error):
        config.validate_config(cfg)


@pytest.mark.parametrize(
    "tls_config",
    [
        # No tls section
        None,
        # tls present but enabled is False
        {"enabled": False},
        # tls enabled is missing (defaults to False)
        {},
        # tls enabled with ca_cert
        {"enabled": True, "ca_cert": "/path/to/cert"},
        # All tls fields present and valid
        {
            "enabled": True,
            "ca_cert": "/path/to/cert",
            "verify_server_cert": False,
            "tls_versions": ["TLSv1.2", "TLSv1.3"],
        },
    ],
)
def test_validate_config_with_valid_tls_should_pass(tls_config):
    """Test that validation passes for valid TLS configurations."""
    cfg = {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "database": "test_db",
        "repository": "test_repo",
    }

    if tls_config is not None:
        cfg["tls"] = tls_config

    # Should not raise any exceptions
    config.validate_config(cfg)
