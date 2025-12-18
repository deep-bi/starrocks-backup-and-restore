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

import os
import tempfile

import pytest


@pytest.fixture
def config_file():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("""
        host: "127.0.0.1"
        port: 9030
        user: "root"
        database: "test_db"
        repository: "test_repo"
        """)
        f.flush()
        config_path = f.name

    yield config_path

    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def invalid_yaml_file():
    """Create a temporary invalid YAML file for testing error handling."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("invalid: yaml: content")
        f.flush()
        config_path = f.name

    yield config_path

    if os.path.exists(config_path):
        os.unlink(config_path)


@pytest.fixture
def setup_password_env(monkeypatch):
    """Setup STARROCKS_PASSWORD environment variable for testing."""
    monkeypatch.setenv("STARROCKS_PASSWORD", "test_password")


@pytest.fixture
def mock_db(mocker):
    """Create a mocked StarRocksDB instance with context manager support."""
    mock = mocker.Mock()
    mock.__enter__ = mocker.Mock(return_value=mock)
    mock.__exit__ = mocker.Mock(return_value=False)
    mocker.patch("starrocks_br.db.StarRocksDB", return_value=mock)
    return mock


@pytest.fixture
def mock_initialized_schema(mocker):
    """Mock schema that already exists (ensure_ops_schema returns False)."""
    return mocker.patch("starrocks_br.schema.ensure_ops_schema", return_value=False)


@pytest.fixture
def mock_uninitialized_schema(mocker):
    """Mock schema that doesn't exist (ensure_ops_schema returns True - was created)."""
    return mocker.patch("starrocks_br.schema.ensure_ops_schema", return_value=True)


@pytest.fixture
def mock_healthy_cluster(mocker):
    """Mock a healthy cluster."""
    return mocker.patch("starrocks_br.health.check_cluster_health", return_value=(True, "Healthy"))


@pytest.fixture
def mock_unhealthy_cluster(mocker):
    """Mock an unhealthy cluster."""
    return mocker.patch(
        "starrocks_br.health.check_cluster_health", return_value=(False, "Cluster is unhealthy")
    )


@pytest.fixture
def mock_repo_exists(mocker):
    """Mock repository verification success."""
    return mocker.patch("starrocks_br.repository.ensure_repository")


@pytest.fixture
def mock_validate_tables_exist(mocker):
    """Mock table validation success (no invalid tables)."""
    return mocker.patch("starrocks_br.planner.validate_tables_exist")
