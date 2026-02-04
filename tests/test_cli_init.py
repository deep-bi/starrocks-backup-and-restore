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

from click.testing import CliRunner

from starrocks_br import cli


def test_init_command_success(config_file, mock_db, setup_password_env, mocker):
    """Test successful init command."""
    runner = CliRunner()

    mocker.patch("starrocks_br.schema.initialize_ops_schema")
    mocker.patch("starrocks_br.repository.ensure_repository")

    result = runner.invoke(cli.init, ["--config", config_file])

    assert result.exit_code == 0
    assert "Next steps:" in result.output
    assert "INSERT INTO ops.table_inventory" in result.output


def test_init_validates_repository_exists(config_file, mock_db, setup_password_env, mocker):
    """Init command should validate that repository exists before creating schema."""
    runner = CliRunner()

    mock_ensure_repo = mocker.patch("starrocks_br.repository.ensure_repository")
    mocker.patch("starrocks_br.schema.initialize_ops_schema")

    result = runner.invoke(cli.init, ["--config", config_file])

    assert result.exit_code == 0
    mock_ensure_repo.assert_called_once_with(mock_db, "test_repo")


def test_init_fails_when_repository_not_found(config_file, mock_db, setup_password_env, mocker):
    """Init command should fail with clear error when repository doesn't exist."""
    runner = CliRunner()

    mocker.patch(
        "starrocks_br.repository.ensure_repository",
        side_effect=RuntimeError(
            "Repository 'test_repo' not found. Please create it first using:\n"
            "  CREATE REPOSITORY test_repo WITH BROKER ON LOCATION '...' PROPERTIES(...)\n"
            "For examples, see: https://docs.starrocks.io/docs/sql-reference/sql-statements/backup_restore/CREATE_REPOSITORY/"
        ),
    )

    result = runner.invoke(cli.init, ["--config", config_file])

    assert result.exit_code == 1
    assert "Repository 'test_repo' not found" in result.output
    assert "CREATE REPOSITORY" in result.output


def test_init_fails_when_repository_has_errors(config_file, mock_db, setup_password_env, mocker):
    """Init command should fail when repository exists but has errors."""
    runner = CliRunner()

    mocker.patch(
        "starrocks_br.repository.ensure_repository",
        side_effect=RuntimeError("Repository 'test_repo' has errors: Connection failed: auth error"),
    )

    result = runner.invoke(cli.init, ["--config", config_file])

    assert result.exit_code == 1
    assert "Repository 'test_repo' has errors" in result.output
    assert "Connection failed" in result.output
