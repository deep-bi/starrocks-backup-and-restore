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

"""Unit tests for the prune module."""

import pytest

from starrocks_br import prune


class TestGetSuccessfulBackups:
    """Unit tests for get_successful_backups function."""

    def test_get_backups_without_group(self, mocker):
        """Test getting backups without group filter."""
        mock_db = mocker.Mock()
        mock_db.query.return_value = [
            ("backup1", "2024-01-01 00:00:00"),
            ("backup2", "2024-01-02 00:00:00"),
        ]

        result = prune.get_successful_backups(mock_db, "test_repo")

        assert len(result) == 2
        assert result[0] == {"label": "backup1", "finished_at": "2024-01-01 00:00:00"}
        assert result[1] == {"label": "backup2", "finished_at": "2024-01-02 00:00:00"}

        query_sql = mock_db.query.call_args[0][0]
        assert "test_repo" in query_sql
        assert "FINISHED" in query_sql
        assert "inventory_group" not in query_sql

    def test_get_backups_with_group(self, mocker):
        """Test getting backups with group filter."""
        mock_db = mocker.Mock()
        mock_db.query.return_value = [
            ("backup1", "2024-01-01 00:00:00", "prod_group"),
            ("backup2", "2024-01-02 00:00:00", "prod_group"),
        ]

        result = prune.get_successful_backups(mock_db, "test_repo", group="prod_group")

        assert len(result) == 2
        assert result[0] == {
            "label": "backup1",
            "finished_at": "2024-01-01 00:00:00",
            "inventory_group": "prod_group",
        }
        assert result[1] == {
            "label": "backup2",
            "finished_at": "2024-01-02 00:00:00",
            "inventory_group": "prod_group",
        }

        query_sql = mock_db.query.call_args[0][0]
        assert "prod_group" in query_sql
        assert "inventory_group" in query_sql
        assert "table_inventory" in query_sql

    def test_get_backups_empty_result(self, mocker):
        """Test getting backups when none exist."""
        mock_db = mocker.Mock()
        mock_db.query.return_value = []

        result = prune.get_successful_backups(mock_db, "test_repo")

        assert result == []

    def test_get_backups_custom_ops_database(self, mocker):
        """Test using custom ops database name."""
        mock_db = mocker.Mock()
        mock_db.query.return_value = []

        prune.get_successful_backups(mock_db, "test_repo", ops_database="custom_ops")

        query_sql = mock_db.query.call_args[0][0]
        assert "custom_ops.backup_history" in query_sql


class TestFilterSnapshotsToDelete:
    """Unit tests for filter_snapshots_to_delete function."""

    def test_keep_last_happy_path(self):
        """Test keep_last strategy with valid count."""
        snapshots = [
            {"label": "backup1", "finished_at": "2024-01-01 00:00:00"},
            {"label": "backup2", "finished_at": "2024-01-02 00:00:00"},
            {"label": "backup3", "finished_at": "2024-01-03 00:00:00"},
            {"label": "backup4", "finished_at": "2024-01-04 00:00:00"},
            {"label": "backup5", "finished_at": "2024-01-05 00:00:00"},
        ]

        result = prune.filter_snapshots_to_delete(snapshots, "keep_last", count=2)

        assert len(result) == 3
        assert result[0]["label"] == "backup1"
        assert result[1]["label"] == "backup2"
        assert result[2]["label"] == "backup3"

    def test_keep_last_exactly_count_snapshots(self):
        """Test keep_last when snapshot count equals keep count."""
        snapshots = [
            {"label": "backup1", "finished_at": "2024-01-01 00:00:00"},
            {"label": "backup2", "finished_at": "2024-01-02 00:00:00"},
        ]

        result = prune.filter_snapshots_to_delete(snapshots, "keep_last", count=2)

        assert result == []

    def test_keep_last_fewer_than_count(self):
        """Test keep_last when fewer snapshots than keep count."""
        snapshots = [
            {"label": "backup1", "finished_at": "2024-01-01 00:00:00"},
        ]

        result = prune.filter_snapshots_to_delete(snapshots, "keep_last", count=5)

        assert result == []

    def test_keep_last_missing_count(self):
        """Test keep_last with missing count parameter."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="positive count"):
            prune.filter_snapshots_to_delete(snapshots, "keep_last")

    def test_keep_last_zero_count(self):
        """Test keep_last with zero count."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="positive count"):
            prune.filter_snapshots_to_delete(snapshots, "keep_last", count=0)

    def test_keep_last_negative_count(self):
        """Test keep_last with negative count."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="positive count"):
            prune.filter_snapshots_to_delete(snapshots, "keep_last", count=-1)

    def test_older_than_happy_path(self):
        """Test older_than strategy with valid timestamp."""
        snapshots = [
            {"label": "backup1", "finished_at": "2023-12-01 00:00:00"},
            {"label": "backup2", "finished_at": "2024-01-01 00:00:00"},
            {"label": "backup3", "finished_at": "2024-02-01 00:00:00"},
        ]

        result = prune.filter_snapshots_to_delete(
            snapshots, "older_than", timestamp="2024-01-15 00:00:00"
        )

        assert len(result) == 2
        assert result[0]["label"] == "backup1"
        assert result[1]["label"] == "backup2"

    def test_older_than_no_matches(self):
        """Test older_than when no snapshots are older than cutoff."""
        snapshots = [
            {"label": "backup1", "finished_at": "2024-02-01 00:00:00"},
            {"label": "backup2", "finished_at": "2024-03-01 00:00:00"},
        ]

        result = prune.filter_snapshots_to_delete(
            snapshots, "older_than", timestamp="2024-01-01 00:00:00"
        )

        assert result == []

    def test_older_than_missing_timestamp(self):
        """Test older_than with missing timestamp."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="requires a timestamp"):
            prune.filter_snapshots_to_delete(snapshots, "older_than")

    def test_older_than_invalid_timestamp_format(self):
        """Test older_than with invalid timestamp format."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="Invalid timestamp format"):
            prune.filter_snapshots_to_delete(snapshots, "older_than", timestamp="invalid-date")

    def test_older_than_wrong_format(self):
        """Test older_than with wrong but valid-looking timestamp format."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="Invalid timestamp format"):
            prune.filter_snapshots_to_delete(
                snapshots, "older_than", timestamp="01/01/2024 00:00:00"
            )

    def test_specific_snapshot_found(self):
        """Test specific strategy when snapshot exists."""
        snapshots = [
            {"label": "backup1", "finished_at": "2024-01-01 00:00:00"},
            {"label": "backup2", "finished_at": "2024-01-02 00:00:00"},
            {"label": "backup3", "finished_at": "2024-01-03 00:00:00"},
        ]

        result = prune.filter_snapshots_to_delete(snapshots, "specific", snapshot="backup2")

        assert len(result) == 1
        assert result[0]["label"] == "backup2"

    def test_specific_snapshot_not_found(self):
        """Test specific strategy when snapshot doesn't exist."""
        snapshots = [
            {"label": "backup1", "finished_at": "2024-01-01 00:00:00"},
            {"label": "backup2", "finished_at": "2024-01-02 00:00:00"},
        ]

        result = prune.filter_snapshots_to_delete(snapshots, "specific", snapshot="nonexistent")

        assert result == []

    def test_specific_missing_snapshot_name(self):
        """Test specific strategy with missing snapshot name."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="requires a snapshot name"):
            prune.filter_snapshots_to_delete(snapshots, "specific")

    def test_multiple_snapshots_found(self):
        """Test multiple strategy with all snapshots found."""
        snapshots = [
            {"label": "backup1", "finished_at": "2024-01-01 00:00:00"},
            {"label": "backup2", "finished_at": "2024-01-02 00:00:00"},
            {"label": "backup3", "finished_at": "2024-01-03 00:00:00"},
            {"label": "backup4", "finished_at": "2024-01-04 00:00:00"},
        ]

        result = prune.filter_snapshots_to_delete(
            snapshots, "multiple", snapshots=["backup1", "backup3"]
        )

        assert len(result) == 2
        assert result[0]["label"] == "backup1"
        assert result[1]["label"] == "backup3"

    def test_multiple_snapshots_partial_match(self):
        """Test multiple strategy with some snapshots not found."""
        snapshots = [
            {"label": "backup1", "finished_at": "2024-01-01 00:00:00"},
            {"label": "backup2", "finished_at": "2024-01-02 00:00:00"},
        ]

        result = prune.filter_snapshots_to_delete(
            snapshots, "multiple", snapshots=["backup1", "nonexistent", "backup2"]
        )

        assert len(result) == 2
        assert result[0]["label"] == "backup1"
        assert result[1]["label"] == "backup2"

    def test_multiple_missing_snapshots_list(self):
        """Test multiple strategy with missing snapshots parameter."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="requires a list of snapshot names"):
            prune.filter_snapshots_to_delete(snapshots, "multiple")

    def test_multiple_empty_snapshots_list(self):
        """Test multiple strategy with empty snapshots list."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="requires a list of snapshot names"):
            prune.filter_snapshots_to_delete(snapshots, "multiple", snapshots=[])

    def test_unknown_strategy(self):
        """Test with unknown pruning strategy."""
        snapshots = [{"label": "backup1", "finished_at": "2024-01-01 00:00:00"}]

        with pytest.raises(ValueError, match="Unknown pruning strategy"):
            prune.filter_snapshots_to_delete(snapshots, "invalid_strategy")


class TestVerifySnapshotExists:
    """Unit tests for verify_snapshot_exists function."""

    def test_snapshot_exists(self, mocker):
        """Test when snapshot exists in repository."""
        mock_db = mocker.Mock()
        mock_db.query.return_value = [["snapshot_data"]]

        result = prune.verify_snapshot_exists(mock_db, "test_repo", "backup1")

        assert result is True
        query_sql = mock_db.query.call_args[0][0]
        assert "SHOW SNAPSHOT" in query_sql
        assert "test_repo" in query_sql
        assert "backup1" in query_sql

    def test_snapshot_not_found(self, mocker):
        """Test when snapshot doesn't exist in repository."""
        mock_db = mocker.Mock()
        mock_db.query.return_value = []

        with pytest.raises(Exception, match="not found"):
            prune.verify_snapshot_exists(mock_db, "test_repo", "nonexistent")

    def test_snapshot_query_error(self, mocker):
        """Test when query fails."""
        mock_db = mocker.Mock()
        mock_db.query.side_effect = Exception("Database error")

        with pytest.raises(Exception, match="Database error"):
            prune.verify_snapshot_exists(mock_db, "test_repo", "backup1")


class TestExecuteDropSnapshot:
    """Unit tests for execute_drop_snapshot function."""

    def test_drop_snapshot_success(self, mocker):
        """Test successful snapshot deletion."""
        mock_db = mocker.Mock()

        prune.execute_drop_snapshot(mock_db, "test_repo", "backup1")

        mock_db.execute.assert_called_once()
        sql = mock_db.execute.call_args[0][0]
        assert "DROP SNAPSHOT" in sql
        assert "test_repo" in sql
        assert "backup1" in sql

    def test_drop_snapshot_failure(self, mocker):
        """Test snapshot deletion failure."""
        mock_db = mocker.Mock()
        mock_db.execute.side_effect = Exception("Drop failed")

        with pytest.raises(Exception, match="Drop failed"):
            prune.execute_drop_snapshot(mock_db, "test_repo", "backup1")


class TestCleanupBackupHistory:
    """Unit tests for cleanup_backup_history function."""

    def test_cleanup_success(self, mocker):
        """Test successful backup history cleanup."""
        mock_db = mocker.Mock()

        prune.cleanup_backup_history(mock_db, "backup1")

        assert mock_db.execute.call_count == 2

        calls = [call[0][0] for call in mock_db.execute.call_args_list]
        assert any("backup_partitions" in call and "backup1" in call for call in calls)
        assert any("backup_history" in call and "backup1" in call for call in calls)

    def test_cleanup_custom_ops_database(self, mocker):
        """Test cleanup with custom ops database."""
        mock_db = mocker.Mock()

        prune.cleanup_backup_history(mock_db, "backup1", ops_database="custom_ops")

        calls = [call[0][0] for call in mock_db.execute.call_args_list]
        assert all("custom_ops" in call for call in calls)

    def test_cleanup_failure(self, mocker):
        """Test cleanup when deletion fails (should not raise)."""
        mock_db = mocker.Mock()
        mock_db.execute.side_effect = Exception("Delete failed")

        prune.cleanup_backup_history(mock_db, "backup1")
