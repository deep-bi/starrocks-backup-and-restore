import pytest

from starrocks_br.repository import ensure_repository


def test_should_raise_when_repository_not_found(mocker):
    """Test that ensure_repository raises an error when repository doesn't exist."""
    db = mocker.Mock()
    db.query.return_value = []

    with pytest.raises(RuntimeError) as err:
        ensure_repository(db, "missing_repo")

    assert "not found" in str(err.value).lower()
    assert "missing_repo" in str(err.value)
    assert db.query.call_count >= 1


def test_should_pass_when_repository_exists(mocker):
    """Test that ensure_repository succeeds when repository exists without errors."""
    db = mocker.Mock()
    db.query.return_value = [
        # | RepoId | RepoName | CreateTime | IsReadOnly | Location | Broker | ErrMsg |
        (
            "34217",
            "minio_repo",
            "2025-10-16 19:00:05",
            "false",
            "s3://backups/starrocks/",
            "",
            "NULL",
        )
    ]

    # Should not raise
    ensure_repository(db, "minio_repo")

    assert db.query.call_count >= 1


def test_should_raise_when_repository_has_errors(mocker):
    """Test that ensure_repository raises an error when repository has error message."""
    db = mocker.Mock()
    db.query.return_value = [
        # | RepoId | RepoName | CreateTime | IsReadOnly | Location | Broker | ErrMsg |
        (
            "34217",
            "broken_repo",
            "2025-10-16 19:00:05",
            "false",
            "s3://backups/",
            "",
            "Connection failed: auth error",
        )
    ]

    with pytest.raises(RuntimeError) as err:
        ensure_repository(db, "broken_repo")

    assert "auth error" in str(err.value).lower()
    assert "broken_repo" in str(err.value)
