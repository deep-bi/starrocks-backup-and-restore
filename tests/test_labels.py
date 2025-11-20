from datetime import datetime

from starrocks_br import labels


def test_should_generate_auto_label_with_no_conflicts(mocker):
    """Test auto-generated date-based labels with no existing conflicts."""
    db = mocker.Mock()
    db.query.return_value = []

    result = labels.determine_backup_label(db, "incremental", "mydb")

    today = datetime.now().strftime("%Y%m%d")
    expected = f"mydb_{today}_incremental"
    assert result == expected


def test_should_generate_auto_label_with_conflicts(mocker):
    """Test auto-generated labels when conflicts exist."""
    today = datetime.now().strftime("%Y%m%d")
    base_label = f"mydb_{today}_incremental"

    db = mocker.Mock()
    db.query.return_value = [(base_label,)]

    result = labels.determine_backup_label(db, "incremental", "mydb")

    expected = f"{base_label}_r1"
    assert result == expected


def test_should_generate_auto_label_with_multiple_conflicts(mocker):
    """Test auto-generated labels with multiple existing conflicts."""
    today = datetime.now().strftime("%Y%m%d")
    base_label = f"mydb_{today}_full"

    db = mocker.Mock()
    db.query.return_value = [(base_label,), (f"{base_label}_r1",), (f"{base_label}_r2",)]

    result = labels.determine_backup_label(db, "full", "mydb")

    expected = f"{base_label}_r3"
    assert result == expected


def test_should_handle_custom_label_with_no_conflicts(mocker):
    """Test custom named labels with no existing conflicts."""
    db = mocker.Mock()
    db.query.return_value = []

    result = labels.determine_backup_label(db, "incremental", "mydb", "my-custom-backup")

    assert result == "my-custom-backup"


def test_should_handle_custom_label_with_conflicts(mocker):
    """Test custom named labels when conflicts exist."""
    db = mocker.Mock()
    db.query.return_value = [("my-custom-backup",)]

    result = labels.determine_backup_label(db, "incremental", "mydb", "my-custom-backup")

    assert result == "my-custom-backup_r1"


def test_should_handle_custom_label_with_multiple_conflicts(mocker):
    """Test custom named labels with multiple existing conflicts."""
    db = mocker.Mock()
    db.query.return_value = [("release-backup",), ("release-backup_r1",), ("release-backup_r2",)]

    result = labels.determine_backup_label(db, "full", "mydb", "release-backup")

    assert result == "release-backup_r3"


def test_should_handle_different_backup_types(mocker):
    """Test that different backup types generate different labels."""
    db = mocker.Mock()
    db.query.return_value = []

    inc_result = labels.determine_backup_label(db, "incremental", "mydb")
    full_result = labels.determine_backup_label(db, "full", "mydb")

    today = datetime.now().strftime("%Y%m%d")
    assert inc_result == f"mydb_{today}_incremental"
    assert full_result == f"mydb_{today}_full"


def test_should_handle_different_database_names(mocker):
    """Test that different database names generate different labels."""
    db = mocker.Mock()
    db.query.return_value = []

    result1 = labels.determine_backup_label(db, "incremental", "db1")
    result2 = labels.determine_backup_label(db, "incremental", "db2")

    today = datetime.now().strftime("%Y%m%d")
    assert result1 == f"db1_{today}_incremental"
    assert result2 == f"db2_{today}_incremental"


def test_should_handle_none_custom_name(mocker):
    """Test that None custom_name generates auto label."""
    db = mocker.Mock()
    db.query.return_value = []

    result = labels.determine_backup_label(db, "full", "mydb", None)

    today = datetime.now().strftime("%Y%m%d")
    expected = f"mydb_{today}_full"
    assert result == expected


def test_should_handle_empty_custom_name(mocker):
    """Test that empty string custom_name generates auto label."""
    db = mocker.Mock()
    db.query.return_value = []

    result = labels.determine_backup_label(db, "full", "mydb", "")

    today = datetime.now().strftime("%Y%m%d")
    expected = f"mydb_{today}_full"
    assert result == expected


def test_should_handle_database_query_error(mocker):
    """Test behavior when database query fails."""
    db = mocker.Mock()
    db.query.side_effect = Exception("Database connection failed")

    result = labels.determine_backup_label(db, "incremental", "mydb", "my-backup")

    assert result == "my-backup"


def test_should_verify_database_query_parameters(mocker):
    """Test that the correct database query is made with proper parameters."""
    db = mocker.Mock()
    db.query.return_value = []

    labels.determine_backup_label(db, "full", "sales_db", "my-backup")

    db.query.assert_called_once()
    call_args = db.query.call_args
    assert "ops.backup_history" in call_args[0][0]
    assert call_args[0][1] == ("my-backup%",)
