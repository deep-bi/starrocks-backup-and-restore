from starrocks_br import labels


def test_should_generate_basic_label_format():
    label = labels.generate_label("mydb", "2025-10-15", "incremental")
    assert label == "mydb_20251015_incremental"


def test_should_handle_label_collision_with_retry_suffix():
    existing_labels = ["mydb_20251015_incremental"]
    label = labels.generate_label("mydb", "2025-10-15", "incremental", existing_labels)
    assert label == "mydb_20251015_incremental_r1"


def test_should_handle_multiple_collisions():
    existing_labels = ["mydb_20251015_incremental", "mydb_20251015_incremental_r1"]
    label = labels.generate_label("mydb", "2025-10-15", "incremental", existing_labels)
    assert label == "mydb_20251015_incremental_r2"


def test_should_handle_different_backup_types():
    inc_label = labels.generate_label("mydb", "2025-10-15", "incremental")
    full_label = labels.generate_label("mydb", "2025-10-15", "full")

    assert inc_label == "mydb_20251015_incremental"
    assert full_label == "mydb_20251015_full"


def test_should_handle_empty_existing_labels():
    label = labels.generate_label("mydb", "2025-10-15", "incremental", [])
    assert label == "mydb_20251015_incremental"


def test_should_handle_different_database_names():
    label1 = labels.generate_label("db1", "2025-10-15", "incremental")
    label2 = labels.generate_label("db2", "2025-10-15", "incremental")
    
    assert label1 == "db1_20251015_incremental"
    assert label2 == "db2_20251015_incremental"


def test_should_resolve_label_without_placeholder_returns_same():
    db = None
    assert labels.resolve_label(db, "my-backup-name") == "my-backup-name"


def test_should_resolve_label_none_returns_none():
    db = None
    assert labels.resolve_label(db, None) is None


def test_should_resolve_versioned_label_first_time(mocker):
    db = mocker.Mock()
    db.query.return_value = []
    result = labels.resolve_label(db, "release-backup-v#r")
    assert result == "release-backup-v1"


def test_should_resolve_versioned_label_increment(mocker):
    db = mocker.Mock()
    db.query.return_value = [("release-backup-v1",), ("release-backup-v3",)]
    result = labels.resolve_label(db, "release-backup-v#r")
    assert result == "release-backup-v4"
