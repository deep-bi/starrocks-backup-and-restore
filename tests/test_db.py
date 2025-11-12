import os
import pytest
from starrocks_br import db


@pytest.fixture
def setup_password_env(monkeypatch):
    """Setup STARROCKS_PASSWORD environment variable for testing."""
    monkeypatch.setenv('STARROCKS_PASSWORD', 'test_password')


def test_should_create_database_connection_object(setup_password_env):
    conn = db.StarRocksDB(
        host='127.0.0.1',
        port=9030,
        user='root',
        password=os.getenv('STARROCKS_PASSWORD'),
        database='test_db'
    )
    
    assert conn.host == '127.0.0.1'
    assert conn.port == 9030
    assert conn.user == 'root'
    assert conn.database == 'test_db'


def test_should_execute_sql_statement(mocker, setup_password_env):
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    conn.execute("INSERT INTO test_table VALUES (1)")
    
    assert mock_cursor.execute.call_count == 1
    assert mock_connection.commit.call_count == 1


def test_should_query_and_return_results(mocker, setup_password_env):
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_cursor.fetchall.return_value = [('row1',), ('row2',)]
    mock_connection.cursor.return_value = mock_cursor
    
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    results = conn.query("SELECT * FROM test_table")
    
    assert len(results) == 2
    assert results[0] == ('row1',)
    assert results[1] == ('row2',)


def test_should_support_context_manager(mocker, setup_password_env):
    mock_connection = mocker.Mock()
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    with conn as db_conn:
        assert db_conn is conn
    
    assert mock_connection.close.call_count == 1


def test_should_pass_tls_arguments_when_tls_enabled(mocker, setup_password_env):
    mock_connect = mocker.patch('mysql.connector.connect')
    tls_config = {
        'enabled': True,
        'ca_cert': '/path/to/ca.pem',
    }

    conn = db.StarRocksDB(
        'localhost',
        9030,
        'root',
        os.getenv('STARROCKS_PASSWORD'),
        'test_db',
        tls_config=tls_config,
    )

    conn.connect()

    called_kwargs = mock_connect.call_args.kwargs
    assert called_kwargs['ssl_ca'] == '/path/to/ca.pem'
    assert called_kwargs['ssl_verify_cert'] is True
    assert called_kwargs['tls_versions'] == ['TLSv1.2', 'TLSv1.3']
    assert 'ssl_cert' not in called_kwargs
    assert 'ssl_key' not in called_kwargs


def test_should_include_client_credentials_when_mutual_tls_enabled(mocker, setup_password_env):
    mock_connect = mocker.patch('mysql.connector.connect')
    tls_config = {
        'enabled': True,
        'ca_cert': '/path/to/ca.pem',
        'client_cert': '/path/to/client.pem',
        'client_key': '/path/to/client.key',
        'verify_server_cert': False,
        'tls_versions': ['TLSv1.3'],
    }

    conn = db.StarRocksDB(
        'localhost',
        9030,
        'root',
        os.getenv('STARROCKS_PASSWORD'),
        'test_db',
        tls_config=tls_config,
    )

    conn.connect()

    called_kwargs = mock_connect.call_args.kwargs
    assert called_kwargs['ssl_ca'] == '/path/to/ca.pem'
    assert called_kwargs['ssl_cert'] == '/path/to/client.pem'
    assert called_kwargs['ssl_key'] == '/path/to/client.key'
    assert called_kwargs['ssl_verify_cert'] is False
    assert called_kwargs['tls_versions'] == ['TLSv1.3']


def test_should_skip_tls_arguments_when_tls_disabled(mocker, setup_password_env):
    mock_connect = mocker.patch('mysql.connector.connect')

    conn = db.StarRocksDB(
        'localhost',
        9030,
        'root',
        os.getenv('STARROCKS_PASSWORD'),
        'test_db',
        tls_config={'enabled': False},
    )

    conn.connect()

    called_kwargs = mock_connect.call_args.kwargs
    assert 'ssl_ca' not in called_kwargs
    assert 'ssl_cert' not in called_kwargs
    assert 'ssl_key' not in called_kwargs
    assert 'tls_versions' not in called_kwargs
    assert 'ssl_verify_cert' not in called_kwargs


def test_should_query_and_cache_timezone(mocker, setup_password_env):
    """Test that timezone property queries and caches the cluster timezone."""
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_cursor.fetchall.return_value = [('time_zone', 'Asia/Shanghai')]
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    tz1 = conn.timezone
    tz2 = conn.timezone
    
    assert tz1 == 'Asia/Shanghai'
    assert tz2 == 'Asia/Shanghai'
    assert mock_cursor.execute.call_count == 1
    assert mock_cursor.fetchall.call_count == 1


def test_should_cache_timezone_after_first_query(mocker, setup_password_env):
    """Test that timezone is cached after first query."""
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_cursor.fetchall.return_value = [('time_zone', 'UTC')]
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    for _ in range(5):
        _ = conn.timezone
    
    assert mock_cursor.execute.call_count == 1


def test_should_handle_empty_timezone_query_result(mocker, setup_password_env):
    """Test that timezone defaults to UTC when query returns no results."""
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_cursor.fetchall.return_value = []
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    tz = conn.timezone
    
    assert tz == 'UTC'


def test_should_handle_dict_result_from_timezone_query(mocker, setup_password_env):
    """Test that timezone property handles dict results from query."""
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_cursor.fetchall.return_value = [{'Variable_name': 'time_zone', 'Value': 'America/New_York'}]
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    tz = conn.timezone
    
    assert tz == 'America/New_York'


def test_should_handle_offset_timezone_string(mocker, setup_password_env):
    """Test that timezone property handles offset timezone strings."""
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_cursor.fetchall.return_value = [('time_zone', '+08:00')]
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    tz = conn.timezone
    
    assert tz == '+08:00'


def test_should_default_to_utc_when_timezone_query_fails(mocker, setup_password_env):
    """Test that timezone property defaults to UTC when query raises an exception."""
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_cursor.fetchall.side_effect = Exception("Database connection error")
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    tz = conn.timezone
    
    assert tz == "UTC"


def test_should_default_to_utc_when_timezone_query_raises_permission_error(mocker, setup_password_env):
    """Test that timezone property defaults to UTC when query raises permission error."""
    conn = db.StarRocksDB('localhost', 9030, 'root', os.getenv('STARROCKS_PASSWORD'), 'test_db')
    
    mock_connection = mocker.Mock()
    mock_cursor = mocker.Mock()
    mock_cursor.fetchall.side_effect = Exception("Access denied")
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch('mysql.connector.connect', return_value=mock_connection)
    
    tz = conn.timezone
    
    assert tz == "UTC"

