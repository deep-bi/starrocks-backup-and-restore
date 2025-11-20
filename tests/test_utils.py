import pytest

from starrocks_br import utils


class TestQuoteIdentifier:
    """Tests for SQL identifier quoting (database, table, partition names)"""

    def test_should_wrap_simple_identifier_in_backticks(self):
        """Simple identifiers should be wrapped in backticks"""
        assert utils.quote_identifier("my_database") == "`my_database`"
        assert utils.quote_identifier("users") == "`users`"
        assert utils.quote_identifier("p20240101") == "`p20240101`"

    def test_should_handle_identifier_with_hyphens(self):
        """Identifiers with hyphens need backticks"""
        assert utils.quote_identifier("my-database") == "`my-database`"
        assert utils.quote_identifier("user-data") == "`user-data`"

    def test_should_handle_identifier_with_reserved_words(self):
        """SQL reserved words should be safely quoted"""
        assert utils.quote_identifier("select") == "`select`"
        assert utils.quote_identifier("from") == "`from`"
        assert utils.quote_identifier("where") == "`where`"
        assert utils.quote_identifier("user") == "`user`"
        assert utils.quote_identifier("order") == "`order`"

    def test_should_handle_identifier_with_spaces(self):
        """Identifiers with spaces (unusual but possible) should be quoted"""
        assert utils.quote_identifier("my database") == "`my database`"

    def test_should_escape_backticks_in_identifier(self):
        """If an identifier contains backticks, they should be escaped"""
        # StarRocks allows backticks to be escaped with double backticks
        assert utils.quote_identifier("my`database") == "`my``database`"
        assert utils.quote_identifier("db`with`ticks") == "`db``with``ticks`"

    def test_should_handle_empty_identifier(self):
        """Empty identifiers should be handled (though invalid in SQL)"""
        # This should either raise ValueError or return empty backticks
        with pytest.raises(ValueError, match="Identifier cannot be empty"):
            utils.quote_identifier("")

    def test_should_handle_none_identifier(self):
        """None should raise an error"""
        with pytest.raises(ValueError, match="Identifier cannot be None"):
            utils.quote_identifier(None)


class TestQuoteValue:
    """Tests for SQL string value escaping"""

    def test_should_wrap_simple_string_in_quotes(self):
        """Simple strings should be wrapped in single quotes"""
        assert utils.quote_value("test_label") == "'test_label'"
        assert utils.quote_value("backup_20240101") == "'backup_20240101'"

    def test_should_escape_single_quotes(self):
        """Single quotes in values should be escaped"""
        # SQL standard: escape ' with ''
        assert utils.quote_value("it's") == "'it''s'"
        assert utils.quote_value("O'Brien") == "'O''Brien'"

    def test_should_escape_backslashes(self):
        """Backslashes should be escaped"""
        # MySQL/StarRocks: escape \ with \\
        assert utils.quote_value("path\\to\\file") == "'path\\\\to\\\\file'"

    def test_should_handle_mixed_special_characters(self):
        """Handle strings with multiple special characters"""
        assert utils.quote_value("test'with\\chars") == "'test''with\\\\chars'"

    def test_should_handle_newlines_and_tabs(self):
        """Newlines and tabs should be escaped"""
        assert utils.quote_value("line1\nline2") == "'line1\\nline2'"
        assert utils.quote_value("col1\tcol2") == "'col1\\tcol2'"

    def test_should_handle_empty_string(self):
        """Empty string should return empty quoted string"""
        assert utils.quote_value("") == "''"

    def test_should_handle_none_value(self):
        """None should be converted to SQL NULL (unquoted)"""
        assert utils.quote_value(None) == "NULL"

    def test_should_prevent_sql_injection_attempts(self):
        """Common SQL injection patterns should be escaped"""
        # Test various injection attempts
        assert utils.quote_value("'; DROP TABLE users; --") == "'''; DROP TABLE users; --'"
        assert utils.quote_value("1' OR '1'='1") == "'1'' OR ''1''=''1'"
        assert utils.quote_value("admin'--") == "'admin''--'"


class TestBuildQualifiedTableName:
    """Tests for building fully qualified table names"""

    def test_should_build_qualified_table_name(self):
        """Should build database.table format with proper quoting"""
        result = utils.build_qualified_table_name("my_db", "my_table")
        assert result == "`my_db`.`my_table`"

    def test_should_handle_special_characters_in_both(self):
        """Should properly quote both database and table"""
        result = utils.build_qualified_table_name("my-db", "user-data")
        assert result == "`my-db`.`user-data`"

    def test_should_handle_reserved_words(self):
        """Should quote reserved words in database and table names"""
        result = utils.build_qualified_table_name("select", "from")
        assert result == "`select`.`from`"

    def test_should_raise_on_invalid_inputs(self):
        """Should raise error on empty or None inputs"""
        with pytest.raises(ValueError):
            utils.build_qualified_table_name("", "table")
        with pytest.raises(ValueError):
            utils.build_qualified_table_name("db", "")
        with pytest.raises(ValueError):
            utils.build_qualified_table_name(None, "table")
        with pytest.raises(ValueError):
            utils.build_qualified_table_name("db", None)
