"""
Comprehensive test suite for DuckDB bit cache handler.

Tests all required functionality for the DuckDB-based bit array cache implementation
following TDD principles.
"""

from unittest.mock import Mock, patch

import pytest

from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy


class TestDuckDBBitCacheHandler:
    """Test suite for DuckDB bit cache handler implementation."""

    @pytest.fixture
    def mock_duckdb_conn(self):
        """Mock DuckDB connection object."""
        conn = Mock()
        conn.execute = Mock()
        conn.fetchone = Mock()
        conn.fetchall = Mock()
        conn.commit = Mock()
        conn.rollback = Mock()
        conn.close = Mock()
        return conn

    @pytest.fixture
    def cache_handler(self, mock_duckdb_conn):
        """Create cache handler instance with mocked connection."""
        with patch("duckdb.connect", return_value=mock_duckdb_conn):
            # Import will happen once implemented
            from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler

            handler = DuckDBBitCacheHandler(database="test.duckdb", table_prefix="test_cache", bitsize=1000)
            handler.conn = mock_duckdb_conn
            return handler

    def test_inheritance(self, cache_handler):
        """Test that handler inherits from AbstractCacheHandler_Lazy."""
        assert isinstance(cache_handler, AbstractCacheHandler_Lazy)


    def test_supported_datatypes(self):
        """Test that only integer datatype is supported."""
        from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler

        supported = DuckDBBitCacheHandler.get_supported_datatypes()
        assert supported == {"integer"}
        assert DuckDBBitCacheHandler.supports_datatype("integer")
        assert not DuckDBBitCacheHandler.supports_datatype("text")
        assert not DuckDBBitCacheHandler.supports_datatype("float")

    def test_init_creates_tables(self, mock_duckdb_conn):
        """Test that initialization creates necessary tables."""
        with patch("duckdb.connect", return_value=mock_duckdb_conn):
            from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler

            DuckDBBitCacheHandler(database=":memory:", table_prefix="cache", bitsize=1000)

            # Should create metadata and queries tables
            calls = mock_duckdb_conn.execute.call_args_list
            create_table_calls = [call for call in calls if "CREATE TABLE" in str(call)]
            assert len(create_table_calls) >= 2  # metadata and queries tables

    def test_register_partition_key(self, cache_handler):
        """Test registering a partition key with specific bitsize."""
        cache_handler.register_partition_key("zipcode", "integer", bitsize=100000)

        # Should create partition table and insert metadata
        calls = cache_handler.conn.execute.call_args_list
        assert any("CREATE TABLE" in str(call) and "cache_zipcode" in str(call) for call in calls)
        assert any("INSERT INTO" in str(call) and "partition_metadata" in str(call) for call in calls)

    def test_register_partition_key_wrong_datatype(self, cache_handler):
        """Test that registering non-integer datatype raises error."""
        with pytest.raises(ValueError, match="only.*integer"):
            cache_handler.register_partition_key("name", "text")

    def test_set_cache_basic(self, cache_handler):
        """Test setting cache with integer partition keys."""
        # Mock different return values for different queries
        # First call gets bitsize, subsequent calls are for inserts
        cache_handler.conn.execute.return_value.fetchone.side_effect = [
            (10000,),  # bitsize query returns tuple with bitsize
            None,  # subsequent calls for inserts
        ]

        result = cache_handler.set_cache("hash123", {1, 5, 10, 100}, "zipcode")
        assert result is True

        # Check INSERT was called with bit array
        insert_calls = [call for call in cache_handler.conn.execute.call_args_list if "INSERT INTO" in str(call) and "cache_zipcode" in str(call)]
        assert len(insert_calls) > 0

    def test_set_cache_exceeds_bitsize(self, cache_handler):
        """Test that setting values exceeding bitsize fails."""
        # Mock partition with bitsize 100
        cache_handler.conn.execute.return_value.fetchone.return_value = (100,)

        result = cache_handler.set_cache("hash123", {1, 5, 150}, "zipcode")
        assert result is False  # Should fail as 150 > bitsize 100

    def test_set_cache_non_integer_values(self, cache_handler):
        """Test that non-integer values are rejected."""
        cache_handler.conn.execute.return_value.fetchone.return_value = (1000,)

        # String values that can't be converted to int
        result = cache_handler.set_cache("hash123", {"abc", "def"}, "zipcode")
        assert result is False

    def test_get_cache(self, cache_handler):
        """Test retrieving cache values."""
        # Mock DuckDB BITSTRING query results - return the bit positions
        cache_handler.conn.execute.return_value.fetchall.return_value = [
            (5,), (10,), (99,)  # Positions where bits are set
        ]

        result = cache_handler.get("hash123", "zipcode")
        assert result == {5, 10, 99}

    def test_get_cache_not_found(self, cache_handler):
        """Test retrieving non-existent cache entry."""
        cache_handler.conn.execute.return_value.fetchall.return_value = []

        result = cache_handler.get("nonexistent", "zipcode")
        assert result is None

    def test_get_cache_null_value(self, cache_handler):
        """Test retrieving null cache value."""
        cache_handler.conn.execute.return_value.fetchall.return_value = []

        result = cache_handler.get("null_hash", "zipcode")
        assert result is None

    def test_exists(self, cache_handler):
        """Test checking if cache entry exists."""
        # Mock that entry exists
        cache_handler.conn.execute.return_value.fetchone.return_value = (1,)

        assert cache_handler.exists("hash123", "zipcode") is True

        # Mock that entry doesn't exist
        cache_handler.conn.execute.return_value.fetchone.return_value = None
        assert cache_handler.exists("nonexistent", "zipcode") is False

    def test_exists_with_query_check(self, cache_handler):
        """Test exists with query metadata check."""
        # First check queries table, then cache table
        cache_handler.conn.execute.return_value.fetchone.side_effect = [
            ("ok",),  # Query status
            (1,),  # Cache exists
        ]

        assert cache_handler.exists("hash123", "zipcode", check_query=True) is True

    def test_delete(self, cache_handler):
        """Test deleting cache entry."""
        cache_handler.conn.execute.return_value.rowcount = 1

        result = cache_handler.delete("hash123", "zipcode")
        assert result is True

        # Check DELETE was called
        delete_calls = [call for call in cache_handler.conn.execute.call_args_list if "DELETE FROM" in str(call)]
        assert len(delete_calls) > 0

    def test_set_null(self, cache_handler):
        """Test setting null value for a key."""
        cache_handler.conn.execute.return_value.fetchone.return_value = ("integer", 1000)

        result = cache_handler.set_null("timeout_hash", "zipcode")
        assert result is True

        # Check INSERT with NULL value
        insert_calls = [call for call in cache_handler.conn.execute.call_args_list if "INSERT INTO" in str(call) and "NULL" in str(call)]
        assert len(insert_calls) > 0

    def test_is_null(self, cache_handler):
        """Test checking if value is null."""
        cache_handler.conn.execute.return_value.fetchone.return_value = (None,)
        assert cache_handler.is_null("timeout_hash", "zipcode") is True

        # Non-null value
        cache_handler.conn.execute.return_value.fetchone.return_value = ("0101010",)
        assert cache_handler.is_null("normal_hash", "zipcode") is False

    def test_get_intersected_single_key(self, cache_handler):
        """Test intersection with single key."""
        # Mock the methods that get_intersected calls
        cache_handler.filter_existing_keys = Mock(return_value={"hash123"})
        cache_handler.get = Mock(return_value={5, 10})

        result, count = cache_handler.get_intersected({"hash123"}, "zipcode")
        assert result == {5, 10}
        assert count == 1

    def test_get_intersected_multiple_keys(self, cache_handler):
        """Test intersection with multiple keys using DuckDB BIT_AND."""
        # Mock filter_existing_keys to return all keys
        cache_handler.filter_existing_keys = Mock(return_value={"hash1", "hash2", "hash3"})

        # Mock DuckDB BIT_AND aggregate result - return intersection positions
        cache_handler.conn.execute.return_value.fetchall.return_value = [
            (5,)  # Only position 5 is in intersection
        ]

        result, count = cache_handler.get_intersected({"hash1", "hash2", "hash3"}, "zipcode")
        assert result == {5}
        assert count == 3

    def test_get_intersected_no_keys_exist(self, cache_handler):
        """Test intersection when no keys exist."""
        cache_handler.filter_existing_keys = Mock(return_value=set())

        result, count = cache_handler.get_intersected({"hash1", "hash2"}, "zipcode")
        assert result is None
        assert count == 0

    def test_filter_existing_keys(self, cache_handler):
        """Test filtering keys that exist in cache."""
        # Mock query result
        cache_handler.conn.execute.return_value.fetchall.return_value = [("hash1",), ("hash3",)]

        result = cache_handler.filter_existing_keys({"hash1", "hash2", "hash3"}, "zipcode")
        assert result == {"hash1", "hash3"}

    def test_get_all_keys(self, cache_handler):
        """Test retrieving all keys for a partition."""
        cache_handler.conn.execute.return_value.fetchall.return_value = [("hash1",), ("hash2",), ("hash3",)]

        result = cache_handler.get_all_keys("zipcode")
        assert result == ["hash1", "hash2", "hash3"]

    def test_set_query(self, cache_handler):
        """Test storing query metadata."""
        query_text = "SELECT * FROM pois WHERE zipcode = 12345"

        result = cache_handler.set_query("hash123", query_text, "zipcode")
        assert result is True

        # Check INSERT was called
        insert_calls = [call for call in cache_handler.conn.execute.call_args_list if "INSERT INTO" in str(call) and "queries" in str(call)]
        assert len(insert_calls) > 0

    def test_get_query(self, cache_handler):
        """Test retrieving query text."""
        query_text = "SELECT * FROM pois WHERE zipcode = 12345"
        cache_handler.conn.execute.return_value.fetchone.return_value = (query_text,)

        result = cache_handler.get_query("hash123", "zipcode")
        assert result == query_text

    def test_get_all_queries(self, cache_handler):
        """Test retrieving all queries for a partition."""
        cache_handler.conn.execute.return_value.fetchall.return_value = [
            ("hash1", "SELECT 1"),
            ("hash2", "SELECT 2"),
        ]

        result = cache_handler.get_all_queries("zipcode")
        assert result == [("hash1", "SELECT 1"), ("hash2", "SELECT 2")]

    def test_set_query_status(self, cache_handler):
        """Test setting query status."""
        result = cache_handler.set_query_status("hash123", "zipcode", "timeout")
        assert result is True

        # Check UPDATE was called
        update_calls = [call for call in cache_handler.conn.execute.call_args_list if "UPDATE" in str(call)]
        assert len(update_calls) > 0

    def test_get_query_status(self, cache_handler):
        """Test retrieving query status."""
        cache_handler.conn.execute.return_value.fetchone.return_value = ("timeout",)

        result = cache_handler.get_query_status("hash123", "zipcode")
        assert result == "timeout"

    def test_get_datatype(self, cache_handler):
        """Test retrieving partition datatype."""
        cache_handler.conn.execute.return_value.fetchone.return_value = ("integer",)

        result = cache_handler.get_datatype("zipcode")
        assert result == "integer"

    def test_get_partition_keys(self, cache_handler):
        """Test retrieving all partition keys and datatypes."""
        cache_handler.conn.execute.return_value.fetchall.return_value = [
            ("zipcode", "integer"),
            ("city_id", "integer"),
        ]

        result = cache_handler.get_partition_keys()
        assert result == [("zipcode", "integer"), ("city_id", "integer")]

    def test_get_intersected_lazy(self, cache_handler):
        """Test lazy intersection SQL generation."""
        # Mock filter_existing_keys
        cache_handler.filter_existing_keys = Mock(return_value={"hash1", "hash2"})

        sql_query, count = cache_handler.get_intersected_lazy({"hash1", "hash2"}, "zipcode")

        assert sql_query is not None
        assert count == 2
        # Should generate SQL with bit operations and unnest
        assert "bit" in sql_query.lower()
        assert "unnest" in sql_query.lower()

    def test_close(self, cache_handler):
        """Test closing the connection."""
        cache_handler.close()
        cache_handler.conn.close.assert_called_once()

    def test_set_entry_high_level(self, cache_handler):
        """Test high-level set_entry method."""
        # Mock exists to return False
        cache_handler.exists = Mock(return_value=False)
        cache_handler.set_cache = Mock(return_value=True)
        cache_handler.set_query = Mock(return_value=True)

        result = cache_handler.set_entry("hash123", {1, 2, 3}, "SELECT * FROM test", "zipcode")

        assert result is True
        cache_handler.set_cache.assert_called_once_with("hash123", {1, 2, 3}, "zipcode")
        cache_handler.set_query.assert_called_once_with("hash123", "SELECT * FROM test", "zipcode")

    def test_error_handling(self, cache_handler):
        """Test error handling."""
        # Simulate database error
        cache_handler.conn.execute.side_effect = Exception("Database error")

        result = cache_handler.set_cache("hash123", {1, 2, 3}, "zipcode")
        assert result is False

        # DuckDB doesn't use rollbacks like PostgreSQL, so just verify error handling works

    def test_concurrent_partition_creation(self, cache_handler):
        """Test handling concurrent partition table creation."""
        # Mock _ensure_partition_table to return True (handles "already exists" gracefully)
        cache_handler._ensure_partition_table = Mock(return_value=True)

        # Should handle gracefully
        cache_handler.register_partition_key("zipcode", "integer", bitsize=10000)

        # Verify the method was called
        cache_handler._ensure_partition_table.assert_called_once_with("zipcode", 10000)

    def test_bitsize_validation(self, cache_handler):
        """Test bitsize constraints and validation."""
        # Test minimum bitsize
        cache_handler.register_partition_key("test1", "integer", bitsize=1)

        # Test large bitsize
        cache_handler.register_partition_key("test2", "integer", bitsize=1000000)

        # Bitsize should be stored in metadata
        calls = cache_handler.conn.execute.call_args_list
        assert any("bitsize" in str(call) for call in calls)
