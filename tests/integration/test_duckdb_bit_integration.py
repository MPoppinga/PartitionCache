"""
Integration tests for DuckDB bit cache handler using actual DuckDB database.

Tests the complete functionality with real database operations.
"""
import os
import tempfile

import pytest

from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler


class TestDuckDBBitIntegration:
    """Integration tests with actual DuckDB database."""

    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database path."""
        # Create a temporary directory and database filename
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test.duckdb")
        yield db_path
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)

    @pytest.fixture
    def cache_handler(self, temp_db_path):
        """Create cache handler with temporary database."""
        handler = DuckDBBitCacheHandler(
            database=temp_db_path,
            table_prefix="test_cache",
            bitsize=10000
        )
        yield handler
        handler.close()

    def test_basic_cache_operations(self, cache_handler):
        """Test basic cache operations with real database."""
        # Register partition key
        cache_handler.register_partition_key("zipcode", "integer", bitsize=10000)

        # Set cache entry
        partition_keys = {1, 5, 10, 100, 1000}
        result = cache_handler.set_cache("test_hash_1", partition_keys, "zipcode")
        assert result is True

        # Get cache entry
        retrieved = cache_handler.get("test_hash_1", "zipcode")
        assert retrieved == partition_keys

        # Check exists
        assert cache_handler.exists("test_hash_1", "zipcode") is True
        assert cache_handler.exists("nonexistent", "zipcode") is False

        # Delete entry
        result = cache_handler.delete("test_hash_1", "zipcode")
        assert result is True

        # Verify deletion
        assert cache_handler.get("test_hash_1", "zipcode") is None
        assert cache_handler.exists("test_hash_1", "zipcode") is False

    def test_intersection_operations(self, cache_handler):
        """Test intersection operations with real database."""
        # Register partition key
        cache_handler.register_partition_key("zipcode", "integer", bitsize=1000)

        # Set multiple cache entries
        cache_handler.set_cache("hash1", {1, 2, 3, 4, 5}, "zipcode")
        cache_handler.set_cache("hash2", {3, 4, 5, 6, 7}, "zipcode")
        cache_handler.set_cache("hash3", {4, 5, 6, 7, 8}, "zipcode")

        # Test single key intersection
        result, count = cache_handler.get_intersected({"hash1"}, "zipcode")
        assert result == {1, 2, 3, 4, 5}
        assert count == 1

        # Test multiple key intersection
        result, count = cache_handler.get_intersected({"hash1", "hash2"}, "zipcode")
        assert result == {3, 4, 5}  # Common elements
        assert count == 2

        # Test three key intersection
        result, count = cache_handler.get_intersected({"hash1", "hash2", "hash3"}, "zipcode")
        assert result == {4, 5}  # Common to all three
        assert count == 3

        # Test with non-existent key
        result, count = cache_handler.get_intersected({"hash1", "nonexistent"}, "zipcode")
        assert result == {1, 2, 3, 4, 5}  # Only hash1 exists
        assert count == 1

    def test_query_metadata_operations(self, cache_handler):
        """Test query metadata storage and retrieval."""
        # Register partition key
        cache_handler.register_partition_key("zipcode", "integer")

        # Set query metadata
        query_text = "SELECT * FROM pois WHERE zipcode IN (12345, 54321)"
        result = cache_handler.set_query("query_hash_1", query_text, "zipcode")
        assert result is True

        # Get query metadata
        retrieved_query = cache_handler.get_query("query_hash_1", "zipcode")
        assert retrieved_query == query_text

        # Set query status
        result = cache_handler.set_query_status("query_hash_1", "zipcode", "timeout")
        assert result is True

        # Get query status
        status = cache_handler.get_query_status("query_hash_1", "zipcode")
        assert status == "timeout"

        # Test exists with query check
        assert cache_handler.exists("query_hash_1", "zipcode", check_query=True) is True

    def test_null_value_operations(self, cache_handler):
        """Test null value storage and checking."""
        # Register partition key
        cache_handler.register_partition_key("zipcode", "integer")

        # Set null value
        result = cache_handler.set_null("timeout_hash", "zipcode")
        assert result is True

        # Check if null
        assert cache_handler.is_null("timeout_hash", "zipcode") is True
        assert cache_handler.is_null("nonexistent", "zipcode") is False

        # Get should return None for null entries
        assert cache_handler.get("timeout_hash", "zipcode") is None

    def test_lazy_intersection_sql(self, cache_handler):
        """Test lazy intersection SQL generation."""
        # Register partition key
        cache_handler.register_partition_key("zipcode", "integer", bitsize=1000)

        # Set cache entries
        cache_handler.set_cache("hash1", {10, 20, 30}, "zipcode")
        cache_handler.set_cache("hash2", {20, 30, 40}, "zipcode")

        # Generate lazy intersection SQL
        sql_query, count = cache_handler.get_intersected_lazy({"hash1", "hash2"}, "zipcode")
        assert sql_query is not None
        assert count == 2
        assert "zipcode" in sql_query
        assert "test_cache_cache_zipcode" in sql_query

    def test_multiple_partitions(self, cache_handler):
        """Test operations with multiple partition keys."""
        # Register multiple partition keys
        cache_handler.register_partition_key("zipcode", "integer", bitsize=10000)
        cache_handler.register_partition_key("city_id", "integer", bitsize=5000)

        # Set data for different partitions
        cache_handler.set_cache("hash1", {1, 2, 3}, "zipcode")
        cache_handler.set_cache("hash1", {10, 20, 30}, "city_id")

        # Verify separate storage
        zipcode_result = cache_handler.get("hash1", "zipcode")
        city_result = cache_handler.get("hash1", "city_id")

        assert zipcode_result == {1, 2, 3}
        assert city_result == {10, 20, 30}

        # Check partition keys list
        partitions = cache_handler.get_partition_keys()
        partition_names = {p[0] for p in partitions}
        assert "zipcode" in partition_names
        assert "city_id" in partition_names

    def test_bitsize_constraints(self, cache_handler):
        """Test bitsize validation and constraints."""
        # Register with small bitsize
        cache_handler.register_partition_key("small", "integer", bitsize=100)

        # Values within bitsize should work
        result = cache_handler.set_cache("hash1", {1, 50, 99}, "small")
        assert result is True

        # Values at bitsize boundary should fail
        result = cache_handler.set_cache("hash2", {1, 50, 100}, "small")
        assert result is False  # 100 >= bitsize 100

        # Values exceeding bitsize should fail
        result = cache_handler.set_cache("hash3", {1, 50, 150}, "small")
        assert result is False  # 150 >= bitsize 100

    def test_high_level_entry_operations(self, cache_handler):
        """Test high-level set_entry method."""
        # Register partition key with larger bitsize to accommodate values
        cache_handler.register_partition_key("zipcode", "integer", bitsize=100000)

        # Use high-level entry method
        query_text = "SELECT count(*) FROM pois WHERE zipcode IN (12345, 54321)"
        partition_keys = {12345, 54321}

        result = cache_handler.set_entry(
            "combined_hash",
            partition_keys,
            query_text,
            "zipcode"
        )
        assert result is True

        # Verify both cache and query were set
        cached_keys = cache_handler.get("combined_hash", "zipcode")
        cached_query = cache_handler.get_query("combined_hash", "zipcode")

        assert cached_keys == partition_keys
        assert cached_query == query_text

    def test_error_conditions(self, cache_handler):
        """Test error handling and edge cases."""
        # Test with non-integer datatype (should raise error)
        with pytest.raises(ValueError):
            cache_handler.register_partition_key("text_key", "text")

        # Test empty partition keys
        cache_handler.register_partition_key("zipcode", "integer")
        result = cache_handler.set_cache("empty_hash", set(), "zipcode")
        assert result is True  # Empty sets should be allowed

        # Test string integers (should convert)
        result = cache_handler.set_cache("string_hash", {"1", "2", "3"}, "zipcode")
        assert result is True
        retrieved = cache_handler.get("string_hash", "zipcode")
        assert retrieved == {1, 2, 3}

        # Test invalid string (should fail)
        result = cache_handler.set_cache("invalid_hash", {"abc", "def"}, "zipcode")
        assert result is False

    def test_factory_integration(self):
        """Test integration with factory method."""
        # Set environment variables
        os.environ["DUCKDB_BIT_PATH"] = ":memory:"
        os.environ["DUCKDB_BIT_TABLE_PREFIX"] = "factory_test"
        os.environ["DUCKDB_BIT_BITSIZE"] = "5000"

        try:
            from partitioncache.cache_handler import get_cache_handler

            # Create handler via factory
            handler = get_cache_handler("duckdb_bit")

            # Verify it's the right type
            assert isinstance(handler, DuckDBBitCacheHandler)
            assert handler.table_prefix == "factory_test"
            assert handler.default_bitsize == 5000

            # Test basic functionality
            handler.register_partition_key("test", "integer")
            assert handler.set_cache("test_hash", {1, 2, 3}, "test") is True
            assert handler.get("test_hash", "test") == {1, 2, 3}

            handler.close()

        finally:
            # Cleanup environment
            for key in ["DUCKDB_BIT_PATH", "DUCKDB_BIT_TABLE_PREFIX", "DUCKDB_BIT_BITSIZE"]:
                os.environ.pop(key, None)

    def test_persistence(self, temp_db_path):
        """Test that data persists across handler instances."""
        # Create first handler instance
        handler1 = DuckDBBitCacheHandler(
            database=temp_db_path,
            table_prefix="persist_test",
            bitsize=1000
        )

        # Store some data
        handler1.register_partition_key("zipcode", "integer")
        handler1.set_cache("persist_hash", {1, 2, 3}, "zipcode")
        handler1.set_query("persist_hash", "SELECT * FROM test", "zipcode")
        handler1.close()

        # Create second handler instance with same database
        handler2 = DuckDBBitCacheHandler(
            database=temp_db_path,
            table_prefix="persist_test",
            bitsize=1000
        )

        # Verify data persists
        cached_data = handler2.get("persist_hash", "zipcode")
        cached_query = handler2.get_query("persist_hash", "zipcode")

        assert cached_data == {1, 2, 3}
        assert cached_query == "SELECT * FROM test"

        # Verify partition metadata persists
        partitions = handler2.get_partition_keys()
        assert len(partitions) > 0
        assert ("zipcode", "integer") in partitions

        handler2.close()
