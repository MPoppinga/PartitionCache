from unittest.mock import MagicMock, patch

import pytest
from bitarray import bitarray
from psycopg import sql as psycopg_sql
from pyroaring import BitMap

from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler


def _get_sql_calls(mock_cursor):
    """Extract only psycopg sql.SQL/Composed calls (not raw string SQL file loads).

    Returns a list of (sql_string, call) tuples for calls using psycopg sql objects.
    """
    results = []
    for call in mock_cursor.execute.call_args_list:
        arg = call[0][0]
        if isinstance(arg, psycopg_sql.SQL | psycopg_sql.Composed):
            results.append((str(arg), call))
    return results


class TestPostgreSQLRoaringBitCacheHandler:
    """Test cases for PostgreSQL Roaring Bitmap Cache Handler."""

    @pytest.fixture
    def mock_db_config(self):
        """Mock database configuration."""
        return {
            "db_name": "test_db",
            "db_host": "localhost",
            "db_user": "test_user",
            "db_password": "test_password",
            "db_port": "5432",
            "db_tableprefix": "test_roaringbit_cache",
        }

    @pytest.fixture
    def mock_cursor(self):
        """Mock cursor for database operations."""
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        cursor.fetchall.return_value = []
        return cursor

    @pytest.fixture
    def mock_db(self, mock_cursor):
        """Mock database connection."""
        db = MagicMock()
        db.cursor.return_value = mock_cursor
        return db

    @pytest.fixture
    def cache_handler(self, mock_db_config, mock_db, mock_cursor):
        """Create a cache handler instance with mocked database."""
        with patch("psycopg.connect", return_value=mock_db):
            # Simulate that the roaringbitmap extension exists
            mock_cursor.fetchone.return_value = (1,)
            handler = PostgreSQLRoaringBitCacheHandler(**mock_db_config)
            handler.cursor = mock_cursor
            handler.db = mock_db
            # Reset mock after initialization to have a clean slate for tests
            mock_cursor.reset_mock()
            return handler


    def test_set_cache_with_integer_set(self, cache_handler, mock_cursor):
        """Test setting a set of integers."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        test_values = {1, 2, 3, 10, 100}
        result = cache_handler.set_cache("test_key", test_values, "test_partition")

        assert result is True

        # Filter to only psycopg sql.SQL/Composed calls (excludes raw SQL file loads)
        sql_calls = _get_sql_calls(mock_cursor)

        # Verify that an INSERT into the correct cache table was called with rb_build
        insert_cache_calls = [(s, c) for s, c in sql_calls if "INSERT INTO" in s and "rb_build" in s]
        assert len(insert_cache_calls) == 1, "Expected exactly one INSERT with rb_build for the cache table"
        insert_sql_str, insert_cache_call = insert_cache_calls[0]
        assert "test_roaringbit_cache_cache_test_partition" in insert_sql_str, (
            f"INSERT should target test_roaringbit_cache_cache_test_partition, got: {insert_sql_str}"
        )
        # Verify the key is passed as the first parameter
        assert insert_cache_call[0][1][0] == "test_key", (
            f"First parameter should be 'test_key', got: {insert_cache_call[0][1][0]}"
        )

        # Verify that an INSERT into the queries table was also called
        insert_queries_calls = [(s, c) for s, c in sql_calls if "INSERT INTO" in s and "queries" in s]
        assert len(insert_queries_calls) == 1, "Expected exactly one INSERT for the queries table"

    def test_set_cache_with_string_integers(self, cache_handler, mock_cursor):
        """Test setting a set of string integers."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        test_values = {"1", "2", "3", "10", "100"}
        result = cache_handler.set_cache("test_key", test_values, "test_partition")

        assert result is True

    def test_set_cache_with_roaringbitmap(self, cache_handler, mock_cursor):
        """Test setting a roaring bitmap directly."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        rb = BitMap([1, 2, 3, 10, 100])

        result = cache_handler.set_cache("test_key", rb, "test_partition")

        assert result is True

    def test_set_cache_with_bitarray(self, cache_handler, mock_cursor):
        """Test setting from a bitarray."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        ba = bitarray(1000)
        ba.setall(0)
        ba[1] = 1
        ba[2] = 1
        ba[3] = 1
        ba[10] = 1
        ba[100] = 1

        result = cache_handler.set_cache("test_key", ba, "test_partition")

        assert result is True

    def test_set_cache_with_list(self, cache_handler, mock_cursor):
        """Test setting from a list."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        test_values = [1, 2, 3, 10, 100]
        result = cache_handler.set_cache("test_key", test_values, "test_partition")

        assert result is True

    def test_set_cache_invalid_datatype(self, cache_handler, mock_cursor):
        """Test setting with invalid datatype raises error."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        test_values = {1.5, 2.7, 3.14}  # Floats should fail

        with pytest.raises(ValueError, match="Only integer values are supported"):
            cache_handler.set_cache("test_key", test_values, "test_partition")

    def test_set_cache_unsupported_value_type(self, cache_handler, mock_cursor):
        """Test setting with unsupported value type."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        with pytest.raises(ValueError, match="Unsupported partition key identifier type"):
            cache_handler.set_cache("test_key", "not_a_valid_type", "test_partition")

    def test_get_existing_key(self, cache_handler, mock_cursor):
        """Test getting an existing key."""
        # Create a real roaring bitmap to get proper serialized data
        rb = BitMap([1, 2, 3, 10, 100])
        serialized_rb = rb.serialize()

        # Mock the _get_partition_datatype method directly
        cache_handler._get_partition_datatype = MagicMock(return_value="integer")

        # Mock the cursor fetchone to return the serialized roaring bitmap
        mock_cursor.fetchone.return_value = (serialized_rb,)

        result = cache_handler.get("test_key", "test_partition")

        assert isinstance(result, BitMap)
        assert set(result) == {1, 2, 3, 10, 100}

    def test_get_nonexistent_key(self, cache_handler, mock_cursor):
        """Test getting a non-existent key."""
        # Mock the _get_partition_datatype method directly
        cache_handler._get_partition_datatype = MagicMock(return_value="integer")

        # Mock that the key doesn't exist
        mock_cursor.fetchone.return_value = None

        result = cache_handler.get("nonexistent_key", "test_partition")
        assert result is None

    def test_get_nonexistent_partition(self, cache_handler, mock_cursor):
        """Test getting from non-existent partition."""
        # Mock partition doesn't exist
        mock_cursor.fetchone.return_value = None

        result = cache_handler.get("test_key", "nonexistent_partition")
        assert result is None

    def test_get_intersected(self, cache_handler, mock_cursor):
        """Test getting intersection of multiple keys."""
        # Create test roaring bitmaps
        rb1 = BitMap([1, 2, 3, 4, 5])
        rb2 = BitMap([3, 4, 5, 6, 7])

        # Expected intersection
        rb_intersection = rb1 & rb2

        # Mock the _get_partition_datatype method directly
        cache_handler._get_partition_datatype = MagicMock(return_value="integer")

        # Mock that both keys exist in the fetchall call
        mock_cursor.fetchall.return_value = [("key1",), ("key2",)]

        # Mock the intersection query result with proper serialized data
        mock_cursor.fetchone.return_value = (rb_intersection.serialize(),)

        result, count = cache_handler.get_intersected({"key1", "key2"}, "test_partition")

        assert count == 2
        assert isinstance(result, BitMap)
        assert result == rb_intersection

    def test_get_intersected_no_keys(self, cache_handler, mock_cursor):
        """Test intersection with no existing keys."""
        mock_cursor.fetchone.return_value = ("integer",)
        mock_cursor.fetchall.return_value = []  # No keys exist

        result, count = cache_handler.get_intersected({"key1", "key2"}, "test_partition")

        assert result is None
        assert count == 0

    def test_register_partition_key_valid(self, cache_handler, mock_cursor):
        """Test registering a valid partition key."""
        # Mock sequence: metadata check returns None (new partition), extension check returns 1
        mock_cursor.fetchone.side_effect = [None, (1,)]

        cache_handler.register_partition_key("new_partition", "integer")

        # register_partition_key delegates to _ensure_partition_table, which calls:
        # 1. _load_sql_functions (raw SQL file content, not a psycopg sql object)
        # 2. SELECT partitioncache_ensure_metadata_tables(...)
        # 3. SELECT partitioncache_bootstrap_partition(...)
        sql_calls = _get_sql_calls(mock_cursor)

        # Verify ensure_metadata_tables was called
        ensure_metadata_calls = [(s, c) for s, c in sql_calls if "partitioncache_ensure_metadata_tables" in s]
        assert len(ensure_metadata_calls) == 1, (
            f"Expected one ensure_metadata_tables call, got {len(ensure_metadata_calls)}"
        )

        # Verify bootstrap_partition was called with correct parameters
        bootstrap_calls = [(s, c) for s, c in sql_calls if "partitioncache_bootstrap_partition" in s]
        assert len(bootstrap_calls) == 1, "Expected one bootstrap_partition call"
        bootstrap_params = bootstrap_calls[0][1][0][1]
        assert bootstrap_params[1] == "new_partition", f"Bootstrap should reference 'new_partition', got: {bootstrap_params[1]}"
        assert bootstrap_params[2] == "integer", f"Bootstrap datatype should be 'integer', got: {bootstrap_params[2]}"
        assert bootstrap_params[3] == "roaringbit", f"Bootstrap handler type should be 'roaringbit', got: {bootstrap_params[3]}"

    def test_register_partition_key_invalid_datatype(self, cache_handler):
        """Test registering with invalid datatype raises error."""
        with pytest.raises(ValueError, match="PostgreSQL roaring bit handler supports only integer datatype"):
            cache_handler.register_partition_key("partition", "text")

    def test_get_datatype(self, cache_handler, mock_cursor):
        """Test getting datatype for a partition."""
        mock_cursor.fetchone.return_value = ("integer",)

        result = cache_handler.get_datatype("test_partition")
        assert result == "integer"

    def test_empty_value_handling(self, cache_handler, mock_cursor):
        """Test handling of empty values."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        # Test empty set
        result = cache_handler.set_cache("test_key", set(), "test_partition")
        assert result is True

        # Test empty list
        result = cache_handler.set_cache("test_key", [], "test_partition")
        assert result is True

    def test_database_error_handling(self, cache_handler, mock_cursor):
        """Test database error handling."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        # Mock database error
        mock_cursor.execute.side_effect = Exception("Database error")

        result = cache_handler.set_cache("test_key", {1, 2, 3}, "test_partition")
        assert result is False

    def test_create_partition_table(self, cache_handler, mock_cursor):
        """Test partition table creation."""
        # Mock extension check returns True
        mock_cursor.fetchone.return_value = (1,)

        cache_handler._create_partition_table("test_partition")

        sql_calls = [str(call[0][0]) for call in mock_cursor.execute.call_args_list]

        # Verify extension check was performed
        extension_checks = [s for s in sql_calls if "pg_extension" in s and "roaringbitmap" in s]
        assert len(extension_checks) == 1, "Should check for roaringbitmap extension"

        # Verify CREATE TABLE with roaringbitmap type and correct table name
        create_table_calls = [s for s in sql_calls if "CREATE TABLE" in s]
        assert len(create_table_calls) == 1, f"Expected exactly one CREATE TABLE call, got {len(create_table_calls)}"
        create_sql = create_table_calls[0]
        assert "roaringbitmap" in create_sql, "CREATE TABLE should use roaringbitmap type"
        assert "test_roaringbit_cache_cache_test_partition" in create_sql, (
            f"CREATE TABLE should target test_roaringbit_cache_cache_test_partition, got: {create_sql}"
        )

        # Verify metadata INSERT into partition_metadata table
        metadata_inserts = [s for s in sql_calls if "INSERT INTO" in s and "partition_metadata" in s]
        assert len(metadata_inserts) == 1, "Should insert into partition_metadata table"

    def test_ensure_partition_table_new_partition(self, cache_handler, mock_cursor):
        """Test ensuring partition table for new partition."""
        # Mock sequence: metadata check returns None (new partition), extension check returns 1
        mock_cursor.fetchone.side_effect = [None, (1,)]

        cache_handler._ensure_partition_table("new_partition", "integer")

        # Filter to only psycopg sql.SQL/Composed calls (excludes raw SQL file loads)
        sql_calls = _get_sql_calls(mock_cursor)

        # Verify ensure_metadata_tables was called
        ensure_metadata_calls = [(s, c) for s, c in sql_calls if "partitioncache_ensure_metadata_tables" in s]
        assert len(ensure_metadata_calls) == 1, "Should call partitioncache_ensure_metadata_tables"

        # Verify bootstrap_partition was called with correct parameters
        bootstrap_calls = [(s, c) for s, c in sql_calls if "partitioncache_bootstrap_partition" in s]
        assert len(bootstrap_calls) == 1, "Should call partitioncache_bootstrap_partition"
        bootstrap_params = bootstrap_calls[0][1][0][1]
        assert bootstrap_params[1] == "new_partition", f"Should reference 'new_partition', got: {bootstrap_params[1]}"
        assert bootstrap_params[2] == "integer", f"Datatype should be 'integer', got: {bootstrap_params[2]}"

    def test_ensure_partition_table_existing_partition(self, cache_handler, mock_cursor):
        """Test ensuring partition table for existing partition."""
        # Mock partition exists
        mock_cursor.fetchone.return_value = ("integer",)

        # Reset call count to ignore __init__ calls
        mock_cursor.reset_mock()

        cache_handler._ensure_partition_table("existing_partition", "integer")

        # Filter to only psycopg sql.SQL/Composed calls (excludes raw SQL file loads)
        sql_calls = _get_sql_calls(mock_cursor)

        # Verify ensure_metadata_tables was called
        ensure_metadata_calls = [(s, c) for s, c in sql_calls if "partitioncache_ensure_metadata_tables" in s]
        assert len(ensure_metadata_calls) == 1, "Should call partitioncache_ensure_metadata_tables"

        # Verify bootstrap_partition was called with correct parameters
        bootstrap_calls = [(s, c) for s, c in sql_calls if "partitioncache_bootstrap_partition" in s]
        assert len(bootstrap_calls) == 1, "Should call partitioncache_bootstrap_partition"
        bootstrap_params = bootstrap_calls[0][1][0][1]
        assert bootstrap_params[0] == "test_roaringbit_cache", f"Should use prefix 'test_roaringbit_cache', got: {bootstrap_params[0]}"
        assert bootstrap_params[1] == "existing_partition", f"Should reference 'existing_partition', got: {bootstrap_params[1]}"
        assert bootstrap_params[2] == "integer", f"Datatype should be 'integer', got: {bootstrap_params[2]}"
        assert bootstrap_params[3] == "roaringbit", f"Handler type should be 'roaringbit', got: {bootstrap_params[3]}"
