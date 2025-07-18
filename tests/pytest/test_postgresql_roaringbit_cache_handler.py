from unittest.mock import MagicMock, patch

import pytest
from bitarray import bitarray
from pyroaring import BitMap

from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler


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

    def test_repr(self, cache_handler):
        """Test string representation."""
        assert repr(cache_handler) == "postgresql_roaringbit"

    def test_get_supported_datatypes(self):
        """Test supported datatypes."""
        supported = PostgreSQLRoaringBitCacheHandler.get_supported_datatypes()
        assert supported == {"integer"}

    def test_set_cache_with_integer_set(self, cache_handler, mock_cursor):
        """Test setting a set of integers."""
        # Mock partition datatype exists
        mock_cursor.fetchone.return_value = ("integer",)

        test_values = {1, 2, 3, 10, 100}
        result = cache_handler.set_cache("test_key", test_values, "test_partition")

        assert result is True

        # Verify the INSERT was called
        assert mock_cursor.execute.call_count >= 1

        # Check that a roaring bitmap was created and serialized
        insert_call = None
        for call in mock_cursor.execute.call_args_list:
            if "INSERT INTO" in str(call[0][0]):
                insert_call = call
                break

        assert insert_call is not None
        # Check that test_key is in the parameters (second element of the call)
        if len(insert_call) > 1 and len(insert_call[1]) > 0:
            assert "test_key" in str(insert_call[1][0])

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

        # Should have called table creation and metadata insertion
        assert mock_cursor.execute.call_count >= 3

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
        cache_handler._create_partition_table("test_partition")

        # Should have called CREATE TABLE and INSERT
        assert mock_cursor.execute.call_count >= 2

        # Check that roaringbitmap type is used
        create_table_call = None
        for call in mock_cursor.execute.call_args_list:
            if "CREATE TABLE" in str(call[0][0]) and "roaringbitmap" in str(call[0][0]):
                create_table_call = call
                break

        assert create_table_call is not None

    def test_ensure_partition_table_new_partition(self, cache_handler, mock_cursor):
        """Test ensuring partition table for new partition."""
        # Mock sequence: metadata check returns None (new partition), extension check returns 1
        mock_cursor.fetchone.side_effect = [None, (1,)]

        cache_handler._ensure_partition_table("new_partition", "integer")

        # Should have created the table (metadata check + extension check + create table)
        assert mock_cursor.execute.call_count >= 3

    def test_ensure_partition_table_existing_partition(self, cache_handler, mock_cursor):
        """Test ensuring partition table for existing partition."""
        # Mock partition exists
        mock_cursor.fetchone.return_value = ("integer",)

        # Reset call count to ignore __init__ calls
        mock_cursor.reset_mock()

        cache_handler._ensure_partition_table("existing_partition", "integer")

        # Should call SQL bootstrap functions
        assert mock_cursor.execute.call_count >= 1

        # Find the bootstrap function call
        bootstrap_call_found = False
        for call in mock_cursor.execute.call_args_list:
            call_sql = str(call[0][0])
            if "partitioncache_bootstrap_partition" in call_sql:
                bootstrap_call_found = True
                break

        assert bootstrap_call_found, "Should have called bootstrap function"
