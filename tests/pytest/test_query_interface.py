"""
Unit tests for cache handler query interface methods.
Tests get_query, get_all_queries, and set_query methods across all cache handlers.
"""

import shutil
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest

from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler


@pytest.fixture
def temp_db_path():
    """Create a temporary directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def mock_rocksdict():
    """Mock RocksDict instance for testing."""
    mock_rdict = MagicMock()
    mock_rdict.get.return_value = None
    mock_rdict.__contains__ = Mock(return_value=False)
    mock_rdict.__setitem__ = Mock()
    mock_rdict.__getitem__ = Mock()
    mock_rdict.__delitem__ = Mock()
    mock_rdict.keys.return_value = []
    mock_rdict.close.return_value = None
    return mock_rdict


@pytest.fixture
def rocksdict_handler(temp_db_path, mock_rocksdict):
    """Create RocksDictCacheHandler with mocked RocksDict."""
    with patch("partitioncache.cache_handler.rocks_dict.Rdict", return_value=mock_rocksdict):
        handler = RocksDictCacheHandler(temp_db_path)
        return handler


class TestQueryInterface:
    """Test suite for query interface methods across cache handlers."""

    def test_set_query_basic(self, rocksdict_handler):
        """Test basic set_query functionality."""
        query_hash = "abc123"
        query_text = "SELECT * FROM test WHERE id = 1"
        partition_key = "test_partition"

        result = rocksdict_handler.set_query(query_hash, query_text, partition_key)

        assert result is True
        # Verify the query was stored with correct key format
        rocksdict_handler.db.__setitem__.assert_called()

    def test_set_query_with_special_characters(self, rocksdict_handler):
        """Test set_query with special characters in query text."""
        query_hash = "def456"
        query_text = "SELECT * FROM test WHERE name = 'O''Brien' AND data->'key' = 'value'"
        partition_key = "test_partition"

        result = rocksdict_handler.set_query(query_hash, query_text, partition_key)

        assert result is True

    def test_get_query_existing(self, rocksdict_handler):
        """Test get_query for existing query."""
        query_hash = "abc123"
        query_text = "SELECT * FROM test WHERE id = 1"
        partition_key = "test_partition"

        # Mock the database to return the query data
        mock_query_data = {"query": query_text, "partition_key": partition_key, "last_seen": datetime.now().isoformat()}
        rocksdict_handler.db.get.return_value = mock_query_data

        result = rocksdict_handler.get_query(query_hash, partition_key)

        assert result == query_text
        expected_key = f"query:{partition_key}:{query_hash}"
        rocksdict_handler.db.get.assert_called_with(expected_key)

    def test_get_query_nonexistent(self, rocksdict_handler):
        """Test get_query for non-existent query."""
        query_hash = "nonexistent"
        partition_key = "test_partition"

        # Mock the database to return None
        rocksdict_handler.db.get.return_value = None

        result = rocksdict_handler.get_query(query_hash, partition_key)

        assert result is None

    def test_get_query_invalid_data(self, rocksdict_handler):
        """Test get_query with invalid/corrupted data."""
        query_hash = "corrupted"
        partition_key = "test_partition"

        # Mock the database to return invalid data
        rocksdict_handler.db.get.return_value = "invalid_data"

        result = rocksdict_handler.get_query(query_hash, partition_key)

        assert result is None

    def test_get_all_queries_multiple(self, rocksdict_handler):
        """Test get_all_queries with multiple queries."""
        partition_key = "test_partition"

        # Mock database keys and query data
        mock_keys = [
            f"query:{partition_key}:hash1",
            f"query:{partition_key}:hash2",
            f"cache:{partition_key}:some_cache_key",  # Should be ignored
            "query:other_partition:hash3",  # Should be ignored
            f"query:{partition_key}:hash4",
        ]

        mock_query_data = {
            f"query:{partition_key}:hash1": {"query": "SELECT * FROM test WHERE id = 1", "partition_key": partition_key},
            f"query:{partition_key}:hash2": {"query": "SELECT * FROM test WHERE id = 2", "partition_key": partition_key},
            f"query:{partition_key}:hash4": {"query": "SELECT * FROM test WHERE id = 4", "partition_key": partition_key},
        }

        def mock_get(key):
            return mock_query_data.get(key)

        rocksdict_handler.db.keys.return_value = mock_keys
        rocksdict_handler.db.get.side_effect = mock_get

        result = rocksdict_handler.get_all_queries(partition_key)

        # Should return 3 queries, excluding cache keys and other partitions
        assert len(result) == 3

        # Verify content
        result_dict = dict(result)
        assert result_dict["hash1"] == "SELECT * FROM test WHERE id = 1"
        assert result_dict["hash2"] == "SELECT * FROM test WHERE id = 2"
        assert result_dict["hash4"] == "SELECT * FROM test WHERE id = 4"

    def test_get_all_queries_empty(self, rocksdict_handler):
        """Test get_all_queries with no queries."""
        partition_key = "empty_partition"

        # Mock no matching keys
        rocksdict_handler.db.keys.return_value = []

        result = rocksdict_handler.get_all_queries(partition_key)

        assert result == []

    def test_get_all_queries_with_corrupted_data(self, rocksdict_handler):
        """Test get_all_queries handles corrupted data gracefully."""
        partition_key = "test_partition"

        mock_keys = [
            f"query:{partition_key}:hash1",
            f"query:{partition_key}:hash2",
        ]

        def mock_get(key):
            if key.endswith("hash1"):
                return {"query": "Valid query"}
            elif key.endswith("hash2"):
                return "corrupted_data"  # Invalid format
            return None

        rocksdict_handler.db.keys.return_value = mock_keys
        rocksdict_handler.db.get.side_effect = mock_get

        result = rocksdict_handler.get_all_queries(partition_key)

        # Should only return the valid query
        assert len(result) == 1
        assert result[0] == ("hash1", "Valid query")

    def test_query_lifecycle(self, rocksdict_handler):
        """Test complete query lifecycle: set, get, get_all."""
        partition_key = "lifecycle_test"

        # Test data
        queries = [
            ("hash1", "SELECT * FROM test WHERE id = 1"),
            ("hash2", "SELECT * FROM test WHERE id = 2"),
            ("hash3", "SELECT * FROM products WHERE category = 'electronics'"),
        ]

        # Set up mock to track stored data
        stored_data = {}

        def mock_setitem(key, value):
            stored_data[key] = value

        def mock_get(key):
            return stored_data.get(key)

        def mock_keys():
            return list(stored_data.keys())

        rocksdict_handler.db.__setitem__.side_effect = mock_setitem
        rocksdict_handler.db.get.side_effect = mock_get
        rocksdict_handler.db.keys.side_effect = mock_keys

        # Step 1: Set queries
        for query_hash, query_text in queries:
            result = rocksdict_handler.set_query(query_hash, query_text, partition_key)
            assert result is True

        # Step 2: Get individual queries
        for query_hash, expected_text in queries:
            result = rocksdict_handler.get_query(query_hash, partition_key)
            assert result == expected_text

        # Step 3: Get all queries
        all_queries = rocksdict_handler.get_all_queries(partition_key)
        assert len(all_queries) == 3

        # Verify all queries are present
        result_dict = dict(all_queries)
        for query_hash, expected_text in queries:
            assert result_dict[query_hash] == expected_text

    def test_query_interface_default_partition_key(self, rocksdict_handler):
        """Test query interface methods with default partition key."""
        query_hash = "default_test"
        query_text = "SELECT COUNT(*) FROM users"

        # Test with default partition key
        result = rocksdict_handler.set_query(query_hash, query_text)
        assert result is True

        # Mock return for get
        mock_data = {"query": query_text, "partition_key": "partition_key"}
        rocksdict_handler.db.get.return_value = mock_data

        result = rocksdict_handler.get_query(query_hash)
        assert result == query_text

        # Verify default partition key was used
        expected_key = f"query:partition_key:{query_hash}"
        rocksdict_handler.db.get.assert_called_with(expected_key)


class TestQueryInterfaceErrorHandling:
    """Test error handling in query interface methods."""

    def test_set_query_exception_handling(self, rocksdict_handler):
        """Test set_query handles exceptions gracefully."""
        query_hash = "error_test"
        query_text = "SELECT * FROM test"
        partition_key = "test_partition"

        # Mock an exception during storage
        rocksdict_handler.db.__setitem__.side_effect = Exception("Storage error")

        result = rocksdict_handler.set_query(query_hash, query_text, partition_key)

        assert result is False

    def test_get_query_exception_handling(self, rocksdict_handler):
        """Test get_query handles exceptions gracefully."""
        query_hash = "error_test"
        partition_key = "test_partition"

        # Mock an exception during retrieval
        rocksdict_handler.db.get.side_effect = Exception("Retrieval error")

        result = rocksdict_handler.get_query(query_hash, partition_key)

        assert result is None

    def test_get_all_queries_exception_handling(self, rocksdict_handler):
        """Test get_all_queries handles exceptions gracefully."""
        partition_key = "test_partition"

        # Mock an exception during key listing
        rocksdict_handler.db.keys.side_effect = Exception("Key listing error")

        result = rocksdict_handler.get_all_queries(partition_key)

        assert result == []


class TestQueryInterfaceIntegration:
    """Integration tests for query interface with cache operations."""

    def test_delete_removes_associated_query(self, rocksdict_handler):
        """Test that deleting a cache entry also removes associated query."""
        query_hash = "integration_test"
        partition_key = "test_partition"

        # Track deletions
        deleted_keys = []

        def mock_delitem(key):
            deleted_keys.append(key)

        def mock_contains(key):
            # Mock that both cache and query keys exist
            return key.startswith("cache:") or key.startswith("query:")

        rocksdict_handler.db.__delitem__.side_effect = mock_delitem
        rocksdict_handler.db.__contains__.side_effect = mock_contains

        # Test delete operation
        result = rocksdict_handler.delete(query_hash, partition_key)

        assert result is True

        # Verify both cache and query keys were deleted
        expected_cache_key = f"cache:{partition_key}:{query_hash}"
        expected_query_key = f"query:{partition_key}:{query_hash}"

        assert expected_cache_key in deleted_keys
        assert expected_query_key in deleted_keys

    def test_query_metadata_consistency(self, rocksdict_handler):
        """Test that query metadata remains consistent with cache operations."""
        query_hash = "consistency_test"
        query_text = "SELECT * FROM products WHERE price > 100"
        partition_key = "products"
        cache_data = {1, 2, 3, 4}

        # Mock storage tracking
        stored_data = {}

        def mock_setitem(key, value):
            stored_data[key] = value

        def mock_get(key):
            return stored_data.get(key)

        rocksdict_handler.db.__setitem__.side_effect = mock_setitem
        rocksdict_handler.db.get.side_effect = mock_get

        # Set cache data and query
        rocksdict_handler.set_cache(query_hash, cache_data, partition_key)
        rocksdict_handler.set_query(query_hash, query_text, partition_key)

        # Verify both are stored correctly
        cache_key = f"cache:{partition_key}:{query_hash}"
        query_key = f"query:{partition_key}:{query_hash}"

        assert cache_key in stored_data
        assert query_key in stored_data

        # Verify query retrieval works
        retrieved_query = rocksdict_handler.get_query(query_hash, partition_key)
        assert retrieved_query == query_text
