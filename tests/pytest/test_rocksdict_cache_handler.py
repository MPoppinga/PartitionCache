import shutil
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest

from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler


@pytest.fixture
def temp_db_path():
    """Create a temporary directory for RocksDB testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def mock_rocksdict():
    """Mock RocksDict instance."""
    mock_rdict = MagicMock()
    mock_rdict.get.return_value = None
    mock_rdict.__contains__ = Mock(return_value=False)
    mock_rdict.__setitem__ = Mock()
    mock_rdict.__getitem__ = Mock()
    mock_rdict.__delitem__ = Mock()
    mock_rdict.keys.return_value = []
    mock_rdict.close.return_value = None
    mock_rdict.compact_range.return_value = None
    return mock_rdict


@pytest.fixture
def cache_handler(temp_db_path, mock_rocksdict):
    """Create RocksDictCacheHandler with mocked RocksDict."""
    with patch("partitioncache.cache_handler.rocksdict_abstract.Rdict", return_value=mock_rocksdict):
        handler = RocksDictCacheHandler(temp_db_path)
        return handler


class TestRocksDictCacheHandler:
    """Test suite for RocksDictCacheHandler."""

    def test_init_creates_db_with_options(self, temp_db_path):
        """Test that initialization creates RocksDB with proper options."""
        with patch("partitioncache.cache_handler.rocksdict_abstract.Rdict") as mock_rdict_class:
            mock_rdict = MagicMock()
            mock_rdict_class.return_value = mock_rdict

            RocksDictCacheHandler(temp_db_path, read_only=True)

            # Verify Rdict was called with correct parameters
            mock_rdict_class.assert_called_once()
            args, kwargs = mock_rdict_class.call_args
            assert args[0] == temp_db_path
            assert "options" in kwargs
            assert "access_type" in kwargs

    def test_get_partition_datatype_exists(self, cache_handler, mock_rocksdict):
        """Test getting existing partition datatype."""
        mock_rocksdict.get.return_value = "integer"
        result = cache_handler._get_partition_datatype("test_partition")
        assert result == "integer"
        mock_rocksdict.get.assert_called_with("_partition_metadata:test_partition")

    def test_get_partition_datatype_not_exists(self, cache_handler, mock_rocksdict):
        """Test getting non-existent partition datatype."""
        mock_rocksdict.get.return_value = None
        result = cache_handler._get_partition_datatype("missing_partition")
        assert result is None

    def test_get_partition_datatype_null_value(self, cache_handler, mock_rocksdict):
        """Test getting partition datatype with NULL value."""
        mock_rocksdict.get.return_value = "NULL"
        result = cache_handler._get_partition_datatype("null_partition")
        assert result is None

    def test_set_partition_datatype(self, cache_handler, mock_rocksdict):
        """Test setting partition datatype."""
        cache_handler._set_partition_datatype("test_partition", "float")
        mock_rocksdict.__setitem__.assert_called_with("_partition_metadata:test_partition", "float")

    def test_get_cache_key(self, cache_handler):
        """Test cache key generation."""
        key = cache_handler._get_cache_key("test_key", "test_partition")
        assert key == "cache:test_partition:test_key"

    def test_get_existing_key(self, cache_handler, mock_rocksdict):
        """Test getting existing cache value."""
        test_set = {1, 2, 3}
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else test_set

        result = cache_handler.get("test_key")
        assert result == test_set

    def test_get_non_existent_partition(self, cache_handler, mock_rocksdict):
        """Test getting from non-existent partition."""
        mock_rocksdict.get.return_value = None
        result = cache_handler.get("test_key", "missing_partition")
        assert result is None

    def test_get_non_existent_key(self, cache_handler, mock_rocksdict):
        """Test getting non-existent key."""
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else None
        result = cache_handler.get("missing_key")
        assert result is None

    def test_get_null_value(self, cache_handler, mock_rocksdict):
        """Test getting NULL value."""
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else "NULL"
        result = cache_handler.get("null_key")
        assert result is None

    def test_exists_true(self, cache_handler, mock_rocksdict):
        """Test exists returns True for existing key."""

        # Mock the metadata lookup
        def mock_get(key):
            if key == "_partition_metadata:partition_key":
                return "integer"
            return None

        mock_rocksdict.get.side_effect = mock_get

        # Mock the contains check for cache key and termination bits
        def mock_contains(key):
            if key == "cache:partition_key:test_key":
                return True  # Cache exists
            elif key in ["cache:partition_key:_LIMIT_test_key", "cache:partition_key:_TIMEOUT_test_key"]:
                return False  # No termination bits
            return False

        mock_rocksdict.__contains__.side_effect = mock_contains

        assert cache_handler.exists("test_key") is True  # Default check_query=False

    def test_exists_false_no_partition(self, cache_handler, mock_rocksdict):
        """Test exists returns False for non-existent partition."""
        mock_rocksdict.get.return_value = None
        assert cache_handler.exists("test_key") is False

    def test_exists_false_no_key(self, cache_handler, mock_rocksdict):
        """Test exists returns False for non-existent key."""
        mock_rocksdict.get.return_value = "integer"
        mock_rocksdict.__contains__.return_value = False
        assert cache_handler.exists("missing_key") is False

    def test_filter_existing_keys(self, cache_handler, mock_rocksdict):
        """Test filtering existing keys."""

        # Mock the metadata lookup
        def mock_get(key):
            if key == "_partition_metadata:partition_key":
                return "integer"
            return None

        mock_rocksdict.get.side_effect = mock_get

        # Mock __contains__ to handle cache keys and termination bits
        def mock_contains(key):
            # No termination bits exist (check first to avoid interference with endswith)
            if "_LIMIT_" in key or "_TIMEOUT_" in key:
                return False
            # Cache keys that exist
            elif key.endswith("key1") or key.endswith("key3"):
                return True
            return False

        mock_rocksdict.__contains__.side_effect = mock_contains

        keys = {"key1", "key2", "key3", "key4"}
        existing = cache_handler.filter_existing_keys(keys)
        assert existing == {"key1", "key3"}  # Default check_query=False

    def test_filter_existing_keys_no_partition(self, cache_handler, mock_rocksdict):
        """Test filtering keys with no partition."""
        mock_rocksdict.get.return_value = None
        keys = {"key1", "key2"}
        existing = cache_handler.filter_existing_keys(keys)
        assert existing == set()

    def test_get_intersected_no_partition(self, cache_handler, mock_rocksdict):
        """Test intersection with no partition."""
        mock_rocksdict.get.return_value = None
        result, count = cache_handler.get_intersected({"key1", "key2"})
        assert result is None
        assert count == 0

    def test_get_intersected_single_key(self, cache_handler, mock_rocksdict):
        """Test intersection with single key."""
        test_set = {1, 2, 3}
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else test_set

        result, count = cache_handler.get_intersected({"key1"})
        assert result == test_set
        assert count == 1

    def test_get_intersected_multiple_keys(self, cache_handler, mock_rocksdict):
        """Test intersection with multiple keys."""

        def mock_get_side_effect(key):
            if key == "_partition_metadata:partition_key":
                return "integer"
            elif key == "cache:partition_key:key1":
                return {1, 2, 3, 4}
            elif key == "cache:partition_key:key2":
                return {3, 4, 5, 6}
            elif key == "cache:partition_key:key3":
                return None
            return None

        mock_rocksdict.get.side_effect = mock_get_side_effect

        result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
        assert result == {3, 4}
        assert count == 2

    def test_get_intersected_no_matches(self, cache_handler, mock_rocksdict):
        """Test intersection with no matching keys."""
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else None

        result, count = cache_handler.get_intersected({"key1", "key2"})
        assert result is None
        assert count == 0

    def test_set_cache_integer_new_partition(self, cache_handler, mock_rocksdict):
        """Test setting integer set with new partition."""
        test_set = {1, 2, 3}
        mock_rocksdict.get.return_value = None  # No existing partition

        result = cache_handler.set_cache("test_key", test_set)
        assert result is True

        # Verify metadata was set
        mock_rocksdict.__setitem__.assert_any_call("_partition_metadata:partition_key", "integer")
        # Verify cache value was set
        mock_rocksdict.__setitem__.assert_any_call("cache:partition_key:test_key", test_set)

    def test_set_cache_float_existing_partition(self, cache_handler, mock_rocksdict):
        """Test setting float set with existing partition."""
        test_set = {1.1, 2.2, 3.3}
        mock_rocksdict.get.return_value = "float"  # Existing partition

        result = cache_handler.set_cache("test_key", test_set)
        assert result is True
        mock_rocksdict.__setitem__.assert_called_with("cache:partition_key:test_key", test_set)

    def test_set_cache_text(self, cache_handler, mock_rocksdict):
        """Test setting text set."""
        test_set = {"a", "b", "c"}
        mock_rocksdict.get.return_value = None

        result = cache_handler.set_cache("test_key", test_set)
        assert result is True
        mock_rocksdict.__setitem__.assert_any_call("_partition_metadata:partition_key", "text")

    def test_set_cache_timestamp(self, cache_handler, mock_rocksdict):
        """Test setting timestamp set."""
        test_set = {datetime(2023, 1, 1), datetime(2023, 1, 2)}
        mock_rocksdict.get.return_value = None

        result = cache_handler.set_cache("test_key", test_set)
        assert result is True
        mock_rocksdict.__setitem__.assert_any_call("_partition_metadata:partition_key", "timestamp")

    def test_set_cache_unsupported_type(self, cache_handler, mock_rocksdict):
        """Test setting unsupported type raises error."""
        test_set = {complex(1, 2)}
        mock_rocksdict.get.return_value = None

        with pytest.raises(ValueError, match="Unsupported partition key identifier type"):
            cache_handler.set_cache("test_key", test_set)

    def test_set_cache_type_mismatch(self, cache_handler, mock_rocksdict):
        """Test setting value with mismatched datatype raises error."""
        test_set = {1, 2, 3}
        mock_rocksdict.get.return_value = "invalid_type"

        with pytest.raises(ValueError, match="Unsupported datatype in metadata"):
            cache_handler.set_cache("test_key", test_set)

    def test_set_cache_exception_handling(self, cache_handler, mock_rocksdict):
        """Test set_cache exception handling."""
        test_set = {1, 2, 3}
        mock_rocksdict.get.return_value = None

        # Exception occurs during cache key storage, not metadata storage
        def side_effect_func(key, value):
            if key.startswith("cache:"):
                raise Exception("Test error")
            # Allow metadata storage to succeed

        mock_rocksdict.__setitem__.side_effect = side_effect_func

        result = cache_handler.set_cache("test_key", test_set)
        assert result is False

    def test_set_null_new_partition(self, cache_handler, mock_rocksdict):
        """Test setting null value with new partition."""
        mock_rocksdict.get.return_value = None

        result = cache_handler.set_null("test_key")
        assert result is True
        mock_rocksdict.__setitem__.assert_any_call("_partition_metadata:partition_key", "integer")
        mock_rocksdict.__setitem__.assert_any_call("cache:partition_key:test_key", "NULL")

    def test_set_null_existing_partition(self, cache_handler, mock_rocksdict):
        """Test setting null value with existing partition."""
        mock_rocksdict.get.return_value = "text"

        result = cache_handler.set_null("test_key")
        assert result is True
        mock_rocksdict.__setitem__.assert_called_with("cache:partition_key:test_key", "NULL")

    def test_set_null_exception_handling(self, cache_handler, mock_rocksdict):
        """Test set_null exception handling."""
        mock_rocksdict.get.return_value = None
        mock_rocksdict.__setitem__.side_effect = Exception("Test error")

        result = cache_handler.set_null("test_key")
        assert result is False

    def test_is_null_true(self, cache_handler, mock_rocksdict):
        """Test is_null returns True for null value."""
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else "NULL"

        assert cache_handler.is_null("test_key") is True

    def test_is_null_false_no_partition(self, cache_handler, mock_rocksdict):
        """Test is_null returns False for non-existent partition."""
        mock_rocksdict.get.return_value = None
        assert cache_handler.is_null("test_key") is False

    def test_is_null_false_not_null(self, cache_handler, mock_rocksdict):
        """Test is_null returns False for non-null value."""
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else {1, 2, 3}
        assert cache_handler.is_null("test_key") is False

    def test_delete_existing_key(self, cache_handler, mock_rocksdict):
        """Test deleting existing key."""
        mock_rocksdict.__contains__.side_effect = lambda k: k.startswith("cache:") or k.startswith("query:")

        result = cache_handler.delete("test_key")
        assert result is True
        mock_rocksdict.__delitem__.assert_any_call("cache:partition_key:test_key")
        mock_rocksdict.__delitem__.assert_any_call("query:partition_key:test_key")

    def test_delete_non_existent_key(self, cache_handler, mock_rocksdict):
        """Test deleting non-existent key."""
        mock_rocksdict.__contains__.return_value = False

        result = cache_handler.delete("missing_key")
        assert result is True  # Returns True even if key doesn't exist

    def test_delete_exception_handling(self, cache_handler, mock_rocksdict):
        """Test delete exception handling."""
        mock_rocksdict.__contains__.return_value = True
        mock_rocksdict.__delitem__.side_effect = Exception("Test error")

        result = cache_handler.delete("test_key")
        assert result is False

    def test_set_query(self, cache_handler, mock_rocksdict):
        """Test setting query."""
        result = cache_handler.set_query("test_key", "SELECT * FROM table")
        assert result is True

        # Verify query was stored
        args, kwargs = mock_rocksdict.__setitem__.call_args
        assert args[0] == "query:partition_key:test_key"
        query_data = args[1]
        assert query_data["query"] == "SELECT * FROM table"
        assert query_data["partition_key"] == "partition_key"
        assert "last_seen" in query_data

    def test_set_query_exception_handling(self, cache_handler, mock_rocksdict):
        """Test set_query exception handling."""
        mock_rocksdict.__setitem__.side_effect = Exception("Test error")

        result = cache_handler.set_query("test_key", "SELECT * FROM table")
        assert result is False

    def test_close_single_instance(self, cache_handler, mock_rocksdict):
        """Test closing single instance."""
        cache_handler._refcount = 1
        cache_handler.close()
        mock_rocksdict.close.assert_called_once()
        assert RocksDictCacheHandler._instance is None
        assert RocksDictCacheHandler._refcount == 0

    def test_close_multiple_references(self, temp_db_path, mock_rocksdict):
        """Test singleton close behavior with multiple references."""
        with patch("partitioncache.cache_handler.rocksdict_abstract.Rdict", return_value=mock_rocksdict):
            RocksDictCacheHandler._instance = None
            RocksDictCacheHandler._refcount = 0

            instance1 = RocksDictCacheHandler.get_instance(temp_db_path)
            instance2 = RocksDictCacheHandler.get_instance(temp_db_path)
            assert instance1 is instance2

            instance1.close()
            mock_rocksdict.close.assert_not_called()
            assert RocksDictCacheHandler._refcount == 1

    def test_compact(self, cache_handler, mock_rocksdict):
        """Test database compaction."""
        cache_handler.compact()
        mock_rocksdict.compact_range.assert_called_with(None, None)

    def test_get_all_keys(self, cache_handler, mock_rocksdict):
        """Test getting all keys for partition."""
        mock_keys = [
            "cache:test_partition:key1",
            "cache:test_partition:key2",
            "cache:other_partition:key3",
            "_partition_metadata:test_partition",
            "query:test_partition:key1",
        ]
        mock_rocksdict.keys.return_value = mock_keys

        result = cache_handler.get_all_keys("test_partition")
        assert set(result) == {"key1", "key2"}

    def test_get_all_keys_empty(self, cache_handler, mock_rocksdict):
        """Test getting all keys for empty partition."""
        mock_rocksdict.keys.return_value = []
        result = cache_handler.get_all_keys("empty_partition")
        assert result == []

    def test_get_partition_keys(self, cache_handler, mock_rocksdict):
        """Test getting all partition keys and datatypes."""
        mock_keys = ["_partition_metadata:partition1", "_partition_metadata:partition2", "cache:partition1:key1", "other_key"]
        mock_rocksdict.keys.return_value = mock_keys
        mock_rocksdict.get.side_effect = lambda k: {"_partition_metadata:partition1": "integer", "_partition_metadata:partition2": "text"}.get(k)

        result = cache_handler.get_partition_keys()
        assert result == [("partition1", "integer"), ("partition2", "text")]

    def test_get_partition_keys_empty(self, cache_handler, mock_rocksdict):
        """Test getting partition keys when none exist."""
        mock_rocksdict.keys.return_value = []
        result = cache_handler.get_partition_keys()
        assert result == []

    def test_get_instance_singleton(self, temp_db_path):
        """Test get_instance singleton behavior."""
        with patch("partitioncache.cache_handler.rocksdict_abstract.Rdict"):
            # Reset class variables
            RocksDictCacheHandler._instance = None
            RocksDictCacheHandler._refcount = 0

            instance1 = RocksDictCacheHandler.get_instance(temp_db_path)
            instance2 = RocksDictCacheHandler.get_instance(temp_db_path)

            assert instance1 is instance2
            assert RocksDictCacheHandler._refcount == 2

    def test_close_clears_singleton(self, temp_db_path):
        """After close(), get_instance() should return a new instance."""
        with patch("partitioncache.cache_handler.rocksdict_abstract.Rdict"):
            RocksDictCacheHandler._instance = None
            RocksDictCacheHandler._refcount = 0

            inst1 = RocksDictCacheHandler.get_instance(temp_db_path)
            inst1.close()

            inst2 = RocksDictCacheHandler.get_instance(temp_db_path)
            assert inst1 is not inst2  # Must be a fresh instance

    def test_get_datatype(self, cache_handler, mock_rocksdict):
        """Test getting datatype for partition."""
        mock_rocksdict.get.return_value = "float"
        result = cache_handler.get_datatype("test_partition")
        assert result == "float"

    def test_register_partition_key_valid(self, cache_handler, mock_rocksdict):
        """Test registering valid partition key."""
        cache_handler.register_partition_key("new_partition", "text")
        mock_rocksdict.__setitem__.assert_called_with("_partition_metadata:new_partition", "text")

    def test_register_partition_key_invalid(self, cache_handler, mock_rocksdict):
        """Test registering invalid partition key raises error."""
        with pytest.raises(ValueError, match="Unsupported datatype"):
            cache_handler.register_partition_key("new_partition", "invalid_type")

    def test_large_set_handling(self, cache_handler, mock_rocksdict):
        """Test handling of large sets."""
        large_set = set(range(100000))
        mock_rocksdict.get.return_value = None

        result = cache_handler.set_cache("large_key", large_set)
        assert result is True
        mock_rocksdict.__setitem__.assert_any_call("cache:partition_key:large_key", large_set)

    def test_unicode_key_handling(self, cache_handler, mock_rocksdict):
        """Test handling of unicode keys."""
        unicode_key = "测试键"
        test_set = {1, 2, 3}
        mock_rocksdict.get.return_value = None

        result = cache_handler.set_cache(unicode_key, test_set)
        assert result is True
        mock_rocksdict.__setitem__.assert_any_call(f"cache:partition_key:{unicode_key}", test_set)

    def test_long_key_handling(self, cache_handler, mock_rocksdict):
        """Test handling of very long keys."""
        long_key = "x" * 1000
        test_set = {1, 2, 3}
        mock_rocksdict.get.return_value = None

        result = cache_handler.set_cache(long_key, test_set)
        assert result is True
        mock_rocksdict.__setitem__.assert_any_call(f"cache:partition_key:{long_key}", test_set)

    def test_mixed_type_partition_keys(self, cache_handler, mock_rocksdict):
        """Test operations with different partition keys."""
        mock_rocksdict.get.side_effect = lambda k: {"_partition_metadata:int_partition": "integer", "_partition_metadata:text_partition": "text"}.get(k)

        # Test integer partition
        int_result = cache_handler.get_datatype("int_partition")
        assert int_result == "integer"

        # Test text partition
        text_result = cache_handler.get_datatype("text_partition")
        assert text_result == "text"

    def test_empty_set_handling(self, cache_handler, mock_rocksdict):
        """Test handling of empty sets."""
        empty_set = set()
        mock_rocksdict.get.return_value = None

        # This should raise StopIteration when trying to get sample from empty set
        with pytest.raises(StopIteration):
            cache_handler.set_cache("empty_key", empty_set)
