from unittest.mock import MagicMock, Mock, patch

import pytest
from bitarray import bitarray
from pyroaring import BitMap

from partitioncache.cache_handler.rocksdict_roaringbit import RocksDictRoaringBitCacheHandler


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
def cache_handler(mock_rocksdict):
    """Create RocksDictRoaringBitCacheHandler with mocked RocksDict."""
    with patch("partitioncache.cache_handler.rocksdict_abstract.Rdict", return_value=mock_rocksdict):
        handler = RocksDictRoaringBitCacheHandler("/tmp/test_roaringbit")
        return handler


class TestRocksDictRoaringBitCacheHandler:
    """Test suite for RocksDictRoaringBitCacheHandler."""

    def test_repr(self, cache_handler):
        assert repr(cache_handler) == "rocksdict_roaringbit"

    def test_supported_datatypes(self, cache_handler):
        assert cache_handler.get_supported_datatypes() == {"integer"}

    def test_get_existing_key(self, cache_handler, mock_rocksdict):
        """Test getting existing bitmap from cache."""
        bm = BitMap([1, 2, 3, 100])
        serialized = bm.serialize()

        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else serialized

        result = cache_handler.get("test_key")
        assert isinstance(result, BitMap)
        assert result == bm

    def test_get_non_existent_partition(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.return_value = None
        result = cache_handler.get("test_key", "missing_partition")
        assert result is None

    def test_get_non_existent_key(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else None
        result = cache_handler.get("missing_key")
        assert result is None

    def test_get_null_value(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else "NULL"
        result = cache_handler.get("null_key")
        assert result is None

    def test_get_intersected_single_key(self, cache_handler, mock_rocksdict):
        bm = BitMap([1, 2, 3])
        serialized = bm.serialize()
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else serialized

        result, count = cache_handler.get_intersected({"key1"})
        assert isinstance(result, BitMap)
        assert result == bm
        assert count == 1

    def test_get_intersected_multiple_keys(self, cache_handler, mock_rocksdict):
        bm1 = BitMap([1, 2, 3, 4])
        bm2 = BitMap([3, 4, 5, 6])

        def mock_get(k):
            if k == "_partition_metadata:partition_key":
                return "integer"
            elif k == "cache:partition_key:key1":
                return bm1.serialize()
            elif k == "cache:partition_key:key2":
                return bm2.serialize()
            return None

        mock_rocksdict.get.side_effect = mock_get

        result, count = cache_handler.get_intersected({"key1", "key2"})
        assert isinstance(result, BitMap)
        assert result == BitMap([3, 4])
        assert count == 2

    def test_get_intersected_no_matches(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else None
        result, count = cache_handler.get_intersected({"key1", "key2"})
        assert result is None
        assert count == 0

    def test_get_intersected_no_partition(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.return_value = None
        result, count = cache_handler.get_intersected({"key1"})
        assert result is None
        assert count == 0

    def test_set_cache_from_set(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.return_value = None  # No existing partition

        result = cache_handler.set_cache("test_key", {1, 2, 3})
        assert result is True

        # Check metadata was set
        mock_rocksdict.__setitem__.assert_any_call("_partition_metadata:partition_key", "integer")
        # Check serialized bitmap was stored
        calls = mock_rocksdict.__setitem__.call_args_list
        cache_call = [c for c in calls if c[0][0] == "cache:partition_key:test_key"][0]
        stored_bm = BitMap.deserialize(cache_call[0][1])
        assert stored_bm == BitMap([1, 2, 3])

    def test_set_cache_from_bitmap(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.return_value = "integer"
        bm = BitMap([10, 20, 30])

        result = cache_handler.set_cache("bm_key", bm)
        assert result is True

        calls = mock_rocksdict.__setitem__.call_args_list
        cache_call = [c for c in calls if c[0][0] == "cache:partition_key:bm_key"][0]
        stored_bm = BitMap.deserialize(cache_call[0][1])
        assert stored_bm == bm

    def test_set_cache_from_bitarray(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.return_value = "integer"
        ba = bitarray("01010")

        result = cache_handler.set_cache("ba_key", ba)
        assert result is True

        calls = mock_rocksdict.__setitem__.call_args_list
        cache_call = [c for c in calls if c[0][0] == "cache:partition_key:ba_key"][0]
        stored_bm = BitMap.deserialize(cache_call[0][1])
        assert stored_bm == BitMap([1, 3])

    def test_set_cache_non_integer_fails(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.return_value = "integer"
        with pytest.raises(ValueError, match="Only integer values"):
            cache_handler.set_cache("text_key", {"a", "b"})

    def test_set_cache_empty(self, cache_handler, mock_rocksdict):
        result = cache_handler.set_cache("empty_key", set())
        assert result is True

    def test_set_cache_wrong_datatype_partition(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.return_value = "text"
        with pytest.raises(ValueError, match="roaring bitmap handler supports only 'integer'"):
            cache_handler.set_cache("wrong_key", {1, 2})

    def test_exists_true(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else None
        mock_rocksdict.__contains__.side_effect = lambda k: k == "cache:partition_key:test_key"
        assert cache_handler.exists("test_key") is True

    def test_exists_false(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.return_value = "integer"
        mock_rocksdict.__contains__.return_value = False
        assert cache_handler.exists("missing_key") is False

    def test_set_null(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.return_value = None
        result = cache_handler.set_null("test_key")
        assert result is True
        mock_rocksdict.__setitem__.assert_any_call("cache:partition_key:test_key", "NULL")

    def test_is_null_true(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else "NULL"
        assert cache_handler.is_null("test_key") is True

    def test_is_null_false(self, cache_handler, mock_rocksdict):
        mock_rocksdict.get.side_effect = lambda k: "integer" if k == "_partition_metadata:partition_key" else "something"
        assert cache_handler.is_null("test_key") is False

    def test_delete(self, cache_handler, mock_rocksdict):
        mock_rocksdict.__contains__.side_effect = lambda k: k.startswith("cache:") or k.startswith("query:")
        result = cache_handler.delete("test_key")
        assert result is True
        mock_rocksdict.__delitem__.assert_any_call("cache:partition_key:test_key")
        mock_rocksdict.__delitem__.assert_any_call("query:partition_key:test_key")

    def test_set_query(self, cache_handler, mock_rocksdict):
        result = cache_handler.set_query("test_key", "SELECT * FROM table")
        assert result is True
        args, kwargs = mock_rocksdict.__setitem__.call_args
        assert args[0] == "query:partition_key:test_key"
        query_data = args[1]
        assert query_data["query"] == "SELECT * FROM table"

    def test_get_all_keys(self, cache_handler, mock_rocksdict):
        mock_keys = [
            "cache:test_partition:key1",
            "cache:test_partition:key2",
            "cache:other_partition:key3",
            "_partition_metadata:test_partition",
        ]
        mock_rocksdict.keys.return_value = mock_keys
        result = cache_handler.get_all_keys("test_partition")
        assert set(result) == {"key1", "key2"}

    def test_get_partition_keys(self, cache_handler, mock_rocksdict):
        mock_keys = ["_partition_metadata:partition1", "_partition_metadata:partition2"]
        mock_rocksdict.keys.return_value = mock_keys
        mock_rocksdict.get.side_effect = lambda k: {"_partition_metadata:partition1": "integer", "_partition_metadata:partition2": "integer"}.get(k)
        result = cache_handler.get_partition_keys()
        assert result == [("partition1", "integer"), ("partition2", "integer")]

    def test_get_instance_singleton(self):
        with patch("partitioncache.cache_handler.rocksdict_abstract.Rdict"):
            RocksDictRoaringBitCacheHandler._instance = None
            RocksDictRoaringBitCacheHandler._refcount = 0
            instance1 = RocksDictRoaringBitCacheHandler.get_instance("/tmp/test1")
            instance2 = RocksDictRoaringBitCacheHandler.get_instance("/tmp/test1")
            assert instance1 is instance2
            assert RocksDictRoaringBitCacheHandler._refcount == 2

    def test_close_clears_singleton(self):
        """After close(), get_instance() should return a new instance."""
        with patch("partitioncache.cache_handler.rocksdict_abstract.Rdict"):
            RocksDictRoaringBitCacheHandler._instance = None
            RocksDictRoaringBitCacheHandler._refcount = 0

            inst1 = RocksDictRoaringBitCacheHandler.get_instance("/tmp/test_close")
            inst1.close()

            inst2 = RocksDictRoaringBitCacheHandler.get_instance("/tmp/test_close")
            assert inst1 is not inst2  # Must be a fresh instance

    def test_register_partition_key_valid(self, cache_handler, mock_rocksdict):
        cache_handler.register_partition_key("new_partition", "integer")
        mock_rocksdict.__setitem__.assert_called_with("_partition_metadata:new_partition", "integer")

    def test_register_partition_key_invalid(self, cache_handler, mock_rocksdict):
        with pytest.raises(ValueError, match="supports only integer datatype"):
            cache_handler.register_partition_key("new_partition", "text")
