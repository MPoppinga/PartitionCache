from unittest.mock import Mock, patch

import pytest
from bitarray import bitarray
from pyroaring import BitMap

from partitioncache.cache_handler.redis_roaringbit import RedisRoaringBitCacheHandler


@pytest.fixture
def mock_redis():
    return Mock()


@pytest.fixture
def cache_handler(mock_redis):
    with patch("partitioncache.cache_handler.redis_abstract.redis.Redis", return_value=mock_redis):
        handler = RedisRoaringBitCacheHandler(
            db_name=0,
            db_host="localhost",
            db_password="test_password",
            db_port=6379,
        )
        return handler


def test_get_non_existent_key(cache_handler, mock_redis):
    cache_key = "cache:partition_key:non_existent_key"
    cache_handler._get_partition_datatype = lambda pk: "integer"
    mock_redis.type.return_value = b"none"
    result = cache_handler.get("non_existent_key")
    assert result is None
    mock_redis.type.assert_called_with(cache_key)


def test_get_null_marker(cache_handler, mock_redis):
    cache_key = "cache:partition_key:null_key"
    cache_handler._get_partition_datatype = lambda pk: "integer"
    mock_redis.type.return_value = b"string"
    mock_redis.get.return_value = b"\x00"
    result = cache_handler.get("null_key")
    assert result is None
    mock_redis.type.assert_called_with(cache_key)


def test_get_valid_bitmap(cache_handler, mock_redis):
    cache_key = "cache:partition_key:bit_key"
    cache_handler._get_partition_datatype = lambda pk: "integer"
    mock_redis.type.return_value = b"string"
    bm = BitMap([1, 3, 5, 100])
    mock_redis.get.return_value = bm.serialize()
    result = cache_handler.get("bit_key")
    assert isinstance(result, BitMap)
    assert result == bm
    mock_redis.type.assert_called_with(cache_key)
    mock_redis.get.assert_called_with(cache_key)


def test_get_no_partition(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: None
    result = cache_handler.get("some_key")
    assert result is None


def test_get_intersected_single_key(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    type_pipe = Mock()
    type_pipe.execute.return_value = [b"string"]
    marker_pipe = Mock()
    marker_pipe.execute.return_value = [b"\x01"]
    mock_redis.pipeline.side_effect = [type_pipe, marker_pipe]
    bm = BitMap([1, 3, 5])
    mock_redis.mget.return_value = [bm.serialize()]
    result, count = cache_handler.get_intersected({"key1"})
    assert isinstance(result, BitMap)
    assert result == bm
    assert count == 1


def test_get_intersected_multiple_keys(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    type_pipe = Mock()
    type_pipe.execute.return_value = [b"string", b"string", b"hash"]
    marker_pipe = Mock()
    marker_pipe.execute.return_value = [b"\x01", b"\x01"]
    mock_redis.pipeline.side_effect = [type_pipe, marker_pipe]

    bm1 = BitMap([1, 2, 3, 4])
    bm2 = BitMap([3, 4, 5, 6])
    mock_redis.mget.return_value = [bm1.serialize(), bm2.serialize()]

    result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
    assert isinstance(result, BitMap)
    assert result == BitMap([3, 4])
    assert count == 2


def test_get_intersected_excludes_null_marker_from_count(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    type_pipe = Mock()
    type_pipe.execute.return_value = [b"string", b"string", b"none"]
    marker_pipe = Mock()
    marker_pipe.execute.return_value = [b"\x00", b"\x01"]
    mock_redis.pipeline.side_effect = [type_pipe, marker_pipe]

    bm = BitMap([1, 3, 5])
    mock_redis.mget.return_value = [bm.serialize()]

    result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
    assert isinstance(result, BitMap)
    assert result == bm
    assert count == 1


def test_get_intersected_empty(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    type_pipe = Mock()
    type_pipe.execute.return_value = [b"none", b"none"]
    marker_pipe = Mock()
    marker_pipe.execute.return_value = []
    mock_redis.pipeline.side_effect = [type_pipe, marker_pipe]
    result, count = cache_handler.get_intersected({"key1", "key2"})
    assert result is None
    assert count == 0


def test_get_intersected_no_partition(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: None
    result, count = cache_handler.get_intersected({"key1"})
    assert result is None
    assert count == 0


def test_set_cache_from_set(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler.set_cache("int_key", {1, 2, 3})
    cache_key = "cache:partition_key:int_key"
    mock_redis.set.assert_called_once()
    call_args = mock_redis.set.call_args
    assert call_args[0][0] == cache_key
    # Verify we can deserialize what was stored
    stored_bm = BitMap.deserialize(call_args[0][1])
    assert stored_bm == BitMap([1, 2, 3])


def test_set_cache_from_bitmap(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    bm = BitMap([10, 20, 30])
    cache_handler.set_cache("bm_key", bm)
    cache_key = "cache:partition_key:bm_key"
    call_args = mock_redis.set.call_args
    assert call_args[0][0] == cache_key
    stored_bm = BitMap.deserialize(call_args[0][1])
    assert stored_bm == bm


def test_set_cache_from_bitarray(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    ba = bitarray("01010")
    cache_handler.set_cache("ba_key", ba)
    call_args = mock_redis.set.call_args
    stored_bm = BitMap.deserialize(call_args[0][1])
    assert stored_bm == BitMap([1, 3])


def test_set_cache_invalid_type(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    with pytest.raises(ValueError, match="Only integer values"):
        cache_handler.set_cache("invalid_key", {"a", "b", "c"})


def test_set_cache_empty(cache_handler, mock_redis):
    result = cache_handler.set_cache("empty_key", set())
    assert result is True
    mock_redis.set.assert_not_called()


def test_set_cache_creates_partition(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: None
    cache_handler._set_partition_metadata = Mock()
    cache_handler.set_cache("new_key", {1, 2})
    cache_handler._set_partition_metadata.assert_called_once_with("partition_key", "integer")


def test_set_cache_wrong_datatype_partition(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "text"
    with pytest.raises(ValueError, match="roaring bitmap handler supports only 'integer'"):
        cache_handler.set_cache("wrong_key", {1, 2})


def test_register_partition_key_valid(cache_handler, mock_redis):
    cache_handler._set_partition_metadata = Mock()
    cache_handler.register_partition_key("new_partition", "integer")
    cache_handler._set_partition_metadata.assert_called_once_with("new_partition", "integer")


def test_register_partition_key_invalid(cache_handler, mock_redis):
    with pytest.raises(ValueError, match="supports only integer datatype"):
        cache_handler.register_partition_key("new_partition", "text")


def test_supported_datatypes(cache_handler):
    assert cache_handler.get_supported_datatypes() == {"integer"}


def test_repr(cache_handler):
    assert repr(cache_handler) == "redis_roaringbit"


def test_close(cache_handler, mock_redis):
    cache_handler.close()
    mock_redis.close.assert_called()
