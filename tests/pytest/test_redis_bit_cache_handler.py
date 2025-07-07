from unittest.mock import Mock, patch

import pytest
from bitarray import bitarray

from partitioncache.cache_handler.redis_bit import RedisBitCacheHandler


@pytest.fixture
def mock_redis():
    return Mock()

@pytest.fixture
def cache_handler(mock_redis):
    with patch('partitioncache.cache_handler.redis_abstract.redis.Redis', return_value=mock_redis):
        handler = RedisBitCacheHandler(
            db_name=0,
            db_host="localhost",
            db_password="test_password",
            db_port=6379,
            bitsize=100
        )
        return handler

def test_get_non_existent_key(cache_handler, mock_redis):
    cache_key = "cache:partition_key:non_existent_key"
    cache_handler._get_partition_datatype = lambda pk: "integer"
    mock_redis.type.return_value = b"none"
    result = cache_handler.get("non_existent_key")
    assert result is None
    mock_redis.type.assert_called_with(cache_key)

def test_get_string_type_null_marker(cache_handler, mock_redis):
    cache_key = "cache:partition_key:null_key"
    mock_redis.type.return_value = b"string"
    mock_redis.get.return_value = b"\x00"
    result = cache_handler.get("null_key")
    assert result is None
    mock_redis.type.assert_called_with(cache_key)

def test_get_bitarray(cache_handler, mock_redis):
    cache_key = "cache:partition_key:bit_key"
    mock_redis.type.return_value = b"string"
    bitstring = b'0101' + b'0' * 97
    mock_redis.get.return_value = bitstring
    result = cache_handler.get("bit_key")
    assert result == {1, 3}
    mock_redis.type.assert_called_with(cache_key)
    mock_redis.get.assert_called_with(cache_key)

def test_get_intersected_single_key(cache_handler, mock_redis):
    cache_key = "cache:partition_key:key1"
    cache_handler._get_partition_datatype = lambda pk: "integer"
    mock_redis.pipeline.return_value = pipe = Mock()
    pipe.type.side_effect = [b"string"]
    pipe.execute.return_value = [b"string"]
    mock_redis.get.return_value = b'0101' + b'0' * 97
    result, count = cache_handler.get_intersected({"key1"})
    assert result == {1, 3}
    assert count == 1
    mock_redis.bitop.assert_not_called()
    mock_redis.get.assert_called_with(cache_key)

def test_get_intersected_multiple_keys(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    mock_redis.pipeline.return_value = pipe = Mock()
    pipe.type.side_effect = [b"string", b"string", b"hash"]
    pipe.execute.return_value = [b"string", b"string", b"hash"]
    mock_redis.bitop.return_value = b'0101' + b'0' * 97
    mock_redis.get.return_value = b'0101' + b'0' * 97

    result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
    assert result == {1,3}
    assert count == 2
    mock_redis.bitop.assert_called()

def test_set_cache_int(cache_handler, mock_redis):
    cache_key = "cache:partition_key:int_bit_key"
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    cache_handler.set_cache("int_bit_key", {1, 2, 3})
    expected_bitarray = bitarray(cache_handler.default_bitsize)
    expected_bitarray.setall(0)
    for k in {1, 2, 3}:
        expected_bitarray[k] = 1
    mock_redis.set.assert_called_with(cache_key, expected_bitarray.to01())

def test_set_cache_invalid_type(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    with pytest.raises(ValueError):
        cache_handler.set_cache("invalid_bit_key", {"a", "b", "c"})

def test_set_cache_out_of_range(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    with pytest.raises(ValueError):
        cache_handler.set_cache("out_of_range_key", {100})

def test_set_null(cache_handler, mock_redis):
    cache_key = "cache:partition_key:null_key"
    cache_handler.set_null("null_key")
    mock_redis.set.assert_called_with(cache_key, "\x00")

def test_delete_existing_key(cache_handler, mock_redis):
    cache_key = "cache:partition_key:existing_key"
    query_key = "query:partition_key:existing_key"
    cache_handler.delete("existing_key")
    # The handler deletes both the cache key and the query key
    mock_redis.delete.assert_any_call(cache_key)
    mock_redis.delete.assert_any_call(query_key)

def test_delete_non_existent_key(cache_handler, mock_redis):
    cache_key = "cache:partition_key:non_existent_key"
    query_key = "query:partition_key:non_existent_key"
    cache_handler.delete("non_existent_key")
    mock_redis.delete.assert_any_call(cache_key)
    mock_redis.delete.assert_any_call(query_key)

def test_get_all_keys(cache_handler, mock_redis):
    # The handler calls keys('cache:partition_key:*')
    mock_redis.keys.return_value = {b'cache:partition_key:key1', b'cache:partition_key:key2', b'cache:partition_key:key3'}
    all_keys = cache_handler.get_all_keys("partition_key")
    assert set(all_keys) == {"key1", "key2", "key3"}
    mock_redis.keys.assert_called_with('cache:partition_key:*')

def test_close(cache_handler, mock_redis):
    cache_handler.close()
    mock_redis.close.assert_called()
