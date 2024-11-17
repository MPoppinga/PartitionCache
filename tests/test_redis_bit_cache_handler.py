import pytest
from unittest.mock import Mock, patch
from bitarray import bitarray
from partitioncache.cache_handler.redis_bit import RedisBitCacheHandler

@pytest.fixture
def mock_redis():
    return Mock()

@pytest.fixture
def cache_handler(mock_redis):
    with patch('partitioncache.cache_handler.redis_bit.redis.Redis', return_value=mock_redis):
        handler = RedisBitCacheHandler(
            db_name=0,
            db_host="localhost",
            db_password="test_password",
            db_port=6379,
            bitsize=100
        )
        return handler

def test_get_non_existent_key(cache_handler, mock_redis):
    mock_redis.type.return_value = b"none"
    result = cache_handler.get("non_existent_key")
    assert result is None
    mock_redis.type.assert_called_with("non_existent_key")

def test_get_string_type_null_marker(cache_handler, mock_redis):
    mock_redis.type.return_value = b"string"
    mock_redis.get.return_value = "\x00"
    result = cache_handler.get("null_key")
    assert result is None
    mock_redis.type.assert_called_with("null_key")
    mock_redis.get.assert_called_with("null_key")

def test_get_bitarray(cache_handler, mock_redis):
    mock_redis.type.return_value = b"string"
    bitstring = b'0101' + b'0' * 97
    mock_redis.get.return_value = bitstring
    result = cache_handler.get(b"bit_key")
    assert result == {1, 3}
    mock_redis.type.assert_called_with(b"bit_key")
    mock_redis.get.assert_called_with(b"bit_key")

def test_get_intersected_single_key(cache_handler, mock_redis):
    mock_redis.pipeline.return_value = pipe = Mock()
    pipe.type.side_effect = [b"string"]
    pipe.execute.return_value = [b"string"]
    mock_redis.get.return_value = b'0101' + b'0' * 97
    result, count = cache_handler.get_intersected({b"key1"})
    assert result == {1, 3}
    assert count == 1
    mock_redis.bitop.assert_not_called()
    mock_redis.get.assert_called_with(b"key1")

def test_get_intersected_multiple_keys(cache_handler, mock_redis):
    mock_redis.pipeline.return_value = pipe = Mock()
    pipe.type.side_effect = [b"string", b"string", b"hash"]
    pipe.execute.return_value = [b"string", b"string", b"hash"]
    mock_redis.bitop.return_value = b'0101' + b'0' * 97
    mock_redis.get.return_value = b'0101' + b'0' * 97

    result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
    assert result == {1,3}
    assert count == 2
    mock_redis.bitop.assert_called()

def test_set_set_int(cache_handler, mock_redis):
    cache_handler.set_set("int_bit_key", {1, 2, 3}, settype=int)
    expected_bitarray = bitarray(cache_handler.bitsize)
    expected_bitarray.setall(0)
    for k in {1, 2, 3}:
        expected_bitarray[k] = 1

    mock_redis.set.assert_called_with("int_bit_key", expected_bitarray.to01())

def test_set_set_invalid_type(cache_handler, mock_redis):
    with pytest.raises(ValueError):
        cache_handler.set_set("invalid_bit_key", {1, 2, 3}, settype=str)

def test_set_set_out_of_range(cache_handler, mock_redis):
    with pytest.raises(ValueError):
        cache_handler.set_set("out_of_range_key", {1000}, settype=int)

def test_set_null(cache_handler, mock_redis):
    cache_handler.set_null("null_key")
    mock_redis.set.assert_called_with("null_key", "\x00")

def test_delete_existing_key(cache_handler, mock_redis):
    cache_handler.delete("existing_key")
    mock_redis.delete.assert_called_with("existing_key")

def test_delete_non_existent_key(cache_handler, mock_redis):
    cache_handler.delete("non_existent_key")
    mock_redis.delete.assert_called_with("non_existent_key")

def test_get_all_keys(cache_handler, mock_redis):
    mock_redis.keys.return_value = {b'key1', b'key2', b'key3'}
    all_keys = cache_handler.get_all_keys()
    # Ensure set comparison to ignore order
    assert set(all_keys) == {"key1", "key2", "key3"}
    mock_redis.keys.assert_called_with('*')

def test_close(cache_handler, mock_redis):
    cache_handler.close()
    mock_redis.close.assert_called()