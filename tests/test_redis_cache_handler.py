import pytest
from unittest.mock import Mock, patch
from partitioncache.cache_handler.redis import RedisCacheHandler

@pytest.fixture
def mock_redis():
    return Mock()

@pytest.fixture
def cache_handler(mock_redis):
    with patch('partitioncache.cache_handler.redis.redis.Redis', return_value=mock_redis):
        handler = RedisCacheHandler(
            db_name=0,
            db_host="localhost",
            db_password="test_password",
            db_port=6379
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

def test_get_string_type_error(cache_handler, mock_redis):
    mock_redis.type.return_value = b"string"
    mock_redis.get.return_value = "some_string"
    with pytest.raises(ValueError):
        cache_handler.get("str_key")
    mock_redis.type.assert_called_with("str_key")
    mock_redis.get.assert_called_with("str_key")

def test_get_set_type_int(cache_handler, mock_redis):
    mock_redis.type.return_value = b"set"
    mock_redis.smembers.return_value = {b'1', b'2', b'3'}
    result = cache_handler.get("int_set_key", settype=int)
    assert result == {1, 2, 3}
    mock_redis.type.assert_called_with("int_set_key")
    mock_redis.smembers.assert_called_with("int_set_key")

def test_get_set_type_str(cache_handler, mock_redis):
    mock_redis.type.return_value = b"set"
    mock_redis.smembers.return_value = {b'foo', b'bar'}
    result = cache_handler.get("str_set_key", settype=str)
    assert result == {"foo", "bar"}
    mock_redis.type.assert_called_with("str_set_key")
    mock_redis.smembers.assert_called_with("str_set_key")

def test_get_set_invalid_type(cache_handler, mock_redis):
    mock_redis.type.return_value = b"hash"
    with pytest.raises(ValueError):
        cache_handler.get("hash_key")
    mock_redis.type.assert_called_with("hash_key")

def test_exists_true(cache_handler, mock_redis):
    mock_redis.exists.return_value = 1
    assert cache_handler.exists("existing_key") is True
    mock_redis.exists.assert_called_with("existing_key")

def test_exists_false(cache_handler, mock_redis):
    mock_redis.exists.return_value = 0
    assert cache_handler.exists("non_existent_key") is False
    mock_redis.exists.assert_called_with("non_existent_key")

def test_set_set_int(cache_handler, mock_redis):
    cache_handler.set_set("int_set_key", {1, 2, 3}, settype=int)
    mock_redis.sadd.assert_called_with("int_set_key", 1, 2, 3)

def test_set_set_invalid_type(cache_handler, mock_redis):
    with pytest.raises(ValueError):
        cache_handler.set_set("invalid_set_key", {1, 2, 3}, settype=float)

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

def test_get_intersected_no_valid_keys(cache_handler, mock_redis):
    mock_redis.pipeline.return_value = pipe = Mock()
    pipe.type.side_effect = [b"hash", b"string"]
    pipe.execute.return_value = [b"hash", b"string"]
    result, count = cache_handler.get_intersected({"key1", "key2"})
    assert result is None
    assert count == 0
    pipe.type.assert_any_call("key1")
    pipe.type.assert_any_call("key2")
    pipe.execute.assert_called()


def test_get_intersected_multiple_keys(cache_handler, mock_redis):
    mock_redis.pipeline.return_value = pipe = Mock()
    pipe.type.side_effect = [b"set", b"set", b"hash"]
    pipe.execute.return_value = [b"set", b"set", b"hash"]
    mock_redis.sinter.return_value = {b'2', b'3'}
    result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
    assert result == {2, 3}
    assert count == 2

def test_close(cache_handler, mock_redis):
    cache_handler.close()
    mock_redis.close.assert_called()