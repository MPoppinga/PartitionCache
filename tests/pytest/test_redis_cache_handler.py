from unittest.mock import Mock, patch

import pytest

from partitioncache.cache_handler.redis_set import RedisCacheHandler


@pytest.fixture
def mock_redis():
    return Mock()


@pytest.fixture
def cache_handler(mock_redis):
    with patch("partitioncache.cache_handler.redis_abstract.redis.Redis", return_value=mock_redis):
        handler = RedisCacheHandler(db_name=0, db_host="localhost", db_password="test_password", db_port=6379)
        return handler


def test_get_non_existent_key(cache_handler, mock_redis):
    cache_key = "cache:partition_key:non_existent_key"

    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="integer")

    # Mock that the cache key doesn't exist
    mock_redis.type.return_value = b"none"

    result = cache_handler.get("non_existent_key")
    assert result is None
    mock_redis.type.assert_called_with(cache_key)


def test_get_string_type_null_marker(cache_handler, mock_redis):
    cache_key = "cache:partition_key:null_key"
    metadata_key = "_partition_metadata:partition_key"
    # First call for metadata check, second call for cache key check
    mock_redis.type.side_effect = [b"string", b"string"]
    mock_redis.get.side_effect = lambda k: b"integer" if k == metadata_key else b"\x00"
    result = cache_handler.get("null_key")
    assert result is None
    mock_redis.type.assert_called_with(cache_key)
    mock_redis.get.assert_called_with(cache_key)


def test_get_string_type_error(cache_handler, mock_redis):
    cache_key = "cache:partition_key:str_key"
    metadata_key = "_partition_metadata:partition_key"
    # First call for metadata check, second call for cache key check
    mock_redis.type.side_effect = [b"string", b"string"]
    mock_redis.get.side_effect = lambda k: b"integer" if k == metadata_key else "some_string"
    with pytest.raises(ValueError):
        cache_handler.get("str_key")
    mock_redis.type.assert_called_with(cache_key)
    mock_redis.get.assert_called_with(cache_key)


def test_get_set_type_int(cache_handler, mock_redis):
    cache_key = "cache:partition_key:int_set_key"

    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="integer")

    # Mock that the cache key is a set
    mock_redis.type.return_value = b"set"
    mock_redis.smembers.return_value = {b"1", b"2", b"3"}

    result = cache_handler.get("int_set_key")
    assert result == {1, 2, 3}
    mock_redis.type.assert_called_with(cache_key)
    mock_redis.smembers.assert_called_with(cache_key)


def test_get_set_type_str(cache_handler, mock_redis):
    cache_key = "cache:partition_key:str_set_key"

    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="text")

    # Mock that the cache key is a set
    mock_redis.type.return_value = b"set"
    mock_redis.smembers.return_value = {b"foo", b"bar"}

    result = cache_handler.get("str_set_key")
    assert result == {"foo", "bar"}
    mock_redis.type.assert_called_with(cache_key)
    mock_redis.smembers.assert_called_with(cache_key)


def test_get_set_invalid_type(cache_handler, mock_redis):
    cache_key = "cache:partition_key:hash_key"
    metadata_key = "_partition_metadata:partition_key"
    # First call for metadata check, second call for cache key check
    mock_redis.type.side_effect = [b"string", b"hash"]
    mock_redis.get.side_effect = lambda k: b"integer" if k == metadata_key else None
    with pytest.raises(ValueError):
        cache_handler.get("hash_key")
    mock_redis.type.assert_called_with(cache_key)


def test_exists_true(cache_handler, mock_redis):
    cache_key = "cache:partition_key:existing_key"
    metadata_key = "_partition_metadata:partition_key"
    # First call for metadata check
    mock_redis.type.return_value = b"string"
    mock_redis.get.side_effect = lambda k: b"integer" if k == metadata_key else None
    mock_redis.exists.return_value = 1
    assert cache_handler.exists("existing_key") is True
    mock_redis.exists.assert_called_with(cache_key)


def test_exists_false(cache_handler, mock_redis):
    cache_key = "cache:partition_key:non_existent_key"
    metadata_key = "_partition_metadata:partition_key"
    mock_redis.type.return_value = b"string"  # Metadata key exists
    mock_redis.get.side_effect = lambda k: b"integer" if k == metadata_key else None
    mock_redis.exists.return_value = 0
    assert cache_handler.exists("non_existent_key") is False
    mock_redis.exists.assert_called_with(cache_key)


def test_set_set_int(cache_handler, mock_redis):
    cache_key = "cache:partition_key:int_set_key"

    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="integer")

    cache_handler.set_set("int_set_key", {1, 2, 3})
    mock_redis.sadd.assert_called_with(cache_key, "1", "2", "3")


def test_set_set_invalid_type(cache_handler, mock_redis):
    with pytest.raises(ValueError):
        cache_handler.set_set("invalid_set_key", {1.1, 2.2, 3.3})


def test_set_null(cache_handler, mock_redis):
    cache_key = "cache:partition_key:null_key"
    metadata_key = "_partition_metadata:partition_key"
    mock_redis.get.side_effect = lambda k: b"integer" if k == metadata_key else None
    cache_handler.set_null("null_key")
    mock_redis.set.assert_called_with(cache_key, "\x00")


def test_delete_existing_key(cache_handler, mock_redis):
    cache_key = "cache:partition_key:existing_key"
    query_key = "query:partition_key:existing_key"
    cache_handler.delete("existing_key")
    mock_redis.delete.assert_any_call(cache_key)
    mock_redis.delete.assert_any_call(query_key)


def test_delete_non_existent_key(cache_handler, mock_redis):
    cache_key = "cache:partition_key:non_existent_key"
    query_key = "query:partition_key:non_existent_key"
    cache_handler.delete("non_existent_key")
    mock_redis.delete.assert_any_call(cache_key)
    mock_redis.delete.assert_any_call(query_key)


def test_get_all_keys(cache_handler, mock_redis):
    mock_redis.keys.return_value = {b"cache:partition_key:key1", b"cache:partition_key:key2", b"cache:partition_key:key3"}
    all_keys = cache_handler.get_all_keys("partition_key")
    assert set(all_keys) == {"key1", "key2", "key3"}
    mock_redis.keys.assert_called_with("cache:partition_key:*")


def test_get_intersected_no_valid_keys(cache_handler, mock_redis):
    keys = {"key1", "key2"}
    cache_keys = [f"cache:partition_key:{k}" for k in keys]
    metadata_key = "_partition_metadata:partition_key"
    mock_redis.type.return_value = b"string"  # Metadata key exists
    mock_redis.get.side_effect = lambda k: b"integer" if k == metadata_key else None
    mock_redis.pipeline.return_value = pipe = Mock()
    pipe.type.side_effect = [b"hash", b"string"]
    pipe.execute.return_value = [b"hash", b"string"]
    result, count = cache_handler.get_intersected(keys)
    assert result is None
    assert count == 0
    for k in cache_keys:
        pipe.type.assert_any_call(k)
    pipe.execute.assert_called()


def test_get_intersected_multiple_keys(cache_handler, mock_redis):
    keys = {"key1", "key2", "key3"}
    cache_keys = [f"cache:partition_key:{k}" for k in keys]

    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="integer")

    mock_redis.pipeline.return_value = pipe = Mock()
    pipe.type.side_effect = [b"set", b"set", b"hash"]
    pipe.execute.return_value = [b"set", b"set", b"hash"]
    mock_redis.sinter.return_value = {b"2", b"3"}

    result, count = cache_handler.get_intersected(keys)
    assert result == {2, 3}
    assert count == 2
    mock_redis.sinter.assert_called_with(*[k for k, t in zip(cache_keys, [b"set", b"set", b"hash"], strict=False) if t == b"set"])


def test_close(cache_handler, mock_redis):
    cache_handler.close()
    mock_redis.close.assert_called()
