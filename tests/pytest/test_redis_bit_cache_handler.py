import uuid
from unittest.mock import Mock, patch

import pytest
from bitarray import bitarray

from partitioncache.cache_handler.redis_bit import RedisBitCacheHandler
from tests.pytest.test_empty_set_contract import REDIS_HOST, REDIS_PORT, _redis_available


def _packed(keys: set[int], bitsize: int = 100) -> bytes:
    bits = bitarray(bitsize, endian="big")
    bits.setall(0)
    for k in keys:
        bits[k] = 1
    return bits.tobytes()


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
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    mock_redis.type.return_value = b"string"
    mock_redis.get.return_value = _packed({1, 3})
    result = cache_handler.get("bit_key")
    assert result == {1, 3}
    mock_redis.type.assert_called_with(cache_key)
    mock_redis.get.assert_called_with(cache_key)

def test_get_intersected_single_key(cache_handler, mock_redis):
    cache_key = "cache:partition_key:key1"
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    type_pipe = Mock()
    type_pipe.execute.return_value = [b"string"]
    marker_pipe = Mock()
    marker_pipe.execute.return_value = [b"\x50\x00", 13]  # GETRANGE and STRLEN per key
    mock_redis.pipeline.side_effect = [type_pipe, marker_pipe]
    mock_redis.get.return_value = _packed({1, 3})
    result, count = cache_handler.get_intersected({"key1"})
    assert result == {1, 3}
    assert count == 1
    mock_redis.bitop.assert_not_called()
    mock_redis.get.assert_called_with(cache_key)

def test_get_intersected_multiple_keys(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    type_pipe = Mock()
    type_pipe.execute.return_value = [b"string", b"string", b"hash"]
    marker_pipe = Mock()
    marker_pipe.execute.return_value = [b"\x00\x00", 13, b"\x50\x00", 13]  # a leading zero byte is data, not the null marker
    mock_redis.pipeline.side_effect = [type_pipe, marker_pipe]
    mock_redis.get.return_value = _packed({1, 3})

    result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
    assert result == {1,3}
    assert count == 2
    mock_redis.bitop.assert_called()

def test_get_intersected_excludes_null_marker_from_count(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    type_pipe = Mock()
    type_pipe.execute.return_value = [b"string", b"string", b"none"]
    marker_pipe = Mock()
    marker_pipe.execute.return_value = [b"\x00", 1, b"\x50\x00", 13]
    mock_redis.pipeline.side_effect = [type_pipe, marker_pipe]
    mock_redis.get.return_value = _packed({1, 3})

    result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
    assert result == {1, 3}
    assert count == 1
    mock_redis.bitop.assert_not_called()

def test_set_cache_int(cache_handler, mock_redis):
    cache_key = "cache:partition_key:int_bit_key"
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    cache_handler.set_cache("int_bit_key", {1, 2, 3})
    # One bit per partition key: 100 bits pack into 13 bytes
    mock_redis.set.assert_called_with(cache_key, b"\x70" + b"\x00" * 12)

def test_set_cache_empty_set_is_stored_as_empty_string(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    cache_handler.set_cache("empty_key", set())
    mock_redis.set.assert_called_with("cache:partition_key:empty_key", b"")

def test_set_cache_invalid_type(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    with pytest.raises(ValueError, match="Only integer values"):
        cache_handler.set_cache("invalid_bit_key", {"a", "b", "c"})

def test_set_cache_out_of_range(cache_handler, mock_redis):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: cache_handler.default_bitsize
    with pytest.raises(ValueError, match="out of range"):
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


@pytest.fixture
def redis_handler():
    if not _redis_available():
        pytest.skip(f"no Redis server at {REDIS_HOST}:{REDIS_PORT}")
    handler = RedisBitCacheHandler(db_name=15, db_host=REDIS_HOST, db_password=None, db_port=REDIS_PORT, bitsize=1000)
    partition_key = f"pk_{uuid.uuid4().hex[:8]}"
    handler.register_partition_key(partition_key, "integer")
    yield handler, partition_key
    for key in handler.get_all_keys(partition_key):
        handler.delete(key, partition_key)
    handler.db.delete(f"_partition_metadata:{partition_key}")
    handler.close()


def test_redis_stores_one_bit_per_partition_key(redis_handler):
    handler, pk = redis_handler
    handler.set_cache("a", {0, 999}, pk)
    assert handler.db.strlen(handler._get_cache_key("a", pk)) == 125
    assert handler.db.getbit(handler._get_cache_key("a", pk), 999) == 1
    assert handler.get("a", pk) == {0, 999}


def test_redis_intersection_with_keys_beyond_first_byte(redis_handler):
    handler, pk = redis_handler
    handler.set_cache("high", {8, 500, 998}, pk)  # first byte is 0x00 but this is no null marker
    handler.set_cache("mixed", {1, 500, 998}, pk)
    handler.set_null("null", pk)
    assert handler.get_intersected({"high", "mixed", "null"}, pk) == ({500, 998}, 2)
    assert handler.get_intersected({"high", "null"}, pk) == ({8, 500, 998}, 1)


def test_redis_intersection_with_empty_sets(redis_handler):
    handler, pk = redis_handler
    handler.set_cache("high", {8, 500}, pk)
    handler.set_cache("empty1", set(), pk)
    handler.set_cache("empty2", set(), pk)
    assert handler.get_intersected({"high", "empty1"}, pk) == (set(), 2)
    assert handler.get_intersected({"empty1", "empty2"}, pk) == (set(), 2)


def _store_legacy(handler, key: str, partition_key: str, keys: set[int], bitsize: int = 1000) -> str:
    """Write an entry the way versions before the packed format did: one ASCII '0'/'1' per partition key."""
    bits = bitarray(bitsize)
    bits.setall(0)
    for k in keys:
        bits[k] = 1
    cache_key = handler._get_cache_key(key, partition_key)
    handler.db.set(cache_key, bits.to01())
    return cache_key


def test_redis_legacy_entry_is_converted_on_get(redis_handler):
    handler, pk = redis_handler
    cache_key = _store_legacy(handler, "legacy", pk, {5, 999})
    assert handler.get("legacy", pk) == {5, 999}
    assert handler.db.strlen(cache_key) == 125
    assert handler.get("legacy", pk) == {5, 999}


def test_redis_legacy_entry_is_converted_before_intersection(redis_handler):
    handler, pk = redis_handler
    cache_key = _store_legacy(handler, "legacy", pk, {5, 999})
    _store_legacy(handler, "legacy_empty", pk, set())
    handler.set_cache("new", {5, 6, 999}, pk)
    assert handler.get_intersected({"legacy", "new"}, pk) == ({5, 999}, 2)
    assert handler.db.strlen(cache_key) == 125
    assert handler.get_intersected({"legacy_empty", "new"}, pk) == (set(), 2)


def test_redis_entry_of_unknown_length_is_ignored(redis_handler):
    handler, pk = redis_handler
    handler.db.set(handler._get_cache_key("broken", pk), b"\x01\x02\x03")
    handler.set_cache("new", {5, 6}, pk)
    assert handler.get("broken", pk) is None
    assert handler.get_intersected({"broken", "new"}, pk) == ({5, 6}, 1)
