"""Unit tests for apply_cache.py module."""

import pytest
from unittest.mock import Mock, MagicMock
from partitioncache.apply_cache import (
    get_partition_keys,
    get_partition_keys_lazy,
    find_p0_alias,
    extend_query_with_partition_keys,
)
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy


class MockCacheHandler(AbstractCacheHandler):
    def __init__(self, return_keys=None):
        self.return_keys = return_keys if return_keys is not None else {1, 2, 3}
        
    def get_intersected(self, hashes):
        return self.return_keys, len(hashes)

    def add(self, hash_value, partition_keys):
        pass

    def remove(self, hash_value):
        pass

    def close(self):
        pass

    def delete(self):
        pass

    def exists(self, hash_value):
        return True

    def filter_existing_keys(self, keys):
        return keys

    def get(self, hash_value):
        return self.return_keys

    def get_all_keys(self):
        return self.return_keys

    def is_null(self, hash_value):
        return False

    def set_null(self, hash_value):
        pass

    def set_set(self, hash_value, partition_keys):
        pass


class MockLazyCacheHandler(AbstractCacheHandler_Lazy):
    def __init__(self, return_subquery=None, used_hashes=1):
        self.return_subquery = return_subquery or "SELECT partition_key FROM mock_table"
        self.used_hashes = used_hashes
        
    def get_intersected_lazy(self, hashes):
        return self.return_subquery, self.used_hashes

    def add(self, hash_value, partition_keys):
        pass

    def remove(self, hash_value):
        pass

    def close(self):
        pass

    def delete(self):
        pass

    def exists(self, hash_value):
        return True

    def filter_existing_keys(self, keys):
        return keys

    def get(self, hash_value):
        return {1, 2, 3}

    def get_all_keys(self):
        return {1, 2, 3}

    def is_null(self, hash_value):
        return False

    def set_null(self, hash_value):
        pass

    def set_set(self, hash_value, partition_keys):
        pass

    def get_intersected(self, hashes):
        return {1, 2, 3}, len(hashes)


def test_get_partition_keys():
    # Test basic functionality
    cache_handler = MockCacheHandler()
    query = """
        SELECT * FROM table1 t1, table2 t2
        WHERE t1.col1 = 'value' 
        AND t2.col2 > 10 
        AND distance(t1.x, t1.y, t2.x, t2.y) < 100
        AND t1.partition_id = t2.partition_id
    """
    partition_key = "partition_id"
    
    keys, total_hashes, used_hashes = get_partition_keys(query, cache_handler, partition_key)
    assert isinstance(keys, set)
    assert keys == {1, 2, 3}
    assert total_hashes > 0
    assert used_hashes > 0

    # Test with empty result
    cache_handler = MockCacheHandler(return_keys=set())
    keys, total_hashes, used_hashes = get_partition_keys(query, cache_handler, partition_key)
    assert isinstance(keys, set)
    assert len(keys) == 0


def test_get_right_amount_of_hashes_3_tables():
    # TODO move to query_processor
    
  
    cache_handler = MockCacheHandler()
    
    # complex query
    query = """
        SELECT * FROM table t1, table t2, table t3
        WHERE t1.col1 = 'value' 
        AND t2.col2 > 10 
        AND distance(t1.x, t1.y, t2.x, t2.y) < 100
        AND distance(t2.x, t2.y, t3.x, t3.y) < 100
        AND t1.partition_id = t2.partition_id
        AND t2.partition_id = t3.partition_id
    """
    partition_key = "partition_id"
    
    keys, total_hashes, used_hashes = get_partition_keys(query, cache_handler, partition_key)
    assert total_hashes == 3 # query + canonicalized query  1-2 + 2-3 + 1-3

def test_get_right_amount_of_hashes_simple_query():
    # TODO move to query_processor
    
  
    cache_handler = MockCacheHandler()
    
    # simple query

    query = """
        SELECT * FROM table t1, table t2
        WHERE t1.col1 = 'value' 
        AND t2.col2 > 10 
        AND distance(t1.x, t1.y, t2.x, t2.y) < 99.5
        AND t1.partition_id = t2.partition_id
    """
    partition_key = "partition_id"
    
    keys, total_hashes, used_hashes = get_partition_keys(query, cache_handler, partition_key)
    assert total_hashes == 2 # query + canonicalized query
    
    # simple query

    query = """
        SELECT * FROM table t1, table t2
        WHERE t1.col1 = 'value' 
        AND t2.col2 > 10 
        AND distance(t1.x, t1.y, t2.x, t2.y) < 100
        AND t1.partition_id = t2.partition_id
    """

    
    keys, total_hashes, used_hashes = get_partition_keys(query, cache_handler, partition_key)
    assert total_hashes == 1 # query
    

def test_get_partition_keys_lazy():
    # Test basic functionality
    cache_handler = MockLazyCacheHandler()
    query = "SELECT * FROM table1 t1 WHERE t1.col1 = 'value'"
    partition_key = "partition_id"
    
    subquery, used_hashes = get_partition_keys_lazy(query, cache_handler, partition_key)
    assert isinstance(subquery, str)
    assert subquery == "SELECT partition_key FROM mock_table"
    assert used_hashes == 1

    # Test with non-lazy cache handler
    regular_cache_handler = MockCacheHandler()
    with pytest.raises(ValueError, match="Cache handler does not support lazy intersection"):
        get_partition_keys_lazy(query, regular_cache_handler, partition_key)


def test_find_p0_alias():
    # Test basic query
    query = "SELECT * FROM table1 t1 WHERE t1.col1 = 'value'"
    assert find_p0_alias(query) == "t1"

    # Test query without alias
    query = "SELECT * FROM table1 WHERE col1 = 'value'"
    assert find_p0_alias(query) == "table1"

    # Test join query
    query = "SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id"
    assert find_p0_alias(query) == "t1"

    # Test query without table
    query = "SELECT 1"
    with pytest.raises(ValueError, match="No table found in query"):
        find_p0_alias(query)


def test_extend_query_with_partition_keys_in():
    query = "SELECT * FROM table1 t1 WHERE t1.col1 = 'value'"
    partition_keys = {1, 2, 3}
    partition_key = "partition_id"

    # Test IN method
    result = extend_query_with_partition_keys(
        query, partition_keys, partition_key, method="IN"
    )
    assert "t1.partition_id IN (1, 2, 3)" in result
    assert "t1.col1 = 'value'" in result

    # Test with empty partition keys
    result = extend_query_with_partition_keys(
        query, set(), partition_key, method="IN"
    )
    assert result == query


def test_extend_query_with_partition_keys_values():
    query = "SELECT * FROM table1 t1 WHERE t1.col1 = 'value'"
    partition_keys = {1, 2, 3}
    partition_key = "partition_id"

    # Test VALUES method
    result = extend_query_with_partition_keys(
        query, partition_keys, partition_key, method="VALUES"
    )
    assert "t1.partition_id IN (VALUES (1), (2), (3))" in result
    assert "t1.col1 = 'value'" in result


def test_extend_query_with_partition_keys_tmp_table():
    query = "SELECT * FROM table1 t1 WHERE t1.col1 = 'value'"
    partition_keys = {1, 2, 3}
    partition_key = "partition_id"

    # Test TMP_TABLE_JOIN method
    result = extend_query_with_partition_keys(
        query, partition_keys, partition_key, method="TMP_TABLE_JOIN"
    )
    assert "CREATE TEMPORARY TABLE tmp_partition_keys" in result
    assert "INSERT INTO tmp_partition_keys" in result
    assert "INNER JOIN tmp_partition_keys AS tmp_t1" in result
    assert "tmp_t1.partition_key = t1.partition_id" in result

    # Test TMP_TABLE_IN method
    result = extend_query_with_partition_keys(
        query, partition_keys, partition_key, method="TMP_TABLE_IN"
    )
    assert "CREATE TEMPORARY TABLE tmp_partition_keys" in result
    assert "INSERT INTO tmp_partition_keys" in result
    assert "t1.partition_id IN (SELECT partition_key FROM tmp_partition_keys)" in result


def test_extend_query_with_partition_keys_custom_alias():
    query = "SELECT * FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id"
    partition_keys = {1, 2, 3}
    partition_key = "partition_id"

    # Test with custom alias
    result = extend_query_with_partition_keys(
        query, partition_keys, partition_key, method="IN", p0_alias="t2"
    )
    assert "t2.partition_id IN (1, 2, 3)" in result

    # Test TMP_TABLE_JOIN with custom alias
    result = extend_query_with_partition_keys(
        query, partition_keys, partition_key, method="TMP_TABLE_JOIN", p0_alias="t2"
    )
    assert "tmp_t2.partition_key = t2.partition_id" in result
    assert "tmp_t1.partition_key = t1.partition_id" not in result 