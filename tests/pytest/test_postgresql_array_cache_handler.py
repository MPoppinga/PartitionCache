from unittest.mock import Mock, patch

import psycopg
import pytest
from psycopg import sql

from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler


@pytest.fixture
def mock_db():
    return Mock()


@pytest.fixture
def mock_cursor():
    mock = Mock()

    # Create a proper mock for as_string that works with psycopg
    def mock_as_string(cursor=None):
        if hasattr(mock, "_last_query_str"):
            return mock._last_query_str
        return "mock_query"

    mock.as_string = mock_as_string

    # Mock encoding attribute that psycopg expects
    mock.adapters = Mock()
    mock.connection = Mock()
    mock.connection.encoding = "utf-8"  # Add proper encoding

    return mock


@pytest.fixture
def cache_handler(mock_db, mock_cursor):
    with patch("psycopg.connect", return_value=mock_db):
        handler = PostgreSQLArrayCacheHandler(
            db_name="test_db", db_host="localhost", db_user="test_user", db_password="test_password", db_port=5432, db_tableprefix="test_cache"
        )
        handler.cursor = mock_cursor  # type: ignore
        handler.db = mock_db  # type: ignore
        # Assume aggregate functions are available for most tests
        handler.array_intersect_agg_available = True
        return handler


def test_set_cache(cache_handler):
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    result = cache_handler.set_cache("key1", {1, 2, 3})
    assert result is True
    assert cache_handler.cursor.execute.called
    cache_handler.db.commit.assert_called()

    # Verify INSERT SQL with ON CONFLICT was issued with correct key and values
    insert_found = False
    for call in cache_handler.cursor.execute.call_args_list:
        call_str = str(call)
        if "INSERT INTO" in call_str and "ON CONFLICT" in call_str and "DO UPDATE SET" in call_str:
            insert_found = True
            # Verify the key parameter is passed
            args = call[0]  # positional args to execute()
            if len(args) > 1 and isinstance(args[1], tuple):
                params = args[1]
                assert params[0] == "key1", f"Expected key 'key1', got {params[0]}"
                # The values should be a list of the set elements
                assert isinstance(params[1], list), f"Expected list for values, got {type(params[1])}"
                assert set(params[1]) == {1, 2, 3}, f"Expected {{1, 2, 3}}, got {set(params[1])}"
            break
    assert insert_found, "Expected INSERT INTO ... ON CONFLICT SQL statement not found"


def test_set_cache_empty(cache_handler):
    cache_handler.cursor.execute.reset_mock()
    cache_handler.set_cache("empty_key", set())
    assert not cache_handler.cursor.execute.called


def test_set_cache_str_type(cache_handler):
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    cache_handler.set_cache("int_key", {1, 2, 3})
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    result = cache_handler.set_cache("str_key", {"a", "b", "c"})
    assert result is False


def test_get(cache_handler):
    # Mock the _get_partition_datatype method directly instead of relying on cursor side effects
    cache_handler._get_partition_datatype = Mock(return_value="integer")
    cache_handler.cursor.fetchone.return_value = ([1, 2, 3],)
    result = cache_handler.get("key1")
    assert result == {1, 2, 3}


def test_get_none(cache_handler):
    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="integer")
    cache_handler.cursor.fetchone.return_value = (None,)
    result = cache_handler.get("non_existent_key")
    assert result is None


def test_get_str_type(cache_handler):
    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="text")
    cache_handler.cursor.fetchone.return_value = (None,)
    result = cache_handler.get("str_key")
    assert result is None


def test_set_null(cache_handler):
    """Test set_null stores NULL partition_keys for a key when partition datatype is known."""
    # Mock _get_partition_datatype to return a known datatype
    cache_handler._get_partition_datatype = Mock(return_value="integer")
    result = cache_handler.set_null("null_key")
    assert result is True
    cache_handler.db.commit.assert_called()

    # Verify INSERT SQL with NULL was issued
    insert_found = False
    for call_args in cache_handler.cursor.execute.call_args_list:
        call_str = str(call_args)
        if "INSERT INTO" in call_str and "ON CONFLICT" in call_str:
            insert_found = True
            # Verify the key and NULL value are passed as parameters
            args = call_args[0]
            if len(args) > 1 and isinstance(args[1], tuple):
                params = args[1]
                assert params[0] == "null_key", f"Expected key 'null_key', got {params[0]}"
                assert params[1] is None, f"Expected None for partition_keys, got {params[1]}"
            break
    assert insert_found, "Expected INSERT INTO ... ON CONFLICT SQL for set_null"


def test_set_null_unknown_datatype(cache_handler):
    """Test set_null returns False when partition datatype cannot be determined.

    PostgreSQLArrayCacheHandler supports 4 datatypes, so when no datatype
    is registered for the partition, it cannot auto-determine which to use.
    """
    cache_handler._get_partition_datatype = Mock(return_value=None)
    result = cache_handler.set_null("null_key")
    assert result is False
    # Verify no INSERT was attempted
    for call_args in cache_handler.cursor.execute.call_args_list:
        call_str = str(call_args)
        assert "INSERT" not in call_str, "INSERT should not be called when datatype is unknown"


def test_is_null(cache_handler):
    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="integer")
    cache_handler.cursor.fetchone.return_value = (None,)
    assert cache_handler.is_null("null_key") is True

    cache_handler.cursor.fetchone.return_value = ([1, 2, 3],)
    assert cache_handler.is_null("non_null_key") is False


def test_exists_existing_key(cache_handler, mock_db, mock_cursor):
    cache_handler.cursor = mock_cursor
    cache_handler.db = mock_db
    mock_cursor.fetchone.side_effect = [("integer",), (1,)]
    assert cache_handler.exists("existing_key") is True


def test_exists_non_existent_key(cache_handler, mock_db, mock_cursor):
    cache_handler.cursor = mock_cursor
    cache_handler.db = mock_db
    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="integer")
    mock_cursor.fetchone.return_value = None
    assert cache_handler.exists("non_existent_key") is False


def test_get_intersected(cache_handler):
    cache_handler._get_partition_datatype = Mock(return_value="integer")
    cache_handler.filter_existing_keys = Mock(return_value={"key1", "key2"})
    cache_handler.cursor.fetchone.return_value = ([3, 4],)

    # Mock the get_intersected_sql method to avoid the as_string encoding issue
    with patch.object(cache_handler, "get_intersected_sql") as mock_sql:
        mock_query = Mock()
        mock_query.as_string.return_value = (
            "SELECT array_intersect_agg(partition_keys) FROM test_cache_cache_partition_key WHERE query_hash = ANY(ARRAY['key1', 'key2'])"
        )
        mock_sql.return_value = mock_query

        result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
        assert result == {3, 4}
        assert count == 2


def test_get_intersected_no_existing_keys(cache_handler):
    cache_handler.filter_existing_keys = Mock(return_value=set())

    result, count = cache_handler.get_intersected({"key1", "key2"})
    assert result is None
    assert count == 0


def test_filter_existing_keys(cache_handler):
    # Set up partition datatype and fetchall return value
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",)]
    existing_keys = cache_handler.filter_existing_keys({"key1", "key2", "key3"})
    assert existing_keys == {"key1", "key2"}


def test_get_intersected_lazy(cache_handler):
    # Set up partition datatype and filter_existing_keys
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    cache_handler.filter_existing_keys = Mock(return_value={"key1", "key2"})
    # The handler will return a string SQL and count
    query, count = cache_handler.get_intersected_lazy({"key1", "key2", "key3"})
    assert isinstance(query, str)
    assert count == 2

    # Verify the lazy SQL structure: should wrap intersection in SELECT unnest(...)
    assert "unnest" in query.lower(), f"Expected 'unnest' in lazy query, got: {query}"
    assert "partition_key" in query.lower(), f"Expected partition column reference in lazy query, got: {query}"
    # Should reference the cache table
    assert "test_cache_cache_partition_key" in query, f"Expected table name in lazy query, got: {query}"
    # Should contain intersection logic (aggregate or fallback)
    assert "array_intersect_agg" in query or "&" in query or "intersect" in query.lower(), (
        f"Expected intersection logic in lazy query, got: {query}"
    )


def test_get_all_keys(cache_handler):
    # Mock the metadata query to return integer datatype
    cache_handler.cursor.fetchone.return_value = ("integer",)
    # Mock the keys query
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",), ("key3",)]

    all_keys = cache_handler.get_all_keys("partition_key")
    # Ensure set comparison to ignore order
    assert set(all_keys) == {"key1", "key2", "key3"}


def test_delete(cache_handler):
    # Set up partition datatype
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    result = cache_handler.delete("key_to_delete")
    assert result is True
    cache_handler.db.commit.assert_called()

    # Verify DELETE SQL was issued with correct key parameter
    delete_calls = []
    for call in cache_handler.cursor.execute.call_args_list:
        call_str = str(call)
        if "DELETE FROM" in call_str:
            delete_calls.append(call)

    # Should have two DELETE calls: one from cache table, one from queries table
    assert len(delete_calls) == 2, f"Expected 2 DELETE calls, got {len(delete_calls)}"

    # First DELETE: from partition-specific cache table
    cache_delete_str = str(delete_calls[0])
    assert "DELETE FROM" in cache_delete_str
    assert "query_hash" in cache_delete_str
    cache_delete_params = delete_calls[0][0][1]  # (sql, params) -> params
    assert cache_delete_params == ("key_to_delete",), f"Expected ('key_to_delete',), got {cache_delete_params}"

    # Second DELETE: from queries table
    queries_delete_str = str(delete_calls[1])
    assert "DELETE FROM" in queries_delete_str
    queries_delete_params = delete_calls[1][0][1]  # (sql, params) -> params
    assert "key_to_delete" in queries_delete_params, f"Expected 'key_to_delete' in params, got {queries_delete_params}"
    assert "partition_key" in queries_delete_params, f"Expected 'partition_key' in params, got {queries_delete_params}"


def test_get_intersected_sql_aggregates():
    """Test SQL generation for intersection using aggregates."""
    # Create a simple mock handler without going through __init__
    handler = Mock(spec=PostgreSQLArrayCacheHandler)
    handler.USE_AGGREGATES = True
    handler.array_intersect_agg_available = True
    handler.tableprefix = "test_cache"

    # Call the actual method
    query = PostgreSQLArrayCacheHandler.get_intersected_sql(handler, {"key1", "key2"}, "p1", "integer")

    # Verify the query structure
    query_str = str(query)
    assert "array_intersect_agg" in query_str
    assert "test_cache_cache_p1" in query_str
    assert "query_hash = ANY" in query_str


def test_get_intersected_sql_fallback_integer():
    """Test SQL generation for integer intersection using fallback."""
    # Create a simple mock handler without going through __init__
    handler = Mock(spec=PostgreSQLArrayCacheHandler)
    handler.USE_AGGREGATES = False
    handler.array_intersect_agg_available = False
    handler.tableprefix = "test_cache"

    # Call the actual method
    query = PostgreSQLArrayCacheHandler.get_intersected_sql(handler, {"key1", "key2"}, "p1", "integer")

    # Verify the query is a psycopg sql.Composed object
    assert isinstance(query, sql.Composed), f"Expected sql.Composed, got {type(query)}"

    # Check the query structure for integer-specific intersection
    query_str = str(query)
    # Should reference the correct table
    assert "test_cache_cache_p1" in query_str, f"Expected table name in query, got: {query_str}"
    # Should use intarray & operator for integer intersection
    assert " & " in query_str, f"Expected '&' operator for integer intersection, got: {query_str}"
    # Should reference partition_keys from aliased subqueries
    assert "partition_keys" in query_str, f"Expected 'partition_keys' column reference, got: {query_str}"
    # Should reference both keys via subquery aliases
    assert "k0" in query_str and "k1" in query_str, f"Expected subquery aliases k0, k1 in query, got: {query_str}"
    # Should contain SELECT ... FROM structure
    assert "SELECT" in query_str, f"Expected SELECT in query, got: {query_str}"
    assert "query_hash" in query_str, f"Expected query_hash filter in query, got: {query_str}"
    # Should NOT use aggregate function
    assert "array_intersect_agg" not in query_str, f"Should not use aggregate in fallback mode, got: {query_str}"


def test_get_intersected_sql_fallback_text():
    """Test SQL generation for text intersection using fallback."""
    # Create a simple mock handler without going through __init__
    handler = Mock(spec=PostgreSQLArrayCacheHandler)
    handler.USE_AGGREGATES = False
    handler.array_intersect_agg_available = False
    handler.tableprefix = "test_cache"

    # Call the actual method
    query = PostgreSQLArrayCacheHandler.get_intersected_sql(handler, {"key1", "key2"}, "p1", "text")

    # Verify the query is a psycopg sql.Composed object
    assert isinstance(query, sql.Composed), f"Expected sql.Composed, got {type(query)}"

    # Check the query structure for text-specific intersection
    query_str = str(query)
    # Should reference the correct table
    assert "test_cache_cache_p1" in query_str, f"Expected table name in query, got: {query_str}"
    # Text intersection uses unnest + intersect pattern (not intarray & operator)
    assert "unnest" in query_str.lower(), f"Expected 'unnest' for text intersection, got: {query_str}"
    assert "intersect" in query_str.lower(), f"Expected 'intersect' for text intersection, got: {query_str}"
    # Should reference partition_keys
    assert "partition_keys" in query_str, f"Expected 'partition_keys' column reference, got: {query_str}"
    # Should NOT use intarray & operator (that's integer-only)
    assert " & " not in query_str, f"Text intersection should not use '&' operator, got: {query_str}"
    # Should NOT use aggregate function
    assert "array_intersect_agg" not in query_str, f"Should not use aggregate in fallback mode, got: {query_str}"
    # Should contain query_hash filter
    assert "query_hash" in query_str, f"Expected query_hash filter in query, got: {query_str}"


def test_init_creates_extensions_and_aggregates():
    """Test that __init__ attempts to create extensions and aggregates."""
    mock_cursor = Mock()
    mock_db = Mock()
    mock_cursor.adapters = Mock()

    with patch("psycopg.connect", return_value=mock_db):
        # Don't mock __init__, just create the handler normally
        handler = PostgreSQLArrayCacheHandler("test_db", "localhost", "test_user", "test_password", 5432, "test_prefix")
        handler.cursor = mock_cursor
        handler.db = mock_db

        # Verify required attributes exist with correct types
        assert hasattr(handler, "array_intersect_agg_available")
        assert isinstance(handler.array_intersect_agg_available, bool), (
            f"Expected bool for array_intersect_agg_available, got {type(handler.array_intersect_agg_available)}"
        )
        assert hasattr(handler, "USE_AGGREGATES")
        assert isinstance(handler.USE_AGGREGATES, bool), f"Expected bool for USE_AGGREGATES, got {type(handler.USE_AGGREGATES)}"
        assert handler.USE_AGGREGATES is True, "USE_AGGREGATES should be True by default"
        # Verify tableprefix was set correctly
        assert handler.tableprefix == "test_prefix", f"Expected tableprefix 'test_prefix', got {handler.tableprefix}"



def test_get_intersected_single_key(cache_handler):
    cache_handler._get_partition_datatype = Mock(return_value="integer")
    cache_handler.filter_existing_keys = Mock(return_value={"key1"})
    cache_handler.cursor.fetchone.return_value = ([1, 2, 3],)

    # Mock the get_intersected_sql method to avoid the as_string encoding issue
    with patch.object(cache_handler, "get_intersected_sql") as mock_sql:
        mock_query = Mock()
        mock_query.as_string.return_value = (
            "SELECT array_intersect_agg(partition_keys) FROM test_cache_cache_partition_key WHERE query_hash = ANY(ARRAY['key1'])"
        )
        mock_sql.return_value = mock_query

        result, count = cache_handler.get_intersected({"key1"})
        assert result == {1, 2, 3}
        assert count == 1


def test_get_intersected_empty_result(cache_handler):
    cache_handler._get_partition_datatype = Mock(return_value="integer")
    cache_handler.filter_existing_keys = Mock(return_value={"key1", "key2"})
    cache_handler.cursor.fetchone.return_value = ([],)

    # Mock the get_intersected_sql method to avoid the as_string encoding issue
    with patch.object(cache_handler, "get_intersected_sql") as mock_sql:
        mock_query = Mock()
        mock_query.as_string.return_value = (
            "SELECT array_intersect_agg(partition_keys) FROM test_cache_cache_partition_key WHERE query_hash = ANY(ARRAY['key1', 'key2'])"
        )
        mock_sql.return_value = mock_query

        result, count = cache_handler.get_intersected({"key1", "key2"})
        assert result == set()
        assert count == 2


def test_get_intersected_lazy_no_existing_keys(cache_handler):
    cache_handler.filter_existing_keys = Mock(return_value=set())

    result, count = cache_handler.get_intersected_lazy({"key1", "key2"})
    assert result is None
    assert count == 0


def test_set_cache_large_numbers(cache_handler):
    large_set = {1000000, 2000000, 3000000}
    cache_handler.cursor.fetchone.side_effect = [(None,)]  # No existing partition
    cache_handler.set_cache("large_key", large_set)

    # Verify INSERT SQL was issued with correct key and large number values
    insert_found = False
    for call in cache_handler.cursor.execute.call_args_list:
        call_str = str(call)
        if "INSERT INTO" in call_str and "ON CONFLICT" in call_str:
            args = call[0]
            if len(args) > 1 and isinstance(args[1], tuple) and len(args[1]) >= 2:
                key_param, values_param = args[1][0], args[1][1]
                if key_param == "large_key":
                    insert_found = True
                    assert isinstance(values_param, list), f"Expected list for values, got {type(values_param)}"
                    assert set(values_param) == large_set, f"Expected {large_set}, got {set(values_param)}"
                    break
    assert insert_found, "Expected INSERT INTO ... ON CONFLICT SQL with 'large_key' not found"


def test_get_very_large_set(cache_handler):
    large_set = set(range(1, 1000001))  # 1 million elements
    # Mock the _get_partition_datatype method directly
    cache_handler._get_partition_datatype = Mock(return_value="integer")
    cache_handler.cursor.fetchone.return_value = (list(large_set),)
    result = cache_handler.get("large_key")
    assert result == large_set


def test_filter_existing_keys_all_exist(cache_handler):
    # Set up partition datatype and fetchall return value
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",), ("key3",)]
    existing_keys = cache_handler.filter_existing_keys({"key1", "key2", "key3"})
    assert existing_keys == {"key1", "key2", "key3"}


def test_filter_existing_keys_none_exist(cache_handler):
    # Set up partition datatype and fetchall return value
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    cache_handler.cursor.fetchall.return_value = []
    existing_keys = cache_handler.filter_existing_keys({"key1", "key2", "key3"})
    assert existing_keys == set()


def test_get_all_keys_empty(cache_handler):
    # Mock the metadata query to return None (no partition exists)
    cache_handler.cursor.fetchone.return_value = None
    all_keys = cache_handler.get_all_keys("non_existent_partition")
    assert all_keys == []


def test_delete_non_existent_key(cache_handler):
    # Set up partition datatype
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    result = cache_handler.delete("non_existent_key")
    assert result is True
    cache_handler.db.commit.assert_called()

    # Verify DELETE SQL was issued with correct key parameter
    delete_calls = []
    for call in cache_handler.cursor.execute.call_args_list:
        call_str = str(call)
        if "DELETE FROM" in call_str:
            delete_calls.append(call)

    # Should have two DELETE calls: one from cache table, one from queries table
    assert len(delete_calls) == 2, f"Expected 2 DELETE calls, got {len(delete_calls)}"

    # Verify the key is passed as a parameter in the first DELETE
    cache_delete_params = delete_calls[0][0][1]
    assert cache_delete_params == ("non_existent_key",), f"Expected ('non_existent_key',), got {cache_delete_params}"


def test_set_cache_update_existing(cache_handler):
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    result = cache_handler.set_cache("existing_key", {4, 5, 6})
    assert result is True
    cache_handler.db.commit.assert_called()

    # Verify INSERT with ON CONFLICT DO UPDATE SET clause (upsert pattern)
    insert_found = False
    for call in cache_handler.cursor.execute.call_args_list:
        call_str = str(call)
        if "INSERT INTO" in call_str and "ON CONFLICT" in call_str and "DO UPDATE SET" in call_str:
            args = call[0]
            if len(args) > 1 and isinstance(args[1], tuple) and len(args[1]) >= 2:
                key_param, values_param = args[1][0], args[1][1]
                if key_param == "existing_key":
                    insert_found = True
                    # Verify the ON CONFLICT clause includes EXCLUDED reference for upsert
                    assert "EXCLUDED" in call_str, f"Expected EXCLUDED in ON CONFLICT clause, got: {call_str}"
                    assert isinstance(values_param, list), f"Expected list for values, got {type(values_param)}"
                    assert set(values_param) == {4, 5, 6}, f"Expected {{4, 5, 6}}, got {set(values_param)}"
                    break
    assert insert_found, "Expected INSERT INTO ... ON CONFLICT ... DO UPDATE SET SQL with 'existing_key' not found"


def test_database_connection_error():
    with patch("psycopg.connect", side_effect=psycopg.OperationalError("Connection failed")):
        with pytest.raises(psycopg.OperationalError):
            PostgreSQLArrayCacheHandler("test_db", "localhost", "test_user", "test_password", 5432, "test_prefix")


def test_very_long_key(cache_handler):
    long_key = "x" * 1000
    cache_handler.cursor.fetchone.side_effect = [(None,)]
    cache_handler.set_cache(long_key, {1, 2, 3})

    # Verify INSERT SQL was issued with the full long key and correct values
    insert_found = False
    for call in cache_handler.cursor.execute.call_args_list:
        call_str = str(call)
        if "INSERT INTO" in call_str and "ON CONFLICT" in call_str:
            args = call[0]
            if len(args) > 1 and isinstance(args[1], tuple) and len(args[1]) >= 2:
                key_param, values_param = args[1][0], args[1][1]
                if key_param == long_key:
                    insert_found = True
                    assert len(key_param) == 1000, f"Expected key length 1000, got {len(key_param)}"
                    assert isinstance(values_param, list), f"Expected list for values, got {type(values_param)}"
                    assert set(values_param) == {1, 2, 3}, f"Expected {{1, 2, 3}}, got {set(values_param)}"
                    break
    assert insert_found, "Expected INSERT INTO ... ON CONFLICT SQL with long key not found"


def test_unicode_key(cache_handler):
    unicode_key = "测试键"
    cache_handler.cursor.fetchone.side_effect = [(None,)]
    cache_handler.set_cache(unicode_key, {1, 2, 3})

    # Verify INSERT SQL was issued with the unicode key and correct values
    insert_found = False
    for call in cache_handler.cursor.execute.call_args_list:
        call_str = str(call)
        if "INSERT INTO" in call_str and "ON CONFLICT" in call_str:
            args = call[0]
            if len(args) > 1 and isinstance(args[1], tuple) and len(args[1]) >= 2:
                key_param, values_param = args[1][0], args[1][1]
                if key_param == unicode_key:
                    insert_found = True
                    assert key_param == "测试键", f"Expected unicode key '测试键', got {key_param}"
                    assert isinstance(values_param, list), f"Expected list for values, got {type(values_param)}"
                    assert set(values_param) == {1, 2, 3}, f"Expected {{1, 2, 3}}, got {set(values_param)}"
                    break
    assert insert_found, "Expected INSERT INTO ... ON CONFLICT SQL with unicode key not found"
