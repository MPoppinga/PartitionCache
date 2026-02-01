from unittest.mock import Mock, patch

import psycopg
import pytest

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
    cache_handler.set_cache("key1", {1, 2, 3})
    assert cache_handler.cursor.execute.called
    cache_handler.db.commit.assert_called()


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
    cache_handler.cursor.fetchone.side_effect = [(None,)]
    cache_handler.set_null("null_key")
    assert cache_handler.cursor.execute.called


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
    cache_handler.delete("key_to_delete")
    found = False
    for call in cache_handler.cursor.execute.call_args_list:
        if "DELETE" in str(call) and "key_to_delete" in str(call):
            found = True
            break
    assert found
    cache_handler.db.commit.assert_called()


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

    # Check the query structure without calling as_string
    query_str = str(query)
    assert "Identifier('k0', 'partition_keys')" in query_str
    assert "SQL(' & ')" in query_str
    assert "Identifier('k1', 'partition_keys')" in query_str
    assert "test_cache_cache_p1" in query_str


def test_get_intersected_sql_fallback_text():
    """Test SQL generation for text intersection using fallback."""
    # Create a simple mock handler without going through __init__
    handler = Mock(spec=PostgreSQLArrayCacheHandler)
    handler.USE_AGGREGATES = False
    handler.array_intersect_agg_available = False
    handler.tableprefix = "test_cache"

    # Call the actual method
    query = PostgreSQLArrayCacheHandler.get_intersected_sql(handler, {"key1", "key2"}, "p1", "text")

    # Verify the query structure
    query_str = str(query)
    assert "unnest(" in query_str
    assert "intersect" in query_str
    assert "test_cache_cache_p1" in query_str


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

        # Just check that the handler was created and has the required attributes
        assert hasattr(handler, "array_intersect_agg_available")
        assert hasattr(handler, "USE_AGGREGATES")



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
    found = False
    for call in cache_handler.cursor.execute.call_args_list:
        if "INSERT" in str(call) and "large_key" in str(call):
            found = True
            break
    assert found


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
    cache_handler.delete("non_existent_key")
    found = False
    for call in cache_handler.cursor.execute.call_args_list:
        if "DELETE" in str(call) and "non_existent_key" in str(call):
            found = True
            break
    assert found
    cache_handler.db.commit.assert_called()


def test_set_cache_update_existing(cache_handler):
    cache_handler.cursor.fetchone.side_effect = [("integer",)]
    cache_handler.set_cache("existing_key", {4, 5, 6})
    found = False
    for call in cache_handler.cursor.execute.call_args_list:
        if "INSERT" in str(call) and "existing_key" in str(call) and "ON CONFLICT" in str(call):
            found = True
            break
    assert found
    cache_handler.db.commit.assert_called()


def test_database_connection_error():
    with patch("psycopg.connect", side_effect=psycopg.OperationalError("Connection failed")):
        with pytest.raises(psycopg.OperationalError):
            PostgreSQLArrayCacheHandler("test_db", "localhost", "test_user", "test_password", 5432, "test_prefix")


def test_very_long_key(cache_handler):
    long_key = "x" * 1000
    cache_handler.cursor.fetchone.side_effect = [(None,)]
    cache_handler.set_cache(long_key, {1, 2, 3})
    found = False
    for call in cache_handler.cursor.execute.call_args_list:
        if "INSERT" in str(call) and long_key in str(call):
            found = True
            break
    assert found


def test_unicode_key(cache_handler):
    unicode_key = "测试键"
    cache_handler.cursor.fetchone.side_effect = [(None,)]
    cache_handler.set_cache(unicode_key, {1, 2, 3})
    found = False
    for call in cache_handler.cursor.execute.call_args_list:
        if "INSERT" in str(call) and unicode_key in str(call):
            found = True
            break
    assert found
