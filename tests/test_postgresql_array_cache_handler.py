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
    return Mock()


@pytest.fixture
def cache_handler(mock_db, mock_cursor):
    with patch("psycopg.connect", return_value=mock_db):
        handler = PostgreSQLArrayCacheHandler(
            db_name="test_db", db_host="localhost", db_user="test_user", db_password="test_password", db_port=5432, db_table="test_cache_table"
        )
        handler.cursor.return_value = mock_cursor  # type: ignore
        handler.db = mock_db  # type: ignore
        return handler


def test_set_set(cache_handler):
    cache_handler.set_set("key1", {1, 2, 3})
    cache_handler.cursor.execute.assert_called_with("INSERT INTO test_cache_table VALUES (%s, %s)", ("key1", [1, 2, 3]))
    cache_handler.db.commit.assert_called()


def test_set_set_empty(cache_handler):
    cache_handler.set_set("empty_key", set())

    # Execute only for init calls
    assert cache_handler.cursor.execute.call_count == 4


def test_set_set_str_type(cache_handler):
    with pytest.raises(ValueError):
        cache_handler.set_set("str_key", {"a", "b", "c"}, settype=str)


def test_get(cache_handler):
    cache_handler.cursor.fetchone.return_value = ([1, 2, 3],)
    result = cache_handler.get("key1")
    assert result == {1, 2, 3}
    cache_handler.cursor.execute.assert_called_with("SELECT value FROM test_cache_table WHERE key = %s", ("key1",))


def test_get_none(cache_handler):
    cache_handler.cursor.fetchone.return_value = None
    result = cache_handler.get("non_existent_key")
    assert result is None


def test_get_str_type(cache_handler):
    with pytest.raises(ValueError):
        cache_handler.get("str_key", settype=str)


def test_set_null(cache_handler):
    cache_handler.set_null("null_key")
    cache_handler.cursor.execute.assert_called_with("INSERT INTO test_cache_table VALUES (%s, %s)", ("null_key", None))


def test_is_null(cache_handler):
    cache_handler.cursor.fetchone.return_value = [None]
    assert cache_handler.is_null("null_key") is True

    cache_handler.cursor.fetchone.return_value = [1, 2, 3]
    assert cache_handler.is_null("non_null_key") is False


def test_exists(cache_handler):
    cache_handler.cursor.fetchone.return_value = ([1, 2, 3],)
    assert cache_handler.exists("existing_key") is True

    cache_handler.cursor.fetchone.return_value = None
    assert cache_handler.exists("non_existent_key") is False


def test_get_intersected(cache_handler):
    cache_handler.filter_existing_keys = Mock(return_value={"key1", "key2"})
    cache_handler.cursor.fetchone.return_value = ([3, 4],)

    result, count = cache_handler.get_intersected({"key1", "key2", "key3"})
    assert result == {3, 4}
    assert count == 2


def test_get_intersected_no_existing_keys(cache_handler):
    cache_handler.filter_existing_keys = Mock(return_value=set())

    result, count = cache_handler.get_intersected({"key1", "key2"})
    assert result is None
    assert count == 0


def test_filter_existing_keys(cache_handler):
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",)]

    existing_keys = cache_handler.filter_existing_keys({"key1", "key2", "key3"})
    assert existing_keys == {"key1", "key2"}


def test_get_intersected_lazy(cache_handler):
    cache_handler.filter_existing_keys = Mock(return_value={"key1", "key2"})

    query, count = cache_handler.get_intersected_lazy({"key1", "key2", "key3"})
    assert isinstance(query, sql.Composed)
    assert count == 2


def test_get_all_keys(cache_handler):
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",), ("key3",)]

    all_keys = cache_handler.get_all_keys()
    # Ensure set comparison to ignore order
    assert set(all_keys) == {"key1", "key2", "key3"}


def test_delete(cache_handler):
    cache_handler.delete("key_to_delete")
    cache_handler.cursor.execute.assert_called_with("DELETE FROM test_cache_table WHERE key = %s", ("key_to_delete",))
    cache_handler.db.commit.assert_called()


# Add these new tests after the existing ones


def test_get_intersected_single_key(cache_handler):
    cache_handler.filter_existing_keys = Mock(return_value={"key1"})
    cache_handler.cursor.fetchone.return_value = ([1, 2, 3],)

    result, count = cache_handler.get_intersected({"key1"})
    assert result == {1, 2, 3}
    assert count == 1


def test_get_intersected_empty_result(cache_handler):
    cache_handler.filter_existing_keys = Mock(return_value={"key1", "key2"})
    cache_handler.cursor.fetchone.return_value = ([],)

    result, count = cache_handler.get_intersected({"key1", "key2"})
    assert result == set()
    assert count == 2


def test_get_intersected_lazy_no_existing_keys(cache_handler):
    cache_handler.filter_existing_keys = Mock(return_value=set())

    result, count = cache_handler.get_intersected_lazy({"key1", "key2"})
    assert result is None
    assert count == 0


def test_set_set_large_numbers(cache_handler):
    large_set = {1000000, 2000000, 3000000}
    cache_handler.set_set("large_key", large_set)
    cache_handler.cursor.execute.assert_called_with("INSERT INTO test_cache_table VALUES (%s, %s)", ("large_key", [1000000, 2000000, 3000000]))


def test_get_very_large_set(cache_handler):
    large_set = set(range(1, 1000001))  # 1 million elements
    cache_handler.cursor.fetchone.return_value = (list(large_set),)

    result = cache_handler.get("large_key")
    assert result == large_set


def test_filter_existing_keys_all_exist(cache_handler):
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",), ("key3",)]

    existing_keys = cache_handler.filter_existing_keys({"key1", "key2", "key3"})
    assert existing_keys == {"key1", "key2", "key3"}


def test_filter_existing_keys_none_exist(cache_handler):
    cache_handler.cursor.fetchall.return_value = []

    existing_keys = cache_handler.filter_existing_keys({"key1", "key2", "key3"})
    assert existing_keys == set()


def test_get_all_keys_empty(cache_handler):
    cache_handler.cursor.fetchall.return_value = []

    all_keys = cache_handler.get_all_keys()
    assert all_keys == []


def test_delete_non_existent_key(cache_handler):
    cache_handler.delete("non_existent_key")
    cache_handler.cursor.execute.assert_called_with("DELETE FROM test_cache_table WHERE key = %s", ("non_existent_key",))
    cache_handler.db.commit.assert_called()


def test_set_set_update_existing(cache_handler):
    cache_handler.set_set("existing_key", {1, 2, 3})
    cache_handler.cursor.execute.assert_called_with("INSERT INTO test_cache_table VALUES (%s, %s)", ("existing_key", [1, 2, 3]))

    # Update the existing key
    cache_handler.set_set("existing_key", {4, 5, 6})
    cache_handler.cursor.execute.assert_called_with("INSERT INTO test_cache_table VALUES (%s, %s)", ("existing_key", [4, 5, 6]))


# Add a test for error handling
def test_database_connection_error():
    with patch("psycopg.connect", side_effect=psycopg.OperationalError):
        with pytest.raises(psycopg.OperationalError):
            PostgreSQLArrayCacheHandler(
                db_name="test_db", db_host="localhost", db_user="test_user", db_password="test_password", db_port=5432, db_table="test_cache_table"
            )


# Test for handling very long keys
def test_very_long_key(cache_handler):
    long_key = "a" * 1000  # 1000 character key
    cache_handler.set_set(long_key, {1, 2, 3})
    cache_handler.cursor.execute.assert_called_with("INSERT INTO test_cache_table VALUES (%s, %s)", (long_key, [1, 2, 3]))


# Test for handling Unicode keys
def test_unicode_key(cache_handler):
    unicode_key = "こんにちは"  # Hello in Japanese
    cache_handler.set_set(unicode_key, {1, 2, 3})
    cache_handler.cursor.execute.assert_called_with("INSERT INTO test_cache_table VALUES (%s, %s)", (unicode_key, [1, 2, 3]))
