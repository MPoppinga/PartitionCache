import pytest
from unittest.mock import Mock, patch
from psycopg import sql
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

INIT_CALLS = [
    "CREATE TABLE IF NOT EXISTS test_bit_cache_table (key TEXT PRIMARY KEY, value bit(101));",
    # Add other necessary SQL commands if applicable
]

@pytest.fixture
def mock_db():
    return Mock()

@pytest.fixture
def mock_cursor():
    return Mock()

@pytest.fixture
def cache_handler(mock_db, mock_cursor):
    with patch('psycopg.connect', return_value=mock_db):
        handler = PostgreSQLBitCacheHandler(
            db_name="test_db",
            db_host="localhost",
            db_user="test_user",
            db_password="test_password",
            db_port=5432,
            db_table="test_bit_cache_table",
            bitsize=100
        )
        handler.cursor.return_value = mock_cursor  # type: ignore
        handler.db.return_value = mock_db  # type: ignore
        return handler

def test_init(cache_handler):
    assert cache_handler.tablename == "test_bit_cache_table"
    assert isinstance(cache_handler, PostgreSQLBitCacheHandler)
    assert isinstance(cache_handler, AbstractCacheHandler_Lazy)
    
    # Check that the necessary SQL commands were executed
    expected_calls = INIT_CALLS
    
    for call in expected_calls:
        cache_handler.cursor.execute.assert_any_call(call) # type: ignore
    
    assert cache_handler.cursor.execute.call_count == len(expected_calls) # type: ignore

def test_set_set(cache_handler):
    cache_handler.set_set("key1", {1, 2, 3})
    expected_bitarray = '0' * 101
    bitarray = ['0'] * 101
    for k in {1, 2, 3}:
        bitarray[k] = '1'
    expected_bitarray = ''.join(bitarray)

    cache_handler.cursor.execute.assert_called_with(
        "INSERT INTO test_bit_cache_table VALUES (%s, %s)",
        ("key1", expected_bitarray)
    )
    cache_handler.db.commit.assert_called()

def test_set_set_empty(cache_handler):
    cache_handler.set_set("empty_key", set())
    
    # Execute only for init calls
    assert cache_handler.cursor.execute.call_count == len(INIT_CALLS)

def test_get(cache_handler):
    expected_bits = '0101' + '0' * 97  # Example bitarray
    cache_handler.cursor.fetchone.return_value = (expected_bits,)
    result = cache_handler.get("key1")
    assert result == {1, 3}
    cache_handler.cursor.execute.assert_called_with(
        "SELECT value FROM test_bit_cache_table WHERE key = %s",
        ("key1",)
    )

def test_get_none(cache_handler):
    cache_handler.cursor.fetchone.return_value = None
    result = cache_handler.get("non_existent_key")
    assert result is None

def test_get_str_type(cache_handler):
    with pytest.raises(ValueError):
        cache_handler.get("str_key", settype=str)

def test_set_null(cache_handler):
    cache_handler.set_null("null_key")
    cache_handler.cursor.execute.assert_called_with(
        "INSERT INTO test_bit_cache_table VALUES (%s, %s)",
        ("null_key", None)
    )

def test_is_null(cache_handler):
    cache_handler.cursor.fetchone.return_value = [None]
    assert cache_handler.is_null("null_key") is True

    cache_handler.cursor.fetchone.return_value = ['0101']
    assert cache_handler.is_null("non_null_key") is False

def test_exists(cache_handler):
    cache_handler.cursor.fetchone.return_value = ['0101']
    assert cache_handler.exists("existing_key") is True

    cache_handler.cursor.fetchone.return_value = None
    assert cache_handler.exists("non_existent_key") is False

def test_filter_existing_keys(cache_handler):
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",)]

    existing_keys = cache_handler.filter_existing_keys({"key1", "key2", "key3"})
    assert existing_keys == {"key1", "key2"}


def test_get_intersected_lazy(cache_handler):
    cache_handler.filter_existing_keys = Mock(return_value={"key1", "key2"})
    cache_handler.get_intersected_sql_wk = Mock(return_value="SELECT BIT_AND(value) FROM ...")

    query, count = cache_handler.get_intersected_lazy({"key1", "key2", "key3"})
    assert isinstance(query, sql.Composed)
    assert count == 2
    cache_handler.get_intersected_sql_wk.assert_called_with({"key1", "key2"})

def test_get_all_keys(cache_handler):
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",), ("key3",)]

    all_keys = cache_handler.get_all_keys()
    # Ensure set comparison to ignore order
    assert set(all_keys) == {"key1", "key2", "key3"}

def test_delete(cache_handler):
    cache_handler.delete("key_to_delete")
    cache_handler.cursor.execute.assert_called_with(
        "DELETE FROM test_bit_cache_table WHERE key = %s",
        ("key_to_delete",)
    )
    cache_handler.db.commit.assert_called()

def test_close(cache_handler):
    cache_handler.close()
    cache_handler.cursor.close.assert_called()
    cache_handler.db.close.assert_called()