import pytest
from unittest.mock import Mock, patch
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
from psycopg import sql
from bitarray import bitarray

INIT_CALLS = [
    (
        "CREATE TABLE IF NOT EXISTS test_bit_cache_table_cache "
        "(query_hash TEXT PRIMARY KEY, partition_keys BIT VARYING, "
        "partition_keys_count integer NOT NULL GENERATED ALWAYS AS "
        "(length(replace(partition_keys::text, '0','')) ) STORED);"
    ),
    (
        "CREATE TABLE IF NOT EXISTS test_bit_cache_table_queries "
        "(query_hash TEXT NOT NULL PRIMARY KEY, query TEXT NOT NULL, "
        "partition_key TEXT NOT NULL, "
        "last_seen TIMESTAMP NOT NULL DEFAULT now());"
    ),
]


@pytest.fixture
def mock_db():
    return Mock()


@pytest.fixture
def mock_cursor():
    return Mock()


@pytest.fixture
def cache_handler(mock_db, mock_cursor):
    with patch("psycopg.connect", return_value=mock_db):
        handler = PostgreSQLBitCacheHandler(
            db_name="test_db",
            db_host="localhost",
            db_user="test_user",
            db_password="test_password",
            db_port=5432,
            db_tableprefix="test_bit_cache_table",
            bitsize=100,
        )
        handler.cursor.return_value = mock_cursor  # type: ignore
        handler.db.return_value = mock_db  # type: ignore
        return handler


def test_init(cache_handler):
    assert cache_handler.tableprefix == "test_bit_cache_table"
    assert isinstance(cache_handler, PostgreSQLBitCacheHandler)
    assert isinstance(cache_handler, AbstractCacheHandler_Lazy)

    assert cache_handler.cursor.execute.call_count == len(INIT_CALLS)  # type: ignore


def test_set_set(cache_handler):
    class FakeBitArray:
        def __init__(self, size):
            self.bits = [0] * size
        def setall(self, val):
            self.bits = [val] * len(self.bits)
        def __setitem__(self, idx, val):
            self.bits[idx] = val
        def to01(self):
            return ''.join(str(b) for b in self.bits)
    with patch("partitioncache.cache_handler.postgresql_bit.bitarray", FakeBitArray):
        cache_handler._get_partition_datatype = lambda pk: "integer"
        cache_handler._get_partition_bitsize = lambda pk: 100
        cache_handler.cursor.execute.reset_mock()
        cache_handler.db.commit.reset_mock()
        cache_handler.set_set("key1", {1, 2, 3})
        found = False
        for call in cache_handler.cursor.execute.call_args_list:
            if "INSERT" in str(call) and "key1" in str(call):
                found = True
                break
        assert found
        cache_handler.db.commit.assert_called()


def test_set_query(cache_handler):
    test_query = "SELECT * FROM test_bit_cache_table_cache WHERE query_hash = %s"
    cache_handler.set_query("key1", test_query)
    # Update expected SQL and params to match implementation
    expected_sql = sql.SQL(
        "INSERT INTO {0} (query_hash, partition_key, query) VALUES (%s, %s, %s) ON CONFLICT (query_hash, partition_key) DO UPDATE SET query = EXCLUDED.query, last_seen = now()"
    ).format(sql.Identifier("test_bit_cache_table_queries"))
    expected_params = ("key1", "partition_key", test_query)
    cache_handler.cursor.execute.assert_called_with(expected_sql, expected_params)
    cache_handler.db.commit.assert_called()


def test_set_set_empty(cache_handler):
    cache_handler.cursor.execute.reset_mock()
    cache_handler.set_set("empty_key", set())

    # Execute only for init calls
    assert cache_handler.cursor.execute.call_count == 0


def test_get(cache_handler):
    # Patch bitarray to return a mock with a .search method
    class MockBitArray:
        def __init__(self, bitstr):
            self.bitstr = bitstr
        def search(self, pattern):
            return [i for i, c in enumerate(self.bitstr) if c == '1']
    with patch("partitioncache.cache_handler.postgresql_bit.bitarray", MockBitArray):
        cache_handler._get_partition_datatype = lambda pk: "integer"
        cache_handler.cursor.fetchone.side_effect = [("0101" + "0" * 96,)]
        cache_handler.cursor.execute.reset_mock()
        result = cache_handler.get("key1")
        assert result == {1, 3}
        found = False
        for call in cache_handler.cursor.execute.call_args_list:
            if "SELECT" in str(call) and "key1" in str(call):
                found = True
                break
        assert found


def test_get_none(cache_handler):
    cache_handler.cursor.fetchone.return_value = None
    result = cache_handler.get("non_existent_key")
    assert result is None


def test_get_str_type(cache_handler):
    # First call returns datatype, second call returns a valid bit string
    cache_handler.cursor.fetchone.side_effect = [("integer",), ("0101",)]
    result = cache_handler.get("str_key")
    assert result == {1, 3}


def test_set_null(cache_handler):
    # Mock partition datatype
    cache_handler.cursor.fetchone.return_value = ("integer",)
    cache_handler.cursor.execute.reset_mock()
    cache_handler.db.commit.reset_mock()
    cache_handler.set_null("null_key")
    # Check for an INSERT call with the correct key and None value
    found = False
    for call in cache_handler.cursor.execute.call_args_list:
        if "INSERT" in str(call) and "null_key" in str(call):
            found = True
            break
    assert found
    cache_handler.db.commit.assert_called()


def test_is_null_null_key(cache_handler):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler.cursor.fetchone.return_value = (None,)
    assert cache_handler.is_null("null_key") is True


def test_is_null_not_null_key(cache_handler):
    cache_handler.cursor.fetchone.return_value = ("0101",)
    assert cache_handler.is_null("not_null_key") is False


def test_exists(cache_handler):
    cache_handler.cursor.fetchone.return_value = ["0101"]
    assert cache_handler.exists("existing_key") is True

    cache_handler.cursor.fetchone.return_value = None
    assert cache_handler.exists("non_existent_key") is False


def test_filter_existing_keys(cache_handler):
    # Mock partition datatype and fetchall
    cache_handler.cursor.fetchone.return_value = ("integer",)
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",)]
    existing_keys = cache_handler.filter_existing_keys({"key1", "key2", "key3"})
    assert existing_keys == {"key1", "key2"}


def test_get_intersected_lazy(cache_handler):
    cache_handler._get_partition_datatype = lambda pk: "integer"
    cache_handler._get_partition_bitsize = lambda pk: 100
    cache_handler.cursor.fetchone.side_effect = [("integer",), (100,), (100,), (100,)]
    cache_handler.filter_existing_keys = Mock(return_value={"key1", "key2"})
    cache_handler.get_intersected_sql_wk = Mock(return_value="SELECT BIT_AND(value) FROM ...")
    query, count = cache_handler.get_intersected_lazy({"key1", "key2", "key3"})
    assert query is not None
    assert count == 2
    cache_handler.get_intersected_sql_wk.assert_called_with({"key1", "key2"}, "partition_key")


def test_get_all_keys(cache_handler):
    # Mock partition datatype and fetchall
    cache_handler.cursor.fetchone.return_value = ("integer",)
    cache_handler.cursor.fetchall.return_value = [("key1",), ("key2",), ("key3",)]
    all_keys = cache_handler.get_all_keys("partition_key")
    assert set(all_keys) == {"key1", "key2", "key3"}


def test_delete(cache_handler):
    # Mock partition datatype
    cache_handler.cursor.fetchone.return_value = ("integer",)
    cache_handler.cursor.execute.reset_mock()
    cache_handler.db.commit.reset_mock()
    cache_handler.delete("key_to_delete")
    # Check for a DELETE call with the correct key
    found = False
    for call in cache_handler.cursor.execute.call_args_list:
        if "DELETE" in str(call) and "key_to_delete" in str(call):
            found = True
            break
    assert found
    cache_handler.db.commit.assert_called()


def test_close(cache_handler):
    cache_handler.close()
    cache_handler.cursor.close.assert_called()
    cache_handler.db.close.assert_called()
