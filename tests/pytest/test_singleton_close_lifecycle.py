from unittest.mock import Mock, patch

import pytest

from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler
from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.redis_set import RedisCacheHandler
from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler


@pytest.fixture(autouse=True)
def reset_singleton_state():
    RedisCacheHandler._instance = None
    RedisCacheHandler._refcount = 0
    PostgreSQLArrayCacheHandler._instance = None
    PostgreSQLArrayCacheHandler._refcount = 0
    DuckDBBitCacheHandler._instance = None
    DuckDBBitCacheHandler._refcount = 0
    RocksDictCacheHandler._instance = None
    RocksDictCacheHandler._refcount = 0
    RocksDictCacheHandler._current_path = None

    yield

    RedisCacheHandler._instance = None
    RedisCacheHandler._refcount = 0
    PostgreSQLArrayCacheHandler._instance = None
    PostgreSQLArrayCacheHandler._refcount = 0
    DuckDBBitCacheHandler._instance = None
    DuckDBBitCacheHandler._refcount = 0
    RocksDictCacheHandler._instance = None
    RocksDictCacheHandler._refcount = 0
    RocksDictCacheHandler._current_path = None


def test_redis_close_non_singleton_does_not_affect_singleton_refcount():
    singleton_db = Mock()
    non_singleton_db = Mock()

    with patch("partitioncache.cache_handler.redis_abstract.redis.Redis", side_effect=[singleton_db, non_singleton_db]):
        singleton = RedisCacheHandler.get_instance(db_name=0, db_host="localhost", db_password="pw", db_port=6379)
        non_singleton = RedisCacheHandler(db_name=0, db_host="localhost", db_password="pw", db_port=6379)

        non_singleton.close()

        assert RedisCacheHandler._instance is singleton
        assert RedisCacheHandler._refcount == 1
        non_singleton_db.close.assert_called_once()
        singleton_db.close.assert_not_called()

        singleton.close()
        singleton_db.close.assert_called_once()
        assert RedisCacheHandler._instance is None
        assert RedisCacheHandler._refcount == 0


def test_postgresql_close_non_singleton_does_not_affect_singleton_refcount():
    singleton_db = Mock()
    singleton_cursor = Mock()
    singleton_db.cursor.return_value = singleton_cursor

    non_singleton_db = Mock()
    non_singleton_cursor = Mock()
    non_singleton_db.cursor.return_value = non_singleton_cursor

    config = {
        "db_name": "db",
        "db_host": "localhost",
        "db_user": "user",
        "db_password": "pw",
        "db_port": 5432,
        "db_tableprefix": "pc",
    }

    with patch("psycopg.connect", side_effect=[singleton_db, non_singleton_db]), patch(
        "partitioncache.cache_handler.postgresql_abstract.threading.active_count", return_value=1
    ):
        singleton = PostgreSQLArrayCacheHandler.get_instance(**config)
        non_singleton = PostgreSQLArrayCacheHandler(**config)

        non_singleton.close()

        assert PostgreSQLArrayCacheHandler._instance is singleton
        assert PostgreSQLArrayCacheHandler._refcount == 1
        non_singleton_cursor.close.assert_called_once()
        non_singleton_db.close.assert_called_once()
        singleton_cursor.close.assert_not_called()

        singleton.close()
        singleton_cursor.close.assert_called_once()
        singleton_db.close.assert_called_once()
        assert PostgreSQLArrayCacheHandler._instance is None
        assert PostgreSQLArrayCacheHandler._refcount == 0


def test_duckdb_close_non_singleton_does_not_affect_singleton_refcount():
    singleton_conn = Mock()
    non_singleton_conn = Mock()

    with patch("duckdb.connect", side_effect=[singleton_conn, non_singleton_conn]), patch(
        "partitioncache.cache_handler.duckdb_bit.threading.active_count", return_value=1
    ):
        singleton = DuckDBBitCacheHandler.get_instance(database=":memory:", table_prefix="pc", bitsize=64)
        non_singleton = DuckDBBitCacheHandler(database=":memory:", table_prefix="pc", bitsize=64)

        non_singleton.close()

        assert DuckDBBitCacheHandler._instance is singleton
        assert DuckDBBitCacheHandler._refcount == 1
        non_singleton_conn.close.assert_called_once()
        singleton_conn.close.assert_not_called()

        singleton.close()
        singleton_conn.close.assert_called_once()
        assert DuckDBBitCacheHandler._instance is None
        assert DuckDBBitCacheHandler._refcount == 0


def test_rocksdict_close_non_singleton_does_not_affect_singleton_refcount(tmp_path):
    singleton_db = Mock()
    non_singleton_db = Mock()
    db_path = str(tmp_path / "cache.db")

    with patch("partitioncache.cache_handler.rocksdict_abstract.Rdict", side_effect=[singleton_db, non_singleton_db]):
        singleton = RocksDictCacheHandler.get_instance(db_path)
        non_singleton = RocksDictCacheHandler(db_path)

        non_singleton.close()

        assert RocksDictCacheHandler._instance is singleton
        assert RocksDictCacheHandler._refcount == 1
        non_singleton_db.close.assert_called_once()
        singleton_db.close.assert_not_called()

        singleton.close()
        singleton_db.close.assert_called_once()
        assert RocksDictCacheHandler._instance is None
        assert RocksDictCacheHandler._refcount == 0
