"""
UNLOGGED_TABLE_JOIN: the key table of TMP_TABLE_JOIN as an unlogged table, which PostgreSQL can read in parallel
workers (temporary tables are parallel restricted). The table is a regular one, so the script drops it itself
after the SELECT; sent as one string, the script is one transaction and leaves no table behind.

The SQL-text tests need no database. TestAgainstPostgreSQL runs with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/
PASSWORD/DB and is skipped without them.
"""

import os
import re
import uuid
from unittest.mock import MagicMock, Mock

import pytest

from partitioncache.apply_cache import (
    apply_cache,
    apply_cache_lazy,
    extend_query_with_partition_keys,
    extend_query_with_partition_keys_lazy,
)
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy
from partitioncache.db_handler.postgres import PostgresDBHandler

QUERY = "SELECT * FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE u.active = TRUE"
LAZY_SUBQUERY = "SELECT region_id FROM cache_regions"
SCRIPT = (
    r"CREATE UNLOGGED TABLE (partitioncache_unl_[0-9a-f]{{16}}) AS \({source}\);\n"
    r"CREATE INDEX \1_idx ON \1 \({column}\);\n"
    r"ANALYZE \1;\n"
    r"(SELECT [^;]*);\n"
    r"DROP TABLE IF EXISTS \1"
)


def _match(script: str, source: str, column: str) -> tuple[str, str]:
    """Return (table name, final SELECT) of a script that has exactly the UNLOGGED_TABLE_JOIN shape."""
    match = re.fullmatch(SCRIPT.format(source=re.escape(source), column=column), script)
    assert match, script
    return match.group(1), match.group(2)


def _table_of(script: str) -> str:
    match = re.match(r"CREATE UNLOGGED TABLE (partitioncache_unl_[0-9a-f]+) AS ", script)
    assert match, script
    return match.group(1)


class TestEager:
    def test_script_shape(self):
        script = extend_query_with_partition_keys(QUERY, {1, 2}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        table, select = _match(script, "SELECT partition_key FROM (VALUES (1), (2)) AS pcache_keys(partition_key)", "partition_key")
        assert f"INNER JOIN {table} AS tmp_u ON tmp_u.partition_key = u.region_id" in select
        assert "tmp_o" not in select
        assert "TEMPORARY" not in script

    def test_without_alias_joins_every_table(self):
        script = extend_query_with_partition_keys(QUERY, {1}, "region_id", method="UNLOGGED_TABLE_JOIN")
        assert "tmp_u.partition_key = u.region_id" in script and "tmp_o.partition_key = o.region_id" in script

    def test_index_and_analyze_do_not_depend_on_analyze_tmp_table(self):
        script = extend_query_with_partition_keys(QUERY, {1}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u", analyze_tmp_table=False)
        assert "CREATE INDEX" in script and "ANALYZE" in script

    def test_string_keys_are_escaped(self):
        script = extend_query_with_partition_keys(QUERY, {"o'brien"}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        assert "(VALUES ('o''brien'))" in script

    def test_empty_key_set_builds_no_table(self):
        script = extend_query_with_partition_keys(QUERY, set(), "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        assert "UNLOGGED" not in script and "DROP" not in script

    def test_other_dialects_are_rejected(self):
        with pytest.raises(ValueError, match="only supported for PostgreSQL"):
            extend_query_with_partition_keys(QUERY, {1}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u", sql_dialect="duckdb")

    def test_apply_cache(self):
        handler = Mock(spec=AbstractCacheHandler)
        handler.get_intersected.return_value = ({1, 2}, 2)
        enhanced, stats = apply_cache(QUERY, handler, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        assert stats["enhanced"] == 1
        assert enhanced.endswith(f"DROP TABLE IF EXISTS {_table_of(enhanced)}")


class TestLazy:
    def test_script_shape(self):
        script = extend_query_with_partition_keys_lazy(QUERY, LAZY_SUBQUERY, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        table, select = _match(script, LAZY_SUBQUERY, "region_id")
        assert f"INNER JOIN {table} AS tmp_u ON tmp_u.region_id = u.region_id" in select
        assert "tmp_o" not in select
        assert "TEMPORARY" not in script

    def test_other_dialects_are_rejected(self):
        with pytest.raises(ValueError, match="only supported for PostgreSQL"):
            extend_query_with_partition_keys_lazy(QUERY, LAZY_SUBQUERY, "region_id", method="UNLOGGED_TABLE_JOIN", sql_dialect="duckdb")

    def test_apply_cache_lazy(self):
        handler = Mock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (LAZY_SUBQUERY, 2)
        enhanced, stats = apply_cache_lazy(QUERY, handler, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        assert stats["enhanced"] == 1
        assert enhanced.endswith(f"DROP TABLE IF EXISTS {_table_of(enhanced)}")

    def test_complete_miss_builds_no_table(self):
        handler = Mock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (None, 0)
        enhanced, stats = apply_cache_lazy(QUERY, handler, "region_id", method="UNLOGGED_TABLE_JOIN")
        assert enhanced == QUERY
        assert stats["enhanced"] == 0


class _ResultSets:
    """A cursor whose script produced these result sets, as (description, rows); None is a statement without rows."""

    rowcount = -1

    def __init__(self, sets):
        self._sets, self._i = sets, 0

    @property
    def description(self):
        current = self._sets[self._i]
        return None if current is None else current[0]

    def fetchall(self):
        return self._sets[self._i][1]

    def nextset(self):
        self._i += 1
        return True if self._i < len(self._sets) else None


class TestPostgresDBHandlerExecute:
    """execute sends the script once and reads the SELECT's rows; the DROP after it has no result set."""

    def test_rows_of_the_select_before_the_drop(self):
        script = extend_query_with_partition_keys(QUERY, {1, 2}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        handler = PostgresDBHandler.__new__(PostgresDBHandler)
        handler.conn = MagicMock()
        handler.cur = _ResultSets([None, None, None, (["pk"], [(7,), (8,)]), None])
        handler.cur.execute = Mock()  # type: ignore[attr-defined]

        assert handler.execute(script) == [7, 8]

        handler.cur.execute.assert_called_once_with(script)  # type: ignore[attr-defined]
        handler.conn.commit.assert_called_once()


def _connection_params() -> dict:
    if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    return {
        "host": os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
        "port": int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
        "user": os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        "password": os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        "dbname": os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
    }


class TestAgainstPostgreSQL:
    """Sent as one string, the script runs in one transaction: no unlogged table is left behind, also on errors."""

    @pytest.fixture
    def db(self):
        import psycopg

        params = _connection_params()
        table = f"unl_points_{uuid.uuid4().hex[:8]}"
        with psycopg.connect(**params, autocommit=True) as conn:
            conn.execute(f"CREATE TABLE {table} AS SELECT g AS id, g % 10 AS region_id FROM generate_series(1, 100) g")
        yield params, table
        with psycopg.connect(**params, autocommit=True) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table}")

    @staticmethod
    def _exists(params: dict, table: str) -> bool:
        import psycopg

        with psycopg.connect(**params, autocommit=True) as conn:
            return conn.execute("SELECT to_regclass(%s) IS NOT NULL", (table,)).fetchone()[0]  # type: ignore[index]

    @staticmethod
    def _script(table: str, lazy: bool, where: str = "TRUE") -> str:
        query = f"SELECT count(*) FROM {table} AS p WHERE {where}"
        if lazy:
            keys = "SELECT g AS region_id FROM generate_series(1, 3) AS g"
            return extend_query_with_partition_keys_lazy(query, keys, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="p")
        return extend_query_with_partition_keys(query, {1, 2, 3}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="p")

    @pytest.mark.parametrize("autocommit", [True, False], ids=["autocommit", "transaction"])
    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    def test_rows_and_no_table_left(self, db, autocommit, lazy):
        import psycopg

        from partitioncache.db_handler.postgres import fetch_final_result_set

        params, table = db
        script = self._script(table, lazy)
        with psycopg.connect(**params, autocommit=autocommit) as conn:
            with conn.cursor() as cur:
                cur.execute(script)
                assert fetch_final_result_set(cur) == [(30,)]
            if not autocommit:
                conn.commit()
        assert not self._exists(params, _table_of(script))

    @pytest.mark.parametrize("autocommit", [True, False], ids=["autocommit", "transaction"])
    def test_error_after_the_create_leaves_no_table(self, db, autocommit):
        import psycopg

        params, table = db
        script = self._script(table, lazy=False, where="p.id / 0 = 1")
        with psycopg.connect(**params, autocommit=autocommit) as conn:
            with pytest.raises(psycopg.errors.DivisionByZero):
                conn.execute(script)
            if not autocommit:
                conn.rollback()
        assert not self._exists(params, _table_of(script))

    def test_connection_lost_before_the_drop_leaves_no_table(self, db):
        """The error aborts the open transaction; the connection closes without rollback or commit."""
        import psycopg

        params, table = db
        script = self._script(table, lazy=False, where="p.id / 0 = 1")
        conn = psycopg.connect(**params)
        with pytest.raises(psycopg.errors.DivisionByZero):
            conn.execute(script)
        conn.pgconn.finish()  # drop the connection without ending the transaction
        assert not self._exists(params, _table_of(script))

    def test_postgres_db_handler(self, db):
        params, table = db
        script = self._script(table, lazy=True)
        handler = PostgresDBHandler(params["host"], params["port"], params["user"], params["password"], params["dbname"])
        try:
            assert handler.execute(script) == [30]
        finally:
            handler.close()
        assert not self._exists(params, _table_of(script))
