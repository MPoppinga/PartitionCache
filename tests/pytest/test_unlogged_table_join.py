"""
UNLOGGED_TABLE_JOIN: the key table of TMP_TABLE_JOIN as an unlogged table, which PostgreSQL can read in parallel
workers (temporary tables are parallel restricted). The table is a regular one, so the caller drops it after
the fetch with get_unlogged_cleanup_sql. The tests check SQL text and call order; they need no database.
"""

import re
from unittest.mock import MagicMock, Mock

import pytest

from partitioncache.apply_cache import (
    apply_cache,
    apply_cache_lazy,
    extend_query_with_partition_keys,
    extend_query_with_partition_keys_lazy,
    get_unlogged_cleanup_sql,
)
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy
from partitioncache.db_handler.postgres import PostgresDBHandler

QUERY = "SELECT * FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE u.active = TRUE"
LAZY_SUBQUERY = "SELECT region_id FROM cache_regions"
SETUP = (
    r"CREATE UNLOGGED TABLE (partitioncache_unl_[0-9a-f]{{16}}) AS \({source}\);\n"
    r"CREATE INDEX \1_idx ON \1 \({column}\);\n"
    r"ANALYZE \1;\n"
    r"(SELECT [^;]*)"
)


def _match(script: str, source: str, column: str) -> tuple[str, str]:
    """Return (table name, final SELECT) of a script that has exactly the UNLOGGED_TABLE_JOIN shape."""
    match = re.fullmatch(SETUP.format(source=re.escape(source), column=column), script)
    assert match, script
    return match.group(1), match.group(2)


class TestEager:
    def test_script_shape(self):
        script = extend_query_with_partition_keys(QUERY, {1, 2}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        table, select = _match(script, "SELECT partition_key FROM (VALUES (1), (2)) AS pcache_keys(partition_key)", "partition_key")
        assert f"INNER JOIN {table} AS tmp_u ON tmp_u.partition_key = u.region_id" in select
        assert "tmp_o" not in select
        assert "DROP" not in script and "TEMPORARY" not in script

    def test_without_alias_joins_every_table(self):
        script = extend_query_with_partition_keys(QUERY, {1}, "region_id", method="UNLOGGED_TABLE_JOIN")
        assert "tmp_u.partition_key = u.region_id" in script and "tmp_o.partition_key = o.region_id" in script

    def test_index_and_analyze_do_not_depend_on_analyze_tmp_table(self):
        script = extend_query_with_partition_keys(QUERY, {1}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u", analyze_tmp_table=False)
        assert "CREATE INDEX" in script and "ANALYZE" in script

    def test_string_keys_are_escaped(self):
        script = extend_query_with_partition_keys(QUERY, {"o'brien"}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        assert "(VALUES ('o''brien'))" in script

    def test_other_dialects_are_rejected(self):
        with pytest.raises(ValueError, match="only supported for PostgreSQL"):
            extend_query_with_partition_keys(QUERY, {1}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u", sql_dialect="duckdb")

    def test_apply_cache(self):
        handler = Mock(spec=AbstractCacheHandler)
        handler.get_intersected.return_value = ({1, 2}, 2)
        enhanced, stats = apply_cache(QUERY, handler, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        assert stats["enhanced"] == 1
        assert get_unlogged_cleanup_sql(enhanced) is not None


class TestLazy:
    def test_script_shape(self):
        script = extend_query_with_partition_keys_lazy(QUERY, LAZY_SUBQUERY, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        table, select = _match(script, LAZY_SUBQUERY, "region_id")
        assert f"INNER JOIN {table} AS tmp_u ON tmp_u.region_id = u.region_id" in select
        assert "tmp_o" not in select
        assert "DROP" not in script and "TEMPORARY" not in script

    def test_other_dialects_are_rejected(self):
        with pytest.raises(ValueError, match="only supported for PostgreSQL"):
            extend_query_with_partition_keys_lazy(QUERY, LAZY_SUBQUERY, "region_id", method="UNLOGGED_TABLE_JOIN", sql_dialect="duckdb")

    def test_apply_cache_lazy(self):
        handler = Mock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (LAZY_SUBQUERY, 2)
        enhanced, stats = apply_cache_lazy(QUERY, handler, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        assert stats["enhanced"] == 1
        assert get_unlogged_cleanup_sql(enhanced) is not None

    def test_complete_miss_builds_no_table(self):
        handler = Mock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (None, 0)
        enhanced, stats = apply_cache_lazy(QUERY, handler, "region_id", method="UNLOGGED_TABLE_JOIN")
        assert enhanced == QUERY
        assert stats["enhanced"] == 0
        assert get_unlogged_cleanup_sql(enhanced) is None


class TestCleanupSql:
    @pytest.mark.parametrize("lazy", [False, True])
    def test_drops_the_table_of_the_script(self, lazy):
        if lazy:
            script = extend_query_with_partition_keys_lazy(QUERY, LAZY_SUBQUERY, "region_id", method="UNLOGGED_TABLE_JOIN")
        else:
            script = extend_query_with_partition_keys(QUERY, {1, 2}, "region_id", method="UNLOGGED_TABLE_JOIN")
        table = re.match(r"CREATE UNLOGGED TABLE (\w+)", script).group(1)  # type: ignore[union-attr]
        assert get_unlogged_cleanup_sql(script) == f"DROP TABLE IF EXISTS {table}"

    @pytest.mark.parametrize(
        "sql",
        [
            QUERY,
            extend_query_with_partition_keys(QUERY, {1, 2}, "region_id", method="IN", p0_alias="u"),
            extend_query_with_partition_keys(QUERY, {1, 2}, "region_id", method="TMP_TABLE_JOIN", p0_alias="u"),
            extend_query_with_partition_keys(QUERY, set(), "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u"),
            "SELECT 'CREATE UNLOGGED TABLE partitioncache_unl_00ff AS (SELECT 1)'",
            "CREATE UNLOGGED TABLE my_table AS (SELECT 1)",
        ],
        ids=["original", "in", "tmp_table_join", "empty_intersection", "text_inside_the_query", "foreign_table"],
    )
    def test_none_for_other_sql(self, sql):
        assert get_unlogged_cleanup_sql(sql) is None


class TestPostgresDBHandlerExecute:
    """execute drops the key table after the fetch and before the commit, on the same connection."""

    @staticmethod
    def _handler(rows):
        handler = PostgresDBHandler.__new__(PostgresDBHandler)
        calls = MagicMock()
        handler.conn, handler.cur = calls.conn, calls.cur
        handler.cur.description = ["pk"]
        handler.cur.fetchall.return_value = rows
        handler.cur.nextset.return_value = None
        return handler, calls

    def test_drop_after_fetch_before_commit(self):
        script = extend_query_with_partition_keys(QUERY, {1, 2}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        handler, calls = self._handler([(7,), (8,)])

        assert handler.execute(script) == [7, 8]

        steps = [name for name, args, _ in calls.mock_calls if name in ("cur.execute", "cur.fetchall", "conn.commit")]
        assert steps == ["cur.execute", "cur.fetchall", "cur.execute", "conn.commit"]
        executed = [args[0] for name, args, _ in calls.mock_calls if name == "cur.execute"]
        assert executed == [script, get_unlogged_cleanup_sql(script)]

    def test_no_drop_for_other_queries(self):
        handler, calls = self._handler([(7,)])
        assert handler.execute(QUERY) == [7]
        handler.cur.execute.assert_called_once_with(QUERY)

    def test_failed_script_is_neither_dropped_nor_committed(self):
        """The error leaves the transaction open and aborted; its rollback removes the table."""
        script = extend_query_with_partition_keys(QUERY, {1, 2}, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u")
        handler, calls = self._handler([])
        handler.cur.execute.side_effect = RuntimeError("canceling statement due to statement timeout")

        with pytest.raises(RuntimeError):
            handler.execute(script)

        handler.cur.execute.assert_called_once_with(script)
        handler.conn.commit.assert_not_called()
