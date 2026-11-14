"""
UNLOGGED_TABLE_IN: the key table of TMP_TABLE_IN as an unlogged table, which parallel workers can read; the script
drops it itself after the SELECT, as UNLOGGED_TABLE_JOIN does. planner_collapse_limit opens a script with SET LOCAL
join_collapse_limit / from_collapse_limit. Key tables get the type of the keys (integer, float, timestamp, text).

The SQL-text tests need no database. TestAgainstPostgreSQL runs with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/
PASSWORD/DB and is skipped without them.
"""

import os
import re
import uuid
from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from partitioncache.apply_cache import (
    apply_cache,
    apply_cache_lazy,
    extend_query_with_partition_keys,
    extend_query_with_partition_keys_lazy,
)
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy

QUERY = "SELECT * FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE u.active = TRUE"
LAZY_SUBQUERY = "SELECT region_id FROM cache_regions"
LIMIT_PREFIX = "SET LOCAL join_collapse_limit = {n};\nSET LOCAL from_collapse_limit = {n};\n"
SCRIPT = (
    r"CREATE UNLOGGED TABLE (partitioncache_unl_[0-9a-f]{{16}}) AS \({source}\);\n"
    r"CREATE INDEX \1_idx ON \1 \({column}\);\n"
    r"ANALYZE \1;\n"
    r"(SELECT [^;]*);\n"
    r"DROP TABLE IF EXISTS \1"
)
EAGER_KEYS = "SELECT partition_key FROM (VALUES (1), (2)) AS pcache_keys(partition_key)"
SCRIPT_METHODS = ["TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"]


def _match(script: str, source: str, column: str) -> tuple[str, str]:
    """Return (table name, SELECT) of a script that has exactly the unlogged shape."""
    match = re.fullmatch(SCRIPT.format(source=re.escape(source), column=column), script)
    assert match, script
    return match.group(1), match.group(2)


def _eager(method="UNLOGGED_TABLE_IN", keys=None, **kwargs):
    return extend_query_with_partition_keys(QUERY, keys or {1, 2}, "region_id", method=method, p0_alias=kwargs.pop("p0_alias", "u"), **kwargs)


def _lazy(method="UNLOGGED_TABLE_IN", **kwargs):
    return extend_query_with_partition_keys_lazy(QUERY, LAZY_SUBQUERY, "region_id", method=method, p0_alias=kwargs.pop("p0_alias", "u"), **kwargs)


class TestEager:
    def test_script_shape(self):
        table, select = _match(_eager(), EAGER_KEYS, "partition_key")
        assert f"u.region_id IN (SELECT partition_key FROM {table})" in select
        assert "TEMPORARY" not in select

    def test_without_alias_restricts_the_p0_table(self):
        script = extend_query_with_partition_keys(QUERY, {1}, "region_id", method="UNLOGGED_TABLE_IN")
        assert "u.region_id IN (SELECT partition_key FROM partitioncache_unl_" in script

    def test_string_keys_are_escaped(self):
        assert "(VALUES ('o''brien'))" in _eager(keys={"o'brien"})

    def test_other_dialects_are_rejected(self):
        with pytest.raises(ValueError, match="UNLOGGED_TABLE_IN is only supported for PostgreSQL"):
            _eager(sql_dialect="duckdb")

    def test_empty_key_set_builds_no_table(self):
        script = extend_query_with_partition_keys(QUERY, set(), "region_id", method="UNLOGGED_TABLE_IN", p0_alias="u")
        assert "UNLOGGED" not in script and "DROP" not in script

    def test_apply_cache(self):
        handler = Mock(spec=AbstractCacheHandler)
        handler.get_intersected.return_value = ({1, 2}, 2)
        enhanced, stats = apply_cache(QUERY, handler, "region_id", method="UNLOGGED_TABLE_IN", p0_alias="u")
        assert stats["enhanced"] == 1
        _match(enhanced, EAGER_KEYS, "partition_key")


class TestLazy:
    def test_script_shape(self):
        table, select = _match(_lazy(), LAZY_SUBQUERY, "region_id")
        assert f"u.region_id IN (SELECT region_id FROM {table})" in select

    def test_other_dialects_are_rejected(self):
        with pytest.raises(ValueError, match="only supported for PostgreSQL"):
            _lazy(sql_dialect="duckdb")

    def test_apply_cache_lazy(self):
        handler = Mock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (LAZY_SUBQUERY, 2)
        enhanced, stats = apply_cache_lazy(QUERY, handler, "region_id", method="UNLOGGED_TABLE_IN", p0_alias="u")
        assert stats["enhanced"] == 1
        _match(enhanced, LAZY_SUBQUERY, "region_id")

    def test_complete_miss_builds_no_table(self):
        handler = Mock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (None, 0)
        enhanced, stats = apply_cache_lazy(QUERY, handler, "region_id", method="UNLOGGED_TABLE_IN", planner_collapse_limit=4)
        assert enhanced == QUERY and stats["enhanced"] == 0


class TestPlannerCollapseLimit:
    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    @pytest.mark.parametrize("method", SCRIPT_METHODS)
    def test_two_set_local_lines_come_first(self, method, lazy):
        script = (_lazy if lazy else _eager)(method, planner_collapse_limit=6)
        assert script.startswith(LIMIT_PREFIX.format(n=6))
        assert script.count("SET LOCAL") == 2

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    @pytest.mark.parametrize("method", SCRIPT_METHODS)
    def test_off_by_default(self, method, lazy):
        assert "SET LOCAL" not in (_lazy if lazy else _eager)(method)

    def test_unlogged_script_keeps_its_shape_after_the_settings(self):
        script = _eager(planner_collapse_limit=1)
        _match(script[len(LIMIT_PREFIX.format(n=1)) :], EAGER_KEYS, "partition_key")

    @pytest.mark.parametrize("value", [0, -1, True, 2.5, "3"])
    def test_not_a_positive_integer(self, value):
        with pytest.raises(ValueError, match="positive integer"):
            _eager(planner_collapse_limit=value)

    @pytest.mark.parametrize("method", ["IN", "VALUES"])
    def test_single_statement_eager_methods_raise(self, method):
        with pytest.raises(ValueError, match="script"):
            _eager(method, planner_collapse_limit=4)

    def test_single_statement_lazy_method_raises(self):
        with pytest.raises(ValueError, match="script"):
            _lazy("IN_SUBQUERY", planner_collapse_limit=4)

    def test_other_dialects_raise(self):
        with pytest.raises(ValueError, match="only supported for PostgreSQL"):
            _eager("TMP_TABLE_IN", planner_collapse_limit=4, sql_dialect="duckdb")

    def test_empty_key_set_is_a_single_statement_without_settings(self):
        script = extend_query_with_partition_keys(QUERY, set(), "region_id", method="TMP_TABLE_IN", p0_alias="u", planner_collapse_limit=4)
        assert "SET LOCAL" not in script

    def test_apply_functions_check_before_the_lookup(self):
        handler = Mock(spec=AbstractCacheHandler_Lazy)
        with pytest.raises(ValueError):
            apply_cache(QUERY, handler, "region_id", method="IN", planner_collapse_limit=4)
        with pytest.raises(ValueError):
            apply_cache_lazy(QUERY, handler, "region_id", method="IN_SUBQUERY", planner_collapse_limit=4)
        handler.get_intersected.assert_not_called()
        handler.get_intersected_lazy.assert_not_called()

    def test_apply_cache_lazy_passes_it_through(self):
        handler = Mock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (LAZY_SUBQUERY, 2)
        enhanced, _ = apply_cache_lazy(QUERY, handler, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="u", planner_collapse_limit=3)
        assert enhanced.startswith(LIMIT_PREFIX.format(n=3) + "CREATE UNLOGGED TABLE ")

    def test_population_checks_the_option(self):
        from partitioncache.population import populate_query_variants

        with pytest.raises(ValueError, match="script"):
            populate_query_variants([], "pk", Mock(), Mock(), method="IN", planner_collapse_limit=4)


class TestTypedKeyTables:
    """A TEXT key column does not compare with a numeric or timestamp partition key column."""

    @pytest.mark.parametrize(
        ("keys", "sql_type"),
        [
            ({1, 2}, "INT"),
            ({1, 2**40}, "BIGINT"),
            ({1.5, 2.25}, "DOUBLE PRECISION"),
            ({1, 2.5}, "DOUBLE PRECISION"),
            ({datetime(2024, 1, 1, 12, 30)}, "TIMESTAMP"),
            ({datetime(2024, 1, 1, tzinfo=UTC)}, "TIMESTAMPTZ"),
            ({"a", "b"}, "TEXT"),
        ],
    )
    def test_temporary_key_table_type(self, keys, sql_type):
        script = extend_query_with_partition_keys(QUERY, keys, "region_id", method="TMP_TABLE_IN", p0_alias="u")
        assert f"(partition_key {sql_type} PRIMARY KEY)" in script

    def test_temporary_key_table_escapes_string_keys(self):
        script = extend_query_with_partition_keys(QUERY, {"o'brien"}, "region_id", method="TMP_TABLE_JOIN", p0_alias="u")
        assert "(VALUES('o''brien'))" in script

    def test_float_keys_are_numbers(self):
        script = extend_query_with_partition_keys(QUERY, {1.5}, "region_id", method="TMP_TABLE_IN", p0_alias="u")
        assert "(VALUES(1.5))" in script

    @pytest.mark.parametrize(("keys", "cast"), [({1.5}, "DOUBLE PRECISION"), ({datetime(2024, 1, 1)}, "TIMESTAMP")])
    def test_unlogged_key_table_casts_to_the_key_type(self, keys, cast):
        script = extend_query_with_partition_keys(QUERY, keys, "region_id", method="UNLOGGED_TABLE_IN", p0_alias="u")
        assert f"SELECT CAST(partition_key AS {cast}) AS partition_key FROM (VALUES (" in script


class TestMonitor:
    """pcache-monitor's --cache-optimization-method UNLOGGED_TABLE_* and --planner-collapse-limit reach the rewrite."""

    @staticmethod
    def _args(method, limit, lazy):
        args = Mock()
        args.enable_cache_optimization, args.disable_cache_optimization, args.prefer_lazy_optimization = True, False, lazy
        args.max_restriction_keys, args.min_cache_hits, args.db_backend = None, 1, "postgresql"
        args.cache_optimization_method, args.planner_collapse_limit, args.join_every_table = method, limit, False
        args.min_component_size, args.follow_graph, args.no_auto_detect_partition_join = 1, True, False
        args.max_component_size, args.partition_join_table, args.bucket_steps = None, None, 1.0
        args.skip_partition_key_joins, args.max_conditions_removed, args.grid_cell_size = [], None, None
        return args

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    @pytest.mark.parametrize("limit", [None, 5])
    @pytest.mark.parametrize("method", ["UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"])
    def test_method_and_limit(self, method, limit, lazy):
        from partitioncache.cli.monitor_cache_queue import apply_cache_optimization

        handler = Mock(spec=AbstractCacheHandler_Lazy if lazy else AbstractCacheHandler)
        handler.get_intersected.return_value = ({1, 2}, 2)
        if lazy:
            handler.get_intersected_lazy.return_value = (LAZY_SUBQUERY, 2)
        query, applied, stats = apply_cache_optimization(QUERY, "h", "region_id", "integer", handler, self._args(method, limit, lazy))
        assert applied and stats["method_used"] == method
        assert query.startswith(LIMIT_PREFIX.format(n=limit) + "CREATE UNLOGGED TABLE " if limit else "CREATE UNLOGGED TABLE ")
        assert re.search(r";\nDROP TABLE IF EXISTS partitioncache_unl_[0-9a-f]{16}$", query)


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
    """Sent as one string, each script runs in one transaction, applies its settings and leaves no key table behind."""

    @pytest.fixture
    def db(self):
        import psycopg

        params = _connection_params()
        table = f"unl_in_points_{uuid.uuid4().hex[:8]}"
        with psycopg.connect(**params, autocommit=True) as conn:
            conn.execute(
                f"CREATE TABLE {table} AS SELECT g AS id, g % 10 AS region_id, (g % 10) + 0.5 AS score, "
                f"TIMESTAMP '2024-01-01' + (g % 10) * INTERVAL '1 day' AS day, 'r' || (g % 10) AS label FROM generate_series(1, 100) g"
            )
        yield params, table
        with psycopg.connect(**params, autocommit=True) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table}")

    @staticmethod
    def _run(params: dict, script: str, autocommit: bool = True) -> list:
        import psycopg

        from partitioncache.db_handler.postgres import fetch_final_result_set

        with psycopg.connect(**params, autocommit=autocommit) as conn:
            with conn.cursor() as cur:
                cur.execute(script)
                rows = fetch_final_result_set(cur)
            if not autocommit:
                conn.commit()
        return rows

    @staticmethod
    def _leftover_tables(params: dict) -> int:
        import psycopg

        with psycopg.connect(**params, autocommit=True) as conn:
            return conn.execute("SELECT count(*) FROM pg_tables WHERE tablename LIKE 'partitioncache_unl_%'").fetchone()[0]  # type: ignore[index]

    @pytest.mark.parametrize("autocommit", [True, False], ids=["autocommit", "transaction"])
    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    def test_rows_settings_and_no_table_left(self, db, lazy, autocommit):
        params, table = db
        query = f"SELECT count(*), current_setting('join_collapse_limit'), current_setting('from_collapse_limit') FROM {table} AS p"
        if lazy:
            keys = "SELECT g AS region_id FROM generate_series(1, 3) AS g"
            script = extend_query_with_partition_keys_lazy(query, keys, "region_id", method="UNLOGGED_TABLE_IN", p0_alias="p", planner_collapse_limit=2)
        else:
            script = extend_query_with_partition_keys(query, {1, 2, 3}, "region_id", method="UNLOGGED_TABLE_IN", p0_alias="p", planner_collapse_limit=2)
        assert self._run(params, script, autocommit) == [(30, "2", "2")]
        assert self._leftover_tables(params) == 0
        assert self._run(params, "SELECT current_setting('join_collapse_limit')") == [("8",)]  # reset with the transaction

    @pytest.mark.parametrize("method", SCRIPT_METHODS)
    @pytest.mark.parametrize(
        ("column", "keys"),
        [
            ("score", {1.5, 2.5}),
            ("day", {datetime(2024, 1, 2), datetime(2024, 1, 3)}),
            ("label", {"r1", "r2"}),
        ],
        ids=["float", "timestamp", "text"],
    )
    def test_typed_keys_restrict_typed_columns(self, db, method, column, keys):
        params, table = db
        script = extend_query_with_partition_keys(f"SELECT count(*) FROM {table} AS p", keys, column, method=method, p0_alias="p")
        assert self._run(params, script) == [(20,)]
        assert self._leftover_tables(params) == 0

    @pytest.mark.parametrize("method", SCRIPT_METHODS)
    def test_string_key_with_a_quote(self, db, method):
        params, table = db
        script = extend_query_with_partition_keys(f"SELECT count(*) FROM {table} AS p", {"r1", "o'brien"}, "label", method=method, p0_alias="p")
        assert self._run(params, script) == [(10,)]

    def test_postgres_db_handler(self, db):
        from partitioncache.db_handler.postgres import PostgresDBHandler

        params, table = db
        script = extend_query_with_partition_keys(f"SELECT p.id FROM {table} AS p", {1}, "region_id", method="UNLOGGED_TABLE_IN", p0_alias="p", planner_collapse_limit=3)
        handler = PostgresDBHandler(params["host"], params["port"], params["user"], params["password"], params["dbname"])
        try:
            assert sorted(handler.execute(script)) == list(range(1, 101, 10))
        finally:
            handler.close()
        assert self._leftover_tables(params) == 0
