"""EXPLAIN ANALYZE of integration scripts in the unified benchmark runner (examples/benchmark/run_benchmark.py).

The script runs in ONE transaction that is rolled back, and the EXPLAIN targets its last SELECT, not its last
statement: a key table dropped after the SELECT must not become the EXPLAIN target, and setup tables must not stay.
The database tests run with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB and are skipped without them.
"""

from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path

import pytest

from partitioncache.apply_cache import extend_query_with_partition_keys

RUNNER = Path(__file__).resolve().parents[2] / "examples" / "benchmark" / "run_benchmark.py"


@pytest.fixture(scope="module")
def runner():
    spec = importlib.util.spec_from_file_location("run_benchmark_explain_under_test", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


class TestExplainTarget:
    def test_single_statement(self, runner):
        assert runner._explain_target("SELECT 1") == ([], "SELECT 1")

    def test_setup_then_select(self, runner):
        script = "CREATE TEMPORARY TABLE t (k INT) ON COMMIT DROP;\nINSERT INTO t VALUES (1);\nSELECT * FROM t"
        assert runner._explain_target(script) == (["CREATE TEMPORARY TABLE t (k INT) ON COMMIT DROP", "INSERT INTO t VALUES (1)"], "SELECT * FROM t")

    def test_statements_after_the_select_are_not_the_target(self, runner):
        script = "CREATE UNLOGGED TABLE u AS (SELECT 1 AS k);\nANALYZE u;\nSELECT * FROM u;\nDROP TABLE IF EXISTS u"
        assert runner._explain_target(script) == (["CREATE UNLOGGED TABLE u AS (SELECT 1 AS k)", "ANALYZE u"], "SELECT * FROM u")

    def test_with_query_and_leading_comment(self, runner):
        script = "CREATE TEMPORARY TABLE t (k INT);\n-- the query\nWITH x AS (SELECT k FROM t) SELECT * FROM x"
        setup, target = runner._explain_target(script)
        assert setup == ["CREATE TEMPORARY TABLE t (k INT)"] and target.endswith("WITH x AS (SELECT k FROM t) SELECT * FROM x")


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
    @pytest.fixture
    def executor(self, runner):
        import psycopg

        params = _connection_params()
        table = f"explain_points_{uuid.uuid4().hex[:8]}"
        executor = runner.PostgreSQLExecutor.__new__(runner.PostgreSQLExecutor)
        executor.conn = psycopg.connect(**params, autocommit=True)  # as PostgreSQLExecutor.__init__ connects
        executor.conn.execute(f"CREATE TABLE {table} AS SELECT g AS id, g % 10 AS region_id FROM generate_series(1, 100) g")
        yield executor, table
        executor.conn.execute(f"DROP TABLE IF EXISTS {table}")
        executor.conn.close()

    @staticmethod
    def _exists(executor, name: str) -> bool:
        return executor.conn.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()[0]

    def test_temp_table_script(self, executor):
        """ON COMMIT DROP: under autocommit a CREATE run on its own was committed, and its table dropped, at once."""
        executor, table = executor
        script = extend_query_with_partition_keys(f"SELECT count(*) FROM {table} AS p", {1, 2}, "region_id", method="TMP_TABLE_JOIN", p0_alias="p")
        plan, ms = executor.explain_analyze(script)
        assert any("tmp_cache_keys_" in line for line in plan) and ms > 0

    def test_script_that_drops_its_key_table_after_the_select(self, executor):
        executor, table = executor
        key_table = f"partitioncache_unl_{uuid.uuid4().hex[:16]}"
        script = (
            f"CREATE UNLOGGED TABLE {key_table} AS (SELECT 1 AS k UNION ALL SELECT 2);\n"
            f"ANALYZE {key_table};\n"
            f"SELECT count(*) FROM {table} AS p INNER JOIN {key_table} AS tmp_p ON tmp_p.k = p.region_id;\n"
            f"DROP TABLE IF EXISTS {key_table}"
        )
        plan, _ = executor.explain_analyze(script)
        assert any(key_table in line for line in plan)
        assert not self._exists(executor, key_table)

    def test_setup_is_rolled_back_when_the_explain_fails(self, executor):
        import psycopg

        executor, table = executor
        key_table = f"partitioncache_unl_{uuid.uuid4().hex[:16]}"
        script = f"CREATE UNLOGGED TABLE {key_table} AS (SELECT 1 AS k);\nSELECT count(*) FROM {table} AS p WHERE p.id / 0 = 1;\nDROP TABLE IF EXISTS {key_table}"
        with pytest.raises(psycopg.errors.DivisionByZero):
            executor.explain_analyze(script)
        assert not self._exists(executor, key_table)
        assert executor.conn.execute("SELECT 1").fetchone() == (1,)  # the connection is usable again

    def test_setup_is_rolled_back_also_without_a_drop(self, executor):
        """A key table the script leaves to its caller is not committed by the EXPLAIN either."""
        executor, table = executor
        key_table = f"partitioncache_unl_{uuid.uuid4().hex[:16]}"
        script = f"CREATE UNLOGGED TABLE {key_table} AS (SELECT 1 AS k);\nSELECT count(*) FROM {table} AS p INNER JOIN {key_table} AS tmp_p ON tmp_p.k = p.region_id"
        executor.explain_analyze(script)
        assert not self._exists(executor, key_table)
