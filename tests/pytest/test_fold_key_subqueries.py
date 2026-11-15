"""
fold_key_subqueries: conditions ``<alias>.<key> IN (<subquery>)`` move into the key table of a key-table method.

The key table keeps the keys for which ``key IN (<subquery>)`` is TRUE, the condition leaves the statement. The
results must equal those without folding. SQLite and DuckDB always run; TestPostgreSQL runs with
PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB and is skipped without them.
"""

import os
import re
import sqlite3
import uuid
from datetime import datetime
from decimal import Decimal

import pytest

from partitioncache.apply_cache import extend_query_with_partition_keys, extend_query_with_partition_keys_lazy

BASE = "SELECT DISTINCT pl.pocket_key FROM pocketkey_list AS pl, pts AS t1 WHERE t1.pocket_key = pl.pocket_key AND t1.kind = 'A'"
META = "pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr WHERE pr.prop_key = 39)"
QUERIES = {
    "geomine_like": f"{BASE} AND {META}",
    "any_form": f"{BASE} AND pl.pocket_key = ANY (SELECT pr.pocket_key FROM props AS pr WHERE pr.prop_key = 39)",
    "two_subqueries": f"{BASE} AND {META} AND t1.pocket_key IN (SELECT o.k FROM other AS o WHERE o.z = 1)",
    "correlated": f"{BASE} AND pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr WHERE pr.prop_key = t1.rnk)",
    "not_in": f"{BASE} AND pl.pocket_key NOT IN (SELECT pr.pocket_key FROM props AS pr WHERE pr.prop_key = 39 AND pr.pocket_key IS NOT NULL)",
    "or_context": f"{BASE} AND ({META} OR t1.rnk = 3)",
}
FOLDED = {"geomine_like": 1, "any_form": 1, "two_subqueries": 2, "correlated": 0, "not_in": 0, "or_context": 0}
# The cache keys: the keys of the query without its metadata conditions (pockets with an 'A' point), a relaxation
KEYS = set(range(2, 61, 2))
TABLE_METHODS = ["TMP_TABLE_IN", "TMP_TABLE_JOIN"]
PG_METHODS = TABLE_METHODS + ["UNLOGGED_TABLE_IN", "UNLOGGED_TABLE_JOIN"]


def _rows():
    pockets = [(k,) for k in range(1, 61)]
    pts = [(k, "AB"[k % 2], k % 7) for k in range(1, 61) for _ in range(2)]
    # props of prop 39: duplicates and NULLs, keys inside and outside the cache keys
    props = [(39, k) for k in range(1, 61, 3)] + [(39, k) for k in range(1, 61, 9)] + [(39, None), (40, 5), (3, 7)]
    other = [(k, 1 if k % 4 else 0) for k in range(1, 61)] + [(None, 1)]
    return pockets, pts, props, other


def _load(execute, executemany):
    pockets, pts, props, other = _rows()
    execute("CREATE TABLE pocketkey_list (pocket_key INTEGER)")
    execute("CREATE TABLE pts (pocket_key INTEGER, kind TEXT, rnk INTEGER)")
    execute("CREATE TABLE props (prop_key INTEGER, pocket_key INTEGER)")
    execute("CREATE TABLE other (k INTEGER, z INTEGER)")
    executemany("INSERT INTO pocketkey_list VALUES (?)", pockets)
    executemany("INSERT INTO pts VALUES (?, ?, ?)", pts)
    executemany("INSERT INTO props VALUES (?, ?)", props)
    executemany("INSERT INTO other VALUES (?, ?)", other)


def _sqlite():
    conn = sqlite3.connect(":memory:")
    _load(conn.execute, conn.executemany)

    def run(script):
        setup, _, select = script.rpartition(";")
        if setup.strip():
            conn.executescript(setup + ";")
        return sorted(row[0] for row in conn.execute(select))

    return run


def _duckdb():
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect()
    _load(conn.execute, conn.executemany)
    return lambda script: sorted(row[0] for row in conn.execute(script).fetchall())


def _number_of_filters(script: str) -> int:
    return script.count("IS NOT TRUE") + len(re.findall(r"pcache_folded\.pcache_key IN \(", script))


@pytest.mark.parametrize("method", TABLE_METHODS)
@pytest.mark.parametrize("name", list(QUERIES))
@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
def test_folding_keeps_the_rows(dialect, name, method):
    if dialect == "sqlite" and name == "any_form":
        pytest.skip("SQLite has no = ANY (subquery)")
    run = _sqlite() if dialect == "sqlite" else _duckdb()
    query = QUERIES[name]
    reference = run(query)
    assert set(reference) <= KEYS  # the cache keys are a superset of the result
    plain = extend_query_with_partition_keys(query, KEYS, "pocket_key", method=method, p0_alias="pl", sql_dialect=dialect)
    folded = extend_query_with_partition_keys(query, KEYS, "pocket_key", method=method, p0_alias="pl", sql_dialect=dialect, fold_key_subqueries=True)
    assert _number_of_filters(folded) == FOLDED[name]
    assert run(plain) == run(folded) == reference


class TestWhatFolds:
    @staticmethod
    def _main_statement(script: str) -> str:
        return script.rpartition(";")[2]

    def test_folded_condition_leaves_the_statement(self):
        script = extend_query_with_partition_keys(QUERIES["two_subqueries"], KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        main = self._main_statement(script)
        assert "props" not in main and "other" not in main and "t1.kind = 'A'" in main

    @pytest.mark.parametrize(
        ("condition", "reason"),
        [
            ("pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr ORDER BY pr.pocket_key LIMIT 3)", "limit"),
            ("pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr WHERE random() < 0.5)", "volatile"),
            ("pl.pocket_key IN (SELECT pr.pocket_key, pr.prop_key FROM props AS pr)", "two columns"),
            ("pl.pocket_key IN (SELECT c.pocket_key FROM keys_cte AS c)", "outer CTE"),
            ("pl.pocket_key IN (SELECT partition_key FROM props)", "unqualified key-table column"),
            ("pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr WHERE prop_key = 39)", "unqualified column"),
            ("pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr WHERE pr.prop_key = %s)", "bind parameter"),
            ("pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr WHERE pr.prop_key = $1)", "numbered parameter"),
            ("t9.pocket_key IN (SELECT pr.pocket_key FROM props AS pr)", "alias not tied by key equality"),
        ],
        ids=lambda value: value if len(value) < 30 else None,
    )
    def test_left_alone(self, condition, reason):
        query = f"WITH keys_cte AS (SELECT 1 AS pocket_key) {BASE.replace('pts AS t1', 'pts AS t1, pts AS t9')} AND {condition}"
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        assert "IS NOT TRUE" not in script, reason

    def test_a_bind_parameter_anywhere_stops_folding(self):
        """A folded subquery moves in front of the main statement, and with it its parameters' positions."""
        query = f"{BASE} AND t1.rnk = %s AND {META}"
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        assert "IS NOT TRUE" not in script

    def test_transaction_stable_time_functions_fold(self):
        query = f"{BASE} AND pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr WHERE pr.prop_key = 39 AND now() IS NOT NULL)"
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        assert script.count("IS NOT TRUE") == 1

    @pytest.mark.parametrize(
        ("keys", "mode", "folds"),
        [
            ({1, 2}, True, True),
            ({Decimal("1.5"), 2}, True, True),
            ({"a", "b"}, True, False),
            ({"a", "b"}, "any_key_type", True),
            ({datetime(2024, 1, 1)}, True, False),
            ({datetime(2024, 1, 1)}, "any_key_type", True),
        ],
    )
    def test_key_types(self, keys, mode, folds):
        """In the key table the condition compares in the table's type; text and timestamp keys need the caller's word."""
        script = extend_query_with_partition_keys(QUERIES["geomine_like"], keys, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=mode)
        assert ("IS NOT TRUE" in script) == folds

    @pytest.mark.parametrize(("datatype", "folds"), [("integer", True), ("text", False), ("float", False), (None, False)])
    def test_lazy_needs_the_datatype(self, datatype, folds):
        script = extend_query_with_partition_keys_lazy(
            QUERIES["geomine_like"], "SELECT 1 AS pocket_key", "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True, partition_datatype=datatype
        )
        assert ("pcache_folded" in script) == folds

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="fold_key_subqueries is False, True or"):
            extend_query_with_partition_keys(QUERIES["geomine_like"], KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries="yes")

    def test_outer_join_does_not_tie_the_alias(self):
        query = (
            "SELECT DISTINCT pl.pocket_key FROM pocketkey_list AS pl LEFT JOIN pts AS t1 ON t1.pocket_key = pl.pocket_key "
            "WHERE t1.pocket_key IN (SELECT pr.pocket_key FROM props AS pr)"
        )
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        assert "IS NOT TRUE" not in script

    def test_inner_join_ties_the_alias(self):
        query = (
            "SELECT DISTINCT pl.pocket_key FROM pocketkey_list AS pl JOIN pts AS t1 ON t1.pocket_key = pl.pocket_key "
            "WHERE t1.pocket_key IN (SELECT pr.pocket_key FROM props AS pr)"
        )
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        assert script.count("IS NOT TRUE") == 1

    @pytest.mark.parametrize("method", ["IN", "VALUES"])
    def test_methods_without_a_key_table_raise(self, method):
        with pytest.raises(ValueError, match="key-table method"):
            extend_query_with_partition_keys(QUERIES["geomine_like"], KEYS, "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=True)

    def test_off_by_default(self):
        script = extend_query_with_partition_keys(QUERIES["geomine_like"], KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl")
        assert "IS NOT TRUE" not in script and "props" in self._main_statement(script)


def _pg_params() -> dict:
    if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    return {
        "host": os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
        "port": int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
        "user": os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        "password": os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        "dbname": os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
    }


class TestPostgreSQL:
    # The metadata subquery of GeoMine: keys come from unnest() over an array column of a property view
    UNNEST_META = "pl.pocket_key IN (SELECT unnest(pa.pocket_keys) FROM props_arrays AS pa WHERE pa.prop_key = 39)"
    LAZY_KEYS = "SELECT g AS pocket_key FROM generate_series(2, 60, 2) AS g"

    @pytest.fixture(scope="class")
    def pg(self):
        psycopg = pytest.importorskip("psycopg")
        from partitioncache.db_handler.postgres import fetch_final_result_set

        schema = f"fold_{uuid.uuid4().hex[:8]}"
        conn = psycopg.connect(**_pg_params(), autocommit=True)
        conn.execute(f"CREATE SCHEMA {schema}")
        conn.execute(f"SET search_path TO {schema}")
        with conn.cursor() as cur:
            _load(cur.execute, lambda sql, rows: cur.executemany(sql.replace("?", "%s"), rows))
            cur.execute("CREATE TABLE props_arrays AS SELECT prop_key, array_agg(pocket_key) AS pocket_keys FROM props GROUP BY prop_key")
            cur.execute("ANALYZE")

        def run(script):
            with conn.cursor() as cur:
                cur.execute(script)
                return sorted(row[0] for row in fetch_final_result_set(cur))

        def explain(script):
            """The plan of the script's main SELECT, with its setup run first in the same rolled back transaction."""
            setup, _, tail = script.rpartition("SELECT DISTINCT")
            main, _, _drop = ("SELECT DISTINCT" + tail).partition(";\nDROP")
            with conn.transaction(force_rollback=True), conn.cursor() as cur:
                if setup.strip():
                    cur.execute(setup)
                cur.execute("EXPLAIN " + main)
                return "\n".join(row[0] for row in cur.fetchall())

        yield run, explain
        conn.execute(f"DROP SCHEMA {schema} CASCADE")
        conn.close()

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    @pytest.mark.parametrize("method", PG_METHODS)
    @pytest.mark.parametrize("name", [*QUERIES, "unnest"])
    def test_folding_keeps_the_rows(self, pg, name, method, lazy):
        run, _ = pg
        query = f"{BASE} AND {self.UNNEST_META}" if name == "unnest" else QUERIES[name]
        if lazy:
            def extend(fold):
                return extend_query_with_partition_keys_lazy(
                    query, self.LAZY_KEYS, "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=fold, partition_datatype="integer"
                )
        else:
            def extend(fold):
                return extend_query_with_partition_keys(query, KEYS, "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=fold)
        assert run(extend(False)) == run(extend(True)) == run(query)

    @pytest.mark.parametrize("method", PG_METHODS)
    def test_char_keys_are_not_folded_by_default(self, pg, method):
        """char(n) compares without padding, text with it: folded, the padded keys read back matched no subquery row.

        (The text key table itself already misses a char(n) column, folded or not; that is a separate, older gap.)
        """
        run, _ = pg
        run("CREATE TABLE IF NOT EXISTS charkeys AS SELECT CAST('k' || g AS char(5)) AS kc, g AS id FROM generate_series(1, 9) g")
        run("CREATE TABLE IF NOT EXISTS charmeta AS SELECT CAST('k' || g AS varchar) AS kv, g % 3 AS prop FROM generate_series(1, 9) g")
        keys = set(run("SELECT kc FROM charkeys"))  # padded: 'k1   '
        query = "SELECT a.id FROM charkeys AS a WHERE a.kc IN (SELECT m.kv FROM charmeta AS m WHERE m.prop = 1)"
        option = extend_query_with_partition_keys(query, keys, "kc", method=method, p0_alias="a", fold_key_subqueries=True)
        plain = extend_query_with_partition_keys(query, keys, "kc", method=method, p0_alias="a")
        assert "pcache_folded" not in option and "IS NOT TRUE" not in option
        assert run(option) == run(plain)

    def test_unnest_leaves_the_plan_and_the_key_table_is_estimated_exactly(self, pg):
        run, explain = pg
        query = f"{BASE} AND {self.UNNEST_META}"
        kwargs = {"method": "UNLOGGED_TABLE_IN", "p0_alias": "pl", "planner_collapse_limit": 20}
        plain = explain(extend_query_with_partition_keys(query, KEYS, "pocket_key", **kwargs))
        folded_script = extend_query_with_partition_keys(query, KEYS, "pocket_key", fold_key_subqueries=True, **kwargs)
        folded = explain(folded_script)
        assert "unnest" in plain.lower() and "unnest" not in folded.lower()
        table = re.search(r"partitioncache_unl_[0-9a-f]+", folded_script).group(0)  # type: ignore[union-attr]
        estimate = int(re.search(rf"Seq Scan on {table}.*?rows=(\d+)", folded).group(1))  # type: ignore[union-attr]
        expected = len(KEYS & (set(range(1, 61, 3)) | set(range(1, 61, 9))))
        assert estimate == expected
