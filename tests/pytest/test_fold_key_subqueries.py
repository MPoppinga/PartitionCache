"""
fold_key_subqueries: conditions ``<alias>.<key> IN (<subquery>)`` move into the key table of a key-table method.

The key table keeps the keys for which ``key IN (<subquery>)`` is TRUE, the condition leaves the statement. The
results must equal those without folding. SQLite and DuckDB always run; TestPostgreSQL runs with
PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB and is skipped without them.
"""

import os
import re
import sqlite3
import time
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
    """The folded subqueries of a script: INTERSECT members (PostgreSQL), DELETE filters (temporary key table, other
    dialects) or IN filters (lazy key table, other dialects)."""
    return len(re.findall(r"\bAS pcache_folded_\d+\b", script)) + script.count("IS NOT TRUE") + len(re.findall(r"pcache_folded\.pcache_key IN \(", script))


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
        assert _number_of_filters(script) == 0, reason

    def test_a_bind_parameter_anywhere_stops_folding(self):
        """A folded subquery moves in front of the main statement, and with it its parameters' positions."""
        query = f"{BASE} AND t1.rnk = %s AND {META}"
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        assert _number_of_filters(script) == 0

    def test_transaction_stable_time_functions_fold(self):
        query = f"{BASE} AND pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr WHERE pr.prop_key = 39 AND now() IS NOT NULL)"
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        assert _number_of_filters(script) == 1

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
        assert (_number_of_filters(script) == 1) == folds

    @pytest.mark.parametrize(("datatype", "folds"), [("integer", True), ("text", False), ("float", False), (None, False)])
    def test_lazy_needs_the_datatype(self, datatype, folds):
        script = extend_query_with_partition_keys_lazy(
            QUERIES["geomine_like"], "SELECT 1 AS pocket_key", "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True, partition_datatype=datatype
        )
        assert (_number_of_filters(script) == 1) == folds

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="fold_key_subqueries is False, True or"):
            extend_query_with_partition_keys(QUERIES["geomine_like"], KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries="yes")

    def test_outer_join_does_not_tie_the_alias(self):
        query = (
            "SELECT DISTINCT pl.pocket_key FROM pocketkey_list AS pl LEFT JOIN pts AS t1 ON t1.pocket_key = pl.pocket_key "
            "WHERE t1.pocket_key IN (SELECT pr.pocket_key FROM props AS pr)"
        )
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        assert _number_of_filters(script) == 0

    def test_inner_join_ties_the_alias(self):
        query = (
            "SELECT DISTINCT pl.pocket_key FROM pocketkey_list AS pl JOIN pts AS t1 ON t1.pocket_key = pl.pocket_key "
            "WHERE t1.pocket_key IN (SELECT pr.pocket_key FROM props AS pr)"
        )
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", fold_key_subqueries=True)
        assert _number_of_filters(script) == 1

    @pytest.mark.parametrize("method", ["IN", "VALUES"])
    def test_methods_without_a_key_table_raise(self, method):
        with pytest.raises(ValueError, match="key-table method"):
            extend_query_with_partition_keys(QUERIES["geomine_like"], KEYS, "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=True)

    def test_mysql_is_refused(self):
        with pytest.raises(ValueError, match="supports the SQL dialects"):
            extend_query_with_partition_keys(QUERIES["geomine_like"], KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", sql_dialect="mysql", fold_key_subqueries=True)

    @pytest.mark.parametrize("method", PG_METHODS)
    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    def test_postgresql_intersects_the_keys_with_the_subqueries(self, method, lazy):
        """No IN filter or DELETE in a PostgreSQL setup; the keys' CTE has a random name (it is visible in the subqueries)."""
        if lazy:
            script = extend_query_with_partition_keys_lazy(
                QUERIES["two_subqueries"], "SELECT 2 AS pocket_key", "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=True, partition_datatype="integer"
            )
        else:
            script = extend_query_with_partition_keys(QUERIES["two_subqueries"], KEYS, "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=True)
        assert _number_of_filters(script) == 2
        assert "DELETE" not in script and "IS NOT TRUE" not in script and " IN (SELECT pr." not in script
        assert re.search(r"WITH pcache_k_[0-9a-f]{8}\b[^;]* AS MATERIALIZED \(", script)

    def test_off_by_default(self):
        script = extend_query_with_partition_keys(QUERIES["geomine_like"], KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl")
        assert _number_of_filters(script) == 0 and "props" in self._main_statement(script)


def test_sqlite_compares_with_the_key_column_affinity():
    """SQLite keeps the IN filter: it applies the key column's affinity, a set operation would compare 1 with '1' as unequal."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE pl (pocket_key INTEGER)")
    conn.execute("CREATE TABLE textmeta (k TEXT)")
    conn.executemany("INSERT INTO pl VALUES (?)", [(k,) for k in range(1, 7)])
    conn.executemany("INSERT INTO textmeta VALUES (?)", [("2",), ("4",), ("9",)])
    query = "SELECT pl.pocket_key FROM pl WHERE pl.pocket_key IN (SELECT tm.k FROM textmeta AS tm)"

    def run(script):
        setup, _, select = script.rpartition(";")
        if setup.strip():
            conn.executescript(setup + ";")
        return sorted(row[0] for row in conn.execute(select))

    folded = extend_query_with_partition_keys(query, {1, 2, 3, 4}, "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", sql_dialect="sqlite", fold_key_subqueries=True)
    assert _number_of_filters(folded) == 1 and "INTERSECT" not in folded
    assert run(folded) == run(query) == [2, 4]


def test_duckdb_filters_a_large_subquery_without_rescans():
    """DuckDB keeps DELETE ... WHERE (k IN (S)) IS NOT TRUE; it plans it as one hash join, not once per key.

    2,000 keys against 400,000 subquery rows: once per key that would be 8e8 comparisons, far beyond the bound.
    """
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect()
    conn.execute("CREATE TABLE pl AS SELECT g AS pocket_key FROM range(1, 400001) AS r(g)")
    conn.execute("CREATE TABLE meta AS SELECT (g * 7) % 400000 + 1 AS pocket_key, g AS id FROM range(1, 400001) AS r(g)")
    query = "SELECT pl.pocket_key FROM pl WHERE pl.pocket_key IN (SELECT m.pocket_key FROM meta AS m WHERE m.id > 0)"
    script = extend_query_with_partition_keys(query, set(range(1, 400001, 200)), "pocket_key", method="TMP_TABLE_IN", p0_alias="pl", sql_dialect="duckdb", fold_key_subqueries=True)
    assert _number_of_filters(script) == 1
    start = time.perf_counter()
    rows = conn.execute(script).fetchall()
    assert time.perf_counter() - start < 20
    assert len(rows) == 2000


def test_lazy_keys_are_read_once_and_the_fallback_stays():
    """The lazy keys appear twice in a PostgreSQL script: in the LIMIT 0 statement that gives the key table their type,
    which plans but does not run them, and in the INSERT. LAZY_ALL_KEYS_PLACEHOLDER (#45) is replaced in both."""
    from partitioncache.cache_handler.abstract import LAZY_ALL_KEYS_PLACEHOLDER

    lazy = f"SELECT unnest(COALESCE((SELECT array_agg(c.k) FROM cache_rows AS c), {LAZY_ALL_KEYS_PLACEHOLDER})) AS pocket_key"
    for method in PG_METHODS:
        script = extend_query_with_partition_keys_lazy(QUERIES["geomine_like"], lazy, "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=True, partition_datatype="integer")
        create, insert = script.split(";\n")[:2]
        assert create.endswith("LIMIT 0)") and "INSERT INTO" in insert
        assert LAZY_ALL_KEYS_PLACEHOLDER not in script
        assert "pocketkey_list" in create and "pocketkey_list" in insert.split(" INTERSECT ")[0]


def _setup_statements(script: str) -> tuple[list[str], str]:
    """The setup statements of a key-table script (up to its key table's ANALYZE) and the key table's name."""
    table = re.search(r"partitioncache_unl_[0-9a-f]+|tmp_cache_keys_\d+", script).group(0)  # type: ignore[union-attr]
    setup = script[: script.index(f"ANALYZE {table};") + len(f"ANALYZE {table};")]
    return [statement for statement in setup.split(";") if statement.strip()], table


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
    UNION_META = "pl.pocket_key IN (SELECT pr.pocket_key FROM props AS pr WHERE pr.prop_key = 39 UNION SELECT o.k FROM other AS o WHERE o.z = 0)"

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

        def setup_plans(script, settings=()):
            """The plans of the script's setup statements (each explained, then run), its key table's column type and
            NULL count, all in one rolled back transaction."""
            statements, table = _setup_statements(script)
            plans = []
            with conn.transaction(force_rollback=True), conn.cursor() as cur:
                for setting in settings:
                    cur.execute(f"SET LOCAL {setting}")
                for statement in statements:
                    head = statement.lstrip().upper()
                    if head.startswith(("WITH", "INSERT", "DELETE")) or (head.startswith("CREATE") and " AS (" in statement):
                        cur.execute("EXPLAIN " + statement)
                        plans.append("\n".join(row[0] for row in cur.fetchall()))
                    cur.execute(statement)
                column_type = cur.execute(f"SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = '{table}'::regclass AND attnum = 1").fetchone()[0]
                column = cur.execute(f"SELECT attname FROM pg_attribute WHERE attrelid = '{table}'::regclass AND attnum = 1").fetchone()[0]
                nulls = cur.execute(f"SELECT count(*) FROM {table} WHERE {column} IS NULL").fetchone()[0]
            return "\n".join(plans), column_type, nulls

        yield run, explain, setup_plans
        conn.execute(f"DROP SCHEMA {schema} CASCADE")
        conn.close()

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    @pytest.mark.parametrize("method", PG_METHODS)
    @pytest.mark.parametrize("name", [*QUERIES, "unnest", "union"])
    def test_folding_keeps_the_rows(self, pg, name, method, lazy):
        run, _, _ = pg
        query = {"unnest": f"{BASE} AND {self.UNNEST_META}", "union": f"{BASE} AND {self.UNION_META}"}.get(name) or QUERIES[name]
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
        run, _, _ = pg
        run("CREATE TABLE IF NOT EXISTS charkeys AS SELECT CAST('k' || g AS char(5)) AS kc, g AS id FROM generate_series(1, 9) g")
        run("CREATE TABLE IF NOT EXISTS charmeta AS SELECT CAST('k' || g AS varchar) AS kv, g % 3 AS prop FROM generate_series(1, 9) g")
        keys = set(run("SELECT kc FROM charkeys"))  # padded: 'k1   '
        query = "SELECT a.id FROM charkeys AS a WHERE a.kc IN (SELECT m.kv FROM charmeta AS m WHERE m.prop = 1)"
        option = extend_query_with_partition_keys(query, keys, "kc", method=method, p0_alias="a", fold_key_subqueries=True)
        plain = extend_query_with_partition_keys(query, keys, "kc", method=method, p0_alias="a")
        assert _number_of_filters(option) == 0
        assert run(option) == run(plain)

    def test_unnest_leaves_the_plan_and_the_key_table_is_estimated_exactly(self, pg):
        _, explain, _ = pg
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

    @staticmethod
    def _extend(query, method, lazy, fold, lazy_keys="SELECT g AS pocket_key FROM generate_series(2, 60, 2) AS g"):
        if lazy:
            return extend_query_with_partition_keys_lazy(query, lazy_keys, "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=fold, partition_datatype="integer")
        return extend_query_with_partition_keys(query, KEYS, "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=fold)

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    @pytest.mark.parametrize("method", PG_METHODS)
    def test_the_setup_has_no_join_or_subplan_over_the_subquery(self, pg, method, lazy):
        """With the subquery far above hash_mem: before, the eager DELETE ... IS NOT TRUE rescanned it for every key."""
        run, _, setup_plans = pg
        run("CREATE TABLE IF NOT EXISTS bigmeta AS SELECT g % 90 AS pocket_key, g AS id FROM generate_series(1, 40000) AS g")
        run("ANALYZE bigmeta")
        query = f"{BASE} AND pl.pocket_key IN (SELECT bm.pocket_key FROM bigmeta AS bm WHERE bm.id > 0)"
        script = self._extend(query, method, lazy, True)
        plans, _, _ = setup_plans(script, settings=("work_mem = '64kB'", "hash_mem_multiplier = 1"))
        assert "SetOp Intersect" in plans
        for node in ("SubPlan", "Semi Join", "Nested Loop", "Hash Join"):
            assert node not in plans, (node, plans)
        assert run(script) == run(self._extend(query, method, lazy, False)) == run(query)

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    @pytest.mark.parametrize("method", PG_METHODS)
    def test_the_key_table_keeps_the_keys_type(self, pg, method, lazy):
        """An integer key with a numeric subquery: a numeric key table would hide the index on the key column."""
        _, _, setup_plans = pg
        query = f"{BASE} AND pl.pocket_key IN (SELECT CAST(pr.pocket_key AS numeric) FROM props AS pr WHERE pr.prop_key = 39)"
        _, folded_type, _ = setup_plans(self._extend(query, method, lazy, True))
        _, plain_type, _ = setup_plans(self._extend(query, method, lazy, False))
        assert folded_type == plain_type and folded_type in ("integer", "bigint")

    @pytest.mark.parametrize("method", PG_METHODS)
    def test_null_cache_keys_stay_out_of_the_key_table(self, pg, method):
        """Set operations treat NULL as equal to NULL: a NULL key and a NULL of the subquery must not make a NULL key row."""
        run, _, setup_plans = pg
        keys = f"{self.LAZY_KEYS} UNION ALL SELECT NULL"
        for name in ("geomine_like", "two_subqueries"):  # props and other hold NULLs
            folded = self._extend(QUERIES[name], method, True, True, lazy_keys=keys)
            assert setup_plans(folded)[2] == 0
            assert run(folded) == run(self._extend(QUERIES[name], method, True, False, lazy_keys=keys)) == run(QUERIES[name])

    @pytest.mark.parametrize("method", PG_METHODS)
    def test_the_limit_0_statement_does_not_run_the_lazy_keys(self, pg, method):
        """Keys that fail when they run: the LIMIT 0 statement still succeeds, the INSERT (which runs them) fails."""
        _, _, _ = pg
        psycopg = pytest.importorskip("psycopg")
        lazy = "SELECT g AS pocket_key FROM generate_series(1, 3) AS g WHERE 1 / (g - g) = 1"
        query = "SELECT pl.pocket_key FROM pocketkey_list AS pl WHERE pl.pocket_key IN (SELECT gs.v FROM generate_series(1, 5) AS gs(v))"
        script = extend_query_with_partition_keys_lazy(query, lazy, "pocket_key", method=method, p0_alias="pl", fold_key_subqueries=True, partition_datatype="integer")
        assert _number_of_filters(script) == 1
        create, insert = script.split(";\n")[:2]
        with psycopg.connect(**_pg_params()) as conn, conn.transaction(force_rollback=True):
            conn.execute(create)
            with pytest.raises(psycopg.errors.DivisionByZero):
                conn.execute(insert)

    def test_a_table_named_like_the_keys_cte(self, pg):
        """The keys' CTE is visible inside the folded subqueries; its random suffix keeps a table named pcache_k apart."""
        run, _, _ = pg
        run("CREATE TABLE IF NOT EXISTS pcache_k AS SELECT pocket_key FROM props WHERE prop_key = 39")
        query = f"{BASE} AND pl.pocket_key IN (SELECT pk.pocket_key FROM pcache_k AS pk)"
        for method in PG_METHODS:
            assert run(self._extend(query, method, False, True)) == run(query)


class TestPopulation:
    """Folding in population: the restricted variants store the same keys with and without it."""

    class _Cache:
        def __init__(self):
            self.entries: dict = {}

        def exists(self, key, partition_key="partition_key"):
            return key in self.entries

        def get_intersected(self, keys, partition_key="partition_key"):
            present = [self.entries[k] for k in keys if k in self.entries]
            return (set.intersection(*present), len(present)) if present else (None, 0)

        def set_query(self, *args, **kwargs):
            return True

        def set_entry(self, key, keys, query_text, partition_key="partition_key", force_update=False):
            self.entries[key] = set(keys)
            return True

    def test_population_stores_the_same_keys(self):
        from partitioncache.population import populate_query_variants
        from partitioncache.query_processor import generate_all_query_hash_pairs

        class Recording:
            """Runs a key-table script (SQLiteDBHandler executes one statement only) and records it."""

            def __init__(self):
                self.conn = sqlite3.connect(":memory:")
                _load(self.conn.execute, self.conn.executemany)
                self.executed: list[str] = []

            def execute(self, script):
                self.executed.append(script)
                setup, _, select = script.rpartition(";")
                if setup.strip():
                    self.conn.executescript(setup + ";")
                return [row[0] for row in self.conn.execute(select)]

        settings = {"min_component_size": 1, "keep_all_attributes": True}
        pairs = generate_all_query_hash_pairs(QUERIES["two_subqueries"], "pocket_key", **settings)
        stored = {}
        for fold in (None, False, True):
            cache, db = self._Cache(), Recording()
            populate_query_variants(
                pairs, "pocket_key", cache, db, restrict=fold is not None, method="TMP_TABLE_IN", sql_dialect="sqlite",
                variant_settings=settings, fold_key_subqueries=bool(fold),
            )
            stored[fold] = cache.entries
            if fold:
                assert any("IS NOT TRUE" in query for query in db.executed)
        assert stored[True] == stored[False] == stored[None]

    def test_population_resolves_unqualified_columns_through_its_db_handler(self):
        """GeoMine's subqueries have bare columns: population folds them with a catalog resolver on its db_handler."""
        from partitioncache.population import populate_query_variants
        from partitioncache.query_processor import generate_all_query_hash_pairs

        class Recording:
            def __init__(self):
                self.conn = sqlite3.connect(":memory:")
                _load(self.conn.execute, self.conn.executemany)
                self.executed: list[str] = []

            def execute(self, script):
                self.executed.append(script)
                setup, _, select = script.rpartition(";")
                if setup.strip():
                    self.conn.executescript(setup + ";")
                return [row[0] for row in self.conn.execute(select)]

        query = f"{BASE} AND pl.pocket_key IN (SELECT pocket_key FROM props WHERE prop_key = 39)"
        settings = {"min_component_size": 1, "keep_all_attributes": True}
        pairs = generate_all_query_hash_pairs(query, "pocket_key", **settings)
        stored = {}
        for fold in (None, True):
            cache, db = self._Cache(), Recording()
            populate_query_variants(
                pairs, "pocket_key", cache, db, restrict=fold is not None, method="TMP_TABLE_IN", sql_dialect="sqlite",
                variant_settings=settings, fold_key_subqueries=bool(fold),
            )
            stored[fold] = cache.entries
            if fold:
                assert any("pragma_table_info('props')" in q for q in db.executed)
                assert any("IS NOT TRUE" in q for q in db.executed)
        assert stored[True] == stored[None]

    def test_population_checks_the_method(self):
        from partitioncache.population import populate_query_variants

        with pytest.raises(ValueError, match="key-table method"):
            populate_query_variants([], "pocket_key", self._Cache(), None, method="IN", fold_key_subqueries=True)  # type: ignore[arg-type]


def test_monitor_passes_the_option():
    from unittest.mock import Mock, patch

    from partitioncache.cache_handler.abstract import AbstractCacheHandler
    from partitioncache.cli.monitor_cache_queue import apply_cache_optimization

    args = Mock()
    args.enable_cache_optimization, args.disable_cache_optimization, args.prefer_lazy_optimization = True, False, False
    args.max_restriction_keys, args.min_cache_hits, args.db_backend = None, 1, "postgresql"
    args.cache_optimization_method, args.join_every_table, args.planner_collapse_limit = "TMP_TABLE_IN", False, None
    args.fold_key_subqueries = "integer"
    args.min_component_size, args.follow_graph, args.no_auto_detect_partition_join = 1, True, False
    args.max_component_size, args.partition_join_table, args.bucket_steps = None, None, 1.0
    args.skip_partition_key_joins, args.max_conditions_removed, args.grid_cell_size = [], None, None
    handler = Mock(spec=AbstractCacheHandler)
    handler.get_intersected.return_value = (KEYS, 2)
    catalog = Mock()
    catalog.execute.return_value = ["pocket_key", "prop_key"]
    with (
        patch("partitioncache.population.extend_query_with_partition_keys", return_value="restricted") as extend,
        patch("partitioncache.cli.monitor_cache_queue.get_db_handler", return_value=catalog) as get_db,
        patch("partitioncache.cli.monitor_cache_queue.get_database_connection_params", return_value={}),
    ):
        apply_cache_optimization(QUERIES["geomine_like"], "h", "pocket_key", "integer", handler, args)
    assert extend.call_args.kwargs["fold_key_subqueries"] is True
    # a catalog resolver on a connection of its own to the queried database, closed after the fragment's restriction
    resolver = extend.call_args.kwargs["fold_column_resolver"]
    assert get_db.call_args.args[0] == "postgres" and resolver("props") == {"pocket_key", "prop_key"}
    catalog.close.assert_called_once()


def test_monitor_without_folding_opens_no_catalog_connection():
    from unittest.mock import Mock, patch

    from partitioncache.cli.monitor_cache_queue import _fold_column_resolver

    with patch("partitioncache.cli.monitor_cache_queue.get_db_handler") as get_db, _fold_column_resolver(False, Mock()) as resolver:
        assert resolver is None
    get_db.assert_not_called()
