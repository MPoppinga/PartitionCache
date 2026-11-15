"""
fold_key_subqueries with unqualified columns in the subquery: they fold only when they bind inside it.

Metadata subqueries are often written with bare columns (``... FROM <view> WHERE stat_key = 84 AND stat_value <= 100``);
without the schema such a column might bind to the outer query. SQL binds an unqualified name to the innermost query
level whose FROM items have it: with a column resolver (the catalog) the library decides that, and folds when every
unqualified column binds inside the subquery. ``fold_assume_inner_columns`` is the caller's word instead.
The SQLite tests always run; TestPostgreSQL runs with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB.
"""

import os
import re
import sqlite3
import uuid

import pytest

from partitioncache.apply_cache import catalog_column_resolver, extend_query_with_partition_keys, extend_query_with_partition_keys_lazy

# A self-join of points with a metadata subquery on the key whose columns are unqualified (views over an array column)
STATEMENT_NUM = (
    "SELECT DISTINCT t1.region_id FROM pois AS t1, pois AS t2 WHERE ((t1.coord_x - "
    "t2.coord_x) * (t1.coord_x - t2.coord_x) + (t1.coord_y - t2.coord_y) * "
    "(t1.coord_y - t2.coord_y) + (t1.coord_z - t2.coord_z) * (t1.coord_z - "
    "t2.coord_z)) <= 57 AND ((t1.coord_x - t2.coord_x) * (t1.coord_x - t2.coord_x) + "
    "(t1.coord_y - t2.coord_y) * (t1.coord_y - t2.coord_y) + (t1.coord_z - "
    "t2.coord_z) * (t1.coord_z - t2.coord_z)) >= 30 AND t1.poi_id <> t2.poi_id AND "
    "(t1.source = 1 OR t1.source = 2) AND t1.category = 4 AND t1.region_id = t2.region_id AND "
    "t1.region_id IN (SELECT region_list_view.region_id FROM region_list_view INTERSECT "
    "SELECT UNNEST(region_stats_num.region_id) AS region_id FROM "
    "region_stats_num WHERE stat_key = 84 AND stat_value <= 100 AND stat_value >= 1) AND "
    "t2.source = 3 AND t2.category = 4"
)
STATEMENT_TEXT_NUM = STATEMENT_NUM.replace(
    "FROM region_list_view INTERSECT ",
    "FROM region_list_view INTERSECT SELECT UNNEST(region_stats_text.region_id) "
    "AS region_id FROM region_stats_text WHERE stat_key = 10 AND stat_value IN ('Mitte', 'Altona', "
    "'Eimsbuettel', 'Eimsbuettel', 'Harburg', 'Bergedorf', 'Harburg', 'Altona', 'Mitte', 'Bergedorf', 'Bergedorf', 'Bergedorf', 'Wandsbek', 'Wandsbek') INTERSECT ",
)
STATEMENTS = {"num": STATEMENT_NUM, "text_num": STATEMENT_TEXT_NUM}
POI_COLUMNS = {"region_id", "coord_x", "coord_y", "coord_z", "poi_id", "source", "category"}
COLUMNS = {
    "pois": POI_COLUMNS,
    "region_list_view": {"region_id"},
    "region_stats_num": {"stat_key", "stat_value", "region_id"},
    "region_stats_text": {"stat_key", "stat_value", "region_id"},
    "meta": {"region_id", "stat_key", "stat_value"},
    "meta_with_type": {"region_id", "stat_key", "category"},
}
KEYS = set(range(1, 40))
TABLE_METHODS = ["TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_IN", "UNLOGGED_TABLE_JOIN"]


def _folds(query: str, **kwargs) -> bool:
    script = extend_query_with_partition_keys(query, KEYS, "region_id", method=kwargs.pop("method", "TMP_TABLE_IN"), p0_alias="t1", fold_key_subqueries=True, **kwargs)
    return bool(re.search(r"\bAS pcache_folded_\d+\b|IS NOT TRUE|pcache_folded\.pcache_key IN \(", script))


def _outer(subquery: str) -> str:
    return f"SELECT DISTINCT t1.region_id FROM pois AS t1 WHERE t1.category = 4 AND t1.region_id IN ({subquery})"


class TestScoping:
    @pytest.mark.parametrize("name", list(STATEMENTS))
    def test_bare_column_subqueries_fold_with_a_resolver_only(self, name):
        assert not _folds(STATEMENTS[name])
        assert _folds(STATEMENTS[name], fold_column_resolver=COLUMNS.get)

    @pytest.mark.parametrize("method", TABLE_METHODS)
    def test_lazy_too(self, method):
        script = extend_query_with_partition_keys_lazy(
            STATEMENT_NUM, "SELECT 1 AS region_id", "region_id", method=method, p0_alias="t1", fold_key_subqueries=True,
            partition_datatype="integer", fold_column_resolver=COLUMNS.get,
        )
        assert "pcache_folded_1" in script and "stat_value" not in script.rpartition("pcache_folded_1")[2]

    def test_a_column_only_the_outer_query_has_is_correlated(self):
        """category exists in the outer table only: it binds outward, the subquery is correlated and stays."""
        assert not _folds(_outer("SELECT meta.region_id FROM meta WHERE category = 4"), fold_column_resolver=COLUMNS.get)

    def test_a_name_in_both_scopes_binds_inward(self):
        assert _folds(_outer("SELECT m.region_id FROM meta_with_type AS m WHERE category = 4"), fold_column_resolver=COLUMNS.get)

    @pytest.mark.parametrize(
        ("subquery", "why"),
        [
            ("SELECT x.region_id FROM unknown_relation AS x WHERE stat_key = 1", "a relation the resolver does not know"),
            ("SELECT generate_series FROM generate_series(1, 3)", "a function without a column alias list"),
            ("SELECT d.region_id FROM (SELECT * FROM meta) AS d WHERE stat_key = 1", "a star in a derived table"),
            ("SELECT m.region_id AS k FROM meta AS m ORDER BY k", "an ORDER BY of a SELECT-list alias"),
            ("SELECT m.region_id FROM meta AS m WHERE ctid IS NOT NULL", "a system column"),
            ("SELECT u.v FROM meta AS m, LATERAL unnest(ARRAY[m.region_id]) AS u(v) WHERE stat_key = 1", "LATERAL"),
        ],
        ids=lambda value: value if len(value) < 45 else None,
    )
    def test_undecidable_or_not_a_column_is_refused(self, subquery, why):
        assert not _folds(_outer(subquery), fold_column_resolver=COLUMNS.get), why

    def test_inner_cte_and_derived_table_columns(self):
        cte = "WITH x AS (SELECT m.region_id, m.stat_key FROM meta AS m) SELECT x.region_id FROM x WHERE stat_key = 1"
        derived = "SELECT d.k FROM (SELECT m.region_id AS k, m.stat_key AS p FROM meta AS m) AS d WHERE p = 1"
        listed = "SELECT u.v FROM unnest(ARRAY[1, 2]) AS u(v) WHERE v > 1"
        for subquery in (cte, derived, listed):
            assert _folds(_outer(subquery), fold_column_resolver=COLUMNS.get), subquery

    def test_nested_subquery_binds_to_the_nearest_level(self):
        subquery = "SELECT m.region_id FROM meta AS m WHERE m.stat_key IN (SELECT n.stat_key FROM meta_with_type AS n WHERE stat_value > 0)"
        # stat_value: not in meta_with_type, but in meta one level out, still inside the subquery
        assert _folds(_outer(subquery), fold_column_resolver=COLUMNS.get)

    def test_assumption_folds_without_a_resolver(self):
        assert _folds(STATEMENT_NUM, fold_assume_inner_columns=True)

    @pytest.mark.parametrize("name", ["partition_key", "pcache_key"])
    def test_assumption_keeps_key_table_names_refused_outside_postgresql(self, name):
        """SQLite and DuckDB put the subquery into a statement over the key table, where these names would bind."""
        query = _outer(f"SELECT m.region_id FROM meta AS m WHERE {name} = 1")
        script = extend_query_with_partition_keys(query, KEYS, "region_id", method="TMP_TABLE_IN", p0_alias="t1", sql_dialect="sqlite",
                                                  fold_key_subqueries=True, fold_assume_inner_columns=True)
        assert "IS NOT TRUE" not in script
        assert _folds(query, fold_assume_inner_columns=True)  # PostgreSQL folds it: a wrong assumption only errors there


def test_sqlite_catalog_resolver_and_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE pts (region_id INTEGER, kind TEXT)")
    conn.execute("CREATE TABLE meta (region_id INTEGER, stat_key INTEGER)")
    conn.executemany("INSERT INTO pts VALUES (?, ?)", [(k, "AB"[k % 2]) for k in range(1, 30)])
    conn.executemany("INSERT INTO meta VALUES (?, ?)", [(k, k % 3) for k in range(1, 30)])
    resolver = catalog_column_resolver(lambda sql: [row[0] for row in conn.execute(sql)], "sqlite")
    assert resolver("meta") == {"region_id", "stat_key"} and resolver("nothing") is None
    query = "SELECT p.region_id FROM pts AS p WHERE p.kind = 'A' AND p.region_id IN (SELECT region_id FROM meta WHERE stat_key = 1)"
    folded = extend_query_with_partition_keys(query, set(range(1, 25)), "region_id", method="TMP_TABLE_IN", p0_alias="p", sql_dialect="sqlite",
                                              fold_key_subqueries=True, fold_column_resolver=resolver)
    assert "IS NOT TRUE" in folded
    setup, _, select = folded.rpartition(";")
    conn.executescript(setup + ";")
    reference = {row[0] for row in conn.execute(query)} & set(range(1, 25))
    assert {row[0] for row in conn.execute(select)} == reference


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
    @pytest.fixture(scope="class")
    def pg(self):
        psycopg = pytest.importorskip("psycopg")
        from partitioncache.db_handler.postgres import fetch_final_result_set

        schema = f"foldcols_{uuid.uuid4().hex[:8]}"
        conn = psycopg.connect(**_pg_params(), autocommit=True)
        conn.execute(f"CREATE SCHEMA {schema}")
        conn.execute(f"SET search_path TO {schema}")
        for statement in (
            "CREATE TABLE pois (poi_id int, region_id int, coord_x float8, coord_y float8, "
            "coord_z float8, source int, category int)",
            "INSERT INTO pois SELECT g, 1 + g % 40, (g * 7 % 11)::float8, (g * 3 % 5)::float8, (g % 4)::float8, "
            "1 + g % 3, 4 FROM generate_series(1, 2400) AS g",
            "CREATE TABLE region_list (region_id int)",
            "INSERT INTO region_list SELECT g FROM generate_series(1, 40) AS g WHERE g % 5 <> 0",
            "CREATE TABLE stats (stat_key int, stat_value_num numeric, stat_value_text text, region_ids int[])",
            "INSERT INTO stats SELECT 84, g, NULL, ARRAY(SELECT h FROM generate_series(1, 40) AS h WHERE h % (g % 4 + 2) = 0) FROM generate_series(1, 120) AS g",
            "INSERT INTO stats VALUES (10, NULL, 'Mitte', ARRAY[1, 2, 3, 6, 9, 12, 15, 18, 21, 24]), (10, NULL, 'Bergedorf', ARRAY[4, 8, 30]), (10, NULL, 'Nowhere', ARRAY[5])",
            "CREATE VIEW region_list_view AS SELECT region_id FROM region_list",
            "CREATE VIEW region_stats_num AS "
            "SELECT stat_key, stat_value_num AS stat_value, region_ids AS region_id FROM stats WHERE stat_value_num IS NOT NULL",
            "CREATE VIEW region_stats_text AS "
            "SELECT stat_key, stat_value_text AS stat_value, region_ids AS region_id FROM stats WHERE stat_value_text IS NOT NULL",
            "CREATE TABLE outer_t (region_id int, partition_key int, pcache_key int)",
            "INSERT INTO outer_t SELECT g, g, g FROM generate_series(1, 30) AS g",
            "CREATE TABLE inner_t (v int)",
            "INSERT INTO inner_t SELECT g FROM generate_series(1, 30) AS g",
            "CREATE TABLE outer_text (region_id text, partition_key text, pcache_key text)",
            "INSERT INTO outer_text SELECT g::text, g::text, g::text FROM generate_series(1, 30) AS g",
            "CREATE TABLE inner_text (v text)",
            "INSERT INTO inner_text SELECT g::text FROM generate_series(1, 30) AS g",
            'CREATE TABLE "MixedCase" ("Key" int, plain int)',
            'CREATE SCHEMA IF NOT EXISTS "Other Schema"',
            'CREATE TABLE "Other Schema"."T" (a int)',
            "CREATE TABLE shadowed (permanent_col int)",
        ):
            conn.execute(statement)
        conn.execute("ANALYZE")

        def run(script):
            with conn.cursor() as cur:
                cur.execute(script)
                return {row[0] for row in fetch_final_result_set(cur)}

        def execute(sql):
            return [row[0] for row in conn.execute(sql).fetchall()]

        yield run, execute, conn, psycopg
        conn.execute('DROP SCHEMA IF EXISTS "Other Schema" CASCADE')
        conn.execute(f"DROP SCHEMA {schema} CASCADE")
        conn.close()

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    @pytest.mark.parametrize("method", TABLE_METHODS)
    @pytest.mark.parametrize("name", list(STATEMENTS))
    def test_bare_column_statements_fold_and_keep_their_rows(self, pg, name, method, lazy):
        run, execute, _, _ = pg
        query = STATEMENTS[name]
        resolver = catalog_column_resolver(execute)

        def extend(fold):
            kwargs = {"method": method, "p0_alias": "t1", "fold_key_subqueries": fold, "fold_column_resolver": resolver}
            if lazy:
                return extend_query_with_partition_keys_lazy(query, "SELECT g AS region_id FROM generate_series(1, 35) AS g", "region_id",
                                                             partition_datatype="integer", **kwargs)
            return extend_query_with_partition_keys(query, set(range(1, 36)), "region_id", **kwargs)

        folded = extend(True)
        assert "pcache_folded_1" in folded
        reference = run(query)
        assert reference, "the test data must give rows"
        assert run(folded) == run(extend(False)) == {k for k in reference if k <= 35}

    @pytest.mark.parametrize("name", ["partition_key", "pcache_key", "region_id"])
    @pytest.mark.parametrize(
        ("method", "lazy", "keys"),
        [
            ("TMP_TABLE_IN", False, set(range(1, 31))),
            ("UNLOGGED_TABLE_JOIN", False, set(range(1, 31))),
            ("TMP_TABLE_JOIN", True, None),
            ("UNLOGGED_TABLE_IN", True, None),
            ("TMP_TABLE_IN", False, {str(k) for k in range(1, 31)}),  # text keys: the typed key table and its staging table
        ],
        ids=["eager_tmp", "eager_unlogged", "lazy_tmp", "lazy_unlogged", "typed_staging"],
    )
    def test_a_wrong_assumption_errors_in_every_postgresql_form(self, pg, method, lazy, keys, name):
        """A column that binds outward, named like a key table's column: folded on the caller's word it must fail, not bind."""
        run, _, _, psycopg = pg
        if keys is not None and isinstance(next(iter(keys)), str):
            query = f"SELECT o.region_id FROM outer_text AS o WHERE o.region_id IN (SELECT i.v FROM inner_text AS i WHERE i.v = {name})"
        else:
            query = f"SELECT o.region_id FROM outer_t AS o WHERE o.region_id IN (SELECT i.v FROM inner_t AS i WHERE i.v = {name})"
        kwargs = {"method": method, "p0_alias": "o", "fold_key_subqueries": "any_key_type", "fold_assume_inner_columns": True}
        if lazy:
            script = extend_query_with_partition_keys_lazy(query, "SELECT g AS region_id FROM generate_series(1, 30) AS g", "region_id",
                                                           partition_datatype="integer", **kwargs)
        else:
            script = extend_query_with_partition_keys(query, keys, "region_id", **kwargs)
        assert "pcache_folded_1" in script
        if keys is not None and isinstance(next(iter(keys)), str):
            assert "pcache_stage_" in script
        with pytest.raises(psycopg.errors.UndefinedColumn):
            run(script)

    def test_the_catalog_resolver(self, pg):
        _, execute, conn, _ = pg
        calls = []

        def counting(sql):
            calls.append(sql)
            return execute(sql)

        resolver = catalog_column_resolver(counting)
        assert resolver('"MixedCase"') == {"Key", "plain"}
        assert resolver("MixedCase") is None  # a bare name folds to lowercase: no such relation
        assert resolver('"Other Schema"."T"') == {"a"}
        assert resolver("region_stats_num") == {"stat_key", "stat_value", "region_id"}  # a view
        assert resolver("does_not_exist") is None
        assert "ctid" not in resolver("inner_t")  # system columns are left out
        before = len(calls)
        resolver('"MixedCase"')
        assert len(calls) == before  # cached
        conn.execute("CREATE TEMPORARY TABLE shadowed (temporary_col int)")
        try:
            assert catalog_column_resolver(execute)("shadowed") == {"temporary_col"}  # the session's temporary table shadows it
        finally:
            conn.execute("DROP TABLE pg_temp.shadowed")


def test_each_decision_is_logged(caplog):
    """pcache-monitor's runs record per fragment whether its key subquery folded, and if not, why."""
    with caplog.at_level("INFO", logger="PartitionCache"):
        _folds(STATEMENT_NUM)
        _folds(STATEMENT_NUM, fold_column_resolver=COLUMNS.get)
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("fold_key_subqueries:")]
    assert lines[0].startswith("fold_key_subqueries: not folded (unqualified column stat_value)")
    assert lines[1].startswith("fold_key_subqueries: folded t1.region_id IN (")
