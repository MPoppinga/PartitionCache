"""
fold_key_subqueries with unqualified columns in the subquery: they fold only when they bind inside it.

GeoMine writes its metadata subqueries with bare columns (``... FROM <view> WHERE prop_key = 84 AND prop_value <= 100``);
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

# Verbatim from 09g's GeoMine queue (results/09g_queue__geomine_p12_S.csv.gz, hashes 05bbbb2e34e0..., 4d48b1cb284a...)
GEOMINE_NUM = (
    "SELECT DISTINCT t1.pocket_key FROM interactiondb_inter_points AS t1, interactiondb_inter_points AS t2 WHERE ((t1.interpoint_coord_x - "
    "t2.interpoint_coord_x) * (t1.interpoint_coord_x - t2.interpoint_coord_x) + (t1.interpoint_coord_y - t2.interpoint_coord_y) * "
    "(t1.interpoint_coord_y - t2.interpoint_coord_y) + (t1.interpoint_coord_z - t2.interpoint_coord_z) * (t1.interpoint_coord_z - "
    "t2.interpoint_coord_z)) <= 57 AND ((t1.interpoint_coord_x - t2.interpoint_coord_x) * (t1.interpoint_coord_x - t2.interpoint_coord_x) + "
    "(t1.interpoint_coord_y - t2.interpoint_coord_y) * (t1.interpoint_coord_y - t2.interpoint_coord_y) + (t1.interpoint_coord_z - "
    "t2.interpoint_coord_z) * (t1.interpoint_coord_z - t2.interpoint_coord_z)) >= 30 AND t1.interpoint_id <> t2.interpoint_id AND "
    "(t1.interpoint_origin = 1 OR t1.interpoint_origin = 2) AND t1.interpoint_type = 4 AND t1.pocket_key = t2.pocket_key AND "
    "t1.pocket_key IN (SELECT interactiondb_search_view_pocket_ligands.pocket_key FROM interactiondb_search_view_pocket_ligands INTERSECT "
    "SELECT UNNEST(interactiondb_search_view_molecule_property_pockets_num.pocket_key) AS pocket_key FROM "
    "interactiondb_search_view_molecule_property_pockets_num WHERE prop_key = 84 AND prop_value <= 100 AND prop_value >= 1) AND "
    "t2.interpoint_origin = 3 AND t2.interpoint_type = 4"
)
GEOMINE_TEXT_NUM = GEOMINE_NUM.replace(
    "FROM interactiondb_search_view_pocket_ligands INTERSECT ",
    "FROM interactiondb_search_view_pocket_ligands INTERSECT SELECT UNNEST(interactiondb_search_view_molecule_property_pockets_text.pocket_key) "
    "AS pocket_key FROM interactiondb_search_view_molecule_property_pockets_text WHERE prop_key = 10 AND prop_value IN ('1UNL', '1UNG', "
    "'3O0G', '3O0G', '1UNH', '4AU8', '1UNH', '1UNG', '1UNL', '4AU8', '4AU8', '4AU8', '1H4L', '1H4L') INTERSECT ",
)
GEOMINE = {"num": GEOMINE_NUM, "text_num": GEOMINE_TEXT_NUM}
POINTS = {"pocket_key", "interpoint_coord_x", "interpoint_coord_y", "interpoint_coord_z", "interpoint_id", "interpoint_origin", "interpoint_type"}
GEOMINE_COLUMNS = {
    "interactiondb_inter_points": POINTS,
    "interactiondb_search_view_pocket_ligands": {"pocket_key"},
    "interactiondb_search_view_molecule_property_pockets_num": {"prop_key", "prop_value", "pocket_key"},
    "interactiondb_search_view_molecule_property_pockets_text": {"prop_key", "prop_value", "pocket_key"},
    "meta": {"pocket_key", "prop_key", "prop_value"},
    "meta_with_type": {"pocket_key", "prop_key", "interpoint_type"},
}
KEYS = set(range(1, 40))
TABLE_METHODS = ["TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_IN", "UNLOGGED_TABLE_JOIN"]


def _folds(query: str, **kwargs) -> bool:
    script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method=kwargs.pop("method", "TMP_TABLE_IN"), p0_alias="t1", fold_key_subqueries=True, **kwargs)
    return bool(re.search(r"\bAS pcache_folded_\d+\b|IS NOT TRUE|pcache_folded\.pcache_key IN \(", script))


def _outer(subquery: str) -> str:
    return f"SELECT DISTINCT t1.pocket_key FROM interactiondb_inter_points AS t1 WHERE t1.interpoint_type = 4 AND t1.pocket_key IN ({subquery})"


class TestScoping:
    @pytest.mark.parametrize("name", list(GEOMINE))
    def test_geomine_subqueries_fold_with_a_resolver_only(self, name):
        assert not _folds(GEOMINE[name])
        assert _folds(GEOMINE[name], fold_column_resolver=GEOMINE_COLUMNS.get)

    @pytest.mark.parametrize("method", TABLE_METHODS)
    def test_lazy_too(self, method):
        script = extend_query_with_partition_keys_lazy(
            GEOMINE_NUM, "SELECT 1 AS pocket_key", "pocket_key", method=method, p0_alias="t1", fold_key_subqueries=True,
            partition_datatype="integer", fold_column_resolver=GEOMINE_COLUMNS.get,
        )
        assert "pcache_folded_1" in script and "prop_value" not in script.rpartition("pcache_folded_1")[2]

    def test_a_column_only_the_outer_query_has_is_correlated(self):
        """interpoint_type exists in the outer table only: it binds outward, the subquery is correlated and stays."""
        assert not _folds(_outer("SELECT meta.pocket_key FROM meta WHERE interpoint_type = 4"), fold_column_resolver=GEOMINE_COLUMNS.get)

    def test_a_name_in_both_scopes_binds_inward(self):
        assert _folds(_outer("SELECT m.pocket_key FROM meta_with_type AS m WHERE interpoint_type = 4"), fold_column_resolver=GEOMINE_COLUMNS.get)

    @pytest.mark.parametrize(
        ("subquery", "why"),
        [
            ("SELECT x.pocket_key FROM unknown_relation AS x WHERE prop_key = 1", "a relation the resolver does not know"),
            ("SELECT generate_series FROM generate_series(1, 3)", "a function without a column alias list"),
            ("SELECT d.pocket_key FROM (SELECT * FROM meta) AS d WHERE prop_key = 1", "a star in a derived table"),
            ("SELECT m.pocket_key AS k FROM meta AS m ORDER BY k", "an ORDER BY of a SELECT-list alias"),
            ("SELECT m.pocket_key FROM meta AS m WHERE ctid IS NOT NULL", "a system column"),
            ("SELECT u.v FROM meta AS m, LATERAL unnest(ARRAY[m.pocket_key]) AS u(v) WHERE prop_key = 1", "LATERAL"),
        ],
        ids=lambda value: value if len(value) < 45 else None,
    )
    def test_undecidable_or_not_a_column_is_refused(self, subquery, why):
        assert not _folds(_outer(subquery), fold_column_resolver=GEOMINE_COLUMNS.get), why

    def test_inner_cte_and_derived_table_columns(self):
        cte = "WITH x AS (SELECT m.pocket_key, m.prop_key FROM meta AS m) SELECT x.pocket_key FROM x WHERE prop_key = 1"
        derived = "SELECT d.k FROM (SELECT m.pocket_key AS k, m.prop_key AS p FROM meta AS m) AS d WHERE p = 1"
        listed = "SELECT u.v FROM unnest(ARRAY[1, 2]) AS u(v) WHERE v > 1"
        for subquery in (cte, derived, listed):
            assert _folds(_outer(subquery), fold_column_resolver=GEOMINE_COLUMNS.get), subquery

    def test_nested_subquery_binds_to_the_nearest_level(self):
        subquery = "SELECT m.pocket_key FROM meta AS m WHERE m.prop_key IN (SELECT n.prop_key FROM meta_with_type AS n WHERE prop_value > 0)"
        # prop_value: not in meta_with_type, but in meta one level out, still inside the subquery
        assert _folds(_outer(subquery), fold_column_resolver=GEOMINE_COLUMNS.get)

    def test_assumption_folds_without_a_resolver(self):
        assert _folds(GEOMINE_NUM, fold_assume_inner_columns=True)

    @pytest.mark.parametrize("name", ["partition_key", "pcache_key"])
    def test_assumption_keeps_key_table_names_refused_outside_postgresql(self, name):
        """SQLite and DuckDB put the subquery into a statement over the key table, where these names would bind."""
        query = _outer(f"SELECT m.pocket_key FROM meta AS m WHERE {name} = 1")
        script = extend_query_with_partition_keys(query, KEYS, "pocket_key", method="TMP_TABLE_IN", p0_alias="t1", sql_dialect="sqlite",
                                                  fold_key_subqueries=True, fold_assume_inner_columns=True)
        assert "IS NOT TRUE" not in script
        assert _folds(query, fold_assume_inner_columns=True)  # PostgreSQL folds it: a wrong assumption only errors there


def test_sqlite_catalog_resolver_and_rows():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE pts (pocket_key INTEGER, kind TEXT)")
    conn.execute("CREATE TABLE meta (pocket_key INTEGER, prop_key INTEGER)")
    conn.executemany("INSERT INTO pts VALUES (?, ?)", [(k, "AB"[k % 2]) for k in range(1, 30)])
    conn.executemany("INSERT INTO meta VALUES (?, ?)", [(k, k % 3) for k in range(1, 30)])
    resolver = catalog_column_resolver(lambda sql: [row[0] for row in conn.execute(sql)], "sqlite")
    assert resolver("meta") == {"pocket_key", "prop_key"} and resolver("nothing") is None
    query = "SELECT p.pocket_key FROM pts AS p WHERE p.kind = 'A' AND p.pocket_key IN (SELECT pocket_key FROM meta WHERE prop_key = 1)"
    folded = extend_query_with_partition_keys(query, set(range(1, 25)), "pocket_key", method="TMP_TABLE_IN", p0_alias="p", sql_dialect="sqlite",
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
            "CREATE TABLE interactiondb_inter_points (interpoint_id int, pocket_key int, interpoint_coord_x float8, interpoint_coord_y float8, "
            "interpoint_coord_z float8, interpoint_origin int, interpoint_type int)",
            "INSERT INTO interactiondb_inter_points SELECT g, 1 + g % 40, (g * 7 % 11)::float8, (g * 3 % 5)::float8, (g % 4)::float8, "
            "1 + g % 3, 4 FROM generate_series(1, 2400) AS g",
            "CREATE TABLE ligands (pocket_key int)",
            "INSERT INTO ligands SELECT g FROM generate_series(1, 40) AS g WHERE g % 5 <> 0",
            "CREATE TABLE props (prop_key int, prop_value_num numeric, prop_value_text text, pocket_keys int[])",
            "INSERT INTO props SELECT 84, g, NULL, ARRAY(SELECT h FROM generate_series(1, 40) AS h WHERE h % (g % 4 + 2) = 0) FROM generate_series(1, 120) AS g",
            "INSERT INTO props VALUES (10, NULL, '1UNL', ARRAY[1, 2, 3, 6, 9, 12, 15, 18, 21, 24]), (10, NULL, '4AU8', ARRAY[4, 8, 30]), (10, NULL, 'ZZZZ', ARRAY[5])",
            "CREATE VIEW interactiondb_search_view_pocket_ligands AS SELECT pocket_key FROM ligands",
            "CREATE VIEW interactiondb_search_view_molecule_property_pockets_num AS "
            "SELECT prop_key, prop_value_num AS prop_value, pocket_keys AS pocket_key FROM props WHERE prop_value_num IS NOT NULL",
            "CREATE VIEW interactiondb_search_view_molecule_property_pockets_text AS "
            "SELECT prop_key, prop_value_text AS prop_value, pocket_keys AS pocket_key FROM props WHERE prop_value_text IS NOT NULL",
            "CREATE TABLE outer_t (pocket_key int, partition_key int, pcache_key int)",
            "INSERT INTO outer_t SELECT g, g, g FROM generate_series(1, 30) AS g",
            "CREATE TABLE inner_t (v int)",
            "INSERT INTO inner_t SELECT g FROM generate_series(1, 30) AS g",
            "CREATE TABLE outer_text (pocket_key text, partition_key text, pcache_key text)",
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
    @pytest.mark.parametrize("name", list(GEOMINE))
    def test_geomine_statements_fold_and_keep_their_rows(self, pg, name, method, lazy):
        run, execute, _, _ = pg
        query = GEOMINE[name]
        resolver = catalog_column_resolver(execute)

        def extend(fold):
            kwargs = {"method": method, "p0_alias": "t1", "fold_key_subqueries": fold, "fold_column_resolver": resolver}
            if lazy:
                return extend_query_with_partition_keys_lazy(query, "SELECT g AS pocket_key FROM generate_series(1, 35) AS g", "pocket_key",
                                                             partition_datatype="integer", **kwargs)
            return extend_query_with_partition_keys(query, set(range(1, 36)), "pocket_key", **kwargs)

        folded = extend(True)
        assert "pcache_folded_1" in folded
        reference = run(query)
        assert reference, "the test data must give rows"
        assert run(folded) == run(extend(False)) == {k for k in reference if k <= 35}

    @pytest.mark.parametrize("name", ["partition_key", "pcache_key", "pocket_key"])
    @pytest.mark.parametrize(
        ("method", "lazy", "keys"),
        [
            ("TMP_TABLE_IN", False, set(range(1, 31))),
            ("UNLOGGED_TABLE_JOIN", False, set(range(1, 31))),
            ("TMP_TABLE_JOIN", True, None),
            ("UNLOGGED_TABLE_IN", True, None),
            ("TMP_TABLE_IN", False, {str(k) for k in range(1, 31)}),  # text keys (this tree has no typed staging table: #68)
        ],
        ids=["eager_tmp", "eager_unlogged", "lazy_tmp", "lazy_unlogged", "text_keys"],
    )
    def test_a_wrong_assumption_errors_in_every_postgresql_form(self, pg, method, lazy, keys, name):
        """A column that binds outward, named like a key table's column: folded on the caller's word it must fail, not bind."""
        run, _, _, psycopg = pg
        if keys is not None and isinstance(next(iter(keys)), str):
            query = f"SELECT o.pocket_key FROM outer_text AS o WHERE o.pocket_key IN (SELECT i.v FROM inner_text AS i WHERE i.v = {name})"
        else:
            query = f"SELECT o.pocket_key FROM outer_t AS o WHERE o.pocket_key IN (SELECT i.v FROM inner_t AS i WHERE i.v = {name})"
        kwargs = {"method": method, "p0_alias": "o", "fold_key_subqueries": "any_key_type", "fold_assume_inner_columns": True}
        if lazy:
            script = extend_query_with_partition_keys_lazy(query, "SELECT g AS pocket_key FROM generate_series(1, 30) AS g", "pocket_key",
                                                           partition_datatype="integer", **kwargs)
        else:
            script = extend_query_with_partition_keys(query, keys, "pocket_key", **kwargs)
        assert "pcache_folded_1" in script
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
        assert resolver("interactiondb_search_view_molecule_property_pockets_num") == {"prop_key", "prop_value", "pocket_key"}  # a view
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
