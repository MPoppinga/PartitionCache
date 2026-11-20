"""isolate_key_table_join: the JOIN methods write the key table's side of the ON clause as COALESCE(k, k).

The rows are those of the plain join. On PostgreSQL the key table's column then stays out of the planner's
equivalence class of the partition key, so the key table is joined to the restricted table instead of to any table
equated on the key. The PostgreSQL tests run with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB.
"""

import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from partitioncache.apply_cache import apply_cache, apply_cache_lazy, extend_query_with_partition_keys, extend_query_with_partition_keys_lazy
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy
from partitioncache.key_table_script import lazy_insertion_parts

JOINED = "SELECT a.pk FROM a, b, c WHERE a.pk = b.pk AND b.pk = c.pk AND b.y = 1 AND c.z = 2"
SUBQUERY = "SELECT 1 AS partition_key"
ISOLATED_ON = re.compile(r"AS tmp_(\w+) ON COALESCE\(tmp_\1\.(\w+), tmp_\1\.\2\) = \1\.pk")
PLAIN_ON = re.compile(r"AS tmp_(\w+) ON tmp_\1\.\w+ = \1\.pk")


def _eager(query, method, **kwargs):
    return extend_query_with_partition_keys(query, {1, 2}, "pk", method=method, **kwargs)


def _lazy(query, method, **kwargs):
    return extend_query_with_partition_keys_lazy(query, SUBQUERY, "pk", method=method, **kwargs)


@pytest.mark.parametrize("extend", [_eager, _lazy], ids=["eager", "lazy"])
@pytest.mark.parametrize("method", ["TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN"])
class TestJoinMethods:
    def test_default_is_unchanged(self, extend, method):
        script = extend(JOINED, method)
        assert "COALESCE" not in script
        assert {m.group(1) for m in PLAIN_ON.finditer(script)} == {"a"}

    def test_isolated_key_side(self, extend, method):
        script = extend(JOINED, method, isolate_key_table_join=True)
        assert {m.group(1) for m in ISOLATED_ON.finditer(script)} == {"a"}
        assert not PLAIN_ON.search(script)

    def test_with_join_every_table_every_join_is_isolated(self, extend, method):
        script = extend(JOINED, method, join_every_table=True, isolate_key_table_join=True)
        assert {m.group(1) for m in ISOLATED_ON.finditer(script)} == {"a", "b", "c"}
        assert not PLAIN_ON.search(script)

    def test_with_folding_the_folded_key_table_is_joined_isolated(self, extend, method):
        query = "SELECT a.pk FROM a, b WHERE a.pk = b.pk AND a.pk IN (SELECT s.k FROM s WHERE s.x = 1)"
        script = extend(query, method, fold_key_subqueries=True, isolate_key_table_join=True, **({"partition_datatype": "integer"} if extend is _lazy else {}))
        assert "INTERSECT" in script and "IN (SELECT s.k" not in script.split(";")[-2 if method == "UNLOGGED_TABLE_JOIN" else -1]
        assert {m.group(1) for m in ISOLATED_ON.finditer(script)} == {"a"}


@pytest.mark.parametrize("method", ["IN", "VALUES", "TMP_TABLE_IN", "UNLOGGED_TABLE_IN"])
def test_refused_for_methods_without_a_joined_key_table(method):
    with pytest.raises(ValueError, match="isolate_key_table_join applies to TMP_TABLE_JOIN and UNLOGGED_TABLE_JOIN"):
        _eager(JOINED, method, isolate_key_table_join=True)
    # also for an empty key set, which is rewritten without a key table
    with pytest.raises(ValueError, match="isolate_key_table_join applies to"):
        extend_query_with_partition_keys(JOINED, set(), "pk", method=method, isolate_key_table_join=True)


@pytest.mark.parametrize("method", ["IN_SUBQUERY", "TMP_TABLE_IN", "UNLOGGED_TABLE_IN"])
def test_refused_for_lazy_methods_without_a_joined_key_table(method):
    with pytest.raises(ValueError, match="isolate_key_table_join applies to"):
        _lazy(JOINED, method, isolate_key_table_join=True)


@pytest.mark.parametrize("dialect", ["postgres", "duckdb", "sqlite"])
def test_rendered_in_every_dialect(dialect):
    script = extend_query_with_partition_keys(JOINED, {1, 2}, "pk", method="TMP_TABLE_JOIN", sql_dialect=dialect, isolate_key_table_join=True)
    assert ISOLATED_ON.search(script)


def test_apply_cache_passes_the_option():
    handler = Mock(spec=AbstractCacheHandler)
    handler.get_intersected.return_value = ({1, 2}, 2)
    plain, _ = apply_cache(JOINED, handler, "pk", method="TMP_TABLE_JOIN", min_component_size=1)
    isolated, _ = apply_cache(JOINED, handler, "pk", method="TMP_TABLE_JOIN", min_component_size=1, isolate_key_table_join=True)
    assert PLAIN_ON.search(plain) and ISOLATED_ON.search(isolated)
    with pytest.raises(ValueError, match="isolate_key_table_join applies to"):
        apply_cache(JOINED, handler, "pk", method="IN", min_component_size=1, isolate_key_table_join=True)


def test_apply_cache_lazy_passes_the_option():
    handler = Mock(spec=AbstractCacheHandler_Lazy)
    handler.get_intersected_lazy.return_value = (SUBQUERY, 2)
    plain, _ = apply_cache_lazy(JOINED, handler, "pk", method="TMP_TABLE_JOIN", min_component_size=1)
    isolated, _ = apply_cache_lazy(JOINED, handler, "pk", method="TMP_TABLE_JOIN", min_component_size=1, isolate_key_table_join=True)
    assert PLAIN_ON.search(plain) and ISOLATED_ON.search(isolated)
    with pytest.raises(ValueError, match="isolate_key_table_join applies to"):
        apply_cache_lazy(JOINED, handler, "pk", method="IN_SUBQUERY", min_component_size=1, isolate_key_table_join=True)


def test_population_passes_the_option():
    from partitioncache.population import restrict_with_cache

    cache = Mock(spec=AbstractCacheHandler)
    cache.get_intersected.return_value = ({1, 2}, 1)
    variant = "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.pk = t2.pk AND t2.y = 1"
    restriction = restrict_with_cache(variant, cache, "pk", {}, method="UNLOGGED_TABLE_JOIN", isolate_key_table_join=True)
    assert restriction.restricted and "COALESCE(tmp_t1." in restriction.query
    with pytest.raises(ValueError, match="isolate_key_table_join applies to"):
        restrict_with_cache(variant, cache, "pk", {}, method="IN", isolate_key_table_join=True)


def test_monitor_passes_the_option():
    from partitioncache.cli.monitor_cache_queue import apply_cache_optimization

    args = Mock()
    args.enable_cache_optimization, args.disable_cache_optimization, args.prefer_lazy_optimization = True, False, False
    args.max_restriction_keys, args.min_cache_hits, args.db_backend = None, 1, "postgresql"
    args.cache_optimization_method, args.join_every_table, args.isolate_key_table_join = "UNLOGGED_TABLE_JOIN", False, True
    args.planner_collapse_limit, args.fold_key_subqueries = None, None
    args.min_component_size, args.follow_graph, args.no_auto_detect_partition_join = 1, True, False
    args.max_component_size, args.partition_join_table, args.bucket_steps = None, None, 1.0
    args.skip_partition_key_joins, args.max_conditions_removed, args.grid_cell_size = [], None, None
    handler = Mock(spec=AbstractCacheHandler)
    handler.get_intersected.return_value = ({1, 2}, 2)
    fragment = "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.pk = t2.pk AND t2.y = 1"

    restricted, applied, _ = apply_cache_optimization(fragment, "h", "pk", "integer", handler, args)

    assert applied and "COALESCE(tmp_t1." in restricted


@pytest.mark.parametrize(
    "cli,argv",
    [
        ("monitor", ["--cache-optimization-method", "TMP_TABLE_IN", "--isolate-key-table-join"]),
        ("add", ["--restriction-method", "TMP_TABLE_IN", "--isolate-key-table-join"]),
    ],
)
def test_cli_refuses_the_flag_without_a_join_method(cli, argv, monkeypatch, capsys):
    import sys

    if cli == "monitor":
        from partitioncache.cli.monitor_cache_queue import main
    else:
        from partitioncache.cli.add_to_cache import main
        argv = ["--query", "SELECT 1", "--partition-key", "pk", "--direct", *argv]
    monkeypatch.setattr(sys, "argv", [cli, *argv])
    with pytest.raises(SystemExit) as exit_info:
        main()
    assert exit_info.value.code == 2
    assert "--isolate-key-table-join needs" in capsys.readouterr().err


# --- PostgreSQL ---------------------------------------------------------------------------------------------------


def _params() -> dict:
    if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    return {
        "host": os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
        "port": int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
        "user": os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        "password": os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        "dbname": os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
    }


@pytest.fixture
def pg():
    import psycopg

    conn = psycopg.connect(**_params(), autocommit=True)
    conn.prepare_threshold = None  # the same statements run against tables of changing types
    conn.execute("SET TimeZone = 'Europe/Berlin'")
    suffix = uuid.uuid4().hex[:8]
    yield conn, suffix
    for table in ("anchor", "keys", "facts"):
        conn.execute(f"DROP TABLE IF EXISTS iso_{table}_{suffix}")
    conn.close()


# Values per key type, with the edge cases of each comparison: -0.0 and 0.0, NaN, infinities, 1.5 and 1.50, trailing
# blanks, an empty string, and a timestamp in the hour that repeats when daylight saving time ends.
KEY_TYPES = {
    "integer": ["1", "2", "-5", "2147483647"],
    "bigint": ["1", "9223372036854775807", "'-9223372036854775808'"],
    "numeric": ["1.50", "-0.0", "-12.5", "123456789012345678901234567890.123"],
    "real": ["'-0.0'", "'NaN'", "'Infinity'", "27.04", "0.7"],
    "double precision": ["'-0.0'", "'NaN'", "'-Infinity'", "27.04", "1e-300"],
    "text": ["'a'", "'a '", "''", "'Ä'"],
    "character(5)": ["'ab'", "'abcde'", "''"],
    "date": ["'2026-01-01'", "'infinity'", "'0001-01-01 BC'"],
    "timestamp": ["'2026-03-29 02:30'", "'infinity'", "'2000-01-01 00:00:00.000001'"],
    "timestamptz": ["'2026-10-25 02:30+02'", "'2026-10-25 02:30+01'", "'-infinity'"],
}


@pytest.mark.parametrize("key_type", list(KEY_TYPES))
def test_coalesce_join_has_the_rows_of_the_plain_join(pg, key_type):
    """COALESCE(k, k) = a.pk, k = a.pk and a.pk IN (keys) give the same rows, for every key type."""
    conn, s = pg
    values = KEY_TYPES[key_type]
    conn.execute(f"CREATE TABLE iso_anchor_{s} (pk {key_type} PRIMARY KEY)")
    conn.execute(f"INSERT INTO iso_anchor_{s} VALUES " + ", ".join(f"({v})" for v in values) + ", (" + {"text": "'zz'", "character(5)": "'zz'", "date": "'2026-02-02'", "timestamp": "'2026-02-02'", "timestamptz": "'2026-02-02 00:00+00'"}.get(key_type, "42") + ")")
    conn.execute(f"CREATE TABLE iso_keys_{s} AS SELECT DISTINCT pk AS k FROM iso_anchor_{s} WHERE pk IN (" + ", ".join(f"{v}::{key_type}" for v in values) + ")")

    def rows(sql):
        return sorted(r[0] for r in conn.execute(sql).fetchall())

    plain = rows(f"SELECT a.pk::text FROM iso_anchor_{s} AS a JOIN iso_keys_{s} AS kt ON kt.k = a.pk")
    isolated = rows(f"SELECT a.pk::text FROM iso_anchor_{s} AS a JOIN iso_keys_{s} AS kt ON COALESCE(kt.k, kt.k) = a.pk")
    semi = rows(f"SELECT a.pk::text FROM iso_anchor_{s} AS a WHERE a.pk IN (SELECT k FROM iso_keys_{s})")
    assert isolated == plain == semi
    assert len(plain) == len(values)
    assert conn.execute(f"SELECT pg_typeof(COALESCE(k, k)) = pg_typeof(k) FROM iso_keys_{s} LIMIT 1").fetchone() == (True,)


BERLIN = timezone(timedelta(hours=1))
LIBRARY_KEYS = {
    "integer": {1, 2, 5},
    "real": {0.5, 27.04, -1.25},
    "text": {"a", "a ", "Ä"},
    "character(5)": {"ab", "abcde"},
    "timestamp": {datetime(2026, 3, 29, 2, 30), datetime(2000, 1, 1, 0, 0, 0, 1)},
    "timestamptz": {datetime(2026, 10, 25, 2, 30, tzinfo=timezone(timedelta(hours=2))), datetime(2026, 10, 25, 2, 30, tzinfo=BERLIN)},
}


@pytest.mark.parametrize("method", ["TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN"])
@pytest.mark.parametrize("key_type", list(LIBRARY_KEYS))
def test_library_scripts_have_the_rows_of_the_plain_join(pg, key_type, method):
    """The library's isolated script returns the rows of its plain script and of the IN form, also with join_every_table."""
    conn, s = pg
    keys = LIBRARY_KEYS[key_type]
    conn.execute(f"CREATE TABLE iso_anchor_{s} (pk {key_type}, x int)")
    conn.execute(f"CREATE TABLE iso_facts_{s} (pk {key_type}, kind int)")
    with conn.cursor() as cursor:
        for key in [*keys, *{"integer": [9], "real": [3.5], "timestamp": [datetime(1999, 1, 1)], "timestamptz": [datetime(1999, 1, 1, tzinfo=BERLIN)]}.get(key_type, ["zz"])]:
            cursor.execute(f"INSERT INTO iso_anchor_{s} VALUES (%s, 1)", (key,))
            cursor.execute(f"INSERT INTO iso_facts_{s} VALUES (%s, 1), (%s, 2), (%s, 2)", (key, key, key))
    query = f"SELECT a.pk::text FROM iso_anchor_{s} AS a, iso_facts_{s} AS f1, iso_facts_{s} AS f2 WHERE f1.pk = a.pk AND f2.pk = a.pk AND f1.kind = 1 AND f2.kind = 2"

    def run(script):
        setup, select, _ = lazy_insertion_parts(script)
        with conn.transaction(force_rollback=True):  # the key table goes with the rolled-back transaction
            if setup:
                conn.execute(setup)
            return sorted(r[0] for r in conn.execute(select).fetchall())

    def script(**kwargs):
        return extend_query_with_partition_keys(query, keys, "pk", p0_alias=kwargs.pop("p0_alias", "a"), **kwargs)

    plain = run(script(method=method))
    isolated = run(script(method=method, isolate_key_table_join=True))
    every = run(script(method=method, p0_alias=None, join_every_table=True, isolate_key_table_join=True))
    semi = run(script(method=method.replace("JOIN", "IN")))
    assert isolated == plain == every == semi
    assert len(plain) == 2 * len(keys)  # one kind-1 row times two kind-2 rows per key


def test_isolated_key_table_is_joined_to_the_restricted_table(pg):
    """A unique list table larger than the key table, a fact table aliased five times and equated with it on the key.

    Plain, the key table's column joins the equivalence class and can be joined to a fact alias; isolated, its only
    join partner is the list table, through whose index it drives the plan, and the estimate stays near the result.
    """
    conn, s = pg
    conn.execute(f"CREATE TABLE iso_anchor_{s} (region_id int PRIMARY KEY)")
    conn.execute(f"INSERT INTO iso_anchor_{s} SELECT g FROM generate_series(1, 20000) g")
    conn.execute(f"CREATE TABLE iso_facts_{s} (region_id int NOT NULL, kind int NOT NULL, v real NOT NULL)")
    conn.execute(
        f"INSERT INTO iso_facts_{s} SELECT r * 10, k, ((r * 7 + k * 13 + rep * 31) % 100)::real "
        "FROM generate_series(1, 2000) r, generate_series(1, 5) k, generate_series(1, 20) rep"
    )
    conn.execute(f"CREATE INDEX ON iso_facts_{s} (region_id)")
    conn.execute(f"ANALYZE iso_anchor_{s}")
    conn.execute(f"ANALYZE iso_facts_{s}")
    aliases = [f"f{i}" for i in range(1, 6)]
    query = (
        f"SELECT DISTINCT a.region_id FROM iso_anchor_{s} AS a, " + ", ".join(f"iso_facts_{s} AS {x}" for x in aliases) + " WHERE "
        + " AND ".join([f"{x}.region_id = a.region_id" for x in aliases] + [f"{x}.kind = {i}" for i, x in enumerate(aliases, 1)] + [f"ABS({x}.v - f1.v) < 3" for x in aliases[1:]])
    )
    keys = set(range(10, 2010, 10))
    script = extend_query_with_partition_keys(query, keys, "region_id", method="UNLOGGED_TABLE_JOIN", p0_alias="a", isolate_key_table_join=True)
    setup, select, _ = lazy_insertion_parts(script)
    with conn.transaction(force_rollback=True):
        conn.execute(setup)
        plan = conn.execute("EXPLAIN (FORMAT JSON) " + select).fetchone()[0][0]["Plan"]
        result = len(conn.execute(select).fetchall())

    def nodes(node):
        yield node
        for child in node.get("Plans", []):
            yield from nodes(child)

    conditions = [
        (node, value)
        for node in nodes(plan)
        for field, value in node.items()
        if field.endswith("Cond") and isinstance(value, str) and "COALESCE(tmp_a." in value
    ]
    assert conditions, json.dumps(plan)[:2000]
    for node, condition in conditions:
        on_list_table = node.get("Relation Name") == f"iso_anchor_{s}" or "a.region_id" in condition
        assert on_list_table, condition
    assert result == len(keys)
    assert result / 10 <= plan["Plan Rows"] <= result * 10
