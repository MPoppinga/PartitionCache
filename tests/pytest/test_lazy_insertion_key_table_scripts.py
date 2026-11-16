"""
Lazy insertion of a fragment restricted with a key-table method (TMP_TABLE_*, UNLOGGED_TABLE_*).

pcache-monitor restricts a fragment with the cache and stores it by lazy insertion (``set_entry_lazy``). With a
key-table method the restricted fragment is a script, and the handlers refused it ("Query contains DELETE or DROP",
for the unlogged table's DROP). Two defects followed:

- ``set_entry_lazy`` still wrote the query row: the fragment read as done (status 'ok') with no cache row. A
  harness that waits for the query rows stopped the monitor while it computed the fragment on its fallback path,
  and the entry was lost.
- The fallback moved every key through Python, so a key-table method was not measured as lazy insertion.

The handlers now run the script's setup, store its SELECT's keys in the database and run its teardown, in one
transaction (``key_table_script.lazy_insertion_parts``), and refuse any other statement. The SQL-text tests need no
database; TestAgainstPostgreSQL runs with PARTITIONCACHE_TEST_PG_HOST/PORT/USER/PASSWORD/DB.
"""

import os
import uuid

import pytest

from partitioncache.apply_cache import extend_query_with_partition_keys, extend_query_with_partition_keys_lazy
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy
from partitioncache.key_table_script import lazy_insertion_parts

SCRIPT_METHODS = ["TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_IN", "UNLOGGED_TABLE_JOIN"]
FRAGMENT = (
    "SELECT DISTINCT s.site_id FROM sites AS s, readings AS r WHERE r.site_id = s.site_id AND r.kind = 'A' AND s.zone = 'north;east'"
)
KEY_SUBQUERY = "s.site_id IN (SELECT m.site_id FROM site_meta AS m WHERE m.level >= 2)"
LAZY_KEYS = "SELECT g AS site_id FROM generate_series(1, 50) AS g"


def _scripts():
    """Every script shape the key-table methods emit for PostgreSQL: eager and lazy, with the collapse limits, folded."""
    for method in SCRIPT_METHODS:
        yield f"{method}/eager", extend_query_with_partition_keys(FRAGMENT, set(range(1, 40)), "site_id", method=method, p0_alias="s")
        yield f"{method}/lazy", extend_query_with_partition_keys_lazy(FRAGMENT, LAZY_KEYS, "site_id", method=method, p0_alias="s")
        yield f"{method}/collapse", extend_query_with_partition_keys_lazy(
            FRAGMENT, LAZY_KEYS, "site_id", method=method, p0_alias="s", planner_collapse_limit=12
        )
        folded = f"{FRAGMENT} AND {KEY_SUBQUERY}"
        yield f"{method}/folded", extend_query_with_partition_keys_lazy(
            folded, LAZY_KEYS, "site_id", method=method, p0_alias="s", fold_key_subqueries=True, partition_datatype="integer"
        )
        yield f"{method}/folded_eager", extend_query_with_partition_keys(
            folded, set(range(1, 40)), "site_id", method=method, p0_alias="s", fold_key_subqueries=True
        )
        yield f"{method}/text_keys", extend_query_with_partition_keys(FRAGMENT, {"a", "b"}, "site_id", method=method, p0_alias="s")


class TestSplitting:
    def test_a_single_query_passes_unchanged(self):
        assert lazy_insertion_parts(FRAGMENT) == ("", FRAGMENT, "")
        assert lazy_insertion_parts(FRAGMENT + ";") == ("", FRAGMENT + ";", "")

    @pytest.mark.parametrize("query", ["SELECT 1 AS k; DELETE FROM sites", "SELECT k FROM t WHERE x = 'DROP TABLE'", "DELETE FROM sites"])
    def test_a_single_query_keeps_the_old_guard(self, query):
        with pytest.raises(ValueError):
            lazy_insertion_parts(query)

    @pytest.mark.parametrize(("name", "script"), list(_scripts()), ids=[name for name, _ in _scripts()])
    def test_every_emitted_script_splits_around_its_select(self, name, script):
        setup, select, teardown = lazy_insertion_parts(script)
        assert setup.strip() and select.lstrip().upper().startswith("SELECT"), name
        assert "zone = 'north;east'" in select, "a semicolon in a literal does not split"
        assert (teardown.strip().upper().startswith("DROP TABLE IF EXISTS PARTITIONCACHE_UNL_")) == name.startswith("UNLOGGED"), name
        assert "".join((setup, select, teardown)).replace(";", "").split() == script.replace(";", "").split()

    @pytest.mark.parametrize(
        "extra",
        [
            "DELETE FROM sites;",
            "UPDATE sites SET zone = 'x';",
            "CREATE TABLE sites_copy AS (SELECT * FROM sites);",
            "DROP TABLE sites;",
            "INSERT INTO sites VALUES (1);",
            "SET LOCAL statement_timeout = 0;",
        ],
    )
    def test_a_statement_the_methods_do_not_emit_is_refused(self, extra):
        script = extend_query_with_partition_keys_lazy(FRAGMENT, LAZY_KEYS, "site_id", method="UNLOGGED_TABLE_JOIN", p0_alias="s")
        with pytest.raises(ValueError):
            lazy_insertion_parts(f"{extra}\n{script}")

    def test_a_teardown_of_another_table_is_refused(self):
        script = extend_query_with_partition_keys_lazy(FRAGMENT, LAZY_KEYS, "site_id", method="UNLOGGED_TABLE_JOIN", p0_alias="s")
        with pytest.raises(ValueError):
            lazy_insertion_parts(f"{script};\nDROP TABLE IF EXISTS sites")


class _Refusing(AbstractCacheHandler_Lazy):
    """A lazy handler whose lazy insertion fails; set_query records the calls."""

    def __init__(self):
        self.queries = []

    def set_cache_lazy(self, key, query, partition_key="partition_key"):
        return False

    def set_query(self, key, querytext, partition_key="partition_key"):
        self.queries.append(key)
        return True

    def exists(self, key, partition_key="partition_key", check_query=False):
        return False

    def __getattr__(self, name):  # the abstract interface's other methods are not called here
        raise AttributeError(name)


_Refusing.__abstractmethods__ = frozenset()


def test_set_entry_lazy_writes_no_query_row_without_its_cache_row():
    handler = _Refusing()
    assert handler.set_entry_lazy("h", "SELECT 1 AS k", "SELECT 1 AS k", "k") is False
    assert handler.queries == []


def _pg_params() -> dict:
    if not os.getenv("PARTITIONCACHE_TEST_PG_HOST"):
        pytest.skip("PARTITIONCACHE_TEST_PG_HOST not set")
    return {
        "db_host": os.getenv("PARTITIONCACHE_TEST_PG_HOST", ""),
        "db_port": int(os.getenv("PARTITIONCACHE_TEST_PG_PORT", "5432")),
        "db_user": os.getenv("PARTITIONCACHE_TEST_PG_USER", "postgres"),
        "db_password": os.getenv("PARTITIONCACHE_TEST_PG_PASSWORD", ""),
        "db_name": os.getenv("PARTITIONCACHE_TEST_PG_DB", "postgres"),
    }


def _handler(backend: str, prefix: str, bitsize: int = 64):
    params = _pg_params()
    if backend == "bit":
        from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

        return PostgreSQLBitCacheHandler(**params, db_tableprefix=prefix, bitsize=bitsize)
    if backend == "roaringbit":
        from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

        return PostgreSQLRoaringBitCacheHandler(**params, db_tableprefix=prefix)
    from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

    return PostgreSQLArrayCacheHandler(**params, db_tableprefix=prefix)


class TestAgainstPostgreSQL:
    """A restricted fragment stored lazily holds exactly the keys of the fragment, and leaves no key table behind."""

    @pytest.fixture(scope="class")
    def data(self):
        import psycopg

        params = _pg_params()
        conn = psycopg.connect(
            host=params["db_host"], port=params["db_port"], user=params["db_user"], password=params["db_password"], dbname=params["db_name"],
            autocommit=True,
        )
        schema_suffix = uuid.uuid4().hex[:6]
        sites, readings = f"lz_sites_{schema_suffix}", f"lz_readings_{schema_suffix}"
        conn.execute(f"CREATE TABLE {sites} (site_id int, zone text)")
        conn.execute(f"CREATE TABLE {readings} (site_id int, kind text, level int)")
        # sites up to 120: a bit handler created with bitsize 64 has to grow while it stores the fragment
        conn.execute(f"INSERT INTO {sites} SELECT g, CASE WHEN g % 3 = 0 THEN 'north' ELSE 'south' END FROM generate_series(1, 120) AS g")
        conn.execute(f"INSERT INTO {readings} SELECT g % 120 + 1, CASE WHEN g % 4 = 0 THEN 'A' ELSE 'B' END, g % 5 FROM generate_series(1, 2000) AS g")
        yield conn, sites, readings
        conn.execute(f"DROP TABLE {sites}, {readings}")
        conn.close()

    @pytest.mark.parametrize("backend", ["array", "bit", "roaringbit"])
    @pytest.mark.parametrize("method", SCRIPT_METHODS)
    def test_restricted_fragment_is_stored_with_its_keys(self, data, backend, method):
        from partitioncache.apply_cache import apply_cache_lazy
        from partitioncache.query_processor import generate_all_query_hash_pairs, hash_query

        conn, sites, readings = data
        fragment = (
            f"SELECT DISTINCT s.site_id FROM {sites} AS s, {readings} AS r "
            "WHERE r.site_id = s.site_id AND r.kind = 'A' AND s.zone = 'north' AND r.level >= 2"
        )
        expected = {row[0] for row in conn.execute(fragment)}
        assert expected, "the data must give the fragment rows"
        prefix = f"lz_{backend}_{uuid.uuid4().hex[:6]}"
        handler = _handler(backend, prefix)
        try:
            handler.register_partition_key("site_id", "integer")
            # Cache the relaxations, so the fragment gets cache hits and is rewritten into a script
            for variant, variant_hash in generate_all_query_hash_pairs(fragment, "site_id", min_component_size=1, warn_no_partition_key=False):
                if variant_hash != hash_query(fragment):
                    assert handler.set_cache(variant_hash, {row[0] for row in conn.execute(variant)}, "site_id")
            script, stats = apply_cache_lazy(fragment, handler, "site_id", method=method, p0_alias="s", min_component_size=1)
            assert stats["enhanced"] == 1 and ";" in script, stats

            assert handler.set_entry_lazy("fragment", script, fragment, "site_id") is True
            assert set(handler.get("fragment", "site_id")) == expected
            assert handler.get_query("fragment", "site_id") == fragment
            left = conn.execute("SELECT count(*) FROM pg_class WHERE relname LIKE 'partitioncache_unl_%' AND relpersistence = 'u'").fetchone()[0]
            assert left == 0, "the unlogged key table is dropped in the same transaction"
        finally:
            for table in ("cache_site_id", "queries", "partition_metadata"):
                handler.cursor.execute(f'DROP TABLE IF EXISTS "{prefix}_{table}" CASCADE')
            handler.db.commit()
            handler.close()

    def test_bit_growth_reruns_the_setup(self, data):
        """A key beyond the bitsize rolls the transaction back, grows the bit arrays and runs the whole script again."""
        conn, sites, _ = data
        prefix = f"lz_grow_{uuid.uuid4().hex[:6]}"
        handler = _handler("bit", prefix, bitsize=64)
        try:
            handler.register_partition_key("site_id", "integer")
            fragment = f"SELECT DISTINCT s.site_id FROM {sites} AS s WHERE s.zone = 'north'"
            expected = {row[0] for row in conn.execute(fragment)}
            assert max(expected) >= 64
            script = extend_query_with_partition_keys(fragment, set(range(1, 121)), "site_id", method="UNLOGGED_TABLE_JOIN", p0_alias="s")
            assert handler.set_entry_lazy("grown", script, fragment, "site_id") is True
            assert set(handler.get("grown", "site_id")) == expected
            assert handler._get_partition_bitsize("site_id") > 64
        finally:
            for table in ("cache_site_id", "queries", "partition_metadata"):
                handler.cursor.execute(f'DROP TABLE IF EXISTS "{prefix}_{table}" CASCADE')
            handler.db.commit()
            handler.close()

    def test_a_refused_script_writes_no_query_row(self, data):
        conn, sites, _ = data
        prefix = f"lz_refused_{uuid.uuid4().hex[:6]}"
        handler = _handler("bit", prefix)
        try:
            handler.register_partition_key("site_id", "integer")
            script = f"DELETE FROM {sites};\nSELECT DISTINCT s.site_id FROM {sites} AS s"
            assert handler.set_entry_lazy("refused", script, "SELECT 1", "site_id") is False
            assert handler.get_query("refused", "site_id") is None
            assert conn.execute(f"SELECT count(*) FROM {sites}").fetchone()[0] == 120, "nothing of the script ran"
        finally:
            for table in ("cache_site_id", "queries", "partition_metadata"):
                handler.cursor.execute(f'DROP TABLE IF EXISTS "{prefix}_{table}" CASCADE')
            handler.db.commit()
            handler.close()
