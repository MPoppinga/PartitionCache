"""apply_cache / apply_cache_lazy against PostgreSQL: the rewritten query returns exactly the rows of the original.

The cache (postgresql_array) holds every variant the generator gives for the query and for the same query selecting
each alias's key, i.e. what different clients could have populated. Covered: lookups without key joins, the JOIN
methods (temporary and unlogged key tables, the renamed key column, join_every_table), use_p0_table and set
operations. Scripts are run in one execute() and read with fetch_final_result_set, as the product does.

The module name contains postgresql_array so the CI job of that backend selects it (-k postgresql_array).
"""

import os
import re

import psycopg
import pytest

from partitioncache.apply_cache import apply_cache, apply_cache_lazy
from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.db_handler.postgres import fetch_final_result_set
from partitioncache.query_processor import generate_all_query_hash_pairs

CONN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "integration_user"),
    "password": os.getenv("PG_PASSWORD", "test_password"),
    "dbname": os.getenv("PG_DBNAME", "partitioncache_integration"),
}
PREFIX = "pc_scope"
CACHE_PREFIX = f"{PREFIX}_cache"
T, D, EV, A, B, MV = (f"{PREFIX}_{name}" for name in ("t", "d", "events", "a", "b", "pk_mv"))
UNLOGGED = "partitioncache_unl_%"


def _drop_tables(connection, pattern):
    for (name,) in connection.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE %s", (pattern,)).fetchall():
        connection.execute(f'DROP TABLE IF EXISTS "{name}" CASCADE')


@pytest.fixture
def conn():
    try:
        connection = psycopg.connect(**CONN, autocommit=True)
    except psycopg.OperationalError as e:
        pytest.skip(f"PostgreSQL not available: {e}")
    _drop_tables(connection, f"{PREFIX}%")
    connection.execute(f"CREATE TABLE {T} (id INTEGER, pk INTEGER, x INTEGER, y INTEGER)")
    connection.execute(f"INSERT INTO {T} SELECT i, i % 20, i % 7, i % 5 FROM generate_series(1, 200) AS i")
    connection.execute(f"CREATE TABLE {D} (id INTEGER, z INTEGER)")
    connection.execute(f"INSERT INTO {D} SELECT i, i % 3 FROM generate_series(1, 200, 4) AS i")
    connection.execute(f"CREATE TABLE {MV} AS SELECT DISTINCT pk FROM {T}")
    # Events in hour buckets w; a login and the failures up to 5 minutes after it, some across a bucket boundary
    connection.execute(f"CREATE TABLE {EV} (id INTEGER, w INTEGER, ts INTEGER, kind TEXT)")
    connection.execute(
        f"INSERT INTO {EV} VALUES (1, 0, 3500, 'login'), (2, 1, 3700, 'fail'), (3, 2, 7300, 'login'), (4, 2, 7400, 'fail'), "
        "(5, 3, 11000, 'login'), (6, 4, 14500, 'fail'), (7, 5, 18100, 'login'), (8, 5, 18200, 'fail'), (9, 6, 21590, 'login'), "
        "(10, 6, 21620, 'fail'), (11, 8, 28790, 'login'), (12, 8, 29000, 'fail')"
    )
    # Two tables with a key of their own each: the keys of b (10-13) are none of a's (0-9)
    connection.execute(f"CREATE TABLE {A} (id INTEGER, w INTEGER, kind TEXT)")
    connection.execute(f"INSERT INTO {A} SELECT i, i % 10, CASE WHEN i % 3 = 0 THEN 'x' ELSE 'y' END FROM generate_series(1, 60) AS i")
    connection.execute(f"CREATE TABLE {B} (id INTEGER, a_id INTEGER, w INTEGER, val INTEGER)")
    connection.execute(f"INSERT INTO {B} SELECT i, i % 60 + 1, 10 + i % 4, i % 9 FROM generate_series(1, 120) AS i")
    yield connection
    _drop_tables(connection, f"{PREFIX}%")
    connection.close()


@pytest.fixture
def cache(conn):
    handler = PostgreSQLArrayCacheHandler(CONN["dbname"], CONN["host"], CONN["user"], CONN["password"], CONN["port"], CACHE_PREFIX)
    yield handler
    handler.db.rollback()
    handler.close()


def _populate(handler, query, partition_key, aliases, **settings):
    """Store every variant of the query and of the query selecting each alias's key, as lookups of both kinds generate them."""
    handler.register_partition_key(partition_key, "integer")
    head = re.match(r"\s*SELECT\s+(.*?)\s+FROM\s", query, re.IGNORECASE | re.DOTALL)
    assert head is not None
    queries = [query] + [query.replace(head.group(1), f"{alias}.{partition_key}", 1) for alias in aliases]
    stored = 0
    for source in queries:
        for keep_all_attributes in (False, True):
            for variant, key in generate_all_query_hash_pairs(
                source, partition_key, min_component_size=1, keep_all_attributes=keep_all_attributes, warn_no_partition_key=False, **settings
            ):
                if not handler.exists(key, partition_key):
                    assert handler.set_cache_lazy(key, variant, partition_key), variant
                    stored += 1
    assert stored > 0


def _rows(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)  # the whole script in one execute(): one transaction
        return sorted(fetch_final_result_set(cur))


def _unlogged_tables(conn):
    return conn.execute("SELECT count(*) FROM pg_tables WHERE tablename LIKE %s", (UNLOGGED,)).fetchone()[0]


def _check(conn, handler, query, partition_key, lazy, method, expect_enhanced=True, **kwargs):
    unlogged_before = _unlogged_tables(conn)
    apply = apply_cache_lazy if lazy else apply_cache
    rewritten, stats = apply(query, handler, partition_key, method=method, min_component_size=1, **kwargs)
    assert stats["enhanced"] == int(expect_enhanced), stats
    assert _rows(conn, rewritten) == _rows(conn, query), rewritten
    assert _unlogged_tables(conn) == unlogged_before, "an unlogged key table was left behind"
    return rewritten


LOOKUPS = [(False, "IN"), (True, "IN_SUBQUERY"), (False, "TMP_TABLE_JOIN"), (True, "TMP_TABLE_JOIN"), (False, "UNLOGGED_TABLE_JOIN"), (True, "UNLOGGED_TABLE_JOIN")]
LOOKUP_IDS = [f"{'lazy' if lazy else 'eager'}-{method}" for lazy, method in LOOKUPS]
JOIN_LOOKUPS = [lookup for lookup in LOOKUPS if lookup[1].endswith("_JOIN")]
JOIN_IDS = [name for name, lookup in zip(LOOKUP_IDS, LOOKUPS, strict=True) if lookup in JOIN_LOOKUPS]


class TestSkipPartitionKeyJoins:
    """Without key joins each alias has its own key: only the variants of the restricted alias's key may restrict it."""

    SELF_JOIN = (
        f"SELECT e1.id, e2.id FROM {EV} AS e1, {EV} AS e2 WHERE e1.kind = 'login' AND e2.kind = 'fail' "
        "AND e2.ts >= e1.ts AND e2.ts <= e1.ts + 300"
    )
    TWO_TABLES = f"SELECT b.w, b.id FROM {A} AS a, {B} AS b WHERE b.a_id = a.id AND a.kind = 'x' AND b.val > 2"

    @pytest.mark.parametrize(("lazy", "method"), LOOKUPS, ids=LOOKUP_IDS)
    def test_self_join_without_a_key_in_the_select(self, conn, cache, lazy, method):
        _populate(cache, self.SELF_JOIN, "w", ["e1", "e2"], skip_partition_key_joins=True)
        _check(conn, cache, self.SELF_JOIN, "w", lazy, method, skip_partition_key_joins=True)

    @pytest.mark.parametrize(("lazy", "method"), LOOKUPS, ids=LOOKUP_IDS)
    def test_restriction_on_the_first_table_while_the_select_names_the_second(self, conn, cache, lazy, method):
        _populate(cache, self.TWO_TABLES, "w", ["a", "b"], skip_partition_key_joins=True)
        _check(conn, cache, self.TWO_TABLES, "w", lazy, method, skip_partition_key_joins=True, p0_alias="a")


class TestJoinMethods:
    # The subquery's p1 has key 5, which the cached keys (y = 1, i.e. pk % 5 = 1) do not hold: joined to the key table,
    # it became empty and the NOT IN kept the rows with x = 4
    NOT_IN_REUSING_P1 = (
        f"SELECT p1.id FROM {T} AS p1 JOIN {T} AS p2 ON p1.pk = p2.pk WHERE p1.x IN (3, 4) AND p2.y = 1 "
        f"AND p1.x NOT IN (SELECT p1.x FROM {D} AS d1 JOIN {T} AS p1 ON d1.id = p1.id WHERE p1.pk = 5 AND p1.x = 4)"
    )
    UNQUALIFIED_KEY = f"SELECT pk, p1.id FROM {T} AS p1 WHERE p1.x = 3 AND pk > 4"
    KEY_JOINED = f"SELECT p1.id, p2.id FROM {T} AS p1, {T} AS p2 WHERE p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1"

    @pytest.mark.parametrize(("lazy", "method"), JOIN_LOOKUPS, ids=JOIN_IDS)
    def test_p0_alias_reused_in_a_not_in_subquery(self, conn, cache, lazy, method):
        _populate(cache, self.NOT_IN_REUSING_P1, "pk", ["p1", "p2"])
        rewritten = _check(conn, cache, self.NOT_IN_REUSING_P1, "pk", lazy, method)
        assert len(re.findall(r"AS tmp_\w+ ON", rewritten)) == 1

    @pytest.mark.parametrize(("lazy", "method"), JOIN_LOOKUPS, ids=JOIN_IDS)
    def test_unqualified_key_column(self, conn, cache, lazy, method):
        _populate(cache, self.UNQUALIFIED_KEY, "pk", ["p1"])
        rewritten = _check(conn, cache, self.UNQUALIFIED_KEY, "pk", lazy, method)
        if lazy:  # the lazy key table's column is named like the key; the eager one partition_key, which does not clash
            assert "tmp_p1.pcache_key = p1.pk" in rewritten

    @pytest.mark.parametrize(("lazy", "method"), JOIN_LOOKUPS, ids=JOIN_IDS)
    def test_join_every_table(self, conn, cache, lazy, method):
        _populate(cache, self.KEY_JOINED, "pk", ["p1", "p2"])
        rewritten = _check(conn, cache, self.KEY_JOINED, "pk", lazy, method, join_every_table=True)
        assert len(re.findall(r"AS tmp_\w+ ON", rewritten)) == 2


class TestUseP0Table:
    # Conditions on p1 only: the variants are relaxations of p1, and the OR must stay as it is
    KEY_EQUALITY_UNDER_OR = f"SELECT p1.id, p2.id FROM {T} AS p1, {T} AS p2 WHERE (p1.pk = p2.pk OR p2.id = p1.id + 1) AND p1.x = 3"

    @pytest.mark.parametrize(("lazy", "method"), [(False, "IN"), (True, "IN_SUBQUERY"), (False, "TMP_TABLE_IN")], ids=["eager-IN", "lazy-IN_SUBQUERY", "eager-TMP_TABLE_IN"])
    def test_or_with_a_key_equality(self, conn, cache, lazy, method):
        _populate(cache, self.KEY_EQUALITY_UNDER_OR, "pk", ["p1", "p2"])
        rewritten = _check(conn, cache, self.KEY_EQUALITY_UNDER_OR, "pk", lazy, method, use_p0_table=True, p0_table_name=MV)
        assert "p1.pk = p2.pk OR" in rewritten and "p2.pk = p0.pk" not in rewritten


class TestSetOperations:
    FIRST_BRANCH_WITHOUT_WHERE = (
        f"SELECT p1.id FROM {T} AS p1 JOIN {T} AS p2 ON p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1 UNION SELECT p1.id FROM {T} AS p1 WHERE p1.y = 2"
    )
    THREE_BRANCHES = (
        f"SELECT p1.id FROM {T} AS p1 WHERE p1.x = 3 UNION SELECT p1.id FROM {T} AS p1 WHERE p1.y = 2 UNION SELECT p1.id FROM {T} AS p1 WHERE p1.y = 4"
    )

    @pytest.mark.parametrize(("lazy", "method"), LOOKUPS, ids=LOOKUP_IDS)
    def test_first_branch_without_where(self, conn, cache, lazy, method):
        _populate(cache, self.FIRST_BRANCH_WITHOUT_WHERE, "pk", [])
        rewritten = _check(conn, cache, self.FIRST_BRANCH_WITHOUT_WHERE, "pk", lazy, method)
        second = rewritten.rpartition(" UNION ")[2]
        assert "pk IN" not in second and "tmp_" not in second

    @pytest.mark.parametrize(("lazy", "method"), LOOKUPS, ids=LOOKUP_IDS)
    def test_three_branches_run_uncached(self, conn, cache, lazy, method):
        _populate(cache, self.THREE_BRANCHES, "pk", [])
        _check(conn, cache, self.THREE_BRANCHES, "pk", lazy, method, expect_enhanced=False)
