"""
The restriction of apply_cache / apply_cache_lazy acts on the SELECT whose variants produced the keys, and only on it.

Every test runs the original and the rewritten SQL on DuckDB and compares the rows. The cache is a fake handler that
stores, for every variant the lookup generates, the keys of running that variant on the same DuckDB database, so a
cache hit holds exactly what population would have stored.
"""

import re

import pytest

from partitioncache import query_processor
from partitioncache.apply_cache import (
    apply_cache,
    apply_cache_lazy,
    extend_query_with_partition_keys,
    extend_query_with_partition_keys_lazy,
    get_partition_keys,
    rewrite_query_with_p0_table,
)
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

duckdb = pytest.importorskip("duckdb")

EAGER_METHODS = ["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]
LAZY_METHODS = ["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]


def _load(conn) -> None:
    conn.execute("CREATE TABLE t (id INTEGER, pk INTEGER, x INTEGER, y INTEGER)")
    conn.execute("INSERT INTO t SELECT i, i % 20, i % 7, i % 5 FROM range(1, 201) AS r(i)")
    conn.execute("CREATE TABLE pk_mv AS SELECT DISTINCT pk FROM t")  # the p0 table of use_p0_table
    # A key column named like the eager key table's column
    conn.execute("CREATE TABLE s (id INTEGER, partition_key INTEGER, x INTEGER)")
    conn.execute("INSERT INTO s SELECT i, i % 20, i % 7 FROM range(1, 101) AS r(i)")
    conn.execute("CREATE TABLE d (id INTEGER, z INTEGER)")
    conn.execute("INSERT INTO d SELECT i, i % 3 FROM range(1, 201, 4) AS r(i)")
    # Events in hour buckets (w); a pair's events are up to 30 minutes apart, so a pair can cross a bucket boundary
    conn.execute("CREATE TABLE events (id INTEGER, w INTEGER, ts INTEGER, kind TEXT)")
    conn.execute(
        "INSERT INTO events VALUES (1, 0, 3500, 'login'), (2, 1, 3700, 'fail'), (3, 2, 7300, 'login'), (4, 2, 7400, 'fail'), "
        "(5, 3, 11000, 'login'), (6, 4, 14500, 'fail'), (7, 5, 18100, 'login'), (8, 5, 18200, 'fail'), (9, 6, 21590, 'login'), "
        "(10, 6, 21620, 'fail'), (11, 8, 28790, 'login'), (12, 8, 29000, 'fail')"
    )


class DuckCache(AbstractCacheHandler_Lazy):
    """A cache whose entries are the keys of running each variant the lookup generates (see ``recording``)."""

    def __init__(self, conn, variants: dict[str, str]):
        self.conn = conn
        self.variants = variants
        self.entries: dict[str, set] = {}
        conn.execute("CREATE TABLE IF NOT EXISTS pcache_entries (h TEXT, k INTEGER)")

    def _entry(self, key: str) -> set | None:
        if key not in self.entries and key in self.variants:
            try:
                rows = self.conn.execute(self.variants[key]).fetchall()
            except duckdb.Error:
                return None  # a variant DuckDB cannot run is not cached
            self.entries[key] = {row[0] for row in rows if row[0] is not None}
            self.conn.executemany("INSERT INTO pcache_entries VALUES (?, ?)", [(key, k) for k in self.entries[key]])
        return self.entries.get(key)

    def filter_existing_keys(self, keys, partition_key="partition_key"):
        return {k for k in keys if self._entry(k) is not None}

    def get(self, key, partition_key="partition_key"):
        return self._entry(key)

    def get_datatype(self, partition_key):
        return "integer"

    def get_intersected(self, keys, partition_key="partition_key"):
        present = [self._entry(k) for k in sorted(self.filter_existing_keys(keys))]
        return (set.intersection(*present), len(present)) if present else (None, 0)

    def get_intersected_lazy(self, keys, partition_key="partition_key"):
        present = sorted(self.filter_existing_keys(keys))
        if not present:
            return None, 0
        members = " INTERSECT ".join(f"SELECT k AS {partition_key} FROM pcache_entries WHERE h = '{h}'" for h in present)
        return f"SELECT * FROM ({members}) AS pcache_lazy", len(present)


DuckCache.__abstractmethods__ = frozenset()


@pytest.fixture
def db(monkeypatch):
    """(run, cache): run executes SQL on DuckDB and returns the sorted rows; the cache knows every generated variant."""
    conn = duckdb.connect()
    _load(conn)
    variants: dict[str, str] = {}
    generate = query_processor.generate_all_query_hash_pairs

    def recording(*args, **kwargs):
        pairs = generate(*args, **kwargs)
        variants.update({h: q for q, h in pairs})
        return pairs

    monkeypatch.setattr(query_processor, "generate_all_query_hash_pairs", recording)

    def run(sql: str) -> list[tuple]:
        return sorted(conn.execute(sql).fetchall(), key=repr)

    return run, DuckCache(conn, variants)


def _eager(run, cache, query, method, **kwargs):
    kwargs.setdefault("min_component_size", 1)  # single-table variants, so that the small queries get cache hits
    rewritten, stats = apply_cache(query, cache, "pk", method=method, sql_dialect="duckdb", **kwargs)
    return run(rewritten), stats, rewritten


def _lazy(run, cache, query, method, **kwargs):
    kwargs.setdefault("min_component_size", 1)
    rewritten, stats = apply_cache_lazy(query, cache, "pk", method=method, sql_dialect="duckdb", **kwargs)
    return run(rewritten), stats, rewritten


SET_OPERATIONS = {
    "union_first_branch_without_where": (
        "SELECT p1.id FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1 UNION SELECT p1.id FROM t AS p1 WHERE p1.y = 2"
    ),
    "union_all": "SELECT p1.id FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1 UNION ALL SELECT p1.id FROM t AS p1 WHERE p1.y = 2",
    # x is no function of the key: a restriction of the second branch removes the x the first branch has
    "intersect": "SELECT p1.x FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1 INTERSECT SELECT p1.x FROM t AS p1 WHERE p1.y = 2",
    "except": "SELECT p1.x FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1 EXCEPT SELECT p1.x FROM t AS p1 WHERE p1.y = 4",
    "parenthesised_branches": (
        "(SELECT p1.id FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1) UNION (SELECT p1.id FROM t AS p1 WHERE p1.y = 2)"
    ),
    "parenthesised_with_where": "(SELECT p1.id FROM t AS p1 WHERE p1.x = 3 AND p1.y = 1) UNION ALL (SELECT p1.id FROM t AS p1 WHERE p1.y = 2) ORDER BY 1",
    "parenthesised_first_branch": (
        "(SELECT p1.id FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1) UNION SELECT p1.id FROM t AS p1 WHERE p1.y = 2"
    ),
    "order_by_limit": (
        "SELECT p1.id FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1 UNION SELECT p1.id FROM t AS p1 WHERE p1.y = 2 "
        "ORDER BY 1 LIMIT 30"
    ),
    "three_branches": (
        "SELECT p1.id FROM t AS p1 WHERE p1.x = 3 UNION SELECT p1.id FROM t AS p1 WHERE p1.y = 2 UNION SELECT p1.id FROM t AS p1 WHERE p1.y = 4"
    ),
    "except_with_where_in_both": "SELECT p1.pk FROM t AS p1 WHERE p1.x = 3 EXCEPT SELECT p1.pk FROM t AS p1 WHERE p1.y = 2",
}
# Variant generation reads these from another branch than the first (the first WHERE in breadth-first order): they run
# without the cache
UNCACHED_SET_OPERATIONS = {"parenthesised_branches", "parenthesised_first_branch", "three_branches"}


@pytest.mark.parametrize("name", list(SET_OPERATIONS))
class TestSetOperations:
    @pytest.mark.parametrize("method", EAGER_METHODS)
    def test_eager(self, db, name, method):
        run, cache = db
        query = SET_OPERATIONS[name]
        rows, stats, rewritten = _eager(run, cache, query, method)
        assert rows == run(query), rewritten
        assert stats["enhanced"] == (name not in UNCACHED_SET_OPERATIONS)

    @pytest.mark.parametrize("method", LAZY_METHODS)
    def test_lazy(self, db, name, method):
        run, cache = db
        query = SET_OPERATIONS[name]
        rows, stats, rewritten = _lazy(run, cache, query, method)
        assert rows == run(query), rewritten
        assert stats["enhanced"] == (name not in UNCACHED_SET_OPERATIONS)


def test_the_restriction_goes_into_the_first_branch(db):
    run, cache = db
    query = SET_OPERATIONS["union_first_branch_without_where"]
    _, stats, rewritten = _eager(run, cache, query, "IN")
    assert stats["enhanced"] == 1
    first, _, second = rewritten.partition(" UNION ")
    assert "p1.pk IN (" in first and "p1.pk IN (" not in second


def test_an_empty_intersection_restricts_the_first_branch_only(db):
    run, cache = db
    # y = pk % 5: no key has a row with y = 1 and one with y = 2, the intersection is empty
    query = "SELECT p1.id FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk AND p1.y = 1 AND p2.y = 2 UNION SELECT p1.id FROM t AS p1 WHERE p1.y = 2"
    rows, stats, rewritten = _eager(run, cache, query, "IN")
    assert stats["enhanced"] == 1
    first, _, second = rewritten.partition(" UNION ")
    assert "FALSE" in first.upper() and "FALSE" not in second.upper()
    assert rows == run(query)


SUBQUERY_REUSING_P1 = (
    "SELECT p1.id FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk WHERE p1.x = 3 AND p2.y = 1 "
    "AND p1.y NOT IN (SELECT p1.y FROM d AS d1 JOIN t AS p1 ON d1.id = p1.id WHERE p1.pk = 4)"
)
UNION_REUSING_P1 = (
    "SELECT p1.pk, p1.id FROM t AS p2 JOIN t AS p1 ON p1.pk = p2.pk WHERE p1.x = 3 AND p2.y = 1 "
    "UNION ALL SELECT p1.pk, p1.id FROM d AS d1 JOIN t AS p1 ON d1.id = p1.id WHERE p1.y = 2"
)
EXISTS_REUSING_P1 = (
    "SELECT p1.id FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk WHERE p1.x = 3 AND p2.y = 1 "
    "AND NOT EXISTS (SELECT 1 FROM d AS d1 JOIN t AS p1 ON d1.id = p1.id WHERE p1.id = p2.id + 1 AND p1.pk = 4)"
)


# The keys of p1 in the rows of each query (of the first branch for the UNION): the key set a correct cache gives for p1
P1_KEYS = {
    "not_in": "SELECT DISTINCT p1.pk AS pk FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk WHERE p1.x = 3 AND p2.y = 1",
    "union": "SELECT DISTINCT p1.pk AS pk FROM t AS p2 JOIN t AS p1 ON p1.pk = p2.pk WHERE p1.x = 3 AND p2.y = 1",
    "not_exists": "SELECT DISTINCT p1.pk AS pk FROM t AS p1 JOIN t AS p2 ON p1.pk = p2.pk WHERE p1.x = 3 AND p2.y = 1",
}
REUSING_P1 = {"not_in": SUBQUERY_REUSING_P1, "union": UNION_REUSING_P1, "not_exists": EXISTS_REUSING_P1}


@pytest.mark.parametrize("name", list(REUSING_P1))
class TestJoinMethodsJoinOnlyTheRestrictedSelect:
    """The key table is joined to the restricted SELECT's tables only, not to a subquery's or another branch's p1."""

    @pytest.mark.parametrize("join_every_table", [False, True])
    def test_direct_eager(self, db, name, join_every_table):
        run, _ = db
        query = REUSING_P1[name]
        keys = {row[0] for row in run(P1_KEYS[name])}
        target = {"join_every_table": True} if join_every_table else {"p0_alias": "p1"}
        rewritten = extend_query_with_partition_keys(query, keys, "pk", method="TMP_TABLE_JOIN", sql_dialect="duckdb", **target)
        assert run(rewritten) == run(query), rewritten
        assert len(re.findall(r"AS tmp_\w+ ON", rewritten)) == (2 if join_every_table else 1)

    @pytest.mark.parametrize("join_every_table", [False, True])
    def test_direct_lazy(self, db, name, join_every_table):
        run, _ = db
        query = REUSING_P1[name]
        target = {"join_every_table": True} if join_every_table else {"p0_alias": "p1"}
        rewritten = extend_query_with_partition_keys_lazy(query, P1_KEYS[name], "pk", method="TMP_TABLE_JOIN", sql_dialect="duckdb", **target)
        assert run(rewritten) == run(query), rewritten
        assert len(re.findall(r"AS tmp_\w+ ON", rewritten)) == (2 if join_every_table else 1)

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    def test_through_the_lookup(self, db, name, lazy):
        """A subquery that reuses an outer alias gets no variants (the alias renaming would capture it): no cache.

        The other branch of a UNION is not decomposed, so the UNION is restricted, in its first branch only.
        """
        run, cache = db
        query = REUSING_P1[name]
        apply = _lazy if lazy else _eager
        for join_every_table in (False, True):
            rows, stats, rewritten = apply(run, cache, query, "TMP_TABLE_JOIN", join_every_table=join_every_table)
            assert rows == run(query), rewritten
            if name == "union":
                assert stats["enhanced"] == 1
                assert len(re.findall(r"AS tmp_\w+ ON", rewritten)) == (2 if join_every_table else 1)
            else:
                assert stats["enhanced"] == 0 and rewritten == query


P0_TABLE_QUERIES = {
    "key_join": "SELECT p1.id, p2.id FROM t AS p1, t AS p2 WHERE p1.pk = p2.pk AND p1.x = 3 AND p2.y = 1",
    "key_join_under_or": "SELECT p1.id, p2.id FROM t AS p1, t AS p2 WHERE (p1.pk = p2.pk OR p2.id = p1.id + 1) AND p1.x = 3 AND p2.y = 1",
    "subquery_table": "SELECT p1.id FROM t AS p1 WHERE p1.x = 3 AND p1.id IN (SELECT q.id FROM t AS q WHERE q.y = 1)",
    "table_not_joined_on_the_key": "SELECT p1.id, d1.z FROM t AS p1, d AS d1 WHERE d1.id = p1.id AND p1.x = 3",
}


class TestUseP0Table:
    @pytest.mark.parametrize("name", list(P0_TABLE_QUERIES))
    def test_rewrite_keeps_the_rows(self, db, name):
        run, _ = db
        query = P0_TABLE_QUERIES[name]
        rewritten = rewrite_query_with_p0_table(query, "pk", sql_dialect="duckdb")
        assert rewritten != query
        assert run(rewritten) == run(query), rewritten

    # Not key_join_under_or: without skip_partition_key_joins the variants take every table to share the key
    @pytest.mark.parametrize("name", ["key_join", "subquery_table", "table_not_joined_on_the_key"])
    @pytest.mark.parametrize("method", ["IN", "TMP_TABLE_IN"])
    def test_apply_cache(self, db, name, method):
        run, cache = db
        query = P0_TABLE_QUERIES[name]
        for apply, kind in ((_eager, method), (_lazy, "IN_SUBQUERY" if method == "IN" else method)):
            rows, stats, rewritten = apply(run, cache, query, kind, use_p0_table=True)
            assert stats["p0_rewritten"] == 1 and stats["enhanced"] == 1
            assert rows == run(query), rewritten

    def test_only_an_exact_key_equality_is_replaced(self):
        rewritten = rewrite_query_with_p0_table(P0_TABLE_QUERIES["key_join_under_or"], "pk")
        assert "(p1.pk = p2.pk OR p2.id = p1.id + 1)" in rewritten
        assert "p2.pk = p0.pk" not in rewritten and "p1.pk = p0.pk" in rewritten

    def test_tables_of_subqueries_are_not_tied(self):
        rewritten = rewrite_query_with_p0_table(P0_TABLE_QUERIES["subquery_table"], "pk")
        assert "q.pk = p0.pk" not in rewritten and rewritten.endswith("AND p1.pk = p0.pk")


# Each login with the failures up to 5 minutes after it; three pairs cross an hour boundary (bucket w)
EVENT_PAIRS = (
    "SELECT e1.id, e2.id FROM events AS e1, events AS e2 WHERE e1.kind = 'login' AND e2.kind = 'fail' "
    "AND e2.ts >= e1.ts AND e2.ts <= e1.ts + 300"
)


class TestSkipPartitionKeyJoins:
    """Without key joins every alias has its own key: only variants of the restricted alias's key may restrict it."""

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    @pytest.mark.parametrize("p0_alias", [None, "e1", "e2"])
    def test_plain_lookup_keeps_the_rows(self, db, lazy, p0_alias):
        run, cache = db
        apply = apply_cache_lazy if lazy else apply_cache
        method = "IN_SUBQUERY" if lazy else "IN"
        rewritten, stats = apply(
            EVENT_PAIRS, cache, "w", method=method, sql_dialect="duckdb", min_component_size=1, skip_partition_key_joins=True, p0_alias=p0_alias
        )
        assert stats["enhanced"] == 1
        assert run(rewritten) == run(EVENT_PAIRS), rewritten

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    def test_dilation_is_unchanged(self, db, lazy):
        run, cache = db
        apply = apply_cache_lazy if lazy else apply_cache
        rewritten, stats = apply(EVENT_PAIRS, cache, "w", sql_dialect="duckdb", min_component_size=1, dilation_buckets=1)
        if not lazy:  # the fake handler has no lazy dilated intersection: that lookup runs uncached
            assert stats["enhanced"] == 1
        assert run(rewritten) == run(EVENT_PAIRS), rewritten

    def test_get_partition_keys_returns_the_p0_alias_keys(self, db):
        run, cache = db
        keys, _, hits = get_partition_keys(EVENT_PAIRS, cache, "w", min_component_size=1, skip_partition_key_joins=True)
        assert hits > 0
        assert {row[0] for row in run(EVENT_PAIRS.replace("SELECT e1.id, e2.id", "SELECT DISTINCT e1.w"))} <= keys

    @pytest.mark.parametrize("lazy", [False, True], ids=["eager", "lazy"])
    def test_a_derived_table_runs_uncached(self, db, lazy):
        _, cache = db
        query = EVENT_PAIRS.replace("FROM events AS e1", "FROM (SELECT * FROM events) AS e1")
        apply = apply_cache_lazy if lazy else apply_cache
        rewritten, stats = apply(query, cache, "w", sql_dialect="duckdb", min_component_size=1, skip_partition_key_joins=True, p0_alias="e1")
        assert rewritten == query and stats["uncached_reason"] == "no_key_source"


UNQUALIFIED_KEY = {
    "select_and_where": ("pk", "SELECT pk, p1.id FROM t AS p1 WHERE p1.x = 3 AND pk > 4"),
    "using": ("pk", "SELECT p1.id, p2.id FROM t AS p1 JOIN t AS p2 USING (pk) WHERE p1.x = 3 AND p2.y = 1"),
    "natural": ("pk", "SELECT q.id FROM t AS p1 NATURAL JOIN (SELECT id, pk FROM t) AS q WHERE p1.x = 3"),
    "named_like_the_eager_column": ("partition_key", "SELECT partition_key, s1.id FROM s AS s1 WHERE s1.x = 3 AND partition_key > 4"),
}


@pytest.mark.parametrize("name", list(UNQUALIFIED_KEY))
class TestJoinMethodsKeepUnqualifiedKeyColumns:
    """The key table's column must not make an unqualified reference to the partition key column ambiguous."""

    def test_eager(self, db, name):
        run, cache = db
        key, query = UNQUALIFIED_KEY[name]
        rewritten, stats = apply_cache(query, cache, key, method="TMP_TABLE_JOIN", sql_dialect="duckdb", min_component_size=1)
        assert stats["enhanced"] == 1
        assert run(rewritten) == run(query), rewritten

    def test_lazy(self, db, name):
        run, cache = db
        key, query = UNQUALIFIED_KEY[name]
        rewritten, stats = apply_cache_lazy(query, cache, key, method="TMP_TABLE_JOIN", sql_dialect="duckdb", min_component_size=1)
        assert stats["enhanced"] == 1
        assert run(rewritten) == run(query), rewritten

    def test_folded(self, db, name):
        run, cache = db
        key, query = UNQUALIFIED_KEY[name]
        alias = "s1" if key == "partition_key" else "p1"
        query = f"{query} AND {alias}.{key} IN (SELECT d1.id % 20 FROM d AS d1 WHERE d1.z = 1)"
        for apply in (apply_cache, apply_cache_lazy):
            rewritten, stats = apply(query, cache, key, method="TMP_TABLE_JOIN", sql_dialect="duckdb", min_component_size=1, fold_key_subqueries=True)
            assert stats["enhanced"] == 1 and "d.z" not in rewritten.rpartition(";")[2] and "d1.z" not in rewritten.rpartition(";")[2]
            assert run(rewritten) == run(query), rewritten


@pytest.mark.parametrize("method", ["TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN"])
@pytest.mark.parametrize("keys", [set(range(1, 30)), {"a", "b"}], ids=["integer", "text"])
@pytest.mark.parametrize("folded", [False, True])
def test_renamed_key_table_scripts_split_for_lazy_insertion(method, keys, folded):
    """Lazy insertion accepts the PostgreSQL scripts whose key table column is renamed (the unchanged ones are pinned elsewhere)."""
    from partitioncache.key_table_script import lazy_insertion_parts

    # An unqualified key column named like the eager and the lazy key table's column
    query = "SELECT DISTINCT partition_key FROM sites AS s WHERE s.zone = 'north'"
    if folded:
        query += " AND s.partition_key IN (SELECT m.site_id FROM site_meta AS m WHERE m.level >= 2)"
    datatype = "integer" if isinstance(next(iter(keys)), int) else "text"
    fold = "any_key_type" if folded else False
    scripts = [
        extend_query_with_partition_keys(query, keys, "partition_key", method=method, p0_alias="s", fold_key_subqueries=fold),
        extend_query_with_partition_keys_lazy(
            query, "SELECT g AS partition_key FROM generate_series(1, 50) AS g", "partition_key", method=method, p0_alias="s",
            fold_key_subqueries=fold, partition_datatype=datatype,
        ),
    ]
    for script in scripts:
        assert "tmp_s.pcache_key = s.partition_key" in script
        setup, select, _ = lazy_insertion_parts(script)
        assert setup.strip() and select.lstrip().startswith("SELECT DISTINCT partition_key")
