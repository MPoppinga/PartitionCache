"""
Tests for structural relaxation detection and sequential, locally restricted cache population.
"""

import sqlite3

import pytest

from partitioncache.db_handler.sqlite import SQLiteDBHandler
from partitioncache.population import populate_query_variants
from partitioncache.query_processor import generate_all_hashes, generate_all_query_hash_pairs, is_relaxation_of, variant_signature


class TestVariantSignature:
    def test_relaxation_independent_of_alias_numbering(self):
        small = "SELECT DISTINCT t1.pk FROM f AS t1, d AS t2 WHERE t1.k = t2.k AND t2.x = 1"
        large = "SELECT DISTINCT t2.pk FROM d AS t1, f AS t2, e AS t3 WHERE t2.k = t1.k AND t1.x = 1 AND t2.j = t3.j"
        assert is_relaxation_of(variant_signature(small, "pk"), variant_signature(large, "pk"))
        assert not is_relaxation_of(variant_signature(large, "pk"), variant_signature(small, "pk"))

    def test_same_text_on_different_tables_is_not_a_relaxation(self):
        """t1.x = 1 filters d in one variant and f in the other."""
        on_d = "SELECT DISTINCT t2.pk FROM d AS t1, f AS t2 WHERE t1.k = t2.k AND t1.x = 1"
        on_f = "SELECT DISTINCT t1.pk FROM f AS t1, d AS t2 WHERE t1.k = t2.k AND t1.x = 1"
        assert not is_relaxation_of(variant_signature(on_d, "pk"), variant_signature(on_f, "pk"))

    def test_different_key_table_is_not_a_relaxation(self):
        a = "SELECT DISTINCT t1.pk FROM f AS t1"
        b = "SELECT DISTINCT t2.pk FROM f AS t1, g AS t2 WHERE t1.k = t2.k"
        assert not is_relaxation_of(variant_signature(a, "pk"), variant_signature(b, "pk"))

    def test_correlated_subquery_mapped(self):
        a = "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.k IN (SELECT s.k FROM s WHERE s.v = t1.v)"
        b = "SELECT DISTINCT t2.pk FROM d AS t1, f AS t2 WHERE t1.k = t2.k AND t2.k IN (SELECT s.k FROM s WHERE s.v = t2.v)"
        assert is_relaxation_of(variant_signature(a, "pk"), variant_signature(b, "pk"))

    def test_quoted_table_names_stay_distinct(self):
        a = 'SELECT DISTINCT a.pk FROM "LO" AS a WHERE a.y = 2'
        b = "SELECT DISTINCT b.pk FROM lo AS b WHERE b.x = 1 AND b.y = 2"
        assert not is_relaxation_of(variant_signature(a, "pk"), variant_signature(b, "pk"))

    def test_unquoted_names_are_case_folded(self):
        a = "SELECT DISTINCT a.PK FROM LO AS a WHERE a.y = 2"
        b = "SELECT DISTINCT b.pk FROM lo AS b WHERE b.x = 1 AND b.Y = 2"
        assert is_relaxation_of(variant_signature(a, "pk"), variant_signature(b, "pk"))

    def test_sqlite_folds_quoted_names_too(self):
        a = 'SELECT DISTINCT a.pk FROM "LO" AS a WHERE a.y = 2'
        b = "SELECT DISTINCT b.pk FROM lo AS b WHERE b.x = 1 AND b.y = 2"
        assert is_relaxation_of(variant_signature(a, "pk", "sqlite"), variant_signature(b, "pk", "sqlite"))

    def test_schemas_distinguish_tables(self):
        """s1.lo and s2.lo are different tables, also when their conditions look alike."""
        a = "SELECT DISTINCT a.pk FROM s1.lo AS a WHERE a.y = 2"
        b = "SELECT DISTINCT b.pk FROM s2.lo AS b WHERE b.x = 1 AND b.y = 2"
        assert not is_relaxation_of(variant_signature(a, "pk"), variant_signature(b, "pk"))
        assert is_relaxation_of(variant_signature(a, "pk"), variant_signature(a.replace("a.y = 2", "a.y = 2 AND a.x = 1"), "pk"))

    def test_generated_variants_keep_schemas_apart(self):
        query = "SELECT a.pk FROM s1.lo a, s2.lo b WHERE a.pk = b.pk AND a.y = 2 AND b.y = 2 AND b.x = 1"
        pairs = generate_all_query_hash_pairs(query, "pk", min_component_size=1, warn_no_partition_key=False)
        signatures = [variant_signature(v, "pk") for v, _ in pairs]
        for sig in signatures:
            assert sig is not None and all(name.startswith(('"s1".', '"s2".')) for name in sig[0])
        for a in signatures:
            for b in signatures:
                if a is not b and is_relaxation_of(a, b):
                    assert a[0] <= b[0]

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT DISTINCT t1.pk FROM f AS t1, (SELECT * FROM d WHERE FALSE) AS s",
            "SELECT DISTINCT t1.pk FROM f AS t1, UNNEST(ARRAY[1]) AS u",
            "SELECT DISTINCT t1.pk FROM f AS t1, LATERAL (SELECT 1) AS s",
            "SELECT DISTINCT t1.pk FROM f AS t1 NATURAL JOIN d AS t2",
            "SELECT DISTINCT t1.pk FROM f AS t1 TABLESAMPLE SYSTEM (10)",
            "SELECT DISTINCT ON (t1.pk) t1.pk FROM f AS t1",
            "SELECT MAX(t1.pk) FROM f AS t1",
            "SELECT DISTINCT t1.pk + 1 FROM f AS t1",
            "SELECT DISTINCT t1.pk, t1.k FROM f AS t1",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE random() < 0.5",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.ts < now()",
            "WITH a AS (SELECT * FROM f WHERE 1 = 0) SELECT a.pk FROM a",
            "SELECT DISTINCT t1.pk FROM f AS t1 CONNECT BY PRIOR t1.pk = t1.k",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.ts < LOCALTIMESTAMP",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.ts < 'now'::timestamp",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE random_normal() > 0",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.k IN (SELECT s.k FROM s TABLESAMPLE BERNOULLI (10))",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.k IN (SELECT s.k FROM s LIMIT 5)",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.k IN (SELECT s.k FROM s ORDER BY s.k LIMIT 5)",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.k IN (SELECT s.k FROM s ORDER BY s.k OFFSET 5)",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.k IN (SELECT DISTINCT ON (s.g) s.k FROM s ORDER BY s.g)",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.ts < 'now'",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.xid = txid_current()",
            "SELECT DISTINCT t1.pk FROM f AS t1 JOIN d AS t2 ON t1.k = t2.k",
            "SELECT DISTINCT t1.pk FROM f AS t1 LEFT JOIN d AS t2 ON t1.k = t2.k",
            "SELECT DISTINCT t1.pk FROM f AS t1, f AS t2 WHERE t1.k = t2.k",
            "SELECT DISTINCT t1.pk FROM f AS t1, d AS t2 WHERE x = 1",
            "SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.k IN (SELECT t1.k FROM s AS t1)",
            "SELECT DISTINCT t1.pk FROM f AS t1 GROUP BY t1.pk",
            "SELECT DISTINCT t1.pk FROM (SELECT * FROM f) AS t1",
            "SELECT t1.pk FROM f AS t1 UNION SELECT t2.pk FROM g AS t2",
        ],
    )
    def test_ambiguous_variants_have_no_signature(self, query):
        assert variant_signature(query, "pk") is None

    def test_finds_every_relaxation_the_cache_lookup_finds(self):
        query = (
            "SELECT lo.lo_orderkey FROM lineorder lo, customer c, supplier s, date d "
            "WHERE lo.lo_custkey = c.c_custkey AND lo.lo_suppkey = s.s_suppkey AND lo.lo_orderdate = d.d_datekey "
            "AND (c.c_region = 'ASIA' OR c.c_region = 'EUROPE') AND s.s_region = 'ASIA' AND d.d_year = 1997"
        )
        pairs = generate_all_query_hash_pairs(query, "lo_custkey", min_component_size=1, warn_no_partition_key=False)
        hashes = {h for _, h in pairs}
        signatures = {h: variant_signature(v, "lo_custkey") for v, h in pairs}
        assert all(s is not None for s in signatures.values())
        for variant, h in pairs:
            lookup = (set(generate_all_hashes(variant, "lo_custkey", min_component_size=1)) & hashes) - {h}
            structural = {o for o in hashes - {h} if is_relaxation_of(signatures[o], signatures[h])}
            assert lookup <= structural, variant


class FakeCache:
    """The three handler methods populate_query_variants uses, backed by a dict."""

    def __init__(self, preset: dict | None = None):
        self.entries: dict[str, set] = dict(preset or {})
        self.queries: dict[str, str] = {}

    def exists(self, key, partition_key="partition_key"):
        return key in self.entries

    def get(self, key, partition_key="partition_key"):
        return self.entries.get(key)

    def set_query(self, key, query_text, partition_key="partition_key"):
        self.queries[key] = query_text
        return True

    def set_entry(self, key, partition_key_identifiers, query_text, partition_key="partition_key", force_update=False):
        self.entries[key] = set(partition_key_identifiers)
        self.queries[key] = query_text
        return True


class CountingDB(SQLiteDBHandler):
    def __init__(self, path):
        super().__init__(path)
        self.executed: list[str] = []

    def execute(self, query):
        self.executed.append(query)
        return super().execute(query)


@pytest.fixture
def star_db(tmp_path):
    path = str(tmp_path / "star.db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE f (pk INTEGER, k INTEGER, j INTEGER);
        CREATE TABLE d (k INTEGER, x INTEGER);
        CREATE TABLE e (j INTEGER, y INTEGER);
        """
    )
    conn.executemany("INSERT INTO f VALUES (?, ?, ?)", [(pk, pk % 7, pk % 5) for pk in range(1, 201)])
    conn.executemany("INSERT INTO d VALUES (?, ?)", [(k, k % 3) for k in range(7)])
    conn.executemany("INSERT INTO e VALUES (?, ?)", [(j, j % 2) for j in range(5)])
    conn.commit()
    conn.close()
    return path


STAR_QUERY = "SELECT f.pk FROM f, d, e WHERE f.k = d.k AND f.j = e.j AND d.x = 1 AND e.y = 0"


def _pairs(query=STAR_QUERY):
    return generate_all_query_hash_pairs(query, "pk", min_component_size=1, partition_key_source_table="f", warn_no_partition_key=False)


class TestPopulateQueryVariants:
    def test_restricted_population_stores_the_same_keys(self, star_db):
        plain, restricted = FakeCache(), FakeCache()
        plain_stats = populate_query_variants(_pairs(), "pk", plain, CountingDB(star_db), restrict=False)
        stats = populate_query_variants(_pairs(), "pk", restricted, CountingDB(star_db))

        assert plain_stats.failed == 0
        assert len(plain.entries) == len(_pairs())
        assert restricted.entries == plain.entries
        assert stats.restricted > 0
        assert stats.failed == 0

    def test_key_limit_skips_large_intersections(self, star_db):
        db = CountingDB(star_db)
        stats = populate_query_variants(_pairs(), "pk", FakeCache(), db, max_restriction_keys=0)
        assert stats.restricted == 0
        assert not any(" IN (" in q for q in db.executed)

    def test_empty_intersection_is_stored_without_execution(self, star_db):
        query = "SELECT f.pk FROM f, d, e WHERE f.k = d.k AND f.j = e.j AND d.x = 7 AND e.y = 0"
        cache, db = FakeCache(), CountingDB(star_db)
        stats = populate_query_variants(_pairs(query), "pk", cache, db)

        assert stats.empty_without_execution > 0
        assert stats.executed + stats.empty_without_execution == len(_pairs(query))
        assert len(db.executed) == stats.executed
        reference = FakeCache()
        populate_query_variants(_pairs(query), "pk", reference, CountingDB(star_db), restrict=False)
        assert cache.entries == reference.entries

    def test_cached_variants_are_not_executed_but_restrict(self, star_db):
        pairs = _pairs()
        full = FakeCache()
        populate_query_variants(pairs, "pk", full, CountingDB(star_db), restrict=False)
        smallest = min(pairs, key=lambda p: len(p[0]))
        cache, db = FakeCache({smallest[1]: full.entries[smallest[1]]}), CountingDB(star_db)

        stats = populate_query_variants(pairs, "pk", cache, db)

        assert stats.already_cached == 1
        assert smallest[0] not in db.executed
        assert cache.entries == full.entries

    def test_failed_variant_does_not_stop_population(self, star_db):
        # The real SQLiteDBHandler error path: it raises, so the failure is not mistaken for "no rows"
        pairs = _pairs() + [("SELECT DISTINCT t1.pk FROM missing_table AS t1", "broken")]
        cache = FakeCache()
        stats = populate_query_variants(pairs, "pk", cache, CountingDB(star_db))
        assert stats.failed == 1
        assert "broken" not in cache.entries
        assert len(cache.entries) == len(pairs) - 1

    def test_failed_relaxation_does_not_empty_the_variants_it_would_restrict(self, star_db):
        """A relaxation whose query fails is neither cached nor used; the larger variants keep their true results."""
        reference = FakeCache()
        populate_query_variants(_pairs(), "pk", reference, CountingDB(star_db), restrict=False)
        pairs = _pairs()
        smallest = min(pairs, key=lambda p: len(p[0]))
        broken = [("SELECT DISTINCT t1.pk FROM missing_table AS t1", h) if h == smallest[1] else (v, h) for v, h in pairs]

        cache = FakeCache()
        stats = populate_query_variants(broken, "pk", cache, CountingDB(star_db))

        assert stats.failed == 1
        assert smallest[1] not in cache.entries
        assert cache.entries == {h: keys for h, keys in reference.entries.items() if h != smallest[1]}

    def test_store_failures_are_counted(self, star_db):
        class FailingCache(FakeCache):
            def set_entry(self, *args, **kwargs):
                return False

        stats = populate_query_variants(_pairs(), "pk", FailingCache(), CountingDB(star_db))
        assert stats.store_failed == stats.executed + stats.empty_without_execution > 0


def test_order_is_reproducible_for_variants_of_the_same_size():
    from partitioncache.query_processor import order_variants_for_population

    pairs = [("SELECT DISTINCT t1.pk FROM a AS t1 WHERE t1.x = 1", "h2"), ("SELECT DISTINCT t1.pk FROM b AS t1 WHERE t1.x = 1", "h1")]
    assert order_variants_for_population(pairs) == order_variants_for_population(list(reversed(pairs)))


def test_restriction_renders_keys_for_the_given_dialect():
    """A key ending in a backslash must be escaped the MySQL way when the database is MySQL."""
    small = ("SELECT DISTINCT t1.pk FROM f AS t1 WHERE t1.x = 1", "small")
    large = ("SELECT DISTINCT t1.pk FROM f AS t1, d AS t2 WHERE t1.k = t2.k AND t1.x = 1", "large")
    cache = FakeCache({"small": {"a\\", "b"}})

    class RecordingDB:
        def __init__(self):
            self.executed = []

        def execute(self, query):
            self.executed.append(query)
            return ["b"]

    db = RecordingDB()
    stats = populate_query_variants([small, large], "pk", cache, db, sql_dialect="mysql")

    assert stats.restricted == 1
    assert "'a\\\\'" in db.executed[0]
    assert cache.entries["large"] == {"b"}
