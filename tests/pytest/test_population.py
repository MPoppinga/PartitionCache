"""
Tests for cache population restricted through the cache lookup of a query.
"""

import sqlite3

import pytest

from partitioncache.db_handler.sqlite import SQLiteDBHandler
from partitioncache.population import populate_query_variants
from partitioncache.query_processor import generate_all_query_hash_pairs


class FakeCache:
    """The handler methods populate_query_variants and its lookup use, backed by a dict."""

    def __init__(self, preset: dict | None = None):
        self.entries: dict[str, set] = dict(preset or {})
        self.queries: dict[str, str] = {}

    def exists(self, key, partition_key="partition_key"):
        return key in self.entries

    def get_intersected(self, keys, partition_key="partition_key"):
        present = [self.entries[k] for k in keys if k in self.entries]
        if not present:
            return None, 0
        return set.intersection(*present), len(present)

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


# A star join as in SSB and TPC-H: the fact table f holds the key, the dimensions filter it
STAR_QUERY = "SELECT f.pk FROM f, d, e WHERE f.k = d.k AND f.j = e.j AND d.x = 1 AND e.y = 0"
STAR_SETTINGS = {"min_component_size": 1, "partition_key_source_table": "f"}


def _pairs(query=STAR_QUERY, settings=STAR_SETTINGS):
    return generate_all_query_hash_pairs(query, "pk", warn_no_partition_key=False, **settings)


def _populate(pairs, cache, db, settings=STAR_SETTINGS, **kwargs):
    return populate_query_variants(pairs, "pk", cache, db, variant_settings=settings, **kwargs)


class TestPopulateQueryVariants:
    def test_restricted_population_stores_the_same_keys(self, star_db):
        plain, restricted = FakeCache(), FakeCache()
        plain_stats = _populate(_pairs(), plain, CountingDB(star_db), restrict=False)
        stats = _populate(_pairs(), restricted, CountingDB(star_db))

        assert plain_stats.failed == 0
        assert len(plain.entries) == len(_pairs())
        assert restricted.entries == plain.entries
        assert stats.restricted > 0
        assert stats.failed == 0

    def test_key_limit_skips_large_intersections(self, star_db):
        db = CountingDB(star_db)
        stats = _populate(_pairs(), FakeCache(), db, max_restriction_keys=0)
        assert stats.restricted == 0
        assert stats.over_key_limit > 0
        assert not any(" IN (" in q for q in db.executed)

    def test_empty_intersection_is_stored_without_execution(self, star_db):
        query = "SELECT f.pk FROM f, d, e WHERE f.k = d.k AND f.j = e.j AND d.x = 7 AND e.y = 0"
        cache, db = FakeCache(), CountingDB(star_db)
        stats = _populate(_pairs(query), cache, db)

        assert stats.empty_without_execution > 0
        assert stats.executed + stats.empty_without_execution == len(_pairs(query))
        assert len(db.executed) == stats.executed
        reference = FakeCache()
        _populate(_pairs(query), reference, CountingDB(star_db), restrict=False)
        assert cache.entries == reference.entries

    def test_cached_variants_are_not_executed_but_restrict(self, star_db):
        pairs = _pairs()
        full = FakeCache()
        _populate(pairs, full, CountingDB(star_db), restrict=False)
        smallest = min(pairs, key=lambda p: len(p[0]))
        cache, db = FakeCache({smallest[1]: full.entries[smallest[1]]}), CountingDB(star_db)

        stats = _populate(pairs, cache, db)

        assert stats.already_cached == 1
        assert stats.restricted > 0
        assert smallest[0] not in db.executed
        assert cache.entries == full.entries

    def test_failed_variant_does_not_stop_population(self, star_db):
        # The real SQLiteDBHandler error path: it raises, so the failure is not mistaken for "no rows"
        pairs = _pairs() + [("SELECT DISTINCT t1.pk FROM missing_table AS t1", "broken")]
        cache = FakeCache()
        stats = _populate(pairs, cache, CountingDB(star_db))
        assert stats.failed == 1
        assert "broken" not in cache.entries
        assert len(cache.entries) == len(pairs) - 1

    def test_failed_relaxation_does_not_empty_the_variants_it_would_restrict(self, star_db):
        """A relaxation whose query fails is neither cached nor used; the larger variants keep their true results."""
        reference = FakeCache()
        _populate(_pairs(), reference, CountingDB(star_db), restrict=False)
        pairs = _pairs()
        smallest = min(pairs, key=lambda p: len(p[0]))
        broken = [("SELECT DISTINCT t1.pk FROM missing_table AS t1", h) if h == smallest[1] else (v, h) for v, h in pairs]

        cache = FakeCache()
        stats = _populate(broken, cache, CountingDB(star_db))

        assert stats.failed == 1
        assert smallest[1] not in cache.entries
        assert cache.entries == {h: keys for h, keys in reference.entries.items() if h != smallest[1]}

    def test_store_failures_are_counted(self, star_db):
        class FailingCache(FakeCache):
            def set_entry(self, *args, **kwargs):
                return False

        stats = _populate(_pairs(), FailingCache(), CountingDB(star_db))
        assert stats.store_failed == stats.executed + stats.empty_without_execution > 0

    def test_restriction_renders_keys_for_the_given_dialect(self):
        """A key ending in a backslash must be escaped the MySQL way when the database is MySQL."""
        pairs = _pairs("SELECT f.pk FROM f, d WHERE f.k = d.k AND f.x = 1")
        small, large = sorted(pairs, key=lambda p: len(p[0]))
        cache = FakeCache({small[1]: {"a\\", "b"}})

        class RecordingDB:
            def __init__(self):
                self.executed = []

            def execute(self, query):
                self.executed.append(query)
                return ["b"]

        db = RecordingDB()
        stats = _populate(pairs, cache, db, sql_dialect="mysql")

        assert stats.restricted == 1
        assert "'a\\\\'" in db.executed[0]
        assert cache.entries[large[1]] == {"b"}


@pytest.fixture
def points_db(tmp_path):
    """Sensor readings: every reading of a pattern is an alias of the same table, joined on the site."""
    path = str(tmp_path / "points.db")
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE readings (site INTEGER, kind TEXT, x REAL, y REAL)")
    rows = [(site, "ABC"[(site * 7 + i) % 3], (site * 13 + i * 5) % 11, (site * 3 + i * 7) % 9) for site in range(1, 61) for i in range(6)]
    conn.executemany("INSERT INTO readings VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def flights_db(tmp_path):
    """OpenSky-like: flight phases and segments, a pattern is a sequence of elements of the same flight."""
    path = str(tmp_path / "flights.db")
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE phases (flight_id INTEGER, phase TEXT, t0 INTEGER, t1 INTEGER, alt INTEGER);
        CREATE TABLE segments (flight_id INTEGER, scale INTEGER, t0 INTEGER, vrate INTEGER);
        """
    )
    phases = [(f, ["CL", "CR", "DE"][(f + i) % 3], 100 * i + f % 17, 100 * i + 40 + f % 23, (f * 37 + i * 11) % 900) for f in range(1, 81) for i in range(5)]
    segments = [(f, 60, 100 * i + (f * 7) % 90, (f + i * 3) % 9) for f in range(1, 81) for i in range(5)]
    conn.executemany("INSERT INTO phases VALUES (?, ?, ?, ?, ?)", phases)
    conn.executemany("INSERT INTO segments VALUES (?, ?, ?, ?)", segments)
    conn.commit()
    conn.close()
    return path


READINGS_LIKE = (
    "SELECT p1.site FROM readings p1, readings p2, readings p3, readings p4 "
    "WHERE p1.site = p2.site AND p1.site = p3.site AND p1.site = p4.site "
    "AND p1.kind = 'A' AND p2.kind = 'B' AND p3.kind = 'C' AND p4.kind = 'A' "
    "AND (p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y) <= 30.5 "
    "AND (p2.x - p3.x) * (p2.x - p3.x) + (p2.y - p3.y) * (p2.y - p3.y) <= 40.25 "
    "AND (p3.x - p4.x) * (p3.x - p4.x) + (p3.y - p4.y) * (p3.y - p4.y) <= 25.75 "
    "AND (p1.x - p4.x) * (p1.x - p4.x) + (p1.y - p4.y) * (p1.y - p4.y) >= 1.5"
)
READINGS_SETTINGS = {"min_component_size": 2, "max_component_size": 5, "follow_graph": True, "keep_all_attributes": True, "bucket_steps": {"x": 1.0}}
OPENSKY_LIKE = (
    "SELECT DISTINCT e1.flight_id FROM phases e1, phases e2, segments e3 "
    "WHERE e1.flight_id = e2.flight_id AND e1.flight_id = e3.flight_id "
    "AND e1.phase = 'DE' AND e1.alt <= 700 AND e2.phase = 'CL' AND e3.scale = 60 AND e3.vrate >= 4 "
    "AND e2.t0 >= e1.t1 + 0 AND e2.t0 <= e1.t1 + 160 AND e3.t0 >= e1.t1 + 0 AND e3.t0 <= e1.t1 + 260"
)
OPENSKY_SETTINGS = {"min_component_size": 1, "follow_graph": True, "keep_all_attributes": True, "bucket_steps": 0}


class TestSelfJoins:
    """Variants that use a table several times are restricted like any other (identified by hash, not by names)."""

    @pytest.mark.parametrize(
        ("db_fixture", "query", "key", "settings"),
        [("points_db", READINGS_LIKE, "site", READINGS_SETTINGS), ("flights_db", OPENSKY_LIKE, "flight_id", OPENSKY_SETTINGS)],
        ids=["readings_like", "opensky_like"],
    )
    def test_restricted_population_stores_the_same_keys(self, request, db_fixture, query, key, settings):
        path = request.getfixturevalue(db_fixture)
        pairs = generate_all_query_hash_pairs(query, key, **settings)
        plain, restricted = FakeCache(), FakeCache()
        populate_query_variants(pairs, key, plain, CountingDB(path), restrict=False)
        stats = populate_query_variants(pairs, key, restricted, CountingDB(path), variant_settings=settings)

        assert len(plain.entries) == len(pairs) and any(plain.entries.values())
        assert restricted.entries == plain.entries
        # Most variants with relaxations are restricted (the smallest have none); a signature by table names restricted none
        assert stats.restricted + stats.empty_without_execution > (len(pairs) - stats.no_relaxation) // 2
        assert stats.failed == stats.lookup_failed == 0

    def test_lookup_with_other_settings_finds_fewer_relaxations_but_stays_correct(self, points_db):
        pairs = generate_all_query_hash_pairs(READINGS_LIKE, "site", **READINGS_SETTINGS)
        plain, matching, defaults = FakeCache(), FakeCache(), FakeCache()
        populate_query_variants(pairs, "site", plain, CountingDB(points_db), restrict=False)
        good = populate_query_variants(pairs, "site", matching, CountingDB(points_db), variant_settings=READINGS_SETTINGS)
        other = populate_query_variants(pairs, "site", defaults, CountingDB(points_db), variant_settings={"min_component_size": 2})

        assert matching.entries == defaults.entries == plain.entries
        assert other.restricted + other.empty_without_execution < good.restricted + good.empty_without_execution


class TestWithoutPartitionKeyJoins:
    """A key whose tables are related by time, not joined on it, as with skip_partition_key_joins."""

    SETTINGS = {"min_component_size": 1, "keep_all_attributes": True, "skip_partition_key_joins": True}
    TIME_JOIN = "SELECT a.pk FROM a, b WHERE a.x = 1 AND b.y = 1 AND b.t >= a.t AND b.t <= a.t + 10"
    B_ONLY = "SELECT b.pk FROM b WHERE b.y = 1"

    @pytest.fixture
    def db(self, tmp_path):
        path = str(tmp_path / "buckets.db")
        conn = sqlite3.connect(path)
        conn.executescript(
            "CREATE TABLE a (pk INTEGER, t INTEGER, x INTEGER); CREATE TABLE b (pk INTEGER, t INTEGER, y INTEGER);"
            "INSERT INTO a VALUES (1, 0, 1), (2, 100, 1); INSERT INTO b VALUES (2, 5, 1), (3, 105, 1);"
        )
        conn.commit()
        conn.close()
        return path

    def test_entries_of_other_queries_do_not_drop_keys(self, db):
        """With the settings of the variants, the lookup never generates 'b alone', whose keys are b's."""
        cache, reference = FakeCache(), FakeCache()
        for query in (self.B_ONLY, self.TIME_JOIN):
            pairs = generate_all_query_hash_pairs(query, "pk", warn_no_partition_key=False, **self.SETTINGS)
            populate_query_variants(pairs, "pk", cache, CountingDB(db), variant_settings=self.SETTINGS)
            populate_query_variants(pairs, "pk", reference, CountingDB(db), restrict=False)
        assert cache.entries == reference.entries
        assert {1, 2} in cache.entries.values()

    @pytest.mark.parametrize("skip", [True, False])
    def test_join_methods_restrict_the_key_table(self, skip):
        import re

        from partitioncache.population import restrict_with_cache

        variant = "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.t <= t2.t AND t2.t <= t1.t + 10"
        cache = FakeCache()
        cache.get_intersected = lambda keys, partition_key="pk": ({1, 2}, 1)  # type: ignore[method-assign]
        restriction = restrict_with_cache(variant, cache, "pk", {"skip_partition_key_joins": skip}, method="TMP_TABLE_JOIN")
        assert restriction.restricted
        assert set(re.findall(r"AS tmp_(\w+) ON", restriction.query)) == {"t1"}

    def test_join_every_table_is_refused_for_tables_not_joined_on_the_key(self):
        from partitioncache.population import restrict_with_cache

        cache = FakeCache()
        cache.get_intersected = lambda keys, partition_key="pk": ({1, 2}, 1)  # type: ignore[method-assign]
        variant = "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.t <= t2.t AND t2.t <= t1.t + 10"
        with pytest.raises(ValueError, match="join_every_table"):
            restrict_with_cache(variant, cache, "pk", {"skip_partition_key_joins": True}, method="TMP_TABLE_JOIN", join_every_table=True)
        with pytest.raises(ValueError, match="every table joined on pk"):
            restrict_with_cache(variant, cache, "pk", {"skip_partition_key_joins": False}, method="TMP_TABLE_JOIN", join_every_table=True)
        joined = "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.pk = t2.pk AND t2.y = 1"
        restriction = restrict_with_cache(joined, cache, "pk", {}, method="TMP_TABLE_JOIN", join_every_table=True)
        assert "tmp_t1" in restriction.query and "tmp_t2" in restriction.query


class TestAddedConstraints:
    """add_constraints yields variants with and without the added condition; the lookup must not add it."""

    QUERY = "SELECT a.id FROM pts AS a, pts AS b WHERE a.pk = b.pk AND a.kind = 'a' AND b.kind = 'b' AND a.size >= 2 AND ABS(a.x - b.x) < 1.6"
    SETTINGS = {"min_component_size": 1, "add_constraints": {"pts": "size = 4"}}

    def test_restricted_population_stores_the_same_keys(self, tmp_path):
        path = str(tmp_path / "pts.db")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE pts (id INTEGER, pk INTEGER, kind TEXT, size INTEGER, x REAL)")
        conn.executemany("INSERT INTO pts VALUES (?, ?, ?, ?, ?)", [(i, i % 9, "ab"[i % 2], 1 + i % 5, (i * 7 % 13) / 2) for i in range(120)])
        conn.commit()
        conn.close()
        pairs = generate_all_query_hash_pairs(self.QUERY, "id", warn_no_partition_key=False, **self.SETTINGS)
        assert any("size = 4" in variant for variant, _ in pairs) and any("size = 4" not in variant for variant, _ in pairs)

        plain, restricted = FakeCache(), FakeCache()
        populate_query_variants(pairs, "id", plain, CountingDB(path), restrict=False)
        stats = populate_query_variants(pairs, "id", restricted, CountingDB(path), variant_settings=self.SETTINGS)

        assert restricted.entries == plain.entries
        assert stats.restricted + stats.empty_without_execution > 0


class TestLookupSettings:
    def test_lookup_settings_are_named_parameters_of_both_lookups(self):
        """A generator setting the lookups do not name would fail every lookup (they take **kwargs only for old names)."""
        import inspect

        from partitioncache.apply_cache import get_partition_keys, get_partition_keys_lazy
        from partitioncache.population import lookup_settings

        for lookup in (get_partition_keys, get_partition_keys_lazy):
            named = {name for name, p in inspect.signature(lookup).parameters.items() if p.kind is not p.VAR_KEYWORD}
            assert set(lookup_settings(None)) <= named, lookup.__name__

    def test_missing_settings_take_the_generator_defaults(self):
        from partitioncache.population import lookup_settings

        settings = lookup_settings(None)
        assert settings["min_component_size"] == 1 and settings["keep_all_attributes"] is True  # get_partition_keys: 2, False
        assert "max_component_size" not in settings and "warn_no_partition_key" not in settings

    def test_unknown_setting_raises(self):
        with pytest.raises(TypeError, match="min_component"):
            populate_query_variants([], "pk", FakeCache(), None, variant_settings={"min_component": 1})  # type: ignore[arg-type]

    def test_spatial_and_grid_variants_run_unrestricted(self, star_db, caplog):
        cache = FakeCache()
        with caplog.at_level("WARNING", logger="PartitionCache"):
            stats = populate_query_variants(_pairs(), "pk", cache, CountingDB(star_db), variant_settings={**STAR_SETTINGS, "grid_cell_size": 100.0})
        assert stats.restricted == 0 and stats.executed == len(_pairs())
        assert "populated unrestricted" in caplog.text

    def test_lazy_needs_a_lazy_handler(self):
        from unittest.mock import Mock

        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        with pytest.raises(ValueError, match="lazy intersection"):
            populate_query_variants([], "pk", FakeCache(), None, lazy=True, method="IN_SUBQUERY")  # type: ignore[arg-type]
        populate_query_variants([], "pk", Mock(spec=AbstractCacheHandler_Lazy), None, lazy=True, method="IN_SUBQUERY", max_restriction_keys=5)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="method"):
            populate_query_variants([], "pk", FakeCache(), None, method="IN_SUBQUERY")  # type: ignore[arg-type]

    def test_lazy_restriction_uses_the_cache_subquery(self):
        from unittest.mock import Mock

        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        pairs = _pairs("SELECT f.pk FROM f, d WHERE f.k = d.k AND f.x = 1")
        cache = Mock(spec=AbstractCacheHandler_Lazy)
        cache.exists.return_value = False
        cache.set_entry.return_value = True
        cache.get_intersected_lazy.return_value = ("SELECT pk FROM cached_keys", 1)
        db = Mock()
        db.execute.return_value = [1]

        stats = _populate(pairs, cache, db, lazy=True, method="IN_SUBQUERY")

        assert stats.restricted == len(pairs) == 2  # the mock reports a hit for every lookup
        assert all("IN (SELECT pk FROM cached_keys)" in call.args[0] for call in db.execute.call_args_list)


class TestNoSilentNoOp:
    def test_warning_when_variants_have_relaxations_but_none_is_restricted(self, star_db, caplog):
        class NothingCached(FakeCache):
            def get_intersected(self, keys, partition_key="partition_key"):
                return None, 0

        with caplog.at_level("WARNING", logger="PartitionCache"):
            stats = _populate(_pairs(), NothingCached(), CountingDB(star_db))
        assert stats.restricted == 0 and stats.no_cached_relaxation > 0
        assert "no variant was restricted" in caplog.text

    def test_no_warning_for_a_query_without_relaxations(self, star_db, caplog):
        pairs = _pairs("SELECT f.pk FROM f WHERE f.k = 1")
        with caplog.at_level("WARNING", logger="PartitionCache"):
            stats = _populate(pairs, FakeCache(), CountingDB(star_db))
        assert stats.no_relaxation == len(pairs) and "restricted" not in caplog.text

    def test_no_warning_when_restriction_is_off(self, star_db, caplog):
        with caplog.at_level("WARNING", logger="PartitionCache"):
            _populate(_pairs(), FakeCache(), CountingDB(star_db), restrict=False)
        assert "restricted" not in caplog.text


def test_order_is_reproducible_for_variants_of_the_same_size():
    from partitioncache.query_processor import order_variants_for_population

    pairs = [("SELECT DISTINCT t1.pk FROM a AS t1 WHERE t1.x = 1", "h2"), ("SELECT DISTINCT t1.pk FROM b AS t1 WHERE t1.x = 1", "h1")]
    assert order_variants_for_population(pairs) == order_variants_for_population(list(reversed(pairs)))


@pytest.mark.parametrize(
    "query, alias",
    [
        ("SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.x = t2.x", "t1"),
        ("SELECT DISTINCT p1.pk FROM a AS t1, list AS p1 WHERE t1.pk = p1.pk", "p1"),
        ("SELECT DISTINCT t2.pk AS pk FROM a AS t1, b AS t2", "t2"),
        ("SELECT DISTINCT pk FROM a", None),
        ("SELECT DISTINCT t1.other FROM a AS t1", None),
        ("SELECT t1.pk, t1.x FROM a AS t1", None),
        ("not sql at all (", None),
    ],
)
def test_variant_key_alias(query, alias):
    from partitioncache.query_processor import variant_key_alias

    assert variant_key_alias(query, "pk") == alias


class TestFloatKeys:
    """VALUES does not restrict float keys (as double precision it would miss a real column); IN and the key tables do."""

    VARIANT = "SELECT DISTINCT t1.pk FROM f AS t1, d AS t2 WHERE t1.k = t2.k AND t2.x = 1"

    @staticmethod
    def _cache(keys):
        cache = FakeCache()
        cache.get_intersected = lambda hashes, partition_key="pk": (set(keys), 1)  # type: ignore[method-assign]
        return cache

    def test_values_leaves_the_variant_unrestricted(self, caplog):
        from partitioncache.population import restrict_with_cache

        with caplog.at_level("WARNING", logger="PartitionCache"):
            restriction = restrict_with_cache(self.VARIANT, self._cache({0.1, 0.2}), "pk", {}, method="VALUES")
        assert (restriction.restricted, restriction.reason, restriction.query) == (False, "float_keys", self.VARIANT)
        assert "use IN" in caplog.text

    @pytest.mark.parametrize("method", ["TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_IN", "UNLOGGED_TABLE_JOIN"])
    def test_key_tables_restrict_them_in_the_column_type(self, method):
        from partitioncache.population import restrict_with_cache

        restriction = restrict_with_cache(self.VARIANT, self._cache({0.1, 0.2}), "pk", {}, method=method)
        assert restriction.restricted and "AS SELECT (array_agg(pcache_src.pk))[1] AS partition_key FROM f AS pcache_src WHERE false HAVING false" in restriction.query

    def test_in_restricts_them(self):
        from partitioncache.population import restrict_with_cache

        restriction = restrict_with_cache(self.VARIANT, self._cache({0.1, 0.2}), "pk", {}, method="IN")
        assert restriction.restricted
        assert "IN ('0.1', '0.2')" in restriction.query or "IN ('0.2', '0.1')" in restriction.query

    def test_population_counts_the_refusal(self, star_db):
        cache = self._cache({1.5})
        db = CountingDB(star_db)
        stats = _populate(_pairs(), cache, db, method="VALUES")
        assert stats.restricted == 0 and stats.float_keys > 0 and stats.failed == 0
        assert not any("VALUES" in q for q in db.executed)
