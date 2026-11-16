"""
Every variant of a query must be implied by it: the keys of the query's rows are in every variant's key set.

The tests cover query shapes whose variants used to be stricter than the query or to read the key from the wrong
table. The end-to-end tests run the query and each variant in SQLite.
"""

import logging
import sqlite3

import pytest

from partitioncache.query_processor import generate_all_query_hash_pairs

_TABLES = {
    "a": ("pk INTEGER, v INTEGER, id INTEGER, z INTEGER", [(1, 1, 10, 0), (2, 1, 20, 0), (3, 2, 30, 0), (5, 1, 50, 0)]),
    "b": (
        "pk INTEGER, w INTEGER, id INTEGER, z INTEGER, pk_old INTEGER, flag INTEGER, score INTEGER, k INTEGER",
        [(4, 2, 10, 1, 1, 0, 1, 1), (9, 2, 20, 10, 5, 1, 5, 2), (2, 2, 30, 0, 2, 0, 3, 1), (7, 3, 40, 1, 3, 0, 2, 2)],
    ),
    "nowhere": ("pk INTEGER, w INTEGER", [(1, 2), (2, 3)]),
    "data_from_2020": ("pk INTEGER, v INTEGER", [(1, 1), (2, 2)]),
}


def _connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    for table, (columns, rows) in _TABLES.items():
        connection.execute(f"CREATE TABLE {table} ({columns})")
        connection.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)
    return connection


def _keys(connection: sqlite3.Connection, sql: str) -> set[int]:
    return {row[0] for row in connection.execute(sql)}


def _variants(query: str, **settings) -> list[str]:
    settings = {"min_component_size": 1, "warn_no_partition_key": False, **settings}
    return sorted(variant for variant, _ in generate_all_query_hash_pairs(query, "pk", **settings))


def _assert_sound(query: str, **settings) -> list[str]:
    """Run the query and every variant; every key of the query must be in every variant's keys."""
    connection = _connection()
    query_keys = _keys(connection, query)
    assert query_keys, f"the data must give the query rows: {query}"
    variants = _variants(query, **settings)
    for variant in variants:
        missing = query_keys - _keys(connection, variant)
        assert not missing, f"{variant} misses keys {sorted(missing)} of {query}"
    return variants


class TestPartitionKeyJoinDetection:
    """Only ``<alias>.<pk> = <alias>.<pk>`` is a key equijoin; the prefix match also took ``x.pk = y.pk + 1``."""

    def test_exact_key_equijoin_is_unchanged(self):
        query = "SELECT x.pk FROM a AS x, b AS y WHERE x.v = 1 AND y.w = 2 AND x.pk = y.pk"
        assert _variants(query) == ["SELECT DISTINCT t1.pk FROM a AS t1 WHERE t1.v = 1", "SELECT DISTINCT t1.pk FROM b AS t1 WHERE t1.w = 2"]

    def test_key_compared_with_an_expression_of_another_key(self, caplog):
        """The keys of x and y differ; the variants of b alone hold y's keys, not x's."""
        query = "SELECT x.pk FROM a AS x, b AS y WHERE x.v = 1 AND y.w = 2 AND x.pk = y.pk + 1"
        with caplog.at_level(logging.WARNING):
            assert _variants(query) == []
        assert "not by key equality" in caplog.text

    def test_key_or_condition_is_no_key_join(self):
        query = "SELECT x.pk FROM a AS x, b AS y WHERE x.v = 1 AND y.w = 2 AND (x.pk = y.pk OR y.flag = 1)"
        assert _variants(query) == []

    def test_key_joined_to_another_column_attaches_the_table(self):
        """``x.pk = y.pk_old`` joins b as a dimension: every variant keeps a and selects its key."""
        query = "SELECT x.pk FROM a AS x, b AS y WHERE x.v = 1 AND y.w = 2 AND x.pk = y.pk_old"
        variants = _variants(query)
        assert variants == [
            "SELECT DISTINCT t1.pk FROM a AS t1 WHERE t1.v = 1",
            "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.pk = t2.pk_old AND t1.v = 1 AND t2.w = 2",
        ]

    def test_condition_between_key_joined_aliases_is_kept(self):
        """When the aliases are key-joined, a further condition on both keys is an ordinary condition."""
        query = "SELECT x.pk FROM a AS x, b AS y WHERE x.v = 1 AND y.w = 2 AND x.pk = y.pk AND x.pk < y.pk + 1"
        variants = _variants(query)
        assert "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.pk < t2.pk + 1 AND t1.pk = t2.pk AND t1.v = 1 AND t2.w = 2" in variants

    def test_key_expression_without_key_joins_is_an_ordinary_condition(self):
        """With skip_partition_key_joins the aliases have their own keys; the condition stays in the variants of both."""
        query = "SELECT x.pk FROM a AS x, b AS y WHERE x.v = 1 AND y.w = 2 AND x.pk = y.pk + 1"
        variants = _variants(query, skip_partition_key_joins=True)
        assert "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.pk = t2.pk + 1 AND t1.v = 1 AND t2.w = 2" in variants
        assert all("FROM b AS t1 WHERE" not in variant for variant in variants)

    def test_partition_key_with_a_regex_metacharacter(self):
        query = "SELECT x.pk$1 FROM a AS x, b AS y WHERE x.v = 1 AND y.w = 2 AND x.pk$1 = y.pk$1"
        variants = sorted(variant for variant, _ in generate_all_query_hash_pairs(query, "pk$1", warn_no_partition_key=False))
        assert variants == ["SELECT DISTINCT t1.pk$1 FROM a AS t1 WHERE t1.v = 1", "SELECT DISTINCT t1.pk$1 FROM b AS t1 WHERE t1.w = 2"]

    @pytest.mark.parametrize(
        "condition",
        ["x.pk = y.pk + 1", "x.pk = y.pk_old", "(x.pk = y.pk OR y.flag = 1)"],
    )
    def test_variants_hold_the_keys_of_the_query(self, condition):
        _assert_sound(f"SELECT x.pk FROM a AS x, b AS y WHERE x.v = 1 AND y.w = 2 AND {condition}")


class TestSubqueryClauses:
    """ORDER BY, LIMIT, OFFSET, GROUP BY and HAVING of a subquery go only where that cannot remove rows of the query."""

    @pytest.mark.parametrize(
        "condition",
        [
            "x.id NOT IN (SELECT id FROM b ORDER BY score DESC LIMIT 1)",
            "NOT EXISTS (SELECT b.k FROM b WHERE b.id = x.id GROUP BY b.k HAVING COUNT(*) > 1)",
            "x.v IN (SELECT MAX(b.w) - 1 FROM b GROUP BY b.k)",
            "x.id IN (SELECT id FROM b ORDER BY score DESC LIMIT 2)",
        ],
    )
    def test_variants_hold_the_keys_of_the_query(self, condition):
        variants = _assert_sound(f"SELECT x.pk FROM a AS x WHERE x.v = 1 AND {condition}")
        assert variants

    def test_negated_subquery_keeps_its_limit(self):
        query = "SELECT x.pk FROM a AS x WHERE x.v = 1 AND x.id NOT IN (SELECT id FROM b ORDER BY score DESC LIMIT 1)"
        assert _variants(query) == ["SELECT DISTINCT t1.pk FROM a AS t1 WHERE NOT t1.id IN (SELECT id FROM b ORDER BY score DESC LIMIT 1) AND t1.v = 1"]

    def test_positive_subquery_loses_its_limit(self):
        query = "SELECT x.pk FROM a AS x WHERE x.v = 1 AND x.id IN (SELECT id FROM b ORDER BY score DESC LIMIT 1)"
        assert _variants(query) == ["SELECT DISTINCT t1.pk FROM a AS t1 WHERE t1.id IN (SELECT id FROM b) AND t1.v = 1"]

    def test_offset_goes_with_its_order_by(self):
        query = "SELECT x.pk FROM a AS x WHERE x.v = 1 AND x.id IN (SELECT id FROM b ORDER BY s OFFSET 10)"
        assert _variants(query) == ["SELECT DISTINCT t1.pk FROM a AS t1 WHERE t1.id IN (SELECT id FROM b) AND t1.v = 1"]
