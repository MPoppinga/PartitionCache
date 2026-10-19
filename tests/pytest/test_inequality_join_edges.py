"""Inequalities between aliases (``p1.id <> p2.id``) must not connect the join graph.

Self-join workloads such as GeoMine add ``<>`` for every pair of aliases so that no two
aliases bind the same row. If those conditions counted as edges, the join graph became
complete and fragment generation emitted subsets connected only by ``<>``, e.g.

    SELECT DISTINCT t1.geom FROM pois AS t1, pois AS t2 WHERE t1.id <> t2.id AND t1.type = 'a' AND t2.type = 'd'

which is a cross product that restricts nothing beyond the single-alias fragments.
"""

import pytest

from partitioncache.query_processor import _is_inequality_condition, generate_partial_queries

# Triangle p1-p2-p3 plus p3-p4, connected by ST_DWithin.
BASE = (
    "SELECT p1.name FROM pois p1, pois p2, pois p3, pois p4 "
    "WHERE p1.type = 'a' AND p2.type = 'b' AND p3.type = 'c' AND p4.type = 'd' "
    "AND ST_DWithin(p1.geom, p2.geom, 100) AND ST_DWithin(p2.geom, p3.geom, 100) "
    "AND ST_DWithin(p1.geom, p3.geom, 100) AND ST_DWithin(p3.geom, p4.geom, 100)"
)


def _pairwise(op: str) -> str:
    return " AND " + " AND ".join(f"p{i}.id {op} p{j}.id" for i in range(1, 5) for j in range(i + 1, 5))


def _fragments(query: str) -> list[str]:
    return generate_partial_queries(
        query,
        partition_key="cell",
        min_component_size=1,
        follow_graph=True,
        auto_detect_partition_join=False,
        skip_partition_key_joins=True,
        geometry_column="geom",
        warn_no_partition_key=False,
    )


class TestInequalityJoinEdges:
    @pytest.mark.parametrize("op", ["<>", "!="])
    def test_pairwise_inequalities_add_no_fragments(self, op):
        assert len(_fragments(BASE + _pairwise(op))) == len(_fragments(BASE)) == 12

    def test_no_fragment_is_connected_only_by_inequalities(self):
        # p4 reaches p1 and p2 only through p3; without p3 they must not share a fragment.
        for fragment in _fragments(BASE + _pairwise("<>")):
            if "'d'" in fragment and ("'a'" in fragment or "'b'" in fragment):
                assert "'c'" in fragment, fragment

    def test_inequalities_stay_in_fragments_with_both_aliases(self):
        fragments = _fragments(BASE + _pairwise("<>"))
        full = [f for f in fragments if all(f"'{t}'" in f for t in "abcd")]
        assert len(full) == 1
        assert full[0].count("<>") == 6

    def test_pair_with_inequality_and_real_join_stays_connected(self):
        query = "SELECT p1.name FROM pois p1, pois p2 WHERE p1.type = 'a' AND p2.type = 'b' AND p1.id <> p2.id AND p1.price < p2.price"
        both = [f for f in _fragments(query) if "'a'" in f and "'b'" in f]
        assert len(both) == 1
        assert "<>" in both[0] and "t1.price < t2.price" in both[0]

    def test_pair_with_only_inequality_is_not_joined(self):
        query = "SELECT p1.name FROM pois p1, pois p2 WHERE p1.type = 'a' AND p2.type = 'b' AND p1.id <> p2.id"
        assert not [f for f in _fragments(query) if "'a'" in f and "'b'" in f]


class TestIsInequalityCondition:
    @pytest.mark.parametrize("condition", ["p1.id <> p2.id", "p1.id != p2.id", "(p1.id <> p2.id)"])
    def test_inequalities(self, condition):
        assert _is_inequality_condition(condition)

    @pytest.mark.parametrize(
        "condition",
        ["p1.id = p2.id", "p1.price < p2.price", "ST_DWithin(p1.geom, p2.geom, 100)", "NOT p1.id = p2.id OR p1.x <> p2.x", "(("],
    )
    def test_other_conditions(self, condition):
        assert not _is_inequality_condition(condition)
