"""Regression: a dimension attached by a spatial join must never yield a fragment whose
only relation lacks the partition key.

Experiment 13 (2026-09-18/19): on the NYC taxi example, ``taxi_trips t`` joined to
``osm_pois p`` through ``ST_DWithin`` produced the fragment
``SELECT DISTINCT t1.trip_id FROM osm_pois AS t1 WHERE t1.poi_type = 'museum'`` -- osm_pois
has no trip_id, the fragment fails at population, and the shared start-point constraint
was never cached. The pk-bearing restriction existed but was gated on a pk equijoin.
"""
from __future__ import annotations

import re

from partitioncache.query_processor import generate_all_query_hash_pairs

TAXI = (
    "SELECT p.name, COUNT(*) FROM taxi_trips t, osm_pois p "
    "WHERE ST_DWithin(t.pickup_geom, p.geom, 200) AND p.poi_type = 'museum' "
    "AND t.duration_seconds > 2700 GROUP BY p.name"
)


def _fragments(query: str, **kw) -> list[str]:
    pairs = generate_all_query_hash_pairs(
        query=query, partition_key="trip_id", min_component_size=1, keep_all_attributes=False,
        strip_select=True, auto_detect_partition_join=False, skip_partition_key_joins=True,
        follow_graph=False, max_conditions_removed=2, **kw,
    )
    return [q for q, _ in pairs]


def test_no_fragment_without_the_fact_table_when_source_table_declared():
    frags = _fragments(TAXI, partition_key_source_table="taxi_trips")
    assert frags, "no fragments generated"
    for f in frags:
        from_clause = re.search(r"FROM\s+(.*?)(?:\s+WHERE|$)", f, re.I | re.S).group(1)
        assert "taxi_trips" in from_clause, f"fragment without the key-carrying relation: {f}"
        alias = re.search(r"SELECT DISTINCT\s+(\w+)\.trip_id", f).group(1)
        assert re.search(rf"taxi_trips AS {alias}\b", f), f"trip_id selected from a non-fact alias: {f}"


def test_dimension_fragment_keeps_the_join_predicate():
    frags = _fragments(TAXI, partition_key_source_table="taxi_trips")
    with_dim = [f for f in frags if "osm_pois" in f]
    assert with_dim, "the dimension's condition must survive in some fragment"
    for f in with_dim:
        assert "ST_DWITHIN" in f.upper(), f"dimension kept without its join predicate: {f}"


def test_undeclared_source_table_is_unchanged_behaviour():
    # Without a declared source table and without alias.trip_id in any condition, no alias is
    # known to bear the key, so the generator must behave exactly as before (this is the
    # byte-identity guarantee for every other workload).
    before = _fragments(TAXI)
    assert any("osm_pois" in f and "taxi_trips" not in f for f in before), (
        "sanity: the old behaviour reproduces the defect only when the source table is not declared"
    )


def test_all_aliases_bearing_the_key_is_unrestricted():
    # GeoB shape: the POI table joined to itself, every alias carries the key.
    q = ("SELECT p1.name, p2.name FROM pois p1, pois p2 WHERE p1.subtype = 'cafe' "
         "AND p2.subtype = 'bakery' AND ST_DWithin(p1.geom, p2.geom, 200)")
    pairs = generate_all_query_hash_pairs(
        query=q, partition_key="zipcode", min_component_size=1, keep_all_attributes=False,
        strip_select=True, auto_detect_partition_join=False, skip_partition_key_joins=True,
        follow_graph=False, max_conditions_removed=1, partition_key_source_table="pois",
    )
    pairs_undeclared = generate_all_query_hash_pairs(
        query=q, partition_key="zipcode", min_component_size=1, keep_all_attributes=False,
        strip_select=True, auto_detect_partition_join=False, skip_partition_key_joins=True,
        follow_graph=False, max_conditions_removed=1,
    )
    assert sorted(h for _, h in pairs) == sorted(h for _, h in pairs_undeclared)
