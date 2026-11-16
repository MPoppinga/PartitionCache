"""The buffer the cached geometries are widened by: none for a single table, the distance-graph diameter otherwise.

Only fragments whose aliases are all bounded to p0 by top-level distance conjuncts may restrict it.
"""

from __future__ import annotations

import math
from unittest.mock import MagicMock

import pytest

from partitioncache.apply_cache import _resolve_buffer_distance, apply_cache, apply_cache_lazy
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy
from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler
from partitioncache.query_processor import compute_buffer_distance, extract_distance_constraints, generate_all_query_hash_pairs

POINT_RADIUS = "SELECT p1.name FROM pois AS p1 WHERE p1.subtype = 'pharmacy' AND ST_DWithin(p1.geom, ST_SetSRID(ST_MakePoint(549100, 5802050), 25832), 50000)"
STAR = (
    "SELECT p1.name FROM pois AS p1, pois AS p2, pois AS p3 WHERE p1.subtype = 'a' AND p2.subtype = 'b' AND p3.subtype = 'c' "
    "AND ST_DWithin(p1.geom, p2.geom, 300) AND ST_DWithin(p1.geom, p3.geom, 400)"
)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (POINT_RADIUS, 0.0),  # the radius around the constant point is part of every cached fragment
        ("SELECT * FROM pois AS p1 WHERE p1.subtype = 'pharmacy'", 0.0),
        (STAR, 700.0),
        ("SELECT * FROM pois AS p1 JOIN pois AS p2 ON ST_DWithin(p1.geom, p2.geom, 250) WHERE p1.subtype = 'a'", 250.0),
    ],
)
def test_buffer_is_derived_from_the_query(query: str, expected: float) -> None:
    assert _resolve_buffer_distance(query, None) == expected


def test_explicit_buffer_wins() -> None:
    assert _resolve_buffer_distance(POINT_RADIUS, 120.0) == 120.0
    assert _resolve_buffer_distance(STAR, 0.0) == 0.0


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM pois AS p1, pois AS p2 WHERE p1.subtype = 'a' AND p2.subtype = 'b'",
        "SELECT * FROM (SELECT * FROM pois) AS s WHERE s.subtype = 'a'",  # a derived table is not a single table
        # the fragments come from the CTE body, which joins two tables
        "WITH c AS (SELECT p1.name FROM pois AS p1, pois AS p2 WHERE p1.subtype = p2.subtype) SELECT * FROM c",
        # a set operation: its second branch joins two tables
        "SELECT p1.geom FROM pois AS p1 UNION SELECT p2.geom FROM pois AS p2, pois AS p3 WHERE p2.subtype = p3.subtype",
    ],
)
def test_several_tables_without_distances_need_an_explicit_buffer(query: str) -> None:
    with pytest.raises(ValueError, match="buffer_distance is required"):
        _resolve_buffer_distance(query, None)


def _bbox_sql(keys: list[str], buffer_distance: float) -> str:
    handler = object.__new__(PostGISBBoxCacheHandler)
    handler.tableprefix = "pc"
    return handler._get_buffered_intersected_sql(keys, "spatial_bbox", buffer_distance).as_string(None)  # type: ignore[arg-type]


def test_bbox_zero_buffer_intersects_the_stored_points_unbuffered() -> None:
    """ST_Buffer of points by 0 is empty: buffering would drop every row of a single-table query."""
    assert _bbox_sql(["h1"], 0.0) == """(SELECT partition_keys FROM "pc_cache_spatial_bbox" WHERE query_hash = 'h1')"""
    multi = _bbox_sql(["h1", "h2"], 0.0)
    assert "ST_Buffer" not in multi and multi.startswith("ST_Intersection((SELECT partition_keys")


def test_bbox_positive_buffer_circumscribes_the_circle() -> None:
    buf = 200.0 / math.cos(math.pi / 16)
    assert _bbox_sql(["h1"], 200.0) == (f"""ST_Buffer((SELECT partition_keys FROM "pc_cache_spatial_bbox" WHERE query_hash = 'h1'), {buf}, 'quad_segs=8')""")
    assert _bbox_sql(["h1", "h2", "h3"], 200.0).count("ST_Buffer(") == 3


# A distance predicate bounds two aliases only as a top-level conjunct with a constant radius.

ATTRIBUTE_TIED = (
    "SELECT p1.id FROM pois p1, pois p2, pois p3, pois p4 WHERE ST_DWithin(p1.geom,p2.geom,100) AND p1.name = p3.name "
    "AND ST_DWithin(p3.geom,p4.geom,100) AND p2.type = 3 AND p4.type = 5"
)
NEGATED = "SELECT p1.id FROM pois p1, stations s WHERE NOT ST_DWithin(p1.geom, s.geom, 100) AND s.kind = 7 AND p1.kind = 3"


@pytest.mark.parametrize(
    ("condition", "expected"),
    [
        ("ST_DWithin(a.geom, b.geom, 2 * 250)", 500.0),
        ("ST_DWithin(a.geom, b.geom, (100 + 150) * 2)", 500.0),
        ("ST_DWithin(a.geom, b.geom, 1000 - 999 / 2)", 501.0),  # integer division, as in PostgreSQL
        ("ST_Distance(a.geom, b.geom) < 2 * 250", 500.0),
        ("ST_Distance(a.geom, b.geom) <= 500", 500.0),
        ("500 > ST_Distance(a.geom, b.geom)", 500.0),
        ("500 >= ST_Distance(a.geom, b.geom)", 500.0),
        ("ST_Distance(a.geom, b.geom) BETWEEN 10 AND 2 * 250", 500.0),
        ("a.kind = 1 AND (ST_DWithin(a.geom, b.geom, 500))", 500.0),
    ],
)
def test_radius_is_read_from_constant_arithmetic_in_both_directions(condition: str, expected: float) -> None:
    assert compute_buffer_distance(f"SELECT a.id FROM pois a, pois b WHERE {condition}") == expected


@pytest.mark.parametrize(
    "condition",
    [
        "NOT ST_DWithin(a.geom, b.geom, 100) AND b.kind = 7",
        "(ST_DWithin(a.geom, b.geom, 100) OR a.kind = 1)",
        "ST_DWithin(a.geom, b.geom, 100) OR ST_DWithin(a.geom, b.geom, 200)",
        "NOT ST_Distance(a.geom, b.geom) > 100",
        "a.id IN (SELECT c.id FROM pois c WHERE ST_DWithin(c.geom, b.geom, 100))",
        "ST_DWithin(a.geom, b.geom, a.radius)",
        "ST_Distance(a.geom, b.geom) < b.radius",
        "ST_Distance(a.geom, b.geom) > 500",
        "100 < ST_Distance(a.geom, b.geom)",
    ],
)
def test_distance_that_does_not_bound_the_pair_is_unknown(condition: str) -> None:
    assert compute_buffer_distance(f"SELECT a.id FROM pois a, pois b WHERE {condition}") is None
    assert extract_distance_constraints(f"SELECT a.id FROM pois a, pois b WHERE {condition}") == []


def test_aliases_tied_only_by_an_attribute_join_are_unbounded() -> None:
    assert compute_buffer_distance(ATTRIBUTE_TIED) is None
    assert compute_buffer_distance(NEGATED) is None


def test_single_alias_needs_no_buffer() -> None:
    assert compute_buffer_distance(POINT_RADIUS) == 0.0


def _hashes(query: str, min_component_size: int = 2) -> dict[str, str]:
    """hash -> variant text, as the spatial lookup generates them."""
    pairs = generate_all_query_hash_pairs(
        query, "spatial_bbox", min_component_size=min_component_size, keep_all_attributes=False, auto_detect_partition_join=False,
        skip_partition_key_joins=True, geometry_column="geom",
    )
    return {h: q for q, h in pairs}


def _bbox_handler(lazy: bool) -> MagicMock:
    handler = MagicMock(spec=AbstractCacheHandler_Lazy if lazy else AbstractCacheHandler)
    handler.filter_existing_keys.side_effect = lambda keys, partition_key: set(keys)  # every variant is cached
    handler.get_spatial_filter_lazy = MagicMock(return_value="SELECT geom FROM cached")
    handler.get_spatial_filter = MagicMock(return_value=(b"\x01", 25832))
    handler.srid = 25832
    handler.spatial_filter_includes_buffer = True
    handler.spatial_filter_type = "geometry"
    return handler


@pytest.mark.parametrize("lazy", [True, False])
def test_fragments_with_aliases_unbounded_to_p0_are_not_used(lazy: bool) -> None:
    """The (p3, p4) fragment, buffered by 100, would restrict p1 although p3 can be anywhere relative to p1."""
    variants = _hashes(ATTRIBUTE_TIED)
    assert any("type = 5" in text for text in variants.values())  # the (p3, p4) fragment is generated and cached
    handler = _bbox_handler(lazy)
    apply = apply_cache_lazy if lazy else apply_cache
    enhanced, stats = apply(query=ATTRIBUTE_TIED, cache_handler=handler, partition_key="spatial_bbox", geometry_column="geom", p0_alias="p1")
    lookup = handler.get_spatial_filter_lazy if lazy else handler.get_spatial_filter
    lookup.assert_called_once()
    used = lookup.call_args.kwargs["keys"]
    assert used and lookup.call_args.kwargs["buffer_distance"] == 100.0
    for key in used:
        assert "type = 5" not in variants[key] and ".name" not in variants[key], variants[key]
    assert any("type = 3" in variants[key] for key in used)
    assert stats["enhanced"] == 1 and stats["cache_hits"] == len(used)


@pytest.mark.parametrize("lazy", [True, False])
def test_query_without_a_bounded_fragment_runs_uncached(lazy: bool) -> None:
    handler = _bbox_handler(lazy)
    apply = apply_cache_lazy if lazy else apply_cache
    enhanced, stats = apply(query=NEGATED, cache_handler=handler, partition_key="spatial_bbox", geometry_column="geom")
    assert enhanced == NEGATED
    assert stats["enhanced"] == 0 and stats["uncached_reason"] == "spatial_unbounded"
    handler.get_spatial_filter_lazy.assert_not_called()
    handler.get_spatial_filter.assert_not_called()


@pytest.mark.parametrize("lazy", [True, False])
def test_fragment_of_p0_alone_is_used_without_buffer(lazy: bool) -> None:
    """With single-table fragments, the one over p0 restricts p0 soundly with buffer 0; the one over s is unbounded."""
    variants = _hashes(NEGATED, min_component_size=1)
    handler = _bbox_handler(lazy)
    apply = apply_cache_lazy if lazy else apply_cache
    _, stats = apply(query=NEGATED, cache_handler=handler, partition_key="spatial_bbox", geometry_column="geom", min_component_size=1)
    lookup = handler.get_spatial_filter_lazy if lazy else handler.get_spatial_filter
    used = lookup.call_args.kwargs["keys"]
    assert [variants[key] for key in used] == ["SELECT DISTINCT t1.geom FROM pois AS t1 WHERE t1.kind = 3"]
    assert lookup.call_args.kwargs["buffer_distance"] == 0.0
    assert stats["enhanced"] == 1


@pytest.mark.parametrize("lazy", [True, False])
def test_explicit_buffer_uses_every_fragment(lazy: bool) -> None:
    """An explicit buffer_distance is the caller's guarantee: no fragment is dropped."""
    variants = _hashes(ATTRIBUTE_TIED)
    handler = _bbox_handler(lazy)
    apply = apply_cache_lazy if lazy else apply_cache
    apply(query=ATTRIBUTE_TIED, cache_handler=handler, partition_key="spatial_bbox", geometry_column="geom", buffer_distance=5000.0)
    lookup = handler.get_spatial_filter_lazy if lazy else handler.get_spatial_filter
    assert set(lookup.call_args.kwargs["keys"]) == set(variants)


def test_cte_query_runs_uncached() -> None:
    """The restriction would go to the outer query, where the CTE body's aliases are not in scope: no bound is known."""
    cte = "WITH c AS (SELECT p1.name FROM pois AS p1, pois AS p2 WHERE ST_DWithin(p1.geom, p2.geom, 300)) SELECT * FROM c"
    assert _resolve_buffer_distance(cte, None) is None


@pytest.mark.parametrize("lazy", [True, False])
def test_h3_mode_restricting_every_alias_of_the_table_runs_uncached(lazy: bool) -> None:
    """The ``column`` mode would also restrict p3 and p4 (same table as p1), which no bounded fragment covers."""
    handler = _bbox_handler(lazy)
    handler.spatial_filter_type = "h3_cell_ids"
    handler.h3_cell_mode = "column"
    handler.get_h3_cell_filter = MagicMock(return_value={1, 2})
    apply = apply_cache_lazy if lazy else apply_cache
    enhanced, stats = apply(query=ATTRIBUTE_TIED, cache_handler=handler, partition_key="spatial_h3", geometry_column="geom", p0_alias="p1")
    assert enhanced == ATTRIBUTE_TIED and stats["uncached_reason"] == "spatial_unbounded"
    handler.get_h3_cell_filter.assert_not_called()


@pytest.mark.parametrize(
    "condition",
    [
        "POWER(a.x - b.x, 2) + POWER(a.y - b.y, 2) < 90000",  # a squared distance: 90000 would be read as the distance
        "POWER(a.x - b.x, 2) + POWER(a.y - b.y, 2) < 0.25",  # squared below 1: read as a distance it would be too small
        "SQRT(POWER(a.x - b.x, 2) + POWER(a.y - b.y, 2)) < 300",
        "ABS(SQRT(POWER(a.x - b.x, 2))) <= 0.1",  # one coordinate only
        "COALESCE(a.x, b.x) < 5",
        "DIST(a.geom, b.geom) < 5",
    ],
)
def test_bound_on_an_expression_other_than_a_distance_call_is_unknown(condition: str) -> None:
    query = f"SELECT a.id FROM pois a, pois b WHERE {condition}"
    assert compute_buffer_distance(query) is None
    assert _resolve_buffer_distance(query, None) is None  # unknown, not "no distance predicate" (no ValueError)


@pytest.mark.parametrize(
    "condition",
    [
        "ST_DistanceSphere(a.geom, b.geom) < 500",
        "ST_Distance(a.geom::geography, b.geom::geography, true) < 500",
        "ST_3DDistance(a.geom, b.geom) <= 500",
        "ST_MaxDistance(a.geom, b.geom) < 500",
    ],
)
def test_postgis_distance_calls_bound_the_pair(condition: str) -> None:
    assert compute_buffer_distance(f"SELECT a.id FROM pois a, pois b WHERE {condition}") == 500.0
