"""The buffer the cached geometries are widened by: none for a single table, the distance-graph diameter otherwise."""

from __future__ import annotations

import math

import pytest

from partitioncache.apply_cache import _resolve_buffer_distance
from partitioncache.cache_handler.postgis_bbox import PostGISBBoxCacheHandler

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


def test_cte_query_gets_the_diameter_of_its_body() -> None:
    cte = "WITH c AS (SELECT p1.name FROM pois AS p1, pois AS p2 WHERE ST_DWithin(p1.geom, p2.geom, 300)) SELECT * FROM c"
    assert _resolve_buffer_distance(cte, None) == 300.0


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
