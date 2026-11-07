"""Unit tests for grid-cell spatial keys (partitioncache.spatial_grid) and their apply_cache routing."""

from __future__ import annotations

import math
from typing import Any, cast
from unittest.mock import MagicMock

import numpy as np
import pytest
import sqlglot

from partitioncache import spatial_grid as sg
from partitioncache.apply_cache import apply_cache, apply_cache_lazy
from partitioncache.query_processor import generate_all_query_hash_pairs

RNG = np.random.default_rng(7)
X0, Y0 = 280_000.0, 5_236_000.0  # Germany in SRID 25832

STAR = (
    "SELECT p1.name, p2.name, p3.name FROM pois AS p1, pois AS p2, pois AS p3 WHERE p1.subtype = 'school' "
    "AND p2.name LIKE '%x%' AND ST_DWithin(p1.geom, p2.geom, 500) AND ST_DWithin(p1.geom, p3.geom, 300)"
)
CHAIN = (
    "SELECT p1.name FROM pois AS p1, pois AS p2, pois AS p3 WHERE p1.subtype = 'cafe' "
    "AND ST_DWithin(p1.geom, p2.geom, 300) AND ST_DWithin(p2.geom, p3.geom, 400) AND ST_DWithin(p1.geom, p2.geom, 250)"
)


# ------------------------------------------------------------------------------------- soundness


@pytest.mark.parametrize(("s", "d"), [(100, 200), (100, 300), (200, 400), (200, 700), (400, 1000), (800, 1800)])
def test_stencil_contains_every_offset_of_a_pair_within_distance(s: float, d: float) -> None:
    offsets = set(sg.stencil(d, s))
    n = 100_000
    x, y = RNG.uniform(X0, X0 + 50_000, n), RNG.uniform(Y0, Y0 + 50_000, n)
    ang = RNG.uniform(0, 2 * math.pi, n)
    r = d * np.sqrt(RNG.uniform(0, 1, n))
    r[: n // 10] = d  # pairs exactly at the limit, which ST_DWithin includes
    x2, y2 = x + r * np.cos(ang), y + r * np.sin(ang)
    di = np.floor(x2 / s) - np.floor(x / s)
    dj = np.floor(y2 / s) - np.floor(y / s)
    assert {(int(a), int(b)) for a, b in zip(di, dj, strict=True)} <= offsets


def test_stencil_is_a_disk_and_centre_first() -> None:
    offsets = sg.stencil(800, 100)
    assert offsets[0] == (0, 0)
    assert len(offsets) < 17 * 17


def test_intersect_widened_equals_brute_force() -> None:
    for _ in range(30):
        s, d = 200, float(RNG.choice([300, 400, 700]))
        offsets = sg.stencil(d, s)
        frags = []
        for _ in range(int(RNG.integers(1, 6))):
            m = int(RNG.integers(1, 400))
            frags.append(set(((RNG.integers(1400, 1460, m) << 16) | RNG.integers(26200, 26260, m)).tolist()))
        expect = set(sg.widen(np.array(sorted(frags[0])), offsets).tolist())
        for f in frags[1:]:
            expect &= set(sg.widen(np.array(sorted(f)), offsets).tolist())
        assert set(sg.intersect_widened(frags, offsets).tolist()) == expect


def test_stopping_early_returns_a_superset() -> None:
    offsets = sg.stencil(400, 200)
    frags = [set(((RNG.integers(1400, 1500, 300) << 16) | RNG.integers(26200, 26300, 300)).tolist()) for _ in range(8)]
    full = set(sg.intersect_widened(frags, offsets).tolist())
    stopped = set(sg.intersect_widened(frags, offsets, min_gain=0.5, max_stall=1).tolist())
    assert full <= stopped


def test_planted_match_survives_noise() -> None:
    s, d = 200, 700.0
    offsets = sg.stencil(d, s)
    for _ in range(200):
        cx, cy = RNG.uniform(X0 + 1e4, X0 + 9e4), RNG.uniform(Y0 + 1e4, Y0 + 9e4)
        pts = [(cx + RNG.uniform(-220, 220), cy + RNG.uniform(-220, 220)) for _ in range(3)]
        assert all(math.dist(a, b) <= d for a in pts for b in pts)
        cells = [int(sg.cells_of([x], [y], s)[0]) for x, y in pts]
        frags = [set(((RNG.integers(1400, 1900, 50) << 16) | RNG.integers(26200, 26700, 50)).tolist()) | {c} for c in cells]
        result = set(sg.intersect_widened(frags, offsets).tolist())
        assert all(c in result for c in cells)


# ---------------------------------------------------------------------------------- query shape


def test_dwithin_edges_ignore_other_distance_expressions() -> None:
    q = CHAIN + " AND SQRT(POWER(p1.x - p3.x, 2)) < 0.008"
    assert sorted(sg.dwithin_edges(q, "geom")) == [("p1", "p2", 250.0), ("p1", "p2", 300.0), ("p2", "p3", 400.0)]


def test_plan_uses_tightest_constraint_and_chain_sums() -> None:
    plan = sg.plan_query(CHAIN, "p1", "geom")
    assert plan.aliases == ["p1", "p2", "p3"]
    assert plan.anchor_distance == {"p2": 250.0, "p3": 650.0}
    assert plan.diameter == 650.0


def test_single_alias_point_query_has_diameter_zero() -> None:
    q = "SELECT p1.name FROM pois AS p1 WHERE ST_DWithin(p1.geom, ST_Transform(ST_SetSRID(ST_MakePoint(10.0, 51.0), 4326), 25832), 50000)"
    plan = sg.plan_query(q, "p1", "geom")
    assert plan.diameter == 0.0 and plan.anchor_distance == {}


def test_unlinked_alias_is_rejected() -> None:
    q = "SELECT p1.name FROM pois AS p1, pois AS p2, pois AS p3 WHERE ST_DWithin(p1.geom, p2.geom, 100)"
    with pytest.raises(ValueError, match="not linked"):
        sg.plan_query(q, "p1", "geom")


MIXED = "SELECT a.id FROM pois AS a, stations AS s WHERE a.type = 'cafe' AND s.kind = 'bus' AND ST_DWithin(a.geom, s.geom, 300)"


def test_plan_spans_the_aliases_of_other_tables() -> None:
    """The fragments store the stations' cells too, so the widening must cover the cafe-station distance."""
    plan = sg.plan_query(MIXED, "a", "geom")
    assert plan.aliases == ["a"] and plan.diameter == 300.0


def test_widening_keeps_every_match_when_fragments_hold_other_tables() -> None:
    """The fragments of MIXED: all cafes, all bus stations, and the cells of both sides of every pair within 300 m."""
    s, d = 100.0, 300.0
    cafes = np.column_stack([RNG.uniform(560_000, 563_000, 3000), RNG.uniform(5_930_000, 5_933_000, 3000)])
    stops = np.column_stack([RNG.uniform(560_000, 563_000, 300), RNG.uniform(5_930_000, 5_933_000, 300)])
    near = np.hypot(cafes[:, None, 0] - stops[None, :, 0], cafes[:, None, 1] - stops[None, :, 1]) <= d
    ci, si = np.nonzero(near)
    cafe_cells, stop_cells = sg.cells_of(cafes[:, 0], cafes[:, 1], s), sg.cells_of(stops[:, 0], stops[:, 1], s)
    fragments = [set(cafe_cells.tolist()), set(stop_cells.tolist()), set(cafe_cells[ci].tolist()) | set(stop_cells[si].tolist())]
    offsets = sg.stencil(sg.plan_query(MIXED, "a", "geom").diameter, s)
    result = set(sg.intersect_widened(fragments, offsets).tolist())
    assert len(ci) > 100 and set(cafe_cells[ci].tolist()) <= result


@pytest.mark.parametrize(
    "condition",
    [
        "NOT ST_DWithin(p1.geom, p2.geom, 100)",
        "(ST_DWithin(p1.geom, p2.geom, 100) OR p1.id = p2.id)",
        "EXISTS (SELECT 1 FROM pois AS p3 WHERE ST_DWithin(p1.geom, p2.geom, 100))",
        "CASE WHEN ST_DWithin(p1.geom, p2.geom, 100) THEN TRUE ELSE p1.id = p2.id END",
        "ST_DWithin(p1.geom::geography, p2.geom::geography, 100)",
    ],
)
def test_only_top_level_dwithin_conjuncts_bound_the_distance(condition: str) -> None:
    q = f"SELECT p1.name FROM pois AS p1, pois AS p2 WHERE p1.subtype = 'a' AND p2.subtype = 'b' AND {condition}"
    with pytest.raises(sg.GridPlanError, match="not linked"):
        sg.plan_query(q, "p1", "geom")


def test_inner_join_on_bounds_the_distance() -> None:
    q = "SELECT p1.name FROM pois AS p1 JOIN pois AS p2 ON ST_DWithin(p1.geom, p2.geom, 250) WHERE p1.subtype = 'a'"
    assert sg.plan_query(q, "p1", "geom").diameter == 250.0


def test_aliases_in_subqueries_are_not_restricted() -> None:
    q = "SELECT p1.name FROM pois AS p1 WHERE p1.subtype = 'a' AND EXISTS (SELECT 1 FROM pois AS p9 WHERE p9.id = p1.id)"
    assert sg.plan_query(q, "p1", "geom").aliases == ["p1"]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT p1.name FROM pois AS p1, pois AS p2 WHERE p1.subtype = 'a' AND NOT ST_DWithin(p1.geom, p2.geom, 100)",
        "WITH c AS (SELECT * FROM pois AS p1 WHERE p1.subtype = 'a') SELECT * FROM c",
        "SELECT s.name FROM (SELECT * FROM pois AS p1) AS s WHERE s.subtype = 'a'",
        "SELECT p1.name FROM pois AS p1 LEFT JOIN pois AS p2 ON ST_DWithin(p1.geom, p2.geom, 100)",
    ],
)
def test_apply_cache_runs_unplannable_grid_queries_uncached(query: str) -> None:
    handler = cast(Any, MagicMock())
    sql, stats = apply_cache(query, handler, "cell100", geometry_column="geom", grid_cell_size=100)
    assert sql == query and stats["enhanced"] == 0 and stats["uncached_reason"]
    assert not handler.filter_existing_keys.called  # planned before the lookup


def test_stencil_keeps_offsets_exactly_at_the_distance() -> None:
    s = 0.1
    assert (2, 2) in sg.stencil(math.sqrt(2) * s, s)  # min distance s * hypot(1, 1), rounding must not drop it


# ------------------------------------------------------------------------------------- rewrites


def _parses(sql: str) -> None:
    for stmt in sqlglot.parse(sql, dialect="postgres"):
        assert stmt is not None


def test_cell_array_restricts_every_alias_and_splices_the_list() -> None:
    cells = np.array([(2000 << 16) | (28000 + k) for k in range(4)])
    out = sg.rewrite_cell_array(STAR, cells, ["p1", "p2", "p3"], "geom", 200)
    _parses(out)
    assert out.upper().count("PARTITIONCACHE_GRID_CELL(") == 3
    assert f"'{{{','.join(map(str, cells.tolist()))}}}'::int[]" in out


def test_cell_star_builds_one_pair_table_per_distance_without_a_hub_join() -> None:
    cells = np.array([(2000 << 16) | (28000 + k) for k in range(4)])
    plan = sg.plan_query(STAR, "p1", "geom")
    setup, query = sg.rewrite_cell_star(STAR, cells, plan, "geom", 200)
    _parses(query)
    assert sum(st.startswith("CREATE TEMPORARY TABLE") for st in setup) == 3  # cell table + 500 m + 300 m pairs
    assert "AS p0" not in query  # the pair tables restrict the anchor; a hub join misled the planner
    assert "_NB_P2.C = PARTITIONCACHE_GRID_CELL(P1.GEOM, 200)" in query.upper() and "_nb_p3.nc" in query
    single = "SELECT p1.name FROM pois AS p1 WHERE p1.subtype = 'x'"
    setup1, query1 = sg.rewrite_cell_star(single, cells, sg.plan_query(single, "p1", "geom"), "geom", 200)
    assert len(setup1) == 3 and "p0.c" in query1


def test_typed_aliases_and_auto_choice() -> None:
    assert sg.typed_aliases(STAR) == {"p1"}
    plan = sg.plan_query(STAR, "p1", "geom")
    assert sg.choose_grid_method(STAR, plan) == "CELL_ARRAY_ANCHOR"  # p2 (LIKE only) and p3 have nothing indexable
    typed = STAR.replace("p2.name LIKE '%x%'", "p2.subtype IN ('library', 'archive')") + " AND p3.subtype = 'hospital'"
    assert sg.typed_aliases(typed) == {"p1", "p2", "p3"}
    assert sg.choose_grid_method(typed, sg.plan_query(typed, "p1", "geom")) == "CELL_STAR"


def test_cell_projection_flattens_all_geometry_columns() -> None:
    multi = "SELECT DISTINCT t1.geom AS geom_1, t2.geom AS geom_2 FROM pois AS t1, pois AS t2 WHERE ST_DWITHIN(t1.geom, t2.geom, 400)"
    out = sg.cell_projection_sql(multi, "geom", 200, "cell200")
    _parses(out)
    assert "partitioncache_grid_cell(sub.geom_1, 200)" in out and "partitioncache_grid_cell(sub.geom_2, 200)" in out
    assert "AS cell200" in out
    single = sg.cell_projection_sql("SELECT DISTINCT t1.geom FROM pois AS t1 WHERE t1.subtype = 'x'", "geom", 200, "cell200")
    assert "partitioncache_grid_cell(sub.geom, 200)" in single


def test_grid_pairs_hash_the_projection_so_cell_sizes_never_collide() -> None:
    kw = {"query": STAR, "partition_key": "cell", "geometry_column": "geom", "warn_no_partition_key": False, **sg.GRID_GENERATION_KWARGS}
    spatial = {h for _, h in generate_all_query_hash_pairs(**kw)}
    g200 = generate_all_query_hash_pairs(**kw, grid_cell_size=200)
    g400 = generate_all_query_hash_pairs(**kw, grid_cell_size=400)
    assert len(g200) == len(spatial) and all(q.startswith("SELECT DISTINCT v.c AS cell") for q, _ in g200)
    h200, h400 = {h for _, h in g200}, {h for _, h in g400}
    assert not h200 & h400 and not h200 & spatial
    with pytest.raises(ValueError, match="geometry_column"):
        generate_all_query_hash_pairs(query=STAR, partition_key="cell", grid_cell_size=200)


def test_population_pairs_cover_the_lookup_hashes() -> None:
    """The flags a grid key forces must give population and lookup the same fragments."""

    population = {
        h
        for _, h in generate_all_query_hash_pairs(
            query=STAR,
            partition_key="cell200",
            geometry_column="geom",
            grid_cell_size=200,
            min_component_size=2,
            keep_all_attributes=False,
            warn_no_partition_key=False,
            **sg.GRID_GENERATION_KWARGS,
        )
    }
    lookup = set(_hashes(STAR))
    assert lookup and lookup <= population


# ---------------------------------------------------------------------------- apply_cache routing


class _SetHandler:
    """Minimal in-memory integer cache: every hash maps to a set of cells (duck-typed AbstractCacheHandler)."""

    def __init__(self, entries: dict[str, set[int]]) -> None:
        self.entries = entries

    def filter_existing_keys(self, keys: set[str], partition_key: str) -> set[str]:  # noqa: ARG002
        return {k for k in keys if k in self.entries}

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | None:  # noqa: ARG002
        return self.entries.get(key)


def _handler(entries: dict[str, set[int]]) -> Any:
    return cast(Any, _SetHandler(entries))


def _hashes(query: str) -> list[str]:
    from partitioncache.query_processor import generate_all_hashes

    return generate_all_hashes(
        query=query,
        partition_key="cell200",
        geometry_column="geom",
        grid_cell_size=200,
        min_component_size=2,
        fix_attributes=False,
        **sg.GRID_GENERATION_KWARGS,
    )


@pytest.mark.parametrize("apply_fn", [apply_cache, apply_cache_lazy])
def test_apply_routes_grid_keys_to_the_cell_rewrite(apply_fn) -> None:
    hashes = _hashes(STAR)
    cell = (2000 << 16) | 28000
    handler = _handler({h: {cell} for h in hashes[:2]})
    out, stats = apply_fn(STAR, handler, "cell200", geometry_column="geom", grid_cell_size=200, grid_method="CELL_ARRAY", p0_alias="p1")
    assert stats["cache_hits"] == 2 and stats["enhanced"] == 1 and stats["grid_cells"] == len(sg.stencil(800, 200))
    assert out.upper().count("PARTITIONCACHE_GRID_CELL(") == 3
    star, st = apply_fn(STAR, handler, "cell200", geometry_column="geom", grid_cell_size=200, grid_method="CELL_STAR", p0_alias="p1")
    assert "CREATE TEMPORARY TABLE _pc_grid_cells" in star and "_nb_p2" in star and st["grid_method"] == "CELL_STAR"
    auto, st = apply_fn(STAR, handler, "cell200", geometry_column="geom", grid_cell_size=200, p0_alias="p1")
    assert st["grid_method"] == "CELL_ARRAY_ANCHOR" and auto.upper().count("PARTITIONCACHE_GRID_CELL(") == 1


def test_apply_with_disjoint_fragments_yields_an_empty_restriction() -> None:
    hashes = _hashes(STAR)
    handler = _handler({hashes[0]: {(1400 << 16) | 26200}, hashes[1]: {(4000 << 16) | 30000}})
    out, stats = apply_cache(STAR, handler, "cell200", geometry_column="geom", grid_cell_size=200, p0_alias="p1")
    assert stats["grid_cells"] == 0 and "FALSE" in out.upper()


def test_apply_grid_rejects_unknown_method_and_missing_geometry() -> None:
    handler = _handler({})
    with pytest.raises(ValueError, match="geometry_column"):
        apply_cache(STAR, handler, "cell200", grid_cell_size=200)
    with pytest.raises(ValueError, match="grid_method"):
        apply_cache(STAR, _handler({h: {1} for h in _hashes(STAR)}), "cell200", geometry_column="geom", grid_cell_size=200, grid_method="IN")


# ------------------------------------------------------------------------------------ packing guard


def test_packing_guard_rejects_negative_and_edge_cells() -> None:
    ok = {(2000 << 16) | 28000}
    assert sg.cells_in_packing_range([ok], k=3)
    assert not sg.cells_in_packing_range([ok, {-5}], k=3)  # negative coordinate: wrong cell
    assert not sg.cells_in_packing_range([{(2000 << 16) | 1}], k=3)  # j - k would wrap into column i - 1
    assert not sg.cells_in_packing_range([{(2000 << 16) | (sg.J_MAX - 1)}], k=3)
    assert not sg.cells_in_packing_range([{(sg.I_MAX - 1) << 16 | 28000}], k=3)


def test_apply_runs_uncached_when_cells_leave_the_packing_range() -> None:
    hashes = _hashes(STAR)
    handler = _handler({hashes[0]: {-1}, hashes[1]: {-1}})
    out, stats = apply_cache(STAR, handler, "cell200", geometry_column="geom", grid_cell_size=200, p0_alias="p1")
    assert out == STAR and stats["uncached_reason"] == "grid cells outside the packing range"


class _ExtentCursor:
    def __init__(self, extent: tuple[float, float, float, float]) -> None:
        self.extent = extent

    def execute(self, sql: str) -> None:  # noqa: ARG002
        pass

    def fetchone(self) -> tuple[float, float, float, float]:
        return self.extent


def test_check_extent_accepts_germany_in_utm_and_rejects_degrees_and_far_north() -> None:
    sg.check_extent(_ExtentCursor((280_000, 5_236_000, 921_000, 6_101_000)), "pois", "geom", 200, 1000)
    with pytest.raises(ValueError, match="x/s"):
        sg.check_extent(_ExtentCursor((-10.0, 35.0, 30.0, 70.0)), "pois", "geom", 200, 1000)  # SRID 4326 degrees
    with pytest.raises(ValueError, match="y/s"):
        sg.check_extent(_ExtentCursor((280_000, 6_600_000, 921_000, 7_900_000)), "pois", "geom", 100, 1000)  # y/s > 65535
