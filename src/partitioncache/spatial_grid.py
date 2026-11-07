"""Grid-cell spatial partition keys: cells, sound widening, intersection and the query rewrites.

A cell is ``floor(x / s) << 16 | floor(y / s)`` of a point in a metric SRID, an int4. For a query whose
fact-table aliases are linked by ``ST_DWithin`` constraints, every alias of a match lies within ``D`` (the
diameter of the constraint graph) of the points every fragment holds for that match. Widening each
fragment's cell set by the disk stencil of radius ``D`` and intersecting the widened sets therefore keeps
every match, for every alias. The widening uses only the Chebyshev bound
``|floor(a / s) - floor(b / s)| <= ceil(|a - b| / s)``, so it is exact in the SRID that ``ST_DWithin``
measures in.

Two rewrites bring the cell set into the query:

* ``CELL_ARRAY`` / ``CELL_ARRAY_ANCHOR``: ``cell(alias) = ANY('{...}'::int[])`` on every fact-table alias,
  or on the anchor only.
* ``CELL_STAR``: a star join. For every non-anchor alias a pair table ``(c, nc)`` holds the cells ``nc``
  of the set within the stencil of that alias's constraint distance to the anchor cell ``c``; the alias
  joins it with ``nb.c = cell(anchor)`` and ``cell(alias) = nb.nc``. This encodes co-location, which a
  flat cell set cannot. There is no separate cell table on the anchor: the pair tables only hold anchor
  cells of the set, and the redundant equality makes the planner underestimate the joins by orders of
  magnitude.
* ``CELL_AUTO``: ``CELL_ARRAY_ANCHOR`` if a non-anchor alias has no equality or IN predicate with
  literals (no index can drive its pair-table join), else ``CELL_STAR``.

The pure functions here have no database access; ``apply_cache`` calls them.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import product
from typing import Literal, TypedDict

import networkx as nx  # type: ignore
import numpy as np
import sqlglot
from sqlglot import exp

SHIFT = 16
J_MASK = (1 << SHIFT) - 1
CELL_FUNCTION = "partitioncache_grid_cell"


# Variant-generation flags a grid key forces on both sides: population (CLI, runner) and lookup must
# generate the same fragments, or a lookup finds no entries.
class GridGenerationKwargs(TypedDict):
    skip_partition_key_joins: bool
    auto_detect_partition_join: bool


GRID_GENERATION_KWARGS: GridGenerationKwargs = {"skip_partition_key_joins": True, "auto_detect_partition_join": False}

GridMethod = Literal["CELL_AUTO", "CELL_STAR", "CELL_ARRAY", "CELL_ARRAY_ANCHOR"]
GRID_METHODS: tuple[str, ...] = ("CELL_AUTO", "CELL_STAR", "CELL_ARRAY", "CELL_ARRAY_ANCHOR")

# Schema-qualified body: PG17+ runs CREATE INDEX and ANALYZE with search_path = pg_catalog, pg_temp. No SET
# clause, because the planner must inline the function for a query to match an expression index on it.
CELL_FUNCTION_DDL = f"""
CREATE OR REPLACE FUNCTION public.{CELL_FUNCTION}(g public.geometry, s float8) RETURNS integer
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$ SELECT (floor(public.ST_X(g) / s)::int << 16) | floor(public.ST_Y(g) / s)::int $$
"""


def cell_sql(alias: str, geometry_column: str, cell_size: float) -> str:
    """SQL expression of an alias's cell; must be written the same way as the expression index."""
    return f"{CELL_FUNCTION}({alias}.{geometry_column}, {_num(cell_size)})"


def _num(x: float) -> str:
    return str(int(x)) if float(x).is_integer() else repr(float(x))


def cells_of(x: Iterable[float], y: Iterable[float], cell_size: float) -> np.ndarray:
    """Python mirror of the SQL cell function (tests, offline tools)."""
    i = np.floor(np.asarray(list(x), dtype=float) / cell_size).astype(np.int64)
    j = np.floor(np.asarray(list(y), dtype=float) / cell_size).astype(np.int64)
    return np.asarray((i << SHIFT) | j, dtype=np.int64)


# ----------------------------------------------------------------------------------------- widening


def stencil(distance: float, cell_size: float) -> list[tuple[int, int]]:
    """Cell offsets that can hold a point within ``distance`` of a point in cell (0, 0), centre first."""
    if distance < 0 or cell_size <= 0:
        raise ValueError(f"need distance >= 0 and cell_size > 0, got {distance}, {cell_size}")
    k = math.ceil(distance / cell_size)
    offsets = [(dx, dy) for dx, dy in product(range(-k, k + 1), repeat=2) if cell_size * math.hypot(max(abs(dx) - 1, 0), max(abs(dy) - 1, 0)) <= distance]
    return sorted(offsets, key=lambda o: o[0] * o[0] + o[1] * o[1])


def packed_offsets(offsets: list[tuple[int, int]]) -> np.ndarray:
    """(dx, dy) as an additive int offset on packed cells; valid while j stays inside [0, 2^16)."""
    return np.array([dx * (1 << SHIFT) + dy for dx, dy in offsets], dtype=np.int64)


def widen(cells: np.ndarray, offsets: list[tuple[int, int]]) -> np.ndarray:
    if len(cells) == 0:
        return np.zeros(0, dtype=np.int64)
    return np.asarray(np.unique((np.asarray(cells, dtype=np.int64)[:, None] + packed_offsets(offsets)[None, :]).ravel()), dtype=np.int64)


def probe(cand: np.ndarray, fragment: np.ndarray, offsets: list[tuple[int, int]]) -> np.ndarray:
    """The candidates with a fragment cell inside their stencil (== cand ∩ widen(fragment)), early exit per candidate."""
    if len(cand) == 0 or len(fragment) == 0:
        return np.zeros(0, dtype=np.int64)
    cand = np.asarray(cand, dtype=np.int64)
    ci, cj = cand >> SHIFT, cand & J_MASK
    k = max(max(abs(dx), abs(dy)) for dx, dy in offsets)
    i0, j0 = int(ci.min()) - k, int(cj.min()) - k
    h, w = int(ci.max()) + k - i0 + 1, int(cj.max()) + k - j0 + 1
    frag = np.asarray(fragment, dtype=np.int64)
    fi, fj = frag >> SHIFT, frag & J_MASK
    inside = (fi >= i0) & (fi < i0 + h) & (fj >= j0) & (fj < j0 + w)
    mask = np.zeros((h, w), dtype=bool)
    mask[fi[inside] - i0, fj[inside] - j0] = True
    li, lj = ci - i0, cj - j0
    todo = np.arange(len(cand))
    hit = np.zeros(len(cand), dtype=bool)
    for dx, dy in offsets:  # stencil() orders them centre first
        if len(todo) == 0:
            break
        found = mask[li[todo] + dx, lj[todo] + dy]
        hit[todo[found]] = True
        todo = todo[~found]
    return np.asarray(cand[hit], dtype=np.int64)


def intersect_widened(fragments: list[Iterable[int]], offsets: list[tuple[int, int]], min_gain: float = 0.0, max_stall: int = 2) -> np.ndarray:
    """∩ widen(F) over all fragments: widen the smallest, probe the others in ascending size (sorted result).

    ``min_gain > 0`` stops once ``max_stall`` probes in a row removed less than that fraction of the
    candidates. Any subset of fragments yields a superset of the full intersection, so this stays sound.
    """
    arrays = sorted((np.fromiter(f, dtype=np.int64) if not isinstance(f, np.ndarray) else f.astype(np.int64) for f in fragments), key=len)
    if not arrays:
        raise ValueError("no fragments")
    cand = widen(arrays[0], offsets)
    stall = 0
    for frag in arrays[1:]:
        if len(cand) == 0:
            break
        before = len(cand)
        cand = probe(cand, frag, offsets)
        if min_gain > 0:
            stall = stall + 1 if before - len(cand) < min_gain * before else 0
            if stall >= max_stall:
                break
    return np.sort(cand)


I_MAX = (1 << 15) - 1  # i << 16 must stay a non-negative int4
J_MAX = J_MASK


def cells_in_packing_range(sets: list[Iterable[int]], k: int) -> bool:
    """True if every cached cell, widened by k cells, stays inside the packing (no sign bit, no j wrap).

    A point outside 0 <= x/s < 2^15, 0 <= y/s < 2^16 packs into a wrong cell, and a cell within k of the
    j range edge would make the additive stencil offsets wrap into the next column; either makes the
    restriction silently unsound, so the caller must then run the query without it. A y that overflowed
    into the i bits cannot be recognised from the packed value: ``check_extent`` at setup covers that.
    """
    for values in sets:
        arr = values if isinstance(values, np.ndarray) else np.fromiter(values, dtype=np.int64)
        if len(arr) == 0:
            continue
        arr = np.asarray(arr, dtype=np.int64)
        if int(arr.min()) < 0:
            return False
        i, j = arr >> SHIFT, arr & J_MASK
        if int(i.min()) < k or int(i.max()) > I_MAX - k or int(j.min()) < k or int(j.max()) > J_MAX - k:
            return False
    return True


def check_extent(cursor, table: str, geometry_column: str, cell_size: float, max_distance: float) -> None:
    """Raise ValueError unless every point of ``table`` packs into a valid cell with room for the stencil.

    Call once at setup for each cell size: the packing needs 0 <= x/s < 2^15 and 0 <= y/s < 2^16 in a
    metric SRID, with ceil(max_distance / s) cells of margin on every side.
    """
    from psycopg import sql  # optional dependency (the "db" extra); only setup code calls this

    cursor.execute(
        sql.SQL("SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM (SELECT ST_Extent({col}) AS e FROM {tbl}) x").format(
            col=sql.Identifier(geometry_column), tbl=sql.Identifier(*table.split("."))
        )
    )
    row = cursor.fetchone()
    if row is None or row[0] is None:
        raise ValueError(f"{table}.{geometry_column} has no geometries; the grid extent cannot be checked")
    xmin, ymin, xmax, ymax = row
    k = math.ceil(max_distance / cell_size)
    bad = []
    if math.floor(xmin / cell_size) < k or math.floor(xmax / cell_size) > I_MAX - k:
        bad.append(f"x/s in [{xmin / cell_size:.0f}, {xmax / cell_size:.0f}] outside [{k}, {I_MAX - k}]")
    if math.floor(ymin / cell_size) < k or math.floor(ymax / cell_size) > J_MAX - k:
        bad.append(f"y/s in [{ymin / cell_size:.0f}, {ymax / cell_size:.0f}] outside [{k}, {J_MAX - k}]")
    if bad:
        raise ValueError(f"{table}.{geometry_column} does not fit the grid packing at cell size {cell_size}: " + "; ".join(bad))


# ------------------------------------------------------------------------------------- query shape


def dwithin_edges(query: str, geometry_column: str) -> list[tuple[str, str, float]]:
    """``ST_DWithin(a.<geom>, b.<geom>, d)`` constraints between two aliases with a literal distance.

    Deliberately narrower than ``query_processor.extract_distance_constraints``: other distance expressions
    (SQRT/POWER, DIST ... BETWEEN) are in other units and say nothing about the metric cells.
    """
    tree = sqlglot.parse_one(query, dialect="postgres")
    edges = []
    for fn in tree.find_all(exp.Func):
        name = fn.name if isinstance(fn, exp.Anonymous) else fn.sql_name()
        if name.upper() != "ST_DWITHIN":
            continue
        args = list(fn.expressions) if isinstance(fn, exp.Anonymous) else [a for a in fn.args.values() if isinstance(a, exp.Expr)]
        if len(args) != 3:
            continue
        a, b, d = args
        if (
            isinstance(a, exp.Column)
            and isinstance(b, exp.Column)
            and a.name == geometry_column
            and b.name == geometry_column
            and a.table
            and b.table
            and a.table != b.table
            and isinstance(d, exp.Literal)
            and not d.is_string
        ):
            edges.append((a.table, b.table, float(d.this)))
    return edges


def alias_distances(query: str, geometry_column: str) -> dict[str, dict[str, float]]:
    """All-pairs shortest paths of the query's ST_DWithin graph over table aliases.

    Several constraints on one pair all hold, so the smallest is used (the tightest sound bound). Unlike
    ``compute_buffer_distance`` there is no point-radius fallback: a single-alias query has diameter 0.
    """
    g = nx.Graph()
    for a, b, d in dwithin_edges(query, geometry_column):
        if g.has_edge(a, b):
            g[a][b]["weight"] = min(g[a][b]["weight"], d)
        else:
            g.add_edge(a, b, weight=d)
    return {a: dict(lengths) for a, lengths in nx.all_pairs_dijkstra_path_length(g, weight="weight")}


def fact_aliases(query: str, anchor: str) -> list[str]:
    """Aliases of the anchor's table, in FROM order (the aliases the cell key applies to)."""
    tree = sqlglot.parse_one(query, dialect="postgres")
    tables = list(tree.find_all(exp.Table))
    base = next((t.name for t in tables if t.alias_or_name == anchor), None)
    if base is None:
        raise ValueError(f"anchor alias {anchor!r} not in query")
    return [t.alias_or_name for t in tables if t.name == base]


@dataclass(frozen=True)
class GridPlan:
    """What the lookup needs for one query: aliases, diameter, per-alias anchor distances."""

    aliases: list[str]
    anchor: str
    diameter: float
    anchor_distance: dict[str, float]


def plan_query(query: str, anchor: str, geometry_column: str) -> GridPlan:
    aliases = fact_aliases(query, anchor)
    dist = alias_distances(query, geometry_column)
    if len(aliases) > 1:
        missing = [a for a in aliases if a not in dist.get(anchor, {})]
        if missing:
            raise ValueError(f"aliases {missing} are not linked to {anchor} by ST_DWithin; no sound cell restriction exists")
    diameter = max((d for a in aliases for b, d in dist.get(a, {}).items() if b in aliases), default=0.0)
    return GridPlan(aliases, anchor, diameter, {a: dist.get(anchor, {}).get(a, 0.0) for a in aliases if a != anchor})


# ------------------------------------------------------------------------------------------- rewrites


def _array_literal(cells: np.ndarray) -> str:
    return "'{" + ",".join(map(str, np.asarray(cells, dtype=np.int64).tolist())) + "}'::int[]"


def _append_where(tree: exp.Expr, condition: str) -> None:
    cond = sqlglot.parse_one(condition, dialect="postgres")
    where = tree.args.get("where")
    tree.set("where", exp.Where(this=cond if where is None else exp.and_(where.this, cond)))


def rewrite_cell_array(query: str, cells: np.ndarray, aliases: list[str], geometry_column: str, cell_size: float) -> str:
    """``cell(alias) = ANY('{...}'::int[])`` for each alias; the key list is spliced, not parsed."""
    token = "__PC_GRID_CELLS__"
    tree = sqlglot.parse_one(query, dialect="postgres")
    for a in aliases:
        _append_where(tree, f"{cell_sql(a, geometry_column, cell_size)} = ANY({token})")
    return tree.sql(dialect="postgres").replace(token, _array_literal(cells))


def rewrite_cell_star(
    query: str, cells: np.ndarray, plan: GridPlan, geometry_column: str, cell_size: float, table_prefix: str = "_pc_grid"
) -> tuple[list[str], str]:
    """Star join through per-distance neighbour pair tables. Returns (setup statements, query).

    Each non-anchor alias joins a pair table ``(c, nc)`` on ``c = cell(anchor)`` and ``nc = cell(alias)``.
    The pair tables hold only cells of the set, so they also restrict the anchor; a single-alias query
    joins the cell table itself instead.
    """
    tmp = f"{table_prefix}_cells"
    setup = [
        f"CREATE TEMPORARY TABLE {tmp} ON COMMIT DROP AS SELECT DISTINCT unnest({_array_literal(cells)}) AS c",
        f"CREATE INDEX ON {tmp} (c)",
        f"ANALYZE {tmp}",
    ]
    tree = sqlglot.parse_one(query, dialect="postgres")
    anchor_cell = cell_sql(plan.anchor, geometry_column, cell_size)
    others = [a for a in plan.aliases if a != plan.anchor]
    if not others:
        tree.append("joins", exp.Join(this=exp.to_table(tmp).as_("p0")))
        _append_where(tree, f"{anchor_cell} = p0.c")
        return setup, tree.sql(dialect="postgres")
    made: dict[tuple[tuple[int, int], ...], str] = {}
    for a in others:
        offs = tuple(stencil(plan.anchor_distance[a], cell_size))
        if offs not in made:
            nb = f"{table_prefix}_nb{len(made)}"
            values = ",".join(f"({o})" for o in packed_offsets(list(offs)).tolist())
            setup += [
                f"CREATE TEMPORARY TABLE {nb} ON COMMIT DROP AS SELECT t.c AS c, t.c + o.o AS nc "
                f"FROM {tmp} t, (VALUES {values}) AS o(o) WHERE t.c + o.o IN (SELECT c FROM {tmp})",
                f"CREATE INDEX ON {nb} (c, nc)",
                f"ANALYZE {nb}",
            ]
            made[offs] = nb
        nb_alias = f"_nb_{a}"
        tree.append("joins", exp.Join(this=exp.to_table(made[offs]).as_(nb_alias)))
        _append_where(tree, f"{nb_alias}.c = {anchor_cell}")
        _append_where(tree, f"{cell_sql(a, geometry_column, cell_size)} = {nb_alias}.nc")
    return setup, tree.sql(dialect="postgres")


def typed_aliases(query: str) -> set[str]:
    """Aliases with an equality or IN predicate against literals (something an index can drive)."""
    tree = sqlglot.parse_one(query, dialect="postgres")
    typed: set[str] = set()
    for node in tree.find_all(exp.EQ, exp.In):
        col = node.this if isinstance(node.this, exp.Column) else None
        if col is None or not col.table:
            continue
        rhs = [node.expression] if isinstance(node, exp.EQ) else list(node.expressions)
        if rhs and all(isinstance(r, exp.Literal) for r in rhs):
            typed.add(col.table)
    return typed


def choose_grid_method(query: str, plan: GridPlan) -> str:
    """CELL_AUTO: the anchor-only array when a non-anchor alias has nothing an index can drive, else the star.

    Such an alias gives the pair-table join no index to start from; restricting the anchor alone keeps the
    selective start and lets the spatial index resolve the alias.
    """
    typed = typed_aliases(query)
    if any(a not in typed for a in plan.aliases if a != plan.anchor):
        return "CELL_ARRAY_ANCHOR"
    return "CELL_STAR"


def cell_projection_sql(fragment_sql: str, geometry_column: str, cell_size: float, partition_key: str) -> str:
    """Population SQL of a spatial fragment for a grid key: the distinct cells of all its geometry columns.

    Spatial fragments select ``alias.geom`` (one alias) or ``alias.geom AS geom_1, ...`` (several aliases).
    """
    import re

    idx = sorted({int(m) for m in re.findall(rf"\b{re.escape(geometry_column)}_(\d+)\b", fragment_sql)})
    cols = [f"sub.{geometry_column}_{i}" for i in idx] or [f"sub.{geometry_column}"]
    values = ", ".join(f"({CELL_FUNCTION}({c}, {_num(cell_size)}))" for c in cols)
    return f"SELECT DISTINCT v.c AS {partition_key} FROM ({fragment_sql}) AS sub CROSS JOIN LATERAL (VALUES {values}) AS v(c) WHERE v.c IS NOT NULL"
