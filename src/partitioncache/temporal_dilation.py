"""Temporal dilation for bucketed partition keys.

A correlation query links two events by a bounded window ("a failed logon followed by a
successful one within an hour"). When the partition key is a time bucket, the two events can
fall in *adjacent* buckets, so intersecting the per-fragment bucket sets is unsound: the
anchor's bucket may be missing from the other fragment's set even though the pair is a real
match.

This is the one-dimensional counterpart of the spatial buffer. ``postgis_bbox`` buffers each
cached geometry *before* intersecting them; here each cached bucket set is dilated by ``k``
buckets before intersection, with ``k = ceil(window / bucket_width)``.

The order matters and is not negotiable. Dilating the *result* of an intersection is unsound,
because the undilated intersection may already be empty::

    dilate(A ∩ B)  ⊆  dilate(A) ∩ dilate(B)

Soundness argument: if two correlated events satisfy ``|t2 - t1| <= W`` then their buckets
satisfy ``|b2 - b1| <= ceil(W / width)``, so the anchor bucket ``b1`` is always contained in
the dilated set built from ``b2``.
"""

import math
from collections.abc import Iterator
from decimal import Decimal, InvalidOperation

import networkx as nx  # type: ignore
import sqlglot
import sqlglot.expressions as exp

__all__ = ["compute_dilation_buckets", "compute_temporal_window", "dilate_bucket_set"]


def compute_dilation_buckets(window: float, bucket_width: float) -> int:
    """Return the number of buckets a cached set must be dilated by.

    Args:
        window: Correlation window in the same unit as ``bucket_width`` (e.g. seconds).
            Typically obtained from :func:`partitioncache.query_processor.compute_buffer_distance`.
        bucket_width: Width of one partition bucket, in the same unit as ``window``.

    Returns:
        ``ceil(window / bucket_width)``, or 0 when the query has no window. A return value of
        0 means the plain intersection is already sound and no dilation is needed.

    Raises:
        ValueError: If ``bucket_width`` is not positive, or ``window`` is negative.

    Note:
        Dilation inflates every cached set by a factor of roughly ``2k + 1``. Choosing a
        bucket width at least as large as the widest correlation window keeps ``k`` at 1; a
        24h window over 1h buckets needs ``k = 24``, which inflates each set ~49x and
        destroys the selectivity the cache exists to provide.
    """
    if bucket_width <= 0:
        raise ValueError(f"bucket_width must be positive, got {bucket_width}")
    if window < 0:
        raise ValueError(f"window must be non-negative, got {window}")
    if window == 0:
        return 0
    return math.ceil(window / bucket_width)


def dilate_bucket_set(values: set[int], dilation: int) -> set[int]:
    """Expand a bucket set by ``dilation`` buckets in both directions.

    Args:
        values: Bucket identifiers from one cached fragment.
        dilation: Number of buckets to expand by. 0 returns the input unchanged.

    Returns:
        A new set containing every bucket within ``dilation`` of an input bucket.

    Raises:
        ValueError: If ``dilation`` is negative.
    """
    if dilation < 0:
        raise ValueError(f"dilation must be non-negative, got {dilation}")
    if dilation == 0 or not values:
        return set(values)
    offsets = range(-dilation, dilation + 1)
    return {value + offset for value in values for offset in offsets}


# A linear expression over time columns: ({alias: coefficient}, constant)
_Linear = tuple[dict[str, Decimal], Decimal]


def _linear(node: exp.Expr, temporal_column: str) -> _Linear | None:
    """``node`` as a linear expression over ``alias.temporal_column`` columns, or None if it is anything else."""
    if isinstance(node, exp.Paren):
        return _linear(node.this, temporal_column)
    if isinstance(node, exp.Column):
        if node.name != temporal_column or not node.table:
            return None
        return {node.table: Decimal(1)}, Decimal(0)
    if isinstance(node, exp.Literal) and not node.is_string:
        try:
            value = Decimal(node.this)
        except InvalidOperation:
            return None
        return ({}, value) if value.is_finite() else None
    if isinstance(node, exp.Neg):
        inner = _linear(node.this, temporal_column)
        return None if inner is None else _scale(inner, Decimal(-1))
    if isinstance(node, exp.Add | exp.Sub):
        left, right = _linear(node.this, temporal_column), _linear(node.expression, temporal_column)
        if left is None or right is None:
            return None
        return _plus(left, right if isinstance(node, exp.Add) else _scale(right, Decimal(-1)))
    return None


def _scale(linear: _Linear, factor: Decimal) -> _Linear:
    return {alias: coef * factor for alias, coef in linear[0].items()}, linear[1] * factor


def _plus(left: _Linear, right: _Linear) -> _Linear:
    coefs = dict(left[0])
    for alias, coef in right[0].items():
        coefs[alias] = coefs.get(alias, Decimal(0)) + coef
    return {alias: coef for alias, coef in coefs.items() if coef != 0}, left[1] + right[1]


def _upper_bounds(node: exp.Expr, temporal_column: str) -> Iterator[_Linear]:
    """Linear expressions ``e`` with ``e <= 0`` for every row the predicate ``node`` accepts.

    Only shapes that bound a difference of time columns are recognised; anything else yields nothing,
    which never tightens the window.
    """
    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.Between):
        yield from _upper_bounds(exp.GTE(this=node.this, expression=node.args["low"]), temporal_column)
        yield from _upper_bounds(exp.LTE(this=node.this, expression=node.args["high"]), temporal_column)
        return
    if isinstance(node, exp.EQ):
        yield from _upper_bounds(exp.LTE(this=node.this, expression=node.expression), temporal_column)
        yield from _upper_bounds(exp.GTE(this=node.this, expression=node.expression), temporal_column)
        return
    if isinstance(node, exp.LT | exp.LTE):
        small, large = node.this, node.expression
    elif isinstance(node, exp.GT | exp.GTE):
        small, large = node.expression, node.this
    else:
        return
    # ABS(x) <= y holds exactly when x <= y and -x <= y
    smalls = [small.this, exp.Neg(this=small.this)] if isinstance(small, exp.Abs) else [small]
    large_linear = _linear(large, temporal_column)
    if large_linear is None:
        return
    for side in smalls:
        small_linear = _linear(side, temporal_column)
        if small_linear is not None:
            yield _plus(small_linear, _scale(large_linear, Decimal(-1)))


def _conjuncts(select: exp.Select) -> Iterator[exp.Expr]:
    """The conditions every result row of ``select`` satisfies: the WHERE conjuncts and the ON conditions of inner joins."""
    conditions = [select.args["where"].this] if select.args.get("where") else []
    for join in select.args.get("joins") or []:
        if join.args.get("on") is not None and not join.side and join.kind not in ("CROSS", "SEMI", "ANTI"):
            conditions.append(join.args["on"])
    for condition in conditions:
        while isinstance(condition, exp.Paren):
            condition = condition.this
        if isinstance(condition, exp.And):
            yield from condition.flatten()
        else:
            yield condition


def compute_temporal_window(query: str, temporal_column: str, partition_key: str | None = None, partition_key_source_table: str | None = None) -> float | None:
    """The largest time difference between two rows of the partition key's table that the query can combine.

    Every alias pair is bounded from the query's conditions on ``alias.temporal_column`` (``b.ts BETWEEN a.ts AND
    a.ts + 3600``, ``ABS(a.ts - b.ts) <= 3600``, or a lower and an upper bound in separate conditions). A pair counts
    only if its difference is bounded from both sides; ``b.ts <= a.ts + 3600`` alone leaves ``b`` unbounded in the
    past. The window is the longest shortest path over these bounds, so chained windows add up.

    The aliases of the partition key's table are those whose table equals ``partition_key_source_table``; if it is
    not given, the table of the column ``partition_key`` in the SELECT list, else the tables whose time column the
    query references (dimension tables joined to them are ignored).

    Args:
        query: The query.
        temporal_column: Name of the time column the bucketed partition key is derived from.
        partition_key: Name of the partition key column, used to find the partition key's table.
        partition_key_source_table: Table that carries the partition key.

    Returns:
        The window in the unit of ``temporal_column``; 0.0 for a single alias, None if two aliases of the
        partition key's table are not connected by bounded time differences (then no dilation is sound).
    """
    parsed = sqlglot.parse_one(query)
    if not isinstance(parsed, exp.Select):
        return None
    alias_to_table = {table.alias_or_name: table.name for table in _outer_tables(parsed)}
    if partition_key_source_table is None and partition_key is not None:
        for projection in parsed.expressions:
            column = projection.find(exp.Column)
            if column is not None and column.name == partition_key and column.table in alias_to_table:
                partition_key_source_table = alias_to_table[column.table]
                break
    if partition_key_source_table is not None:
        tables = {partition_key_source_table}
    else:
        # The tables with the time column; dimension tables joined to them carry neither it nor the partition key
        tables = {alias_to_table[col.table] for col in parsed.find_all(exp.Column) if col.name == temporal_column and col.table in alias_to_table}
    aliases = {alias for alias, table in alias_to_table.items() if not tables or table in tables}
    if len(aliases) <= 1:
        return 0.0

    # Tightest bounds lower <= t_b - t_a <= upper per alias pair (a < b); every conjunct holds, so the tightest is sound
    lower: dict[tuple[str, str], Decimal] = {}
    upper: dict[tuple[str, str], Decimal] = {}
    for conjunct in _conjuncts(parsed):
        for coefs, constant in _upper_bounds(conjunct, temporal_column):
            # coefs * t + constant <= 0 with t_b - t_a: coefs {b: 1, a: -1} -> d <= -constant, {b: -1, a: 1} -> d >= constant
            if len(coefs) != 2 or sorted(coefs.values()) != [Decimal(-1), Decimal(1)] or not set(coefs) <= set(alias_to_table):
                continue
            a, b = sorted(coefs)
            if coefs[b] == 1:
                upper[(a, b)] = min(upper.get((a, b), -constant), -constant)
            else:
                lower[(a, b)] = max(lower.get((a, b), constant), constant)

    graph = nx.Graph()
    graph.add_nodes_from(aliases)
    for pair in upper.keys() & lower.keys():
        if lower[pair] > upper[pair]:
            continue  # contradictory bounds: the query returns no rows; leave the pair out rather than guess
        graph.add_edge(*pair, weight=float(max(abs(lower[pair]), abs(upper[pair]))))

    if not all(alias in graph and nx.has_path(graph, next(iter(aliases)), alias) for alias in aliases):
        return None
    lengths = dict(nx.all_pairs_dijkstra_path_length(graph, weight="weight"))
    return float(max(lengths[a][b] for a in aliases for b in aliases))


def _outer_tables(select: exp.Select) -> list[exp.Table]:
    """The tables of the outer FROM clause and its joins (not those of subqueries)."""
    sources = [select.args["from_"].this] if select.args.get("from_") else []
    sources += [join.this for join in select.args.get("joins") or []]
    return [source for source in sources if isinstance(source, exp.Table)]
