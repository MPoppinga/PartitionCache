"""
Extend a given SQL query with the cache functionality.

This module provides a function `apply_cache` that takes a SQL query, a cache handler, and a partition identifier as input.
It utilizes the given cache to extend the SQL query with the set of possible partition keys

"""

import functools
import math
import numbers
import random
import secrets
from collections.abc import Callable, Collection, Iterable
from datetime import datetime
from decimal import Decimal
from logging import getLogger
from typing import Any, Literal, ParamSpec

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp

from partitioncache.cache_handler.abstract import LAZY_ALL_KEYS_PLACEHOLDER, AbstractCacheHandler, AbstractCacheHandler_Lazy
from partitioncache.query_processor import (
    compute_buffer_distance,
    detect_partition_join_from_query,
    generate_all_hashes,
    handle_deprecated_kwargs,
    normalize_regex_operators,
)
from partitioncache.temporal_dilation import compute_dilation_buckets, compute_temporal_window, dilate_bucket_set

logger = getLogger("PartitionCache")



def _resolve_dilation(
    query: str,
    temporal_bucket_width: float | None,
    dilation_buckets: int | None,
    temporal_column: str | None = None,
    partition_key: str | None = None,
    partition_key_source_table: str | None = None,
) -> int | None:
    """Resolve how many buckets the cached sets must be dilated by.

    Mirrors how the spatial path resolves ``buffer_distance``: an explicit value wins, and otherwise the window
    is read from the query's conditions on ``temporal_column`` (see :func:`compute_temporal_window`).

    Returns 0 when temporal dilation is not requested or the query combines a single row of the partition key's
    table, and None when two of its aliases are not bounded in time against each other: then no dilation is
    sound and the cache must not be used for the query.
    """
    if dilation_buckets is not None:
        if dilation_buckets < 0:
            raise ValueError(f"dilation_buckets must be non-negative, got {dilation_buckets}")
        return dilation_buckets
    if temporal_bucket_width is None:
        return 0
    if temporal_column is None:
        raise ValueError("temporal_bucket_width requires temporal_column, the time column the partition key buckets")
    window = compute_temporal_window(query, temporal_column, partition_key, partition_key_source_table)
    if window is None:
        return None
    return compute_dilation_buckets(window, temporal_bucket_width)


def _has_single_from_item(query: str) -> bool:
    """Whether the outermost FROM clause holds exactly one table (no join, no derived table, no CTE)."""
    try:
        parsed = sqlglot.parse_one(query)
    except sqlglot.errors.ParseError:
        return False
    if parsed.find(exp.With) is not None:  # the fragments are generated from the CTE bodies, which may join tables
        return False
    if not isinstance(parsed, exp.Select):  # a set operation: its other branches may join tables
        return False
    if parsed.args.get("from_") is None:
        return False
    return not parsed.args.get("joins") and isinstance(parsed.args["from_"].this, exp.Table)


def _resolve_buffer_distance(query: str, buffer_distance: float | None) -> float:
    """The distance the cached geometries are widened by before they restrict the query.

    An explicit value wins. A query over a single table needs no widening: every cached fragment holds that
    table's own geometries, so each row the query returns lies in every cached set. A radius around a constant
    point, as in ``ST_DWithin(p1.geom, ST_MakePoint(...), r)``, must therefore not widen the filter by r.
    Otherwise it is the diameter of the query's distance graph (:func:`compute_buffer_distance`).
    """
    if buffer_distance is not None:
        return buffer_distance
    if _has_single_from_item(query):
        return 0.0
    buffer_distance = compute_buffer_distance(query)
    if buffer_distance == 0.0:
        raise ValueError("buffer_distance is required when geometry_column is set and query has no distance constraints")
    return buffer_distance


def _is_temporal(temporal_bucket_width: float | None, dilation_buckets: int | None) -> bool:
    """Whether the partition key is a time bucket; its variants are then generated without partition key joins.

    An equality ``t1.bucket = t2.bucket`` that the query does not contain would drop pairs across a bucket
    boundary, which no dilation can bring back. Population must use the same setting (``skip_partition_key_joins``),
    or the variant hashes differ.
    """
    return temporal_bucket_width is not None or dilation_buckets is not None


def _temporal_lookup(
    query: str,
    cache_handler: AbstractCacheHandler,
    partition_key: str,
    temporal_bucket_width: float | None,
    dilation_buckets: int | None,
    temporal_column: str | None,
    partition_key_source_table: str | None,
    lazy: bool,
) -> tuple[int, str | None]:
    """The dilation for a lookup, and why it must run without the cache (None if it may use it).

    Reasons, also reported as ``uncached_reason`` in the statistics of apply_cache and apply_cache_lazy:
    ``temporal_unbounded`` (rows of the partition key's table without a time bound to each other),
    ``no_dilated_lazy`` (a lazy lookup on a handler without a dilated intersection) and ``non_integer_key``
    (buckets of a non-integer partition key cannot be dilated). In each case the cache would return too few
    partition keys.
    """
    if lazy and not isinstance(cache_handler, AbstractCacheHandler_Lazy):
        raise ValueError("Cache handler does not support lazy intersection")  # a usage error, not a reason to run uncached
    dilation = _resolve_dilation(query, temporal_bucket_width, dilation_buckets, temporal_column, partition_key, partition_key_source_table)
    if dilation is None:
        logger.warning(f"Rows of the query are not bounded in time against each other on '{temporal_column}'; the query runs without the cache")
        return 0, "temporal_unbounded"
    if dilation == 0:
        return 0, None
    if lazy and getattr(cache_handler, "get_intersected_dilated_lazy", None) is None:
        # The Python fallback materialises the sets and cannot produce a subquery
        logger.warning(f"{type(cache_handler).__name__} has no lazy dilated intersection; the query runs without the cache (the non-lazy API supports dilation)")
        return dilation, "no_dilated_lazy"
    datatype = cache_handler.get_datatype(partition_key)
    if datatype is not None and datatype != "integer":
        logger.warning(f"Temporal dilation needs an integer partition key; '{partition_key}' is {datatype}, the query runs without the cache")
        return dilation, "non_integer_key"
    return dilation, None


def _uncached_stats(reason: str, gate_stats: dict[str, int]) -> dict[str, Any]:
    return {"generated_variants": 0, "cache_hits": 0, "enhanced": 0, "p0_rewritten": 0, "uncached_reason": reason, **gate_stats}


def _intersect_dilated_fallback(
    cache_handler: AbstractCacheHandler, hashes: set[str], partition_key: str, dilation: int
) -> tuple[set[int] | None, int]:
    """Generic dilate-then-intersect for handlers without a native dilated intersection.

    Fetches each cached set individually, dilates it, and intersects in Python. Slower than a
    SQL-side implementation but available on every backend. The per-set order is what makes it
    sound: dilating the intersection instead would be unsound, since the undilated
    intersection may already be empty.
    """
    if cache_handler.get_datatype(partition_key) != "integer":
        logger.warning(f"Temporal dilation needs an integer partition key; '{partition_key}' is not, the query runs without the cache")
        return None, 0
    existing = cache_handler.filter_existing_keys(hashes, partition_key)
    if not existing:
        return None, 0

    intersection: set[int] | None = None
    for key in existing:
        values = cache_handler.get(key, partition_key)
        if values is None:
            continue
        dilated = dilate_bucket_set({int(v) for v in values}, dilation)  # type: ignore[arg-type]
        intersection = dilated if intersection is None else intersection & dilated
    return intersection, len(existing)


def _first_branch_query(parsed: exp.Expr) -> str | None:
    """The first SELECT of a set operation as a query of its own, with the statement's WITH clause; None if there is none."""
    select = _restricted_select(parsed)
    if select is None:
        return None
    branch = select.copy()
    with_ = parsed.args.get("with_")
    if with_ is not None and branch.args.get("with_") is None:
        branch.set("with_", with_.copy())
    return branch.sql()


def _variants_of_first_branch(query: str, hashes: list[str], generate: Callable[[str], list[str]]) -> bool:
    """Whether the variants of a set operation are variants of its first SELECT, the one the cache restricts; True for other queries.

    Variant generation reads a set operation's tables from its first FROM and its conditions from the first WHERE in
    breadth-first order, which is another branch's when the first branch is parenthesised or the operation chains three
    branches. Such variants are no relaxations of the first branch, and their keys must not restrict it: the query then
    runs without the cache. ``generate`` returns the hashes of a query with the lookup's settings.
    """
    try:
        parsed = sqlglot.parse_one(query)
    except sqlglot.errors.ParseError:
        return True  # the variant generation parsed it; a parse error here leaves the decision to it
    if not isinstance(parsed, exp.SetOperation) or not hashes:
        return True
    branch = _first_branch_query(parsed)
    if branch is not None and set(hashes) <= set(generate(branch)):
        return True
    logger.warning("The variants of the set operation are not all variants of its first SELECT, which the cache restricts; the query runs without the cache")
    return False


def get_partition_keys(
    query: str,
    cache_handler: AbstractCacheHandler,
    partition_key: str,
    min_component_size=2,
    canonicalize_queries=False,
    follow_graph: bool = True,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
    bucket_steps: float | dict[str, float] | list = 1.0,
    add_constraints: dict[str, str] | None = None,
    remove_constraints_all: list[str] | None = None,
    remove_constraints_add: list[str] | None = None,
    skip_partition_key_joins: bool = False,
    geometry_column: str | None = None,
    max_conditions_removed: int | None = None,
    protected_patterns: list[str] | None = None,
    temporal_bucket_width: float | None = None,
    dilation_buckets: int | None = None,
    temporal_column: str | None = None,
    partition_key_source_table: str | None = None,
    keep_all_attributes: bool = False,
    **kwargs: Any,
) -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int, int]:
    """
    Using the partition cache to get the partition keys for a given query.

    Args:
        query (str): The SQL query to be checked in the cache.
        cache_handler (AbstractCacheHandler): The cache handler object.
        partition_key: The identifier for the partition.
        min_component_size: Minimum size of query components to consider.
        canonicalize_queries: Whether to canonicalize queries before hashing.
        auto_detect_partition_join: Whether to auto-detect partition-join tables.
        partition_join_table: Explicitly specified partition-join table alias or name.
        bucket_steps: Step size for normalizing distance conditions (e.g., 1.0, 0.5, etc.)
        add_constraints: Dict mapping table names to constraints to add (e.g., {"table": "col = val"})
        remove_constraints_all: List of attribute names to remove from all query variants
        remove_constraints_add: List of attribute names to remove, creating additional variants
        protected_patterns: Conditions matching these patterns are never removed during variant generation.
        temporal_bucket_width: Width of one bucket of a time-bucket partition key, in the unit of ``temporal_column``;
            enables temporal dilation: each cached set is widened by ceil(window / width) buckets before the
            intersection, with the window read from the query's conditions on ``temporal_column``.
        dilation_buckets: Number of buckets to widen by, instead of the window (0 turns dilation off).
        temporal_column: The time column the partition key buckets; required with ``temporal_bucket_width``.
            With either temporal parameter, variants are generated without partition key joins, and a query
            whose rows are not bounded in time against each other runs without the cache.
        keep_all_attributes: If False (default), lookup also probes variants with single-table attribute
            conditions removed (more hits, more variant-generation cost). True probes exactly the variants
            that generate_all_query_hash_pairs produces with its defaults.

    Returns:
       tuple containing:
       - set[int] | set[str] | set[float] | set[datetime] | None: The set of partition keys.
       - int: Total number of query variant hashes generated
       - int: Number of cache hits
    """
    # Handle deprecated star_join_* parameter names
    deprecated = handle_deprecated_kwargs(kwargs, "get_partition_keys")
    if "partition_join_table" in deprecated:
        if partition_join_table is not None:
            raise TypeError("Cannot pass both 'partition_join_table' and deprecated 'star_join_table'")
        partition_join_table = deprecated["partition_join_table"]
    if "auto_detect_partition_join" in deprecated:
        auto_detect_partition_join = deprecated["auto_detect_partition_join"]

    dilation, uncached_reason = _temporal_lookup(
        query, cache_handler, partition_key, temporal_bucket_width, dilation_buckets, temporal_column, partition_key_source_table, lazy=False
    )
    if uncached_reason is not None:
        return None, 0, 0
    if _is_temporal(temporal_bucket_width, dilation_buckets):
        skip_partition_key_joins = True

    # Generate all hashes for the given query (Only consider subqueries with two ore more components that are connected, allow modifying attributes)
    generation: dict[str, Any] = {
        "partition_key_source_table": partition_key_source_table,
        "partition_key": partition_key,
        "min_component_size": min_component_size,
        "follow_graph": follow_graph,
        "fix_attributes": keep_all_attributes,
        "canonicalize_queries": canonicalize_queries,
        "auto_detect_partition_join": auto_detect_partition_join,
        "partition_join_table": partition_join_table,
        "bucket_steps": bucket_steps,
        "add_constraints": add_constraints,
        "remove_constraints_all": remove_constraints_all,
        "remove_constraints_add": remove_constraints_add,
        "skip_partition_key_joins": skip_partition_key_joins,
        "geometry_column": geometry_column,
        "max_conditions_removed": max_conditions_removed,
        "protected_patterns": protected_patterns,
    }
    cache_entry_hashes = generate_all_hashes(query=query, **generation)
    if not _variants_of_first_branch(query, cache_entry_hashes, lambda branch: generate_all_hashes(query=branch, **generation)):
        return None, 0, 0

    logger.info(f"Found {len(cache_entry_hashes)} subqueries in query")

    # Get the partition keys from the cache based on the hashes
    if dilation > 0:
        # Temporal dilation: each cached bucket set is widened before intersection so that a
        # correlated pair spanning two adjacent buckets is not silently dropped.
        dilated_intersect = getattr(cache_handler, "get_intersected_dilated", None)
        if dilated_intersect is not None:
            partition_keys, count = dilated_intersect(set(cache_entry_hashes), partition_key, dilation)
        else:
            partition_keys, count = _intersect_dilated_fallback(cache_handler, set(cache_entry_hashes), partition_key, dilation)
    else:
        partition_keys, count = cache_handler.get_intersected(set(cache_entry_hashes), partition_key=partition_key)

    logger.info(f"Extended query with {count} hashes")
    return partition_keys, len(cache_entry_hashes), count


def get_partition_keys_lazy(
    query: str,
    cache_handler: AbstractCacheHandler,
    partition_key: str,
    min_component_size=2,
    canonicalize_queries=False,
    follow_graph: bool = True,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
    bucket_steps: float | dict[str, float] | list = 1.0,
    add_constraints: dict[str, str] | None = None,
    remove_constraints_all: list[str] | None = None,
    remove_constraints_add: list[str] | None = None,
    skip_partition_key_joins: bool = False,
    geometry_column: str | None = None,
    max_conditions_removed: int | None = None,
    protected_patterns: list[str] | None = None,
    temporal_bucket_width: float | None = None,
    dilation_buckets: int | None = None,
    temporal_column: str | None = None,
    partition_key_source_table: str | None = None,
    keep_all_attributes: bool = False,
    **kwargs: Any,
) -> tuple[str | None, int, int]:
    """
    Gets the lazy intersection representation of the partition keys for the given query.

    Args:
        query (str): The SQL query to be checked in the cache.
        cache_handler (AbstractCacheHandler): The cache handler object that supports lazy intersection.
        partition_key (str): The identifier for the partition.
        min_component_size (int): Minimum size of query components to consider for cache lookup.
        canonicalize_queries (bool): Whether to canonicalize queries before hashing.
        follow_graph (bool): Whether to follow the query graph for generating variants.
        auto_detect_partition_join: Whether to auto-detect partition-join tables.
        partition_join_table: Explicitly specified partition-join table alias or name.
        bucket_steps: Step size for normalizing distance conditions (e.g., 1.0, 0.5, etc.)
        add_constraints: Dict mapping table names to constraints to add (e.g., {"table": "col = val"})
        remove_constraints_all: List of attribute names to remove from all query variants
        remove_constraints_add: List of attribute names to remove, creating additional variants
        protected_patterns: Conditions matching these patterns are never removed during variant generation.
        temporal_bucket_width: Width of one bucket of a time-bucket partition key, in the unit of ``temporal_column``;
            enables temporal dilation: each cached set is widened by ceil(window / width) buckets before the
            intersection, with the window read from the query's conditions on ``temporal_column``.
        dilation_buckets: Number of buckets to widen by, instead of the window (0 turns dilation off).
        temporal_column: The time column the partition key buckets; required with ``temporal_bucket_width``.
            With either temporal parameter, variants are generated without partition key joins, and a query
            whose rows are not bounded in time against each other runs without the cache.
        keep_all_attributes: If False (default), lookup also probes variants with single-table attribute
            conditions removed (more hits, more variant-generation cost). True probes exactly the variants
            that generate_all_query_hash_pairs produces with its defaults.

    Returns:
        tuple[str, int, int]: A tuple containing:
            - Lazy SQL subquery string (empty if no cache hits)
            - Total number of query variant hashes generated
            - Number of cache hits (hashes found in cache)

    Raises:
        ValueError: If cache handler does not support lazy intersection.
    """
    # Handle deprecated star_join_* parameter names
    deprecated = handle_deprecated_kwargs(kwargs, "get_partition_keys_lazy")
    if "partition_join_table" in deprecated:
        if partition_join_table is not None:
            raise TypeError("Cannot pass both 'partition_join_table' and deprecated 'star_join_table'")
        partition_join_table = deprecated["partition_join_table"]
    if "auto_detect_partition_join" in deprecated:
        auto_detect_partition_join = deprecated["auto_detect_partition_join"]

    if not isinstance(cache_handler, AbstractCacheHandler_Lazy):
        raise ValueError("Cache handler does not support lazy intersection")

    dilation, uncached_reason = _temporal_lookup(
        query, cache_handler, partition_key, temporal_bucket_width, dilation_buckets, temporal_column, partition_key_source_table, lazy=True
    )
    if uncached_reason is not None:
        return None, 0, 0
    if _is_temporal(temporal_bucket_width, dilation_buckets):
        skip_partition_key_joins = True

    generation: dict[str, Any] = {
        "partition_key_source_table": partition_key_source_table,
        "partition_key": partition_key,
        "min_component_size": min_component_size,
        "fix_attributes": keep_all_attributes,
        "canonicalize_queries": canonicalize_queries,
        "follow_graph": follow_graph,
        "auto_detect_partition_join": auto_detect_partition_join,
        "partition_join_table": partition_join_table,
        "bucket_steps": bucket_steps,
        "add_constraints": add_constraints,
        "remove_constraints_all": remove_constraints_all,
        "remove_constraints_add": remove_constraints_add,
        "skip_partition_key_joins": skip_partition_key_joins,
        "geometry_column": geometry_column,
        "max_conditions_removed": max_conditions_removed,
        "protected_patterns": protected_patterns,
    }
    hashes = generate_all_hashes(query=query, **generation)
    if not _variants_of_first_branch(query, hashes, lambda branch: generate_all_hashes(query=branch, **generation)):
        return None, 0, 0

    if dilation > 0:
        lazy_cache_subquery, used_hashes = cache_handler.get_intersected_dilated_lazy(set(hashes), partition_key, dilation)  # type: ignore[attr-defined]
    else:
        lazy_cache_subquery, used_hashes = cache_handler.get_intersected_lazy(set(hashes), partition_key=partition_key)

    return lazy_cache_subquery, len(hashes), used_hashes


def find_p0_alias(query: str, partition_key: str, auto_detect_partition_join: bool = True, partition_join_table: str | None = None) -> str:
    """
    Find the appropriate table alias for cache restrictions.

    Uses the query processor's partition-join detection logic.
    If no partition-join table is detected, falls back to the first table in the query,
    unless the partition key column is qualified only with other aliases; then the first
    such alias (SELECT list first) is used.

    Args:
        query: SQL query to analyze
        partition_key: The partition key column name
        auto_detect_partition_join: Whether to auto-detect partition-join tables
        partition_join_table: Explicitly specified partition-join table alias or name

    Returns:
        The alias to use for cache restrictions
    """
    # First check if query has tables at all
    parsed_query = sqlglot.parse_one(query)
    first_table = parsed_query.find(exp.Table)
    if first_table is None:
        raise ValueError("No table found in query")

    # Use query processor's partition-join detection only if query has FROM clause
    try:
        partition_join_alias = detect_partition_join_from_query(
            query=query,
            partition_key=partition_key,
            auto_detect_partition_join=auto_detect_partition_join,
            partition_join_table=partition_join_table,
        )

        if partition_join_alias:
            return partition_join_alias
    except (IndexError, ValueError):
        # If partition-join detection fails (e.g., no FROM clause), fall through to default
        pass

    # Fallback to the first table, unless only other tables carry the partition key: generated
    # variants without partition_key_source_table can list a dimension before the fact table
    first_alias = first_table.alias_or_name
    select = parsed_query if isinstance(parsed_query, exp.Select) else parsed_query.find(exp.Select)
    if select is not None:
        key_columns = [*(col for e in select.expressions for col in e.find_all(exp.Column)), *select.find_all(exp.Column)]
        key_aliases: list[str] = [col.table for col in key_columns if col.name == partition_key and col.table and col.parent_select is select]
        if key_aliases and first_alias not in key_aliases:
            return key_aliases[0]
    return first_alias


def rewrite_query_with_p0_table( # TODO: Check duplication with query_processor
    query: str,
    partition_key: str,
    mv_table_name: str | None = None,
    p0_alias: str = "p0",
    sql_dialect: str = "postgres",
) -> str:
    """
    Rewrite a query to use a star-schema pattern with a p0 table for better optimizer decisions.

    Transforms multi-table queries to use a central p0 table, converting:
    SELECT * FROM tt AS t1, tt AS t2 WHERE t1.partition_key = t2.partition_key

    Into:
    SELECT * FROM tt AS t1, tt AS t2, partition_key_mv AS p0
    WHERE t1.partition_key = p0.partition_key AND t2.partition_key = p0.partition_key

    Args:
        query (str): The original SQL query to rewrite.
        partition_key (str): The identifier for the partition.
        mv_table_name (str | None): Name of the materialized view table. Defaults to {partition_key}_mv.
        p0_alias (str): Alias name for the p0 table. Defaults to "p0".
        sql_dialect (str): sqlglot dialect used to parse and render the query. Defaults to PostgreSQL.

    Returns:
        str: The rewritten query with star-schema p0 table integration.
    """
    if mv_table_name is None:
        mv_table_name = f"{partition_key}_mv"

    parsed_query = sqlglot.parse_one(query, read=sql_dialect)

    # Check if p0 table already exists
    existing_tables = [table.name for table in parsed_query.find_all(exp.Table)]
    if mv_table_name in existing_tables:
        return query

    # Find all table aliases that have partition_key joins
    table_aliases_with_partition_key = []
    for table in parsed_query.find_all(exp.Table):
        alias = table.alias if table.alias else table.name
        table_aliases_with_partition_key.append(alias)

    if not table_aliases_with_partition_key:
        return query

    # Add p0 table to FROM clause
    from_clause = parsed_query.find(exp.From)
    if from_clause is None:
        return query

    # Convert to comma-separated tables format including p0
    current_table = from_clause.this
    from_clause.set("this", exp.Table(this=f"{current_table.sql(dialect=sql_dialect)}, {mv_table_name} AS {p0_alias}"))

    # Remove existing partition_key join conditions and replace with star-schema joins
    where_clause = _outer_where(parsed_query)
    if where_clause is None:
        # Without a WHERE the star joins were dropped and p0 was cross-joined with every row
        where_clause = exp.Where()
        parsed_query.set("where", where_clause)
    if where_clause:
        # Collect all conditions
        conditions = []

        def collect_conditions(node):
            if isinstance(node, exp.And):
                collect_conditions(node.left)
                collect_conditions(node.right)
            elif node is not None:
                conditions.append(node)

        collect_conditions(where_clause.this)

        # Filter out partition_key join conditions and add star-schema joins
        new_conditions = []
        for condition in conditions:
            condition_sql = condition.sql()
            # Skip existing partition_key joins between tables
            is_partition_join = False
            for alias1 in table_aliases_with_partition_key:
                for alias2 in table_aliases_with_partition_key:
                    if alias1 != alias2:
                        if f"{alias1}.{partition_key} = {alias2}.{partition_key}" in condition_sql:
                            is_partition_join = True
                            break
                if is_partition_join:
                    break

            if not is_partition_join:
                new_conditions.append(condition)

        # Add star-schema joins through p0
        for alias in table_aliases_with_partition_key:
            p0_join = exp.EQ(this=exp.Identifier(this=f"{alias}.{partition_key}"), expression=exp.Identifier(this=f"{p0_alias}.{partition_key}"))
            new_conditions.append(p0_join)

        # Rebuild WHERE clause with simpler structure
        if new_conditions:
            new_where = new_conditions[0]
            for condition in new_conditions[1:]:
                new_where = exp.And(this=new_where, expression=condition)
            where_clause.set("this", new_where)

    return parsed_query.sql(dialect=sql_dialect)


def _format_partition_key_for_sql(pk: int | str | float | datetime, sql_dialect: str = "postgres") -> str:
    """Format a single partition key as a literal for a key table of ``_get_partition_key_sql_type``'s type."""
    return _format_spliced_partition_key(pk, sql_dialect)


# Methods whose keys are typed by a VALUES list or a key table, not by the partition key column
_TYPED_KEY_METHODS = ("VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_IN", "UNLOGGED_TABLE_JOIN")
# ... of which a VALUES list cannot take the column's type; the key tables of non-integer keys do (_typed_key_table_setup)
FLOAT_KEY_REFUSING_METHODS = ("VALUES",)


def _refuse_float_keys(partition_keys: set[int] | set[str] | set[float] | set[datetime], method: str) -> None:
    """Raise ValueError for float keys with a method whose key list is not typed by the partition key column.

    A float key read back from a ``real`` (float4) column is the double nearest to its text (0.1); a VALUES list of
    such keys is double precision, and ``real = double precision`` compares 0.100000001490116 with 0.1, so every key
    would be missed without an error. The literals of ``IN`` and the key tables of non-integer keys
    (``_typed_key_table_setup``) take the column's type instead.
    """
    if method in FLOAT_KEY_REFUSING_METHODS and any(isinstance(k, float) for k in partition_keys):
        raise ValueError(
            f"{method} cannot restrict float partition keys exactly (a real column would silently match none); use method IN"
        )


def _get_partition_key_sql_type(partition_keys: set[int] | set[str] | set[float] | set[datetime], sql_dialect: str = "postgres") -> str:
    """The column type of a key table for ``partition_keys``: integer, numeric, timestamp, or text.

    Decimal and timestamp keys need their own type: a TEXT key column does not compare with a numeric or timestamp
    partition key column (PostgreSQL has no ``text = numeric`` operator). Float keys are refused before
    (``_refuse_float_keys``).
    """
    if not partition_keys:
        return "TEXT"
    if all(isinstance(k, int) for k in partition_keys):
        # Use BIGINT if any value exceeds 32-bit integer range (e.g. H3 cell IDs)
        max_int32 = 2**31 - 1
        if any(abs(k) > max_int32 for k in partition_keys):  # type: ignore[arg-type]
            return "BIGINT"
        return "INT"
    if all(isinstance(k, int | Decimal) for k in partition_keys):
        return "NUMERIC"
    if all(isinstance(k, datetime) for k in partition_keys):
        if sql_dialect != "mysql" and any(k.tzinfo is not None for k in partition_keys):  # type: ignore[union-attr]
            return "TIMESTAMPTZ"
        return "TIMESTAMP"
    return "TEXT"


def _key_list_sentinel() -> str:
    """Return a unique identifier that stands in for a partition-key list during the sqlglot rewrite.

    The token is a plain lowercase identifier, so sqlglot renders it verbatim (no quoting, no
    rewriting) in every dialect, and the random suffix makes a collision with the user's query
    text practically impossible. `_splice_key_list` still asserts that it occurs exactly once.
    """
    return f"__pcache_keys_{secrets.token_hex(8)}__"


# Dialects with typed timestamp literals; SQLite stores timestamps as text
_TYPED_LITERAL_DIALECTS = ("postgres", "duckdb")


def _format_spliced_partition_key(pk: int | str | float | datetime, sql_dialect: str = "postgres") -> str:
    """Format one key as a literal for a spliced (unparsed) key list or a key table.

    Integers and finite decimals (psycopg's type for numeric columns) as numbers. Strings as string literals rendered
    by sqlglot for ``sql_dialect``, so quotes and backslashes are escaped the way that dialect expects (MySQL treats a
    backslash as an escape character); the former parse-based path rejected such keys with a sqlglot TokenError.
    Timestamps as ``CAST('...' AS TIMESTAMP|TIMESTAMPTZ)`` in PostgreSQL and DuckDB: as bare string literals they made
    ``IN (VALUES ...)`` a text column, which neither compares with a timestamp column. Float keys stay string
    literals, which ``IN`` converts to the column's type exactly (VALUES and the key tables refuse them,
    ``_refuse_float_keys``); SQLite writes infinity as 9e999, since the string 'inf' matches no REAL value.
    """
    if isinstance(pk, int):
        return str(pk)
    if isinstance(pk, Decimal) and pk.is_finite():
        return str(pk)
    if isinstance(pk, float) and sql_dialect == "sqlite" and math.isinf(pk):
        return "9e999" if pk > 0 else "-9e999"
    if isinstance(pk, datetime) and sql_dialect in _TYPED_LITERAL_DIALECTS:
        literal = exp.Literal.string(pk.isoformat(sep=" ")).sql(dialect=sql_dialect)
        return f"CAST({literal} AS {'TIMESTAMPTZ' if pk.tzinfo is not None else 'TIMESTAMP'})"
    return exp.Literal.string(str(pk)).sql(dialect=sql_dialect)


def _splice_key_list(rendered_sql: str, sentinel: str, key_list: str) -> str:
    """Replace the single occurrence of `sentinel` in `rendered_sql` with the formatted key list.

    Formerly the key list itself was parsed with sqlglot (`parse_one(f"... IN ({keys})")`), which
    is linear in the number of keys but with a large constant: a 100k-key list took seconds. The
    list is now formatted by `_format_partition_key_for_sql` and joined with the separators
    sqlglot would have produced (", " inside IN, "), (" inside VALUES), so the output is
    byte-identical to the parsed form.
    """
    occurrences = rendered_sql.count(sentinel)
    if occurrences != 1:
        raise AssertionError(f"partition-key placeholder {sentinel!r} occurs {occurrences} times in the rendered query, expected exactly once")
    return rendered_sql.replace(sentinel, key_list, 1)


def _restricted_select(parsed_query: exp.Expr) -> exp.Select | None:
    """Return the SELECT the cache restricts: the query itself, or the first branch of a set operation.

    The variants are generated from the first SELECT of a set operation, so their keys restrict that branch only;
    a restriction in another branch drops rows of a UNION and adds rows to an EXCEPT. Parentheses around a branch
    are looked through. None when the first branch is not a SELECT (VALUES, for one).
    """
    node = parsed_query
    while not isinstance(node, exp.Select):
        if isinstance(node, exp.SetOperation | exp.Subquery | exp.Paren):
            node = node.this
        else:
            return None
    return node


def _outer_where(parsed_query: exp.Expr) -> exp.Where | None:
    """Return the WHERE clause of the restricted SELECT (``_restricted_select``), not one of a FILTER, subquery or CTE.

    ``find(exp.Where)`` returns the first WHERE anywhere in the tree, so a query without an outer WHERE got the
    condition in a nested one, and a set operation whose first branch has no WHERE got it in another branch.
    """
    select = _restricted_select(parsed_query)
    return select.args.get("where") if select is not None else None


def _add_where_condition(parsed_query: exp.Expr, condition_expr: exp.Expr) -> None:
    """Add a WHERE condition to the restricted SELECT of a parsed query, creating its WHERE clause if needed.

    Raises:
        ValueError: When the query has no restricted SELECT (``_restricted_select``).
    """
    select = _restricted_select(parsed_query)
    if select is None:
        raise ValueError(f"The query has no SELECT to restrict (first branch: {type(parsed_query).__name__}); run it without the cache")
    existing_where: exp.Where | None = select.args.get("where")
    if existing_where is None:
        select.set("where", exp.Where(this=condition_expr))
    else:
        existing_where.args["this"] = exp.and_(existing_where.args["this"], condition_expr)


def _from_clauses(parsed_query: exp.Expr) -> list[exp.Join | exp.From]:
    """The top-level JOIN clauses and the FROM clause of the restricted SELECT (``_restricted_select``).

    These are the tables the JOIN methods may join the key table to: a table of a subquery, a CTE or another branch of a
    set operation is not restricted by the cached keys, even when it reuses the p0 alias.
    """
    select = _restricted_select(parsed_query)
    if select is None:
        return []
    from_ = select.args.get("from_")
    return [*(select.args.get("joins") or []), *([from_] if from_ is not None else [])]


EMPTY_INTERSECTION_COMMENT = "PartitionCache: cache intersection is empty, query cannot return rows"


def _add_empty_intersection_condition(query: str, sql_dialect: str = "postgres") -> str:
    """Restrict a query to no rows because the cache proved that no partition can match.

    Every cache entry is a superset of the partitions that can contribute to the query, so an empty
    intersection of valid entries means the query has no result. The empty set itself cannot be
    expressed with the regular methods (``IN ()`` is invalid SQL, ``VALUES`` needs at least one row,
    and an empty temporary table has no known key type), so a commented ``FALSE`` condition is added
    to the WHERE of the restricted SELECT (the first branch of a set operation) instead. The query shape
    is kept, so aggregates still return their empty-input result (e.g. ``COUNT(*) = 0``).
    """
    parsed_query = sqlglot.parse_one(query, read=sql_dialect)
    select = _restricted_select(parsed_query)
    if select is None:
        logger.warning("The query has no SELECT to restrict to no rows; it runs without the cache")
        return query
    condition = exp.false()
    condition.add_comments([EMPTY_INTERSECTION_COMMENT])
    select.where(condition, copy=False)
    return parsed_query.sql(dialect=sql_dialect)


def _temp_table_ddl(table_name: str, sql_dialect: str) -> tuple[str, str]:
    """Return (prefix, suffix) for a CREATE TEMPORARY TABLE in the given dialect.

    PostgreSQL supports ON COMMIT DROP, which scopes the temp table to the surrounding
    transaction; the emitted SQL is unchanged from before this helper existed, so the
    single-transaction requirement documented in docs/api_reference.md still holds.

    DuckDB rejects the clause outright ("Only ON COMMIT PRESERVE ROWS is supported"), which
    made every TMP_TABLE_* method fail there. For such dialects the table is session-scoped
    instead, so a DROP TABLE IF EXISTS is emitted first to keep a long session from
    accumulating stale tables.
    """
    if sql_dialect == "postgres":
        return "", " ON COMMIT DROP"
    return f"DROP TABLE IF EXISTS {table_name};\n", ""


def _create_tmp_table_setup(
    partition_keys: set[int] | set[str] | set[float] | set[datetime], analyze_tmp_table: bool, sql_dialect: str = "postgres", key_filters: list[str] | None = None
) -> tuple[str, str]:
    """Create the SQL for temporary table setup.

    ``key_filters`` are folded subqueries (``_fold_key_subqueries``); the table then holds exactly the keys the folded
    conditions keep. PostgreSQL fills it with ``_folded_fill_sql``. The other dialects delete, after the INSERT, every
    key for which ``key IN (<subquery>)`` is not TRUE: in PostgreSQL that condition is a subplan that is hashed only
    when the estimated subquery fits in hash_mem, and otherwise scanned once per key. SQLite compares the IN with the
    key column's affinity, which a set operation would not.

    Returns:
        tuple[str, str]: (setup_sql, table_name) where table_name is a random temporary table name.
    """
    table_name = f"tmp_cache_keys_{random.randint(100000, 999999)}"
    partition_keys_str = "),(".join(_format_partition_key_for_sql(pk, sql_dialect) for pk in partition_keys)
    partition_key_type = _get_partition_key_sql_type(partition_keys, sql_dialect)

    # ON COMMIT DROP: the temp table is dropped at the end of the surrounding transaction instead of
    # persisting for the whole session. The full setup+query script must therefore run in ONE transaction
    # (see execute_with_temp_tables / PostgresDBHandler.execute and docs/api_reference.md).
    ddl_prefix, ddl_suffix = _temp_table_ddl(table_name, sql_dialect)
    if key_filters and sql_dialect == "postgres":
        setup_sql = f"CREATE TEMPORARY TABLE {table_name} (partition_key {partition_key_type} PRIMARY KEY){ddl_suffix};\n"
        setup_sql += _folded_fill_sql(table_name, "partition_key", key_filters, partition_keys=partition_keys, sql_dialect=sql_dialect)
    else:
        setup_sql = f"""{ddl_prefix}CREATE TEMPORARY TABLE {table_name} (partition_key {partition_key_type} PRIMARY KEY){ddl_suffix};
                    INSERT INTO {table_name} (partition_key) VALUES ({partition_keys_str});
                    """
        for key_filter in key_filters or ():
            setup_sql += f"DELETE FROM {table_name} WHERE (partition_key IN ({key_filter})) IS NOT TRUE;\n"

    if analyze_tmp_table:
        # PRIMARY KEY above already creates a B-tree index; ANALYZE updates statistics for the planner
        setup_sql += f"ANALYZE {table_name};"

    return setup_sql, table_name


UNLOGGED_TABLE_PREFIX = "partitioncache_unl_"
# The key table is joined to the p0 table (as the IN methods restrict it), or with join_every_table to every table
_TABLE_JOIN_METHODS = ("TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN")
# Methods whose rewrite is a script (key table setup, then the SELECT); only these take planner_collapse_limit
_SCRIPT_METHODS = ("TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN")
# The planner settings of planner_collapse_limit, emitted in this order as the first statements of the script
COLLAPSE_LIMIT_SETTINGS = ("join_collapse_limit", "from_collapse_limit")


def _check_planner_collapse_limit(planner_collapse_limit: int | None, method: str, sql_dialect: str) -> None:
    """Raise ValueError unless ``planner_collapse_limit`` is None or a positive int usable with ``method`` and ``sql_dialect``.

    The limit is set with ``SET LOCAL`` statements in front of the script, so it needs a method that emits a
    script (``_SCRIPT_METHODS``) and PostgreSQL. The single-statement methods (``IN``, ``VALUES``, ``IN_SUBQUERY``)
    raise instead of silently ignoring the option.
    """
    if planner_collapse_limit is None:
        return
    if isinstance(planner_collapse_limit, bool) or not isinstance(planner_collapse_limit, int) or planner_collapse_limit < 1:
        raise ValueError(f"planner_collapse_limit must be a positive integer or None, got {planner_collapse_limit!r}")
    if method not in _SCRIPT_METHODS:
        raise ValueError(f"planner_collapse_limit needs a method that emits a script {_SCRIPT_METHODS}, not {method!r}")
    if sql_dialect != "postgres":
        raise ValueError(f"planner_collapse_limit is only supported for PostgreSQL, not for SQL dialect {sql_dialect!r}")


def _planner_collapse_limit_sql(planner_collapse_limit: int | None) -> str:
    """Return the ``SET LOCAL`` statements that open a script for ``planner_collapse_limit``; '' when it is None.

    ``SET LOCAL`` lasts until the end of the transaction. Sent as ONE string, the script is one transaction (also
    under autocommit, as an implicit transaction block), so the settings cover exactly the script's SELECT and
    are reset afterwards. ``geqo`` is not touched.
    """
    if planner_collapse_limit is None:
        return ""
    return "".join(f"SET LOCAL {setting} = {planner_collapse_limit};\n" for setting in COLLAPSE_LIMIT_SETTINGS)


def _unlogged_keys_sql(partition_keys: set[int] | set[str] | set[float] | set[datetime], sql_dialect: str) -> str:
    """Return the SELECT over a VALUES list of the keys that fills an eager unlogged key table (column ``partition_key``).

    String keys are escaped string literals; float and timestamp keys are cast to their type, as in the temporary
    key table (``_get_partition_key_sql_type``).
    """
    key_rows = "), (".join(_format_partition_key_for_sql(pk, sql_dialect) for pk in partition_keys)
    key_type = _get_partition_key_sql_type(partition_keys, sql_dialect)
    column = "partition_key" if key_type in ("INT", "BIGINT", "TEXT") else f"CAST(partition_key AS {key_type}) AS partition_key"
    return f"SELECT {column} FROM (VALUES ({key_rows})) AS pcache_keys(partition_key)"


def _every_table_joined_on_partition_key(parsed_query: exp.Expr, partition_key: str) -> bool:
    """Whether the tables the JOIN methods join to are connected by equalities ``a.<key> = b.<key>``.

    Only then is joining every table to the keys equivalent to joining the p0 table: through the equalities, every
    table's key equals the p0 table's. Equalities under OR or NOT do not hold for every row and do not count.
    """
    clauses = _from_clauses(parsed_query)
    aliases = {clause.alias_or_name for clause in clauses}
    if len(aliases) <= 1:
        return True

    conjuncts: list[exp.Expr] = []

    def collect(condition: exp.Expr | None) -> None:
        if condition is None:
            return
        condition = condition.unnest()
        if isinstance(condition, exp.And):
            for operand in condition.flatten():
                collect(operand)
        else:
            conjuncts.append(condition)

    where = _outer_where(parsed_query)
    collect(where.this if where is not None else None)
    for join in clauses:
        collect(join.args.get("on"))

    component = {alias: alias for alias in aliases}

    def root(alias: str) -> str:
        while component[alias] != alias:
            alias = component[alias]
        return alias

    for condition in conjuncts:
        left, right = (condition.this, condition.expression) if isinstance(condition, exp.EQ) else (None, None)
        if isinstance(left, exp.Column) and isinstance(right, exp.Column) and left.name == right.name == partition_key:
            if left.table in aliases and right.table in aliases:
                component[root(left.table)] = root(right.table)
    return len({root(alias) for alias in aliases}) == 1


def _restriction_target(
    query: str,
    parsed_query: exp.Expr,
    partition_key: str,
    method: str,
    p0_alias: str | None,
    join_every_table: bool,
    auto_detect_partition_join: bool,
    partition_join_table: str | None,
) -> str | None:
    """The alias the restriction goes on, or None when a JOIN method joins every table (``join_every_table``).

    Raises:
        ValueError: For join_every_table with another method or a p0_alias, or when not every table is joined on the
            partition key: joined to the keys, the rows of a table related to the p0 table otherwise (a time window,
            a distance) outside the key set would be dropped, and with them result rows.
    """
    if not join_every_table:
        if p0_alias is None:
            p0_alias = find_p0_alias(query, partition_key, auto_detect_partition_join, partition_join_table)
        return p0_alias
    if method not in _TABLE_JOIN_METHODS:
        raise ValueError(f"join_every_table applies to {' and '.join(_TABLE_JOIN_METHODS)}, not to {method}")
    if p0_alias is not None:
        raise ValueError("join_every_table joins every table to the keys; it takes no p0_alias")
    if not _every_table_joined_on_partition_key(parsed_query, partition_key):
        raise ValueError(
            f"join_every_table needs every table joined on {partition_key} (a.{partition_key} = b.{partition_key}); "
            "otherwise it drops result rows. Join the p0 table only (the default)."
        )
    return None


def _create_unlogged_table_setup(
    keys_sql: str,
    key_column: str,
    sql_dialect: str,
    method: str = "UNLOGGED_TABLE_JOIN",
    key_filters: list[str] | None = None,
    partition_keys: set[int] | set[str] | set[float] | set[datetime] | None = None,
) -> tuple[str, str]:
    """Create the setup SQL of ``UNLOGGED_TABLE_JOIN`` / ``UNLOGGED_TABLE_IN``: an unlogged key table with index and statistics.

    With folded subqueries (``key_filters``) the table is created in the keys' type and filled by ``_folded_fill_sql``:
    from ``partition_keys`` (eager) or from ``keys_sql`` (lazy).

    PostgreSQL does not let parallel workers read temporary tables; an unlogged table can be read inside the
    parallel part of the plan. It is a regular table of the current schema, so the script drops it itself after
    the SELECT (``_unlogged_table_teardown``). Sent as ONE string, the script is one transaction: an error before
    the DROP rolls the CREATE back, so no table is left behind, also under autocommit.

    Returns:
        tuple[str, str]: (setup_sql, table_name).
    """
    if sql_dialect != "postgres":
        raise ValueError(f"{method} is only supported for PostgreSQL, not for SQL dialect {sql_dialect!r}")
    table_name = f"{UNLOGGED_TABLE_PREFIX}{secrets.token_hex(8)}"
    if not key_filters:
        setup_sql = f"CREATE UNLOGGED TABLE {table_name} AS ({keys_sql});\n"
    elif partition_keys is not None:
        setup_sql = f"CREATE UNLOGGED TABLE {table_name} ({key_column} {_get_partition_key_sql_type(partition_keys, sql_dialect)});\n"
        setup_sql += _folded_fill_sql(table_name, key_column, key_filters, partition_keys=partition_keys, sql_dialect=sql_dialect)
    else:
        setup_sql = f"CREATE UNLOGGED TABLE {table_name} AS ({_typed_like_keys_sql(keys_sql, key_column)});\n"
        setup_sql += _folded_fill_sql(table_name, key_column, key_filters, keys_sql=keys_sql)
    setup_sql += f"CREATE INDEX {table_name}_idx ON {table_name} ({key_column});\nANALYZE {table_name};\n"
    return setup_sql, table_name


def _unlogged_table_teardown(table_name: str) -> str:
    """Return the statement that ends an unlogged method's script, after its SELECT: the DROP of the key table."""
    return f";\nDROP TABLE IF EXISTS {table_name}"


def _key_source_table(parsed_query: exp.Expr, alias: str | None, sql_dialect: str) -> str | None:
    """The plain table of ``alias`` in the outer FROM / JOIN clauses, rendered without its alias, or None.

    None for a subquery, a CTE or an alias that is not there: the key table of non-integer keys takes its column type
    from this table (``_typed_key_table_setup``).
    """
    if alias is None:
        return None
    with_ = parsed_query.args.get("with") or parsed_query.args.get("with_")
    ctes = {cte.alias_or_name for cte in with_.expressions} if with_ is not None else set()
    from_ = parsed_query.args.get("from_") or parsed_query.args.get("from")
    for clause in [*([from_] if from_ is not None else []), *(parsed_query.args.get("joins") or [])]:
        if clause.alias_or_name == alias:
            table = clause.this
            if not isinstance(table, exp.Table) or table.name in ctes:
                return None
            table = table.copy()
            table.set("alias", None)
            return table.sql(dialect=sql_dialect)
    return None


def _untyped_key_rows(partition_keys: set[int] | set[str] | set[float] | set[datetime], sql_dialect: str) -> str:
    """``VALUES ('k1'), ...`` with the keys as string literals, which the key table's column type converts, as IN does."""

    def literal(pk: int | str | float | datetime) -> str:
        if isinstance(pk, float) and sql_dialect == "sqlite" and math.isinf(pk):
            return "9e999" if pk > 0 else "-9e999"  # SQLite keeps the string 'inf' as text in a REAL column
        return exp.Literal.string(str(pk)).sql(dialect=sql_dialect)

    return "VALUES " + ", ".join(f"({literal(pk)})" for pk in partition_keys)


def _typed_key_table_setup(
    kind: str,
    key_column: str,
    source_table: str,
    partition_key: str,
    rows_sql: str,
    analyze: bool,
    sql_dialect: str,
    key_filters: list[str],
    method: str,
) -> tuple[str, str]:
    """Create a key table in the type of the key column of ``source_table`` and fill it with ``rows_sql``.

    For non-integer keys. A key list typed by its Python values (text, double precision) can compare differently from
    the key column: a ``char(n)`` column compares without the padding its keys were read back with, and a ``real``
    column in double precision. Created from the column (PostgreSQL: its type without the length or precision
    modifier; other dialects: ``LIMIT 0``), the table converts the keys on INSERT exactly as the column's own
    comparison with an IN literal would. A domain stays the domain, so its own constraints still apply to the keys.

    Args:
        kind: "TEMPORARY" or "UNLOGGED".
        key_column: The key table's column name.
        source_table: The p0 table (``_key_source_table``).
        partition_key: Its key column.
        rows_sql: ``VALUES (...)`` of untyped literals (``_untyped_key_rows``) or the lazy key subquery.
        analyze: ANALYZE the table (always for unlogged tables).
        key_filters: Folded subqueries (``_fold_key_subqueries``). PostgreSQL converts the keys in a staging table of the
            column's type and fills the key table with ``_folded_fill_sql`` (an INTERSECT in that type); the other
            dialects delete the keys outside them, as ``_create_tmp_table_setup`` does.

    Returns:
        tuple[str, str]: (setup_sql, table_name).
    """
    if kind == "UNLOGGED":
        if sql_dialect != "postgres":
            raise ValueError(f"{method} is only supported for PostgreSQL, not for SQL dialect {sql_dialect!r}")
        table_name = f"{UNLOGGED_TABLE_PREFIX}{secrets.token_hex(8)}"
        create = f"CREATE UNLOGGED TABLE {table_name}"
    else:
        table_name = f"tmp_cache_keys_{random.randint(100000, 999999)}"
        ddl_prefix, ddl_suffix = _temp_table_ddl(table_name, sql_dialect)
        create = f"{ddl_prefix}CREATE TEMPORARY TABLE {table_name}{ddl_suffix}"
    if sql_dialect == "postgres":
        # The column's type without its modifier: varchar(4) as varchar, char(5) as bpchar (still blank-padded),
        # numeric(10,2) as numeric, timestamp(0) as timestamp. A key the modifier would reject, round or truncate can
        # match no row of the column; with the modifier, the INSERT failed on it (value too long, numeric overflow) or
        # rounded it. array_agg keeps the element type (also uuid, boolean) and drops the modifier; WHERE false HAVING
        # false creates the table empty without reading the source.
        typed_like = f"AS SELECT (array_agg(pcache_src.{partition_key}))[1] AS {key_column} FROM {source_table} AS pcache_src WHERE false HAVING false"
    else:
        typed_like = f"AS SELECT pcache_src.{partition_key} AS {key_column} FROM {source_table} AS pcache_src LIMIT 0"
    if key_filters and sql_dialect == "postgres":
        # The keys are converted to the column's type in a staging table first, so that the INTERSECT compares them
        # as the column does; untyped literals in the INTERSECT itself would be resolved to text
        stage = f"pcache_stage_{secrets.token_hex(4)}"
        setup_sql = f"CREATE TEMPORARY TABLE {stage} ON COMMIT DROP {typed_like};\n"
        setup_sql += f"INSERT INTO {stage} ({key_column}) {rows_sql};\n"
        setup_sql += f"{create} {typed_like};\n"
        setup_sql += _folded_fill_sql(table_name, key_column, key_filters, keys_sql=f"SELECT {stage}.{key_column} FROM {stage}")
        setup_sql += f"DROP TABLE {stage};\n"
    else:
        setup_sql = f"{create} {typed_like};\n"
        setup_sql += f"INSERT INTO {table_name} ({key_column}) {rows_sql};\n"
        for key_filter in key_filters:
            setup_sql += f"DELETE FROM {table_name} WHERE ({key_column} IN ({key_filter})) IS NOT TRUE;\n"
    setup_sql += f"CREATE INDEX {table_name}_idx ON {table_name} ({key_column});\n"
    if analyze or kind == "UNLOGGED":
        setup_sql += f"ANALYZE {table_name};\n"
    return setup_sql, table_name


def _key_type_guard_sql(column: str, column_types: tuple[str, ...]) -> str:
    """A condition that fails the query, with a message, when ``column`` is of one of ``column_types`` (PostgreSQL).

    For the methods whose key list cannot take the column's type (VALUES, the lazy IN_SUBQUERY): a ``char(n)`` or
    ``real`` column would silently match none of the keys. ``IS NOT NULL`` keeps the planner's estimate of the
    condition near 1, and the message depends on the column, so the error is not raised at planning time.
    """
    types = ", ".join(f"'{column_type}'::regtype" for column_type in column_types)
    return (
        f"(CASE WHEN pg_typeof({column}) IN ({types}) THEN CAST('PartitionCache: a key column of type ' || pg_typeof({column})::text || "
        "' needs method IN or a key-table method' AS INTEGER) ELSE 1 END) IS NOT NULL"
    )


def _cache_datatype(cache_handler: Any, partition_key: str) -> str | None:
    """The cache's datatype of the partition key, or None if the handler cannot tell."""
    try:
        datatype = cache_handler.get_datatype(partition_key)
    except Exception:  # noqa: BLE001 - a handler without metadata: the scripts are built as for integer keys
        return None
    return datatype if isinstance(datatype, str) else None


def _typed_source(parsed_query: exp.Expr, query: str, partition_key: str, method: str, p0_alias: str | None,
                  auto_detect_partition_join: bool, partition_join_table: str | None, sql_dialect: str) -> str:
    """The p0 table that gives a non-integer key table its column type; ValueError if the p0 alias is no plain table."""
    alias = p0_alias if p0_alias is not None else find_p0_alias(query, partition_key, auto_detect_partition_join, partition_join_table)
    source_table = _key_source_table(parsed_query, alias, sql_dialect)
    if source_table is None:
        raise ValueError(
            f"{method} with non-integer keys builds its key table in the type of the key column of {alias!r}, which must be a plain "
            "table (not a subquery or CTE); use method IN"
        )
    return source_table


# Functions whose result can differ between two statements of the script, which is one transaction: a folded subquery
# runs in the key table's statement. Best effort, a user-defined volatile function is not recognised; now() and the
# other transaction-stable time functions are allowed.
_VOLATILE_FUNCTION_NAMES = frozenset(
    {
        "random", "rand", "random_normal", "setseed", "clock_timestamp", "statement_timestamp", "timeofday", "nextval",
        "currval", "setval", "lastval", "gen_random_uuid", "uuid", "uuid_generate_v1", "uuid_generate_v4", "txid_current",
        "pg_backend_pid", "current_setting",
    }
)
# fold_key_subqueries modes: True folds for integer and decimal keys, "any_key_type" also for text and timestamp keys
FOLD_ANY_KEY_TYPE = "any_key_type"


def _outer_conjuncts(condition: exp.Expr | None) -> list[exp.Expr]:
    """The top-level AND operands of a condition, parentheses removed; an OR or NOT is one operand."""
    if condition is None:
        return []
    condition = condition.unnest()
    if isinstance(condition, exp.And):
        return [operand for part in condition.flatten() for operand in _outer_conjuncts(part)]
    return [condition]


def _key_component(parsed_query: exp.Expr, partition_key: str, p0_alias: str | None) -> set[str]:
    """The aliases whose partition key equals the p0 alias's in every result row.

    Connected to it by equalities ``a.<key> = b.<key>`` among the top-level conjuncts of the outer WHERE and of the
    ON clauses of inner joins; an outer join's ON does not hold for null-extended rows. With p0_alias None
    (join_every_table, whose guard checked that every table is connected) every table of the FROM and JOIN clauses.
    """
    joins = [join for join in (parsed_query.args.get("joins") or []) if not join.side]
    from_ = parsed_query.args.get("from_") or parsed_query.args.get("from")
    aliases = {clause.alias_or_name for clause in [*(parsed_query.args.get("joins") or []), *([from_] if from_ is not None else [])]}
    if p0_alias is None:
        return aliases
    where = _outer_where(parsed_query)
    conjuncts = _outer_conjuncts(where.this if where is not None else None)
    for join in joins:
        conjuncts += _outer_conjuncts(join.args.get("on"))
    component, changed = {p0_alias}, True
    while changed:
        changed = False
        for condition in conjuncts:
            if isinstance(condition, exp.EQ) and isinstance(condition.this, exp.Column) and isinstance(condition.expression, exp.Column):
                left, right = condition.this, condition.expression
                if left.name == right.name == partition_key and {left.table, right.table} & component and {left.table, right.table} - component:
                    component |= {left.table, right.table}
                    changed = True
    return component


_FOLDED_KEY_COLUMN_NAME = "pcache_key"


def _unfoldable_reason(
    subquery: exp.Expr,
    outer_ctes: set[str],
    column_resolver: "ColumnResolver | None" = None,
    assume_inner_columns: bool = False,
    sql_dialect: str = "postgres",
) -> str | None:
    """Why an IN subquery cannot be evaluated on its own in the key table's statement, or None if it can.

    It must give the same rows there: every column bound inside the subquery itself (a qualified one by a table of the
    subquery, an unqualified one by ``_unqualified_column_reason``), no CTE of the outer query, no volatile function,
    no LIMIT / OFFSET / FETCH / TABLESAMPLE, no LATERAL, and one output column.
    """
    select = subquery
    while isinstance(select, exp.SetOperation):
        select = select.this
    if not isinstance(select, exp.Select) or len(select.expressions) != 1 or isinstance(select.expressions[0], exp.Star):
        return "not a one-column query"
    if any(subquery.find(node_type) for node_type in (exp.Limit, exp.Offset, exp.Fetch, exp.TableSample)):
        return "LIMIT, OFFSET, FETCH or TABLESAMPLE"
    if subquery.find(exp.Lateral) or any(join.args.get("kind") == "LATERAL" for join in subquery.find_all(exp.Join)):
        return "LATERAL"
    for function in subquery.find_all(exp.Func):
        name = function.name if isinstance(function, exp.Anonymous) else function.sql_name()
        if name.lower() in _VOLATILE_FUNCTION_NAMES:
            return f"volatile function {name}"
    inner_ctes = {cte.alias_or_name for cte in subquery.find_all(exp.CTE)}
    defined = set(inner_ctes)
    for table in subquery.find_all(exp.Table):
        if table.name in outer_ctes and table.name not in inner_ctes:
            return f"reference to the CTE {table.name} of the query"
        defined |= {table.name, table.alias_or_name}
    defined |= {alias.name for alias in subquery.find_all(exp.TableAlias)}
    for column in subquery.find_all(exp.Column):
        if not column.table:
            reason = _unqualified_column_reason(column, subquery, column_resolver, assume_inner_columns, sql_dialect)
            if reason is not None:
                return reason
        elif column.table not in defined:
            return f"correlated ({column.sql()})"
    return None


# Resolves a relation as written in SQL ("t", "s.t", '"S"."T"') to its column names, or None if it is unknown
ColumnResolver = Callable[[str], Collection[str] | None]

# Columns of the statements the non-PostgreSQL fold forms put a subquery into (DELETE ... WHERE partition_key IN (S),
# pcache_folded.pcache_key IN (S)): an outward unqualified column of these names would bind to them silently
_KEY_TABLE_COLUMN_NAMES = frozenset({"partition_key", _FOLDED_KEY_COLUMN_NAME})


def _unqualified_column_reason(
    column: exp.Column, subquery: exp.Expr, column_resolver: "ColumnResolver | None", assume_inner_columns: bool, sql_dialect: str
) -> str | None:
    """Why an unqualified column of a candidate subquery may not bind inside it, or None if it binds inside.

    SQL binds an unqualified name to the innermost query level whose FROM items have such a column. The column is
    walked from its own SELECT outward, up to the subquery's root: it binds inside if a level's FROM items have it,
    outward (a correlated subquery, which stays in the statement) if none does, and the case is undecidable if a FROM
    item on the way has unknown columns. A name that is no FROM column (a SELECT-list alias in ORDER BY, a system column
    such as ctid, a whole-row reference) is thereby refused, which is the safe side.

    With ``assume_inner_columns`` the caller states that every unqualified column binds inside. On PostgreSQL a folded
    subquery is a derived table in an INTERSECT of the key table's statement, with no query level around it; a column
    that really bound outward then fails with "column does not exist", it cannot bind elsewhere. The other dialects
    put the subquery into a statement over the key table, so the key table's column names stay refused there.
    """
    name = column.name
    if assume_inner_columns:
        if sql_dialect != "postgres" and name.lower() in _KEY_TABLE_COLUMN_NAMES:
            return f"unqualified column {name} could bind to the key table"
        return None
    if column_resolver is None:
        return f"unqualified column {name}"
    quoted = bool(column.this.args.get("quoted")) if isinstance(column.this, exp.Identifier) else False
    inside = {id(select) for select in subquery.find_all(exp.Select)}
    select = column.find_ancestor(exp.Select)
    while select is not None and id(select) in inside:
        columns = _from_item_columns(select, subquery, column_resolver, sql_dialect)
        if columns is None:
            return f"unqualified column {name}: the columns of a FROM item are unknown"
        if _column_matches(name, quoted, columns, sql_dialect):
            return None
        select = select.find_ancestor(exp.Select)
    return f"unqualified column {name} binds outside the subquery"


def _column_matches(name: str, quoted: bool, columns: Collection[str], sql_dialect: str) -> bool:
    """Whether an identifier names one of ``columns``: a quoted one exactly, a bare one as the dialect folds it."""
    if quoted:
        return name in columns
    if sql_dialect == "postgres":
        return name.lower() in columns
    return name.lower() in {c.lower() for c in columns}


def _from_item_columns(select: exp.Select, subquery: exp.Expr, column_resolver: "ColumnResolver", sql_dialect: str) -> set[str] | None:
    """The column names of a SELECT's FROM items (FROM and JOINs), or None if one item's columns are unknown."""
    from_ = select.args.get("from") or select.args.get("from_")
    items = ([from_.this] if from_ is not None else []) + [join.this for join in select.args.get("joins") or []]
    inner_ctes = {cte.alias_or_name: cte for cte in subquery.find_all(exp.CTE)}
    columns: set[str] = set()
    for item in items:
        item_columns = _item_columns(item, inner_ctes, column_resolver, sql_dialect)
        if item_columns is None:
            return None
        columns |= item_columns
    return columns


def _alias_columns(item: exp.Expr) -> set[str] | None:
    alias = item.args.get("alias")
    names = [col.name for col in alias.columns] if isinstance(alias, exp.TableAlias) else []
    return set(names) if names else None


def _item_columns(item: exp.Expr, inner_ctes: dict[str, exp.CTE], column_resolver: "ColumnResolver", sql_dialect: str) -> set[str] | None:
    """The columns of one FROM item: a relation through the resolver, a CTE or derived table through its outputs."""
    listed = _alias_columns(item)
    if listed is not None:
        return listed
    if isinstance(item, exp.Table) and isinstance(item.this, exp.Identifier):
        if not item.args.get("db") and item.name in inner_ctes:
            return _select_output_names(inner_ctes[item.name].this, _alias_columns(inner_ctes[item.name]))
        relation = item.copy()
        relation.set("alias", None)
        resolved = column_resolver(relation.sql(dialect=sql_dialect))
        return set(resolved) if resolved else None
    if isinstance(item, exp.Subquery):
        return _select_output_names(item.this, None)
    return None  # a function, VALUES or UNNEST without a column alias list: unknown


def _select_output_names(query: exp.Expr, listed: set[str] | None) -> set[str] | None:
    """The output column names of a query, or None if one is unnamed or a star."""
    if listed is not None:
        return listed
    while isinstance(query, exp.SetOperation):
        query = query.this
    if not isinstance(query, exp.Select):
        return None
    names = set()
    for projection in query.expressions:
        if isinstance(projection, exp.Star) or (isinstance(projection, exp.Column) and isinstance(projection.this, exp.Star)):
            return None
        name = projection.alias_or_name
        if not name:
            return None
        names.add(name)
    return names


def catalog_column_resolver(execute: Callable[[str], Iterable[Any]], sql_dialect: str = "postgres") -> ColumnResolver:
    """A ``ColumnResolver`` that reads the database catalog through ``execute`` (SQL in, first column of the rows out).

    For ``fold_key_subqueries``: it decides whether an unqualified column of a subquery binds inside it. It must run on
    the connection that executes the query, before the query's script: the relation is resolved as written, with that
    connection's search_path, its temporary tables (which shadow permanent ones) and its views. A resolver built from
    another connection, or from a schema dictionary, is the caller's assertion that the names resolve the same way.
    Results are cached for the resolver's life: build one per query (or per batch) so that a schema change is seen.
    ``PostgresDBHandler.execute``, ``SQLiteDBHandler.execute`` and ``DuckDBHandler.execute`` fit as ``execute``.
    """

    def literal(text: str) -> str:
        return "'" + text.replace("'", "''") + "'"

    @functools.cache
    def resolve(relation: str) -> frozenset[str] | None:
        if sql_dialect == "postgres":
            # to_regclass parses the name as written (quotes, schema) and is NULL for an unknown relation; attnum > 0
            # leaves out the system columns (ctid, xmin, ...), which therefore stay refused
            sql_text = (
                "SELECT a.attname FROM pg_catalog.pg_attribute AS a "
                f"WHERE a.attrelid = to_regclass({literal(relation)}) AND a.attnum > 0 AND NOT a.attisdropped"
            )
        else:
            table = exp.to_table(relation, dialect=sql_dialect)
            if sql_dialect == "sqlite":
                schema = f", {literal(table.db)}" if table.db else ""
                sql_text = f"SELECT name FROM pragma_table_info({literal(table.name)}{schema})"
            elif sql_dialect == "duckdb":
                schema = f" AND table_schema = {literal(table.db)}" if table.db else ""
                sql_text = f"SELECT column_name FROM information_schema.columns WHERE table_name = {literal(table.name)}{schema}"
            else:
                return None
        try:
            names = frozenset(str(row[0] if isinstance(row, tuple | list) else row) for row in execute(sql_text))
        except Exception:  # noqa: BLE001 - an unreadable catalog leaves the column undecided, i.e. refused
            logger.debug(f"Catalog lookup of {relation!r} failed", exc_info=True)
            return None
        return names or None

    return resolve


def _fold_key_subqueries(
    parsed_query: exp.Expr,
    partition_key: str,
    p0_alias: str | None,
    sql_dialect: str,
    column_resolver: "ColumnResolver | None" = None,
    assume_inner_columns: bool = False,
) -> list[str]:
    """Remove the foldable conditions ``<alias>.<key> IN (<subquery>)`` from the outer WHERE; return their subqueries.

    A condition is foldable when it is a top-level conjunct of the outer WHERE (not under OR or NOT, so never NOT IN),
    written as ``IN (<subquery>)`` or ``= ANY (<subquery>)``, on the partition key of the p0 alias or of an alias tied to
    it by key equalities (``_key_component``), and its subquery can run on its own (``_unfoldable_reason``). The key
    table then keeps only the keys for which ``key IN (<subquery>)`` is TRUE, the condition the row filter applied.
    """
    where = _outer_where(parsed_query)
    if where is None or not isinstance(parsed_query, exp.Select):
        return []
    if parsed_query.find(exp.Placeholder, exp.Parameter):
        # A folded subquery moves in front of the main statement and with it its bind parameters' positions
        return []
    with_ = parsed_query.args.get("with") or parsed_query.args.get("with_")
    outer_ctes = {cte.alias_or_name for cte in with_.expressions} if with_ is not None else set()
    component = _key_component(parsed_query, partition_key, p0_alias)
    kept: list[exp.Expr] = []
    folded: list[str] = []
    for condition in _outer_conjuncts(where.this):
        column, subquery = None, None
        if isinstance(condition, exp.In) and isinstance(condition.args.get("query"), exp.Subquery):
            column, subquery = condition.this, condition.args["query"].this
        elif isinstance(condition, exp.EQ) and isinstance(condition.expression, exp.Any) and isinstance(condition.expression.this, exp.Subquery):
            column, subquery = condition.this, condition.expression.this.this
        candidate = isinstance(column, exp.Column) and column.name == partition_key and subquery is not None
        reason = None
        if candidate and column.table not in component:
            reason = f"alias {column.table} not tied to the restricted alias by key equalities"
        elif candidate:
            reason = _unfoldable_reason(subquery, outer_ctes, column_resolver, assume_inner_columns, sql_dialect)
        if candidate and reason is None:
            folded.append(subquery.sql(dialect=sql_dialect))
            logger.info(f"fold_key_subqueries: folded {column.sql()} IN ({_abbreviated(subquery)})")
        else:
            if candidate:
                logger.info(f"fold_key_subqueries: not folded ({reason}): {column.sql()} IN ({_abbreviated(subquery)})")
            kept.append(condition)
    if folded:
        if kept:
            where.set("this", exp.and_(*kept))
        else:
            parsed_query.set("where", None)
    return folded


def _abbreviated(expression: exp.Expr, limit: int = 120) -> str:
    """An expression's SQL for a log line, cut to ``limit`` characters."""
    text = expression.sql()
    return text if len(text) <= limit else text[: limit - 3] + "..."


# The key column's name around the folded conditions of a key table's defining query
_FOLDED_KEY_COLUMN = _FOLDED_KEY_COLUMN_NAME


def _filtered_keys_sql(keys_sql: str, column: str, key_filters: list[str]) -> str:
    """The keys (column ``column``) of ``keys_sql`` for which ``key IN (<subquery>)`` is TRUE for every folded subquery.

    For the dialects other than PostgreSQL, which uses ``_folded_fill_sql`` (see there why).

    The WHERE keeps a key only when the condition is TRUE, as the row filter did. The keys are renamed to
    ``_FOLDED_KEY_COLUMN`` around the conditions, so that an unqualified column of a subquery cannot bind to them.
    """
    if not key_filters:
        return keys_sql
    key = f"pcache_folded.{_FOLDED_KEY_COLUMN}"
    conditions = " AND ".join(f"{key} IN ({key_filter})" for key_filter in key_filters)
    return (
        f"SELECT {key} AS {column} FROM (SELECT pcache_keys_inner.{column} AS {_FOLDED_KEY_COLUMN} FROM ({keys_sql}) AS pcache_keys_inner) "
        f"AS pcache_folded WHERE {conditions}"
    )


def _kept_keys_sql(cte: str, column: str, key_filters: list[str]) -> str:
    """The keys of the common table expression ``cte`` (column ``column``) that every folded subquery contains:
    K INTERSECT S1 INTERSECT S2 ...

    A set operation has no join for a misestimate to turn into a nested loop or a per-key subplan, which is what
    ``k IN (S)`` became: a semi join that de-duplicated and hashed all of S, a nested loop when the keys were estimated
    at a few rows, or (as ``(k IN (S)) IS NOT TRUE``) a subplan rescanned for every key. PostgreSQL's HashSetOp hashes
    the input with fewer estimated groups and streams the other, a tie keeping the left. ``cte`` is MATERIALIZED, so
    the keys are estimated at the default number of groups like an unnest or other unanalyzed subquery, and stay the
    hashed side unless a subquery's statistics show fewer groups; a VALUES list read directly is estimated higher and
    lost to an unnest of millions of rows. Each subquery is a derived table: a set operation or a set-returning target
    list inside it stays inside.

    Set operations treat NULL as equal to NULL. The keys hold no NULL, so the result holds none, and the result are
    the keys for which ``key IN (S)`` is TRUE for every S, as the folded row filter required.
    """
    members = "".join(f" INTERSECT SELECT * FROM ({key_filter}) AS pcache_folded_{i}" for i, key_filter in enumerate(key_filters, 1))
    return f"SELECT {cte}.{column} FROM {cte}{members}"


def _typed_like_keys_sql(keys_sql: str, column: str) -> str:
    """A query of no rows whose column ``column`` has the type of ``keys_sql``'s: a folded key table created from it
    keeps the keys' type, whatever the common type of the keys and the subqueries is. ``keys_sql`` is not run."""
    return f"SELECT pcache_keys_inner.{column} FROM ({keys_sql}) AS pcache_keys_inner LIMIT 0"


def _folded_fill_sql(
    table_name: str,
    column: str,
    key_filters: list[str],
    keys_sql: str | None = None,
    partition_keys: set[int] | set[str] | set[float] | set[datetime] | None = None,
    sql_dialect: str = "postgres",
) -> str:
    """The INSERT (PostgreSQL) that fills a folded key table with the keys every folded subquery contains.

    The keys are the eager ``partition_keys`` (Python values, never NULL) or the lazy ``keys_sql`` without its NULLs,
    in a MATERIALIZED common table expression (see ``_kept_keys_sql``). The table already has the keys' type; the
    INSERT assigns the kept keys, a subset of the keys, to it. The CTE's name has a random suffix: a CTE is visible
    inside the folded subqueries, which must not bind one of their tables to it.
    """
    cte = f"pcache_k_{secrets.token_hex(4)}"
    if partition_keys is not None:
        rows = "), (".join(_format_partition_key_for_sql(pk, sql_dialect) for pk in partition_keys)
        definition = f"{cte} ({column}) AS MATERIALIZED (VALUES ({rows}))"
    else:
        definition = (
            f"{cte} AS MATERIALIZED (SELECT pcache_keys_inner.{column} FROM ({keys_sql}) AS pcache_keys_inner "
            f"WHERE pcache_keys_inner.{column} IS NOT NULL)"
        )
    return f"WITH {definition} INSERT INTO {table_name} ({column}) {_kept_keys_sql(cte, column, key_filters)};\n"


# The dialects whose folded scripts are tested; MySQL, for one, has INTERSECT only from 8.0.31
FOLD_DIALECTS = ("postgres", "duckdb", "sqlite")


def _check_fold_key_subqueries(fold_key_subqueries: bool | str, method: str, sql_dialect: str = "postgres") -> None:
    if fold_key_subqueries not in (False, True, FOLD_ANY_KEY_TYPE):
        raise ValueError(f"fold_key_subqueries is False, True or {FOLD_ANY_KEY_TYPE!r}, not {fold_key_subqueries!r}")
    if fold_key_subqueries and method not in _SCRIPT_METHODS:
        raise ValueError(f"fold_key_subqueries needs a key-table method {_SCRIPT_METHODS}, not {method!r}")
    if fold_key_subqueries and sql_dialect not in FOLD_DIALECTS:
        raise ValueError(f"fold_key_subqueries supports the SQL dialects {FOLD_DIALECTS}, not {sql_dialect!r}")


def _fold_allowed(fold_key_subqueries: bool | str, key_kind: str | None) -> bool:
    """Whether folding may run for keys of ``key_kind``: "integer", "decimal", "text", "timestamp", "float" or None.

    In the key table, ``key IN (<subquery>)`` compares in the key table's column type, not in the key column's. For
    integer and decimal keys that is the same comparison. For text and timestamp keys it can differ (char(n), citext, a
    non-default or non-deterministic collation, time zones), and a difference loses rows silently, so they fold only
    with ``FOLD_ANY_KEY_TYPE``, by which the caller states that the key column compares like text / its timestamp
    type. Float keys never fold (a real column compares in double precision), nor keys of unknown type.
    """
    if not fold_key_subqueries or key_kind in (None, "float"):
        return False
    return key_kind in ("integer", "decimal") or fold_key_subqueries == FOLD_ANY_KEY_TYPE


def _key_kind(partition_keys: set[int] | set[str] | set[float] | set[datetime]) -> str | None:
    """The kind of a key set for ``_fold_allowed``; None for a mix of kinds."""
    kinds = {
        "float" if isinstance(k, float) else "integer" if isinstance(k, int) else "decimal" if isinstance(k, Decimal)
        else "timestamp" if isinstance(k, datetime) else "text" if isinstance(k, str) else None
        for k in partition_keys
    }
    if kinds <= {"integer", "decimal"} and kinds:
        return "decimal" if "decimal" in kinds else "integer"
    return kinds.pop() if len(kinds) == 1 else None


def extend_query_with_partition_keys(
    query: str,
    partition_keys: set[int] | set[str] | set[float] | set[datetime],
    partition_key: str,
    method: Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"] = "IN",
    p0_alias: str | None = None,
    analyze_tmp_table: bool = True,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
    sql_dialect: str = "postgres",
    join_every_table: bool = False,
    planner_collapse_limit: int | None = None,
    fold_key_subqueries: bool | str = False,
    fold_column_resolver: ColumnResolver | None = None,
    fold_assume_inner_columns: bool = False,
) -> str:
    """
    Extend a given SQL query with the cache functionality.

    This covers basic cases, in many cases this may not be fitting, in these cases the SQL rewrite should happen in the calling software where
    the original query was created.

    Args:
        query (str): The SQL query to be extended.
        partition_keys (set[int] | set[str]| set[float] | set[datetime]): The set of partition keys to extend the query with.
        partition_key (str): The identifier for the partition.
        method (Literal["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"]): The
            method to use to extend the query. ``UNLOGGED_TABLE_JOIN`` / ``UNLOGGED_TABLE_IN`` (PostgreSQL only) are
            ``TMP_TABLE_JOIN`` / ``TMP_TABLE_IN`` with an unlogged table, always with index and ANALYZE; the script
            ends with its DROP, after the SELECT. Execute it as ONE string so that it runs in one transaction.
        p0_alias (str | None): The alias of the table to use for the partition key; None detects it (``find_p0_alias``).
            The JOIN methods join the key table to this table only.
        analyze_tmp_table (bool): Whether to create index and analyze. (only for temporary table methods)
        auto_detect_partition_join: Whether to auto-detect partition-join tables.
        partition_join_table: Explicitly specified partition-join table alias or name.
        sql_dialect: sqlglot dialect used to parse and render the query. Defaults to PostgreSQL.
        join_every_table: JOIN methods only: join the key table to every table of the query instead of the p0 table,
            as they did without p0_alias before 2026-11. Raises ValueError unless every table is joined on the
            partition key, where it is equivalent; it gives the planner the key table at every table.
        planner_collapse_limit: If set to N, the script begins with ``SET LOCAL join_collapse_limit = N;`` and
            ``SET LOCAL from_collapse_limit = N;`` (``geqo`` is not touched). PostgreSQL and the script methods
            (TMP_TABLE_*, UNLOGGED_TABLE_*) only; other methods raise ValueError. Not applied to an empty
            ``partition_keys`` set, whose rewrite is a single statement. None (default) is off.
        fold_key_subqueries: Key-table methods only: move the conditions ``<alias>.<key> IN (<subquery>)`` (also
            ``= ANY``) on the restricted alias, or on one tied to it by key equalities, into the key table: it keeps the
            keys for which the condition is TRUE, and the condition leaves the statement, so the planner sees one table
            of known size instead of the subquery. Only uncorrelated, deterministic subqueries without LIMIT that are
            top-level conjuncts of the outer WHERE are folded; others stay. Other methods raise ValueError. True folds
            for integer and decimal keys, ``"any_key_type"`` also for text and timestamp keys (see ``_fold_allowed``);
            float keys never fold, nor a query with bind parameters. An unqualified column of a subquery must bind
            inside it: decided with ``fold_column_resolver``, or stated by ``fold_assume_inner_columns``; else refused.
        fold_column_resolver: Resolves a relation of a subquery to its columns (``catalog_column_resolver`` reads the
            catalog). With it, a subquery whose unqualified columns all bind to its own FROM items folds; one with a
            column that binds outward (correlated) or to a FROM item of unknown columns stays. Build it on the
            connection that executes the query, before the script runs.
        fold_assume_inner_columns: The caller states that every unqualified column of a subquery binds inside it.
            Default False. On PostgreSQL a wrong statement makes the script fail ("column does not exist"), it cannot
            change the rows; on the other dialects the key table's column names (partition_key, pcache_key) stay
            refused.

    Returns:
        str: The extended SQL query as string. An empty ``partition_keys`` set is a valid cache result
        (the intersection proved that no partition matches); the query is then restricted with a
        commented ``AND FALSE`` regardless of ``method``.

    Raises:
        ValueError: For float keys with VALUES: typed as double precision, they would silently miss a ``real``
            column; ``IN`` and the key-table methods restrict them exactly. For a key-table method with non-integer keys
            when the p0 alias is not a plain table, whose key column type the key table could not take.
    """
    _check_planner_collapse_limit(planner_collapse_limit, method, sql_dialect)
    _check_fold_key_subqueries(fold_key_subqueries, method, sql_dialect)
    if not partition_keys:
        return _add_empty_intersection_condition(query, sql_dialect)
    _refuse_float_keys(partition_keys, method)

    # Parsed and rendered in the query's dialect: the generic writer turns ARRAY['a'] into ARRAY('a'), which PostgreSQL rejects
    parsed_query = sqlglot.parse_one(query, read=sql_dialect)
    p0_alias = _restriction_target(query, parsed_query, partition_key, method, p0_alias, join_every_table, auto_detect_partition_join, partition_join_table)
    key_filters = (
        _fold_key_subqueries(parsed_query, partition_key, p0_alias, sql_dialect, fold_column_resolver, fold_assume_inner_columns)
        if _fold_allowed(fold_key_subqueries, _key_kind(partition_keys))
        else []
    )
    # Non-integer keys: the key tables take the key column's type (char(n), real); a VALUES list cannot and is guarded.
    # Integer keys keep their scripts.
    typed_source = None
    if method in _SCRIPT_METHODS and _key_kind(partition_keys) != "integer":
        typed_source = _typed_source(parsed_query, query, partition_key, method, p0_alias, auto_detect_partition_join, partition_join_table, sql_dialect)
    if method == "VALUES" and sql_dialect == "postgres" and any(isinstance(k, str) for k in partition_keys):
        _add_where_condition(parsed_query, sqlglot.parse_one(_key_type_guard_sql(f"{p0_alias}.{partition_key}", ("character",)), read=sql_dialect))

    if method == "IN":
        # The key list is not parsed: a placeholder identifier goes through sqlglot and the
        # formatted list is spliced into the rendered SQL afterwards (see _splice_key_list).
        sentinel = _key_list_sentinel()
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN ({sentinel})", read=sql_dialect)
        _add_where_condition(parsed_query, partition_expr)
        key_list = ", ".join(_format_spliced_partition_key(pk, sql_dialect) for pk in partition_keys)
        return _splice_key_list(parsed_query.sql(dialect=sql_dialect), sentinel, key_list)

    elif method == "VALUES":
        sentinel = _key_list_sentinel()
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN (VALUES({sentinel}))", read=sql_dialect)
        _add_where_condition(parsed_query, partition_expr)
        key_list = "), (".join(_format_spliced_partition_key(pk, sql_dialect) for pk in partition_keys)
        return _splice_key_list(parsed_query.sql(dialect=sql_dialect), sentinel, key_list)

    elif method == "TMP_TABLE_JOIN" or method == "UNLOGGED_TABLE_JOIN":
        if typed_source is not None:
            kind = "UNLOGGED" if method == "UNLOGGED_TABLE_JOIN" else "TEMPORARY"
            tmp_table_setup, table_name = _typed_key_table_setup(
                kind, "partition_key", typed_source, partition_key, _untyped_key_rows(partition_keys, sql_dialect), analyze_tmp_table, sql_dialect, key_filters, method
            )
        elif method == "UNLOGGED_TABLE_JOIN":
            tmp_table_setup, table_name = _create_unlogged_table_setup(
                _unlogged_keys_sql(partition_keys, sql_dialect), "partition_key", sql_dialect, method, key_filters, partition_keys
            )
        else:
            tmp_table_setup, table_name = _create_tmp_table_setup(partition_keys, analyze_tmp_table, sql_dialect, key_filters)

        # Add as inner join to the p0 table, or with join_every_table to every table, of the restricted SELECT only
        for from_clause in _from_clauses(parsed_query):
            # Get alias of the table
            table_alias = from_clause.alias_or_name

            if p0_alias is not None and table_alias != p0_alias:
                continue

            # Create the new join expression using sqlglot
            join_expr = (
                from_clause.this.sql(dialect=sql_dialect)  # Original table
                + " "  # Space between tables
                + exp.Join(
                    this=exp.Identifier(this=f"{table_name} AS tmp_{table_alias}"),
                    on=exp.EQ(
                        this=exp.Identifier(this=f"tmp_{table_alias}.partition_key"),
                        expression=exp.Identifier(this=f"{table_alias}.{partition_key}"),
                    ),
                    kind="INNER",
                ).sql(dialect=sql_dialect)  # New join expression
            )

            # Replace the old join expression with the new one
            from_clause.this.replace(join_expr)

        script = _planner_collapse_limit_sql(planner_collapse_limit) + tmp_table_setup + parsed_query.sql(dialect=sql_dialect)
        if method == "UNLOGGED_TABLE_JOIN":
            script += _unlogged_table_teardown(table_name)
        return script

    elif method == "TMP_TABLE_IN" or method == "UNLOGGED_TABLE_IN":
        if typed_source is not None:
            kind = "UNLOGGED" if method == "UNLOGGED_TABLE_IN" else "TEMPORARY"
            tmp_table_setup, table_name = _typed_key_table_setup(
                kind, "partition_key", typed_source, partition_key, _untyped_key_rows(partition_keys, sql_dialect), analyze_tmp_table, sql_dialect, key_filters, method
            )
        elif method == "UNLOGGED_TABLE_IN":
            tmp_table_setup, table_name = _create_unlogged_table_setup(
                _unlogged_keys_sql(partition_keys, sql_dialect), "partition_key", sql_dialect, method, key_filters, partition_keys
            )
        else:
            tmp_table_setup, table_name = _create_tmp_table_setup(partition_keys, analyze_tmp_table, sql_dialect, key_filters)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN (SELECT partition_key FROM {table_name})", read=sql_dialect)
        _add_where_condition(parsed_query, partition_expr)
        script = _planner_collapse_limit_sql(planner_collapse_limit) + tmp_table_setup + parsed_query.sql(dialect=sql_dialect)
        if method == "UNLOGGED_TABLE_IN":
            script += _unlogged_table_teardown(table_name)
        return script

    raise ValueError(f"Unknown method {method!r}")


def _create_tmp_table_setup_from_subquery(
    lazy_subquery: str, partition_key: str, analyze_tmp_table: bool, sql_dialect: str = "postgres", key_filters: list[str] | None = None
) -> tuple[str, str]:
    """Create the SQL for temporary table setup from a lazy subquery.

    With folded subqueries (``key_filters``), PostgreSQL creates the table in the keys' type and fills it with
    ``_folded_fill_sql``; the other dialects filter the keys with ``_filtered_keys_sql``.
    """

    table_name = f"tmp_cache_keys_{random.randint(100000, 999999)}"
    # ON COMMIT DROP: dropped at transaction end (script must run in one transaction).
    ddl_prefix, ddl_suffix = _temp_table_ddl(table_name, sql_dialect)
    if key_filters and sql_dialect == "postgres":
        setup_sql = f"CREATE TEMPORARY TABLE {table_name}{ddl_suffix} AS ({_typed_like_keys_sql(lazy_subquery, partition_key)});\n"
        setup_sql += _folded_fill_sql(table_name, partition_key, key_filters, keys_sql=lazy_subquery)
    else:
        setup_sql = f"{ddl_prefix}CREATE TEMPORARY TABLE {table_name}{ddl_suffix} AS ({_filtered_keys_sql(lazy_subquery, partition_key, key_filters or [])});\n"

    if analyze_tmp_table:
        setup_sql += f"CREATE INDEX {table_name}_idx ON {table_name} ({partition_key});\n"
        setup_sql += f"ANALYZE {table_name};\n"

    return setup_sql, table_name


def _all_partition_keys_sql(parsed_query: exp.Expr, p0_alias: str | None, partition_key: str, sql_dialect: str) -> str | None:
    """Return an SQL array of all partition keys of the tables the lazy restriction applies to.

    It replaces LAZY_ALL_KEYS_PLACEHOLDER, so that a lazy intersection whose cache entries are gone at execution
    time does not restrict the query. The restriction targets the table with alias ``p0_alias``, or with a JOIN
    method and join_every_table (no alias) every table of the FROM and JOIN clauses. Returns None when a target is not a
    plain table (a subquery or a CTE, whose rows a separate statement could not read).
    """
    # The same clauses TMP_TABLE_JOIN joins the key table to
    clauses = _from_clauses(parsed_query)
    if p0_alias is not None:
        clauses = [clause for clause in clauses if clause.alias_or_name == p0_alias][:1]
    targets: list[exp.Expr] = [clause.this for clause in clauses]
    cte_names = {cte.alias_or_name for cte in parsed_query.find_all(exp.CTE)}
    if not targets or any(not isinstance(target, exp.Table) or target.name in cte_names for target in targets):
        return None

    sources = []
    for target in targets:
        table = target.copy()
        table.set("alias", None)
        sources.append(f"SELECT {partition_key} AS v FROM {table.sql(dialect=sql_dialect)}")
    return f"(SELECT array_agg(DISTINCT pcache_all_keys.v) FROM ({' UNION ALL '.join(sources)}) AS pcache_all_keys WHERE pcache_all_keys.v IS NOT NULL)"


def extend_query_with_partition_keys_lazy(
    query: str,
    lazy_subquery: str,
    partition_key: str,
    method: Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"] = "IN_SUBQUERY",
    p0_alias: str | None = None,
    analyze_tmp_table: bool = True,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
    sql_dialect: str = "postgres",
    join_every_table: bool = False,
    planner_collapse_limit: int | None = None,
    fold_key_subqueries: bool | str = False,
    fold_column_resolver: ColumnResolver | None = None,
    fold_assume_inner_columns: bool = False,
    partition_datatype: str | None = None,
) -> str:
    """
    Extend a given SQL query with the cache functionality using a lazy SQL subquery.

    This function takes a lazy SQL subquery (returned from get_partition_keys_lazy) and integrates
    it into the original query using different methods for optimal performance.

    Args:
        query (str): The SQL query to be extended.
        lazy_subquery (str): The SQL subquery that returns partition keys.
        partition_key (str): The identifier for the partition.
        method (Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"]): The
            method to use to extend the query. ``UNLOGGED_TABLE_JOIN`` / ``UNLOGGED_TABLE_IN`` (PostgreSQL only) are
            ``TMP_TABLE_JOIN`` / ``TMP_TABLE_IN`` with an unlogged table, always with index and ANALYZE; the script
            ends with its DROP, after the SELECT. Execute it as ONE string so that it runs in one transaction.
        p0_alias (str | None): The alias of the table to use for the partition key. If None, will be auto-detected.
            When use_p0_table=True, cache restrictions automatically target the p0 table regardless of this parameter.
        analyze_tmp_table (bool): Whether to create index and analyze. (only for temporary table methods)
        auto_detect_partition_join: Whether to auto-detect partition-join tables.
        partition_join_table: Explicitly specified partition-join table alias or name.
        sql_dialect: sqlglot dialect used to parse and render the query. Defaults to PostgreSQL.
        join_every_table: JOIN methods only: join the key table to every table instead of the p0 table; see
            ``extend_query_with_partition_keys``.
        planner_collapse_limit: If set to N, the script begins with ``SET LOCAL join_collapse_limit = N;`` and
            ``SET LOCAL from_collapse_limit = N;``; see ``extend_query_with_partition_keys``. None (default) is off.
        fold_key_subqueries: Key-table methods only: fold key subqueries into the key table; see
            ``extend_query_with_partition_keys``, also for ``fold_column_resolver`` and ``fold_assume_inner_columns``.
        partition_datatype: The cache's datatype of the partition key ("integer", "text", "float", ...), as the lazy
            keys are not known here. For a non-integer datatype the key tables take the key column's type and
            IN_SUBQUERY is guarded against a char(n) / real column, which it would silently miss; folding needs it too.
            None (unknown): neither, the script is built as for integer keys.

    Returns:
        str: The extended SQL query as string.
    """
    _check_planner_collapse_limit(planner_collapse_limit, method, sql_dialect)
    _check_fold_key_subqueries(fold_key_subqueries, method, sql_dialect)
    if not lazy_subquery or not lazy_subquery.strip():
        return query

    parsed_query = sqlglot.parse_one(query, read=sql_dialect)
    p0_alias = _restriction_target(query, parsed_query, partition_key, method, p0_alias, join_every_table, auto_detect_partition_join, partition_join_table)
    key_filters = (
        _fold_key_subqueries(parsed_query, partition_key, p0_alias, sql_dialect, fold_column_resolver, fold_assume_inner_columns)
        if _fold_allowed(fold_key_subqueries, partition_datatype)
        else []
    )

    if LAZY_ALL_KEYS_PLACEHOLDER in lazy_subquery:
        # The handler's subquery reads cache entries that may be evicted before it runs; it then falls back to all keys
        all_keys = _all_partition_keys_sql(parsed_query, p0_alias, partition_key, sql_dialect)
        if all_keys is None:
            logger.debug(
                f"The cache restriction targets no plain table (alias {p0_alias!r}); if its cache entries are evicted before the query runs, "
                "the query returns no rows"
            )
        else:
            lazy_subquery = lazy_subquery.replace(LAZY_ALL_KEYS_PLACEHOLDER, all_keys)

    # Non-integer keys (the cache's datatype): the key tables take the key column's type; IN_SUBQUERY cannot and is
    # guarded against the column types it would silently miss. Integer keys, and an unknown datatype, keep their scripts.
    typed_source = None
    if method in _SCRIPT_METHODS and partition_datatype not in (None, "integer"):
        typed_source = _typed_source(parsed_query, query, partition_key, method, p0_alias, auto_detect_partition_join, partition_join_table, sql_dialect)
    guarded_types = {"text": ("character",), "float": ("real",)}.get(partition_datatype or "")
    if method == "IN_SUBQUERY" and sql_dialect == "postgres" and guarded_types:
        _add_where_condition(parsed_query, sqlglot.parse_one(_key_type_guard_sql(f"{p0_alias}.{partition_key}", guarded_types), read=sql_dialect))

    if method == "IN_SUBQUERY":
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN ({lazy_subquery})", read=sql_dialect)
        _add_where_condition(parsed_query, partition_expr)
        return parsed_query.sql(dialect=sql_dialect)

    elif method == "TMP_TABLE_IN" or method == "UNLOGGED_TABLE_IN":
        # Create the key table (temporary or unlogged) from the lazy subquery, then use IN with SELECT
        if typed_source is not None:
            kind = "UNLOGGED" if method == "UNLOGGED_TABLE_IN" else "TEMPORARY"
            tmp_table_setup, table_name = _typed_key_table_setup(
                kind, partition_key, typed_source, partition_key, lazy_subquery, analyze_tmp_table, sql_dialect, key_filters, method
            )
        elif method == "UNLOGGED_TABLE_IN":
            tmp_table_setup, table_name = _create_unlogged_table_setup(lazy_subquery, partition_key, sql_dialect, method, key_filters)
        else:
            tmp_table_setup, table_name = _create_tmp_table_setup_from_subquery(lazy_subquery, partition_key, analyze_tmp_table, sql_dialect, key_filters)
        partition_expr = sqlglot.parse_one(
            f"{p0_alias}.{partition_key} IN (SELECT {partition_key} FROM {table_name})", read=sql_dialect
        )
        _add_where_condition(parsed_query, partition_expr)
        script = _planner_collapse_limit_sql(planner_collapse_limit) + tmp_table_setup + parsed_query.sql(dialect=sql_dialect)
        if method == "UNLOGGED_TABLE_IN":
            script += _unlogged_table_teardown(table_name)
        return script

    elif method == "TMP_TABLE_JOIN" or method == "UNLOGGED_TABLE_JOIN":
        # Create the key table (temporary or unlogged) from the lazy subquery, then JOIN
        if typed_source is not None:
            kind = "UNLOGGED" if method == "UNLOGGED_TABLE_JOIN" else "TEMPORARY"
            tmp_table_setup, table_name = _typed_key_table_setup(
                kind, partition_key, typed_source, partition_key, lazy_subquery, analyze_tmp_table, sql_dialect, key_filters, method
            )
        elif method == "UNLOGGED_TABLE_JOIN":
            tmp_table_setup, table_name = _create_unlogged_table_setup(lazy_subquery, partition_key, sql_dialect, method, key_filters)
        else:
            tmp_table_setup, table_name = _create_tmp_table_setup_from_subquery(lazy_subquery, partition_key, analyze_tmp_table, sql_dialect, key_filters)

        # Add as inner join to the p0 table, or with join_every_table to every table, of the restricted SELECT only
        for from_clause in _from_clauses(parsed_query):
            # Get alias of the table
            table_alias = from_clause.alias_or_name

            if p0_alias is not None and table_alias != p0_alias:
                continue

            # Create the new join expression using sqlglot
            join_expr = (
                from_clause.this.sql(dialect=sql_dialect)  # Original table
                + " "  # Space between tables
                + exp.Join(
                    this=exp.Identifier(this=f"{table_name} AS tmp_{table_alias}"),
                    on=exp.EQ(
                        this=exp.Identifier(this=f"tmp_{table_alias}.{partition_key}"),
                        expression=exp.Identifier(this=f"{table_alias}.{partition_key}"),
                    ),
                    kind="INNER",
                ).sql(dialect=sql_dialect)  # New join expression
            )

            # Replace the old join expression with the new one
            from_clause.this.replace(join_expr)

        script = _planner_collapse_limit_sql(planner_collapse_limit) + tmp_table_setup + parsed_query.sql(dialect=sql_dialect)
        if method == "UNLOGGED_TABLE_JOIN":
            script += _unlogged_table_teardown(table_name)
        return script

    raise ValueError(f"Unknown method {method!r}")


def extend_query_with_spatial_filter_lazy(
    query: str,
    spatial_filter_sql: str,
    geometry_column: str,
    buffer_distance: float,
    srid: int = 4326,
    p0_alias: str | None = None,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
    use_intersects: bool = False,
    spatial_method: Literal["SUBDIVIDE_INLINE", "SUBDIVIDE_TMP_TABLE", "DUMP_TMP_TABLE", "DUMP_CTE"] = "SUBDIVIDE_TMP_TABLE",
    subdivide_max_vertices: int = 256,
) -> str:
    """
    Extend a SQL query with a spatial filter using ST_DWithin or ST_Intersects.

    Uses ST_Subdivide to break the filter geometry into smaller pieces with tight
    bounding boxes, enabling efficient GiST index usage.

    When ``use_intersects=False`` (default), uses ST_DWithin with buffer_distance:
        For geographic SRIDs (4326): ST_DWithin(...::geography, ...::geography, {dist})
        For metric SRIDs (e.g., 25832): ST_DWithin({alias}.{geom}, ({filter})::geometry, {dist})

    When ``use_intersects=True``, uses ST_Intersects (no additional buffer). Use this
    when the spatial filter geometry already includes the buffer distance (e.g., BBox handler).
        For metric SRIDs: ST_Intersects({alias}.{geom}, ({filter})::geometry)
        For geographic SRIDs (4326): ST_Intersects(...::geography, ...::geography)

    Args:
        query: The original SQL query.
        spatial_filter_sql: SQL subquery returning a geometry to filter against.
        geometry_column: The geometry column name in the target table.
        buffer_distance: Buffer distance in meters. Ignored when use_intersects=True.
        srid: SRID of the spatial data. If 4326, uses geography cast. Otherwise uses geometry directly.
        p0_alias: Table alias to apply the filter to. If None, auto-detected.
        auto_detect_partition_join: Whether to auto-detect partition-join tables for alias detection.
        partition_join_table: Explicitly specified partition-join table alias or name.
        use_intersects: If True, use ST_Intersects instead of ST_DWithin. Use when the
            spatial filter geometry already includes the buffer distance.
        spatial_method: Method for applying the spatial filter:
            - "SUBDIVIDE_TMP_TABLE": Create a temp table with ST_Subdivide + GiST index (default, optimal).
            - "SUBDIVIDE_INLINE": Inline ST_Subdivide in an EXISTS subquery (simpler, no temp table).
            - "DUMP_TMP_TABLE": ST_Dump into temp table + GiST index (no subdivide, preserves original pieces).
            - "DUMP_CTE": ST_Dump in CTE MATERIALIZED (no temp table, no index, simplest approach).
        subdivide_max_vertices: Maximum vertices per subdivided piece (default 256). Lower values
            create more, smaller pieces with tighter bounding boxes for better GiST selectivity.
            Only used by SUBDIVIDE_* methods.

    Returns:
        The extended SQL query with spatial filter.
    """
    if not spatial_filter_sql or not spatial_filter_sql.strip():
        return query

    if p0_alias is None:
        # Find the first table in the query as fallback
        parsed_query = sqlglot.parse_one(query)
        first_table = parsed_query.find(exp.Table)
        if first_table is None:
            raise ValueError("No table found in query")
        p0_alias = first_table.alias_or_name

    geom_expr = f"({spatial_filter_sql})::geometry"

    if spatial_method == "SUBDIVIDE_TMP_TABLE":
        sf_table = f"_pcache_sf_{random.randint(100000, 999999)}"

        if srid == 4326:
            dump_expr = f"ST_Transform({geom_expr}, 4326)"
        else:
            dump_expr = geom_expr

        prep_sql = (
            f"DROP TABLE IF EXISTS {sf_table}; "
            f"CREATE TEMPORARY TABLE {sf_table} ON COMMIT DROP AS "
            f"SELECT ST_Subdivide((ST_Dump({dump_expr})).geom, {subdivide_max_vertices}) AS geom; "
            f"CREATE INDEX ON {sf_table} USING GIST (geom); "
            f"ANALYZE {sf_table}; "
        )

        if use_intersects:
            if srid == 4326:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {sf_table} sf "
                    f"WHERE ST_Intersects(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, sf.geom::geography))"
                )
            else:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {sf_table} sf "
                    f"WHERE ST_Intersects({p0_alias}.{geometry_column}, sf.geom))"
                )
        elif srid == 4326:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {sf_table} sf "
                f"WHERE ST_DWithin(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, sf.geom::geography, {buffer_distance}))"
            )
        else:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {sf_table} sf "
                f"WHERE ST_DWithin({p0_alias}.{geometry_column}, sf.geom, {buffer_distance}))"
            )

        parsed_query = sqlglot.parse_one(query)
        spatial_expr = sqlglot.parse_one(spatial_condition)
        _add_where_condition(parsed_query, spatial_expr)
        return prep_sql + parsed_query.sql()

    elif spatial_method == "DUMP_TMP_TABLE":
        # ST_Dump into temp table + GiST index. No ST_Subdivide — preserves original geometry pieces.
        sf_table = f"_pcache_sf_{random.randint(100000, 999999)}"

        if srid == 4326:
            dump_expr = f"ST_Transform({geom_expr}, 4326)"
        else:
            dump_expr = geom_expr

        prep_sql = (
            f"DROP TABLE IF EXISTS {sf_table}; "
            f"CREATE TEMPORARY TABLE {sf_table} ON COMMIT DROP AS "
            f"SELECT (ST_Dump({dump_expr})).geom AS geom; "
            f"CREATE INDEX ON {sf_table} USING GIST (geom); "
            f"ANALYZE {sf_table}; "
        )

        if use_intersects:
            if srid == 4326:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {sf_table} sf "
                    f"WHERE ST_Intersects(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, sf.geom::geography))"
                )
            else:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {sf_table} sf "
                    f"WHERE ST_Intersects({p0_alias}.{geometry_column}, sf.geom))"
                )
        elif srid == 4326:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {sf_table} sf "
                f"WHERE ST_DWithin(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, sf.geom::geography, {buffer_distance}))"
            )
        else:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {sf_table} sf "
                f"WHERE ST_DWithin({p0_alias}.{geometry_column}, sf.geom, {buffer_distance}))"
            )

        parsed_query = sqlglot.parse_one(query)
        spatial_expr = sqlglot.parse_one(spatial_condition)
        _add_where_condition(parsed_query, spatial_expr)
        return prep_sql + parsed_query.sql()

    elif spatial_method == "DUMP_CTE":
        # ST_Dump in CTE MATERIALIZED — no temp table, no GiST index.
        # PostgreSQL materializes the CTE and uses it as a scan source.
        cte_name = f"_pcache_sf_{random.randint(100000, 999999)}"

        if srid == 4326:
            dump_expr = f"ST_Transform({geom_expr}, 4326)"
        else:
            dump_expr = geom_expr

        cte_sql = f"WITH {cte_name} AS MATERIALIZED (SELECT (ST_Dump({dump_expr})).geom AS geom) "

        if use_intersects:
            if srid == 4326:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {cte_name} sf "
                    f"WHERE ST_Intersects(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, sf.geom::geography))"
                )
            else:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {cte_name} sf "
                    f"WHERE ST_Intersects({p0_alias}.{geometry_column}, sf.geom))"
                )
        elif srid == 4326:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {cte_name} sf "
                f"WHERE ST_DWithin(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, sf.geom::geography, {buffer_distance}))"
            )
        else:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {cte_name} sf "
                f"WHERE ST_DWithin({p0_alias}.{geometry_column}, sf.geom, {buffer_distance}))"
            )

        parsed_query = sqlglot.parse_one(query)
        spatial_expr = sqlglot.parse_one(spatial_condition)
        _add_where_condition(parsed_query, spatial_expr)
        return cte_sql + parsed_query.sql()

    else:  # SUBDIVIDE_INLINE
        # Use ST_Subdivide directly (handles multi-geometries internally).
        # Cannot chain ST_Dump + ST_Subdivide as nested SRFs in FROM clause.
        if srid == 4326:
            subdivide_expr = f"ST_Subdivide(ST_Transform({geom_expr}, 4326), {subdivide_max_vertices})"
        else:
            subdivide_expr = f"ST_Subdivide({geom_expr}, {subdivide_max_vertices})"

        if use_intersects:
            if srid == 4326:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {subdivide_expr} AS _pcache_sf "
                    f"WHERE ST_Intersects(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, _pcache_sf::geography))"
                )
            else:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {subdivide_expr} AS _pcache_sf "
                    f"WHERE ST_Intersects({p0_alias}.{geometry_column}, _pcache_sf))"
                )
        elif srid == 4326:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {subdivide_expr} AS _pcache_sf "
                f"WHERE ST_DWithin(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, _pcache_sf::geography, {buffer_distance}))"
            )
        else:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {subdivide_expr} AS _pcache_sf "
                f"WHERE ST_DWithin({p0_alias}.{geometry_column}, _pcache_sf, {buffer_distance}))"
            )

        parsed_query = sqlglot.parse_one(query)
        spatial_expr = sqlglot.parse_one(spatial_condition)
        _add_where_condition(parsed_query, spatial_expr)
        return parsed_query.sql()


def extend_query_with_spatial_filter(
    query: str,
    spatial_filter_wkb: bytes,
    geometry_column: str,
    buffer_distance: float,
    srid: int = 4326,
    p0_alias: str | None = None,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
    use_intersects: bool = False,
    spatial_method: Literal["SUBDIVIDE_INLINE", "SUBDIVIDE_TMP_TABLE", "DUMP_TMP_TABLE", "DUMP_CTE"] = "SUBDIVIDE_TMP_TABLE",
    subdivide_max_vertices: int = 256,
) -> str:
    """
    Extend a SQL query with a pre-computed spatial filter geometry (WKB bytes).

    Non-lazy counterpart to extend_query_with_spatial_filter_lazy(). Instead of embedding
    a SQL subquery, this uses a WKB literal from a pre-executed spatial filter.

    Uses ST_Subdivide to break the filter geometry into smaller pieces with tight
    bounding boxes, enabling efficient GiST index usage.

    When ``use_intersects=False`` (default), uses ST_DWithin with buffer_distance.
    When ``use_intersects=True``, uses ST_Intersects (no additional buffer). Use this
    when the spatial filter geometry already includes the buffer distance.

    Args:
        query: The original SQL query.
        spatial_filter_wkb: Pre-computed spatial filter geometry as WKB bytes.
        geometry_column: The geometry column name in the target table.
        buffer_distance: Buffer distance in meters. Ignored when use_intersects=True.
        srid: SRID of the spatial filter geometry (default: 4326). If 4326, uses geography cast.
        p0_alias: Table alias to apply the filter to. If None, auto-detected.
        auto_detect_partition_join: Whether to auto-detect partition-join tables for alias detection.
        partition_join_table: Explicitly specified partition-join table alias or name.
        use_intersects: If True, use ST_Intersects instead of ST_DWithin. Use when the
            spatial filter geometry already includes the buffer distance.
        spatial_method: Method for applying the spatial filter:
            - "SUBDIVIDE_TMP_TABLE": Create a temp table with ST_Subdivide + GiST index (default, optimal).
            - "SUBDIVIDE_INLINE": Inline ST_Subdivide in an EXISTS subquery (simpler, no temp table).
        subdivide_max_vertices: Maximum vertices per subdivided piece (default 256). Lower values
            create more, smaller pieces with tighter bounding boxes for better GiST selectivity.

    Returns:
        The extended SQL query with spatial filter.
    """
    if not spatial_filter_wkb:
        return query

    if p0_alias is None:
        parsed_query = sqlglot.parse_one(query)
        first_table = parsed_query.find(exp.Table)
        if first_table is None:
            raise ValueError("No table found in query")
        p0_alias = first_table.alias_or_name

    # Convert WKB bytes to hex string for SQL embedding
    wkb_hex = spatial_filter_wkb.hex()
    wkb_geom = f"ST_GeomFromWKB('\\x{wkb_hex}', {srid})"

    if spatial_method == "SUBDIVIDE_TMP_TABLE":
        sf_table = f"_pcache_sf_{random.randint(100000, 999999)}"

        if srid == 4326:
            dump_expr = f"ST_Transform({wkb_geom}::geometry, 4326)"
        else:
            dump_expr = f"{wkb_geom}::geometry"

        prep_sql = (
            f"DROP TABLE IF EXISTS {sf_table}; "
            f"CREATE TEMPORARY TABLE {sf_table} ON COMMIT DROP AS "
            f"SELECT ST_Subdivide((ST_Dump({dump_expr})).geom, {subdivide_max_vertices}) AS geom; "
            f"CREATE INDEX ON {sf_table} USING GIST (geom); "
            f"ANALYZE {sf_table}; "
        )

        if use_intersects:
            if srid == 4326:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {sf_table} sf "
                    f"WHERE ST_Intersects(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, sf.geom::geography))"
                )
            else:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {sf_table} sf "
                    f"WHERE ST_Intersects({p0_alias}.{geometry_column}, sf.geom))"
                )
        elif srid == 4326:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {sf_table} sf "
                f"WHERE ST_DWithin(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, sf.geom::geography, {buffer_distance}))"
            )
        else:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {sf_table} sf "
                f"WHERE ST_DWithin({p0_alias}.{geometry_column}, sf.geom, {buffer_distance}))"
            )

        parsed_query = sqlglot.parse_one(query)
        spatial_expr = sqlglot.parse_one(spatial_condition)
        _add_where_condition(parsed_query, spatial_expr)
        return prep_sql + parsed_query.sql()

    else:  # SUBDIVIDE_INLINE
        # Use ST_Subdivide directly (handles multi-geometries internally).
        # Cannot chain ST_Dump + ST_Subdivide as nested SRFs in FROM clause.
        if srid == 4326:
            subdivide_expr = f"ST_Subdivide(ST_Transform({wkb_geom}::geometry, 4326), {subdivide_max_vertices})"
        else:
            subdivide_expr = f"ST_Subdivide({wkb_geom}::geometry, {subdivide_max_vertices})"

        if use_intersects:
            if srid == 4326:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {subdivide_expr} AS _pcache_sf "
                    f"WHERE ST_Intersects(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, _pcache_sf::geography))"
                )
            else:
                spatial_condition = (
                    f"EXISTS (SELECT 1 FROM {subdivide_expr} AS _pcache_sf "
                    f"WHERE ST_Intersects({p0_alias}.{geometry_column}, _pcache_sf))"
                )
        elif srid == 4326:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {subdivide_expr} AS _pcache_sf "
                f"WHERE ST_DWithin(ST_Transform({p0_alias}.{geometry_column}, 4326)::geography, _pcache_sf::geography, {buffer_distance}))"
            )
        else:
            spatial_condition = (
                f"EXISTS (SELECT 1 FROM {subdivide_expr} AS _pcache_sf "
                f"WHERE ST_DWithin({p0_alias}.{geometry_column}, _pcache_sf, {buffer_distance}))"
            )

        parsed_query = sqlglot.parse_one(query)
        spatial_expr = sqlglot.parse_one(spatial_condition)
        _add_where_condition(parsed_query, spatial_expr)
        return parsed_query.sql()


def extend_query_with_h3_cell_filter_lazy(
    query: str,
    cell_sql: str,
    geometry_column: str,
    srid: int,
    resolution: int,
    p0_alias: str | None = None,
) -> str:
    """
    Extend a SQL query with an H3 cell membership filter (lazy/SQL-based).

    Instead of geometry reconstruction (ST_Intersects/ST_DWithin), uses pure H3 cell
    lookup: h3_lat_lng_to_cell(centroid, resolution)::bigint IN (SELECT cell FROM tmp_table).

    Creates a temp table from the cell SQL, indexes it, and adds a WHERE condition.

    Args:
        query: The original SQL query.
        cell_sql: SQL query producing rows of (cell BIGINT) — the allowed H3 cells.
        geometry_column: The geometry column name in the target table.
        srid: SRID of the geometry data. If != 4326, ST_Transform is applied before h3_lat_lng_to_cell.
        resolution: H3 resolution level (0-15).
        p0_alias: Table alias to apply the filter to. If None, auto-detected.

    Returns:
        The extended SQL query with H3 cell membership filter.
    """
    if not cell_sql or not cell_sql.strip():
        return query

    if p0_alias is None:
        parsed_query = sqlglot.parse_one(query)
        first_table = parsed_query.find(exp.Table)
        if first_table is None:
            raise ValueError("No table found in query")
        p0_alias = first_table.alias_or_name

    tmp_table = f"_pcache_h3_cells_{random.randint(100000, 999999)}"

    # Build centroid expression with SRID handling
    if srid != 4326:
        centroid_expr = f"ST_Transform(ST_Centroid({p0_alias}.{geometry_column}), 4326)::point"
    else:
        centroid_expr = f"ST_Centroid({p0_alias}.{geometry_column})::point"

    prep_sql = (
        f"DROP TABLE IF EXISTS {tmp_table}; "
        f"CREATE TEMPORARY TABLE {tmp_table} ON COMMIT DROP AS {cell_sql}; "
        f"CREATE INDEX ON {tmp_table} USING btree (cell); "
        f"ANALYZE {tmp_table}; "
    )

    spatial_condition = f"h3_lat_lng_to_cell({centroid_expr}, {resolution})::bigint IN (SELECT cell FROM {tmp_table})"

    parsed_query = sqlglot.parse_one(query)
    spatial_expr = sqlglot.parse_one(spatial_condition)
    _add_where_condition(parsed_query, spatial_expr)
    return prep_sql + parsed_query.sql()


def extend_query_with_h3_cell_filter(
    query: str,
    cell_ids: set[int],
    geometry_column: str,
    srid: int,
    resolution: int,
    p0_alias: str | None = None,
) -> str:
    """
    Extend a SQL query with an H3 cell membership filter (non-lazy/pre-computed).

    Same as extend_query_with_h3_cell_filter_lazy but uses pre-computed cell IDs
    instead of a SQL subquery. Creates a temp table from VALUES clause.

    Args:
        query: The original SQL query.
        cell_ids: Set of H3 cell IDs (as bigint) to filter against.
        geometry_column: The geometry column name in the target table.
        srid: SRID of the geometry data. If != 4326, ST_Transform is applied before h3_lat_lng_to_cell.
        resolution: H3 resolution level (0-15).
        p0_alias: Table alias to apply the filter to. If None, auto-detected.

    Returns:
        The extended SQL query with H3 cell membership filter.
    """
    if not cell_ids:
        return query

    if p0_alias is None:
        parsed_query = sqlglot.parse_one(query)
        first_table = parsed_query.find(exp.Table)
        if first_table is None:
            raise ValueError("No table found in query")
        p0_alias = first_table.alias_or_name

    tmp_table = f"_pcache_h3_cells_{random.randint(100000, 999999)}"

    # Build centroid expression with SRID handling
    if srid != 4326:
        centroid_expr = f"ST_Transform(ST_Centroid({p0_alias}.{geometry_column}), 4326)::point"
    else:
        centroid_expr = f"ST_Centroid({p0_alias}.{geometry_column})::point"

    # Build VALUES clause for cell IDs
    values_list = ", ".join(f"({cell_id}::bigint)" for cell_id in sorted(cell_ids))

    prep_sql = (
        f"DROP TABLE IF EXISTS {tmp_table}; "
        f"CREATE TEMPORARY TABLE {tmp_table} ON COMMIT DROP AS SELECT cell FROM (VALUES {values_list}) AS v(cell); "
        f"CREATE INDEX ON {tmp_table} USING btree (cell); "
        f"ANALYZE {tmp_table}; "
    )

    spatial_condition = f"h3_lat_lng_to_cell({centroid_expr}, {resolution})::bigint IN (SELECT cell FROM {tmp_table})"

    parsed_query = sqlglot.parse_one(query)
    spatial_expr = sqlglot.parse_one(spatial_condition)
    _add_where_condition(parsed_query, spatial_expr)
    return prep_sql + parsed_query.sql()


def extend_query_with_h3_cell_lookup(
    query: str,
    cell_ids: set[int],
    cell_mode: str = "inline",
    h3_cell_table: str | None = None,
    h3_cell_column: str = "h3_cell_id",
    h3_cell_id_column: str | None = None,
    p0_alias: str | None = None,
    geometry_column: str | None = None,
    srid: int = 4326,
    resolution: int = 9,
    filter_all_tables: bool = True,
) -> str:
    """
    Filter query via H3 cell ID lookup. Supports MV, direct column, and inline modes.

    For cross-join queries (e.g., ``FROM pois p1, pois p2``), filtering only p0 leaves
    the planner without cardinality info for other tables, often causing bad join order
    choices. When ``filter_all_tables=True`` (default), all table aliases referencing
    the same base table as p0 are filtered, giving the planner accurate cardinality
    estimates for all sides of the join.

    Args:
        query: The original SQL query.
        cell_ids: Set of H3 cell IDs to filter against.
        cell_mode: Lookup mode — "mv" (materialized view JOIN), "column" (direct column
            on fact table), or "inline" (h3_lat_lng_to_cell per row, no pre-computation).
        h3_cell_table: Table/view containing pre-computed H3 cell IDs (required for "mv").
        h3_cell_column: H3 cell column name in the lookup table or fact table.
        h3_cell_id_column: Entity ID column in the MV, matching the fact table PK (required for "mv").
        p0_alias: Table alias to apply filter to. If None, auto-detected.
        geometry_column: Geometry column name (required for "inline" mode).
        srid: SRID of the geometry data.
        resolution: H3 resolution level.
        filter_all_tables: If True (default), apply H3 cell filter to ALL table aliases
            that reference the same base table as p0. Critical for cross-join performance.

    Returns:
        The extended SQL query with H3 cell filtering.
    """
    if not cell_ids:
        return query

    if cell_mode == "inline":
        if geometry_column is None:
            raise ValueError("geometry_column required for inline H3 cell mode")
        return extend_query_with_h3_cell_filter(
            query=query,
            cell_ids=cell_ids,
            geometry_column=geometry_column,
            srid=srid,
            resolution=resolution,
            p0_alias=p0_alias,
        )

    parsed_query = sqlglot.parse_one(query)

    if p0_alias is None:
        first_table = parsed_query.find(exp.Table)
        if first_table is None:
            raise ValueError("No table found in query")
        p0_alias = first_table.alias_or_name

    # Determine which table aliases to filter
    aliases_to_filter = [p0_alias]
    if filter_all_tables:
        # Find the base table name for p0
        p0_base_table = None
        for table in parsed_query.find_all(exp.Table):
            if table.alias_or_name == p0_alias:
                p0_base_table = table.name
                break

        if p0_base_table:
            # Find all other aliases referencing the same base table
            for table in parsed_query.find_all(exp.Table):
                alias = table.alias_or_name
                if alias != p0_alias and table.name == p0_base_table:
                    aliases_to_filter.append(alias)
            if len(aliases_to_filter) > 1:
                logger.info(f"H3 cell filter: filtering {len(aliases_to_filter)} table aliases: {aliases_to_filter}")

    tmp_table = f"_pcache_h3_cells_{random.randint(100000, 999999)}"

    # Build VALUES clause for cell IDs
    values_list = ", ".join(f"({cell_id}::bigint)" for cell_id in sorted(cell_ids))

    prep_sql = (
        f"DROP TABLE IF EXISTS {tmp_table}; "
        f"CREATE TEMPORARY TABLE {tmp_table} ON COMMIT DROP AS SELECT cell FROM (VALUES {values_list}) AS v(cell); "
        f"CREATE INDEX ON {tmp_table} USING btree (cell); "
        f"ANALYZE {tmp_table}; "
    )

    if cell_mode == "mv":
        if h3_cell_table is None:
            raise ValueError("h3_cell_table required for mv H3 cell mode")
        if h3_cell_id_column is None:
            raise ValueError("h3_cell_id_column required for mv H3 cell mode")

    # Add H3 cell filter for each alias
    for alias in aliases_to_filter:
        if cell_mode == "mv":
            spatial_condition = (
                f"{alias}.{h3_cell_id_column} IN ("
                f"SELECT {h3_cell_id_column} FROM {h3_cell_table} "
                f"WHERE {h3_cell_column} IN (SELECT cell FROM {tmp_table})"
                f")"
            )
        elif cell_mode == "column":
            spatial_condition = f"{alias}.{h3_cell_column} IN (SELECT cell FROM {tmp_table})"
        else:
            raise ValueError(f"Unsupported H3 cell mode: {cell_mode}. Use 'mv', 'column', or 'inline'.")

        spatial_expr = sqlglot.parse_one(spatial_condition)
        _add_where_condition(parsed_query, spatial_expr)

    return prep_sql + parsed_query.sql()


def _check_cost_gate(query: str, cost_estimator: Callable[[str], float | None] | None, min_query_cost: float | None) -> tuple[bool, dict[str, int]]:
    """Decide whether the planner cost of ``query`` is too low for the cache to pay off.

    Returns ``(gated, gate_stats)``. ``gate_stats`` is empty when the gate is not configured; otherwise it holds
    ``cost_gated`` (0/1) and, when the estimator produced a value, ``query_cost`` (rounded). A failing or cost-less
    estimator never blocks the cache: the gate is a performance optimisation, not a correctness check.
    """
    if min_query_cost is None:
        return False, {}
    if cost_estimator is None:
        raise ValueError("min_query_cost requires a cost_estimator (e.g. PostgresDBHandler.estimate_cost)")
    try:
        raw_cost: Any = cost_estimator(query)  # custom estimators may return Decimal, numpy numbers or junk
        cost: Any = raw_cost
    except Exception as e:
        logger.warning(f"Query cost estimation failed, applying the cache anyway: {e}")
        return False, {"cost_gated": 0}
    try:
        valid = not isinstance(cost, bool) and isinstance(cost, numbers.Real | Decimal) and math.isfinite(cost)
        cost = float(cost) if valid else None
    except (ValueError, OverflowError):  # Decimal('sNaN'), integers beyond the float range
        cost = None
    if cost is None:
        if raw_cost is not None:
            logger.warning(f"Query cost estimator returned {raw_cost!r}, not a finite number; applying the cache anyway")
        return False, {"cost_gated": 0}
    gated = cost < min_query_cost
    if gated:
        logger.info(f"Planner cost {cost:.0f} is below min_query_cost {min_query_cost:.0f}; cache not applied")
    return gated, {"cost_gated": int(gated), "query_cost": round(cost)}


# ParamSpec instead of PEP 695 type parameters: the package supports Python 3.11
_P = ParamSpec("_P")


def _apply_grid_cache(
    query: str,
    cache_handler: AbstractCacheHandler,
    partition_key: str,
    geometry_column: str,
    grid_cell_size: float,
    grid_method: str,
    p0_alias: str | None,
    gate_stats: dict[str, int],
    hash_kwargs: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Grid-cell spatial key (``partitioncache.spatial_grid``): eager lookup, sound 2-D widening, rewrite.

    The cached sets are fetched and widened in Python (widen the smallest, probe the rest); the cell set then
    enters the query as a star join over neighbour pair tables (CELL_STAR), as a constant array on every
    fact-table alias (CELL_ARRAY) or on the anchor only (CELL_ARRAY_ANCHOR); CELL_AUTO picks between the
    star and the anchor-only array from the query's predicates.
    """
    from partitioncache import spatial_grid

    if grid_method not in spatial_grid.GRID_METHODS:
        raise ValueError(f"grid_method must be one of {spatial_grid.GRID_METHODS}, got {grid_method!r}")
    # Plan first: a query without a sound cell restriction (an alias not bounded by a top-level ST_DWithin, a CTE,
    # a derived table) runs uncached without paying for the lookup
    try:
        anchor = p0_alias or spatial_grid.outer_tables(spatial_grid.outer_select(query))[0][0]
        plan = spatial_grid.plan_query(query, anchor, geometry_column)
    except spatial_grid.GridPlanError as e:
        logger.info(f"Grid key {partition_key}: {e}; running uncached")
        return query, _uncached_stats(str(e), gate_stats)
    hashes = generate_all_hashes(
        query=query,
        partition_key=partition_key,
        geometry_column=geometry_column,
        grid_cell_size=grid_cell_size,
        **spatial_grid.GRID_GENERATION_KWARGS,
        **hash_kwargs,
    )
    stats: dict[str, Any] = {"generated_variants": len(hashes), "cache_hits": 0, "enhanced": 0, "p0_rewritten": 0, **gate_stats}
    if not hashes:
        return query, stats
    existing = sorted(cache_handler.filter_existing_keys(set(hashes), partition_key))
    sets = [v for v in (cache_handler.get(h, partition_key) for h in existing) if v is not None]
    stats["cache_hits"] = len(sets)
    if not sets:
        return query, stats

    offsets = spatial_grid.stencil(plan.diameter, grid_cell_size)
    if not spatial_grid.cells_in_packing_range(sets, max(max(abs(dx), abs(dy)) for dx, dy in offsets)):  # type: ignore[arg-type]
        logger.warning("Grid cells outside the packing range (negative or too close to the j limit); running uncached")
        return query, _uncached_stats("grid cells outside the packing range", gate_stats)
    cells = spatial_grid.intersect_widened(sets, offsets)  # type: ignore[arg-type]
    stats["grid_cells"] = len(cells)
    stats["enhanced"] = 1
    if len(cells) == 0:
        return _add_empty_intersection_condition(query), stats
    if grid_method == "CELL_AUTO":
        grid_method = spatial_grid.choose_grid_method(query, plan)
    stats["grid_method"] = grid_method
    if grid_method == "CELL_STAR":
        setup, star_query = spatial_grid.rewrite_cell_star(query, cells, plan, geometry_column, grid_cell_size)
        return "; ".join([*setup, star_query]), stats
    aliases = plan.aliases if grid_method == "CELL_ARRAY" else [plan.anchor]
    return spatial_grid.rewrite_cell_array(query, cells, aliases, geometry_column, grid_cell_size), stats



def _runs_uncached_on_sql_errors(func: Callable[_P, tuple[str, dict[str, int]]]) -> Callable[_P, tuple[str, dict[str, int]]]:
    """Return the query unchanged when the cache cannot process its SQL, instead of failing the query.

    The cache is an optimization: SQL that the variant generation or the rewrite cannot parse or handle makes the
    query run without the cache, with a warning and ``sql_error: 1`` in the statistics. This covers sqlglot errors,
    RecursionError (very deeply nested SQL, e.g. thousands of conjuncts) and IndexError (SQL shapes the variant
    generator does not expect, e.g. no FROM clause). Configuration errors (ValueError, e.g. a handler without lazy
    support) and errors of the cache backend still raise.
    """

    @functools.wraps(func)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> tuple[str, dict[str, int]]:
        try:
            return func(*args, **kwargs)
        except (sqlglot.errors.SqlglotError, RecursionError, IndexError) as e:
            query = kwargs["query"] if "query" in kwargs else args[0]
            first_line = str(e).splitlines()[0] if str(e) else ""
            logger.warning(f"{func.__name__}: the query cannot be processed ({type(e).__name__}: {first_line}); it runs without the cache")
            return str(query), {"generated_variants": 0, "cache_hits": 0, "enhanced": 0, "p0_rewritten": 0, "sql_error": 1}

    return wrapper


def _accepts_regex_operators(fn: Callable[_P, tuple[str, dict[str, int]]]) -> Callable[_P, tuple[str, dict[str, int]]]:
    """Run ``fn`` on the query with PostgreSQL's regex match operators rewritten (:func:`normalize_regex_operators`).

    When ``fn`` returns the query unchanged, for example without cache hits, the caller's own text is returned, so
    a query the cache does not change keeps its operators (``REGEXP_LIKE`` needs PostgreSQL 15+).
    """

    @functools.wraps(fn)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> tuple[str, dict[str, int]]:
        original = kwargs["query"] if "query" in kwargs else args[0]
        normalized = normalize_regex_operators(original)  # type: ignore[arg-type]
        if "query" in kwargs:
            kwargs["query"] = normalized
        else:
            args = (normalized, *args[1:])  # type: ignore[assignment]
        enhanced_query, stats = fn(*args, **kwargs)
        return (original if enhanced_query == normalized else enhanced_query), stats  # type: ignore[return-value]

    return wrapper


@_runs_uncached_on_sql_errors
@_accepts_regex_operators
def apply_cache_lazy(
    query: str,
    cache_handler: AbstractCacheHandler_Lazy,
    partition_key: str,
    method: Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"] = "IN_SUBQUERY",
    p0_alias: str | None = None,
    min_component_size: int = 2,
    canonicalize_queries: bool = False,
    follow_graph: bool = True,
    analyze_tmp_table: bool = True,
    use_p0_table: bool = False,
    p0_table_name: str | None = None,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
    bucket_steps: float | dict[str, float] | list = 1.0,
    add_constraints: dict[str, str] | None = None,
    remove_constraints_all: list[str] | None = None,
    remove_constraints_add: list[str] | None = None,
    skip_partition_key_joins: bool = False,
    geometry_column: str | None = None,
    buffer_distance: float | None = None,
    spatial_method: Literal["SUBDIVIDE_INLINE", "SUBDIVIDE_TMP_TABLE", "DUMP_TMP_TABLE", "DUMP_CTE"] = "SUBDIVIDE_TMP_TABLE",
    subdivide_max_vertices: int = 256,
    max_conditions_removed: int | None = None,
    protected_patterns: list[str] | None = None,
    temporal_bucket_width: float | None = None,
    dilation_buckets: int | None = None,
    temporal_column: str | None = None,
    sql_dialect: str = "postgres",
    partition_key_source_table: str | None = None,
    cost_estimator: Callable[[str], float | None] | None = None,
    grid_cell_size: float | None = None,
    grid_method: str = "CELL_AUTO",
    min_query_cost: float | None = None,
    keep_all_attributes: bool = False,
    join_every_table: bool = False,
    planner_collapse_limit: int | None = None,
    fold_key_subqueries: bool | str = False,
    fold_column_resolver: ColumnResolver | None = None,
    fold_assume_inner_columns: bool = False,
    **kwargs: Any,
) -> tuple[str, dict[str, int]]:
    """
    Complete wrapper function that applies partition cache to a query using lazy intersection.

    Args:
        query (str): The original SQL query to be enhanced with cache functionality.
        cache_handler (AbstractCacheHandler_Lazy): The lazy cache handler instance.
        partition_key (str): The identifier for the partition.
        method (Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"]): The method to
            use for query extension. An ``UNLOGGED_TABLE_*`` script ends with the DROP of its key table; execute it as ONE string.
        p0_alias (str | None): For regular queries: table alias for cache restrictions.
                               For p0 queries: alias name for the p0 table (defaults to "p0").
        min_component_size (int): Minimum size of query components to consider for cache lookup.
        canonicalize_queries (bool): Whether to canonicalize queries before hashing.
        follow_graph (bool): Whether to follow the query graph for generating variants.
        analyze_tmp_table (bool): Whether to create index and analyze for temporary table methods.
        use_p0_table (bool): Whether to rewrite the query to use a p0 table (star-schema).
        p0_table_name (str | None): Name of the p0 table. Defaults to {partition_key}_mv.
        auto_detect_partition_join: Whether to auto-detect partition-join tables.
        partition_join_table: Explicitly specified partition-join table alias or name.
        bucket_steps: Step size for normalizing distance conditions (e.g., 1.0, 0.5, etc.)
        add_constraints: Dict mapping table names to constraints to add (e.g., {"table": "col = val"})
        remove_constraints_all: List of attribute names to remove from all query variants
        remove_constraints_add: List of attribute names to remove, creating additional variants
        protected_patterns: Conditions matching these patterns are never removed during variant generation.
        temporal_bucket_width: Width of one bucket of a time-bucket partition key, in the unit of ``temporal_column``;
            enables temporal dilation: each cached set is widened by ceil(window / width) buckets before the
            intersection, with the window read from the query's conditions on ``temporal_column``.
        dilation_buckets: Number of buckets to widen by, instead of the window (0 turns dilation off).
        temporal_column: The time column the partition key buckets; required with ``temporal_bucket_width``.
            With either temporal parameter, variants are generated without partition key joins, and a query
            whose rows are not bounded in time against each other runs without the cache.
        keep_all_attributes: If False (default), lookup also probes variants with single-table attribute
            conditions removed (more hits, more variant-generation cost). True probes exactly the variants
            that generate_all_query_hash_pairs produces with its defaults.
        join_every_table: TMP_TABLE_JOIN and UNLOGGED_TABLE_JOIN only: join the key table to every table instead of
            the p0 table; raises ValueError unless every table is joined on the partition key. See
            extend_query_with_partition_keys.
        planner_collapse_limit: If set to N, a rewritten script begins with ``SET LOCAL join_collapse_limit = N;`` and
            ``SET LOCAL from_collapse_limit = N;`` (``geqo`` is not touched). Needs a script method (TMP_TABLE_*,
            UNLOGGED_TABLE_*) and PostgreSQL, else ValueError (checked before the lookup). Not applied when the query
            is returned unchanged, nor to the spatial and grid paths. None (default) is off.
        sql_dialect: sqlglot dialect used to parse and render non-spatial queries. Defaults to PostgreSQL.
        geometry_column: If set, enables spatial cache mode. Uses this geometry column for variant
            SELECT clauses and spatial filter application. Requires a spatial cache handler with
            get_spatial_filter_lazy() method.
        buffer_distance: Buffer distance in meters for spatial filter. If None and geometry_column
            is set, auto-derived from distance constraints in the query (graph diameter); 0 for a
            query over a single table, whose rows lie in every cached set. Required when
            geometry_column is set and a query over several tables has no distance constraints.
        spatial_method: Method for applying the spatial filter:
            - "SUBDIVIDE_TMP_TABLE": Create a temp table with ST_Subdivide + GiST index (default, optimal).
            - "SUBDIVIDE_INLINE": Inline ST_Subdivide in an EXISTS subquery (simpler, no temp table).
        cost_estimator: Callable returning the planner's cost estimate for a query without executing it, or None
            when no estimate is available (e.g. ``PostgresDBHandler.estimate_cost``). Must run against the data
            database, which is not necessarily the cache database. Only used together with ``min_query_cost``.
        min_query_cost: If set, the cache is skipped when the estimated cost of the original query is below this
            value: for cheap queries the cache overhead exceeds the possible saving. The check runs before variant
            generation, so a gated query costs one planning pass only. Cost units are installation-specific
            (``seq_page_cost``, ``random_page_cost``) and have to be calibrated. A failing estimator or one that
            returns None never blocks the cache. Requires ``cost_estimator``.

    Returns:
        tuple[str, dict[str, int]]: Enhanced query and statistics (same keys as ``apply_cache``).
    """
    # Handle deprecated kwargs
    deprecated = handle_deprecated_kwargs(kwargs, "apply_cache_lazy")
    if "partition_join_table" in deprecated:
        if partition_join_table is not None:
            raise TypeError("Cannot pass both 'partition_join_table' and deprecated 'star_join_table'")
        partition_join_table = deprecated["partition_join_table"]
    if "auto_detect_partition_join" in deprecated:
        auto_detect_partition_join = deprecated["auto_detect_partition_join"]
    _check_fold_key_subqueries(fold_key_subqueries, method, sql_dialect)
    _check_planner_collapse_limit(planner_collapse_limit, method, sql_dialect)

    gated, gate_stats = _check_cost_gate(query, cost_estimator, min_query_cost)
    if gated:
        return query, {"generated_variants": 0, "cache_hits": 0, "enhanced": 0, "p0_rewritten": 0, **gate_stats}

    if _is_temporal(temporal_bucket_width, dilation_buckets):
        _, uncached_reason = _temporal_lookup(
            query, cache_handler, partition_key, temporal_bucket_width, dilation_buckets, temporal_column, partition_key_source_table, lazy=True
        )
        if uncached_reason is not None:
            return query, _uncached_stats(uncached_reason, gate_stats)

    if grid_cell_size is not None:
        if geometry_column is None:
            raise ValueError("grid_cell_size requires geometry_column")
        return _apply_grid_cache(
            query, cache_handler, partition_key, geometry_column, grid_cell_size, grid_method, p0_alias, gate_stats,
            {"partition_key_source_table": partition_key_source_table, "min_component_size": min_component_size,
             "fix_attributes": keep_all_attributes, "canonicalize_queries": canonicalize_queries, "follow_graph": follow_graph,
             "partition_join_table": partition_join_table, "bucket_steps": bucket_steps, "add_constraints": add_constraints,
             "remove_constraints_all": remove_constraints_all, "remove_constraints_add": remove_constraints_add,
             "max_conditions_removed": max_conditions_removed, "protected_patterns": protected_patterns},
        )

    # Spatial mode is selected by passing a geometry column
    if geometry_column is not None:
        # Spatial path: generate hashes once, use get_spatial_filter_lazy() directly.
        # Unlike the non-spatial path, we skip get_partition_keys_lazy() / get_intersected_lazy()
        # because spatial handlers need get_spatial_filter_lazy() (buffered geometric intersection),
        # not partition key intersection (cell ID / bbox intersection).
        hashes = generate_all_hashes(
            partition_key_source_table=partition_key_source_table,
            query=query,
            partition_key=partition_key,
            min_component_size=min_component_size,
            fix_attributes=keep_all_attributes,
            canonicalize_queries=canonicalize_queries,
            follow_graph=follow_graph,
            auto_detect_partition_join=False,
            partition_join_table=partition_join_table,
            bucket_steps=bucket_steps,
            add_constraints=add_constraints,
            remove_constraints_all=remove_constraints_all,
            remove_constraints_add=remove_constraints_add,
            skip_partition_key_joins=True,
            geometry_column=geometry_column,
            max_conditions_removed=max_conditions_removed,
            protected_patterns=protected_patterns,
        )

        generated_variants = len(hashes)
        stats: dict[str, int] = {"generated_variants": generated_variants, "cache_hits": 0, "enhanced": 0, "p0_rewritten": 0, **gate_stats}
        if not hashes:
            return query, stats

        existing_hashes = set(cache_handler.filter_existing_keys(set(hashes), partition_key))
        stats["cache_hits"] = len(existing_hashes)
        if not existing_hashes:
            return query, stats

        buffer_distance = _resolve_buffer_distance(query, buffer_distance)

        srid = getattr(cache_handler, "srid", 4326)
        spatial_filter_type = getattr(cache_handler, "spatial_filter_type", "geometry")

        if spatial_filter_type == "h3_cell_ids":
            # Pure H3 cell IDs path — k-ring expansion in Python, no PostGIS at query time
            if not hasattr(cache_handler, "get_h3_cell_filter"):
                raise ValueError("Cache handler does not support H3 cell filtering (missing get_h3_cell_filter method)")

            cell_ids = cache_handler.get_h3_cell_filter(  # type: ignore[attr-defined]
                keys=existing_hashes,
                partition_key=partition_key,
                buffer_distance=buffer_distance,
            )
            if not cell_ids:
                return query, stats

            enhanced_query = extend_query_with_h3_cell_lookup(
                query=query,
                cell_ids=cell_ids,
                cell_mode=getattr(cache_handler, "h3_cell_mode", "inline"),
                h3_cell_table=getattr(cache_handler, "h3_cell_table", None),
                h3_cell_column=getattr(cache_handler, "h3_cell_column", "h3_cell_id"),
                h3_cell_id_column=getattr(cache_handler, "h3_cell_id_column", None),
                p0_alias=p0_alias,
                geometry_column=geometry_column,
                srid=srid,
                resolution=getattr(cache_handler, "resolution", 9),
            )
        else:
            # Existing paths: geometry or h3_cell (inline)
            if not hasattr(cache_handler, "get_spatial_filter_lazy"):
                raise ValueError("Cache handler does not support spatial filtering (missing get_spatial_filter_lazy method)")

            spatial_filter_sql = cache_handler.get_spatial_filter_lazy(  # type: ignore[attr-defined]
                keys=existing_hashes,
                partition_key=partition_key,
                buffer_distance=buffer_distance,
            )

            if not spatial_filter_sql:
                return query, stats

            if spatial_filter_type == "h3_cell":
                resolution = getattr(cache_handler, "resolution", 9)
                enhanced_query = extend_query_with_h3_cell_filter_lazy(
                    query=query,
                    cell_sql=spatial_filter_sql,
                    geometry_column=geometry_column,
                    srid=srid,
                    resolution=resolution,
                    p0_alias=p0_alias,
                )
            else:
                use_intersects = getattr(cache_handler, "spatial_filter_includes_buffer", False)
                enhanced_query = extend_query_with_spatial_filter_lazy(
                    query=query,
                    spatial_filter_sql=spatial_filter_sql,
                    geometry_column=geometry_column,
                    buffer_distance=buffer_distance,
                    srid=srid,
                    p0_alias=p0_alias,
                    auto_detect_partition_join=auto_detect_partition_join,
                    partition_join_table=partition_join_table,
                    use_intersects=use_intersects,
                    spatial_method=spatial_method,
                    subdivide_max_vertices=subdivide_max_vertices,
                )

        stats["enhanced"] = 1
        return enhanced_query, stats

    # Non-spatial path: use partition key lazy intersection
    # Step 1: Generate query variants from original query
    lazy_cache_subquery, generated_variants, used_hashes = get_partition_keys_lazy(
        query=query,
        cache_handler=cache_handler,
        partition_key=partition_key,
        min_component_size=min_component_size,
        canonicalize_queries=canonicalize_queries,
        follow_graph=follow_graph,
        auto_detect_partition_join=auto_detect_partition_join,
        partition_join_table=partition_join_table,
        bucket_steps=bucket_steps,
        add_constraints=add_constraints,
        remove_constraints_all=remove_constraints_all,
        remove_constraints_add=remove_constraints_add,
        skip_partition_key_joins=skip_partition_key_joins,
        max_conditions_removed=max_conditions_removed,
        protected_patterns=protected_patterns,
        temporal_bucket_width=temporal_bucket_width,
        dilation_buckets=dilation_buckets,
        temporal_column=temporal_column,
        keep_all_attributes=keep_all_attributes,
        # Only temporal lookups use it here, so that other lookups keep the variants they had
        partition_key_source_table=partition_key_source_table if _is_temporal(temporal_bucket_width, dilation_buckets) else None,
    )

    # Step 2: Optionally rewrite with p0 table
    working_query = query
    p0_rewritten = 0
    if use_p0_table and _is_temporal(temporal_bucket_width, dilation_buckets):
        # The p0 rewrite joins every alias on the partition key (alias.w = p0.w), which drops the pairs across a bucket
        # boundary that temporal dilation keeps; restrict the anchor alias instead (p0_alias names the p0 table here)
        logger.warning("use_p0_table is ignored for a temporal partition key: it would put all aliases into the same bucket")
        use_p0_table = False
        p0_alias = None
    if use_p0_table:
        p0_table_alias = p0_alias if p0_alias else "p0"
        working_query = rewrite_query_with_p0_table(
            query=query,
            partition_key=partition_key,
            mv_table_name=p0_table_name,
            p0_alias=p0_table_alias,
            sql_dialect=sql_dialect,
        )
        p0_rewritten = 1 if working_query != query else 0

    # Create statistics
    stats = {"generated_variants": generated_variants, "cache_hits": used_hashes, "enhanced": 0, "p0_rewritten": p0_rewritten, **gate_stats}

    # If no cache hits, return working query
    if not lazy_cache_subquery or not lazy_cache_subquery.strip():
        return working_query, stats

    # Step 3: Apply partition key restrictions
    cache_target_alias: str | None
    if use_p0_table and p0_rewritten:
        cache_target_alias = p0_alias if p0_alias else "p0"
    else:
        cache_target_alias = p0_alias

    enhanced_query = extend_query_with_partition_keys_lazy(
        query=working_query,
        lazy_subquery=lazy_cache_subquery,
        partition_key=partition_key,
        method=method,
        p0_alias=cache_target_alias,
        analyze_tmp_table=analyze_tmp_table,
        auto_detect_partition_join=auto_detect_partition_join,
        partition_join_table=partition_join_table,
        sql_dialect=sql_dialect,
        join_every_table=join_every_table,
        planner_collapse_limit=planner_collapse_limit,
        fold_key_subqueries=fold_key_subqueries,
        fold_column_resolver=fold_column_resolver,
        fold_assume_inner_columns=fold_assume_inner_columns,
        partition_datatype=_cache_datatype(cache_handler, partition_key),
    )

    stats["enhanced"] = 1
    return enhanced_query, stats


@_runs_uncached_on_sql_errors
@_accepts_regex_operators
def apply_cache(
    query: str,
    cache_handler: AbstractCacheHandler,
    partition_key: str,
    method: Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"] = "IN",
    p0_alias: str | None = None,
    min_component_size: int = 2,
    canonicalize_queries: bool = False,
    follow_graph: bool = True,
    analyze_tmp_table: bool = True,
    use_p0_table: bool = False,
    p0_table_name: str | None = None,
    auto_detect_partition_join: bool = True,
    partition_join_table: str | None = None,
    bucket_steps: float | dict[str, float] | list = 1.0,
    add_constraints: dict[str, str] | None = None,
    remove_constraints_all: list[str] | None = None,
    remove_constraints_add: list[str] | None = None,
    skip_partition_key_joins: bool = False,
    geometry_column: str | None = None,
    buffer_distance: float | None = None,
    spatial_method: Literal["SUBDIVIDE_INLINE", "SUBDIVIDE_TMP_TABLE", "DUMP_TMP_TABLE", "DUMP_CTE"] = "SUBDIVIDE_TMP_TABLE",
    subdivide_max_vertices: int = 256,
    temporal_bucket_width: float | None = None,
    dilation_buckets: int | None = None,
    temporal_column: str | None = None,
    partition_key_source_table: str | None = None,
    cost_estimator: Callable[[str], float | None] | None = None,
    grid_cell_size: float | None = None,
    grid_method: str = "CELL_AUTO",
    min_query_cost: float | None = None,
    keep_all_attributes: bool = False,
    max_conditions_removed: int | None = None,
    protected_patterns: list[str] | None = None,
    sql_dialect: str = "postgres",
    join_every_table: bool = False,
    planner_collapse_limit: int | None = None,
    fold_key_subqueries: bool | str = False,
    fold_column_resolver: ColumnResolver | None = None,
    fold_assume_inner_columns: bool = False,
    **kwargs: Any,
) -> tuple[str, dict[str, int]]:
    """
    Complete wrapper function that applies partition cache to a query using regular cache handlers.

    This is the "whole package" function that:
    1. Generates all query variants/hashes from the original query
    2. Gets the partition keys from the cache based on those hashes
    3. Optionally rewrites the original query to use a p0 table for optimizer hints
    4. Combines the working query (original or p0-rewritten) with the partition key constraints
    5. Returns the complete enhanced query ready for execution

    Supports spatial mode when geometry_column is set: uses spatial cache handlers to get
    a pre-computed geometry filter (WKB) and applies it via ST_DWithin.

    Args:
        query (str): The original SQL query to be enhanced with cache functionality.
        cache_handler (AbstractCacheHandler): The cache handler instance.
        partition_key (str): The identifier for the partition.
        method (Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN", "UNLOGGED_TABLE_JOIN", "UNLOGGED_TABLE_IN"]): The method to
            use for query extension. An ``UNLOGGED_TABLE_*`` script ends with the DROP of its key table; execute it as ONE string.
        p0_alias (str | None): The alias of the table to use for cache restrictions in regular queries.
            Ignored when use_p0_table=True (cache targets p0 table automatically).
        min_component_size (int): Minimum size of query components to consider for cache lookup.
        canonicalize_queries (bool): Whether to canonicalize queries before hashing.
        follow_graph (bool): Whether to follow the query graph for generating variants.
        analyze_tmp_table (bool): Whether to create index and analyze for temporary table methods.
        use_p0_table (bool): Whether to rewrite the query to use a p0 table for optimizer hints.
        p0_table_name (str | None): Name of the p0 table. Defaults to {partition_key}_mv.
        auto_detect_partition_join: Whether to auto-detect partition-join tables.
        partition_join_table: Explicitly specified partition-join table alias or name.
        bucket_steps: Step size for normalizing distance conditions (e.g., 1.0, 0.5, etc.)
        add_constraints: Dict mapping table names to constraints to add (e.g., {"table": "col = val"})
        remove_constraints_all: List of attribute names to remove from all query variants
        remove_constraints_add: List of attribute names to remove, creating additional variants
        max_conditions_removed: Most attribute conditions a lookup variant may drop (None: no limit). Use the value
            the cache was populated with, as in apply_cache_lazy.
        protected_patterns: Conditions matching these patterns are never removed during variant generation.
        skip_partition_key_joins (bool): If True, skip partition key joins in variant generation.
        temporal_bucket_width: Width of one bucket of a time-bucket partition key, in the unit of ``temporal_column``;
            enables temporal dilation: each cached set is widened by ceil(window / width) buckets before the
            intersection, with the window read from the query's conditions on ``temporal_column``.
        dilation_buckets: Number of buckets to widen by, instead of the window (0 turns dilation off).
        temporal_column: The time column the partition key buckets; required with ``temporal_bucket_width``.
            With either temporal parameter, variants are generated without partition key joins, and a query
            whose rows are not bounded in time against each other runs without the cache.
        keep_all_attributes: If False (default), lookup also probes variants with single-table attribute
            conditions removed (more hits, more variant-generation cost). True probes exactly the variants
            that generate_all_query_hash_pairs produces with its defaults.
        sql_dialect: sqlglot dialect used to parse and render non-spatial queries. Defaults to PostgreSQL.
        join_every_table: TMP_TABLE_JOIN and UNLOGGED_TABLE_JOIN only: join the key table to every table instead of
            the p0 table; raises ValueError unless every table is joined on the partition key. See
            extend_query_with_partition_keys.
        planner_collapse_limit: If set to N, a rewritten script begins with ``SET LOCAL join_collapse_limit = N;`` and
            ``SET LOCAL from_collapse_limit = N;``; see ``apply_cache_lazy``. None (default) is off.
        geometry_column: If set, enables spatial cache mode. Uses this geometry column for variant
            SELECT clauses and spatial filter application. Requires a spatial cache handler with
            get_spatial_filter() method.
        buffer_distance: Buffer distance in meters for spatial filter. If None and geometry_column
            is set, auto-derived from distance constraints in the query (graph diameter); 0 for a
            query over a single table, whose rows lie in every cached set. Required when
            geometry_column is set and a query over several tables has no distance constraints.
        spatial_method: Method for applying the spatial filter:
            - "SUBDIVIDE_TMP_TABLE": Create a temp table with ST_Subdivide + GiST index (default, optimal).
            - "SUBDIVIDE_INLINE": Inline ST_Subdivide in an EXISTS subquery (simpler, no temp table).
        cost_estimator: Callable returning the planner's cost estimate for a query without executing it, or None
            when no estimate is available (e.g. ``PostgresDBHandler.estimate_cost``). Must run against the data
            database, which is not necessarily the cache database. Only used together with ``min_query_cost``.
        min_query_cost: If set, the cache is skipped when the estimated cost of the original query is below this
            value: for cheap queries the cache overhead exceeds the possible saving. The check runs before variant
            generation, so a gated query costs one planning pass only. Cost units are installation-specific
            (``seq_page_cost``, ``random_page_cost``) and have to be calibrated. A failing estimator or one that
            returns None never blocks the cache. Requires ``cost_estimator``.

    Returns:
        tuple[str, dict[str, int]]: A tuple containing:
            - Enhanced SQL query string (original query if no cache hits)
            - Cache statistics dictionary with keys:
                - 'generated_variants': Total number of query variant hashes generated
                - 'cache_hits': Number of cache hits (hashes found in cache)
                - 'enhanced': 1 if query was enhanced, 0 if returned unchanged
                - 'p0_rewritten': 1 if query was rewritten with p0 table, 0 otherwise
                - 'cost_gated' / 'query_cost': only when ``min_query_cost`` is set; 'cost_gated' is 1 if the cache
                  was skipped because the planner cost was below the threshold, 'query_cost' the rounded estimate

    Example:
        ```python
        # Non-spatial usage
        enhanced_query, stats = partitioncache.apply_cache(
            "SELECT * FROM poi AS p1 WHERE ST_DWithin(p1.geom, ST_Point(1, 2), 1000)",
            cache_handler,
            partition_key="zipcode",
            method="TMP_TABLE_IN",
            use_p0_table=True,
        )

        # Spatial usage
        enhanced_query, stats = partitioncache.apply_cache(
            "SELECT * FROM poi AS p1 WHERE p1.type = 'restaurant'",
            spatial_cache_handler,
            partition_key="spatial_h3",
            geometry_column="geom",
            buffer_distance=500.0,
        )
        ```
    """
    # Handle deprecated star_join_* parameter names
    deprecated = handle_deprecated_kwargs(kwargs, "apply_cache")
    if "partition_join_table" in deprecated:
        if partition_join_table is not None:
            raise TypeError("Cannot pass both 'partition_join_table' and deprecated 'star_join_table'")
        partition_join_table = deprecated["partition_join_table"]
    if "auto_detect_partition_join" in deprecated:
        auto_detect_partition_join = deprecated["auto_detect_partition_join"]
    _check_fold_key_subqueries(fold_key_subqueries, method, "postgres")
    _check_planner_collapse_limit(planner_collapse_limit, method, "postgres")  # the eager rewrite below renders PostgreSQL

    gated, gate_stats = _check_cost_gate(query, cost_estimator, min_query_cost)
    if gated:
        return query, {"generated_variants": 0, "cache_hits": 0, "enhanced": 0, "p0_rewritten": 0, **gate_stats}

    if _is_temporal(temporal_bucket_width, dilation_buckets):
        _, uncached_reason = _temporal_lookup(
            query, cache_handler, partition_key, temporal_bucket_width, dilation_buckets, temporal_column, partition_key_source_table, lazy=False
        )
        if uncached_reason is not None:
            return query, _uncached_stats(uncached_reason, gate_stats)

    if grid_cell_size is not None:
        if geometry_column is None:
            raise ValueError("grid_cell_size requires geometry_column")
        return _apply_grid_cache(
            query, cache_handler, partition_key, geometry_column, grid_cell_size, grid_method, p0_alias, gate_stats,
            {"partition_key_source_table": partition_key_source_table, "min_component_size": min_component_size,
             "fix_attributes": keep_all_attributes, "canonicalize_queries": canonicalize_queries, "follow_graph": True,
             "partition_join_table": partition_join_table, "bucket_steps": bucket_steps, "add_constraints": add_constraints,
             "remove_constraints_all": remove_constraints_all, "remove_constraints_add": remove_constraints_add,
             "max_conditions_removed": max_conditions_removed, "protected_patterns": protected_patterns},
        )

    # Spatial mode is selected by passing a geometry column
    if geometry_column is not None:
        # Spatial path: generate hashes with spatial params, apply spatial filter
        buffer_distance = _resolve_buffer_distance(query, buffer_distance)

        # Step 1: Generate hashes with spatial params
        cache_entry_hashes = generate_all_hashes(
            partition_key_source_table=partition_key_source_table,
            query=query,
            partition_key=partition_key,
            min_component_size=min_component_size,
            follow_graph=True,
            fix_attributes=keep_all_attributes,
            canonicalize_queries=canonicalize_queries,
            auto_detect_partition_join=False,
            partition_join_table=partition_join_table,
            bucket_steps=bucket_steps,
            add_constraints=add_constraints,
            remove_constraints_all=remove_constraints_all,
            remove_constraints_add=remove_constraints_add,
            max_conditions_removed=max_conditions_removed,
            protected_patterns=protected_patterns,
            skip_partition_key_joins=True,
            geometry_column=geometry_column,
        )

        generated_variants = len(cache_entry_hashes)
        stats: dict[str, int] = {"generated_variants": generated_variants, "cache_hits": 0, "enhanced": 0, "p0_rewritten": 0, **gate_stats}

        # Count cache hits
        used_hashes = len(cache_handler.filter_existing_keys(set(cache_entry_hashes), partition_key))  # type: ignore[attr-defined]
        stats["cache_hits"] = used_hashes

        if used_hashes == 0:
            logger.info(f"No spatial cache hits found for query. Generated {generated_variants} subqueries")
            return query, stats

        # Step 2: Route by spatial_filter_type
        srid = getattr(cache_handler, "srid", 4326)
        spatial_filter_type = getattr(cache_handler, "spatial_filter_type", "geometry")

        if spatial_filter_type == "h3_cell_ids":
            # Pure H3 cell IDs path — k-ring expansion in Python, no PostGIS at query time
            if not hasattr(cache_handler, "get_h3_cell_filter"):
                raise ValueError("Cache handler does not support H3 cell filtering (missing get_h3_cell_filter method)")

            cell_ids = cache_handler.get_h3_cell_filter(  # type: ignore[attr-defined]
                keys=set(cache_entry_hashes),
                partition_key=partition_key,
                buffer_distance=buffer_distance,
            )
            if not cell_ids:
                return query, stats

            enhanced_query = extend_query_with_h3_cell_lookup(
                query=query,
                cell_ids=cell_ids,
                cell_mode=getattr(cache_handler, "h3_cell_mode", "inline"),
                h3_cell_table=getattr(cache_handler, "h3_cell_table", None),
                h3_cell_column=getattr(cache_handler, "h3_cell_column", "h3_cell_id"),
                h3_cell_id_column=getattr(cache_handler, "h3_cell_id_column", None),
                p0_alias=p0_alias,
                geometry_column=geometry_column,
                srid=srid,
                resolution=getattr(cache_handler, "resolution", 9),
            )
        else:
            # Existing paths: geometry or h3_cell (inline)
            if not hasattr(cache_handler, "get_spatial_filter"):
                raise ValueError("Cache handler does not support spatial filtering (missing get_spatial_filter method)")

            spatial_result = cache_handler.get_spatial_filter(  # type: ignore[attr-defined]
                keys=set(cache_entry_hashes),
                partition_key=partition_key,
                buffer_distance=buffer_distance,
            )
            if not spatial_result:
                return query, stats

            if spatial_filter_type == "h3_cell":
                # H3 path: spatial_result is set[int] (cell IDs)
                resolution = getattr(cache_handler, "resolution", 9)
                enhanced_query = extend_query_with_h3_cell_filter(
                    query=query,
                    cell_ids=spatial_result,
                    geometry_column=geometry_column,
                    srid=srid,
                    resolution=resolution,
                    p0_alias=p0_alias,
                )
            else:
                # Geometry path: spatial_result is (WKB bytes, SRID)
                spatial_filter_wkb, spatial_srid = spatial_result
                use_intersects = getattr(cache_handler, "spatial_filter_includes_buffer", False)
                enhanced_query = extend_query_with_spatial_filter(
                    query=query,
                    spatial_filter_wkb=spatial_filter_wkb,
                    geometry_column=geometry_column,
                    buffer_distance=buffer_distance,
                    srid=spatial_srid,
                    p0_alias=p0_alias,
                    auto_detect_partition_join=auto_detect_partition_join,
                    partition_join_table=partition_join_table,
                    use_intersects=use_intersects,
                    spatial_method=spatial_method,
                    subdivide_max_vertices=subdivide_max_vertices,
                )

        stats["enhanced"] = 1
        logger.info(f"Successfully enhanced query with spatial cache. Generated {generated_variants} subqueries, {used_hashes} cache hits")
        return enhanced_query, stats

    # Non-spatial path (original logic)
    # Step 1: Generate all query variants from ORIGINAL query (not p0-rewritten)
    partition_keys, generated_variants, used_hashes = get_partition_keys(
        query=query,
        cache_handler=cache_handler,
        partition_key=partition_key,
        min_component_size=min_component_size,
        canonicalize_queries=canonicalize_queries,
        follow_graph=follow_graph,
        auto_detect_partition_join=auto_detect_partition_join,
        partition_join_table=partition_join_table,
        bucket_steps=bucket_steps,
        add_constraints=add_constraints,
        remove_constraints_all=remove_constraints_all,
        remove_constraints_add=remove_constraints_add,
        skip_partition_key_joins=skip_partition_key_joins,
        max_conditions_removed=max_conditions_removed,
        protected_patterns=protected_patterns,
        temporal_bucket_width=temporal_bucket_width,
        dilation_buckets=dilation_buckets,
        temporal_column=temporal_column,
        keep_all_attributes=keep_all_attributes,
        # Only temporal lookups use it here, so that other lookups keep the variants they had
        partition_key_source_table=partition_key_source_table if _is_temporal(temporal_bucket_width, dilation_buckets) else None,
    )

    # Step 2: Optionally rewrite original query with p0 table
    working_query = query
    p0_rewritten = 0
    if use_p0_table and _is_temporal(temporal_bucket_width, dilation_buckets):
        # The p0 rewrite joins every alias on the partition key (alias.w = p0.w), which drops the pairs across a bucket
        # boundary that temporal dilation keeps; restrict the anchor alias instead (p0_alias names the p0 table here)
        logger.warning("use_p0_table is ignored for a temporal partition key: it would put all aliases into the same bucket")
        use_p0_table = False
        p0_alias = None
    if use_p0_table:
        p0_table_alias = p0_alias if p0_alias else "p0"
        working_query = rewrite_query_with_p0_table(
            query=query,
            partition_key=partition_key,
            mv_table_name=p0_table_name,
            p0_alias=p0_table_alias,
            sql_dialect=sql_dialect,
        )
        p0_rewritten = 1 if working_query != query else 0

    # Create statistics dictionary
    stats = {"generated_variants": generated_variants, "cache_hits": used_hashes, "enhanced": 0, "p0_rewritten": p0_rewritten, **gate_stats}

    # If no cache hits, return working query (potentially p0-rewritten)
    if partition_keys is None:
        logger.info(f"No cache hits found for query. Generated {generated_variants} subqueries, {used_hashes} cache hits")
        return working_query, stats

    # Cache hits whose intersection is empty prove the query has no result; restrict it to no rows
    # instead of discarding the hits and running it unfiltered.
    if not partition_keys:
        stats["enhanced"] = 1
        logger.info(f"Cache intersection of {used_hashes} cache hits is empty, restricting query to no rows")
        return _add_empty_intersection_condition(working_query, sql_dialect), stats

    # Step 3: Apply the partition keys to the working query
    # Determine the correct alias for cache restrictions
    cache_target_alias: str | None
    if use_p0_table and p0_rewritten:
        # P0 table was added, target the p0 table for cache restrictions
        cache_target_alias = p0_alias if p0_alias else "p0"
    else:
        # Regular query, use the provided p0_alias or auto-detect
        cache_target_alias = p0_alias

    enhanced_query = extend_query_with_partition_keys(
        query=working_query,
        partition_keys=partition_keys,
        partition_key=partition_key,
        method=method,
        p0_alias=cache_target_alias,
        analyze_tmp_table=analyze_tmp_table,
        auto_detect_partition_join=auto_detect_partition_join,
        partition_join_table=partition_join_table,
        sql_dialect=sql_dialect,
        join_every_table=join_every_table,
        planner_collapse_limit=planner_collapse_limit,
        fold_key_subqueries=fold_key_subqueries,
        fold_column_resolver=fold_column_resolver,
        fold_assume_inner_columns=fold_assume_inner_columns,
    )

    stats["enhanced"] = 1
    logger.info(f"Successfully enhanced query with cache. Generated {generated_variants} subqueries, {used_hashes} cache hits")

    return enhanced_query, stats
