"""
Extend a given SQL query with the cache functionality.

This module provides a function `apply_cache` that takes a SQL query, a cache handler, and a partition identifier as input.
It utilizes the given cache to extend the SQL query with the set of possible partition keys

"""

from logging import getLogger
from typing import Literal
from datetime import datetime
import random

import sqlglot
import sqlglot.expressions as exp

from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy
from partitioncache.query_processor import generate_all_hashes

logger = getLogger("PartitionCache")


def get_partition_keys(
    query: str, cache_handler: AbstractCacheHandler, partition_key: str, min_component_size=2, canonicalize_queries=False
) -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int, int]:
    """
    Using the partition cache to get the partition keys for a given query.

    Args:
        query (str): The SQL query to be checked in the cache.
        cache_handler (AbstractCacheHandler): The cache handler object.
        partition_key: The identifier for the partition.

    Returns:
       set[int] | set[str] | set[float] | set[datetime] | None: The set of partition keys.
    """

    # Generate all hashes for the given query (Only consider subqueries with two ore more components that are connected, allow modifying attributes)
    cache_entry_hashes = generate_all_hashes(
        query=query,
        partition_key=partition_key,
        min_component_size=min_component_size,
        follow_graph=True,
        fix_attributes=False,
        canonicalize_queries=canonicalize_queries,
    )

    logger.info(f"Found {len(cache_entry_hashes)} subqueries in query")

    # Get the partition keys from the cache based on the hashes
    partition_keys, count = cache_handler.get_intersected(set(cache_entry_hashes), partition_key=partition_key)

    logger.info(f"Extended query with {count} hashes")
    return partition_keys, len(cache_entry_hashes), count


def get_partition_keys_lazy(
    query: str,
    cache_handler: AbstractCacheHandler,
    partition_key: str,
    min_component_size=2,
    canonicalize_queries=False,
    follow_graph=True,
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

    Returns:
        tuple[str, int, int]: A tuple containing:
            - Lazy SQL subquery string (empty if no cache hits)
            - Total number of query variant hashes generated
            - Number of cache hits (hashes found in cache)

    Raises:
        ValueError: If cache handler does not support lazy intersection.
    """
    hashses = generate_all_hashes(
        query=query,
        partition_key=partition_key,
        min_component_size=min_component_size,
        fix_attributes=False,
        canonicalize_queries=canonicalize_queries,
        follow_graph=follow_graph,
    )

    if not isinstance(cache_handler, AbstractCacheHandler_Lazy):
        raise ValueError("Cache handler does not support lazy intersection")

    lazy_cache_subquery, used_hashes = cache_handler.get_intersected_lazy(set(hashses), partition_key=partition_key)

    return lazy_cache_subquery, len(hashses), used_hashes


def find_p0_alias(query: str, partition_key: str) -> str:
    """
    Find the appropriate table alias for cache restrictions.

    If a p0 table exists (e.g., partition_key_mv as p0), use that.
    Otherwise, use the first table in the query.
    """
    parsed_query = sqlglot.parse_one(query)

    # First check for p0 table (star-schema pattern)
    for table in parsed_query.find_all(exp.Table):
        alias = table.alias if table.alias else table.name
        table_name = table.name if hasattr(table, "name") else str(table.this)

        # Check if this is a p0 table (ends with _mv and matches partition_key)
        if table_name == f"{partition_key}_mv" or alias == "p0":
            return alias

    # Fallback to first table
    first_table = parsed_query.find(exp.Table)
    if first_table is None:
        raise ValueError("No table found in query")
    return first_table.alias_or_name


def rewrite_query_with_p0_table(
    query: str,
    partition_key: str,
    mv_table_name: str | None = None,
    p0_alias: str = "p0",
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

    Returns:
        str: The rewritten query with star-schema p0 table integration.
    """
    if mv_table_name is None:
        mv_table_name = f"{partition_key}_mv"

    parsed_query = sqlglot.parse_one(query)

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
    from_clause.set("this", exp.Table(this=f"{current_table.sql()}, {mv_table_name} AS {p0_alias}"))

    # Remove existing partition_key join conditions and replace with star-schema joins
    where_clause = parsed_query.find(exp.Where)
    if where_clause:
        # Collect all conditions
        conditions = []

        def collect_conditions(node):
            if isinstance(node, exp.And):
                collect_conditions(node.left)
                collect_conditions(node.right)
            else:
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

    return parsed_query.sql()


def _format_partition_key_for_sql(pk: int | str | float | datetime) -> str:
    """Format a single partition key for SQL representation."""
    if isinstance(pk, int):
        return str(pk)
    return f"'{pk}'"


def _get_partition_key_sql_type(partition_keys: set[int] | set[str] | set[float] | set[datetime]) -> str:
    """Determine the SQL type for partition keys."""
    if not partition_keys:
        return "TEXT"
    sample_key = next(iter(partition_keys))
    return "INT" if isinstance(sample_key, int) else "TEXT"


def _add_where_condition(parsed_query: exp.Expression, condition_expr: exp.Expression) -> None:
    """Add a WHERE condition to a parsed query, creating WHERE clause if needed."""
    existing_where: exp.Where | None = parsed_query.find(exp.Where)
    if existing_where is None:
        new_where = exp.Where(this=condition_expr)
        parsed_query.args["where"] = new_where
    else:
        existing_where.args["this"] = exp.and_(existing_where.args["this"], condition_expr)


def _create_tmp_table_setup(partition_keys: set[int] | set[str] | set[float] | set[datetime], analyze_tmp_table: bool) -> str:
    """Create the SQL for temporary table setup."""
    partition_keys_str = "),(".join(_format_partition_key_for_sql(pk) for pk in partition_keys)
    partition_key_type = _get_partition_key_sql_type(partition_keys)

    setup_sql = f"""CREATE TEMPORARY TABLE tmp_partition_keys (partition_key {partition_key_type} PRIMARY KEY);
                    INSERT INTO tmp_partition_keys (partition_key) (VALUES({partition_keys_str}));
                    """

    if analyze_tmp_table:
        setup_sql += "CREATE INDEX tmp_partition_keys_idx ON tmp_partition_keys USING HASH(partition_key);ANALYZE tmp_partition_keys;"

    return setup_sql


def extend_query_with_partition_keys(
    query: str,
    partition_keys: set[int] | set[str] | set[float] | set[datetime],
    partition_key: str,
    method: Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN"] = "IN",
    p0_alias: str | None = None,
    analyze_tmp_table: bool = True,
) -> str:
    """
    Extend a given SQL query with the cache functionality.

    This covers basic cases, in many cases this may not be fitting, in these cases the SQL rewrite should happen in the calling software where
    the original query was created.

    Args:
        query (str): The SQL query to be extended.
        partition_keys (set[int] | set[str]| set[float] | set[datetime]): The set of partition keys to extend the query with.
        partition_key (str): The identifier for the partition.
        method (Literal["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]): The method to use to extend the query.
        p0_alias (str | None): The alias of the table to use for the partition key.
        analyze_tmp_table (bool): Whether to create index and analyze. (only for temporary table methods)

    Returns:
        str: The extended SQL query as string.
    """
    if not partition_keys:
        return query

    if p0_alias is None and method != "TMP_TABLE_JOIN":
        # No alias provided, try to find it (TMP_TABLE_JOIN does join on all tables)
        p0_alias = find_p0_alias(query, partition_key)

    parsed_query = sqlglot.parse_one(query)

    if method == "IN":
        partition_keys_str = ",".join(_format_partition_key_for_sql(pk) for pk in partition_keys)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN ({partition_keys_str})")
        _add_where_condition(parsed_query, partition_expr)
        return parsed_query.sql()

    elif method == "VALUES":
        partition_keys_str = "),(".join(_format_partition_key_for_sql(pk) for pk in partition_keys)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN (VALUES({partition_keys_str}))")
        _add_where_condition(parsed_query, partition_expr)
        return parsed_query.sql()

    elif method == "TMP_TABLE_JOIN":
        tmp_table_setup = _create_tmp_table_setup(partition_keys, analyze_tmp_table)

        # Add as inner join to all tables
        from_clauses: list[exp.Join | exp.From] = list(parsed_query.find_all(exp.Join))

        # Add first table from FROM as not present in JOINs
        from_1st = parsed_query.find(exp.From)
        if from_1st is not None:
            from_clauses.append(from_1st)

        for from_clause in from_clauses:
            # Get alias of the table
            table_alias = from_clause.alias_or_name

            if p0_alias is not None and table_alias != p0_alias:
                continue

            # Create the new join expression using sqlglot
            join_expr = (
                from_clause.this.sql()  # Original table
                + " "  # Space between tables
                + exp.Join(
                    this=exp.Identifier(this=f"tmp_partition_keys AS tmp_{table_alias}"),
                    on=exp.EQ(
                        this=exp.Identifier(this=f"tmp_{table_alias}.partition_key"),
                        expression=exp.Identifier(this=f"{table_alias}.{partition_key}"),
                    ),
                    kind="INNER",
                ).sql()  # New join expression
            )

            # Replace the old join expression with the new one
            from_clause.this.replace(join_expr)

        return tmp_table_setup + parsed_query.sql()

    elif method == "TMP_TABLE_IN":
        tmp_table_setup = _create_tmp_table_setup(partition_keys, analyze_tmp_table)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN (SELECT partition_key FROM tmp_partition_keys)")
        _add_where_condition(parsed_query, partition_expr)
        return tmp_table_setup + parsed_query.sql()


def _create_tmp_table_setup_from_subquery(lazy_subquery: str, partition_key: str, analyze_tmp_table: bool) -> tuple[str, str]:
    """Create the SQL for temporary table setup from a lazy subquery."""

    table_name = f"tmp_cache_keys_{random.randint(100000, 999999)}"
    setup_sql = f"CREATE TEMPORARY TABLE {table_name} AS ({lazy_subquery});\n"

    if analyze_tmp_table:
        setup_sql += f"CREATE INDEX {table_name}_idx ON {table_name} ({partition_key});\n"
        setup_sql += f"ANALYZE {table_name};\n"

    return setup_sql, table_name


def extend_query_with_partition_keys_lazy(
    query: str,
    lazy_subquery: str,
    partition_key: str,
    method: Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"] = "IN_SUBQUERY",
    p0_alias: str | None = None,
    analyze_tmp_table: bool = True,
) -> str:
    """
    Extend a given SQL query with the cache functionality using a lazy SQL subquery.

    This function takes a lazy SQL subquery (returned from get_partition_keys_lazy) and integrates
    it into the original query using different methods for optimal performance.

    Args:
        query (str): The SQL query to be extended.
        lazy_subquery (str): The SQL subquery that returns partition keys.
        partition_key (str): The identifier for the partition.
        method (Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]): The method to use to extend the query.
        p0_alias (str | None): The alias of the table to use for the partition key. If None, will be auto-detected.
            When use_p0_table=True, cache restrictions automatically target the p0 table regardless of this parameter.
        analyze_tmp_table (bool): Whether to create index and analyze. (only for temporary table methods)

    Returns:
        str: The extended SQL query as string.
    """
    if not lazy_subquery or not lazy_subquery.strip():
        return query

    if p0_alias is None and method != "TMP_TABLE_JOIN":
        # No alias provided, try to find it (TMP_TABLE_JOIN does join on all tables)
        p0_alias = find_p0_alias(query, partition_key)

    parsed_query = sqlglot.parse_one(query)

    if method == "IN_SUBQUERY":
        # Direct integration: original_query AND p0_alias.partition_key IN (lazy_subquery)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN ({lazy_subquery})")
        _add_where_condition(parsed_query, partition_expr)
        return parsed_query.sql()

    elif method == "TMP_TABLE_IN":
        # Create temporary table from lazy subquery, then use IN with SELECT
        tmp_table_setup, table_name = _create_tmp_table_setup_from_subquery(lazy_subquery, partition_key, analyze_tmp_table)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN (SELECT {partition_key} FROM {table_name})")
        _add_where_condition(parsed_query, partition_expr)
        return tmp_table_setup + parsed_query.sql()

    elif method == "TMP_TABLE_JOIN":
        # Create temporary table from lazy subquery, then JOIN
        tmp_table_setup, table_name = _create_tmp_table_setup_from_subquery(lazy_subquery, partition_key, analyze_tmp_table)

        # Add as inner join to all tables
        from_clauses: list[exp.Join | exp.From] = list(parsed_query.find_all(exp.Join))

        # Add first table from FROM as not present in JOINs
        from_1st = parsed_query.find(exp.From)
        if from_1st is not None:
            from_clauses.append(from_1st)

        for from_clause in from_clauses:
            # Get alias of the table
            table_alias = from_clause.alias_or_name

            if p0_alias is not None and table_alias != p0_alias:
                continue

            # Create the new join expression using sqlglot
            join_expr = (
                from_clause.this.sql()  # Original table
                + " "  # Space between tables
                + exp.Join(
                    this=exp.Identifier(this=f"{table_name} AS tmp_{table_alias}"),
                    on=exp.EQ(
                        this=exp.Identifier(this=f"tmp_{table_alias}.{partition_key}"),
                        expression=exp.Identifier(this=f"{table_alias}.{partition_key}"),
                    ),
                    kind="INNER",
                ).sql()  # New join expression
            )

            # Replace the old join expression with the new one
            from_clause.this.replace(join_expr)

        return tmp_table_setup + parsed_query.sql()


def apply_cache_lazy(
    query: str,
    cache_handler: AbstractCacheHandler_Lazy,
    partition_key: str,
    method: Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"] = "IN_SUBQUERY",
    p0_alias: str | None = None,
    min_component_size: int = 2,
    canonicalize_queries: bool = False,
    follow_graph: bool = True,
    analyze_tmp_table: bool = True,
    use_p0_table: bool = False,
    p0_table_name: str | None = None,
) -> tuple[str, dict[str, int]]:
    """
    Complete wrapper function that applies partition cache to a query using lazy intersection.

    Args:
        query (str): The original SQL query to be enhanced with cache functionality.
        cache_handler (AbstractCacheHandler_Lazy): The lazy cache handler instance.
        partition_key (str): The identifier for the partition.
        method (Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]): The method to use for query extension.
        p0_alias (str | None): For regular queries: table alias for cache restrictions.
                               For p0 queries: alias name for the p0 table (defaults to "p0").
        min_component_size (int): Minimum size of query components to consider for cache lookup.
        canonicalize_queries (bool): Whether to canonicalize queries before hashing.
        follow_graph (bool): Whether to follow the query graph for generating variants.
        analyze_tmp_table (bool): Whether to create index and analyze for temporary table methods.
        use_p0_table (bool): Whether to rewrite the query to use a p0 table (star-schema).
        p0_table_name (str | None): Name of the p0 table. Defaults to {partition_key}_mv.

    Returns:
        tuple[str, dict[str, int]]: Enhanced query and statistics.
    """
    # Step 1: Generate query variants from original query
    lazy_cache_subquery, generated_variants, used_hashes = get_partition_keys_lazy(
        query=query,
        cache_handler=cache_handler,
        partition_key=partition_key,
        min_component_size=min_component_size,
        canonicalize_queries=canonicalize_queries,
        follow_graph=follow_graph,
    )

    # Step 2: Optionally rewrite with p0 table
    working_query = query
    p0_rewritten = 0
    if use_p0_table:
        p0_table_alias = p0_alias if p0_alias else "p0"
        working_query = rewrite_query_with_p0_table(
            query=query,
            partition_key=partition_key,
            mv_table_name=p0_table_name,
            p0_alias=p0_table_alias,
        )
        p0_rewritten = 1 if working_query != query else 0

    # Create statistics
    stats = {"generated_variants": generated_variants, "cache_hits": used_hashes, "enhanced": 0, "p0_rewritten": p0_rewritten}

    # If no cache hits, return working query
    if not lazy_cache_subquery or not lazy_cache_subquery.strip():
        return working_query, stats

    # Step 3: Apply cache restrictions
    if use_p0_table and p0_rewritten:
        # Target p0 table for cache restrictions
        cache_target_alias = p0_alias if p0_alias else "p0"
    else:
        # Regular query - use provided alias or auto-detect
        cache_target_alias = p0_alias

    enhanced_query = extend_query_with_partition_keys_lazy(
        query=working_query,
        lazy_subquery=lazy_cache_subquery,
        partition_key=partition_key,
        method=method,
        p0_alias=cache_target_alias,
        analyze_tmp_table=analyze_tmp_table,
    )

    stats["enhanced"] = 1
    return enhanced_query, stats


def apply_cache(
    query: str,
    cache_handler: AbstractCacheHandler,
    partition_key: str,
    method: Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN"] = "IN",
    p0_alias: str | None = None,
    min_component_size: int = 2,
    canonicalize_queries: bool = False,
    analyze_tmp_table: bool = True,
    use_p0_table: bool = False,
    p0_table_name: str | None = None,
) -> tuple[str, dict[str, int]]:
    """
    Complete wrapper function that applies partition cache to a query using regular cache handlers.

    This is the "whole package" function that:
    1. Generates all query variants/hashes from the original query
    2. Gets the partition keys from the cache based on those hashes
    3. Optionally rewrites the original query to use a p0 table for optimizer hints
    4. Combines the working query (original or p0-rewritten) with the partition key constraints
    5. Returns the complete enhanced query ready for execution

    Args:
        query (str): The original SQL query to be enhanced with cache functionality.
        cache_handler (AbstractCacheHandler): The cache handler instance.
        partition_key (str): The identifier for the partition.
        method (Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN"]): The method to use for query extension.
        p0_alias (str | None): The alias of the table to use for cache restrictions in regular queries.
            Ignored when use_p0_table=True (cache targets p0 table automatically).
        min_component_size (int): Minimum size of query components to consider for cache lookup.
        canonicalize_queries (bool): Whether to canonicalize queries before hashing.
        analyze_tmp_table (bool): Whether to create index and analyze for temporary table methods.
        use_p0_table (bool): Whether to rewrite the query to use a p0 table for optimizer hints.
        p0_table_name (str | None): Name of the p0 table. Defaults to {partition_key}_mv.
        p0_method (Literal["JOIN", "IN"]): The method to use for p0 table rewriting.

    Returns:
        tuple[str, dict[str, int]]: A tuple containing:
            - Enhanced SQL query string (original query if no cache hits)
            - Cache statistics dictionary with keys:
                - 'generated_variants': Total number of query variant hashes generated
                - 'cache_hits': Number of cache hits (hashes found in cache)
                - 'enhanced': 1 if query was enhanced, 0 if returned unchanged
                - 'p0_rewritten': 1 if query was rewritten with p0 table, 0 otherwise

    Example:
        ```python
        enhanced_query, stats = partitioncache.apply_cache(
            "SELECT * FROM poi AS p1 WHERE ST_DWithin(p1.geom, ST_Point(1, 2), 1000)",
            cache_handler,
            partition_key="zipcode",
            method="TMP_TABLE_IN",
            use_p0_table=True,
        )
        ```
    """
    # Step 1: Generate all query variants from ORIGINAL query (not p0-rewritten)
    partition_keys, generated_variants, used_hashes = get_partition_keys(
        query=query,
        cache_handler=cache_handler,
        partition_key=partition_key,
        min_component_size=min_component_size,
        canonicalize_queries=canonicalize_queries,
    )

    # Step 2: Optionally rewrite original query with p0 table
    working_query = query
    p0_rewritten = 0
    if use_p0_table:
        p0_table_alias = p0_alias if p0_alias else "p0"
        working_query = rewrite_query_with_p0_table(
            query=query,
            partition_key=partition_key,
            mv_table_name=p0_table_name,
            p0_alias=p0_table_alias,
        )
        p0_rewritten = 1 if working_query != query else 0

    # Create statistics dictionary
    stats = {"generated_variants": generated_variants, "cache_hits": used_hashes, "enhanced": 0, "p0_rewritten": p0_rewritten}

    # If no cache hits, return working query (potentially p0-rewritten)
    if not partition_keys:
        logger.info(f"No cache hits found for query. Generated {generated_variants} subqueries, {used_hashes} cache hits")
        return working_query, stats

    # Step 3: Apply the partition keys to the working query
    # Determine the correct alias for cache restrictions
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
    )

    stats["enhanced"] = 1
    logger.info(f"Successfully enhanced query with cache. Generated {generated_variants} subqueries, {used_hashes} cache hits")

    return enhanced_query, stats
