"""
Extend a given SQL query with the cache functionality.

This module provides a function `apply_cache` that takes a SQL query, a cache handler, and a partition identifier as input.
It utilizes the given cache to extend the SQL query with the set of possible partition keys

"""

from logging import getLogger
from typing import Literal
from datetime import datetime

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
) -> tuple:
    """
    Gets the lazy intersection representation of the partition keys for the given query and the number of hashes used.
    """
    hashses = generate_all_hashes(
        query=query,
        partition_key=partition_key,
        min_component_size=min_component_size,
        fix_attributes=False,
        canonicalize_queries=canonicalize_queries,
        follow_graph=follow_graph,
    )

    # -> tuple[sql.Composed | None , int]
    if not isinstance(cache_handler, AbstractCacheHandler_Lazy):
        raise ValueError("Cache handler does not support lazy intersection")

    lazy_cache_subquery, used_hashes = cache_handler.get_intersected_lazy(set(hashses), partition_key=partition_key)

    return lazy_cache_subquery, used_hashes


def find_p0_alias(query: str) -> str:
    """
    Find the alias of the first table in the query.
    """
    x = sqlglot.parse_one(query)
    table = x.find(exp.Table)
    if table is None:
        raise ValueError("No table found in query")
    return table.alias_or_name


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
        p0_alias (str | None): The alias of the table to use for the partition key. If not set for JOIN methods, it will JOIN on all tables.
        analyze_tmp_table (bool): Whether to create index and analyze. (only for temporary table methods)

    Returns:
        str: The extended SQL query as string.
    """
    if not partition_keys:
        return query

    if p0_alias is None and method != "TMP_TABLE_JOIN":
        # No alias provided, try to find it (TMP_TABLE_JOIN does join on all tables)
        p0_alias = find_p0_alias(query)

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


def _create_tmp_table_setup_from_subquery(lazy_subquery: str, partition_key: str, analyze_tmp_table: bool) -> str:
    """Create the SQL for temporary table setup from a lazy subquery."""
    setup_sql = f"CREATE TEMPORARY TABLE tmp_cache_keys AS ({lazy_subquery});\n"

    if analyze_tmp_table:
        setup_sql += f"CREATE INDEX tmp_cache_keys_idx ON tmp_cache_keys ({partition_key});\n"
        setup_sql += "ANALYZE tmp_cache_keys;\n"

    return setup_sql


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
        p0_alias (str | None): The alias of the table to use for the partition key. If not set for JOIN methods, it will JOIN on all tables.
        analyze_tmp_table (bool): Whether to create index and analyze. (only for temporary table methods)

    Returns:
        str: The extended SQL query as string.
    """
    if not lazy_subquery or not lazy_subquery.strip():
        return query

    if p0_alias is None and method != "TMP_TABLE_JOIN":
        # No alias provided, try to find it (TMP_TABLE_JOIN does join on all tables)
        p0_alias = find_p0_alias(query)

    parsed_query = sqlglot.parse_one(query)

    if method == "IN_SUBQUERY":
        # Direct integration: original_query AND p0_alias.partition_key IN (lazy_subquery)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN ({lazy_subquery})")
        _add_where_condition(parsed_query, partition_expr)
        return parsed_query.sql()

    elif method == "TMP_TABLE_IN":
        # Create temporary table from lazy subquery, then use IN with SELECT
        tmp_table_setup = _create_tmp_table_setup_from_subquery(lazy_subquery, partition_key, analyze_tmp_table)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN (SELECT {partition_key} FROM tmp_cache_keys)")
        _add_where_condition(parsed_query, partition_expr)
        return tmp_table_setup + parsed_query.sql()

    elif method == "TMP_TABLE_JOIN":
        # Create temporary table from lazy subquery, then JOIN
        tmp_table_setup = _create_tmp_table_setup_from_subquery(lazy_subquery, partition_key, analyze_tmp_table)

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
                    this=exp.Identifier(this=f"tmp_cache_keys AS tmp_{table_alias}"),
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
