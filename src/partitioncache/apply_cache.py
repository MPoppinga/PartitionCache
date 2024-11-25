"""
Extend a given SQL query with the cache functionality.

This module provides a function `apply_cache` that takes a SQL query, a cache handler, and a partition identifier as input.
It utilizes the given cache to extend the SQL query with the set of possible partition keys

"""

from logging import getLogger
from typing import Literal

import sqlglot
import sqlglot.expressions as exp

from partitioncache.cache_handler.abstract import AbstractCacheHandler
from partitioncache.query_processor import generate_all_hashes

logger = getLogger("PartitionCache")


def get_partition_keys(
    query: str, cache_handler: AbstractCacheHandler, partition_key: str, canonicalize_queries=False
) -> tuple[set[int] | set[str] | None, int, int]:
    """
    Using the partition cache to get the partition keys for a given query.

    Args:
        query (str): The SQL query to be checked in the cache.
        cache_handler (AbstractCacheHandler): The cache handler object.
        partition_key: The identifier for the partition.

    Returns:
       set[int] | set[str] | None: The set of partition keys.
    """

    # Generate all hashes for the given query (Only consider subqueries with two ore more components that are connected, allow modifying attributes)
    cache_entry_hashes = generate_all_hashes(
        query=query,
        partition_key=partition_key,
        min_component_size=2,
        follow_graph=True,
        fix_attributes=False,
        canonicalize_queries=canonicalize_queries,
    )

    logger.info(f"Found {len(cache_entry_hashes)} subqueries in query")

    # Get the partition keys from the cache based on the hashes
    partition_keys, count = cache_handler.get_intersected(set(cache_entry_hashes))

    logger.info(f"Extended query with {count} hashes")
    return partition_keys, len(cache_entry_hashes), count


def get_partition_keys_lazy(query: str, cache_handler: AbstractCacheHandler, partition_key: str, canonicalize_queries=False) -> tuple:
    """
    Gets the lazy intersection representation of the partition keys for the given query and the number of hashes used.
    """
    hashses = generate_all_hashes(
        query=query,
        partition_key=partition_key,
        min_component_size=2,
        fix_attributes=False,
        canonicalize_queries=canonicalize_queries,
    )

    # -> tuple[sql.Composed | None , int]
    lazy_cache_subquery, used_hashes = cache_handler.get_intersected_lazy(set(hashses))
    return lazy_cache_subquery, used_hashes


def find_p0_alias(query: str) -> str:
    """
    Find the alias of the first table in the query.
    """
    x = sqlglot.parse_one(query)
    table = x.find(exp.Table)
    if table is None:
        raise ValueError("No table found in query")
    return table.alias


def extend_query_with_partition_keys(
    query: str, partition_keys: set[int] | set[str], partition_key: str, method: Literal["IN", "VALUES", "TMP_TABLE_JOIN"] = "IN", p0_alias: str | None = None
) -> str:
    """
    Extend a given SQL query with the cache functionality.

    This covers basic cases, in many cases this may not be fitting, in these cases the SQL rewrite should happen in the calling software where the original query was created.

    Args:
        query (str): The SQL query to be extended.
        partition_keys (set[int] | set[str]): The set of partition keys to extend the query with.
        partition_key (str): The identifier for the partition.
        method (Literal["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]): The method to use to extend the query.
        p0_alias (str | None): The alias of the table to use for the partition key. If not set for JOIN methods, it will JOIN on all tables.

    Returns:
        str: The extended SQL query as string.
    """
    if not partition_keys:
        return query

    if p0_alias is None and method != "TMP_TABLE_JOIN":
        # No alias provided, try to find it (TMP_TABLE_JOIN does join on all tables)
        p0_alias = find_p0_alias(query)

    if method == "IN":
        # Convert partition_keys to string representation for SQL
        partition_keys_str = ",".join(str(pk) for pk in partition_keys)

        x = sqlglot.parse_one(query)

        # Find the WHERE clause or create a new one
        where: exp.Where | None = x.find(exp.Where)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN ({partition_keys_str})")
        if where is None:
            where = exp.Where(this=partition_expr)
        else:
            # Add partition condition to existing WHERE with AND
            where.args["this"] = exp.and_(where.args["this"], partition_expr)

        return x.sql()

    elif method == "VALUES":
        partition_keys_str = "),(".join(str(pk) for pk in partition_keys)

        x = sqlglot.parse_one(query)
        where: exp.Where | None = x.find(exp.Where)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN (VALUES({partition_keys_str}))")
        if where is None:
            where = exp.Where(this=partition_expr)
        else:
            where.args["this"] = exp.and_(where.args["this"], partition_expr)
        return x.sql()

    elif method == "TMP_TABLE_JOIN":
        ## TMP TABLE Setup

        newquery = f"""CREATE TEMPORARY TABLE tmp_partition_keys (partition_key INT PRIMARY KEY);
                    INSERT INTO tmp_partition_keys (partition_key) (VALUES({"),(".join(str(pk) for pk in partition_keys)}));
                    CREATE INDEX tmp_partition_keys_idx ON tmp_partition_keys USING HASH(partition_key);
                    ANALYZE tmp_partition_keys;
                    """

        ## Add as inner join to all tables
        x = sqlglot.parse_one(query)

        from_clauses: list[exp.Join | exp.From] = list(x.find_all(exp.Join))

        # Add first table from FROM as not present in JOINs
        from_1st = x.find(exp.From)
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
                    on=exp.EQ(this=exp.Identifier(this=f"tmp_{table_alias}.partition_key"), expression=exp.Identifier(this=f"{table_alias}.{partition_key}")),
                    kind="INNER",
                ).sql()  # New join expression
            )

            # Replace the old join expression with the new one
            from_clause.this.replace(join_expr)

        # Return the new query with the tmp table setup
        return newquery + x.sql()
