"""
Extend a given SQL query with the cache functionality.

This module provides a function `apply_cache` that takes a SQL query, a cache handler, and a partition identifier as input.
It utilizes the given cache to extend the SQL query with the set of possible partition keys

"""

from partitioncache.cache_handler.abstract import AbstractCacheHandler
from partitioncache.query_processor import generate_all_hashes
from logging import getLogger

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
