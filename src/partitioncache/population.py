"""
Sequential cache population for the query variants of one query.

Variants are executed smallest first. Each variant is restricted to the intersection of the keys of its
already populated relaxations, which are kept in memory, so no cache lookup or hash generation is needed
per variant. A relaxation is recognised structurally (``variant_signature``/``is_relaxation_of``):
its tables and conditions are a subset of the variant's, so its keys are a superset of the variant's keys.
"""

from dataclasses import dataclass
from logging import getLogger
from typing import Any, Literal

from partitioncache.apply_cache import extend_query_with_partition_keys
from partitioncache.cache_handler.abstract import AbstractCacheHandler
from partitioncache.db_handler.abstract import AbstractDBHandler
from partitioncache.query_processor import is_relaxation_of, order_variants_for_population, variant_signature

logger = getLogger("PartitionCache")


@dataclass
class PopulationStats:
    """Counts of what ``populate_query_variants`` did with the variants of one query."""

    executed: int = 0
    restricted: int = 0
    empty_without_execution: int = 0
    already_cached: int = 0
    failed: int = 0
    store_failed: int = 0


def populate_query_variants(
    query_hash_pairs: list[tuple[str, str]],
    partition_key: str,
    cache_handler: AbstractCacheHandler,
    db_handler: AbstractDBHandler,
    restrict: bool = True,
    max_restriction_keys: int | None = None,
    method: Literal["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"] = "IN",
    sql_dialect: str = "postgres",
) -> PopulationStats:
    """Execute and cache the variants of one query, restricting each with the keys of its relaxations.

    Args:
        query_hash_pairs: (variant_query, hash) pairs of one query, as from ``generate_all_query_hash_pairs``.
        partition_key: The partition key column the variants select.
        cache_handler: Where the variant keys are stored. Variants already cached are not executed;
            their keys are read once so they can restrict the remaining variants.
        db_handler: Executes the (restricted) variants and returns their partition keys. It must raise on
            errors: a failure reported as an empty result would be cached as "no rows" and, through the
            intersection, empty every variant it restricts (SQLiteDBHandler logs errors and returns []).
        restrict: Restrict variants with the keys of their populated relaxations.
        max_restriction_keys: Only restrict when the intersection has at most this many keys; a restriction
            to most of the keys filters little and costs a long key list. None means no limit.
        method: How the keys are attached, see ``extend_query_with_partition_keys``.
        sql_dialect: Dialect of the database, used to render the key literals (e.g. "mysql").

    Returns:
        PopulationStats. An empty intersection means the variant has no rows; it is stored as an empty
        set without being executed.
    """
    stats = PopulationStats()
    ordered = order_variants_for_population(query_hash_pairs)
    signatures = {variant_hash: variant_signature(variant, partition_key, sql_dialect) for variant, variant_hash in ordered} if restrict else {}
    populated: dict[str, set[Any]] = {}

    for variant, variant_hash in ordered:
        signature = signatures.get(variant_hash)
        if cache_handler.exists(variant_hash, partition_key):
            stats.already_cached += 1
            cache_handler.set_query(variant_hash, variant, partition_key)  # keep the query text current
            if signature is not None:
                cached = cache_handler.get(variant_hash, partition_key)
                if cached is not None:
                    populated[variant_hash] = set(cached)
            continue

        to_execute = variant
        if signature is not None:
            relaxation_keys = [
                keys for other_hash, keys in populated.items() if (other := signatures[other_hash]) is not None and is_relaxation_of(other, signature)
            ]
            if relaxation_keys:
                relaxation_keys.sort(key=len)
                intersection = relaxation_keys[0].intersection(*relaxation_keys[1:])
                if not intersection:
                    if not cache_handler.set_entry(variant_hash, set(), variant, partition_key, force_update=True):
                        logger.warning(f"Failed to store the empty result of variant {variant_hash}")
                        stats.store_failed += 1
                    populated[variant_hash] = set()
                    stats.empty_without_execution += 1
                    continue
                if max_restriction_keys is None or len(intersection) <= max_restriction_keys:
                    to_execute = extend_query_with_partition_keys(variant, intersection, partition_key, method=method, sql_dialect=sql_dialect)
                    stats.restricted += 1

        try:
            result = set(db_handler.execute(to_execute))
        except Exception as e:
            logger.warning(f"Failed to execute variant {variant_hash}: {e}")
            stats.failed += 1
            # The DB handlers commit after each query but do not roll back on errors; without this an
            # aborted PostgreSQL transaction would make every following variant fail as well
            connection = getattr(db_handler, "conn", None)
            if connection is not None:
                connection.rollback()
            continue
        if not cache_handler.set_entry(variant_hash, result, variant, partition_key, force_update=True):
            logger.warning(f"Failed to store the result of variant {variant_hash}")
            stats.store_failed += 1
        stats.executed += 1
        if signature is not None:
            populated[variant_hash] = result

    return stats
