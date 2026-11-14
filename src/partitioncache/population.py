"""
Cache population for the query variants of one query, restricted through the cache as a query is.

Variants are executed smallest first. Before a variant runs, the cache is looked up for it with the lookup functions
of a query (``get_partition_keys`` / ``get_partition_keys_lazy``): its sub-variants are generated with the settings the
variants were generated with, hashed, and the cached entries among them intersected. A sub-variant joins a subset of
the variant's tables and keeps a subset of its conditions (distance bounds possibly widened by ``bucket_steps``), so
its keys are a superset of the variant's keys, and so is the intersection. Restricted to it, the variant returns the
same keys as unrestricted. Self-joins need no special case: sub-variants are identified by hash, not by table names.
"""

import inspect
from collections.abc import Mapping
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Literal

from partitioncache.apply_cache import (
    extend_query_with_partition_keys,
    extend_query_with_partition_keys_lazy,
    get_partition_keys,
    get_partition_keys_lazy,
)
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy
from partitioncache.db_handler.abstract import AbstractDBHandler
from partitioncache.query_processor import generate_all_query_hash_pairs, order_variants_for_population, variant_key_alias

logger = getLogger("PartitionCache")

EAGER_METHODS = ("IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN")
LAZY_METHODS = ("IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN")

# The defaults of generate_all_query_hash_pairs; those of get_partition_keys differ (min_component_size,
# keep_all_attributes), so a lookup that relied on them would generate other sub-variants than the variants
_GENERATION_DEFAULTS = {
    name: parameter.default
    for name, parameter in inspect.signature(generate_all_query_hash_pairs).parameters.items()
    if parameter.default is not inspect.Parameter.empty
}
# Shape only the decomposition of the original query: a variant is no larger than max_component_size, and it
# already selects the partition key. add_constraints adds conditions: the decomposition yields the variants with and
# without them, but looked up, the constrained "sub-variants" of an unconstrained variant are stricter than it, not
# relaxations. A constrained variant keeps its constraint as a condition, so its relaxations are still found.
_DECOMPOSITION_ONLY_SETTINGS = frozenset({"max_component_size", "warn_no_partition_key", "strip_select", "add_constraints"})
# Keys whose restriction is not a key list (spatial filter, widened grid cells); their variants run unrestricted
_KEY_LIST_LESS_SETTINGS = ("geometry_column", "grid_cell_size")


def lookup_settings(variant_settings: Mapping[str, Any] | None) -> dict[str, Any]:
    """The keyword arguments of get_partition_keys(_lazy) that generate a variant's sub-variants as its variants were generated.

    A lookup with other settings can generate sub-variants that are not relaxations of the variant: with
    skip_partition_key_joins=False, a variant of a key generated with True gets single-table variants of its other
    tables and variants with an added key join, and their cached entries can lack keys of the variant. The added
    conditions of add_constraints are left out for the same reason.

    Args:
        variant_settings: The keyword arguments of generate_all_query_hash_pairs the variants were generated with;
            missing ones take its defaults.

    Raises:
        TypeError: For a name generate_all_query_hash_pairs does not take.
        ValueError: For a spatial or grid key (geometry_column or grid_cell_size set).
    """
    settings = dict(variant_settings or {})
    unknown = sorted(set(settings) - set(_GENERATION_DEFAULTS))
    if unknown:
        raise TypeError(f"Not a setting of generate_all_query_hash_pairs: {unknown}")
    present = [name for name in _KEY_LIST_LESS_SETTINGS if settings.get(name) is not None]
    if present:
        raise ValueError(f"Variants generated with {present} are restricted with a spatial filter or widened cells, not by this lookup")
    merged = {**_GENERATION_DEFAULTS, **settings}
    return {name: value for name, value in merged.items() if name not in _DECOMPOSITION_ONLY_SETTINGS and name != "grid_cell_size"}


@dataclass
class Restriction:
    """The outcome of ``restrict_with_cache`` for one query variant."""

    query: str  # the query to execute: the restricted variant, or the variant itself
    restricted: bool = False
    empty: bool = False  # the eager intersection is empty: the variant has no rows
    generated: int = 0  # sub-variant hashes the lookup generated, the variant's own included
    hits: int = 0
    keys: int | None = None  # size of the eager intersection
    method: str | None = None
    # Why the variant is not restricted: no_relaxation, no_cached_relaxation, below_min_hits, over_key_limit, no_key_alias
    reason: str | None = None


def restrict_with_cache(
    query: str,
    cache_handler: AbstractCacheHandler,
    partition_key: str,
    variant_settings: Mapping[str, Any] | None = None,
    *,
    lazy: bool = False,
    method: str = "IN",
    max_restriction_keys: int | None = None,
    min_cache_hits: int = 1,
    sql_dialect: str = "postgres",
) -> Restriction:
    """Restrict a query variant to the cached keys of its relaxations, with the lookup and extension functions of a query.

    The restricted variant returns the same keys as the variant, as long as every cached entry holds the keys of its
    query and ``variant_settings`` are those the variants were generated with (see ``lookup_settings``).
    For a key generated without partition key joins (skip_partition_key_joins), the tables of a variant are not all
    joined on the key; then only the table whose key the variant selects is restricted, as restricting every table
    (TMP_TABLE_JOIN without an alias) would drop result rows. Other keys are restricted as a query is.

    Args:
        query: The query variant.
        cache_handler: The cache to look up; lazy needs an AbstractCacheHandler_Lazy.
        partition_key: The partition key the variant selects.
        variant_settings: The keyword arguments of generate_all_query_hash_pairs the variants were generated with.
        lazy: Restrict with a subquery on the cache (``get_partition_keys_lazy``) instead of the fetched keys.
        method: One of EAGER_METHODS, or with lazy one of LAZY_METHODS.
        max_restriction_keys: Eager only: do not restrict when the intersection has more keys.
        min_cache_hits: Eager only: do not restrict with fewer cached relaxations.
        sql_dialect: Dialect of the database the variant runs on.

    Raises:
        ValueError: For an unknown method, a key limit with lazy, or a spatial or grid key.
    """
    kwargs = lookup_settings(variant_settings)
    if method not in (LAZY_METHODS if lazy else EAGER_METHODS):
        raise ValueError(f"Unknown {'lazy' if lazy else 'eager'} restriction method {method!r}")
    if lazy and max_restriction_keys is not None:
        raise ValueError("A key limit needs the key count, which only the eager lookup fetches")

    p0_alias = None
    if kwargs["skip_partition_key_joins"]:
        p0_alias = variant_key_alias(query, partition_key, sql_dialect)
        if p0_alias is None:
            return Restriction(query, reason="no_key_alias")

    if lazy:
        subquery, generated, hits = get_partition_keys_lazy(query=query, cache_handler=cache_handler, partition_key=partition_key, **kwargs)
        if hits == 0 or not subquery:
            return Restriction(query, generated=generated, hits=hits, reason=_miss(generated))
        restricted = extend_query_with_partition_keys_lazy(
            query=query,
            lazy_subquery=subquery,
            partition_key=partition_key,
            method=method,  # type: ignore[arg-type]
            p0_alias=p0_alias,
            analyze_tmp_table=True,
            sql_dialect=sql_dialect,
        )
        return Restriction(restricted, restricted=True, generated=generated, hits=hits, method=method)

    keys, generated, hits = get_partition_keys(query=query, cache_handler=cache_handler, partition_key=partition_key, **kwargs)
    if keys is None or hits == 0:
        return Restriction(query, generated=generated, hits=hits, reason=_miss(generated))
    if hits < min_cache_hits:
        return Restriction(query, generated=generated, hits=hits, keys=len(keys), reason="below_min_hits")
    if max_restriction_keys is not None and len(keys) > max_restriction_keys:
        # A restriction to most of the keys filters little and costs a long key list
        return Restriction(query, generated=generated, hits=hits, keys=len(keys), reason="over_key_limit")
    restricted = extend_query_with_partition_keys(
        query=query,
        partition_keys=keys,
        partition_key=partition_key,
        method=method,  # type: ignore[arg-type]
        p0_alias=p0_alias,
        analyze_tmp_table=True,
        sql_dialect=sql_dialect,
    )
    return Restriction(restricted, restricted=True, empty=not keys, generated=generated, hits=hits, keys=len(keys), method=method)


def _miss(generated: int) -> str:
    # The lookup generates the variant's own hash too; it is not cached, or the variant would not run
    return "no_relaxation" if generated <= 1 else "no_cached_relaxation"


@dataclass
class RestrictionCounts:
    """Variants by the outcome of their restriction."""

    restricted: int = 0
    empty_without_execution: int = 0
    no_relaxation: int = 0  # the lookup generated no sub-variant besides the variant (a smallest variant)
    no_cached_relaxation: int = 0  # none of its sub-variants was cached
    below_min_hits: int = 0
    over_key_limit: int = 0
    no_key_alias: int = 0  # no key joins, and the variant selects no qualified key column
    lookup_failed: int = 0

    def count_unrestricted(self, reason: str) -> None:
        setattr(self, reason, getattr(self, reason) + 1)

    def unrestricted_warning(self) -> str | None:
        """A warning if variants had relaxation candidates but none was restricted, else None."""
        candidates = self.no_cached_relaxation + self.below_min_hits + self.over_key_limit + self.no_key_alias + self.lookup_failed
        if self.restricted or self.empty_without_execution or not candidates:
            return None
        return (
            f"Restriction is on, but no variant was restricted: {self.no_cached_relaxation} found no cached relaxation, "
            f"{self.below_min_hits} too few, {self.over_key_limit} too many keys, {self.no_key_alias} no qualified key "
            f"column, {self.lookup_failed} lookups failed. Relaxations that failed, timed out or are still running are "
            "not cached; otherwise check that the lookup uses the settings the variants were generated with."
        )


@dataclass
class PopulationStats(RestrictionCounts):
    """Counts of what ``populate_query_variants`` did with the variants of one query."""

    executed: int = 0
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
    method: Literal["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN", "IN_SUBQUERY"] = "IN",
    sql_dialect: str = "postgres",
    variant_settings: Mapping[str, Any] | None = None,
    lazy: bool = False,
) -> PopulationStats:
    """Execute and cache the variants of one query, smallest first, each restricted to the cached keys of its relaxations.

    Args:
        query_hash_pairs: (variant_query, hash) pairs of one query, as from ``generate_all_query_hash_pairs``.
        partition_key: The partition key column the variants select.
        cache_handler: Where the variant keys are stored and looked up. Variants already cached are not executed.
        db_handler: Executes the (restricted) variants and returns their partition keys. It must raise on errors:
            a failure reported as an empty result would be cached as "no rows" and, through the intersection,
            empty every variant it restricts (SQLiteDBHandler logs errors and returns []).
        restrict: Restrict each variant with the cache, see ``restrict_with_cache``.
        max_restriction_keys: Eager only: restrict only when the intersection has at most this many keys; a
            restriction to most of the keys filters little and costs a long key list. None means no limit.
        method: How the keys are attached: IN, VALUES, TMP_TABLE_IN or TMP_TABLE_JOIN; with lazy IN_SUBQUERY,
            TMP_TABLE_IN or TMP_TABLE_JOIN.
        sql_dialect: Dialect of the database, used to render the restriction (e.g. "mysql").
        variant_settings: The keyword arguments of generate_all_query_hash_pairs the pairs were generated with,
            e.g. ``{"min_component_size": 1, "bucket_steps": 0}``. The lookup of each variant generates its
            sub-variants with them; missing ones take the defaults of generate_all_query_hash_pairs. With other
            settings than the pairs', the lookup finds fewer relaxations, and with another skip_partition_key_joins
            it can hit entries that are not relaxations.
        lazy: Restrict with a subquery on the cache instead of the fetched keys; needs an AbstractCacheHandler_Lazy
            whose cache the database can read, and no key limit.

    Returns:
        PopulationStats. An empty eager intersection means the variant has no rows; it is stored as an empty set
        without being executed. A warning is logged if variants had relaxations but none was restricted.
    """
    key_list_less = [name for name in _KEY_LIST_LESS_SETTINGS if (variant_settings or {}).get(name) is not None]
    if restrict and key_list_less:
        logger.warning(f"Variants generated with {key_list_less} are populated unrestricted: their restriction is not a key list")
        restrict = False
    if restrict:
        lookup_settings(variant_settings)  # raises for unknown settings
        if method not in (LAZY_METHODS if lazy else EAGER_METHODS):
            raise ValueError(f"Unknown {'lazy' if lazy else 'eager'} restriction method {method!r}")
        if lazy and not isinstance(cache_handler, AbstractCacheHandler_Lazy):
            raise ValueError("lazy restriction needs a cache handler with lazy intersection")
        if lazy and max_restriction_keys is not None:
            raise ValueError("A key limit needs the key count, which only the eager lookup fetches")

    stats = PopulationStats()
    for variant, variant_hash in order_variants_for_population(query_hash_pairs):
        if cache_handler.exists(variant_hash, partition_key):
            stats.already_cached += 1
            cache_handler.set_query(variant_hash, variant, partition_key)  # keep the query text current
            continue

        to_execute = variant
        if restrict:
            try:
                restriction = restrict_with_cache(
                    variant,
                    cache_handler,
                    partition_key,
                    variant_settings,
                    lazy=lazy,
                    method=method,
                    max_restriction_keys=max_restriction_keys,
                    sql_dialect=sql_dialect,
                )
            except Exception as e:
                logger.warning(f"Cache lookup failed for variant {variant_hash}, it runs unrestricted: {e}")
                stats.lookup_failed += 1
            else:
                if restriction.empty:
                    if not cache_handler.set_entry(variant_hash, set(), variant, partition_key, force_update=True):
                        logger.warning(f"Failed to store the empty result of variant {variant_hash}")
                        stats.store_failed += 1
                    stats.empty_without_execution += 1
                    continue
                if restriction.restricted:
                    to_execute = restriction.query
                    stats.restricted += 1
                else:
                    stats.count_unrestricted(restriction.reason or "no_relaxation")

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

    warning = stats.unrestricted_warning() if restrict else None
    if warning is not None:
        logger.warning(warning)
    return stats
