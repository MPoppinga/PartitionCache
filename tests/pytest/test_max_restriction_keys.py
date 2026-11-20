"""max_restriction_keys: a cache restriction with more partition keys than the limit is not applied."""

from unittest.mock import Mock

from partitioncache.apply_cache import apply_cache, apply_cache_lazy
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy

QUERY = "SELECT a.pk FROM a, b WHERE a.pk = b.pk AND b.y = 1"
SUBQUERY = "SELECT 1 AS partition_key"


def test_apply_cache_leaves_out_a_restriction_above_the_limit():
    handler = Mock(spec=AbstractCacheHandler)
    handler.get_intersected.return_value = ({1, 2, 3}, 2)
    applied, applied_stats = apply_cache(QUERY, handler, "pk", min_component_size=1, max_restriction_keys=3)
    assert applied != QUERY and applied_stats["enhanced"] == 1 and "uncached_reason" not in applied_stats
    skipped, skipped_stats = apply_cache(QUERY, handler, "pk", min_component_size=1, max_restriction_keys=2)
    assert skipped == QUERY
    assert skipped_stats["enhanced"] == 0 and skipped_stats["cache_hits"] == 2 and skipped_stats["uncached_reason"] == "weak_restriction"


def test_apply_cache_lazy_counts_the_keys_only_with_a_limit():
    handler = Mock(spec=AbstractCacheHandler_Lazy)
    handler.get_intersected_lazy.return_value = (SUBQUERY, 2)
    handler.count_lazy_keys.return_value = 5
    applied, applied_stats = apply_cache_lazy(QUERY, handler, "pk", min_component_size=1)
    assert applied != QUERY and applied_stats["enhanced"] == 1
    handler.count_lazy_keys.assert_not_called()
    skipped, skipped_stats = apply_cache_lazy(QUERY, handler, "pk", min_component_size=1, max_restriction_keys=4)
    assert skipped == QUERY and skipped_stats["enhanced"] == 0 and skipped_stats["uncached_reason"] == "weak_restriction"
    handler.count_lazy_keys.assert_called_once_with(SUBQUERY)
    at_limit, at_limit_stats = apply_cache_lazy(QUERY, handler, "pk", min_component_size=1, max_restriction_keys=5)
    assert at_limit != QUERY and at_limit_stats["enhanced"] == 1


def test_apply_cache_lazy_applies_the_restriction_when_the_handler_cannot_count():
    handler = Mock(spec=AbstractCacheHandler_Lazy)
    handler.get_intersected_lazy.return_value = (SUBQUERY, 2)
    handler.count_lazy_keys.return_value = None
    applied, stats = apply_cache_lazy(QUERY, handler, "pk", min_component_size=1, max_restriction_keys=0)
    assert applied != QUERY and stats["enhanced"] == 1 and "uncached_reason" not in stats
    assert AbstractCacheHandler_Lazy.count_lazy_keys(handler, SUBQUERY) is None


VARIANT = "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.pk = t2.pk AND t2.y = 1"


def test_population_applies_the_limit_to_a_lazy_restriction():
    from partitioncache.population import restrict_with_cache

    cache = Mock(spec=AbstractCacheHandler_Lazy)
    cache.get_intersected_lazy.return_value = (SUBQUERY, 1)
    cache.count_lazy_keys.return_value = 5
    over = restrict_with_cache(VARIANT, cache, "pk", {}, lazy=True, method="IN_SUBQUERY", max_restriction_keys=4)
    assert not over.restricted and over.reason == "over_key_limit" and over.keys == 5 and over.query == VARIANT
    within = restrict_with_cache(VARIANT, cache, "pk", {}, lazy=True, method="IN_SUBQUERY", max_restriction_keys=5)
    assert within.restricted
    cache.count_lazy_keys.return_value = None  # a handler that cannot count restricts
    assert restrict_with_cache(VARIANT, cache, "pk", {}, lazy=True, method="IN_SUBQUERY", max_restriction_keys=0).restricted


def test_monitor_keeps_the_lazy_path_with_a_limit_on_a_postgresql_handler():
    from partitioncache.cache_handler.postgresql_abstract import PostgreSQLAbstractCacheHandler
    from partitioncache.cli.monitor_cache_queue import apply_cache_optimization

    args = Mock()
    args.enable_cache_optimization, args.disable_cache_optimization, args.prefer_lazy_optimization = True, False, True
    args.max_restriction_keys, args.min_cache_hits, args.db_backend = 4, 1, "postgresql"
    args.cache_optimization_method, args.join_every_table, args.isolate_key_table_join = "IN_SUBQUERY", False, False
    args.planner_collapse_limit, args.fold_key_subqueries = None, None
    args.min_component_size, args.follow_graph, args.no_auto_detect_partition_join = 1, True, False
    args.max_component_size, args.partition_join_table, args.bucket_steps = None, None, 1.0
    args.skip_partition_key_joins, args.max_conditions_removed, args.grid_cell_size = [], None, None
    handler = Mock(spec=PostgreSQLAbstractCacheHandler)
    handler.get_intersected_lazy.return_value = (SUBQUERY, 2)
    handler.count_lazy_keys.return_value = 5

    query, applied, _ = apply_cache_optimization(VARIANT, "h", "pk", "integer", handler, args)
    assert not applied and query == VARIANT
    handler.get_intersected.assert_not_called()

    handler.count_lazy_keys.return_value = 4
    query, applied, _ = apply_cache_optimization(VARIANT, "h", "pk", "integer", handler, args)
    assert applied and "IN (SELECT" in query
