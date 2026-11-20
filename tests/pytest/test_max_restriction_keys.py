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
