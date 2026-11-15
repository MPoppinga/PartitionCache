"""
TMP_TABLE_JOIN and UNLOGGED_TABLE_JOIN join the key table to the p0 table by default; join_every_table opts in to
joining every table, which is only equivalent when every table is joined on the partition key.
"""

import re
from unittest.mock import Mock

import pytest

from partitioncache.apply_cache import apply_cache, apply_cache_lazy, extend_query_with_partition_keys, extend_query_with_partition_keys_lazy
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy

JOINED = "SELECT a.pk FROM a, b, c WHERE a.pk = b.pk AND b.pk = c.pk AND b.y = 1 AND c.z = 2"
TIME_RELATED = "SELECT a.pk FROM a, b WHERE b.t >= a.t AND b.t <= a.t + 10 AND b.y = 1"
SUBQUERY = "SELECT 1 AS pk"


def _joined_aliases(script: str) -> set[str]:
    return set(re.findall(r"AS tmp_(\w+) ON", script))


def _eager(query, method, **kwargs):
    return extend_query_with_partition_keys(query, {1, 2}, "pk", method=method, **kwargs)


def _lazy(query, method, **kwargs):
    return extend_query_with_partition_keys_lazy(query, SUBQUERY, "pk", method=method, **kwargs)


@pytest.mark.parametrize("extend", [_eager, _lazy], ids=["eager", "lazy"])
@pytest.mark.parametrize("method", ["TMP_TABLE_JOIN", "UNLOGGED_TABLE_JOIN"])
class TestJoinMethods:
    def test_default_joins_the_p0_table(self, extend, method):
        assert _joined_aliases(extend(JOINED, method)) == {"a"}
        assert _joined_aliases(extend(TIME_RELATED, method)) == {"a"}

    def test_given_alias_is_joined(self, extend, method):
        assert _joined_aliases(extend(JOINED, method, p0_alias="b")) == {"b"}

    def test_join_every_table_when_every_table_is_joined_on_the_key(self, extend, method):
        assert _joined_aliases(extend(JOINED, method, join_every_table=True)) == {"a", "b", "c"}

    @pytest.mark.parametrize(
        "query",
        [
            TIME_RELATED,
            "SELECT a.pk FROM a, b, c WHERE a.pk = b.pk AND b.y = 1 AND c.z = a.z",  # c is not joined on the key
            "SELECT a.pk FROM a, b WHERE (a.pk = b.pk OR b.y = 1)",  # under OR the equality does not hold for every row
            "SELECT lo.pk FROM lineorder AS lo, customer AS c WHERE lo.custkey = c.custkey",  # a dimension, joined otherwise
        ],
    )
    def test_join_every_table_is_refused_otherwise(self, extend, method, query):
        with pytest.raises(ValueError, match="every table joined on pk"):
            extend(query, method, join_every_table=True)

    def test_join_every_table_takes_no_alias(self, extend, method):
        with pytest.raises(ValueError, match="no p0_alias"):
            extend(JOINED, method, p0_alias="a", join_every_table=True)


def test_join_every_table_joins_through_join_on_conditions():
    query = "SELECT a.pk FROM a JOIN b ON a.pk = b.pk JOIN c ON (c.pk = b.pk AND c.z = 2)"
    assert _joined_aliases(_eager(query, "TMP_TABLE_JOIN", join_every_table=True)) == {"a", "b", "c"}


@pytest.mark.parametrize("method", ["IN", "VALUES", "TMP_TABLE_IN"])
def test_join_every_table_is_only_for_the_join_methods(method):
    with pytest.raises(ValueError, match="applies to TMP_TABLE_JOIN and UNLOGGED_TABLE_JOIN"):
        _eager(JOINED, method, join_every_table=True)


def test_apply_cache_passes_the_option():
    handler = Mock(spec=AbstractCacheHandler)
    handler.get_intersected.return_value = ({1, 2}, 2)
    default, _ = apply_cache(JOINED, handler, "pk", method="TMP_TABLE_JOIN", min_component_size=1)
    every, _ = apply_cache(JOINED, handler, "pk", method="TMP_TABLE_JOIN", min_component_size=1, join_every_table=True)
    assert _joined_aliases(default) == {"a"} and _joined_aliases(every) == {"a", "b", "c"}


def test_apply_cache_lazy_passes_the_option():
    handler = Mock(spec=AbstractCacheHandler_Lazy)
    handler.get_intersected_lazy.return_value = (SUBQUERY, 2)
    default, _ = apply_cache_lazy(JOINED, handler, "pk", method="TMP_TABLE_JOIN", min_component_size=1)
    every, _ = apply_cache_lazy(JOINED, handler, "pk", method="TMP_TABLE_JOIN", min_component_size=1, join_every_table=True)
    assert _joined_aliases(default) == {"a"} and _joined_aliases(every) == {"a", "b", "c"}


def test_monitor_passes_the_option():
    from partitioncache.cli.monitor_cache_queue import apply_cache_optimization

    args = Mock()
    args.enable_cache_optimization, args.disable_cache_optimization, args.prefer_lazy_optimization = True, False, False
    args.max_restriction_keys, args.min_cache_hits, args.db_backend = None, 1, "postgresql"
    args.cache_optimization_method, args.join_every_table = "TMP_TABLE_JOIN", True
    args.planner_collapse_limit = None
    args.fold_key_subqueries = None
    args.min_component_size, args.follow_graph, args.no_auto_detect_partition_join = 1, True, False
    args.max_component_size, args.partition_join_table, args.bucket_steps = None, None, 1.0
    args.skip_partition_key_joins, args.max_conditions_removed, args.grid_cell_size = [], None, None
    handler = Mock(spec=AbstractCacheHandler)
    handler.get_intersected.return_value = ({1, 2}, 2)
    fragment = "SELECT DISTINCT t1.pk FROM a AS t1, b AS t2 WHERE t1.pk = t2.pk AND t2.y = 1"

    restricted, applied, _ = apply_cache_optimization(fragment, "h", "pk", "integer", handler, args)

    assert applied and _joined_aliases(restricted) == {"t1", "t2"}
