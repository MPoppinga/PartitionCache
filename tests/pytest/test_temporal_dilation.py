"""Tests for temporal dilation of bucketed partition keys.

The property under test is soundness: intersecting per-fragment bucket sets drops correlated
pairs whose two events fall in adjacent buckets. Dilating each set *before* the intersection
restores them. Dilating afterwards does not, and that asymmetry is what these tests pin down.
"""

import pytest

from partitioncache.apply_cache import _intersect_dilated_fallback, _resolve_dilation
from partitioncache.temporal_dilation import compute_dilation_buckets, dilate_bucket_set


class TestComputeDilationBuckets:
    @pytest.mark.parametrize(
        ("window", "width", "expected"),
        [
            (3600, 3600, 1),      # window equals bucket width -> one neighbour each side
            (10, 3600, 1),        # any non-zero window needs at least one bucket
            (86400, 3600, 24),    # 24h window on 1h buckets: the pathological case
            (86400, 86400, 1),    # same window on day buckets stays at 1
            (5400, 3600, 2),      # 1.5 buckets rounds up
            (0, 3600, 0),         # no window -> plain intersection is already sound
        ],
    )
    def test_dilation(self, window: float, width: float, expected: int):
        assert compute_dilation_buckets(window, width) == expected

    @pytest.mark.parametrize(("window", "width"), [(3600, 0), (3600, -1), (-1, 3600)])
    def test_invalid_inputs(self, window: float, width: float):
        with pytest.raises(ValueError):
            compute_dilation_buckets(window, width)


class TestDilateBucketSet:
    def test_zero_dilation_is_identity(self):
        assert dilate_bucket_set({5, 9}, 0) == {5, 9}

    def test_single_bucket_expansion(self):
        assert dilate_bucket_set({5}, 1) == {4, 5, 6}

    def test_wider_expansion(self):
        assert dilate_bucket_set({5}, 2) == {3, 4, 5, 6, 7}

    def test_overlapping_ranges_merge(self):
        assert dilate_bucket_set({5, 6}, 1) == {4, 5, 6, 7}

    def test_empty_set(self):
        assert dilate_bucket_set(set(), 3) == set()

    def test_does_not_mutate_input(self):
        original = {5}
        dilate_bucket_set(original, 1)
        assert original == {5}

    def test_negative_dilation_rejected(self):
        with pytest.raises(ValueError):
            dilate_bucket_set({1}, -1)


class TestResolveDilation:
    QUERY = (
        "SELECT e1.w FROM auth_events AS e1, auth_events AS e2 "
        "WHERE e1.event_code = 4625 AND e2.event_code = 4624 "
        "AND e2.src_user_id = e1.src_user_id "
        "AND e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 3600"
    )

    def test_derived_from_query_window(self):
        assert _resolve_dilation(self.QUERY, 3600.0, None, "ts_epoch", "w") == 1
        assert _resolve_dilation(self.QUERY, 900.0, None, "ts_epoch", "w") == 4

    def test_window_needs_the_time_column(self):
        with pytest.raises(ValueError, match="temporal_column"):
            _resolve_dilation(self.QUERY, 3600.0, None)

    def test_explicit_override_wins(self):
        assert _resolve_dilation(self.QUERY, 3600.0, 7) == 7

    def test_explicit_zero_disables_dilation(self):
        """--no-dilation must be able to turn the feature off for the A/B measurement."""
        assert _resolve_dilation(self.QUERY, 3600.0, 0) == 0

    def test_disabled_without_bucket_width(self):
        assert _resolve_dilation(self.QUERY, None, None) == 0

    def test_query_without_window_needs_no_dilation(self):
        query = "SELECT e1.w FROM auth_events AS e1 WHERE e1.event_code = 4625"
        assert _resolve_dilation(query, 3600.0, None, "ts_epoch", "w") == 0

    def test_unbounded_pair_allows_no_dilation(self):
        query = "SELECT e1.w FROM auth_events AS e1, auth_events AS e2 WHERE e2.ts_epoch <= e1.ts_epoch + 3600"
        assert _resolve_dilation(query, 3600.0, None, "ts_epoch", "w") is None

    def test_negative_override_rejected(self):
        with pytest.raises(ValueError):
            _resolve_dilation(self.QUERY, 3600.0, -1)


class _FakeHandler:
    """Minimal cache handler exposing only what the generic fallback needs."""

    def __init__(self, sets: dict[str, set[int]], datatype: str = "integer"):
        self._sets = sets
        self._datatype = datatype

    def get_datatype(self, partition_key):
        return self._datatype

    def filter_existing_keys(self, keys, partition_key="partition_key"):
        return {k for k in keys if k in self._sets}

    def get(self, key, partition_key="partition_key"):
        return self._sets.get(key)


class TestIntersectDilatedFallback:
    # The anchor event sits in bucket 10; its partner sits in bucket 11, just across the
    # boundary. Fragment A sees only bucket 10, fragment B only bucket 11.
    STRADDLING = {"a": {10}, "b": {11}}

    def test_undilated_intersection_loses_the_pair(self):
        """Baseline: this is the bug dilation exists to fix."""
        handler = _FakeHandler(self.STRADDLING)
        result, count = _intersect_dilated_fallback(handler, {"a", "b"}, "bucket_1h", 0)
        assert result == set()
        assert count == 2

    def test_dilation_recovers_the_anchor_bucket(self):
        handler = _FakeHandler(self.STRADDLING)
        result, _ = _intersect_dilated_fallback(handler, {"a", "b"}, "bucket_1h", 1)
        assert result == {10, 11}

    def test_dilating_after_intersection_would_be_unsound(self):
        """dilate(A ∩ B) is empty here while dilate(A) ∩ dilate(B) is not.

        This is why each cached set must be widened before the intersection, matching the
        buffer-then-intersect order the spatial handlers use.
        """
        a, b = self.STRADDLING["a"], self.STRADDLING["b"]
        assert dilate_bucket_set(a & b, 1) == set()
        assert dilate_bucket_set(a, 1) & dilate_bucket_set(b, 1) == {10, 11}

    def test_non_straddling_case_is_unaffected_in_content(self):
        handler = _FakeHandler({"a": {10}, "b": {10}})
        result, _ = _intersect_dilated_fallback(handler, {"a", "b"}, "bucket_1h", 1)
        assert 10 in result

    def test_no_cache_hits(self):
        handler = _FakeHandler({})
        assert _intersect_dilated_fallback(handler, {"a"}, "bucket_1h", 1) == (None, 0)

    def test_missing_set_is_skipped(self):
        """A key that filters as present but returns None must not poison the intersection."""

        class Handler(_FakeHandler):
            def get(self, key, partition_key="partition_key"):
                return None if key == "b" else self._sets.get(key)

        handler = Handler({"a": {10}, "b": {11}})
        result, _ = _intersect_dilated_fallback(handler, {"a", "b"}, "bucket_1h", 1)
        assert result == {9, 10, 11}

    def test_non_integer_keys_run_without_the_cache(self):
        handler = _FakeHandler({"a": {"x"}}, datatype="text")
        assert _intersect_dilated_fallback(handler, {"a"}, "bucket_1h", 1) == (None, 0)


class TestTemporalLookup:
    """A temporal key gets variants without partition key joins; queries no dilation can cover run without the cache."""

    QUERY = "SELECT e1.w FROM ev AS e1, ev AS e2 WHERE e1.kind = 1 AND e2.kind = 2 AND e2.ts BETWEEN e1.ts AND e1.ts + 1200"

    def _handler(self, sets):
        from unittest.mock import MagicMock

        from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

        handler = MagicMock(spec=AbstractCacheHandler_Lazy)
        handler.get_datatype.return_value = "integer"
        handler.filter_existing_keys.side_effect = lambda keys, partition_key="w": {k for k in keys if k in sets}
        handler.get.side_effect = lambda key, partition_key="w": sets.get(key)
        return handler

    def test_temporal_key_uses_the_variants_without_partition_key_joins(self):
        from partitioncache.apply_cache import get_partition_keys
        from partitioncache.query_processor import generate_all_query_hash_pairs

        pairs = generate_all_query_hash_pairs(self.QUERY, "w", min_component_size=1, skip_partition_key_joins=True, warn_no_partition_key=False)
        assert all(" = t2.w" not in variant and "t1.w = " not in variant for variant, _ in pairs)
        handler = self._handler({key: {10} for _, key in pairs})
        keys, generated, hits = get_partition_keys(self.QUERY, handler, "w", min_component_size=1, temporal_bucket_width=3600, temporal_column="ts")
        assert hits == len(pairs) == generated
        assert keys == {9, 10, 11}

    def test_unbounded_query_runs_without_the_cache(self):
        from partitioncache.apply_cache import get_partition_keys, get_partition_keys_lazy

        query = "SELECT e1.w FROM ev AS e1, ev AS e2 WHERE e1.kind = 1 AND e2.ts <= e1.ts + 1200"
        handler = self._handler({})
        assert get_partition_keys(query, handler, "w", temporal_bucket_width=3600, temporal_column="ts") == (None, 0, 0)
        assert get_partition_keys_lazy(query, handler, "w", temporal_bucket_width=3600, temporal_column="ts") == (None, 0, 0)
        handler.get_intersected.assert_not_called()
        handler.get_intersected_lazy.assert_not_called()

    def test_lazy_handler_without_dilated_intersection_runs_without_the_cache(self):
        from partitioncache.apply_cache import get_partition_keys_lazy

        handler = self._handler({})
        assert not hasattr(handler, "get_intersected_dilated_lazy")
        assert get_partition_keys_lazy(self.QUERY, handler, "w", temporal_bucket_width=3600, temporal_column="ts") == (None, 0, 0)
        handler.get_intersected_lazy.assert_not_called()
