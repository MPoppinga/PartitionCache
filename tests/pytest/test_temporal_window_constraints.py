"""Tests for compute_temporal_window: the time window a correlation query spans between rows of the partition key's table.

The window decides how far each cached bucket set is dilated. Too small a window loses rows, so only conditions on the
configured time column count, and only differences bounded from both sides.
"""

import pytest

from partitioncache.query_processor import compute_buffer_distance
from partitioncache.temporal_dilation import compute_temporal_window

EVENTS = "SELECT e1.w FROM auth_events AS e1, auth_events AS e2 WHERE e1.event_code = 4625 AND "


def _window(condition: str) -> float | None:
    return compute_temporal_window(EVENTS + condition, "ts", "w")


class TestTwoSidedWindows:
    @pytest.mark.parametrize(
        ("condition", "expected"),
        [
            ("e2.ts BETWEEN e1.ts AND e1.ts + 3600", 3600.0),
            ("e2.ts BETWEEN e1.ts - 1800 AND e1.ts + 1800", 1800.0),
            # Asymmetric window: the farther side counts
            ("e2.ts BETWEEN e1.ts - 60 AND e1.ts + 900", 900.0),
            ("ABS(e1.ts - e2.ts) <= 3600", 3600.0),
            ("ABS(e1.ts - e2.ts) < 10.5", 10.5),
            ("3600 >= ABS(e2.ts - e1.ts)", 3600.0),
            # Lower and upper bound in separate conditions, as the variant generator writes a BETWEEN
            ("e1.ts <= e2.ts AND e2.ts <= e1.ts + 1200", 1200.0),
            ("e2.ts - e1.ts <= 600 AND e2.ts - e1.ts >= -300", 600.0),
            ("e1.ts = e2.ts", 0.0),
            ("(e2.ts BETWEEN e1.ts AND e1.ts + 3600)", 3600.0),
            # Several bounds on one pair: the tightest holds for every row
            ("e2.ts BETWEEN e1.ts AND e1.ts + 3600 AND ABS(e1.ts - e2.ts) <= 100", 100.0),
        ],
    )
    def test_window(self, condition, expected):
        assert _window(condition) == expected

    def test_on_condition_of_an_inner_join(self):
        query = "SELECT e1.w FROM auth_events AS e1 JOIN auth_events AS e2 ON e2.ts BETWEEN e1.ts AND e1.ts + 600"
        assert compute_temporal_window(query, "ts", "w") == 600.0


class TestOtherColumnsAndUnitsDoNotCount:
    """A bound on another column read as the window gave k = 1 instead of 2 and lost rows."""

    @pytest.mark.parametrize(
        "condition",
        [
            "ABS(e1.alt - e2.alt) < 100 AND e2.ts BETWEEN e1.ts AND e1.ts + 1200",
            "e2.ts BETWEEN e1.ts AND e1.ts + 1200 AND ABS(e1.alt - e2.alt) < 100",
            "ST_DWithin(e1.geom, e2.geom, 50) AND e2.ts BETWEEN e1.ts AND e1.ts + 1200",
            "ST_DWithin(e1.geom, e2.geom, 5556) AND e2.ts BETWEEN e1.ts AND e1.ts + 1200",
        ],
    )
    def test_only_the_time_column_counts(self, condition):
        assert _window(condition) == 1200.0

    def test_spatial_buffer_ignores_time_windows(self):
        query = EVENTS + "ST_DWithin(e1.geom, e2.geom, 50) AND e2.ts BETWEEN e1.ts AND e1.ts + 1200"
        assert compute_buffer_distance(query) == 50.0


class TestUnboundedPairs:
    """Without a bound on both sides, the other row can be arbitrarily far away: no dilation is sound (None)."""

    @pytest.mark.parametrize(
        "condition",
        [
            "e2.ts <= e1.ts + 1200",
            "3600 >= e2.ts - e1.ts",
            "e2.ts > e1.ts",
            "e2.ts >= e1.ts - 900",
            "e2.ts BETWEEN e1.ts AND e1.other_ts",
            "e2.user_id = e1.user_id",
            # Inside OR the window need not hold
            "(e2.ts BETWEEN e1.ts AND e1.ts + 5 OR e1.x = 1)",
            # Contradictory bounds are left out rather than guessed
            "e2.ts - e1.ts >= 10 AND e2.ts - e1.ts <= 5",
        ],
    )
    def test_unbounded(self, condition):
        assert _window(condition) is None

    def test_outer_join_condition_does_not_bound(self):
        query = "SELECT e1.w FROM auth_events AS e1 LEFT JOIN auth_events AS e2 ON e2.ts BETWEEN e1.ts AND e1.ts + 600"
        assert compute_temporal_window(query, "ts", "w") is None

    def test_third_alias_without_a_window(self):
        query = "SELECT e1.w FROM ev AS e1, ev AS e2, ev AS e3 WHERE e2.ts BETWEEN e1.ts AND e1.ts + 600 AND e3.kind = 1"
        assert compute_temporal_window(query, "ts", "w") is None


class TestAliasesOfThePartitionKeyTable:
    def test_single_alias(self):
        assert compute_temporal_window("SELECT e1.w FROM ev AS e1 WHERE e1.kind = 1", "ts", "w") == 0.0

    def test_dimension_table_is_ignored(self):
        query = "SELECT e1.w FROM ev AS e1, users AS u, ev AS e2 WHERE u.id = e1.uid AND e2.ts BETWEEN e1.ts AND e1.ts + 60"
        assert compute_temporal_window(query, "ts", "w") == 60.0

    def test_without_the_partition_key_in_select_every_alias_counts(self):
        """The generator then selects the partition key from every table, also from logins, which no window bounds."""
        query = "SELECT COUNT(*) FROM events AS e, logins AS l WHERE e.uid = l.uid AND e.ts > 0 AND l.kind = 1"
        assert compute_temporal_window(query, "ts", "w") is None
        query = "SELECT COUNT(*) FROM ev AS e1, ev AS e2 WHERE e2.ts BETWEEN e1.ts AND e1.ts + 60"
        assert compute_temporal_window(query, "ts", "w") == 60.0

    def test_explicit_source_table(self):
        query = "SELECT 1 FROM ev AS e1, logins AS l WHERE l.uid = e1.uid"
        assert compute_temporal_window(query, "ts", partition_key_source_table="ev") == 0.0
        assert compute_temporal_window(query, "ts") is None


class TestChainedWindows:
    def test_chain_adds_up(self):
        """e1 -600-> e2 -300-> e3 spans 900 seconds end to end."""
        query = "SELECT e1.w FROM ev AS e1, ev AS e2, ev AS e3 WHERE e2.ts BETWEEN e1.ts AND e1.ts + 600 AND e3.ts BETWEEN e2.ts AND e2.ts + 300"
        assert compute_temporal_window(query, "ts", "w") == 900.0

    def test_star_runs_through_the_hub(self):
        query = "SELECT e1.w FROM ev AS e1, ev AS e2, ev AS e3 WHERE e2.ts BETWEEN e1.ts AND e1.ts + 600 AND e3.ts BETWEEN e1.ts AND e1.ts + 300"
        assert compute_temporal_window(query, "ts", "w") == 900.0
