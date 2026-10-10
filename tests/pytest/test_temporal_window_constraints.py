"""Tests for bounded range window constraints (pass 3 of extract_distance_constraints).

The motivating case is the correlation window of a detection rule, which plays the same role
in time that a distance constraint plays in space:

    e2.ts BETWEEN e1.ts AND e1.ts + 3600      <->    ST_DWithin(p1.geom, p2.geom, 400)

These windows must feed compute_buffer_distance() so that a temporal partition key can be
dilated by the correct number of buckets before intersection.
"""

import pytest

from partitioncache.query_processor import (
    compute_buffer_distance,
    extract_distance_constraints,
)

EVENTS_FROM = "SELECT 1 FROM auth_events AS e1, auth_events AS e2 WHERE e1.event_code = 4625 AND "


def _constraints(condition: str) -> list[tuple[str, str, float]]:
    return extract_distance_constraints(EVENTS_FROM + condition)


class TestRangeWindowRecognition:
    """Window shapes that must be recognised."""

    @pytest.mark.parametrize(
        ("condition", "expected"),
        [
            # Ordered window, the index-friendly form used by the security-log benchmark
            ("e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 3600", 3600.0),
            # Symmetric window: half-widths on both sides
            ("e2.ts_epoch BETWEEN e1.ts_epoch - 1800 AND e1.ts_epoch + 1800", 1800.0),
            # Asymmetric window takes the wider side
            ("e2.ts_epoch BETWEEN e1.ts_epoch - 60 AND e1.ts_epoch + 900", 900.0),
            ("ABS(e1.ts_epoch - e2.ts_epoch) <= 3600", 3600.0),
            ("e2.ts_epoch - e1.ts_epoch <= 3600", 3600.0),
            ("e2.ts_epoch - e1.ts_epoch < 600", 600.0),
            # Mirrored comparison: literal on the left
            ("3600 >= e2.ts_epoch - e1.ts_epoch", 3600.0),
            # Bound expressed as a column plus a literal offset
            ("e2.ts_epoch <= e1.ts_epoch + 3600", 3600.0),
            ("e2.ts_epoch < e1.ts_epoch + 600", 600.0),
            # Float widths survive
            ("ABS(e1.ts_epoch - e2.ts_epoch) <= 10.5", 10.5),
        ],
    )
    def test_window_extracted(self, condition: str, expected: float):
        result = _constraints(condition)
        assert result == [("e1", "e2", expected)]


class TestRangeWindowRejection:
    """Predicates that must NOT be read as a window."""

    @pytest.mark.parametrize(
        "condition",
        [
            # Unbounded ordering constraint carries no width
            "e2.ts_epoch > e1.ts_epoch",
            # Equality join is not a window
            "e2.user_id = e1.user_id",
            # Single alias: not a constraint between two aliases
            "e1.ts_epoch <= 3600",
            # Bounded, but both bounds are plain columns (no literal offset)
            "e2.ts_epoch BETWEEN e1.ts_epoch AND e1.other_ts",
            # Zero-width window is not a useful buffer
            "e2.ts_epoch - e1.ts_epoch <= 0",
            # Lower bound only: unbounded above, so there is no window width
            "e2.ts_epoch >= e1.ts_epoch - 900",
            "e2.ts_epoch <= e1.ts_epoch",
        ],
    )
    def test_no_window_extracted(self, condition: str):
        assert _constraints(condition) == []

    def test_three_aliases_ignored(self):
        """A predicate spanning three aliases is not a pairwise window."""
        query = "SELECT 1 FROM ev AS e1, ev AS e2, ev AS e3 WHERE e1.ts - e2.ts + e3.ts <= 60"
        assert extract_distance_constraints(query) == []


class TestPassPrecedence:
    """Pass 3 must not disturb the ST_DWithin and is_distance_function passes."""

    def test_st_dwithin_wins_for_same_pair(self):
        """When pass 1 already claimed the alias pair, pass 3 stays out of the way."""
        query = (
            "SELECT * FROM t AS p1, t AS p2 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 500) AND p2.ts - p1.ts <= 99"
        )
        assert extract_distance_constraints(query) == [("p1", "p2", 500.0)]

    def test_spatial_and_temporal_pairs_coexist(self):
        """A spatial edge and a temporal edge on different pairs both survive."""
        query = (
            "SELECT * FROM t AS p1, t AS p2, t AS p3 "
            "WHERE ST_DWithin(p1.geom, p2.geom, 500) "
            "AND p3.ts BETWEEN p2.ts AND p2.ts + 120"
        )
        assert extract_distance_constraints(query) == [
            ("p1", "p2", 500.0),
            ("p2", "p3", 120.0),
        ]


class TestBufferDistanceFromWindows:
    """compute_buffer_distance() must chain windows the same way it chains distances."""

    def test_single_window(self):
        query = EVENTS_FROM + "e2.ts_epoch BETWEEN e1.ts_epoch AND e1.ts_epoch + 3600"
        assert compute_buffer_distance(query) == 3600.0

    def test_chained_windows_sum_along_path(self):
        """Sigma chained correlation: e1 -600-> e2 -300-> e3 spans 900 seconds end to end."""
        query = (
            "SELECT 1 FROM ev AS e1, ev AS e2, ev AS e3 "
            "WHERE e2.ts BETWEEN e1.ts AND e1.ts + 600 "
            "AND e3.ts BETWEEN e2.ts AND e2.ts + 300"
        )
        assert compute_buffer_distance(query) == 900.0

    def test_star_pattern_takes_two_largest(self):
        """All windows anchored on e1: diameter runs through the hub."""
        query = (
            "SELECT 1 FROM ev AS e1, ev AS e2, ev AS e3 "
            "WHERE e2.ts BETWEEN e1.ts AND e1.ts + 600 "
            "AND e3.ts BETWEEN e1.ts AND e1.ts + 300"
        )
        assert compute_buffer_distance(query) == 900.0

    def test_no_window_yields_zero(self):
        query = EVENTS_FROM + "e2.user_id = e1.user_id"
        assert compute_buffer_distance(query) == 0.0
