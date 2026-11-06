"""Per-attribute bucket steps (dict form of bucket_steps) and multi-level buckets (list form)."""

from partitioncache.query_processor import generate_all_query_hash_pairs, normalize_attribute_conditions

Q = "SELECT * FROM segs AS t1 WHERE t1.alt_max <= 650 AND t1.vrate <= -1.5 AND t1.turn >= 400 AND t1.scale = 300"


def test_upper_bounds_round_up_lower_bounds_round_down():
    out = normalize_attribute_conditions(Q, {"alt_max": 500, "vrate": 1, "turn": 180})
    assert "t1.alt_max <= 1000" in out
    assert "t1.vrate <= -1" in out
    assert "t1.turn >= 360" in out
    assert "t1.scale = 300" in out  # equality untouched


def test_unmatched_and_on_grid_values_unchanged():
    out = normalize_attribute_conditions("SELECT * FROM t AS t1 WHERE t1.a <= 1000 AND t1.b < 7", {"a": 500})
    assert "t1.a <= 1000" in out and "t1.b < 7" in out


def test_number_on_left_and_between():
    out = normalize_attribute_conditions("SELECT * FROM t AS t1 WHERE 650 >= t1.alt AND t1.gap BETWEEN 70 AND 610",
                                         {"alt": 500, "gap": 300})
    assert "1000 >= t1.alt" in out
    assert "BETWEEN 0 AND 900" in out


def test_function_output_comparison():
    out = normalize_attribute_conditions("SELECT * FROM f AS t1 WHERE circle_ext(t1.x, t1.y, 1200) <= 2600",
                                         {"circle_ext(": 1000})
    assert "<= 3000" in out and "1200" in out  # argument untouched, only the bound


def test_levels_add_one_variant_set_per_level():
    base = {h for _, h in generate_all_query_hash_pairs(Q, "flight_id", bucket_steps=0)}
    one = {h for _, h in generate_all_query_hash_pairs(Q, "flight_id", bucket_steps={"alt_max": 500})}
    two = {h for _, h in generate_all_query_hash_pairs(Q, "flight_id", bucket_steps=[{"alt_max": 500}, {"alt_max": 2000}])}
    assert base < one < two
    assert len(two) == len(base) + 2


def test_negative_between_and_not_are_loosened():
    out = normalize_attribute_conditions(
        "SELECT * FROM t AS t1 WHERE t1.vrate BETWEEN -3.5 AND -1.5 AND NOT t1.alt_max > 650", {"vrate": 1, "alt_max": 500}
    )
    assert "BETWEEN -4 AND -1" in out
    assert "NOT t1.alt_max > 1000" in out  # NOT (x > 650) is x <= 650, loosened to x <= 1000


def test_lazy_lookup_keep_all_attributes_probes_fewer_variants():
    from unittest.mock import MagicMock

    from partitioncache.apply_cache import get_partition_keys_lazy
    from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

    def probe():
        handler = MagicMock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (None, 0)
        return handler

    q = "SELECT * FROM t AS t1, t AS t2 WHERE t1.pk = t2.pk AND t1.a = 1 AND t1.b = 2 AND t2.c = 3"
    loose, strict = probe(), probe()
    get_partition_keys_lazy(q, loose, "pk", min_component_size=1, bucket_steps=0)
    get_partition_keys_lazy(q, strict, "pk", min_component_size=1, bucket_steps=0, keep_all_attributes=True)
    n_loose = len(loose.get_intersected_lazy.call_args.args[0])
    n_strict = len(strict.get_intersected_lazy.call_args.args[0])
    assert 0 < n_strict < n_loose


def test_radius_of_st_dwithin_with_per_attribute_steps():
    q = "SELECT * FROM p AS a, p AS b WHERE ST_DWithin(a.g, b.g, 150) AND ABS(a.alt - b.alt) < 30"
    out = normalize_attribute_conditions(q, {"st_dwithin": 1000, "abs(": 50})
    assert "ST_DWITHIN(a.g, b.g, 1000)" in out and "< 50" in out
    assert "ST_DWITHIN(a.g, b.g, 150)" in normalize_attribute_conditions(q, {"abs(": 50})  # radius not matched: unchanged


def test_negative_lower_bound_rounds_down():
    """Rounding a negative lower bound toward zero would tighten it (vrate >= -2 drops vrate = -2.3)."""
    out = normalize_attribute_conditions("SELECT * FROM t AS t1 WHERE t1.vrate >= -2.5 AND t1.v > -0.5", {"vrate": 1, "v": 1})
    assert "t1.vrate >= -3" in out
    assert "t1.v > -1" in out


def test_float_steps_leave_plain_attributes_and_negative_bounds_alone():
    query = "SELECT * FROM t AS t1 WHERE t1.a <= 2.5 AND DIST(t1.x, t1.y) >= -2.5"
    assert normalize_attribute_conditions(query, {}) == query
    from partitioncache.query_processor import normalize_distance_conditions

    assert normalize_distance_conditions(query, 1.0) == "SELECT * FROM t AS t1 WHERE t1.a <= 2.5 AND DIST(t1.x, t1.y) >= -2.5"


def test_every_row_of_the_query_satisfies_the_bucketed_query():
    import random
    import sqlite3

    rng = random.Random(3)
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (pk INTEGER, a REAL, b REAL)")
    conn.executemany("INSERT INTO t VALUES (?, ?, ?)", [(i, rng.uniform(-10, 10), rng.uniform(-10, 10)) for i in range(300)])
    conditions = [
        "t1.a <= -2.5",
        "t1.a >= -2.5",
        "NOT t1.a < -2.5",
        "t1.a BETWEEN -3.5 AND -1.5 OR t1.b > 4.2",
        "NOT (t1.a > 1.5 AND -0.5 >= t1.b)",
        "t1.a NOT BETWEEN -1.5 AND 2.5",
        "ABS(t1.a - t1.b) < 3.3",
    ]
    for condition in conditions:
        query = f"SELECT t1.pk FROM t AS t1 WHERE {condition}"
        original = set(conn.execute(query))
        bucketed = normalize_attribute_conditions(query, {"t1.a": 2, "b": 0.5, "abs(": 1})
        assert bucketed != query, condition
        assert original <= set(conn.execute(bucketed)), (condition, bucketed)


def test_lookup_entry_points_pass_keep_all_attributes():
    from unittest.mock import MagicMock

    from partitioncache.apply_cache import apply_cache, apply_cache_lazy, get_partition_keys
    from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

    q = "SELECT * FROM t AS t1, t AS t2 WHERE t1.pk = t2.pk AND t1.a = 1 AND t1.b = 2 AND t2.c = 3"

    def probed(call, method_name, **kwargs):
        handler = MagicMock(spec=AbstractCacheHandler_Lazy)
        handler.get_intersected_lazy.return_value = (None, 0)
        handler.get_intersected.return_value = (None, 0)
        call(q, handler, "pk", min_component_size=1, bucket_steps=0, **kwargs)
        return len(getattr(handler, method_name).call_args.args[0])

    for call, method_name in ((apply_cache_lazy, "get_intersected_lazy"), (apply_cache, "get_intersected"), (get_partition_keys, "get_intersected")):
        assert 0 < probed(call, method_name, keep_all_attributes=True) < probed(call, method_name)


def test_cli_bucket_steps_accepts_numbers_dicts_and_levels():
    import argparse

    import pytest

    from partitioncache.cli.common_args import parse_bucket_steps

    assert parse_bucket_steps("0.5") == 0.5
    assert parse_bucket_steps('{"alt": 500, "vrate": 1}') == {"alt": 500.0, "vrate": 1.0}
    assert parse_bucket_steps('[1, {"alt": 2000}]') == [1.0, {"alt": 2000.0}]
    for bad in ("x", '{"a": "b"}', "[[1]]", "true"):
        with pytest.raises(argparse.ArgumentTypeError):
            parse_bucket_steps(bad)
    # The CLI's float conversion gives the same variants as the integer steps of the Python API
    assert generate_all_query_hash_pairs(Q, "flight_id", bucket_steps=parse_bucket_steps('{"alt_max": 500}')) == generate_all_query_hash_pairs(
        Q, "flight_id", bucket_steps={"alt_max": 500}
    )
