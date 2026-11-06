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
