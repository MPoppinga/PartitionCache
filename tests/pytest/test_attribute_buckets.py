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
    assert "NOT t1.alt_max > 500" in out  # under NOT a lower bound rounds down, so the condition still loosens
