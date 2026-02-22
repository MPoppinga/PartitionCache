"""
Unit tests for alias counting bug fix in extract_and_group_query_conditions.

Tests that conditions referencing a single table alias multiple times
are correctly classified using distinct alias counting, not total occurrence counting.

Key distinction (with regex remap):
- Single alias (any number of references) → attribute_conditions (regex remap handles all)
- Two distinct aliases → distance_conditions
- Three+ distinct aliases → other_functions
"""

from partitioncache.query_processor import extract_and_group_query_conditions


class TestAliasCountingDistinctAliases:
    """Test that alias counting uses distinct aliases, not total occurrences."""

    def test_single_alias_single_ref_is_attribute(self):
        """t.fare > 10 (one alias, one reference) → attribute_conditions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE t.fare_amount > 50"
        )
        (
            attribute_conditions, _, _, _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        assert "t" in attribute_conditions
        assert len(attribute_conditions["t"]) == 1
        assert attribute_conditions["t"][0] == "t.fare_amount > 50"

    def test_single_alias_multi_ref_is_attribute(self):
        """t.col1 * t.col2 > 3 (one alias, multiple refs) → attribute_conditions (regex remap handles it)."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE t.trip_distance * 1609.34 / NULLIF(t.total_amount, 0) > 3"
        )
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # Should NOT be in distance_conditions (only 1 distinct alias)
        assert len(distance_conditions) == 0
        # With regex remap, single-alias multi-ref goes to attribute_conditions
        assert "t" in attribute_conditions
        assert len(attribute_conditions["t"]) == 1
        assert "trip_distance" in attribute_conditions["t"][0]

    def test_single_alias_function_calls_multi_ref(self):
        """ST_Distance(t.pickup_geom, t.dropoff_geom) > 100 is single-alias, multi-ref → attribute_conditions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE ST_Distance(t.pickup_geom, t.dropoff_geom) > 100"
        )
        (
            attribute_conditions,
            distance_conditions,
            _other_functions,
            _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # NOT distance_conditions (only 1 distinct alias, not 2)
        assert len(distance_conditions) == 0
        # With regex remap, goes to attribute_conditions
        assert "t" in attribute_conditions
        assert any("st_distance" in c.lower() for c in attribute_conditions["t"])

    def test_two_alias_condition_classified_as_distance(self):
        """ST_DWithin(t.geom, p.geom, 500) should be distance_conditions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p "
            "WHERE ST_DWithin(t.geom, p.geom, 500)"
        )
        (
            _,
            distance_conditions,
            _, _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # Two distinct aliases => distance_conditions
        assert len(distance_conditions) > 0
        key = ("p", "t") if ("p", "t") in distance_conditions else ("t", "p")
        assert key in distance_conditions

    def test_zero_alias_literal_condition(self):
        """A condition without any alias (e.g., '1 = 1') should not crash."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE 1 = 1 AND t.fare > 10"
        )
        (
            attribute_conditions,
            _, _, _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # t.fare > 10 should be in attribute_conditions
        assert "t" in attribute_conditions
        assert any("fare" in c for c in attribute_conditions["t"])

    def test_complex_single_alias_not_distance(self):
        """Complex expression with many references to same alias is NOT distance_conditions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0) > 3"
        )
        (
            attribute_conditions,
            distance_conditions,
            _other_functions,
            _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # 4 occurrences of 't.' but only 1 distinct alias → NOT distance_conditions
        assert len(distance_conditions) == 0
        # With regex remap, single-alias conditions go to attribute_conditions
        assert "t" in attribute_conditions
        assert len(attribute_conditions["t"]) == 1

    def test_multi_alias_complex_condition(self):
        """Condition referencing 3 aliases should go to other_functions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p1, pois p2 "
            "WHERE SOME_FUNC(t.geom, p1.geom, p2.geom) < 100"
        )
        (
            _,
            distance_conditions,
            other_functions,
            _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # 3 distinct aliases => other_functions (not distance which requires exactly 2)
        assert len(other_functions) > 0

    def test_or_condition_single_alias_is_attribute(self):
        """OR condition with single alias goes to attribute_conditions (regex remap handles it)."""
        query = (
            "SELECT rp.search_space FROM random_point rp "
            "WHERE (rp.field2 = 2 OR rp.field2 = 3) AND rp.field1 = 1"
        )
        (
            attribute_conditions,
            _,
            _other_functions,
            _,
            or_conditions,
            _, _, _,
        ) = extract_and_group_query_conditions(query, "search_space")

        # Simple single-ref condition → attribute_conditions (with alias)
        assert "rp" in attribute_conditions
        assert any("field1" in c for c in attribute_conditions["rp"])
        # Single-alias OR condition also goes to attribute_conditions with regex remap
        assert any("field2" in c for c in attribute_conditions["rp"])


class TestRegexAliasRemap:
    """Test edge cases in regex-based alias remapping during query generation.

    Verifies that alias substitution uses word boundaries to avoid false matches
    (e.g., 't1' should not match inside 't10').
    """

    def test_alias_t1_vs_t10_word_boundary(self):
        """Alias 't1' must not be falsely remapped when 't10' is also present."""
        from partitioncache.query_processor import generate_partial_queries

        query = (
            "SELECT t1.id FROM table1 t1, table10 t10 "
            "WHERE t1.val > 5 AND t10.val < 100"
        )
        variants = generate_partial_queries(
            query, "id", min_component_size=1, warn_no_partition_key=False
        )

        # Every variant referencing table10 should keep 't10' intact
        for v in variants:
            if "table10" in v:
                assert "t10" in v or "AS t" in v, (
                    f"table10 alias was corrupted by t1 remap: {v}"
                )

    def test_alias_prefix_p_vs_p0(self):
        """Alias 'p' must not collide with 'p0' during remapping."""
        from partitioncache.query_processor import generate_partial_queries

        query = (
            "SELECT p.id FROM products p, p0_region p0 "
            "WHERE p.price > 10 AND p.region_id = p0.region_id"
        )
        variants = generate_partial_queries(
            query, "region_id", min_component_size=1, warn_no_partition_key=False
        )

        # Verify we get variants and none have corrupted aliases
        assert len(variants) > 0, "Should produce at least one variant"
        for v in variants:
            # Aliases should be cleanly remapped (t1, t2, p1, etc.)
            # but never produce broken tokens like 'p00' from 'p' → 'p0' collision
            assert "p00" not in v, f"Alias collision produced 'p00': {v}"
