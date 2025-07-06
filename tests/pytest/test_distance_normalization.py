"""
Unit tests for distance condition normalization
"""

import pytest

from partitioncache.query_processor import clean_query, extract_conjunctive_conditions, normalize_distance_conditions


class TestDistanceNormalization:
    """Test distance condition normalization with various bucket sizes and edge cases."""

    def test_parentheses_flattening(self):
        """Test that unnecessary parentheses around WHERE clause are flattened."""
        # Query with parentheses around entire WHERE clause
        query_with_parens = """
        SELECT cd.pdb_id FROM complex_data AS cd, data_points AS p1
        WHERE (p1.complex_data_id = cd.complex_data_id 
               AND p1.element = 16
               AND ABS(SQRT(POWER(p1.x - 2.0, 2))) <= 0.1)
        """

        # Query without parentheses around WHERE clause
        query_without_parens = """
        SELECT cd.pdb_id FROM complex_data AS cd, data_points AS p1
        WHERE p1.complex_data_id = cd.complex_data_id 
               AND p1.element = 16
               AND ABS(SQRT(POWER(p1.x - 2.0, 2))) <= 0.1
        """

        # Both should produce the same number of conditions after cleaning
        cleaned_with_parens = clean_query(query_with_parens)
        cleaned_without_parens = clean_query(query_without_parens)

        conditions_with_parens = extract_conjunctive_conditions(cleaned_with_parens)
        conditions_without_parens = extract_conjunctive_conditions(cleaned_without_parens)

        assert len(conditions_with_parens) == len(conditions_without_parens) == 3
        assert len(conditions_with_parens) > 1, "Parentheses should be flattened to extract individual conditions"

    def test_bucket_steps_default(self):
        """Test default bucket_steps=1.0 behavior."""
        query = """
        SELECT p.id FROM data_points p 
        WHERE ABS(SQRT(POWER(p.x - 1.0, 2))) <= 0.1
        """

        cleaned = clean_query(query)
        normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)

        # 0.1 should round up to 1.0 with bucket_steps=1.0
        assert "<= 1" in normalized
        assert "<= 0.1" not in normalized

    def test_bucket_boundary_values(self):
        """Test behavior at exact bucket boundaries."""
        test_cases = [
            # (original_value, bucket_steps, expected_normalized_value)
            (0.1, 1.0, 1.0),  # Round up
            (0.9, 1.0, 1.0),  # Round up
            (1.0, 1.0, 1.0),  # Exact boundary - keep same
            (1.1, 1.0, 2.0),  # Round up
            (2.0, 1.0, 2.0),  # Exact boundary - keep same
            (0.1, 0.5, 0.5),  # Round up with smaller bucket
            (0.5, 0.5, 0.5),  # Exact boundary - keep same
            (0.6, 0.5, 1.0),  # Round up with smaller bucket
        ]

        for original_value, bucket_steps, expected_value in test_cases:
            query = f"""
            SELECT p.id FROM data_points p 
            WHERE ABS(SQRT(POWER(p.x - 1.0, 2))) <= {original_value}
            """

            cleaned = clean_query(query)
            normalized = normalize_distance_conditions(cleaned, bucket_steps=bucket_steps)

            expected_str = f"<= {int(expected_value) if expected_value.is_integer() else expected_value:g}"
            assert expected_str in normalized, f"Expected {expected_str} for value {original_value} with bucket_steps {bucket_steps}, got: {normalized}"

    def test_less_than_vs_less_than_equal(self):
        """Test that < and <= operators are handled correctly."""
        test_cases = [
            ("<=", 0.1, 1.0, 1.0),
            ("<", 0.1, 1.0, 1.0),
            ("<=", 1.0, 1.0, 1.0),  # Exact boundary
            ("<", 1.0, 1.0, 1.0),  # Exact boundary
        ]

        for operator, original_value, bucket_steps, expected_value in test_cases:
            query = f"""
            SELECT p.id FROM data_points p 
            WHERE ABS(SQRT(POWER(p.x - 1.0, 2))) {operator} {original_value}
            """

            cleaned = clean_query(query)
            normalized = normalize_distance_conditions(cleaned, bucket_steps=bucket_steps)

            expected_str = f"{operator} {int(expected_value) if expected_value.is_integer() else expected_value:g}"
            assert expected_str in normalized, f"Expected {expected_str} for {operator} {original_value}"

    def test_multiple_distance_conditions(self):
        """Test query with multiple distance conditions like the user's complex query."""
        query = """
        SELECT cd.pdb_id FROM complex_data AS cd, data_points AS p1, data_points AS p2
        WHERE p1.complex_data_id = cd.complex_data_id 
        AND p2.complex_data_id = cd.complex_data_id 
        AND ABS(SQRT(POWER(p1.x - p2.x, 2) + POWER(p1.y - p2.y, 2))) <= 0.1
        AND ABS(SQRT(POWER(p1.x - p2.x, 2))) <= 0.2
        """

        cleaned = clean_query(query)
        normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)

        # Both 0.1 and 0.2 should become 1.0
        normalized_conditions = normalized.split(" AND ")
        distance_conditions = [c for c in normalized_conditions if "<=" in c and "ABS" in c]

        assert len(distance_conditions) == 2
        for condition in distance_conditions:
            assert "<= 1" in condition

    def test_robust_parsing_with_complex_expressions(self):
        """Test that complex expressions don't break the parsing."""
        # Complex query similar to user's original
        query = """
        SELECT cd.pdb_id FROM complex_data AS cd, data_points AS p1, data_points AS p2
        WHERE p1.complex_data_id = cd.complex_data_id 
        AND p2.complex_data_id = cd.complex_data_id 
        AND p1.element = 16 
        AND p2.element = 6
        AND ABS(SQRT(POWER(p1.x - p2.x, 2) + POWER(p1.y - p2.y, 2) + POWER(p1.z - p2.z, 2)) - 1.8147338647856879) <= 0.1
        """

        cleaned = clean_query(query)

        # Should not raise an exception
        try:
            normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)
            # Should successfully normalize the distance condition
            assert "<= 1" in normalized
            assert "<= 0.1" not in normalized
        except Exception as e:
            pytest.fail(f"Complex expression parsing failed: {e}")

    def test_no_distance_conditions(self):
        """Test that queries without distance conditions are unchanged."""
        query = """
        SELECT cd.pdb_id FROM complex_data AS cd, data_points AS p1
        WHERE p1.complex_data_id = cd.complex_data_id 
        AND p1.element = 16
        """

        cleaned = clean_query(query)
        normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)

        # Query should be essentially unchanged (except for whitespace normalization)
        assert "element = 16" in normalized
        # SQLGlot may normalize the order of columns in equality comparisons
        assert ("complex_data_id = cd.complex_data_id" in normalized or 
                "cd.complex_data_id = p1.complex_data_id" in normalized)

    def test_negative_values_skipped(self):
        """Test that negative distance values are skipped."""
        query = """
        SELECT p.id FROM data_points p 
        WHERE ABS(SQRT(POWER(p.x - 1.0, 2))) <= -0.1
        """

        cleaned = clean_query(query)
        normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)

        # Negative value should be left unchanged (sqlglot may reorder comparison)
        assert ("<= -0.1" in normalized or "-0.1 >=" in normalized)

    def test_non_distance_function_conditions_skipped(self):
        """Test that non-distance function conditions are skipped when restrict_to_dist_functions=True."""
        query = """
        SELECT p.id FROM data_points p 
        WHERE p.value <= 0.1
        AND ABS(SQRT(POWER(p.x - 1.0, 2))) <= 0.1
        """

        cleaned = clean_query(query)
        normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0, restrict_to_dist_functions=True)

        # Non-distance function condition should be unchanged
        assert "p.value <= 0.1" in normalized
        # Distance function condition should be normalized
        assert "<= 1" in normalized

    def test_original_user_query_parsing(self):
        """Test that the user's original problematic query can now be processed."""
        # User's original query that was causing parsing errors
        user_query = """
        SELECT cd.pdb_id,
               "p4".id AS match_4,
               "p1".id AS match_1,
               "p5".id AS match_5,
               "p2".id AS match_2,
               "p3".id AS match_3,
               "p6".id AS match_6
        FROM complex_data AS cd,
             data_points AS "p1",
             data_points AS "p2",
             data_points AS "p3",
             data_points AS "p4",
             data_points AS "p5",
             data_points AS "p6"
        WHERE ("p1".complex_data_id = "cd".complex_data_id
               AND "p2".complex_data_id = "cd".complex_data_id
               AND "p3".complex_data_id = "cd".complex_data_id
               AND "p4".complex_data_id = "cd".complex_data_id
               AND "p5".complex_data_id = "cd".complex_data_id
               AND "p6".complex_data_id = "cd".complex_data_id
               AND "p4".element = 16
               AND "p4".origin = 'MET'
               AND "p1".element = 16
               AND "p1".origin = 'MET'
               AND "p5".element = 6
               AND "p5".origin = 'MET'
               AND "p2".element = 6
               AND "p2".origin = 'MET'
               AND "p3".element = 6
               AND "p3".origin = 'MET'
               AND "p6".element = 8
               AND "p6".origin = 'MET'
               AND ABS(SQRT(POWER("p4".x - "p1".x, 2) + POWER("p4".y - "p1".y, 2) + POWER("p4".z - "p1".z, 2)) - 11.136691339890856) <= 0.1
               AND ABS(SQRT(POWER("p5".x - "p1".x, 2) + POWER("p5".y - "p1".y, 2) + POWER("p5".z - "p1".z, 2)) - 1.8147338647856879) <= 0.1)
        """

        try:
            cleaned = clean_query(user_query)

            # Should extract multiple individual conditions
            conditions = extract_conjunctive_conditions(cleaned)
            assert len(conditions) > 10  # Should have many individual conditions, not just 1

            # Should successfully normalize without throwing parsing errors
            normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)

            # All 0.1 distance thresholds should become 1.0
            distance_conditions = [c for c in conditions if "<= 0.1" in c]
            for condition in distance_conditions:
                assert "ABS" in condition  # Should be distance functions

            # Check that normalization worked
            assert "<= 1" in normalized
            # Should not contain the original 0.1 thresholds
            normalized_lines = normalized.split(" AND ")
            distance_normalized = [line for line in normalized_lines if "ABS" in line and "<=" in line]
            for line in distance_normalized:
                assert "<= 0.1" not in line, f"Found unnormalized 0.1 in: {line}"

        except Exception as e:
            pytest.fail(f"User's original query should not cause parsing errors: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
