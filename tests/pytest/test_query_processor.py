"""
Consolidated query processor tests covering all core functionality.
This replaces multiple fragmented test files with comprehensive coverage.
"""

import pytest

from partitioncache.query_processor import (
    clean_query,
    extract_conjunctive_conditions,
    generate_all_query_hash_pairs,
    is_distance_function,
    normalize_distance_conditions,
)


class TestBasicFunctionality:
    """Test basic query processor functions."""

    def test_normalize_distance_conditions_between(self):
        """Test BETWEEN clause normalization."""
        # Test case: Lower bound and upper bound
        query = "SELECT * FROM table WHERE distance BETWEEN 1.6 AND 3.6"
        expected_result = "SELECT * FROM table WHERE distance BETWEEN 1 AND 4"
        assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

        # Test case: Lower bound and upper bound but no dist function
        query = "SELECT * FROM table WHERE distance BETWEEN 1.6 AND 3.6"
        expected_result = "SELECT * FROM table WHERE distance BETWEEN 1.6 AND 3.6"
        assert normalize_distance_conditions(query, restrict_to_dist_functions=True) == expected_result

        # Test case: Lower bound and upper bound based on dist function
        query = "SELECT * FROM table AS a, table AS b WHERE DIST(a, b) BETWEEN 1.6 AND 3.6"
        expected_result = "SELECT * FROM table AS a, table AS b WHERE DIST(a, b) BETWEEN 1 AND 4"
        assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    def test_normalize_distance_conditions_bucket_steps_validation(self):
        """Test bucket_steps edge cases."""
        query = "SELECT * FROM table WHERE distance BETWEEN 1.6 AND 3.6"
        
        # Test case: bucket_steps = 0 should return original query (no normalization)
        result = normalize_distance_conditions(query, bucket_steps=0, restrict_to_dist_functions=False)
        assert result == query  # No changes when bucket_steps is 0
        
        # Test case: negative bucket_steps should also return original query
        result = normalize_distance_conditions(query, bucket_steps=-1.0, restrict_to_dist_functions=False)
        assert result == query  # No changes when bucket_steps is negative
        
        # Test case: valid bucket_steps should work
        result = normalize_distance_conditions(query, bucket_steps=0.5, restrict_to_dist_functions=False)
        expected = "SELECT * FROM table WHERE distance BETWEEN 1.5 AND 4"
        assert result == expected

    def test_extract_conjunctive_conditions(self):
        """Test extraction of AND conditions."""
        # Test case: Single condition
        query = "SELECT * FROM table WHERE attribute = 'value'"
        expected_result = ["attribute = 'value'"]
        assert extract_conjunctive_conditions(query) == expected_result

        # Test case: Multiple conditions
        query = "SELECT * FROM table WHERE attribute = 'value' AND attribute2 > 0.1"
        expected_result = ["attribute = 'value'", "attribute2 > 0.1"]
        assert extract_conjunctive_conditions(query) == expected_result

        # Test case: No conditions
        query = "SELECT * FROM table"
        expected_result = []
        assert extract_conjunctive_conditions(query) == expected_result

        # Test case: condition with OR
        query = "SELECT * FROM table WHERE (attribute = 'value' OR attribute2 > 0.1) AND attribute3 < 0.5"
        expected_result = ["(attribute = 'value' OR attribute2 > 0.1)", "attribute3 < 0.5"]
        assert extract_conjunctive_conditions(query) == expected_result

    def test_clean_query_semicolon_handling(self):
        """Test that clean_query properly handles trailing semicolons."""
        # Test single semicolon
        query_with_semicolon = "SELECT * FROM users WHERE id = 1;"
        query_without_semicolon = "SELECT * FROM users WHERE id = 1"

        cleaned_with = clean_query(query_with_semicolon)
        cleaned_without = clean_query(query_without_semicolon)

        # Both should produce the same result
        assert cleaned_with == cleaned_without
        assert ";" not in cleaned_with

        # Test multiple semicolons
        query_multiple_semicolons = "SELECT * FROM users WHERE id = 1;;;"
        cleaned_multiple = clean_query(query_multiple_semicolons)
        assert cleaned_multiple == cleaned_without
        assert ";" not in cleaned_multiple


class TestParenthesesHandling:
    """Test parentheses handling - the core bug fix."""

    def test_parentheses_around_where_clause_flattening(self):
        """Test the specific bug fix: parentheses around entire WHERE clause."""
        # This was the bug that caused the user's issue
        query_with_wrapper_parens = """
        SELECT cd.pdb_id FROM complex_data AS cd, data_points AS p1
        WHERE (p1.complex_data_id = cd.complex_data_id
               AND p1.element = 16
               AND p1.origin = 'MET')
        """

        query_without_wrapper_parens = """
        SELECT cd.pdb_id FROM complex_data AS cd, data_points AS p1
        WHERE p1.complex_data_id = cd.complex_data_id
               AND p1.element = 16
               AND p1.origin = 'MET'
        """

        cleaned_with = clean_query(query_with_wrapper_parens)
        cleaned_without = clean_query(query_without_wrapper_parens)

        # After cleaning, both should extract the same number of conditions
        conditions_with = extract_conjunctive_conditions(cleaned_with)
        conditions_without = extract_conjunctive_conditions(cleaned_without)

        assert len(conditions_with) == len(conditions_without) == 3
        # SQLGlot may normalize the order of equality comparisons
        assert ("p1.complex_data_id = cd.complex_data_id" in conditions_with or
                "cd.complex_data_id = p1.complex_data_id" in conditions_with)
        assert "p1.element = 16" in conditions_with
        assert "p1.origin = 'MET'" in conditions_with

    def test_necessary_parentheses_preserved(self):
        """Test that necessary parentheses inside expressions are preserved."""
        query = """
        SELECT p.id FROM data_points p
        WHERE (p.x > 0 AND (p.y = 1 OR p.z = 2)) AND p.active = true
        """

        cleaned = clean_query(query)
        conditions = extract_conjunctive_conditions(cleaned)

        # Should extract conditions, SQLGlot may flatten/reorganize the logical structure
        # Updated expectation: SQLGlot might extract more granular conditions
        assert len(conditions) >= 2
        assert any("(p.y = 1 OR p.z = 2)" in cond for cond in conditions)
        # Note: sqlglot normalizes 'true' to 'TRUE'
        assert any("p.active = TRUE" in cond for cond in conditions)

    def test_complex_subqueries_preserved(self):
        """Test that subqueries with their own parentheses are preserved."""
        query = """
        SELECT u.id FROM users u
        WHERE (u.department_id IN (SELECT d.id FROM departments d WHERE d.active = true)
               AND u.status = 'active')
        """

        cleaned = clean_query(query)
        conditions = extract_conjunctive_conditions(cleaned)

        # Should flatten outer parentheses but preserve subquery structure
        assert len(conditions) == 2
        assert any("IN (SELECT" in cond for cond in conditions)
        assert "u.status = 'active'" in conditions


class TestDistanceConditions:
    """Test distance condition parsing and normalization."""

    def test_complex_distance_expressions(self):
        """Test complex distance expressions like user's original query."""
        query = """
        SELECT * FROM data_points p1, data_points p2
        WHERE ABS(SQRT(POWER(p1.x - p2.x, 2) + POWER(p1.y - p2.y, 2) + POWER(p1.z - p2.z, 2)) - 1.8147338647856879) <= 0.1
        """

        cleaned = clean_query(query)
        normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)

        # Should successfully normalize the complex expression
        assert "<= 1" in normalized
        assert "<= 0.1" not in normalized

    def test_multiple_distance_conditions(self):
        """Test multiple distance conditions in the same query."""
        query = """
        SELECT * FROM points p1, points p2, points p3
        WHERE ABS(SQRT(POWER(p1.x - p2.x, 2))) <= 0.1
        AND ABS(SQRT(POWER(p2.x - p3.x, 2))) <= 0.2
        AND ABS(SQRT(POWER(p1.x - p3.x, 2))) < 0.15
        """

        cleaned = clean_query(query)
        normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)

        # All distance thresholds should be normalized
        assert normalized.count("<= 1") == 2  # Two <= conditions
        assert normalized.count("< 1") == 1   # One < condition
        assert "<= 0.1" not in normalized
        assert "<= 0.2" not in normalized
        assert "< 0.15" not in normalized

    def test_is_distance_function_detection(self):
        """Test the is_distance_function helper with various inputs."""
        # Should be detected as distance functions
        distance_functions = [
            "ABS(SQRT(POWER(p1.x - p2.x, 2)))",
            "DIST(p1, p2)",
            "DISTANCE(p1.x, p1.y, p2.x, p2.y)",
            "SQRT(p1.x + p2.y)",
        ]

        for func in distance_functions:
            assert is_distance_function(func), f"{func} should be detected as distance function"

        # Should NOT be detected as distance functions
        non_distance_functions = [
            "p.value",
            "p.name = 'test'",
            "COUNT(*)",
            "SUM(p.value)",  # No comma/plus in right context
        ]

        for func in non_distance_functions:
            assert not is_distance_function(func), f"{func} should NOT be detected as distance function"


class TestRobustness:
    """Test robustness and error handling."""

    def test_malformed_distance_conditions(self):
        """Test error handling for malformed distance conditions."""
        # Query with malformed distance condition (no numeric value)
        query = """
        SELECT * FROM points p WHERE ABS(SQRT(POWER(p.x, 2))) <= INVALID_VALUE
        """

        cleaned = clean_query(query)
        # Should not crash, should leave condition unchanged or log warning
        try:
            normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)
            # Should either succeed (leaving condition unchanged) or handle gracefully
            assert "INVALID_VALUE" in normalized  # Unchanged due to parsing error
        except Exception as e:
            pytest.fail(f"Should handle malformed conditions gracefully: {e}")

    def test_negative_distance_values(self):
        """Test that negative distance values are skipped."""
        query = """
        SELECT p.id FROM data_points p
        WHERE ABS(SQRT(POWER(p.x - 1.0, 2))) <= -0.1
        """

        cleaned = clean_query(query)
        normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)

        # Negative value should be left unchanged (SQLGlot may reorder comparison)
        assert ("<= -0.1" in normalized or "-0.1 >=" in normalized)

    def test_function_calls_with_commas(self):
        """Test that function calls with commas don't break condition extraction."""
        query = """
        SELECT p.id FROM points p
        WHERE (POWER(p.x - 1.0, 2) + POWER(p.y - 2.0, 2) <= 1.0
               AND p.category IN ('A', 'B', 'C'))
        """

        cleaned = clean_query(query)
        conditions = extract_conjunctive_conditions(cleaned)

        assert len(conditions) == 2
        assert any("POWER" in cond and "<= 1.0" in cond for cond in conditions)
        assert "p.category IN ('A', 'B', 'C')" in conditions

    def test_quotes_handling(self):
        """Test that basic quote types are handled correctly."""
        # Test basic quotes that should work
        basic_query = """
        SELECT p.id FROM products p
        WHERE (p.name = 'simple' AND p.description = "basic")
        """

        cleaned = clean_query(basic_query)
        conditions = extract_conjunctive_conditions(cleaned)
        assert len(conditions) == 2

        # Test mixed quotes
        mixed_query = 'SELECT p.id FROM products p WHERE p.name = "test" AND p.category = \'books\''
        cleaned_mixed = clean_query(mixed_query)
        conditions_mixed = extract_conjunctive_conditions(cleaned_mixed)
        assert len(conditions_mixed) == 2


class TestIntegrationScenarios:
    """Test end-to-end integration scenarios."""

    def test_user_original_query_end_to_end(self):
        """Test the user's original query that caused the issue works end-to-end."""
        # User's original problematic query (shortened for test)
        user_query = """
        SELECT cd.pdb_id, p1.id AS match_1, p2.id AS match_2
        FROM complex_data AS cd, data_points AS p1, data_points AS p2
        WHERE (p1.complex_data_id = cd.complex_data_id
               AND p2.complex_data_id = cd.complex_data_id
               AND p1.element = 16
               AND p1.origin = 'MET'
               AND p2.element = 6
               AND p2.origin = 'MET'
               AND ABS(SQRT(POWER(p1.x - p2.x, 2) + POWER(p1.y - p2.y, 2))) <= 0.1)
        """

        # Should not raise exceptions during processing
        try:
            # Test query cleaning
            cleaned = clean_query(user_query)
            assert len(cleaned) > 0

            # Test condition extraction
            conditions = extract_conjunctive_conditions(cleaned)
            assert len(conditions) >= 6  # Should extract individual conditions

            # Test distance normalization
            normalized = normalize_distance_conditions(cleaned, bucket_steps=1.0)
            assert "<= 1" in normalized  # Distance condition should be normalized

            # Test query hash generation (this was failing before)
            hash_pairs = generate_all_query_hash_pairs(
                user_query,
                "complex_data_id",
                min_component_size=1,
                follow_graph=True,
                keep_all_attributes=True,
            )
            assert len(hash_pairs) > 0  # Should generate query variants

        except Exception as e:
            pytest.fail(f"User's original query should process without errors: {e}")

    def test_cache_population_vs_lookup_parameters(self):
        """Test that parameter differences between cache population and lookup are handled."""
        test_query = """
        SELECT * FROM table1 t1, table2 t2
        WHERE t1.id = t2.id AND t1.value > 5 AND t2.status = 'active'
        """

        # Cache population parameters (from monitor job)
        generate_all_query_hash_pairs(
            test_query,
            "id",
            min_component_size=1,
            keep_all_attributes=True,  # Fixed attributes
            follow_graph=True,
        )

        # Cache lookup parameters (from API)
        cache_lookup_hashes = generate_all_query_hash_pairs(
            test_query,
            "id",
            min_component_size=2,
            keep_all_attributes=False,  # Allow attribute variations
            follow_graph=True,
        )

        # Should generate different numbers but cache_lookup should be subset of cache_population
        # when cache_population also has attribute variations
        cache_population_with_variations = generate_all_query_hash_pairs(
            test_query,
            "id",
            min_component_size=1,
            keep_all_attributes=False,  # Allow variations like lookup
            follow_graph=True,
        )

        lookup_hashes = {pair[1] for pair in cache_lookup_hashes}
        population_hashes_with_var = {pair[1] for pair in cache_population_with_variations}

        # All lookup hashes should be found in population when variations are enabled
        missing = lookup_hashes - population_hashes_with_var
        assert len(missing) == 0, f"Found {len(missing)} missing hashes when variations enabled"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
