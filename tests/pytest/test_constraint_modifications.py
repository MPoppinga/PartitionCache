"""
Test suite for constraint modification functionality in query processor.

Tests the new constraint modification features added to generate_all_query_hash_pairs:
- bucket_steps parameter for normalize_distance_conditions
- add_constraints for adding table-specific constraints
- remove_constraints_all for removing attributes from all variants
- remove_constraints_add for creating additional variants with removed constraints
"""


from partitioncache.query_processor import (
    _add_constraints_to_query,
    _apply_constraint_modifications,
    _remove_constraints_from_query,
    generate_all_query_hash_pairs,
    normalize_distance_conditions,
)


class TestBucketStepsParameter:
    """Test bucket_steps parameter functionality."""

    def test_bucket_steps_default_behavior(self):
        """Test that default bucket_steps behavior is preserved."""
        query = "SELECT * FROM points WHERE ABS(SQRT(POWER(x - 1.0, 2))) BETWEEN 1.6 AND 3.6"

        # Default behavior should be bucket_steps=1.0
        result_default = normalize_distance_conditions(query)
        result_explicit = normalize_distance_conditions(query, bucket_steps=1.0)

        assert result_default == result_explicit
        # BETWEEN gets normalized to proper boundaries
        assert "BETWEEN 1 AND 4" in result_default

    def test_bucket_steps_different_values(self):
        """Test bucket_steps with different values."""
        query = "SELECT * FROM points WHERE ABS(SQRT(POWER(x - 1.0, 2))) BETWEEN 1.6 AND 3.6"

        # Test bucket_steps=0.5
        result_half = normalize_distance_conditions(query, bucket_steps=0.5)
        assert "BETWEEN 1.5 AND 4" in result_half

        # Test bucket_steps=2.0
        result_two = normalize_distance_conditions(query, bucket_steps=2.0)
        assert "BETWEEN 0 AND 4" in result_two

    def test_bucket_steps_in_generate_hash_pairs(self):
        """Test bucket_steps parameter in generate_all_query_hash_pairs."""
        query = "SELECT * FROM points WHERE ABS(SQRT(POWER(x - 1.0, 2))) BETWEEN 1.6 AND 3.6"
        partition_key = "region_id"

        # Test with different bucket_steps values
        pairs_default = generate_all_query_hash_pairs(query, partition_key, bucket_steps=1.0, warn_no_partition_key=False)
        pairs_half = generate_all_query_hash_pairs(query, partition_key, bucket_steps=0.5, warn_no_partition_key=False)

        # Should generate different variants due to different normalization
        assert len(pairs_default) > 0
        assert len(pairs_half) > 0

        # Extract query texts
        queries_default = [pair[0] for pair in pairs_default]
        queries_half = [pair[0] for pair in pairs_half]

        # Should have different normalized versions
        has_bucket_1_normalized = any(">= 1" in q and "<= 4" in q for q in queries_default)
        has_bucket_half_normalized = any(">= 1.5" in q and "<= 4" in q for q in queries_half)

        assert has_bucket_1_normalized
        assert has_bucket_half_normalized


class TestAddConstraints:
    """Test add_constraints functionality."""

    def test_add_constraints_basic(self):
        """Test basic constraint addition to a single table."""
        query = "SELECT * FROM points WHERE x > 0"
        add_constraints = {"points": "size = 4"}

        result = _add_constraints_to_query(query, add_constraints)

        # Should contain both original and new constraints
        assert "x > 0" in result
        assert "size = 4" in result
        assert "AND" in result

    def test_add_constraints_multiple_tables(self):
        """Test adding constraints to multiple tables."""
        query = "SELECT * FROM points p, regions r WHERE p.region_id = r.id"
        add_constraints = {"points": "size = 4", "regions": "active = true"}

        result = _add_constraints_to_query(query, add_constraints)

        # Should contain all constraints
        assert "size = 4" in result
        assert "active = TRUE" in result

    def test_add_constraints_no_existing_where(self):
        """Test adding constraints to query without existing WHERE clause."""
        query = "SELECT * FROM points"
        add_constraints = {"points": "size = 4"}

        result = _add_constraints_to_query(query, add_constraints)

        # Should add WHERE clause
        assert "WHERE" in result
        assert "size = 4" in result

    def test_add_constraints_empty_dict(self):
        """Test that empty add_constraints dict returns original query."""
        query = "SELECT * FROM points WHERE x > 0"

        result = _add_constraints_to_query(query, {})

        assert result == query

    def test_add_constraints_none(self):
        """Test that None add_constraints returns original query."""
        query = "SELECT * FROM points WHERE x > 0"

        result = _add_constraints_to_query(query, None)

        assert result == query

    def test_add_constraints_invalid_sql(self):
        """Test behavior with invalid SQL constraints."""
        query = "SELECT * FROM points WHERE x > 0"
        add_constraints = {"points": "invalid constraint syntax"}

        # Should handle gracefully and return original query
        result = _add_constraints_to_query(query, add_constraints)

        # Depending on implementation, might return original or attempt to add
        assert isinstance(result, str)


class TestRemoveConstraints:
    """Test remove_constraints functionality."""

    def test_remove_constraints_basic(self):
        """Test basic constraint removal."""
        query = "SELECT * FROM points WHERE x > 0 AND size = 4 AND y < 10"
        attributes_to_remove = ["size"]

        result = _remove_constraints_from_query(query, attributes_to_remove)

        # Should remove size constraint but keep others
        assert "x > 0" in result
        assert "y < 10" in result
        assert "size = 4" not in result

    def test_remove_constraints_multiple_attributes(self):
        """Test removing multiple attributes."""
        query = "SELECT * FROM points WHERE x > 0 AND size = 4 AND color = 'red' AND y < 10"
        attributes_to_remove = ["size", "color"]

        result = _remove_constraints_from_query(query, attributes_to_remove)

        # Should remove both size and color constraints
        assert "x > 0" in result
        assert "y < 10" in result
        assert "size = 4" not in result
        assert "color = 'red'" not in result

    def test_remove_constraints_all_conditions(self):
        """Test removing all conditions from WHERE clause."""
        query = "SELECT * FROM points WHERE size = 4 AND color = 'red'"
        attributes_to_remove = ["size", "color"]

        result = _remove_constraints_from_query(query, attributes_to_remove)

        # Should remove WHERE clause entirely or make it minimal
        assert "size = 4" not in result
        assert "color = 'red'" not in result

    def test_remove_constraints_nonexistent_attribute(self):
        """Test removing non-existent attributes."""
        query = "SELECT * FROM points WHERE x > 0 AND y < 10"
        attributes_to_remove = ["nonexistent"]

        result = _remove_constraints_from_query(query, attributes_to_remove)

        # Should keep original constraints
        assert "x > 0" in result
        assert "y < 10" in result

    def test_remove_constraints_empty_list(self):
        """Test that empty remove list returns original query."""
        query = "SELECT * FROM points WHERE x > 0 AND size = 4"

        result = _remove_constraints_from_query(query, [])

        assert result == query

    def test_remove_constraints_none(self):
        """Test that None remove list returns original query."""
        query = "SELECT * FROM points WHERE x > 0 AND size = 4"

        result = _remove_constraints_from_query(query, None)

        assert result == query


class TestApplyConstraintModifications:
    """Test _apply_constraint_modifications function."""

    def test_apply_modifications_add_only(self):
        """Test applying only add_constraints modifications."""
        queries = {"SELECT * FROM points WHERE x > 0"}
        add_constraints = {"points": "size = 4"}

        result = _apply_constraint_modifications(queries, add_constraints=add_constraints)

        # Should return both original and modified queries
        assert len(result) == 2
        original_preserved = any("size = 4" not in q for q in result)
        constraint_added = any("size = 4" in q for q in result)
        assert original_preserved
        assert constraint_added

    def test_apply_modifications_remove_all_only(self):
        """Test applying only remove_constraints_all modifications."""
        queries = {"SELECT * FROM points WHERE x > 0 AND size = 4"}
        remove_constraints_all = ["size"]

        result = _apply_constraint_modifications(queries, remove_constraints_all=remove_constraints_all)

        # Should modify all queries to remove size constraint
        assert len(result) == 1
        for query in result:
            assert "size = 4" not in query
            assert "x > 0" in query

    def test_apply_modifications_remove_add_only(self):
        """Test applying only remove_constraints_add modifications."""
        queries = {"SELECT * FROM points WHERE x > 0 AND size = 4"}
        remove_constraints_add = ["size"]

        result = _apply_constraint_modifications(queries, remove_constraints_add=remove_constraints_add)

        # Should return both original and modified queries
        assert len(result) == 2
        original_preserved = any("size = 4" in q for q in result)
        constraint_removed = any("size = 4" not in q for q in result)
        assert original_preserved
        assert constraint_removed

    def test_apply_modifications_combined(self):
        """Test applying multiple modification types together."""
        queries = {"SELECT * FROM points WHERE x > 0 AND size = 4 AND color = 'red'"}
        add_constraints = {"points": "active = true"}
        remove_constraints_all = ["color"]
        remove_constraints_add = ["size"]

        result = _apply_constraint_modifications(
            queries, add_constraints=add_constraints, remove_constraints_all=remove_constraints_all, remove_constraints_add=remove_constraints_add
        )

        # Should have multiple variants
        assert len(result) > 1

        # All queries should have color removed (remove_constraints_all)
        for query in result:
            assert "color = 'red'" not in query

        # Should have variants with and without size (remove_constraints_add)
        has_size = any("size = 4" in q for q in result)
        no_size = any("size = 4" not in q for q in result)
        assert has_size
        assert no_size

        # Should have variants with added constraint
        has_active = any("active = TRUE" in q for q in result)
        assert has_active

    def test_apply_modifications_order_dependency(self):
        """Test that modifications are applied in correct order (remove_all -> remove_add -> add)."""
        queries = {"SELECT * FROM points WHERE x > 0 AND size = 4 AND color = 'red'"}
        add_constraints = {"points": "size = 8"}  # Add different size constraint
        remove_constraints_all = ["color"]
        remove_constraints_add = ["size"]

        result = _apply_constraint_modifications(
            queries, add_constraints=add_constraints, remove_constraints_all=remove_constraints_all, remove_constraints_add=remove_constraints_add
        )

        # Should have variant with size = 8 (replacement via remove then add)
        has_new_size = any("size = 8" in q for q in result)
        assert has_new_size


class TestGenerateHashPairsIntegration:
    """Test integration of constraint modifications with generate_all_query_hash_pairs."""

    def test_generate_with_bucket_steps(self):
        """Test hash generation with custom bucket_steps."""
        query = "SELECT * FROM points WHERE ABS(SQRT(POWER(x - 1.0, 2))) BETWEEN 1.6 AND 3.6"
        partition_key = "region_id"

        pairs = generate_all_query_hash_pairs(query, partition_key, bucket_steps=0.5, warn_no_partition_key=False)

        assert len(pairs) > 0
        # Should have normalized distance variants
        queries = [pair[0] for pair in pairs]
        has_normalized = any(">= 1.5" in q and "<= 4" in q for q in queries)
        assert has_normalized

    def test_generate_with_add_constraints(self):
        """Test hash generation with add_constraints."""
        query = "SELECT * FROM points WHERE x > 0"
        partition_key = "region_id"
        add_constraints = {"points": "size = 4"}

        pairs = generate_all_query_hash_pairs(query, partition_key, add_constraints=add_constraints, warn_no_partition_key=False)

        assert len(pairs) > 0
        # Should have variants with added constraint
        queries = [pair[0] for pair in pairs]
        has_constraint = any("size = 4" in q for q in queries)
        assert has_constraint

    def test_generate_with_remove_constraints(self):
        """Test hash generation with remove_constraints."""
        query = "SELECT * FROM points WHERE x > 0 AND size = 4"
        partition_key = "region_id"
        remove_constraints_add = ["size"]

        pairs = generate_all_query_hash_pairs(query, partition_key, remove_constraints_add=remove_constraints_add, warn_no_partition_key=False)

        assert len(pairs) > 0
        # Should have variants both with and without size constraint
        queries = [pair[0] for pair in pairs]
        has_size = any("size = 4" in q for q in queries)
        no_size = any("size = 4" not in q and "x > 0" in q for q in queries)
        assert has_size
        assert no_size

    def test_generate_with_all_modifications(self):
        """Test hash generation with all modification types."""
        query = "SELECT * FROM points WHERE x > 0 AND size = 4 AND color = 'red' AND ABS(SQRT(POWER(x - 1.0, 2))) BETWEEN 1.6 AND 3.6"
        partition_key = "region_id"

        pairs = generate_all_query_hash_pairs(
            query,
            partition_key,
            bucket_steps=0.5,
            add_constraints={"points": "active = true"},
            remove_constraints_all=["color"],
            remove_constraints_add=["size"],
            warn_no_partition_key=False,
        )

        assert len(pairs) > 0
        queries = [pair[0] for pair in pairs]

        # All should have color removed
        for q in queries:
            assert "color = 'red'" not in q

        # Should have distance normalization variants
        has_normalized = any(">= 1.5" in q and "<= 4" in q for q in queries)
        assert has_normalized

        # Should have variants with added constraint
        has_active = any("active = TRUE" in q for q in queries)
        assert has_active

        # Should have variants with and without size
        has_size = any("size = 4" in q for q in queries)
        no_size = any("size = 4" not in q for q in queries)
        assert has_size
        assert no_size

    def test_generate_maintains_existing_functionality(self):
        """Test that new parameters don't break existing functionality."""
        query = "SELECT * FROM points p1, points p2 WHERE p1.region_id = p2.region_id AND p1.x > 0"
        partition_key = "region_id"

        # Generate with old parameters only
        pairs_old = generate_all_query_hash_pairs(query, partition_key, min_component_size=2, follow_graph=True)

        # Generate with new parameters set to None/defaults
        pairs_new = generate_all_query_hash_pairs(
            query,
            partition_key,
            min_component_size=2,
            follow_graph=True,
            bucket_steps=1.0,
            add_constraints=None,
            remove_constraints_all=None,
            remove_constraints_add=None,
        )

        # Should produce same results
        assert len(pairs_old) == len(pairs_new)
        assert set(pairs_old) == set(pairs_new)


class TestRobustnessAndErrorHandling:
    """Test robustness and error handling for constraint modifications."""

    def test_malformed_queries(self):
        """Test behavior with malformed SQL queries."""
        malformed_query = "SELECT * FROM WHERE AND"

        # Should handle gracefully
        result_add = _add_constraints_to_query(malformed_query, {"table": "col = val"})
        result_remove = _remove_constraints_from_query(malformed_query, ["col"])

        assert isinstance(result_add, str)
        assert isinstance(result_remove, str)

    def test_complex_constraint_expressions(self):
        """Test with complex constraint expressions."""
        query = "SELECT * FROM points WHERE x > 0"
        complex_constraint = "size BETWEEN 1 AND 10 AND color IN ('red', 'blue')"

        result = _add_constraints_to_query(query, {"points": complex_constraint})

        assert "size BETWEEN 1 AND 10" in result
        assert "color IN ('red', 'blue')" in result

    def test_empty_query_set(self):
        """Test _apply_constraint_modifications with empty query set."""
        result = _apply_constraint_modifications(
            set(), add_constraints={"table": "col = val"}, remove_constraints_all=["attr"], remove_constraints_add=["attr2"]
        )

        assert result == set()

    def test_unicode_and_special_characters(self):
        """Test handling of unicode and special characters in constraints."""
        query = "SELECT * FROM points WHERE x > 0"
        unicode_constraint = "name = 'café'"

        result = _add_constraints_to_query(query, {"points": unicode_constraint})

        assert "café" in result
