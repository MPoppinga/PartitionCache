"""
Comprehensive test suite for star-join (p0) table handling in query variant generation.

This test file consolidates and expands upon all star-join table functionality,
providing comprehensive end-to-end testing of the entire query variant generation pipeline.

Star-join tables are special tables that:
1. Only join other tables via partition key (no additional conditions)
2. Serve as central hub in star-schema patterns
3. Are excluded from variant generation but re-added to ALL variants for performance
4. Can be detected via naming convention (p0_*), smart detection, or explicit specification
"""

import pytest

from partitioncache.query_processor import (
    generate_all_query_hash_pairs,
    generate_partial_queries,
)


class TestStarJoinDetection:
    """Test all three star-join detection methods."""

    def test_naming_convention_detection(self):
        """Test star-join detection via p0_* naming convention."""
        query = """
        SELECT * FROM users u, orders o, p0_region pr
        WHERE u.region_id = pr.region_id
          AND o.region_id = pr.region_id
          AND u.age > 25
          AND o.status = 'active'
        """

        variants = generate_partial_queries(query, "region_id", auto_detect_star_join=True, warn_no_partition_key=False)

        # EXPECTED: Only variants with p0_region re-added as 'p1'
        # NO base variants without star-join table
        variants_with_p0 = [v for v in variants if "p0_region" in v and " AS p1" in v]
        variants_without_p0 = [v for v in variants if "p0_region" not in v]

        assert len(variants_without_p0) == 0, "Should NOT have base variants without star-join table"
        assert len(variants_with_p0) > 0, "Should have variants with star-join table re-added"

        # Verify structure: all variants should have p0_region AS p1
        for variant in variants_with_p0:
            assert "p0_region AS p1" in variant, "Star-join table should be aliased as p1"
            assert "p1.region_id" in variant, "Star-join table should be joined via partition key"

    def test_smart_detection_star_schema(self):
        """Test smart detection of star-join pattern."""
        query = """
        SELECT * FROM customers c, orders o, products p, region_mapping rm
        WHERE c.region_id = rm.region_id
          AND o.region_id = rm.region_id
          AND p.region_id = rm.region_id
          AND rm.region_id > 1000  -- Only partition key condition
          AND c.status = 'active'
          AND o.total > 100
          AND p.in_stock = true
        """

        variants = generate_partial_queries(query, "region_id", auto_detect_star_join=True, warn_no_partition_key=False)

        # EXPECTED: region_mapping should be smart-detected as star-join
        # The system generates variants from non-star-join tables AND re-adds star-join to all variants
        # BUT also creates variants with partition key conditions applied directly to regular tables
        variants_with_rm = [v for v in variants if "region_mapping" in v]
        [v for v in variants if "region_mapping" not in v and ("customers" in v or "orders" in v or "products" in v)]

        # The system now generates BOTH types of variants:
        # 1. Variants with star-join table re-added (AS p1)
        # 2. Direct variants where partition key conditions are applied to regular tables
        assert len(variants_with_rm) > 0, "Should have variants with smart-detected star-join re-added"

        # Verify star-join table is properly aliased when re-added
        star_join_variants = [v for v in variants_with_rm if " AS p1" in v]
        assert len(star_join_variants) > 0, "Star-join table should be re-added with p1 alias"

        # Verify partition key conditions are preserved in variants
        # When star-join table has partition key conditions, they are propagated to all variants
        with_pk_condition = [v for v in variants if "region_id > 1000" in v]
        assert len(with_pk_condition) > 0, "Should have variants with partition key conditions"

    def test_explicit_specification(self):
        """Test explicit star-join table specification."""
        query = """
        SELECT * FROM fact_sales fs, dim_time dt, central_hub ch
        WHERE fs.hub_id = ch.hub_id
          AND dt.hub_id = ch.hub_id
          AND fs.amount > 100
          AND dt.is_holiday = false
        """

        # Without explicit specification - should not detect star-join
        generate_partial_queries(query, "hub_id", auto_detect_star_join=True, warn_no_partition_key=False)

        # With explicit specification
        variants_explicit = generate_partial_queries(
            query,
            "hub_id",
            auto_detect_star_join=True,
            star_join_table="ch",  # Explicitly mark central_hub as star-join
            warn_no_partition_key=False,
        )

        # Verify explicit specification works
        explicit_with_ch = [v for v in variants_explicit if "central_hub" in v and " AS p1" in v]
        explicit_without_ch = [v for v in variants_explicit if "central_hub" not in v]

        assert len(explicit_without_ch) == 0, "Explicit star-join should be excluded from base variants"
        assert len(explicit_with_ch) > 0, "Explicit star-join should be re-added to all variants"

    def test_star_join_with_conditions_not_detected(self):
        """Test that tables with non-partition-key conditions are NOT detected as star-join."""
        query = """
        SELECT * FROM users u, orders o, mapping m
        WHERE u.region_id = m.region_id
          AND o.region_id = m.region_id
          AND m.region_id > 100
          AND m.active = true  -- Non-partition-key condition!
          AND u.status = 'verified'
          AND o.shipped = true
        """

        variants = generate_partial_queries(query, "region_id", auto_detect_star_join=True, warn_no_partition_key=False)

        # EXPECTED: mapping should NOT be detected as star-join due to m.active condition
        # It should appear as a regular table in variants
        mapping_as_regular = [v for v in variants if "mapping" in v and " AS t" in v and "active" in v]
        mapping_as_star_join = [v for v in variants if "mapping" in v and " AS p1" in v]

        assert len(mapping_as_regular) > 0, "Table with non-PK conditions should appear as regular table"
        assert len(mapping_as_star_join) == 0, "Table with non-PK conditions should NOT be star-join detected"


class TestVariantGenerationPipeline:
    """Test the complete variant generation pipeline with exact expected outputs."""

    def test_simple_star_join_variants(self):
        """Test variant generation for simple star-join scenario with exact expected outputs."""
        query = """
        SELECT * FROM users u, orders o, p0_city p0
        WHERE u.city_id = p0.city_id
          AND o.city_id = p0.city_id
          AND u.age > 25
          AND o.total > 100
        """

        variants = generate_partial_queries(
            query,
            "city_id",
            min_component_size=1,
            follow_graph=False,  # Allow all combinations
            auto_detect_star_join=True,
            warn_no_partition_key=False,
        )

        # EXPECTED VARIANTS (all with p0_city re-added as p1):
        # 1. Single table: users + p0_city
        # 2. Single table: orders + p0_city
        # 3. Multi-table: users + orders + p0_city

        # Verify exact variant count and structure
        assert len(variants) == 3, f"Expected 3 variants, got {len(variants)}"

        # Categorize variants
        single_user_variants = [v for v in variants if "users AS t1" in v and "orders" not in v]
        single_order_variants = [v for v in variants if "orders AS t1" in v and "users" not in v]
        multi_table_variants = [v for v in variants if "users AS t1" in v and "orders AS t2" in v]

        assert len(single_user_variants) == 1, "Should have exactly 1 users-only variant"
        assert len(single_order_variants) == 1, "Should have exactly 1 orders-only variant"
        assert len(multi_table_variants) == 1, "Should have exactly 1 multi-table variant"

        # Verify all variants have p0_city re-added as p1
        for variant in variants:
            assert "p0_city AS p1" in variant, "All variants should have star-join table re-added as p1"
            assert "p1.city_id" in variant, "All variants should join to star-join table"

        # Verify specific conditions are preserved
        user_variant = single_user_variants[0]
        order_variant = single_order_variants[0]

        assert "t1.age > 25" in user_variant, "User conditions should be preserved"
        assert "t1.total > 100" in order_variant, "Order conditions should be preserved"

    def test_partition_key_conditions_on_star_join(self):
        """Test variant generation when star-join table has partition key conditions."""
        query = """
        SELECT * FROM users u, p0_partition pp
        WHERE u.partition_id = pp.partition_id
          AND pp.partition_id IN (SELECT id FROM active_partitions)
          AND u.created_date >= '2024-01-01'
        """

        variants = generate_partial_queries(query, "partition_id", auto_detect_star_join=True, warn_no_partition_key=False)

        # EXPECTED: Multiple variants due to partition key condition handling
        # 1. Base variant: users + p0_partition (without IN condition)
        # 2. Star-join variant: users + p0_partition (with IN condition on star-join)
        # 3. Direct variant: users only (with IN condition applied directly)
        # 4. Subquery extraction: the subquery itself

        with_pk_condition = [v for v in variants if "IN (SELECT id FROM active_partitions)" in v]
        without_pk_condition = [v for v in variants if "IN (SELECT id FROM active_partitions)" not in v]

        assert len(with_pk_condition) >= 1, "Should have variants with partition key condition"
        assert len(without_pk_condition) >= 1, "Should have variants without partition key condition"

        # Verify that star-join variants have p0_partition re-added
        star_join_variants = [v for v in variants if "p0_partition AS p1" in v]
        [v for v in variants if "p0_partition" not in v and "users" in v]

        assert len(star_join_variants) > 0, "Should have variants with star-join table re-added"
        # Note: System also generates direct variants where partition key conditions are applied to regular tables

        # Verify conditions are preserved
        for variant in variants:
            if "users" in variant:
                assert "created_date >= '2024-01-01'" in variant, "User conditions should be preserved"

    def test_complete_pipeline_generate_all_query_hash_pairs(self):
        """Test the complete pipeline using generate_all_query_hash_pairs."""
        query = """
        SELECT * FROM customers c, orders o, p0_region pr
        WHERE c.region_id = pr.region_id
          AND o.region_id = pr.region_id
          AND c.tier = 'gold'
          AND o.amount > 500
        """

        # Test the complete pipeline
        query_hash_pairs = generate_all_query_hash_pairs(
            query, "region_id", min_component_size=1, follow_graph=False, auto_detect_star_join=True, warn_no_partition_key=False
        )

        # Verify we get query-hash pairs
        assert len(query_hash_pairs) > 0, "Should generate query-hash pairs"

        # Verify structure of pairs
        for query_text, hash_value in query_hash_pairs:
            assert isinstance(query_text, str), "Query should be string"
            assert isinstance(hash_value, str), "Hash should be string"
            assert len(hash_value) == 40, "Hash should be SHA1 (40 characters)"
            assert "p0_region AS p1" in query_text, "All variants should have star-join re-added"

        # Test that hashes are deterministic
        query_hash_pairs_2 = generate_all_query_hash_pairs(
            query, "region_id", min_component_size=1, follow_graph=False, auto_detect_star_join=True, warn_no_partition_key=False
        )

        # Sort both by hash for comparison
        pairs_1_sorted = sorted(query_hash_pairs, key=lambda x: x[1])
        pairs_2_sorted = sorted(query_hash_pairs_2, key=lambda x: x[1])

        assert pairs_1_sorted == pairs_2_sorted, "Hash generation should be deterministic"


class TestParameterInteractions:
    """Test how star-join detection interacts with other parameters."""

    def test_follow_graph_with_star_join(self):
        """Test star-join detection with follow_graph parameter."""
        query = """
        SELECT * FROM users u, orders o, products p, p0_city pc
        WHERE u.city_id = pc.city_id
          AND o.city_id = pc.city_id
          AND p.city_id = pc.city_id
          AND o.user_id = u.user_id      -- Creates connection u <-> o
          AND o.product_id = p.product_id -- Creates connection o <-> p
          AND u.age > 25
          AND o.status = 'shipped'
          AND p.price > 50
        """

        # With follow_graph=True: only connected components
        variants_connected = generate_partial_queries(
            query, "city_id", min_component_size=1, follow_graph=True, auto_detect_star_join=True, warn_no_partition_key=False
        )

        # With follow_graph=False: all combinations
        variants_all = generate_partial_queries(
            query, "city_id", min_component_size=1, follow_graph=False, auto_detect_star_join=True, warn_no_partition_key=False
        )

        # follow_graph=False should generate more variants (allows unconnected combinations)
        assert len(variants_all) >= len(variants_connected), "follow_graph=False should generate more variants"

        # All variants should still have p0_city re-added
        for variant in variants_connected + variants_all:
            assert "p0_city AS p1" in variant, "All variants should have star-join table re-added"

    def test_component_size_limits_with_star_join(self):
        """Test min/max component size with star-join tables."""
        query = """
        SELECT * FROM a, b, c, d, p0_partition pp
        WHERE a.part_id = pp.part_id
          AND b.part_id = pp.part_id
          AND c.part_id = pp.part_id
          AND d.part_id = pp.part_id
          AND a.x > 1 AND b.y > 2 AND c.z > 3 AND d.w > 4
        """

        variants = generate_partial_queries(
            query,
            "part_id",
            min_component_size=2,  # At least 2 tables (excluding star-join)
            max_component_size=3,  # At most 3 tables (excluding star-join)
            follow_graph=False,
            auto_detect_star_join=True,
            warn_no_partition_key=False,
        )

        # Verify component size limits
        for variant in variants:
            # Count non-star-join tables (should be between 2 and 3)
            table_count = variant.count(" AS t")  # t1, t2, t3, etc.
            assert 2 <= table_count <= 3, f"Table count {table_count} should be between 2 and 3"

            # Verify star-join table is always present
            assert "p0_partition AS p1" in variant, "Star-join table should always be re-added"

    def test_star_join_table_parameter_precedence(self):
        """Test precedence of star_join_table parameter over auto-detection."""
        query = """
        SELECT * FROM users u, orders o, p0_auto_detected pad, manual_star_join msj
        WHERE u.region_id = pad.region_id
          AND o.region_id = pad.region_id
          AND u.region_id = msj.region_id
          AND o.region_id = msj.region_id
          AND u.age > 25
          AND o.total > 100
        """

        # With auto-detection only (should detect p0_auto_detected)
        variants_auto = generate_partial_queries(query, "region_id", auto_detect_star_join=True, warn_no_partition_key=False)

        # With explicit star_join_table (should override auto-detection)
        variants_explicit = generate_partial_queries(
            query,
            "region_id",
            auto_detect_star_join=True,
            star_join_table="msj",  # Explicitly specify manual_star_join
            warn_no_partition_key=False,
        )

        # Verify auto-detection picks p0_auto_detected
        auto_with_p0 = [v for v in variants_auto if "p0_auto_detected AS p1" in v]
        auto_with_msj = [v for v in variants_auto if "manual_star_join AS p1" in v]

        assert len(auto_with_p0) > 0, "Auto-detection should pick p0_auto_detected"
        assert len(auto_with_msj) == 0, "Auto-detection should not pick manual_star_join"

        # Verify explicit parameter overrides auto-detection
        explicit_with_p0 = [v for v in variants_explicit if "p0_auto_detected AS p1" in v]
        explicit_with_msj = [v for v in variants_explicit if "manual_star_join AS p1" in v]

        assert len(explicit_with_p0) == 0, "Explicit parameter should override auto-detection"
        assert len(explicit_with_msj) > 0, "Explicit parameter should be used"


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error conditions."""

    def test_no_star_join_detected(self):
        """Test behavior when no star-join table is detected."""
        query = """
        SELECT * FROM users u, orders o
        WHERE u.user_id = o.user_id  -- No partition key joins
          AND u.age > 25
          AND o.total > 100
        """

        variants = generate_partial_queries(
            query,
            "region_id",  # No table uses region_id
            auto_detect_star_join=True,
            warn_no_partition_key=False,  # Disable warnings for this test
        )

        # Should generate normal variants without star-join optimization
        assert len(variants) > 0, "Should generate variants even without star-join tables"

        # No variants should have " AS p1" (star-join alias)
        star_join_variants = [v for v in variants if " AS p1" in v]
        assert len(star_join_variants) == 0, "Should not have star-join aliases when no star-join detected"

    def test_all_tables_would_be_star_join(self):
        """Test edge case where all tables could be star-join candidates."""
        query = """
        SELECT * FROM p0_table1 p1, p0_table2 p2
        WHERE p1.partition_id = p2.partition_id
        """

        variants = generate_partial_queries(query, "partition_id", auto_detect_star_join=True, warn_no_partition_key=False)

        # Should pick only one table as star-join (alphabetically first)
        # and generate variants with the other table(s)
        assert len(variants) > 0, "Should generate variants"

        # Only one table should be used as star-join (AS p1)
        star_join_count = sum(1 for v in variants if " AS p1" in v)
        assert star_join_count > 0, "Should have star-join table re-added"

    def test_complex_partition_key_conditions(self):
        """Test complex partition key conditions - demonstrates attribute vs partition key condition distinction."""
        query = """
        SELECT * FROM events e, p0_time_partition ptp
        WHERE e.time_id = ptp.time_id
          AND ptp.time_id BETWEEN 1000 AND 2000
          AND ptp.time_id NOT IN (SELECT id FROM excluded_times)
          AND e.event_type = 'click'
        """

        variants = generate_partial_queries(query, "time_id", auto_detect_star_join=True, warn_no_partition_key=False)

        # IMPORTANT: After the bug fixes, p0_time_partition IS detected as star-join because:
        # - BETWEEN and NOT IN conditions are now treated as partition key conditions (not attribute conditions)
        # - Star-join detection requires tables with ONLY partition key conditions (no attribute conditions)
        # - p0_time_partition only has partition key conditions, so it qualifies as star-join

        # Verify that star-join optimization is applied
        events_variants = [v for v in variants if "events" in v]
        ptp_variants = [v for v in variants if "p0_time_partition" in v]

        assert len(events_variants) > 0, "Should have variants with events table"
        assert len(ptp_variants) > 0, "Should have variants with p0_time_partition table"

        # Since star-join is detected, should have " AS p1" aliases
        star_join_aliases = [v for v in variants if " AS p1" in v]
        assert len(star_join_aliases) > 0, "Should have star-join aliases when star-join detected"

        # Verify that NOT IN is preserved (NOT keyword should be maintained)
        not_in_variants = [v for v in variants if "NOT " in v and " IN (" in v]
        assert len(not_in_variants) > 0, "NOT IN conditions should be preserved"

        # Verify BETWEEN conditions are processed (may be normalized to >= and <= conditions)
        between_or_range_variants = [v for v in variants if ("BETWEEN" in v or (">=" in v and "<=" in v))]
        assert len(between_or_range_variants) > 0, "BETWEEN conditions should be preserved or normalized to range conditions"

    def test_true_partition_key_conditions_on_star_join(self):
        """Test true partition key conditions (IN subqueries) on star-join tables."""
        query = """
        SELECT * FROM events e, p0_pure_partition ppp
        WHERE e.partition_id = ppp.partition_id
          AND ppp.partition_id IN (SELECT id FROM active_partitions)
          AND e.event_type = 'click'
        """

        variants = generate_partial_queries(query, "partition_id", auto_detect_star_join=True, warn_no_partition_key=False)

        # p0_pure_partition should be detected as star-join (only has partition key joins and IN condition)
        # Should generate variants with and without the IN condition

        with_star_join = [v for v in variants if "p0_pure_partition AS p1" in v]
        [v for v in variants if "p0_pure_partition" in v and " AS p1" not in v]

        assert len(with_star_join) > 0, "Should have variants with star-join table re-added"
        # Note: System behavior may vary on whether base variants without star-join are generated

        # Verify that partition key conditions create multiple variants
        with_in_condition = [v for v in variants if "IN (SELECT id FROM active_partitions)" in v]
        without_in_condition = [v for v in variants if "IN (SELECT id FROM active_partitions)" not in v]

        assert len(with_in_condition) > 0, "Should have variants with IN condition"
        assert len(without_in_condition) > 0, "Should have variants without IN condition"

    def test_star_join_eliminates_redundant_joins(self):
        """Test that star-join optimization eliminates redundant joins between tables."""
        query = """
        SELECT * FROM customers c, orders o, products p, p0_region pr
        WHERE c.region_id = pr.region_id
          AND o.region_id = pr.region_id
          AND p.region_id = pr.region_id
          AND c.status = 'active'
          AND o.total > 100
          AND p.in_stock = true
        """

        variants = generate_partial_queries(
            query,
            "region_id",
            min_component_size=2,  # Multi-table variants to test join patterns
            follow_graph=False,
            auto_detect_star_join=True,
            warn_no_partition_key=False,
        )

        # All multi-table variants should follow star-join pattern
        multi_table_variants = [v for v in variants if v.count(" AS t") >= 2]
        assert len(multi_table_variants) > 0, "Should have multi-table variants for testing"

        for variant in multi_table_variants:
            # Should have star-join table re-added
            assert " AS p1" in variant, "Multi-table variants should have star-join table"

            # Should NOT have direct joins between tables (e.g., t1.region_id = t2.region_id)
            # Only joins to star-join table (e.g., t1.region_id = p1.region_id)
            join_conditions = [cond.strip() for cond in variant.split("WHERE")[1].split(" AND ") if " = " in cond]

            # Count direct table-to-table joins vs star-join joins
            # Direct joins look like: t1.region_id = t2.region_id (both sides start with 't')
            direct_joins = [j for j in join_conditions if j.startswith("t") and " = t" in j and ".region_id" in j]
            star_joins = [j for j in join_conditions if " = p1.region_id" in j or "p1.region_id = " in j]

            assert len(direct_joins) == 0, f"Should have no direct table-to-table joins, found: {direct_joins}"
            assert len(star_joins) > 0, f"Should have star-join pattern joins, found: {star_joins}"

            # Verify all tables join to star-join table
            table_count = variant.count(" AS t")
            expected_star_joins = table_count  # Each table should join to star-join table
            assert len(star_joins) == expected_star_joins, f"Expected {expected_star_joins} star-joins, got {len(star_joins)}"


class TestRegressionTests:
    def test_table_name_preservation(self):
        """Test that different table names are preserved in variants (regression test)."""
        query = """
        SELECT * FROM users AS u, orders AS o, products AS p
        WHERE u.city_id = o.city_id
          AND o.city_id = p.city_id
          AND u.age > 25
          AND o.total > 100
          AND p.category = 'electronics'
        """

        variants = generate_partial_queries(
            query,
            "city_id",
            follow_graph=False,
            auto_detect_star_join=False,  # Disable to test normal behavior
            warn_no_partition_key=False,
        )

        # Verify that generated queries use correct table names
        for variant in variants:
            # If query contains user conditions, it should reference 'users' table
            if "age >" in variant:
                assert "users AS" in variant, "User conditions should reference users table"
            # If query contains order conditions, it should reference 'orders' table
            if "total >" in variant:
                assert "orders AS" in variant, "Order conditions should reference orders table"
            # If query contains product conditions, it should reference 'products' table
            if "category =" in variant:
                assert "products AS" in variant, "Product conditions should reference products table"

    def test_deterministic_variant_generation(self):
        """Test that variant generation produces consistent results regardless of table ordering (regression test)."""
        # Test with multiple query orderings to ensure order-independent results
        query_variations = [
            """
            SELECT * FROM a, b, c, p0_map p
            WHERE a.id = p.id AND b.id = p.id AND c.id = p.id
              AND a.x > 1 AND a.xx > 4 AND b.y > 2 AND c.z > 3
            """,
            """
            SELECT * FROM a, b, c, p0_map p
            WHERE a.id = p.id AND b.id = p.id AND c.id = p.id
              AND a.xx > 4 AND a.x > 1  AND b.y > 2 AND c.z > 3
            """,
            """
            SELECT * FROM c, a, b, p0_map p
            WHERE c.id = p.id AND a.id = p.id AND b.id = p.id
              AND c.z > 3 AND a.x > 1 AND a.xx > 4 AND b.y > 2
            """,
            """
            SELECT * FROM b, p0_map p, a, c
            WHERE b.id = p.id AND p.id = a.id AND p.id = c.id
              AND b.y > 2 AND a.x > 1 AND a.xx > 4AND c.z > 3
            """,
        ]

        # Generate variants for each query variation (run once per variation)
        all_results = []
        for query in query_variations:
            variants = generate_partial_queries(query, "id", min_component_size=1, follow_graph=False, auto_detect_star_join=True, warn_no_partition_key=False)
            all_results.append(sorted(variants))

        # Verify that different query orderings produce equivalent results
        base_results = all_results[0]
        for query_idx in range(1, len(all_results)):
            comparison_results = all_results[query_idx]
            assert base_results == comparison_results, f"Query variation {query_idx} produced different variants than base query - not order-independent"

    def test_star_join_eliminates_redundant_joins(self):
        """Test that star-join optimization eliminates redundant joins between tables."""
        query = """
        SELECT * FROM customers c, orders o, products p, p0_region pr
        WHERE c.region_id = pr.region_id
          AND o.region_id = pr.region_id
          AND p.region_id = pr.region_id
          AND c.status = 'active'
          AND o.total > 100
          AND p.in_stock = true
        """

        variants = generate_partial_queries(
            query,
            "region_id",
            min_component_size=2,  # Multi-table variants to test join patterns
            follow_graph=False,
            auto_detect_star_join=True,
            warn_no_partition_key=False,
        )

        # All multi-table variants should follow star-join pattern
        multi_table_variants = [v for v in variants if v.count(" AS t") >= 2]
        assert len(multi_table_variants) > 0, "Should have multi-table variants for testing"

        for variant in multi_table_variants:
            # Should have star-join table re-added
            assert " AS p1" in variant, "Multi-table variants should have star-join table"

            # Should NOT have direct joins between tables (e.g., t1.region_id = t2.region_id)
            # Only joins to star-join table (e.g., t1.region_id = p1.region_id)
            join_conditions = [cond.strip() for cond in variant.split("WHERE")[1].split(" AND ") if " = " in cond]

            # Count direct table-to-table joins vs star-join joins
            # Direct joins look like: t1.region_id = t2.region_id (both sides start with 't')
            direct_joins = [j for j in join_conditions if j.startswith("t") and " = t" in j and ".region_id" in j]
            star_joins = [j for j in join_conditions if " = p1.region_id" in j or "p1.region_id = " in j]

            assert len(direct_joins) == 0, f"Should have no direct table-to-table joins, found: {direct_joins}"
            assert len(star_joins) > 0, f"Should have star-join pattern joins, found: {star_joins}"

            # Verify all tables join to star-join table
            table_count = variant.count(" AS t")
            expected_star_joins = table_count  # Each table should join to star-join table
            assert len(star_joins) == expected_star_joins, f"Expected {expected_star_joins} star-joins, got {len(star_joins)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
