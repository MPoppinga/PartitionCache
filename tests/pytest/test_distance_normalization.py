"""
Unit tests for distance condition normalization
"""

import pytest
import sqlglot

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


class TestBucketingLoosensTheWholeCondition:
    """Bucketing must loosen the condition as a whole (no false negatives) and must never break the query."""

    @pytest.mark.parametrize(
        ("condition", "bucketed"),
        [
            ("DIST(a.x, a.y) < 2.5 OR a.type = 3", "DIST(a.x, a.y) < 3 OR a.type = 3"),
            ("a.v = 1 AND (DIST(a.x, a.y) < 2.5 OR a.type = 3)", "a.v = 1 AND (DIST(a.x, a.y) < 3 OR a.type = 3)"),
            ("NOT DIST(a.x, a.y) BETWEEN 2.5 AND 5.5", "NOT DIST(a.x, a.y) BETWEEN 3 AND 5"),
            ("NOT DIST(a.x, a.y) < 2.5", "NOT DIST(a.x, a.y) < 2"),
            ("NOT (DIST(a.x, a.y) >= 2.5 AND a.type = 3)", "NOT (DIST(a.x, a.y) >= 3 AND a.type = 3)"),
            ("2.5 > DIST(a.x, a.y)", "3 > DIST(a.x, a.y)"),
            ("DIST(a.x, a.y) BETWEEN 1.6 AND 3.6", "DIST(a.x, a.y) BETWEEN 1 AND 4"),
            # Not monotone in the comparison: left unchanged
            ("COALESCE(DIST(a.x, a.y) < 2.5, FALSE)", "COALESCE(DIST(a.x, a.y) < 2.5, FALSE)"),
            ("a.pk IN (SELECT b.pk FROM points AS b WHERE DIST(b.x, b.y) < 2.5)", "a.pk IN (SELECT b.pk FROM points AS b WHERE DIST(b.x, b.y) < 2.5)"),
        ],
    )
    def test_bucketed_condition(self, condition, bucketed):
        assert normalize_distance_conditions(f"SELECT a.pk FROM points AS a WHERE {condition}", bucket_steps=1) == (
            f"SELECT a.pk FROM points AS a WHERE {bucketed}"
        )

    @pytest.mark.parametrize(
        "condition",
        [
            "DIST(a.x, a.y) < 2.5 OR a.type = 3",
            "NOT DIST(a.x, a.y) BETWEEN 2.5 AND 5.5",
            "NOT DIST(a.x, a.y) <= 2.5",
            "NOT (DIST(a.x, a.y) > 4.2 OR a.type = 1)",
            "DIST(a.x, a.y) > 1.5 AND NOT DIST(a.x, a.y) >= 6.5",
            "3.3 <= DIST(a.x, a.y)",
        ],
    )
    def test_every_row_of_the_query_satisfies_the_bucketed_query(self, condition):
        import sqlite3

        conn = sqlite3.connect(":memory:")
        conn.create_function("DIST", 2, lambda x, y: abs(x - y))
        conn.execute("CREATE TABLE points (pk INTEGER, x REAL, y REAL, type INTEGER)")
        conn.executemany("INSERT INTO points VALUES (?, 0, ?, ?)", [(i, i * 0.05, i % 4) for i in range(200)])
        query = f"SELECT a.pk FROM points AS a WHERE {condition}"
        original = {row[0] for row in conn.execute(query)}
        bucketed = {row[0] for row in conn.execute(normalize_distance_conditions(query, bucket_steps=1))}
        assert original <= bucketed
        assert original

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT a.pk FROM points AS a, points AS b WHERE a.pk = b.pk AND a.type = 1 AND b.type = 2 AND DIST(a.x, b.x) NOT BETWEEN 2.5 AND 5.5",
            "SELECT a.pk FROM points AS a WHERE a.v = 1 AND (DIST(a.x, a.y) < 2.5 OR a.type = 3)",
        ],
    )
    def test_variant_generation_does_not_raise_and_keeps_or_branches(self, query):
        from partitioncache.query_processor import generate_all_query_hash_pairs

        variants = [v for v, _ in generate_all_query_hash_pairs(query, "pk", min_component_size=1, warn_no_partition_key=False, bucket_steps=1.0)]
        assert variants
        # Both distance conditions are disjunctions (NOT BETWEEN as < OR >); no variant may keep only one side
        for variant in variants:
            if "DIST(" in variant:
                assert " OR " in variant, variant


class TestBucketBoundsAreExact:
    """Float division and 6-digit formatting could move a bound past its value and tighten the condition."""

    @pytest.mark.parametrize(
        ("condition", "steps", "bucketed"),
        [
            ("DIST(a.x, a.y) >= 1234567.5", 1, "DIST(a.x, a.y) >= 1234567"),
            ("DIST(a.x, a.y) > 12.34567", 1e-05, "DIST(a.x, a.y) > 12.34567"),
            ("DIST(a.x, a.y) > 1.7", 0.1, "DIST(a.x, a.y) > 1.7"),
            ("DIST(a.x, a.y) < 1.71", 0.1, "DIST(a.x, a.y) < 1.8"),
            ("(DIST(a.x, a.y)) < 2.5", 1, "(DIST(a.x, a.y)) < 3"),
            ("DIST(a.x, a.y) < 1e3", 1, "DIST(a.x, a.y) < 1000"),
        ],
    )
    def test_bound(self, condition, steps, bucketed):
        query = f"SELECT a.pk FROM points AS a WHERE {condition}"
        assert normalize_distance_conditions(query, bucket_steps=steps) == f"SELECT a.pk FROM points AS a WHERE {bucketed}"

    def test_bounds_beyond_decimal_precision_stay_unchanged(self):
        query = "SELECT a.pk FROM points AS a WHERE DIST(a.x, a.y) < 1e30 AND DIST(a.x, a.z) < 2.5"
        assert normalize_distance_conditions(query, bucket_steps=1) == "SELECT a.pk FROM points AS a WHERE DIST(a.x, a.y) < 1e30 AND DIST(a.x, a.z) < 3"
        assert normalize_distance_conditions(query, bucket_steps=float("inf")) == query

    @pytest.mark.parametrize("steps", [0.1, 0.25, 1, 1e-05, 1000])
    def test_upper_bounds_never_drop_below_and_lower_bounds_never_rise_above_the_value(self, steps):
        from decimal import Decimal

        values = [Decimal(i) / 100 for i in range(0, 1000, 7)] + [Decimal("1234567.5"), Decimal("12.34567"), Decimal("98765432.123")]
        for value in values:
            upper = normalize_distance_conditions(f"SELECT a.pk FROM t AS a WHERE DIST(a.x, a.y) < {value}", bucket_steps=steps)
            lower = normalize_distance_conditions(f"SELECT a.pk FROM t AS a WHERE DIST(a.x, a.y) > {value}", bucket_steps=steps)
            assert Decimal(upper.rsplit("< ", 1)[1]) >= value, (value, upper)
            assert Decimal(lower.rsplit("> ", 1)[1]) <= value, (value, lower)


class TestRadiusBucketing:
    """The radius of ST_DWithin / ST_DFullyWithin is loosened like an upper bound of a distance comparison."""

    @pytest.mark.parametrize(
        ("condition", "bucketed"),
        [
            ("ST_DWithin(a.g, b.g, 512.3)", "ST_DWITHIN(a.g, b.g, 520)"),
            ("ST_DWithin(a.g, b.g, 500)", "ST_DWITHIN(a.g, b.g, 500)"),
            ("NOT ST_DWithin(a.g, b.g, 512.3)", "NOT ST_DWITHIN(a.g, b.g, 510)"),
            ("ST_DWithin(a.g, b.g, 512.3) OR a.t = 1", "ST_DWITHIN(a.g, b.g, 520) OR a.t = 1"),
            ("ST_DWithin(a.g, ST_MakePoint(1, 2), 99.5, TRUE)", "ST_DWITHIN(a.g, ST_MAKEPOINT(1, 2), 100, TRUE)"),
            ("ST_DFullyWithin(a.g, b.g, (2.5))", "ST_DFULLYWITHIN(a.g, b.g, 10)"),
            # Radius not a literal, not monotone in the radius, or inside a subquery: left unchanged
            ("ST_DWithin(a.g, b.g, a.r)", "ST_DWITHIN(a.g, b.g, a.r)"),
            ("ST_DWithin(a.g, b.g, -5.5)", "ST_DWITHIN(a.g, b.g, -5.5)"),
            ("COALESCE(ST_DWithin(a.g, b.g, 5.5), FALSE)", "COALESCE(ST_DWITHIN(a.g, b.g, 5.5), FALSE)"),
            ("a.pk IN (SELECT c.pk FROM p AS c WHERE ST_DWithin(c.g, a.g, 5.5))", "a.pk IN (SELECT c.pk FROM p AS c WHERE ST_DWITHIN(c.g, a.g, 5.5))"),
        ],
    )
    def test_bucketed_radius(self, condition, bucketed):
        query = f"SELECT a.pk FROM p AS a, p AS b WHERE {condition}"
        assert normalize_distance_conditions(query, bucket_steps=10) == f"SELECT a.pk FROM p AS a, p AS b WHERE {bucketed}"

    @pytest.mark.parametrize(
        "condition",
        ["ST_DWithin(a.x, b.x, 2.5)", "NOT ST_DWithin(a.x, b.x, 2.5)", "ST_DWithin(a.x, b.x, 1.5) OR NOT ST_DWithin(a.x, b.x, 6.5)"],
    )
    def test_every_row_of_the_query_satisfies_the_bucketed_query(self, condition):
        import sqlite3

        conn = sqlite3.connect(":memory:")
        conn.create_function("ST_DWITHIN", 3, lambda x, y, r: abs(x - y) <= r)
        conn.execute("CREATE TABLE points (pk INTEGER, x REAL)")
        conn.executemany("INSERT INTO points VALUES (?, ?)", [(i, i * 0.05) for i in range(200)])
        query = f"SELECT a.pk, b.pk FROM points AS a, points AS b WHERE a.pk = 0 AND {condition}"
        original = set(conn.execute(query))
        bucketed_query = normalize_distance_conditions(query, bucket_steps=1)
        assert bucketed_query != sqlglot.parse_one(query).sql()
        assert original <= set(conn.execute(bucketed_query))
        assert original

    def test_variants_include_the_bucketed_radius(self):
        from partitioncache.query_processor import generate_all_query_hash_pairs

        query = "SELECT a.pk FROM p AS a, p AS b WHERE a.pk = b.pk AND a.t = 1 AND ST_DWithin(a.g, b.g, 512.3)"
        variants = [v for v, _ in generate_all_query_hash_pairs(query, "pk", min_component_size=1, warn_no_partition_key=False, bucket_steps=10)]
        assert any("ST_DWITHIN(t2.g, t1.g, 520)" in v for v in variants)
        assert any("ST_DWITHIN(t2.g, t1.g, 512.3)" in v for v in variants)
