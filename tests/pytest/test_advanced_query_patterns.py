"""
Tests for advanced SQL patterns in the query processor:
- CTEs (WITH clauses)
- Window functions (OVER / PARTITION BY)
- Set operations (UNION, INTERSECT, EXCEPT)
- Subqueries in FROM clause

These tests document current behavior and verify graceful handling
of patterns that the fragment generator may not fully decompose.
"""

from partitioncache.query_processor import (
    clean_query,
    extract_conjunctive_conditions,
    generate_all_query_hash_pairs,
)


class TestCleanQueryAdvancedPatterns:
    """Test that clean_query handles advanced SQL patterns without crashing."""

    def test_clean_query_with_cte(self):
        """clean_query should parse and normalize a CTE query."""
        query = """
        WITH active_users AS (
            SELECT user_id, country_id FROM users WHERE active = true
        )
        SELECT * FROM active_users WHERE country_id = 1
        """
        result = clean_query(query)
        # Should not crash; result should be valid normalized SQL
        # Verify CTE structure is preserved with normalized boolean (true -> TRUE)
        assert result.startswith("WITH active_users AS"), "CTE WITH clause should be preserved at start"
        assert "SELECT user_id, country_id FROM users WHERE active = TRUE" in result, "CTE body should be normalized (true -> TRUE)"
        assert "SELECT * FROM active_users WHERE country_id = 1" in result, "Main SELECT should reference CTE alias"

    def test_clean_query_with_multiple_ctes(self):
        """clean_query should handle multiple CTEs."""
        query = """
        WITH
            cte1 AS (SELECT id, zipcode FROM users WHERE active = true),
            cte2 AS (SELECT id, region FROM locations WHERE type = 'city')
        SELECT cte1.zipcode FROM cte1
        JOIN cte2 ON cte1.id = cte2.id
        WHERE cte1.zipcode = 1001
        """
        result = clean_query(query)
        # Verify both CTEs are preserved with normalized booleans
        assert result.startswith("WITH cte1 AS"), "First CTE should be preserved"
        assert "cte2 AS" in result, "Second CTE should be preserved"
        assert "active = TRUE" in result, "Boolean normalization should apply inside CTEs"
        assert "type = 'city'" in result, "String literal conditions should be preserved"
        assert "JOIN cte2 ON cte1.id = cte2.id" in result, "JOIN clause should be preserved"
        assert "cte1.zipcode = 1001" in result, "WHERE condition should be preserved"

    def test_clean_query_with_window_function(self):
        """clean_query should handle window functions."""
        query = """
        SELECT user_id, zipcode,
               ROW_NUMBER() OVER (PARTITION BY zipcode ORDER BY created_at DESC) as rn
        FROM users
        WHERE zipcode = 1001
        """
        result = clean_query(query)
        # Verify window function is fully preserved (OVER, PARTITION BY, ORDER BY within window)
        assert "ROW_NUMBER() OVER" in result, "Window function ROW_NUMBER() OVER should be preserved"
        assert "PARTITION BY zipcode" in result, "Window PARTITION BY should be preserved"
        assert "ORDER BY created_at DESC" in result, "Window ORDER BY should be preserved"
        assert "WHERE zipcode = 1001" in result, "WHERE clause should be preserved"
        assert "AS rn" in result, "Window alias should be preserved"

    def test_clean_query_removes_order_by_but_keeps_window_order(self):
        """ORDER BY on main query should be removed, but window ORDER BY should be kept."""
        query = """
        SELECT user_id,
               RANK() OVER (ORDER BY score DESC) as rank
        FROM users
        WHERE zipcode = 1001
        ORDER BY user_id
        """
        result = clean_query(query)
        # Main ORDER BY should be removed; window ORDER BY should remain
        expected = "SELECT user_id, RANK() OVER (ORDER BY score DESC) AS rank FROM users WHERE zipcode = 1001"
        assert result == expected, f"Expected: {expected}, got: {result}"

    def test_clean_query_with_union(self):
        """clean_query should handle UNION queries."""
        query = """
        SELECT id, zipcode FROM users WHERE zipcode = 1001
        UNION
        SELECT id, zipcode FROM customers WHERE zipcode = 1001
        """
        result = clean_query(query)
        # Verify full normalized UNION structure
        assert "UNION" in result, "UNION keyword should be preserved"
        assert "UNION ALL" not in result, "Plain UNION should not become UNION ALL"
        # Both arms should be preserved
        assert "FROM users WHERE zipcode = 1001" in result, "First UNION arm should be preserved"
        assert "FROM customers WHERE zipcode = 1001" in result, "Second UNION arm should be preserved"

    def test_clean_query_with_union_all(self):
        """clean_query should handle UNION ALL queries."""
        query = """
        SELECT id, zipcode FROM users WHERE zipcode = 1001
        UNION ALL
        SELECT id, zipcode FROM customers WHERE zipcode = 1001
        """
        result = clean_query(query)
        # Verify UNION ALL is preserved (not collapsed to UNION)
        assert "UNION ALL" in result, "UNION ALL should be preserved as UNION ALL, not plain UNION"
        # Both arms should be preserved
        assert "FROM users WHERE zipcode = 1001" in result, "First UNION ALL arm should be preserved"
        assert "FROM customers WHERE zipcode = 1001" in result, "Second UNION ALL arm should be preserved"

    def test_clean_query_with_intersect(self):
        """clean_query should handle INTERSECT queries."""
        query = """
        SELECT zipcode FROM users WHERE active = true
        INTERSECT
        SELECT zipcode FROM customers WHERE premium = true
        """
        result = clean_query(query)
        # Verify full INTERSECT structure is preserved with normalization
        assert "INTERSECT" in result, "INTERSECT keyword should be preserved"
        assert "FROM users WHERE active = TRUE" in result, "First arm should be normalized (true -> TRUE)"
        assert "FROM customers WHERE premium = TRUE" in result, "Second arm should be normalized (true -> TRUE)"

    def test_clean_query_with_except(self):
        """clean_query should handle EXCEPT queries."""
        query = """
        SELECT zipcode FROM users
        EXCEPT
        SELECT zipcode FROM blacklisted_regions
        """
        result = clean_query(query)
        # Verify exact normalized output for EXCEPT
        expected = "SELECT zipcode FROM users EXCEPT SELECT zipcode FROM blacklisted_regions"
        assert result == expected, f"EXCEPT query should normalize to: {expected}, got: {result}"

    def test_clean_query_with_subquery_in_from(self):
        """clean_query should handle subqueries in FROM clause."""
        query = """
        SELECT t.zipcode, t.cnt
        FROM (SELECT zipcode, COUNT(*) as cnt FROM users GROUP BY zipcode) t
        WHERE t.zipcode = 1001
        """
        result = clean_query(query)
        # Verify subquery in FROM is preserved with proper normalization
        assert "SELECT t.zipcode, t.cnt FROM" in result, "Outer SELECT should be preserved"
        assert "COUNT(*) AS cnt" in result, "Aggregate function should be preserved with normalized alias"
        assert "GROUP BY zipcode" in result, "GROUP BY should be preserved"
        assert "AS t" in result, "Derived table alias should be preserved"
        assert "t.zipcode = 1001" in result, "WHERE condition should be preserved"


class TestCTEFragmentGeneration:
    """Test fragment generation with CTE (WITH clause) queries.

    The fragment generator currently processes CTEs by extracting tables from
    the CTE body (not the CTE alias). Fragments are generated from the inner
    query's tables. This provides partial but useful cache coverage.
    """

    def test_simple_cte_extracts_inner_table(self):
        """CTE should generate fragments from the inner table (test_locations)."""
        query = """
        WITH filtered AS (
            SELECT id, zipcode FROM test_locations WHERE population > 10000
        )
        SELECT * FROM filtered WHERE zipcode = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        # Should not crash; generates 2 fragments from the actual inner table
        assert len(pairs) == 2, f"Should generate exactly 2 fragments, got {len(pairs)}"
        fragment_texts = {qt.lower() for qt, _ in pairs}
        # Both fragments should reference test_locations (the real table), not the CTE alias "filtered"
        assert all("test_locations" in t for t in fragment_texts), "All fragments should reference test_locations"
        assert any("zipcode = 1001" in t for t in fragment_texts), "One fragment should include the partition key condition"
        # Verify hashes are non-empty SHA1 hex strings
        for _, query_hash in pairs:
            assert len(query_hash) == 40, "Hash should be a 40-char SHA1 hex digest"

    def test_cte_with_join_in_main_query(self):
        """CTE used as a table in a JOIN should still produce fragments."""
        query = """
        WITH user_data AS (
            SELECT id, zipcode FROM test_locations WHERE region = 'northeast'
        )
        SELECT u.zipcode, b.name
        FROM user_data u, test_businesses b
        WHERE u.zipcode = b.zipcode AND u.zipcode = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        # Should not crash; CTE inner table is extracted producing 2 fragments
        assert len(pairs) == 2, f"Should generate exactly 2 fragments from CTE inner table, got {len(pairs)}"
        fragment_texts = {qt.lower() for qt, _ in pairs}
        # Fragments should reference test_locations (the actual table inside the CTE)
        assert all("test_locations" in t for t in fragment_texts), "Fragments should reference test_locations"
        assert any("zipcode = 1001" in t for t in fragment_texts), "One fragment should include the partition key condition"
        for _, query_hash in pairs:
            assert len(query_hash) == 40, "Hash should be a 40-char SHA1 hex digest"

    def test_nested_cte(self):
        """Nested CTEs should not crash the fragment generator."""
        query = """
        WITH
            base AS (SELECT id, zipcode, region FROM test_locations),
            filtered AS (SELECT id, zipcode FROM base WHERE region = 'northeast')
        SELECT * FROM filtered WHERE zipcode = 1001
        """
        # Should not crash - nested CTEs produce fragments (CTE resolution is imperfect,
        # so some fragments may reference CTE aliases like "filtered" instead of real tables)
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 4, f"Should generate exactly 4 fragments for nested CTEs, got {len(pairs)}"
        fragment_texts = {qt.lower() for qt, _ in pairs}
        # At least one fragment should include the partition key condition
        assert any("zipcode = 1001" in t for t in fragment_texts), "Some fragments should include partition key condition"
        for _, query_hash in pairs:
            assert len(query_hash) == 40, "Hash should be a 40-char SHA1 hex digest"

    def test_cte_with_aggregation(self):
        """CTE with GROUP BY should not crash the fragment generator."""
        query = """
        WITH region_stats AS (
            SELECT zipcode, COUNT(*) as loc_count
            FROM test_locations
            GROUP BY zipcode
        )
        SELECT * FROM region_stats WHERE zipcode = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 2, f"Should generate exactly 2 fragments, got {len(pairs)}"
        assert all(len(h) == 40 for _, h in pairs), "All hashes should be 40-char SHA1 hex digests"


class TestWindowFunctionFragmentGeneration:
    """Test fragment generation with window function queries.

    Window functions are stripped during fragment generation (via sqlglot
    normalization). The WHERE conditions are correctly extracted, so
    fragments are generated normally from the base tables.
    """

    def test_simple_window_function(self):
        """Window function should be stripped; WHERE conditions extracted."""
        query = """
        SELECT id, zipcode, population,
               ROW_NUMBER() OVER (ORDER BY population DESC) as rank
        FROM test_locations
        WHERE zipcode = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 2, f"Should generate exactly 2 fragments, got {len(pairs)}"
        for _, query_hash in pairs:
            assert len(query_hash) == 40, "Hash should be a 40-char SHA1 hex digest"

    def test_window_partition_by_same_as_cache_partition(self):
        """Window PARTITION BY should not interfere with cache partition key."""
        query = """
        SELECT id, zipcode, population,
               RANK() OVER (PARTITION BY zipcode ORDER BY population DESC) as rank
        FROM test_locations
        WHERE zipcode IN (1001, 1002)
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 2, f"Should generate exactly 2 fragments, got {len(pairs)}"

    def test_window_with_multiple_tables(self):
        """Window function in a multi-table query should work."""
        query = """
        SELECT t1.zipcode, t1.name, t2.name as business_name,
               COUNT(*) OVER (PARTITION BY t1.zipcode) as businesses_in_zip
        FROM test_locations t1, test_businesses t2
        WHERE t1.zipcode = t2.region_id AND t1.zipcode = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 8, f"Should generate exactly 8 fragments for 2-table join, got {len(pairs)}"

    def test_multiple_window_functions(self):
        """Multiple window functions should not prevent fragment generation."""
        query = """
        SELECT id, zipcode, population,
               ROW_NUMBER() OVER (ORDER BY id) as row_num,
               SUM(population) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
        FROM test_locations
        WHERE zipcode BETWEEN 1000 AND 2000
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 4, f"Should generate exactly 4 fragments for BETWEEN query, got {len(pairs)}"


class TestSetOperationFragmentGeneration:
    """Test fragment generation with UNION/INTERSECT/EXCEPT queries.

    The fragment generator processes only the first SELECT arm of set
    operations (UNION, INTERSECT, EXCEPT). This provides partial but
    useful coverage - fragments from the first query are cached.
    """

    def test_simple_union_processes_first_arm(self):
        """UNION should generate fragments from at least the first arm."""
        query = """
        SELECT id, zipcode FROM test_locations WHERE zipcode = 1001
        UNION
        SELECT id, region_id FROM test_businesses WHERE region_id = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 2, f"Should generate exactly 2 fragments from first UNION arm, got {len(pairs)}"
        # Verify fragments reference test_locations (the first arm)
        fragment_texts = [qt.lower() for qt, _ in pairs]
        has_first_table = any("test_locations" in t for t in fragment_texts)
        assert has_first_table, "Fragments should reference first arm's table"

    def test_union_all(self):
        """UNION ALL should generate fragments without crashing."""
        query = """
        SELECT zipcode FROM test_locations WHERE population > 10000
        UNION ALL
        SELECT region_id FROM test_businesses WHERE rating > 4.0
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 1, f"Should generate exactly 1 fragment from first UNION ALL arm, got {len(pairs)}"

    def test_intersect(self):
        """INTERSECT should generate fragments from the first arm."""
        query = """
        SELECT zipcode FROM test_locations WHERE region = 'northeast'
        INTERSECT
        SELECT region_id FROM test_businesses WHERE business_type = 'restaurant'
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 1, f"Should generate exactly 1 fragment from first INTERSECT arm, got {len(pairs)}"

    def test_except(self):
        """EXCEPT should generate fragments from the first arm."""
        query = """
        SELECT zipcode FROM test_locations
        EXCEPT
        SELECT region_id FROM test_businesses WHERE rating < 3.0
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 1, f"Should generate exactly 1 fragment from first EXCEPT arm, got {len(pairs)}"

    def test_three_way_union(self):
        """Three-way UNION should generate fragments from at least one arm."""
        query = """
        SELECT id, zipcode FROM test_locations WHERE region = 'northeast'
        UNION
        SELECT id, zipcode FROM test_locations WHERE region = 'west'
        UNION
        SELECT id, zipcode FROM test_locations WHERE region = 'southeast'
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 1, f"Should generate exactly 1 fragment from three-way UNION, got {len(pairs)}"


class TestSubqueryInFromClause:
    """Test fragment generation with subqueries in the FROM clause."""

    def test_simple_subquery_in_from(self):
        """Subquery used as a derived table in FROM should generate fragments."""
        query = """
        SELECT t.zipcode, t.cnt
        FROM (SELECT zipcode, COUNT(*) as cnt FROM test_locations GROUP BY zipcode) t
        WHERE t.zipcode = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 2, f"Should generate exactly 2 fragments, got {len(pairs)}"
        for _, query_hash in pairs:
            assert len(query_hash) == 40, "Hash should be a 40-char SHA1 hex digest"

    def test_subquery_with_join(self):
        """Subquery in FROM joined with a regular table produces no fragments (parse limitation)."""
        query = """
        SELECT t.zipcode, b.name
        FROM (SELECT DISTINCT zipcode FROM test_locations WHERE population > 10000) t,
             test_businesses b
        WHERE t.zipcode = b.region_id AND t.zipcode = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 0, f"Subquery-in-FROM with join produces 0 fragments (parse limitation), got {len(pairs)}"


class TestExistingPatternsNotRegressed:
    """Ensure standard patterns still work after any changes."""

    def test_simple_single_table(self):
        """Basic single-table query should always work."""
        query = "SELECT * FROM test_locations t1 WHERE t1.zipcode = 1001"
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 2, f"Single-table query should generate exactly 2 fragments, got {len(pairs)}"

    def test_multi_table_join(self):
        """Multi-table join with partition key should always work."""
        query = """
        SELECT t1.zipcode, t2.name
        FROM test_locations t1, test_businesses t2
        WHERE SQRT(POWER(t1.x - t2.x, 2) + POWER(t1.y - t2.y, 2)) <= 0.01
        AND t1.zipcode = 1001
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 8, f"2-table join should generate exactly 8 fragments, got {len(pairs)}"

    def test_in_subquery(self):
        """IN subquery should still work."""
        query = """
        SELECT * FROM test_locations t1
        WHERE t1.zipcode IN (SELECT zipcode FROM test_locations WHERE region = 'northeast')
        """
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 3, f"IN subquery should generate exactly 3 fragments, got {len(pairs)}"

    def test_between_condition(self):
        """BETWEEN condition on partition key should work."""
        query = "SELECT * FROM test_locations t1 WHERE t1.zipcode BETWEEN 1000 AND 2000"
        pairs = generate_all_query_hash_pairs(query, "zipcode")
        assert len(pairs) == 4, f"BETWEEN query should generate exactly 4 fragments, got {len(pairs)}"

    def test_hash_consistency(self):
        """Same query should always produce same hashes."""
        query = "SELECT * FROM test_locations t1 WHERE t1.zipcode = 1001"
        pairs1 = generate_all_query_hash_pairs(query, "zipcode")
        pairs2 = generate_all_query_hash_pairs(query, "zipcode")

        hashes1 = sorted([h for _, h in pairs1])
        hashes2 = sorted([h for _, h in pairs2])
        assert hashes1 == hashes2, "Hashes should be deterministic"


class TestExtractConditionsAdvanced:
    """Test condition extraction from advanced SQL patterns."""

    def test_extract_conditions_simple(self):
        """Basic AND conditions should be extracted correctly."""
        query = "SELECT * FROM t1 WHERE t1.a = 1 AND t1.b = 2"
        conditions = extract_conjunctive_conditions(query)
        assert len(conditions) == 2

    def test_extract_conditions_with_or(self):
        """OR conditions should be kept together (not split)."""
        query = "SELECT * FROM t1 WHERE t1.a = 1 AND (t1.b = 2 OR t1.b = 3)"
        conditions = extract_conjunctive_conditions(query)
        # Should get 2 conjunctive conditions: a=1 AND (b=2 OR b=3)
        assert len(conditions) == 2

    def test_extract_conditions_complex_where(self):
        """Complex WHERE with multiple AND conditions."""
        query = """
        SELECT * FROM t1, t2
        WHERE t1.id = t2.id AND t1.zipcode = 1001 AND t2.type = 'restaurant'
        """
        conditions = extract_conjunctive_conditions(query)
        assert len(conditions) == 3
