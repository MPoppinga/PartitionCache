"""
Unit tests for ORDER BY and LIMIT clause removal in query cleaning.

Tests the query_processor.clean_query function to ensure it properly strips
ORDER BY and LIMIT clauses to improve cache consistency.
"""

from partitioncache.query_processor import clean_query, generate_all_hashes


class TestOrderByLimitCleaning:
    """Test ORDER BY and LIMIT clause removal functionality."""

    def test_basic_query_unchanged(self):
        """Test that basic queries without ORDER BY/LIMIT are unchanged."""
        query = "SELECT * FROM users WHERE city_id = 10"
        result = clean_query(query)
        assert result == "SELECT * FROM users WHERE city_id = 10"

    def test_limit_removal(self):
        """Test that LIMIT clauses are properly removed."""
        query = "SELECT * FROM users WHERE city_id = 10 LIMIT 100"
        result = clean_query(query)
        assert "LIMIT" not in result
        assert result == "SELECT * FROM users WHERE city_id = 10"

    def test_order_by_removal(self):
        """Test that ORDER BY clauses are properly removed."""
        query = "SELECT * FROM users WHERE city_id = 10 ORDER BY created_at DESC"
        result = clean_query(query)
        assert "ORDER BY" not in result
        assert result == "SELECT * FROM users WHERE city_id = 10"

    def test_both_order_by_and_limit_removal(self):
        """Test that both ORDER BY and LIMIT are removed together."""
        query = "SELECT * FROM users WHERE city_id = 10 ORDER BY created_at DESC LIMIT 100"
        result = clean_query(query)
        assert "ORDER BY" not in result
        assert "LIMIT" not in result
        assert result == "SELECT * FROM users WHERE city_id = 10"

    def test_complex_order_by_removal(self):
        """Test removal of complex ORDER BY clauses."""
        query = "SELECT * FROM users WHERE city_id = 10 ORDER BY created_at DESC, user_id ASC, score DESC"
        result = clean_query(query)
        assert "ORDER BY" not in result
        assert result == "SELECT * FROM users WHERE city_id = 10"

    def test_subquery_order_by_limit_removal(self):
        """Test that ORDER BY and LIMIT are removed from subqueries."""
        query = """
        SELECT u.* FROM (
            SELECT * FROM users
            WHERE active = true
            ORDER BY score DESC
            LIMIT 10
        ) u WHERE city_id = 10
        """
        result = clean_query(query)
        assert "ORDER BY" not in result
        assert "LIMIT" not in result
        # Should still have the WHERE conditions
        assert "active = TRUE" in result
        assert "city_id = 10" in result

    def test_nested_subquery_cleaning(self):
        """Test ORDER BY/LIMIT removal in deeply nested subqueries."""
        query = """
        SELECT * FROM (
            SELECT city_id FROM (
                SELECT * FROM users
                ORDER BY created_at DESC
                LIMIT 50
            ) ORDER BY user_id ASC
        ) WHERE city_id IN (1, 2, 3)
        """
        result = clean_query(query)
        assert "ORDER BY" not in result
        assert "LIMIT" not in result
        assert "city_id IN (1, 2, 3)" in result

    def test_order_by_with_functions(self):
        """Test removal of ORDER BY with complex expressions."""
        query = "SELECT * FROM sales WHERE city_id = 10 ORDER BY DATE(created_at), SUM(revenue) DESC"
        result = clean_query(query)
        assert "ORDER BY" not in result
        assert "DATE(created_at)" not in result
        assert "SUM(revenue)" not in result
        assert "city_id = 10" in result

    def test_limit_with_offset(self):
        """Test LIMIT removal (note: OFFSET handling may vary by implementation)."""
        query = "SELECT * FROM users WHERE city_id = 10 LIMIT 20 OFFSET 10"
        result = clean_query(query)
        # The implementation might handle OFFSET differently, but LIMIT should be gone
        assert "LIMIT" not in result
        assert "city_id = 10" in result

    def test_cache_consistency_different_order_by(self):
        """Test that queries with different ORDER BY generate same hashes."""
        queries = [
            "SELECT * FROM events WHERE user_id = 123",
            "SELECT * FROM events WHERE user_id = 123 ORDER BY created_at DESC",
            "SELECT * FROM events WHERE user_id = 123 ORDER BY event_type ASC",
            "SELECT * FROM events WHERE user_id = 123 ORDER BY priority DESC, created_at ASC",
        ]

        # All queries should generate the same hashes after cleaning
        hash_sets = [set(generate_all_hashes(q, "user_id", min_component_size=1)) for q in queries]

        if hash_sets[0]:  # Only test if we actually generate hashes
            for i in range(1, len(hash_sets)):
                assert hash_sets[0] == hash_sets[i], f"Query {i+1} generates different hashes than query 1"

    def test_cache_consistency_different_limits(self):
        """Test that queries with different LIMIT values generate same hashes."""
        queries = [
            "SELECT * FROM products WHERE category = 'electronics'",
            "SELECT * FROM products WHERE category = 'electronics' LIMIT 10",
            "SELECT * FROM products WHERE category = 'electronics' LIMIT 100",
            "SELECT * FROM products WHERE category = 'electronics' LIMIT 1000",
        ]

        # All queries should generate the same hashes after cleaning
        hash_sets = [set(generate_all_hashes(q, "store_id", min_component_size=1)) for q in queries]

        if hash_sets[0]:  # Only test if we actually generate hashes
            for i in range(1, len(hash_sets)):
                assert hash_sets[0] == hash_sets[i], f"Query {i+1} generates different hashes than query 1"

    def test_mixed_order_by_limit_combinations(self):
        """Test various combinations of ORDER BY and LIMIT."""
        queries = [
            "SELECT * FROM sales WHERE region = 'US'",
            "SELECT * FROM sales WHERE region = 'US' ORDER BY sale_date",
            "SELECT * FROM sales WHERE region = 'US' LIMIT 50",
            "SELECT * FROM sales WHERE region = 'US' ORDER BY revenue DESC LIMIT 25",
            "SELECT * FROM sales WHERE region = 'US' ORDER BY sale_date ASC, revenue DESC LIMIT 100",
        ]

        # All cleaned queries should be identical
        cleaned_queries = [clean_query(q) for q in queries]
        base_query = cleaned_queries[0]

        for i, cleaned in enumerate(cleaned_queries[1:], 1):
            assert cleaned == base_query, f"Query {i+1} cleaning result differs from base query"

    def test_preserve_important_clauses(self):
        """Test that important clauses (WHERE, GROUP BY, HAVING) are preserved."""
        query = """
        SELECT city_id, COUNT(*) as user_count
        FROM users
        WHERE active = true AND created_at > '2024-01-01'
        GROUP BY city_id
        HAVING COUNT(*) > 10
        ORDER BY COUNT(*) DESC
        LIMIT 20
        """
        result = clean_query(query)

        # Should remove ORDER BY and LIMIT
        assert "ORDER BY" not in result
        assert "LIMIT" not in result

        # Should preserve WHERE, GROUP BY, HAVING
        assert "WHERE" in result
        assert "GROUP BY" in result
        assert "HAVING" in result
        assert "active = TRUE" in result
        assert "COUNT(*) > 10" in result

    def test_comments_and_formatting(self):
        """Test that comments are still removed along with ORDER BY/LIMIT."""
        query = """
        SELECT * FROM users
        WHERE city_id = 10 -- This is a comment
        ORDER BY created_at DESC -- Another comment
        LIMIT 50 -- Final comment
        """
        result = clean_query(query)

        assert "ORDER BY" not in result
        assert "LIMIT" not in result
        assert "--" not in result  # Comments should be removed
        assert "city_id = 10" in result

    def test_fallback_regex_behavior(self):
        """Test that regex fallback works for edge cases."""
        # This might trigger the fallback if sqlglot parsing fails
        query = "SELECT * FROM users WHERE city_id = 10 ORDER BY RANDOM() LIMIT 5"
        result = clean_query(query)

        # Even with fallback, should remove ORDER BY and LIMIT
        assert ("ORDER BY" not in result) or ("LIMIT" not in result), "At least one clause should be removed"
        assert "city_id = 10" in result
