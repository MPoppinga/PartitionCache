"""
Tests for query processor robustness and edge cases.

These tests are designed to catch issues like the KeyError with unknown aliases
and ensure the correct processing order for p0 table functionality.
"""

from unittest.mock import Mock, patch

import pytest

from partitioncache.apply_cache import apply_cache_lazy, rewrite_query_with_p0_table
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy
from partitioncache.query_processor import extract_and_group_query_conditions, generate_all_hashes


class TestQueryProcessorRobustness:
    """Test query processor robustness with complex queries and edge cases."""

    def test_extract_conditions_with_simple_where_clause(self):
        """Test that WHERE clause queries work correctly (JOIN keywords not supported)."""
        query = "SELECT * FROM users AS u WHERE u.active = true"

        # This should not raise a KeyError
        try:
            result = extract_and_group_query_conditions(query, "user_id")
            attribute_conditions, distance_conditions, other_functions, partition_key_conditions, or_conditions, table_aliases, table = result

            # Basic sanity checks
            assert isinstance(attribute_conditions, dict)
            assert isinstance(table_aliases, list)
            assert len(table_aliases) >= 1
        except KeyError as e:
            pytest.fail(f"KeyError raised for simple WHERE query: {e}")

    def test_extract_conditions_with_multiple_tables(self):
        """Test with multiple tables in FROM clause (WHERE-based joins)."""
        query = """
        SELECT u.name, o.total 
        FROM users AS u, orders AS o, addresses AS a
        WHERE u.id = o.user_id AND u.address_id = a.id AND u.active = true AND o.status = 'complete'
        """

        try:
            result = extract_and_group_query_conditions(query, "user_id")
            attribute_conditions, distance_conditions, other_functions, partition_key_conditions, or_conditions, table_aliases, table = result

            # Should have detected multiple table aliases
            assert len(table_aliases) >= 2
            assert isinstance(attribute_conditions, dict)
        except KeyError as e:
            pytest.fail(f"KeyError raised for multiple table query: {e}")

    def test_extract_conditions_with_p0_table(self):
        """Test with p0 table patterns that previously caused issues."""
        query = """
        SELECT * FROM users AS u, zipcode_mv AS p0_zipcode 
        WHERE u.zipcode = p0_zipcode.zipcode AND u.active = true
        """

        try:
            result = extract_and_group_query_conditions(query, "zipcode")
            attribute_conditions, distance_conditions, other_functions, partition_key_conditions, or_conditions, table_aliases, table = result

            # Should handle p0 aliases without error
            assert isinstance(attribute_conditions, dict)
            assert isinstance(table_aliases, list)
        except KeyError as e:
            pytest.fail(f"KeyError raised for p0 table query: {e}")

    def test_extract_conditions_with_unknown_aliases_graceful(self):
        """Test that unknown aliases are handled gracefully."""
        # This query has an alias 'p3' that might not be properly detected
        query = "SELECT * FROM users AS u WHERE p3.unknown_field = 'value'"

        try:
            result = extract_and_group_query_conditions(query, "user_id")
            # Should not crash even with problematic aliases
            assert result is not None
        except KeyError as e:
            # If it still fails, at least document the specific issue
            pytest.fail(f"KeyError with unknown alias 'p3': {e}")

    def test_generate_all_hashes_ensures_hash_generation(self):
        """Test that hash generation works and generates hashes when partition_key is present."""
        query = "SELECT * FROM poi AS p1 WHERE p1.zipcode = 'test'"

        try:
            hashes = generate_all_hashes(
                query=query,
                partition_key="zipcode",
                min_component_size=1,
                follow_graph=True,
                fix_attributes=True,
                canonicalize_queries=False,
            )

            # Should return a list of hashes without error and generate at least one hash
            assert isinstance(hashes, list)
            assert len(hashes) > 0, "Should generate at least one hash when partition_key is present"
        except Exception as e:
            pytest.fail(f"Hash generation failed: {e}")


class TestP0TableProcessingOrder:
    """Test that p0 table processing follows the correct order."""

    def create_mock_cache_handler(self, lazy_subquery: str | None = None, used_hashes: int = 0):
        """Create a mock cache handler for testing."""
        mock_handler = Mock(spec=AbstractCacheHandler_Lazy)
        mock_handler.get_intersected_lazy.return_value = (lazy_subquery, used_hashes)
        return mock_handler

    def test_processing_order_original_query_analyzed_first(self):
        """Test that query analysis happens on original query, not p0-rewritten query."""
        original_query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=2)

        # Patch get_partition_keys_lazy to verify it gets called with original query
        with patch("partitioncache.apply_cache.get_partition_keys_lazy") as mock_get_keys:
            mock_get_keys.return_value = (lazy_subquery, 5, 2)

            enhanced_query, stats = apply_cache_lazy(
                query=original_query, cache_handler=mock_handler, partition_key="zipcode", use_p0_table=True, p0_alias="u"
            )

            # Verify get_partition_keys_lazy was called with the ORIGINAL query
            mock_get_keys.assert_called_once()
            call_args = mock_get_keys.call_args[1]  # Get keyword arguments
            assert call_args["query"] == original_query  # Should be original, not p0-rewritten

    def test_p0_rewrite_applied_to_original_not_cached(self):
        """Test that p0 rewrite is applied to original query, then cache is applied to result."""
        original_query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=2)

        enhanced_query, stats = apply_cache_lazy(
            query=original_query, cache_handler=mock_handler, partition_key="zipcode", use_p0_table=True, p0_alias="u"
        )

        # The result should contain both p0 table and cache constraint
        assert "zipcode_mv" in enhanced_query  # P0 table present
        assert "u.zipcode IN" in enhanced_query  # Cache constraint present
        assert stats["p0_rewritten"] == 1
        assert stats["enhanced"] == 1

    def test_no_cache_hits_still_applies_p0_table(self):
        """Test that p0 table is still applied even when there are no cache hits."""
        original_query = "SELECT * FROM users AS u WHERE u.active = true"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=None, used_hashes=0)

        enhanced_query, stats = apply_cache_lazy(
            query=original_query, cache_handler=mock_handler, partition_key="zipcode", use_p0_table=True, p0_alias="u"
        )

        # Should have p0 table but no cache enhancement
        assert "zipcode_mv" in enhanced_query  # P0 table present
        assert "u.zipcode IN" not in enhanced_query  # No cache constraint
        assert stats["p0_rewritten"] == 1
        assert stats["enhanced"] == 0  # No cache hits

    def test_cache_hits_without_p0_table(self):
        """Test that cache works normally without p0 table."""
        original_query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=2)

        enhanced_query, stats = apply_cache_lazy(
            query=original_query,
            cache_handler=mock_handler,
            partition_key="zipcode",
            use_p0_table=False,  # Disabled
            p0_alias="u",
        )

        # Should have cache but no p0 table
        assert "zipcode_mv" not in enhanced_query  # No P0 table
        assert "u.zipcode IN" in enhanced_query  # Cache constraint present
        assert stats["p0_rewritten"] == 0
        assert stats["enhanced"] == 1


class TestP0TableRobustness:
    """Test p0 table functionality robustness."""

    def test_p0_table_with_already_present_mv_table(self):
        """Test that p0 table logic handles queries that already have mv tables."""
        query_with_mv = "SELECT * FROM users AS u, zipcode_mv AS zm WHERE u.zipcode = zm.zipcode AND u.active = true"

        # Should return unchanged when mv table already present
        result = rewrite_query_with_p0_table(query_with_mv, "zipcode", p0_alias="u")
        assert result == query_with_mv  # Should be unchanged

    def test_p0_table_with_complex_where_conditions(self):
        """Test p0 table with complex WHERE conditions."""
        query = """
        SELECT u.name, COUNT(o.id) as order_count
        FROM users AS u, orders AS o
        WHERE u.id = o.user_id AND o.status = 'complete' AND u.created_at > '2023-01-01' AND (u.region = 'US' OR u.region = 'CA')
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 0
        ORDER BY order_count DESC
        """

        # Should handle complex queries without error
        result = rewrite_query_with_p0_table(query, "region_id", p0_alias="u")

        # Should preserve original complexity while adding p0 table
        assert "region_id_mv AS u" in result
        assert "u.region_id = u.region_id" in result
        assert "GROUP BY" in result
        assert "HAVING" in result
        assert "ORDER BY" in result

    def test_p0_table_with_subqueries(self):
        """Test p0 table with queries containing subqueries."""
        query = """
        SELECT * FROM users AS u 
        WHERE u.id IN (SELECT user_id FROM orders WHERE total > 100)
        AND u.active = true
        """

        # Should handle subqueries without error
        result = rewrite_query_with_p0_table(query, "zipcode", p0_alias="u")

        # Should preserve subquery while adding p0 constraint
        assert "SELECT user_id FROM orders WHERE total > 100" in result
        assert "u.zipcode = u.zipcode" in result
        assert "zipcode_mv AS u" in result

    def test_p0_table_auto_alias_detection(self):
        """Test that p0 table properly detects table aliases regardless of parameter."""
        # Test query with t1, t2, t3 aliases (like query processor generates)
        query = "SELECT DISTINCT t1.zipcode FROM pois AS t1, poi_categories AS t2, locations AS t3 WHERE t1.id = t2.poi_id"

        # Should detect t1 as main table alias and use it correctly
        result = rewrite_query_with_p0_table(query, "zipcode", p0_alias="p1")  # Wrong alias intentionally

        # Should use detected alias t1, not the wrong p0_alias parameter
        assert "t1.zipcode = p1.zipcode" in result
        assert "zipcode_mv AS p1" in result
        assert "p0.zipcode" not in result  # Should use specified p0_alias

    def test_p0_table_generated_query_aliases(self):
        """Test p0 table with aliases typical of query processor output."""
        # Simulate query processor generated query with numeric aliases
        query = "SELECT DISTINCT t1.zipcode FROM pois AS t1 WHERE t1.category = 'restaurant'"

        result = rewrite_query_with_p0_table(query, "zipcode")

        # Should correctly use t1 alias
        assert "t1.zipcode = p0.zipcode" in result
        assert "zipcode_mv AS p0" in result

    def test_p0_table_mixed_aliases(self):
        """Test p0 table with mixed alias patterns."""
        query = "SELECT * FROM poi AS p3, locations AS loc1 WHERE p3.location_id = loc1.id"

        result = rewrite_query_with_p0_table(query, "region_id")

        # Should use first table alias (p3)
        assert "p3.region_id = p0.region_id" in result
        assert "region_id_mv AS p0" in result
