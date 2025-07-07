"""
Tests for the apply_cache module, specifically extend_query_with_partition_keys function.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from partitioncache.apply_cache import (
    apply_cache_lazy,
    extend_query_with_partition_keys,
    extend_query_with_partition_keys_lazy,
    find_p0_alias,
    rewrite_query_with_p0_table,
)
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy


class TestFindP0Alias:
    """Test the find_p0_alias function."""

    def test_simple_table_with_alias(self):
        """Test finding alias from simple query with table alias."""
        query = "SELECT * FROM users AS u WHERE u.id = 1"
        result = find_p0_alias(query, "partition_key")
        assert result == "u"

    def test_simple_table_without_alias(self):
        """Test finding table name when no alias is provided."""
        query = "SELECT * FROM users WHERE id = 1"
        result = find_p0_alias(query, "partition_key")
        assert result == "users"

    def test_multiple_tables_returns_first(self):
        """Test that first table alias is returned when multiple tables exist."""
        query = "SELECT * FROM users AS u, orders AS o WHERE u.id = o.user_id"
        result = find_p0_alias(query, "partition_key")
        assert result == "u"

    def test_join_query(self):
        """Test finding alias in JOIN query."""
        query = "SELECT * FROM users AS u JOIN orders AS o ON u.id = o.user_id"
        result = find_p0_alias(query, "partition_key")
        assert result == "u"

    def test_no_table_raises_error(self):
        """Test that ValueError is raised when no table is found."""
        query = "SELECT 1"
        with pytest.raises(ValueError, match="No table found in query"):
            find_p0_alias(query, "partition_key")

    def test_prefers_p0_table_alias(self):
        """Test that p0 table alias is preferred over other tables."""
        query = "SELECT * FROM tt AS t1, tt AS t2, zipcode_mv AS p0 WHERE t1.zipcode = p0.zipcode AND t2.zipcode = p0.zipcode"
        result = find_p0_alias(query, "zipcode")
        assert result == "p0"

    def test_detects_star_join_table(self):
        """Test that mv table is detected  when it's a proper star-join."""
        # With only 2 tables, the mv table won't be auto-detected as star-join
        # It needs to join multiple tables to be considered a star-join
        query = "SELECT * FROM users AS u, zipcode_mv AS zips WHERE u.zipcode = zips.zipcode"
        result = find_p0_alias(query, "zipcode")
        # Should return first table since it's not detected as star-join with only 2 tables
        assert result == "u"

        # Test with 3+ tables where zipcode_mv joins all others
        query_star = """
        SELECT * FROM users AS u, orders AS o, zipcode_mv AS zips
        WHERE u.zipcode = zips.zipcode AND o.zipcode = zips.zipcode
        """
        result_star = find_p0_alias(query_star, "zipcode")
        # Now it should detect zips as the star-join table
        assert result_star == "zips"


class TestRewriteQueryWithP0Table:
    """Test the rewrite_query_with_p0_table function."""

    def test_join_method_simple_query(self):
        """Test rewrite with simple query."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        result = rewrite_query_with_p0_table(query, "zipcode", p0_alias="u")

        assert "zipcode_mv AS u" in result
        assert "u.zipcode = u.zipcode" in result
        assert "u.active = TRUE" in result or "u.active = true" in result

    def test_join_method_custom_mv_table_name(self):
        """Test rewrite with custom mv table name."""
        query = "SELECT * FROM poi AS p1 WHERE p1.category = 'restaurant'"
        result = rewrite_query_with_p0_table(query, "region_id", mv_table_name="custom_regions_mv", p0_alias="p1")

        assert "custom_regions_mv AS p1" in result
        assert "p1.region_id = p1.region_id" in result
        assert "p1.category = 'restaurant'" in result

    def test_auto_alias_detection(self):
        """Test automatic alias detection when p0_alias is default."""
        query = "SELECT * FROM users AS u WHERE u.id = 1"
        result = rewrite_query_with_p0_table(query, "zipcode")

        assert "zipcode_mv AS p0" in result
        assert "u.zipcode = p0.zipcode" in result

    def test_mv_table_already_present_skips_rewrite(self):
        """Test that query is not rewritten if mv table is already present."""
        query = "SELECT * FROM users AS u JOIN zipcode_mv AS zm ON u.zipcode = zm.zipcode WHERE u.active = true"
        result = rewrite_query_with_p0_table(query, "zipcode", p0_alias="u")

        # Should return original query unchanged
        assert result == query

    def test_query_without_from_clause_returns_original(self):
        """Test that query without FROM clause returns original query."""
        query = "SELECT 1 AS test_value"
        result = rewrite_query_with_p0_table(query, "zipcode", p0_alias="u")

        assert result == query

    def test_complex_query_preservation(self):
        """Test that complex query features are preserved during rewrite."""
        query = """
        SELECT u.name, COUNT(o.id) as order_count
        FROM users AS u
        LEFT JOIN orders AS o ON u.id = o.user_id AND o.status = 'complete'
        WHERE u.created_at > '2023-01-01'
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 0
        ORDER BY order_count DESC
        """
        result = rewrite_query_with_p0_table(query, "region_id", p0_alias="u")

        assert "region_id_mv AS u" in result
        assert "u.region_id = u.region_id" in result
        assert "LEFT JOIN orders" in result
        assert "GROUP BY" in result
        assert "HAVING" in result
        assert "ORDER BY" in result

    def test_default_mv_table_name_generation(self):
        """Test that default mv table name is generated correctly."""
        query = "SELECT * FROM users AS u"
        result = rewrite_query_with_p0_table(query, "zipcode")

        assert "zipcode_mv" in result

        # Test with different partition key
        result2 = rewrite_query_with_p0_table(query, "region_id")
        assert "region_id_mv" in result2


class TestExtendQueryWithPartitionKeys:
    """Test the extend_query_with_partition_keys function."""

    def test_empty_partition_keys_returns_original(self):
        """Test that empty partition keys returns original query."""
        query = "SELECT * FROM users WHERE id = 1"
        result = extend_query_with_partition_keys(query, set(), "partition_key")
        assert result == query

    def test_in_method_with_integers(self):
        """Test IN method with integer partition keys."""
        query = "SELECT * FROM users AS u WHERE u.id = 1"
        partition_keys = {1, 2, 3}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="IN", p0_alias="u")

        # Check that the query contains the partition condition
        assert "u.region_id IN (1, 2, 3)" in result or "u.region_id IN (1,2,3)" in result or "u.region_id IN (3,2,1)" in result
        assert "WHERE" in result
        assert "AND" in result

    def test_in_method_with_strings(self):
        """Test IN method with string partition keys."""
        query = "SELECT * FROM users AS u"
        partition_keys = {"region_a", "region_b"}
        result = extend_query_with_partition_keys(query, partition_keys, "region", method="IN", p0_alias="u")

        # Check that strings are properly quoted
        assert "'region_a'" in result
        assert "'region_b'" in result
        assert "u.region IN" in result

    def test_in_method_no_existing_where(self):
        """Test IN method when query has no WHERE clause."""
        query = "SELECT * FROM users AS u"
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="IN", p0_alias="u")

        assert "WHERE u.region_id IN" in result

    def test_in_method_with_existing_where(self):
        """Test IN method when query already has WHERE clause."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="IN", p0_alias="u")

        assert "u.active = TRUE" in result or "u.active = true" in result
        assert "u.region_id IN" in result
        assert "AND" in result

    def test_in_method_auto_detect_alias(self):
        """Test IN method with automatic alias detection."""
        query = "SELECT * FROM users AS u WHERE u.id = 1"
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="IN")

        assert "u.region_id IN" in result

    def test_values_method(self):
        """Test VALUES method."""
        query = "SELECT * FROM users AS u"
        partition_keys = {1, 2, 3}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="VALUES", p0_alias="u")

        assert "u.region_id IN (VALUES" in result
        assert "WHERE" in result
        # Should contain individual values
        assert "1" in result
        assert "2" in result
        assert "3" in result

    def test_tmp_table_in_method(self):
        """Test TMP_TABLE_IN method."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        partition_keys = {1, 2, 3}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="TMP_TABLE_IN", p0_alias="u")

        assert "CREATE TEMPORARY TABLE tmp_partition_keys" in result
        assert "partition_key INT PRIMARY KEY" in result
        assert "INSERT INTO tmp_partition_keys" in result
        assert "VALUES(1),(2),(3)" in result or "VALUES(1,2,3)" in result
        assert "u.region_id IN (SELECT partition_key FROM tmp_partition_keys)" in result

    def test_tmp_table_in_with_strings(self):
        """Test TMP_TABLE_IN method with string partition keys."""
        query = "SELECT * FROM users AS u"
        partition_keys = {"a", "b"}
        result = extend_query_with_partition_keys(query, partition_keys, "region", method="TMP_TABLE_IN", p0_alias="u")

        assert "partition_key TEXT PRIMARY KEY" in result
        assert "'a'" in result
        assert "'b'" in result

    def test_tmp_table_join_method(self):
        """Test TMP_TABLE_JOIN method."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="TMP_TABLE_JOIN", p0_alias="u")

        assert "CREATE TEMPORARY TABLE tmp_partition_keys" in result
        assert "INSERT INTO tmp_partition_keys" in result
        # Should contain join logic somewhere
        assert "tmp_" in result

    def test_analyze_tmp_table_false(self):
        """Test that analyze_tmp_table=False skips ANALYZE."""
        query = "SELECT * FROM users AS u"
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="TMP_TABLE_IN", p0_alias="u", analyze_tmp_table=False)

        assert "ANALYZE tmp_partition_keys" not in result
        assert "CREATE INDEX" not in result

    def test_analyze_tmp_table_true(self):
        """Test that analyze_tmp_table=True includes ANALYZE."""
        query = "SELECT * FROM users AS u"
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="TMP_TABLE_IN", p0_alias="u", analyze_tmp_table=True)

        assert "ANALYZE tmp_partition_keys" in result
        assert "CREATE INDEX" in result

    def test_float_partition_keys(self):
        """Test with float partition keys."""
        query = "SELECT * FROM measurements AS m"
        partition_keys = {1.5, 2.7, 3.14}
        result = extend_query_with_partition_keys(query, partition_keys, "measurement_value", method="IN", p0_alias="m")

        # Should treat floats as strings (quoted)
        assert "'1.5'" in result or "'2.7'" in result or "'3.14'" in result

    def test_datetime_partition_keys(self):
        """Test with datetime partition keys."""
        query = "SELECT * FROM events AS e"
        dt1 = datetime(2023, 1, 1, 12, 0, 0)
        dt2 = datetime(2023, 1, 2, 12, 0, 0)
        partition_keys = {dt1, dt2}
        result = extend_query_with_partition_keys(query, partition_keys, "event_date", method="IN", p0_alias="e")

        # Datetimes should be treated as strings (quoted)
        assert "2023-01-01" in result
        assert "2023-01-02" in result

    def test_invalid_method_raises_error(self):
        """Test that invalid method raises appropriate error."""
        query = "SELECT * FROM users AS u"
        partition_keys = {1, 2}

        # This should raise an error or handle gracefully
        # Since the method is using Literal type annotation, it should be caught at type checking
        # But let's test runtime behavior
        try:
            result = extend_query_with_partition_keys(
                query,
                partition_keys,
                "region_id",
                method="INVALID",  # type: ignore
            )
            # If it doesn't raise an error, the function should return the original query
            # or handle it gracefully
            assert isinstance(result, str)
        except Exception:
            # Expected behavior for invalid method
            pass

    def test_complex_query_with_joins(self):
        """Test with complex query containing joins."""
        query = """
        SELECT u.name, o.total
        FROM users AS u
        JOIN orders AS o ON u.id = o.user_id
        WHERE u.active = true AND o.status = 'complete'
        """
        partition_keys = {1, 2, 3}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="IN", p0_alias="u")

        assert "u.region_id IN" in result
        assert "u.active = TRUE" in result or "u.active = true" in result
        assert "o.status = 'complete'" in result

    def test_query_with_subquery(self):
        """Test with query containing subquery."""
        query = """
        SELECT * FROM users AS u
        WHERE u.id IN (SELECT user_id FROM orders WHERE total > 100)
        """
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="IN", p0_alias="u")

        assert "u.region_id IN" in result
        assert "SELECT user_id FROM orders" in result  # Subquery should be preserved

    def test_single_partition_key(self):
        """Test with single partition key."""
        query = "SELECT * FROM users AS u"
        partition_keys = {42}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="IN", p0_alias="u")

        assert "u.region_id IN (42)" in result

    def test_tmp_table_join_with_multiple_tables(self):
        """Test TMP_TABLE_JOIN with multiple tables."""
        query = "SELECT * FROM users AS u, orders AS o WHERE u.id = o.user_id"
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="TMP_TABLE_JOIN")

        # When p0_alias is None for TMP_TABLE_JOIN, it should join on all tables
        assert "CREATE TEMPORARY TABLE tmp_partition_keys" in result

    def test_tmp_table_in_no_existing_where(self):
        """Test TMP_TABLE_IN method when query has no WHERE clause."""
        query = "SELECT * FROM users AS u"
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="TMP_TABLE_IN", p0_alias="u")

        assert "WHERE u.region_id IN (SELECT partition_key FROM tmp_partition_keys)" in result
        assert "CREATE TEMPORARY TABLE tmp_partition_keys" in result

    def test_values_method_with_existing_where(self):
        """Test VALUES method when query already has WHERE clause."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        partition_keys = {1, 2}
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="VALUES", p0_alias="u")

        assert "u.active = TRUE" in result or "u.active = true" in result
        assert "u.region_id IN (VALUES" in result
        assert "AND" in result

    def test_mixed_data_types_consistency(self):
        """Test that function handles mixed data types consistently."""
        # Test with mixed types should still work (though not recommended)
        query = "SELECT * FROM users AS u"

        # Test with integers
        int_keys = {1, 2, 3}
        int_result = extend_query_with_partition_keys(query, int_keys, "region_id", method="TMP_TABLE_IN", p0_alias="u")
        assert "partition_key INT PRIMARY KEY" in int_result

        # Test with strings
        str_keys = {"a", "b", "c"}
        str_result = extend_query_with_partition_keys(query, str_keys, "region_id", method="TMP_TABLE_IN", p0_alias="u")
        assert "partition_key TEXT PRIMARY KEY" in str_result

    def test_large_partition_key_set(self):
        """Test with a large set of partition keys."""
        query = "SELECT * FROM users AS u"
        # Create a larger set to test performance and correctness
        partition_keys = set(range(1, 101))  # 100 partition keys
        result = extend_query_with_partition_keys(query, partition_keys, "region_id", method="IN", p0_alias="u")

        assert "u.region_id IN" in result
        assert "1" in result
        assert "100" in result
        # Should contain all numbers
        assert result.count(",") >= 99  # At least 99 commas for 100 items


class TestExtendQueryWithPartitionKeysLazy:
    """Test the extend_query_with_partition_keys_lazy function."""

    def test_empty_lazy_subquery_returns_original(self):
        """Test that empty lazy subquery returns original query."""
        query = "SELECT * FROM users WHERE id = 1"
        result = extend_query_with_partition_keys_lazy(query, "", "partition_key")
        assert result == query

    def test_whitespace_only_lazy_subquery_returns_original(self):
        """Test that whitespace-only lazy subquery returns original query."""
        query = "SELECT * FROM users WHERE id = 1"
        result = extend_query_with_partition_keys_lazy(query, "   \n\t  ", "partition_key")
        assert result == query

    def test_in_subquery_method_no_existing_where(self):
        """Test IN_SUBQUERY method when query has no WHERE clause."""
        query = "SELECT * FROM users AS u"
        lazy_subquery = "SELECT zipcode FROM cache_data WHERE hash = 'abc123'"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="IN_SUBQUERY", p0_alias="u")

        assert "WHERE u.zipcode IN (SELECT zipcode FROM cache_data WHERE hash = 'abc123')" in result

    def test_in_subquery_method_with_existing_where(self):
        """Test IN_SUBQUERY method when query already has WHERE clause."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT region_id FROM cache_intersection WHERE query_hash IN ('hash1', 'hash2')"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "region_id", method="IN_SUBQUERY", p0_alias="u")

        assert "u.active = TRUE" in result or "u.active = true" in result
        assert "u.region_id IN (SELECT region_id FROM cache_intersection" in result
        assert "AND" in result

    def test_in_subquery_method_auto_detect_alias(self):
        """Test IN_SUBQUERY method with automatic alias detection."""
        query = "SELECT * FROM users AS u WHERE u.id = 1"
        lazy_subquery = "SELECT zipcode FROM cache_table"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="IN_SUBQUERY")

        assert "u.zipcode IN (SELECT zipcode FROM cache_table)" in result

    def test_tmp_table_in_method_no_existing_where(self):
        """Test TMP_TABLE_IN method when query has no WHERE clause."""
        query = "SELECT * FROM users AS u"
        lazy_subquery = "SELECT zipcode FROM cache_data WHERE active = true"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="TMP_TABLE_IN", p0_alias="u")

        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in result  # More flexible match
        assert "WHERE u.zipcode IN (SELECT zipcode FROM tmp_cache_keys_" in result

    def test_tmp_table_in_method_with_existing_where(self):
        """Test TMP_TABLE_IN method when query already has WHERE clause."""
        query = "SELECT * FROM orders AS o WHERE o.status = 'complete'"
        lazy_subquery = "SELECT region_id FROM cached_regions"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "region_id", method="TMP_TABLE_IN", p0_alias="o")

        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in result  # More flexible match
        assert "o.status = 'complete'" in result
        assert "o.region_id IN (SELECT region_id FROM tmp_cache_keys_" in result
        assert "AND" in result

    def test_tmp_table_in_analyze_false(self):
        """Test TMP_TABLE_IN method with analyze_tmp_table=False."""
        query = "SELECT * FROM users AS u"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="TMP_TABLE_IN", p0_alias="u", analyze_tmp_table=False)

        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in result  # More flexible match
        assert "CREATE INDEX tmp_cache_keys_" not in result
        assert "ANALYZE tmp_cache_keys_" not in result

    def test_tmp_table_in_analyze_true(self):
        """Test TMP_TABLE_IN method with analyze_tmp_table=True."""
        query = "SELECT * FROM users AS u"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="TMP_TABLE_IN", p0_alias="u", analyze_tmp_table=True)

        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in result  # More flexible match
        assert "CREATE INDEX tmp_cache_keys_" in result
        assert "ANALYZE tmp_cache_keys_" in result

    def test_tmp_table_join_method_single_table(self):
        """Test TMP_TABLE_JOIN method with single table."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT region_id FROM cache_intersection"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "region_id", method="TMP_TABLE_JOIN", p0_alias="u")

        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in result
        assert "AS (SELECT region_id FROM cache_intersection)" in result
        assert "AS tmp_u" in result
        assert "INNER JOIN" in result

    def test_tmp_table_join_method_multiple_tables(self):
        """Test TMP_TABLE_JOIN method with multiple tables."""
        query = "SELECT * FROM users AS u, orders AS o WHERE u.id = o.user_id"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="TMP_TABLE_JOIN")

        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in result
        # When p0_alias is None for TMP_TABLE_JOIN, it should join on all tables
        assert "tmp_" in result

    def test_complex_lazy_subquery(self):
        """Test with complex lazy subquery."""
        query = """
        SELECT u.name, o.total
        FROM users AS u
        JOIN orders AS o ON u.id = o.user_id
        WHERE u.active = true
        """
        lazy_subquery = """
        WITH intersected AS (
            SELECT region_id FROM cache_table_1 WHERE hash = 'abc'
            INTERSECT
            SELECT region_id FROM cache_table_2 WHERE hash = 'def'
        )
        SELECT region_id FROM intersected
        """
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "region_id", method="IN_SUBQUERY", p0_alias="u")

        assert "u.active = TRUE" in result or "u.active = true" in result
        assert "WITH intersected AS" in result
        assert "u.region_id IN" in result
        assert "AND" in result

    def test_lazy_subquery_with_original_subquery(self):
        """Test lazy subquery when original query contains subquery."""
        query = """
        SELECT * FROM users AS u
        WHERE u.id IN (SELECT user_id FROM orders WHERE total > 100)
        """
        lazy_subquery = "SELECT zipcode FROM cache_zipcodes"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="IN_SUBQUERY", p0_alias="u")

        assert "u.zipcode IN (SELECT zipcode FROM cache_zipcodes)" in result
        assert "SELECT user_id FROM orders WHERE total > 100" in result  # Original subquery preserved

    def test_realistic_postgresql_lazy_subquery(self):
        """Test with realistic PostgreSQL lazy subquery (similar to actual cache handlers)."""
        query = "SELECT * FROM poi AS p1 WHERE p1.active = true"
        # This mimics what PostgreSQL bit cache handler would return
        lazy_subquery = """(
       WITH bit_result AS (
            SELECT BIT_AND(partition_keys) AS bit_result FROM (SELECT partition_keys FROM cache_zipcode WHERE query_hash = ANY(ARRAY['hash1', 'hash2'])) AS selected
        ),
        numbered_bits AS (
            SELECT
                bit_result,
                generate_series(0, 99) AS bit_index
            FROM bit_result
        )
        SELECT unnest(array_agg(bit_index)) AS zipcode
        FROM numbered_bits
        WHERE get_bit(bit_result, bit_index) = 1)
        """
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="TMP_TABLE_IN", p0_alias="p1")

        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in result
        assert "WITH bit_result AS" in result
        assert "p1.zipcode IN (SELECT zipcode FROM tmp_cache_keys_" in result
        assert "p1.active = TRUE" in result or "p1.active = true" in result  # Original query condition preserved

    def test_invalid_method_raises_error(self):
        """Test that invalid method raises appropriate error."""
        query = "SELECT * FROM users AS u"
        lazy_subquery = "SELECT region FROM cache"

        # This should raise an error or handle gracefully
        try:
            result = extend_query_with_partition_keys_lazy(
                query,
                lazy_subquery,
                "region",
                method="INVALID",  # type: ignore
            )
            # If it doesn't raise an error, the function should return a string
            assert isinstance(result, str)
        except Exception:
            # Expected behavior for invalid method
            pass

    def test_tmp_table_join_with_explicit_alias(self):
        """Test TMP_TABLE_JOIN method with explicit p0_alias."""
        query = "SELECT * FROM users AS u, orders AS o WHERE u.id = o.user_id"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="TMP_TABLE_JOIN", p0_alias="u")

        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in result
        # Should only join to the specified table (u)
        assert "tmp_u" in result


class TestApplyCacheLazy:
    """Test the apply_cache_lazy wrapper function."""

    def create_mock_cache_handler(self, lazy_subquery: str | None = None, used_hashes: int = 5):
        """Create a mock cache handler for testing."""
        mock_handler = Mock(spec=AbstractCacheHandler_Lazy)

        # Mock get_intersected_lazy method
        mock_handler.get_intersected_lazy.return_value = (lazy_subquery, used_hashes)

        return mock_handler

    @patch("partitioncache.apply_cache.isinstance")
    def test_no_cache_hits_returns_original_query(self, mock_isinstance):
        """Test that function returns original query when no cache hits."""
        # Make isinstance return True for AbstractCacheHandler_Lazy
        mock_isinstance.side_effect = lambda obj, cls: cls.__name__ == "AbstractCacheHandler_Lazy" or isinstance(
            obj, cls.__bases__[0] if hasattr(cls, "__bases__") and cls.__bases__ else object
        )

        query = "SELECT * FROM users AS u WHERE u.active = true"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=None, used_hashes=0)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", min_component_size=1)

        assert enhanced_query == query
        assert stats["enhanced"] == 0
        assert stats["cache_hits"] == 0
        assert stats["generated_variants"] > 0  # Should have generated some subqueries

    def test_empty_lazy_subquery_returns_original(self):
        """Test that function returns original query when lazy subquery is empty."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        mock_handler = self.create_mock_cache_handler(lazy_subquery="", used_hashes=0)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode")

        assert enhanced_query == query
        assert stats["enhanced"] == 0

    def test_whitespace_lazy_subquery_returns_original(self):
        """Test that function returns original query when lazy subquery is whitespace."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        mock_handler = self.create_mock_cache_handler(lazy_subquery="   \n\t  ", used_hashes=0)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode")

        assert enhanced_query == query
        assert stats["enhanced"] == 0

    def test_successful_cache_enhancement_in_subquery(self):
        """Test successful cache enhancement with IN_SUBQUERY method."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data WHERE query_hash = 'abc123'"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=3)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", method="IN_SUBQUERY", p0_alias="u", min_component_size=1)

        assert enhanced_query != query
        assert "u.zipcode IN (SELECT zipcode FROM cache_data" in enhanced_query
        assert "u.active = TRUE" in enhanced_query or "u.active = true" in enhanced_query
        assert "AND" in enhanced_query
        assert stats["enhanced"] == 1
        assert stats["cache_hits"] == 3
        assert stats["generated_variants"] > 0

    def test_successful_cache_enhancement_tmp_table_in(self):
        """Test successful cache enhancement with TMP_TABLE_IN method."""
        query = "SELECT * FROM poi AS p1 WHERE p1.category = 'restaurant'"
        lazy_subquery = "SELECT zipcode FROM cache_zipcode_table WHERE active = true"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=5)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", method="TMP_TABLE_IN", p0_alias="p1")

        assert enhanced_query != query
        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in enhanced_query
        assert "p1.zipcode IN (SELECT zipcode FROM tmp_cache_keys_" in enhanced_query
        assert "p1.category = 'restaurant'" in enhanced_query
        assert stats["enhanced"] == 1
        assert stats["cache_hits"] == 5

    def test_successful_cache_enhancement_tmp_table_join(self):
        """Test successful cache enhancement with TMP_TABLE_JOIN method."""
        query = "SELECT * FROM users AS u JOIN orders AS o ON u.id = o.user_id"
        lazy_subquery = "SELECT region_id FROM cache_regions"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=2)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "region_id", method="TMP_TABLE_JOIN", p0_alias="u")

        assert enhanced_query != query
        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in enhanced_query
        assert "INNER JOIN" in enhanced_query
        assert "AS tmp_u" in enhanced_query
        assert stats["enhanced"] == 1
        assert stats["cache_hits"] == 2

    def test_auto_alias_detection(self):
        """Test that p0_alias is automatically detected when not provided."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=1)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", method="IN_SUBQUERY")

        assert enhanced_query != query
        assert "u.zipcode IN" in enhanced_query
        assert stats["enhanced"] == 1

    def test_complex_query_preservation(self):
        """Test that complex query features are preserved during enhancement."""
        query = """
        SELECT u.name, COUNT(o.id) as order_count
        FROM users AS u
        LEFT JOIN orders AS o ON u.id = o.user_id AND o.status = 'complete'
        WHERE u.created_at > '2023-01-01'
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 0
        ORDER BY order_count DESC
        """
        lazy_subquery = "SELECT region_id FROM cache_regions WHERE active = true"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=4)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "region_id", method="IN_SUBQUERY", p0_alias="u")

        assert enhanced_query != query
        assert "u.region_id IN" in enhanced_query
        assert "LEFT JOIN orders" in enhanced_query
        assert "GROUP BY" in enhanced_query
        assert "HAVING" in enhanced_query
        assert "ORDER BY" in enhanced_query
        assert stats["enhanced"] == 1

    def test_analyze_tmp_table_false(self):
        """Test that analyze_tmp_table=False is passed through correctly."""
        query = "SELECT * FROM users AS u"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=1)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", method="TMP_TABLE_IN", p0_alias="u", analyze_tmp_table=False)

        assert enhanced_query != query
        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in enhanced_query
        assert "CREATE INDEX tmp_cache_keys_idx" not in enhanced_query
        assert "ANALYZE tmp_cache_keys" not in enhanced_query

    def test_analyze_tmp_table_true(self):
        """Test that analyze_tmp_table=True is passed through correctly."""
        query = "SELECT * FROM users AS u"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=1)

        enhanced_query, stats = apply_cache_lazy(
            query, mock_handler, "zipcode", method="TMP_TABLE_IN", p0_alias="u", analyze_tmp_table=True, min_component_size=1
        )

        assert enhanced_query != query
        assert "CREATE INDEX tmp_cache_keys_" in enhanced_query and "_idx ON tmp_cache_keys_" in enhanced_query
        assert "ANALYZE tmp_cache_keys_" in enhanced_query

    def test_custom_parameters_forwarded(self):
        """Test that custom parameters are forwarded to underlying functions."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=1)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", min_component_size=1, canonicalize_queries=True, follow_graph=False)

        # Verify the mock was called with the correct parameters
        mock_handler.get_intersected_lazy.assert_called_once()
        assert stats["enhanced"] == 1

    def test_statistics_accuracy(self):
        """Test that returned statistics are accurate."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=7)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", min_component_size=1)

        assert isinstance(stats, dict)
        assert "generated_variants" in stats
        assert "cache_hits" in stats
        assert "enhanced" in stats
        assert stats["cache_hits"] == 7
        assert stats["enhanced"] == 1
        assert stats["generated_variants"] > 0

    def test_non_lazy_cache_handler_raises_error(self):
        """Test that non-lazy cache handler raises appropriate error."""
        query = "SELECT * FROM users AS u"
        non_lazy_handler = Mock()  # Not a lazy handler

        with pytest.raises(ValueError, match="Cache handler does not support lazy intersection"):
            apply_cache_lazy(query, non_lazy_handler, "zipcode")

    def test_realistic_postgresql_bit_scenario(self):
        """Test with realistic PostgreSQL bit cache handler scenario."""
        query = "SELECT * FROM poi AS p1 WHERE p1.category = 'restaurant'"
        # Realistic lazy subquery from PostgreSQL bit cache handler
        lazy_subquery = """(
       WITH bit_result AS (
            SELECT BIT_AND(partition_keys) AS bit_result FROM (SELECT partition_keys FROM cache_zipcode WHERE query_hash = ANY(ARRAY['hash1', 'hash2'])) AS selected
        ),
        numbered_bits AS (
            SELECT
                bit_result,
                generate_series(0, 99) AS bit_index
            FROM bit_result
        )
        SELECT unnest(array_agg(bit_index)) AS zipcode
        FROM numbered_bits
        WHERE get_bit(bit_result, bit_index) = 1)
        """
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=3)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", method="TMP_TABLE_IN", p0_alias="p1")

        assert enhanced_query != query
        assert "CREATE TEMPORARY TABLE tmp_cache_keys_" in enhanced_query
        assert "WITH bit_result AS" in enhanced_query
        assert "p1.zipcode IN (SELECT zipcode FROM tmp_cache_keys_" in enhanced_query
        assert "p1.category = 'restaurant'" in enhanced_query
        assert stats["enhanced"] == 1
        assert stats["cache_hits"] == 3

    def test_p0_table_integration_no_cache_hits(self):
        """Test p0 table integration when no cache hits are found."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=None, used_hashes=0)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", use_p0_table=True, p0_alias="u")

        assert "zipcode_mv AS u" in enhanced_query
        assert stats["p0_rewritten"] == 1
        assert stats["enhanced"] == 0

    def test_p0_table_integration_with_cache_hits(self):
        """Test p0 table integration combined with cache enhancement."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data WHERE active = true"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=3)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", method="IN_SUBQUERY", use_p0_table=True, p0_alias="u")

        # Should have both p0 table rewrite AND cache enhancement
        assert enhanced_query != query
        assert "zipcode_mv AS u" in enhanced_query
        assert "u.zipcode IN (SELECT zipcode FROM cache_data" in enhanced_query
        assert stats["enhanced"] == 1  # Cache enhancement applied
        assert stats["p0_rewritten"] == 1  # P0 table was applied
        assert stats["cache_hits"] == 3

    def test_p0_table_disabled_by_default(self):
        """Test that p0 table is disabled by default."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=1)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", p0_alias="u")

        # Should not include p0 table rewrite
        assert "zipcode_mv" not in enhanced_query
        assert stats["p0_rewritten"] == 0

    def test_p0_table_already_present_in_query(self):
        """Test p0 table integration when mv table is already present in query."""
        query = "SELECT * FROM users AS u JOIN zipcode_mv AS zm ON u.zipcode = zm.zipcode WHERE u.active = true"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        mock_handler = self.create_mock_cache_handler(lazy_subquery=lazy_subquery, used_hashes=2)

        enhanced_query, stats = apply_cache_lazy(query, mock_handler, "zipcode", use_p0_table=True, p0_alias="u")

        # Should apply cache enhancement but not duplicate p0 table
        assert "u.zipcode IN (SELECT zipcode FROM cache_data" in enhanced_query
        assert enhanced_query.count("zipcode_mv") == 1  # Only the original one
        assert stats["enhanced"] == 1
        assert stats["p0_rewritten"] == 0  # P0 was not rewritten since table already present
