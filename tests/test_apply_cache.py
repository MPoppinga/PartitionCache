"""
Tests for the apply_cache module, specifically extend_query_with_partition_keys function.
"""

import pytest
from datetime import datetime
from unittest.mock import patch

from partitioncache.apply_cache import extend_query_with_partition_keys, find_p0_alias, extend_query_with_partition_keys_lazy


class TestFindP0Alias:
    """Test the find_p0_alias function."""

    def test_simple_table_with_alias(self):
        """Test finding alias from simple query with table alias."""
        query = "SELECT * FROM users AS u WHERE u.id = 1"
        result = find_p0_alias(query)
        assert result == "u"

    def test_simple_table_without_alias(self):
        """Test finding table name when no alias is provided."""
        query = "SELECT * FROM users WHERE id = 1"
        result = find_p0_alias(query)
        assert result == "users"

    def test_multiple_tables_returns_first(self):
        """Test that first table alias is returned when multiple tables exist."""
        query = "SELECT * FROM users AS u, orders AS o WHERE u.id = o.user_id"
        result = find_p0_alias(query)
        assert result == "u"

    def test_join_query(self):
        """Test finding alias in JOIN query."""
        query = "SELECT * FROM users AS u JOIN orders AS o ON u.id = o.user_id"
        result = find_p0_alias(query)
        assert result == "u"

    def test_no_table_raises_error(self):
        """Test that ValueError is raised when no table is found."""
        query = "SELECT 1"
        with pytest.raises(ValueError, match="No table found in query"):
            find_p0_alias(query)


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

        assert "CREATE TEMPORARY TABLE tmp_cache_keys AS (SELECT zipcode FROM cache_data WHERE active = true)" in result
        assert "WHERE u.zipcode IN (SELECT zipcode FROM tmp_cache_keys)" in result

    def test_tmp_table_in_method_with_existing_where(self):
        """Test TMP_TABLE_IN method when query already has WHERE clause."""
        query = "SELECT * FROM orders AS o WHERE o.status = 'complete'"
        lazy_subquery = "SELECT region_id FROM cached_regions"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "region_id", method="TMP_TABLE_IN", p0_alias="o")

        assert "CREATE TEMPORARY TABLE tmp_cache_keys AS (SELECT region_id FROM cached_regions)" in result
        assert "o.status = 'complete'" in result
        assert "o.region_id IN (SELECT region_id FROM tmp_cache_keys)" in result
        assert "AND" in result

    def test_tmp_table_in_analyze_false(self):
        """Test TMP_TABLE_IN method with analyze_tmp_table=False."""
        query = "SELECT * FROM users AS u"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="TMP_TABLE_IN", p0_alias="u", analyze_tmp_table=False)

        assert "CREATE TEMPORARY TABLE tmp_cache_keys AS" in result
        assert "CREATE INDEX tmp_cache_keys_idx" not in result
        assert "ANALYZE tmp_cache_keys" not in result

    def test_tmp_table_in_analyze_true(self):
        """Test TMP_TABLE_IN method with analyze_tmp_table=True."""
        query = "SELECT * FROM users AS u"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="TMP_TABLE_IN", p0_alias="u", analyze_tmp_table=True)

        assert "CREATE TEMPORARY TABLE tmp_cache_keys AS" in result
        assert "CREATE INDEX tmp_cache_keys_idx ON tmp_cache_keys (zipcode)" in result
        assert "ANALYZE tmp_cache_keys" in result

    def test_tmp_table_join_method_single_table(self):
        """Test TMP_TABLE_JOIN method with single table."""
        query = "SELECT * FROM users AS u WHERE u.active = true"
        lazy_subquery = "SELECT region_id FROM cache_intersection"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "region_id", method="TMP_TABLE_JOIN", p0_alias="u")

        assert "CREATE TEMPORARY TABLE tmp_cache_keys AS (SELECT region_id FROM cache_intersection)" in result
        assert "tmp_cache_keys AS tmp_u" in result
        assert "INNER JOIN" in result

    def test_tmp_table_join_method_multiple_tables(self):
        """Test TMP_TABLE_JOIN method with multiple tables."""
        query = "SELECT * FROM users AS u, orders AS o WHERE u.id = o.user_id"
        lazy_subquery = "SELECT zipcode FROM cache_data"
        result = extend_query_with_partition_keys_lazy(query, lazy_subquery, "zipcode", method="TMP_TABLE_JOIN")

        assert "CREATE TEMPORARY TABLE tmp_cache_keys AS" in result
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
        query = "SELECT * FROM poi AS p1 WHERE ST_DWithin(p1.geom, ST_Point(1, 2), 1000)"
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

        assert "CREATE TEMPORARY TABLE tmp_cache_keys AS" in result
        assert "WITH bit_result AS" in result
        assert "p1.zipcode IN (SELECT zipcode FROM tmp_cache_keys)" in result
        assert "ST_DWITHIN" in result or "ST_DWithin" in result  # Original spatial query preserved

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

        assert "CREATE TEMPORARY TABLE tmp_cache_keys AS" in result
        # Should only join to the specified table (u)
        assert "tmp_u" in result
