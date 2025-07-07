"""
Test cases for the empty WHERE clause bug fix in query_processor.py

This test ensures that query fragment generation never produces malformed SQL
with incomplete WHERE clauses.
"""

from partitioncache.query_processor import generate_all_query_hash_pairs


class TestEmptyWhereClauseFix:
    """Test that query fragment generation handles empty WHERE clauses correctly."""

    def test_simple_select_no_where_conditions(self):
        """Test that a simple SELECT without WHERE conditions generates valid SQL."""
        query = "SELECT DISTINCT t1.complex_data_id FROM data_points AS t1"
        partition_key = "complex_data_id"

        query_hash_pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key=partition_key,
            min_component_size=1,
            follow_graph=False,
            keep_all_attributes=True
        )

        # Verify we get some fragments
        assert len(query_hash_pairs) > 0

        # Check that no fragment ends with "WHERE " (incomplete WHERE clause)
        for query_fragment, _ in query_hash_pairs:
            assert not query_fragment.rstrip().endswith("WHERE"), f"Fragment has incomplete WHERE clause: {query_fragment}"

        # Check that fragments are valid SQL (basic validation)
        for query_fragment, _ in query_hash_pairs:
            # Should not contain " WHERE " without conditions following
            if " WHERE " in query_fragment:
                where_index = query_fragment.find(" WHERE ")
                remainder = query_fragment[where_index + 7:].strip()
                assert remainder, f"WHERE clause is empty in fragment: {query_fragment}"

    def test_select_with_join_no_attribute_conditions(self):
        """Test SELECT with JOINs but no attribute conditions generates valid SQL."""
        query = """
        SELECT DISTINCT t1.partition_key
        FROM table1 AS t1
        JOIN table2 AS t2 ON t1.partition_key = t2.partition_key
        """
        partition_key = "partition_key"

        query_hash_pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key=partition_key,
            min_component_size=1,
            follow_graph=False,
            keep_all_attributes=True
        )

        # Verify we get some fragments
        assert len(query_hash_pairs) > 0

        # Check that no fragment ends with incomplete WHERE clause
        for query_fragment, _ in query_hash_pairs:
            assert not query_fragment.rstrip().endswith("WHERE"), f"Fragment has incomplete WHERE clause: {query_fragment}"

    def test_select_with_existing_where_clause(self):
        """Test that existing WHERE clauses are preserved correctly."""
        query = """
        SELECT DISTINCT t1.partition_key
        FROM table1 AS t1
        WHERE t1.status = 'active'
        """
        partition_key = "partition_key"

        query_hash_pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key=partition_key,
            min_component_size=1,
            follow_graph=False,
            keep_all_attributes=True
        )

        # Verify we get some fragments
        assert len(query_hash_pairs) > 0

        # Check that fragments with WHERE clauses have proper conditions
        for query_fragment, _ in query_hash_pairs:
            if " WHERE " in query_fragment:
                where_index = query_fragment.find(" WHERE ")
                remainder = query_fragment[where_index + 7:].strip()
                assert remainder, f"WHERE clause is empty in fragment: {query_fragment}"

    def test_complex_multi_table_query_no_conditions(self):
        """Test complex multi-table query without attribute conditions."""
        query = """
        SELECT DISTINCT t1.user_id
        FROM users AS t1
        JOIN orders AS t2 ON t1.user_id = t2.user_id
        JOIN products AS t3 ON t2.product_id = t3.product_id
        """
        partition_key = "user_id"

        query_hash_pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key=partition_key,
            min_component_size=1,
            follow_graph=True,
            keep_all_attributes=True
        )

        # Verify we get fragments
        assert len(query_hash_pairs) > 0

        # Check that no fragment has incomplete WHERE clause
        for query_fragment, _ in query_hash_pairs:
            assert not query_fragment.rstrip().endswith("WHERE"), f"Fragment has incomplete WHERE clause: {query_fragment}"

            # If WHERE exists, it should have conditions
            if " WHERE " in query_fragment:
                where_index = query_fragment.find(" WHERE ")
                remainder = query_fragment[where_index + 7:].strip()
                assert remainder, f"WHERE clause is empty in fragment: {query_fragment}"

    def test_star_join_scenario_no_conditions(self):
        """Test star-join scenario without attribute conditions."""
        query = """
        SELECT DISTINCT t1.region_id
        FROM regions AS t1
        JOIN cities AS t2 ON t1.region_id = t2.region_id
        JOIN stores AS t3 ON t2.city_id = t3.city_id
        """
        partition_key = "region_id"

        query_hash_pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key=partition_key,
            min_component_size=1,
            follow_graph=True,
            keep_all_attributes=True,
            auto_detect_star_join=True
        )

        # Check all generated fragments
        for query_fragment, _ in query_hash_pairs:
            # Must not end with incomplete WHERE
            assert not query_fragment.rstrip().endswith("WHERE"), f"Fragment has incomplete WHERE clause: {query_fragment}"

            # If WHERE exists, must have conditions
            if " WHERE " in query_fragment:
                where_index = query_fragment.find(" WHERE ")
                remainder = query_fragment[where_index + 7:].strip()
                assert remainder, f"WHERE clause is empty in fragment: {query_fragment}"

    def test_edge_case_empty_string_conditions(self):
        """Test edge case where conditions might be empty strings."""
        # This tests the internal logic that might create empty condition lists
        query = "SELECT t1.id FROM simple_table AS t1"
        partition_key = "id"

        query_hash_pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key=partition_key,
            min_component_size=1,
            follow_graph=False,
            keep_all_attributes=True
        )

        # Should generate at least one fragment (the original table)
        assert len(query_hash_pairs) > 0

        # All fragments must be valid SQL without incomplete WHERE
        for query_fragment, _ in query_hash_pairs:
            assert not query_fragment.rstrip().endswith("WHERE"), f"Fragment has incomplete WHERE clause: {query_fragment}"

    def test_fragments_are_valid_sql_structure(self):
        """Test that all generated fragments have valid SQL structure."""
        query = "SELECT DISTINCT t1.partition_key FROM test_table AS t1"
        partition_key = "partition_key"

        query_hash_pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key=partition_key,
            min_component_size=1,
            follow_graph=False,
            keep_all_attributes=True
        )

        for query_fragment, _ in query_hash_pairs:
            # Basic SQL structure validation
            assert query_fragment.strip().upper().startswith("SELECT"), f"Fragment doesn't start with SELECT: {query_fragment}"
            assert " FROM " in query_fragment.upper(), f"Fragment missing FROM clause: {query_fragment}"

            # WHERE clause validation
            if " WHERE " in query_fragment.upper():
                where_parts = query_fragment.upper().split(" WHERE ")
                assert len(where_parts) == 2, f"Multiple WHERE clauses in fragment: {query_fragment}"
                where_conditions = where_parts[1].strip()
                assert where_conditions, f"Empty WHERE conditions in fragment: {query_fragment}"
