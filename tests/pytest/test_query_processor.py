from partitioncache.query_processor import (
    extract_conjunctive_conditions,
    normalize_distance_conditions,
    generate_partial_queries,
    clean_query,
)


def test_normalize_distance_conditions_between():
    # Test case: Lower bound and upper bound
    query = "SELECT * FROM table WHERE distance BETWEEN 1.6 AND 3.6"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN 1 AND 4"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case: Lower bound and upper bound but no fdist fuction
    query = "SELECT * FROM table WHERE distance BETWEEN 1.6 AND 3.6"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN 1.6 AND 3.6"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=True) == expected_result

    # Test case: Lower bound and upper bound based on dist function
    query = "SELECT * FROM table AS a, table AS b WHERE DIST(a, b) BETWEEN 1.6 AND 3.6"
    expected_result = "SELECT * FROM table AS a, table AS b WHERE DIST(a, b) BETWEEN 1 AND 4"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case: Lower bound and upper bound are floats
    query = "SELECT * FROM table WHERE distance BETWEEN 1.6 AND 3.6"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN 1.5 AND 4"
    assert normalize_distance_conditions(query, bucket_steps=0.5, restrict_to_dist_functions=False) == expected_result

    # Test case: Lower bound and upper bound are negative integers (ignore)
    query = "SELECT * FROM table WHERE distance BETWEEN -3 AND -1"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN -3 AND -1"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case: Lower bound and upper bound are negative floats (ignore)
    query = "SELECT * FROM table WHERE distance BETWEEN -3.6 AND -1.6"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN -3.6 AND -1.6"
    assert normalize_distance_conditions(query, bucket_steps=0.5, restrict_to_dist_functions=False) == expected_result

    # Test case: Lower bound and upper bound are zero
    query = "SELECT * FROM table WHERE distance BETWEEN 0 AND 0"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN 0 AND 0"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case: Lower bound and upper bound are the same
    query = "SELECT * FROM table WHERE distance BETWEEN 2 AND 2"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN 2 AND 2"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case: Lower bound and upper bound are already normalized
    query = "SELECT * FROM table WHERE distance BETWEEN 1 AND 4"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN 1 AND 4"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case: Lower bound and upper bound are already normalized
    query = "SELECT * FROM table WHERE distance BETWEEN 1.5 AND 4"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN 1.5 AND 4"
    assert normalize_distance_conditions(query, bucket_steps=0.5, restrict_to_dist_functions=False) == expected_result

    # Test Additional conditions
    query = "SELECT * FROM table WHERE distance BETWEEN 1.5 AND 4 AND attribute = 'value' AND attribute2 > 0.1"
    expected_result = "SELECT * FROM table WHERE distance BETWEEN 1.5 AND 4 AND attribute = 'value' AND attribute2 > 0.1"
    assert normalize_distance_conditions(query) == expected_result


def test_normalize_distance_conditions():
    ## Testing whithout BETWEEN

    # Test case 9: Lower bound and upper bound are integers
    query = "SELECT * FROM table WHERE distance > 1.6 AND distance < 3.6"
    expected_result = "SELECT * FROM table WHERE distance > 1 AND distance < 4"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case 10: Table name contains distance
    query = "SELECT * FROM table_distance WHERE distance > 1.6 AND distance < 3.6"
    expected_result = "SELECT * FROM table_distance WHERE distance > 1 AND distance < 4"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case 11: Table has alias
    query = "SELECT * FROM table_distance AS td WHERE td.distance > 1.6 AND td.distance < 3.6"
    expected_result = "SELECT * FROM table_distance AS td WHERE td.distance > 1 AND td.distance < 4"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case 12: Multiple smaller than
    query = "SELECT * FROM table_distance AS td WHERE td.distance > 1.6 AND td.distance > 3.6"
    expected_result = "SELECT * FROM table_distance AS td WHERE td.distance > 1 AND td.distance > 3"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case 13: Multiple larger than
    query = "SELECT * FROM table_distance AS td WHERE td.distance < 1.6 AND td.distance < 3.6"
    expected_result = "SELECT * FROM table_distance AS td WHERE td.distance < 2 AND td.distance < 4"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=False) == expected_result

    # Test case: Multiple larger than
    query = "SELECT * FROM table_distance AS td WHERE td.distance < 1.6 AND td.distance < 3.6"
    expected_result = "SELECT * FROM table_distance AS td WHERE td.distance < 1.6 AND td.distance < 3.6"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=True) == expected_result

    # Test case 14: Multiple tables with distance to each other based on dist() function
    query = "SELECT * FROM table_distance AS td, td2 WHERE DIST(td.geom, td2.geom) > 1.6 AND DIST(td.geom, td2.geom) < 3.6"
    expected_result = "SELECT * FROM table_distance AS td, td2 WHERE DIST(td.geom, td2.geom) > 1 AND DIST(td.geom, td2.geom) < 4"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=True) == expected_result

    # Test case 15: Multiple tables with distance to each other based on ST_Distance function
    query = "SELECT * FROM table_distance AS td, td2  WHERE ST_DISTANCE(td.geom, td2.geom) > 1.6 AND ST_DISTANCE(td.geom, td2.geom) < 3.6"
    expected_result = "SELECT * FROM table_distance AS td, td2 WHERE ST_DISTANCE(td.geom, td2.geom) > 1 AND ST_DISTANCE(td.geom, td2.geom) < 4"
    assert normalize_distance_conditions(query, restrict_to_dist_functions=True) == expected_result

    # Test case 16: Multiple tables with distance to each other based on manually calculated distance
    query = "SELECT * FROM table_distance AS td, td2  WHERE SQRT((td.x - td2.x) ^ 2 + (td.y - td2.y) ^ 2) > 1.6 AND SQRT((td.x - td2.x) ^ 2 + (td.y - td2.y) ^ 2) < 3.6"
    expected_result = (
        "SELECT * FROM table_distance AS td, td2 WHERE SQRT((td.x - td2.x) ^ 2 + (td.y - td2.y) ^ 2) > 1 AND SQRT((td.x - td2.x) ^ 2 + (td.y - td2.y) ^ 2) < 4"
    )

    assert normalize_distance_conditions(query, restrict_to_dist_functions=True) == expected_result


def test_subquery_builder_2_tables():
    query = "SELECT DISTINCT * FROM table AS t1, table AS t2 WHERE t1.search_space = t2.search_space AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.attribute = 'value' AND t2.attribute2 > 0.1"
    partition_key = "search_space"
    min_component_size = 1
    follow_graph = True
    fix_attributes = True

    expected_result = [
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute2 > 0.1",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute = 'value'",
    ]

    assert sorted(generate_partial_queries(query, partition_key, min_component_size, follow_graph, fix_attributes)) == sorted(expected_result)


def test_subquery_builder_2_tables_with_1subquery():
    query = "SELECT DISTINCT * FROM table AS t1, table AS t2 WHERE t1.search_space = t2.search_space AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.attribute = 'value' AND t2.attribute2 > 0.1 AND t1.search_space IN (SELECT kk FROM t4)"
    partition_key = "search_space"
    min_component_size = 1
    follow_graph = True
    fix_attributes = True

    expected_result = [
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute2 > 0.1",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute = 'value'",
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space AND t1.search_space IN (SELECT kk FROM t4)",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute2 > 0.1 AND t1.search_space IN (SELECT kk FROM t4)",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute = 'value' AND t1.search_space IN (SELECT kk FROM t4)",
        "SELECT kk FROM t4",
    ]
    assert sorted(generate_partial_queries(query, partition_key, min_component_size, follow_graph, fix_attributes)) == sorted(expected_result)


def test_subquery_builder_2_tables_with_1subquery_aliasdiff():
    query = "SELECT DISTINCT * FROM table AS t1, table AS t2 WHERE t1.search_space = t2.search_space AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.attribute = 'value' AND t2.attribute2 > 0.1 AND t2.search_space IN (SELECT kk FROM t4)"
    partition_key = "search_space"
    min_component_size = 1
    follow_graph = True
    fix_attributes = True

    expected_result = [
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute2 > 0.1",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute = 'value'",
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space AND t1.search_space IN (SELECT kk FROM t4)",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute2 > 0.1 AND t1.search_space IN (SELECT kk FROM t4)",
        "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute = 'value' AND t1.search_space IN (SELECT kk FROM t4)",
        "SELECT kk FROM t4",
    ]

    assert sorted(generate_partial_queries(query, partition_key, min_component_size, follow_graph, fix_attributes)) == sorted(expected_result)


def test_subquery_builder_2_tables_with_2subquery_aliasdiff():
    query = "SELECT DISTINCT * FROM table AS t1, table AS t2 WHERE t1.search_space = t2.search_space AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space IN (VALUES (1), (2), (3)) AND t1.attribute = 'value' AND t2.attribute2 > 0.1 AND t2.search_space IN (SELECT kk FROM t4)"
    partition_key = "search_space"
    min_component_size = 1
    follow_graph = True
    fix_attributes = True

    expected_result = sorted(
        [
            "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space",
            "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute2 > 0.1",
            "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute = 'value'",
            "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space AND t1.search_space IN (VALUES (1), (2), (3))",
            "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute2 > 0.1 AND t1.search_space IN (VALUES (1), (2), (3))",
            "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute = 'value' AND t1.search_space IN (VALUES (1), (2), (3))",
            "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space AND t1.search_space IN (SELECT kk FROM t4)",
            "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute2 > 0.1 AND t1.search_space IN (SELECT kk FROM t4)",
            "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute = 'value' AND t1.search_space IN (SELECT kk FROM t4)",
            "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space AND t1.search_space IN (VALUES (1), (2), (3)) AND t1.search_space IN (SELECT kk FROM t4)",
            "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute2 > 0.1 AND t1.search_space IN (VALUES (1), (2), (3)) AND t1.search_space IN (SELECT kk FROM t4)",
            "SELECT DISTINCT t1.search_space FROM table AS t1 WHERE t1.attribute = 'value' AND t1.search_space IN (VALUES (1), (2), (3)) AND t1.search_space IN (SELECT kk FROM t4)",
            "SELECT kk FROM t4",
            "VALUES (1), (2), (3)",
        ]
    )
    x = sorted(generate_partial_queries(query, partition_key, min_component_size, follow_graph, fix_attributes))
    assert x == expected_result


def test_subquery_builder_3_tables():
    query = "SELECT DISTINCT * FROM table AS t1, table AS t2, table AS t3 WHERE t1.search_space = t2.search_space AND t1.search_space = t3.search_space AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND DIST(t1.g, t3.g) BETWEEN 8.6 AND 9.6 AND DIST(t2.g, t3.g) BETWEEN 1.6 AND 3.6 AND t1.attribute = 'value' AND t2.attribute2 > 0.1 AND t3.attribute3 < 0.5"
    partition_key = "search_space"
    min_component_size = 2
    follow_graph = True
    fix_attributes = True

    expected_result = [
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute2 > 0.1 AND t2.attribute3 < 0.5 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space",
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space",
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute3 < 0.5 AND DIST(t1.g, t2.g) BETWEEN 8.6 AND 9.6 AND t1.search_space = t2.search_space",
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2, table AS t3 WHERE t1.attribute = 'value' AND t2.attribute2 > 0.1 AND t3.attribute3 < 0.5 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND DIST(t1.g, t3.g) BETWEEN 8.6 AND 9.6 AND DIST(t2.g, t3.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space AND t1.search_space = t3.search_space AND t2.search_space = t3.search_space",
    ]
    assert sorted(generate_partial_queries(query, partition_key, min_component_size, follow_graph, fix_attributes)) == sorted(expected_result)


def test_subquery_builder_3_tables_connected_only():
    query = "SELECT DISTINCT * FROM table AS t1, table AS t2, table AS t3 WHERE t1.search_space = t2.search_space AND t1.search_space = t3.search_space AND DIST(t1.g, t3.g) BETWEEN 1.6 AND 3.6 AND t1.attribute = 'value' AND t2.attribute2 > 0.1 AND t3.attribute3 < 0.5"
    partition_key = "search_space"
    min_component_size = 2
    follow_graph = True
    fix_attributes = True

    expected_result = [
        "SELECT DISTINCT t1.search_space FROM table AS t1, table AS t2 WHERE t1.attribute = 'value' AND t2.attribute3 < 0.5 AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.search_space = t2.search_space"
    ]

    assert sorted(generate_partial_queries(query, partition_key, min_component_size, follow_graph, fix_attributes)) == sorted(expected_result)


def test_extract_conditions():
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

    # Test case: condition with OR
    query = "SELECT * FROM table AS t1, tables as t3 WHERE (t1.attribute = 'value' OR t3.attribute2 > 0.1) AND t1.attribute3 < 0.5"
    expected_result = [
        "(t1.attribute = 'value' OR t3.attribute2 > 0.1)",
        "t1.attribute3 < 0.5",
    ]
    assert extract_conjunctive_conditions(query) == expected_result

    # Test case: Subquery in WHERE clause
    query = "SELECT * FROM table WHERE table.a = 'value' AND table.b IN (SELECT attribute FROM table2 WHERE attribute2 = 'value')"
    expected_result = [
        "table.a = 'value'",
        "table.b IN (SELECT attribute FROM table2 WHERE attribute2 = 'value')",
    ]
    assert extract_conjunctive_conditions(query) == expected_result


def test_clean_query_semicolon_handling():
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

    # Test semicolon with whitespace
    query_semicolon_whitespace = "SELECT * FROM users WHERE id = 1  ;  "
    cleaned_whitespace = clean_query(query_semicolon_whitespace)
    assert cleaned_whitespace == cleaned_without
    assert ";" not in cleaned_whitespace

    # Test complex query with semicolon
    complex_query = "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true;"
    cleaned_complex = clean_query(complex_query)
    assert ";" not in cleaned_complex
    assert "SELECT" in cleaned_complex  # Query should still be valid
