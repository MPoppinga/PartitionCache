from partitioncache.query_processor import extract_and_group_query_conditions


def test_extract_query_conditions():
    query = "SELECT * FROM table AS t1, table AS t2 WHERE t1.search_space = t2.search_space AND DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6 AND t1.attribute = 'value' AND t2.attribute2 > 0.1;"
    partition_key = "search_space"
    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        table,
    ) = extract_and_group_query_conditions(query, partition_key)

    # Test attribute_conditions
    expected_attribute_conditions = {
        "t1": ["attribute = 'value'"],
        "t2": ["attribute2 > 0.1"],
    }
    assert attribute_conditions == expected_attribute_conditions

    # Test distance_conditions
    expected_distance_conditions = {
        ("t1", "t2"): ["DIST(t1.g, t2.g) BETWEEN 1.6 AND 3.6"]
    }
    assert distance_conditions == expected_distance_conditions

    # Test other_functions
    assert len(other_functions) == 0


    # Test partition_key_conditions
    assert partition_key_conditions == []

    # Test table_aliases
    expected_table_aliases = ["t1", "t2"]
    assert table_aliases == expected_table_aliases

    # Test table
    expected_table = "table"
    assert table == expected_table


def test_basic_query():
    query = """
    SELECT DISTINCT rp0.search_space
    FROM random_point AS rp0, random_point AS rp1
    WHERE rp0.search_space = rp1.search_space
    AND rp0.field1 = 1
    AND dist(rp0.geo, rp1.geo) <= 4.0
    """
    partition_key = "search_space"

    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        table,
    ) = extract_and_group_query_conditions(query, partition_key)

    assert attribute_conditions == {"rp0": ["field1 = 1"]}
    assert distance_conditions == {("rp0", "rp1"): ["DIST(rp0.geo, rp1.geo) <= 4.0"]}
    assert or_conditions == {}
    assert partition_key_conditions == []
    assert set(table_aliases) == {"rp0", "rp1"}
    assert table == "random_point"


def test_complex_query():
    query = """
    SELECT DISTINCT rp0.search_space
    FROM random_point AS rp0, random_point AS rp1, random_point AS rp2, random_point AS rp3
    WHERE rp0.search_space = rp1.search_space
    AND rp0.search_space = rp2.search_space
    AND rp0.search_space = rp3.search_space
    AND (rp2.field2 = 2 OR rp2.field2 = 3)
    AND rp0.field1 = 1
    AND DIST(rp0.geo, rp3.geo) <= 4.0
    AND rp1.search_space IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    AND rp0.search_space IN (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10))
    """
    partition_key = "search_space"

    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        table,
    ) = extract_and_group_query_conditions(query, partition_key)

    assert dict(or_conditions) == {("rp2",): ["(rp2.field2 = 2 OR rp2.field2 = 3)"]}
    assert dict(attribute_conditions) == {"rp0": ["field1 = 1"]}

    assert dict(distance_conditions) == {
        ("rp0", "rp3"): ["DIST(rp0.geo, rp3.geo) <= 4.0"]
    }
    assert partition_key_conditions == [
        "search_space IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)",
        "search_space IN (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10))",
    ]
    assert set(table_aliases) == {"rp0", "rp1", "rp2", "rp3"}
    assert table == "random_point"


def test_query_with_subquery():
    query = """
    SELECT DISTINCT rp0.search_space
    FROM random_point AS rp0
    WHERE rp0.field1 = 1
    AND rp0.search_space IN (SELECT x FROM other_table WHERE y = 1 AND z = 2)
    """
    partition_key = "search_space"

    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        table,
    ) = extract_and_group_query_conditions(query, partition_key)

    assert attribute_conditions == {"rp0": ["field1 = 1"]}
    assert distance_conditions == {}
    assert or_conditions == {}
    assert partition_key_conditions == [
        "search_space IN (SELECT x FROM other_table WHERE y = 1 AND z = 2)"
    ]
    assert table_aliases == ["rp0"]
    assert table == "random_point"


def test_query_with_multiple_distance_conditions():
    query = """
    SELECT DISTINCT rp0.search_space
    FROM random_point AS rp0, random_point AS rp1, random_point AS rp2
    WHERE DIST(rp0.geo, rp1.geo) <= 4.0
    AND DIST(rp0.geo, rp2.geo) <= 5.0
    AND DIST(rp1.geo, rp2.geo) <= 3.0
    """
    partition_key = "search_space"

    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        table,
    ) = extract_and_group_query_conditions(query, partition_key)

    assert attribute_conditions == {}
    assert distance_conditions == {
        ("rp0", "rp1"): ["DIST(rp0.geo, rp1.geo) <= 4.0"],
        ("rp0", "rp2"): ["DIST(rp0.geo, rp2.geo) <= 5.0"],
        ("rp1", "rp2"): ["DIST(rp1.geo, rp2.geo) <= 3.0"],
    }
    assert len(other_functions) == 0
    assert partition_key_conditions == []
    assert set(table_aliases) == {"rp0", "rp1", "rp2"}
    assert table == "random_point"


def test_query_with_no_conditions():
    query = """
    SELECT DISTINCT rp0.search_space
    FROM random_point AS rp0
    """
    partition_key = "search_space"

    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        table,
    ) = extract_and_group_query_conditions(query, partition_key)

    assert attribute_conditions == {}
    assert distance_conditions == {}
    assert len(other_functions) == 0
    assert partition_key_conditions == []
    assert or_conditions == {}
    assert table_aliases == ["rp0"]
    assert table == "random_point"


def test_query_with_3_or_conditions():
    query = """
    SELECT DISTINCT rp0.search_space
    FROM random_point AS rp0, random_point AS rp1, random_point AS rp2
    WHERE rp0.search_space = rp1.search_space
    AND rp0.search_space = rp2.search_space
    AND rp0.field1 = 1
    AND (rp0.field2 = 2 OR rp1.field2 = 3 OR rp2.field2 = 4)
    AND (rp0.field3 = 5 OR rp1.field3 = 6 OR rp2.field3 = 7)
    AND (rp0.field3 = 5 OR rp1.field3 = 6 OR rp1.field3 = 7)
    """
    partition_key = "search_space"

    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        table,
    ) = extract_and_group_query_conditions(query, partition_key)

    assert attribute_conditions == {"rp0": ["field1 = 1"]}
    assert distance_conditions == {}
    assert len(other_functions) == 0
    assert partition_key_conditions == []
    assert or_conditions == {
        ("rp0", "rp1", "rp2"): [
            "(rp0.field2 = 2 OR rp1.field2 = 3 OR rp2.field2 = 4)",
            "(rp0.field3 = 5 OR rp1.field3 = 6 OR rp2.field3 = 7)",
        ],
        ("rp0", "rp1"): ["(rp0.field3 = 5 OR rp1.field3 = 6 OR rp1.field3 = 7)"],
    }
    assert set(table_aliases) == {"rp0", "rp1", "rp2"}
    assert table == "random_point"


def test_udf_with_3_tables():
    query = """
    SELECT DISTINCT rp0.search_space
    FROM random_point AS rp0, random_point AS rp1, random_point AS rp2
    WHERE rp0.search_space = rp1.search_space
    AND rp0.search_space = rp2.search_space
    AND rp0.field1 = 1
    AND MY_UDF(rp0.attr, rp1.attr, rp2.attr) > 10
    """
    partition_key = "search_space"

    (
        attribute_conditions,
        distance_conditions,
        other_functions,
        partition_key_conditions,
        or_conditions,
        table_aliases,
        table,
    ) = extract_and_group_query_conditions(query, partition_key)

    assert attribute_conditions == {"rp0": ["field1 = 1"]}
    assert distance_conditions == {}
    assert other_functions == {("rp0", "rp1", "rp2"): ["MY_UDF(rp0.attr, rp1.attr, rp2.attr) > 10"]}
    assert partition_key_conditions == []
    assert or_conditions == {}
    assert set(table_aliases) == {"rp0", "rp1", "rp2"}
    assert table == "random_point"

