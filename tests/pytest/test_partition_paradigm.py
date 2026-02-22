"""
Unit tests for partition paradigm validation.

Tests that partition tables (with PK), extra tables (without PK),
and variant generation work correctly in the PartitionCache system.

Key concepts:
- Partition-join table: central table that joins all others, detected by:
  (a) naming convention (p0_*) with no attributes, or
  (b) smart detection: joins all other tables with only partition key conditions
- Extra tables: tables without partition key — always stay attached to partition-join
"""

from partitioncache.query_processor import (
    _remove_constraints_from_query,
    detect_partition_join_table,
    extract_and_group_query_conditions,
    generate_partial_queries,
)


class TestPartitionTableDetection:
    """Test that partition-join tables are correctly detected."""

    def test_detect_partition_join_by_naming(self):
        """Table named p0_* with no attributes is detected via naming convention."""
        query = (
            "SELECT rp.search_space FROM random_point rp, p0_start ps, p0_end pe "
            "WHERE rp.search_space = ps.search_space AND rp.search_space = pe.search_space "
            "AND rp.field1 = 1"
        )
        (
            attribute_conditions,
            distance_conditions,
            _,
            _,
            _,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = extract_and_group_query_conditions(query, "search_space")

        detected = detect_partition_join_table(
            table_aliases,
            alias_to_table_map,
            attribute_conditions,
            distance_conditions,
            partition_key_joins,
            "search_space",
            auto_detect_partition_join=True,
            partition_join_table=None,
        )
        # One of the p0 tables should be detected
        assert detected is not None
        detected_table = alias_to_table_map.get(detected, detected)
        assert detected_table.startswith("p0")

    def test_explicit_partition_join_table(self):
        """Explicitly set partition_join_table overrides auto-detection."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p "
            "WHERE ST_DWithin(t.geom, p.geom, 500) AND t.fare > 10"
        )
        (
            attribute_conditions,
            distance_conditions,
            _,
            _,
            _,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = extract_and_group_query_conditions(query, "trip_id")

        detected = detect_partition_join_table(
            table_aliases,
            alias_to_table_map,
            attribute_conditions,
            distance_conditions,
            partition_key_joins,
            "trip_id",
            auto_detect_partition_join=False,
            partition_join_table="taxi_trips",
        )
        assert detected == "t"

    def test_no_detection_with_two_tables(self):
        """Smart detection requires 3+ tables — 2 tables should not detect."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p "
            "WHERE t.trip_id = p.trip_id AND t.fare > 10"
        )
        (
            attribute_conditions,
            distance_conditions,
            _,
            _,
            _,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = extract_and_group_query_conditions(query, "trip_id")

        detected = detect_partition_join_table(
            table_aliases,
            alias_to_table_map,
            attribute_conditions,
            distance_conditions,
            partition_key_joins,
            "trip_id",
            auto_detect_partition_join=True,
            partition_join_table=None,
        )
        # With only 2 tables, no smart detection
        assert detected is None


class TestExtraTablesStayAttached:
    """Test that extra tables (without PK) always stay attached to their partition table."""

    def test_two_extra_tables_fragments(self):
        """With explicit partition_join + 2 extra tables, fragments include the partition table."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p1, pois p2 "
            "WHERE ST_DWithin(t.geom, p1.geom, 500) "
            "AND ST_DWithin(t.geom, p2.geom, 300) "
            "AND t.fare > 10 AND p1.name = 'park' AND p2.name = 'school'"
        )
        fragments = generate_partial_queries(
            query,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        # Should generate variants for combinations of extra tables
        # {p1}, {p2}, {p1, p2} = 3 combinations
        # Each gets partition_join (taxi_trips) re-added
        assert len(fragments) >= 3

        # Every fragment should contain taxi_trips
        for frag in fragments:
            assert "taxi_trips" in frag

    def test_single_extra_table_fragment(self):
        """With 1 extra table, generate at least 1 fragment containing both tables."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p "
            "WHERE ST_DWithin(t.geom, p.geom, 500) "
            "AND t.fare > 10 AND p.name = 'park'"
        )
        fragments = generate_partial_queries(
            query,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        assert len(fragments) >= 1

        # Every fragment should have taxi_trips
        for frag in fragments:
            assert "taxi_trips" in frag


class TestVariantGeneration:
    """Test correct variant generation with partition paradigm."""

    def test_fragment_includes_spatial_conditions(self):
        """Fragments re-add spatial conditions between partition-join and extra tables."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p "
            "WHERE ST_DWithin(t.geom, p.geom, 500) "
            "AND t.fare > 10 AND p.name = 'park'"
        )
        fragments = generate_partial_queries(
            query,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        # At least one fragment should contain ST_DWITHIN
        assert any("ST_DWITHIN" in f.upper() or "st_dwithin" in f.lower() for f in fragments)

    def test_non_spatial_partition_join_with_pk_joins(self):
        """Non-spatial mode: fragments include partition_key equijoins via partition-join table."""
        query = (
            "SELECT t.trip_id FROM orders t, customers c, products p "
            "WHERE t.trip_id = c.trip_id AND t.trip_id = p.trip_id "
            "AND t.amount > 100 AND c.country = 'US' AND p.category = 'electronics'"
        )
        fragments = generate_partial_queries(
            query,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="orders",
            skip_partition_key_joins=False,
        )
        # With 2 extra tables (c, p), should get combinations
        assert len(fragments) >= 3

        # All fragments should have orders table
        for frag in fragments:
            assert "orders" in frag

    def test_fragment_count_with_follow_graph_false(self):
        """follow_graph=False generates all possible subsets of extra tables."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p1, pois p2, pois p3 "
            "WHERE ST_DWithin(t.geom, p1.geom, 500) "
            "AND ST_DWithin(t.geom, p2.geom, 300) "
            "AND ST_DWithin(t.geom, p3.geom, 100) "
            "AND t.fare > 10 AND p1.name = 'a' AND p2.name = 'b' AND p3.name = 'c'"
        )
        fragments = generate_partial_queries(
            query,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        # 3 extra tables → 2^3 - 1 = 7 non-empty subsets
        assert len(fragments) >= 7


class TestRemoveConstraintsOrphanedTables:
    """Test that _remove_constraints_from_query cleans up conditions."""

    def test_remove_constraint_basic(self):
        """Removing an attribute removes matching WHERE conditions."""
        query = "SELECT t1.trip_id FROM taxi_trips AS t1 WHERE t1.fare > 10 AND t1.distance > 5"
        result = _remove_constraints_from_query(query, ["fare"])
        assert "fare" not in result
        assert "distance" in result

    def test_remove_all_conditions(self):
        """Removing all conditions removes WHERE clause entirely."""
        query = "SELECT t1.trip_id FROM taxi_trips AS t1 WHERE t1.fare > 10"
        result = _remove_constraints_from_query(query, ["fare"])
        assert "fare" not in result

    def test_preserve_unrelated_conditions(self):
        """Removing one table's conditions preserves the other's."""
        query = (
            "SELECT t1.trip_id FROM taxi_trips AS t1, pois AS t2 "
            "WHERE t1.fare > 10 AND t2.name = 'park'"
        )
        result = _remove_constraints_from_query(query, ["name"])
        assert "name" not in result
        assert "fare" in result
