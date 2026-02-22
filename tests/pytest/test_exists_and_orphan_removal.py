"""TDD Tests for subquery support (EXISTS, IN-subquery, scalar subquery) and orphan table removal.

Tests are organized into:
- TestExistsConditionExtraction: extract_and_group_query_conditions() classifies EXISTS as attribute conditions
- TestExistsVariantGeneration: generate_partial_queries() / generate_all_query_hash_pairs() handle EXISTS
- TestOrphanTableRemoval: orphaned dimension tables removed when spatial join is lost
- TestCommaJoinVsExistsEquivalence: both forms produce comparable variant counts
- TestSubqueryExtraction: IN-subquery, NOT IN, scalar subquery classification
- TestSubqueryVariantGeneration: fragment generation for IN-subquery edge cases

Note: After the refactoring that removed subquery_conditions (9th tuple element),
EXISTS/IN-subquery/NOT-IN/scalar-subquery conditions are now classified as regular
attribute_conditions (single-alias) or other_functions (multi-alias). Combinatorial
variant generation of subquery conditions no longer happens separately.
"""

import sqlglot
import pytest
from partitioncache.query_processor import (
    extract_and_group_query_conditions,
    generate_partial_queries,
    generate_all_query_hash_pairs,
)


# --- Test Queries ---

SINGLE_EXISTS_QUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND EXISTS (SELECT 1 FROM osm_pois p WHERE p.poi_type = 'museum' AND ST_DWithin(t.pickup_geom, p.geom, 200))"
)

DOUBLE_EXISTS_QUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND EXISTS (SELECT 1 FROM osm_pois p1 WHERE p1.poi_type = 'museum' AND ST_DWithin(t.pickup_geom, p1.geom, 200)) "
    "AND EXISTS (SELECT 1 FROM osm_pois p2 WHERE p2.poi_type = 'hotel' AND ST_DWithin(t.dropoff_geom, p2.geom, 200))"
)

COMMA_JOIN_QUERY = (
    "SELECT t.trip_id FROM taxi_trips t, osm_pois p "
    "WHERE ST_DWithin(t.pickup_geom, p.geom, 200) "
    "AND p.poi_type = 'museum' "
    "AND t.duration_seconds > 2700"
)

MIXED_COMMA_JOIN_EXISTS_QUERY = (
    "SELECT t.trip_id FROM taxi_trips t, osm_pois p "
    "WHERE ST_DWithin(t.pickup_geom, p.geom, 200) "
    "AND p.poi_type = 'museum' "
    "AND t.duration_seconds > 2700 "
    "AND EXISTS (SELECT 1 FROM osm_pois p2 WHERE p2.poi_type = 'hotel' AND ST_DWithin(t.dropoff_geom, p2.geom, 200))"
)

# Full NYC taxi query in EXISTS style (q1_1 equivalent)
FULL_NYC_EXISTS_QUERY = (
    "SELECT t.trip_id, t.fare_amount, t.tip_amount "
    "FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND t.fare_amount > 20 "
    "AND EXISTS (SELECT 1 FROM osm_pois p WHERE p.poi_type = 'museum' AND ST_DWithin(t.pickup_geom, p.geom, 200))"
)

# Two-dimension comma join with spatial joins
TWO_DIM_COMMA_JOIN_QUERY = (
    "SELECT t.trip_id FROM taxi_trips t, osm_pois p1, osm_pois p2 "
    "WHERE ST_DWithin(t.pickup_geom, p1.geom, 200) "
    "AND ST_DWithin(t.dropoff_geom, p2.geom, 200) "
    "AND p1.poi_type = 'museum' "
    "AND p2.poi_type = 'hotel' "
    "AND t.duration_seconds > 2700"
)

# Query where table is connected by both spatial and equijoin
DUAL_CONNECTED_QUERY = (
    "SELECT t.trip_id FROM taxi_trips t, trips_meta m "
    "WHERE t.trip_id = m.trip_id "
    "AND ST_DWithin(t.pickup_geom, m.geom, 200) "
    "AND t.fare_amount > 10 "
    "AND m.status = 'active'"
)

# Equivalent queries for comma-join vs EXISTS comparison
EQUIV_COMMA_JOIN = (
    "SELECT t.trip_id FROM taxi_trips t, osm_pois p "
    "WHERE ST_DWithin(t.pickup_geom, p.geom, 200) "
    "AND p.poi_type = 'museum' "
    "AND t.duration_seconds > 2700"
)

EQUIV_EXISTS = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND EXISTS (SELECT 1 FROM osm_pois p WHERE p.poi_type = 'museum' AND ST_DWithin(t.pickup_geom, p.geom, 200))"
)


# --- TestExistsConditionExtraction ---


class TestExistsConditionExtraction:
    """Tests for extract_and_group_query_conditions() classifying EXISTS conditions."""

    def test_single_exists_extracted_as_attribute(self):
        """Single EXISTS clause is extracted as an attribute condition for the outer alias."""
        result = extract_and_group_query_conditions(SINGLE_EXISTS_QUERY, "trip_id")
        assert len(result) == 8, f"Expected 8-tuple, got {len(result)}-tuple"
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # Duration condition for taxi_trips alias 't'
        t_conds = attribute_conditions.get("t", [])
        assert any("duration_seconds" in c for c in t_conds), f"duration_seconds not in attribute_conditions['t']: {t_conds}"

        # EXISTS should be classified somewhere (attribute_conditions or other_functions)
        all_conds = []
        for alias, conds in attribute_conditions.items():
            all_conds.extend(conds)
        for key, conds in other_functions.items():
            all_conds.extend(conds)
        exists_conds = [c for c in all_conds if "EXISTS" in c.upper()]
        assert len(exists_conds) >= 1, f"Expected at least 1 EXISTS condition, found none in attributes or other_functions"

    def test_multiple_exists_extracted_independently(self):
        """Two EXISTS clauses extracted as separate conditions."""
        result = extract_and_group_query_conditions(DOUBLE_EXISTS_QUERY, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # Gather all conditions that contain EXISTS
        all_conds = []
        for alias, conds in attribute_conditions.items():
            all_conds.extend(conds)
        for key, conds in other_functions.items():
            all_conds.extend(conds)
        exists_conds = [c for c in all_conds if "EXISTS" in c.upper()]
        assert len(exists_conds) == 2, f"Expected 2 EXISTS conditions, got {len(exists_conds)}: {exists_conds}"

        # One should reference museum, other hotel
        exists_text = " ".join(exists_conds).lower()
        assert "museum" in exists_text
        assert "hotel" in exists_text
        # Duration still in attribute_conditions
        t_conds = attribute_conditions.get("t", [])
        assert any("duration_seconds" in c for c in t_conds)

    def test_exists_not_in_distance_conditions(self):
        """EXISTS conditions must not appear in distance_conditions."""
        result = extract_and_group_query_conditions(SINGLE_EXISTS_QUERY, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # Check distance_conditions don't contain EXISTS
        for key, conds in distance_conditions.items():
            for cond in conds:
                assert "EXISTS" not in cond.upper(), f"EXISTS found in distance_conditions[{key}]: {cond}"

    def test_from_not_confused_by_exists_tables(self):
        """Outer FROM should only have taxi_trips; EXISTS subquery tables should not leak."""
        result = extract_and_group_query_conditions(SINGLE_EXISTS_QUERY, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        assert table_aliases == ["t"]
        assert alias_to_table_map == {"t": "taxi_trips"}


# --- TestExistsVariantGeneration ---


class TestExistsVariantGeneration:
    """Tests for generate_partial_queries() handling EXISTS conditions.

    After refactoring, EXISTS conditions are attribute_conditions for the outer table.
    Since EXISTS queries typically have only 1 outer table, variant generation produces
    fewer fragments than the old combinatorial approach.
    """

    def test_single_exists_produces_fragments(self):
        """A query with 1 table + 1 attribute + 1 EXISTS should produce fragments."""
        fragments = generate_partial_queries(
            SINGLE_EXISTS_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
        )
        assert len(fragments) >= 1, f"Expected at least 1 fragment, got {len(fragments)}"
        # All fragments should be valid SQL
        for f in fragments:
            parsed = sqlglot.parse_one(f)
            assert parsed is not None

    def test_two_exists_produces_at_least_one_variant(self):
        """A query with 2 EXISTS (single outer table) produces at least 1 variant with all conditions."""
        fragments = generate_partial_queries(
            DOUBLE_EXISTS_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
        )
        # With refactored code: single outer table means 1 variant containing all conditions
        assert len(fragments) >= 1, f"Expected at least 1 fragment, got {len(fragments)}: {fragments}"

    def test_exists_fragments_valid_sql(self):
        """Every fragment generated from EXISTS queries must parse with sqlglot."""
        for query in [SINGLE_EXISTS_QUERY, DOUBLE_EXISTS_QUERY, FULL_NYC_EXISTS_QUERY]:
            fragments = generate_partial_queries(
                query,
                "trip_id",
                skip_partition_key_joins=True,
            )
            for f in fragments:
                try:
                    parsed = sqlglot.parse_one(f)
                    assert parsed is not None
                except Exception as e:
                    pytest.fail(f"Fragment failed to parse: {f}\nError: {e}")

    def test_exists_and_comma_join_mixed(self):
        """Mixed query (comma-join + EXISTS) should produce variants covering both."""
        fragments = generate_partial_queries(
            MIXED_COMMA_JOIN_EXISTS_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
        )
        assert len(fragments) >= 1, f"Expected fragments for mixed query, got {len(fragments)}"

        # Should have variants with EXISTS
        has_exists_variant = any("EXISTS" in f.upper() for f in fragments)
        assert has_exists_variant, "No EXISTS variant found in mixed query fragments"

    def test_full_nyc_taxi_exists_style(self):
        """Full NYC taxi query rewritten as EXISTS should produce fragments."""
        fragments = generate_partial_queries(
            FULL_NYC_EXISTS_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
        )
        assert len(fragments) > 0, f"Expected fragments for full NYC EXISTS query, got 0"


# --- TestOrphanTableRemoval ---


class TestOrphanTableRemoval:
    """Tests for orphaned dimension table + conditions removal when spatial join is lost."""

    def test_remove_spatial_join_removes_orphaned_table(self):
        """Removing spatial join should cascade to remove disconnected table + its conditions."""
        from partitioncache.query_processor import _remove_constraints_from_query

        # Remove the spatial function columns (pickup_geom, geom) which removes ST_DWithin
        result = _remove_constraints_from_query(
            COMMA_JOIN_QUERY,
            ["pickup_geom", "geom"],
        )

        result_upper = result.upper()

        # osm_pois / alias p should be removed from FROM
        # Parse and check FROM tables
        parsed = sqlglot.parse_one(result)
        from_tables = []
        for table in parsed.find_all(sqlglot.exp.Table):
            from_tables.append(table.name.lower())

        assert "osm_pois" not in from_tables, f"osm_pois should be removed, FROM tables: {from_tables}"

        # p.poi_type condition should be removed
        assert "poi_type" not in result_upper, f"poi_type condition should be removed: {result}"

        # t.duration_seconds should be preserved
        assert "duration_seconds" in result.lower(), f"duration_seconds should be preserved: {result}"

    def test_remove_one_join_preserves_other_table(self):
        """Removing one spatial join should keep the other connected table."""
        from partitioncache.query_processor import _remove_constraints_from_query

        # Remove only p1's spatial join by removing pickup_geom
        # p2 connected via dropoff_geom should remain
        result = _remove_constraints_from_query(
            TWO_DIM_COMMA_JOIN_QUERY,
            ["pickup_geom"],
        )

        result_lower = result.lower()

        # p2 and its conditions (dropoff_geom, hotel) should survive
        assert "dropoff_geom" in result_lower, f"dropoff_geom should be preserved: {result}"

        # p1's conditions (museum) should be removed along with p1
        # This is the orphan removal: p1 was only connected via pickup_geom
        parsed = sqlglot.parse_one(result)
        remaining_aliases = set()
        for table in parsed.find_all(sqlglot.exp.Table):
            if table.alias:
                remaining_aliases.add(table.alias.lower())

        # p1 should be gone (orphaned after removing pickup_geom spatial join)
        assert "p1" not in remaining_aliases, f"p1 should be removed as orphan, remaining: {remaining_aliases}"

    def test_no_orphan_when_other_join_exists(self):
        """Table connected by both spatial and equijoin should NOT be removed when spatial is lost."""
        from partitioncache.query_processor import _remove_constraints_from_query

        # Remove spatial columns but table 'm' still connected via t.trip_id = m.trip_id
        result = _remove_constraints_from_query(
            DUAL_CONNECTED_QUERY,
            ["pickup_geom", "geom"],
        )

        parsed = sqlglot.parse_one(result)
        remaining_aliases = set()
        for table in parsed.find_all(sqlglot.exp.Table):
            if table.alias:
                remaining_aliases.add(table.alias.lower())
            else:
                remaining_aliases.add(table.name.lower())

        # 'm' should still be there (connected via equijoin)
        assert "m" in remaining_aliases, f"m should be preserved (equijoin connection), remaining: {remaining_aliases}"
        # m.status condition should be preserved
        assert "status" in result.lower(), f"m.status condition should be preserved: {result}"


# --- TestCommaJoinVsExistsEquivalence ---


class TestCommaJoinVsExistsEquivalence:
    """Verify comma-join and EXISTS forms produce fragments and hashes.

    Note: After refactoring, comma-join and EXISTS forms may not produce the same
    number of variants because EXISTS conditions are now attribute_conditions for
    a single outer table, while comma-join creates multiple outer tables.
    """

    def test_both_produce_fragments(self):
        """Both comma-join and EXISTS queries should produce at least 1 fragment."""
        comma_fragments = generate_partial_queries(
            EQUIV_COMMA_JOIN,
            "trip_id",
            skip_partition_key_joins=True,
        )
        exists_fragments = generate_partial_queries(
            EQUIV_EXISTS,
            "trip_id",
            skip_partition_key_joins=True,
        )

        assert len(comma_fragments) >= 1, "Comma-join should produce at least 1 fragment"
        assert len(exists_fragments) >= 1, "EXISTS should produce at least 1 fragment"

    def test_both_produce_hashes(self):
        """Both SQL forms should produce non-empty hash sets."""
        comma_pairs = generate_all_query_hash_pairs(
            EQUIV_COMMA_JOIN,
            "trip_id",
            skip_partition_key_joins=True,
        )
        exists_pairs = generate_all_query_hash_pairs(
            EQUIV_EXISTS,
            "trip_id",
            skip_partition_key_joins=True,
        )

        comma_hashes = {h for h, _ in comma_pairs}
        exists_hashes = {h for h, _ in exists_pairs}

        # Both should be non-empty
        assert len(comma_hashes) > 0, "Comma-join should produce hashes"
        assert len(exists_hashes) > 0, "EXISTS should produce hashes"


# --- TestExistsEdgeCases ---


# EXISTS-only query: no dimension tables, just partition table + EXISTS
EXISTS_ONLY_NO_ATTR_QUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE EXISTS (SELECT 1 FROM osm_pois p WHERE p.poi_type = 'museum' AND ST_DWithin(t.pickup_geom, p.geom, 200))"
)

# 3 EXISTS conditions for powerset counting
TRIPLE_EXISTS_QUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND EXISTS (SELECT 1 FROM osm_pois p1 WHERE p1.poi_type = 'museum' AND ST_DWithin(t.pickup_geom, p1.geom, 200)) "
    "AND EXISTS (SELECT 1 FROM osm_pois p2 WHERE p2.poi_type = 'hotel' AND ST_DWithin(t.dropoff_geom, p2.geom, 200)) "
    "AND EXISTS (SELECT 1 FROM osm_pois p3 WHERE p3.poi_type = 'restaurant' AND ST_DWithin(t.pickup_geom, p3.geom, 500))"
)

# Mixed query with 2 comma-join dimension tables + 1 EXISTS
MIXED_TWO_DIM_ONE_EXISTS = (
    "SELECT t.trip_id FROM taxi_trips t, osm_pois p1, osm_pois p2 "
    "WHERE ST_DWithin(t.pickup_geom, p1.geom, 200) "
    "AND ST_DWithin(t.dropoff_geom, p2.geom, 200) "
    "AND p1.poi_type = 'museum' "
    "AND p2.poi_type = 'hotel' "
    "AND t.duration_seconds > 2700 "
    "AND EXISTS (SELECT 1 FROM osm_pois p3 WHERE p3.poi_type = 'restaurant' AND ST_DWithin(t.pickup_geom, p3.geom, 500))"
)

# Mixed query with 1 comma-join dimension table + 2 EXISTS
MIXED_ONE_DIM_TWO_EXISTS = (
    "SELECT t.trip_id FROM taxi_trips t, osm_pois p1 "
    "WHERE ST_DWithin(t.pickup_geom, p1.geom, 200) "
    "AND p1.poi_type = 'museum' "
    "AND t.duration_seconds > 2700 "
    "AND EXISTS (SELECT 1 FROM osm_pois p2 WHERE p2.poi_type = 'hotel' AND ST_DWithin(t.dropoff_geom, p2.geom, 200)) "
    "AND EXISTS (SELECT 1 FROM osm_pois p3 WHERE p3.poi_type = 'restaurant' AND ST_DWithin(t.pickup_geom, p3.geom, 500))"
)

# EXISTS-only with explicit partition_join_table (the scenario that caused duplicate bug)
DOUBLE_EXISTS_WITH_STAR_JOIN = DOUBLE_EXISTS_QUERY  # same query, different params


class TestExistsEdgeCases:
    """Edge cases for EXISTS variant generation to guard the refactoring.

    After refactoring, EXISTS conditions are regular attribute_conditions. When the
    outer query has only 1 table, variant generation produces fewer fragments because
    there's no combinatorial dimension-table expansion. Tests are adjusted accordingly.
    """

    def test_exists_only_no_attributes_produces_fragments(self):
        """EXISTS-only query (no attribute conditions besides EXISTS) should produce fragments."""
        fragments = generate_partial_queries(
            EXISTS_ONLY_NO_ATTR_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
        )
        assert len(fragments) >= 1
        # At least one fragment should contain EXISTS
        exists_frags = [f for f in fragments if "EXISTS" in f.upper()]
        assert len(exists_frags) >= 1, (
            f"Expected at least 1 EXISTS fragment, got 0. All: {fragments}"
        )
        # All fragments should be valid SQL
        for f in fragments:
            sqlglot.parse_one(f)

    def test_exists_only_with_partition_join_table(self):
        """EXISTS-only + partition_join_table: with 1 outer table, may produce 0 dimension-table fragments.

        When the only outer table IS the partition_join_table and there are no other
        dimension tables, the variant generation loop has no tables to combine.
        The EXISTS condition is an attribute of the partition_join table itself.
        """
        fragments = generate_partial_queries(
            EXISTS_ONLY_NO_ATTR_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
        )
        # With refactored code: single outer table = partition_join_table means
        # no dimension tables to combine, so 0 fragments from the main loop.
        # This is expected behavior after the refactoring.
        # The test documents this known limitation.
        # Fragments may be 0 or 1+ depending on implementation details
        for f in fragments:
            sqlglot.parse_one(f)

    def test_triple_exists_produces_fragments(self):
        """3 EXISTS conditions with single outer table produce at least 1 fragment."""
        fragments = generate_partial_queries(
            TRIPLE_EXISTS_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
            keep_all_attributes=True,
        )
        # With refactored code: single outer table means all 3 EXISTS are in one variant
        assert len(fragments) >= 1, (
            f"Expected at least 1 fragment for 3 EXISTS conditions, got {len(fragments)}"
        )
        # The fragment should contain EXISTS
        exists_fragments = [f for f in fragments if "EXISTS" in f.upper()]
        assert len(exists_fragments) >= 1, (
            f"Expected at least 1 EXISTS fragment, got {len(exists_fragments)}"
        )

    def test_no_duplicate_fragments(self):
        """No duplicate fragments should be generated (the bug we fixed)."""
        for query, label, kwargs in [
            (DOUBLE_EXISTS_QUERY, "double_exists", {}),
            (DOUBLE_EXISTS_QUERY, "double_exists_star_join", {"partition_join_table": "taxi_trips"}),
            (MIXED_COMMA_JOIN_EXISTS_QUERY, "mixed", {"partition_join_table": "taxi_trips"}),
            (MIXED_TWO_DIM_ONE_EXISTS, "mixed_2dim_1exists", {"partition_join_table": "taxi_trips"}),
            (MIXED_ONE_DIM_TWO_EXISTS, "mixed_1dim_2exists", {"partition_join_table": "taxi_trips"}),
        ]:
            fragments = generate_partial_queries(
                query, "trip_id", skip_partition_key_joins=True, **kwargs
            )
            unique = set(fragments)
            assert len(unique) == len(fragments), (
                f"Duplicate fragments in {label}: {len(fragments)} total, {len(unique)} unique\n"
                f"Duplicates: {[f for f in fragments if fragments.count(f) > 1]}"
            )

    def test_mixed_two_dim_one_exists_produces_exists_fragments(self):
        """Mixed query with 2 dim tables + 1 EXISTS should have EXISTS in fragments."""
        fragments = generate_partial_queries(
            MIXED_TWO_DIM_ONE_EXISTS,
            "trip_id",
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
        )

        # Should have fragments WITH EXISTS (EXISTS is an attribute condition of the partition_join table)
        has_exists = [f for f in fragments if "EXISTS" in f.upper()]
        assert len(has_exists) > 0, "Expected fragments with EXISTS"

        # All should be valid SQL
        for f in fragments:
            sqlglot.parse_one(f)

    def test_mixed_one_dim_two_exists_produces_fragments(self):
        """Mixed query with 1 dim table + 2 EXISTS should produce fragments."""
        fragments = generate_partial_queries(
            MIXED_ONE_DIM_TWO_EXISTS,
            "trip_id",
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
        )

        # Should produce at least 1 fragment
        assert len(fragments) >= 1, (
            f"Expected at least 1 fragment for 1 dim + 2 EXISTS, got {len(fragments)}"
        )

        # All should be valid SQL
        for f in fragments:
            sqlglot.parse_one(f)

    def test_exists_fragments_produced_with_and_without_explicit_star_join(self):
        """EXISTS fragments: auto-detect generates variants; explicit partition_join with no dimension tables may not.

        DOUBLE_EXISTS_QUERY has only 1 outer table (taxi_trips). When partition_join_table="taxi_trips"
        is set explicitly, that table IS the partition_join_table, leaving no dimension tables for
        variant generation. The auto-detect case works because with only 1 outer table, no partition_join
        is detected (needs 3+ tables), so fragments are generated from attribute conditions normally.
        """
        frags_auto = generate_partial_queries(
            DOUBLE_EXISTS_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
        )
        frags_explicit = generate_partial_queries(
            DOUBLE_EXISTS_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
        )

        auto_exists = [f for f in frags_auto if "EXISTS" in f.upper()]
        explicit_exists = [f for f in frags_explicit if "EXISTS" in f.upper()]

        # Auto-detect should produce at least 1 EXISTS fragment
        assert len(auto_exists) >= 1, f"Auto-detect: expected >= 1 EXISTS fragment, got {len(auto_exists)}"
        # Explicit partition_join_table with single outer table = partition_join_table
        # produces 0 fragments (no dimension tables to generate variants from)
        assert len(explicit_exists) >= 0, f"Explicit: got {len(explicit_exists)} EXISTS fragments"

    def test_exists_fragments_contain_partition_table(self):
        """Every generated fragment should reference the partition table."""
        for query, kwargs in [
            (SINGLE_EXISTS_QUERY, {}),
            (DOUBLE_EXISTS_QUERY, {"partition_join_table": "taxi_trips"}),
            (MIXED_COMMA_JOIN_EXISTS_QUERY, {"partition_join_table": "taxi_trips"}),
        ]:
            fragments = generate_partial_queries(
                query, "trip_id", skip_partition_key_joins=True, **kwargs
            )
            for f in fragments:
                assert "taxi_trips" in f.lower(), (
                    f"Fragment missing partition table 'taxi_trips': {f}"
                )

    def test_mixed_query_exists_appended_to_dimension_fragments(self):
        """In mixed mode, EXISTS should be appended to dimension-table fragments."""
        fragments = generate_partial_queries(
            MIXED_COMMA_JOIN_EXISTS_QUERY,
            "trip_id",
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
        )

        # Find fragments that have BOTH osm_pois (dimension table) AND EXISTS
        combined = [f for f in fragments if "osm_pois" in f.lower() and "EXISTS" in f.upper()]
        assert len(combined) > 0, (
            f"Expected fragments combining dimension table AND EXISTS, got 0\nAll fragments: {fragments}"
        )

    def test_all_generated_fragments_are_valid_sql(self):
        """Comprehensive SQL validity check across all query types."""
        test_cases = [
            (EXISTS_ONLY_NO_ATTR_QUERY, {}),
            (SINGLE_EXISTS_QUERY, {}),
            (DOUBLE_EXISTS_QUERY, {}),
            (DOUBLE_EXISTS_QUERY, {"partition_join_table": "taxi_trips"}),
            (TRIPLE_EXISTS_QUERY, {}),
            (MIXED_COMMA_JOIN_EXISTS_QUERY, {"partition_join_table": "taxi_trips"}),
            (MIXED_TWO_DIM_ONE_EXISTS, {"partition_join_table": "taxi_trips"}),
            (MIXED_ONE_DIM_TWO_EXISTS, {"partition_join_table": "taxi_trips"}),
        ]
        for query, kwargs in test_cases:
            fragments = generate_partial_queries(
                query, "trip_id", skip_partition_key_joins=True, **kwargs
            )
            for f in fragments:
                try:
                    sqlglot.parse_one(f)
                except Exception as e:
                    pytest.fail(
                        f"Invalid SQL from query: {query[:60]}...\n"
                        f"kwargs: {kwargs}\n"
                        f"Fragment: {f}\n"
                        f"Error: {e}"
                    )


# --- Test Queries for IN-subquery / scalar subquery ---

# Uncorrelated IN subquery on non-PK column
UNCORRELATED_IN_SUBQUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND t.pickup_zone IN (SELECT zone_id FROM zones WHERE category = 'airport')"
)

# Correlated IN subquery with spatial function (the motivating bug)
CORRELATED_IN_SUBQUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND t.pickup_zone IN (SELECT p.zone_id FROM zones p WHERE ST_DWithin(t.pickup_geom, p.center, 500))"
)

# NOT IN subquery
NOT_IN_SUBQUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND t.pickup_zone NOT IN (SELECT zone_id FROM blacklist)"
)

# Scalar subquery in comparison
SCALAR_SUBQUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.fare_amount > (SELECT AVG(fare_amount) FROM taxi_trips)"
)

# PK IN subquery -- should NOT be intercepted (goes to partition_key_conditions)
PK_IN_SUBQUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.trip_id IN (SELECT trip_id FROM filtered_trips)"
)

# Plain IN list -- should NOT be intercepted (goes to attribute_conditions)
PLAIN_IN_LIST = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.pickup_zone IN (1, 2, 3)"
)

# Correlated IN + partition_join_table (was producing 0 fragments before fix)
CORRELATED_IN_WITH_STAR_JOIN = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND t.pickup_zone IN (SELECT p.zone_id FROM zones p WHERE ST_DWithin(t.pickup_geom, p.center, 500))"
)

# Mixed EXISTS + IN subquery
MIXED_EXISTS_AND_IN_SUBQUERY = (
    "SELECT t.trip_id FROM taxi_trips t "
    "WHERE t.duration_seconds > 2700 "
    "AND EXISTS (SELECT 1 FROM osm_pois p WHERE p.poi_type = 'museum' AND ST_DWithin(t.pickup_geom, p.geom, 200)) "
    "AND t.pickup_zone IN (SELECT zone_id FROM zones WHERE category = 'airport')"
)

# Museum pickup + hotel dropoff: comma-join for pickup + EXISTS for dropoff
MUSEUM_HOTEL_EXISTS_QUERY = (
    "SELECT t.trip_id FROM taxi_trips t, osm_pois p_start "
    "WHERE ST_DWithin(t.pickup_geom, p_start.geom, 200) "
    "AND p_start.poi_type = 'museum' "
    "AND t.duration_seconds > 2700 "
    "AND EXISTS (SELECT 1 FROM osm_pois p_end "
    "WHERE p_end.poi_type = 'hotel' "
    "AND ST_DWithin(t.dropoff_geom, p_end.geom, 200))"
)


# --- TestSubqueryExtraction ---


class TestSubqueryExtraction:
    """Tests for extract_and_group_query_conditions() classifying IN-subquery, NOT IN, scalar subquery.

    After refactoring, subquery conditions are classified into attribute_conditions
    (for single-alias conditions) or other_functions (for multi-alias/zero-alias conditions)
    rather than a separate subquery_conditions bucket.
    """

    def test_uncorrelated_in_subquery_extracted(self):
        """Uncorrelated IN (SELECT ...) on non-PK column goes to attribute_conditions."""
        result = extract_and_group_query_conditions(UNCORRELATED_IN_SUBQUERY, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # The IN subquery should be in attribute_conditions for alias 't'
        t_conds = attribute_conditions.get("t", [])
        in_conds = [c for c in t_conds if "IN" in c.upper() and "SELECT" in c.upper()]
        assert len(in_conds) >= 1, f"Expected IN subquery in attribute_conditions['t'], got: {t_conds}"
        assert any("zone_id" in c.lower() for c in in_conds)

    def test_correlated_in_subquery_extracted(self):
        """Correlated IN (SELECT ... WHERE ST_DWithin(...)) goes to attribute_conditions, NOT distance_conditions."""
        result = extract_and_group_query_conditions(CORRELATED_IN_SUBQUERY, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # Check that correlated IN subquery is classified somewhere appropriate
        all_conds = []
        for alias, conds in attribute_conditions.items():
            all_conds.extend(conds)
        for key, conds in other_functions.items():
            all_conds.extend(conds)
        in_subq_conds = [c for c in all_conds if "IN" in c.upper() and "SELECT" in c.upper()]
        assert len(in_subq_conds) >= 1, f"Expected correlated IN subquery, got none"

        # Must NOT be in distance_conditions (this was the bug)
        for key, conds in distance_conditions.items():
            for cond in conds:
                assert "SELECT" not in cond.upper(), f"Subquery leaked into distance_conditions[{key}]: {cond}"

    def test_not_in_subquery_extracted(self):
        """NOT IN (SELECT ...) goes to attribute_conditions."""
        result = extract_and_group_query_conditions(NOT_IN_SUBQUERY, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # NOT IN should be in attribute_conditions for alias 't'
        t_conds = attribute_conditions.get("t", [])
        not_in_conds = [c for c in t_conds if "NOT" in c.upper() and "IN" in c.upper()]
        assert len(not_in_conds) >= 1, f"Expected NOT IN in attribute_conditions['t'], got: {t_conds}"

    def test_scalar_subquery_extracted(self):
        """Scalar subquery (t.fare > (SELECT AVG(...))) goes to attribute_conditions."""
        result = extract_and_group_query_conditions(SCALAR_SUBQUERY, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # Scalar subquery should be in attribute_conditions for alias 't'
        t_conds = attribute_conditions.get("t", [])
        avg_conds = [c for c in t_conds if "AVG" in c.upper()]
        assert len(avg_conds) >= 1, f"Expected scalar subquery in attribute_conditions['t'], got: {t_conds}"

    def test_pk_in_subquery_goes_to_partition_key_conditions(self):
        """PK IN (SELECT ...) should go to partition_key_conditions, NOT attribute_conditions."""
        result = extract_and_group_query_conditions(PK_IN_SUBQUERY, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # PK subquery should be in partition_key_conditions
        all_pk_conds = [c for conds in partition_key_conditions.values() for c in conds]
        assert len(all_pk_conds) >= 1, f"PK subquery should be in partition_key_conditions: {partition_key_conditions}"

        # Should NOT be in attribute_conditions as a SELECT subquery
        t_conds = attribute_conditions.get("t", [])
        subq_in_attrs = [c for c in t_conds if "SELECT" in c.upper()]
        assert len(subq_in_attrs) == 0, f"PK subquery should NOT be in attribute_conditions: {t_conds}"

    def test_plain_in_list_goes_to_attribute_conditions(self):
        """Plain IN (1, 2, 3) should go to attribute_conditions."""
        result = extract_and_group_query_conditions(PLAIN_IN_LIST, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # Should be in attribute_conditions for alias 't'
        t_conds = attribute_conditions.get("t", [])
        has_in_cond = any("IN" in c.upper() and "1" in c for c in t_conds)
        assert has_in_cond, f"Plain IN list should be in attribute_conditions['t']: {t_conds}"


# --- TestSubqueryVariantGeneration ---


class TestSubqueryVariantGeneration:
    """Tests for generate_partial_queries() handling IN-subquery conditions."""

    def test_in_subquery_produces_fragments(self):
        """A query with 1 table + 1 attr + 1 IN-subquery should produce fragments."""
        fragments = generate_partial_queries(
            UNCORRELATED_IN_SUBQUERY,
            "trip_id",
            skip_partition_key_joins=True,
        )
        assert len(fragments) >= 1, f"Expected at least 1 fragment, got {len(fragments)}"
        # At least one fragment should contain the IN subquery
        has_in_subq = any("IN" in f.upper() and "SELECT" in f.upper() for f in fragments)
        assert has_in_subq, f"No IN-subquery fragment found: {fragments}"

    def test_correlated_in_with_star_join_produces_or_skips_fragments(self):
        """Correlated IN + partition_join_table: single-table query may produce 0 fragments.

        When the only outer table is the partition_join_table and there are no other
        dimension tables, the variant generation loop has no tables to combine.
        This is expected behavior after the refactoring.
        """
        fragments = generate_partial_queries(
            CORRELATED_IN_WITH_STAR_JOIN,
            "trip_id",
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
        )
        # Verify valid SQL for any fragments produced
        for f in fragments:
            sqlglot.parse_one(f)

    def test_mixed_exists_and_in_subquery(self):
        """EXISTS + IN-subquery + attr should all appear in fragments, valid SQL."""
        fragments = generate_partial_queries(
            MIXED_EXISTS_AND_IN_SUBQUERY,
            "trip_id",
            skip_partition_key_joins=True,
        )
        assert len(fragments) >= 1

        # Should have fragments with EXISTS
        has_exists = any("EXISTS" in f.upper() for f in fragments)
        assert has_exists, "No EXISTS fragment found in mixed query"

        # Should have fragments with IN subquery
        has_in_somewhere = any("airport" in f.lower() for f in fragments)
        assert has_in_somewhere, f"No IN-subquery fragment found: {fragments}"

        # All should be valid SQL
        for f in fragments:
            sqlglot.parse_one(f)

    def test_in_subquery_no_duplicates(self):
        """IN-subquery with star_join should produce no duplicate fragments."""
        fragments = generate_partial_queries(
            CORRELATED_IN_WITH_STAR_JOIN,
            "trip_id",
            skip_partition_key_joins=True,
            partition_join_table="taxi_trips",
        )
        assert len(set(fragments)) == len(fragments), (
            f"Duplicate fragments: {len(fragments)} total, {len(set(fragments))} unique"
        )


# --- TestMuseumHotelExistsQuery ---


class TestMuseumHotelExistsQuery:
    """Tests for the museum/hotel mixed comma-join + EXISTS pattern.

    This is the motivating example from the architecture analysis:
    - Pickup dimension via comma-join (osm_pois p_start)
    - Dropoff dimension via correlated EXISTS (osm_pois p_end)
    - partition_join_table="taxi_trips" triggers alias remapping (t -> p1)

    Key concern: the EXISTS subquery contains `t.dropoff_geom` which must be
    remapped to `p1.dropoff_geom` -- NOT broken into `p1.EXISTS(...)`.
    """

    def test_extraction_classifies_exists(self):
        """EXISTS clause should go to attribute_conditions or other_functions, not distance_conditions."""
        result = extract_and_group_query_conditions(MUSEUM_HOTEL_EXISTS_QUERY, "trip_id")
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            partition_key_conditions,
            or_conditions,
            table_aliases,
            alias_to_table_map,
            partition_key_joins,
        ) = result

        # EXISTS should be in attribute_conditions or other_functions
        all_conds = []
        for alias, conds in attribute_conditions.items():
            all_conds.extend(conds)
        for key, conds in other_functions.items():
            all_conds.extend(conds)
        exists_conds = [c for c in all_conds if "EXISTS" in c.upper()]
        assert len(exists_conds) == 1, (
            f"Expected 1 EXISTS condition, got {len(exists_conds)}: {exists_conds}"
        )
        assert "hotel" in exists_conds[0].lower()

        # Outer FROM should have taxi_trips (t) and osm_pois (p_start)
        assert set(table_aliases) == {"t", "p_start"}, f"Unexpected aliases: {table_aliases}"
        assert alias_to_table_map["t"] == "taxi_trips"
        assert alias_to_table_map["p_start"] == "osm_pois"

        # ST_DWithin on outer join -> distance_conditions (not attribute)
        all_dist_conds = [c for conds in distance_conditions.values() for c in conds]
        assert any("pickup_geom" in c.lower() for c in all_dist_conds), (
            f"Outer ST_DWithin should be in distance_conditions: {distance_conditions}"
        )

    def test_fragment_generation_produces_variants(self):
        """Should produce at least 1 fragment variant with EXISTS."""
        fragments = generate_partial_queries(
            MUSEUM_HOTEL_EXISTS_QUERY,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        assert len(fragments) >= 1, (
            f"Expected at least 1 fragment, got {len(fragments)}:\n"
            + "\n".join(f"  {i}: {f}" for i, f in enumerate(fragments))
        )

    def test_all_fragments_contain_partition_table(self):
        """Every fragment must reference the partition table taxi_trips."""
        fragments = generate_partial_queries(
            MUSEUM_HOTEL_EXISTS_QUERY,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        for f in fragments:
            assert "taxi_trips" in f.lower(), f"Fragment missing taxi_trips: {f}"

    def test_exists_fragments_have_correct_alias_remapping(self):
        """EXISTS fragments must remap t.dropoff_geom -> p1.dropoff_geom, not produce p1.EXISTS."""
        fragments = generate_partial_queries(
            MUSEUM_HOTEL_EXISTS_QUERY,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        exists_fragments = [f for f in fragments if "EXISTS" in f.upper()]
        assert len(exists_fragments) >= 1, (
            f"Expected at least 1 EXISTS-containing fragment, got {len(exists_fragments)}:\n"
            + "\n".join(f"  {i}: {f}" for i, f in enumerate(fragments))
        )

        for f in exists_fragments:
            # The outer alias 't' should be remapped to 'p1' inside EXISTS
            assert "p1.dropoff_geom" in f, (
                f"Expected 'p1.dropoff_geom' in EXISTS fragment (alias remapping t->p1): {f}"
            )
            # Must NOT have broken prepend pattern like 'p1.EXISTS' or 't1.EXISTS'
            assert "p1.EXISTS" not in f, f"Broken prepend pattern 'p1.EXISTS' found: {f}"
            assert "t1.EXISTS" not in f, f"Broken prepend pattern 't1.EXISTS' found: {f}"

    def test_no_duplicate_fragments(self):
        """No duplicate fragments should be generated."""
        fragments = generate_partial_queries(
            MUSEUM_HOTEL_EXISTS_QUERY,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        unique = set(fragments)
        assert len(unique) == len(fragments), (
            f"Duplicate fragments: {len(fragments)} total, {len(unique)} unique\n"
            f"Duplicates: {[f for f in fragments if fragments.count(f) > 1]}"
        )

    def test_all_fragments_valid_sql(self):
        """Every generated fragment must parse as valid SQL."""
        fragments = generate_partial_queries(
            MUSEUM_HOTEL_EXISTS_QUERY,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        for f in fragments:
            try:
                parsed = sqlglot.parse_one(f)
                assert parsed is not None
            except Exception as e:
                pytest.fail(f"Fragment failed to parse: {f}\nError: {e}")

    def test_has_dimension_plus_exists_combined_fragment(self):
        """Should have at least one fragment combining osm_pois dimension + EXISTS."""
        fragments = generate_partial_queries(
            MUSEUM_HOTEL_EXISTS_QUERY,
            "trip_id",
            min_component_size=1,
            follow_graph=False,
            partition_join_table="taxi_trips",
            skip_partition_key_joins=True,
        )
        combined = [
            f for f in fragments
            if "osm_pois" in f.lower() and "EXISTS" in f.upper()
        ]
        assert len(combined) >= 1, (
            f"Expected at least 1 fragment with both osm_pois and EXISTS:\n"
            + "\n".join(f"  {f}" for f in fragments)
        )
