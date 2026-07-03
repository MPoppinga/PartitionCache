"""
Tests for partition_key_source_table parameter and auto-detection.

When skip_partition_key_joins=True, not all tables have the partition key column.
The partition_key_source_table parameter tells the system which table(s) contain the
partition key, enabling correct SELECT clauses and filtering of invalid fragments.

Auto-detection: When partition_key_source_table is None and skip_partition_key_joins=True,
the system parses the original SELECT clause to find which table has the partition key column.

Without this parameter or auto-detection, multi-table fragments may SELECT partition_key from
a dimension table that doesn't have the column (e.g., SELECT t1.trip_id FROM osm_pois AS t1).
"""

import logging
import re

from partitioncache.query_processor import (
    generate_all_query_hash_pairs,
    generate_partial_queries,
)

# -- Shared query fixtures --

# NYC-taxi-style: fact table (taxi_trips) has trip_id, dimension table (osm_pois) does not
FACT_DIM_QUERY = (
    "SELECT t.trip_id "
    "FROM taxi_trips t, osm_pois p_start, osm_pois p_end "
    "WHERE t.duration_seconds > 2700 "
    "AND ST_DWithin(t.pickup_geom, p_start.geom, 200) "
    "AND p_start.poi_type = 'museum' "
    "AND ST_DWithin(t.dropoff_geom, p_end.geom, 200) "
    "AND p_end.poi_type = 'hotel'"
)

# Complex variant with multiple fact-table conditions
FACT_DIM_QUERY_COMPLEX = (
    "SELECT t.trip_id "
    "FROM taxi_trips t, osm_pois p_start, osm_pois p_end "
    "WHERE t.duration_seconds > 2700 "
    "AND t.pickup_hour BETWEEN 1 AND 4 "
    "AND ST_DWithin(t.pickup_geom, p_start.geom, 200) "
    "AND p_start.poi_type = 'museum' "
    "AND ST_DWithin(t.dropoff_geom, p_end.geom, 200) "
    "AND p_end.poi_type = 'hotel'"
)

# Two tables, both with partition key (standard case)
BOTH_HAVE_PK_QUERY = (
    "SELECT * "
    "FROM points p1, lines p2 "
    "WHERE p1.size > 5 "
    "AND ST_DWithin(p1.geom, p2.geom, 100) "
    "AND p2.weight < 10"
)


def _extract_from_tables(sql: str) -> list[str]:
    """Extract table names from the FROM clause of a SQL fragment."""
    m = re.search(r"FROM\s+(.*?)\s+WHERE", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        m = re.search(r"FROM\s+(.*?)$", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return []
    from_clause = m.group(1)
    # Extract table names (before AS or space)
    tables = []
    for part in from_clause.split(","):
        part = part.strip()
        # "taxi_trips AS t1" -> "taxi_trips"
        table_name = part.split()[0] if part else ""
        tables.append(table_name.lower())
    return tables


def _extract_select_table_alias(sql: str) -> str | None:
    """Extract the table alias used in the SELECT DISTINCT clause."""
    m = re.match(r"SELECT\s+DISTINCT\s+(\w+)\.", sql, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


class TestPartitionKeySourceTable:
    """Test that partition_key_source_table (explicit or auto-detected) enables correct multi-table fragment generation."""

    def test_multi_table_fragments_select_from_fact_table(self):
        """Multi-table fragments should SELECT partition_key from the fact table, not dimension tables.

        Auto-detection: SELECT t.trip_id -> taxi_trips is the source table (t alias = taxi_trips).
        """
        pairs = generate_all_query_hash_pairs(
            query=FACT_DIM_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
        )

        # Should have multi-table fragments (fact + dimension)
        multi_table = [(q, h) for q, h in pairs if "osm_pois" in q.lower() and "taxi_trips" in q.lower()]
        assert len(multi_table) > 0, "Should generate multi-table fragments with both fact and dimension tables"

        # ALL multi-table fragments must SELECT trip_id from taxi_trips alias, not from osm_pois alias
        for fragment, _hash in multi_table:
            select_alias = _extract_select_table_alias(fragment)
            assert select_alias is not None, f"Fragment should have SELECT DISTINCT alias.trip_id: {fragment}"
            # The alias used in SELECT should map to taxi_trips, not osm_pois
            # Check that "taxi_trips AS <select_alias>" appears in FROM
            pattern = rf"taxi_trips\s+AS\s+{re.escape(select_alias)}\b"
            assert re.search(pattern, fragment, re.IGNORECASE), (
                f"SELECT alias '{select_alias}' should reference taxi_trips, not osm_pois. Fragment: {fragment}"
            )

    def test_dimension_only_fragments_excluded(self):
        """Fragments with only dimension tables (no fact table) should be excluded."""
        pairs = generate_all_query_hash_pairs(
            query=FACT_DIM_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
        )

        for fragment, _hash in pairs:
            tables = _extract_from_tables(fragment)
            # Every fragment must include taxi_trips (the partition key source)
            assert "taxi_trips" in tables, (
                f"Fragment without partition_key_source_table 'taxi_trips' should be excluded: {fragment}"
            )

    def test_fact_only_fragments_still_generated(self):
        """Single fact-table fragments should still be generated correctly."""
        pairs = generate_all_query_hash_pairs(
            query=FACT_DIM_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
        )

        # Should have fact-only fragments
        fact_only = [q for q, _ in pairs if "taxi_trips" in q.lower() and "osm_pois" not in q.lower()]
        assert len(fact_only) > 0, "Should generate fact-table-only fragments"

        # Fact-only fragments should still SELECT from t1 (which IS taxi_trips)
        for fragment in fact_only:
            assert "trip_id" in fragment.lower(), f"Fact-only fragment should reference trip_id: {fragment}"

    def test_more_fragments_with_auto_detect_than_manual_filter(self):
        """With auto-detected source table, we get MORE valid fragments than manual filtering.

        Without source table detection, the benchmark had to manually filter out broken fragments,
        leaving only fact-table-only fragments. With auto-detection, multi-table fragments
        are valid and included.
        """
        # With auto-detection (new behavior - parses SELECT t.trip_id to find taxi_trips)
        pairs_auto = generate_all_query_hash_pairs(
            query=FACT_DIM_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
        )

        # Simulate benchmark's manual filter (only keep taxi_trips-only fragments)
        def fact_only_filter(sql: str) -> bool:
            m = re.search(r"FROM\s+(.*?)\s+WHERE", sql, re.IGNORECASE | re.DOTALL)
            if m:
                from_clause = m.group(1)
                return "taxi_trips" in from_clause and "osm_pois" not in from_clause
            return False

        fact_only_from_auto = [q for q, _ in pairs_auto if fact_only_filter(q)]

        assert len(pairs_auto) > len(fact_only_from_auto), (
            f"Auto-detected source table should produce multi-table fragments beyond fact-only "
            f"(total={len(pairs_auto)}, fact-only={len(fact_only_from_auto)})"
        )

    def test_complex_query_generates_condition_variant_fragments(self):
        """With keep_all_attributes=False, multi-table fragments should have condition subsets."""
        pairs = generate_all_query_hash_pairs(
            query=FACT_DIM_QUERY_COMPLEX,
            partition_key="trip_id",
            min_component_size=1,
            keep_all_attributes=False,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
        )

        # Multi-table fragments with spatial joins should exist
        spatial_fragments = [q for q, _ in pairs if "st_dwithin" in q.lower()]
        assert len(spatial_fragments) > 0, "Should generate multi-table fragments with spatial conditions"

        # All spatial fragments must SELECT trip_id from a taxi_trips alias
        for fragment in spatial_fragments:
            select_alias = _extract_select_table_alias(fragment)
            assert select_alias is not None, f"Fragment missing SELECT alias: {fragment}"
            pattern = rf"taxi_trips\s+AS\s+{re.escape(select_alias)}\b"
            assert re.search(pattern, fragment, re.IGNORECASE), (
                f"Spatial fragment SELECT alias '{select_alias}' must map to taxi_trips: {fragment}"
            )


class TestPartitionKeySourceTableBackwardCompat:
    """Verify backward compatibility: existing behavior unchanged when parameter is not used."""

    def test_no_source_table_default_behavior(self):
        """Without partition_key_source_table, behavior matches existing (all fragments, select from t1)."""
        pairs_default = generate_all_query_hash_pairs(
            query=BOTH_HAVE_PK_QUERY,
            partition_key="pdb_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            follow_graph=False,
            skip_partition_key_joins=False,
        )

        # Should produce fragments (existing behavior)
        assert len(pairs_default) > 0

    def test_source_table_none_same_as_omitted(self):
        """Explicitly passing partition_key_source_table=None should match default."""
        pairs_default = generate_all_query_hash_pairs(
            query=BOTH_HAVE_PK_QUERY,
            partition_key="pdb_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            follow_graph=False,
            skip_partition_key_joins=False,
        )

        pairs_none = generate_all_query_hash_pairs(
            query=BOTH_HAVE_PK_QUERY,
            partition_key="pdb_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            follow_graph=False,
            skip_partition_key_joins=False,
            partition_key_source_table=None,
        )

        # Same fragments
        default_queries = sorted(q for q, _ in pairs_default)
        none_queries = sorted(q for q, _ in pairs_none)
        assert default_queries == none_queries


class TestPartitionKeySourceTableAutoDetect:
    """Test auto-detection of partition_key_source_table from SELECT clause."""

    def test_auto_detect_from_select_clause(self):
        """When partition_key_source_table is None and skip_partition_key_joins=True,
        auto-detect which table has the partition key from SELECT clause."""
        # No explicit partition_key_source_table - should auto-detect from SELECT t.trip_id
        pairs = generate_all_query_hash_pairs(
            query=FACT_DIM_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
        )

        # Multi-table fragments should exist and SELECT from taxi_trips
        multi_table = [(q, h) for q, h in pairs if "osm_pois" in q.lower() and "taxi_trips" in q.lower()]
        assert len(multi_table) > 0, "Auto-detection should enable multi-table fragments"

        for fragment, _hash in multi_table:
            select_alias = _extract_select_table_alias(fragment)
            assert select_alias is not None, f"Fragment should have SELECT DISTINCT alias.trip_id: {fragment}"
            pattern = rf"taxi_trips\s+AS\s+{re.escape(select_alias)}\b"
            assert re.search(pattern, fragment, re.IGNORECASE), (
                f"Auto-detected: SELECT alias '{select_alias}' should map to taxi_trips: {fragment}"
            )

    def test_auto_detect_only_when_skip_partition_key_joins(self):
        """Auto-detection should NOT activate when skip_partition_key_joins=False."""
        # With skip_partition_key_joins=False, all tables assumed to have PK
        pairs = generate_all_query_hash_pairs(
            query=BOTH_HAVE_PK_QUERY,
            partition_key="pdb_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            follow_graph=False,
            skip_partition_key_joins=False,
        )

        # Standard behavior: fragments include all table combinations
        assert len(pairs) > 0, "Standard behavior should produce fragments"

    def test_auto_detect_skipped_when_partition_join_set(self):
        """When partition_join_table is set, the partition-join mechanism handles SELECT.
        The combination filter should not activate even if auto-detection resolved a source table."""
        pairs = generate_all_query_hash_pairs(
            query=FACT_DIM_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
            partition_join_table="taxi_trips",
        )

        # Partition-join mechanism re-adds fact table to all fragments, so they should be valid
        assert len(pairs) > 0, "Partition-join should produce valid fragments"

    def test_explicit_overrides_auto_detect(self):
        """Explicit partition_key_source_table should produce same results as auto-detection
        when SELECT references the same table."""
        pairs_auto = generate_all_query_hash_pairs(
            query=FACT_DIM_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
        )

        pairs_explicit = generate_all_query_hash_pairs(
            query=FACT_DIM_QUERY,
            partition_key="trip_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            auto_detect_partition_join=False,
            skip_partition_key_joins=True,
            follow_graph=False,
            partition_key_source_table="taxi_trips",
        )

        # Auto-detect and explicit should produce identical fragments
        auto_queries = sorted(q for q, _ in pairs_auto)
        explicit_queries = sorted(q for q, _ in pairs_explicit)
        assert auto_queries == explicit_queries, (
            f"Auto-detect and explicit should match. Auto={len(auto_queries)}, Explicit={len(explicit_queries)}"
        )

    def test_auto_detect_select_star_falls_back(self):
        """With SELECT * (no explicit PK reference), auto-detection can't determine source table."""
        # BOTH_HAVE_PK_QUERY uses SELECT * - no table-qualified PK reference
        pairs = generate_all_query_hash_pairs(
            query=BOTH_HAVE_PK_QUERY,
            partition_key="pdb_id",
            min_component_size=1,
            keep_all_attributes=True,
            strip_select=True,
            follow_graph=False,
            skip_partition_key_joins=True,
        )

        # Should still produce fragments (default behavior without source table filtering)
        assert len(pairs) > 0, "SELECT * should fall back to default behavior"


class TestPartitionKeySourceTableGeneratePartial:
    """Test the lower-level generate_partial_queries function directly."""

    def test_partial_queries_exclude_dimension_only(self):
        """generate_partial_queries should exclude combinations without the source table."""
        from partitioncache.query_processor import clean_query

        query = clean_query(FACT_DIM_QUERY)
        results = generate_partial_queries(
            query,
            partition_key="trip_id",
            min_component_size=1,
            follow_graph=False,
            skip_partition_key_joins=True,
            warn_no_partition_key=False,
            partition_key_source_table="taxi_trips",
        )

        # ALL results should include taxi_trips in FROM
        for r in results:
            assert "taxi_trips" in r.lower(), (
                f"Fragment without taxi_trips should be excluded: {r}"
            )

    def test_partial_queries_select_from_source_table(self):
        """All partial queries should SELECT partition_key from the source table."""
        from partitioncache.query_processor import clean_query

        query = clean_query(FACT_DIM_QUERY)
        results = generate_partial_queries(
            query,
            partition_key="trip_id",
            min_component_size=1,
            follow_graph=False,
            skip_partition_key_joins=True,
            warn_no_partition_key=False,
            partition_key_source_table="taxi_trips",
        )

        for r in results:
            select_alias = _extract_select_table_alias(r)
            if select_alias is None:
                continue  # Skip non-SELECT fragments (e.g., raw subqueries)
            # The alias in SELECT should be for taxi_trips
            pattern = rf"taxi_trips\s+AS\s+{re.escape(select_alias)}\b"
            assert re.search(pattern, r, re.IGNORECASE), (
                f"SELECT alias '{select_alias}' must reference taxi_trips: {r}"
            )


class TestPartitionKeyWarningDowngrade:
    """Test that partition key warnings are downgraded to INFO when source table context is available."""

    def test_auto_detect_downgrades_warnings_to_info(self, caplog):
        """With auto-detected source table, partition key messages should be INFO, not WARNING."""
        with caplog.at_level(logging.DEBUG, logger="PartitionCache"):
            generate_all_query_hash_pairs(
                query=FACT_DIM_QUERY,
                partition_key="trip_id",
                min_component_size=1,
                keep_all_attributes=True,
                strip_select=True,
                auto_detect_partition_join=False,
                skip_partition_key_joins=True,
                follow_graph=False,
                warn_no_partition_key=True,
            )

        # Should have INFO-level messages about partition key (downgraded from WARNING)
        pk_info = [r for r in caplog.records if "partition key" in r.message.lower() and r.levelno == logging.INFO]
        assert len(pk_info) > 0, "Should have INFO-level partition key messages for source/dimension tables"

        # Should NOT have WARNING-level messages about partition key
        pk_warn = [r for r in caplog.records if "does not use partition key" in r.message and r.levelno == logging.WARNING]
        assert len(pk_warn) == 0, f"Should not have WARNING-level partition key messages, got: {[r.message for r in pk_warn]}"

    def test_no_source_table_keeps_warnings(self, caplog):
        """Without source table context, partition key messages should remain WARNING."""
        with caplog.at_level(logging.DEBUG, logger="PartitionCache"):
            generate_all_query_hash_pairs(
                query=BOTH_HAVE_PK_QUERY,
                partition_key="nonexistent_key",
                min_component_size=1,
                keep_all_attributes=True,
                strip_select=True,
                follow_graph=False,
                skip_partition_key_joins=False,
                warn_no_partition_key=True,
            )

        # Should have WARNING-level messages (no source table to downgrade)
        pk_warn = [r for r in caplog.records if "does not use partition key" in r.message and r.levelno == logging.WARNING]
        assert len(pk_warn) > 0, "Should have WARNING-level partition key messages when no source table context"
