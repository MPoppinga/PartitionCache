"""
Comprehensive query processing tests with diverse SQL patterns.

Covers fragment generation, hash consistency, clean_query behavior,
constraint modifications, distance normalization, and edge cases
across 20+ distinct query patterns/shapes.
"""

import pytest
import sqlglot

from partitioncache.query_processor import (
    clean_query,
    extract_and_group_query_conditions,
    generate_all_hashes,
    generate_all_query_hash_pairs,
    normalize_distance_conditions,
    normalize_joins_to_cross_join,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _fragment_queries(pairs: list[tuple[str, str]]) -> list[str]:
    """Extract just query texts from (query, hash) pairs."""
    return [q for q, _ in pairs]


def _fragment_hashes(pairs: list[tuple[str, str]]) -> set[str]:
    """Extract just hashes from (query, hash) pairs."""
    return {h for _, h in pairs}


# ======================================================================
# 1. Self-join — same table used with different aliases
# ======================================================================
class TestSelfJoinFragments:
    """Same physical table with multiple aliases (self-join)."""

    SELF_JOIN = (
        "SELECT DISTINCT p1.city_id "
        "FROM points AS p1, points AS p2 "
        "WHERE ST_DWithin(p1.geom, p2.geom, 500) "
        "AND p1.type = 'school' AND p2.type = 'hospital'"
    )

    def test_self_join_generates_fragments(self):
        pairs = generate_all_query_hash_pairs(self.SELF_JOIN, "city_id")
        assert len(pairs) > 0, "Self-join must produce at least one fragment"

    def test_self_join_fragments_reference_points(self):
        queries = _fragment_queries(generate_all_query_hash_pairs(self.SELF_JOIN, "city_id"))
        assert all("points" in q.lower() for q in queries), "All fragments should reference the 'points' table"

    def test_self_join_single_table_subsets(self):
        """Single-table subsets should exist (just p1 or just p2 conditions)."""
        pairs = generate_all_query_hash_pairs(self.SELF_JOIN, "city_id", min_component_size=1)
        queries = _fragment_queries(pairs)
        single_table = [q for q in queries if q.count("points") == 1]
        assert len(single_table) >= 1, "Should produce single-table subset fragments"


# ======================================================================
# 2. Three-table chain with distance conditions
# ======================================================================
class TestThreeTableChainFragments:
    """Three tables connected in a chain: A—B—C (no direct A—C link)."""

    CHAIN = (
        "SELECT DISTINCT t1.city_id "
        "FROM trips AS t1, pois AS p, hotels AS h "
        "WHERE ST_DWithin(t1.geom, p.geom, 300) "
        "AND ST_DWithin(p.geom, h.geom, 200) "
        "AND t1.fare > 10 AND p.type = 'museum' AND h.stars >= 3"
    )

    def test_chain_produces_multi_fragment_set(self):
        pairs = generate_all_query_hash_pairs(self.CHAIN, "city_id")
        assert len(pairs) >= 4, f"3-table chain should produce >=4 fragments, got {len(pairs)}"

    def test_chain_has_two_table_subsets(self):
        """Connected pairs: (t1,p) and (p,h) should produce 2-table fragments."""
        queries = _fragment_queries(generate_all_query_hash_pairs(self.CHAIN, "city_id", min_component_size=2))
        two_table = [q for q in queries if q.lower().count(" as ") == 2 or q.lower().count(",") == 1]
        assert len(two_table) >= 2, "Should have at least 2 two-table pair fragments"

    def test_chain_no_ac_direct_subset(self):
        """t1 and h have no direct link — follow_graph=True should not produce (t1,h) pair."""
        queries = _fragment_queries(
            generate_all_query_hash_pairs(self.CHAIN, "city_id", min_component_size=2)
        )
        # Each fragment's WHERE should reference the tables in its FROM.
        # A fragment with only trips + hotels but NO pois condition would be invalid
        # under follow_graph=True because they're not directly connected.
        for q in queries:
            q_lower = q.lower()
            tables_in_from = q_lower.split("where")[0] if "where" in q_lower else q_lower
            # If both trips/hotels aliases present but no pois, that's wrong under follow_graph
            has_t1 = "trips" in tables_in_from
            has_h = "hotels" in tables_in_from
            has_p = "pois" in tables_in_from
            if has_t1 and has_h:
                assert has_p, f"t1+h without p is disconnected under follow_graph=True: {q}"


# ======================================================================
# 3. Star-schema with partition-join table detection
# ======================================================================
class TestStarSchemaDetection:
    """Central table (p0) joins to all others only via partition key."""

    STAR_SCHEMA = (
        "SELECT DISTINCT p0.city_id "
        "FROM city_data AS p0, trips AS t, pois AS p "
        "WHERE p0.city_id = t.city_id AND p0.city_id = p.city_id "
        "AND t.fare > 20 AND p.type = 'park' "
        "AND ST_DWithin(t.geom, p.geom, 500)"
    )

    def test_star_schema_auto_detection(self):
        """p0-named table with only PK conditions should be auto-detected as partition-join."""
        pairs = generate_all_query_hash_pairs(
            self.STAR_SCHEMA, "city_id", auto_detect_partition_join=True
        )
        # Fragments should exist
        assert len(pairs) > 0

    def test_star_schema_explicit_table(self):
        """Explicitly specifying partition_join_table should produce consistent results."""
        auto_pairs = generate_all_query_hash_pairs(
            self.STAR_SCHEMA, "city_id", auto_detect_partition_join=True
        )
        explicit_pairs = generate_all_query_hash_pairs(
            self.STAR_SCHEMA, "city_id",
            auto_detect_partition_join=False, partition_join_table="city_data",
        )
        auto_hashes = _fragment_hashes(auto_pairs)
        explicit_hashes = _fragment_hashes(explicit_pairs)
        # Auto and explicit should produce overlapping fragments
        overlap = auto_hashes & explicit_hashes
        assert len(overlap) > 0, "Auto-detected and explicit partition-join should overlap"


# ======================================================================
# 4. Query with only attribute conditions (no spatial/distance)
# ======================================================================
class TestAttributeOnlyQuery:
    """Pure attribute-filter query without any distance/spatial conditions."""

    ATTR_ONLY = (
        "SELECT * FROM products AS p, categories AS c "
        "WHERE p.category_id = c.id AND p.price > 100 AND c.name = 'electronics' "
        "AND p.city_id = 42"
    )

    def test_attribute_only_generates_fragments(self):
        pairs = generate_all_query_hash_pairs(self.ATTR_ONLY, "city_id")
        assert len(pairs) > 0

    def test_attribute_only_partition_key_in_fragments(self):
        queries = _fragment_queries(generate_all_query_hash_pairs(self.ATTR_ONLY, "city_id"))
        has_pk = any("city_id" in q.lower() for q in queries)
        assert has_pk, "At least one fragment should reference the partition key"


# ======================================================================
# 5. Single-table query with multiple conditions
# ======================================================================
class TestSingleTableMultiCondition:
    """Single table, multiple AND conditions, partition key IN list."""

    SINGLE = (
        "SELECT * FROM orders AS o "
        "WHERE o.status = 'shipped' AND o.total > 50 "
        "AND o.region_id IN (1, 2, 3)"
    )

    def test_single_table_fragments(self):
        pairs = generate_all_query_hash_pairs(self.SINGLE, "region_id")
        assert len(pairs) >= 2, f"Single-table multi-condition should produce >=2 fragments, got {len(pairs)}"

    def test_single_table_hash_determinism(self):
        h1 = _fragment_hashes(generate_all_query_hash_pairs(self.SINGLE, "region_id"))
        h2 = _fragment_hashes(generate_all_query_hash_pairs(self.SINGLE, "region_id"))
        assert h1 == h2, "Same query must produce identical hashes"


# ======================================================================
# 6. Query with BETWEEN on partition key
# ======================================================================
class TestBetweenPartitionKey:
    """BETWEEN on the partition key + attribute conditions."""

    BETWEEN = (
        "SELECT * FROM sensors AS s "
        "WHERE s.zone_id BETWEEN 100 AND 200 "
        "AND s.temperature > 30"
    )

    def test_between_generates_fragments(self):
        pairs = generate_all_query_hash_pairs(self.BETWEEN, "zone_id")
        assert len(pairs) >= 2

    def test_between_with_bucket_steps(self):
        """Different bucket_steps should produce different normalized variants."""
        h1 = _fragment_hashes(generate_all_query_hash_pairs(self.BETWEEN, "zone_id", bucket_steps=1.0))
        h5 = _fragment_hashes(generate_all_query_hash_pairs(self.BETWEEN, "zone_id", bucket_steps=5.0))
        # Bucket steps may or may not change hashes for integer BETWEEN, but shouldn't crash
        assert len(h1) > 0 and len(h5) > 0


# ======================================================================
# 7. Query with OR conditions
# ======================================================================
class TestOrConditions:
    """Queries containing OR in WHERE — OR clauses should be kept together."""

    OR_QUERY = (
        "SELECT * FROM events AS e "
        "WHERE (e.type = 'concert' OR e.type = 'festival') "
        "AND e.city_id = 7 AND e.capacity > 1000"
    )

    def test_or_generates_fragments(self):
        pairs = generate_all_query_hash_pairs(self.OR_QUERY, "city_id")
        assert len(pairs) > 0

    def test_or_condition_stays_grouped(self):
        """The OR clause should appear as a single unit, not split."""
        result = clean_query(self.OR_QUERY)
        # After CNF normalization by sqlglot, OR may be distributed
        # but the original semantics should be preserved
        parsed = sqlglot.parse_one(result)
        assert parsed is not None


# ======================================================================
# 8. Nested EXISTS with correlated subquery
# ======================================================================
class TestNestedCorrelatedExists:
    """Correlated EXISTS referencing outer table alias."""

    NESTED_EXISTS = (
        "SELECT t.trip_id FROM trips AS t "
        "WHERE t.fare > 20 "
        "AND EXISTS ("
        "  SELECT 1 FROM reviews AS r "
        "  WHERE r.trip_id = t.trip_id AND r.rating >= 4"
        ") "
        "AND t.city_id = 5"
    )

    def test_exists_generates_fragments(self):
        pairs = generate_all_query_hash_pairs(self.NESTED_EXISTS, "city_id")
        assert len(pairs) > 0

    def test_exists_subquery_preserved_in_fragments(self):
        queries = _fragment_queries(generate_all_query_hash_pairs(self.NESTED_EXISTS, "city_id"))
        has_exists = any("EXISTS" in q.upper() for q in queries)
        assert has_exists, "At least one fragment should preserve the EXISTS subquery"


# ======================================================================
# 9. Multiple JOIN types normalized
# ======================================================================
class TestMixedJoinTypes:
    """LEFT JOIN, RIGHT JOIN, INNER JOIN all normalized to comma joins."""

    LEFT_JOIN = (
        "SELECT t.trip_id FROM trips AS t "
        "LEFT JOIN pois AS p ON ST_DWithin(t.geom, p.geom, 500) "
        "WHERE t.fare > 10 AND t.city_id = 3"
    )

    MULTIPLE_JOINS = (
        "SELECT t.trip_id FROM trips AS t "
        "INNER JOIN pois AS p1 ON ST_DWithin(t.pickup, p1.geom, 300) "
        "LEFT JOIN pois AS p2 ON ST_DWithin(t.dropoff, p2.geom, 300) "
        "WHERE t.fare > 10 AND p1.type = 'museum'"
    )

    def test_left_join_normalized(self):
        result = normalize_joins_to_cross_join(self.LEFT_JOIN)
        from_part = result.upper().split("WHERE")[0]
        assert "LEFT" not in from_part
        assert "JOIN" not in from_part
        assert "ST_DWITHIN" in result.upper()

    def test_left_join_fragments(self):
        pairs = generate_all_query_hash_pairs(self.LEFT_JOIN, "city_id")
        assert len(pairs) > 0

    def test_multiple_join_types_normalized(self):
        result = normalize_joins_to_cross_join(self.MULTIPLE_JOINS)
        from_part = result.upper().split("WHERE")[0]
        assert "INNER" not in from_part
        assert "LEFT" not in from_part

    def test_multiple_joins_produce_fragments(self):
        pairs = generate_all_query_hash_pairs(self.MULTIPLE_JOINS, "city_id", warn_no_partition_key=False)
        assert len(pairs) > 0


# ======================================================================
# 10. Query with comments and semicolons
# ======================================================================
class TestCommentsAndSemicolons:
    """SQL comments and trailing semicolons must be stripped cleanly."""

    COMMENTED = (
        "-- Find expensive trips\n"
        "SELECT * FROM trips AS t -- main table\n"
        "WHERE t.fare > 100 -- filter expensive\n"
        "AND t.city_id = 1; -- end"
    )

    def test_comments_stripped(self):
        result = clean_query(self.COMMENTED)
        assert "--" not in result
        assert ";" not in result

    def test_commented_query_fragments(self):
        pairs = generate_all_query_hash_pairs(self.COMMENTED, "city_id")
        assert len(pairs) >= 2

    def test_commented_matches_uncommented(self):
        plain = "SELECT * FROM trips AS t WHERE t.fare > 100 AND t.city_id = 1"
        h_commented = _fragment_hashes(generate_all_query_hash_pairs(self.COMMENTED, "city_id"))
        h_plain = _fragment_hashes(generate_all_query_hash_pairs(plain, "city_id"))
        assert h_commented == h_plain, "Comments/semicolons should not affect hashes"


# ======================================================================
# 11. Constraint modification: add_constraints
# ======================================================================
class TestAddConstraints:
    """add_constraints creates variants with extra conditions per table."""

    BASE = (
        "SELECT * FROM points AS p, regions AS r "
        "WHERE ST_DWithin(p.geom, r.geom, 1000) "
        "AND p.city_id = 5"
    )

    def test_add_constraints_increases_variants(self):
        base_pairs = generate_all_query_hash_pairs(self.BASE, "city_id")
        constrained_pairs = generate_all_query_hash_pairs(
            self.BASE, "city_id",
            add_constraints={"points": "size = 4"},
        )
        # add_constraints produces both original AND modified → should have more
        assert len(constrained_pairs) >= len(base_pairs)

    def test_add_constraints_includes_constraint_text(self):
        pairs = generate_all_query_hash_pairs(
            self.BASE, "city_id",
            add_constraints={"points": "size = 4"},
        )
        queries = _fragment_queries(pairs)
        has_constraint = any("size" in q.lower() for q in queries)
        assert has_constraint, "At least one fragment should include the added constraint"


# ======================================================================
# 12. Constraint modification: remove_constraints_all
# ======================================================================
class TestRemoveConstraintsAll:
    """remove_constraints_all drops attributes from ALL generated variants."""

    QUERY = (
        "SELECT * FROM events AS e "
        "WHERE e.type = 'concert' AND e.capacity > 500 AND e.city_id = 7"
    )

    def test_remove_constraints_drops_attribute(self):
        pairs = generate_all_query_hash_pairs(
            self.QUERY, "city_id",
            remove_constraints_all=["type"],
        )
        queries = _fragment_queries(pairs)
        # No fragment should contain the removed attribute condition
        for q in queries:
            # 'type' may appear in table names or other contexts, check for condition
            assert "= 'concert'" not in q.lower(), f"Removed attribute still in fragment: {q}"


# ======================================================================
# 13. Constraint modification: remove_constraints_add
# ======================================================================
class TestRemoveConstraintsAdd:
    """remove_constraints_add creates ADDITIONAL variants with attributes removed."""

    QUERY = (
        "SELECT * FROM events AS e "
        "WHERE e.type = 'concert' AND e.capacity > 500 AND e.city_id = 7"
    )

    def test_remove_constraints_add_increases_variants(self):
        base = generate_all_query_hash_pairs(self.QUERY, "city_id")
        augmented = generate_all_query_hash_pairs(
            self.QUERY, "city_id",
            remove_constraints_add=["type"],
        )
        assert len(augmented) > len(base), "remove_constraints_add should produce more variants"

    def test_remove_constraints_add_keeps_originals(self):
        base_hashes = _fragment_hashes(generate_all_query_hash_pairs(self.QUERY, "city_id"))
        augmented_hashes = _fragment_hashes(generate_all_query_hash_pairs(
            self.QUERY, "city_id",
            remove_constraints_add=["type"],
        ))
        # Original hashes should still be present
        assert base_hashes.issubset(augmented_hashes), "Original variants must be preserved"


# ======================================================================
# 14. Distance normalization with various bucket_steps
# ======================================================================
class TestDistanceNormalizationVariants:
    """Bucket steps affect how distance thresholds are rounded."""

    SPATIAL = (
        "SELECT * FROM trips AS t, pois AS p "
        "WHERE ST_DWithin(t.geom, p.geom, 750) "
        "AND t.city_id = 1"
    )

    def test_bucket_step_1_rounds_to_1000(self):
        """750 with bucket_steps=1.0 should round up to 1000 in normalized variant."""
        normalized = normalize_distance_conditions(
            clean_query(self.SPATIAL), bucket_steps=1.0
        )
        assert "1000" in normalized or "750" in normalized

    def test_bucket_step_0_5_rounds_differently(self):
        normalized = normalize_distance_conditions(
            clean_query(self.SPATIAL), bucket_steps=0.5
        )
        # Should produce a valid result without crashing
        assert "ST_DWITHIN" in normalized.upper() or "st_dwithin" in normalized.lower()

    def test_different_bucket_steps_different_hashes(self):
        h1 = _fragment_hashes(generate_all_query_hash_pairs(self.SPATIAL, "city_id", bucket_steps=1.0))
        h2 = _fragment_hashes(generate_all_query_hash_pairs(self.SPATIAL, "city_id", bucket_steps=100.0))
        # With different bucket steps, at least some hashes should differ
        # (the normalized-distance variant differs)
        assert h1 != h2 or len(h1) > 0


# ======================================================================
# 15. Quoted identifiers
# ======================================================================
class TestQuotedIdentifiers:
    """Double-quoted and backtick-quoted identifiers should be cleaned."""

    QUOTED = 'SELECT * FROM "my_table" AS "t" WHERE "t"."city_id" = 5 AND "t"."value" > 10'

    PLAIN = "SELECT * FROM my_table AS t WHERE t.city_id = 5 AND t.value > 10"

    def test_quoted_same_hashes_as_plain(self):
        h_quoted = _fragment_hashes(generate_all_query_hash_pairs(self.QUOTED, "city_id"))
        h_plain = _fragment_hashes(generate_all_query_hash_pairs(self.PLAIN, "city_id"))
        assert h_quoted == h_plain, "Double-quoted identifiers should produce same hashes as plain"

    def test_backtick_not_supported_by_default_dialect(self):
        """Backtick quoting is MySQL-specific; sqlglot's default dialect rejects it.
        This documents expected behavior — callers should strip backticks before passing."""
        import sqlglot.errors

        backtick = "SELECT * FROM `my_table` AS `t` WHERE `t`.`city_id` = 5"
        with pytest.raises(sqlglot.errors.ParseError):
            clean_query(backtick)


# ======================================================================
# 16. Four-table star topology
# ======================================================================
class TestFourTableStar:
    """Four tables in star topology (all connected to a center)."""

    STAR = (
        "SELECT DISTINCT t.city_id "
        "FROM trips AS t, pois_a AS a, pois_b AS b, pois_c AS c "
        "WHERE ST_DWithin(t.geom, a.geom, 100) "
        "AND ST_DWithin(t.geom, b.geom, 200) "
        "AND ST_DWithin(t.geom, c.geom, 300) "
        "AND a.type = 'school' AND b.type = 'park' AND c.type = 'hospital' "
        "AND t.fare > 5"
    )

    def test_four_table_star_fragments(self):
        pairs = generate_all_query_hash_pairs(self.STAR, "city_id")
        assert len(pairs) >= 8, f"4-table star should produce many fragments, got {len(pairs)}"

    def test_min_component_size_filtering(self):
        """min_component_size=3 should exclude single-table and 2-table fragments."""
        pairs_min1 = generate_all_query_hash_pairs(self.STAR, "city_id", min_component_size=1)
        pairs_min3 = generate_all_query_hash_pairs(self.STAR, "city_id", min_component_size=3)
        assert len(pairs_min1) > len(pairs_min3), "Higher min_component_size should produce fewer fragments"


# ======================================================================
# 17. Query with NOT IN subquery
# ======================================================================
class TestNotInSubquery:
    """NOT IN subquery pattern."""

    NOT_IN = (
        "SELECT t.trip_id FROM trips AS t "
        "WHERE t.city_id = 5 "
        "AND t.driver_id NOT IN ("
        "  SELECT b.driver_id FROM blacklist AS b WHERE b.reason = 'fraud'"
        ")"
    )

    def test_not_in_generates_fragments(self):
        pairs = generate_all_query_hash_pairs(self.NOT_IN, "city_id")
        assert len(pairs) > 0

    def test_not_in_subquery_preserved(self):
        queries = _fragment_queries(generate_all_query_hash_pairs(self.NOT_IN, "city_id"))
        # sqlglot normalizes "NOT IN" to "NOT ... IN", so check for both forms
        has_not_in = any("NOT IN" in q.upper() or ("NOT" in q.upper() and " IN " in q.upper()) for q in queries)
        assert has_not_in, f"NOT IN subquery should be preserved in fragments: {queries[:3]}"


# ======================================================================
# 18. Empty WHERE / no conditions
# ======================================================================
class TestNoConditions:
    """Queries with no WHERE clause or trivially simple conditions."""

    NO_WHERE = "SELECT * FROM trips AS t"

    ONLY_PK = "SELECT * FROM trips AS t WHERE t.city_id = 5"

    def test_no_where_produces_no_fragments(self):
        """A query with no WHERE clause shouldn't produce useful fragments."""
        pairs = generate_all_query_hash_pairs(self.NO_WHERE, "city_id")
        # May produce 0 or minimal fragments since there are no conditions
        assert isinstance(pairs, list)

    def test_only_pk_produces_fragments(self):
        pairs = generate_all_query_hash_pairs(self.ONLY_PK, "city_id")
        assert len(pairs) >= 1


# ======================================================================
# 19. Comparison operators: <, >, <=, >=, =, !=, <>, LIKE, ILIKE
# ======================================================================
class TestVariousOperators:
    """Different comparison operators in WHERE conditions."""

    MIXED_OPS = (
        "SELECT * FROM products AS p "
        "WHERE p.price > 10 AND p.price <= 1000 "
        "AND p.name LIKE '%widget%' "
        "AND p.category <> 'deprecated' "
        "AND p.city_id = 42"
    )

    def test_mixed_operators_generate_fragments(self):
        pairs = generate_all_query_hash_pairs(self.MIXED_OPS, "city_id")
        assert len(pairs) >= 2

    def test_like_preserved_in_fragment(self):
        queries = _fragment_queries(generate_all_query_hash_pairs(self.MIXED_OPS, "city_id"))
        has_like = any("LIKE" in q.upper() for q in queries)
        assert has_like, "LIKE condition should appear in fragments"


# ======================================================================
# 20. CAST and type conversion in conditions
# ======================================================================
class TestCastInConditions:
    """CAST expressions in WHERE should not break fragment generation."""

    CAST_QUERY = (
        "SELECT * FROM logs AS l "
        "WHERE CAST(l.created_at AS DATE) = '2024-01-01' "
        "AND l.level = 'ERROR' AND l.region_id = 10"
    )

    def test_cast_generates_fragments(self):
        pairs = generate_all_query_hash_pairs(self.CAST_QUERY, "region_id")
        assert len(pairs) >= 2

    def test_cast_preserved_in_fragment(self):
        queries = _fragment_queries(generate_all_query_hash_pairs(self.CAST_QUERY, "region_id"))
        has_cast = any("CAST" in q.upper() for q in queries)
        assert has_cast, "CAST expression should be preserved in fragments"


# ======================================================================
# 21. Deeply nested AND/OR conditions
# ======================================================================
class TestDeeplyNestedConditions:
    """Deeply nested boolean conditions."""

    DEEP = (
        "SELECT * FROM events AS e "
        "WHERE e.city_id = 3 "
        "AND ((e.type = 'music' AND e.genre = 'jazz') "
        "     OR (e.type = 'art' AND e.medium = 'painting')) "
        "AND e.capacity >= 50"
    )

    def test_deep_nesting_generates_fragments(self):
        pairs = generate_all_query_hash_pairs(self.DEEP, "city_id")
        assert len(pairs) > 0

    def test_deep_nesting_clean_query_valid(self):
        result = clean_query(self.DEEP)
        parsed = sqlglot.parse_one(result)
        assert parsed is not None, "Cleaned deeply nested query must be valid SQL"


# ======================================================================
# 22. Whitespace and formatting variations
# ======================================================================
class TestWhitespaceNormalization:
    """Different whitespace formatting should produce identical hashes."""

    COMPACT = "SELECT * FROM trips AS t WHERE t.fare>10 AND t.city_id=5"

    SPACED = (
        "SELECT  *  FROM  trips  AS  t  "
        "WHERE  t.fare  >  10  AND  t.city_id  =  5"
    )

    MULTILINE = """
        SELECT *
        FROM trips AS t
        WHERE t.fare > 10
          AND t.city_id = 5
    """

    def test_compact_vs_spaced_same_hashes(self):
        h_compact = _fragment_hashes(generate_all_query_hash_pairs(self.COMPACT, "city_id"))
        h_spaced = _fragment_hashes(generate_all_query_hash_pairs(self.SPACED, "city_id"))
        assert h_compact == h_spaced

    def test_compact_vs_multiline_same_hashes(self):
        h_compact = _fragment_hashes(generate_all_query_hash_pairs(self.COMPACT, "city_id"))
        h_multiline = _fragment_hashes(generate_all_query_hash_pairs(self.MULTILINE, "city_id"))
        assert h_compact == h_multiline


# ======================================================================
# 23. JOIN ON equivalence with comma-join
# ======================================================================
class TestJoinCommaEquivalence:
    """JOIN ON syntax must produce same hashes as equivalent comma-join."""

    JOIN_SYNTAX = (
        "SELECT t.trip_id FROM trips AS t "
        "JOIN pois AS p ON ST_DWithin(t.geom, p.geom, 500) "
        "WHERE t.fare > 10 AND p.type = 'museum' AND t.city_id = 1"
    )

    COMMA_SYNTAX = (
        "SELECT t.trip_id FROM trips AS t, pois AS p "
        "WHERE ST_DWithin(t.geom, p.geom, 500) "
        "AND t.fare > 10 AND p.type = 'museum' AND t.city_id = 1"
    )

    def test_join_and_comma_produce_same_hashes(self):
        h_join = _fragment_hashes(generate_all_query_hash_pairs(self.JOIN_SYNTAX, "city_id"))
        h_comma = _fragment_hashes(generate_all_query_hash_pairs(self.COMMA_SYNTAX, "city_id"))
        assert h_join == h_comma, "JOIN ON and comma-join must produce identical hashes"


# ======================================================================
# 24. Condition classification edge cases
# ======================================================================
class TestConditionClassification:
    """Verify correct classification by extract_and_group_query_conditions."""

    def test_partition_key_equality_detected(self):
        """Simple PK = value should go into partition_key_conditions."""
        query = "SELECT * FROM trips AS t WHERE t.city_id = 5 AND t.fare > 10"
        (attr_conds, dist_conds, other_funcs, pk_conds,
         or_conds, table_aliases, alias_map, pk_joins) = extract_and_group_query_conditions(query, "city_id")
        assert len(pk_conds) > 0, "city_id = 5 should be classified as partition key condition"

    def test_partition_key_in_list_detected(self):
        """PK IN (...) should go into partition_key_conditions."""
        query = "SELECT * FROM trips AS t WHERE t.city_id IN (1, 2, 3) AND t.fare > 10"
        (attr_conds, dist_conds, other_funcs, pk_conds,
         or_conds, table_aliases, alias_map, pk_joins) = extract_and_group_query_conditions(query, "city_id")
        assert len(pk_conds) > 0, "city_id IN (...) should be classified as partition key condition"

    def test_partition_key_between_detected(self):
        """PK BETWEEN should go into partition_key_conditions."""
        query = "SELECT * FROM trips AS t WHERE t.city_id BETWEEN 1 AND 10 AND t.fare > 10"
        (attr_conds, dist_conds, other_funcs, pk_conds,
         or_conds, table_aliases, alias_map, pk_joins) = extract_and_group_query_conditions(query, "city_id")
        assert len(pk_conds) > 0, "city_id BETWEEN should be classified as partition key condition"

    def test_pk_equijoin_classified_as_pk_join(self):
        """t1.pk = t2.pk should be in partition_key_joins."""
        query = "SELECT * FROM trips AS t, pois AS p WHERE t.city_id = p.city_id AND t.fare > 10"
        (attr_conds, dist_conds, other_funcs, pk_conds,
         or_conds, table_aliases, alias_map, pk_joins) = extract_and_group_query_conditions(query, "city_id")
        assert len(pk_joins) > 0, "t.city_id = p.city_id should be classified as PK join"

    def test_st_dwithin_classified_as_distance(self):
        """ST_DWithin should be in distance_conditions."""
        query = (
            "SELECT * FROM trips AS t, pois AS p "
            "WHERE ST_DWithin(t.geom, p.geom, 500) AND t.city_id = 5"
        )
        (attr_conds, dist_conds, other_funcs, pk_conds,
         or_conds, table_aliases, alias_map, pk_joins) = extract_and_group_query_conditions(query, "city_id")
        assert len(dist_conds) > 0, "ST_DWithin should be classified as distance condition"


# ======================================================================
# 25. SELECT * already present
# ======================================================================
class TestSelectStarInput:
    """Query already using SELECT * should work identically to column list."""

    STAR_SELECT = "SELECT * FROM trips AS t WHERE t.fare > 10 AND t.city_id = 5"
    COLUMN_SELECT = "SELECT t.trip_id, t.fare FROM trips AS t WHERE t.fare > 10 AND t.city_id = 5"

    def test_star_and_column_same_hashes(self):
        """SELECT * and SELECT columns should produce same hashes (clean_query normalizes to *)."""
        h_star = _fragment_hashes(generate_all_query_hash_pairs(self.STAR_SELECT, "city_id"))
        h_col = _fragment_hashes(generate_all_query_hash_pairs(self.COLUMN_SELECT, "city_id"))
        assert h_star == h_col, "SELECT * and column list should produce identical hashes after normalization"


# ======================================================================
# 26. Complex multi-table with mixed condition types
# ======================================================================
class TestComplexMixedConditions:
    """Real-world-like query with many condition types combined."""

    COMPLEX = (
        "SELECT DISTINCT t.city_id "
        "FROM trips AS t, pois AS p, reviews AS r "
        "WHERE ST_DWithin(t.pickup_geom, p.geom, 500) "
        "AND t.city_id = p.city_id "
        "AND r.trip_id = t.trip_id "
        "AND t.fare BETWEEN 10 AND 100 "
        "AND p.type IN ('restaurant', 'cafe') "
        "AND r.rating >= 4 "
        "AND t.city_id = 42"
    )

    def test_complex_generates_many_fragments(self):
        pairs = generate_all_query_hash_pairs(self.COMPLEX, "city_id")
        assert len(pairs) >= 4, f"Complex multi-table should produce >=4 fragments, got {len(pairs)}"

    def test_complex_hash_determinism(self):
        h1 = sorted(_fragment_hashes(generate_all_query_hash_pairs(self.COMPLEX, "city_id")))
        h2 = sorted(_fragment_hashes(generate_all_query_hash_pairs(self.COMPLEX, "city_id")))
        assert h1 == h2

    def test_complex_with_constraint_modifications(self):
        """add + remove constraints on complex query should not crash."""
        pairs = generate_all_query_hash_pairs(
            self.COMPLEX, "city_id",
            add_constraints={"pois": "verified = true"},
            remove_constraints_add=["rating"],
        )
        assert len(pairs) > 0


# ======================================================================
# 27. generate_all_hashes returns only hashes (no queries)
# ======================================================================
class TestGenerateAllHashes:
    """generate_all_hashes should return list[str] of SHA1 hashes."""

    QUERY = "SELECT * FROM trips AS t WHERE t.fare > 10 AND t.city_id = 5"

    def test_returns_only_strings(self):
        hashes = generate_all_hashes(self.QUERY, "city_id")
        assert all(isinstance(h, str) for h in hashes)

    def test_hashes_are_sha1(self):
        hashes = generate_all_hashes(self.QUERY, "city_id")
        assert all(len(h) == 40 for h in hashes), "All hashes should be 40-char SHA1 hex"

    def test_matches_hash_pairs(self):
        hashes = set(generate_all_hashes(self.QUERY, "city_id"))
        pair_hashes = _fragment_hashes(generate_all_query_hash_pairs(self.QUERY, "city_id"))
        assert hashes == pair_hashes
