"""
Unit tests for alias counting bug fix in extract_and_group_query_conditions.

Tests that conditions referencing a single table alias multiple times
are correctly classified using distinct alias counting, not total occurrence counting.

Key distinction (with regex remap):
- Single alias (any number of references) → attribute_conditions (regex remap handles all)
- Two distinct aliases → distance_conditions
- Three+ distinct aliases → other_functions
"""

import pytest

from partitioncache.query_processor import extract_and_group_query_conditions


class TestAliasCountingDistinctAliases:
    """Test that alias counting uses distinct aliases, not total occurrences."""

    def test_single_alias_single_ref_is_attribute(self):
        """t.fare > 10 (one alias, one reference) → attribute_conditions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE t.fare_amount > 50"
        )
        (
            attribute_conditions, _, _, _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        assert "t" in attribute_conditions
        assert len(attribute_conditions["t"]) == 1
        assert attribute_conditions["t"][0] == "t.fare_amount > 50"

    def test_single_alias_multi_ref_is_attribute(self):
        """t.col1 * t.col2 > 3 (one alias, multiple refs) → attribute_conditions (regex remap handles it)."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE t.trip_distance * 1609.34 / NULLIF(t.total_amount, 0) > 3"
        )
        (
            attribute_conditions,
            distance_conditions,
            other_functions,
            _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # Should NOT be in distance_conditions (only 1 distinct alias)
        assert len(distance_conditions) == 0
        # With regex remap, single-alias multi-ref goes to attribute_conditions
        assert "t" in attribute_conditions
        assert len(attribute_conditions["t"]) == 1
        assert "trip_distance" in attribute_conditions["t"][0]

    def test_single_alias_function_calls_multi_ref(self):
        """ST_Distance(t.pickup_geom, t.dropoff_geom) > 100 is single-alias, multi-ref → attribute_conditions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE ST_Distance(t.pickup_geom, t.dropoff_geom) > 100"
        )
        (
            attribute_conditions,
            distance_conditions,
            _other_functions,
            _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # NOT distance_conditions (only 1 distinct alias, not 2)
        assert len(distance_conditions) == 0
        # With regex remap, goes to attribute_conditions
        assert "t" in attribute_conditions
        assert any("st_distance" in c.lower() for c in attribute_conditions["t"])

    def test_two_alias_condition_classified_as_distance(self):
        """ST_DWithin(t.geom, p.geom, 500) should be distance_conditions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p "
            "WHERE ST_DWithin(t.geom, p.geom, 500)"
        )
        (
            _,
            distance_conditions,
            _, _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # Two distinct aliases => distance_conditions
        assert len(distance_conditions) > 0
        key = ("p", "t") if ("p", "t") in distance_conditions else ("t", "p")
        assert key in distance_conditions

    def test_zero_alias_literal_condition(self):
        """A condition without any alias (e.g., '1 = 1') should not crash."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE 1 = 1 AND t.fare > 10"
        )
        (
            attribute_conditions,
            _, _, _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # t.fare > 10 should be in attribute_conditions
        assert "t" in attribute_conditions
        assert any("fare" in c for c in attribute_conditions["t"])

    def test_complex_single_alias_not_distance(self):
        """Complex expression with many references to same alias is NOT distance_conditions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t "
            "WHERE t.trip_distance * 1609.34 / NULLIF(ST_Distance(t.pickup_geom, t.dropoff_geom), 0) > 3"
        )
        (
            attribute_conditions,
            distance_conditions,
            _other_functions,
            _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # 4 occurrences of 't.' but only 1 distinct alias → NOT distance_conditions
        assert len(distance_conditions) == 0
        # With regex remap, single-alias conditions go to attribute_conditions
        assert "t" in attribute_conditions
        assert len(attribute_conditions["t"]) == 1

    def test_multi_alias_complex_condition(self):
        """Condition referencing 3 aliases should go to other_functions."""
        query = (
            "SELECT t.trip_id FROM taxi_trips t, pois p1, pois p2 "
            "WHERE SOME_FUNC(t.geom, p1.geom, p2.geom) < 100"
        )
        (
            _,
            distance_conditions,
            other_functions,
            _, _, _, _, _,
        ) = extract_and_group_query_conditions(query, "trip_id")

        # 3 distinct aliases => other_functions (not distance which requires exactly 2)
        assert len(other_functions) > 0

    def test_or_condition_single_alias_is_attribute(self):
        """OR condition with single alias goes to attribute_conditions (regex remap handles it)."""
        query = (
            "SELECT rp.search_space FROM random_point rp "
            "WHERE (rp.field2 = 2 OR rp.field2 = 3) AND rp.field1 = 1"
        )
        (
            attribute_conditions,
            _,
            _other_functions,
            _,
            or_conditions,
            _, _, _,
        ) = extract_and_group_query_conditions(query, "search_space")

        # Simple single-ref condition → attribute_conditions (with alias)
        assert "rp" in attribute_conditions
        assert any("field1" in c for c in attribute_conditions["rp"])
        # Single-alias OR condition also goes to attribute_conditions with regex remap
        assert any("field2" in c for c in attribute_conditions["rp"])


class TestRegexAliasRemap:
    """Test edge cases in regex-based alias remapping during query generation.

    Verifies that alias substitution uses word boundaries to avoid false matches
    (e.g., 't1' should not match inside 't10').
    """

    def test_alias_t1_vs_t10_word_boundary(self):
        """Alias 't1' must not be falsely remapped when 't10' is also present."""
        from partitioncache.query_processor import generate_partial_queries

        query = (
            "SELECT t1.id FROM table1 t1, table10 t10 "
            "WHERE t1.val > 5 AND t10.val < 100"
        )
        variants = generate_partial_queries(
            query, "id", min_component_size=1, warn_no_partition_key=False
        )

        # Every variant referencing table10 should keep 't10' intact
        for v in variants:
            if "table10" in v:
                assert "t10" in v or "AS t" in v, (
                    f"table10 alias was corrupted by t1 remap: {v}"
                )

    def test_alias_prefix_p_vs_p0(self):
        """Alias 'p' must not collide with 'p0' during remapping."""
        from partitioncache.query_processor import generate_partial_queries

        query = (
            "SELECT p.id FROM products p, p0_region p0 "
            "WHERE p.price > 10 AND p.region_id = p0.region_id"
        )
        variants = generate_partial_queries(
            query, "region_id", min_component_size=1, warn_no_partition_key=False
        )

        # Verify we get variants and none have corrupted aliases
        assert len(variants) > 0, "Should produce at least one variant"
        for v in variants:
            # Aliases should be cleanly remapped (t1, t2, p1, etc.)
            # but never produce broken tokens like 'p00' from 'p' → 'p0' collision
            assert "p00" not in v, f"Alias collision produced 'p00': {v}"


class TestInputAliasesLookLikeCanonicalAliases:
    """Input aliases named like the canonical output aliases (t<N>, p<N>) must not collide.

    Renaming one alias after the other turned ``a.price < t1.price`` into
    ``t2.price < t2.price``: the fragment never matches, its cache entry is empty, and the
    eager path then restricts the whole query to no rows.
    """

    @staticmethod
    def _hashes(query: str, partition_key: str, **kwargs) -> list[str]:
        from partitioncache.query_processor import generate_all_query_hash_pairs

        pairs = generate_all_query_hash_pairs(query, partition_key, min_component_size=1, warn_no_partition_key=False, **kwargs)
        return sorted(h for _, h in pairs)

    def test_rename_aliases_is_single_pass(self):
        from partitioncache.query_processor import _rename_aliases

        assert _rename_aliases("a.price < t1.price", {"a": "t1", "t1": "t2"}) == "t1.price < t2.price"
        assert _rename_aliases("t2.x = t1.x", {"t2": "t1", "t1": "t2"}) == "t1.x = t2.x"
        assert _rename_aliases("t1.x + t10.x", {"t1": "t2", "t10": "t1"}) == "t2.x + t1.x"
        assert _rename_aliases("a.x, a_b.x, a", {"a": "t1"}, qualified_only=True) == "t1.x, a_b.x, a"

    def test_alias_t1_keeps_two_sided_condition(self):
        from partitioncache.query_processor import generate_partial_queries

        query = "SELECT * FROM pois AS a, pois AS t1 WHERE a.price < t1.price AND a.city_id = t1.city_id"
        variants = generate_partial_queries(query, "city_id", min_component_size=1, warn_no_partition_key=False)

        assert any("t1.price < t2.price" in v for v in variants), variants
        assert not any("t2.price < t2.price" in v for v in variants), variants
        neutral = "SELECT * FROM pois AS a, pois AS b WHERE a.price < b.price AND a.city_id = b.city_id"
        assert self._hashes(query, "city_id") == self._hashes(neutral, "city_id")

    def test_swapped_canonical_aliases(self):
        query = "SELECT * FROM pois AS t2, pois AS t1 WHERE t2.price < t1.price AND t2.city_id = t1.city_id"
        neutral = "SELECT * FROM pois AS a, pois AS b WHERE a.price < b.price AND a.city_id = b.city_id"
        assert self._hashes(query, "city_id") == self._hashes(neutral, "city_id")

    def test_spatial_partition_join_with_input_alias_p1(self):
        from partitioncache.query_processor import generate_partial_queries

        kwargs = {"auto_detect_partition_join": True, "skip_partition_key_joins": True, "warn_no_partition_key": False}
        query = "SELECT * FROM p0_cell h, pois p1 WHERE ST_DWithin(h.geom, p1.geom, 100) AND p1.kind = 'x'"
        neutral = "SELECT * FROM p0_cell h, pois b WHERE ST_DWithin(h.geom, b.geom, 100) AND b.kind = 'x'"

        variants = generate_partial_queries(query, "cell_id", **kwargs)
        assert variants == generate_partial_queries(neutral, "cell_id", **kwargs)
        assert "ST_DWITHIN(p1.geom, t1.geom, 100)" in variants[0]

    def test_preserved_select_clause_with_alias_t1(self):
        from partitioncache.query_processor import generate_partial_queries

        query = "SELECT a.name, t1.name FROM pois a, pois t1 WHERE a.city_id = t1.city_id AND a.price < t1.price"
        variants = generate_partial_queries(query, "city_id", strip_select=False, warn_no_partition_key=False)

        assert "SELECT t1.name, t2.name FROM pois AS t1, pois AS t2 WHERE t1.city_id = t2.city_id AND t1.price < t2.price" in variants


class TestAliasLikeTextInStringLiterals:
    """Text inside a string literal that looks like ``alias.column`` must neither be renamed nor steer
    alias detection; otherwise conditions are attached to the wrong table (wrong cache entries).

    Property checked: the generated variants do not depend on the literal's content.
    """

    CASES = [
        (
            "SELECT * FROM users AS u, shops AS s WHERE u.city_id = s.city_id AND u.city_id = 1 AND u.name = {L} AND DIST(u.g, s.g) < 2.5",
            "city_id",
            {},
        ),
        (
            "SELECT * FROM lineorder AS lo, customer AS c, supplier AS s WHERE lo.lo_custkey = c.c_custkey "
            "AND lo.lo_suppkey = s.s_suppkey AND c.c_region = {L} AND s.s_nation = 'X'",
            "lo_orderkey",
            {},
        ),
        ("SELECT * FROM lineorder AS lo, customer AS c WHERE lo.lo_custkey = c.c_custkey AND c.c_region = {L}", "c_custkey", {}),
        (
            "SELECT * FROM users AS u, shops AS s WHERE u.city_id = s.city_id "
            "AND u.city_id IN (SELECT z.city_id FROM zones AS z WHERE z.label = {L}) AND s.kind = 'k'",
            "city_id",
            {},
        ),
        (
            "SELECT * FROM trips AS t, pois AS p WHERE ST_DWithin(t.geom, p.geom, 100) AND p.name = {L} AND t.fare > 3",
            "geom",
            {"skip_partition_key_joins": True, "geometry_column": "geom"},
        ),
    ]
    LITERALS = ["'u.city_id'", "'s.x lo.lo_custkey c.c_custkey z.city_id t.geom p.name'", "'{pk}'"]

    @staticmethod
    def _variants(query: str, partition_key: str, kwargs: dict, literal: str) -> list[str]:
        from partitioncache.query_processor import generate_all_query_hash_pairs

        pairs = generate_all_query_hash_pairs(query, partition_key, warn_no_partition_key=False, **kwargs)
        return sorted(q.replace(literal, "'plain'") for q, _ in pairs)

    @pytest.mark.parametrize("template,partition_key,kwargs", CASES)
    @pytest.mark.parametrize("literal", LITERALS)
    def test_variants_do_not_depend_on_literal_content(self, template, partition_key, kwargs, literal):
        literal = literal.format(pk=partition_key)
        baseline = self._variants(template.format(L="'plain'"), partition_key, kwargs, "'plain'")
        with_literal = self._variants(template.format(L=literal), partition_key, kwargs, literal)

        assert baseline
        assert with_literal == baseline

    def test_literal_in_preserved_select_clause_is_not_renamed(self):
        from partitioncache.query_processor import generate_partial_queries

        query = "SELECT 'u.name' AS label, u.name FROM users u, shops s WHERE u.city_id = s.city_id AND s.kind = 'k'"
        variants = generate_partial_queries(query, "city_id", strip_select=False, warn_no_partition_key=False)

        assert variants
        assert all("'u.name' AS label" in v for v in variants)

    def test_comments_with_quotes_in_preserved_select_clause(self):
        """A quote inside a comment of the kept SELECT list must not stop alias renaming."""
        from partitioncache.query_processor import generate_all_query_hash_pairs

        query = "SELECT u.name, -- user's name\n u.id, -- user's id\n u.age FROM users u WHERE u.city_id = 1 AND u.age > 3"
        pairs = generate_all_query_hash_pairs(query, "city_id", min_component_size=1, strip_select=False, warn_no_partition_key=False)

        assert pairs
        for variant, _ in pairs:
            assert "u." not in variant and "/*" not in variant, variant
            assert variant.startswith("SELECT t1.name, t1.id, t1.age FROM users AS t1")
