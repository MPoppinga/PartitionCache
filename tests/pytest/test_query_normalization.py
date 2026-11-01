"""
Unit tests for query normalization: JOIN ON → comma syntax, GROUP BY/HAVING removal.

Tests that clean_query() properly normalizes JOIN ON syntax to comma-separated FROM
with conditions in WHERE, and strips GROUP BY/HAVING clauses.
"""

import sqlglot
from sqlglot import exp

from partitioncache.query_processor import (
    clean_query,
    extract_and_group_query_conditions,
    generate_all_hashes,
    generate_all_query_hash_pairs,
    normalize_joins_to_cross_join,
    query_contains_top_level_outer_join,
)


class TestJoinNormalization:
    """Test JOIN ON → comma join conversion."""

    def test_simple_inner_join(self):
        """Simple INNER JOIN ON → comma join + WHERE condition."""
        query = """
        SELECT t.trip_id
        FROM taxi_trips t
        JOIN pois p ON ST_DWithin(t.geom, p.geom, 500)
        WHERE t.fare > 10
        """
        result = clean_query(query)

        # Should not contain JOIN keyword
        assert "JOIN" not in result.upper().split("WHERE")[0]
        # Should have both tables in FROM
        assert "taxi_trips" in result
        assert "pois" in result
        # Should have original WHERE condition
        assert "fare" in result
        # Should have join condition in WHERE
        assert "ST_DWITHIN" in result.upper() or "st_dwithin" in result.lower()

    def test_multiple_joins(self):
        """Multiple JOINs should all be converted."""
        query = """
        SELECT t.trip_id
        FROM taxi_trips t
        JOIN pois p1 ON ST_DWithin(t.pickup_geom, p1.geom, 500)
        JOIN pois p2 ON ST_DWithin(t.dropoff_geom, p2.geom, 500)
        WHERE t.fare > 10
        """
        result = clean_query(query)

        # No JOINs in FROM clause
        from_clause = result.upper().split("WHERE")[0]
        assert "JOIN" not in from_clause
        # All tables present
        assert "taxi_trips" in result
        # Both join conditions in WHERE
        where_part = result.upper().split("WHERE")[1] if "WHERE" in result.upper() else ""
        assert where_part.count("ST_DWITHIN") == 2

    def test_no_join_query_unchanged(self):
        """Queries without JOINs should be unaffected."""
        query = "SELECT t.trip_id FROM taxi_trips t WHERE t.fare > 10"
        result = clean_query(query)
        assert "taxi_trips" in result
        assert "fare" in result

    def test_join_with_equality_condition(self):
        """JOIN with equality condition (e.g., ON t.id = p.trip_id)."""
        query = """
        SELECT t.trip_id
        FROM taxi_trips t
        JOIN payments p ON t.trip_id = p.trip_id
        WHERE t.fare > 10
        """
        result = clean_query(query)

        from_clause = result.upper().split("WHERE")[0]
        assert "JOIN" not in from_clause
        assert "taxi_trips" in result
        assert "payments" in result

    def test_left_join_not_converted(self):
        """LEFT JOINs must NOT be rewritten to comma joins.

        The comma-join rewrite has inner-join semantics and would drop preserved-side
        rows without a join partner, which could exclude valid partitions (false
        negatives). Outer joins are therefore left intact by normalization.
        """
        query = """
        SELECT t.trip_id
        FROM taxi_trips t
        LEFT JOIN pois p ON ST_DWithin(t.geom, p.geom, 500)
        WHERE t.fare > 10
        """
        result = normalize_joins_to_cross_join(query)
        # The outer join is preserved, not flattened into the WHERE clause
        assert "LEFT JOIN" in result.upper()
        assert "taxi_trips" in result
        assert "pois" in result


class TestGroupByHavingRemoval:
    """Test GROUP BY and HAVING removal."""

    def test_group_by_removal(self):
        """GROUP BY should be removed."""
        query = """
        SELECT city_id, COUNT(*) as cnt
        FROM users
        WHERE active = true
        GROUP BY city_id
        """
        result = clean_query(query)
        assert "GROUP BY" not in result.upper()
        assert "active" in result.upper() or "ACTIVE" in result.upper()

    def test_having_removal(self):
        """HAVING should be removed."""
        query = """
        SELECT city_id, COUNT(*) as cnt
        FROM users
        WHERE active = true
        GROUP BY city_id
        HAVING COUNT(*) > 10
        """
        result = clean_query(query)
        assert "GROUP BY" not in result.upper()
        assert "HAVING" not in result.upper()

    def test_order_by_still_removed(self):
        """ORDER BY removal (existing) should still work with GROUP BY removal."""
        query = """
        SELECT city_id, COUNT(*) as cnt
        FROM users
        WHERE active = true
        GROUP BY city_id
        HAVING COUNT(*) > 10
        ORDER BY cnt DESC
        LIMIT 20
        """
        result = clean_query(query)
        assert "GROUP BY" not in result.upper()
        assert "HAVING" not in result.upper()
        assert "ORDER BY" not in result.upper()
        assert "LIMIT" not in result.upper()
        # WHERE should still be present
        assert "WHERE" in result.upper()

    def test_where_preserved_after_group_by_removal(self):
        """WHERE conditions must survive GROUP BY/HAVING removal."""
        query = """
        SELECT department, AVG(salary)
        FROM employees
        WHERE hire_date > '2020-01-01'
        GROUP BY department
        HAVING AVG(salary) > 50000
        """
        result = clean_query(query)
        assert "GROUP BY" not in result.upper()
        assert "HAVING" not in result.upper()
        assert "hire_date" in result


class TestJoinAndGroupByInteraction:
    """Test that JOIN normalization + GROUP BY removal work together."""

    def test_join_with_group_by(self):
        """Full analytical query: JOIN + GROUP BY + HAVING + ORDER BY."""
        query = """
        SELECT p.name, COUNT(*) as trip_count, AVG(t.fare) as avg_fare
        FROM taxi_trips t
        JOIN pois p ON ST_DWithin(t.pickup_geom, p.geom, 500)
        WHERE t.fare > 5
        GROUP BY p.name
        HAVING COUNT(*) > 10
        ORDER BY trip_count DESC
        LIMIT 20
        """
        result = clean_query(query)

        # JOIN normalized
        from_clause = result.upper().split("WHERE")[0]
        assert "JOIN" not in from_clause

        # GROUP BY, HAVING, ORDER BY, LIMIT removed
        assert "GROUP BY" not in result.upper()
        assert "HAVING" not in result.upper()
        assert "ORDER BY" not in result.upper()
        assert "LIMIT" not in result.upper()

        # WHERE condition and join condition preserved
        assert "fare" in result
        assert "ST_DWITHIN" in result.upper() or "st_dwithin" in result.lower()


class TestFragmentEquivalence:
    """Test that original and adapted queries produce equivalent fragments."""

    def test_adapted_vs_original_produce_same_conditions(self):
        """An adapted query (flat join) and original (JOIN ON) should extract same conditions."""
        original = """
        SELECT t.trip_id
        FROM taxi_trips t
        JOIN pois p ON ST_DWithin(t.pickup_geom, p.geom, 500)
        WHERE t.fare > 10
        AND t.trip_distance > 1
        """

        adapted = """
        SELECT t.trip_id
        FROM taxi_trips t, pois p
        WHERE ST_DWithin(t.pickup_geom, p.geom, 500)
        AND t.fare > 10
        AND t.trip_distance > 1
        """

        cleaned_original = clean_query(original)
        cleaned_adapted = clean_query(adapted)

        # Both should produce the same conditions after extraction
        orig_result = extract_and_group_query_conditions(cleaned_original, "trip_id")
        adapted_result = extract_and_group_query_conditions(cleaned_adapted, "trip_id")

        # attribute_conditions should match
        assert dict(orig_result[0]) == dict(adapted_result[0])
        # distance_conditions should match
        assert dict(orig_result[1]) == dict(adapted_result[1])

    def test_hash_equivalence(self):
        """Original and adapted queries should produce the same hashes."""
        original = """
        SELECT t.trip_id
        FROM taxi_trips t
        JOIN pois p ON ST_DWithin(t.pickup_geom, p.geom, 500)
        WHERE t.fare > 10
        """

        adapted = """
        SELECT t.trip_id
        FROM taxi_trips t, pois p
        WHERE ST_DWithin(t.pickup_geom, p.geom, 500)
        AND t.fare > 10
        """

        orig_hashes = set(h for _, h in generate_all_query_hash_pairs(
            original, "trip_id", warn_no_partition_key=False
        ))
        adapted_hashes = set(h for _, h in generate_all_query_hash_pairs(
            adapted, "trip_id", warn_no_partition_key=False
        ))

        assert orig_hashes == adapted_hashes


class TestJoinNormalizationSubqueryScope:
    """JOIN normalization must only affect the outermost SELECT scope,
    not JOINs inside subqueries (EXISTS, IN, scalar subqueries, CTEs)."""

    @staticmethod
    def _get_outer_top_level_conditions(sql: str) -> list[exp.Expression]:
        """Parse SQL and return only the top-level WHERE conditions of the outermost SELECT,
        excluding conditions nested inside EXISTS/IN subqueries."""
        parsed = sqlglot.parse_one(sql)
        outer_select = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
        assert outer_select is not None
        outer_where = outer_select.args.get("where")
        if not outer_where:
            return []
        where_expr = outer_where.this
        if isinstance(where_expr, exp.And):
            return list(where_expr.flatten())
        return [where_expr]

    def test_join_inside_exists_not_promoted_to_outer_where(self):
        """JOIN ON inside an EXISTS subquery must stay inside the subquery."""
        query = (
            "SELECT t.trip_id FROM taxi_trips AS t "
            "WHERE t.fare > 10 "
            "AND EXISTS ("
            "SELECT 1 FROM pois AS p JOIN regions AS r ON p.region_id = r.id "
            "WHERE ST_DWithin(t.geom, p.geom, 500)"
            ")"
        )
        from partitioncache.query_processor import normalize_joins_to_cross_join

        result = normalize_joins_to_cross_join(query)

        # Get top-level conditions of the outer WHERE
        conditions = self._get_outer_top_level_conditions(result)

        # The outer FROM only has alias 't'. Any top-level condition referencing
        # 'r' or having 'region_id' means an inner condition leaked out.
        for cond in conditions:
            # Skip EXISTS/IN nodes — those legitimately contain subquery refs
            if cond.find(exp.Exists) or cond.find(exp.In):
                continue
            cond_sql = cond.sql()
            referenced_tables = {col.table for col in cond.find_all(exp.Column) if col.table}
            assert "r" not in referenced_tables, (
                f"Inner join condition leaked to outer WHERE as top-level condition: {cond_sql}\n"
                f"Full result: {result}"
            )

    def test_join_inside_in_subquery_not_promoted(self):
        """JOIN ON inside an IN-subquery must stay inside the subquery."""
        query = (
            "SELECT t.trip_id FROM taxi_trips AS t "
            "WHERE t.region_id IN ("
            "SELECT r.id FROM regions AS r "
            "JOIN countries AS c ON r.country_id = c.id "
            "WHERE c.name = 'Germany'"
            ")"
        )
        from partitioncache.query_processor import normalize_joins_to_cross_join

        result = normalize_joins_to_cross_join(query)

        # Get top-level conditions of the outer WHERE
        conditions = self._get_outer_top_level_conditions(result)

        # 'c' and 'country_id' should not appear as a standalone outer condition
        for cond in conditions:
            if cond.find(exp.Exists) or cond.find(exp.In):
                continue
            referenced_tables = {col.table for col in cond.find_all(exp.Column) if col.table}
            assert "c" not in referenced_tables, (
                f"Inner join condition leaked to outer WHERE: {cond.sql()}\n"
                f"Full result: {result}"
            )

    def test_clean_query_with_nested_join_produces_valid_sql(self):
        """clean_query on a query with JOINs inside EXISTS must produce valid SQL."""
        query = (
            "SELECT t.trip_id FROM taxi_trips AS t "
            "WHERE t.fare > 10 "
            "AND EXISTS ("
            "SELECT 1 FROM pois AS p JOIN regions AS r ON p.region_id = r.id "
            "WHERE ST_DWithin(t.geom, p.geom, 500)"
            ")"
        )
        result = clean_query(query)

        # Must be parseable (valid SQL)
        parsed = sqlglot.parse_one(result)
        assert parsed is not None

        # The EXISTS subquery must still exist in the result
        assert "EXISTS" in result.upper()


class TestStripSelectPreservation:
    """generate_all_query_hash_pairs() calls clean_query() which rewrites SELECT
    to *. When strip_select=False, the original columns should be preserved."""

    @staticmethod
    def _extract_select_clause(sql: str) -> str:
        """Extract the SELECT clause (between SELECT and FROM) from a SQL string."""
        parsed = sqlglot.parse_one(sql)
        select_stmt = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
        if select_stmt and select_stmt.expressions:
            return ", ".join(e.sql() for e in select_stmt.expressions)
        return ""

    def test_strip_select_false_preserves_columns(self):
        """With strip_select=False, fragment SELECT clauses should contain original column names."""
        query = (
            "SELECT t.trip_id, t.fare, t.duration "
            "FROM taxi_trips AS t "
            "WHERE t.fare > 10 AND t.city_id = 5"
        )
        pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key="city_id",
            strip_select=False,
        )

        assert len(pairs) > 0

        # Check only the SELECT clause of each fragment (not WHERE clause)
        all_queries = [q for q, _ in pairs]
        select_clauses = [self._extract_select_clause(q) for q in all_queries]
        has_original_columns = any(
            "trip_id" in sc.lower() or "fare" in sc.lower() or "duration" in sc.lower()
            for sc in select_clauses
        )
        assert has_original_columns, (
            f"strip_select=False should preserve original SELECT columns, "
            f"but SELECT clauses are: {select_clauses}"
        )

    def test_strip_select_true_uses_partition_key_select(self):
        """With strip_select=True (default), fragment SELECT uses partition key only."""
        query = (
            "SELECT t.trip_id, t.fare, t.duration "
            "FROM taxi_trips AS t "
            "WHERE t.fare > 10 AND t.city_id = 5"
        )
        pairs = generate_all_query_hash_pairs(
            query=query,
            partition_key="city_id",
            strip_select=True,
        )
        assert len(pairs) > 0

        # Check SELECT clauses don't have trip_id (only partition key)
        all_queries = [q for q, _ in pairs]
        select_clauses = [self._extract_select_clause(q) for q in all_queries]
        has_trip_id = any("trip_id" in sc.lower() for sc in select_clauses)
        assert not has_trip_id, (
            f"strip_select=True should NOT preserve trip_id in SELECT: {select_clauses}"
        )


class TestOuterJoinExclusion:
    """Outer joins (LEFT/RIGHT/FULL) must be excluded from variant generation.

    Rewriting them into comma-joins has inner-join semantics and drops preserved-side
    rows without a join partner. The resulting cached partition-key set would be a subset
    of the original query's set, so applying such a cache entry could exclude valid
    partitions (false negatives). Such queries must produce no variants and be executed
    unchanged.
    """

    def test_detector_true_for_top_level_outer_joins(self):
        assert query_contains_top_level_outer_join("SELECT a.x FROM a LEFT JOIN b ON a.id = b.id")
        assert query_contains_top_level_outer_join("SELECT a.x FROM a RIGHT JOIN b ON a.id = b.id")
        assert query_contains_top_level_outer_join("SELECT a.x FROM a FULL JOIN b ON a.id = b.id")
        assert query_contains_top_level_outer_join("SELECT a.x FROM a LEFT OUTER JOIN b ON a.id = b.id")

    def test_detector_false_for_inner_and_comma_joins(self):
        assert not query_contains_top_level_outer_join("SELECT a.x FROM a JOIN b ON a.id = b.id")
        assert not query_contains_top_level_outer_join("SELECT a.x FROM a INNER JOIN b ON a.id = b.id")
        assert not query_contains_top_level_outer_join("SELECT a.x FROM a, b WHERE a.id = b.id")
        assert not query_contains_top_level_outer_join("SELECT a.x FROM a WHERE a.x > 5")

    def test_detector_false_for_outer_join_inside_subquery(self):
        """An outer join nested in EXISTS/IN is preserved verbatim and is safe."""
        exists_q = (
            "SELECT a.x FROM a WHERE EXISTS "
            "(SELECT 1 FROM b LEFT JOIN c ON b.id = c.id WHERE b.a_id = a.id)"
        )
        in_q = (
            "SELECT a.x FROM a WHERE a.id IN "
            "(SELECT b.a_id FROM b LEFT JOIN c ON b.id = c.id)"
        )
        assert not query_contains_top_level_outer_join(exists_q)
        assert not query_contains_top_level_outer_join(in_q)

    def test_detector_false_on_unparseable(self):
        assert not query_contains_top_level_outer_join("this is not sql")

    def test_generate_all_query_hash_pairs_empty_for_outer_join(self):
        query = (
            "SELECT o.region FROM orders o "
            "LEFT JOIN shipments s ON o.id = s.order_id "
            "WHERE o.region > 5"
        )
        assert generate_all_query_hash_pairs(query, "region", warn_no_partition_key=False) == []

    def test_generate_all_hashes_empty_for_outer_join(self):
        query = (
            "SELECT o.region FROM orders o "
            "LEFT JOIN shipments s ON o.id = s.order_id "
            "WHERE o.region > 5"
        )
        assert generate_all_hashes(query, "region", warn_no_partition_key=False) == []

    def test_inner_join_still_generates_variants(self):
        """Regression: inner joins must still decompose into cache variants."""
        query = (
            "SELECT o.region FROM orders o "
            "JOIN shipments s ON o.id = s.order_id "
            "WHERE o.region > 5"
        )
        pairs = generate_all_query_hash_pairs(query, "region", warn_no_partition_key=False)
        assert len(pairs) > 0


class TestStringLiteralPreservation:
    """clean_query() must not rewrite string literals (otherwise non-equivalent queries share a hash)."""

    BASE = "SELECT * FROM users AS u WHERE u.city_id = 1 AND u.name = {literal}"

    def _hashes(self, query: str) -> set[str]:
        return set(generate_all_hashes(query, "city_id", warn_no_partition_key=False))

    def test_doubled_quote_inside_literal_survives_bucketing(self):
        from partitioncache.query_processor import normalize_distance_conditions

        query = "SELECT * FROM t AS a, t AS b WHERE a.name = 'it''s  two' AND DIST(a.g, b.g) BETWEEN 1.6 AND 3.6"
        normalized = normalize_distance_conditions(query, bucket_steps=1.0)
        assert "'it''s  two'" in normalized
        assert "BETWEEN 1 AND 4" in normalized

    def test_whitespace_inside_literal_is_kept(self):
        double_space = clean_query(self.BASE.format(literal="'a  b'"))
        single_space = clean_query(self.BASE.format(literal="'a b'"))

        assert "'a  b'" in double_space
        assert double_space != single_space
        # Variants that keep the condition must keep the literal byte for byte
        # (the distance-bucketing variant used to collapse it to 'a b')
        variants = [q for q, _ in generate_all_query_hash_pairs(self.BASE.format(literal="'a  b'"), "city_id", warn_no_partition_key=False)]
        assert not any("'a b'" in q for q in variants)
        assert self._hashes(self.BASE.format(literal="'a  b'")) != self._hashes(self.BASE.format(literal="'a b'"))

    def test_equals_sign_inside_literal_is_kept(self):
        spaced = clean_query(self.BASE.format(literal="'x = y'"))
        compact = clean_query(self.BASE.format(literal="'x=y'"))

        assert "'x = y'" in spaced
        assert spaced != compact

    def test_double_dash_inside_literal_is_not_a_comment(self):
        cleaned = clean_query("SELECT * FROM users AS u WHERE u.name = 'a--b' AND u.city_id = 1")

        assert "'a--b'" in cleaned
        assert "u.city_id = 1" in cleaned

    def test_quote_characters_inside_literal_are_kept(self):
        assert "'a\"b'" in clean_query(self.BASE.format(literal="'a\"b'"))
        assert "'a`b'" in clean_query(self.BASE.format(literal="'a`b'"))

    def test_quoted_identifiers_are_still_unquoted(self):
        quoted = clean_query('SELECT * FROM "users" AS "u" WHERE "u"."city_id" = 1')

        assert quoted == clean_query("SELECT * FROM users AS u WHERE u.city_id = 1")

    def test_comments_and_layout_do_not_change_the_result(self):
        plain = "SELECT * FROM users AS u WHERE u.city_id = 1 AND u.age > 30"
        commented = """
        SELECT *   -- all columns
        FROM users AS u /* block comment */
        WHERE u.city_id=1
          AND u.age > 30;
        """

        assert clean_query(commented) == clean_query(plain)
        assert self._hashes(commented) == self._hashes(plain)
