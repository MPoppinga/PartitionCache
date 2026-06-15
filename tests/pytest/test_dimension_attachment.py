"""
Tests for dimension-table attachment in data-warehouse style queries.

Covers the reclassification of attachment joins (fact.partition_key = dim.column,
e.g. ``lo.lo_custkey = c.c_custkey``) as join-graph edges, the pk-bearing
combination filter, and the assembly changes (pk-equijoin injection restricted to
pk-bearing aliases, SELECT alias selection, pk-condition remap).

Query fixtures mirror examples/ssb_benchmark/queries/original/ (inlined for test
independence).
"""

import sqlglot
from sqlglot import exp

from partitioncache.query_processor import (
    clean_query,
    extract_and_group_query_conditions,
    generate_all_query_hash_pairs,
    generate_partial_queries,
)

# SSB Q7.1: single fact-dimension join via partition key (lo_custkey)
SSB_Q7_1 = """
SELECT c.c_nation, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c
WHERE lo.lo_custkey = c.c_custkey
  AND c.c_region = 'AMERICA'
GROUP BY c.c_nation
ORDER BY revenue DESC
"""

# SSB Q3.1: three dimensions, three different fact FK columns
SSB_Q3_1 = """
SELECT c.c_nation, s.s_nation, d.d_year, SUM(lo.lo_revenue) AS revenue
FROM lineorder lo, customer c, supplier s, date_dim d
WHERE lo.lo_custkey = c.c_custkey
  AND lo.lo_suppkey = s.s_suppkey
  AND lo.lo_orderdate = d.d_datekey
  AND c.c_region = 'ASIA'
  AND s.s_region = 'ASIA'
  AND d.d_year >= 1992 AND d.d_year <= 1997
GROUP BY c.c_nation, s.s_nation, d.d_year
ORDER BY d.d_year ASC, revenue DESC
"""

# SSB Q1.1: date attributes sort before lineorder attributes in fragment assembly
SSB_Q1_1 = """
SELECT SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM lineorder lo, date_dim d
WHERE lo.lo_orderdate = d.d_datekey
  AND d.d_year = 1993
  AND lo.lo_discount >= 1 AND lo.lo_discount <= 3
  AND lo.lo_quantity < 25
"""


def _extract(query: str, partition_key: str):
    return extract_and_group_query_conditions(clean_query(query), partition_key)


# ======================================================================
# Phase 1: classification of attachment joins
# ======================================================================
class TestAttachmentJoinClassification:
    """Attachment joins (alias.pk = other_alias.other_column) must become edges."""

    def test_fk_join_routed_to_distance_conditions(self):
        attribute_conditions, distance_conditions, _other, pk_conditions, _or, aliases, _a2t, pk_joins = _extract(SSB_Q7_1, "lo_custkey")

        assert dict(pk_conditions) == {}, f"FK join must not be a partition key condition: {dict(pk_conditions)}"
        assert dict(pk_joins) == {}
        assert ("c", "lo") in distance_conditions, f"FK join must form an edge, got edges: {list(distance_conditions)}"
        edge_conditions = distance_conditions[("c", "lo")]
        assert len(edge_conditions) == 1
        assert "lo_custkey" in edge_conditions[0] and "c_custkey" in edge_conditions[0]
        assert attribute_conditions["c"] == ["c.c_region = 'AMERICA'"]
        assert set(aliases) == {"lo", "c"}

    def test_fk_join_reversed_operands_routed(self):
        query = SSB_Q7_1.replace("lo.lo_custkey = c.c_custkey", "c.c_custkey = lo.lo_custkey")
        _attr, distance_conditions, _other, pk_conditions, _or, _aliases, _a2t, _pk_joins = _extract(query, "lo_custkey")

        assert dict(pk_conditions) == {}
        assert ("c", "lo") in distance_conditions

    def test_attachment_join_for_other_partition_key_unaffected(self):
        """For a different partition key the same join is an ordinary edge (today's behavior)."""
        _attr, distance_conditions, _other, pk_conditions, _or, _aliases, _a2t, _pk_joins = _extract(SSB_Q7_1, "lo_suppkey")

        assert dict(pk_conditions) == {}
        assert ("c", "lo") in distance_conditions

    def test_pk_pk_join_still_partition_key_join(self):
        query = """
        SELECT a.zipcode FROM pois AS a, pois AS b
        WHERE a.zipcode = b.zipcode AND a.kind = 'cafe' AND b.kind = 'bar'
        """
        _attr, distance_conditions, _other, pk_conditions, _or, _aliases, _a2t, pk_joins = _extract(query, "zipcode")

        assert ("a", "b") in pk_joins, f"pk=pk equijoin must stay a partition key join: {dict(pk_joins)}"
        assert dict(distance_conditions) == {}
        assert dict(pk_conditions) == {}

    def test_single_alias_pk_conditions_unchanged(self):
        query = """
        SELECT lo.lo_custkey FROM lineorder AS lo
        WHERE lo.lo_custkey IN (SELECT c_custkey FROM customer WHERE c_region = 'ASIA')
          AND lo.lo_custkey BETWEEN 10 AND 90
          AND lo.lo_discount = 3
        """
        _attr, distance_conditions, _other, pk_conditions, _or, _aliases, _a2t, _pk_joins = _extract(query, "lo_custkey")

        assert "lo" in pk_conditions
        # clean_query splits BETWEEN into <= and >= -> 3 pk conditions (2 bounds + IN subquery)
        assert len(pk_conditions["lo"]) == 3, f"IN subquery and BETWEEN bounds must stay pk conditions: {pk_conditions['lo']}"
        assert dict(distance_conditions) == {}

    def test_fk_join_with_expression_not_reclassified(self):
        query = SSB_Q7_1.replace("lo.lo_custkey = c.c_custkey", "lo.lo_custkey = c.c_custkey + 1")
        _attr, distance_conditions, _other, pk_conditions, _or, _aliases, _a2t, _pk_joins = _extract(query, "lo_custkey")

        assert "c" in pk_conditions or "lo" in pk_conditions, "non-plain-column equality must keep current classification"
        assert ("c", "lo") not in distance_conditions


# ======================================================================
# Phase 2: edge connectivity, combination filter, pattern size
# ======================================================================
class TestPkBearingCombinationFilter:
    def test_attachment_edge_connects_dimension(self):
        """The reclassified FK join must act as a graph edge in connected-subgraph enumeration."""
        fragments = generate_partial_queries(
            clean_query(SSB_Q7_1), "lo_custkey", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        two_table = [f for f in fragments if "lineorder" in f and "customer" in f]
        assert len(two_table) >= 1, f"Expected a fragment spanning both tables, got: {fragments}"

    def test_dimension_only_fragments_dropped(self):
        fragments = generate_partial_queries(
            clean_query(SSB_Q3_1), "lo_custkey", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        assert fragments, "Fragments must be generated"
        for f in fragments:
            assert "lineorder" in f, f"Every fragment must contain the fact table: {f}"

    def test_exact_fragment_count_ssb_q3(self):
        """{lo} x all dimension subsets = 8 fragments; proves no pattern-size explosion."""
        fragments = generate_partial_queries(
            clean_query(SSB_Q3_1), "lo_custkey", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        assert len(set(fragments)) == 8, f"Expected 8 fragments, got {len(set(fragments))}: {sorted(fragments, key=len)}"

    def test_fragment_count_bounded_star_with_four_dims(self):
        query = """
        SELECT f.part_id FROM facts AS f, d_one AS a, d_two AS b, d_three AS c, d_four AS d
        WHERE f.part_id = a.a_id AND f.b_ref = b.b_id AND f.c_ref = c.c_id AND f.d_ref = d.d_id
          AND a.x = 1 AND b.x = 2 AND c.x = 3 AND d.x = 4 AND f.size > 0
        """
        fragments = generate_partial_queries(
            clean_query(query), "part_id", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        assert len(set(fragments)) == 16, f"Star with 4 dims must yield 2^4 fragments, got {len(set(fragments))}"

        capped = generate_partial_queries(
            clean_query(query), "part_id", min_component_size=1, follow_graph=True, warn_no_partition_key=False, max_component_size=2
        )
        assert len(set(capped)) == 5, f"max_component_size=2 must cap to fact + 4 pairs, got {len(set(capped))}"

    def test_gate_requires_attachment_join(self):
        """A pk literal alone (no attachment join) must not change behavior (gating proof)."""
        query = (
            "SELECT * FROM products AS p, categories AS c "
            "WHERE p.category_id = c.id AND p.price > 100 AND c.name = 'electronics' AND p.city_id = 42"
        )
        fragments = generate_partial_queries(
            clean_query(query), "city_id", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        categories_only = [f for f in fragments if "categories" in f and "products" not in f]
        assert len(categories_only) >= 1, "Without an attachment join, dimension-only fragments must remain (today's behavior)"
        two_table = [f for f in fragments if "categories" in f and "products" in f]
        assert any("city_id = " in f and f.count("city_id") >= 2 for f in two_table), "pk equijoin injection must remain without the gate"

    def test_pk_absent_keeps_all_combinations(self):
        query = "SELECT t1.pdb_id FROM tab1 AS t1, tab2 AS t2 WHERE t1.x = t2.x AND t1.y = 5 AND t2.z = 7"
        fragments = generate_partial_queries(
            clean_query(query), "pdb_id", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        singles = [f for f in fragments if ("tab1" in f) != ("tab2" in f)]
        assert len(singles) == 2, f"Single-table fragments must remain when pk is only in SELECT: {fragments}"
        two_table = [f for f in fragments if "tab1" in f and "tab2" in f]
        assert any("pdb_id = " in f for f in two_table), "pk equijoin injection must remain when no alias is pk-bearing"


# ======================================================================
# Phase 3: assembly (equijoin injection, SELECT alias, pk-condition remap)
# ======================================================================
def _referenced_aliases_valid(fragment: str) -> bool:
    """Every alias referenced in a column must be declared in the fragment's FROM."""
    parsed = sqlglot.parse_one(fragment)
    declared = {t.alias_or_name for t in parsed.find_all(exp.Table)}
    referenced = {col.table for col in parsed.find_all(exp.Column) if col.table}
    return referenced <= declared


class TestAttachmentAssembly:
    def test_no_pk_equijoin_injected_onto_dimension(self):
        fragments = generate_partial_queries(
            clean_query(SSB_Q7_1), "lo_custkey", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        for f in fragments:
            parsed = sqlglot.parse_one(f)
            lineorder_aliases = {t.alias_or_name for t in parsed.find_all(exp.Table) if t.name == "lineorder"}
            for col in parsed.find_all(exp.Column):
                if col.name == "lo_custkey":
                    assert col.table in lineorder_aliases, f"lo_custkey referenced on non-lineorder alias '{col.table}' in: {f}"

    def test_fk_join_preserved_in_two_table_fragment(self):
        fragments = generate_partial_queries(
            clean_query(SSB_Q7_1), "lo_custkey", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        two_table = [f for f in fragments if "lineorder" in f and "customer" in f]
        assert len(two_table) == 1
        fragment = two_table[0]
        assert "c_custkey" in fragment and "lo_custkey" in fragment, f"FK join must be preserved: {fragment}"
        assert "c_region = 'AMERICA'" in fragment, f"Dimension filter must be preserved: {fragment}"

    def test_select_uses_fact_alias_when_dimension_sorts_first(self):
        fragments = generate_partial_queries(
            clean_query(SSB_Q1_1), "lo_orderdate", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        for f in fragments:
            parsed = sqlglot.parse_one(f)
            lineorder_aliases = {t.alias_or_name for t in parsed.find_all(exp.Table) if t.name == "lineorder"}
            select_col = parsed.selects[0].find(exp.Column)
            assert select_col is not None
            if "lineorder" in f:
                assert select_col.table in lineorder_aliases, f"SELECT must use the fact alias: {f}"

    def test_all_fragments_valid_for_all_ssb_partition_keys(self):
        for pk in ["lo_custkey", "lo_suppkey", "lo_orderdate"]:
            fragments = generate_partial_queries(
                clean_query(SSB_Q3_1), pk, min_component_size=1, follow_graph=True, warn_no_partition_key=False
            )
            assert fragments, f"No fragments for {pk}"
            for f in fragments:
                assert _referenced_aliases_valid(f), f"Dangling alias reference for pk={pk} in: {f}"

    def test_pk_condition_remapped_to_pk_bearing_alias(self):
        # lo.lo_quantity sorts after c.c_region, so customer becomes t1 in the
        # two-table fragment — the remap must still target the lineorder alias.
        query = SSB_Q7_1.replace(
            "AND c.c_region = 'AMERICA'",
            "AND c.c_region = 'AMERICA' AND lo.lo_quantity < 25 AND lo.lo_custkey IN (1, 2, 3)",
        )
        fragments = generate_partial_queries(
            clean_query(query), "lo_custkey", min_component_size=1, follow_graph=True, warn_no_partition_key=False
        )
        with_in = [f for f in fragments if "IN (1, 2, 3)" in f]
        assert with_in, f"Variants with the pk IN condition must exist: {fragments}"
        for f in with_in:
            parsed = sqlglot.parse_one(f)
            lineorder_aliases = {t.alias_or_name for t in parsed.find_all(exp.Table) if t.name == "lineorder"}
            for in_expr in parsed.find_all(exp.In):
                col = in_expr.this
                assert isinstance(col, exp.Column) and col.name == "lo_custkey"
                assert col.table in lineorder_aliases, f"pk IN condition must attach to the fact alias: {f}"
            assert _referenced_aliases_valid(f), f"Dangling alias in pk-condition variant: {f}"

    def test_fragment_generation_deterministic(self):
        runs = [sorted(generate_all_query_hash_pairs(SSB_Q3_1, "lo_custkey", min_component_size=1, follow_graph=True)) for _ in range(2)]
        assert runs[0] == runs[1]
