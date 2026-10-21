"""Behaviour that must not change with the sqlglot version.

Fragment hashes are computed from sqlglot's rendering, so a sqlglot upgrade must not alter
how a query is parsed or printed. sqlglot 30 changed two things this test file pins down:

- ``ST_MakePoint`` and ``ST_Point`` are parsed into one ``StPoint`` node and printed as
  ``ST_POINT``. In PostGIS the third argument of ``ST_Point`` is the SRID, so
  ``ST_MakePoint(x, y, z)`` would silently turn into a 2D point with SRID ``z``.
- ``exp.Or`` and ``exp.And`` became subclasses of ``exp.Func``, which moved a cross-alias
  OR condition into the join graph.
"""

import pytest
import sqlglot

from partitioncache.query_processor import extract_and_group_query_conditions, generate_all_query_hash_pairs


class TestSpatialFunctionRendering:
    @pytest.mark.parametrize("dialect", [None, "postgres"])
    @pytest.mark.parametrize(
        "sql, expected",
        [
            ("SELECT ST_MakePoint(10.0, 51.0)", "SELECT ST_MAKEPOINT(10.0, 51.0)"),
            ("SELECT ST_MakePoint(1, 2, 3)", "SELECT ST_MAKEPOINT(1, 2, 3)"),
            ("SELECT ST_MakePoint(1, 2, 3, 4)", "SELECT ST_MAKEPOINT(1, 2, 3, 4)"),
            ("SELECT ST_Point(1, 2, 4326)", "SELECT ST_POINT(1, 2, 4326)"),
            ("SELECT ST_Point(1, 2)", "SELECT ST_POINT(1, 2)"),
            ("SELECT ST_Distance(a.geom, b.geom)", "SELECT ST_DISTANCE(a.geom, b.geom)"),
            ("SELECT ST_Distance(a.geom, b.geom, TRUE)", "SELECT ST_DISTANCE(a.geom, b.geom, TRUE)"),
        ],
    )
    def test_function_keeps_its_name_and_arguments(self, sql, expected, dialect):
        assert sqlglot.parse_one(sql, read=dialect).sql(dialect=dialect) == expected

    def test_fragment_keeps_st_makepoint(self):
        query = (
            "SELECT p1.zipcode FROM pois p1 WHERE p1.name LIKE '%Museum%' "
            "AND ST_DWithin(p1.geom, ST_Transform(ST_SetSRID(ST_MakePoint(10.0, 51.0), 4326), 25832), 50000)"
        )
        fragments = [q for q, _ in generate_all_query_hash_pairs(query, "zipcode", min_component_size=1, warn_no_partition_key=False)]
        assert fragments
        assert all("ST_MAKEPOINT(10.0, 51.0)" in f and "ST_POINT" not in f for f in fragments)


class TestOrConditionsAreNotFunctions:
    def _groups(self, condition: str):
        query = f"SELECT p1.id FROM pois p1, pois p2 WHERE p1.type = 'a' AND p2.type = 'b' AND {condition}"
        _attribute, distance, other, _pk, or_conditions, _aliases, _map, _joins = extract_and_group_query_conditions(query, "zipcode")
        return distance, other, or_conditions

    def test_cross_alias_or_is_an_or_condition(self):
        distance, _other, or_conditions = self._groups("(p1.x = 1 OR p2.x = 2)")
        assert distance == {}
        assert or_conditions == {("p1", "p2"): ["(p1.x = 1 OR p2.x = 2)"]}

    def test_or_containing_a_function_stays_a_distance_condition(self):
        distance, _other, or_conditions = self._groups("(ST_DWithin(p1.geom, p2.geom, 5) OR p1.x = 1)")
        assert distance == {("p1", "p2"): ["(ST_DWITHIN(p1.geom, p2.geom, 5) OR p1.x = 1)"]}
        assert or_conditions == {}
