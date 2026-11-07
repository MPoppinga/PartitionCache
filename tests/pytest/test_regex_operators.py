"""PostgreSQL regex match operators (~, !~, ~*, !~*) in queries: read, rewritten to REGEXP_LIKE, and hashed."""

from __future__ import annotations

from unittest.mock import Mock

import pytest
from sqlglot.errors import ParseError

from partitioncache.apply_cache import apply_cache
from partitioncache.query_processor import generate_all_hashes, generate_all_query_hash_pairs, normalize_regex_operators

BASE = "SELECT * FROM t AS a WHERE "


@pytest.mark.parametrize(
    ("condition", "expected"),
    [
        ("a.x ~ '^Bio'", "REGEXP_LIKE(a.x, '^Bio')"),
        ("a.x !~ '^Bio'", "NOT REGEXP_LIKE(a.x, '^Bio')"),
        ("a.x ~* 'bio'", "REGEXP_LIKE(a.x, 'bio', 'i')"),
        ("a.x !~* 'bio'", "NOT REGEXP_LIKE(a.x, 'bio', 'i')"),
        ("a.x ~ '^Bio' AND a.y LIKE 'a~b'", "REGEXP_LIKE(a.x, '^Bio') AND a.y LIKE 'a~b'"),
    ],
)
def test_regex_operators_become_regexp_like(condition: str, expected: str) -> None:
    assert normalize_regex_operators(BASE + condition) == BASE + expected


@pytest.mark.parametrize(
    "condition",
    ["a.s LIKE 'a~b'", "~a.i = 3", "REGEXP_LIKE(a.x, '^Bio')", "a.x = 1"],
)
def test_other_queries_are_returned_unchanged(condition: str) -> None:
    query = BASE + condition
    assert normalize_regex_operators(query) is query


def test_tilde_against_a_non_literal_is_rejected() -> None:
    """PostGIS's bounding-box containment a.geom ~ b.geom must not be read as a regex match."""
    with pytest.raises(ParseError, match="string literal"):
        normalize_regex_operators("SELECT * FROM t AS a, t AS b WHERE a.geom ~ b.geom")


def test_unparseable_query_keeps_the_parse_error() -> None:
    with pytest.raises(ParseError):
        normalize_regex_operators(BASE + "a.x ~")


SPATIAL = "SELECT p1.name FROM pois AS p1, pois AS p2 WHERE {cond} AND p2.subtype = 'school' AND ST_DWithin(p1.geom, p2.geom, 500)"
KW = {"partition_key": "spatial", "geometry_column": "geom", "skip_partition_key_joins": True, "min_component_size": 1}


def test_regex_query_generates_the_hashes_of_its_regexp_like_form() -> None:
    """a ~ 'p' and REGEXP_LIKE(a, 'p') share cache entries; ~* gets fragments PostgreSQL can execute."""
    tilde = generate_all_query_hash_pairs(SPATIAL.format(cond="p1.name ~ '^Bio'"), warn_no_partition_key=False, **KW)
    explicit = generate_all_query_hash_pairs(SPATIAL.format(cond="REGEXP_LIKE(p1.name, '^Bio')"), warn_no_partition_key=False, **KW)
    assert tilde and sorted(tilde) == sorted(explicit)
    assert any("REGEXP_LIKE(t1.name, '^Bio')" in q for q, _ in tilde)

    insensitive = generate_all_hashes(SPATIAL.format(cond="p1.name ~* 'bio'"), **KW)
    assert set(insensitive) == set(generate_all_hashes(SPATIAL.format(cond="REGEXP_LIKE(p1.name, 'bio', 'i')"), **KW))


def test_inequality_against_a_literal_stays_an_attribute_condition() -> None:
    pairs = generate_all_query_hash_pairs(SPATIAL.format(cond="p1.name <> ''"), warn_no_partition_key=False, **KW)
    assert any(q.endswith("FROM pois AS t1 WHERE t1.name <> ''") for q, _ in pairs)


def test_apply_cache_accepts_regex_operators() -> None:
    handler = Mock()
    handler.get_intersected.return_value = ({1, 2}, 1)
    query = "SELECT * FROM t AS p1 WHERE p1.name ~ '^Bio' AND p1.zipcode > 0"
    sql, stats = apply_cache(query, handler, "zipcode", min_component_size=1)
    assert stats["enhanced"] == 1
    assert "REGEXP_LIKE(p1.name, '^Bio')" in sql and "IN (1, 2)" in sql


def test_apply_cache_returns_the_callers_text_when_it_changes_nothing() -> None:
    """Without cache hits the query keeps its operators: REGEXP_LIKE needs PostgreSQL 15+."""
    handler = Mock()
    handler.get_intersected.return_value = (None, 0)
    handler.filter_existing_keys.return_value = set()
    query = "SELECT * FROM t AS p1 WHERE p1.name ~* 'bio' AND p1.zipcode > 0"
    sql, stats = apply_cache(query, handler, "zipcode", min_component_size=1)
    assert stats["enhanced"] == 0 and sql == query


def test_apply_cache_runs_postgis_tilde_queries_uncached() -> None:
    query = "SELECT * FROM t AS p1, t AS p2 WHERE p1.geom ~ p2.geom AND p1.zipcode = p2.zipcode"
    sql, stats = apply_cache(query, Mock(), "zipcode")
    assert sql == query and stats["sql_error"] == 1
