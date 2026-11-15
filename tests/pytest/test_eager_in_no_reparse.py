"""
Equivalence and performance tests for the eager IN / VALUES rewrite in
extend_query_with_partition_keys.

Background: the eager IN and VALUES branches used to build the full key list as text and hand it
to sqlglot (`parse_one(f"{alias}.{key} IN ({keys})")`). Parsing is linear in the number of keys
but with a large constant, so a 100k-key cache hit spent seconds in the rewrite. The branches now
parse a unique placeholder identifier and splice the formatted key list into the rendered SQL.

`_reference_old_implementation` below reproduces the former code path verbatim (separators and
all) so the new output can be checked against it byte for byte.

Documented deviations from the old path:
  * keys containing a single quote: the old path raised sqlglot.errors.TokenError (the quote
    terminated the literal). The new path doubles the quote, which is what sqlglot renders for a
    correctly escaped literal; the test checks that the literal round-trips to the original key.
  * bool keys are outside the documented key types (int, str, float, datetime) and are not covered.
"""

import time
from datetime import datetime

import pytest
import sqlglot
import sqlglot.expressions as exp

from partitioncache.apply_cache import (
    _add_where_condition,
    _splice_key_list,
    extend_query_with_partition_keys,
)

QUERY_NO_WHERE = "SELECT p.id FROM regions AS p JOIN stores AS a ON a.region_id = p.region_id"
QUERY_WITH_WHERE = QUERY_NO_WHERE + " WHERE a.category = 'C' AND a.x > 1.5"
QUERY_WITH_OR = QUERY_NO_WHERE + " WHERE a.category = 'C' OR a.category = 'N'"
QUERIES = [QUERY_NO_WHERE, QUERY_WITH_WHERE, QUERY_WITH_OR]


def _format_partition_key_for_sql(pk) -> str:
    """The key literals of the old implementation, typed as now: integers bare, timestamps cast, floats and strings quoted.

    The reference parses the literals with sqlglot; the new implementation splices the same text unparsed.
    """
    if isinstance(pk, int):
        return str(pk)
    if isinstance(pk, datetime):
        return f"CAST('{pk.isoformat(sep=' ')}' AS TIMESTAMP)"
    return f"'{pk}'"


def _reference_old_implementation(query: str, partition_keys, partition_key: str, method: str, p0_alias: str) -> str:
    """The IN / VALUES branches as they were before the placeholder rewrite (b5ae672)."""
    parsed_query = sqlglot.parse_one(query)
    if method == "IN":
        partition_keys_str = ",".join(_format_partition_key_for_sql(pk) for pk in partition_keys)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN ({partition_keys_str})")
    elif method == "VALUES":
        partition_keys_str = "),(".join(_format_partition_key_for_sql(pk) for pk in partition_keys)
        partition_expr = sqlglot.parse_one(f"{p0_alias}.{partition_key} IN (VALUES({partition_keys_str}))")
    else:
        raise ValueError(method)
    _add_where_condition(parsed_query, partition_expr)
    return parsed_query.sql()


def _int_keys(n: int) -> set[int]:
    return set(range(1, n + 1))


def _str_keys(n: int) -> set[str]:
    # characters that must survive untouched: space, double quote, semicolon, backslash, colon, percent, underscore
    specials = ["plain", "with space", 'dq"inside', "semi;colon", "back\\slash", "syn:th:0", "pct%und_"]
    return {f"{specials[i % len(specials)]}#{i}" for i in range(n)}


def _float_keys(n: int) -> set[float]:
    return {i + 0.25 for i in range(n)}


def _datetime_keys(n: int) -> set[datetime]:
    return {datetime(2024, 1, 1, 0, 0, 0).replace(day=1 + (i % 28), hour=i % 24, minute=i % 60) for i in range(n)}


KEY_FACTORIES = {"int": _int_keys, "str": _str_keys, "float": _float_keys, "datetime": _datetime_keys}


@pytest.mark.parametrize("method", ["IN", "VALUES"])
@pytest.mark.parametrize("key_type", list(KEY_FACTORIES))
@pytest.mark.parametrize("n_keys", [0, 1, 10, 1000])
@pytest.mark.parametrize("query", QUERIES)
def test_byte_equal_to_old_implementation(method, key_type, n_keys, query):
    # The same set object is passed to both implementations so iteration order is identical.
    keys = KEY_FACTORIES[key_type](n_keys)
    if method == "VALUES" and key_type == "float" and n_keys > 0:
        pytest.skip("VALUES refuses float keys, see test_values_refuses_float_keys")
    if method == "VALUES" and key_type == "str" and n_keys > 0:
        pytest.skip("VALUES guards string keys against a char(n) column, see test_values_guards_string_keys")
    new = extend_query_with_partition_keys(query, keys, "region_id", method=method, p0_alias="p")
    if n_keys == 0:
        # An empty key set never reaches the IN/VALUES branches: the query is restricted to no rows.
        assert "FALSE /* PartitionCache: cache intersection is empty" in new and "__pcache_keys_" not in new
        return
    old = _reference_old_implementation(query, keys, "region_id", method, "p")
    assert new == old


@pytest.mark.parametrize("n_keys", [1, 10])
def test_values_refuses_float_keys(n_keys):
    """A VALUES list of float keys is double precision and would silently miss a real column; IN keeps them."""
    with pytest.raises(ValueError, match="use method IN"):
        extend_query_with_partition_keys(QUERY_WITH_WHERE, _float_keys(n_keys), "region_id", method="VALUES", p0_alias="p")


@pytest.mark.parametrize("method", ["IN", "VALUES"])
def test_output_is_well_formed_and_contains_every_key(method):
    keys = _str_keys(50)
    new = extend_query_with_partition_keys(QUERY_WITH_WHERE, keys, "region_id", method=method, p0_alias="p")
    parsed = sqlglot.parse_one(new)
    literals = {lit.this for lit in parsed.find(exp.In).find_all(exp.Literal) if lit.is_string}
    assert literals == keys
    assert "__pcache_keys_" not in new


@pytest.mark.parametrize("method", ["IN", "VALUES"])
def test_single_quote_in_key_is_escaped_where_old_path_raised(method):
    keys = {"it's", "o'neil", "plain"}
    # TokenError or ParseError, depending on where the stray quote lands in the key order.
    with pytest.raises((sqlglot.errors.TokenError, sqlglot.errors.ParseError)):
        _reference_old_implementation(QUERY_WITH_WHERE, keys, "region_id", method, "p")
    new = extend_query_with_partition_keys(QUERY_WITH_WHERE, keys, "region_id", method=method, p0_alias="p")
    assert "'it''s'" in new
    assert "'o''neil'" in new
    parsed = sqlglot.parse_one(new, read="postgres")
    restriction = next(node for node in parsed.find_all(exp.In) if node.this.sql() == "p.region_id")
    literals = {lit.this for lit in restriction.find_all(exp.Literal) if lit.is_string}
    assert literals == keys
    # Rendering the parsed result reproduces the spliced output: the escaping is what sqlglot would emit.
    assert parsed.sql(dialect="postgres") == new


def test_values_guards_string_keys():
    """String keys in a VALUES list are text; against a char(n) column the guard raises instead of matching no row."""
    new = extend_query_with_partition_keys(QUERY_WITH_WHERE, {"a", "b"}, "region_id", method="VALUES", p0_alias="p")
    assert "PG_TYPEOF(p.region_id) IN (CAST('character' AS REGTYPE))" in new
    assert "needs method IN or a key-table method" in new
    for keys in ({1, 2}, {datetime(2024, 1, 1)}):
        assert "pg_typeof" not in extend_query_with_partition_keys(QUERY_WITH_WHERE, keys, "region_id", method="VALUES", p0_alias="p")
    assert "pg_typeof" not in extend_query_with_partition_keys(QUERY_WITH_WHERE, {"a"}, "region_id", method="VALUES", p0_alias="p", sql_dialect="duckdb")


def test_placeholder_must_occur_exactly_once():
    with pytest.raises(AssertionError):
        _splice_key_list("SELECT 1", "__pcache_keys_deadbeef__", "1, 2")
    with pytest.raises(AssertionError):
        _splice_key_list("SELECT __pcache_keys_deadbeef__, __pcache_keys_deadbeef__", "__pcache_keys_deadbeef__", "1, 2")
    assert _splice_key_list("SELECT x IN (__pcache_keys_deadbeef__)", "__pcache_keys_deadbeef__", "1, 2") == "SELECT x IN (1, 2)"


@pytest.mark.parametrize("method", ["IN", "VALUES"])
def test_placeholder_is_rendered_verbatim_by_sqlglot(method):
    # The placeholder must come out of sqlglot unquoted and unchanged, otherwise the splice would miss it.
    from partitioncache.apply_cache import _key_list_sentinel

    sentinel = _key_list_sentinel()
    inner = sentinel if method == "IN" else f"VALUES({sentinel})"
    rendered = sqlglot.parse_one(f"p.region_id IN ({inner})").sql()
    assert rendered.count(sentinel) == 1
    assert f'"{sentinel}"' not in rendered


@pytest.mark.parametrize("method", ["IN", "VALUES"])
def test_rewrite_of_100k_int_keys_is_fast(method):
    keys = _int_keys(100_000)
    t0 = time.perf_counter()
    new = extend_query_with_partition_keys(QUERY_WITH_WHERE, keys, "region_id", method=method, p0_alias="p")
    elapsed = time.perf_counter() - t0
    assert new.count(",") >= 99_999
    assert elapsed < 2.0, f"{method}: rewrite of 100k keys took {elapsed:.2f} s"


def test_backslash_keys_are_escaped_for_mysql():
    """MySQL treats a backslash as an escape character; a key ending in one must not swallow the next key."""
    keys = {"a\\", "b"}
    new = extend_query_with_partition_keys(QUERY_WITH_WHERE, keys, "region_id", method="IN", p0_alias="p", sql_dialect="mysql")
    parsed = sqlglot.parse_one(new, read="mysql")
    literals = {lit.this for lit in parsed.find(exp.In).find_all(exp.Literal) if lit.is_string}
    assert literals == keys
    values = extend_query_with_partition_keys(QUERY_WITH_WHERE, keys, "region_id", method="VALUES", p0_alias="p", sql_dialect="mysql")
    assert "('a\\\\')" in values
