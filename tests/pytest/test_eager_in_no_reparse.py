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
    _format_partition_key_for_sql,
    _splice_key_list,
    extend_query_with_partition_keys,
)

QUERY_NO_WHERE = "SELECT p.id FROM pockets AS p JOIN atoms AS a ON a.pocket_key = p.pocket_key"
QUERY_WITH_WHERE = QUERY_NO_WHERE + " WHERE a.element = 'C' AND a.x > 1.5"
QUERY_WITH_OR = QUERY_NO_WHERE + " WHERE a.element = 'C' OR a.element = 'N'"
QUERIES = [QUERY_NO_WHERE, QUERY_WITH_WHERE, QUERY_WITH_OR]


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
    new = extend_query_with_partition_keys(query, keys, "pocket_key", method=method, p0_alias="p")
    if n_keys == 0:
        # An empty key set never reaches the IN/VALUES branches: the query is restricted to no rows.
        assert "FALSE /* PartitionCache: cache intersection is empty" in new and "__pcache_keys_" not in new
        return
    old = _reference_old_implementation(query, keys, "pocket_key", method, "p")
    assert new == old


@pytest.mark.parametrize("method", ["IN", "VALUES"])
def test_output_is_well_formed_and_contains_every_key(method):
    keys = _str_keys(50)
    new = extend_query_with_partition_keys(QUERY_WITH_WHERE, keys, "pocket_key", method=method, p0_alias="p")
    parsed = sqlglot.parse_one(new)
    literals = {lit.this for lit in parsed.find(exp.In).find_all(exp.Literal) if lit.is_string}
    assert literals == keys
    assert "__pcache_keys_" not in new


@pytest.mark.parametrize("method", ["IN", "VALUES"])
def test_single_quote_in_key_is_escaped_where_old_path_raised(method):
    keys = {"it's", "o'neil", "plain"}
    # TokenError or ParseError, depending on where the stray quote lands in the key order.
    with pytest.raises((sqlglot.errors.TokenError, sqlglot.errors.ParseError)):
        _reference_old_implementation(QUERY_WITH_WHERE, keys, "pocket_key", method, "p")
    new = extend_query_with_partition_keys(QUERY_WITH_WHERE, keys, "pocket_key", method=method, p0_alias="p")
    assert "'it''s'" in new
    assert "'o''neil'" in new
    parsed = sqlglot.parse_one(new)
    literals = {lit.this for lit in parsed.find(exp.In).find_all(exp.Literal) if lit.is_string}
    assert literals == keys
    # Rendering the parsed result reproduces the spliced output: the escaping is what sqlglot would emit.
    assert parsed.sql() == new


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
    rendered = sqlglot.parse_one(f"p.pocket_key IN ({inner})").sql()
    assert rendered.count(sentinel) == 1
    assert f'"{sentinel}"' not in rendered


@pytest.mark.parametrize("method", ["IN", "VALUES"])
def test_rewrite_of_100k_int_keys_is_fast(method):
    keys = _int_keys(100_000)
    t0 = time.perf_counter()
    new = extend_query_with_partition_keys(QUERY_WITH_WHERE, keys, "pocket_key", method=method, p0_alias="p")
    elapsed = time.perf_counter() - t0
    assert new.count(",") >= 99_999
    assert elapsed < 2.0, f"{method}: rewrite of 100k keys took {elapsed:.2f} s"
