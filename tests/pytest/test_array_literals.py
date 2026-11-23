"""PostgreSQL array constructors (ARRAY[...]) in queries: fragments and rewritten queries keep them executable."""

from __future__ import annotations

from typing import Literal

import pytest

from partitioncache.apply_cache import extend_query_with_partition_keys
from partitioncache.query_processor import clean_query, generate_all_query_hash_pairs

QUERY = (
    "SELECT DISTINCT t1.store_id FROM items AS t1, items AS t2 "
    "WHERE t1.store_id = t2.store_id AND abs(t1.price - t2.price) <= 2 AND t1.id <> t2.id "
    "AND t1.store_id IN (SELECT store_id FROM store_tags WHERE tag_id = 7 AND ({condition}))"
)
CONDITIONS = {
    "ILIKE ANY": ("tag_value ILIKE ANY (ARRAY['%blue%'])", "ARRAY['%blue%']"),
    "= ANY": ("tag_id = ANY (ARRAY[1, 2])", "ARRAY[1, 2]"),
    "<> ALL": ("tag_value <> ALL (ARRAY['red', 'green'])", "ARRAY['red', 'green']"),
}


@pytest.mark.parametrize("name", CONDITIONS)
def test_fragments_keep_the_array_constructor(name: str) -> None:
    condition, rendered = CONDITIONS[name]
    pairs = generate_all_query_hash_pairs(QUERY.format(condition=condition), "store_id", warn_no_partition_key=False)
    assert pairs
    with_array = [fragment for fragment, _ in pairs if "ARRAY" in fragment.upper()]
    assert with_array, "the condition should survive in some fragments"
    for fragment in with_array:
        assert rendered in fragment
        assert "ARRAY(" not in fragment.upper()


def test_clean_query_keeps_the_array_constructor() -> None:
    cleaned = clean_query("SELECT * FROM t AS a WHERE a.x ILIKE ANY (ARRAY['%a%', '%b%'])")
    assert "ARRAY['%a%', '%b%']" in cleaned


@pytest.mark.parametrize("method", ["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"])
def test_rewritten_query_keeps_the_array_constructor(method: Literal["IN", "VALUES", "TMP_TABLE_IN", "TMP_TABLE_JOIN"]) -> None:
    query = "SELECT * FROM items AS t1 WHERE t1.tags && ARRAY['a', 'b'] AND t1.price > 3"
    rewritten = extend_query_with_partition_keys(query, {1, 2}, "store_id", method=method, p0_alias="t1")
    assert "ARRAY['a', 'b']" in rewritten
    assert "ARRAY(" not in rewritten.upper()
