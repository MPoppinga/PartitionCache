"""
Variant hashes stay byte-identical: cache entries are found by them, so a change to variant generation that alters
one hash loses its entries.

The statements are a self-join with a long IN list and an inline subquery on the partition key, the case a
performance change to generate_partial_queries targets; the digests were computed with main before that change
(79a2f60) and do not depend on PYTHONHASHSEED. scripts/compare_variant_hashes.py compares two trees over a larger
corpus.
"""

import hashlib

import pytest

import partitioncache.query_processor as query_processor
from partitioncache.query_processor import generate_all_hashes, generate_all_query_hash_pairs

CODES = ", ".join(f"'{chr(65 + i % 26)}{i:03d}'" for i in range(300))
BASE = ("SELECT DISTINCT r.region_id FROM pois AS t1, pois AS t2, regions AS r "
        "WHERE t1.region_id = r.region_id AND t2.region_id = r.region_id AND t1.subtype = 'cafe' AND t2.subtype = 'pharmacy' "
        f"AND r.region_code IN ({CODES})")
STATEMENTS = {
    "inline_with_distance": BASE + " AND ST_DWithin(t1.geom, t2.geom, 4.7) AND r.region_id IN (SELECT unnest(s.region_ids) FROM region_stats AS s WHERE s.stat_key = 39 AND s.stat_value >= 2.5)",
    "inline_without_distance": BASE + " AND r.region_id IN (SELECT unnest(s.region_ids) FROM region_stats AS s WHERE s.stat_key = 39)",
    "plain_with_distance": BASE + " AND ST_DWithin(t1.geom, t2.geom, 3.2)",
}
SETTINGS = {
    "population": {"min_component_size": 2, "max_component_size": 5, "follow_graph": True, "keep_all_attributes": True, "bucket_steps": 1.0},
    "lookup": {"min_component_size": 1, "follow_graph": True, "fix_attributes": False, "canonicalize_queries": False, "warn_no_partition_key": False},
    "lookup_canonical": {"min_component_size": 1, "follow_graph": True, "fix_attributes": True, "canonicalize_queries": True, "warn_no_partition_key": False},
    "constraints": {
        "min_component_size": 1,
        "follow_graph": True,
        "keep_all_attributes": True,
        "add_constraints": {"regions": "area_class = 4"},
        "remove_constraints_add": ["subtype"],
    },
}

# (number of hashes, sha256 of the sorted hashes, first 16 hex digits), computed at main 79a2f60
EXPECTED = {
    "inline_with_distance/population": (5, "5b3ad4bc77ea4721"),
    "inline_with_distance/lookup": (11, "0010b678ee5b2fc5"),
    "inline_with_distance/lookup_canonical": (11, "0010b678ee5b2fc5"),
    "inline_with_distance/constraints": (19, "8c7c82c8f2428d4e"),
    "inline_without_distance/population": (1, "773cf7952cfbf2e4"),
    "inline_without_distance/lookup": (7, "2dfa01a53657c59f"),
    "inline_without_distance/lookup_canonical": (7, "2dfa01a53657c59f"),
    "inline_without_distance/constraints": (11, "55f606f8800f063b"),
    "plain_with_distance/population": (2, "0345bb28284961bc"),
    "plain_with_distance/lookup": (5, "5c8006eecf262d5f"),
    "plain_with_distance/lookup_canonical": (5, "5c8006eecf262d5f"),
    "plain_with_distance/constraints": (9, "1f5bf4c48e754e2b"),
}


@pytest.mark.parametrize("case", list(EXPECTED))
def test_hashes_are_unchanged(case):
    name, label = case.split("/")
    settings = SETTINGS[label]
    if label in ("population", "constraints"):
        hashes = sorted(h for _, h in generate_all_query_hash_pairs(STATEMENTS[name], "region_id", **settings))
    else:
        hashes = sorted(generate_all_hashes(STATEMENTS[name], "region_id", **settings))
    assert (len(hashes), hashlib.sha256("\n".join(hashes).encode()).hexdigest()[:16]) == EXPECTED[case]


@pytest.mark.parametrize(("name", "generations"), [("inline_without_distance", 1), ("inline_with_distance", 2)])
def test_variants_are_generated_once_when_bucketing_changes_nothing(monkeypatch, name, generations):
    """Without a distance condition to round, the bucketed statement is the statement itself: its variants once."""
    calls = []
    original = query_processor.generate_partial_queries

    def counting(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(query_processor, "generate_partial_queries", counting)
    generate_all_hashes(STATEMENTS[name], "region_id", **SETTINGS["lookup"])
    assert len(calls) == generations
