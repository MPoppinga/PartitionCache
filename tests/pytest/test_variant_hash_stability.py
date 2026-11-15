"""
Variant hashes stay byte-identical: cache entries are found by them, so a change to variant generation that alters
one hash loses its entries.

The statements have the shape of 09g's KLIFS fragment (a long IN list, an inline metadata subquery), the case a
performance change to generate_partial_queries targets; the digests were computed with main before that change
(8c45168) and do not depend on PYTHONHASHSEED. scripts/compare_variant_hashes.py compares two trees over a larger
corpus.
"""

import hashlib

import pytest

import partitioncache.query_processor as query_processor
from partitioncache.query_processor import generate_all_hashes, generate_all_query_hash_pairs

PDBS = ", ".join(f"'{chr(65 + i % 26)}{i:03d}'" for i in range(300))
BASE = ("SELECT DISTINCT p1.pocket_key FROM interactiondb_inter_points AS t1, interactiondb_inter_points AS t2, interactiondb_pockets AS p1 "
        "WHERE t1.pocket_key = p1.pocket_key AND t2.pocket_key = p1.pocket_key AND t1.type = 'HBD' AND t2.type = 'AR' "
        f"AND p1.pdb_code IN ({PDBS})")
STATEMENTS = {
    "inline_with_distance": BASE + " AND ST_DWithin(t1.geom, t2.geom, 4.7) AND p1.pocket_key IN (SELECT unnest(m.pocket_key) FROM meta AS m WHERE m.prop_key = 39 AND m.prop_value >= 2.5)",
    "inline_without_distance": BASE + " AND p1.pocket_key IN (SELECT unnest(m.pocket_key) FROM meta AS m WHERE m.prop_key = 39)",
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
        "add_constraints": {"interactiondb_pockets": "size = 4"},
        "remove_constraints_add": ["type"],
    },
}

# (number of hashes, sha256 of the sorted hashes, first 16 hex digits), computed at 8c45168
EXPECTED = {
    "inline_with_distance/population": (5, "0c56fa6d9825777a"),
    "inline_with_distance/lookup": (11, "1c10e4644750b1bc"),
    "inline_with_distance/lookup_canonical": (11, "1c10e4644750b1bc"),
    "inline_with_distance/constraints": (19, "2af1e7231007c96f"),
    "inline_without_distance/population": (1, "5cd2d85bb973f2cd"),
    "inline_without_distance/lookup": (7, "9df0050536d1c699"),
    "inline_without_distance/lookup_canonical": (7, "9df0050536d1c699"),
    "inline_without_distance/constraints": (11, "41921b4a85375678"),
    "plain_with_distance/population": (2, "13ca02e1ba6e4d94"),
    "plain_with_distance/lookup": (5, "f89d9122fe551a15"),
    "plain_with_distance/lookup_canonical": (5, "f89d9122fe551a15"),
    "plain_with_distance/constraints": (9, "f0dc32ae0503f035"),
}


@pytest.mark.parametrize("case", list(EXPECTED))
def test_hashes_are_unchanged(case):
    name, label = case.split("/")
    settings = SETTINGS[label]
    if label in ("population", "constraints"):
        hashes = sorted(h for _, h in generate_all_query_hash_pairs(STATEMENTS[name], "pocket_key", **settings))
    else:
        hashes = sorted(generate_all_hashes(STATEMENTS[name], "pocket_key", **settings))
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
    generate_all_hashes(STATEMENTS[name], "pocket_key", **SETTINGS["lookup"])
    assert len(calls) == generations
