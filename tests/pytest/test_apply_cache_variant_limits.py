"""apply_cache takes max_conditions_removed and protected_patterns, as apply_cache_lazy does, on every lookup path."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

from partitioncache.apply_cache import apply_cache
from partitioncache.query_processor import generate_all_hashes

ac = importlib.import_module("partitioncache.apply_cache")  # the package re-exports a function of the same name

QUERY = "SELECT * FROM pois AS p1, pois AS p2 WHERE p1.subtype = 'a' AND p2.subtype = 'b' AND p1.zipcode = p2.zipcode AND p1.name LIKE '%x%'"
SPATIAL = "SELECT * FROM pois AS p1, pois AS p2 WHERE p1.subtype = 'a' AND p2.subtype = 'b' AND p1.name LIKE '%x%' AND ST_DWithin(p1.geom, p2.geom, 300)"
LIMITS = {"max_conditions_removed": 1, "protected_patterns": ["subtype"]}


def _no_hits_handler() -> MagicMock:
    handler = MagicMock()
    handler.get_intersected.return_value = (None, 0)
    handler.filter_existing_keys.return_value = set()
    return handler


def test_standard_lookup_receives_the_limits() -> None:
    with patch.object(ac, "get_partition_keys", return_value=(None, 0, 0)) as lookup:
        apply_cache(QUERY, _no_hits_handler(), "zipcode", **LIMITS)
    assert {k: lookup.call_args.kwargs[k] for k in LIMITS} == LIMITS


def test_spatial_lookup_receives_the_limits() -> None:
    with patch.object(ac, "generate_all_hashes", return_value=[]) as hashes:
        apply_cache(SPATIAL, _no_hits_handler(), "spatial_bbox", geometry_column="geom", buffer_distance=300, **LIMITS)
    assert {k: hashes.call_args.kwargs[k] for k in LIMITS} == LIMITS


def test_grid_lookup_receives_the_limits() -> None:
    with patch.object(ac, "generate_all_hashes", return_value=[]) as hashes:
        apply_cache(SPATIAL, _no_hits_handler(), "cell200", geometry_column="geom", grid_cell_size=200, p0_alias="p1", **LIMITS)
    assert {k: hashes.call_args.kwargs[k] for k in LIMITS} == LIMITS


def test_the_limit_changes_the_lookup_variants() -> None:
    """The eager lookup now probes the variants the cache was populated with, not every removal combination."""
    counts = {}
    for limit in (0, None):
        _, stats = apply_cache(QUERY, _no_hits_handler(), "zipcode", min_component_size=1, max_conditions_removed=limit)
        counts[limit] = stats["generated_variants"]
    expected = len(set(generate_all_hashes(QUERY, "zipcode", min_component_size=1, fix_attributes=False, max_conditions_removed=0)))
    assert counts[0] == expected < counts[None]
