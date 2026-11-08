"""The H3 k-ring radius must keep every pair within the buffer distance (rocksdict_h3_grouped)."""

from __future__ import annotations

import math
import random

import pytest

h3 = pytest.importorskip("h3")

from partitioncache.cache_handler.rocksdict_h3_grouped import sound_kring_radius  # noqa: E402


def _offset(lat: float, lng: float, dist_m: float, bearing: float) -> tuple[float, float]:
    r = 6_371_008.8
    d = dist_m / r
    la1, lo1 = math.radians(lat), math.radians(lng)
    la2 = math.asin(math.sin(la1) * math.cos(d) + math.cos(la1) * math.sin(d) * math.cos(bearing))
    lo2 = lo1 + math.atan2(math.sin(bearing) * math.sin(d) * math.cos(la1), math.cos(d) - math.sin(la1) * math.sin(la2))
    return math.degrees(la2), math.degrees(lo2)


def test_average_edge_rule_was_too_small_at_resolution_9() -> None:
    # Two res-9 cells two rings apart can be ~171 m apart in central Europe, so D = 200 m needs k >= 2;
    # ceil(D / average edge) gives 1 there.
    cells = [h3.str_to_int(h3.latlng_to_cell(48.0 + i * 0.3, 7.0 + j * 0.4, 9)) for i in range(20) for j in range(20)]
    assert math.ceil(200 / h3.average_hexagon_edge_length(9, unit="m")) == 1
    assert sound_kring_radius(cells, 200.0) >= 2


@pytest.mark.parametrize(("res", "distances"), [(9, (171.0, 185.0, 200.0, 400.0, 700.0)), (10, (100.0, 200.0)), (8, (1000.0,))])
def test_pairs_within_distance_stay_within_k(res: int, distances: tuple[float, ...]) -> None:
    rng = random.Random(res)
    for d in distances:
        starts = [(rng.uniform(47.3, 55.0), rng.uniform(5.9, 15.0)) for _ in range(4000)]
        cells = [h3.str_to_int(h3.latlng_to_cell(lat, lng, res)) for lat, lng in starts]
        k = sound_kring_radius(cells, d)
        for lat, lng in starts:
            dist = d if rng.random() < 0.3 else d * math.sqrt(rng.random())
            lat2, lng2 = _offset(lat, lng, dist, rng.uniform(0, 2 * math.pi))
            assert h3.grid_distance(h3.latlng_to_cell(lat, lng, res), h3.latlng_to_cell(lat2, lng2, res)) <= k, (res, d, k)


def test_zero_distance_and_no_cells_need_no_rings() -> None:
    assert sound_kring_radius([], 500.0) == 0
    assert sound_kring_radius([h3.str_to_int(h3.latlng_to_cell(48.0, 11.0, 9))], 0.0) == 0
