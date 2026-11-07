"""Grid-cell keys through the PostgreSQL queue processor: fragments queued with their cell projection are
populated by partitioncache_manual_process_queue and then restrict the query soundly.

The processor runs the fragment SQL it receives, so a grid fragment is an ordinary integer fragment for it.
Requires PostgreSQL with PostGIS and the processor functions (the CI integration image).
"""

from __future__ import annotations

import math
import random
from collections import Counter

import pytest

from partitioncache import spatial_grid as sg
from partitioncache.apply_cache import apply_cache
from partitioncache.query_processor import generate_all_query_hash_pairs
from partitioncache.queue import push_to_query_fragment_queue

TABLE = "pc_grid_qp_pois"  # no "queue" or "_cache_" in the name: db_session truncates such tables
KEY = "pc_grid_qp_cell200"
QUERY = (
    f"SELECT p1.name AS school, p2.name AS library FROM {TABLE} AS p1, {TABLE} AS p2 "
    "WHERE p1.subtype = 'school' AND p2.subtype = 'library' AND ST_DWithin(p1.geom, p2.geom, 400)"
)


@pytest.fixture
def grid_table(db_session):
    with db_session.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
        if cur.fetchone() is None:
            pytest.skip("PostGIS not installed")
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(f"CREATE TABLE {TABLE} (id serial PRIMARY KEY, name text, subtype text, geom geometry(Point, 25832))")
        cur.execute(sg.CELL_FUNCTION_DDL)
        rng = random.Random(5)
        x0, y0 = 560_000.0, 5_930_000.0
        rows = [(f"poi {i}", rng.choice(["school", "library", "cafe"]), x0 + rng.uniform(0, 10_000), y0 + rng.uniform(0, 10_000)) for i in range(1200)]
        for k in range(20):  # 1 cm inside the limit (must be found) or 0.5 m beyond it (must not)
            cx, cy = x0 + rng.uniform(500, 9500), y0 + rng.uniform(500, 9500)
            a, d = rng.uniform(0, 2 * math.pi), 399.99 if k % 2 == 0 else 400.5
            rows += [(f"school planted {k}", "school", cx, cy), (f"library planted {k}", "library", cx + d * math.cos(a), cy + d * math.sin(a))]
        cur.executemany(f"INSERT INTO {TABLE} (name, subtype, geom) VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 25832))", rows)
        cur.execute(f"CREATE INDEX ON {TABLE} USING gist (geom)")
        cur.execute(f"ANALYZE {TABLE}")
    yield
    with db_session.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")


@pytest.fixture
def fresh_grid_key(cache_client):
    """No cache table for KEY, so the processor creates and registers it as for a new key.

    The cache_client teardown drops the metadata table but only the cache tables of its own keys; a leftover
    KEY table would make the processor skip the registration and the lookup would find no datatype.
    """
    table = f"{cache_client.tableprefix}_cache_{KEY}"

    def drop() -> None:  # on the handler's connection: its open read transaction would block a DROP from another one
        cache_client.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cache_client.db.commit()

    drop()
    yield
    drop()


@pytest.mark.parametrize("method", ["CELL_STAR", "CELL_ARRAY", "CELL_ARRAY_ANCHOR"])
def test_queued_grid_fragments_restrict_the_query_soundly(db_session, cache_client, manual_queue_processor, grid_table, fresh_grid_key, method):
    if repr(cache_client) != "postgresql_array":
        pytest.skip(f"the manual processor fixture configures the array backend, not {cache_client!r}")
    from partitioncache.queue import clear_all_queues, reset_queue_handler

    reset_queue_handler()
    clear_all_queues()
    pairs = generate_all_query_hash_pairs(
        QUERY,
        KEY,
        geometry_column="geom",
        grid_cell_size=200,
        min_component_size=2,
        keep_all_attributes=False,
        warn_no_partition_key=False,
        **sg.GRID_GENERATION_KWARGS,
    )
    assert push_to_query_fragment_queue(query_hash_pairs=pairs, partition_key=KEY, partition_datatype="integer")
    with db_session.cursor() as cur:
        cur.execute("SELECT * FROM partitioncache_manual_process_queue(%s)", (len(pairs) + 2,))

    sql, stats = apply_cache(QUERY, cache_client, KEY, geometry_column="geom", grid_cell_size=200, grid_method=method, p0_alias="p1")
    assert stats["cache_hits"] == len(pairs) and stats["enhanced"] == 1 and stats["grid_cells"] > 0

    with db_session.cursor() as cur:
        cur.execute(QUERY)
        reference = Counter(cur.fetchall())
    planted = {(f"school planted {k}", f"library planted {k}") for k in range(20)}
    assert planted & set(reference) == {(f"school planted {k}", f"library planted {k}") for k in range(0, 20, 2)}
    with db_session.transaction(), db_session.cursor() as cur:  # CELL_STAR's temp tables are ON COMMIT DROP
        statements = [s for s in sql.split("; ") if s.strip()]
        for st in statements[:-1]:
            cur.execute(st)
        cur.execute(statements[-1])
        assert Counter(cur.fetchall()) == reference
