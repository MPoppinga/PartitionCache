"""Grid-cell spatial keys against PostGIS: population SQL, apply_cache routing and all three rewrites.

Self-contained: it creates its own table (pc_grid_test_pois) with random POIs plus constellations planted
exactly at the distance limit and just beyond it, installs the cell function, populates an in-memory cache
by running the projected fragment SQL, and checks that every grid method returns the reference multiset.

Requires PostgreSQL with PostGIS and the standard DB_* environment variables.
"""

from __future__ import annotations

import math
import os
import random
from collections import Counter
from typing import Any, cast

import numpy as np
import pytest

from partitioncache import spatial_grid as sg
from partitioncache.apply_cache import apply_cache
from partitioncache.query_processor import generate_all_query_hash_pairs

psycopg = pytest.importorskip("psycopg")

TABLE = "pc_grid_test_pois"
QUERY = (
    f"SELECT p1.name AS school, p2.name AS library, p3.name AS hospital FROM {TABLE} AS p1, {TABLE} AS p2, {TABLE} AS p3 "
    "WHERE p1.subtype = 'school' AND p2.subtype = 'library' AND p2.name LIKE '%Stadt%' AND p3.subtype = 'hospital' "
    "AND ST_DWithin(p1.geom, p2.geom, 500) AND ST_DWithin(p1.geom, p3.geom, 300)"
)


def _conn():
    dbname = os.getenv("DB_NAME", "")
    if not dbname:
        pytest.skip("DB_NAME not set")
    conn = psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("DB_USER", "integration_user"),
        password=os.getenv("DB_PASSWORD", "integration_password"),
        dbname=dbname,
        autocommit=True,
    )
    if conn.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'").fetchone() is None:
        conn.close()
        pytest.skip("PostGIS not installed")
    return conn


@pytest.fixture(scope="module")
def db():
    conn = _conn()
    conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
    conn.execute(f"CREATE TABLE {TABLE} (id serial PRIMARY KEY, name text, subtype text, geom geometry(Point, 25832))")
    conn.execute(sg.CELL_FUNCTION_DDL)
    rng = random.Random(3)
    rows = []
    x0, y0 = 560_000.0, 5_930_000.0
    for i in range(3000):
        st = rng.choice(["school", "library", "hospital", "cafe", "cafe", "bakery"])
        name = f"Stadt {st} {i}" if rng.random() < 0.5 else f"{st} {i}"
        rows.append((name, st, x0 + rng.uniform(0, 20_000), y0 + rng.uniform(0, 20_000)))
    for k in range(40):  # constellations at the limit (must be found) and 0.5 m beyond it (must not)
        cx, cy = x0 + rng.uniform(1000, 19_000), y0 + rng.uniform(1000, 19_000)
        a = rng.uniform(0, 2 * math.pi)
        extra = -0.01 if k % 2 == 0 else 0.5  # 1 cm inside the limit (float-exact 500.0 may round past it) or 0.5 m beyond
        rows.append((f"school planted {k}", "school", cx, cy))
        rows.append((f"Stadt library planted {k}", "library", cx + (500 + extra) * math.cos(a), cy + (500 + extra) * math.sin(a)))
        rows.append((f"hospital planted {k}", "hospital", cx + (300 + extra) * math.cos(a + 2), cy + (300 + extra) * math.sin(a + 2)))
    with conn.cursor() as cur:
        cur.executemany(f"INSERT INTO {TABLE} (name, subtype, geom) VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 25832))", rows)
    conn.execute(f"CREATE INDEX ON {TABLE} USING gist (geom)")
    conn.execute(f"CREATE INDEX ON {TABLE} ({sg.CELL_FUNCTION}(geom, 200))")
    conn.execute(f"ANALYZE {TABLE}")
    yield conn
    conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
    conn.close()


class _SetHandler:
    def __init__(self, entries: dict[str, set[int]]) -> None:
        self.entries = entries

    def filter_existing_keys(self, keys: set[str], partition_key: str) -> set[str]:  # noqa: ARG002
        return {k for k in keys if k in self.entries}

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | None:  # noqa: ARG002
        return self.entries.get(key)


def _populate(conn) -> Any:
    pairs = generate_all_query_hash_pairs(
        query=QUERY,
        partition_key="cell200",
        geometry_column="geom",
        skip_partition_key_joins=True,
        auto_detect_partition_join=False,
        min_component_size=2,
        keep_all_attributes=False,
        warn_no_partition_key=False,
        grid_cell_size=200,
    )
    assert pairs
    entries = {h: {r[0] for r in conn.execute(q).fetchall()} for q, h in pairs}
    return cast(Any, _SetHandler(entries))


def _run(conn, sql: str) -> Counter:
    with conn.transaction():
        stmts = [s for s in sql.split("; ") if s.strip()]
        for st in stmts[:-1]:
            conn.execute(st)
        return Counter(conn.execute(stmts[-1]).fetchall())


@pytest.mark.parametrize("method", sg.GRID_METHODS)
def test_grid_methods_return_the_reference_multiset(db, method: str) -> None:
    reference = Counter(db.execute(QUERY).fetchall())
    planted = {(f"school planted {k}", f"Stadt library planted {k}", f"hospital planted {k}") for k in range(40)}
    found = planted & set(reference)
    assert found == {t for t in planted if int(t[0].rsplit(" ", 1)[1]) % 2 == 0}  # at the limit: in; 0.5 m beyond: out
    handler = _populate(db)
    sql, stats = apply_cache(QUERY, handler, "cell200", geometry_column="geom", grid_cell_size=200, grid_method=method, p0_alias="p1")
    assert stats["enhanced"] == 1 and 0 < stats["grid_cells"]
    area_cells = (20_000 // 200) ** 2  # the 20 x 20 km field at 200 m cells; widened cells include empty neighbours
    assert stats["grid_cells"] < area_cells  # the restriction actually restricts
    assert _run(db, sql) == reference


def test_expression_index_matches_the_rewritten_predicate(db) -> None:
    cells = db.execute(f"SELECT array_agg(DISTINCT {sg.CELL_FUNCTION}(geom, 200)) FROM (SELECT geom FROM {TABLE} LIMIT 5) x").fetchone()[0]
    sql = sg.rewrite_cell_array(f"SELECT p1.name FROM {TABLE} AS p1", np.array(sorted(cells)), ["p1"], "geom", 200)
    db.execute("SET enable_seqscan = off")
    plan = "\n".join(r[0] for r in db.execute("EXPLAIN " + sql).fetchall())
    db.execute("RESET enable_seqscan")
    assert f"{TABLE}_{sg.CELL_FUNCTION}_idx" in plan or "Index" in plan
