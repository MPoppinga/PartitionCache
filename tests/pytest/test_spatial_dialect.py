"""The spatial rewrite parses and renders the query in its SQL dialect (PostgreSQL by default).

sqlglot's default dialect silently changed PostgreSQL constructs: ``to_char(ts, 'YYYY')`` became ``CAST(ts AS TEXT)``,
``a::real`` became ``CAST(a AS FLOAT)``, and ``ARRAY['a']`` and ``j->>'k'`` became SQL that PostgreSQL rejects.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from partitioncache.apply_cache import (
    apply_cache,
    apply_cache_lazy,
    extend_query_with_h3_cell_filter,
    extend_query_with_h3_cell_filter_lazy,
    extend_query_with_h3_cell_lookup,
    extend_query_with_spatial_filter,
    extend_query_with_spatial_filter_lazy,
)
from partitioncache.cache_handler.abstract import AbstractCacheHandler, AbstractCacheHandler_Lazy

POSTGRES_QUERY = (
    "SELECT p1.id FROM pois AS p1, pois AS p2 WHERE TO_CHAR(p1.ts, 'YYYY') = '2020' AND CAST(p1.a AS REAL) > 1 "
    "AND p1.tags = ARRAY['a'] AND p1.j ->> 'k' = 'v' AND ST_DWITHIN(p1.geom, p2.geom, 100)"
)
POSTGRES_CONSTRUCTS = ["TO_CHAR(p1.ts, 'YYYY') = '2020'", "CAST(p1.a AS REAL) > 1", "p1.tags = ARRAY['a']", "p1.j ->> 'k' = 'v'"]
SPATIAL_METHODS = ["SUBDIVIDE_TMP_TABLE", "SUBDIVIDE_INLINE", "DUMP_TMP_TABLE", "DUMP_CTE"]


def _assert_postgres(sql: str) -> None:
    for construct in POSTGRES_CONSTRUCTS:
        assert construct in sql, (construct, sql)
    for generic in ["CAST(p1.ts AS TEXT)", "AS FLOAT", "ARRAY('a')", "JSON_EXTRACT_SCALAR"]:
        assert generic not in sql, (generic, sql)


def _rewrites(query: str, **dialect: str) -> list[str]:
    """Every spatial rewrite of ``query``: each method, ST_DWithin and ST_Intersects, both SRID branches, H3."""
    out = []
    for method in SPATIAL_METHODS:
        for srid in (4326, 25832):
            for use_intersects in (False, True):
                common = {"geometry_column": "geom", "buffer_distance": 100.0, "srid": srid, "p0_alias": "p1", "use_intersects": use_intersects}
                out.append(extend_query_with_spatial_filter_lazy(query, "SELECT geom FROM cached", spatial_method=method, **common, **dialect))  # type: ignore[arg-type]
                if method in ("SUBDIVIDE_TMP_TABLE", "SUBDIVIDE_INLINE"):
                    out.append(extend_query_with_spatial_filter(query, b"\x01\x02", spatial_method=method, **common, **dialect))  # type: ignore[arg-type]
    for srid in (4326, 25832):
        out.append(extend_query_with_h3_cell_filter_lazy(query, "SELECT 1 AS cell", "geom", srid, 9, p0_alias="p1", **dialect))
        out.append(extend_query_with_h3_cell_filter(query, {1, 2}, "geom", srid, 9, p0_alias="p1", **dialect))
    for mode in ("inline", "mv", "column"):
        out.append(
            extend_query_with_h3_cell_lookup(
                query, {1, 2}, cell_mode=mode, h3_cell_table="h3_mv", h3_cell_id_column="id", p0_alias="p1", geometry_column="geom", **dialect
            )
        )
    return out


@pytest.mark.parametrize("index", range(len(_rewrites("SELECT p1.id FROM pois AS p1"))))
def test_every_spatial_rewrite_keeps_postgres_constructs(index: int) -> None:
    _assert_postgres(_rewrites(POSTGRES_QUERY)[index])


def test_explicit_postgres_dialect_is_the_default() -> None:
    assert [_strip_random(s) for s in _rewrites(POSTGRES_QUERY)] == [_strip_random(s) for s in _rewrites(POSTGRES_QUERY, sql_dialect="postgres")]


def _strip_random(sql: str) -> str:
    import re

    return re.sub(r"_pcache_(sf|h3_cells)_\d+", r"_pcache_\1_N", sql)


def test_condition_snippets_render_as_before() -> None:
    """A query without dialect-specific SQL comes out as with sqlglot's default dialect, apart from the case of POINT."""
    plain = "SELECT p1.name FROM pois AS p1, pois AS p2 WHERE p1.subtype = 'a' AND ST_DWithin(p1.geom, p2.geom, 300)"
    before = [_strip_random(s) for s in _rewrites(plain, sql_dialect="")]
    after = [_strip_random(s).replace("AS POINT)", "AS point)") for s in _rewrites(plain)]
    assert after == before
    assert any("::geography" not in s and "CAST(sf.geom AS GEOGRAPHY)" in s for s in after)


def _handler(lazy: bool, spatial_filter_type: str = "geometry") -> MagicMock:
    handler = MagicMock(spec=AbstractCacheHandler_Lazy if lazy else AbstractCacheHandler)
    handler.filter_existing_keys.side_effect = lambda keys, partition_key: set(keys)
    handler.get_spatial_filter_lazy = MagicMock(return_value="SELECT geom FROM cached")
    handler.get_spatial_filter = MagicMock(return_value=(b"\x01", 25832))
    handler.get_h3_cell_filter = MagicMock(return_value={1, 2})
    handler.srid = 25832
    handler.resolution = 9
    handler.h3_cell_mode = "inline"
    handler.spatial_filter_includes_buffer = True
    handler.spatial_filter_type = spatial_filter_type
    return handler


@pytest.mark.parametrize("lazy", [True, False])
@pytest.mark.parametrize("spatial_filter_type", ["geometry", "h3_cell_ids"])
@pytest.mark.parametrize("method", SPATIAL_METHODS)
def test_apply_cache_renders_spatial_rewrite_in_postgres(lazy: bool, spatial_filter_type: str, method: str) -> None:
    if not lazy and method.startswith("DUMP"):
        pytest.skip("the eager path has the SUBDIVIDE methods only")
    apply = apply_cache_lazy if lazy else apply_cache
    enhanced, stats = apply(
        query=POSTGRES_QUERY, cache_handler=_handler(lazy, spatial_filter_type), partition_key="spatial_bbox", geometry_column="geom",
        spatial_method=method,  # type: ignore[arg-type]
    )
    assert stats["enhanced"] == 1
    _assert_postgres(enhanced)


@pytest.mark.parametrize("lazy", [True, False])
def test_spatial_cache_refuses_other_dialects(lazy: bool) -> None:
    apply = apply_cache_lazy if lazy else apply_cache
    with pytest.raises(ValueError, match="PostgreSQL"):
        apply(query=POSTGRES_QUERY, cache_handler=_handler(lazy), partition_key="spatial_bbox", geometry_column="geom", sql_dialect="duckdb")
