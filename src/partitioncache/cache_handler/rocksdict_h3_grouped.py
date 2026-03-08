"""
RocksDict H3 Grouped Spatial Cache Handler.

Thin subclass of RocksDictCacheHandler that adds:
- PostgreSQL connection for H3 cell conversion and geometry reconstruction
- get_spatial_filter() for apply_cache() integration
- spatial_filter_type / spatial_filter_includes_buffer properties

All storage and intersection logic lives in RocksDictCacheHandler, which
polymorphically handles list[frozenset[int]] (grouped match sets) via
connected-component intersection.
"""

from __future__ import annotations

from logging import getLogger

import psycopg
from psycopg import sql

from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

logger = getLogger("PartitionCache")


class RocksDictH3GroupedCacheHandler(RocksDictCacheHandler):
    """
    Spatial extension of RocksDictCacheHandler for H3 grouped match sets.

    Adds PostgreSQL connection for:
    - Converting geometry (WKB) to H3 cell IDs during cache population
    - Reconstructing geometry from surviving H3 cells for spatial filtering

    Requires: PostgreSQL + PostGIS + h3-pg extension.
    """

    _instance = None
    _refcount = 0
    _current_path = None

    def __init__(
        self,
        db_path: str,
        db_name: str,
        db_host: str,
        db_user: str,
        db_password: str,
        db_port: str | int,
        resolution: int = 9,
        srid: int = 4326,
        read_only: bool = False,
    ) -> None:
        super().__init__(db_path, read_only=read_only)

        self.resolution = resolution
        self.srid = srid

        self.pg_conn = psycopg.connect(
            dbname=db_name,
            host=db_host,
            user=db_user,
            password=db_password,
            port=int(db_port),
            autocommit=True,
        )
        self.pg_cursor = self.pg_conn.cursor()

    def __repr__(self) -> str:
        return "rocksdict_h3_grouped"

    @property
    def spatial_filter_type(self) -> str:
        """Returns geometry — spatial filter is a buffered polygon."""
        return "geometry"

    @property
    def spatial_filter_includes_buffer(self) -> bool:
        """Buffer is baked into the reconstructed geometry."""
        return True

    def geom_to_h3_cell(self, geom_value: bytes | memoryview | str) -> int | None:
        """Convert a geometry value to an H3 cell ID via PostgreSQL.

        Accepts geometry in any format psycopg3 may return: hex-encoded EWKB string,
        raw WKB bytes, or memoryview. Uses ``::geometry`` cast which handles all formats.
        """
        try:
            if self.srid != 4326:
                h3_sql = sql.SQL(
                    "SELECT h3_lat_lng_to_cell("
                    "  ST_Transform(ST_Centroid(%s::geometry), 4326)::point,"
                    "  {res}"
                    ")::bigint"
                ).format(res=sql.Literal(self.resolution))
            else:
                h3_sql = sql.SQL(
                    "SELECT h3_lat_lng_to_cell("
                    "  ST_Centroid(%s::geometry)::point,"
                    "  {res}"
                    ")::bigint"
                ).format(res=sql.Literal(self.resolution))

            # Pass value as-is: psycopg3 returns hex EWKB strings,
            # PostgreSQL's ::geometry cast handles hex, binary, and EWKB
            self.pg_cursor.execute(h3_sql, (geom_value,))
            result = self.pg_cursor.fetchone()
            if result and result[0] is not None:
                return int(result[0])
            return None
        except Exception as e:
            logger.warning(f"H3 cell conversion failed: {e}")
            return None

    def get_spatial_filter(
        self,
        keys: set[str],
        partition_key: str = "partition_key",
        buffer_distance: float = 0.0,
    ) -> tuple[bytes, int] | None:
        """
        Reconstruct geometry from surviving H3 cells after grouped intersection.

        1. get_intersected() returns surviving cell IDs (connected-component)
        2. Convert cells to polygon boundaries via h3_cell_to_boundary
        3. Collect, transform to target SRID, buffer

        Returns:
            Tuple of (WKB bytes, SRID) or None if no cache hits.
        """
        try:
            result, count = self.get_intersected(keys, partition_key)
            if result is None or not result:
                return None

            cell_list = list(result)

            # Build geometry reconstruction SQL
            if self.srid != 4326:
                geom_sql = sql.SQL(
                    "SELECT ST_AsBinary(ST_Buffer("
                    "  ST_Transform("
                    "    ST_Collect(ST_SetSRID(h3_cell_to_boundary(cell::h3index)::geometry, 4326)),"
                    "    {target_srid}"
                    "  ),"
                    "  {buf}"
                    ")) FROM unnest(%s::bigint[]) AS cell"
                ).format(
                    target_srid=sql.Literal(self.srid),
                    buf=sql.Literal(buffer_distance),
                )
            elif buffer_distance > 0:
                geom_sql = sql.SQL(
                    "SELECT ST_AsBinary(ST_Buffer("
                    "  ST_Collect(ST_SetSRID(h3_cell_to_boundary(cell::h3index)::geometry, 4326)),"
                    "  {buf}"
                    ")) FROM unnest(%s::bigint[]) AS cell"
                ).format(buf=sql.Literal(buffer_distance))
            else:
                geom_sql = sql.SQL(
                    "SELECT ST_AsBinary("
                    "  ST_Collect(ST_SetSRID(h3_cell_to_boundary(cell::h3index)::geometry, 4326))"
                    ") FROM unnest(%s::bigint[]) AS cell"
                )

            self.pg_cursor.execute(geom_sql, (cell_list,))
            row = self.pg_cursor.fetchone()
            if row is None or row[0] is None:
                return None

            return bytes(row[0]), self.srid

        except Exception as e:
            logger.error(f"Failed to get spatial filter geometry: {e}")
            return None

    def close(self) -> None:
        """Close both RocksDict and PostgreSQL connections."""
        try:
            self.pg_cursor.close()
            self.pg_conn.close()
        except Exception:
            pass
        super().close()
