"""
RocksDict H3 Grouped Spatial Cache Handler.

Thin subclass of RocksDictCacheHandler that adds:
- PostgreSQL connection for H3 cell conversion during cache population
- get_h3_cell_filter() for pure H3 cell-based spatial filtering (no PostGIS at query time)
- K-ring expansion for approximate spatial buffering
- spatial_filter_type / spatial_filter_includes_buffer properties

All storage and intersection logic lives in RocksDictCacheHandler, which
polymorphically handles list[frozenset[int]] (grouped match sets) via
connected-component intersection.
"""

from __future__ import annotations

import math
from logging import getLogger

import psycopg
from psycopg import sql

from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler, _grouped_kring_intersection

logger = getLogger("PartitionCache")


class RocksDictH3GroupedCacheHandler(RocksDictCacheHandler):
    """
    Spatial extension of RocksDictCacheHandler for H3 grouped match sets.

    Adds PostgreSQL connection for:
    - Converting geometry (WKB) to H3 cell IDs during cache population

    Spatial filtering uses pure H3 cell IDs with k-ring expansion (Python h3 library).
    No PostGIS required at query/filter time.

    Requires: PostgreSQL + h3-pg extension (for cache population).
    Requires: Python h3 library (for k-ring expansion at filter time).
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
        h3_cell_mode: str = "inline",
        h3_cell_table: str | None = None,
        h3_cell_column: str = "h3_cell_id",
        h3_cell_id_column: str | None = None,
        read_only: bool = False,
    ) -> None:
        super().__init__(db_path, read_only=read_only)

        self.resolution = resolution
        self.srid = srid
        self.h3_cell_mode = h3_cell_mode
        self.h3_cell_table = h3_cell_table
        self.h3_cell_column = h3_cell_column
        self.h3_cell_id_column = h3_cell_id_column

        self.pg_conn = psycopg.connect(
            dbname=db_name,
            host=db_host,
            user=db_user,
            password=db_password,
            port=int(db_port),
            autocommit=True,
        )
        self.pg_cursor = self.pg_conn.cursor()

    @classmethod
    def get_instance(cls, db_path: str, read_only: bool = False, **kwargs):  # type: ignore[override]
        """Singleton factory that forwards the PostgreSQL/H3 config to ``__init__``.

        The base ``RocksDictAbstractCacheHandler.get_instance`` only forwards ``db_path`` and
        ``read_only`` and constructs ``cls(db_path, read_only=read_only)``. This handler additionally
        requires ``db_host``/``db_name``/``db_user``/``db_password``/``db_port`` (and optional
        ``resolution``/``srid``/``h3_cell_*`` settings) for the PostgreSQL connection used during H3
        conversion, so the base factory raised ``unexpected keyword argument 'db_host'``. Override to
        forward the full keyword config while preserving the singleton/refcount semantics.
        """
        if cls._instance is None or cls._current_path != db_path:
            if cls._instance is not None:
                try:
                    cls._instance.db.close()
                except Exception:
                    pass
                cls._instance = None
                cls._refcount = 0
            cls._instance = cls(db_path, read_only=read_only, **kwargs)
            cls._current_path = db_path
        cls._refcount += 1
        return cls._instance

    def __repr__(self) -> str:
        return "rocksdict_h3_grouped"

    @property
    def spatial_filter_type(self) -> str:
        """Returns h3_cell_ids — spatial filter is a set of H3 cell IDs."""
        return "h3_cell_ids"

    @property
    def spatial_filter_includes_buffer(self) -> bool:
        """Buffer is baked into the k-ring expansion."""
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

    def get_h3_cell_filter(
        self,
        keys: set[str],
        partition_key: str = "partition_key",
        buffer_distance: float = 0.0,
    ) -> set[int] | None:
        """
        Get H3 cell IDs for spatial filtering via k-ring expansion.

        1. Load grouped match sets for each key from cache
        2. Compute k from buffer_distance and resolution
        3. Expand each group's cells with grid_disk(cell, k)
        4. Per-variant merge, cross-variant intersection
        5. Return surviving cell IDs

        Args:
            keys: Cache keys (variant hashes) to intersect.
            partition_key: Partition key namespace.
            buffer_distance: Buffer distance in meters for k-ring expansion.

        Returns:
            Set of H3 cell IDs, or None if no cache hits.
        """
        try:
            # Load grouped match sets for each key — no h3 import needed yet
            fragment_groups: list[list[frozenset[int]]] = []
            for key in keys:
                value = self.get(key, partition_key=partition_key)
                if value is not None and isinstance(value, list):
                    fragment_groups.append(value)

            if not fragment_groups:
                return None

            # Now we need h3 for k-ring expansion
            try:
                import h3 as h3_lib
            except ImportError as e:
                raise ImportError("h3 library required for H3 cell filtering: pip install h3") from e

            # Compute k from buffer_distance
            if buffer_distance > 0:
                edge_length = h3_lib.average_hexagon_edge_length(self.resolution, unit="m")
                k = math.ceil(buffer_distance / edge_length)
            else:
                k = 0

            # K-ring expansion + cross-variant intersection
            result = _grouped_kring_intersection(fragment_groups, k)

            if not result:
                return None

            return result

        except ImportError:
            raise
        except Exception as e:
            logger.error(f"Failed to get H3 cell filter: {e}")
            return None

    def close(self) -> None:
        """Close both RocksDict and PostgreSQL connections."""
        try:
            self.pg_cursor.close()
            self.pg_conn.close()
        except Exception:
            pass
        super().close()
