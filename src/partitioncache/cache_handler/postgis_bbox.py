"""
PostGIS Bounding Box Cache Handler.

Stores cache entries as PostGIS geometry collections (MultiPoint, MultiPolygon, etc.)
by collecting actual geometries from query results.
Supports spatial filtering via geometric ST_Intersection of buffered multipolygons.
Requires: PostgreSQL + PostGIS extension.
"""

import warnings
from logging import getLogger

from psycopg import sql

from partitioncache.cache_handler.postgis_spatial_abstract import PostGISSpatialAbstractCacheHandler

logger = getLogger("PartitionCache")


class PostGISBBoxCacheHandler(PostGISSpatialAbstractCacheHandler):
    """
    Spatial cache handler using PostGIS bounding box geometries.

    Stores partition data as PostGIS geometry collections by collecting actual
    geometries from query results (ST_Collect). Supports spatial filtering via
    geometric ST_Intersection of buffered multipolygons.

    .. deprecated:: cell_size parameter
        The ``cell_size`` parameter is no longer used. Previously, geometries were
        snapped to a grid and converted to envelope cells. Now raw geometries are
        stored directly, which is both simpler and more precise.
    """

    @property
    def spatial_filter_includes_buffer(self) -> bool:
        """BBox handler bakes buffer_distance into the spatial filter geometry."""
        return True

    def __init__(
        self,
        db_name: str,
        db_host: str,
        db_user: str,
        db_password: str,
        db_port: str | int,
        db_tableprefix: str,
        cell_size: float = 0.01,
        geometry_column: str = "geom",
        srid: int = 4326,
        timeout: str = "0",
    ) -> None:
        if cell_size != 0.01:
            warnings.warn(
                "cell_size parameter is deprecated and no longer used. "
                "Raw geometries are now stored directly without grid snapping.",
                DeprecationWarning,
                stacklevel=2,
            )
        self.cell_size = cell_size  # Kept for backward compat but unused
        super().__init__(db_name, db_host, db_user, db_password, db_port, db_tableprefix, geometry_column, srid, timeout)

    def __repr__(self) -> str:
        return "postgis_bbox"

    def _ensure_partition_table(self, partition_key: str, datatype: str, **kwargs) -> bool:
        """Create cache table with geometry column and GiST index for bounding boxes."""
        try:
            table_name = f"{self.tableprefix}_cache_{partition_key}"

            # Create the cache table with geometry column
            self.cursor.execute(
                sql.SQL(
                    """CREATE TABLE IF NOT EXISTS {table} (
                        query_hash TEXT PRIMARY KEY,
                        partition_keys geometry,
                        partition_keys_count INTEGER
                    )"""
                ).format(table=sql.Identifier(table_name))
            )

            # Create GiST spatial index
            index_name = f"{self.tableprefix}_cache_{partition_key}_gist_idx"
            self.cursor.execute(
                sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {table} USING GIST (partition_keys)").format(
                    idx=sql.Identifier(index_name),
                    table=sql.Identifier(table_name),
                )
            )

            # Ensure metadata entry exists
            metadata_table = f"{self.tableprefix}_partition_metadata"
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {table} (partition_key, datatype) VALUES (%s, %s) "
                    "ON CONFLICT (partition_key) DO NOTHING"
                ).format(table=sql.Identifier(metadata_table)),
                (partition_key, datatype),
            )

            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to ensure BBox partition table for {partition_key}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            raise

    def set_cache(self, key, partition_key_identifiers, partition_key="partition_key"):
        """
        Store bounding box geometry in the cache.

        partition_key_identifiers should be a WKB bytes object (geometry) or a set
        that will be ignored (use set_cache_lazy for proper spatial population).
        """
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                if not self._ensure_partition_table(partition_key, "geometry"):
                    return False

            table_name = f"{self.tableprefix}_cache_{partition_key}"

            # For BBox, partition_key_identifiers should be WKB bytes
            if isinstance(partition_key_identifiers, bytes):
                self.cursor.execute(
                    sql.SQL(
                        "INSERT INTO {table} (query_hash, partition_keys) "
                        "VALUES (%s, %s) "
                        "ON CONFLICT (query_hash) DO UPDATE SET "
                        "partition_keys = EXCLUDED.partition_keys"
                    ).format(table=sql.Identifier(table_name)),
                    (key, partition_key_identifiers),
                )
            else:
                logger.warning("BBox set_cache expects WKB bytes. Use set_cache_lazy for SQL-based population.")
                return False

            self._update_queries_table(key, partition_key)
            return True
        except Exception as e:
            logger.error(f"Failed to set BBox cache for key {key}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return False

    def set_cache_lazy(self, key: str, query: str, partition_key: str = "partition_key") -> bool:
        """
        Store geometry collection by wrapping the fragment query with ST_Collect aggregation.

        The fragment query should return rows with a geometry column (or multiple geometry
        columns named geom_1, geom_2, ... for multi-alias spatial fragments). Multi-column
        fragments are flattened via CROSS JOIN LATERAL VALUES before collection.
        """
        try:
            if "DELETE " in query.upper() or "DROP " in query.upper():
                logger.error("Query contains DELETE or DROP")
                return False

            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                logger.error(f"Partition key '{partition_key}' not registered")
                return False

            table_name = f"{self.tableprefix}_cache_{partition_key}"
            geom_col = self.geometry_column

            # Detect multi-column geometry format (geom_1, geom_2, ...) from grouped fragments
            import re

            geom_col_indices = sorted({int(m) for m in re.findall(rf'\b{re.escape(geom_col)}_(\d+)\b', query)})

            if geom_col_indices:
                # Multi-column fragment: flatten via LATERAL VALUES into single geometry column
                values_list = ", ".join(f"(sub.{geom_col}_{i})" for i in geom_col_indices)
                lazy_insert_query = sql.SQL(
                    """
                    INSERT INTO {table} (query_hash, partition_keys, partition_keys_count)
                    SELECT {key},
                           ST_Collect(DISTINCT g.{geom_col}),
                           COUNT(DISTINCT g.{geom_col})
                    FROM ({query}) AS sub
                    CROSS JOIN LATERAL (VALUES """
                    + values_list
                    + """) AS g({geom_col})
                    WHERE g.{geom_col} IS NOT NULL
                    ON CONFLICT (query_hash) DO UPDATE SET
                        partition_keys = EXCLUDED.partition_keys,
                        partition_keys_count = EXCLUDED.partition_keys_count
                    """
                ).format(
                    table=sql.Identifier(table_name),
                    key=sql.Literal(key),
                    geom_col=sql.Identifier(geom_col),
                    query=sql.SQL(query),  # type: ignore[arg-type]
                )
            else:
                # Single-column fragment: collect directly
                lazy_insert_query = sql.SQL(
                    """
                    INSERT INTO {table} (query_hash, partition_keys, partition_keys_count)
                    SELECT {key},
                           ST_Collect(DISTINCT sub.{geom_col}),
                           COUNT(DISTINCT sub.{geom_col})
                    FROM ({query}) AS sub
                    ON CONFLICT (query_hash) DO UPDATE SET
                        partition_keys = EXCLUDED.partition_keys,
                        partition_keys_count = EXCLUDED.partition_keys_count
                    """
                ).format(
                    table=sql.Identifier(table_name),
                    key=sql.Literal(key),
                    geom_col=sql.Identifier(geom_col),
                    query=sql.SQL(query),  # type: ignore[arg-type]
                )

            self.cursor.execute(lazy_insert_query)
            self.db.commit()

            self._update_queries_table(key, partition_key)
            return True
        except Exception as e:
            logger.error(f"Failed to set BBox cache lazily for key {key}: {e}")
            if not self.db.closed:
                self.db.rollback()
            return False

    def get(self, key, partition_key="partition_key"):  # type: ignore[override]
        """Get bounding box geometry as WKB bytes from cache."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        table_name = f"{self.tableprefix}_cache_{partition_key}"
        self.cursor.execute(
            sql.SQL("SELECT ST_AsBinary(partition_keys) FROM {table} WHERE query_hash = %s").format(
                table=sql.Identifier(table_name)
            ),
            (key,),
        )
        result = self.cursor.fetchone()
        if result is None or result[0] is None:
            return None
        return bytes(result[0])

    def get_intersected(self, keys, partition_key="partition_key"):  # type: ignore[override]
        """Get geometric intersection of all cached geometries. Returns WKB bytes."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return None, 0

            filtered_keys = self.filter_existing_keys(keys, partition_key)
            if not filtered_keys:
                return None, 0

            intersect_sql = self._get_intersected_sql(filtered_keys, partition_key)
            query = sql.SQL("SELECT ST_AsBinary({intersect})").format(intersect=intersect_sql)
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is None or result[0] is None:
                return None, 0

            return bytes(result[0]), len(filtered_keys)
        except Exception as e:
            logger.error(f"Failed to get BBox intersection: {e}")
            return None, 0

    def _get_intersected_sql(self, keys: set[str], partition_key: str) -> sql.Composed:
        """Build SQL for chained ST_Intersection of raw geometry collections.

        Directly intersects the stored multipolygon/multipoint geometry collections.
        For a single key, returns the raw partition_keys geometry.
        For multiple keys, chains ST_Intersection(A, B).
        """
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        keys_list = list(keys)

        if len(keys_list) == 1:
            return sql.SQL("(SELECT partition_keys FROM {table} WHERE query_hash = {key})").format(
                table=sql.Identifier(table_name),
                key=sql.Literal(keys_list[0]),
            )

        # Build chained ST_Intersection on raw geometry collections
        result = sql.SQL("(SELECT partition_keys FROM {table} WHERE query_hash = {key})").format(
            table=sql.Identifier(table_name),
            key=sql.Literal(keys_list[0]),
        )

        for i in range(1, len(keys_list)):
            inner = sql.SQL("(SELECT partition_keys FROM {table} WHERE query_hash = {key})").format(
                table=sql.Identifier(table_name),
                key=sql.Literal(keys_list[i]),
            )
            result = sql.SQL("ST_Intersection({a}, {b})").format(a=result, b=inner)

        return result

    def get_intersected_lazy(self, keys, partition_key="partition_key"):
        """Get lazy intersection SQL for BBox geometries."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return None, 0

            filtered_keys = self.filter_existing_keys(keys, partition_key)
            if not filtered_keys:
                return None, 0

            # Return the ST_Intersection chain directly as the lazy result
            intersect_sql = self._get_intersected_sql(filtered_keys, partition_key)
            return intersect_sql.as_string(self.cursor), len(filtered_keys)
        except Exception as e:
            logger.error(f"Failed to get BBox lazy intersection: {e}")
            return None, 0

    def _get_buffered_intersected_sql(
        self, keys: set[str], partition_key: str, buffer_distance: float
    ) -> sql.Composed:
        """Build SQL for chained ST_Intersection of buffered geometry collections.

        Each stored geometry collection is buffered by ``buffer_distance`` before
        intersection. This produces the "co-occurrence zone" — the area within
        ``buffer_distance`` of ALL fragment types simultaneously.

        For a single key: ``ST_Buffer(partition_keys, buf)``
        For multiple keys: ``ST_Intersection(ST_Buffer(A, buf), ST_Buffer(B, buf))``
        """
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        keys_list = list(keys)
        buf = buffer_distance

        if len(keys_list) == 1:
            return sql.SQL(
                "ST_Buffer((SELECT partition_keys FROM {table} WHERE query_hash = {key}), {buf})"
            ).format(
                table=sql.Identifier(table_name),
                key=sql.Literal(keys_list[0]),
                buf=sql.Literal(buf),
            )

        result = sql.SQL(
            "ST_Buffer((SELECT partition_keys FROM {table} WHERE query_hash = {key}), {buf})"
        ).format(
            table=sql.Identifier(table_name),
            key=sql.Literal(keys_list[0]),
            buf=sql.Literal(buf),
        )

        for i in range(1, len(keys_list)):
            inner = sql.SQL(
                "ST_Buffer((SELECT partition_keys FROM {table} WHERE query_hash = {key}), {buf})"
            ).format(
                table=sql.Identifier(table_name),
                key=sql.Literal(keys_list[i]),
                buf=sql.Literal(buf),
            )
            result = sql.SQL("ST_Intersection({a}, {b})").format(a=result, b=inner)

        return result

    def get_spatial_filter(
        self,
        keys: set[str],
        partition_key: str = "partition_key",
        buffer_distance: float = 0.0,
    ) -> tuple[bytes, int] | None:
        """
        Get spatial filter geometry as WKB bytes for bounding boxes.

        Each cached geometry collection is buffered by ``buffer_distance`` before
        intersection. The resulting geometry already includes the full buffer, so
        callers should use ``ST_Intersects`` (not ``ST_DWithin``) to avoid double-buffering.

        Args:
            keys: Set of cache keys to intersect.
            partition_key: Partition key namespace.
            buffer_distance: Buffer distance in the SRID's native unit (meters
                for metric SRIDs). Applied to each geometry before intersection.

        Returns:
            Tuple of (WKB bytes, SRID) for the spatial filter geometry,
            or None if no cache hits.
        """
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return None

            filtered_keys = self.filter_existing_keys(keys, partition_key)
            if not filtered_keys:
                return None

            intersect_sql = self._get_buffered_intersected_sql(filtered_keys, partition_key, buffer_distance)
            query = sql.SQL("SELECT ST_AsBinary(({intersect_sql})::geometry)").format(
                intersect_sql=intersect_sql,
            )
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is None or result[0] is None:
                return None
            return bytes(result[0]), self.srid
        except Exception as e:
            logger.error(f"Failed to get BBox spatial filter as WKB: {e}")
            return None

    def get_spatial_filter_lazy(
        self,
        keys: set[str],
        partition_key: str = "partition_key",
        buffer_distance: float = 0.0,
    ) -> str | None:
        """
        Get spatial filter geometry SQL for bounding boxes.

        Each cached geometry collection is buffered by ``buffer_distance`` before
        intersection. The resulting geometry already includes the full buffer, so
        callers should use ``ST_Intersects`` (not ``ST_DWithin``) to avoid double-buffering.

        Args:
            keys: Set of cache keys to intersect.
            partition_key: Partition key namespace.
            buffer_distance: Buffer distance in the SRID's native unit (meters
                for metric SRIDs). Applied to each geometry before intersection.

        Returns:
            SQL string producing a geometry, or None if no cache hits.
        """
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return None

            filtered_keys = self.filter_existing_keys(keys, partition_key)
            if not filtered_keys:
                return None

            intersect_sql = self._get_buffered_intersected_sql(filtered_keys, partition_key, buffer_distance)
            return intersect_sql.as_string(self.cursor)
        except Exception as e:
            logger.error(f"Failed to get BBox spatial filter: {e}")
            return None
