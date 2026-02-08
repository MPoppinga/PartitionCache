"""
PostGIS Bounding Box Cache Handler.

Stores cache entries as PostGIS MultiPolygon geometries representing grid-based bounding boxes.
Derives spatial cells from geometry columns by snapping centroids to a grid and creating envelope boxes.
Requires: PostgreSQL + PostGIS extension.
"""

from logging import getLogger

from psycopg import sql

from partitioncache.cache_handler.postgis_spatial_abstract import PostGISSpatialAbstractCacheHandler

logger = getLogger("PartitionCache")


class PostGISBBoxCacheHandler(PostGISSpatialAbstractCacheHandler):
    """
    Spatial cache handler using PostGIS bounding box geometries.

    Stores partition data as PostGIS geometry (MultiPolygon of grid cell envelopes).
    Derives cells by snapping geometry centroids to a grid (ST_SnapToGrid) and
    creating bounding boxes (ST_MakeEnvelope) around each cell center.
    Supports spatial filtering via geometric ST_Intersection.
    """

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
        self.cell_size = cell_size
        self.half_cell = cell_size / 2.0
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
        Store bounding box geometry by wrapping the fragment query with grid-based BB aggregation.

        The fragment query should return rows with a geometry column. This method wraps it
        to snap centroids to a grid, create envelope boxes, and collect them into a MultiPolygon.
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

            # Wrap fragment with grid-based bounding box aggregation
            lazy_insert_query = sql.SQL(
                """
                WITH grid_cells AS (
                    SELECT ST_SnapToGrid(ST_Centroid(sub.{geom_col}), {cell_size}) AS cell_origin
                    FROM ({query}) AS sub
                    GROUP BY cell_origin
                )
                INSERT INTO {table} (query_hash, partition_keys, partition_keys_count)
                SELECT {key},
                       ST_Collect(ST_MakeEnvelope(
                           ST_X(cell_origin) - {half_cell}, ST_Y(cell_origin) - {half_cell},
                           ST_X(cell_origin) + {half_cell}, ST_Y(cell_origin) + {half_cell},
                           {srid}
                       )),
                       COUNT(*)
                FROM grid_cells
                ON CONFLICT (query_hash) DO UPDATE SET
                    partition_keys = EXCLUDED.partition_keys,
                    partition_keys_count = EXCLUDED.partition_keys_count
                """
            ).format(
                table=sql.Identifier(table_name),
                key=sql.Literal(key),
                geom_col=sql.Identifier(geom_col),
                cell_size=sql.Literal(self.cell_size),
                half_cell=sql.Literal(self.half_cell),
                srid=sql.Literal(self.srid),
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
        """Get geometric intersection of all cached bounding boxes. Returns WKB bytes."""
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
        """Build SQL for chained ST_Intersection of geometry bounding boxes.

        Uses ST_Envelope to reduce multi-polygon collections to simple rectangles
        before intersection, which is much faster for large cell collections.
        """
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        keys_list = list(keys)

        if len(keys_list) == 1:
            return sql.SQL("(SELECT partition_keys FROM {table} WHERE query_hash = {key})").format(
                table=sql.Identifier(table_name),
                key=sql.Literal(keys_list[0]),
            )

        # Build chained ST_Intersection on envelopes (bounding boxes)
        # Using ST_Envelope reduces multi-polygon collections to simple rectangles
        # which makes intersection fast even with thousands of cells
        result = sql.SQL("ST_Envelope((SELECT partition_keys FROM {table} WHERE query_hash = {key}))").format(
            table=sql.Identifier(table_name),
            key=sql.Literal(keys_list[0]),
        )

        for i in range(1, len(keys_list)):
            inner = sql.SQL("ST_Envelope((SELECT partition_keys FROM {table} WHERE query_hash = {key}))").format(
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

    def get_spatial_filter(
        self,
        keys: set[str],
        partition_key: str = "partition_key",
        buffer_distance: float = 0.0,
    ) -> bytes | None:
        """
        Get spatial filter geometry as WKB bytes for bounding boxes.

        Executes the intersection SQL from _get_intersected_sql() and returns
        the resulting geometry as WKB binary bytes.

        Args:
            keys: Set of cache keys to intersect.
            partition_key: Partition key namespace.
            buffer_distance: Buffer distance in meters (applied externally via ST_DWithin).

        Returns:
            WKB bytes representing the spatial filter geometry, or None if no cache hits.
        """
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return None

            filtered_keys = self.filter_existing_keys(keys, partition_key)
            if not filtered_keys:
                return None

            intersect_sql = self._get_intersected_sql(filtered_keys, partition_key)
            query = sql.SQL("SELECT ST_AsBinary(({intersect_sql})::geometry)").format(
                intersect_sql=intersect_sql,
            )
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is None or result[0] is None:
                return None
            return bytes(result[0])
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

        Returns the ST_Intersection of all cached bounding box geometries.
        The buffer is applied via ST_DWithin in the query extension step (not here),
        since the ST_DWithin uses geography cast for meter-based distance.

        Args:
            keys: Set of cache keys to intersect.
            partition_key: Partition key namespace.
            buffer_distance: Buffer distance in meters (applied externally via ST_DWithin).

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

            intersect_sql = self._get_intersected_sql(filtered_keys, partition_key)
            return intersect_sql.as_string(self.cursor)
        except Exception as e:
            logger.error(f"Failed to get BBox spatial filter: {e}")
            return None
