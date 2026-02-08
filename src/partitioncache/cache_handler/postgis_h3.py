"""
PostGIS H3 Cache Handler.

Stores cache entries as arrays of H3 hexagonal cell indices (BIGINT[]).
Derives spatial cells from geometry columns using h3-pg extension.
Requires: PostgreSQL + PostGIS + h3-pg extension.
"""

from logging import getLogger

from psycopg import sql

from partitioncache.cache_handler.postgis_spatial_abstract import PostGISSpatialAbstractCacheHandler

logger = getLogger("PartitionCache")


class PostGISH3CacheHandler(PostGISSpatialAbstractCacheHandler):
    """
    Spatial cache handler using H3 hexagonal cells.

    Stores partition data as BIGINT[] arrays of H3 cell indices.
    Derives cells from geometry columns via h3_lat_lng_to_cell(ST_Centroid(geom)::point, resolution).
    Supports buffered spatial filtering via h3_grid_disk (k-ring expansion).
    """

    def __init__(
        self,
        db_name: str,
        db_host: str,
        db_user: str,
        db_password: str,
        db_port: str | int,
        db_tableprefix: str,
        resolution: int = 9,
        geometry_column: str = "geom",
        srid: int = 4326,
        timeout: str = "0",
    ) -> None:
        self.resolution = resolution
        super().__init__(db_name, db_host, db_user, db_password, db_port, db_tableprefix, geometry_column, srid, timeout)

    def _check_extensions(self) -> None:
        """Verify h3-pg extension is available."""
        try:
            self.cursor.execute("SELECT h3_lat_lng_to_cell(POINT(0, 0), 9)")
            self.db.commit()
        except Exception as e:
            logger.warning(f"h3-pg extension check failed: {e}. Ensure h3-pg is installed.")
            try:
                self.db.rollback()
            except Exception:
                pass

    def __repr__(self) -> str:
        return "postgis_h3"

    def _ensure_partition_table(self, partition_key: str, datatype: str, **kwargs) -> bool:
        """Create cache table with BIGINT[] column for H3 cell indices."""
        try:
            table_name = f"{self.tableprefix}_cache_{partition_key}"

            # Create the cache table with BIGINT[] for H3 cell indices
            self.cursor.execute(
                sql.SQL(
                    """CREATE TABLE IF NOT EXISTS {table} (
                        query_hash TEXT PRIMARY KEY,
                        partition_keys BIGINT[],
                        partition_keys_count INTEGER
                    )"""
                ).format(table=sql.Identifier(table_name))
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
            logger.error(f"Failed to ensure H3 partition table for {partition_key}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            raise

    def set_cache(self, key, partition_key_identifiers, partition_key="partition_key"):
        """Store H3 cell indices in the cache."""
        if not partition_key_identifiers:
            return True

        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                if not self._ensure_partition_table(partition_key, "geometry"):
                    return False

            val = list(partition_key_identifiers)
            table_name = f"{self.tableprefix}_cache_{partition_key}"

            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {table} (query_hash, partition_keys, partition_keys_count) "
                    "VALUES (%s, %s, %s) "
                    "ON CONFLICT (query_hash) DO UPDATE SET "
                    "partition_keys = EXCLUDED.partition_keys, "
                    "partition_keys_count = EXCLUDED.partition_keys_count"
                ).format(table=sql.Identifier(table_name)),
                (key, val, len(val)),
            )

            self._update_queries_table(key, partition_key)
            return True
        except Exception as e:
            logger.error(f"Failed to set H3 cache for key {key}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass
            return False

    def set_cache_lazy(self, key: str, query: str, partition_key: str = "partition_key") -> bool:
        """
        Store H3 cell indices by wrapping the fragment query with H3 conversion.

        The fragment query should return rows with a geometry column. This method wraps it
        to extract distinct H3 cell indices via h3_lat_lng_to_cell(ST_Centroid(geom)::point, resolution).
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

            # Build centroid expression - transform to WGS84 if SRID != 4326
            # h3_lat_lng_to_cell expects WGS84 (EPSG:4326) coordinates
            geom_id = sql.Identifier(geom_col)
            if self.srid != 4326:
                centroid_sub = sql.SQL("ST_Transform(ST_Centroid(sub.{gc}), 4326)::point").format(gc=geom_id)
                centroid_sub2 = sql.SQL("ST_Transform(ST_Centroid(sub2.{gc}), 4326)::point").format(gc=geom_id)
            else:
                centroid_sub = sql.SQL("ST_Centroid(sub.{gc})::point").format(gc=geom_id)
                centroid_sub2 = sql.SQL("ST_Centroid(sub2.{gc})::point").format(gc=geom_id)

            # Wrap fragment with H3 cell conversion
            lazy_insert_query = sql.SQL(
                """
                INSERT INTO {table} (query_hash, partition_keys, partition_keys_count)
                SELECT {key},
                       ARRAY(SELECT DISTINCT h3_lat_lng_to_cell({centroid_sub}, {resolution})::bigint
                             FROM ({query}) AS sub),
                       (SELECT COUNT(DISTINCT h3_lat_lng_to_cell({centroid_sub2}, {resolution})::bigint)
                        FROM ({query}) AS sub2)
                ON CONFLICT (query_hash) DO UPDATE SET
                    partition_keys = EXCLUDED.partition_keys,
                    partition_keys_count = EXCLUDED.partition_keys_count
                """
            ).format(
                table=sql.Identifier(table_name),
                key=sql.Literal(key),
                centroid_sub=centroid_sub,
                centroid_sub2=centroid_sub2,
                resolution=sql.Literal(self.resolution),
                query=sql.SQL(query),  # type: ignore[arg-type]
            )

            self.cursor.execute(lazy_insert_query)
            self.db.commit()

            self._update_queries_table(key, partition_key)
            return True
        except Exception as e:
            logger.error(f"Failed to set H3 cache lazily for key {key}: {e}")
            if not self.db.closed:
                self.db.rollback()
            return False

    def get(self, key, partition_key="partition_key"):
        """Get H3 cell indices from cache."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        table_name = f"{self.tableprefix}_cache_{partition_key}"
        self.cursor.execute(
            sql.SQL("SELECT partition_keys FROM {table} WHERE query_hash = %s").format(
                table=sql.Identifier(table_name)
            ),
            (key,),
        )
        result = self.cursor.fetchone()
        if result is None or result[0] is None:
            return None
        return set(result[0])

    def get_intersected(self, keys, partition_key="partition_key"):
        """Get intersection of H3 cell arrays (standard BIGINT[] intersection)."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return None, 0

            filtered_keys = self.filter_existing_keys(keys, partition_key)
            if not filtered_keys:
                return None, 0

            query = self._get_intersected_sql(filtered_keys, partition_key)
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            if result is None or result[0] is None:
                return None, 0

            return set(result[0]), len(filtered_keys)
        except Exception as e:
            logger.error(f"Failed to get H3 intersection: {e}")
            return None, 0

    def _get_intersected_sql(self, keys: set[str], partition_key: str) -> sql.Composed:
        """Build SQL for BIGINT[] array intersection using INTERSECT (intarray & only supports integer[])."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        keys_list = list(keys)

        if len(keys_list) == 1:
            return sql.SQL("SELECT partition_keys FROM {table} WHERE query_hash = {key}").format(
                table=sql.Identifier(table_name),
                key=sql.Literal(keys_list[0]),
            )

        # Build INTERSECT-based array intersection for BIGINT[] arrays
        intersect_parts = [
            sql.SQL("SELECT unnest(partition_keys) FROM {table} WHERE query_hash = {key}").format(
                table=sql.Identifier(table_name),
                key=sql.Literal(key),
            )
            for key in keys_list
        ]

        intersect_query = sql.SQL(" INTERSECT ").join(intersect_parts)
        return sql.SQL("SELECT ARRAY({intersect}) AS partition_keys").format(
            intersect=intersect_query,
        )

    def get_intersected_lazy(self, keys, partition_key="partition_key"):
        """Get lazy intersection SQL for H3 cell arrays."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return None, 0

            filtered_keys = self.filter_existing_keys(keys, partition_key)
            if not filtered_keys:
                return None, 0

            intersect_sql = self._get_intersected_sql(filtered_keys, partition_key)
            query = sql.SQL("SELECT unnest(({intersectsql})) AS {partition_col}").format(
                intersectsql=intersect_sql,
                partition_col=sql.Identifier(partition_key),
            )
            return query.as_string(), len(filtered_keys)
        except Exception as e:
            logger.error(f"Failed to get H3 lazy intersection: {e}")
            return None, 0

    def get_spatial_filter_lazy(
        self,
        keys: set[str],
        partition_key: str = "partition_key",
        buffer_distance: float = 0.0,
    ) -> str | None:
        """
        Get spatial filter geometry SQL for H3 cells with optional k-ring buffer.

        Returns SQL that produces a geometry by:
        1. Intersecting all cached H3 cell arrays
        2. Expanding cells by k-ring based on buffer_distance
        3. Converting cells to boundaries and unioning into a single geometry

        Args:
            keys: Set of cache keys to intersect.
            partition_key: Partition key namespace.
            buffer_distance: Buffer distance in meters. Converted to k-ring radius.

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

            # Calculate k-ring size from buffer distance
            if buffer_distance > 0:
                # k = ceil(buffer_distance / h3_edge_length_at_resolution)
                # Use SQL function for accuracy
                k_ring_sql = sql.SQL(
                    """
                    WITH intersected AS ({intersect_sql}),
                    cells AS (SELECT unnest(partition_keys)::h3index AS cell FROM intersected),
                    k_val AS (
                        SELECT CEIL({buffer_m} / h3_get_hexagon_edge_length_avg({res}, 'm'))::INTEGER AS k
                    ),
                    buffered AS (
                        SELECT DISTINCT h3_grid_disk(cells.cell, k_val.k) AS cell
                        FROM cells, k_val
                    )
                    SELECT ST_Union(ST_SetSRID(h3_cell_to_boundary(cell)::geometry, 4326)) FROM buffered
                    """
                ).format(
                    intersect_sql=intersect_sql,
                    buffer_m=sql.Literal(buffer_distance),
                    res=sql.Literal(self.resolution),
                )
            else:
                k_ring_sql = sql.SQL(
                    """
                    WITH intersected AS ({intersect_sql}),
                    cells AS (SELECT unnest(partition_keys)::h3index AS cell FROM intersected)
                    SELECT ST_Union(ST_SetSRID(h3_cell_to_boundary(cell)::geometry, 4326)) FROM cells
                    """
                ).format(intersect_sql=intersect_sql)

            return k_ring_sql.as_string(self.cursor)
        except Exception as e:
            logger.error(f"Failed to get H3 spatial filter: {e}")
            return None
