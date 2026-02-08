"""
PostGIS Spatial Abstract Cache Handler.

Shared base class for spatial cache handlers (H3, BBox).
Extracts common infrastructure: geometry_column, srid, PostGIS extension checks,
queries table updates, and the unified "geometry" datatype.
"""

from abc import abstractmethod
from logging import getLogger

from psycopg import sql
from psycopg.errors import IntegrityError

from partitioncache.cache_handler.postgresql_abstract import PostgreSQLAbstractCacheHandler

logger = getLogger("PartitionCache")


class PostGISSpatialAbstractCacheHandler(PostgreSQLAbstractCacheHandler):
    """
    Abstract base class for PostGIS-based spatial cache handlers.

    Provides shared infrastructure for spatial handlers:
    - geometry_column and srid configuration
    - PostGIS extension verification
    - Unified "geometry" datatype
    - Common queries table update helper

    Subclasses implement storage-specific logic (H3 cells vs bounding box geometries).
    """

    def __init__(
        self,
        db_name: str,
        db_host: str,
        db_user: str,
        db_password: str,
        db_port: str | int,
        db_tableprefix: str,
        geometry_column: str = "geom",
        srid: int = 4326,
        timeout: str = "0",
    ) -> None:
        self.geometry_column = geometry_column
        self.srid = srid
        super().__init__(db_name, db_host, db_user, db_password, db_port, db_tableprefix, timeout)

        # Verify PostGIS is available
        try:
            self.cursor.execute("SELECT PostGIS_Version()")
            self.db.commit()
        except Exception as e:
            logger.warning(f"PostGIS extension check failed: {e}. Ensure PostGIS is installed.")
            try:
                self.db.rollback()
            except Exception:
                pass

        # Subclass-specific extension checks (e.g. h3-pg)
        self._check_extensions()

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """All spatial handlers use the 'geometry' datatype."""
        return {"geometry"}

    def _check_extensions(self) -> None:
        """Check for subclass-specific extensions. Override in subclasses if needed."""

    def _update_queries_table(self, key: str, partition_key: str, query_text: str = "") -> None:
        """Update the queries table with a cache entry reference.

        Inserts a new entry or updates last_seen on conflict.
        """
        queries_table = f"{self.tableprefix}_queries"
        try:
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {table} (query_hash, partition_key, query) VALUES (%s, %s, %s)"
                ).format(table=sql.Identifier(queries_table)),
                (key, partition_key, query_text),
            )
            self.db.commit()
        except IntegrityError:
            self.db.rollback()
            self.cursor.execute(
                sql.SQL(
                    "UPDATE {table} SET last_seen = now() WHERE query_hash = %s AND partition_key = %s"
                ).format(table=sql.Identifier(queries_table)),
                (key, partition_key),
            )
            self.db.commit()

    @abstractmethod
    def _ensure_partition_table(self, partition_key: str, datatype: str, **kwargs) -> bool:
        """Create the cache table with backend-specific column types."""

    @abstractmethod
    def set_cache(self, key, partition_key_identifiers, partition_key="partition_key") -> bool:
        """Store cache entries using backend-specific format."""
        ...

    @abstractmethod
    def set_cache_lazy(self, key: str, query: str, partition_key: str = "partition_key") -> bool:
        """Store cache entries by wrapping a fragment query with backend-specific SQL."""
        ...

    @abstractmethod
    def get(self, key, partition_key="partition_key"):
        """Get cached data for a key."""
        ...

    @abstractmethod
    def get_intersected(self, keys, partition_key="partition_key"):
        """Get intersection of cached data across multiple keys."""
        ...

    @abstractmethod
    def _get_intersected_sql(self, keys: set[str], partition_key: str) -> sql.Composed:
        """Build SQL for intersecting cached data."""
        ...

    @abstractmethod
    def get_intersected_lazy(self, keys, partition_key="partition_key"):
        """Get lazy intersection SQL."""
        ...

    @abstractmethod
    def get_spatial_filter_lazy(
        self,
        keys: set[str],
        partition_key: str = "partition_key",
        buffer_distance: float = 0.0,
    ) -> str | None:
        """Get spatial filter geometry SQL for cache-aware query optimization."""
        ...
