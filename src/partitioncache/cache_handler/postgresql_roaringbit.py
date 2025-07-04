from datetime import datetime
from logging import getLogger

from bitarray import bitarray
from psycopg import sql
from pyroaring import BitMap

from partitioncache.cache_handler.postgresql_abstract import PostgreSQLAbstractCacheHandler

logger = getLogger("PartitionCache")


class PostgreSQLRoaringBitCacheHandler(PostgreSQLAbstractCacheHandler):
    def __repr__(self) -> str:
        return "postgresql_roaringbit"

    def __init__(self, db_name: str, db_host: str, db_user: str, db_password: str, db_port: str | int, db_tableprefix: str) -> None:
        """
        Initialize the cache handler with the given db name.
        This handler supports multiple partition keys but only integer datatypes (for roaring bitmaps).
        """
        super().__init__(db_name, db_host, db_user, db_password, db_port, db_tableprefix)

        # Enable roaringbitmap extension if not already enabled
        try:
            self.cursor.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap;")
            self.db.commit()
        except Exception as e:
            logger.warning(f"Failed to create roaringbitmap extension: {e}")
            # Continue anyway - extension might already exist

    def _create_partition_table(self, partition_key: str) -> None:
        """Create a cache table for a specific partition key."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"

        try:
            # Check if roaringbitmap extension is available
            self.cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'roaringbitmap';")
            if not self.cursor.fetchone():
                # Try to create the extension first
                try:
                    self.cursor.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap;")
                    self.db.commit()
                    logger.info("Successfully created roaringbitmap extension")
                except Exception as ext_error:
                    logger.error(f"roaringbitmap extension not found and cannot be created: {ext_error}")
                    raise RuntimeError("roaringbitmap extension required but not available") from ext_error

            # Create the cache table for roaring bitmaps with improved error handling
            self.cursor.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                    query_hash TEXT PRIMARY KEY,
                    partition_keys roaringbitmap,
                    partition_keys_count integer GENERATED ALWAYS AS (
                        CASE
                            WHEN partition_keys IS NULL THEN NULL
                            ELSE rb_cardinality(partition_keys)
                        END
                    ) STORED
                );""").format(sql.Identifier(table_name))
            )

            # Insert metadata
            self.cursor.execute(
                sql.SQL("INSERT INTO {0} (partition_key, datatype) VALUES (%s, %s) ON CONFLICT (partition_key) DO NOTHING").format(
                    sql.Identifier(self.tableprefix + "_partition_metadata")
                ),
                (partition_key, "integer"),
            )

            self.db.commit()

        except Exception as e:
            logger.error(f"Failed to create roaringbitmap table {table_name}: {e}")
            self.db.rollback()
            raise

    def _ensure_partition_table(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Ensure a partition table exists."""
        try:
            # First ensure metadata table exists before trying to query it
            self._ensure_metadata_table_exists()

            existing_datatype = self._get_partition_datatype(partition_key)

            if existing_datatype is None:
                # Create new table
                self._create_partition_table(partition_key)
        except Exception as e:
            logger.error(f"Failed to ensure roaringbit partition table for {partition_key}: {e}")
            # Rollback and recreate metadata table if needed
            try:
                self.db.rollback()
                logger.warning(f"Attempting to recreate roaringbit metadata table after error: {e}")
                self._recreate_metadata_table(self.get_supported_datatypes())
                self._create_partition_table(partition_key)
            except Exception as recreate_error:
                logger.error(f"Failed to recreate roaringbit partition table for {partition_key}: {recreate_error}")
                # Avoid infinite retry loops
                self.db.rollback()
                raise

    def set_set(
        self,
        key: str,
        value: set[int] | set[str] | set[float] | set[datetime] | BitMap | bitarray | list,
        partition_key: str = "partition_key",
    ) -> bool:
        """
        Set the value of the given key in the cache for a specific partition key.
        Only integer values are supported for roaring bitmaps.

        Args:
            key: The cache key
            value: Can be a set of integers, a BitMap, a bitarray, or a list of integers
            partition_key: The partition key identifier
        """
        if not value:
            return True

        try:
            # Ensure partition table exists
            self._ensure_partition_table(partition_key, "integer")

            # Convert input to a list of integers for rb_build
            if isinstance(value, BitMap):
                value_list = list(value)
            elif isinstance(value, bitarray):
                value_list = [i for i, bit in enumerate(value) if bit]
            elif isinstance(value, list | set):
                # Validate that all items can be converted to integers without loss
                value_list = []
                for item in value:
                    if not isinstance(item, int) and isinstance(item, float) and not item.is_integer():
                        raise ValueError("Only integer values are supported for roaring bitmaps")
                    value_list.append(int(item))  # type: ignore
            else:
                raise ValueError(f"Unsupported value type for roaring bitmap: {type(value)}")

            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {0} (query_hash, partition_keys) VALUES (%s, rb_build(%s)) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys"
                ).format(sql.Identifier(table_name)),
                (key, value_list),
            )
            self.db.commit()
            return True
        except ValueError as e:
            logger.error(f"Invalid value for key {key} in partition {partition_key}: {e}")
            raise e
        except Exception as e:
            logger.error("Failed to set value for key %s in partition %s: %s", key, partition_key, e)
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error("Failed to rollback transaction: %s", rollback_error)
            return False

    def get(self, key: str, partition_key: str = "partition_key") -> BitMap | None:  # type: ignore
        """Get value from partition-specific cache table."""

        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        table_name = f"{self.tableprefix}_cache_{partition_key}"
        self.cursor.execute(
            sql.SQL("SELECT partition_keys::bytea FROM {0} WHERE query_hash = %s").format(sql.Identifier(table_name)),
            (key,),
        )
        result = self.cursor.fetchone()
        if result is None or result[0] is None:
            return None

        # Deserialize roaring bitmap from bytes and return as BitMap
        rb = BitMap.deserialize(result[0])
        return rb

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[BitMap | None, int]:  # type: ignore
        """Get intersection from partition-specific table."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None, 0

        # Check which exist
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        query = sql.SQL("SELECT query_hash FROM {0} WHERE query_hash = ANY(%s) AND partition_keys IS NOT NULL").format(sql.Identifier(table_name))
        self.cursor.execute(query, (list(keys),))
        keys_set = {x[0] for x in self.cursor.fetchall()}

        if not keys_set:
            return None, 0

        q = self.get_intersected_sql(partition_key)
        self.cursor.execute(q, (list(keys_set),))

        result = self.cursor.fetchone()
        if result is None or result[0] is None:
            return None, 0

        # Deserialize the intersected roaring bitmap and return as BitMap
        rb = BitMap.deserialize(result[0])
        return rb, len(keys_set)

    def get_intersected_sql(self, partition_key: str = "partition_key") -> sql.Composed:
        """Get intersection SQL for partition-specific table."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        return sql.SQL("SELECT rb_and_agg(partition_keys)::bytea FROM (SELECT partition_keys FROM {0} WHERE query_hash = ANY(%s)) AS selected").format(
            sql.Identifier(table_name)
        )

    def get_intersected_sql_wk(self, keys, partition_key: str = "partition_key") -> str:
        """Get intersection SQL with keys for partition-specific table. Using ANY with properly escaped literals."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        # Use sql.Literal for each key to properly escape them and build array
        escaped_keys = [sql.Literal(key) for key in keys]
        keys_part = sql.SQL("ARRAY[{}]").format(sql.SQL(", ").join(escaped_keys))
        return (
            sql.SQL("SELECT rb_and_agg(partition_keys) AS rb_result FROM (SELECT partition_keys FROM {0} WHERE query_hash = ANY({1})) AS selected")
            .format(
                sql.Identifier(table_name),
                keys_part,
            )
            .as_string()
        )

    def get_intersected_lazy(self, keys: set[str], partition_key: str = "partition_key") -> tuple[str | None, int]:
        """Get lazy intersection for partition-specific table."""
        filtered_keys = self.filter_existing_keys(keys, partition_key)

        if not filtered_keys:
            return None, 0

        intersect_sql_str = self.get_intersected_sql_wk(filtered_keys, partition_key)

        r = (
            sql.SQL("""(
        WITH rb_result AS (
            {0}
        )
        SELECT unnest(rb_to_array(rb_result)) AS {1}
        FROM rb_result
        WHERE rb_result IS NOT NULL)
        """)
            .format(
                sql.SQL(intersect_sql_str),  # type: ignore
                sql.Identifier(partition_key),
            )
            .as_string()
        )

        return r, len(filtered_keys)

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """PostgreSQL roaring bit handler supports only integer datatype."""
        return {"integer"}

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype != "integer":
            raise ValueError("PostgreSQL roaring bit handler supports only integer datatype")
        self._ensure_partition_table(partition_key, datatype)
