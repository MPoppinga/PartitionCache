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

        # Create metadata table to track partition keys (no bitsize needed for roaring bitmaps)
        self.cursor.execute(
            sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                partition_key TEXT PRIMARY KEY,
                datatype TEXT NOT NULL CHECK (datatype = 'integer'),
                created_at TIMESTAMP DEFAULT now()
            );""").format(sql.Identifier(self.tableprefix + "_partition_metadata"))
        )

        # Create queries table (shared across all partition keys)
        self.cursor.execute(
            sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
            query_hash TEXT NOT NULL,
            query TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ok' CHECK (status IN ('ok', 'timeout', 'failed')),
            last_seen TIMESTAMP NOT NULL DEFAULT now(),
            PRIMARY KEY (query_hash, partition_key)
        );""").format(sql.Identifier(self.tableprefix + "_queries"))
        )

        self.db.commit()

    def _create_partition_table(self, partition_key: str) -> None:
        """Create a cache table for a specific partition key."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"

        # Create the cache table for roaring bitmaps
        self.cursor.execute(
            sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                query_hash TEXT PRIMARY KEY,
                partition_keys roaringbitmap,
                partition_keys_count integer NOT NULL GENERATED ALWAYS AS (rb_cardinality(partition_keys)) STORED
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

    def _ensure_partition_table(self, partition_key: str) -> None:
        """Ensure a partition table exists."""
        existing_datatype = self._get_partition_datatype(partition_key)

        if existing_datatype is None:
            # Create new table
            self._create_partition_table(partition_key)

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
            self._ensure_partition_table(partition_key)

            # Convert input to roaring bitmap
            if isinstance(value, BitMap):
                rb = value
            elif isinstance(value, bitarray):
                # Convert bitarray to roaring bitmap
                rb = BitMap()
                for pos in value.search(bitarray("1")):
                    rb.add(pos)
            elif isinstance(value, list | set):
                rb = BitMap()
                for item in value:
                    if isinstance(item, int):
                        rb.add(item)
                    elif isinstance(item, str):
                        rb.add(int(item))
                    else:
                        raise ValueError(f"Only integer values are supported for roaring bitmaps: {item} : {value}")
            else:
                raise ValueError(f"Unsupported value type for roaring bitmap: {type(value)}")

            # Convert roaring bitmap to bytes for storage
            if isinstance(rb, BitMap):
                rb_bytes = rb.serialize()
            else:
                # This path should not be reached due to the checks above, but it satisfies the type checker
                raise ValueError("Could not create BitMap instance")

            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {0} (query_hash, partition_keys) VALUES (%s, %s) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys"
                ).format(sql.Identifier(table_name)),
                (key, rb_bytes),
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

        # Deserialize roaring bitmap from bytes
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

        # Deserialize the intersected roaring bitmap
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

    def get_datatype(self, partition_key: str) -> str | None:
        """Get the datatype of the cache handler. If the partition key is not set, return None."""
        return self._get_partition_datatype(partition_key)

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype != "integer":
            raise ValueError("PostgreSQL roaring bit handler supports only integer datatype")
        self._ensure_partition_table(partition_key)
