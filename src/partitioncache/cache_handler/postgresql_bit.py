from datetime import datetime
from logging import getLogger

from bitarray import bitarray
from psycopg import sql

from partitioncache.cache_handler.postgresql_abstract import PostgreSQLAbstractCacheHandler

logger = getLogger("PartitionCache")


class PostgreSQLBitCacheHandler(PostgreSQLAbstractCacheHandler):
    def __repr__(self) -> str:
        return "postgresql_bit"

    def __init__(self, db_name: str, db_host: str, db_user: str, db_password: str, db_port: str | int, db_tableprefix: str, bitsize: int) -> None:
        """
        Initialize the cache handler with the given db name.
        This handler supports multiple partition keys but only integer datatypes (for bit arrays).
        """
        self.default_bitsize = bitsize  # Bitsize should be configured correctly by user (bitsize=1001 to store values 0-1000)
        super().__init__(db_name, db_host, db_user, db_password, db_port, db_tableprefix)

    def _recreate_metadata_table(self, supported_datatypes: set[str]) -> None:
        """
        Recreate metadata table with bitsize column for bit arrays.
        Extends the base metadata table functionality.
        """
        try:
            # Create metadata table with bitsize column for bit arrays
            self.cursor.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                    partition_key TEXT PRIMARY KEY,
                    datatype TEXT NOT NULL CHECK (datatype = 'integer'),
                    bitsize INTEGER NOT NULL DEFAULT {1},
                    created_at TIMESTAMP DEFAULT now()
                );""").format(
                    sql.Identifier(self.tableprefix + "_partition_metadata"), 
                    sql.Literal(self.default_bitsize)
                )
            )

            # Create queries table (same as base implementation)
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
        except Exception as e:
            logger.error(f"Failed to recreate bit metadata table: {e}")
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback after metadata table creation error: {rollback_error}")
            raise

    def _get_partition_bitsize(self, partition_key: str) -> int | None:
        """Get the bitsize for a partition key from metadata."""
        self.cursor.execute(
            sql.SQL("SELECT bitsize FROM {0} WHERE partition_key = %s").format(sql.Identifier(self.tableprefix + "_partition_metadata")), (partition_key,)
        )
        result = self.cursor.fetchone()
        return result[0] if result else None

    def _set_partition_bitsize(self, partition_key: str, bitsize: int) -> None:
        """Update the bitsize for a partition key in metadata."""
        self.cursor.execute(
            sql.SQL("UPDATE {0} SET bitsize = %s WHERE partition_key = %s").format(sql.Identifier(self.tableprefix + "_partition_metadata")),
            (bitsize, partition_key),
        )
        self.db.commit()

    def _create_partition_table(self, partition_key: str, bitsize: int | None = None) -> None:
        """Create a cache table for a specific partition key."""
        if bitsize is None:
            bitsize = self.default_bitsize

        table_name = f"{self.tableprefix}_cache_{partition_key}"

        # Create the cache table for bit arrays
        self.cursor.execute(
            sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                query_hash TEXT PRIMARY KEY,
                partition_keys BIT VARYING,
                partition_keys_count integer GENERATED ALWAYS AS (
                    CASE
                        WHEN partition_keys IS NULL THEN NULL
                        ELSE length(replace(partition_keys::text, '0',''))
                    END
                ) STORED
            );""").format(sql.Identifier(table_name))
        )

        # Insert metadata
        self.cursor.execute(
            sql.SQL("INSERT INTO {0} (partition_key, datatype, bitsize) VALUES (%s, %s, %s) ON CONFLICT (partition_key) DO NOTHING").format(
                sql.Identifier(self.tableprefix + "_partition_metadata")
            ),
            (partition_key, "integer", bitsize),
        )

        self.db.commit()

    def _ensure_partition_table(self, partition_key: str, bitsize: int | None = None) -> None:
        """Ensure a partition table exists."""
        try:
            # First ensure metadata table exists before trying to query it
            self._ensure_metadata_table_exists()
            
            existing_datatype = self._get_partition_datatype(partition_key)

            if existing_datatype is None:
                # Create new table
                self._create_partition_table(partition_key, bitsize)
        except Exception as e:
            logger.error(f"Failed to ensure partition table for {partition_key}: {e}")
            # Rollback and recreate metadata table if needed
            try:
                self.db.rollback()
                logger.warning(f"Attempting to recreate metadata table after error: {e}")
                self._recreate_metadata_table()
                self._create_partition_table(partition_key, bitsize)
            except Exception as recreate_error:
                logger.error(f"Failed to recreate partition table for {partition_key}: {recreate_error}")
                # Avoid infinite retry loops
                self.db.rollback()
                raise


    def set_set(self, key: str, value: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """
        Set the value of the given key in the cache for a specific partition key.
        Only integer values are supported for bit arrays.
        """
        if not value:
            return True

        try:
            # Ensure partition table exists
            self._ensure_partition_table(partition_key)

            # Get the bitsize for this partition
            bitsize = self._get_partition_bitsize(partition_key)
            if bitsize is None:
                bitsize = self.default_bitsize
            bitsize = int(bitsize)
            val = bitarray(bitsize)
            val.setall(0)
            for k in value:
                if isinstance(k, int):
                    val[k] = 1
                elif isinstance(k, str):
                    val[int(k)] = 1
                else:
                    raise ValueError(f"Only integer values are supported for bit arrays: {k} : {value}")
            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {0} (query_hash, partition_keys) VALUES (%s, %s) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys"
                ).format(sql.Identifier(table_name)),
                (key, val.to01()),
            )
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to set value for key {key} in partition {partition_key}: {e}")
            # Rollback the transaction to prevent "current transaction is aborted" error
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
            return False

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | None:
        """Get value from partition-specific cache table."""

        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        table_name = f"{self.tableprefix}_cache_{partition_key}"
        self.cursor.execute(
            sql.SQL("SELECT partition_keys FROM {0} WHERE query_hash = %s").format(sql.Identifier(table_name)),
            (key,),
        )
        result = self.cursor.fetchone()
        if result is None:
            return None

        # Handle NULL values stored via set_null()
        if result[0] is None:
            return None

        bitarray_result = bitarray(result[0])
        return set(bitarray_result.search(bitarray("1")))

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | None, int]:
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
        if result is None:
            return None, 0
        r = set(bitarray(result[0]).search(bitarray("1")))
        return r, len(keys_set)

    def get_intersected_sql(self, partition_key: str = "partition_key") -> sql.Composed:
        """Get intersection SQL for partition-specific table."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        return sql.SQL("SELECT BIT_AND(partition_keys) FROM (SELECT partition_keys FROM {0} WHERE query_hash = ANY(%s)) AS selected").format(
            sql.Identifier(table_name)
        )

    def get_intersected_sql_wk(self, keys, partition_key: str = "partition_key") -> str:
        """Get intersection SQL with keys for partition-specific table. Using ANY with properly escaped literals."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        # Use sql.Literal for each key to properly escape them and build array
        escaped_keys = [sql.Literal(key) for key in keys]
        keys_part = sql.SQL("ARRAY[{}]").format(sql.SQL(", ").join(escaped_keys))
        return (
            sql.SQL("SELECT BIT_AND(partition_keys) AS bit_result FROM (SELECT partition_keys FROM {0} WHERE query_hash = ANY({1})) AS selected")
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

        # Get the bitsize for this partition
        bitsize = self._get_partition_bitsize(partition_key)
        if bitsize is None:
            bitsize = self.default_bitsize
        bitsize = int(bitsize)

        r = (
            sql.SQL("""(
       WITH bit_result AS (
            {0}
        ),
        numbered_bits AS (
            SELECT
                bit_result,
                generate_series(0, {1} - 1) AS bit_index
            FROM bit_result
        )
        SELECT unnest(array_agg(bit_index)) AS {2}
        FROM numbered_bits
        WHERE get_bit(bit_result, bit_index) = 1)
        """)
            .format(
                sql.SQL(intersect_sql_str),  # type: ignore
                sql.Literal(bitsize),
                sql.Identifier(partition_key),
            )
            .as_string()
        )

        return r, len(filtered_keys)

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """PostgreSQL bit handler supports only integer datatype."""
        return {"integer"}

    def get_datatype(self, partition_key: str) -> str | None:
        """Get the datatype of the cache handler. If the partition key is not set, return None."""
        return self._get_partition_datatype(partition_key)

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype != "integer":
            raise ValueError("PostgreSQL bit handler supports only integer datatype")
        if "bitsize" in kwargs:
            bitsize = kwargs["bitsize"]
        else:
            bitsize = self.default_bitsize
        self._ensure_partition_table(partition_key, bitsize)
