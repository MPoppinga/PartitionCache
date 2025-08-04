import time
from datetime import datetime
from logging import getLogger

from bitarray import bitarray
from psycopg import sql
from psycopg.errors import IntegrityError

from partitioncache.cache_handler.postgresql_abstract import PostgreSQLAbstractCacheHandler

logger = getLogger("PartitionCache")


class PostgreSQLBitCacheHandler(PostgreSQLAbstractCacheHandler):
    def __repr__(self) -> str:
        return "postgresql_bit"

    def __init__(
        self, db_name: str, db_host: str, db_user: str, db_password: str, db_port: str | int, db_tableprefix: str, bitsize: int, timeout: str = "0"
    ) -> None:
        """
        Initialize the cache handler with the given db name.
        This handler supports multiple partition keys but only integer datatypes (for bit arrays).

        Args:
            timeout: Statement timeout in seconds (default: "0" for no timeout)
        """
        self.default_bitsize = bitsize
        super().__init__(db_name, db_host, db_user, db_password, db_port, db_tableprefix, timeout)

    def _recreate_metadata_table(self, supported_datatypes: set[str]) -> None:
        """
        Recreate metadata table with bitsize column for bit arrays.
        Extends the base metadata table functionality.
        """
        try:
            # Check if the metadata table already exists
            self.cursor.execute(sql.SQL("SELECT to_regclass('{0}')").format(sql.Identifier(self.tableprefix + "_partition_metadata")))
            result = self.cursor.fetchone()
            if result and result[0]:
                return

            logger.warning(f"! Setting up {self.tableprefix} cache handler: Loading SQL functions")
            # Load SQL functions first
            self._load_sql_functions()

            # Create metadata table with bitsize column for bit arrays
            self.cursor.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                    partition_key TEXT PRIMARY KEY,
                    datatype TEXT NOT NULL CHECK (datatype = 'integer'),
                    bitsize INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT now()
                );""").format(sql.Identifier(self.tableprefix + "_partition_metadata"))
            )
            logger.info("BIT METADATA: Bit metadata table created successfully")

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
            logger.info("BIT METADATA: Queries table created successfully")

            # Create trigger function for handling bitsize changes (now that SQL functions are loaded)
            self._create_bitsize_trigger()
            logger.info("BIT METADATA: Bitsize trigger created successfully")

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

    def _load_sql_functions(self) -> None:
        """Load SQL functions from the cache handlers SQL file."""
        try:
            from pathlib import Path

            # Get the SQL file path for cache handlers
            sql_cache_file = Path(__file__).parent / "postgresql_cache_handlers.sql"

            if not sql_cache_file.exists():
                logger.warning(f"SQL cache handlers file not found at {sql_cache_file}")
                return

            # Read and execute the SQL file
            sql_cache_content = sql_cache_file.read_text()
            logger.info(f"LOADING SQL: Executing SQL functions file ({len(sql_cache_content)} chars)")
            self.cursor.execute(sql_cache_content)  # type: ignore[arg-type]
            logger.info("LOADING SQL: SQL functions executed successfully")
        except Exception as e:
            logger.warning(f"Failed to load SQL functions (this is OK if they're already loaded): {e}")

    def _create_bitsize_trigger(self) -> None:
        """Create trigger function and trigger for handling bitsize changes."""
        try:
            logger.info(f"TRIGGER: Calling partitioncache_create_bitsize_trigger for {self.tableprefix}")
            # Call the SQL function to create the trigger
            self.cursor.execute(sql.SQL("SELECT partitioncache_create_bitsize_trigger(%s)"), (self.tableprefix,))
            # TODO Need additional testing
            logger.info(f"TRIGGER: Bitsize trigger created successfully for {self.tableprefix}")
        except Exception as e:
            logger.error(f"TRIGGER: Failed to create bitsize trigger for {self.tableprefix}: {e}")
            raise

    def _create_partition_table(self, partition_key: str, bitsize: int) -> None:
        """Create a cache table for a specific partition key."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"

        # Create the cache table for bit arrays with fixed BIT(bitsize)
        self.cursor.execute(
            sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                query_hash TEXT PRIMARY KEY,
                partition_keys BIT({1}),
                partition_keys_count integer GENERATED ALWAYS AS (
                    CASE
                        WHEN partition_keys IS NULL THEN NULL
                        ELSE length(replace(partition_keys::text, '0',''))
                    END
                ) STORED
            );""").format(sql.Identifier(table_name), sql.Literal(bitsize))
        )

        # Insert metadata
        self.cursor.execute(
            sql.SQL("INSERT INTO {0} (partition_key, datatype, bitsize) VALUES (%s, %s, %s) ON CONFLICT (partition_key) DO NOTHING").format(
                sql.Identifier(self.tableprefix + "_partition_metadata")
            ),
            (partition_key, "integer", bitsize),
        )

        self.db.commit()

    def _ensure_partition_table(self, partition_key: str, datatype: str, **kwargs) -> tuple[bool, int]:
        """Ensure a partition table exists using SQL bootstrap function with proper concurrency control.

        Returns:
            tuple: (success, actual_bitsize) - The actual bitsize that was set/retrieved
        """
        bitsize = kwargs.get("bitsize", self.default_bitsize)
        try:
            # Check if partition already exists with sufficient bitsize
            existing_bitsize = self._get_partition_bitsize(partition_key)
            if existing_bitsize is not None and existing_bitsize >= bitsize:
                return True, existing_bitsize

            # Use advisory lock to prevent race conditions in schema creation
            # Generate consistent lock ID from partition key
            lock_id = hash(f"{self.tableprefix}_{partition_key}") % (2**31 - 1)
            logger.info(f"ACQUIRING ADVISORY LOCK: {lock_id} for partition {partition_key}")

            # Try to acquire lock with timeout
            self.cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_id,))
            result = self.cursor.fetchone()
            lock_acquired = result[0] if result else False

            if not lock_acquired:
                # Another thread is creating this partition, wait and check if it's done
                logger.info(f"Skipping partition creation: Another thread is creating {partition_key}")

                for _ in range(10):  # Wait up to 10 seconds
                    time.sleep(1)
                    existing_bitsize = self._get_partition_bitsize(partition_key)
                    if existing_bitsize is not None and existing_bitsize >= bitsize:
                        logger.info(f"PARTITION CREATED BY OTHER THREAD: {partition_key} with bitsize {existing_bitsize}")
                        return True, existing_bitsize
                    elif existing_bitsize is not None:
                        logger.info(f"PARTITION CREATED BY OTHER THREAD BUT INSUFFICIENT: {partition_key} bitsize {existing_bitsize} < required {bitsize}")
                        break  # Exit loop and try to acquire lock to expand

                # If still not created, try to acquire lock blocking
                logger.info(f"FALLBACK TO BLOCKING LOCK: {lock_id} for partition {partition_key}")
                self.cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))

            # Double-check if partition was created while waiting for lock
            existing_bitsize = self._get_partition_bitsize(partition_key)
            if existing_bitsize is not None and existing_bitsize >= bitsize:
                return True, existing_bitsize

            # Load SQL functions first to ensure they're available
            self._load_sql_functions()

            # Use SQL bootstrap function to ensure metadata tables exist
            self.cursor.execute(
                sql.SQL("SELECT partitioncache_ensure_metadata_tables(%s)"),
                (self.tableprefix,)
            )

            # Use SQL bootstrap function to create partition table and metadata entry
            # Let the bootstrap function handle metadata creation atomically
            self.cursor.execute(
                sql.SQL("SELECT partitioncache_bootstrap_partition(%s, %s, %s, %s, %s)"),
                (self.tableprefix, partition_key, datatype, "bit", bitsize)
            )

            # Get the actual bitsize that was set (for validation)
            actual_bitsize = self._get_partition_bitsize(partition_key)
            if actual_bitsize is None:
                raise ValueError(f"No bitsize found for partition {partition_key} after bootstrap")

            logger.info(f"PARTITION BOOTSTRAP COMPLETED: {partition_key} with bitsize {actual_bitsize}")
            self.db.commit()
            return True, actual_bitsize
        except Exception as e:
            logger.error(f"Failed to ensure partition table for {partition_key}: {e}")
            self.db.rollback()
            raise

    def set_cache(self, key: str, partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """
        Set the partition key identifiers of the given hash in the cache for a specific partition key.
        Only integer values are supported for bit arrays.
        """
        if not partition_key_identifiers:
            return True

        try:
            # Convert all keys to integers first
            int_keys = []
            for k in partition_key_identifiers:
                if isinstance(k, int):
                    int_keys.append(k)
                elif isinstance(k, str):
                    int_keys.append(int(k))
                else:
                    raise ValueError(f"Only integer values are supported for bit arrays: {k} : {partition_key_identifiers}")

            # Determine required bitsize based on data
            max_value = max(int_keys)
            required_bitsize = max(max_value + 1, self.default_bitsize)  # Ensure at least default size

            # Atomically ensure partition table exists and get actual bitsize
            _, actual_bitsize = self._ensure_partition_table(partition_key, "integer", bitsize=required_bitsize)

            # Validate all keys fit within the actual bitsize
            if max_value >= actual_bitsize:
                raise ValueError(f"Partition key {max_value} exceeds bitsize {actual_bitsize} for partition {partition_key}")

            # Create fixed-length bitarray using actual bitsize
            val = bitarray(actual_bitsize)
            val.setall(0)
            for k in int_keys:
                val[k] = 1
            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {0} (query_hash, partition_keys) VALUES (%s, %s) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys"
                ).format(sql.Identifier(table_name)),
                (key, val.to01()),
            )

            # Also create entry in queries table for exists() method to work properly
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {0} (query_hash, partition_key, query) VALUES (%s, %s, %s) ON CONFLICT (query_hash, partition_key) DO UPDATE SET last_seen = now()"
                ).format(sql.Identifier(self.tableprefix + "_queries")),
                (key, partition_key, ""),  # Empty query text for cache entries
            )

            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to set partition key identifiers for hash {key} in partition {partition_key}: {e}")
            try:
                self.db.rollback()
            except Exception:
                pass  # Ignore rollback errors
            return False

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | None:
        """Get value from partition-specific cache table."""

        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        table_name = f"{self.tableprefix}_cache_{partition_key}"
        try:
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
        except Exception as e:
            # Cache table might not exist yet - this is OK, return None
            if "does not exist" in str(e).lower() or "relation" in str(e).lower():
                try:
                    self.db.rollback()
                except Exception:
                    pass
                return None
            else:
                logger.error(f"Failed to get value for key {key} in partition {partition_key}: {e}")
                try:
                    self.db.rollback()
                except Exception:
                    pass
                return None

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | None, int]:
        """Get intersection from partition-specific table."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None, 0

        # Check which exist
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        try:
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
        except Exception as e:
            # Cache table might not exist yet - this is OK, return None
            if "does not exist" in str(e).lower() or "relation" in str(e).lower():
                try:
                    self.db.rollback()
                except Exception:
                    pass
                return None, 0
            else:
                logger.error(f"Failed to get intersected values for keys {keys} in partition {partition_key}: {e}")
                try:
                    self.db.rollback()
                except Exception:
                    pass
                return None, 0

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
        filtered_keys = self.filter_existing_keys(keys, partition_key)  # TODO: Check if this is needed, or if we can just use the keys directly

        if not filtered_keys:
            return None, 0

        intersect_sql_str = self.get_intersected_sql_wk(filtered_keys, partition_key)

        r = (
            sql.SQL("""(
       WITH bit_result AS (
            {0}
        ),
        numbered_bits AS (
            SELECT
                bit_result,
                generate_series(0, bit_length(bit_result) - 1) AS bit_index
            FROM bit_result
        )
        SELECT unnest(array_agg(bit_index)) AS {1}
        FROM numbered_bits
        WHERE get_bit(bit_result, bit_index) = 1)
        """)
            .format(
                sql.SQL(intersect_sql_str),  # type: ignore
                sql.Identifier(partition_key),
            )
            .as_string()
        )

        return r, len(filtered_keys)

    def set_cache_lazy(self, key: str, query: str, partition_key: str = "partition_key") -> bool:
        """
        Store partition key identifiers in cache by executing the provided query directly.

        Args:
            key: Cache key (query hash)
            query: SQL query that returns partition key values to cache
            partition_key: Partition key namespace

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Security check
            if "DELETE " in query.upper() or "DROP " in query.upper():
                logger.error("Query contains DELETE or DROP")
                return False

            # Get partition bitsize - assumes integer datatype
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None or datatype != "integer":
                logger.error(f"Partition key '{partition_key}' not registered with integer datatype")
                return False

            bitsize = self._get_partition_bitsize(partition_key)
            if bitsize is None:
                logger.error(f"No bitsize found for partition key '{partition_key}'")
                return False

            table_name = f"{self.tableprefix}_cache_{partition_key}"

            # Build lazy insertion query that creates a bit array from query results
            lazy_insert_query = sql.SQL(
                """
                WITH query_result AS (
                    {query}
                ),
                bit_positions AS (
                    SELECT {partition_col}::INTEGER AS position
                    FROM query_result
                    WHERE {partition_col}::INTEGER >= 0 AND {partition_col}::INTEGER < {bitsize}
                ),
                bit_array AS (
                    SELECT generate_series(0, {bitsize} - 1) AS bit_index
                ),
                bit_string AS (
                    SELECT string_agg(
                        CASE WHEN bit_array.bit_index IN (SELECT position FROM bit_positions)
                             THEN '1'
                             ELSE '0'
                        END,
                        ''
                        ORDER BY bit_array.bit_index
                    ) AS bit_value
                    FROM bit_array
                )
                INSERT INTO {table_name} (query_hash, partition_keys)
                SELECT {key}, bit_value::BIT({bitsize})
                FROM bit_string
                ON CONFLICT (query_hash) DO UPDATE SET
                    partition_keys = EXCLUDED.partition_keys
                """
            ).format(
                query=sql.SQL(query),  # type: ignore[arg-type]
                partition_col=sql.Identifier(partition_key),
                bitsize=sql.Literal(bitsize),
                table_name=sql.Identifier(table_name),
                key=sql.Literal(key)
            )

            self.cursor.execute(lazy_insert_query)

            # Also store in queries table for existence checks
            try:
                self.cursor.execute(
                    sql.SQL("INSERT INTO {table} (query_hash, partition_key, query) VALUES (%s, %s, %s)").format(
                        table=sql.Identifier(self.tableprefix + "_queries")
                    ),
                    (key, partition_key, "")
                )
            except IntegrityError:
                # Update if insert fails
                self.cursor.execute(
                    sql.SQL("UPDATE {table} SET last_seen = now() WHERE query_hash = %s AND partition_key = %s").format(
                        table=sql.Identifier(self.tableprefix + "_queries")
                    ),
                    (key, partition_key)
                )

            self.db.commit()
            return True

        except Exception as e:
            logger.error(f"Failed to set cache lazily for key {key}: {e}")
            if not self.db.closed:
                self.db.rollback()
            return False

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """PostgreSQL bit handler supports only integer datatype."""
        return {"integer"}

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype != "integer":
            raise ValueError("PostgreSQL bit handler supports only integer datatype")
        if "bitsize" in kwargs:
            bitsize = kwargs["bitsize"]
        else:
            bitsize = self.default_bitsize
        # Use the new atomic _ensure_partition_table method (ignore return values)
        _, _ = self._ensure_partition_table(partition_key, datatype, bitsize=bitsize)
