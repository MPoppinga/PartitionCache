import time
from datetime import datetime
from logging import getLogger

from psycopg import sql

from partitioncache.cache_handler.postgresql_abstract import PostgreSQLAbstractCacheHandler

logger = getLogger("PartitionCache")


class PostgreSQLArrayCacheHandler(PostgreSQLAbstractCacheHandler):
    USE_AGGREGATES = True

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """PostgreSQL array handler supports all datatypes."""
        return {"integer", "float", "text", "timestamp"}

    def __repr__(self) -> str:
        return "postgresql_array"

    def __init__(self, db_name, db_host, db_user, db_password, db_port, db_tableprefix) -> None:
        super().__init__(db_name, db_host, db_user, db_password, db_port, db_tableprefix)

        # Load SQL functions first
        self._load_sql_functions()

        # Setup array-specific extensions and aggregates
        try:
            self.cursor.execute("SELECT partitioncache_setup_array_extensions()")
            result = self.cursor.fetchone()
            self.array_intersect_agg_available = result[0] if result else False
            self.db.commit()
        except Exception as e:
            logger.warning(f"Failed to setup array extensions. Performance might be suboptimal. Error: {e}")
            self.array_intersect_agg_available = False
            if self.db.closed is False:
                self.db.rollback()

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
            self.cursor.execute(sql_cache_content)  # type: ignore[arg-type]
            logger.debug("Successfully loaded SQL functions from cache handlers file")
        except Exception as e:
            logger.debug(f"Failed to load SQL functions (this is OK if they're already loaded): {e}")

    def _create_partition_table(self, partition_key: str, datatype: str) -> None:
        """Create a cache table for a specific partition key using SQL bootstrap function."""
        try:
            # Use SQL bootstrap function to create partition table and metadata entry
            self.cursor.execute(
                sql.SQL("SELECT partitioncache_bootstrap_partition(%s, %s, %s, %s)"),
                (self.tableprefix, partition_key, datatype, "array")
            )
            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to create partition table for {partition_key}: {e}")
            # Rollback the transaction to prevent "current transaction is aborted" error
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction during table creation: {rollback_error}")
            raise

    def _ensure_partition_table(self, partition_key: str, datatype: str, **kwargs) -> bool:
        """Ensure a partition table exists with the correct datatype using SQL bootstrap function."""
        try:
            # Load SQL functions first to ensure they're available
            self._load_sql_functions()

            # Use SQL bootstrap function to ensure metadata tables exist
            self.cursor.execute(
                sql.SQL("SELECT partitioncache_ensure_metadata_tables(%s)"),
                (self.tableprefix,)
            )

            # Check if partition already exists and validate datatype
            existing_datatype = self._get_partition_datatype(partition_key)
            if existing_datatype is not None and existing_datatype != datatype:
                raise ValueError(f"Partition key '{partition_key}' already exists with datatype '{existing_datatype}', cannot use datatype '{datatype}'")

            # Use SQL bootstrap function to create partition table and metadata entry
            self.cursor.execute(
                sql.SQL("SELECT partitioncache_bootstrap_partition(%s, %s, %s, %s)"),
                (self.tableprefix, partition_key, datatype, "array")
            )

            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to ensure partition table for {partition_key}: {e}")
            # Rollback and retry with manual table creation as fallback
            try:
                self.db.rollback()
                logger.warning(f"Attempting manual table creation after bootstrap failure: {e}")
                self._create_partition_table(partition_key, datatype)
            except Exception as recreate_error:
                logger.error(f"Failed to create partition table for {partition_key}: {recreate_error}")
                self.db.rollback()
                raise
            return True

    def set_cache(self, key: str, partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """Store partition key identifiers in the cache for a specific partition key (column)."""
        if not partition_key_identifiers:
            return True

        try:
            # Try to get datatype from metadata
            datatype = self._get_partition_datatype(partition_key)

            # Determine the datatype of the incoming partition key identifiers
            sample = next(iter(partition_key_identifiers))
            if isinstance(sample, int):
                identifier_datatype = "integer"
            elif isinstance(sample, float):
                identifier_datatype = "float"
            elif isinstance(sample, str):
                identifier_datatype = "text"
            elif isinstance(sample, datetime):
                identifier_datatype = "timestamp"
            else:
                logger.error(f"Unsupported partition key identifier type: {type(sample)}")
                return False

            if datatype is None:
                # Partition key is not set in metadata, so we use the inferred datatype
                datatype = identifier_datatype
                # Create the partition table with the inferred datatype
                if not self._ensure_partition_table(partition_key, datatype):
                    return False
            else:
                # Validate that the identifier datatype matches the existing partition datatype
                if datatype != identifier_datatype:
                    logger.error(f"Identifier datatype '{identifier_datatype}' does not match partition datatype '{datatype}' for partition '{partition_key}'")
                    return False

            # Convert set to list for PostgreSQL array serialization
            val = list(partition_key_identifiers)

            # Get partition-specific table
            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {0} (query_hash, partition_keys) VALUES (%s, %s) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys"
                ).format(sql.Identifier(table_name)),
                (key, val),
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
            logger.error(f"Failed to set cache for hash {hash} in partition {partition_key}: {e}")
            # Rollback the transaction to prevent "current transaction is aborted" error
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
            return False

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | None:
        """Get value from partition-specific cache table."""

        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        table_name = f"{self.tableprefix}_cache_{partition_key}"
        self.cursor.execute(sql.SQL("SELECT partition_keys FROM {0} WHERE query_hash = %s").format(sql.Identifier(table_name)), (key,))
        result = self.cursor.fetchone()
        if result is None or result[0] is None:
            return None

        return set(result[0])

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]:
        """Get intersection from partition-specific table."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return None, 0

            # Check which keys exist
            filtered_keys = self.filter_existing_keys(keys, partition_key)
            if not filtered_keys:
                return None, 0

            t = time.time()
            query: sql.Composed = self.get_intersected_sql(filtered_keys, partition_key, datatype)
            logger.debug(query.as_string(self.cursor))
            self.cursor.execute(query)

            result = self.cursor.fetchone()
            if result is None or result[0] is None:
                return None, 0

            logger.info(f"Getting Cache Query took {time.time() - t} seconds, got {len(result[0])} partition_keys using {len(filtered_keys)} hashkeys")
            return set(result[0]), len(filtered_keys)
        except Exception as e:
            logger.error(f"Failed to get intersection in partition {partition_key}: {e}")
            return None, 0

    def get_intersected_lazy(self, keys: set[str], partition_key: str = "partition_key") -> tuple[str | None, int]:
        """Get lazy intersection for partition-specific table."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return None, 0

            filtered_keys = self.filter_existing_keys(keys, partition_key)
            if not filtered_keys:
                return None, 0

            query = sql.SQL("SELECT unnest(({intersectsql})) as {partition_col}").format(
                intersectsql=self.get_intersected_sql(filtered_keys, partition_key, datatype), partition_col=sql.Identifier(partition_key)
            )
            return query.as_string(), len(filtered_keys)
        except Exception as e:
            logger.error(f"Failed to get lazy intersection in partition {partition_key}: {e}")
            return None, 0

    def get_intersected_sql(self, keys: set[str], partition_key: str, datatype: str) -> sql.Composed:
        """Return the query for intersection of all sets in the partition-specific cache."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        keys_list = list(keys)

        if self.USE_AGGREGATES and self.array_intersect_agg_available:
            return sql.SQL("SELECT array_intersect_agg(partition_keys) FROM {0} WHERE query_hash = ANY({1})").format(
                sql.Identifier(table_name), sql.Literal(keys_list)
            )

        # Fallback to original implementation
        if datatype == "integer":
            # Use intarray intersection for integers
            select_parts = [sql.SQL("({})").format(sql.SQL(" & ").join(sql.Identifier(f"k{i}", "partition_keys") for i in range(len(keys_list))))]
        else:
            # Use standard array intersection for other types
            intersect_expr = sql.Identifier("k0", "partition_keys")
            for i in range(1, len(keys_list)):
                intersect_expr = sql.SQL("array(select unnest({}) intersect select unnest({}))").format(
                    intersect_expr, sql.Identifier(f"k{i}", "partition_keys")
                )
            select_parts = [intersect_expr]

        # Create the FROM part of the query
        from_parts = [
            sql.SQL("(SELECT partition_keys FROM {0} WHERE query_hash = {1}) AS {2}").format(
                sql.Identifier(table_name), sql.Literal(key), sql.Identifier(f"k{i}")
            )
            for i, key in enumerate(keys_list)
        ]

        # Combine all parts into the final query
        query = sql.SQL("SELECT {select} FROM {from_}").format(select=sql.SQL(", ").join(select_parts), from_=sql.SQL(", ").join(from_parts))

        return query
