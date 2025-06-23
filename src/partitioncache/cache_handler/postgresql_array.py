import time
from logging import getLogger
from datetime import datetime
from typing import Set


from psycopg import sql

from partitioncache.cache_handler.postgresql_abstract import PostgreSQLAbstractCacheHandler


logger = getLogger("PartitionCache")


class PostgreSQLArrayCacheHandler(PostgreSQLAbstractCacheHandler):
    USE_AGGREGATES = True

    @classmethod
    def get_supported_datatypes(cls) -> Set[str]:
        """PostgreSQL array handler supports all datatypes."""
        return {"integer", "float", "text", "timestamp"}

    def __repr__(self) -> str:
        return "postgresql_array"

    def __init__(self, db_name, db_host, db_user, db_password, db_port, db_tableprefix) -> None:
        super().__init__(db_name, db_host, db_user, db_password, db_port, db_tableprefix)

        # Ensure intarray extension is present for integer-specific operators and indexes.
        try:
            self.cursor.execute("CREATE EXTENSION IF NOT EXISTS intarray;")
            self.db.commit()
        except Exception as e:
            logger.warning(f"Failed to create intarray extension (this is optional but recommended for integer intersections): {e}")

        self.array_intersect_agg_available = False

        # Create custom aggregate for array intersection if it doesn't exist.
        try:
            self.cursor.execute(
                """
                CREATE OR REPLACE FUNCTION _array_intersect(anyarray, anyarray)
                RETURNS anyarray AS $$
                    SELECT CASE 
                        WHEN $1 IS NULL THEN $2
                        WHEN $2 IS NULL THEN $1
                        ELSE ARRAY(SELECT unnest($1) INTERSECT SELECT unnest($2) ORDER BY 1)
                    END;
                $$ LANGUAGE sql IMMUTABLE;
            """
            )
            # Drop aggregate if it exists to recreate it, as CREATE AGGREGATE IF NOT EXISTS is not available.
            self.cursor.execute("DROP AGGREGATE IF EXISTS array_intersect_agg(anyarray);")
            self.cursor.execute(
                """
                CREATE AGGREGATE array_intersect_agg(anyarray) (
                    SFUNC = _array_intersect,
                    STYPE = anyarray
                );
            """
            )
            self.db.commit()
            self.array_intersect_agg_available = True
        except Exception as e:
            logger.warning(f"Failed to create custom array_intersect_agg. Performance for non-integer arrays might be suboptimal. Error: {e}")
            if self.db.closed is False:
                self.db.rollback()

        try:
            self.cursor.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                    partition_key TEXT PRIMARY KEY,
                    datatype TEXT NOT NULL CHECK (datatype IN ('integer', 'float', 'text', 'timestamp')),
                    created_at TIMESTAMP DEFAULT now()
                );""").format(sql.Identifier(self.tableprefix + "_partition_metadata"))
            )

            # Shared queries table
            self.cursor.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                    query_hash TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    query TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ok' CHECK (status IN ('ok', 'timeout', 'failed')),
                    last_seen TIMESTAMP NOT NULL DEFAULT now(),
                    PRIMARY KEY (query_hash, partition_key)
                );""").format(sql.Identifier(self.tableprefix + "_queries"))
            )

            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to create metadata tables: {e}")
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback after metadata table creation error: {rollback_error}")
            raise

    def _create_partition_table(self, partition_key: str, datatype: str) -> None:
        """Create a cache table for a specific partition key with appropriate datatype."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"

        # Determine the appropriate SQL datatype for the array column
        if datatype == "integer":
            sql_datatype = "INTEGER[]"
        elif datatype == "float":
            sql_datatype = "NUMERIC[]"
        elif datatype == "text":
            sql_datatype = "TEXT[]"
        elif datatype == "timestamp":
            sql_datatype = "TIMESTAMP[]"
        else:
            raise ValueError(f"Unsupported datatype: {datatype}")

        try:
            # Create the cache table
            self.cursor.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                    query_hash TEXT PRIMARY KEY,
                    partition_keys {1},
                    partition_keys_count integer NOT NULL GENERATED ALWAYS AS (cardinality(partition_keys)) STORED
                );""").format(sql.Identifier(table_name), sql.SQL(sql_datatype))
            )

            # Create appropriate index based on datatype
            if datatype == "integer":
                # Use intarray-specific GIN index for integers - with error handling
                try:
                    self.cursor.execute(
                        sql.SQL("CREATE INDEX IF NOT EXISTS {0} ON {1} USING GIN (partition_keys gin__int_ops);").format(
                            sql.Identifier(f"idx_{table_name}_partition_keys"), sql.Identifier(table_name)
                        )
                    )
                except Exception as index_error:
                    logger.warning(f"Failed to create intarray GIN index, falling back to standard GIN index: {index_error}")
                    # Fallback to standard GIN index if intarray fails
                    self.cursor.execute(
                        sql.SQL("CREATE INDEX IF NOT EXISTS {0} ON {1} USING GIN (partition_keys);").format(
                            sql.Identifier(f"idx_{table_name}_partition_keys"), sql.Identifier(table_name)
                        )
                    )
            else:
                # Use standard GIN index for other types
                self.cursor.execute(
                    sql.SQL("CREATE INDEX IF NOT EXISTS {0} ON {1} USING GIN (partition_keys);").format(
                        sql.Identifier(f"idx_{table_name}_partition_keys"), sql.Identifier(table_name)
                    )
                )

            # Insert metadata
            self.cursor.execute(
                sql.SQL("INSERT INTO {0} (partition_key, datatype) VALUES (%s, %s) ON CONFLICT (partition_key) DO NOTHING").format(
                    sql.Identifier(self.tableprefix + "_partition_metadata")
                ),
                (partition_key, datatype),
            )

            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to create partition table {table_name}: {e}")
            # Rollback the transaction to prevent "current transaction is aborted" error
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction during table creation: {rollback_error}")
            raise

    def _ensure_partition_table(self, partition_key: str, datatype: str) -> bool:
        """Ensure a partition table exists with the correct datatype."""
        existing_datatype = self._get_partition_datatype(partition_key)

        if existing_datatype is None:
            # Create new table
            self._create_partition_table(partition_key, datatype)
            return True
        elif existing_datatype != datatype:
            raise ValueError(f"Partition key '{partition_key}' already exists with datatype '{existing_datatype}', cannot use datatype '{datatype}'")
        return True

    def set_set(self, key: str, value: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """Set the value of the given key in the cache for a specific partition key."""
        if not value:
            return True

        try:
            # Try to get datatype from metadata
            datatype = self._get_partition_datatype(partition_key)

            # Determine the datatype of the incoming value
            sample = next(iter(value))
            if isinstance(sample, int):
                value_datatype = "integer"
            elif isinstance(sample, float):
                value_datatype = "float"
            elif isinstance(sample, str):
                value_datatype = "text"
            elif isinstance(sample, datetime):
                value_datatype = "timestamp"
            else:
                logger.error(f"Unsupported value type: {type(sample)}")
                return False

            if datatype is None:
                # Partition key is not set in metadata, so we use the inferred datatype
                datatype = value_datatype
                # Create the partition table with the inferred datatype
                if not self._ensure_partition_table(partition_key, datatype):
                    return False
            else:
                # Validate that the value datatype matches the existing partition datatype
                if datatype != value_datatype:
                    logger.error(f"Value datatype '{value_datatype}' does not match partition datatype '{datatype}' for partition '{partition_key}'")
                    return False

            # Convert set to list for PostgreSQL array serialization
            val = list(value)

            # Get partition-specific table
            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {0} (query_hash, partition_keys) VALUES (%s, %s) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys"
                ).format(sql.Identifier(table_name)),
                (key, val),
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

    def get_datatype(self, partition_key: str) -> str | None:
        """Get the datatype of the cache handler. If the partition key is not set, return None."""
        return self._get_partition_datatype(partition_key)

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype not in self.get_supported_datatypes():
            raise ValueError(f"PostgreSQL array handler supports only {self.get_supported_datatypes()} datatypes")
        self._ensure_partition_table(partition_key, datatype)
