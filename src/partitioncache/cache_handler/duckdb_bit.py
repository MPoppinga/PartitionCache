"""
DuckDB-based bit cache handler for PartitionCache.

This handler uses DuckDB as a persistent or in-memory database to store
cache entries as native DuckDB BITSTRING types. It supports only integer
partition keys and provides high-performance set operations through
DuckDB's native bitwise operations and aggregates.

Features:
- Native DuckDB BITSTRING data type storage
- BIT_AND aggregate for intersection operations
- Bitwise operators for set operations
- High-performance analytical queries
"""

import re
import threading
from datetime import datetime
from logging import getLogger

import duckdb

from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

logger = getLogger("PartitionCache")


def validate_identifier(identifier: str, identifier_type: str = "identifier") -> None:
    """
    Validate SQL identifier (table name, column name, etc.) to prevent SQL injection.

    Args:
        identifier: The identifier to validate
        identifier_type: Type of identifier for error messages

    Raises:
        ValueError: If identifier contains invalid characters
    """
    # Allow alphanumeric, underscore, and hyphen
    if not re.match(r"^[a-zA-Z0-9_-]+$", identifier):
        msg = f"Invalid {identifier_type}: '{identifier}'. Only alphanumeric characters, underscores, and hyphens are allowed."
        raise ValueError(msg)

    # Check length
    if len(identifier) > 128:
        msg = f"{identifier_type} too long: '{identifier}'. Maximum length is 128 characters."
        raise ValueError(msg)


class DuckDBBitCacheHandler(AbstractCacheHandler_Lazy):
    """
    DuckDB-based bit cache handler using native BITSTRING types.

    Features:
    - File-based or in-memory storage via DuckDB
    - Native DuckDB BITSTRING representation for integer partition keys
    - DuckDB BIT_AND aggregate for high-performance intersections
    - Native bitwise operators (&, |, ^, ~) for set operations
    - Lazy cache SQL generation for cross-database queries
    - Thread-safe operations with connection per instance

    Implementation:
    Uses DuckDB's native BITSTRING data type and bitwise functions for optimal
    performance in analytical workloads.
    """

    _instance = None
    _refcount = 0
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls, *args, **kwargs):
        """Get singleton instance with thread safety."""
        with cls._lock:
            # Skip singleton for multi-threaded environments
            if threading.active_count() > 1:
                return cls(*args, **kwargs)

            if cls._instance is None:
                cls._instance = cls(*args, **kwargs)
            cls._refcount += 1
            return cls._instance

    def __init__(self, database: str = ":memory:", table_prefix: str = "partitioncache", bitsize: int = 100000) -> None:
        """
        Initialize DuckDB bit cache handler.

        Args:
            database: Path to DuckDB database file or ":memory:" for in-memory
            table_prefix: Prefix for cache tables
            bitsize: Default bitsize for bit arrays
        """
        # Validate table prefix to prevent SQL injection
        validate_identifier(table_prefix, "table prefix")

        self.database = database
        self.table_prefix = table_prefix
        self.default_bitsize = bitsize
        self._cached_datatypes = {}

        # Create connection
        self.conn = duckdb.connect(database)

        # Enable memory optimization for large bit arrays
        self.conn.execute("SET memory_limit = '2GB'")
        self.conn.execute("SET threads = 4")

        # Initialize tables
        self._create_metadata_tables()

    def __repr__(self) -> str:
        """Return string representation."""
        return "duckdb_bit"

    def _get_safe_table_name(self, partition_key: str) -> str:
        """
        Get safe table name for partition after validation.

        Args:
            partition_key: Partition key name

        Returns:
            str: Safe table name

        Raises:
            ValueError: If partition key contains invalid characters
        """
        validate_identifier(partition_key, "partition key")
        return f"{self.table_prefix}_cache_{partition_key}"

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """DuckDB bit handler supports only integer datatype."""
        return {"integer"}

    def _create_metadata_tables(self) -> None:
        """Create metadata and queries tables."""
        try:
            # Create partition metadata table
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_prefix}_partition_metadata (
                    partition_key TEXT PRIMARY KEY,
                    datatype TEXT NOT NULL CHECK (datatype = 'integer'),
                    bitsize INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT now()
                )
            """)

            # Create queries table for metadata
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_prefix}_queries (
                    query_hash TEXT NOT NULL,
                    query TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ok' CHECK (status IN ('ok', 'timeout', 'failed')),
                    last_seen TIMESTAMP DEFAULT now(),
                    PRIMARY KEY (query_hash, partition_key)
                )
            """)

            logger.debug("Created metadata tables for DuckDB bit cache handler")

        except Exception as e:
            logger.error(f"Failed to create metadata tables: {e}")
            raise

    def _ensure_partition_table(self, partition_key: str, bitsize: int) -> bool:
        """
        Ensure partition-specific cache table exists.

        Args:
            partition_key: Name of the partition key
            bitsize: Size of bit array for this partition

        Returns:
            bool: True if successful
        """
        try:
            # Get safe table name (includes validation)
            table_name = self._get_safe_table_name(partition_key)

            # Create cache table with native DuckDB BITSTRING
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    query_hash TEXT PRIMARY KEY,
                    partition_keys BITSTRING,  -- Native DuckDB BITSTRING type
                    partition_keys_count INTEGER,
                    created_at TIMESTAMP DEFAULT now()
                )
            """)

            # Insert metadata - use INSERT OR REPLACE for DuckDB compatibility
            try:
                self.conn.execute(
                    f"""
                    INSERT INTO {self.table_prefix}_partition_metadata
                    (partition_key, datatype, bitsize)
                    VALUES (?, 'integer', ?)
                """,
                    (partition_key, bitsize),
                )
            except duckdb.ConstraintException:
                # Update existing entry if insert fails due to primary key constraint
                self.conn.execute(
                    f"""
                    UPDATE {self.table_prefix}_partition_metadata
                    SET bitsize = ?
                    WHERE partition_key = ?
                """,
                    (bitsize, partition_key),
                )

            return True

        except Exception as e:
            # Don't treat "table already exists" as an error
            if "already exists" in str(e).lower():
                return True
            logger.error(f"Failed to create partition table for {partition_key}: {e}")
            return False

    def _get_partition_bitsize(self, partition_key: str) -> int | None:
        """Get bitsize for a partition from metadata."""
        try:
            result = self.conn.execute(
                f"""
                SELECT bitsize FROM {self.table_prefix}_partition_metadata
                WHERE partition_key = ?
            """,
                (partition_key,),
            ).fetchone()

            return result[0] if result else None
        except Exception:
            return None

    def _get_partition_datatype(self, partition_key: str) -> str | None:
        """Get datatype for a partition from metadata."""
        if partition_key in self._cached_datatypes:
            return self._cached_datatypes[partition_key]

        try:
            result = self.conn.execute(f"""
                SELECT datatype FROM {self.table_prefix}_partition_metadata
                WHERE partition_key = ?
            """, (partition_key,)).fetchone()

            if result:
                self._cached_datatypes[partition_key] = result[0]
                return result[0]
            return None
        except Exception:
            return None

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """
        Register a partition key with the cache handler.

        Args:
            partition_key: Name of the partition key
            datatype: Must be 'integer' for bit handler
            **kwargs: bitsize parameter for bit array size
        """
        if datatype != "integer":
            raise ValueError("DuckDB bit handler supports only integer datatype")

        bitsize = kwargs.get("bitsize", self.default_bitsize)

        if not self._ensure_partition_table(partition_key, bitsize):
            raise RuntimeError(f"Failed to register partition key: {partition_key}")

    def _convert_to_integers(self, partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime]) -> list[int] | None:
        """
        Convert partition key identifiers to integers.

        Args:
            partition_key_identifiers: Set of values to convert

        Returns:
            List of integers or None if conversion fails
        """
        int_keys = []
        for k in partition_key_identifiers:
            if isinstance(k, int):
                int_keys.append(k)
            elif isinstance(k, str):
                try:
                    int_keys.append(int(k))
                except ValueError:
                    logger.error(f"Cannot convert to integer: {k}")
                    return None
            else:
                logger.error(f"Only integer values supported for bit arrays: {k}")
                return None
        return int_keys

    def _validate_and_prepare_bitsize(self, int_keys: list[int], partition_key: str) -> int | None:
        """
        Validate and prepare bitsize for the partition.

        Args:
            int_keys: List of integer keys
            partition_key: Partition key name

        Returns:
            Actual bitsize or None if validation fails
        """
        max_value = max(int_keys)
        existing_bitsize = self._get_partition_bitsize(partition_key)

        if existing_bitsize is None:
            # Create new partition with appropriate bitsize
            required_bitsize = max(max_value + 1, self.default_bitsize)
            if not self._ensure_partition_table(partition_key, required_bitsize):
                return None
            return required_bitsize
        else:
            # Validate values fit in existing bitsize
            if max_value >= existing_bitsize:
                logger.error(f"Value {max_value} exceeds bitsize {existing_bitsize} for partition {partition_key}")
                return None
            return existing_bitsize

    def _build_bitstring_expression(self, int_keys: list[int], bitsize: int) -> str:
        """
        Build DuckDB BITSTRING expression for the given integer keys.

        Args:
            int_keys: List of integer keys
            bitsize: Size of the bitstring

        Returns:
            SQL expression for creating the bitstring
        """
        bitstring_expr = "REPEAT('0', ?)::BITSTRING"

        # Set bits for each integer value using DuckDB's set_bit function
        for k in int_keys:
            bitstring_expr = f"set_bit({bitstring_expr}, {k}, 1)"

        return bitstring_expr

    def _store_cache_entry(self, table_name: str, key: str, bitstring_expr: str, bitsize: int, count: int) -> None:
        """
        Store or update cache entry in the table.

        Args:
            table_name: Name of the cache table
            key: Cache key
            bitstring_expr: SQL expression for bitstring
            bitsize: Size of the bitstring
            count: Number of partition keys
        """
        try:
            self.conn.execute(
                f"""
                INSERT INTO {table_name} (query_hash, partition_keys, partition_keys_count)
                VALUES (?, {bitstring_expr}, ?)
            """,
                (key, bitsize, count),
            )
        except duckdb.ConstraintException:
            # If insert fails due to primary key constraint, do update
            self.conn.execute(
                f"""
                UPDATE {table_name}
                SET partition_keys = {bitstring_expr}, partition_keys_count = ?
                WHERE query_hash = ?
            """,
                (bitsize, count, key),
            )

    def set_cache(self, key: str, partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime], partition_key: str = "partition_key") -> bool:
        """
        Store partition key identifiers as DuckDB BITSTRING.

        Args:
            key: Cache key (query hash)
            partition_key_identifiers: Set of integer values to store
            partition_key: Partition key name

        Returns:
            bool: True if successful

        Note: Creates DuckDB native BITSTRING with bits set for each integer value.
        """
        if not partition_key_identifiers:
            return True

        try:
            # Convert all values to integers
            int_keys = self._convert_to_integers(partition_key_identifiers)
            if int_keys is None:
                return False

            # Validate and prepare bitsize
            actual_bitsize = self._validate_and_prepare_bitsize(int_keys, partition_key)
            if actual_bitsize is None:
                return False

            # Build bitstring expression
            bitstring_expr = self._build_bitstring_expression(int_keys, actual_bitsize)

            # Store cache entry
            table_name = self._get_safe_table_name(partition_key)
            self._store_cache_entry(table_name, key, bitstring_expr, actual_bitsize, len(int_keys))

            # Also store in queries table for existence checks
            try:
                self.conn.execute(
                    f"""
                    INSERT INTO {self.table_prefix}_queries (query_hash, partition_key, query)
                    VALUES (?, ?, '')
                """,
                    (key, partition_key),
                )
            except duckdb.ConstraintException:
                # Update if insert fails due to primary key constraint
                self.conn.execute(
                    f"""
                    UPDATE {self.table_prefix}_queries
                    SET last_seen = now()
                    WHERE query_hash = ? AND partition_key = ?
                """,
                    (key, partition_key),
                )

            return True

        except Exception as e:
            logger.error(f"Failed to set cache for key {key}: {e}")
            return False

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | None:
        """
        Retrieve partition key identifiers from cache.

        Args:
            key: Cache key to look up
            partition_key: Partition key name

        Returns:
            Set of integer partition keys or None if not found
        """
        try:
            table_name = self._get_safe_table_name(partition_key)

            # Use DuckDB's native bitstring functions to extract set bits
            result = self.conn.execute(
                f"""
                WITH bit_positions AS (
                    SELECT
                        range(bit_length(partition_keys)) AS pos_array
                    FROM {table_name}
                    WHERE query_hash = ?
                ),
                positions AS (
                    SELECT UNNEST(pos_array) AS pos
                    FROM bit_positions
                )
                SELECT pos AS position
                FROM positions
                WHERE get_bit(
                    (SELECT partition_keys FROM {table_name} WHERE query_hash = ?),
                    pos::INTEGER
                ) = 1
            """, (key, key)).fetchall()

            if not result:
                return None

            return {row[0] for row in result}

        except Exception as e:
            logger.debug(f"Failed to get cache for key {key}: {e}")
            return None

    def get_intersected(self, keys: set[str], partition_key: str = "partition_key") -> tuple[set[int] | None, int]:
        """
        Get intersection of multiple cache entries using DuckDB's BIT_AND aggregate.

        Args:
            keys: Set of cache keys to intersect
            partition_key: Partition key name

        Returns:
            Tuple of (intersected set, number of keys that existed)

        Note: Uses DuckDB's native BIT_AND aggregate for high-performance intersection.
        """
        try:
            # Filter to existing keys
            existing_keys = self.filter_existing_keys(keys, partition_key)
            if not existing_keys:
                return None, 0

            if len(existing_keys) == 1:
                # Single key - just return its value
                key = next(iter(existing_keys))
                result = self.get(key, partition_key)
                return result, 1

            # Multiple keys - use DuckDB's BIT_AND aggregate for intersection
            table_name = self._get_safe_table_name(partition_key)
            key_placeholders = ",".join("?" * len(existing_keys))

            # Use BIT_AND aggregate to compute intersection of all bitstrings
            intersection_query = f"""
                WITH intersection_result AS (
                    SELECT bit_and(partition_keys) AS intersection_bits
                    FROM {table_name}
                    WHERE query_hash IN ({key_placeholders})
                ),
                bit_positions AS (
                    SELECT range(bit_length(intersection_bits)) AS pos_array
                    FROM intersection_result
                ),
                positions AS (
                    SELECT UNNEST(pos_array) AS pos
                    FROM bit_positions
                )
                SELECT pos AS position
                FROM positions, intersection_result
                WHERE get_bit(intersection_bits, pos::INTEGER) = 1
            """

            result = self.conn.execute(intersection_query, tuple(existing_keys)).fetchall()

            if not result:
                return set(), len(existing_keys)

            return {row[0] for row in result}, len(existing_keys)

        except Exception as e:
            logger.error(f"Failed to get intersection for keys {keys}: {e}")
            return None, 0

    def get_intersected_lazy(self, keys: set[str], partition_key: str = "partition_key") -> tuple[str | None, int]:
        """
        Generate SQL query for lazy intersection.

        Args:
            keys: Set of cache keys to intersect
            partition_key: Partition key name

        Returns:
            Tuple of (SQL query string, number of existing keys)
        """
        try:
            existing_keys = self.filter_existing_keys(keys, partition_key)
            if not existing_keys:
                return None, 0

            table_name = self._get_safe_table_name(partition_key)
            key_list = list(existing_keys)

            if len(key_list) == 1:
                # Single key - extract bit positions from BITSTRING
                sql_query = f"""
                (
                    WITH bit_positions AS (
                        SELECT range(bit_length(partition_keys)) AS pos_array
                        FROM {table_name}
                        WHERE query_hash = '{key_list[0]}'
                    ),
                    positions AS (
                        SELECT UNNEST(pos_array) AS pos
                        FROM bit_positions
                    )
                    SELECT pos AS {partition_key}
                    FROM positions
                    WHERE get_bit(
                        (SELECT partition_keys FROM {table_name} WHERE query_hash = '{key_list[0]}'),
                        pos::INTEGER
                    ) = 1
                )
                """
            else:
                # Multiple keys - use BIT_AND aggregate for intersection
                quoted_keys = "','".join(key_list)
                sql_query = f"""
                (
                    WITH intersection_result AS (
                        SELECT bit_and(partition_keys) AS intersection_bits
                        FROM {table_name}
                        WHERE query_hash IN ('{quoted_keys}')
                    ),
                    bit_positions AS (
                        SELECT range(bit_length(intersection_bits)) AS pos_array
                        FROM intersection_result
                    ),
                    positions AS (
                        SELECT UNNEST(pos_array) AS pos
                        FROM bit_positions
                    )
                    SELECT pos AS {partition_key}
                    FROM positions, intersection_result
                    WHERE get_bit(intersection_bits, pos::INTEGER) = 1
                )
                """

            return sql_query, len(existing_keys)

        except Exception as e:
            logger.error(f"Failed to generate lazy intersection SQL: {e}")
            return None, 0

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

            # Get or determine bitsize
            existing_bitsize = self._get_partition_bitsize(partition_key)
            if existing_bitsize is None:
                # Create new partition with default bitsize
                if not self._ensure_partition_table(partition_key, self.default_bitsize):
                    return False
                actual_bitsize = self.default_bitsize
            else:
                actual_bitsize = existing_bitsize

            table_name = self._get_safe_table_name(partition_key)

            # Create DuckDB BITSTRING using lazy query execution
            # Build a query that creates the bitstring from the input query results
            lazy_insert_query = f"""
            WITH query_result AS (
                {query}
            ),
            bit_positions AS (
                SELECT {partition_key}::INTEGER AS position
                FROM query_result
                WHERE {partition_key}::INTEGER >= 0 AND {partition_key}::INTEGER < {actual_bitsize}
            ),
            bit_array AS (
                SELECT generate_series(0, {actual_bitsize} - 1) AS bit_index
            ),
            bit_string AS (
                SELECT string_agg(
                    CASE WHEN bit_array.bit_index IN (SELECT position FROM bit_positions)
                         THEN '1'
                         ELSE '0'
                    END,
                    ''
                    ORDER BY bit_array.bit_index
                ) AS bit_value,
                COUNT(DISTINCT bit_positions.position) AS partition_count
                FROM bit_array
                LEFT JOIN bit_positions ON bit_array.bit_index = bit_positions.position
            )
            INSERT INTO {table_name} (query_hash, partition_keys, partition_keys_count)
            SELECT ?, bit_value::BITSTRING, partition_count
            FROM bit_string
            ON CONFLICT (query_hash) DO UPDATE SET
                partition_keys = EXCLUDED.partition_keys,
                partition_keys_count = EXCLUDED.partition_keys_count
            """

            self.conn.execute(lazy_insert_query, (key,))

            # Also store in queries table for existence checks
            try:
                self.conn.execute(
                    f"""
                    INSERT INTO {self.table_prefix}_queries (query_hash, partition_key, query)
                    VALUES (?, ?, '')
                """,
                    (key, partition_key),
                )
            except duckdb.ConstraintException:
                # Update if insert fails due to primary key constraint
                self.conn.execute(
                    f"""
                    UPDATE {self.table_prefix}_queries
                    SET last_seen = now()
                    WHERE query_hash = ? AND partition_key = ?
                """,
                    (key, partition_key),
                )

            return True

        except Exception as e:
            logger.error(f"Failed to set cache lazily for key {key}: {e}")
            return False

    def exists(self, key: str, partition_key: str = "partition_key", check_query: bool = False) -> bool:
        """
        Check if cache entry exists.

        Args:
            key: Cache key to check
            partition_key: Partition key name
            check_query: Whether to check query metadata first

        Returns:
            bool: True if exists
        """
        try:
            if check_query:
                # First check query status
                status = self.get_query_status(key, partition_key)
                if status is None:
                    return False
                if status in ("timeout", "failed"):
                    return True  # Don't check cache for failed queries

            # Check cache entry
            table_name = self._get_safe_table_name(partition_key)
            result = self.conn.execute(
                f"""
                SELECT 1 FROM {table_name} WHERE query_hash = ?
            """,
                (key,),
            ).fetchone()

            return result is not None

        except Exception:
            return False

    def delete(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Delete cache entry.

        Args:
            key: Cache key to delete
            partition_key: Partition key name

        Returns:
            bool: True if successful
        """
        try:
            table_name = self._get_safe_table_name(partition_key)

            # Delete from cache table
            self.conn.execute(
                f"""
                DELETE FROM {table_name} WHERE query_hash = ?
            """,
                (key,),
            )

            # Delete from queries table
            self.conn.execute(
                f"""
                DELETE FROM {self.table_prefix}_queries
                WHERE query_hash = ? AND partition_key = ?
            """,
                (key, partition_key),
            )

            return True

        except Exception as e:
            logger.error(f"Failed to delete cache entry {key}: {e}")
            return False

    def set_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Store null value for a cache key.

        Args:
            key: Cache key
            partition_key: Partition key name

        Returns:
            bool: True if successful
        """
        try:
            # Ensure partition table exists
            if not self._get_partition_bitsize(partition_key):
                self.register_partition_key(partition_key, "integer")

            table_name = self._get_safe_table_name(partition_key)
            self.conn.execute(
                f"""
                INSERT INTO {table_name} (query_hash, partition_keys, partition_keys_count)
                VALUES (?, NULL, NULL)
                ON CONFLICT (query_hash) DO UPDATE SET
                partition_keys = NULL, partition_keys_count = NULL
            """,
                (key,),
            )

            return True

        except Exception as e:
            logger.error(f"Failed to set null for key {key}: {e}")
            return False

    def is_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """
        Check if cache entry is null.

        Args:
            key: Cache key to check
            partition_key: Partition key name

        Returns:
            bool: True if null
        """
        try:
            table_name = self._get_safe_table_name(partition_key)
            result = self.conn.execute(
                f"""
                SELECT partition_keys FROM {table_name} WHERE query_hash = ?
            """,
                (key,),
            ).fetchone()

            return result is not None and result[0] is None

        except Exception:
            return False

    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key", check_query: bool = False) -> set:
        """
        Filter keys to only those that exist in cache.

        Args:
            keys: Set of keys to filter
            partition_key: Partition key name
            check_query: Whether to check query metadata

        Returns:
            Set of existing keys
        """
        if not keys:
            return set()

        try:
            if check_query:
                # Check queries table first
                result = self.conn.execute(
                    f"""
                    SELECT query_hash FROM {self.table_prefix}_queries
                    WHERE query_hash IN ({",".join("?" * len(keys))})
                    AND partition_key = ?
                    AND status IN ('ok', 'timeout', 'failed')
                """,
                    tuple(keys) + (partition_key,),
                ).fetchall()

                query_keys = {row[0] for row in result}

                # For 'ok' status, also check cache exists
                ok_keys = self.conn.execute(
                    f"""
                    SELECT query_hash FROM {self.table_prefix}_queries
                    WHERE query_hash IN ({",".join("?" * len(query_keys))})
                    AND partition_key = ? AND status = 'ok'
                """,
                    tuple(query_keys) + (partition_key,),
                ).fetchall()

                if ok_keys:
                    table_name = self._get_safe_table_name(partition_key)
                    cache_keys = self.conn.execute(
                        f"""
                        SELECT query_hash FROM {table_name}
                        WHERE query_hash IN ({",".join("?" * len(ok_keys))})
                    """,
                        tuple(row[0] for row in ok_keys),
                    ).fetchall()

                    existing_cache_keys = {row[0] for row in cache_keys}
                else:
                    existing_cache_keys = set()

                # Include timeout/failed keys + existing cache keys
                failed_keys = query_keys - {row[0] for row in ok_keys}
                return existing_cache_keys | failed_keys

            else:
                # Just check cache table
                table_name = self._get_safe_table_name(partition_key)
                result = self.conn.execute(
                    f"""
                    SELECT query_hash FROM {table_name}
                    WHERE query_hash IN ({",".join("?" * len(keys))})
                """,
                    tuple(keys),
                ).fetchall()

                return {row[0] for row in result}

        except Exception as e:
            logger.debug(f"Failed to filter existing keys: {e}")
            return set()

    def get_all_keys(self, partition_key: str) -> list:
        """
        Get all cache keys for a partition.

        Args:
            partition_key: Partition key name

        Returns:
            List of cache keys
        """
        try:
            table_name = self._get_safe_table_name(partition_key)
            result = self.conn.execute(f"""
                SELECT query_hash FROM {table_name} ORDER BY created_at DESC
            """).fetchall()

            return [row[0] for row in result]

        except Exception:
            return []

    def set_query(self, key: str, querytext: str, partition_key: str = "partition_key") -> bool:
        """
        Store query metadata.

        Args:
            key: Cache key
            querytext: SQL query text
            partition_key: Partition key name

        Returns:
            bool: True if successful
        """
        try:
            self.conn.execute(
                f"""
                INSERT INTO {self.table_prefix}_queries (query_hash, partition_key, query)
                VALUES (?, ?, ?)
                ON CONFLICT (query_hash, partition_key) DO UPDATE SET
                query = EXCLUDED.query, last_seen = now()
            """,
                (key, partition_key, querytext),
            )

            return True

        except Exception as e:
            logger.error(f"Failed to set query for key {key}: {e}")
            return False

    def get_query(self, key: str, partition_key: str = "partition_key") -> str | None:
        """
        Retrieve query text.

        Args:
            key: Cache key
            partition_key: Partition key name

        Returns:
            Query text or None if not found
        """
        try:
            result = self.conn.execute(
                f"""
                SELECT query FROM {self.table_prefix}_queries
                WHERE query_hash = ? AND partition_key = ?
            """,
                (key, partition_key),
            ).fetchone()

            return result[0] if result else None

        except Exception:
            return None

    def get_all_queries(self, partition_key: str) -> list[tuple[str, str]]:
        """
        Get all queries for a partition.

        Args:
            partition_key: Partition key name

        Returns:
            List of (query_hash, query_text) tuples
        """
        try:
            result = self.conn.execute(
                f"""
                SELECT query_hash, query FROM {self.table_prefix}_queries
                WHERE partition_key = ? ORDER BY last_seen DESC
            """,
                (partition_key,),
            ).fetchall()

            return result

        except Exception:
            return []

    def set_query_status(self, key: str, partition_key: str = "partition_key", status: str = "ok") -> bool:
        """
        Set query status.

        Args:
            key: Cache key
            partition_key: Partition key name
            status: Status ('ok', 'timeout', 'failed')

        Returns:
            bool: True if successful
        """
        try:
            self.conn.execute(
                f"""
                UPDATE {self.table_prefix}_queries
                SET status = ?, last_seen = now()
                WHERE query_hash = ? AND partition_key = ?
            """,
                (status, key, partition_key),
            )

            return True

        except Exception as e:
            logger.error(f"Failed to set query status for key {key}: {e}")
            return False

    def get_query_status(self, key: str, partition_key: str = "partition_key") -> str | None:
        """
        Get query status.

        Args:
            key: Cache key
            partition_key: Partition key name

        Returns:
            Status string or None if not found
        """
        try:
            result = self.conn.execute(
                f"""
                SELECT status FROM {self.table_prefix}_queries
                WHERE query_hash = ? AND partition_key = ?
            """,
                (key, partition_key),
            ).fetchone()

            return result[0] if result else None

        except Exception:
            return None

    def get_datatype(self, partition_key: str) -> str | None:
        """
        Get datatype for a partition.

        Args:
            partition_key: Partition key name

        Returns:
            Datatype string or None if not found
        """
        return self._get_partition_datatype(partition_key)

    def get_partition_keys(self) -> list[tuple[str, str]]:
        """
        Get all registered partition keys and their datatypes.

        Returns:
            List of (partition_key, datatype) tuples
        """
        try:
            result = self.conn.execute(f"""
                SELECT partition_key, datatype FROM {self.table_prefix}_partition_metadata
                ORDER BY created_at
            """).fetchall()

            return result

        except Exception:
            return []

    def close(self) -> None:
        """Close database connection."""
        try:
            with self._lock:
                cls = type(self)
                is_singleton_instance = cls._instance is self and cls._refcount > 0

                if is_singleton_instance:
                    cls._refcount -= 1
                    if cls._refcount > 0:
                        return

                if self.conn:
                    self.conn.close()

                if is_singleton_instance:
                    cls._instance = None
                    cls._refcount = 0
        except Exception as e:
            logger.error(f"Error closing DuckDB connection: {e}")
