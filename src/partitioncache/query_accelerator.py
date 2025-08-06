"""
DuckDB In-Memory Query Accelerator for PartitionCache.

This module provides a query acceleration layer using DuckDB's in-memory engine
with PostgreSQL extension support. It allows for faster query execution while
maintaining compatibility with existing cache backends.

Features:
- In-memory DuckDB instance with PostgreSQL extension
- Table preloading from PostgreSQL for faster queries
- Transparent query acceleration with fallback support
- Compatible with all existing cache handlers
- Configurable table preloading and connection management
"""

import concurrent.futures
import threading
import time
from typing import Any

import duckdb
import psycopg

from partitioncache.logging_utils import get_thread_aware_logger

logger = get_thread_aware_logger("PartitionCache")


class DuckDBQueryAccelerator:
    """
    DuckDB-based query accelerator for PostgreSQL database queries.

    This accelerator creates an in-memory DuckDB instance with PostgreSQL extension
    support, allowing for faster analytical queries while maintaining full compatibility
    with existing PostgreSQL-based workflows.

    Features:
    - In-memory DuckDB with PostgreSQL extension
    - Configurable table preloading from PostgreSQL
    - Automatic connection management and retry logic
    - Query performance monitoring and statistics
    - Graceful fallback to original PostgreSQL queries
    """

    def __init__(
        self,
        postgresql_connection_params: dict[str, Any],
        preload_tables: list[str] | None = None,
        duckdb_memory_limit: str = "2GB",
        duckdb_threads: int = 4,
        enable_statistics: bool = True,
        duckdb_database_path: str = "/tmp/partitioncache_accel.duckdb",
        force_reload_tables: bool = False,
        query_timeout: float = 0
    ):
        """
        Initialize DuckDB query accelerator.

        Args:
            postgresql_connection_params: PostgreSQL connection parameters
            preload_tables: List of table names to preload into DuckDB
            duckdb_memory_limit: Memory limit for DuckDB instance
            duckdb_threads: Number of threads for DuckDB
            enable_statistics: Whether to collect performance statistics
            duckdb_database_path: Path to DuckDB database file (':memory:' for in-memory)
            force_reload_tables: Force reload tables from PostgreSQL even if they exist in DuckDB
            query_timeout: Query timeout in seconds (0 = no timeout)
        """
        self.postgresql_params = postgresql_connection_params
        self.tables_to_preload = preload_tables or []
        self.duckdb_memory_limit = duckdb_memory_limit
        self.duckdb_threads = duckdb_threads
        self.enable_statistics = enable_statistics
        self.duckdb_database_path = duckdb_database_path
        self.force_reload_tables = force_reload_tables
        self.query_timeout = query_timeout

        # Performance statistics
        self.stats = {
            "queries_accelerated": 0,
            "queries_fallback": 0,
            "queries_timeout": 0,
            "total_acceleration_time": 0.0,
            "total_fallback_time": 0.0,
            "tables_preloaded": 0,
            "preload_time": 0.0,
            "connection_errors": 0
        }

        # DuckDB thread safety: Each thread uses its own cursor from the connection
        # No global lock needed when using thread-local cursors properly
        # Small lock only for statistics updates (minimal contention)
        self._stats_lock = threading.Lock()

        # Connection objects
        self.duckdb_conn: duckdb.DuckDBPyConnection | None = None
        self.postgresql_conn: psycopg.Connection | None = None
        self._initialized = False
        self._preload_completed = False
        self._last_query_time = 0.0

    def __repr__(self) -> str:
        """Return string representation."""
        return f"DuckDBQueryAccelerator(preloaded_tables={len(self.tables_to_preload)}, initialized={self._initialized})"

    def _update_stats(self, **updates: Any) -> None:
        """Thread-safe statistics update."""
        with self._stats_lock:
            for key, value in updates.items():
                if key in self.stats:
                    if isinstance(value, int | float) and isinstance(self.stats[key], int | float):
                        self.stats[key] += value
                    else:
                        self.stats[key] = value

    def initialize(self) -> bool:
        """
        Initialize DuckDB connection and PostgreSQL extension.

        Returns:
            bool: True if initialization successful, False otherwise
        """
        try:
            logger.info(f"Initializing DuckDB query accelerator with database: {self.duckdb_database_path}")

            # Create DuckDB connection (file-based or in-memory)
            self.duckdb_conn = duckdb.connect(self.duckdb_database_path)

            # Configure DuckDB settings
            self.duckdb_conn.execute(f"SET memory_limit = '{self.duckdb_memory_limit}'")
            self.duckdb_conn.execute(f"SET threads = {self.duckdb_threads}")

            # Install and load PostgreSQL extension
            logger.debug("Installing PostgreSQL extension for DuckDB...")
            self.duckdb_conn.execute("INSTALL postgres")
            self.duckdb_conn.execute("LOAD postgres")

            # Create PostgreSQL connection for metadata and preloading
            self._connect_postgresql()

            self._initialized = True
            logger.info("DuckDB query accelerator initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize DuckDB query accelerator: {e}")
            self._update_stats(connection_errors=1)
            self._cleanup_connections()
            return False

    def _connect_postgresql(self) -> None:
        """Establish PostgreSQL connection for metadata operations."""
        try:
            # Build connection string from parameters
            conn_params = self.postgresql_params.copy()

            # Handle timeout parameter separately (not a connection parameter)
            timeout = conn_params.pop("timeout", None)

            # Create connection
            self.postgresql_conn = psycopg.connect(**conn_params)

            # Set timeout if specified
            if timeout and timeout != "0":
                timeout_ms = int(float(timeout) * 1000)
                with self.postgresql_conn.cursor() as cursor:
                    cursor.execute(f"SET statement_timeout = {timeout_ms}")

            logger.debug("PostgreSQL connection established for accelerator")

        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL for accelerator: {e}")
            self._update_stats(connection_errors=1)
            raise

    def preload_tables(self) -> bool:
        """
        Preload specified tables from PostgreSQL into DuckDB.

        Returns:
            bool: True if preloading successful, False otherwise
        """
        if not self._initialized or not self.tables_to_preload:
            logger.debug("No tables to preload or accelerator not initialized")
            return True

        if self._preload_completed:
            logger.debug("Tables already preloaded")
            return True

        # Check if using persistent database and tables already exist (unless forced to reload)
        if (self.duckdb_database_path != ":memory:" and
            not self.force_reload_tables and
            self._tables_exist_in_duckdb()):
            logger.info("Tables already exist in DuckDB, skipping PostgreSQL reload")
            self._preload_completed = True
            return True

        logger.info(f"Preloading {len(self.tables_to_preload)} tables into DuckDB...")
        preload_start = time.perf_counter()

        try:
            # Install and load PostgreSQL extension if not already loaded
            self.duckdb_conn.execute("INSTALL postgres")
            self.duckdb_conn.execute("LOAD postgres")

            # Build connection string for DuckDB PostgreSQL extension
            pg_conn_str = self._build_duckdb_postgres_connection_string()

            # Check if postgres_db is already attached (for persistent databases)
            try:
                # Try to query if the database exists
                result = self.duckdb_conn.execute("SELECT 1 FROM duckdb_databases() WHERE database_name = 'postgres_db'").fetchone()
                if result is None:
                    # Not attached, attach it now
                    self.duckdb_conn.execute(f"ATTACH '{pg_conn_str}' AS postgres_db (TYPE POSTGRES)")
                else:
                    logger.debug("PostgreSQL database already attached as postgres_db")
            except Exception:
                # If the check fails, try to attach (for older DuckDB versions)
                try:
                    self.duckdb_conn.execute(f"ATTACH '{pg_conn_str}' AS postgres_db (TYPE POSTGRES)")
                except Exception as attach_error:
                    # If attach fails with "already exists", that's OK
                    if "already exists" in str(attach_error):
                        logger.debug("PostgreSQL database already attached as postgres_db")
                    else:
                        raise

            tables_loaded = 0

            for table_name in self.tables_to_preload:
                try:
                    logger.debug(f"Preloading table: {table_name}")

                    # Validate table exists in PostgreSQL
                    if not self._table_exists_in_postgresql(table_name):
                        logger.warning(f"Table {table_name} does not exist in PostgreSQL, skipping")
                        continue

                    # Drop existing table if force reload is enabled or in memory mode
                    if self.force_reload_tables or self.duckdb_database_path == ":memory:":
                        try:
                            self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                            logger.debug(f"Dropped existing table {table_name} for reload")
                        except Exception:
                            pass  # Table might not exist

                    # Create table in DuckDB by copying from PostgreSQL
                    # Use CREATE OR REPLACE for better handling
                    try:
                        self.duckdb_conn.execute(f"""
                            CREATE OR REPLACE TABLE {table_name} AS
                            SELECT * FROM postgres_db.{table_name}
                        """)
                    except Exception as create_error:
                        # If CREATE OR REPLACE is not supported, try regular CREATE
                        if "CREATE OR REPLACE" in str(create_error):
                            self.duckdb_conn.execute(f"""
                                CREATE TABLE {table_name} AS
                                SELECT * FROM postgres_db.{table_name}
                            """)
                        else:
                            raise

                    # Get row count for logging
                    result = self.duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    row_count = result[0] if result else 0

                    logger.debug(f"Preloaded table {table_name} with {row_count:,} rows")
                    tables_loaded += 1

                except Exception as table_error:
                    logger.warning(f"Failed to preload table {table_name}: {table_error}")
                    continue

            preload_duration = time.perf_counter() - preload_start
            with self._stats_lock:
                self.stats["tables_preloaded"] = tables_loaded
                self.stats["preload_time"] = preload_duration
            self._preload_completed = True

            logger.info(f"Successfully preloaded {tables_loaded}/{len(self.tables_to_preload)} tables in {preload_duration:.2f}s")
            return True

        except Exception as e:
            logger.error(f"Failed to preload tables: {e}")
            self._update_stats(connection_errors=1)
            return False

    def _build_duckdb_postgres_connection_string(self) -> str:
        """Build PostgreSQL connection string for DuckDB postgres extension."""
        params = self.postgresql_params

        # Build connection string components
        host = params.get("host", "localhost")
        port = params.get("port", 5432)
        dbname = params.get("dbname") or params.get("database")
        user = params.get("user")
        password = params.get("password")

        conn_str = f"host={host} port={port}"
        if dbname:
            conn_str += f" dbname={dbname}"
        if user:
            conn_str += f" user={user}"
        if password:
            conn_str += f" password={password}"

        return conn_str

    def _table_exists_in_postgresql(self, table_name: str) -> bool:
        """Check if table, view, or materialized view exists in PostgreSQL."""
        try:
            with self.postgresql_conn.cursor() as cursor:
                # Check for tables and views in information_schema.tables
                cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_name = %s", (table_name,))
                if cursor.fetchone() is not None:
                    return True

                # Check for materialized views in pg_matviews
                cursor.execute("SELECT 1 FROM pg_matviews WHERE matviewname = %s", (table_name,))
                return cursor.fetchone() is not None
        except Exception as e:
            logger.debug(f"Failed to check table/view existence for {table_name}: {e}")
            return False

    def _tables_exist_in_duckdb(self) -> bool:
        """Check if all preload tables exist in DuckDB."""
        try:
            for table_name in self.tables_to_preload:
                # Query DuckDB's information schema to check if table exists
                result = self.duckdb_conn.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                    (table_name,)
                ).fetchone()
                if result is None:
                    logger.debug(f"Table {table_name} not found in DuckDB")
                    return False
            logger.debug(f"All {len(self.tables_to_preload)} tables exist in DuckDB")
            return True
        except Exception as e:
            logger.debug(f"Failed to check table existence in DuckDB: {e}")
            return False

    def execute_query(self, query: str) -> set[Any]:
        """
        Execute query using DuckDB acceleration with PostgreSQL fallback.

        Args:
            query: SQL query to execute

        Returns:
            Set of query results
        """
        if not self._initialized:
            logger.debug("Accelerator not initialized, using fallback")
            self._update_stats(queries_fallback=1)
            return self._execute_fallback(query)

        try:
            # Try DuckDB acceleration first with timeout support
            start_time = time.perf_counter()

            logger.debug(f"Executing query with DuckDB acceleration:(timeout = {self.query_timeout}s) {query[:100]}...")

            if self.query_timeout > 0:
                # Use thread-in-thread approach for timeout
                result_set = self._execute_with_timeout(query)
            else:
                # Execute query with thread-local cursor
                cursor = self.duckdb_conn.cursor()
                result = cursor.execute(query).fetchall()
                result_set = {row[0] if len(row) == 1 else row for row in result}
                cursor.close()

            duration = time.perf_counter() - start_time
            self._update_stats(queries_accelerated=1, total_acceleration_time=duration)
            self._last_query_time = duration

            logger.debug(f"DuckDB query completed in {duration:.3f}s, returned {len(result_set)} results")
            return result_set

        except concurrent.futures.TimeoutError:
            logger.warning(f"DuckDB query timed out after {self.query_timeout}s, falling back to PostgreSQL")
            self._update_stats(queries_timeout=1, queries_fallback=1)
            return self._execute_fallback(query)
        except Exception as e:
            logger.warning(f"DuckDB query failed, falling back to PostgreSQL: {e}")
            # Increment fallback counter since we're attempting fallback
            self._update_stats(queries_fallback=1)
            return self._execute_fallback(query)

    def _execute_with_timeout(self, query: str) -> set[Any]:
        """
        Execute DuckDB query with timeout using simple threading approach.

        Args:
            query: SQL query to execute

        Returns:
            Set of query results

        Raises:
            concurrent.futures.TimeoutError: If query times out
        """
        result_container = {"result": None, "exception": None, "completed": False}

        def _duckdb_query_worker():
            """Worker function that executes the DuckDB query."""
            cursor = None
            try:
                # Create thread-local cursor for thread safety
                cursor = self.duckdb_conn.cursor()
                result = cursor.execute(query).fetchall()
                result_container["result"] = {row[0] if len(row) == 1 else row for row in result}
                result_container["completed"] = True
                logger.debug(f"DuckDB query worker completed successfully with {len(result)} rows")
            except Exception as e:
                logger.debug(f"DuckDB query worker failed: {e}")
                result_container["exception"] = e
                result_container["completed"] = True
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass

        # Start the worker thread as daemon to avoid blocking main thread
        worker_thread = threading.Thread(target=_duckdb_query_worker, daemon=True)
        worker_thread.start()
        logger.debug(f"Started DuckDB query worker thread with {self.query_timeout}s timeout")

        # Wait for completion with timeout
        worker_thread.join(timeout=self.query_timeout)

        if worker_thread.is_alive():
            # Query timed out - attempt to interrupt connection
            logger.warning(f"DuckDB query timed out after {self.query_timeout}s, attempting to interrupt")
            try:
                if self.duckdb_conn:
                    self.duckdb_conn.interrupt()
                    logger.debug("DuckDB connection interrupted successfully")
            except Exception as interrupt_error:
                logger.debug(f"Failed to interrupt DuckDB connection: {interrupt_error}")

            # Wait a bit more for graceful cleanup
            worker_thread.join(timeout=1.0)

            # Raise timeout error to trigger fallback
            raise concurrent.futures.TimeoutError(f"Query timed out after {self.query_timeout} seconds")

        # Check if worker thread completed successfully
        if not result_container["completed"]:
            raise Exception("Query execution failed - worker thread did not complete")

        # Check for exceptions in worker thread
        if result_container["exception"]:
            raise result_container["exception"]

        return result_container["result"]

    def _execute_fallback(self, query: str) -> set[Any]:
        """Execute query using PostgreSQL fallback."""
        try:
            start_time = time.perf_counter()

            logger.debug(f"Executing query with PostgreSQL fallback: {query[:100]}...")

            with self.postgresql_conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()

            # Convert to set for compatibility
            result_set = {row[0] if len(row) == 1 else row for row in result}

            duration = time.perf_counter() - start_time
            self._update_stats(total_fallback_time=duration)
            self._last_query_time = duration

            logger.debug(f"PostgreSQL fallback completed in {duration:.3f}s, returned {len(result_set)} results")
            return result_set

        except Exception as e:
            logger.error(f"Both DuckDB and PostgreSQL queries failed: {e}")
            self._update_stats(connection_errors=1)
            raise

    def get_statistics(self) -> dict[str, Any]:
        """
        Get performance statistics for the accelerator.

        Returns:
            Dict containing performance metrics
        """
        with self._stats_lock:
            stats = self.stats.copy()

        # Calculate derived metrics
        total_queries = stats["queries_accelerated"] + stats["queries_fallback"]
        stats["total_queries"] = total_queries

        # Always include acceleration_rate (default to 0.0 if no queries)
        if total_queries > 0:
            stats["acceleration_rate"] = stats["queries_accelerated"] / total_queries
        else:
            stats["acceleration_rate"] = 0.0

        # Always include average times (default to 0.0 if no queries)
        if stats["queries_accelerated"] > 0:
            stats["avg_acceleration_time"] = stats["total_acceleration_time"] / stats["queries_accelerated"]
        else:
            stats["avg_acceleration_time"] = 0.0

        if stats["queries_fallback"] > 0:
            stats["avg_fallback_time"] = stats["total_fallback_time"] / stats["queries_fallback"]
        else:
            stats["avg_fallback_time"] = 0.0

        # Add last_query_time (always include, default to 0.0)
        stats["last_query_time"] = getattr(self, '_last_query_time', 0.0)

        stats["initialized"] = self._initialized
        stats["preload_completed"] = self._preload_completed

        return stats

    def log_statistics(self) -> None:
        """Log current performance statistics."""
        if not self.enable_statistics:
            return

        stats = self.get_statistics()

        logger.info("=== DuckDB Query Accelerator Statistics ===")
        logger.info(f"Total queries executed: {stats['total_queries']}")
        logger.info(f"Queries accelerated: {stats['queries_accelerated']} ({stats.get('acceleration_rate', 0):.1%})")
        logger.info(f"Queries fallback: {stats['queries_fallback']}")
        logger.info(f"Tables preloaded: {stats['tables_preloaded']} (took {stats['preload_time']:.2f}s)")

        if stats.get("avg_acceleration_time"):
            logger.info(f"Average acceleration time: {stats['avg_acceleration_time']:.3f}s")
        if stats.get("avg_fallback_time"):
            logger.info(f"Average fallback time: {stats['avg_fallback_time']:.3f}s")

        if stats["connection_errors"] > 0:
            logger.warning(f"Connection errors: {stats['connection_errors']}")

    def _cleanup_connections(self) -> None:
        """Clean up database connections."""
        try:
            if self.duckdb_conn:
                self.duckdb_conn.close()
                self.duckdb_conn = None
        except Exception as e:
            logger.debug(f"Error closing DuckDB connection: {e}")

        try:
            if self.postgresql_conn and not self.postgresql_conn.closed:
                self.postgresql_conn.close()
                self.postgresql_conn = None
        except Exception as e:
            logger.debug(f"Error closing PostgreSQL connection: {e}")

    def close(self) -> None:
        """Close accelerator and clean up resources."""
        logger.debug("Closing DuckDB query accelerator...")

        if self.enable_statistics:
            self.log_statistics()

        self._cleanup_connections()
        self._initialized = False
        self._preload_completed = False

        logger.debug("DuckDB query accelerator closed")

    def __enter__(self):
        """Context manager entry."""
        if not self._initialized:
            self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def create_query_accelerator(
    postgresql_connection_params: dict[str, Any],
    preload_tables: list[str] | None = None,
    query_timeout: float = 0,
    **kwargs
) -> DuckDBQueryAccelerator | None:
    """
    Factory function to create and initialize a DuckDB query accelerator.

    Args:
        postgresql_connection_params: PostgreSQL connection parameters
        preload_tables: List of table names to preload
        query_timeout: Query timeout in seconds (0 = no timeout)
        **kwargs: Additional accelerator configuration options

    Returns:
        Initialized DuckDBQueryAccelerator instance or None if initialization fails
    """
    try:
        accelerator = DuckDBQueryAccelerator(
            postgresql_connection_params=postgresql_connection_params,
            preload_tables=preload_tables,
            query_timeout=query_timeout,
            **kwargs
        )

        if accelerator.initialize():
            return accelerator
        else:
            logger.warning("Failed to initialize query accelerator")
            return None

    except Exception as e:
        logger.error(f"Failed to create query accelerator: {e}")
        return None
