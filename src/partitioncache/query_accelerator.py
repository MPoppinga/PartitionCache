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
import re
import threading
import time
from typing import Any

import duckdb
import psycopg

from partitioncache.logging_utils import get_thread_aware_logger

logger = get_thread_aware_logger("PartitionCache")


class DuckDBImportError(ImportError):
    """Backward-compatible exception type used by integration tests and CLI wrappers."""


def validate_table_name(table_name: str) -> None:
    """
    Validate table name to prevent SQL injection.

    Args:
        table_name: The table name to validate

    Raises:
        ValueError: If table name contains invalid characters
    """
    # Allow alphanumeric, underscore, and basic punctuation that's safe in SQL identifiers
    if not re.match(r"^[a-zA-Z0-9_\-\.]+$", table_name):
        msg = f"Invalid table name: '{table_name}'. Only alphanumeric characters, underscores, hyphens, and dots are allowed."
        raise ValueError(msg)

    # Check length
    if len(table_name) > 128:
        msg = f"Table name too long: '{table_name}'. Maximum length is 128 characters."
        raise ValueError(msg)


def _quote(identifier: str) -> str:
    """Quote an SQL identifier (the same rules in DuckDB and PostgreSQL)."""
    return '"' + identifier.replace('"', '""') + '"'


def _conninfo_value(value: Any) -> str:
    """Quote a libpq connection string value, so spaces, quotes and backslashes survive."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


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
        duckdb_database_path: str = ":memory:",
        force_reload_tables: bool = False,
        query_timeout: float = 0,
        read_unloaded_from_postgres: bool = False,
    ):
        """
        Initialize DuckDB query accelerator.

        Args:
            postgresql_connection_params: PostgreSQL connection parameters
            preload_tables: List of table names to preload into DuckDB
            duckdb_memory_limit: Memory limit for DuckDB instance
            duckdb_threads: Number of threads for DuckDB
            enable_statistics: Whether to collect performance statistics
            duckdb_database_path: Path to DuckDB database file (':memory:', the default, for in-memory). Tables in a
                file are reused on the next start without reloading (a snapshot), unless force_reload_tables is set.
            force_reload_tables: Force reload tables from PostgreSQL even if they exist in DuckDB
            query_timeout: Query timeout in seconds (0 = no timeout)
            read_unloaded_from_postgres: Let DuckDB read tables that were not preloaded directly from PostgreSQL
                (through the attached database) instead of running the whole query on PostgreSQL.
        """
        self.postgresql_params = postgresql_connection_params
        self.tables_to_preload = preload_tables or []
        self.duckdb_memory_limit = duckdb_memory_limit
        self.duckdb_threads = duckdb_threads
        self.enable_statistics = enable_statistics
        self.duckdb_database_path = duckdb_database_path
        self.force_reload_tables = force_reload_tables
        self.query_timeout = query_timeout
        self.read_unloaded_from_postgres = read_unloaded_from_postgres
        self._search_path: str | None = None

        # Performance statistics
        self.stats = {
            "queries_accelerated": 0,
            "queries_fallback": 0,
            "queries_timeout": 0,
            "total_acceleration_time": 0.0,
            "total_fallback_time": 0.0,
            "tables_preloaded": 0,
            "preload_time": 0.0,
            "connection_errors": 0,
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
            # PostgreSQL semantics for integer division (7 / 2 = 3); DuckDB returns 3.5 by default
            self.duckdb_conn.execute("SET GLOBAL integer_division = true")  # global: queries run on cursors

            # Install and load PostgreSQL extension
            logger.debug("Installing PostgreSQL extension for DuckDB...")
            self.duckdb_conn.execute("INSTALL postgres")
            self.duckdb_conn.execute("LOAD postgres")

            # Create PostgreSQL connection for metadata and preloading
            self._connect_postgresql()

            if self.read_unloaded_from_postgres:
                self._attach_postgresql()

            if self.duckdb_database_path != ":memory:" and not self.force_reload_tables:
                logger.warning(
                    f"DuckDB database file {self.duckdb_database_path}: tables already in it are reused without reloading, so "
                    "changes in PostgreSQL since they were loaded are not seen (use force_reload_tables or ':memory:')"
                )

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

            # Autocommit: no idle transaction holding locks, and a failed fallback query does not abort the next ones
            self.postgresql_conn = psycopg.connect(**conn_params, autocommit=True)

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
        if self.duckdb_database_path != ":memory:" and not self.force_reload_tables and self._tables_exist_in_duckdb():
            logger.info("Tables already exist in DuckDB, skipping PostgreSQL reload")
            self._preload_completed = True
            return True

        logger.info(f"Preloading {len(self.tables_to_preload)} tables into DuckDB...")
        preload_start = time.perf_counter()

        try:
            self._attach_postgresql()

            tables_loaded = 0

            for table_name in self.tables_to_preload:
                try:
                    # Validate table name to prevent SQL injection
                    validate_table_name(table_name)

                    logger.debug(f"Preloading table: {table_name}")

                    relation = self._resolve_postgresql_relation(table_name)
                    if relation is None:
                        logger.warning(f"Table {table_name} does not exist in PostgreSQL, skipping")
                        continue
                    schema, name = relation
                    local_schema = self._local_schema(table_name, schema)
                    local = f"{_quote(local_schema)}.{_quote(name)}"

                    self.duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote(local_schema)}")
                    self.duckdb_conn.execute(f"CREATE OR REPLACE TABLE {local} AS SELECT * FROM postgres_db.{_quote(schema)}.{_quote(name)}")
                    if "." in table_name and schema == "public":
                        # Queries may also name the table public.<name>; DuckDB's default schema is main
                        self.duckdb_conn.execute("CREATE SCHEMA IF NOT EXISTS public")
                        self.duckdb_conn.execute(f"CREATE OR REPLACE VIEW public.{_quote(name)} AS SELECT * FROM {local}")

                    result = self.duckdb_conn.execute(f"SELECT COUNT(*) FROM {local}").fetchone()
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
        """Build the libpq connection string for DuckDB's postgres extension, with every value quoted."""
        params = self.postgresql_params
        values = {
            "host": params.get("host", "localhost"),
            "port": params.get("port", 5432),
            "dbname": params.get("dbname") or params.get("database"),
            "user": params.get("user"),
            "password": params.get("password"),
        }
        return " ".join(f"{key}={_conninfo_value(value)}" for key, value in values.items() if value is not None and value != "")

    def _attach_postgresql(self) -> None:
        """Attach the PostgreSQL database read-only as postgres_db (once) and, if enabled, put it on the search path."""
        self.duckdb_conn.execute("INSTALL postgres")
        self.duckdb_conn.execute("LOAD postgres")
        attached = self.duckdb_conn.execute("SELECT 1 FROM duckdb_databases() WHERE database_name = 'postgres_db'").fetchone()
        if attached is None:
            conn_str = self._build_duckdb_postgres_connection_string().replace("'", "''")
            self.duckdb_conn.execute(f"ATTACH '{conn_str}' AS postgres_db (TYPE POSTGRES, READ_ONLY)")
        if self.read_unloaded_from_postgres:
            row = self.duckdb_conn.execute("SELECT current_database()").fetchone()
            catalog = (row[0] if row else "memory").replace("'", "''")
            # search_path cannot be set globally; _cursor() sets it on every cursor
            self._search_path = f"{catalog}.main,postgres_db.public"
            self.duckdb_conn.execute(f"SET search_path = '{self._search_path}'")

    def _cursor(self) -> "duckdb.DuckDBPyConnection":
        """A cursor for one query, with the search path that reaches the attached PostgreSQL database if enabled."""
        cursor = self.duckdb_conn.cursor()
        if self._search_path is not None:
            cursor.execute(f"SET search_path = '{self._search_path}'")
        return cursor

    def _resolve_postgresql_relation(self, table_name: str) -> tuple[str, str] | None:
        """(schema, name) of the table, view or materialized view, as written (quoted) or case-folded; None if missing."""
        parts = table_name.split(".")
        candidates = [".".join(_quote(part) for part in parts), table_name]
        with self.postgresql_conn.cursor() as cursor:
            for candidate in candidates:
                try:
                    cursor.execute(
                        "SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass(%s)",
                        (candidate,),
                    )
                except psycopg.Error:
                    continue  # not a valid name in this spelling
                row = cursor.fetchone()
                if row is not None:
                    return row[0], row[1]
        return None

    @staticmethod
    def _local_schema(table_name: str, schema: str) -> str:
        """DuckDB schema for a preloaded table: main for unqualified names and public, else the PostgreSQL schema."""
        return schema if "." in table_name and schema != "public" else "main"

    def _table_exists_in_postgresql(self, table_name: str) -> bool:
        """Check if table, view, or materialized view exists in PostgreSQL."""
        try:
            return self._resolve_postgresql_relation(table_name) is not None
        except Exception as e:
            logger.debug(f"Failed to check table/view existence for {table_name}: {e}")
            return False

    def _tables_exist_in_duckdb(self) -> bool:
        """Check if all preload tables exist in DuckDB."""
        try:
            for table_name in self.tables_to_preload:
                relation = self._resolve_postgresql_relation(table_name)
                if relation is None:
                    continue  # not in PostgreSQL either; preloading would skip it
                schema, name = relation
                result = self.duckdb_conn.execute(
                    "SELECT 1 FROM duckdb_tables() WHERE database_name = current_database() AND schema_name = ? AND table_name = ?",
                    (self._local_schema(table_name, schema), name),
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
                cursor = self._cursor()
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
        result_container: dict[str, Any] = {"result": None, "exception": None, "completed": False, "cursor": None}

        def _duckdb_query_worker():
            """Worker function that executes the DuckDB query."""
            cursor = None
            try:
                # Create thread-local cursor for thread safety
                cursor = self._cursor()
                result_container["cursor"] = cursor
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
            # The query runs on the worker's cursor (its own connection); interrupting the parent connection would not stop it
            target = result_container["cursor"] or self.duckdb_conn
            try:
                if target is not None:
                    target.interrupt()
                    logger.debug("DuckDB query interrupted")
            except Exception as interrupt_error:
                logger.debug(f"Failed to interrupt DuckDB query: {interrupt_error}")

            # Wait for the worker to stop before falling back
            worker_thread.join(timeout=10.0)
            if worker_thread.is_alive():
                logger.error("DuckDB query did not stop after the interrupt; it keeps running in the background")

            # Raise timeout error to trigger fallback
            timeout_msg = f"Query timed out after {self.query_timeout} seconds"
            raise concurrent.futures.TimeoutError(timeout_msg)

        # Check if worker thread completed successfully
        if not result_container["completed"]:
            error_msg = "Query execution failed - worker thread did not complete"
            raise Exception(error_msg)

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
        stats["last_query_time"] = getattr(self, "_last_query_time", 0.0)

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
    postgresql_connection_params: dict[str, Any], preload_tables: list[str] | None = None, query_timeout: float = 0, **kwargs
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
            postgresql_connection_params=postgresql_connection_params, preload_tables=preload_tables, query_timeout=query_timeout, **kwargs
        )

        if accelerator.initialize():
            return accelerator
        else:
            logger.warning("Failed to initialize query accelerator")
            return None

    except Exception as e:
        logger.error(f"Failed to create query accelerator: {e}")
        return None
