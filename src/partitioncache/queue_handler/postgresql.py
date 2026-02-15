"""
PostgreSQL queue handler implementation.
"""

import select
import time
from logging import getLogger

import psycopg
from psycopg import sql

from partitioncache.queue_handler.abstract import AbstractPriorityQueueHandler

logger = getLogger("PartitionCache")


class PostgreSQLQueueHandler(AbstractPriorityQueueHandler):
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, table_prefix: str):
        """
        Initialize the PostgreSQL queue handler.

        Args:
            host (str): PostgreSQL host
            port (int): PostgreSQL port
            user (str): PostgreSQL username
            password (str): PostgreSQL password
            dbname (str): PostgreSQL database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self._connection = None

        self.table_prefix = table_prefix
        self.original_queue_table = f"{self.table_prefix}_original_query_queue"
        self.fragment_queue_table = f"{self.table_prefix}_query_fragment_queue"

        # Initialize tables and functions on first connection
        self._initialize_tables()
        self._deploy_non_blocking_functions()

        logger.info(f"PostgreSQLQueueHandler initialized for tables {self.original_queue_table} and {self.fragment_queue_table}")

    def _get_connection(self):
        """Get PostgreSQL connection with proper configuration."""
        # Always create fresh connections to avoid transaction state pollution
        # that can cause hanging in concurrent scenarios
        return psycopg.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.dbname)

    def _get_persistent_connection(self):
        """Get a persistent connection for initialization tasks only."""
        if self._connection is None or self._connection.closed:
            self._connection = psycopg.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.dbname)
        return self._connection

    def _initialize_tables(self):
        """Initialize PostgreSQL tables for queue storage."""
        try:
            conn = self._get_persistent_connection()
            cursor = conn.cursor()

            # Check if the original query queue table already exists
            cursor.execute(sql.SQL("SELECT to_regclass('{0}')").format(sql.Identifier(self.original_queue_table)))
            if cursor.fetchone()[0]:
                # Migration: add cache_backend column to existing fragment queue tables
                cursor.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS cache_backend TEXT").format(sql.Identifier(self.fragment_queue_table))
                )
                conn.commit()
                return

            # Create original query queue table with partition_key and priority
            cursor.execute(
                sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    query TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    partition_datatype TEXT,
                    priority INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(query, partition_key)
                )
            """).format(sql.Identifier(self.original_queue_table))
            )

            # Create query fragment queue table with partition_key and priority
            cursor.execute(
                sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY,
                    query TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    partition_datatype TEXT,
                    cache_backend TEXT,
                    priority INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(hash, partition_key)
                )
            """).format(sql.Identifier(self.fragment_queue_table))
            )

            # Create trigger functions for notifications
            cursor.execute("""
                CREATE OR REPLACE FUNCTION notify_original_query_insert()
                RETURNS TRIGGER AS $$
                BEGIN
                    PERFORM pg_notify('original_query_available', '');
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """)

            cursor.execute("""
                CREATE OR REPLACE FUNCTION notify_query_fragment_insert()
                RETURNS TRIGGER AS $$
                BEGIN
                    PERFORM pg_notify('query_fragment_available', '');
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """)

            # Create triggers using IF NOT EXISTS to avoid conflicts
            cursor.execute(
                sql.SQL("""
                CREATE OR REPLACE TRIGGER trigger_notify_original_query_insert
                    AFTER INSERT ON {}
                    FOR EACH ROW EXECUTE FUNCTION notify_original_query_insert();
            """).format(sql.Identifier(self.original_queue_table))
            )

            cursor.execute(
                sql.SQL("""
                CREATE OR REPLACE TRIGGER trigger_notify_query_fragment_insert
                    AFTER INSERT ON {}
                    FOR EACH ROW EXECUTE FUNCTION notify_query_fragment_insert();
            """).format(sql.Identifier(self.fragment_queue_table))
            )

            # Also create UPDATE triggers to catch ON CONFLICT DO UPDATE cases
            cursor.execute(
                sql.SQL("""
                 CREATE OR REPLACE TRIGGER trigger_notify_original_query_update
                    AFTER UPDATE ON {}
                    FOR EACH ROW EXECUTE FUNCTION notify_original_query_insert();
            """).format(sql.Identifier(self.original_queue_table))
            )

            cursor.execute(
                sql.SQL("""
                CREATE OR REPLACE TRIGGER trigger_notify_query_fragment_update
                    AFTER UPDATE ON {}
                    FOR EACH ROW EXECUTE FUNCTION notify_query_fragment_insert();
            """).format(sql.Identifier(self.fragment_queue_table))
            )

            # Create indexes for better performance
            cursor.execute(
                sql.SQL("""
                CREATE INDEX IF NOT EXISTS {}
                ON {}(priority DESC, created_at ASC)
            """).format(sql.Identifier(f"idx_{self.original_queue_table}_priority_created_at"), sql.Identifier(self.original_queue_table))
            )

            cursor.execute(
                sql.SQL("""
                CREATE INDEX IF NOT EXISTS {}
                ON {}(priority DESC, created_at ASC)
            """).format(sql.Identifier(f"idx_{self.fragment_queue_table}_priority_created_at"), sql.Identifier(self.fragment_queue_table))
            )

            cursor.execute(
                sql.SQL("""
                CREATE INDEX IF NOT EXISTS {}
                ON {}(partition_key)
            """).format(sql.Identifier(f"idx_{self.original_queue_table}_partition_key"), sql.Identifier(self.original_queue_table))
            )

            cursor.execute(
                sql.SQL("""
                CREATE INDEX IF NOT EXISTS {}
                ON {}(partition_key)
            """).format(sql.Identifier(f"idx_{self.fragment_queue_table}_partition_key"), sql.Identifier(self.fragment_queue_table))
            )

            conn.commit()
            logger.debug("PostgreSQL queue tables initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL queue tables: {e}")
            raise

    def _deploy_non_blocking_functions(self):
        """Deploy non-blocking queue upsert functions."""
        try:
            conn = self._get_persistent_connection()
            cursor = conn.cursor()

            # Check if functions already exist
            cursor.execute("""
                SELECT routine_name
                FROM information_schema.routines
                WHERE routine_schema = 'public'
                AND routine_name IN ('non_blocking_fragment_queue_upsert', 'non_blocking_original_queue_upsert')
            """)
            existing_functions = [row[0] for row in cursor.fetchall()]

            if len(existing_functions) == 2:
                logger.debug("Non-blocking queue functions already exist")
                return

            logger.info("Deploying non-blocking queue functions...")

            # Load and execute the SQL functions from the same directory
            from pathlib import Path

            sql_file = Path(__file__).parent / "postgresql_queue_helper.sql"
            if not sql_file.exists():
                logger.warning(f"Non-blocking queue functions SQL file not found: {sql_file}")
                return

            with open(sql_file) as f:
                sql_content = f.read()

            # Execute the SQL to create base functions
            cursor.execute(sql.SQL(sql_content))  # type: ignore

            conn.commit()

            logger.info(f"Non-blocking queue functions deployed successfully for tables: {self.original_queue_table}, {self.fragment_queue_table}")

        except Exception as e:
            logger.warning(f"Failed to deploy non-blocking queue functions (will use fallback): {e}")
            # Don't raise - the system can work without these functions, just with potential blocking

    def push_to_original_query_queue(self, query: str, partition_key: str, partition_datatype: str | None = None) -> bool:
        """
        Push an original query to the original query queue (implements AbstractQueueHandler interface).
        Uses non-blocking upsert for proper concurrency handling and priority management.

        Args:
            query (str): The original query to be pushed to the original query queue.
            partition_key (str): The partition key for this query.
            partition_datatype (str): The datatype of the partition key (default: None).

        Returns:
            bool: True if the query was pushed successfully, False otherwise.
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Use non-blocking upsert function for proper concurrency handling
            try:
                cursor.execute(
                    "SELECT non_blocking_original_queue_upsert(%s, %s, %s, %s, %s)", (query, partition_key, partition_datatype, 1, self.original_queue_table)
                )
                result = cursor.fetchone()[0]  # type: ignore
                conn.commit()
                logger.debug(f"Non-blocking original queue upsert result: {result}")
                return True
            except Exception as func_error:
                # Fallback to traditional approach if function doesn't exist
                logger.debug(f"Non-blocking function not available, using fallback: {func_error}")
                cursor.execute(
                    sql.SQL("""
                    INSERT INTO {} (query, partition_key, partition_datatype, priority, updated_at, created_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (query, partition_key)
                    DO UPDATE SET
                        priority = {}.priority + 1,
                        updated_at = CURRENT_TIMESTAMP
                """).format(sql.Identifier(self.original_queue_table), sql.Identifier(self.original_queue_table)),
                    (query, partition_key, partition_datatype, 1),
                )
                conn.commit()
                logger.debug("Used fallback original queue upsert")
                return True

        except Exception as e:
            logger.error(f"Failed to push query to PostgreSQL original query queue: {e}")
            return False
        finally:
            # Ensure fresh connection is properly closed
            if conn and not conn.closed:
                try:
                    conn.close()
                except Exception:
                    pass

    def push_to_query_fragment_queue(self, query_hash_pairs: list[tuple[str, str]], partition_key: str, partition_datatype: str | None = None, cache_backend: str | None = None) -> bool:
        """
        Push query fragments to the fragment queue (implements AbstractQueueHandler interface).
        Uses batch non-blocking upsert for optimal performance and proper concurrency handling.

        Args:
            query_hash_pairs (List[Tuple[str, str]]): List of (query, hash) tuples to push.
            partition_key (str): The partition key for these query fragments.
            partition_datatype (str): The datatype of the partition key (default: None).
            cache_backend (str): The cache backend to use for processing (default: None, uses processor config).

        Returns:
            bool: True if all fragments were pushed successfully, False otherwise.
        """
        if not query_hash_pairs:
            return True

        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Use batch non-blocking upsert for optimal performance
            try:
                # Prepare arrays for batch upsert
                hashes = [hash_value for _, hash_value in query_hash_pairs]
                queries = [query for query, _ in query_hash_pairs]
                partition_keys = [partition_key] * len(query_hash_pairs)
                partition_datatypes = [partition_datatype] * len(query_hash_pairs)
                priorities = [1] * len(query_hash_pairs)  # Default priority
                cache_backends = [cache_backend] * len(query_hash_pairs)

                # Use base batch function directly with table parameter
                cursor.execute(
                    "SELECT * FROM non_blocking_fragment_queue_batch_upsert(%s, %s, %s, %s, %s, %s, %s)",
                    (hashes, partition_keys, partition_datatypes, queries, priorities, self.fragment_queue_table, cache_backends),
                )
                results = cursor.fetchall()
                conn.commit()

                success_count = len([r for r in results if r[3] in ("inserted", "updated")])
                logger.debug(f"Batch upsert results: {success_count}/{len(query_hash_pairs)} successful")
                return True

            except Exception as func_error:
                # Fallback to individual inserts if batch function doesn't exist
                logger.debug(f"Batch function not available, using individual fallback: {func_error}")
                for query, hash_value in query_hash_pairs:
                    cursor.execute(
                        sql.SQL("""
                        INSERT INTO {} (query, hash, partition_key, partition_datatype, cache_backend, priority, updated_at, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (hash, partition_key) DO UPDATE SET
                            priority = {}.priority + 1,
                            updated_at = CURRENT_TIMESTAMP
                    """).format(sql.Identifier(self.fragment_queue_table), sql.Identifier(self.fragment_queue_table)),
                        (query, hash_value, partition_key, partition_datatype, cache_backend, 1),
                    )

                conn.commit()
                logger.debug(f"Pushed {len(query_hash_pairs)} query fragments using fallback method")
                return True

        except Exception as e:
            logger.error(f"Failed to push query fragments to PostgreSQL queue handler: {e}")
            return False
        finally:
            # Ensure fresh connection is properly closed
            if conn and not conn.closed:
                try:
                    conn.close()
                except Exception:
                    pass

    def push_to_original_query_queue_with_priority(self, query: str, partition_key: str, priority: int = 1, partition_datatype: str | None = None) -> bool:
        """
        Push an original query to the original query queue with specified priority.
        If the query already exists, increment its priority.

        Args:
            query (str): The original query to be pushed to the original query queue.
            partition_key (str): The partition key for this query.
            priority (int): Initial priority for the query (default: 1).
            partition_datatype (str): The datatype of the partition key (default: "integer").

        Returns:
            bool: True if the query was pushed/updated successfully, False otherwise.
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Try non-blocking upsert function first
            try:
                cursor.execute(
                    "SELECT non_blocking_original_queue_upsert(%s, %s, %s, %s, %s)",
                    (query, partition_key, partition_datatype, priority, self.original_queue_table),
                )
                result = cursor.fetchone()[0]  # type: ignore
                conn.commit()
                logger.debug(f"Non-blocking original queue upsert result: {result}")
                return True
            except Exception as func_error:
                # Fallback to traditional approach if function doesn't exist
                logger.debug(f"Non-blocking function not available, using fallback: {func_error}")
                cursor.execute(
                    sql.SQL("""
                    INSERT INTO {} (query, partition_key, partition_datatype, priority, updated_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (query, partition_key)
                    DO UPDATE SET
                        priority = {}.priority + 1,
                        updated_at = CURRENT_TIMESTAMP
                """).format(sql.Identifier(self.original_queue_table), sql.Identifier(self.original_queue_table)),
                    (query, partition_key, partition_datatype, priority),
                )
                conn.commit()
                logger.debug("Used fallback original queue upsert")
                return True

        except Exception as e:
            logger.error(f"Failed to push query to PostgreSQL original query queue: {e}")
            return False
        finally:
            # Ensure fresh connection is properly closed
            if conn and not conn.closed:
                try:
                    conn.close()
                except Exception:
                    pass

    def push_to_query_fragment_queue_with_priority(
        self, query_hash_pairs: list[tuple[str, str]], partition_key: str, priority: int = 1, partition_datatype: str | None = None, cache_backend: str | None = None
    ) -> bool:
        """
        Push query fragments with specified priority.
        Uses non-blocking upsert to avoid concurrency issues with locked rows.
        If a fragment already exists and is not being processed, increment its priority.

        Args:
            query_hash_pairs (List[Tuple[str, str]]): List of (query, hash) tuples to push.
            partition_key (str): The partition key for these query fragments.
            priority (int): Initial priority for the query fragments (default: 1).
            partition_datatype (str): The datatype of the partition key (default: "integer").

        Returns:
            bool: True if all fragments were pushed/updated successfully, False otherwise.
        """
        if not query_hash_pairs:
            return True

        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Try batch non-blocking upsert first
            try:
                # Prepare arrays for batch upsert
                hashes = [hash_value for _, hash_value in query_hash_pairs]
                queries = [query for query, _ in query_hash_pairs]
                partition_keys = [partition_key] * len(query_hash_pairs)
                partition_datatypes = [partition_datatype] * len(query_hash_pairs)
                priorities = [priority] * len(query_hash_pairs)
                cache_backends = [cache_backend] * len(query_hash_pairs)

                # Use base batch function directly with table parameter
                cursor.execute(
                    "SELECT * FROM non_blocking_fragment_queue_batch_upsert(%s, %s, %s, %s, %s, %s, %s)",
                    (hashes, partition_keys, partition_datatypes, queries, priorities, self.fragment_queue_table, cache_backends),
                )
                results = cursor.fetchall()
                conn.commit()

                success_count = len([r for r in results if r[3] in ("inserted", "updated")])
                logger.debug(f"Batch upsert with priority results: {success_count}/{len(query_hash_pairs)} successful")
                return True
            except Exception as func_error:
                # Fallback to traditional approach if function doesn't exist
                logger.debug(f"Non-blocking function not available, using fallback: {func_error}")
                for query, hash_value in query_hash_pairs:
                    cursor.execute(
                        sql.SQL("""
                        INSERT INTO {} (query, hash, partition_key, partition_datatype, cache_backend, priority, updated_at, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (hash, partition_key)
                        DO UPDATE SET
                            priority = {}.priority + %s,
                            updated_at = CURRENT_TIMESTAMP
                    """).format(sql.Identifier(self.fragment_queue_table), sql.Identifier(self.fragment_queue_table)),
                        (query, hash_value, partition_key, partition_datatype, cache_backend, priority, priority),
                    )

                conn.commit()
                logger.debug(f"Used fallback fragment queue upsert for {len(query_hash_pairs)} items")
                return True

        except Exception as e:
            logger.error(f"Failed to push query fragments to PostgreSQL queue handler: {e}")
            return False
        finally:
            # Ensure fresh connection is properly closed
            if conn and not conn.closed:
                try:
                    conn.close()
                except Exception:
                    pass

    def pop_from_original_query_queue(self) -> tuple[str, str, str] | None:
        """
        Pop an original query from the original query queue.
        Uses PostgreSQL's SELECT FOR UPDATE SKIP LOCKED for atomic operations.

        Returns:
            Tuple[str, str, str] or None: (query, partition_key, partition_datatype) tuple if available, None if queue is empty.
        """
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Begin transaction for atomic pop operation
            cursor.execute("BEGIN")

            # Get the highest priority entry (highest priority first, then oldest)
            cursor.execute(
                sql.SQL("""
                SELECT id, query, partition_key, partition_datatype FROM {}
                ORDER BY priority DESC, created_at ASC
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """).format(sql.Identifier(self.original_queue_table))
            )

            entry = cursor.fetchone()
            if not entry:
                cursor.execute("ROLLBACK")
                return None

            entry_id, query, partition_key, partition_datatype = entry

            # Delete the entry
            cursor.execute(sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(self.original_queue_table)), (entry_id,))
            cursor.execute("COMMIT")

            logger.debug("Popped query from PostgreSQL original query queue")
            return (query, partition_key, partition_datatype or "")

        except Exception as e:
            logger.error(f"Failed to pop from PostgreSQL original query queue: {e}")
            try:
                if cursor:
                    cursor.execute("ROLLBACK")
            except Exception:
                pass
            return None

    def pop_from_original_query_queue_blocking(self, timeout: int = 60) -> tuple[str, str, str] | None:
        """
        Pop an original query from the original query queue with blocking wait.
        Uses PostgreSQL LISTEN/NOTIFY for efficient blocking with timeout fallback.

        Args:
            timeout (int): Maximum time to wait in seconds (default: 60)

        Returns:
            Tuple[str, str, str] or None: (query, partition_key, partition_datatype) tuple if available, None if timeout or error occurred.
        """

        # First try to get an item immediately
        result = self.pop_from_original_query_queue()
        if result is not None:
            return result

        # If no item available, set up LISTEN for notifications
        listen_conn = None
        start_time = time.time()

        try:
            # Create a separate connection for LISTEN/NOTIFY
            listen_conn = psycopg.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.dbname)
            listen_conn.autocommit = True

            # Start listening for notifications
            cursor = listen_conn.cursor()
            cursor.execute("LISTEN original_query_available")

            # Main blocking loop with LISTEN/NOTIFY
            while time.time() - start_time < timeout:
                remaining_time = max(0, timeout - (time.time() - start_time))
                if remaining_time <= 0:
                    break

                # Wait for notification with timeout (max 5 seconds per iteration)
                wait_time = min(5.0, remaining_time)

                try:
                    # Use select to wait for notifications with timeout
                    if select.select([listen_conn.fileno()], [], [], wait_time):
                        # Check for notifications
                        for notify in listen_conn.notifies():
                            if notify.channel == "original_query_available":
                                # Try to pop an item after notification
                                result = self.pop_from_original_query_queue()
                                if result is not None:
                                    return result
                                break
                    else:
                        # Timeout on select, check for items anyway (safety net)
                        result = self.pop_from_original_query_queue()
                        if result is not None:
                            return result

                except (OSError, psycopg.Error, psycopg.OperationalError) as e:
                    logger.debug(f"LISTEN/NOTIFY error, falling back to polling: {e}")
                    # Fall back to polling for remainder of timeout
                    break

            # Final check or fallback polling
            remaining_time = max(0, timeout - (time.time() - start_time))
            if remaining_time > 0:
                result = self.pop_from_original_query_queue()
                if result is not None:
                    return result

            logger.debug(f"Blocking pop from original query queue timed out after {timeout} seconds")
            return None

        except Exception as e:
            logger.error(f"Error during blocking pop from original query queue: {e}")
            # Fallback to simple polling for remaining time
            remaining_time = max(0, timeout - (time.time() - start_time))
            if remaining_time > 1.0:
                time.sleep(min(1.0, remaining_time))
                return self.pop_from_original_query_queue()
            return None
        finally:
            if listen_conn:
                try:
                    cursor = listen_conn.cursor()
                    cursor.execute("UNLISTEN original_query_available")
                    listen_conn.close()
                except Exception:
                    pass

    def pop_from_query_fragment_queue(self) -> tuple[str, str, str, str, str | None] | None:
        """
        Pop a query fragment from the query fragment queue.
        Uses PostgreSQL's SELECT FOR UPDATE SKIP LOCKED for atomic operations.

        Returns:
            Tuple[str, str, str, str, str | None] or None: (query, hash, partition_key, partition_datatype, cache_backend) tuple if available, None if queue is empty.
        """
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Begin transaction for atomic pop operation
            cursor.execute("BEGIN")

            # Get the highest priority entry (highest priority first, then oldest)
            cursor.execute(
                sql.SQL("""
                SELECT id, query, hash, partition_key, partition_datatype, cache_backend FROM {}
                ORDER BY priority DESC, created_at ASC
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """).format(sql.Identifier(self.fragment_queue_table))
            )

            entry = cursor.fetchone()
            if not entry:
                cursor.execute("ROLLBACK")
                return None

            entry_id, query, hash_value, partition_key, partition_datatype, item_cache_backend = entry

            # Delete the entry
            cursor.execute(sql.SQL("DELETE FROM {} WHERE id = %s").format(sql.Identifier(self.fragment_queue_table)), (entry_id,))
            cursor.execute("COMMIT")

            logger.debug("Popped query fragment from PostgreSQL query fragment queue")
            return (query, hash_value, partition_key, partition_datatype or "", item_cache_backend)

        except Exception as e:
            logger.error(f"Failed to pop from PostgreSQL query fragment queue: {e}")
            try:
                if cursor:
                    cursor.execute("ROLLBACK")
            except Exception:
                pass
            return None

    def pop_from_query_fragment_queue_blocking(self, timeout: int = 60) -> tuple[str, str, str, str, str | None] | None:
        """
        Pop a query fragment from the query fragment queue with blocking wait.
        Uses PostgreSQL LISTEN/NOTIFY for efficient blocking with timeout fallback.

        Args:
            timeout (int): Maximum time to wait in seconds (default: 60)

        Returns:
            Tuple[str, str, str, str, str | None] or None: (query, hash, partition_key, partition_datatype, cache_backend) tuple if available, None if timeout or error occurred.
        """

        # First try to get an item immediately
        result = self.pop_from_query_fragment_queue()
        if result is not None:
            return result

        # If no item available, set up LISTEN for notifications
        listen_conn = None
        start_time = time.time()

        try:
            # Create a separate connection for LISTEN/NOTIFY
            listen_conn = psycopg.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.dbname)
            listen_conn.autocommit = True

            # Start listening for notifications
            cursor = listen_conn.cursor()
            cursor.execute("LISTEN query_fragment_available")

            # Main blocking loop with LISTEN/NOTIFY
            while time.time() - start_time < timeout:
                remaining_time = max(0, timeout - (time.time() - start_time))
                if remaining_time <= 0:
                    break

                # Wait for notification with timeout (max 5 seconds per iteration)
                wait_time = min(5.0, remaining_time)

                try:
                    # Use select to wait for notifications with timeout
                    if select.select([listen_conn.fileno()], [], [], wait_time):
                        # Check for notifications
                        for notify in listen_conn.notifies():
                            if notify.channel == "query_fragment_available":
                                # Try to pop an item after notification
                                result = self.pop_from_query_fragment_queue()
                                if result is not None:
                                    return result
                                break
                    else:
                        # Timeout on select, check for items anyway (safety net)
                        result = self.pop_from_query_fragment_queue()
                        if result is not None:
                            return result

                except (OSError, psycopg.Error, psycopg.OperationalError) as e:
                    logger.debug(f"LISTEN/NOTIFY error, falling back to polling: {e}")
                    # Fall back to polling for remainder of timeout
                    break

            # Final check or fallback polling
            remaining_time = max(0, timeout - (time.time() - start_time))
            if remaining_time > 0:
                result = self.pop_from_query_fragment_queue()
                if result is not None:
                    return result

            logger.debug(f"Blocking pop from query fragment queue timed out after {timeout} seconds")
            return None

        except Exception as e:
            logger.error(f"Error during blocking pop from query fragment queue: {e}")
            # Fallback to simple polling for remaining time
            remaining_time = max(0, timeout - (time.time() - start_time))
            if remaining_time > 1.0:
                time.sleep(min(1.0, remaining_time))
                return self.pop_from_query_fragment_queue()
            return None
        finally:
            if listen_conn:
                try:
                    cursor = listen_conn.cursor()
                    cursor.execute("UNLISTEN query_fragment_available")
                    listen_conn.close()
                except Exception:
                    pass

    def get_queue_lengths(self) -> dict:
        """
        Get the current lengths of both original query and query fragment queues.

        Returns:
            dict: Dictionary with 'original_query_queue' and 'query_fragment_queue' queue lengths.
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(self.original_queue_table)))
            original_query_result = cursor.fetchone()
            original_query_count = original_query_result[0] if original_query_result else 0

            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(self.fragment_queue_table)))
            query_fragment_result = cursor.fetchone()
            query_fragment_count = query_fragment_result[0] if query_fragment_result else 0

            return {"original_query_queue": original_query_count, "query_fragment_queue": query_fragment_count}
        except (psycopg.OperationalError, psycopg.DatabaseError, OSError) as e:
            logger.warning(f"Failed to get PostgreSQL queue lengths (connection issue): {e}")
            return {"original_query_queue": 0, "query_fragment_queue": 0}
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL queue lengths: {e}")
            return {"original_query_queue": 0, "query_fragment_queue": 0}
        finally:
            # Ensure connection is properly closed
            if conn and not conn.closed:
                try:
                    conn.close()
                except Exception:
                    pass

    def clear_original_query_queue(self) -> int:
        """
        Clear the original query queue and return the number of entries cleared.
        Uses TRUNCATE for optimal performance with fallback to DELETE.

        Returns:
            int: Number of entries cleared from the original query queue.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # First, get the count before clearing (for TRUNCATE which doesn't return rowcount)
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(self.original_queue_table)))
            count_result = cursor.fetchone()
            deleted_count = count_result[0] if count_result else 0

            # Try TRUNCATE first for optimal performance
            try:
                cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(self.original_queue_table)))
                conn.commit()
                logger.debug(f"Cleared {deleted_count} entries from PostgreSQL original query queue using TRUNCATE")
                return deleted_count
            except Exception as truncate_error:
                # TRUNCATE failed (likely permissions), fallback to DELETE
                logger.debug(f"TRUNCATE failed ({truncate_error}), falling back to DELETE")
                cursor.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(self.original_queue_table)))
                deleted_count = cursor.rowcount or 0
                conn.commit()
                logger.debug(f"Cleared {deleted_count} entries from PostgreSQL original query queue using DELETE fallback")
                return deleted_count

        except Exception as e:
            logger.error(f"Failed to clear PostgreSQL original query queue: {e}")
            return 0

    def clear_query_fragment_queue(self) -> int:
        """
        Clear the query fragment queue and return the number of entries cleared.
        Uses TRUNCATE for optimal performance with fallback to DELETE.

        Returns:
            int: Number of entries cleared from the query fragment queue.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # First, get the count before clearing (for TRUNCATE which doesn't return rowcount)
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(self.fragment_queue_table)))
            count_result = cursor.fetchone()
            deleted_count = count_result[0] if count_result else 0

            # Try TRUNCATE first for optimal performance
            try:
                cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(self.fragment_queue_table)))
                conn.commit()
                logger.debug(f"Cleared {deleted_count} entries from PostgreSQL query fragment queue using TRUNCATE")
                return deleted_count
            except Exception as truncate_error:
                # TRUNCATE failed (likely permissions), fallback to DELETE
                logger.debug(f"TRUNCATE failed ({truncate_error}), falling back to DELETE")
                cursor.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(self.fragment_queue_table)))
                deleted_count = cursor.rowcount or 0
                conn.commit()
                logger.debug(f"Cleared {deleted_count} entries from PostgreSQL query fragment queue using DELETE fallback")
                return deleted_count

        except Exception as e:
            logger.error(f"Failed to clear PostgreSQL query fragment queue: {e}")
            return 0

    def clear_all_queues(self) -> tuple[int, int]:
        """
        Clear both original query and query fragment queues.
        Uses TRUNCATE for optimal performance with fallback to DELETE.

        Returns:
            Tuple[int, int]: (original_query_cleared, query_fragment_cleared) number of entries cleared.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Get counts before clearing (for TRUNCATE which doesn't return rowcount)
            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(self.original_queue_table)))
            original_result = cursor.fetchone()
            original_query_count = original_result[0] if original_result else 0

            cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(self.fragment_queue_table)))
            fragment_result = cursor.fetchone()
            query_fragment_count = fragment_result[0] if fragment_result else 0

            # Try TRUNCATE first for optimal performance
            try:
                # TRUNCATE both tables in a single transaction for atomicity
                cursor.execute(sql.SQL("TRUNCATE TABLE {}, {}").format(sql.Identifier(self.original_queue_table), sql.Identifier(self.fragment_queue_table)))
                conn.commit()
                logger.debug(f"Cleared all PostgreSQL queues using TRUNCATE: {original_query_count} original, {query_fragment_count} fragments")
                return (original_query_count, query_fragment_count)

            except Exception as truncate_error:
                # TRUNCATE failed (likely permissions), fallback to DELETE
                logger.debug(f"TRUNCATE failed ({truncate_error}), falling back to DELETE")

                # Clear both tables using DELETE
                cursor.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(self.original_queue_table)))
                original_query_deleted = cursor.rowcount or 0

                cursor.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(self.fragment_queue_table)))
                query_fragment_deleted = cursor.rowcount or 0

                conn.commit()
                logger.debug(f"Cleared all PostgreSQL queues using DELETE fallback: {original_query_deleted} original, {query_fragment_deleted} fragments")
                return (original_query_deleted, query_fragment_deleted)

        except Exception as e:
            logger.error(f"Failed to clear all PostgreSQL queues: {e}")
            return (0, 0)

    def close(self) -> None:
        """
        Close the queue handler and release any resources.
        """
        if self._connection and not self._connection.closed:
            try:
                self._connection.close()
            except Exception as e:
                logger.error(f"Error closing PostgreSQL connection: {e}")
            finally:
                self._connection = None
