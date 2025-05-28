"""
PostgreSQL queue handler implementation.
"""

from typing import List, Tuple, Optional
from logging import getLogger

from partitioncache.queue_handler.abstract import AbstractPriorityQueueHandler

logger = getLogger("PartitionCache")


class PostgreSQLQueueHandler(AbstractPriorityQueueHandler):
    """
    PostgreSQL implementation of the priority queue handler.
    Uses PostgreSQL tables for original query and query fragment queues with priority support.
    """

    def __init__(self, host: str, port: int, user: str, password: str, dbname: str):
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

        # Initialize tables on first connection
        self._initialize_tables()

    def _get_connection(self):
        """Get PostgreSQL connection with proper configuration."""
        if self._connection is None or self._connection.closed:
            import psycopg

            self._connection = psycopg.connect(host=self.host, port=self.port, user=self.user, password=self.password, dbname=self.dbname)
        return self._connection

    def _initialize_tables(self):
        """Initialize PostgreSQL tables for queue storage."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Create original query queue table with partition_key and priority
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS original_query_queue (
                    id SERIAL PRIMARY KEY,
                    query TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    partition_datatype TEXT,
                    priority INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(query, partition_key)
                )
            """)

            # Create query fragment queue table with partition_key and priority
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS query_fragment_queue (
                    id SERIAL PRIMARY KEY,
                    query TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    partition_datatype TEXT,
                    priority INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(hash, partition_key)
                )
            """)

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
            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS trigger_notify_original_query_insert
                    AFTER INSERT ON original_query_queue
                    FOR EACH ROW EXECUTE FUNCTION notify_original_query_insert();
            """)

            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS trigger_notify_query_fragment_insert
                    AFTER INSERT ON query_fragment_queue
                    FOR EACH ROW EXECUTE FUNCTION notify_query_fragment_insert();
            """)

            # Also create UPDATE triggers to catch ON CONFLICT DO UPDATE cases
            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS trigger_notify_original_query_update
                    AFTER UPDATE ON original_query_queue
                    FOR EACH ROW EXECUTE FUNCTION notify_original_query_insert();
            """)

            cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS trigger_notify_query_fragment_update
                    AFTER UPDATE ON query_fragment_queue
                    FOR EACH ROW EXECUTE FUNCTION notify_query_fragment_insert();
            """)

            # Create indexes for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_original_query_queue_priority_created_at
                ON original_query_queue(priority DESC, created_at ASC)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_query_fragment_queue_priority_created_at
                ON query_fragment_queue(priority DESC, created_at ASC)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_original_query_queue_partition_key
                ON original_query_queue(partition_key)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_query_fragment_queue_partition_key
                ON query_fragment_queue(partition_key)
            """)

            conn.commit()
            logger.debug("PostgreSQL queue tables initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL queue tables: {e}")
            raise

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
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO original_query_queue (query, partition_key, partition_datatype, priority, updated_at) 
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (query, partition_key) 
                DO UPDATE SET 
                    priority = original_query_queue.priority + 1,
                    updated_at = CURRENT_TIMESTAMP
            """,
                (query, partition_key, partition_datatype, priority),
            )

            conn.commit()
            logger.debug("Pushed/updated query in PostgreSQL original query queue")
            return True
        except Exception as e:
            logger.error(f"Failed to push query to PostgreSQL original query queue: {e}")
            return False

    def push_to_query_fragment_queue_with_priority(
        self, query_hash_pairs: List[Tuple[str, str]], partition_key: str, priority: int = 1, partition_datatype: str | None = None
    ) -> bool:
        """
        Push query fragments with specified priority.
        If a fragment already exists, increment its priority.

        Args:
            query_hash_pairs (List[Tuple[str, str]]): List of (query, hash) tuples to push to fragment queue.
            partition_key (str): The partition key for these query fragments.
            priority (int): Initial priority for the fragments (default: 1).
            partition_datatype (str): The datatype of the partition key (default: "integer").

        Returns:
            bool: True if all fragments were pushed/updated successfully, False otherwise.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Insert all query-hash pairs with priority support
            for query, hash_value in query_hash_pairs:
                cursor.execute(
                    """
                    INSERT INTO query_fragment_queue (query, hash, partition_key, partition_datatype, priority, updated_at) 
                    VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (hash, partition_key) 
                    DO UPDATE SET 
                        priority = query_fragment_queue.priority + 1,
                        updated_at = CURRENT_TIMESTAMP
                """,
                    (query, hash_value, partition_key, partition_datatype, priority),
                )

            conn.commit()
            logger.debug(f"Pushed/updated {len(query_hash_pairs)} fragments in PostgreSQL query fragment queue")
            return True
        except Exception as e:
            logger.error(f"Failed to push fragments to PostgreSQL query fragment queue: {e}")
            return False

    def pop_from_original_query_queue(self) -> Optional[Tuple[str, str, str]]:
        """
        Pop an original query from the original query queue.
        Prioritizes queries with highest priority, then oldest creation time.

        Returns:
            Tuple[str, str, str] or None: (query, partition_key, partition_datatype) tuple if available, None if queue is empty or error occurred.
        """
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Use a transaction to ensure atomicity
            cursor.execute("BEGIN")

            # Get the highest priority entry (highest priority first, then oldest)
            cursor.execute("""
                SELECT id, query, partition_key, partition_datatype FROM original_query_queue 
                ORDER BY priority DESC, created_at ASC 
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """)

            result = cursor.fetchone()
            if result is None:
                cursor.execute("ROLLBACK")
                return None

            entry_id, query, partition_key, partition_datatype = result

            # Delete the entry
            cursor.execute("DELETE FROM original_query_queue WHERE id = %s", (entry_id,))
            cursor.execute("COMMIT")

            return (query, partition_key, partition_datatype)
        except Exception as e:
            logger.error(f"Failed to pop from PostgreSQL original query queue: {e}")
            try:
                if cursor:
                    cursor.execute("ROLLBACK")
            except Exception:
                pass
            return None

    def pop_from_original_query_queue_blocking(self, timeout: int = 60) -> Optional[Tuple[str, str, str]]:
        """
        Pop an original query from the original query queue with blocking wait.
        Uses PostgreSQL LISTEN/NOTIFY for efficient blocking with timeout fallback.

        Args:
            timeout (int): Maximum time to wait in seconds (default: 60)

        Returns:
            Tuple[str, str, str] or None: (query, partition_key, partition_datatype) tuple if available, None if timeout or error occurred.
        """
        import psycopg
        import time
        import select

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

                except (OSError, psycopg.Error) as e:
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
                    listen_conn.close()
                except Exception:
                    pass

    def pop_from_query_fragment_queue(self) -> Optional[Tuple[str, str, str, str]]:
        """
        Pop a query fragment from the query fragment queue.
        Prioritizes fragments with highest priority, then oldest creation time.

        Returns:
            Tuple[str, str, str, str] or None: (query, hash, partition_key, partition_datatype) tuple if available, None if queue is empty or error occurred.
        """
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Use a transaction to ensure atomicity
            cursor.execute("BEGIN")

            # Get the highest priority entry (highest priority first, then oldest)
            cursor.execute("""
                SELECT id, query, hash, partition_key, partition_datatype FROM query_fragment_queue 
                ORDER BY priority DESC, created_at ASC 
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """)

            result = cursor.fetchone()
            if result is None:
                cursor.execute("ROLLBACK")
                return None

            entry_id, query, hash_value, partition_key, partition_datatype = result

            # Delete the entry
            cursor.execute("DELETE FROM query_fragment_queue WHERE id = %s", (entry_id,))
            cursor.execute("COMMIT")

            return (query, hash_value, partition_key, partition_datatype)
        except Exception as e:
            logger.error(f"Failed to pop from PostgreSQL query fragment queue: {e}")
            try:
                if cursor:
                    cursor.execute("ROLLBACK")
            except Exception:
                pass
            return None

    def pop_from_query_fragment_queue_blocking(self, timeout: int = 60) -> Optional[Tuple[str, str, str, str]]:
        """
        Pop a query fragment from the query fragment queue with blocking wait.
        Uses PostgreSQL LISTEN/NOTIFY for efficient blocking with timeout fallback.

        Args:
            timeout (int): Maximum time to wait in seconds (default: 60)

        Returns:
            Tuple[str, str, str, str] or None: (query, hash, partition_key, partition_datatype) tuple if available, None if timeout or error occurred.
        """
        import psycopg
        import time
        import select

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

                except (OSError, psycopg.Error) as e:
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
                    listen_conn.close()
                except Exception:
                    pass

    def get_queue_lengths(self) -> dict:
        """
        Get the current lengths of both original query and query fragment queues.

        Returns:
            dict: Dictionary with 'original_query_queue' and 'query_fragment_queue' queue lengths.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM original_query_queue")
            original_query_result = cursor.fetchone()
            original_query_count = original_query_result[0] if original_query_result else 0

            cursor.execute("SELECT COUNT(*) FROM query_fragment_queue")
            query_fragment_result = cursor.fetchone()
            query_fragment_count = query_fragment_result[0] if query_fragment_result else 0

            return {"original_query_queue": original_query_count, "query_fragment_queue": query_fragment_count}
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL queue lengths: {e}")
            return {"original_query_queue": 0, "query_fragment_queue": 0}

    def clear_original_query_queue(self) -> int:
        """
        Clear the original query queue and return the number of entries cleared.

        Returns:
            int: Number of entries cleared from the original query queue.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM original_query_queue")
            deleted_count = cursor.rowcount or 0
            conn.commit()
            logger.info(f"Cleared {deleted_count} entries from PostgreSQL original query queue")
            return deleted_count
        except Exception as e:
            logger.error(f"Failed to clear PostgreSQL original query queue: {e}")
            return 0

    def clear_query_fragment_queue(self) -> int:
        """
        Clear the query fragment queue and return the number of entries cleared.

        Returns:
            int: Number of entries cleared from the query fragment queue.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM query_fragment_queue")
            deleted_count = cursor.rowcount or 0
            conn.commit()
            logger.info(f"Cleared {deleted_count} entries from PostgreSQL query fragment queue")
            return deleted_count
        except Exception as e:
            logger.error(f"Failed to clear PostgreSQL query fragment queue: {e}")
            return 0

    def clear_all_queues(self) -> Tuple[int, int]:
        """
        Clear both original query and query fragment queues.

        Returns:
            Tuple[int, int]: (original_query_cleared, query_fragment_cleared) number of entries cleared.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Clear both tables
            cursor.execute("DELETE FROM original_query_queue")
            original_query_deleted = cursor.rowcount or 0

            cursor.execute("DELETE FROM query_fragment_queue")
            query_fragment_deleted = cursor.rowcount or 0

            conn.commit()

            logger.info(f"Cleared {original_query_deleted} entries from original query queue, {query_fragment_deleted} entries from query fragment queue")
            return (original_query_deleted, query_fragment_deleted)
        except Exception as e:
            logger.error(f"Failed to clear PostgreSQL queues: {e}")
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
