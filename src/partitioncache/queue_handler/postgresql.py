"""
PostgreSQL queue handler implementation.
"""

from typing import List, Tuple, Optional
from logging import getLogger

from partitioncache.queue_handler.abstract import AbstractQueueHandler

logger = getLogger("PartitionCache")


class PostgreSQLQueueHandler(AbstractQueueHandler):
    """
    PostgreSQL implementation of the queue handler.
    Uses PostgreSQL tables for original query and query fragment queues.
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

            # Create original query queue table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS original_query_queue (
                    query TEXT NOT NULL PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create query fragment queue table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS query_fragment_queue (
                    query TEXT NOT NULL,
                    hash TEXT NOT NULL PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_original_query_queue_created_at
                ON original_query_queue(created_at)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_query_fragment_queue_created_at
                ON query_fragment_queue(created_at)
            """)

            conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL queue tables: {e}")
            raise

    def push_to_original_query_queue(self, query: str) -> bool:
        """
        Push an original query to the original query queue to be processed into fragments.

        Args:
            query (str): The original query to be pushed to the original query queue.

        Returns:
            bool: True if the query was pushed successfully, False otherwise.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("INSERT INTO original_query_queue (query) VALUES (%s)", (query,))
            conn.commit()
            logger.debug("Pushed query to PostgreSQL original query queue")
            return True
        except Exception as e:
            logger.error(f"Failed to push query to PostgreSQL original query queue: {e}")
            return False

    def push_to_query_fragment_queue(self, query_hash_pairs: List[Tuple[str, str]]) -> bool:
        """
        Push query fragments (as query-hash pairs) directly to the query fragment queue.

        Args:
            query_hash_pairs (List[Tuple[str, str]]): List of (query, hash) tuples to push to fragment queue.

        Returns:
            bool: True if all fragments were pushed successfully, False otherwise.
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Insert all query-hash pairs
            for query, hash_value in query_hash_pairs:
                cursor.execute(
                    "INSERT INTO query_fragment_queue (query, hash) VALUES (%s, %s) ON CONFLICT (hash) DO NOTHING",
                    (query, hash_value),
                )

            conn.commit()
            logger.debug(f"Pushed {len(query_hash_pairs)} fragments to PostgreSQL query fragment queue")
            return True
        except Exception as e:
            logger.error(f"Failed to push fragments to PostgreSQL query fragment queue: {e}")
            return False

    def pop_from_original_query_queue(self) -> Optional[str]:
        """
        Pop an original query from the original query queue.

        Returns:
            str or None: The query string if available, None if queue is empty or error occurred.
        """
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Use a transaction to ensure atomicity
            cursor.execute("BEGIN")

            # Get the oldest entry
            cursor.execute("""
                SELECT id, query FROM original_query_queue 
                ORDER BY created_at ASC, id ASC 
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """)

            result = cursor.fetchone()
            if result is None:
                cursor.execute("ROLLBACK")
                return None

            entry_id, query = result

            # Delete the entry
            cursor.execute("DELETE FROM original_query_queue WHERE id = %s", (entry_id,))
            cursor.execute("COMMIT")

            return query
        except Exception as e:
            logger.error(f"Failed to pop from PostgreSQL original query queue: {e}")
            try:
                if cursor:
                    cursor.execute("ROLLBACK")
            except Exception:
                pass
            return None

    def pop_from_query_fragment_queue(self) -> Optional[Tuple[str, str]]:
        """
        Pop a query fragment from the query fragment queue.

        Returns:
            Tuple[str, str] or None: (query, hash) tuple if available, None if queue is empty or error occurred.
        """
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Use a transaction to ensure atomicity
            cursor.execute("BEGIN")

            # Get the oldest entry
            cursor.execute("""
                SELECT id, query, hash FROM query_fragment_queue 
                ORDER BY created_at ASC, id ASC 
                LIMIT 1 FOR UPDATE SKIP LOCKED
            """)

            result = cursor.fetchone()
            if result is None:
                cursor.execute("ROLLBACK")
                return None

            entry_id, query, hash_value = result

            # Delete the entry
            cursor.execute("DELETE FROM query_fragment_queue WHERE id = %s", (entry_id,))
            cursor.execute("COMMIT")

            return (query, hash_value)
        except Exception as e:
            logger.error(f"Failed to pop from PostgreSQL query fragment queue: {e}")
            try:
                if cursor:
                    cursor.execute("ROLLBACK")
            except Exception:
                pass
            return None

    def get_queue_lengths(self) -> dict:
        """
        Get the current lengths of both original query and query fragment queues.

        Returns:
            dict: Dictionary with 'original_query' and 'query_fragment' queue lengths.
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

            return {"original_query": original_query_count, "query_fragment": query_fragment_count}
        except Exception as e:
            logger.error(f"Failed to get PostgreSQL queue lengths: {e}")
            return {"original_query": 0, "query_fragment": 0}

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

            logger.info(
                f"Cleared {original_query_deleted} entries from original query queue, "
                f"{query_fragment_deleted} entries from query fragment queue"
            )
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
