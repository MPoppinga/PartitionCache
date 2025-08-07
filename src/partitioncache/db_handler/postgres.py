"""
Handles the connection to a PostgreSQL database
"""

from logging import getLogger

import psycopg

from partitioncache.db_handler.abstract import AbstractDBHandler

logger = getLogger("PartitionCache")


class PostgresDBHandler(AbstractDBHandler):
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, timeout: str = "0") -> None:
        # PostgreSQL statement_timeout expects milliseconds when specified as a number without unit
        # Convert seconds to milliseconds
        timeout_ms = int(timeout) * 1000 if timeout != "0" else 0
        conn = psycopg.connect(host=host, port=port, user=user, password=password, dbname=dbname, options=f"-c statement_timeout={timeout_ms}")
        self.conn = conn
        self.cur = conn.cursor()

    def execute(self, query) -> list:
        logger.info(f"POSTGRES EXECUTE: Starting query execution (first 100 chars): {query[:100]}...")
        try:
            self.cur.execute(query)
            logger.info(f"POSTGRES EXECUTE: Query completed, rowcount={self.cur.rowcount}")
        except Exception as e:
            logger.error(f"POSTGRES EXECUTE ERROR: {type(e).__name__}: {e}")
            raise

        # return first column of all rows if not empty
        if self.cur.rowcount == 0:
            return []
        return [row[0] for row in self.cur.fetchall() if row[0]]

    def close(self) -> None:
        self.conn.close()
        self.cur.close()
