import sqlite3
from logging import getLogger

from partitioncache.db_handler.abstract import AbstractDBHandler

logger = getLogger("PartitionCache")


class SQLiteDBHandler(AbstractDBHandler):
    def __init__(self, db_path: str) -> None:
        try:
            self.conn = sqlite3.connect(db_path)
            self.cur = self.conn.cursor()
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database at {db_path}: {e}")
            raise

    def execute(self, query: str) -> list:
        try:
            self.cur.execute(query)
            self.conn.commit()  # Ensure changes are committed
            if self.cur.rowcount == 0:
                return []
            return [row[0] for row in self.cur.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error executing query: {query}")
            logger.error(e)
            return []

    def close(self) -> None:
        try:
            self.conn.close()
            self.cur.close()
        except sqlite3.Error as e:
            logger.error(f"Error closing SQLite connection: {e}")
            raise
