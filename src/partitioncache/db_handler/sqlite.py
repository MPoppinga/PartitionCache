import sqlite3
from typing import List
from logging import getLogger
from partitioncache.db_handler.abstract import AbstractDBHandler

logger = getLogger("PartitionCache")


class SQLiteDBHandler(AbstractDBHandler):
    def __init__(self, db_path: str) -> None:
        self.conn = sqlite3.connect(db_path)
        self.cur = self.conn.cursor()

    def execute(self, query: str) -> List:
        try:
            self.cur.execute(query)
        except Exception as e:
            logger.error(f"Error executing query: {query}")
            logger.error(e)
            return []
        return [x[0] for x in self.cur.fetchall()]
