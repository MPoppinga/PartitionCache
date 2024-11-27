"""
Handles the connection to a MySQL/MariaDB database
"""

from logging import getLogger
from typing import Any, List

import mysql.connector
from mysql.connector import pooling

from partitioncache.db_handler.abstract import AbstractDBHandler

logger = getLogger("PartitionCache")


def open_mysql_connection(host: str, port: int, user: str, password: str, dbname: str) -> Any:
    dbconfig = {"host": host, "port": port, "user": user, "password": password, "database": dbname, "charset": "utf8mb4", "collation": "utf8mb4_general_ci"}
    conn = mysql.connector.connect(**dbconfig)
    cur = conn.cursor(buffered=True)
    return conn, cur


class MySQLDBHandler(AbstractDBHandler):
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, timeout: str = "0") -> None:
        dbconfig = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": dbname,
            "charset": "utf8mb4",
            "collation": "utf8mb4_general_ci",
            "pool_name": "mypool",
            "pool_size": 5,
            "connect_timeout": int(timeout) if timeout != "0" else None,
            "consume_results": True
        }
        try:
            self.conn = mysql.connector.connect(**dbconfig)
            self.cur = self.conn.cursor(buffered=True)
        except mysql.connector.Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise

    def execute(self, query) -> List:
        try:
            self.cur.execute(query)
            self.conn.commit()  # Ensure changes are committed

            # return first column of all rows if not empty
            if self.cur.rowcount == 0:
                return []

            result = [row[0] for row in self.cur.fetchall()]  # type: ignore
            self.cur.reset()  # Reset the cursor after fetching
            return result
        except mysql.connector.Error as e:
            logger.error(f"Error executing query: {query}")
            logger.error(e)
            return []

    def close(self) -> None:
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
        except mysql.connector.Error as e:
            logger.error(f"Error closing MySQL connection: {e}")
            raise
