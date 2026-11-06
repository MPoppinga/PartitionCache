"""
Handles the connection to a MySQL/MariaDB database
"""

import json
from logging import getLogger
from typing import Any

import mysql.connector

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
            "consume_results": True,
        }
        try:
            self.conn = mysql.connector.connect(**dbconfig)
            self.cur = self.conn.cursor(buffered=True)
        except mysql.connector.Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise

    def execute(self, query) -> list:
        try:
            self.cur.execute(query)
            self.conn.commit()  # Ensure changes are committed

            # return first column of all rows if not empty
            if self.cur.rowcount == 0:
                return []

            result = [row[0] for row in self.cur.fetchall() if row[0] is not None]  # type: ignore
            self.cur.reset()  # Reset the cursor after fetching
            return result
        except mysql.connector.Error as e:
            # Raise instead of returning []: callers cache the result, and an empty list is a valid
            # "no rows" result that restricts later queries to nothing
            logger.error(f"Error executing query: {query}")
            logger.error(e)
            raise

    def estimate_cost(self, query: str) -> float | None:
        """Return ``query_block.cost_info.query_cost`` from ``EXPLAIN FORMAT=JSON``; the query is not executed.

        MariaDB's JSON plan has no ``cost_info``, and MySQL puts no cost at the top level of some plans (e.g.
        UNION, where it is under ``query_block.union_result``); it returns None there.
        """
        self.cur.execute(f"EXPLAIN FORMAT=JSON {query}")
        row = self.cur.fetchone()
        self.cur.reset()
        if not row or not row[0]:
            return None
        raw: Any = row[0]
        plan: dict[str, Any] = json.loads(raw) if isinstance(raw, str | bytes | bytearray) else raw
        cost = plan.get("query_block", {}).get("cost_info", {}).get("query_cost")
        if cost is None:
            logger.debug("EXPLAIN FORMAT=JSON has no query_block.cost_info.query_cost; no cost estimate")
            return None
        return float(cost)

    def close(self) -> None:
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
        except mysql.connector.Error as e:
            logger.error(f"Error closing MySQL connection: {e}")
            raise
