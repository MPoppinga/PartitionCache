"""
Handles the connection to a PostgreSQL database
"""

from typing import Any, List

import psycopg

from partitioncache.db_handler.abstract import AbstractDBHandler


def open_postgres_connection(host: str, port: int, user: str, password: str, dbname: str) -> Any:
    conn = psycopg.connect(host=host, port=port, user=user, password=password, dbname=dbname)
    cur = conn.cursor()
    return conn, cur


class PostgresDBHandler(AbstractDBHandler):
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, timeout: str = "0") -> None:
        conn = psycopg.connect(host=host, port=port, user=user, password=password, dbname=dbname, options=f"-c statement_timeout={timeout}")
        self.conn = conn
        self.cur = conn.cursor()

    def execute(self, query) -> List:
        self.cur.execute(query)

        # return first column of all rows if not empty
        if self.cur.rowcount == 0:
            return []
        return [row[0] for row in self.cur.fetchall()]

    def close(self) -> None:
        self.conn.close()
        self.cur.close()