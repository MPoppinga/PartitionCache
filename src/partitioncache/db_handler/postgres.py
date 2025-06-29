"""
Handles the connection to a PostgreSQL database
"""


import psycopg

from partitioncache.db_handler.abstract import AbstractDBHandler


class PostgresDBHandler(AbstractDBHandler):
    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, timeout: str = "0") -> None:
        conn = psycopg.connect(host=host, port=port, user=user, password=password, dbname=dbname, options=f"-c statement_timeout={timeout}")
        self.conn = conn
        self.cur = conn.cursor()

    def execute(self, query) -> list:
        self.cur.execute(query)

        # return first column of all rows if not empty
        if self.cur.rowcount == 0:
            return []
        return [row[0] for row in self.cur.fetchall() if row[0]]

    def close(self) -> None:
        self.conn.close()
        self.cur.close()
