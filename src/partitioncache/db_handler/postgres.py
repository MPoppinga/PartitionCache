"""
Handles the connection to a PostgreSQL database
"""

from logging import getLogger

import psycopg

from partitioncache.db_handler.abstract import AbstractDBHandler

logger = getLogger("PartitionCache")


def fetch_final_result_set(cursor: psycopg.Cursor) -> list:
    """Return the rows of the last result-producing statement of an executed (multi-)statement script.

    The temp-table integration methods (``TMP_TABLE_IN`` / ``TMP_TABLE_JOIN``) and the spatial filters emit
    multi-statement scripts of the form
    ``CREATE TEMPORARY TABLE ... ON COMMIT DROP; INSERT/ANALYZE ...; SELECT ...``.
    After ``cursor.execute(script)`` psycopg positions the cursor on the FIRST statement's result, so a plain
    ``fetchall()`` would read the ``CREATE TABLE`` (which produces no rows). This walks ``nextset()`` to the
    final result-producing statement and returns its rows. Works for single-statement queries too (one set).

    The whole script must be executed in a single ``cursor.execute`` call so that it runs in ONE transaction;
    ``ON COMMIT DROP`` then drops the temp table only after the final ``SELECT`` has produced its rows.

    Args:
        cursor: A psycopg cursor on which ``execute`` has already been called.

    Returns:
        The rows of the last result set, or an empty list if no statement produced rows.
    """
    rows: list = []
    while True:
        if cursor.description is not None:
            rows = cursor.fetchall()
        if not cursor.nextset():
            break
    return rows


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

        # Walk to the final result-producing statement: integration scripts (TMP_TABLE_IN/JOIN, spatial)
        # emit `CREATE TEMPORARY TABLE ... ON COMMIT DROP; ...; SELECT`, where the rows are in the LAST
        # result set, not the first. Returns first column of all rows if not empty.
        rows = fetch_final_result_set(self.cur)
        result = [row[0] for row in rows if row[0]]
        # Commit so the surrounding transaction ends: this fires ON COMMIT DROP for any temp table created
        # by the script (otherwise the table would linger for the whole session). Rows are already fetched.
        # Mirrors the commit in the MySQL/SQLite handlers.
        self.conn.commit()
        return result

    def close(self) -> None:
        self.conn.close()
        self.cur.close()
