"""
Handles the connection to a DuckDB database
"""

import duckdb

from partitioncache.db_handler.abstract import AbstractDBHandler


class DuckDBHandler(AbstractDBHandler):
    def __init__(self, database: str = ":memory:", timeout: int = 0) -> None:
        """
        Initialize DuckDB connection.

        Args:
            database: Path to database file or ":memory:" for in-memory database
            timeout: Query timeout in seconds (0 means no timeout)
        """
        self.conn = duckdb.connect(database)
        if timeout > 0:
            self.conn.execute(f"SET statement_timeout = '{timeout}s'")

    def execute(self, query) -> list:
        """Execute a query and return the first column of all rows."""
        result = self.conn.execute(query)

        # Get all rows
        rows = result.fetchall()

        # Return first column of all rows if not empty
        if not rows:
            return []
        return [row[0] for row in rows if row[0] is not None]

    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
