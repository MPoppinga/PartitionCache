import time
from logging import getLogger

import psycopg
from psycopg import sql

from partitioncache.cache_handler.abstract import AbstractCacheHandler

logger = getLogger("PartitionCache")


class PostgreSQLArrayCacheHandler(AbstractCacheHandler):
    def __init__(self, db_name, db_host, db_user, db_password, db_port, db_table) -> None:
        """
        Initialize the cache handler with the given db name."""
        self.db = psycopg.connect(dbname=db_name, host=db_host, password=db_password, port=db_port, user=db_user)
        self.tablename = db_table
        self.cursor = self.db.cursor()
        self.cursor.execute("CREATE EXTENSION IF NOT EXISTS intarray;")
        self.cursor.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (key TEXT PRIMARY KEY, value INTEGER[]);").format(sql.Identifier(self.tablename)))
        self.cursor.execute(
            sql.SQL("CREATE INDEX IF NOT EXISTS idx_large_sets_elements ON {} USING GIN (value gin__int_ops);").format(sql.Identifier(self.tablename))
        )
        self.cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS idx_large_sets_keys ON {} (key);").format(sql.Identifier(self.tablename)))

        self.allow_lazy = True

    def close(self):
        self.cursor.close()
        self.db.close()

    def set_set(self, key: str, value: set[int] | set[str], settype=int) -> None:
        """
        Set the value of the given key in the cache.
        """

        if not value:
            return

        if settype is str:
            raise ValueError("Only integer values are supported")

        val = list(value)
        self.cursor.execute(f"INSERT INTO {self.tablename} VALUES (%s, %s)", (key, val))  # type: ignore
        self.db.commit()

    def get(self, key: str, settype=int) -> set[int] | set[str] | None:
        if settype is str:
            raise ValueError("Only integer values are supported")

        self.cursor.execute(f"SELECT value FROM {self.tablename} WHERE key = %s", (key,))  # type: ignore
        result = self.cursor.fetchone()
        if result is None:
            return None
        return set(result[0])

    def set_null(self, key: str) -> None:
        self.cursor.execute(f"INSERT INTO {self.tablename} VALUES (%s, %s)", (key, None))  # type: ignore

    def is_null(self, key: str) -> bool:
        self.cursor.execute(f"SELECT value FROM {self.tablename} WHERE key = %s", (key,))  # type: ignore
        result = self.cursor.fetchone()
        if result == [None]:
            return True
        return False

    def exists(self, key: str) -> bool:
        """
        Returns True if the key exists in the cache, otherwise False.
        """
        self.cursor.execute(f"SELECT value FROM {self.tablename} WHERE key = %s", (key,))  # type: ignore
        x = self.cursor.fetchone()
        if x is None:
            return False
        return True

    def get_intersected(self, keys: set[str]) -> tuple[set[int] | set[str] | None, int]:
        # Check which exits

        filtered_keys = self.filter_existing_keys(keys)

        if not filtered_keys:
            return None, 0
        t = time.time()
        query: sql.Composed = self.get_intersected_sql(filtered_keys)
        logger.debug(query.as_string())
        self.cursor.execute(query)

        result = self.cursor.fetchone()

        if result is None:
            return None, 0
        logger.info(f"Getting Cache Query took {time.time() - t} seconds, got {len(result[0])} partition_keys using {len(filtered_keys)} hashkeys")

        return set(result[0]), len(filtered_keys)

    def filter_existing_keys(self, keys: set) -> set:
        """
        Returns the set of keys that exist in the cache.
        """
        self.cursor.execute(f"SELECT key FROM {self.tablename} WHERE key = ANY(%s) AND value IS NOT NULL", [list(keys)])  # type: ignore
        keys_set = set(x[0] for x in self.cursor.fetchall())
        logger.info(f"Found {len(keys_set)} existing hashkeys")
        return keys_set

    def get_intersected_lazy(self, keys: set[str]) -> tuple[sql.Composed | None, int]:
        fitered_keys = self.filter_existing_keys(keys)

        if not fitered_keys:
            return None, 0

        query = sql.SQL("SELECT unnest(({intersectsql})) as pocket_key").format(intersectsql=self.get_intersected_sql(fitered_keys))
        return query, len(fitered_keys)

    def get_intersected_sql(self, keys: set[str]) -> sql.Composed:
        """
        Returns the query for intersection of all sets in the cache that are associated with the given keys.
        """
        keys_list = list(keys)

        # Create the SELECT part of the query
        select_parts = [sql.SQL("({})").format(sql.SQL(" & ").join(sql.Identifier(f"k{i}", "value") for i in range(len(keys_list))))]

        # Create the FROM part of the query
        from_parts = [
            sql.SQL("(SELECT value FROM {} WHERE key = {}) AS {}").format(sql.Identifier(self.tablename), sql.Literal(key), sql.Identifier(f"k{i}"))
            for i, key in enumerate(keys_list)
        ]

        # Combine all parts into the final query
        query = sql.SQL("SELECT {select} FROM {from_}").format(select=sql.SQL(", ").join(select_parts), from_=sql.SQL(", ").join(from_parts))

        return query

    def get_all_keys(self) -> list:
        self.cursor.execute(sql.SQL("SELECT key FROM {}").format(sql.Identifier(self.tablename)))
        set_keys = [x[0] for x in self.cursor.fetchall()]
        return set_keys

    def delete(self, key: str) -> None:
        self.cursor.execute(f"DELETE FROM {self.tablename} WHERE key = %s", (key,))  # type: ignore
        self.db.commit()
