from logging import getLogger

import psycopg
from bitarray import bitarray
from psycopg import sql

from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

logger = getLogger("PartitionCache")


class PostgreSQLBitCacheHandler(AbstractCacheHandler_Lazy):
    def __init__(self, db_name, db_host, db_user, db_password, db_port, db_table, bitsize) -> None:
        """
        Initialize the cache handler with the given db name."""
        self.db = psycopg.connect(dbname=db_name, host=db_host, password=db_password, port=db_port, user=db_user)
        self.tablename = db_table
        self.bitsize = bitsize + 1  # Add one to the bitsize to avoid off by one errors
        self.cursor = self.db.cursor()
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.tablename} (key TEXT PRIMARY KEY, value bit({self.bitsize}));")  # type: ignore

    def close(self):
        self.cursor.close()
        self.db.close()

    def set_set(self, key: str, value: set[int] | set[str], settype=int) -> None:
        """
        Set the value of the given key in the cache.
        """
        val = bitarray(self.bitsize)

        for k in value:
            val[k] = 1

        if not value:
            return

        if settype is str:
            raise ValueError("Only integer values are supported")

        self.cursor.execute(f"INSERT INTO {self.tablename} VALUES (%s, %s)", (key, val.to01()))  # type: ignore
        self.db.commit()

    def get(self, key: str, settype=int) -> set[int] | set[str] | None:
        if settype is str:
            raise ValueError("Only integer values are supported")

        self.cursor.execute(f"SELECT value FROM {self.tablename} WHERE key = %s", (key,))  # type: ignore
        result = self.cursor.fetchone()
        if result is None:
            return None
        bitarray_result = bitarray(result[0])

        return set(bitarray_result.search(bitarray("1")))

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
        query = sql.SQL("SELECT key FROM {0} WHERE key = ANY(%s) AND value IS NOT NULL").format(sql.Identifier(self.tablename))
        self.cursor.execute(query, (list(keys),))
        keys_set = set(x[0] for x in self.cursor.fetchall())

        if not keys_set:
            return None, 0

        q = self.get_intersected_sql()
        self.cursor.execute(q, (list(keys_set),))

        result = self.cursor.fetchone()
        if result is None:
            return None, 0
        r = set(bitarray(result[0]).search(bitarray("1")))
        return r, len(keys_set)

    def get_intersected_sql(self) -> sql.Composed:
        return sql.SQL("SELECT BIT_AND(value) FROM (SELECT value FROM {0} WHERE key = ANY(%s)) AS selected").format(sql.Identifier(self.tablename))

    def get_intersected_sql_wk(self, keys) -> str:
        return f"SELECT BIT_AND(value) AS bit_result FROM (SELECT value FROM {self.tablename} WHERE key IN ('{'\', \''.join(list(keys))}')) AS selected"

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

        intersect_sql_str = self.get_intersected_sql_wk(fitered_keys)

        r = sql.SQL("""(
       WITH bit_result AS (
            {0}
        ),
        numbered_bits AS (
            SELECT
                bit_result,
                generate_series(0, {1} - 1) AS bit_index
            FROM bit_result
        )
        SELECT unnest(array_agg(bit_index)) AS pocket_key
        FROM numbered_bits
        WHERE get_bit(bit_result, bit_index) = 1)
        """).format(
            sql.SQL(intersect_sql_str),  # type: ignore
            sql.Literal(self.bitsize - 1),
        )

        return r, len(fitered_keys)

    def get_all_keys(self) -> list:
        self.cursor.execute(sql.SQL("SELECT key FROM {}").format(sql.Identifier(self.tablename)))
        set_keys = [x[0] for x in self.cursor.fetchall()]
        return set_keys

    def delete(self, key: str) -> None:
        self.cursor.execute(f"DELETE FROM {self.tablename} WHERE key = %s", (key,))  # type: ignore
        self.db.commit()
