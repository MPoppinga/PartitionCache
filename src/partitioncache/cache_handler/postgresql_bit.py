from logging import getLogger

import psycopg
from bitarray import bitarray
from psycopg import sql

from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy, AbstractCacheHandler_Query

logger = getLogger("PartitionCache")


class PostgreSQLBitCacheHandler(AbstractCacheHandler_Lazy, AbstractCacheHandler_Query):
    def __init__(self, db_name, db_host, db_user, db_password, db_port, db_table, bitsize) -> None:
        """
        Initialize the cache handler with the given db name."""
        self.db = psycopg.connect(dbname=db_name, host=db_host, password=db_password, port=db_port, user=db_user)
        self.tablename = db_table
        self.bitsize = bitsize + 1  # Add one to the bitsize to avoid off by one errors
        self.cursor = self.db.cursor()
        self.cursor.execute(
            sql.SQL("""CREATE TABLE IF NOT EXISTS {}(
            query_hash TEXT PRIMARY KEY,
            partition_keys BIT VARYING,
            partition_keys_count integer NOT NULL GENERATED ALWAYS AS (length(replace(partition_keys::text, '0','')) ) STORED
        );""").format(sql.Identifier(self.tablename + "_cache"))
        ) 
        self.cursor.execute(
            sql.SQL("""CREATE TABLE IF NOT EXISTS {}(
            query_hash TEXT NOT NULL PRIMARY KEY,
            query TEXT NOT NULL,
            last_seen TIMESTAMP NOT NULL DEFAULT now()
        );""").format(sql.Identifier(self.tablename + "_queries"))
        ) 
        self.db.commit()

    def close(self):
        self.cursor.close()
        self.db.close()

    def set_set(self, key: str, value: set[int] | set[str], settype=int) -> None:
        """
        Set the value of the given key in the cache.
        """
        val = bitarray(self.bitsize)

        for k in value:
            if isinstance(k, int):
                val[k] = 1
            elif isinstance(k, str):
                val[int(k)] = 1

        if not value:
            return

        if settype is str:
            raise ValueError("Only integer values are supported")

        self.cursor.execute(f"INSERT INTO {self.tablename}_cache VALUES (%s, %s)", (key, val.to01()))  # type: ignore
        self.db.commit()

    def set_query(self, key: str, querytext: str) -> None:
        self.cursor.execute(
            sql.SQL("""INSERT INTO {0} VALUES (%s, %s)
                            ON CONFLICT (query_hash) DO UPDATE SET query = %s, last_seen = now()""").format(
                sql.Identifier(self.tablename + "_queries")
            ),
            (key, querytext, querytext),
        )
        self.db.commit()

    def get(self, key: str, settype=int) -> set[int] | set[str] | None:
        if settype is str:
            raise ValueError("Only integer values are supported")

        self.cursor.execute(
            sql.SQL("SELECT partition_keys FROM {0} WHERE query_hash = %s").format(sql.Identifier(self.tablename + "_cache")),
            (key,),
        )
        result = self.cursor.fetchone()
        if result is None:
            return None
        bitarray_result = bitarray(result[0])

        return set(bitarray_result.search(bitarray("1")))

    def set_null(self, key: str) -> None:
        self.cursor.execute(
            sql.SQL("INSERT INTO {0} VALUES (%s, %s)").format(sql.Identifier(self.tablename + "_cache")),
            (key, None),
        )

    def is_null(self, key: str) -> bool:
        self.cursor.execute(
            sql.SQL("SELECT partition_keys FROM {0} WHERE query_hash = %s").format(sql.Identifier(self.tablename + "_cache")),
            (key,),
        )
        result = self.cursor.fetchone()
        if result == [None]:
            return True
        return False

    def exists(self, key: str) -> bool:
        """
        Returns True if the key exists in the cache, otherwise False.
        """
        self.cursor.execute(
            sql.SQL("SELECT partition_keys FROM {0} WHERE query_hash = %s").format(sql.Identifier(self.tablename + "_cache")),
            (key,),
        )
        x = self.cursor.fetchone()
        if x is None:
            return False
        return True

    def get_intersected(self, keys: set[str]) -> tuple[set[int] | set[str] | None, int]:
        # Check which exits
        query = sql.SQL("SELECT query_hash FROM {0} WHERE query_hash = ANY(%s) AND partition_keys IS NOT NULL").format(
            sql.Identifier(self.tablename + "_cache")
        )
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
        return sql.SQL("SELECT BIT_AND(partition_keys) FROM (SELECT partition_keys FROM {0} WHERE query_hash = ANY(%s)) AS selected").format(
            sql.Identifier(self.tablename + "_cache")
        )

    def get_intersected_sql_wk(self, keys) -> str:
        in_part = "', '".join(list(keys))
        return (
            sql.SQL("SELECT BIT_AND(partition_keys) AS bit_result FROM "
                    "(SELECT partition_keys FROM {0} WHERE query_hash IN ({1})) AS selected").format(
                sql.Identifier(self.tablename + "_cache"),
                sql.Literal(in_part),
            )
            .as_string()
        )

    def filter_existing_keys(self, keys: set) -> set:
        """
        Returns the set of keys that exist in the cache.
        """
        self.cursor.execute(
            sql.SQL("SELECT query_hash FROM {0} WHERE query_hash = ANY(%s) AND partition_keys IS NOT NULL").format(
                sql.Identifier(self.tablename + "_cache")
            ),
            [list(keys)],
        )
        query_hashkeys = set(x[0] for x in self.cursor.fetchall())
        logger.info(f"Found {len(query_hashkeys)} existing hashkeys")
        return query_hashkeys

    def get_intersected_lazy(self, keys: set[str]) -> tuple[str | None, int]:
        fitered_keys = self.filter_existing_keys(keys)

        if not fitered_keys:
            return None, 0

        intersect_sql_str = self.get_intersected_sql_wk(fitered_keys)

        r = (
            sql.SQL("""(
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
        """)
            .format(
                sql.SQL(intersect_sql_str),  # type: ignore
                sql.Literal(self.bitsize - 1),
            )
            .as_string()
        )

        return r, len(fitered_keys)

    def get_all_keys(self) -> list:
        self.cursor.execute(sql.SQL("SELECT query_hash FROM {}").format(sql.Identifier(self.tablename + "_cache")))
        set_keys = [x[0] for x in self.cursor.fetchall()]
        return set_keys

    def delete(self, key: str) -> None:
        self.cursor.execute(f"DELETE FROM {self.tablename}_cache WHERE query_hash = %s", (key,))  # type: ignore
        self.db.commit()
