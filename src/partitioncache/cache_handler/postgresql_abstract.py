import psycopg
from psycopg import sql
from functools import cache
from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy
from logging import getLogger

logger = getLogger("PartitionCache")


class PostgreSQLAbstractCacheHandler(AbstractCacheHandler_Lazy):
    _instance = None
    _refcount = 0

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        cls._refcount += 1
        return cls._instance

    def __init__(self, db_name: str, db_host: str, db_user: str, db_password: str, db_port: str | int, db_table: str) -> None:
        """
        Initialize the cache handler with the given db name.
        This handler supports multiple partition keys with datatypes: integer, float, text, timestamp.
        Creates distinct tables per partition key based on datatype.
        """
        self.db = psycopg.connect(dbname=db_name, host=db_host, password=db_password, port=db_port, user=db_user)
        self.tableprefix = db_table
        self.cursor = self.db.cursor()

    @cache
    def _get_partition_datatype(self, partition_key: str) -> str | None:
        """Get the datatype for a partition key from metadata."""
        self.cursor.execute(
            sql.SQL("SELECT datatype FROM {0} WHERE partition_key = %s").format(sql.Identifier(self.tableprefix + "_partition_metadata")), (partition_key,)
        )
        result = self.cursor.fetchone()
        return result[0] if result else None

    def close(self):
        self._refcount -= 1
        if self._refcount <= 0:
            # Actually close the connection
            try:
                if self.cursor:
                    self.cursor.close()
                if self.db:
                    self.db.close()
            except Exception as e:
                logger.error(f"Error closing PostgreSQL connection: {e}")
            self._instance = None
            self._refcount = 0

    def set_query(self, key: str, querytext: str, partition_key: str = "partition_key") -> bool:
        """Store a query in the cache associated with the given key."""
        try:
            query_sql = sql.SQL(
                "INSERT INTO {0} (query_hash, partition_key, query) VALUES (%s, %s, %s) "
                "ON CONFLICT (query_hash, partition_key) DO UPDATE SET "
                "query = EXCLUDED.query, last_seen = now()"
            ).format(sql.Identifier(self.tableprefix + "_queries"))

            self.cursor.execute(query_sql, (key, partition_key, querytext))
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to set query for key {key}: {e}")
            return False

    def set_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """Set null value in partition-specific table."""
        try:
            # Ensure partition exists with default datatype
            self._get_partition_datatype(partition_key)

            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(
                sql.SQL("INSERT INTO {0} (query_hash, value) VALUES (%s, %s) ON CONFLICT (query_hash) DO UPDATE SET value = EXCLUDED.value").format(
                    sql.Identifier(table_name)
                ),
                (key, None),
            )
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to set null for key {key} in partition {partition_key}: {e}")
            return False

    def is_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """Check if key has null value in partition-specific table."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return False

        table_name = f"{self.tableprefix}_cache_{partition_key}"
        self.cursor.execute(
            sql.SQL("SELECT partition_keys FROM {0} WHERE query_hash = %s").format(sql.Identifier(table_name)),
            (key,),
        )
        result = self.cursor.fetchone()
        if result == [None] or result == (None,) or result is None:
            return True
        if isinstance(result, (tuple, list)) and len(result) > 0 and result[0] is None:
            return True
        return False

    def exists(self, key: str, partition_key: str = "partition_key") -> bool:
        """Check if key exists in partition-specific cache."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return False

            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(sql.SQL("SELECT 1 FROM {0} WHERE query_hash = %s").format(sql.Identifier(table_name)), (key,))
            result = self.cursor.fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"Failed to check existence for key {key} in partition {partition_key}: {e}")
            return False

    def get_all_keys(self, partition_key: str) -> list:
        """Get all keys for a specific partition key."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return []

        table_name = f"{self.tableprefix}_cache_{partition_key}"
        self.cursor.execute(sql.SQL("SELECT query_hash FROM {}").format(sql.Identifier(table_name)))
        return [x[0] for x in self.cursor.fetchall()]

    def delete(self, key: str, partition_key: str = "partition_key") -> bool:
        """Delete from partition-specific table."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return False

            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(sql.SQL("DELETE FROM {0} WHERE query_hash = %s").format(sql.Identifier(table_name)), (key,))

            # Also delete from queries table
            self.cursor.execute(
                sql.SQL("DELETE FROM {0} WHERE partition_key = %s AND query_hash = %s").format(sql.Identifier(self.tableprefix + "_queries")),
                (partition_key, key),
            )

            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to delete key {key} from partition {partition_key}: {e}")
            return False

    def delete_partition(self, partition_key: str) -> bool:
        """Delete an entire partition and all its data."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                logger.warning(f"Partition {partition_key} does not exist")
                return False

            # Drop the partition-specific table
            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(sql.SQL("DROP TABLE IF EXISTS {0}").format(sql.Identifier(table_name)))

            # Delete from queries table
            self.cursor.execute(sql.SQL("DELETE FROM {0} WHERE partition_key = %s").format(sql.Identifier(self.tableprefix + "_queries")), (partition_key,))

            # Delete from metadata table
            self.cursor.execute(
                sql.SQL("DELETE FROM {0} WHERE partition_key = %s").format(sql.Identifier(self.tableprefix + "_partition_metadata")), (partition_key,)
            )

            self._get_partition_datatype.cache_clear()

            self.db.commit()
            logger.info(f"Deleted partition {partition_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete partition {partition_key}: {e}")
            return False

    def prune_old_queries(self, days_old: int = 30) -> int:
        """Remove queries that haven't been seen for specified days."""
        try:
            # Get queries older than specified days
            self.cursor.execute(
                sql.SQL("SELECT partition_key, query_hash FROM {0} WHERE last_seen < now() - interval '%s days'").format(
                    sql.Identifier(self.tableprefix + "_queries")
                ),
                (days_old,),
            )

            old_queries = self.cursor.fetchall()
            removed_count = 0

            # Delete from partition-specific cache tables
            for partition_key, query_hash in old_queries:
                datatype = self._get_partition_datatype(partition_key)
                if datatype:
                    table_name = f"{self.tableprefix}_cache_{partition_key}"
                    self.cursor.execute(sql.SQL("DELETE FROM {0} WHERE query_hash = %s").format(sql.Identifier(table_name)), (query_hash,))
                    removed_count += 1

            # Delete from queries table
            self.cursor.execute(
                sql.SQL("DELETE FROM {0} WHERE last_seen < now() - interval '%s days'").format(sql.Identifier(self.tableprefix + "_queries")), (days_old,)
            )

            self.db.commit()
            logger.info(f"Pruned {removed_count} old queries (older than {days_old} days)")
            return removed_count
        except Exception as e:
            logger.error(f"Failed to prune old queries: {e}")
            return 0

    def get_partition_keys(self) -> list[tuple[str, str]]:
        """Get all partition keys and their datatypes."""
        try:
            self.cursor.execute(
                sql.SQL("SELECT partition_key, datatype FROM {} ORDER BY partition_key").format(sql.Identifier(self.tableprefix + "_partition_metadata"))
            )
            return [(row[0], row[1]) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get partition keys: {e}")
            return []
