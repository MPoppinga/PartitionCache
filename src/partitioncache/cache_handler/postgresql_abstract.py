import secrets
import threading
from abc import abstractmethod
from collections.abc import Callable
from contextlib import contextmanager
from functools import wraps
from logging import getLogger
from pathlib import Path
from typing import ParamSpec, TypeVar

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus

from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

logger = getLogger("PartitionCache")

P = ParamSpec("P")
R = TypeVar("R")


def _ends_read_transaction(fn: Callable[P, R]) -> Callable[P, R]:  # noqa: UP047 -- PEP 695 syntax needs Python 3.12, package supports 3.10
    """Mark a read-only handler method so it leaves the connection idle. See _read_txn."""

    @wraps(fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with args[0]._read_txn():  # type: ignore[attr-defined]
            return fn(*args, **kwargs)

    return wrapper


class PostgreSQLAbstractCacheHandler(AbstractCacheHandler_Lazy):
    _instance = None
    _refcount = 0
    _cached_datatype: dict[str, str] = {}

    @classmethod
    def get_instance(cls, *args, **kwargs):
        # If externally threaded, skip singleton pattern and always create new instances
        if hasattr(threading.current_thread(), "ident") and threading.active_count() > 1:
            return cls(*args, **kwargs)

        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        cls._refcount += 1
        return cls._instance

    def __init__(self, db_name: str, db_host: str, db_user: str, db_password: str, db_port: str | int, db_tableprefix: str, timeout: str = "0") -> None:
        """
        Initialize the cache handler with the given db name.
        This handler supports multiple partition keys with datatypes: integer, float, text, timestamp.
        Creates distinct tables per partition key based on datatype.

        Args:
            timeout: Statement timeout in seconds (default: "0" for no timeout)
        """
        self.db = psycopg.connect(dbname=db_name, host=db_host, password=db_password, port=db_port, user=db_user, options=f"-c statement_timeout={int(timeout)*1000}" )
        self.tableprefix = db_tableprefix
        self.cursor = self.db.cursor()
        # Datatype cache must be instance-scoped: different tests/backends can reuse
        # the same partition key name (e.g. "zipcode") with different prefixes/datatypes.
        self._cached_datatype = {}

        # Create metadata tables with supported datatypes
        self._recreate_metadata_table(self.get_supported_datatypes())
        logger.info(f"CACHE_HANDLER {self.__class__.__name__}: Initialization complete for tableprefix={db_tableprefix}")

    def _recreate_metadata_table(self, supported_datatypes: set[str]) -> None:
        """
        Recreate metadata table if it was dropped during cleanup.

        Args:
            supported_datatypes: Set of supported datatype strings (e.g., {'integer', 'float', 'text', 'timestamp'})
        """
        try:
            # Create datatype constraint based on supported datatypes
            datatype_constraint = sql.SQL("CHECK (datatype IN ({}))").format(sql.SQL(", ").join(sql.Literal(dt) for dt in sorted(supported_datatypes)))

            # Create metadata table with datatype constraint
            self.cursor.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                    partition_key TEXT PRIMARY KEY,
                    datatype TEXT NOT NULL {1},
                    created_at TIMESTAMP DEFAULT now()
                );""").format(sql.Identifier(self.tableprefix + "_partition_metadata"), datatype_constraint)
            )
            logger.info("METADATA: Partition metadata table created successfully")

            # Create queries table
            self.cursor.execute(
                sql.SQL("""CREATE TABLE IF NOT EXISTS {0} (
                    query_hash TEXT NOT NULL,
                    query TEXT NOT NULL,
                    partition_key TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ok' CHECK (status IN ('ok', 'timeout', 'failed')),
                    last_seen TIMESTAMP NOT NULL DEFAULT now(),
                    PRIMARY KEY (query_hash, partition_key)
                );""").format(sql.Identifier(self.tableprefix + "_queries"))
            )

            self.db.commit()
        except Exception as e:
            logger.error(f"Failed to recreate metadata table: {e}")
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback after metadata table creation error: {rollback_error}")
            raise

    @contextmanager
    def _serialized_ddl(self):
        """Serialize this prefix's one-time DDL against other workers on the same prefix.

        Handler setup runs CREATE TABLE, ALTER TABLE and CREATE OR REPLACE FUNCTION against
        objects every worker shares. Four workers building handlers for one prefix while a
        fifth is already writing deadlock on the metadata table and on pg_proc; PostgreSQL
        breaks the cycle by killing one, which surfaces as a lost fragment.

        The lock is transaction-scoped, so it releases at the unit's own commit and nothing
        leaks on error. It has to be the FIRST statement of the transaction: a read taken
        first would hold its own lock already and invert the order between two workers.
        hashtext() is used rather than Python's hash(), which is salted per process and so
        gives each worker a different lock id for the same prefix.
        """
        if self.db.info.transaction_status != TransactionStatus.IDLE:
            logger.debug("DDL lock taken inside an open transaction; lock order is not guaranteed")
        self._take_ddl_lock()
        yield

    def _take_ddl_lock(self) -> None:
        """Take this prefix's transaction-scoped DDL advisory lock (see _serialized_ddl)."""
        self.cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (self.tableprefix,))

    def _load_sql_functions(self) -> None:
        """Load the SQL functions from postgresql_cache_handlers.sql.

        Usually called as the first statement inside _serialized_ddl, where the advisory
        lock belongs to the open transaction. A failed load aborts that transaction, and the
        rollback that clears it also releases the lock, so the lock is taken again before
        the DDL that follows. No savepoint is used: handlers may share one connection
        across threads, and another thread's commit destroys a savepoint in between.
        """
        sql_cache_file = Path(__file__).parent / "postgresql_cache_handlers.sql"
        if not sql_cache_file.exists():
            logger.warning(f"SQL cache handlers file not found at {sql_cache_file}")
            return

        in_transaction = self.db.info.transaction_status == TransactionStatus.INTRANS
        try:
            self.cursor.execute(sql_cache_file.read_text())  # type: ignore[arg-type]
            logger.debug("Successfully loaded SQL functions from cache handlers file")
        except Exception as e:
            logger.warning(f"Failed to load SQL functions (this is OK if they're already loaded): {e}")
            # Continuing on an aborted transaction makes every later statement fail with
            # "current transaction is aborted", which hides the real error behind it.
            try:
                self.db.rollback()
                if in_transaction:
                    self._take_ddl_lock()  # the rollback released the caller's DDL lock
            except Exception:
                logger.debug("rollback after SQL function load failed", exc_info=True)

    @contextmanager
    def _read_txn(self):
        """End a transaction that a read opened.

        These methods only SELECT and never commit, so without this the connection stays
        `idle in transaction` holding an AccessShareLock on whatever it touched until the
        handler is closed. That is enough to stall the processor: one worker's unfinished
        read of _partition_metadata blocks another worker's ALTER TABLE on the same table,
        and since there is no deadlock cycle PostgreSQL never breaks it.

        Only a transaction opened inside the block is ended. If the caller was already in
        one it owns the pending work, so ending it here could discard a write.
        """
        owns = self.db.info.transaction_status == TransactionStatus.IDLE
        try:
            yield
        finally:
            if owns and self.db.info.transaction_status != TransactionStatus.IDLE:
                self.db.rollback()  # a read changed nothing, so discard rather than commit

    def _ensure_metadata_table_exists(self) -> None:
        """Ensure the metadata table exists, create it if it doesn't."""
        try:
            # Test if metadata table exists by trying to query it
            with self._read_txn():
                self.cursor.execute(sql.SQL("SELECT 1 FROM {0} LIMIT 1").format(sql.Identifier(self.tableprefix + "_partition_metadata")))
        except Exception:
            # Metadata table doesn't exist, create it
            logger.info(f"Creating metadata table: {self.tableprefix}_partition_metadata")
            self._recreate_metadata_table(self.get_supported_datatypes())

    @_ends_read_transaction
    def _get_partition_datatype(self, partition_key: str) -> str | None:
        """Get the datatype for a partition key from metadata."""
        if partition_key in self._cached_datatype:
            return self._cached_datatype[partition_key]

        try:
            self.cursor.execute(
                sql.SQL("SELECT datatype FROM {0} WHERE partition_key = %s").format(sql.Identifier(self.tableprefix + "_partition_metadata")), (partition_key,)
            )
            result = self.cursor.fetchone()
            if result:
                self._cached_datatype[partition_key] = result[0]
            return result[0] if result else None
        except Exception as e:
            # Metadata table might not exist - rollback and return None
            try:
                if "does not exist" in str(e).lower() or "relation" in str(e).lower():
                    self.db.rollback()
                    return None
                else:
                    logger.error(f"Failed to get datatype for partition {partition_key}: {e}")
                    self.db.rollback()
                    return None
            except Exception:
                return None

    def close(self):
        cls = type(self)
        is_singleton_instance = cls._instance is self and cls._refcount > 0

        if is_singleton_instance:
            cls._refcount -= 1
            if cls._refcount > 0:
                return

        # Actually close the connection
        try:
            if self.cursor:
                self.cursor.close()
            if self.db:
                self.db.close()
        except Exception as e:
            logger.error(f"Error closing PostgreSQL connection: {e}")
        finally:
            if is_singleton_instance:
                cls._instance = None
                cls._refcount = 0

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
            # Rollback the transaction to prevent "current transaction is aborted" error
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
            return False

    @_ends_read_transaction
    def get_query(self, key: str, partition_key: str = "partition_key") -> str | None:
        """Retrieve the query text associated with the given key."""
        try:
            query_sql = sql.SQL("SELECT query FROM {0} WHERE query_hash = %s AND partition_key = %s").format(sql.Identifier(self.tableprefix + "_queries"))

            self.cursor.execute(query_sql, (key, partition_key))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.debug(f"Failed to get query for key {key}: {e}")
            return None

    @_ends_read_transaction
    def get_all_queries(self, partition_key: str) -> list[tuple[str, str]]:
        """Retrieve all query hash and text pairs for a specific partition."""
        try:
            query_sql = sql.SQL("SELECT query_hash, query FROM {0} WHERE partition_key = %s ORDER BY last_seen DESC").format(
                sql.Identifier(self.tableprefix + "_queries")
            )

            self.cursor.execute(query_sql, (partition_key,))
            return self.cursor.fetchall()
        except Exception as e:
            logger.debug(f"Failed to get all queries for partition {partition_key}: {e}")
            return []

    def _store_keys_lazily(
        self, table_name: str, key: str, query: str, partition_key: str, array_type: str = "INTEGER[]", value_expr: str = "$2", check: str = ""
    ) -> None:
        """Run a variant query and store its partition keys without moving them through Python.

        A single INSERT ... SELECT keeps the keys in the database, but PostgreSQL never plans parallel
        workers for a statement that writes, so the variant query ran single-threaded. A DO block runs the
        variant with EXECUTE ... INTO, which can use parallel workers, and the INSERT as a second statement,
        both on the server: the keys never reach the client.

        The keys stay a typed array (array_type, e.g. "INTEGER[]") and reach the INSERT as parameter $2
        (the cache key is $1); value_expr turns them into the column value, e.g. "rb_build($2)". NULL keys
        belong to no partition and are dropped, as on the non-lazy path. ARRAY(subquery) is '{}' for zero
        rows and never NULL, so an empty result cannot be mistaken for the NULL marker of set_null; a
        missing value raises instead.

        check is PL/pgSQL run between the two statements, with the keys in v_keys and the variables
        v_min / v_max (bigint) available, e.g. to reject keys a bit array cannot hold.
        """
        select = sql.SQL("SELECT ARRAY(SELECT {col} FROM ({query}) AS query_result WHERE {col} IS NOT NULL)").format(
            col=sql.Identifier(partition_key),
            query=sql.SQL(query),  # type: ignore[arg-type]
        )
        insert = (
            sql.SQL("INSERT INTO {table} (query_hash, partition_keys) VALUES ($1, ").format(table=sql.Identifier(table_name)).as_string(self.db)
            + value_expr
            + ") ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys"
        )
        select_literal = sql.Literal(select.as_string(self.db)).as_string(self.db)
        insert_literal = sql.Literal(insert).as_string(self.db)
        key_literal = sql.Literal(key).as_string(self.db)
        tag = "pc" + secrets.token_hex(6)
        while any(f"${tag}$" in literal for literal in (select_literal, insert_literal, key_literal)):
            tag = "pc" + secrets.token_hex(6)
        block = (
            f"DO ${tag}$ DECLARE v_keys {array_type}; v_min bigint; v_max bigint; BEGIN "
            f"EXECUTE {select_literal} INTO v_keys; "
            "IF v_keys IS NULL THEN RAISE EXCEPTION 'Lazy insertion query returned no key array'; END IF; "
            f"{check} "
            f"EXECUTE {insert_literal} USING {key_literal}, v_keys; "
            f"END ${tag}$"
        )
        # No parameters are passed, so psycopg does not interpret % in the block
        self.cursor.execute(block)  # type: ignore[arg-type]

    def set_null(self, key: str, partition_key: str = "partition_key") -> bool:
        """Set null value in partition-specific table."""
        try:
            # Ensure partition exists with default datatype
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                # For handlers that only support one datatype, create partition with that datatype
                supported_datatypes = self.get_supported_datatypes()
                if len(supported_datatypes) == 1:
                    datatype = next(iter(supported_datatypes))
                    self.register_partition_key(partition_key, datatype)
                else:
                    # For multi-datatype handlers, we can't determine datatype automatically
                    return False

            table_name = f"{self.tableprefix}_cache_{partition_key}"
            self.cursor.execute(
                sql.SQL(
                    "INSERT INTO {0} (query_hash, partition_keys) VALUES (%s, %s) ON CONFLICT (query_hash) DO UPDATE SET partition_keys = EXCLUDED.partition_keys"
                ).format(sql.Identifier(table_name)),
                (key, None),
            )
            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to set null for key {key} in partition {partition_key}: {e}")
            # Rollback the transaction to prevent "current transaction is aborted" error
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
            return False

    @_ends_read_transaction
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
        if isinstance(result, tuple | list) and len(result) > 0 and result[0] is None:
            return True
        return False

    def exists(self, key: str, partition_key: str = "partition_key", check_query: bool = False) -> bool:
        """Check if hash exists in partition-specific cache and optionally validate query status."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return False

            if not check_query:
                # Fast mode: Check cache table existence only
                return self._check_cache_exists(key, partition_key)
            else:
                # Query mode: Check query metadata first
                query_status = self.get_query_status(key, partition_key)
                if query_status is None:
                    return False  # No query -> False
                elif query_status == "ok":
                    # Query OK -> also check cache entry exists
                    return self._check_cache_exists(key, partition_key)
                else:  # timeout or failed
                    return True  # Query has error status -> True (no cache check)
        except Exception as e:
            # If table doesn't exist or other error, rollback to prevent transaction abort
            try:
                if "does not exist" in str(e).lower() or "relation" in str(e).lower():
                    # Table doesn't exist - rollback and return False
                    self.db.rollback()
                    return False
                else:
                    # Other error - log and rollback
                    logger.error(f"Failed to check existence for hash {key} in partition {partition_key}: {e}")
                    self.db.rollback()
                    return False
            except Exception:
                # Rollback failed - return False anyway
                return False

    @_ends_read_transaction
    def _check_cache_exists(self, key: str, partition_key: str) -> bool:
        """Helper method to check if a hash exists in the cache table."""
        table_name = f"{self.tableprefix}_cache_{partition_key}"
        self.cursor.execute(
            sql.SQL("""
                SELECT 1 FROM {cache_table}
                WHERE query_hash = %s
            """).format(cache_table=sql.Identifier(table_name)),
            (key,),
        )
        result = self.cursor.fetchone()
        return result is not None

    @_ends_read_transaction
    def filter_existing_keys(self, keys: set, partition_key: str = "partition_key", check_query: bool = False) -> set:
        """Return the set of keys that exist in the partition-specific cache."""
        try:
            datatype = self._get_partition_datatype(partition_key)
            if datatype is None:
                return set()

            if not check_query:
                # Fast mode: Check cache table existence only
                table_name = f"{self.tableprefix}_cache_{partition_key}"
                self.cursor.execute(
                    sql.SQL("""
                        SELECT query_hash FROM {cache_table}
                        WHERE query_hash = ANY(%s) AND partition_keys IS NOT NULL
                    """).format(cache_table=sql.Identifier(table_name)),
                    (list(keys),),
                )
                keys_set = {x[0] for x in self.cursor.fetchall()}
                logger.info(f"Found {len(keys_set)} existing hashkeys for partition {partition_key}")
                return keys_set
            else:
                # Query mode: Check each key's query status
                existing_keys = set()
                table_name = f"{self.tableprefix}_cache_{partition_key}"

                for key in keys:
                    query_status = self.get_query_status(key, partition_key)
                    if query_status is None:
                        continue  # No query -> exclude key
                    elif query_status == "ok":
                        # Query OK -> also check cache entry exists
                        self.cursor.execute(
                            sql.SQL("""
                                SELECT 1 FROM {cache_table}
                                WHERE query_hash = %s
                            """).format(cache_table=sql.Identifier(table_name)),
                            (key,),
                        )
                        if self.cursor.fetchone():
                            existing_keys.add(key)
                    else:  # timeout or failed
                        existing_keys.add(key)  # Query has error status -> include key

                logger.info(f"Found {len(existing_keys)} existing hashkeys for partition {partition_key}")
                return existing_keys
        except Exception as e:
            logger.error(f"Failed to filter existing keys in partition {partition_key}: {e}")
            return set()

    @_ends_read_transaction
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
            # Rollback the transaction to prevent "current transaction is aborted" error
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
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

            self._cached_datatype.pop(partition_key, None)  # Remove from cached datatypes

            self.db.commit()
            logger.info(f"Deleted partition {partition_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete partition {partition_key}: {e}")
            # Rollback the transaction to prevent "current transaction is aborted" error
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
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
            # Rollback the transaction to prevent "current transaction is aborted" error
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
            return 0

    @_ends_read_transaction
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

    def get_datatype(self, partition_key: str) -> str | None:
        """Get the datatype of the cache handler. If the partition key is not set, return None."""
        return self._get_partition_datatype(partition_key)

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype not in self.get_supported_datatypes():
            raise ValueError(f"Handler supports only {self.get_supported_datatypes()} datatypes, got: {datatype}")
        self._ensure_partition_table(partition_key, datatype, **kwargs)

    def set_query_status(self, key: str, partition_key: str = "partition_key", status: str = "ok") -> bool:
        """Set the status of a query in the queries table."""
        try:
            # Valid statuses based on the CHECK constraint
            valid_statuses = {"ok", "timeout", "failed"}
            if status not in valid_statuses:
                raise ValueError(f"Invalid status '{status}'. Must be one of: {valid_statuses}")

            query_sql = sql.SQL("UPDATE {0} SET status = %s, last_seen = now() WHERE query_hash = %s AND partition_key = %s").format(
                sql.Identifier(self.tableprefix + "_queries")
            )

            self.cursor.execute(query_sql, (status, key, partition_key))
            rows_affected = self.cursor.rowcount

            if rows_affected == 0:
                # Query doesn't exist in queries table, try to insert it with the status
                # This handles cases where set_query_status is called before set_query
                logger.warning(f"Query {key} not found in queries table, cannot set status to {status}")
                return False

            self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to set query status for key {key}: {e}")
            try:
                self.db.rollback()
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
            return False

    @_ends_read_transaction
    def get_query_status(self, key: str, partition_key: str = "partition_key") -> str | None:
        """Get the status of a query from the queries table."""
        try:
            query_sql = sql.SQL("SELECT status FROM {0} WHERE query_hash = %s AND partition_key = %s").format(sql.Identifier(self.tableprefix + "_queries"))

            self.cursor.execute(query_sql, (key, partition_key))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.debug(f"Failed to get query status for key {key}: {e}")
            return None

    @abstractmethod
    def _ensure_partition_table(self, partition_key: str, datatype: str, **kwargs)-> bool | tuple[bool, int]:
        """Ensure a partition table exists. Must be implemented by subclasses."""
        pass

    @classmethod
    @abstractmethod
    def get_supported_datatypes(cls) -> set[str]:
        """Return the set of supported datatypes for this cache handler."""
        pass
