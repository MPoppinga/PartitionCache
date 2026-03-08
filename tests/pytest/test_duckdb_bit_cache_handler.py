"""
Comprehensive test suite for DuckDB bit cache handler.

Tests all required functionality for the DuckDB-based bit array cache implementation
following TDD principles.

Assertion strategy: Each test verifies both the behavioral outcome (return value)
AND the actual SQL/parameters passed to the database, ensuring correct table names,
SQL operations, and parameter binding.

Table naming convention: {table_prefix}_cache_{partition_key} for cache tables,
{table_prefix}_partition_metadata for metadata, {table_prefix}_queries for query metadata.
With table_prefix="test_cache" and partition_key="zipcode":
  - Cache table: test_cache_cache_zipcode
  - Metadata table: test_cache_partition_metadata
  - Queries table: test_cache_queries
"""

from unittest.mock import Mock, patch

import pytest

from partitioncache.cache_handler.abstract import AbstractCacheHandler_Lazy

# Constants matching the fixture's table_prefix="test_cache"
TABLE_PREFIX = "test_cache"
CACHE_TABLE = f"{TABLE_PREFIX}_cache_zipcode"
METADATA_TABLE = f"{TABLE_PREFIX}_partition_metadata"
QUERIES_TABLE = f"{TABLE_PREFIX}_queries"


def get_sql_from_call(call_obj) -> str:
    """Extract the SQL string from a mock call object.

    conn.execute is called as execute(sql_string, params_tuple).
    call.args[0] is always the SQL string.
    """
    return call_obj.args[0] if call_obj.args else ""


def get_params_from_call(call_obj) -> tuple | None:
    """Extract the params tuple from a mock call object.

    conn.execute is called as execute(sql_string, params_tuple).
    call.args[1] is the params tuple if present.
    """
    if call_obj.args and len(call_obj.args) > 1:
        return call_obj.args[1]
    return None


class TestDuckDBBitCacheHandler:
    """Test suite for DuckDB bit cache handler implementation."""

    @pytest.fixture
    def mock_duckdb_conn(self):
        """Mock DuckDB connection object."""
        conn = Mock()
        conn.execute = Mock()
        conn.fetchone = Mock()
        conn.fetchall = Mock()
        conn.commit = Mock()
        conn.rollback = Mock()
        conn.close = Mock()
        return conn

    @pytest.fixture
    def cache_handler(self, mock_duckdb_conn):
        """Create cache handler instance with mocked connection."""
        with patch("duckdb.connect", return_value=mock_duckdb_conn):
            from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler

            handler = DuckDBBitCacheHandler(database="test.duckdb", table_prefix="test_cache", bitsize=1000)
            handler.conn = mock_duckdb_conn
            return handler

    def test_inheritance(self, cache_handler):
        """Test that handler inherits from AbstractCacheHandler_Lazy."""
        assert isinstance(cache_handler, AbstractCacheHandler_Lazy)

    def test_supported_datatypes(self):
        """Test that only integer datatype is supported."""
        from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler

        supported = DuckDBBitCacheHandler.get_supported_datatypes()
        assert supported == {"integer"}
        assert DuckDBBitCacheHandler.supports_datatype("integer")
        assert not DuckDBBitCacheHandler.supports_datatype("text")
        assert not DuckDBBitCacheHandler.supports_datatype("float")

    def test_init_creates_tables(self, mock_duckdb_conn):
        """Test that initialization creates the partition_metadata and queries tables.

        _create_metadata_tables() issues two CREATE TABLE IF NOT EXISTS statements:
        1. {table_prefix}_partition_metadata - stores partition key metadata (datatype, bitsize)
        2. {table_prefix}_queries - stores query hash/text/status metadata
        """
        with patch("duckdb.connect", return_value=mock_duckdb_conn):
            from partitioncache.cache_handler.duckdb_bit import DuckDBBitCacheHandler

            DuckDBBitCacheHandler(database=":memory:", table_prefix="cache", bitsize=1000)

            calls = mock_duckdb_conn.execute.call_args_list
            create_table_calls = [call for call in calls if "CREATE TABLE" in get_sql_from_call(call)]
            # Exactly 2 CREATE TABLE calls: partition_metadata and queries
            assert len(create_table_calls) >= 2

            # Verify the metadata table has correct name and schema columns
            metadata_sql = get_sql_from_call(create_table_calls[0])
            assert "cache_partition_metadata" in metadata_sql
            assert "partition_key" in metadata_sql
            assert "datatype" in metadata_sql
            assert "bitsize" in metadata_sql

            # Verify the queries table has correct name and schema columns
            queries_sql = get_sql_from_call(create_table_calls[1])
            assert "cache_queries" in queries_sql
            assert "query_hash" in queries_sql
            assert "query" in queries_sql
            assert "partition_key" in queries_sql
            assert "status" in queries_sql

    def test_register_partition_key(self, cache_handler):
        """Test registering a partition key creates cache table and inserts metadata.

        register_partition_key calls _ensure_partition_table which:
        1. Creates a cache table: {prefix}_cache_{partition_key} with BITSTRING column
        2. Inserts into {prefix}_partition_metadata with (partition_key, 'integer', bitsize)
        """
        cache_handler.register_partition_key("zipcode", "integer", bitsize=100000)

        calls = cache_handler.conn.execute.call_args_list
        sql_strings = [get_sql_from_call(c) for c in calls]

        # Verify cache table creation SQL references correct table and has BITSTRING column
        create_calls = [s for s in sql_strings if "CREATE TABLE" in s and CACHE_TABLE in s]
        assert len(create_calls) == 1, f"Expected CREATE TABLE for {CACHE_TABLE}"
        create_sql = create_calls[0]
        assert "BITSTRING" in create_sql, "Cache table must use BITSTRING column type"
        assert "query_hash" in create_sql, "Cache table must have query_hash column"
        assert "partition_keys" in create_sql, "Cache table must have partition_keys column"

        # Verify metadata INSERT references correct table and passes correct params
        insert_calls = [c for c in calls if "INSERT INTO" in get_sql_from_call(c) and METADATA_TABLE in get_sql_from_call(c)]
        assert len(insert_calls) == 1, f"Expected INSERT INTO {METADATA_TABLE}"
        insert_params = get_params_from_call(insert_calls[0])
        assert insert_params == ("zipcode", 100000), (
            f"Metadata INSERT should pass (partition_key, bitsize) = ('zipcode', 100000), got {insert_params}"
        )

    def test_register_partition_key_wrong_datatype(self, cache_handler):
        """Test that registering non-integer datatype raises error."""
        with pytest.raises(ValueError, match="only.*integer"):
            cache_handler.register_partition_key("name", "text")

    def test_set_cache_basic(self, cache_handler):
        """Test setting cache with integer partition keys.

        set_cache flow:
        1. Queries _partition_metadata for bitsize (SELECT bitsize ... WHERE partition_key = ?)
        2. Builds a BITSTRING expression with set_bit() calls for each integer value
        3. INSERTs into cache table with (key, bitstring_expr, count) params
        4. INSERTs into queries table for existence tracking
        """
        cache_handler.conn.execute.return_value.fetchone.side_effect = [
            (10000,),  # _get_partition_bitsize returns 10000
            None,  # subsequent calls
        ]

        result = cache_handler.set_cache("hash123", {1, 5, 10, 100}, "zipcode")
        assert result is True

        calls = cache_handler.conn.execute.call_args_list

        # Verify bitsize lookup queries the metadata table with partition_key param.
        # Filter for SELECT (not CREATE TABLE which also mentions "bitsize" as a column name).
        bitsize_calls = [
            c for c in calls
            if "SELECT" in get_sql_from_call(c) and "bitsize" in get_sql_from_call(c).lower() and METADATA_TABLE in get_sql_from_call(c)
        ]
        assert len(bitsize_calls) >= 1, "Should query metadata table for bitsize"
        bitsize_params = get_params_from_call(bitsize_calls[0])
        assert bitsize_params == ("zipcode",), f"Bitsize query should use partition_key param, got {bitsize_params}"

        # Verify INSERT into cache table uses a BITSTRING literal
        cache_insert_calls = [c for c in calls if "INSERT INTO" in get_sql_from_call(c) and CACHE_TABLE in get_sql_from_call(c)]
        assert len(cache_insert_calls) >= 1, f"Should INSERT INTO {CACHE_TABLE}"
        insert_sql = get_sql_from_call(cache_insert_calls[0])
        # The SQL should contain a BITSTRING literal (built as Python string, not nested set_bit)
        assert "::BITSTRING" in insert_sql, "INSERT SQL should use a BITSTRING literal"
        # Verify the key "hash123" is passed as a parameter
        insert_params = get_params_from_call(cache_insert_calls[0])
        assert insert_params is not None
        assert "hash123" in insert_params, f"Cache INSERT params should contain key 'hash123', got {insert_params}"
        # Verify count (4) is a parameter for partition_keys_count
        assert 4 in insert_params, f"Cache INSERT params should contain count 4, got {insert_params}"

        # Verify queries table insert for existence tracking
        queries_insert_calls = [c for c in calls if "INSERT INTO" in get_sql_from_call(c) and QUERIES_TABLE in get_sql_from_call(c)]
        assert len(queries_insert_calls) >= 1, f"Should INSERT INTO {QUERIES_TABLE}"
        queries_params = get_params_from_call(queries_insert_calls[0])
        assert "hash123" in queries_params, "Queries INSERT should contain the cache key"
        assert "zipcode" in queries_params, "Queries INSERT should contain the partition key"

    def test_set_cache_exceeds_bitsize(self, cache_handler):
        """Test that setting values exceeding bitsize fails.

        When max(int_keys) >= existing_bitsize, _validate_and_prepare_bitsize returns None,
        causing set_cache to return False. Here 150 >= 100.
        """
        cache_handler.conn.execute.return_value.fetchone.return_value = (100,)

        result = cache_handler.set_cache("hash123", {1, 5, 150}, "zipcode")
        assert result is False

    def test_set_cache_non_integer_values(self, cache_handler):
        """Test that non-integer string values are rejected.

        _convert_to_integers tries int() conversion on each value;
        "abc"/"def" fail, so it returns None and set_cache returns False.
        """
        cache_handler.conn.execute.return_value.fetchone.return_value = (1000,)

        result = cache_handler.set_cache("hash123", {"abc", "def"}, "zipcode")
        assert result is False

    def test_get_cache(self, cache_handler):
        """Test retrieving cache values extracts bit positions from BITSTRING.

        get() builds a CTE-based SQL that:
        1. Reads partition_keys BITSTRING from {cache_table} WHERE query_hash = ?
        2. Generates range(bit_length(...)) as positions
        3. Filters positions WHERE get_bit(...) = 1
        Returns the set of positions with bits set.
        """
        cache_handler.conn.execute.return_value.fetchall.return_value = [
            (5,), (10,), (99,)  # Positions where bits are set
        ]

        result = cache_handler.get("hash123", "zipcode")
        assert result == {5, 10, 99}

        # Verify the SQL queries the correct cache table and uses get_bit for extraction
        calls = cache_handler.conn.execute.call_args_list
        get_call = calls[-1]  # The last execute call is the get query
        get_sql = get_sql_from_call(get_call)
        assert CACHE_TABLE in get_sql, f"GET query should reference {CACHE_TABLE}"
        assert "get_bit" in get_sql, "GET query should use get_bit() to extract set positions"
        assert "bit_length" in get_sql, "GET query should use bit_length() for range generation"
        assert "query_hash" in get_sql, "GET query should filter by query_hash"
        # Verify the key is passed as parameter (used twice in the CTE: once for range, once for get_bit)
        get_params = get_params_from_call(get_call)
        assert get_params == ("hash123", "hash123"), f"GET should pass key twice for CTE, got {get_params}"

    def test_get_cache_not_found(self, cache_handler):
        """Test retrieving non-existent cache entry returns None.

        When fetchall returns empty list, get() returns None.
        """
        cache_handler.conn.execute.return_value.fetchall.return_value = []

        result = cache_handler.get("nonexistent", "zipcode")
        assert result is None

        # Verify the correct table was queried with the right key
        calls = cache_handler.conn.execute.call_args_list
        get_call = calls[-1]
        get_sql = get_sql_from_call(get_call)
        assert CACHE_TABLE in get_sql, f"GET query should reference {CACHE_TABLE}"
        get_params = get_params_from_call(get_call)
        assert get_params == ("nonexistent", "nonexistent"), "GET should pass 'nonexistent' key"

    def test_get_cache_null_value(self, cache_handler):
        """Test retrieving null/missing cache value returns None.

        When the BITSTRING row doesn't exist or has no bits set, fetchall returns []
        and get() returns None.
        """
        cache_handler.conn.execute.return_value.fetchall.return_value = []

        result = cache_handler.get("null_hash", "zipcode")
        assert result is None

    def test_exists(self, cache_handler):
        """Test checking if cache entry exists queries correct table.

        exists() runs: SELECT 1 FROM {cache_table} WHERE query_hash = ?
        Returns True if fetchone returns a row, False if None.
        """
        # Mock that entry exists
        cache_handler.conn.execute.return_value.fetchone.return_value = (1,)
        assert cache_handler.exists("hash123", "zipcode") is True

        # Verify SQL targets the correct cache table with correct params
        exists_call = cache_handler.conn.execute.call_args_list[-1]
        exists_sql = get_sql_from_call(exists_call)
        assert "SELECT 1" in exists_sql, "exists() should use SELECT 1"
        assert CACHE_TABLE in exists_sql, f"exists() should query {CACHE_TABLE}"
        assert "query_hash" in exists_sql, "exists() should filter by query_hash"
        exists_params = get_params_from_call(exists_call)
        assert exists_params == ("hash123",), f"exists() should pass key as param, got {exists_params}"

        # Mock that entry doesn't exist
        cache_handler.conn.execute.return_value.fetchone.return_value = None
        assert cache_handler.exists("nonexistent", "zipcode") is False

    def test_exists_with_query_check(self, cache_handler):
        """Test exists with check_query=True checks query status first.

        When check_query=True, exists() first calls get_query_status() which queries
        the queries table. If status is 'ok', it then checks the cache table.
        If status is 'timeout' or 'failed', it returns True without checking cache.
        """
        cache_handler.conn.execute.return_value.fetchone.side_effect = [
            ("ok",),  # get_query_status returns 'ok' from queries table
            (1,),  # Cache exists check returns a row
        ]

        assert cache_handler.exists("hash123", "zipcode", check_query=True) is True

        calls = cache_handler.conn.execute.call_args_list
        # First call: query status check against queries table
        status_call = calls[-2]
        status_sql = get_sql_from_call(status_call)
        assert QUERIES_TABLE in status_sql, f"Status check should query {QUERIES_TABLE}"
        assert "status" in status_sql, "Status check should SELECT status column"
        status_params = get_params_from_call(status_call)
        assert "hash123" in status_params, "Status check should pass key as param"
        assert "zipcode" in status_params, "Status check should pass partition_key as param"

        # Second call: cache existence check against cache table
        cache_call = calls[-1]
        cache_sql = get_sql_from_call(cache_call)
        assert CACHE_TABLE in cache_sql, f"Cache check should query {CACHE_TABLE}"

    def test_delete(self, cache_handler):
        """Test deleting cache entry removes from both cache and queries tables.

        delete() issues two DELETE statements:
        1. DELETE FROM {cache_table} WHERE query_hash = ?
        2. DELETE FROM {queries_table} WHERE query_hash = ? AND partition_key = ?
        """
        cache_handler.conn.execute.return_value.rowcount = 1

        result = cache_handler.delete("hash123", "zipcode")
        assert result is True

        calls = cache_handler.conn.execute.call_args_list
        delete_calls = [c for c in calls if "DELETE FROM" in get_sql_from_call(c)]
        assert len(delete_calls) == 2, "delete() should issue 2 DELETE statements (cache + queries)"

        # First DELETE: from cache table
        cache_delete_sql = get_sql_from_call(delete_calls[0])
        assert CACHE_TABLE in cache_delete_sql, f"First DELETE should target {CACHE_TABLE}"
        cache_delete_params = get_params_from_call(delete_calls[0])
        assert cache_delete_params == ("hash123",), f"Cache DELETE should pass key, got {cache_delete_params}"

        # Second DELETE: from queries table
        queries_delete_sql = get_sql_from_call(delete_calls[1])
        assert QUERIES_TABLE in queries_delete_sql, f"Second DELETE should target {QUERIES_TABLE}"
        queries_delete_params = get_params_from_call(delete_calls[1])
        assert queries_delete_params == ("hash123", "zipcode"), (
            f"Queries DELETE should pass (key, partition_key), got {queries_delete_params}"
        )

    def test_set_null(self, cache_handler):
        """Test setting null value inserts NULL BITSTRING into cache table.

        set_null() INSERTs with partition_keys=NULL and partition_keys_count=NULL
        using ON CONFLICT ... DO UPDATE for upsert behavior.
        """
        cache_handler.conn.execute.return_value.fetchone.return_value = ("integer", 1000)

        result = cache_handler.set_null("timeout_hash", "zipcode")
        assert result is True

        calls = cache_handler.conn.execute.call_args_list
        # Find the INSERT with NULL into the cache table
        null_insert_calls = [c for c in calls if "INSERT INTO" in get_sql_from_call(c) and CACHE_TABLE in get_sql_from_call(c)]
        assert len(null_insert_calls) >= 1, f"set_null should INSERT INTO {CACHE_TABLE}"
        insert_sql = get_sql_from_call(null_insert_calls[0])
        assert "NULL" in insert_sql, "set_null INSERT should set partition_keys to NULL"
        assert "ON CONFLICT" in insert_sql, "set_null should use ON CONFLICT for upsert"
        insert_params = get_params_from_call(null_insert_calls[0])
        assert insert_params == ("timeout_hash",), f"set_null should pass key as param, got {insert_params}"

    def test_is_null(self, cache_handler):
        """Test checking if value is null queries partition_keys column.

        is_null() runs: SELECT partition_keys FROM {cache_table} WHERE query_hash = ?
        Returns True when the row exists but partition_keys is None.
        Returns False when partition_keys has a value.
        """
        cache_handler.conn.execute.return_value.fetchone.return_value = (None,)
        assert cache_handler.is_null("timeout_hash", "zipcode") is True

        # Verify the SQL reads partition_keys from the correct table
        is_null_call = cache_handler.conn.execute.call_args_list[-1]
        is_null_sql = get_sql_from_call(is_null_call)
        assert CACHE_TABLE in is_null_sql, f"is_null should query {CACHE_TABLE}"
        assert "partition_keys" in is_null_sql, "is_null should SELECT partition_keys column"
        is_null_params = get_params_from_call(is_null_call)
        assert is_null_params == ("timeout_hash",), f"is_null should pass key as param, got {is_null_params}"

        # Non-null value
        cache_handler.conn.execute.return_value.fetchone.return_value = ("0101010",)
        assert cache_handler.is_null("normal_hash", "zipcode") is False

    def test_get_intersected_single_key(self, cache_handler):
        """Test intersection with single key delegates to get().

        When only one key exists, get_intersected calls self.get(key, partition_key)
        directly instead of using BIT_AND aggregate.
        """
        cache_handler.filter_existing_keys = Mock(return_value={"hash123"})
        cache_handler.get = Mock(return_value={5, 10})

        result, count = cache_handler.get_intersected({"hash123"}, "zipcode")
        assert result == {5, 10}
        assert count == 1

        # Verify get was called with the single key and partition_key
        cache_handler.get.assert_called_once_with("hash123", "zipcode")

    def test_get_intersected_multiple_keys(self, cache_handler):
        """Test intersection with multiple keys uses DuckDB BIT_AND aggregate.

        For multiple keys, get_intersected builds a CTE with:
        1. bit_and(partition_keys) aggregate over rows WHERE query_hash IN (?,?,?)
        2. Extracts positions WHERE get_bit(intersection_bits, pos) = 1
        """
        cache_handler.filter_existing_keys = Mock(return_value={"hash1", "hash2", "hash3"})

        cache_handler.conn.execute.return_value.fetchall.return_value = [
            (5,)  # Only position 5 is in intersection of all three
        ]

        result, count = cache_handler.get_intersected({"hash1", "hash2", "hash3"}, "zipcode")
        assert result == {5}
        assert count == 3

        # Verify the SQL uses BIT_AND aggregate on the correct cache table
        intersect_call = cache_handler.conn.execute.call_args_list[-1]
        intersect_sql = get_sql_from_call(intersect_call)
        assert "bit_and" in intersect_sql.lower(), "Multi-key intersection should use bit_and() aggregate"
        assert CACHE_TABLE in intersect_sql, f"Intersection query should reference {CACHE_TABLE}"
        assert "get_bit" in intersect_sql, "Intersection should use get_bit() to extract positions"
        assert "query_hash IN" in intersect_sql, "Intersection should filter by query_hash IN (...)"
        # Verify 3 placeholder params for the 3 keys
        intersect_params = get_params_from_call(intersect_call)
        assert len(intersect_params) == 3, f"Should pass 3 key params, got {len(intersect_params)}"
        assert set(intersect_params) == {"hash1", "hash2", "hash3"}, (
            f"Params should contain all 3 keys, got {intersect_params}"
        )

    def test_get_intersected_no_keys_exist(self, cache_handler):
        """Test intersection when no keys exist returns (None, 0)."""
        cache_handler.filter_existing_keys = Mock(return_value=set())

        result, count = cache_handler.get_intersected({"hash1", "hash2"}, "zipcode")
        assert result is None
        assert count == 0

    def test_filter_existing_keys(self, cache_handler):
        """Test filtering keys checks cache table for existing query_hash values.

        filter_existing_keys runs:
        SELECT query_hash FROM {cache_table} WHERE query_hash IN (?,?,?)
        Returns only the keys that have rows in the cache table.
        """
        cache_handler.conn.execute.return_value.fetchall.return_value = [("hash1",), ("hash3",)]

        result = cache_handler.filter_existing_keys({"hash1", "hash2", "hash3"}, "zipcode")
        assert result == {"hash1", "hash3"}

        # Verify the SQL queries the correct table with IN clause
        filter_call = cache_handler.conn.execute.call_args_list[-1]
        filter_sql = get_sql_from_call(filter_call)
        assert CACHE_TABLE in filter_sql, f"filter_existing_keys should query {CACHE_TABLE}"
        assert "query_hash" in filter_sql, "filter should SELECT query_hash"
        assert "IN" in filter_sql, "filter should use IN clause"
        filter_params = get_params_from_call(filter_call)
        # 3 placeholder params for the 3 input keys
        assert len(filter_params) == 3, f"Should pass 3 key params, got {len(filter_params)}"
        assert set(filter_params) == {"hash1", "hash2", "hash3"}, f"Params should contain all input keys, got {filter_params}"

    def test_get_all_keys(self, cache_handler):
        """Test retrieving all keys queries cache table ordered by created_at DESC.

        get_all_keys runs:
        SELECT query_hash FROM {cache_table} ORDER BY created_at DESC
        Returns a list of keys.
        """
        cache_handler.conn.execute.return_value.fetchall.return_value = [("hash1",), ("hash2",), ("hash3",)]

        result = cache_handler.get_all_keys("zipcode")
        assert result == ["hash1", "hash2", "hash3"]

        # Verify the SQL queries the correct table
        all_keys_call = cache_handler.conn.execute.call_args_list[-1]
        all_keys_sql = get_sql_from_call(all_keys_call)
        assert CACHE_TABLE in all_keys_sql, f"get_all_keys should query {CACHE_TABLE}"
        assert "query_hash" in all_keys_sql, "get_all_keys should SELECT query_hash"
        assert "ORDER BY" in all_keys_sql, "get_all_keys should ORDER BY created_at"

    def test_set_query(self, cache_handler):
        """Test storing query metadata in the queries table.

        set_query runs INSERT INTO {queries_table} (query_hash, partition_key, query)
        VALUES (?, ?, ?) with ON CONFLICT ... DO UPDATE for upsert.
        """
        query_text = "SELECT * FROM pois WHERE zipcode = 12345"

        result = cache_handler.set_query("hash123", query_text, "zipcode")
        assert result is True

        calls = cache_handler.conn.execute.call_args_list
        insert_calls = [c for c in calls if "INSERT INTO" in get_sql_from_call(c) and QUERIES_TABLE in get_sql_from_call(c)]
        assert len(insert_calls) == 1, f"set_query should INSERT INTO {QUERIES_TABLE}"

        insert_sql = get_sql_from_call(insert_calls[0])
        assert "ON CONFLICT" in insert_sql, "set_query should use ON CONFLICT for upsert"
        assert "query_hash" in insert_sql
        assert "partition_key" in insert_sql
        assert "query" in insert_sql

        insert_params = get_params_from_call(insert_calls[0])
        assert insert_params == ("hash123", "zipcode", query_text), (
            f"set_query should pass (key, partition_key, query_text), got {insert_params}"
        )

    def test_get_query(self, cache_handler):
        """Test retrieving query text from queries table.

        get_query runs:
        SELECT query FROM {queries_table} WHERE query_hash = ? AND partition_key = ?
        """
        query_text = "SELECT * FROM pois WHERE zipcode = 12345"
        cache_handler.conn.execute.return_value.fetchone.return_value = (query_text,)

        result = cache_handler.get_query("hash123", "zipcode")
        assert result == query_text

        # Verify SQL and params
        get_call = cache_handler.conn.execute.call_args_list[-1]
        get_sql = get_sql_from_call(get_call)
        assert QUERIES_TABLE in get_sql, f"get_query should query {QUERIES_TABLE}"
        assert "query" in get_sql, "get_query should SELECT query column"
        assert "query_hash" in get_sql and "partition_key" in get_sql, "get_query should filter by both query_hash and partition_key"
        get_params = get_params_from_call(get_call)
        assert get_params == ("hash123", "zipcode"), f"get_query should pass (key, partition_key), got {get_params}"

    def test_get_all_queries(self, cache_handler):
        """Test retrieving all queries for a partition.

        get_all_queries runs:
        SELECT query_hash, query FROM {queries_table}
        WHERE partition_key = ? ORDER BY last_seen DESC
        """
        cache_handler.conn.execute.return_value.fetchall.return_value = [
            ("hash1", "SELECT 1"),
            ("hash2", "SELECT 2"),
        ]

        result = cache_handler.get_all_queries("zipcode")
        assert result == [("hash1", "SELECT 1"), ("hash2", "SELECT 2")]

        # Verify SQL and params
        all_q_call = cache_handler.conn.execute.call_args_list[-1]
        all_q_sql = get_sql_from_call(all_q_call)
        assert QUERIES_TABLE in all_q_sql, f"get_all_queries should query {QUERIES_TABLE}"
        assert "query_hash" in all_q_sql, "Should SELECT query_hash"
        assert "query" in all_q_sql, "Should SELECT query column"
        assert "ORDER BY" in all_q_sql, "Should ORDER BY last_seen"
        all_q_params = get_params_from_call(all_q_call)
        assert all_q_params == ("zipcode",), f"get_all_queries should pass partition_key, got {all_q_params}"

    def test_set_query_status(self, cache_handler):
        """Test setting query status updates the queries table.

        set_query_status runs:
        UPDATE {queries_table} SET status = ?, last_seen = now()
        WHERE query_hash = ? AND partition_key = ?
        """
        result = cache_handler.set_query_status("hash123", "zipcode", "timeout")
        assert result is True

        calls = cache_handler.conn.execute.call_args_list
        update_calls = [c for c in calls if "UPDATE" in get_sql_from_call(c) and QUERIES_TABLE in get_sql_from_call(c)]
        assert len(update_calls) == 1, f"set_query_status should UPDATE {QUERIES_TABLE}"

        update_sql = get_sql_from_call(update_calls[0])
        assert "status" in update_sql, "UPDATE should SET status column"
        assert "last_seen" in update_sql, "UPDATE should SET last_seen timestamp"
        update_params = get_params_from_call(update_calls[0])
        # Parameters are (status, key, partition_key) per the implementation
        assert update_params == ("timeout", "hash123", "zipcode"), (
            f"set_query_status should pass (status, key, partition_key), got {update_params}"
        )

    def test_get_query_status(self, cache_handler):
        """Test retrieving query status from queries table.

        get_query_status runs:
        SELECT status FROM {queries_table} WHERE query_hash = ? AND partition_key = ?
        """
        cache_handler.conn.execute.return_value.fetchone.return_value = ("timeout",)

        result = cache_handler.get_query_status("hash123", "zipcode")
        assert result == "timeout"

        # Verify SQL and params
        status_call = cache_handler.conn.execute.call_args_list[-1]
        status_sql = get_sql_from_call(status_call)
        assert QUERIES_TABLE in status_sql, f"get_query_status should query {QUERIES_TABLE}"
        assert "status" in status_sql, "Should SELECT status column"
        status_params = get_params_from_call(status_call)
        assert status_params == ("hash123", "zipcode"), (
            f"get_query_status should pass (key, partition_key), got {status_params}"
        )

    def test_get_datatype(self, cache_handler):
        """Test retrieving partition datatype queries the metadata table.

        get_datatype delegates to _get_partition_datatype which runs:
        SELECT datatype FROM {metadata_table} WHERE partition_key = ?
        """
        cache_handler.conn.execute.return_value.fetchone.return_value = ("integer",)

        result = cache_handler.get_datatype("zipcode")
        assert result == "integer"

        # Verify SQL and params
        dt_call = cache_handler.conn.execute.call_args_list[-1]
        dt_sql = get_sql_from_call(dt_call)
        assert METADATA_TABLE in dt_sql, f"get_datatype should query {METADATA_TABLE}"
        assert "datatype" in dt_sql, "Should SELECT datatype column"
        dt_params = get_params_from_call(dt_call)
        assert dt_params == ("zipcode",), f"get_datatype should pass partition_key, got {dt_params}"

    def test_get_partition_keys(self, cache_handler):
        """Test retrieving all partition keys queries the metadata table.

        get_partition_keys runs:
        SELECT partition_key, datatype FROM {metadata_table} ORDER BY created_at
        """
        cache_handler.conn.execute.return_value.fetchall.return_value = [
            ("zipcode", "integer"),
            ("city_id", "integer"),
        ]

        result = cache_handler.get_partition_keys()
        assert result == [("zipcode", "integer"), ("city_id", "integer")]

        # Verify SQL targets metadata table
        pk_call = cache_handler.conn.execute.call_args_list[-1]
        pk_sql = get_sql_from_call(pk_call)
        assert METADATA_TABLE in pk_sql, f"get_partition_keys should query {METADATA_TABLE}"
        assert "partition_key" in pk_sql, "Should SELECT partition_key"
        assert "datatype" in pk_sql, "Should SELECT datatype"
        assert "ORDER BY" in pk_sql, "Should ORDER BY created_at"

    def test_get_intersected_lazy(self, cache_handler):
        """Test lazy intersection SQL generation for multiple keys.

        For 2+ keys, generates a CTE with:
        1. bit_and(partition_keys) aggregate for query_hash IN ('key1','key2')
        2. UNNEST(range(bit_length(...))) to generate positions
        3. get_bit(...) = 1 to filter set positions
        4. SELECT pos AS {partition_key} for use in subqueries
        """
        cache_handler.filter_existing_keys = Mock(return_value={"hash1", "hash2"})

        sql_query, count = cache_handler.get_intersected_lazy({"hash1", "hash2"}, "zipcode")

        assert sql_query is not None
        assert count == 2

        sql_lower = sql_query.lower()
        # Verify the generated SQL contains required DuckDB bitstring operations
        assert "bit_and" in sql_lower, "Multi-key lazy SQL should use bit_and() aggregate"
        assert "get_bit" in sql_lower, "Lazy SQL should use get_bit() for position extraction"
        assert "unnest" in sql_lower, "Lazy SQL should use UNNEST for position iteration"
        assert "bit_length" in sql_lower, "Lazy SQL should use bit_length() for range"
        # Verify correct table reference
        assert CACHE_TABLE.lower() in sql_lower, f"Lazy SQL should reference {CACHE_TABLE}"
        # Verify the output column is named after the partition_key for subquery use
        assert "as zipcode" in sql_lower, "Lazy SQL output column should be aliased as partition_key name"
        # Verify both keys appear in the IN clause (embedded directly, not parameterized)
        assert "hash1" in sql_query, "Lazy SQL should contain hash1 key"
        assert "hash2" in sql_query, "Lazy SQL should contain hash2 key"

    def test_get_intersected_lazy_single_key(self, cache_handler):
        """Test lazy intersection SQL generation for a single key.

        For 1 key, generates direct extraction without bit_and aggregate:
        SELECT pos AS {partition_key} FROM positions WHERE get_bit(...) = 1
        """
        cache_handler.filter_existing_keys = Mock(return_value={"hash_single"})

        sql_query, count = cache_handler.get_intersected_lazy({"hash_single"}, "zipcode")

        assert sql_query is not None
        assert count == 1

        sql_lower = sql_query.lower()
        # Single key should NOT use bit_and (no aggregation needed)
        assert "bit_and" not in sql_lower, "Single-key lazy SQL should not use bit_and()"
        # Should still use get_bit for position extraction
        assert "get_bit" in sql_lower, "Single-key lazy SQL should use get_bit()"
        assert CACHE_TABLE.lower() in sql_lower, f"Lazy SQL should reference {CACHE_TABLE}"
        assert "hash_single" in sql_query, "Lazy SQL should contain the key"
        assert "as zipcode" in sql_lower, "Output column should be aliased as partition_key name"

    def test_close(self, cache_handler):
        """Test closing the connection."""
        cache_handler.close()
        cache_handler.conn.close.assert_called_once()

    def test_set_entry_high_level(self, cache_handler):
        """Test high-level set_entry method orchestrates set_cache and set_query."""
        cache_handler.exists = Mock(return_value=False)
        cache_handler.set_cache = Mock(return_value=True)
        cache_handler.set_query = Mock(return_value=True)

        result = cache_handler.set_entry("hash123", {1, 2, 3}, "SELECT * FROM test", "zipcode")

        assert result is True
        cache_handler.set_cache.assert_called_once_with("hash123", {1, 2, 3}, "zipcode")
        cache_handler.set_query.assert_called_once_with("hash123", "SELECT * FROM test", "zipcode")

    def test_error_handling(self, cache_handler):
        """Test error handling returns False and does not raise.

        When conn.execute raises an exception, set_cache catches it and returns False.
        """
        cache_handler.conn.execute.side_effect = Exception("Database error")

        result = cache_handler.set_cache("hash123", {1, 2, 3}, "zipcode")
        assert result is False

    def test_concurrent_partition_creation(self, cache_handler):
        """Test handling concurrent partition table creation delegates to _ensure_partition_table."""
        cache_handler._ensure_partition_table = Mock(return_value=True)

        cache_handler.register_partition_key("zipcode", "integer", bitsize=10000)

        cache_handler._ensure_partition_table.assert_called_once_with("zipcode", 10000)

    def test_bitsize_validation(self, cache_handler):
        """Test bitsize is correctly passed through to metadata INSERT.

        register_partition_key calls _ensure_partition_table which INSERTs
        the bitsize value into the partition_metadata table.
        """
        cache_handler.register_partition_key("test1", "integer", bitsize=1)
        cache_handler.register_partition_key("test2", "integer", bitsize=1000000)

        calls = cache_handler.conn.execute.call_args_list

        # Find all metadata INSERT calls and verify bitsize values are passed correctly
        metadata_inserts = [c for c in calls if "INSERT INTO" in get_sql_from_call(c) and METADATA_TABLE in get_sql_from_call(c)]
        assert len(metadata_inserts) >= 2, "Should have at least 2 metadata INSERTs (one per partition)"

        # Collect all bitsize values from the params
        bitsize_values = set()
        for c in metadata_inserts:
            params = get_params_from_call(c)
            if params:
                # Params are (partition_key, bitsize) per _ensure_partition_table
                bitsize_values.add(params[1])
        assert 1 in bitsize_values, "Bitsize 1 should be stored in metadata"
        assert 1000000 in bitsize_values, "Bitsize 1000000 should be stored in metadata"
