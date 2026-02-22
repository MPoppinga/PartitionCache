"""
Unit tests for metadata table creation consolidation.

This module tests that all PostgreSQL cache handlers correctly create
metadata tables with appropriate datatype constraints.
"""

from unittest.mock import Mock, patch

import pytest


class TestMetadataTableCreation:
    """Test metadata table creation for all PostgreSQL cache handlers."""

    @pytest.fixture
    def mock_db_connection(self):
        """Mock database connection and cursor."""
        mock_db = Mock()
        mock_cursor = Mock()
        mock_db.cursor.return_value = mock_cursor
        mock_db.commit.return_value = None
        mock_db.rollback.return_value = None
        mock_cursor.execute.return_value = None
        mock_cursor.fetchone.return_value = None
        return mock_db, mock_cursor

    @patch('psycopg.connect')
    def test_postgresql_array_metadata_table_creation(self, mock_connect, mock_db_connection):
        """Test that PostgreSQL array handler creates metadata table with correct constraints."""
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

        mock_db, mock_cursor = mock_db_connection
        mock_connect.return_value = mock_db

        # Initialize handler
        handler = PostgreSQLArrayCacheHandler(
            db_name="test", db_host="localhost", db_user="test",
            db_password="test", db_port="5432", db_tableprefix="test"
        )

        # Init flow for PostgreSQLArrayCacheHandler:
        #   super().__init__() -> _recreate_metadata_table() (2 calls: metadata + queries tables)
        #   _load_sql_functions() (1 call: loads SQL file content)
        #   SELECT partitioncache_setup_array_extensions() (1 call)
        # Total: exactly 4 execute calls
        assert mock_cursor.execute.call_count == 4, (
            f"Expected exactly 4 execute calls (metadata table + queries table + SQL functions + array extensions), "
            f"got {mock_cursor.execute.call_count}"
        )

        # Check that datatype constraint includes all supported types
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        # Verify call order and content:
        # Call 0: CREATE TABLE partition_metadata with datatype constraint
        first_call_sql = str(execute_calls[0])
        assert "partition_metadata" in first_call_sql, (
            f"First call should create partition_metadata table, got: {first_call_sql}"
        )

        # Call 1: CREATE TABLE queries
        second_call_sql = str(execute_calls[1])
        assert "queries" in second_call_sql, (
            f"Second call should create queries table, got: {second_call_sql}"
        )
        assert "query_hash" in second_call_sql, "Queries table should have query_hash column"
        assert "partition_key" in second_call_sql, "Queries table should have partition_key column"

        # Call 3: SELECT partitioncache_setup_array_extensions()
        fourth_call_sql = str(execute_calls[3])
        assert "partitioncache_setup_array_extensions" in fourth_call_sql, (
            f"Fourth call should invoke partitioncache_setup_array_extensions, got: {fourth_call_sql}"
        )

        # Verify metadata table SQL contains all supported datatype constraints
        metadata_calls = [call for call in execute_calls if "partition_metadata" in str(call)]
        assert len(metadata_calls) >= 1, (
            "At least one SQL call should reference partition_metadata table creation"
        )
        metadata_sql = str(metadata_calls[0])

        # Should contain constraint for supported datatypes
        assert "integer" in metadata_sql
        assert "float" in metadata_sql
        assert "text" in metadata_sql
        assert "timestamp" in metadata_sql

        handler.close()

    @patch('psycopg.connect')
    def test_postgresql_bit_metadata_table_creation(self, mock_connect, mock_db_connection):
        """Test that PostgreSQL bit handler creates metadata table with bitsize column."""
        from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler

        mock_db, mock_cursor = mock_db_connection
        mock_connect.return_value = mock_db

        # Initialize handler
        handler = PostgreSQLBitCacheHandler(
            db_name="test", db_host="localhost", db_user="test",
            db_password="test", db_port="5432", db_tableprefix="test", bitsize=1000
        )

        # Init flow for PostgreSQLBitCacheHandler (overrides _recreate_metadata_table):
        #   super().__init__() -> _recreate_metadata_table():
        #     1. SELECT to_regclass(...) - check if table exists
        #     2. _load_sql_functions() - loads SQL file content
        #     3. CREATE TABLE partition_metadata (with bitsize column)
        #     4. CREATE TABLE queries
        #     5. _create_bitsize_trigger() - SELECT partitioncache_create_bitsize_trigger(...)
        # Total: exactly 5 execute calls
        assert mock_cursor.execute.call_count == 5, (
            f"Expected exactly 5 execute calls (regclass check + SQL functions + metadata table + queries table + bitsize trigger), "
            f"got {mock_cursor.execute.call_count}"
        )

        # Verify SQL call content by order
        execute_calls = mock_cursor.execute.call_args_list

        # Call 0: SELECT to_regclass(...) to check if table exists
        first_call_sql = str(execute_calls[0])
        assert "to_regclass" in first_call_sql, (
            f"First call should check table existence via to_regclass, got: {first_call_sql}"
        )

        # Call 2: CREATE TABLE partition_metadata with bitsize
        metadata_call_sql = str(execute_calls[2])
        assert "partition_metadata" in metadata_call_sql, (
            f"Third call should create partition_metadata table, got: {metadata_call_sql}"
        )
        assert "bitsize" in metadata_call_sql.lower(), (
            f"Metadata table CREATE should include bitsize column, got: {metadata_call_sql}"
        )
        assert "integer" in metadata_call_sql.lower(), (
            f"Metadata table should have integer datatype constraint, got: {metadata_call_sql}"
        )

        # Call 3: CREATE TABLE queries
        queries_call_sql = str(execute_calls[3])
        assert "queries" in queries_call_sql, (
            f"Fourth call should create queries table, got: {queries_call_sql}"
        )

        # Call 4: SELECT partitioncache_create_bitsize_trigger
        trigger_call_sql = str(execute_calls[4])
        assert "partitioncache_create_bitsize_trigger" in trigger_call_sql, (
            f"Fifth call should create bitsize trigger, got: {trigger_call_sql}"
        )

        handler.close()

    @patch('psycopg.connect')
    def test_postgresql_roaringbit_metadata_table_creation(self, mock_connect, mock_db_connection):
        """Test that PostgreSQL roaringbit handler creates metadata table with integer constraint."""
        from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

        mock_db, mock_cursor = mock_db_connection
        mock_connect.return_value = mock_db

        # Initialize handler
        handler = PostgreSQLRoaringBitCacheHandler(
            db_name="test", db_host="localhost", db_user="test",
            db_password="test", db_port="5432", db_tableprefix="test"
        )

        # Init flow for PostgreSQLRoaringBitCacheHandler (overrides _recreate_metadata_table):
        #   super().__init__() -> _recreate_metadata_table():
        #     1. _load_sql_functions() - loads SQL file content
        #     2. CREATE TABLE partition_metadata (integer-only constraint)
        #     3. CREATE TABLE queries
        #   Then in __init__():
        #     4. CREATE EXTENSION IF NOT EXISTS roaringbitmap
        # Total: exactly 4 execute calls
        assert mock_cursor.execute.call_count == 4, (
            f"Expected exactly 4 execute calls (SQL functions + metadata table + queries table + roaringbitmap extension), "
            f"got {mock_cursor.execute.call_count}"
        )

        # Check that only integer datatype is supported
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

        # Verify call order:
        # Call 1: CREATE TABLE partition_metadata
        assert "partition_metadata" in str(execute_calls[1]), (
            f"Second execute call should create partition_metadata table, got: {str(execute_calls[1])}"
        )

        # Call 2: CREATE TABLE queries
        assert "queries" in str(execute_calls[2]), (
            f"Third execute call should create queries table, got: {str(execute_calls[2])}"
        )

        # Call 3: CREATE EXTENSION roaringbitmap
        assert "roaringbitmap" in str(execute_calls[3]), (
            f"Fourth execute call should create roaringbitmap extension, got: {str(execute_calls[3])}"
        )

        # Look for the specific roaringbit metadata table creation, not the general SQL function
        metadata_calls = [call for call in execute_calls if "partition_metadata" in str(call) and "CREATE TABLE IF NOT EXISTS" in str(call)]

        assert len(metadata_calls) >= 1, (
            "At least one SQL call should be a CREATE TABLE for partition_metadata"
        )
        # Find the specific roaringbit metadata table creation with CHECK constraint
        roaringbit_metadata_call = None
        for call in metadata_calls:
            if "CHECK (datatype = 'integer')" in str(call):
                roaringbit_metadata_call = call
                break

        assert roaringbit_metadata_call is not None, "RoaringBit specific metadata table creation not found"
        metadata_sql = str(roaringbit_metadata_call)

        # Should contain only integer constraint
        assert "integer" in metadata_sql
        assert "CHECK (datatype = 'integer')" in metadata_sql

        handler.close()

    def test_get_supported_datatypes_methods(self):
        """Test that all handlers have correct get_supported_datatypes methods."""
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
        from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
        from partitioncache.cache_handler.postgresql_roaringbit import PostgreSQLRoaringBitCacheHandler

        # Test array handler supports all types
        array_types = PostgreSQLArrayCacheHandler.get_supported_datatypes()
        assert array_types == {"integer", "float", "text", "timestamp"}

        # Test bit handler supports only integer
        bit_types = PostgreSQLBitCacheHandler.get_supported_datatypes()
        assert bit_types == {"integer"}

        # Test roaringbit handler supports only integer
        roaringbit_types = PostgreSQLRoaringBitCacheHandler.get_supported_datatypes()
        assert roaringbit_types == {"integer"}

    @patch('psycopg.connect')
    def test_ensure_metadata_table_exists_functionality(self, mock_connect, mock_db_connection):
        """Test that _ensure_metadata_table_exists works correctly."""
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

        mock_db, mock_cursor = mock_db_connection
        mock_connect.return_value = mock_db

        handler = PostgreSQLArrayCacheHandler(
            db_name="test", db_host="localhost", db_user="test",
            db_password="test", db_port="5432", db_tableprefix="test"
        )

        # Reset mock to test _ensure_metadata_table_exists
        mock_cursor.reset_mock()

        # Test when table doesn't exist (exception thrown on first call only)
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # First call fails (SELECT check)
                raise Exception("relation does not exist")
            return None  # Subsequent calls succeed

        mock_cursor.execute.side_effect = side_effect

        # Call _ensure_metadata_table_exists
        handler._ensure_metadata_table_exists()

        # _ensure_metadata_table_exists flow when table doesn't exist:
        # 1 SELECT check (fails) + _recreate_metadata_table (2: metadata + queries) = 3 calls.
        # Using >= 3 because _recreate_metadata_table may trigger additional internal calls.
        assert mock_cursor.execute.call_count >= 3, (
            f"Expected at least 3 execute calls (SELECT check + metadata table + queries table), "
            f"got {mock_cursor.execute.call_count}"
        )

        # Verify the first call was the SELECT existence check
        first_call_sql = str(mock_cursor.execute.call_args_list[0][0][0])
        assert "SELECT" in first_call_sql and "partition_metadata" in first_call_sql, (
            f"First call should be a SELECT check on partition_metadata, got: {first_call_sql}"
        )

        handler.close()

    @patch('psycopg.connect')
    def test_metadata_table_creation_error_handling(self, mock_connect, mock_db_connection):
        """Test error handling during metadata table creation."""
        from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler

        mock_db, mock_cursor = mock_db_connection
        mock_connect.return_value = mock_db

        # Make metadata table creation fail after some successful calls
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:  # First call succeeds (for metadata table creation)
                raise Exception("Database error")
            return None

        mock_cursor.execute.side_effect = side_effect
        mock_db.rollback.return_value = None

        # Should raise exception during initialization
        with pytest.raises(Exception, match="Database error"):
            PostgreSQLArrayCacheHandler(
                db_name="test", db_host="localhost", db_user="test",
                db_password="test", db_port="5432", db_tableprefix="test"
            )

        # Should have attempted rollback
        assert mock_db.rollback.called

    def test_abstract_class_defines_required_methods(self):
        """Test that abstract class defines the required abstract methods."""

        from partitioncache.cache_handler.postgresql_abstract import PostgreSQLAbstractCacheHandler

        # Should have _recreate_metadata_table method
        assert hasattr(PostgreSQLAbstractCacheHandler, '_recreate_metadata_table')
        assert hasattr(PostgreSQLAbstractCacheHandler, '_ensure_metadata_table_exists')

        # get_supported_datatypes should be abstract
        assert hasattr(PostgreSQLAbstractCacheHandler, 'get_supported_datatypes')
        method = PostgreSQLAbstractCacheHandler.get_supported_datatypes
        assert getattr(method, '__isabstractmethod__', False), "get_supported_datatypes should be abstract"
