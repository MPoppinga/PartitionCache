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

        # Verify metadata table creation was called
        assert mock_cursor.execute.call_count >= 2  # At least metadata and queries tables

        # Check that datatype constraint includes all supported types
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        metadata_calls = [call for call in execute_calls if "partition_metadata" in str(call)]

        assert len(metadata_calls) > 0, "Metadata table creation should be called"
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

        # Verify metadata table creation was called
        assert mock_cursor.execute.call_count >= 2

        # Check that bitsize column is included
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        metadata_calls = [call for call in execute_calls if "partition_metadata" in str(call)]

        assert len(metadata_calls) > 0, "Metadata table creation should be called"
        metadata_sql = str(metadata_calls[0])

        # Should contain bitsize column and integer constraint
        assert "bitsize" in metadata_sql
        assert "integer" in metadata_sql
        assert "DEFAULT NULL" in metadata_sql or "bitsize INTEGER" in metadata_sql

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

        # Verify metadata table creation was called
        assert mock_cursor.execute.call_count >= 2

        # Check that only integer datatype is supported
        execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        # Look for the specific roaringbit metadata table creation, not the general SQL function
        metadata_calls = [call for call in execute_calls if "partition_metadata" in str(call) and "CREATE TABLE IF NOT EXISTS" in str(call)]

        assert len(metadata_calls) > 0, "Metadata table creation should be called"
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

        # Should attempt to recreate metadata table
        assert mock_cursor.execute.call_count >= 2  # Check + Create calls

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
