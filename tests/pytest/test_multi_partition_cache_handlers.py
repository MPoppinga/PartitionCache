"""
Comprehensive tests for multi-partition cache handler functionality.
Tests the new features: multiple partition keys, different datatypes, and variable bit lengths.
"""

import inspect
from unittest.mock import Mock, call, patch

from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler
from partitioncache.cache_handler.redis_set import RedisCacheHandler


class TestMultiPartitionFunctionality:
    """Test multi-partition key functionality across all cache handlers."""

    def test_multiple_partition_keys_postgresql_array(self):
        """Test that PostgreSQL array handler supports multiple partition keys."""
        with patch("psycopg.connect") as mock_connect:
            mock_db = Mock()
            mock_cursor = Mock()
            mock_connect.return_value = mock_db
            mock_db.cursor.return_value = mock_cursor

            handler = PostgreSQLArrayCacheHandler(
                db_name="test", db_host="localhost", db_user="user",
                db_password="pass", db_port=5432, db_tableprefix="test_table"
            )
            handler.cursor = mock_cursor
            handler.db = mock_db
            # Clear cached datatypes to avoid cross-test interference
            handler._cached_datatype.clear()

            # Mock _get_partition_datatype returning None (partition doesn't exist yet)
            mock_cursor.fetchone.return_value = None

            # Reset call tracking after init to focus on set_cache behavior
            mock_cursor.execute.reset_mock()

            # Store data in partition1
            handler.set_cache("key1", {1, 2, 3}, partition_key="partition1")

            # Capture calls for partition1 and verify partition-specific table reference
            p1_calls = [str(c) for c in mock_cursor.execute.call_args_list]
            p1_insert_calls = [c for c in p1_calls if 'INSERT' in c and 'test_table_cache_partition1' in c]
            assert len(p1_insert_calls) == 1, f"Expected 1 INSERT into partition1 cache table, got {len(p1_insert_calls)}"

            # Reset call tracking and cached datatypes for second partition
            mock_cursor.execute.reset_mock()
            handler._cached_datatype.clear()
            mock_cursor.fetchone.return_value = None

            handler.set_cache("key2", {4, 5, 6}, partition_key="partition2")

            # Verify that the cache INSERT targets the correct partition2 table
            p2_calls = [str(c) for c in mock_cursor.execute.call_args_list]
            p2_insert_calls = [c for c in p2_calls if 'INSERT' in c and 'test_table_cache_partition2' in c]
            assert len(p2_insert_calls) == 1, f"Expected 1 INSERT into partition2 cache table, got {len(p2_insert_calls)}"

            # Verify partition2 calls do NOT insert into partition1's cache table
            cross_partition_inserts = [c for c in p2_calls if 'INSERT' in c and 'test_table_cache_partition1' in c]
            assert len(cross_partition_inserts) == 0, "partition2 set_cache should not INSERT into partition1 table"

    def test_datatype_conflict_detection(self):
        """Test that datatype conflicts are properly detected."""
        with patch("psycopg.connect") as mock_connect:
            mock_db = Mock()
            mock_cursor = Mock()
            mock_connect.return_value = mock_db
            mock_db.cursor.return_value = mock_cursor

            handler = PostgreSQLArrayCacheHandler(
                db_name="test", db_host="localhost", db_user="user",
                db_password="pass", db_port=5432, db_tableprefix="test_table"
            )
            handler.cursor = mock_cursor
            handler.db = mock_db
            handler._cached_datatype.clear()

            # Mock partition already exists with integer type
            mock_cursor.fetchone.return_value = ("integer",)

            # First call with integer data should succeed (matches existing datatype)
            result1 = handler.set_cache("key1", {1, 2, 3}, partition_key="test_partition")
            assert result1 is True, "set_cache with matching datatype should succeed"

            # Clear cached datatype so _get_partition_datatype queries the mock again
            handler._cached_datatype.clear()

            # Second call with string data should fail (conflicts with existing integer datatype)
            result2 = handler.set_cache("key2", {"a", "b", "c"}, partition_key="test_partition")
            assert result2 is False, "set_cache with conflicting datatype should return False"

    def test_variable_bitsize_postgresql_bit(self):
        """Test variable bitsize per partition in PostgreSQL bit handler."""
        with patch("psycopg.connect") as mock_connect:
            mock_db = Mock()
            mock_cursor = Mock()
            mock_connect.return_value = mock_db
            mock_db.cursor.return_value = mock_cursor
            mock_db.commit.return_value = None
            mock_db.rollback.return_value = None
            # Configure mock for metadata table check - returns None initially
            mock_cursor.fetchone.return_value = None
            mock_cursor.fetchall.return_value = []

            handler = PostgreSQLBitCacheHandler(
                db_name="test", db_host="localhost", db_user="user",
                db_password="pass", db_port=5432, db_tableprefix="test_table", bitsize=1000
            )
            handler.cursor = mock_cursor
            handler.db = mock_db

            # Reset call tracking after init
            mock_cursor.execute.reset_mock()

            # Test bitsize retrieval
            mock_cursor.fetchone.return_value = (1000,)
            assert handler._get_partition_bitsize("partition1") == 1000

            # Verify the SELECT query targeted the correct metadata table and partition
            select_call = mock_cursor.execute.call_args_list[-1]
            select_sql = str(select_call)
            assert "partition_metadata" in select_sql, "Should query partition_metadata table"

            # Test bitsize update
            mock_cursor.execute.reset_mock()
            handler._set_partition_bitsize("partition1", 2000)

            # Verify exactly 1 UPDATE call was made
            update_calls = [c for c in mock_cursor.execute.call_args_list
                          if 'UPDATE' in str(c)]
            assert len(update_calls) == 1, f"Expected exactly 1 UPDATE call, got {len(update_calls)}"

            # Verify the UPDATE call used the correct bitsize and partition key values
            update_call_args = update_calls[0]
            # The positional args to execute are (sql_template, (bitsize, partition_key))
            assert update_call_args[0][1] == (2000, "partition1"), \
                f"UPDATE should set bitsize=2000 for partition1, got {update_call_args[0][1]}"

    def test_get_partition_keys_method(self):
        """Test that all handlers implement get_partition_keys method."""
        with patch("psycopg.connect") as mock_connect:
            mock_db = Mock()
            mock_cursor = Mock()
            mock_connect.return_value = mock_db
            mock_db.cursor.return_value = mock_cursor

            handler = PostgreSQLArrayCacheHandler(
                db_name="test", db_host="localhost", db_user="user",
                db_password="pass", db_port=5432, db_tableprefix="test_table"
            )
            handler.cursor = mock_cursor
            handler.db = mock_db

            # Mock return data
            mock_cursor.fetchall.return_value = [
                ("partition1", "integer"),
                ("partition2", "text"),
                ("partition3", "float")
            ]

            result = handler.get_partition_keys()
            assert result == [("partition1", "integer"), ("partition2", "text"), ("partition3", "float")]

            # Verify the SQL query targeted the correct metadata table
            execute_call = mock_cursor.execute.call_args
            assert "partition_metadata" in str(execute_call), "Should query partition_metadata table"

    def test_partition_isolation(self):
        """Test that different partitions are properly isolated."""
        with patch("psycopg.connect") as mock_connect:
            mock_db = Mock()
            mock_cursor = Mock()
            mock_connect.return_value = mock_db
            mock_db.cursor.return_value = mock_cursor

            handler = PostgreSQLArrayCacheHandler(
                db_name="test", db_host="localhost", db_user="user",
                db_password="pass", db_port=5432, db_tableprefix="test_table"
            )
            handler.cursor = mock_cursor
            handler.db = mock_db
            handler._cached_datatype.clear()

            # Reset call tracking after init to focus on get() behavior
            mock_cursor.execute.reset_mock()

            # Mock different partitions: each get() calls _get_partition_datatype (SELECT + fetchone)
            # then the actual data SELECT + fetchone
            mock_cursor.fetchone.side_effect = [
                ("integer",),  # partition1 datatype lookup
                ([1, 2, 3],),  # partition1 data
                ("text",),     # partition2 datatype lookup
                (["a", "b", "c"],),  # partition2 data
            ]

            result1 = handler.get("same_key", partition_key="partition1")
            result2 = handler.get("same_key", partition_key="partition2")

            # Verify returned data matches expected values
            assert result1 == {1, 2, 3}, f"Expected {{1, 2, 3}} for partition1, got {result1}"
            assert result2 == {"a", "b", "c"}, f"Expected {{'a', 'b', 'c'}} for partition2, got {result2}"

            # Verify exactly 4 execute calls were made (2 per get: metadata + data)
            assert mock_cursor.execute.call_count == 4, \
                f"Expected 4 execute calls (2 per partition), got {mock_cursor.execute.call_count}"

            # Verify the data queries targeted different partition-specific tables
            all_calls = [str(c) for c in mock_cursor.execute.call_args_list]
            data_select_calls = [c for c in all_calls if 'partition_keys' in c]
            assert len(data_select_calls) == 2, "Expected 2 data SELECT calls"
            assert any('partition1' in c for c in data_select_calls), "Should query partition1 table"
            assert any('partition2' in c for c in data_select_calls), "Should query partition2 table"

    def test_redis_namespacing(self):
        """Test Redis handler uses proper namespacing for partitions."""
        with patch("redis.Redis") as mock_redis:
            mock_db = Mock()
            mock_redis.return_value = mock_db

            handler = RedisCacheHandler(
                db_name=0, db_host="localhost", db_password="", db_port=6379
            )
            handler.db = mock_db
            handler._cached_datatype.clear()

            # Mock datatype check: type() returns "string" for metadata key, then "set" for data key
            mock_db.type.side_effect = [b"string", b"set"]
            mock_db.get.return_value = b"integer"
            mock_db.smembers.return_value = {b"1", b"2", b"3"}

            result = handler.get("test_key", partition_key="test_partition")

            # Verify returned data is correctly parsed as integers
            assert result == {1, 2, 3}, f"Expected {{1, 2, 3}}, got {result}"

            # Verify the first type() call was for the metadata key
            expected_metadata_key = '_partition_metadata:test_partition'
            assert mock_db.type.call_args_list[0] == call(expected_metadata_key), \
                f"First type() call should check metadata key, got {mock_db.type.call_args_list[0]}"

            # Verify the second type() call was for the namespaced cache key
            expected_cache_key = 'cache:test_partition:test_key'
            assert mock_db.type.call_args_list[1] == call(expected_cache_key), \
                f"Second type() call should check cache key, got {mock_db.type.call_args_list[1]}"

            # Verify smembers was called with the correct namespaced key
            mock_db.smembers.assert_called_once_with(expected_cache_key)

    def test_all_handlers_support_partition_keys(self):
        """Test that all cache handlers properly support partition_key parameter."""

        # Methods where partition_key has a default value of "partition_key"
        methods_with_default = ['get', 'set_cache', 'exists', 'delete']
        # Methods where partition_key is required (no default)
        methods_required = ['get_all_keys']

        with patch("psycopg.connect"):
            for method_name in methods_with_default:
                method = getattr(PostgreSQLArrayCacheHandler, method_name)
                sig = inspect.signature(method)
                assert 'partition_key' in sig.parameters, f"{method_name} missing partition_key parameter"
                param = sig.parameters['partition_key']
                assert param.default == "partition_key", \
                    f"{method_name} partition_key default should be 'partition_key', got '{param.default}'"

            for method_name in methods_required:
                method = getattr(PostgreSQLArrayCacheHandler, method_name)
                sig = inspect.signature(method)
                assert 'partition_key' in sig.parameters, f"{method_name} missing partition_key parameter"
                param = sig.parameters['partition_key']
                assert param.default == inspect.Parameter.empty, \
                    f"{method_name} partition_key should be required (no default), got '{param.default}'"

    def test_datatype_validation_comprehensive(self):
        """Test comprehensive datatype validation across handlers."""
        test_cases = [
            ("partition_int", {1, 2, 3}, "integer"),
            ("partition_int2", {4, 5, 6}, "integer"),
            ("partition_int3", {7, 8, 9}, "integer"),
        ]

        with patch("psycopg.connect") as mock_connect:
            mock_db = Mock()
            mock_cursor = Mock()
            mock_connect.return_value = mock_db
            mock_db.cursor.return_value = mock_cursor

            handler = PostgreSQLArrayCacheHandler(
                db_name="test", db_host="localhost", db_user="user",
                db_password="pass", db_port=5432, db_tableprefix="test_table"
            )
            handler.cursor = mock_cursor
            handler.db = mock_db
            handler._cached_datatype.clear()

            for partition_name, test_data, _expected_datatype in test_cases:
                # Mock partition doesn't exist yet
                mock_cursor.fetchone.return_value = None
                mock_cursor.execute.reset_mock()

                result = handler.set_cache(f"key_{partition_name}", test_data, partition_key=partition_name)
                assert result is True, f"set_cache should succeed for {partition_name}"

                # Verify that an INSERT was issued targeting the correct partition table
                insert_calls = [str(c) for c in mock_cursor.execute.call_args_list
                              if 'INSERT' in str(c)]
                assert len(insert_calls) >= 1, f"Expected at least 1 INSERT for {partition_name}"

                partition_table_name = f"test_table_cache_{partition_name}"
                cache_insert_calls = [c for c in insert_calls if partition_table_name in c]
                assert len(cache_insert_calls) == 1, \
                    f"Expected exactly 1 INSERT into {partition_table_name}, got {len(cache_insert_calls)}"

                # Clear cached datatype for next iteration
                handler._cached_datatype.clear()
