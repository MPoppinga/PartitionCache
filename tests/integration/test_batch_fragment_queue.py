"""Test batch fragment queue upsert functionality."""
import os

import pytest
from psycopg.rows import dict_row

from partitioncache.queue_handler import get_queue_handler


def test_batch_fragment_queue_upsert(db_connection):
    """Test the non_blocking_fragment_queue_batch_upsert function."""
    connection = db_connection

    # Create queue handler to ensure tables exist
    queue_handler = get_queue_handler("postgresql")

    # Get the actual table names being used
    fragment_queue_table = queue_handler.fragment_queue_table

    with connection.cursor(row_factory=dict_row) as cursor:
        # First, ensure the batch function exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = 'public'
                AND p.proname = 'non_blocking_fragment_queue_batch_upsert'
            );
        """)
        function_exists = cursor.fetchone()['exists']

        if not function_exists:
            # Load the helper SQL file to create the function
            sql_file = os.path.join(
                os.path.dirname(__file__),
                '../../src/partitioncache/queue_handler/postgresql_queue_helper.sql'
            )
            with open(sql_file) as f:
                cursor.execute(f.read())
            connection.commit()

        # Clear the fragment queue
        cursor.execute(f"DELETE FROM {fragment_queue_table};")
        connection.commit()

        # Check what batch functions exist
        cursor.execute("""
            SELECT p.proname, pg_get_function_identity_arguments(p.oid) as args
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = 'public'
            AND p.proname LIKE 'non_blocking_fragment_queue_batch%';
        """)
        print("Available batch functions:")
        for row in cursor.fetchall():
            print(f"  {row['proname']}({row['args']})")

        # Test 1: Insert new items
        cursor.execute(f"""
            SELECT * FROM non_blocking_fragment_queue_batch_upsert(
                ARRAY['hash1', 'hash2', 'hash3']::TEXT[],
                ARRAY['city_id', 'city_id', 'region_id']::TEXT[],
                ARRAY['integer', 'integer', 'text']::TEXT[],
                ARRAY['SELECT * FROM table1', 'SELECT * FROM table2', 'SELECT * FROM table3']::TEXT[],
                ARRAY[1, 2, 3]::INT[],
                '{fragment_queue_table}'
            ) ORDER BY idx;
        """)
        results = cursor.fetchall()

        assert len(results) == 3
        assert all(r['status'] == 'inserted' for r in results)
        assert results[0]['hash'] == 'hash1'
        assert results[1]['hash'] == 'hash2'
        assert results[2]['hash'] == 'hash3'

        # Verify items were actually inserted
        cursor.execute(f"SELECT * FROM {fragment_queue_table} ORDER BY hash;")
        queue_items = cursor.fetchall()
        assert len(queue_items) == 3
        assert queue_items[0]['priority'] == 1
        assert queue_items[1]['priority'] == 2
        assert queue_items[2]['priority'] == 3

        # Test 2: Update existing items (should increase priority)
        cursor.execute(f"""
            SELECT * FROM non_blocking_fragment_queue_batch_upsert(
                ARRAY['hash1', 'hash2', 'hash4']::TEXT[],
                ARRAY['city_id', 'city_id', 'city_id']::TEXT[],
                ARRAY['integer', 'integer', 'integer']::TEXT[],
                ARRAY['SELECT * FROM table1', 'SELECT * FROM table2', 'SELECT * FROM table4']::TEXT[],
                ARRAY[5, 5, 5]::INT[],
                '{fragment_queue_table}'
            ) ORDER BY idx;
        """)
        results = cursor.fetchall()

        assert len(results) == 3
        assert results[0]['status'] == 'updated'
        assert results[1]['status'] == 'updated'
        assert results[2]['status'] == 'inserted'

        # Verify priorities were updated
        cursor.execute(f"SELECT * FROM {fragment_queue_table} WHERE hash IN ('hash1', 'hash2') ORDER BY hash;")
        updated_items = cursor.fetchall()
        assert updated_items[0]['priority'] == 6  # 1 + 5
        assert updated_items[1]['priority'] == 7  # 2 + 5

        # Test 3: Test with NULL priorities (should default to 1)
        cursor.execute(f"""
            SELECT * FROM non_blocking_fragment_queue_batch_upsert(
                ARRAY['hash5']::TEXT[],
                ARRAY['city_id']::TEXT[],
                ARRAY['integer']::TEXT[],
                ARRAY['SELECT * FROM table5']::TEXT[],
                NULL::INT[],
                '{fragment_queue_table}'
            );
        """)
        results = cursor.fetchall()
        assert results[0]['status'] == 'inserted'

        cursor.execute(f"SELECT priority FROM {fragment_queue_table} WHERE hash = 'hash5';")
        priority = cursor.fetchone()['priority']
        assert priority == 1  # Default priority

        # Test 4: Test with empty arrays (should return empty)
        cursor.execute(f"""
            SELECT * FROM non_blocking_fragment_queue_batch_upsert(
                ARRAY[]::TEXT[],
                ARRAY[]::TEXT[],
                ARRAY[]::TEXT[],
                ARRAY[]::TEXT[],
                NULL::INT[],
                '{fragment_queue_table}'
            );
        """)
        results = cursor.fetchall()
        assert len(results) == 0


def test_batch_function_error_handling(db_connection):
    """Test error handling in batch function."""
    connection = db_connection

    # Create queue handler to get table name
    queue_handler = get_queue_handler("postgresql")
    fragment_queue_table = queue_handler.fragment_queue_table

    with connection.cursor() as cursor:
        # Test mismatched array lengths
        with pytest.raises(Exception) as exc_info:
            cursor.execute(f"""
                SELECT * FROM non_blocking_fragment_queue_batch_upsert(
                    ARRAY['hash1', 'hash2']::TEXT[],
                    ARRAY['city_id']::TEXT[],  -- Only one element
                    ARRAY['integer', 'integer']::TEXT[],
                    ARRAY['SELECT 1', 'SELECT 2']::TEXT[],
                    NULL::INT[],
                    '{fragment_queue_table}'
                );
            """)
        assert "All input arrays must have the same length" in str(exc_info.value)

