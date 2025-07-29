"""
Integration tests for non-blocking queue upsert functions.
Tests concurrent behavior and verifies the functions handle locking correctly.
"""

import threading

import pytest

from partitioncache.queue_handler import get_queue_handler


class TestNonBlockingQueueFunctions:
    """Test the non-blocking queue upsert functions."""

    @pytest.fixture
    def postgresql_handler(self, db_connection):
        """Get PostgreSQL queue handler for testing."""
        handler = get_queue_handler("postgresql")
        # Functions are automatically deployed during handler initialization

        # Clear queues before each test
        handler.clear_original_query_queue()
        handler.clear_query_fragment_queue()

        return handler

    def test_fragment_queue_upsert_new_item(self, postgresql_handler, db_connection):
        """Test inserting a new item to fragment queue."""
        cursor = db_connection.cursor()

        # Test inserting new item
        cursor.execute(
            "SELECT non_blocking_fragment_queue_upsert(%s, %s, %s, %s, %s, %s)",
            ("test_hash", "test_partition", "text", "SELECT 1", 1, postgresql_handler.fragment_queue_table),
        )
        result = cursor.fetchone()[0]
        db_connection.commit()

        assert result == "inserted"

        # Verify item was inserted
        cursor.execute(f"SELECT hash, partition_key, priority FROM {postgresql_handler.fragment_queue_table} WHERE hash = %s", ("test_hash",))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "test_hash"
        assert row[1] == "test_partition"
        assert row[2] == 1

    def test_fragment_queue_upsert_existing_unlocked(self, postgresql_handler, db_connection):
        """Test updating an existing unlocked item."""
        cursor = db_connection.cursor()

        # Insert initial item
        cursor.execute(
            "SELECT non_blocking_fragment_queue_upsert(%s, %s, %s, %s, %s, %s)",
            ("test_hash", "test_partition", "text", "SELECT 1", 1, postgresql_handler.fragment_queue_table),
        )
        db_connection.commit()

        # Update the same item (should increment priority)
        cursor.execute(
            "SELECT non_blocking_fragment_queue_upsert(%s, %s, %s, %s, %s, %s)",
            ("test_hash", "test_partition", "text", "SELECT 1", 1, postgresql_handler.fragment_queue_table),
        )
        result = cursor.fetchone()[0]
        db_connection.commit()

        assert result == "updated"

        # Verify priority was incremented
        cursor.execute(f"SELECT priority FROM {postgresql_handler.fragment_queue_table} WHERE hash = %s", ("test_hash",))
        priority = cursor.fetchone()[0]
        assert priority == 2

    def test_fragment_queue_upsert_locked_item(self, postgresql_handler, db_connection):
        """Test that upsert is skipped when item is locked."""
        cursor = db_connection.cursor()

        # Insert initial item
        cursor.execute(
            "SELECT non_blocking_fragment_queue_upsert(%s, %s, %s, %s, %s, %s)",
            ("test_hash", "test_partition", "text", "SELECT 1", 1, postgresql_handler.fragment_queue_table),
        )
        db_connection.commit()

        # Start a transaction that locks the row
        db_connection.autocommit = False
        cursor.execute("BEGIN")
        cursor.execute(f"SELECT id FROM {postgresql_handler.fragment_queue_table} WHERE hash = %s FOR UPDATE", ("test_hash",))

        # In another connection, try to upsert (should be skipped)
        handler2 = get_queue_handler("postgresql")
        conn2 = handler2._get_connection() # type: ignore[attr-defined]
        cursor2 = conn2.cursor()

        cursor2.execute(
            "SELECT non_blocking_fragment_queue_upsert(%s, %s, %s, %s, %s, %s)",
            ("test_hash", "test_partition", "text", "SELECT 1", 1, postgresql_handler.fragment_queue_table),
        )
        result = cursor2.fetchone()[0]
        conn2.commit()

        assert result == "skipped_locked"

        # Release the lock
        cursor.execute("ROLLBACK")
        db_connection.autocommit = True

        # Verify priority wasn't changed
        cursor.execute(f"SELECT priority FROM {postgresql_handler.fragment_queue_table} WHERE hash = %s", ("test_hash",))
        priority = cursor.fetchone()[0]
        assert priority == 1

    def test_original_queue_upsert_functions(self, postgresql_handler, db_connection):
        """Test the original queue upsert functions work similarly."""
        cursor = db_connection.cursor()

        # Test insert
        cursor.execute(
            "SELECT non_blocking_original_queue_upsert(%s, %s, %s, %s, %s)", ("SELECT 1", "test_partition", "text", 1, postgresql_handler.original_queue_table)
        )
        result = cursor.fetchone()[0]
        db_connection.commit()

        assert result == "inserted"

        # Test update
        cursor.execute(
            "SELECT non_blocking_original_queue_upsert(%s, %s, %s, %s, %s)", ("SELECT 1", "test_partition", "text", 1, postgresql_handler.original_queue_table)
        )
        result = cursor.fetchone()[0]
        db_connection.commit()

        assert result == "updated"

        # Verify priority was incremented
        cursor.execute(f"SELECT priority FROM {postgresql_handler.original_queue_table} WHERE query = %s", ("SELECT 1",))
        priority = cursor.fetchone()[0]
        assert priority == 2

    def test_concurrent_upserts_no_blocking(self, postgresql_handler, db_connection):
        """Test that concurrent upserts don't block each other."""
        results = []
        errors = []

        def worker(worker_id):
            try:
                # Create direct connections to avoid handler initialization concurrency
                import psycopg

                conn = psycopg.connect(
                    host=postgresql_handler.host,
                    port=postgresql_handler.port,
                    user=postgresql_handler.user,
                    password=postgresql_handler.password,
                    dbname=postgresql_handler.dbname,
                )
                cursor = conn.cursor()

                # Each worker tries to upsert the same item
                cursor.execute(
                    "SELECT non_blocking_fragment_queue_upsert(%s, %s, %s, %s, %s, %s)",
                    ("concurrent_hash", "test_partition", "text", f"SELECT {worker_id}", 1, postgresql_handler.fragment_queue_table),
                )
                result = cursor.fetchone()[0] # type: ignore[index]
                conn.commit()
                conn.close()
                results.append((worker_id, result))
            except Exception as e:
                errors.append((worker_id, str(e)))

        # Start 5 concurrent workers
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 5

        # One should be inserted, others should be updated, skipped, or have concurrent issues
        result_types = [result[1] for result in results]
        assert "inserted" in result_types
        assert all(rt in ["inserted", "updated", "skipped_locked", "skipped_concurrent"] for rt in result_types)

        # Verify that at least one entry was actually added to the queue
        cursor = db_connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {postgresql_handler.fragment_queue_table} WHERE hash = %s", ("concurrent_hash",))
        count = cursor.fetchone()[0]
        assert count > 0, "No entries found in queue despite successful function calls"

        # Verify the entry has correct data
        cursor.execute(f"SELECT hash, partition_key, priority FROM {postgresql_handler.fragment_queue_table} WHERE hash = %s", ("concurrent_hash",))
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "concurrent_hash"
        assert row[1] == "test_partition"
        assert row[2] >= 1  # Priority should be at least 1, potentially higher if multiple updates succeeded

    def test_python_handler_uses_non_blocking_functions(self, postgresql_handler):
        """Test that the Python handler correctly uses the non-blocking functions."""
        # Test original queue
        success = postgresql_handler.push_to_original_query_queue("SELECT 1", "test_partition", "text")
        assert success

        # Test fragment queue
        query_hash_pairs = [("SELECT 1", "hash1"), ("SELECT 2", "hash2")]
        success = postgresql_handler.push_to_query_fragment_queue(query_hash_pairs, "test_partition", "text")
        assert success

        # Verify items were inserted
        lengths = postgresql_handler.get_queue_lengths()
        assert lengths["original_query_queue"] == 1
        assert lengths["query_fragment_queue"] == 2

    def test_regular_vs_priority_queue_behavior(self, postgresql_handler, db_connection):
        """Test that both regular and priority queue functions now use proper upsert behavior."""
        cursor = db_connection.cursor()

        # Test regular functions now use proper upsert (no longer ON CONFLICT DO NOTHING)
        success1 = postgresql_handler.push_to_original_query_queue("SELECT regular", "test_partition", "text")
        success2 = postgresql_handler.push_to_original_query_queue("SELECT regular", "test_partition", "text")  # Same query
        assert success1 and success2

        # Check priority is now incremented for regular function too (no more ON CONFLICT DO NOTHING)
        cursor.execute(f"SELECT priority FROM {postgresql_handler.original_queue_table} WHERE query = %s", ("SELECT regular",))
        priority = cursor.fetchone()[0]
        assert priority == 2, "Regular function now properly increments priority on duplicate"

        # Test priority functions (same behavior as regular functions now)
        success1 = postgresql_handler.push_to_original_query_queue_with_priority("SELECT priority", "test_partition", 1, "text")
        success2 = postgresql_handler.push_to_original_query_queue_with_priority("SELECT priority", "test_partition", 1, "text")  # Same query
        assert success1 and success2

        # Check priority incremented for priority function
        cursor.execute(f"SELECT priority FROM {postgresql_handler.original_queue_table} WHERE query = %s", ("SELECT priority",))
        priority = cursor.fetchone()[0]
        assert priority > 1, "Priority function should increment priority on duplicate"

        # Test fragment queue behavior - now both functions use proper upsert
        success1 = postgresql_handler.push_to_query_fragment_queue([("SELECT frag", "regular_hash")], "test_partition", "text")
        success2 = postgresql_handler.push_to_query_fragment_queue([("SELECT frag", "regular_hash")], "test_partition", "text")  # Same

        cursor.execute(f"SELECT priority FROM {postgresql_handler.fragment_queue_table} WHERE hash = %s", ("regular_hash",))
        priority_regular = cursor.fetchone()[0]
        assert priority_regular == 2, "Regular fragment function now properly increments priority on duplicate"

        success1 = postgresql_handler.push_to_query_fragment_queue_with_priority([("SELECT frag2", "priority_hash")], "test_partition", 1, "text")
        success2 = postgresql_handler.push_to_query_fragment_queue_with_priority([("SELECT frag2", "priority_hash")], "test_partition", 1, "text")  # Same

        cursor.execute(f"SELECT priority FROM {postgresql_handler.fragment_queue_table} WHERE hash = %s", ("priority_hash",))
        priority_with_increment = cursor.fetchone()[0]
        assert priority_with_increment > 1, "Priority fragment function should increment priority on duplicate"
