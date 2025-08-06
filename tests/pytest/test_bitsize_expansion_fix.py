"""
Test for bitsize expansion fix in PostgreSQL bit cache handler.

This test specifically validates the fix for the off-by-one error where
partition key values equal to the bitsize would cause validation failures.
"""

from unittest.mock import Mock, patch

import pytest

from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler


class TestBitsizeExpansionFix:
    """Test cases for the bitsize expansion fix."""

    @pytest.fixture
    def mock_db(self):
        """Mock database connection."""
        return Mock()

    @pytest.fixture
    def mock_cursor(self):
        """Mock database cursor."""
        cursor = Mock()
        # Mock fetchone to return None for metadata table check
        cursor.fetchone.return_value = None
        return cursor

    @pytest.fixture
    def cache_handler(self, mock_db, mock_cursor):
        """Create a mocked PostgreSQL bit cache handler."""
        with patch("psycopg.connect", return_value=mock_db):
            with patch.object(PostgreSQLBitCacheHandler, "_recreate_metadata_table"):
                handler = PostgreSQLBitCacheHandler(
                    db_name="test_db",
                    db_host="localhost",
                    db_user="test_user",
                    db_password="test_password",
                    db_port=5432,
                    db_tableprefix="test_bitsize_cache",
                    bitsize=1000,
                )
                handler.cursor = mock_cursor
                handler.db = mock_db
                return handler

    def test_ensure_partition_table_new_partition(self, cache_handler):
        """Test _ensure_partition_table with a new partition."""
        # Mock no existing partition
        cache_handler._get_partition_bitsize = Mock(return_value=None)

        # Mock the bootstrap process
        with patch.object(cache_handler, "_load_sql_functions"), patch.object(cache_handler, "cursor") as mock_cursor:
            mock_cursor.execute.return_value = None
            mock_cursor.fetchone.return_value = [True]  # Advisory lock acquired
            cache_handler._get_partition_bitsize = Mock(side_effect=[None, None, 2000])  # Not found initially, then created with requested bitsize

            success, actual_bitsize = cache_handler._ensure_partition_table("new_partition", "integer", bitsize=2000)

            assert success is True
            assert actual_bitsize == 2000

    def test_ensure_partition_table_sufficient_existing_bitsize(self, cache_handler):
        """Test _ensure_partition_table with existing sufficient bitsize."""
        # Mock existing partition with sufficient bitsize
        cache_handler._get_partition_bitsize = Mock(return_value=2000)

        success, actual_bitsize = cache_handler._ensure_partition_table("existing_partition", "integer", bitsize=1500)

        assert success is True
        assert actual_bitsize == 2000
        # Should return early without calling bootstrap

    def test_ensure_partition_table_insufficient_existing_bitsize(self, cache_handler):
        """Test _ensure_partition_table with existing insufficient bitsize (the bug case)."""
        # Mock existing partition with insufficient bitsize (the original bug scenario)
        initial_bitsize = 1283828
        required_bitsize = 1283829

        # Mock the sequence: initial check finds insufficient bitsize, then bootstrap expands it
        bitsize_calls = [initial_bitsize, initial_bitsize, required_bitsize]  # Third call after bootstrap
        cache_handler._get_partition_bitsize = Mock(side_effect=bitsize_calls)

        # Mock the bootstrap process
        with patch.object(cache_handler, "_load_sql_functions"), patch.object(cache_handler, "cursor") as mock_cursor:
            mock_cursor.execute.return_value = None
            mock_cursor.fetchone.return_value = [True]  # Advisory lock acquired

            success, actual_bitsize = cache_handler._ensure_partition_table("pocket_key", "integer", bitsize=required_bitsize)

            assert success is True
            assert actual_bitsize == required_bitsize
            # Should have proceeded to bootstrap since existing bitsize was insufficient

    def test_set_cache_bitsize_expansion_scenario(self, cache_handler):
        """Test the complete set_cache scenario that was failing."""
        # Simulate the exact failing scenario from the original error
        partition_key_identifiers = {1283828}  # The problematic value
        partition_key = "pocket_key"

        # Verify the calculation logic that caused the original bug
        max_value = max(partition_key_identifiers)
        required_bitsize = max_value + 1  # 1283829

        # The bug was that existing bitsize of 1283828 was considered sufficient for max_value 1283828
        # But validation would fail because max_value >= actual_bitsize (1283828 >= 1283828)
        assert max_value == 1283828
        assert required_bitsize == 1283829

        # Mock existing partition with insufficient bitsize initially
        cache_handler._get_partition_bitsize = Mock(side_effect=[1283828, 1283828, 1283829])

        with (
            patch.object(cache_handler, "_load_sql_functions"),
            patch.object(cache_handler, "cursor") as mock_cursor,
            patch("bitarray.bitarray") as mock_bitarray,
        ):
            # Mock database operations
            mock_cursor.execute.return_value = None
            mock_cursor.fetchone.return_value = [True]  # Advisory lock acquired
            mock_bitarray.return_value.to01.return_value = "0" * 1283829

            # This should succeed with the fix (would fail with the old logic)
            result = cache_handler.set_cache("test_hash", partition_key_identifiers, partition_key)

            assert result is True
            # Verify that _ensure_partition_table was called and would expand bitsize
            assert cache_handler._get_partition_bitsize.call_count >= 2

    def test_validation_logic_edge_cases(self, cache_handler):
        """Test edge cases around the validation logic."""
        test_cases = [
            # (existing_bitsize, required_bitsize, should_bootstrap)
            (None, 1000, True),  # New partition
            (2000, 1000, False),  # Sufficient existing
            (1000, 2000, True),  # Insufficient existing
            (1283828, 1283829, True),  # The original bug case
            (1000, 1000, False),  # Exact match
        ]

        for existing_bitsize, required_bitsize, should_bootstrap in test_cases:
            cache_handler._get_partition_bitsize = Mock(return_value=existing_bitsize)

            with patch.object(cache_handler, "_load_sql_functions"), patch.object(cache_handler, "cursor") as mock_cursor:
                if should_bootstrap:
                    mock_cursor.execute.return_value = None
                    mock_cursor.fetchone.return_value = [True]
                    # Mock the bootstrap result
                    cache_handler._get_partition_bitsize = Mock(side_effect=[existing_bitsize, existing_bitsize, required_bitsize])

                success, actual_bitsize = cache_handler._ensure_partition_table("test_partition", "integer", bitsize=required_bitsize)

                assert success is True
                if should_bootstrap:
                    assert actual_bitsize == required_bitsize
                else:
                    assert actual_bitsize == existing_bitsize

    def test_fix_prevents_original_error(self, cache_handler):
        """Test that the fix prevents the original ValueError."""
        # The exact scenario that was failing
        max_value = 1283828
        required_bitsize = max_value + 1  # 1283829
        existing_bitsize = 1283828  # What was causing the failure

        # With the fix, this should NOT return early but should bootstrap
        cache_handler._get_partition_bitsize = Mock(side_effect=[existing_bitsize, existing_bitsize, required_bitsize])

        with patch.object(cache_handler, "_load_sql_functions"), patch.object(cache_handler, "cursor") as mock_cursor:
            mock_cursor.execute.return_value = None
            mock_cursor.fetchone.return_value = [True]

            success, actual_bitsize = cache_handler._ensure_partition_table("pocket_key", "integer", bitsize=required_bitsize)

            # The key assertion: the validation that was failing should now pass
            assert max_value < actual_bitsize  # This is the check that was failing before
            assert actual_bitsize == required_bitsize
            assert success is True

    def test_concurrency_insufficient_bitsize_expansion(self, cache_handler):
        """Test that concurrent threads handle insufficient bitsize correctly."""
        # Simulate the concurrency scenario from the logs
        max_value = 1283828
        required_bitsize = max_value + 1  # 1283829
        existing_insufficient_bitsize = 1283828

        # Mock the scenario where another thread created partition with insufficient bitsize
        # First call: no partition exists (None)
        # Second call in wait loop: partition exists but insufficient (1283828)
        # Third call after bootstrap: expanded bitsize (1283829)
        cache_handler._get_partition_bitsize = Mock(
            side_effect=[
                None,  # Initial check - no partition
                existing_insufficient_bitsize,  # Found by another thread but insufficient
                existing_insufficient_bitsize,  # Still insufficient after double-check
                required_bitsize,  # After bootstrap expansion
            ]
        )

        with (
            patch.object(cache_handler, "_load_sql_functions"),
            patch.object(cache_handler, "cursor") as mock_cursor,
            patch("time.sleep"),
        ):  # Mock sleep to speed up test
            # Mock advisory lock not acquired initially (another thread working)
            # Then acquired for expansion
            mock_cursor.execute.return_value = None
            mock_cursor.fetchone.side_effect = [
                [False],  # First lock attempt fails (another thread working)
                [True],  # Second lock attempt succeeds (for expansion)
            ]

            success, actual_bitsize = cache_handler._ensure_partition_table("pocket_key", "integer", bitsize=required_bitsize)

            # Should succeed with expanded bitsize
            assert success is True
            assert actual_bitsize == required_bitsize
            assert max_value < actual_bitsize  # Critical validation passes
