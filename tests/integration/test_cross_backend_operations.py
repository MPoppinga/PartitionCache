"""
Integration tests for export/import/copy operations between different cache backends.
Tests data integrity when moving data and queries metadata across backend types.
"""

import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from partitioncache.cache_handler import get_cache_handler
from partitioncache.cli.manage_cache import copy_cache, export_cache, restore_cache


@pytest.fixture
def temp_directories():
    """Create temporary directories for different cache backends."""
    dirs = {
        "rocksdict_src": tempfile.mkdtemp(prefix="test_rocksdict_src_"),
        "rocksdict_dst": tempfile.mkdtemp(prefix="test_rocksdict_dst_"),
        "export_file": tempfile.mktemp(suffix=".pkl"),
    }
    yield dirs

    # Cleanup
    for path in dirs.values():
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.isfile(path):
            os.remove(path)


@pytest.fixture
def test_data():
    """Test data for cache population."""
    return {
        "partitions": [
            ("city_id", "integer"),
            ("region_id", "integer"),
            ("product_category", "text"),
        ],
        "cache_entries": [
            # (query_hash, partition_keys, partition_key, query_text)
            ("city_hash_1", {1, 2, 3}, "city_id", "SELECT * FROM locations WHERE city_id IN (1,2,3)"),
            ("city_hash_2", {4, 5, 6}, "city_id", "SELECT * FROM locations WHERE city_id IN (4,5,6)"),
            ("region_hash_1", {10, 20}, "region_id", "SELECT * FROM regions WHERE region_id IN (10,20)"),
            ("product_hash_1", {"electronics", "books"}, "product_category", "SELECT * FROM products WHERE category IN ('electronics','books')"),
        ],
    }


class TestCrossBackendExportImport:
    """Test export/import operations between different backends."""

    def populate_cache(self, cache_handler, test_data):
        """Helper to populate cache with test data."""
        # Register partitions
        for partition_key, datatype in test_data["partitions"]:
            cache_handler.register_partition_key(partition_key, datatype)

        # Add cache entries with queries
        for query_hash, partition_keys, partition_key, query_text in test_data["cache_entries"]:
            cache_handler.set_cache(query_hash, partition_keys, partition_key)
            cache_handler.set_query(query_hash, query_text, partition_key)

        return cache_handler

    def verify_cache_integrity(self, cache_handler, test_data, expected_partitions=None):
        """Helper to verify cache data integrity."""
        if expected_partitions is None:
            expected_partitions = [p[0] for p in test_data["partitions"]]

        # Verify partitions exist
        actual_partitions = cache_handler.get_partition_keys()
        actual_partition_names = [p[0] if isinstance(p, tuple) else p for p in actual_partitions]

        for expected_partition in expected_partitions:
            assert expected_partition in actual_partition_names, f"Partition {expected_partition} missing"

        # Verify cache entries
        for query_hash, expected_keys, partition_key, expected_query in test_data["cache_entries"]:
            if partition_key not in expected_partitions:
                continue

            # Check cache data
            actual_keys = cache_handler.get(query_hash, partition_key)
            assert actual_keys == expected_keys, f"Cache data mismatch for {query_hash}: {actual_keys} != {expected_keys}"

            # Check query data
            actual_query = cache_handler.get_query(query_hash, partition_key)
            assert actual_query == expected_query, f"Query mismatch for {query_hash}: {actual_query} != {expected_query}"

        return True

    def test_rocksdict_export_import_full(self, temp_directories, test_data):
        """Test full export/import cycle with rocksdict backend."""
        src_dir = temp_directories["rocksdict_src"]
        dst_dir = temp_directories["rocksdict_dst"]
        export_file = temp_directories["export_file"]

        # Set up source cache
        os.environ["CACHE_BACKEND"] = "rocksdict"
        os.environ["ROCKSDB_DICT_PATH"] = src_dir

        src_cache = get_cache_handler("rocksdict")
        self.populate_cache(src_cache, test_data)

        # Verify source data
        assert self.verify_cache_integrity(src_cache, test_data)

        # Get counts before export
        src_partitions = src_cache.get_partition_keys()
        src_total_keys = sum(len(src_cache.get_all_keys(p[0])) for p in src_partitions)
        src_total_queries = sum(len(src_cache.get_all_queries(p[0])) for p in src_partitions)

        src_cache.close()

        # Export
        export_cache("rocksdict", export_file)

        # Verify export file
        assert Path(export_file).exists()
        assert Path(export_file).stat().st_size > 0

        # Set up target cache
        os.environ["ROCKSDB_DICT_PATH"] = dst_dir

        # Import
        restore_cache("rocksdict", export_file)

        # Verify target data
        dst_cache = get_cache_handler("rocksdict")
        assert self.verify_cache_integrity(dst_cache, test_data)

        # Verify counts match
        dst_partitions = dst_cache.get_partition_keys()
        dst_total_keys = sum(len(dst_cache.get_all_keys(p[0])) for p in dst_partitions)
        dst_total_queries = sum(len(dst_cache.get_all_queries(p[0])) for p in dst_partitions)

        assert len(dst_partitions) == len(src_partitions)
        assert dst_total_keys == src_total_keys
        assert dst_total_queries == src_total_queries

        dst_cache.close()

    def test_selective_export_import(self, temp_directories, test_data):
        """Test selective export/import with --partition-key parameter."""
        src_dir = temp_directories["rocksdict_src"]
        dst_dir = temp_directories["rocksdict_dst"]
        export_file = temp_directories["export_file"]

        # Set up source cache
        os.environ["CACHE_BACKEND"] = "rocksdict"
        os.environ["ROCKSDB_DICT_PATH"] = src_dir

        src_cache = get_cache_handler("rocksdict")
        self.populate_cache(src_cache, test_data)
        src_cache.close()

        # Export only city_id partition
        export_cache("rocksdict", export_file, partition_key="city_id")

        # Set up target cache
        os.environ["ROCKSDB_DICT_PATH"] = dst_dir

        # Import
        restore_cache("rocksdict", export_file)

        # Verify only city_id data was imported
        dst_cache = get_cache_handler("rocksdict")
        assert self.verify_cache_integrity(dst_cache, test_data, expected_partitions=["city_id"])

        # Verify other partitions don't exist
        assert dst_cache.get_datatype("region_id") is None
        assert dst_cache.get_datatype("product_category") is None

        # Verify city_id has correct data
        city_keys = dst_cache.get_all_keys("city_id")
        city_queries = dst_cache.get_all_queries("city_id")

        expected_city_entries = [e for e in test_data["cache_entries"] if e[2] == "city_id"]
        assert len(city_keys) == len(expected_city_entries)
        assert len(city_queries) == len(expected_city_entries)

        dst_cache.close()

    def test_import_to_existing_cache(self, temp_directories, test_data):
        """Test importing to a cache that already has some data."""
        src_dir = temp_directories["rocksdict_src"]
        dst_dir = temp_directories["rocksdict_dst"]
        export_file = temp_directories["export_file"]

        # Set up source cache
        os.environ["CACHE_BACKEND"] = "rocksdict"
        os.environ["ROCKSDB_DICT_PATH"] = src_dir

        src_cache = get_cache_handler("rocksdict")
        self.populate_cache(src_cache, test_data)
        src_cache.close()

        # Export
        export_cache("rocksdict", export_file)

        # Set up target cache with some existing data
        os.environ["ROCKSDB_DICT_PATH"] = dst_dir

        dst_cache = get_cache_handler("rocksdict")
        dst_cache.register_partition_key("existing_partition", "integer")
        dst_cache.set_cache("existing_hash", {100, 200}, "existing_partition")
        dst_cache.set_query("existing_hash", "SELECT * FROM existing WHERE id IN (100,200)", "existing_partition")
        dst_cache.close()

        # Import (should merge with existing data)
        restore_cache("rocksdict", export_file)

        # Verify both existing and imported data
        dst_cache = get_cache_handler("rocksdict")

        # Check imported data
        assert self.verify_cache_integrity(dst_cache, test_data)

        # Check existing data is still there
        existing_keys = dst_cache.get_all_keys("existing_partition")
        existing_queries = dst_cache.get_all_queries("existing_partition")

        assert len(existing_keys) == 1
        assert len(existing_queries) == 1
        assert dst_cache.get("existing_hash", "existing_partition") == {100, 200}
        assert dst_cache.get_query("existing_hash", "existing_partition") == "SELECT * FROM existing WHERE id IN (100,200)"

        dst_cache.close()

    def test_import_with_target_partition_remapping(self, temp_directories, test_data):
        """Test importing with --partition-key remapping to different partition."""
        src_dir = temp_directories["rocksdict_src"]
        dst_dir = temp_directories["rocksdict_dst"]
        export_file = temp_directories["export_file"]

        # Set up source cache
        os.environ["CACHE_BACKEND"] = "rocksdict"
        os.environ["ROCKSDB_DICT_PATH"] = src_dir

        src_cache = get_cache_handler("rocksdict")
        self.populate_cache(src_cache, test_data)
        src_cache.close()

        # Export city_id partition
        export_cache("rocksdict", export_file, partition_key="city_id")

        # Set up target cache
        os.environ["ROCKSDB_DICT_PATH"] = dst_dir

        # Import to different partition name
        restore_cache("rocksdict", export_file, target_partition_key="remapped_cities")

        # Verify data was imported to remapped partition
        dst_cache = get_cache_handler("rocksdict")

        # Original partition should not exist
        assert dst_cache.get_datatype("city_id") is None

        # Remapped partition should exist with correct data
        assert dst_cache.get_datatype("remapped_cities") == "integer"

        remapped_keys = dst_cache.get_all_keys("remapped_cities")
        remapped_queries = dst_cache.get_all_queries("remapped_cities")

        # Should have city_id entries mapped to new partition
        expected_city_entries = [e for e in test_data["cache_entries"] if e[2] == "city_id"]
        assert len(remapped_keys) == len(expected_city_entries)
        assert len(remapped_queries) == len(expected_city_entries)

        # Verify data integrity in remapped partition
        for query_hash, expected_keys, partition_key, expected_query in expected_city_entries:
            actual_keys = dst_cache.get(query_hash, "remapped_cities")
            actual_query = dst_cache.get_query(query_hash, "remapped_cities")

            assert actual_keys == expected_keys
            assert actual_query == expected_query

        dst_cache.close()

    def test_empty_cache_export_import(self, temp_directories):
        """Test export/import of empty cache."""
        src_dir = temp_directories["rocksdict_src"]
        dst_dir = temp_directories["rocksdict_dst"]
        export_file = temp_directories["export_file"]

        # Set up empty source cache
        os.environ["CACHE_BACKEND"] = "rocksdict"
        os.environ["ROCKSDB_DICT_PATH"] = src_dir

        src_cache = get_cache_handler("rocksdict")
        src_cache.close()

        # Export empty cache
        export_cache("rocksdict", export_file)

        # Verify export file exists but is small
        assert Path(export_file).exists()
        export_size = Path(export_file).stat().st_size
        assert export_size > 0  # Should have metadata structure
        assert export_size < 1000  # But should be small

        # Set up target cache
        os.environ["ROCKSDB_DICT_PATH"] = dst_dir

        # Import empty cache
        restore_cache("rocksdict", export_file)

        # Verify target is also empty
        dst_cache = get_cache_handler("rocksdict")
        partitions = dst_cache.get_partition_keys()
        assert len(partitions) == 0
        dst_cache.close()


class TestCrossBackendCopy:
    """Test copy operations between same backend types (simulated cross-backend)."""

    def test_copy_with_queries_metadata(self, temp_directories, test_data):
        """Test copy operation preserves queries metadata."""
        src_dir = temp_directories["rocksdict_src"]
        dst_dir = temp_directories["rocksdict_dst"]

        # Set up source cache
        os.environ["CACHE_BACKEND"] = "rocksdict"
        os.environ["ROCKSDB_DICT_PATH"] = src_dir

        src_cache = get_cache_handler("rocksdict")

        # Register partitions and add data
        for partition_key, datatype in test_data["partitions"]:
            src_cache.register_partition_key(partition_key, datatype)

        for query_hash, partition_keys, partition_key, query_text in test_data["cache_entries"]:
            src_cache.set_cache(query_hash, partition_keys, partition_key)
            src_cache.set_query(query_hash, query_text, partition_key)

        src_cache.close()

        # Perform copy operation
        # Note: This would normally fail due to RocksDB locking with same backend type,
        # but we test the logic by mocking the operation
        from unittest.mock import MagicMock, patch

        # Mock the copy operation to avoid RocksDB locking issues
        mock_dst_cache = MagicMock()
        mock_dst_cache.get_datatype.return_value = None
        mock_dst_cache.exists.return_value = False
        mock_dst_cache.set_cache.return_value = True
        mock_dst_cache.set_query.return_value = True

        with patch("partitioncache.cli.manage_cache.get_cache_handler") as mock_get_handler:
            # First call returns source cache, second returns mock destination
            os.environ["ROCKSDB_DICT_PATH"] = src_dir
            real_src_cache = get_cache_handler("rocksdict")
            mock_get_handler.side_effect = [real_src_cache, mock_dst_cache]

            # Test copy operation
            copy_cache("rocksdict", "rocksdict")

            real_src_cache.close()

        # Verify copy operations were called correctly
        # Check that register_partition_key was called for each partition
        expected_register_calls = len(test_data["partitions"])
        assert mock_dst_cache.register_partition_key.call_count == expected_register_calls

        # Check that set_cache was called for each cache entry
        expected_set_calls = len(test_data["cache_entries"])
        assert mock_dst_cache.set_cache.call_count == expected_set_calls

        # Check that set_query was called for each query
        assert mock_dst_cache.set_query.call_count == expected_set_calls

    def test_selective_copy_with_queries(self, temp_directories, test_data):
        """Test selective copy with partition filtering."""
        src_dir = temp_directories["rocksdict_src"]

        # Set up source cache
        os.environ["CACHE_BACKEND"] = "rocksdict"
        os.environ["ROCKSDB_DICT_PATH"] = src_dir

        src_cache = get_cache_handler("rocksdict")

        # Register partitions and add data
        for partition_key, datatype in test_data["partitions"]:
            src_cache.register_partition_key(partition_key, datatype)

        for query_hash, partition_keys, partition_key, query_text in test_data["cache_entries"]:
            src_cache.set_cache(query_hash, partition_keys, partition_key)
            src_cache.set_query(query_hash, query_text, partition_key)

        src_cache.close()

        # Mock selective copy operation
        from unittest.mock import MagicMock, patch

        mock_dst_cache = MagicMock()
        mock_dst_cache.get_datatype.return_value = None
        mock_dst_cache.exists.return_value = False
        mock_dst_cache.set_cache.return_value = True
        mock_dst_cache.set_query.return_value = True

        with patch("partitioncache.cli.manage_cache.get_cache_handler") as mock_get_handler:
            os.environ["ROCKSDB_DICT_PATH"] = src_dir
            real_src_cache = get_cache_handler("rocksdict")
            mock_get_handler.side_effect = [real_src_cache, mock_dst_cache]

            # Test selective copy (only city_id)
            copy_cache("rocksdict", "rocksdict", partition_key="city_id")

            real_src_cache.close()

        # Verify only city_id partition was processed
        # Should register only 1 partition (city_id)
        assert mock_dst_cache.register_partition_key.call_count == 1

        # Should copy only city_id entries
        expected_city_entries = [e for e in test_data["cache_entries"] if e[2] == "city_id"]
        assert mock_dst_cache.set_cache.call_count == len(expected_city_entries)
        assert mock_dst_cache.set_query.call_count == len(expected_city_entries)


class TestDataIntegrityValidation:
    """Test data integrity validation across operations."""

    def test_complex_data_types_preservation(self, temp_directories):
        """Test that complex data types are preserved correctly."""
        src_dir = temp_directories["rocksdict_src"]
        dst_dir = temp_directories["rocksdict_dst"]
        export_file = temp_directories["export_file"]

        # Set up complex test data
        complex_data = {
            "timestamp_data": {datetime(2023, 1, 1), datetime(2023, 12, 31)},
            "float_data": {1.5, 2.7, 3.14159},
            "text_data": {"category_a", "category_b", "special chars: éñ"},
            "integer_data": {1, 1000000, -5},
        }

        complex_queries = {
            "timestamp_query": "SELECT * FROM events WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31'",
            "float_query": "SELECT * FROM measurements WHERE value IN (1.5, 2.7, 3.14159)",
            "text_query": "SELECT * FROM categories WHERE name IN ('category_a', 'category_b', 'special chars: éñ')",
            "integer_query": "SELECT * FROM items WHERE id IN (1, 1000000, -5)",
        }

        # Set up source cache
        os.environ["CACHE_BACKEND"] = "rocksdict"
        os.environ["ROCKSDB_DICT_PATH"] = src_dir

        src_cache = get_cache_handler("rocksdict")

        # Register partitions with different datatypes
        src_cache.register_partition_key("timestamp_partition", "timestamp")
        src_cache.register_partition_key("float_partition", "float")
        src_cache.register_partition_key("text_partition", "text")
        src_cache.register_partition_key("integer_partition", "integer")

        # Add complex data
        src_cache.set_cache("timestamp_hash", complex_data["timestamp_data"], "timestamp_partition")
        src_cache.set_query("timestamp_hash", complex_queries["timestamp_query"], "timestamp_partition")

        src_cache.set_cache("float_hash", complex_data["float_data"], "float_partition")
        src_cache.set_query("float_hash", complex_queries["float_query"], "float_partition")

        src_cache.set_cache("text_hash", complex_data["text_data"], "text_partition")
        src_cache.set_query("text_hash", complex_queries["text_query"], "text_partition")

        src_cache.set_cache("integer_hash", complex_data["integer_data"], "integer_partition")
        src_cache.set_query("integer_hash", complex_queries["integer_query"], "integer_partition")

        src_cache.close()

        # Export and import
        export_cache("rocksdict", export_file)

        os.environ["ROCKSDB_DICT_PATH"] = dst_dir
        restore_cache("rocksdict", export_file)

        # Verify complex data integrity
        dst_cache = get_cache_handler("rocksdict")

        # Check each data type
        assert dst_cache.get("timestamp_hash", "timestamp_partition") == complex_data["timestamp_data"]
        assert dst_cache.get("float_hash", "float_partition") == complex_data["float_data"]
        assert dst_cache.get("text_hash", "text_partition") == complex_data["text_data"]
        assert dst_cache.get("integer_hash", "integer_partition") == complex_data["integer_data"]

        # Check queries
        assert dst_cache.get_query("timestamp_hash", "timestamp_partition") == complex_queries["timestamp_query"]
        assert dst_cache.get_query("float_hash", "float_partition") == complex_queries["float_query"]
        assert dst_cache.get_query("text_hash", "text_partition") == complex_queries["text_query"]
        assert dst_cache.get_query("integer_hash", "integer_partition") == complex_queries["integer_query"]

        dst_cache.close()

    def test_large_dataset_integrity(self, temp_directories):
        """Test integrity with larger datasets."""
        src_dir = temp_directories["rocksdict_src"]
        dst_dir = temp_directories["rocksdict_dst"]
        export_file = temp_directories["export_file"]

        # Generate large test dataset
        large_data = []
        for i in range(100):  # 100 cache entries
            query_hash = f"large_hash_{i:03d}"
            partition_keys = set(range(i * 10, (i + 1) * 10))  # 10 keys each
            query_text = f"SELECT * FROM large_table WHERE id IN ({','.join(map(str, partition_keys))})"
            large_data.append((query_hash, partition_keys, query_text))

        # Set up source cache
        os.environ["CACHE_BACKEND"] = "rocksdict"
        os.environ["ROCKSDB_DICT_PATH"] = src_dir

        src_cache = get_cache_handler("rocksdict")
        src_cache.register_partition_key("large_partition", "integer")

        # Add large dataset
        for query_hash, partition_keys, query_text in large_data:
            src_cache.set_cache(query_hash, partition_keys, "large_partition")
            src_cache.set_query(query_hash, query_text, "large_partition")

        # Verify source counts
        src_keys = src_cache.get_all_keys("large_partition")
        src_queries = src_cache.get_all_queries("large_partition")
        assert len(src_keys) == 100
        assert len(src_queries) == 100

        src_cache.close()

        # Export and import
        export_cache("rocksdict", export_file)

        os.environ["ROCKSDB_DICT_PATH"] = dst_dir
        restore_cache("rocksdict", export_file)

        # Verify target integrity
        dst_cache = get_cache_handler("rocksdict")

        dst_keys = dst_cache.get_all_keys("large_partition")
        dst_queries = dst_cache.get_all_queries("large_partition")
        assert len(dst_keys) == 100
        assert len(dst_queries) == 100

        # Spot check data integrity
        for i in [0, 25, 50, 75, 99]:  # Check subset
            query_hash, expected_keys, expected_query = large_data[i]

            actual_keys = dst_cache.get(query_hash, "large_partition")
            actual_query = dst_cache.get_query(query_hash, "large_partition")

            assert actual_keys == expected_keys
            assert actual_query == expected_query

        dst_cache.close()
