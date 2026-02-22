"""
Integration tests for maintenance operations:
- Prune old queries
- Cleanup (termination entries, large entries)
- Eviction (oldest and largest strategies)
- Partition deletion

Tests verify the full lifecycle with real PostgreSQL backends.
"""

import os
import subprocess

import pytest

from partitioncache.cache_handler import get_cache_handler
from partitioncache.query_processor import generate_all_query_hash_pairs


@pytest.fixture
def maintenance_cache(db_session):
    """
    Create a postgresql_array cache handler specifically for maintenance tests.
    Uses a dedicated prefix to avoid conflicts with other tests.
    """
    original_backend = os.getenv("CACHE_BACKEND")
    original_prefix = os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX")

    os.environ["CACHE_BACKEND"] = "postgresql_array"
    os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"

    if not os.getenv("DB_NAME"):
        os.environ["DB_NAME"] = os.getenv("PG_DBNAME", "partitioncache_integration")
    if not os.getenv("DB_HOST"):
        os.environ["DB_HOST"] = os.getenv("PG_HOST", "localhost")
    if not os.getenv("DB_PORT"):
        os.environ["DB_PORT"] = os.getenv("PG_PORT", "5432")
    if not os.getenv("DB_USER"):
        os.environ["DB_USER"] = os.getenv("PG_USER", "integration_user")
    if not os.getenv("DB_PASSWORD"):
        os.environ["DB_PASSWORD"] = os.getenv("PG_PASSWORD", "integration_password")

    handler = None
    try:
        handler = get_cache_handler("postgresql_array", singleton=False)
        handler.register_partition_key("zipcode", "integer")
        handler.register_partition_key("region", "text")
        yield handler
    except Exception as e:
        pytest.skip(f"Cannot create maintenance cache handler: {e}")
    finally:
        # Close handler first to release DB connections/locks
        if handler is not None:
            try:
                handler.close()
            except Exception:
                pass
        # Cleanup tables
        try:
            with db_session.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS maint_test_cache_cache_zipcode CASCADE;")
                cur.execute("DROP TABLE IF EXISTS maint_test_cache_cache_region CASCADE;")
                cur.execute("DROP TABLE IF EXISTS maint_test_cache_partition_metadata CASCADE;")
                cur.execute("DROP TABLE IF EXISTS maint_test_cache_queries CASCADE;")
        except Exception:
            pass

        if original_backend:
            os.environ["CACHE_BACKEND"] = original_backend
        elif "CACHE_BACKEND" in os.environ:
            del os.environ["CACHE_BACKEND"]

        if original_prefix:
            os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = original_prefix
        elif "PG_ARRAY_CACHE_TABLE_PREFIX" in os.environ:
            del os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"]


def _populate_cache(handler, partition_key="zipcode"):
    """Helper to populate cache with test data and query metadata.

    Returns a list of unique hashes that were inserted.
    """
    test_queries = [
        "SELECT * FROM test_locations t1 WHERE t1.zipcode = 1001",
        "SELECT * FROM test_locations t1, test_businesses t2 WHERE t1.zipcode = t2.region_id AND t1.zipcode = 1001",
        "SELECT * FROM test_locations t1 WHERE t1.zipcode BETWEEN 1000 AND 2000",
    ]

    seen_hashes: set[str] = set()
    unique_hashes: list[str] = []
    for query in test_queries:
        pairs = generate_all_query_hash_pairs(query, partition_key)
        for query_text, query_hash in pairs:
            handler.set_cache(query_hash, {1001, 1002, 90210}, partition_key)
            handler.set_query(query_hash, query_text, partition_key)
            if query_hash not in seen_hashes:
                seen_hashes.add(query_hash)
                unique_hashes.append(query_hash)

    return unique_hashes


class TestPruneOldQueries:
    """Tests for the prune maintenance operation."""

    def test_prune_removes_old_queries(self, maintenance_cache, db_session):
        """Prune should remove queries older than N days."""
        hashes = _populate_cache(maintenance_cache)
        assert len(hashes) > 0

        # Artificially age some queries by updating their last_seen timestamp
        with db_session.cursor() as cur:
            # Age half the queries to 60 days ago
            aged_hashes = hashes[: len(hashes) // 2]
            for h in aged_hashes:
                cur.execute(
                    "UPDATE maint_test_cache_queries SET last_seen = NOW() - INTERVAL '60 days' WHERE query_hash = %s",
                    (h,),
                )

        # Count before prune
        keys_before = maintenance_cache.get_all_keys("zipcode")
        count_before = len(keys_before)

        # Prune queries older than 30 days
        removed = maintenance_cache.prune_old_queries(days_old=30)

        assert removed > 0, "Should have removed some old queries"
        assert removed == len(aged_hashes), f"Expected {len(aged_hashes)} removed, got {removed}"

        # Verify aged queries are gone from cache table
        keys_after = maintenance_cache.get_all_keys("zipcode")
        assert len(keys_after) < count_before, "Cache should have fewer entries after pruning"

        # Verify recent queries are still present
        for h in hashes[len(hashes) // 2 :]:
            assert maintenance_cache.exists(h, "zipcode"), f"Recent query {h} should still exist"

    def test_prune_leaves_recent_queries_untouched(self, maintenance_cache):
        """Prune should not affect queries within the retention window."""
        hashes = _populate_cache(maintenance_cache)

        # Prune with very large window - nothing should be removed
        removed = maintenance_cache.prune_old_queries(days_old=365)
        assert removed == 0, "No queries should be pruned with 365 day window"

        # Verify all queries still exist
        for h in hashes:
            assert maintenance_cache.exists(h, "zipcode"), f"Query {h} should still exist"

    def test_prune_handles_multiple_partitions(self, maintenance_cache, db_session):
        """Prune should work across multiple partition keys."""
        # Populate zipcode partition
        zipcode_hashes = _populate_cache(maintenance_cache, "zipcode")

        # Populate region partition
        region_hash = "region_test_hash_1"
        maintenance_cache.set_cache(region_hash, {"northeast", "west"}, "region")
        maintenance_cache.set_query(region_hash, "SELECT * FROM test", "region")

        # Age all zipcode queries
        with db_session.cursor() as cur:
            for h in zipcode_hashes:
                cur.execute(
                    "UPDATE maint_test_cache_queries SET last_seen = NOW() - INTERVAL '60 days' WHERE query_hash = %s AND partition_key = 'zipcode'",
                    (h,),
                )

        removed = maintenance_cache.prune_old_queries(days_old=30)

        assert removed == len(zipcode_hashes), "Should remove aged zipcode queries"

        # Region query should still exist
        assert maintenance_cache.exists(region_hash, "region"), "Recent region query should still exist"


class TestCleanupTerminationEntries:
    """Tests for removing _LIMIT_ and _TIMEOUT_ prefixed entries."""

    def test_remove_termination_entries(self, maintenance_cache):
        """Cleanup should remove _LIMIT_ and _TIMEOUT_ prefixed entries."""
        # Add normal entries
        maintenance_cache.set_cache("normal_hash_1", {1001, 1002}, "zipcode")
        maintenance_cache.set_cache("normal_hash_2", {90210}, "zipcode")

        # Add termination entries
        maintenance_cache.set_cache("_LIMIT_hash_1", {1001}, "zipcode")
        maintenance_cache.set_cache("_TIMEOUT_hash_2", {1002}, "zipcode")
        maintenance_cache.set_cache("_LIMIT_hash_3", {90210}, "zipcode")

        keys_before = maintenance_cache.get_all_keys("zipcode")
        assert len(keys_before) == 5, f"Expected 5 keys, got {len(keys_before)}"

        # Import and run cleanup
        from partitioncache.cli.manage_cache import remove_termination_entries

        os.environ["CACHE_BACKEND"] = "postgresql_array"
        os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"
        remove_termination_entries("postgresql_array")

        # Normal entries should remain
        assert maintenance_cache.exists("normal_hash_1", "zipcode"), "Normal entry should remain"
        assert maintenance_cache.exists("normal_hash_2", "zipcode"), "Normal entry should remain"

        # Termination entries should be gone
        assert not maintenance_cache.exists("_LIMIT_hash_1", "zipcode"), "_LIMIT_ entry should be removed"
        assert not maintenance_cache.exists("_TIMEOUT_hash_2", "zipcode"), "_TIMEOUT_ entry should be removed"
        assert not maintenance_cache.exists("_LIMIT_hash_3", "zipcode"), "_LIMIT_ entry should be removed"

    def test_cleanup_termination_no_termination_entries(self, maintenance_cache):
        """Cleanup should be safe when no termination entries exist."""
        maintenance_cache.set_cache("normal_hash_1", {1001, 1002}, "zipcode")
        maintenance_cache.set_cache("normal_hash_2", {90210}, "zipcode")

        from partitioncache.cli.manage_cache import remove_termination_entries

        os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"
        # Should not raise
        remove_termination_entries("postgresql_array")

        keys_after = maintenance_cache.get_all_keys("zipcode")
        assert len(keys_after) == 2, "All normal entries should remain"


class TestCleanupLargeEntries:
    """Tests for removing entries exceeding a size threshold."""

    def test_remove_large_entries(self, maintenance_cache):
        """Cleanup should remove entries with more than max_entries items."""
        # Add small entry (2 items)
        maintenance_cache.set_cache("small_hash", {1001, 1002}, "zipcode")

        # Add medium entry (5 items)
        maintenance_cache.set_cache("medium_hash", {1001, 1002, 90210, 10001, 10002}, "zipcode")

        # Add large entry (10 items)
        maintenance_cache.set_cache("large_hash", set(range(1, 11)), "zipcode")

        from partitioncache.cli.manage_cache import remove_large_entries

        os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"
        # Remove entries with more than 5 items
        remove_large_entries("postgresql_array", max_entries=5)

        # Small and medium should remain
        assert maintenance_cache.exists("small_hash", "zipcode"), "Small entry should remain"
        assert maintenance_cache.exists("medium_hash", "zipcode"), "Medium entry should remain"

        # Large should be removed
        assert not maintenance_cache.exists("large_hash", "zipcode"), "Large entry should be removed"

    def test_remove_large_entries_keeps_equal_size(self, maintenance_cache):
        """Entries at exactly the threshold should be kept."""
        maintenance_cache.set_cache("exact_hash", {1, 2, 3, 4, 5}, "zipcode")
        maintenance_cache.set_cache("over_hash", {1, 2, 3, 4, 5, 6}, "zipcode")

        from partitioncache.cli.manage_cache import remove_large_entries

        os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"
        remove_large_entries("postgresql_array", max_entries=5)

        assert maintenance_cache.exists("exact_hash", "zipcode"), "Exact-size entry should remain"
        assert not maintenance_cache.exists("over_hash", "zipcode"), "Over-size entry should be removed"


class TestEviction:
    """Tests for cache eviction strategies."""

    def test_evict_oldest_strategy(self, maintenance_cache, db_session):
        """Eviction with 'oldest' strategy should remove entries with oldest last_seen."""
        # Populate with multiple entries and set different ages
        hashes = []
        for i in range(10):
            h = f"evict_oldest_hash_{i}"
            maintenance_cache.set_cache(h, {i + 1000}, "zipcode")
            maintenance_cache.set_query(h, f"SELECT * FROM t WHERE z = {i}", "zipcode")
            hashes.append(h)

        # Age the first 5 entries
        with db_session.cursor() as cur:
            for i, h in enumerate(hashes[:5]):
                cur.execute(
                    "UPDATE maint_test_cache_queries SET last_seen = NOW() - INTERVAL '%s days' WHERE query_hash = %s AND partition_key = 'zipcode'",
                    (30 + i, h),
                )

        keys_before = len(maintenance_cache.get_all_keys("zipcode"))
        assert keys_before == 10

        from partitioncache.cli.manage_cache import evict_cache_manual

        os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"
        # Evict to keep only 5 entries
        evict_cache_manual("postgresql_array", strategy="oldest", threshold=5)

        keys_after = maintenance_cache.get_all_keys("zipcode")
        assert len(keys_after) <= 5, f"Should have at most 5 entries after eviction, got {len(keys_after)}"

        # The newer entries should remain
        for h in hashes[5:]:
            assert maintenance_cache.exists(h, "zipcode"), f"Newer entry {h} should remain"

    def test_evict_largest_strategy(self, maintenance_cache, db_session):
        """Eviction with 'largest' strategy should remove entries with most partition keys."""
        # Create entries of different sizes
        maintenance_cache.set_cache("small_1", {1001}, "zipcode")
        maintenance_cache.set_query("small_1", "SELECT 1", "zipcode")
        maintenance_cache.set_cache("small_2", {1002}, "zipcode")
        maintenance_cache.set_query("small_2", "SELECT 2", "zipcode")
        maintenance_cache.set_cache("medium_1", {1001, 1002, 90210}, "zipcode")
        maintenance_cache.set_query("medium_1", "SELECT 3", "zipcode")
        maintenance_cache.set_cache("large_1", set(range(1, 100)), "zipcode")
        maintenance_cache.set_query("large_1", "SELECT 4", "zipcode")

        keys_before = len(maintenance_cache.get_all_keys("zipcode"))
        assert keys_before == 4

        from partitioncache.cli.manage_cache import evict_cache_manual

        os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"
        # Evict to keep only 2 entries
        evict_cache_manual("postgresql_array", strategy="largest", threshold=2)

        keys_after = maintenance_cache.get_all_keys("zipcode")
        assert len(keys_after) <= 2, f"Should have at most 2 entries, got {len(keys_after)}"

        # The smallest entries should be the ones that remain
        assert maintenance_cache.exists("small_1", "zipcode"), "Smallest entry should survive eviction"
        assert maintenance_cache.exists("small_2", "zipcode"), "Smallest entry should survive eviction"

    def test_evict_below_threshold_is_noop(self, maintenance_cache):
        """Eviction should do nothing when already below threshold."""
        maintenance_cache.set_cache("only_hash", {1001}, "zipcode")
        maintenance_cache.set_query("only_hash", "SELECT 1", "zipcode")

        from partitioncache.cli.manage_cache import evict_cache_manual

        os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"
        evict_cache_manual("postgresql_array", strategy="oldest", threshold=10)

        assert maintenance_cache.exists("only_hash", "zipcode"), "Entry should remain when below threshold"


class TestPartitionDeletion:
    """Tests for deleting entire partitions."""

    def test_delete_partition_removes_all_data(self, maintenance_cache):
        """Deleting a partition should remove the table, queries, and metadata."""
        # Populate both partitions
        maintenance_cache.set_cache("zip_hash_1", {1001, 1002}, "zipcode")
        maintenance_cache.set_query("zip_hash_1", "SELECT 1", "zipcode")
        maintenance_cache.set_cache("region_hash_1", {"northeast"}, "region")
        maintenance_cache.set_query("region_hash_1", "SELECT 2", "region")

        # Delete zipcode partition
        success = maintenance_cache.delete_partition("zipcode")
        assert success is True, "Partition deletion should succeed"

        # Zipcode data should be gone
        assert maintenance_cache._get_partition_datatype("zipcode") is None, "Partition metadata should be removed"

        # Region partition should be unaffected
        assert maintenance_cache.exists("region_hash_1", "region"), "Other partition should be unaffected"

    def test_delete_nonexistent_partition(self, maintenance_cache):
        """Deleting a non-existent partition should return False."""
        result = maintenance_cache.delete_partition("nonexistent_partition")
        assert result is False, "Should return False for non-existent partition"

    def test_delete_partition_via_cli(self, maintenance_cache):
        """Test partition deletion via CLI subprocess."""
        # Populate data
        maintenance_cache.set_cache("cli_hash", {1001}, "zipcode")

        env = os.environ.copy()
        env["CACHE_BACKEND"] = "postgresql_array"
        env["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"
        env["DB_HOST"] = os.getenv("PG_HOST", os.getenv("DB_HOST", "localhost"))
        env["DB_PORT"] = os.getenv("PG_PORT", os.getenv("DB_PORT", "5432"))
        env["DB_USER"] = os.getenv("PG_USER", os.getenv("DB_USER", "integration_user"))
        env["DB_PASSWORD"] = os.getenv("PG_PASSWORD", os.getenv("DB_PASSWORD", "integration_password"))
        env["DB_NAME"] = os.getenv("PG_DBNAME", os.getenv("DB_NAME", "partitioncache_integration"))

        result = subprocess.run(
            [
                "python",
                "-m",
                "partitioncache.cli.manage_cache",
                "maintenance",
                "partition",
                "--delete",
                "zipcode",
                "--type",
                "postgresql_array",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )

        assert result.returncode == 0, f"CLI partition delete failed: {result.stderr}"
        # Verify partition was actually deleted
        # CLI runs in a subprocess, so invalidate local in-memory datatype cache first.
        maintenance_cache._cached_datatype.pop("zipcode", None)
        assert maintenance_cache._get_partition_datatype("zipcode") is None, "Partition should be deleted after CLI operation"


class TestMaintenanceCLI:
    """Test maintenance operations via CLI subprocess."""

    @pytest.fixture(autouse=True)
    def cli_env(self):
        """Provide consistent CLI environment."""
        self.env = os.environ.copy()
        self.env["CACHE_BACKEND"] = "postgresql_array"
        self.env["PG_ARRAY_CACHE_TABLE_PREFIX"] = "maint_test_cache"
        self.env["DB_HOST"] = os.getenv("PG_HOST", os.getenv("DB_HOST", "localhost"))
        self.env["DB_PORT"] = os.getenv("PG_PORT", os.getenv("DB_PORT", "5432"))
        self.env["DB_USER"] = os.getenv("PG_USER", os.getenv("DB_USER", "integration_user"))
        self.env["DB_PASSWORD"] = os.getenv("PG_PASSWORD", os.getenv("DB_PASSWORD", "integration_password"))
        self.env["DB_NAME"] = os.getenv("PG_DBNAME", os.getenv("DB_NAME", "partitioncache_integration"))

    def test_maintenance_help(self):
        """Maintenance subcommand should show help."""
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "maintenance", "--help"],
            capture_output=True,
            text=True,
            env=self.env,
            timeout=30,
        )
        assert result.returncode == 0
        assert "prune" in result.stdout.lower()
        assert "cleanup" in result.stdout.lower()
        assert "evict" in result.stdout.lower()

    def test_prune_cli(self, maintenance_cache):
        """Test prune via CLI."""
        _populate_cache(maintenance_cache)

        result = subprocess.run(
            [
                "python",
                "-m",
                "partitioncache.cli.manage_cache",
                "maintenance",
                "prune",
                "--days",
                "30",
                "--type",
                "postgresql_array",
            ],
            capture_output=True,
            text=True,
            env=self.env,
            timeout=30,
        )
        assert result.returncode == 0, f"CLI prune failed: {result.stderr}"
        # Prune with 30 day window should not remove recently created entries
        keys_after = maintenance_cache.get_all_keys("zipcode")
        assert len(keys_after) > 0, "Recently created entries should survive prune with 30 day window"

    def test_cleanup_remove_termination_cli(self, maintenance_cache):
        """Test cleanup --remove-termination via CLI."""
        maintenance_cache.set_cache("_LIMIT_test", {1001}, "zipcode")
        maintenance_cache.set_cache("normal_test", {1002}, "zipcode")

        result = subprocess.run(
            [
                "python",
                "-m",
                "partitioncache.cli.manage_cache",
                "maintenance",
                "cleanup",
                "--remove-termination",
                "--type",
                "postgresql_array",
            ],
            capture_output=True,
            text=True,
            env=self.env,
            timeout=30,
        )
        assert result.returncode == 0, f"CLI cleanup failed: {result.stderr}"
        assert not maintenance_cache.exists("_LIMIT_test", "zipcode"), "_LIMIT_ entry should be removed by CLI cleanup"
        assert maintenance_cache.exists("normal_test", "zipcode"), "Normal entry should survive CLI cleanup"

    def test_cleanup_remove_large_cli(self, maintenance_cache):
        """Test cleanup --remove-large via CLI."""
        maintenance_cache.set_cache("small_cli", {1001}, "zipcode")
        maintenance_cache.set_cache("big_cli", set(range(1, 20)), "zipcode")

        result = subprocess.run(
            [
                "python",
                "-m",
                "partitioncache.cli.manage_cache",
                "maintenance",
                "cleanup",
                "--remove-large",
                "5",
                "--type",
                "postgresql_array",
            ],
            capture_output=True,
            text=True,
            env=self.env,
            timeout=30,
        )
        assert result.returncode == 0, f"CLI cleanup failed: {result.stderr}"
        assert maintenance_cache.exists("small_cli", "zipcode"), "Small entry should survive CLI cleanup"
        assert not maintenance_cache.exists("big_cli", "zipcode"), "Large entry should be removed by CLI cleanup"

    def test_evict_cli(self, maintenance_cache, db_session):
        """Test evict via CLI."""
        for i in range(5):
            h = f"evict_cli_{i}"
            maintenance_cache.set_cache(h, {i + 1000}, "zipcode")
            maintenance_cache.set_query(h, f"SELECT {i}", "zipcode")

        result = subprocess.run(
            [
                "python",
                "-m",
                "partitioncache.cli.manage_cache",
                "maintenance",
                "evict",
                "--strategy",
                "oldest",
                "--threshold",
                "2",
                "--type",
                "postgresql_array",
            ],
            capture_output=True,
            text=True,
            env=self.env,
            timeout=30,
        )
        assert result.returncode == 0, f"CLI evict failed: {result.stderr}"
        keys_after = maintenance_cache.get_all_keys("zipcode")
        assert len(keys_after) <= 2, f"Should have at most 2 entries after eviction, got {len(keys_after)}"
