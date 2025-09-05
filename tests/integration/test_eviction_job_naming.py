"""Integration tests for eviction manager job naming functionality."""

import logging

import pytest

from partitioncache.cli.postgresql_cache_eviction import construct_eviction_job_name


class TestEvictionJobNaming:
    """Test suite for eviction manager job naming and conflict prevention."""

    def test_job_name_construction_variations(self):
        """Test eviction job name construction with various database names and table prefixes."""
        # Test cases: (database_name, table_prefix, expected_job_name)
        test_cases = [
            # Simple database names
            ("mydb", None, "partitioncache_evict_mydb"),
            ("mydb", "partitioncache", "partitioncache_evict_mydb_default"),
            ("mydb", "partitioncache_cache1", "partitioncache_evict_mydb_cache1"),
            ("mydb", "custom_prefix", "partitioncache_evict_mydb_customprefix"),
            # Database names with underscores
            ("my_app_db", None, "partitioncache_evict_my_app_db"),
            ("my_app_db", "partitioncache", "partitioncache_evict_my_app_db_default"),
            # Edge cases
            ("db", "partitioncache_", "partitioncache_evict_db_default"),
            ("db", "_", "partitioncache_evict_db_custom"),
            # Long names are now preserved without truncation
            (
                "very_long_database_name_that_exceeds_limit",
                "partitioncache_with_long_suffix",
                "partitioncache_evict_very_long_database_name_that_exceeds_limit_withlongsuffix",
            ),
        ]

        for db_name, table_prefix, expected_name in test_cases:
            actual_name = construct_eviction_job_name(db_name, table_prefix)

            # All names should match exactly (no truncation)
            assert actual_name == expected_name, f"Mismatch for ({db_name}, {table_prefix}): expected {expected_name}, got {actual_name}"

    def test_job_name_uniqueness_across_databases(self):
        """Test that different databases get unique job names even with same table prefix."""
        databases = ["db1", "db2", "production", "staging"]
        table_prefix = "partitioncache_cache"

        job_names = []
        for db in databases:
            job_name = construct_eviction_job_name(db, table_prefix)
            job_names.append(job_name)

        # All job names should be unique
        assert len(job_names) == len(set(job_names)), f"Duplicate job names found: {job_names}"

        # All should contain the database name
        for db, job_name in zip(databases, job_names, strict=False):
            assert db in job_name, f"Database '{db}' not in job name '{job_name}'"

    def test_job_name_uniqueness_same_database_different_prefixes(self):
        """Test that same database with different prefixes gets unique job names."""
        database = "testdb"
        prefixes = [None, "partitioncache", "partitioncache_cache1", "partitioncache_cache2", "custom_cache", "another_prefix"]

        job_names = []
        for prefix in prefixes:
            job_name = construct_eviction_job_name(database, prefix)
            job_names.append(job_name)

        # All job names should be unique
        assert len(job_names) == len(set(job_names)), f"Duplicate job names found: {job_names}"

    def test_job_name_no_length_limit(self):
        """Test that long job names are preserved without truncation."""
        # Create a really long database name
        long_db = "extremely_long_database_name_that_will_definitely_exceed_previous_postgresql_limit"
        long_prefix = "partitioncache_another_very_long_suffix_here"

        job_name = construct_eviction_job_name(long_db, long_prefix)

        # Check the full job name is preserved (no truncation)
        expected_suffix = long_prefix.replace("partitioncache", "").replace("_", "") or "default"
        expected_name = f"partitioncache_evict_{long_db}_{expected_suffix}"
        assert job_name == expected_name, f"Job name was unexpectedly modified: {job_name}"

        # Verify it's longer than the old 63-char "limit"
        assert len(job_name) > 63, f"Job name should be longer than 63 chars: {len(job_name)}"

    @pytest.mark.integration
    def test_sql_function_consistency(self, db_session):
        """Test that SQL and Python functions produce the same job names."""
        from partitioncache.queue import get_queue_provider_name

        # Only run for PostgreSQL queue provider (eviction uses same backend)
        if get_queue_provider_name() != "postgresql":
            pytest.skip("This test is specific to PostgreSQL eviction manager")

        # Test cases for consistency check
        test_cases = [
            ("testdb", "partitioncache"),
            ("testdb", "partitioncache_cache1"),
            ("testdb", "custom_prefix"),
            ("testdb", None),
        ]

        for db_name, table_prefix in test_cases:
            # Get Python result
            python_job_name = construct_eviction_job_name(db_name, table_prefix)

            # Get SQL result
            with db_session.cursor() as cur:
                if table_prefix:
                    cur.execute("SELECT partitioncache_construct_eviction_job_name(%s, %s)", (db_name, table_prefix))
                else:
                    cur.execute("SELECT partitioncache_construct_eviction_job_name(%s, NULL)", (db_name,))
                sql_job_name = cur.fetchone()[0]

            # Both should produce the same result
            assert python_job_name == sql_job_name, f"Mismatch for ({db_name}, {table_prefix}): Python={python_job_name}, SQL={sql_job_name}"

    @pytest.mark.integration
    @pytest.mark.slow
    def test_multi_database_eviction_no_conflicts(self, db_session):
        """Test that multiple databases can have eviction managers without job name conflicts."""
        from partitioncache.cli.postgresql_cache_eviction import get_table_prefix
        from partitioncache.queue import get_queue_provider_name

        # Only run for PostgreSQL queue provider
        if get_queue_provider_name() != "postgresql":
            pytest.skip("This test is specific to PostgreSQL eviction manager")

        # Get table prefix from environment
        try:
            from argparse import Namespace

            args = Namespace(table_prefix=None)
            table_prefix = get_table_prefix(args)
        except Exception:
            table_prefix = "partitioncache"

        config_table = f"{table_prefix}_eviction_config"

        # Simulate multiple databases with same table prefix
        test_databases = ["evict_db1", "evict_db2", "evict_db3"]

        # Generate job configurations for each database
        configs = []
        for db in test_databases:
            job_name = construct_eviction_job_name(db, table_prefix)
            configs.append({"job_name": job_name, "target_database": db, "table_prefix": table_prefix})

        try:
            # Create config table if not exists
            with db_session.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {config_table} (
                        job_name TEXT PRIMARY KEY,
                        enabled BOOLEAN NOT NULL DEFAULT false,
                        frequency_minutes INTEGER NOT NULL DEFAULT 60,
                        strategy TEXT NOT NULL DEFAULT 'oldest',
                        threshold INTEGER NOT NULL DEFAULT 1000,
                        table_prefix TEXT NOT NULL,
                        target_database TEXT NOT NULL,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            db_session.commit()

            # Insert all configurations
            for config in configs:
                with db_session.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {config_table}
                        (job_name, table_prefix, frequency_minutes, enabled, strategy, threshold, target_database)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (job_name) DO NOTHING
                    """,
                        (config["job_name"], config["table_prefix"], 60, False, "oldest", 1000, config["target_database"]),
                    )
            db_session.commit()

            # Verify all were inserted successfully
            with db_session.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT job_name, target_database
                    FROM {config_table}
                    WHERE job_name IN %s
                """,
                    (tuple(c["job_name"] for c in configs),),
                )

                results = cur.fetchall()

                # Should have one entry per database
                assert len(results) == len(test_databases), f"Expected {len(test_databases)} configs, got {len(results)}"

                # Each should have unique job name
                job_names = [r[0] for r in results]
                assert len(job_names) == len(set(job_names)), f"Duplicate job names found: {job_names}"

                # Each should reference correct database
                for job_name, target_db in results:
                    assert target_db in job_name, f"Job name '{job_name}' doesn't contain database '{target_db}'"

        finally:
            # Cleanup test data
            with db_session.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM {config_table}
                    WHERE job_name IN %s
                """,
                    (tuple(c["job_name"] for c in configs),),
                )
            db_session.commit()
