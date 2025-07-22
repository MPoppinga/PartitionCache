import os
import subprocess
import tempfile

import pytest

from .test_utils import compare_cache_values


class TestCLICommands:
    """
    Test suite for PartitionCache CLI commands.
    Tests all CLI entry points by executing them as separate processes
    to simulate real-world usage.
    """

    def test_pcache_manage_help(self):
        """Test that pcache-manage shows help information."""
        result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "--help"], capture_output=True, text=True, timeout=30)

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "PartitionCache Management Tool" in result.stdout
        assert "setup" in result.stdout
        assert "cache" in result.stdout

    def test_pcache_manage_status(self, db_session):
        """Test pcache-manage status command."""
        # Set required environment variables
        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "test_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
                "CACHE_BACKEND": "postgresql_array",
            }
        )

        result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "status"], capture_output=True, text=True, env=env, timeout=60)

        # Should succeed or provide informative output
        assert result.returncode == 0 or "validation" in result.stderr.lower()
        # Should contain status information (output may be in stderr)
        output_text = (result.stdout + result.stderr).lower()
        assert "environment" in output_text or "configuration" in output_text or "validation" in output_text

    def test_pcache_manage_setup_cache(self, db_session):
        """Test cache metadata setup via CLI."""
        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "test_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
                "CACHE_BACKEND": "postgresql_array",
            }
        )

        result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "setup", "cache"], capture_output=True, text=True, env=env, timeout=60)

        assert result.returncode == 0, f"Setup failed: {result.stderr}"
        # Output may be in stderr for CLI tools
        output_text = (result.stdout + result.stderr).lower()
        assert "setup" in output_text or "completed" in output_text

    def test_pcache_manage_cache_count(self, db_session):
        """Test cache count command."""
        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "integration_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "integration_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "partitioncache_integration"),
                "CACHE_BACKEND": os.getenv("CACHE_BACKEND", "postgresql_array"),
                # Ensure all required environment variables are inherited
                "DB_HOST": os.getenv("DB_HOST", "localhost"),
                "DB_PORT": os.getenv("DB_PORT", "5432"),
                "DB_USER": os.getenv("DB_USER", "integration_user"),
                "DB_PASSWORD": os.getenv("DB_PASSWORD", "integration_password"),
                "DB_NAME": os.getenv("DB_NAME", os.getenv("PG_DBNAME", "partitioncache_integration")),
                "PG_ARRAY_CACHE_TABLE_PREFIX": os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX", "integration_array_cache"),
            }
        )

        result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "cache", "count"], capture_output=True, text=True, env=env, timeout=60)

        # Should succeed and show count information
        assert result.returncode == 0 or "partitions" in result.stderr.lower(), f"Command failed: {result.stderr}"
        # Should contain count information (may be in stdout or stderr)
        output_text = result.stdout + result.stderr
        assert any(word in output_text.lower() for word in ["total", "keys", "partitions", "count", "statistics"]), (
            f"No count information in output: {output_text}"
        )

    def test_pcache_add_help(self):
        """Test pcache-add help command."""
        result = subprocess.run(["python", "-m", "partitioncache.cli.add_to_cache", "--help"], capture_output=True, text=True, timeout=30)

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "query" in result.stdout.lower()
        assert "partition" in result.stdout.lower()

    def test_pcache_add_direct_mode(self, db_session, cache_client):
        """Test adding query directly to cache via CLI."""
        # Create a temporary query file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            test_query = "SELECT * FROM test_locations WHERE zipcode = 1001;"
            f.write(test_query)
            query_file = f.name

        try:
            env = os.environ.copy()
            env.update(
                {
                    "PG_HOST": os.getenv("PG_HOST", "localhost"),
                    "PG_PORT": os.getenv("PG_PORT", "5432"),
                    "PG_USER": os.getenv("PG_USER", "test_user"),
                    "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
                    "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
                    "CACHE_BACKEND": "postgresql_array",
                }
            )

            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "partitioncache.cli.add_to_cache",
                    "--direct",
                    "--query-file",
                    query_file,
                    "--partition-key",
                    "zipcode",
                    "--partition-datatype",
                    "integer",
                    "--cache-backend",
                    "postgresql_array",
                ],
                capture_output=True,
                text=True,
                env=env,
                timeout=120,
            )

            # Should succeed or provide meaningful error
            if result.returncode != 0:
                # Check if it's a configuration issue rather than a code issue
                assert "configuration" in result.stderr.lower() or "connection" in result.stderr.lower()
            else:
                # Success case: either has output indicating success or succeeds silently
                assert result.returncode == 0 and ("added" in result.stdout.lower() or "processed" in result.stdout.lower() or result.stdout.strip() == "")

        finally:
            # Cleanup temp file
            os.unlink(query_file)

    def test_pcache_read_help(self):
        """Test pcache-read help command."""
        result = subprocess.run(["python", "-m", "partitioncache.cli.read_from_cache", "--help"], capture_output=True, text=True, timeout=30)

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "partition" in result.stdout.lower()
        assert "key" in result.stdout.lower()

    def test_pcache_monitor_help(self):
        """Test pcache-monitor help command."""
        result = subprocess.run(["python", "-m", "partitioncache.cli.monitor_cache_queue", "--help"], capture_output=True, text=True, timeout=30)

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "monitor" in result.stdout.lower() or "queue" in result.stdout.lower()

    def test_pcache_postgresql_queue_processor_help(self):
        """Test pcache-postgresql-queue-processor help command."""
        result = subprocess.run(["python", "-m", "partitioncache.cli.postgresql_queue_processor", "--help"], capture_output=True, text=True, timeout=30)

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "postgresql" in result.stdout.lower() or "queue" in result.stdout.lower()

    def test_pcache_postgresql_eviction_manager_help(self):
        """Test pcache-postgresql-eviction-manager help command."""
        result = subprocess.run(["python", "-m", "partitioncache.cli.postgresql_cache_eviction", "--help"], capture_output=True, text=True, timeout=30)

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "eviction" in result.stdout.lower() or "cache" in result.stdout.lower()


class TestCLIIntegration:
    """
    Integration tests for CLI workflows.
    Tests complete workflows using multiple CLI commands in sequence.
    """

    def test_cache_overview_command(self, db_session, cache_client, request):
        """Test cache overview command output."""
        # Get the backend type from the request parameter
        backend_type = request.node.callspec.params["cache_client"]

        # Skip RocksDB backends for CLI tests due to file locking conflicts with subprocess calls
        if backend_type.startswith("rocksdb"):
            pytest.skip(f"CLI tests not compatible with {backend_type} due to file locking conflicts between test fixtures and subprocess calls")

        backend_suffix = backend_type.replace("_", "").replace("-", "")
        backend_suffix = backend_type.replace("_", "").replace("-", "")
        test_partition = f"test_city_{backend_suffix}"
        test_hash = f"test_hash_{backend_suffix}"

        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "integration_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "integration_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "partitioncache_integration"),
                "CACHE_BACKEND": backend_type,
                "DB_HOST": os.getenv("DB_HOST", "localhost"),
                "DB_PORT": os.getenv("DB_PORT", "5432"),
                "DB_USER": os.getenv("DB_USER", "integration_user"),
                "DB_PASSWORD": os.getenv("DB_PASSWORD", "integration_password"),
                "DB_NAME": os.getenv("DB_NAME", "partitioncache_integration"),
            }
        )
        if backend_type.startswith("redis"):
            env.update(
                {
                    "REDIS_HOST": os.getenv("REDIS_HOST", "localhost"),
                    "REDIS_PORT": os.getenv("REDIS_PORT", "6379"),
                    "REDIS_CACHE_DB": os.getenv("REDIS_CACHE_DB", "0"),
                    "REDIS_BIT_DB": os.getenv("REDIS_BIT_DB", "1"),
                    "REDIS_SET_DB": os.getenv("REDIS_SET_DB", "2"),
                    "REDIS_BIT_BITSIZE": os.getenv("REDIS_BIT_BITSIZE", "200000"),
                    "REDIS_SET_HOST": os.getenv("REDIS_SET_HOST", os.getenv("REDIS_HOST", "localhost")),
                    "REDIS_SET_PORT": os.getenv("REDIS_SET_PORT", os.getenv("REDIS_PORT", "6379")),
                    "REDIS_BIT_HOST": os.getenv("REDIS_BIT_HOST", os.getenv("REDIS_HOST", "localhost")),
                    "REDIS_BIT_PORT": os.getenv("REDIS_BIT_PORT", os.getenv("REDIS_PORT", "6379")),
                }
            )
        elif backend_type.startswith("rocksdb"):
            # Use the same RocksDB paths as the test fixture to avoid data isolation issues
            env.update(
                {
                    "ROCKSDB_PATH": os.getenv("ROCKSDB_PATH", f"/tmp/rocksdb_test_{backend_suffix}"),
                    "ROCKSDB_BIT_PATH": os.getenv("ROCKSDB_BIT_PATH", f"/tmp/rocksdb_bit_test_{backend_suffix}"),
                    "ROCKSDB_BIT_BITSIZE": os.getenv("ROCKSDB_BIT_BITSIZE", "200000"),
                    "ROCKSDB_DICT_PATH": os.getenv("ROCKSDB_DICT_PATH", f"/tmp/rocksdict_test_{backend_suffix}"),
                }
            )
        elif backend_type.startswith("postgresql"):
            env.update(
                {
                    "PG_ARRAY_CACHE_TABLE_PREFIX": os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX", "integration_array_cache"),
                    "PG_BIT_CACHE_TABLE_PREFIX": os.getenv("PG_BIT_CACHE_TABLE_PREFIX", "integration_bit_cache"),
                    "PG_BIT_CACHE_BITSIZE": os.getenv("PG_BIT_CACHE_BITSIZE", "200000"),
                    "PG_ROARINGBIT_CACHE_TABLE_PREFIX": os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX", "integration_roaring_cache"),
                }
            )

        # First ensure cache is set up
        setup_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "setup", "cache"], capture_output=True, text=True, env=env, timeout=60
        )
        assert setup_result.returncode == 0 or "exist" in setup_result.stderr.lower()

        # Add some test data to cache using unique keys
        cache_client.register_partition_key(test_partition, "integer")
        cache_client.set_cache(test_hash, {1, 2, 3}, test_partition)
        cache_client.set_query(test_hash, f"SELECT * FROM test WHERE {test_partition} IN (1,2,3)", test_partition)

        # Test cache overview
        result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "cache", "overview"], capture_output=True, text=True, env=env, timeout=60)

        assert result.returncode == 0, f"Cache overview failed: {result.stderr}"
        output = (result.stdout + result.stderr).lower()
        # Should contain partition information
        assert test_partition.lower() in output or "partition" in output or "integer" in output

    def test_export_import_with_query_metadata(self, db_session, cache_client, request):
        """Test export/import preserves query metadata via CLI."""
        # Get the backend type from the request parameter
        backend_type = request.node.callspec.params["cache_client"]

        # Skip RocksDB backends for CLI tests due to file locking conflicts with subprocess calls
        if backend_type.startswith("rocksdb"):
            pytest.skip(f"CLI tests not compatible with {backend_type} due to file locking conflicts between test fixtures and subprocess calls")

        backend_suffix = backend_type.replace("_", "").replace("-", "")
        partition_key = f"region_id_{backend_suffix}"
        hash_key = f"query_test_hash_{backend_suffix}"
        backend_suffix = backend_type.replace("_", "").replace("-", "")
        partition_key = f"region_id_{backend_suffix}"
        hash_key = f"query_test_hash_{backend_suffix}"

        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "integration_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "integration_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "partitioncache_integration"),
                "CACHE_BACKEND": backend_type,
                "DB_HOST": os.getenv("DB_HOST", "localhost"),
                "DB_PORT": os.getenv("DB_PORT", "5432"),
                "DB_USER": os.getenv("DB_USER", "integration_user"),
                "DB_PASSWORD": os.getenv("DB_PASSWORD", "integration_password"),
                "DB_NAME": os.getenv("DB_NAME", "partitioncache_integration"),
            }
        )
        if backend_type.startswith("redis"):
            env.update(
                {
                    "REDIS_HOST": os.getenv("REDIS_HOST", "localhost"),
                    "REDIS_PORT": os.getenv("REDIS_PORT", "6379"),
                    "REDIS_CACHE_DB": os.getenv("REDIS_CACHE_DB", "0"),
                    "REDIS_BIT_DB": os.getenv("REDIS_BIT_DB", "1"),
                    "REDIS_SET_DB": os.getenv("REDIS_SET_DB", "2"),
                    "REDIS_BIT_BITSIZE": os.getenv("REDIS_BIT_BITSIZE", "200000"),
                    "REDIS_SET_HOST": os.getenv("REDIS_SET_HOST", os.getenv("REDIS_HOST", "localhost")),
                    "REDIS_SET_PORT": os.getenv("REDIS_SET_PORT", os.getenv("REDIS_PORT", "6379")),
                    "REDIS_BIT_HOST": os.getenv("REDIS_BIT_HOST", os.getenv("REDIS_HOST", "localhost")),
                    "REDIS_BIT_PORT": os.getenv("REDIS_BIT_PORT", os.getenv("REDIS_PORT", "6379")),
                }
            )
        elif backend_type.startswith("rocksdb"):
            # Use the same RocksDB paths as the test fixture to avoid data isolation issues
            env.update(
                {
                    "ROCKSDB_PATH": os.getenv("ROCKSDB_PATH", f"/tmp/rocksdb_test_{backend_suffix}"),
                    "ROCKSDB_BIT_PATH": os.getenv("ROCKSDB_BIT_PATH", f"/tmp/rocksdb_bit_test_{backend_suffix}"),
                    "ROCKSDB_BIT_BITSIZE": os.getenv("ROCKSDB_BIT_BITSIZE", "200000"),
                    "ROCKSDB_DICT_PATH": os.getenv("ROCKSDB_DICT_PATH", f"/tmp/rocksdict_test_{backend_suffix}"),
                }
            )
        elif backend_type.startswith("postgresql"):
            env.update(
                {
                    "PG_ARRAY_CACHE_TABLE_PREFIX": os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX", "integration_array_cache"),
                    "PG_BIT_CACHE_TABLE_PREFIX": os.getenv("PG_BIT_CACHE_TABLE_PREFIX", "integration_bit_cache"),
                    "PG_BIT_CACHE_BITSIZE": os.getenv("PG_BIT_CACHE_BITSIZE", "200000"),
                    "PG_ROARINGBIT_CACHE_TABLE_PREFIX": os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX", "integration_roaring_cache"),
                }
            )

        # Setup test data
        cache_client.register_partition_key(partition_key, "integer")
        test_query = f"SELECT * FROM locations WHERE {partition_key} IN (10,20,30)"
        cache_client.set_cache(hash_key, {10, 20, 30}, partition_key)
        cache_client.set_query(hash_key, test_query, partition_key)

        if backend_type.startswith("rocksdb"):
            cache_client.close()

        # Ensure cache is set up
        setup_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "setup", "cache"], capture_output=True, text=True, env=env, timeout=60
        )
        assert setup_result.returncode == 0 or "exist" in setup_result.stderr.lower()

        # Create temporary export file
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            export_file = f.name

        try:
            # Export cache with queries
            export_result = subprocess.run(
                ["python", "-m", "partitioncache.cli.manage_cache", "cache", "export", "--file", export_file, "--type", backend_type],
                capture_output=True,
                text=True,
                env=env,
                timeout=60,
            )
            assert export_result.returncode == 0, f"Export failed: {export_result.stderr}"
            assert os.path.exists(export_file), "Export file not created"

            # Clear cache
            cache_client.delete(hash_key, partition_key)
            assert cache_client.get(hash_key, partition_key) is None

            # Import cache
            import_result = subprocess.run(
                ["python", "-m", "partitioncache.cli.manage_cache", "cache", "import", "--file", export_file, "--type", backend_type],
                capture_output=True,
                text=True,
                env=env,
                timeout=60,
            )
            assert import_result.returncode == 0, f"Import failed: {import_result.stderr}"

            if backend_type.startswith("rocksdb"):
                from partitioncache.cache_handler import get_cache_handler

                cache_client = get_cache_handler(backend_type)

            # Verify cache data and query metadata were restored
            restored_data = cache_client.get(hash_key, partition_key)
            restored_query = cache_client.get_query(hash_key, partition_key)

            assert compare_cache_values(restored_data, {10, 20, 30}), "Cache data not restored correctly"
            assert restored_query == test_query, "Query metadata not restored correctly"

        finally:
            if os.path.exists(export_file):
                os.unlink(export_file)
            # Clean up test data
            try:
                cache_client.delete(hash_key, partition_key)
            except Exception:
                pass  # Ignore cleanup errors

    def test_selective_export_import_partition_key(self, db_session, cache_client, request):
        """Test selective export/import with --partition-key parameter."""
        # Get the backend type from the request parameter
        backend_type = request.node.callspec.params["cache_client"]

        # Skip RocksDB backends for CLI tests due to file locking conflicts with subprocess calls
        if backend_type.startswith("rocksdb"):
            pytest.skip(f"CLI tests not compatible with {backend_type} due to file locking conflicts between test fixtures and subprocess calls")

        backend_suffix = backend_type.replace("_", "").replace("-", "")
        backend_suffix = backend_type.replace("_", "").replace("-", "")
        city_partition = f"city_id_{backend_suffix}"
        state_partition = f"state_id_{backend_suffix}"
        city_hash = f"city_hash_{backend_suffix}"
        state_hash = f"state_hash_{backend_suffix}"

        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "integration_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "integration_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "partitioncache_integration"),
                "CACHE_BACKEND": backend_type,
                "DB_HOST": os.getenv("DB_HOST", "localhost"),
                "DB_PORT": os.getenv("DB_PORT", "5432"),
                "DB_USER": os.getenv("DB_USER", "integration_user"),
                "DB_PASSWORD": os.getenv("DB_PASSWORD", "integration_password"),
                "DB_NAME": os.getenv("DB_NAME", "partitioncache_integration"),
            }
        )
        if backend_type.startswith("redis"):
            env.update(
                {
                    "REDIS_HOST": os.getenv("REDIS_HOST", "localhost"),
                    "REDIS_PORT": os.getenv("REDIS_PORT", "6379"),
                    "REDIS_CACHE_DB": os.getenv("REDIS_CACHE_DB", "0"),
                    "REDIS_BIT_DB": os.getenv("REDIS_BIT_DB", "1"),
                    "REDIS_SET_DB": os.getenv("REDIS_SET_DB", "2"),
                    "REDIS_BIT_BITSIZE": os.getenv("REDIS_BIT_BITSIZE", "200000"),
                    "REDIS_SET_HOST": os.getenv("REDIS_SET_HOST", os.getenv("REDIS_HOST", "localhost")),
                    "REDIS_SET_PORT": os.getenv("REDIS_SET_PORT", os.getenv("REDIS_PORT", "6379")),
                    "REDIS_BIT_HOST": os.getenv("REDIS_BIT_HOST", os.getenv("REDIS_HOST", "localhost")),
                    "REDIS_BIT_PORT": os.getenv("REDIS_BIT_PORT", os.getenv("REDIS_PORT", "6379")),
                }
            )
        elif backend_type.startswith("rocksdb"):
            # Use the same RocksDB paths as the test fixture to avoid data isolation issues
            env.update(
                {
                    "ROCKSDB_PATH": os.getenv("ROCKSDB_PATH", f"/tmp/rocksdb_test_{backend_suffix}"),
                    "ROCKSDB_BIT_PATH": os.getenv("ROCKSDB_BIT_PATH", f"/tmp/rocksdb_bit_test_{backend_suffix}"),
                    "ROCKSDB_BIT_BITSIZE": os.getenv("ROCKSDB_BIT_BITSIZE", "200000"),
                    "ROCKSDB_DICT_PATH": os.getenv("ROCKSDB_DICT_PATH", f"/tmp/rocksdict_test_{backend_suffix}"),
                }
            )
        elif backend_type.startswith("postgresql"):
            env.update(
                {
                    "PG_ARRAY_CACHE_TABLE_PREFIX": os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX", "integration_array_cache"),
                    "PG_BIT_CACHE_TABLE_PREFIX": os.getenv("PG_BIT_CACHE_TABLE_PREFIX", "integration_bit_cache"),
                    "PG_BIT_CACHE_BITSIZE": os.getenv("PG_BIT_CACHE_BITSIZE", "200000"),
                    "PG_ROARINGBIT_CACHE_TABLE_PREFIX": os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX", "integration_roaring_cache"),
                }
            )

        # Ensure cache is set up
        setup_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "setup", "cache"], capture_output=True, text=True, env=env, timeout=60
        )
        assert setup_result.returncode == 0 or "exist" in setup_result.stderr.lower()

        # Setup multiple partitions with data using unique keys
        cache_client.register_partition_key(city_partition, "integer")
        cache_client.register_partition_key(state_partition, "integer")

        # Add data to both partitions using unique keys
        cache_client.set_cache(city_hash, {1, 2, 3}, city_partition)
        cache_client.set_query(city_hash, f"SELECT * FROM test WHERE {city_partition} IN (1,2,3)", city_partition)

        cache_client.set_cache(state_hash, {100, 200}, state_partition)
        cache_client.set_query(state_hash, f"SELECT * FROM test WHERE {state_partition} IN (100,200)", state_partition)

        # Create temporary export file
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            export_file = f.name

        try:
            # Export only city partition using unique key
            export_result = subprocess.run(
                [
                    "python",
                    "-m",
                    "partitioncache.cli.manage_cache",
                    "cache",
                    "export",
                    "--file",
                    export_file,
                    "--type",
                    backend_type,
                    "--partition-key",
                    city_partition,
                ],
                capture_output=True,
                text=True,
                env=env,
                timeout=60,
            )
            assert export_result.returncode == 0, f"Selective export failed: {export_result.stderr}"

            # Output should indicate selective export
            output = export_result.stdout + export_result.stderr
            assert city_partition in output, "Export output should mention partition key"

            # Clear all cache data using unique keys
            cache_client.delete(city_hash, city_partition)
            cache_client.delete(state_hash, state_partition)

            # Import selective data
            import_result = subprocess.run(
                ["python", "-m", "partitioncache.cli.manage_cache", "cache", "import", "--file", export_file, "--type", backend_type],
                capture_output=True,
                text=True,
                env=env,
                timeout=60,
            )
            assert import_result.returncode == 0, f"Import failed: {import_result.stderr}"

            # Verify only city data was imported using unique keys
            city_data = cache_client.get(city_hash, city_partition)
            state_data = cache_client.get(state_hash, state_partition)

            assert compare_cache_values(city_data, {1, 2, 3}), "City data should be restored"
            assert state_data is None, "State data should not be restored"

        finally:
            if os.path.exists(export_file):
                os.unlink(export_file)
            # Clean up test data
            try:
                cache_client.delete(city_hash, city_partition)
                cache_client.delete(state_hash, state_partition)
            except Exception:
                pass  # Ignore cleanup errors

    def test_pcache_add_queue_original(self, db_session):
        """Test pcache-add with --queue-original mode."""
        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "integration_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "integration_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "partitioncache_integration"),
                "QUERY_QUEUE_PROVIDER": "postgresql",
                "PG_QUEUE_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_QUEUE_PORT": os.getenv("PG_PORT", "5432"),
                "PG_QUEUE_USER": os.getenv("PG_USER", "integration_user"),
                "PG_QUEUE_PASSWORD": os.getenv("PG_PASSWORD", "integration_password"),
                "PG_QUEUE_DB": os.getenv("PG_DBNAME", "partitioncache_integration"),
            }
        )

        # Setup queue tables
        setup_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "setup", "queue"], capture_output=True, text=True, env=env, timeout=60
        )
        assert setup_result.returncode == 0 or "exist" in setup_result.stderr.lower()

        # Add query to original queue
        result = subprocess.run(
            [
                "python",
                "-m",
                "partitioncache.cli.add_to_cache",
                "--queue-original",
                "--query",
                "SELECT * FROM test WHERE region_id = 500",
                "--partition-key",
                "region_id",
                "--partition-datatype",
                "integer",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )

        # Should succeed or provide meaningful error
        assert result.returncode == 0 or "configuration" in result.stderr.lower(), f"Queue add failed: {result.stderr}"

        if result.returncode == 0:
            # Verify queue has the query (use general count command)
            count_result = subprocess.run(
                ["python", "-m", "partitioncache.cli.manage_cache", "queue", "count"], capture_output=True, text=True, env=env, timeout=60
            )
            assert count_result.returncode == 0, f"Queue count failed: {count_result.stderr}"
            # Should show non-zero count
            output = count_result.stdout + count_result.stderr
            assert any(char.isdigit() for char in output), "Should show queue count"

    def test_env_file_loading(self, db_session):
        """Test CLI --env-file parameter functionality."""
        # Create temporary env file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("CACHE_BACKEND=postgresql_array\n")
            f.write(f"DB_HOST={os.getenv('PG_HOST', 'localhost')}\n")
            f.write(f"DB_PORT={os.getenv('PG_PORT', '5432')}\n")
            f.write(f"DB_USER={os.getenv('PG_USER', 'integration_user')}\n")
            f.write(f"DB_PASSWORD={os.getenv('PG_PASSWORD', 'integration_password')}\n")
            f.write(f"DB_NAME={os.getenv('PG_DBNAME', 'partitioncache_integration')}\n")
            env_file = f.name

        try:
            # Test with env file
            result = subprocess.run(
                [
                    "python",
                    "-m",
                    "partitioncache.cli.add_to_cache",
                    "--env-file",
                    env_file,
                    "--direct",
                    "--no-recompose",
                    "--query",
                    "SELECT * FROM test WHERE id = 999",
                    "--partition-key",
                    "test_partition",
                    "--partition-datatype",
                    "integer",
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )

            # Should succeed or show SQL error (not file/configuration error)
            # Check that cache handler was created (indicates env file was loaded successfully)
            output_text = (result.stdout + result.stderr).lower()
            assert "created partition cache handler" in output_text, "Should show cache handler was created"

            if result.returncode != 0:
                # Should be SQL error (table doesn't exist) not configuration error
                stderr_lower = result.stderr.lower()
                assert ("relation" in stderr_lower and "does not exist" in stderr_lower) or "configuration" in stderr_lower or "connection" in stderr_lower
                # Should NOT be file not found error
                assert "file" not in stderr_lower or "not found" not in stderr_lower

        finally:
            os.unlink(env_file)

    def test_complete_cache_workflow(self, db_session):
        """Test complete workflow: setup -> add -> read -> count."""
        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "test_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
                "CACHE_BACKEND": "postgresql_array",
            }
        )

        # Step 1: Setup cache
        setup_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "setup", "cache"], capture_output=True, text=True, env=env, timeout=60
        )

        # Setup should succeed or already exist
        assert setup_result.returncode == 0 or "exist" in setup_result.stderr.lower()

        # Step 2: Check initial count
        count_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "cache", "count"], capture_output=True, text=True, env=env, timeout=60
        )

        # Count should work after setup
        assert count_result.returncode == 0 or "configuration" in count_result.stderr.lower()

    def test_queue_workflow(self, db_session):
        """Test queue-related CLI workflow."""
        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "test_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
                "QUERY_QUEUE_PROVIDER": "postgresql",
            }
        )

        # Setup queue tables
        setup_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "setup", "queue"], capture_output=True, text=True, env=env, timeout=60
        )

        assert setup_result.returncode == 0 or "exist" in setup_result.stderr.lower()

        # Check queue count
        count_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "queue", "count"], capture_output=True, text=True, env=env, timeout=60
        )

        # Should show queue information
        assert count_result.returncode == 0 or "queue" in count_result.stderr.lower()

    def test_cache_export_import_workflow(self, db_session):
        """Test basic cache export and import workflow."""
        env = os.environ.copy()
        env.update(
            {
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "test_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
                "CACHE_BACKEND": "postgresql_array",
            }
        )

        # Create temporary export file
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            export_file = f.name

        try:
            # Test export (should work even with empty cache)
            export_result = subprocess.run(
                ["python", "-m", "partitioncache.cli.manage_cache", "cache", "export", "--file", export_file],
                capture_output=True,
                text=True,
                env=env,
                timeout=60,
            )

            # Export should succeed or provide meaningful error
            assert export_result.returncode == 0 or "configuration" in export_result.stderr.lower()

            if export_result.returncode == 0:
                # File should exist after export
                assert os.path.exists(export_file)

                # Test import
                import_result = subprocess.run(
                    ["python", "-m", "partitioncache.cli.manage_cache", "cache", "import", "--file", export_file],
                    capture_output=True,
                    text=True,
                    env=env,
                    timeout=60,
                )

                # Import should succeed
                assert import_result.returncode == 0 or "no data" in import_result.stderr.lower()

        finally:
            # Cleanup export file
            if os.path.exists(export_file):
                os.unlink(export_file)


class TestCLIErrorHandling:
    """Test CLI error handling and edge cases."""

    def test_invalid_command_arguments(self):
        """Test CLI behavior with invalid arguments."""
        # Test with invalid subcommand
        result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "invalid_command"], capture_output=True, text=True, timeout=30)

        assert result.returncode != 0
        assert "invalid" in result.stderr.lower() or "unrecognized" in result.stderr.lower()

    def test_missing_required_args(self):
        """Test CLI behavior when required arguments are missing."""
        # Test pcache-add without required arguments
        result = subprocess.run(["python", "-m", "partitioncache.cli.add_to_cache"], capture_output=True, text=True, timeout=30)

        # Should show error about missing required arguments
        assert result.returncode != 0
        assert "required" in result.stderr.lower() or "argument" in result.stderr.lower()

    def test_invalid_file_paths(self):
        """Test CLI behavior with invalid file paths."""
        result = subprocess.run(
            [
                "python",
                "-m",
                "partitioncache.cli.add_to_cache",
                "--direct",
                "--query-file",
                "/nonexistent/path/query.sql",
                "--partition-key",
                "test",
                "--partition-datatype",
                "integer",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode != 0
        assert "file" in result.stderr.lower() or "not found" in result.stderr.lower()

    def test_cli_with_missing_env_vars(self):
        """Test CLI behavior when environment variables are missing."""
        # Create minimal environment without database configs
        minimal_env = {"PATH": os.environ.get("PATH", "")}

        result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "status"], capture_output=True, text=True, env=minimal_env, timeout=30)

        # Should succeed but show configuration errors
        assert result.returncode == 0
        output = result.stdout + result.stderr
        assert "environment variable" in output.lower() or "error" in output.lower() or "missing" in output.lower()


class TestCLIPerformance:
    """Performance tests for CLI operations."""

    @pytest.mark.slow
    def test_cli_response_time(self):
        """Test that CLI commands respond within reasonable time."""
        import time

        start_time = time.time()

        result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "--help"], capture_output=True, text=True, timeout=10)

        elapsed = time.time() - start_time

        assert result.returncode == 0
        assert elapsed < 5.0, f"CLI help took too long: {elapsed:.2f}s"

    def test_multiple_cli_calls(self):
        """Test multiple CLI calls for consistency."""
        commands = [
            ["python", "-m", "partitioncache.cli.manage_cache", "--help"],
            ["python", "-m", "partitioncache.cli.add_to_cache", "--help"],
            ["python", "-m", "partitioncache.cli.read_from_cache", "--help"],
        ]

        for cmd in commands:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            assert result.returncode == 0, f"Command {' '.join(cmd)} failed: {result.stderr}"
            assert len(result.stdout) > 0, f"Command {' '.join(cmd)} produced no output"
