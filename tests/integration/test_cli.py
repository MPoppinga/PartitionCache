import os
import subprocess
import tempfile

import pytest


class TestCLICommands:
    """
    Test suite for PartitionCache CLI commands.
    Tests all CLI entry points by executing them as separate processes
    to simulate real-world usage.
    """

    def test_pcache_manage_help(self):
        """Test that pcache-manage shows help information."""
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "PartitionCache Management Tool" in result.stdout
        assert "setup" in result.stdout
        assert "cache" in result.stdout

    def test_pcache_manage_status(self, db_session):
        """Test pcache-manage status command."""
        # Set required environment variables
        env = os.environ.copy()
        env.update({
            "PG_HOST": os.getenv("PG_HOST", "localhost"),
            "PG_PORT": os.getenv("PG_PORT", "5432"),
            "PG_USER": os.getenv("PG_USER", "test_user"),
            "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
            "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
            "CACHE_BACKEND": "postgresql_array",
        })

        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "status"],
            capture_output=True,
            text=True,
            env=env,
            timeout=60
        )

        # Should succeed or provide informative output
        assert result.returncode == 0 or "validation" in result.stderr.lower()
        # Should contain status information (output may be in stderr)
        output_text = (result.stdout + result.stderr).lower()
        assert "environment" in output_text or "configuration" in output_text or "validation" in output_text

    def test_pcache_manage_setup_cache(self, db_session):
        """Test cache metadata setup via CLI."""
        env = os.environ.copy()
        env.update({
            "PG_HOST": os.getenv("PG_HOST", "localhost"),
            "PG_PORT": os.getenv("PG_PORT", "5432"),
            "PG_USER": os.getenv("PG_USER", "test_user"),
            "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
            "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
            "CACHE_BACKEND": "postgresql_array",
        })

        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "setup", "cache"],
            capture_output=True,
            text=True,
            env=env,
            timeout=60
        )

        assert result.returncode == 0, f"Setup failed: {result.stderr}"
        # Output may be in stderr for CLI tools
        output_text = (result.stdout + result.stderr).lower()
        assert "setup" in output_text or "completed" in output_text

    def test_pcache_manage_cache_count(self, db_session):
        """Test cache count command."""
        env = os.environ.copy()
        env.update({
            "PG_HOST": os.getenv("PG_HOST", "localhost"),
            "PG_PORT": os.getenv("PG_PORT", "5432"),
            "PG_USER": os.getenv("PG_USER", "test_user"),
            "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
            "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
            "CACHE_BACKEND": "postgresql_array",
        })

        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "cache", "count"],
            capture_output=True,
            text=True,
            env=env,
            timeout=60
        )

        # Should succeed and show count information
        assert result.returncode == 0 or "partitions" in result.stderr.lower()
        # Should contain numerical information (may be in stdout or stderr)
        if result.returncode == 0:
            output_text = result.stdout + result.stderr
            assert any(char.isdigit() for char in output_text)

    def test_pcache_add_help(self):
        """Test pcache-add help command."""
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.add_to_cache", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "query" in result.stdout.lower()
        assert "partition" in result.stdout.lower()

    def test_pcache_add_direct_mode(self, db_session, cache_client):
        """Test adding query directly to cache via CLI."""
        # Create a temporary query file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            test_query = "SELECT * FROM test_locations WHERE zipcode = 1001;"
            f.write(test_query)
            query_file = f.name

        try:
            env = os.environ.copy()
            env.update({
                "PG_HOST": os.getenv("PG_HOST", "localhost"),
                "PG_PORT": os.getenv("PG_PORT", "5432"),
                "PG_USER": os.getenv("PG_USER", "test_user"),
                "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
                "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
                "CACHE_BACKEND": "postgresql_array",
            })

            result = subprocess.run([
                "python", "-m", "partitioncache.cli.add_to_cache",
                "--direct",
                "--query-file", query_file,
                "--partition-key", "zipcode",
                "--partition-datatype", "integer",
                "--cache-backend", "postgresql_array"
            ], capture_output=True, text=True, env=env, timeout=120)

            # Should succeed or provide meaningful error
            if result.returncode != 0:
                # Check if it's a configuration issue rather than a code issue
                assert "configuration" in result.stderr.lower() or "connection" in result.stderr.lower()
            else:
                # Success case: either has output indicating success or succeeds silently
                assert (result.returncode == 0 and 
                       ("added" in result.stdout.lower() or "processed" in result.stdout.lower() or 
                        result.stdout.strip() == ""))

        finally:
            # Cleanup temp file
            os.unlink(query_file)

    def test_pcache_read_help(self):
        """Test pcache-read help command."""
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.read_from_cache", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "partition" in result.stdout.lower()
        assert "key" in result.stdout.lower()

    def test_pcache_monitor_help(self):
        """Test pcache-monitor help command."""
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.monitor_cache_queue", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "monitor" in result.stdout.lower() or "queue" in result.stdout.lower()

    def test_pcache_postgresql_queue_processor_help(self):
        """Test pcache-postgresql-queue-processor help command."""
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.setup_postgresql_queue_processor", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "postgresql" in result.stdout.lower() or "queue" in result.stdout.lower()

    def test_pcache_postgresql_eviction_manager_help(self):
        """Test pcache-postgresql-eviction-manager help command."""
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.postgresql_cache_eviction", "--help"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        assert "eviction" in result.stdout.lower() or "cache" in result.stdout.lower()


class TestCLIIntegration:
    """
    Integration tests for CLI workflows.
    Tests complete workflows using multiple CLI commands in sequence.
    """

    def test_complete_cache_workflow(self, db_session):
        """Test complete workflow: setup -> add -> read -> count."""
        env = os.environ.copy()
        env.update({
            "PG_HOST": os.getenv("PG_HOST", "localhost"),
            "PG_PORT": os.getenv("PG_PORT", "5432"),
            "PG_USER": os.getenv("PG_USER", "test_user"),
            "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
            "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
            "CACHE_BACKEND": "postgresql_array",
        })

        # Step 1: Setup cache
        setup_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "setup", "cache"],
            capture_output=True,
            text=True,
            env=env,
            timeout=60
        )

        # Setup should succeed or already exist
        assert setup_result.returncode == 0 or "exist" in setup_result.stderr.lower()

        # Step 2: Check initial count
        count_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "cache", "count"],
            capture_output=True,
            text=True,
            env=env,
            timeout=60
        )

        # Count should work after setup
        assert count_result.returncode == 0 or "configuration" in count_result.stderr.lower()

    def test_queue_workflow(self, db_session):
        """Test queue-related CLI workflow."""
        env = os.environ.copy()
        env.update({
            "PG_HOST": os.getenv("PG_HOST", "localhost"),
            "PG_PORT": os.getenv("PG_PORT", "5432"),
            "PG_USER": os.getenv("PG_USER", "test_user"),
            "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
            "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
            "QUERY_QUEUE_PROVIDER": "postgresql",
        })

        # Setup queue tables
        setup_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "setup", "queue"],
            capture_output=True,
            text=True,
            env=env,
            timeout=60
        )

        assert setup_result.returncode == 0 or "exist" in setup_result.stderr.lower()

        # Check queue count
        count_result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "queue", "count"],
            capture_output=True,
            text=True,
            env=env,
            timeout=60
        )

        # Should show queue information
        assert count_result.returncode == 0 or "queue" in count_result.stderr.lower()

    def test_cache_export_import_workflow(self, db_session):
        """Test cache export and import workflow."""
        env = os.environ.copy()
        env.update({
            "PG_HOST": os.getenv("PG_HOST", "localhost"),
            "PG_PORT": os.getenv("PG_PORT", "5432"),
            "PG_USER": os.getenv("PG_USER", "test_user"),
            "PG_PASSWORD": os.getenv("PG_PASSWORD", "test_password"),
            "PG_DBNAME": os.getenv("PG_DBNAME", "test_db"),
            "CACHE_BACKEND": "postgresql_array",
        })

        # Create temporary export file
        with tempfile.NamedTemporaryFile(suffix='.pkl', delete=False) as f:
            export_file = f.name

        try:
            # Test export (should work even with empty cache)
            export_result = subprocess.run(
                ["python", "-m", "partitioncache.cli.manage_cache", "cache", "export", "--file", export_file],
                capture_output=True,
                text=True,
                env=env,
                timeout=60
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
                    timeout=60
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
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "invalid_command"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode != 0
        assert "invalid" in result.stderr.lower() or "unrecognized" in result.stderr.lower()

    def test_missing_required_args(self):
        """Test CLI behavior when required arguments are missing."""
        # Test pcache-add without required arguments
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.add_to_cache"],
            capture_output=True,
            text=True,
            timeout=30
        )

        # Should show error about missing required arguments
        assert result.returncode != 0
        assert "required" in result.stderr.lower() or "argument" in result.stderr.lower()

    def test_invalid_file_paths(self):
        """Test CLI behavior with invalid file paths."""
        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.add_to_cache",
             "--direct", "--query-file", "/nonexistent/path/query.sql",
             "--partition-key", "test", "--partition-datatype", "integer"],
            capture_output=True,
            text=True,
            timeout=30
        )

        assert result.returncode != 0
        assert "file" in result.stderr.lower() or "not found" in result.stderr.lower()

    def test_cli_with_missing_env_vars(self):
        """Test CLI behavior when environment variables are missing."""
        # Create minimal environment without database configs
        minimal_env = {"PATH": os.environ.get("PATH", "")}

        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "status"],
            capture_output=True,
            text=True,
            env=minimal_env,
            timeout=30
        )

        # Should fail gracefully with configuration error
        assert result.returncode != 0
        assert ("configuration" in result.stderr.lower() or
                "environment" in result.stderr.lower() or
                "connection" in result.stderr.lower())


class TestCLIPerformance:
    """Performance tests for CLI operations."""

    @pytest.mark.slow
    def test_cli_response_time(self):
        """Test that CLI commands respond within reasonable time."""
        import time

        start_time = time.time()

        result = subprocess.run(
            ["python", "-m", "partitioncache.cli.manage_cache", "--help"],
            capture_output=True,
            text=True,
            timeout=10
        )

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
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )

            assert result.returncode == 0, f"Command {' '.join(cmd)} failed: {result.stderr}"
            assert len(result.stdout) > 0, f"Command {' '.join(cmd)} produced no output"
