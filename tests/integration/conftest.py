import os
import signal
import tempfile
import time

import psycopg
import pytest

from partitioncache.cache_handler import get_cache_handler


# Configure pytest markers
def pytest_configure(config):
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "performance: marks tests as performance-related")
    config.addinivalue_line("markers", "stress: marks tests as stress tests")
    config.addinivalue_line("markers", "concurrent: marks tests as concurrency tests")


# Test data based on OSM example pattern but simplified (no PostGIS)
TEST_PARTITION_KEYS = [
    ("zipcode", "integer"),
    ("region", "text"),
]

SAMPLE_TEST_DATA = {
    "zipcode": {
        1001: {"name": "Boston Center", "population": 15000},
        1002: {"name": "Boston North", "population": 12000},
        90210: {"name": "Beverly Hills", "population": 32000},
        10001: {"name": "Manhattan", "population": 45000},
    },
    "region": {
        "northeast": {"cities": 4, "total_pop": 72000},
        "west": {"cities": 1, "total_pop": 32000},
        "southeast": {"cities": 2, "total_pop": 28000},
    }
}


@pytest.fixture(scope="session")
def db_connection():
    """
    Session-scoped database connection fixture.
    Sets up PostgreSQL connection and creates test schema.
    """
    # Get connection parameters from environment
    conn_params = {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "user": os.getenv("PG_USER", "test_user"),
        "password": os.getenv("PG_PASSWORD", "test_password"),
        "dbname": os.getenv("PG_DBNAME", "test_db"),
    }

    try:
        conn = psycopg.connect(**conn_params, connect_timeout=10)
    except Exception as e:
        pytest.skip(f"Cannot connect to PostgreSQL database: {e}")
        return None  # This won't be reached due to skip, but helps with type hints
    conn.autocommit = True

    # Create test tables with sample data
    with conn.cursor() as cur:
        # Create pg_cron extension if not exists
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")

        # Create test tables based on OSM pattern
        # Use DROP/CREATE instead of IF NOT EXISTS to avoid sequence conflicts
        cur.execute("DROP TABLE IF EXISTS test_locations CASCADE;")
        cur.execute("""
            CREATE TABLE test_locations (
                id SERIAL PRIMARY KEY,
                zipcode INTEGER,
                region TEXT,
                name TEXT,
                population INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create table for cron job testing
        cur.execute("DROP TABLE IF EXISTS test_cron_results CASCADE;")
        cur.execute("""
            CREATE TABLE test_cron_results (
                id SERIAL PRIMARY KEY,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Insert sample data (no need to truncate as table was just created)
        for zipcode, data in SAMPLE_TEST_DATA["zipcode"].items():
            # Determine region based on zipcode
            if zipcode < 20000:
                region = "northeast"
            elif zipcode < 80000:
                region = "southeast"
            else:
                region = "west"

            cur.execute("""
                INSERT INTO test_locations (zipcode, region, name, population)
                VALUES (%s, %s, %s, %s);
            """, (zipcode, region, data["name"], data["population"]))

    yield conn

    # Cleanup
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_locations CASCADE;")
        cur.execute("DROP TABLE IF EXISTS test_cron_results CASCADE;")
        # Clean up any test cron jobs
        try:
            cur.execute("DELETE FROM cron.job WHERE jobname LIKE 'test_%';")
        except Exception:
            pass  # pg_cron might not be available

    conn.close()


@pytest.fixture(scope="function")
def db_session(db_connection):
    """
    Function-scoped database session with transaction rollback.
    Ensures test isolation by cleaning up data between tests.
    """
    conn = db_connection

    # Start fresh transaction
    with conn.cursor() as cur:
        # Clean up any existing test cache data
        cur.execute("""
            SELECT tablename FROM pg_tables 
            WHERE tablename LIKE 'partitioncache_%' AND schemaname = 'public';
        """)
        cache_tables = cur.fetchall()

        for (table_name,) in cache_tables:
            try:
                cur.execute(f"TRUNCATE {table_name} CASCADE;")
            except Exception:
                pass  # Table might not exist or have dependencies

        # Reset test table data to ensure clean state
        cur.execute("TRUNCATE test_locations RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE test_cron_results RESTART IDENTITY CASCADE;")

        # Re-insert sample data for consistent test state
        for zipcode, data in SAMPLE_TEST_DATA["zipcode"].items():
            # Determine region based on zipcode
            if zipcode < 20000:
                region = "northeast"
            elif zipcode < 80000:
                region = "southeast"
            else:
                region = "west"

            cur.execute("""
                INSERT INTO test_locations (zipcode, region, name, population)
                VALUES (%s, %s, %s, %s);
            """, (zipcode, region, data["name"], data["population"]))

    yield conn

    # Cleanup after test
    with conn.cursor() as cur:
        # Remove any test cron jobs
        try:
            cur.execute("DELETE FROM cron.job WHERE jobname LIKE 'test_%';")
        except Exception:
            pass  # pg_cron might not be available


# Base backends that should always be available
CACHE_BACKENDS = [
    "postgresql_array",
    "postgresql_bit",
    "postgresql_roaringbit",
]

# Add Redis backends if available
if os.getenv("REDIS_HOST"):
    CACHE_BACKENDS.extend(["redis_set", "redis_bit"])

# Add RocksDB backends if available
def _is_rocksdb_available():
    """Check if RocksDB is available for testing."""
    try:
        import rocksdb
        return True
    except ImportError:
        return False

def _is_rocksdict_available():
    """Check if rocksdict is available for testing."""
    try:
        import rocksdict
        return True
    except ImportError:
        return False

# Add RocksDB backends if the module is available
if _is_rocksdb_available():
    CACHE_BACKENDS.extend(["rocksdb_set", "rocksdb_bit"])

# Add rocksdict backend if available (should always be available via pip)
if _is_rocksdict_available():
    CACHE_BACKENDS.append("rocksdict")


@pytest.fixture(params=CACHE_BACKENDS)
def cache_client(request, db_session):
    """
    Parameterized fixture providing all cache backend implementations.
    Each test using this fixture will run once per cache backend.
    """
    cache_backend = request.param

    # Skip PostgreSQL backends if database connection not available
    if cache_backend.startswith("postgresql") and db_session is None:
        pytest.skip(f"PostgreSQL database not available for {cache_backend}")

    # Set environment variables for cache backend
    original_backend = os.getenv("CACHE_BACKEND")
    original_env_vars = {}

    os.environ["CACHE_BACKEND"] = cache_backend

    # Set up backend-specific environment variables
    if cache_backend == "rocksdb_set":
        temp_dir = tempfile.mkdtemp(prefix="rocksdb_test_")
        original_env_vars["ROCKSDB_PATH"] = os.getenv("ROCKSDB_PATH")
        os.environ["ROCKSDB_PATH"] = temp_dir
    elif cache_backend == "rocksdb_bit":
        temp_dir = tempfile.mkdtemp(prefix="rocksdb_bit_test_")
        original_env_vars["ROCKSDB_BIT_PATH"] = os.getenv("ROCKSDB_BIT_PATH")
        original_env_vars["ROCKSDB_BIT_BITSIZE"] = os.getenv("ROCKSDB_BIT_BITSIZE")
        os.environ["ROCKSDB_BIT_PATH"] = temp_dir
        os.environ["ROCKSDB_BIT_BITSIZE"] = "200000"
    elif cache_backend == "rocksdict":
        temp_dir = tempfile.mkdtemp(prefix="rocksdict_test_")
        # rocksdict uses the db_path parameter directly, no env vars needed

    try:
        # Create cache handler
        if cache_backend == "rocksdict":
            # rocksdict needs the path passed directly
            temp_dir = tempfile.mkdtemp(prefix="rocksdict_test_")
            from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler
            cache_handler = RocksDictCacheHandler(temp_dir)
        else:
            cache_handler = get_cache_handler(cache_backend)

        # Setup partition keys for testing
        for partition_key, datatype in TEST_PARTITION_KEYS:
            try:
                cache_handler.register_partition_key(partition_key, datatype)
            except Exception:
                # Some backends might not need explicit registration
                pass

        yield cache_handler

    except Exception as e:
        pytest.skip(f"Cannot create cache handler for {cache_backend}: {e}")
    finally:
        # Cleanup
        try:
            # Clear all test data with timeout protection
            for partition_key, _ in TEST_PARTITION_KEYS:
                try:
                    keys = cache_handler.get_all_keys(partition_key)
                    # Limit key cleanup to prevent hanging
                    for key in list(keys)[:50]:  # Process max 50 keys to avoid infinite loops
                        cache_handler.delete(key, partition_key)
                except Exception:
                    pass

            # Properly close cache handler
            if hasattr(cache_handler, 'close'):
                cache_handler.close()
        except Exception:
            pass

        # Restore original environment variables
        if original_backend:
            os.environ["CACHE_BACKEND"] = original_backend
        elif "CACHE_BACKEND" in os.environ:
            del os.environ["CACHE_BACKEND"]

        # Restore RocksDB-specific environment variables
        for env_var, original_value in original_env_vars.items():
            if original_value is not None:
                os.environ[env_var] = original_value
            elif env_var in os.environ:
                del os.environ[env_var]

        # Clean up temporary directories for RocksDB backends
        if cache_backend in ["rocksdb_set", "rocksdb_bit", "rocksdict"]:
            try:
                import shutil
                if cache_backend == "rocksdb_set" and "ROCKSDB_PATH" in os.environ:
                    shutil.rmtree(os.environ["ROCKSDB_PATH"], ignore_errors=True)
                elif cache_backend == "rocksdb_bit" and "ROCKSDB_BIT_PATH" in os.environ:
                    shutil.rmtree(os.environ["ROCKSDB_BIT_PATH"], ignore_errors=True)
                elif cache_backend == "rocksdict" and hasattr(cache_handler, 'db') and hasattr(cache_handler.db, 'name'):
                    # For rocksdict, clean up the temp directory created for testing
                    db_path = getattr(cache_handler.db, 'name', None)
                    if db_path and os.path.exists(os.path.dirname(db_path)):
                        shutil.rmtree(os.path.dirname(db_path), ignore_errors=True)
            except Exception:
                pass  # Ignore cleanup errors


@pytest.fixture
def sample_queries():
    """Fixture providing sample SQL queries for testing."""
    return {
        "zipcode_simple": "SELECT * FROM test_locations WHERE zipcode = 1001;",
        "zipcode_range": "SELECT * FROM test_locations WHERE zipcode BETWEEN 1000 AND 2000;",
        "region_filter": "SELECT * FROM test_locations WHERE region = 'northeast';",
        "complex_join": """
            SELECT l1.name, l1.population, l2.name as nearby
            FROM test_locations l1 
            JOIN test_locations l2 ON l1.region = l2.region 
            WHERE l1.zipcode = 1001 AND l2.zipcode != l1.zipcode;
        """,
    }


@pytest.fixture
def wait_for_cron():
    """Utility fixture for waiting on cron job execution."""
    def _wait(seconds=70):
        """Wait for cron job to execute (default just over 1 minute)."""
        time.sleep(seconds)
    return _wait
