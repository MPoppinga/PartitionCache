import os
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

    conn = psycopg.connect(**conn_params)
    conn.autocommit = True

    # Create test tables with sample data
    with conn.cursor() as cur:
        # Create pg_cron extension if not exists
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")

        # Create test tables based on OSM pattern
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_locations (
                id SERIAL PRIMARY KEY,
                zipcode INTEGER,
                region TEXT,
                name TEXT,
                population INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Insert sample data
        cur.execute("TRUNCATE test_locations;")
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
        # Clean up any test cron jobs
        cur.execute("DELETE FROM cron.job WHERE jobname LIKE 'test_%';")

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

    yield conn

    # Cleanup after test
    with conn.cursor() as cur:
        # Remove any test cron jobs
        cur.execute("DELETE FROM cron.job WHERE jobname LIKE 'test_%';")


# Parameterized cache client fixture covering all backends
CACHE_BACKENDS = [
    "postgresql_array",
    "postgresql_bit", 
    "postgresql_roaringbit",
]

# Add Redis backends if available
if os.getenv("REDIS_HOST"):
    CACHE_BACKENDS.extend(["redis_set", "redis_bit"])


@pytest.fixture(params=CACHE_BACKENDS)
def cache_client(request, db_session):
    """
    Parameterized fixture providing all cache backend implementations.
    Each test using this fixture will run once per cache backend.
    """
    cache_backend = request.param

    # Set environment variables for cache backend
    original_backend = os.getenv("CACHE_BACKEND")
    os.environ["CACHE_BACKEND"] = cache_backend

    try:
        # Create cache handler
        cache_handler = get_cache_handler(cache_backend)

        # Setup partition keys for testing
        for partition_key, datatype in TEST_PARTITION_KEYS:
            try:
                cache_handler.register_partition_key(partition_key, datatype)
            except Exception:
                # Some backends might not need explicit registration
                pass

        yield cache_handler

    finally:
        # Cleanup
        try:
            # Clear all test data
            for partition_key, _ in TEST_PARTITION_KEYS:
                try:
                    keys = cache_handler.get_all_keys(partition_key)
                    for key in keys:
                        cache_handler.delete(key, partition_key)
                except Exception:
                    pass

            cache_handler.close()
        except Exception:
            pass

        # Restore original environment
        if original_backend:
            os.environ["CACHE_BACKEND"] = original_backend
        elif "CACHE_BACKEND" in os.environ:
            del os.environ["CACHE_BACKEND"]


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
