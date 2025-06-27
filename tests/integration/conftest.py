import logging
import os
import tempfile
import time

import filelock
import psycopg
import pytest

from partitioncache.cache_handler import get_cache_handler

logger = logging.getLogger("PartitionCache")


# Configure pytest markers
def pytest_configure(config):
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")
    config.addinivalue_line("markers", "performance: marks tests as performance-related")
    config.addinivalue_line("markers", "stress: marks tests as stress tests")
    config.addinivalue_line("markers", "concurrent: marks tests as concurrency tests")
    config.addinivalue_line("markers", "serial: marks tests that should run serially to avoid concurrency issues")


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
    },
}


@pytest.fixture(scope="session")
def db_connection(tmp_path_factory):
    """
    Session-scoped database connection fixture.
    Sets up PostgreSQL connection and creates test schema.
    Handles parallel execution with pytest-xdist by using a file lock.
    """
    # Get connection parameters from environment
    conn_params = {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "user": os.getenv("PG_USER", "integration_user"),
        "password": os.getenv("PG_PASSWORD", "test_password"),
        "dbname": os.getenv("PG_DBNAME", "partitioncache_integration"),
    }

    # Use a file lock to ensure that the session setup is only performed by one worker
    # in a parallel testing environment. tmp_path_factory is session-scoped.
    lock_path = tmp_path_factory.getbasetemp().parent / "db_setup.lock"

    with filelock.FileLock(str(lock_path)):
        # The first worker acquires the lock and sets up the database.
        # Other workers will block until this is complete.
        conn_setup = None
        try:
            conn_setup = psycopg.connect(**conn_params, connect_timeout=10)
            conn_setup.autocommit = True
            with conn_setup.cursor() as cur:
                # Check if setup has already been done to avoid re-running
                cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'test_locations');")
                row = cur.fetchone()
                if row and not row[0]:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")

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

                    # Create table for spatial cache testing
                    cur.execute("DROP TABLE IF EXISTS test_businesses CASCADE;")
                    cur.execute("""
                        CREATE TABLE test_businesses (
                            id SERIAL PRIMARY KEY,
                            name TEXT NOT NULL,
                            business_type TEXT NOT NULL,
                            region_id INTEGER NOT NULL,
                            city_id INTEGER NOT NULL,
                            zipcode TEXT NOT NULL,
                            x DECIMAL(10,6) NOT NULL,  -- longitude-like coordinate
                            y DECIMAL(10,6) NOT NULL,  -- latitude-like coordinate  
                            rating DECIMAL(2,1) DEFAULT 3.0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)

                    # Create test_spatial_points table for spatial cache testing
                    cur.execute("DROP TABLE IF EXISTS test_spatial_points CASCADE;")
                    cur.execute("""
                        CREATE TABLE test_spatial_points (
                            id SERIAL PRIMARY KEY,
                            name TEXT NOT NULL,
                            point_type TEXT NOT NULL,
                            region_id INTEGER NOT NULL,
                            city_id INTEGER NOT NULL,
                            zipcode TEXT NOT NULL,
                            x DECIMAL(10,6) NOT NULL,  -- longitude-like coordinate
                            y DECIMAL(10,6) NOT NULL,  -- latitude-like coordinate  
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)

                    # Insert sample spatial test data
                    sample_businesses = [
                        ("Pizza Palace", "restaurant", 1, 101, "10001", -74.0060, 40.7128, 4.2),
                        ("Corner Pharmacy", "pharmacy", 1, 101, "10001", -74.0050, 40.7130, 3.8),
                        ("Joe's Diner", "restaurant", 1, 102, "10002", -74.0070, 40.7125, 3.9),
                        ("Health Plus", "pharmacy", 2, 201, "90210", -118.2437, 34.0522, 4.1),
                        ("Sunset Bistro", "restaurant", 2, 201, "90210", -118.2440, 34.0520, 4.5),
                        ("Metro Drugs", "pharmacy", 2, 202, "90211", -118.2430, 34.0525, 3.7),
                        # Add business types expected by spatial queries
                        ("Downtown Market", "supermarket", 1, 101, "10001", -74.0055, 40.7129, 4.0),
                        ("City Cafe", "cafe", 1, 101, "10001", -74.0058, 40.7127, 4.3),
                        ("First Bank", "bank", 1, 101, "10001", -74.0052, 40.7131, 3.9),
                        ("West Market", "supermarket", 2, 201, "90210", -118.2440, 34.0523, 4.1),
                        ("Sunset Cafe", "cafe", 2, 201, "90210", -118.2438, 34.0521, 4.2),
                        ("Pacific Bank", "bank", 2, 201, "90210", -118.2435, 34.0524, 4.0),
                    ]

                    for name, btype, region_id, city_id, zipcode, x, y, rating in sample_businesses:
                        cur.execute(
                            """
                            INSERT INTO test_businesses (name, business_type, region_id, city_id, zipcode, x, y, rating)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                        """,
                            (name, btype, region_id, city_id, zipcode, x, y, rating),
                        )

                    # Insert sample spatial points data
                    sample_spatial_points = [
                        ("Central Park", "park", 1, 101, "10001", -74.0059, 40.7131),
                        ("Library Square", "library", 1, 101, "10001", -74.0055, 40.7125),
                        ("School District 1", "school", 1, 102, "10002", -74.0065, 40.7120),
                        ("Hospital Center", "hospital", 2, 201, "90210", -118.2435, 34.0525),
                        ("Beach Park", "park", 2, 201, "90210", -118.2445, 34.0515),
                        ("Metro Station", "transport", 2, 202, "90211", -118.2425, 34.0530),
                    ]

                    for name, ptype, region_id, city_id, zipcode, x, y in sample_spatial_points:
                        cur.execute(
                            """
                            INSERT INTO test_spatial_points (name, point_type, region_id, city_id, zipcode, x, y)
                            VALUES (%s, %s, %s, %s, %s, %s, %s);
                        """,
                            (name, ptype, region_id, city_id, zipcode, x, y),
                        )
        except Exception as e:
            pytest.skip(f"DB setup failed: {e}")
        finally:
            if conn_setup and not conn_setup.closed:
                conn_setup.close()

    # Now all workers can connect to the initialized database.
    try:
        conn = psycopg.connect(**conn_params, connect_timeout=10)
    except Exception as e:
        pytest.skip(f"Cannot connect to PostgreSQL database: {e}")
        return None

    conn.autocommit = True
    yield conn

    # The teardown part of a session-scoped fixture runs in each worker.
    # To avoid issues, we don't clean up the database here.
    # The setup of the next test session will handle it with DROP TABLE.
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
        # Complete cleanup of all cache tables (handle all possible prefixes)
        cache_prefixes = ["partitioncache_%", "%_cache_%", "%queue%"]
        all_cache_tables = set()

        for prefix in cache_prefixes:
            cur.execute(
                """
                SELECT tablename FROM pg_tables 
                WHERE tablename LIKE %s AND schemaname = 'public';
            """,
                (prefix,),
            )
            tables = cur.fetchall()
            all_cache_tables.update(table[0] for table in tables)

        # Clean cache tables (truncate instead of drop to preserve schema)
        for table_name in all_cache_tables:
            try:
                # Try truncate first to preserve schema
                cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
            except Exception as e:
                try:
                    # If truncate fails, try drop as fallback
                    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                except Exception:
                    logger.warning(f"Failed to clean table {table_name}: {e}")

        # Reset test table data to ensure clean state - handle missing tables gracefully
        for table_name in ["test_locations", "test_cron_results", "test_businesses", "test_spatial_points"]:
            try:
                cur.execute(f"TRUNCATE {table_name} RESTART IDENTITY CASCADE;")
            except Exception:
                # Table might not exist - recreate if needed
                if table_name == "test_businesses":
                    cur.execute("""
                        CREATE TABLE test_businesses (
                            id SERIAL PRIMARY KEY,
                            name TEXT NOT NULL,
                            business_type TEXT NOT NULL,
                            region_id INTEGER NOT NULL,
                            city_id INTEGER NOT NULL,
                            zipcode TEXT NOT NULL,
                            x DECIMAL(10,6) NOT NULL,
                            y DECIMAL(10,6) NOT NULL,  
                            rating DECIMAL(2,1) DEFAULT 3.0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                elif table_name == "test_locations":
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
                elif table_name == "test_cron_results":
                    cur.execute("""
                        CREATE TABLE test_cron_results (
                            id SERIAL PRIMARY KEY,
                            message TEXT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                elif table_name == "test_spatial_points":
                    cur.execute("""
                        CREATE TABLE test_spatial_points (
                            id SERIAL PRIMARY KEY,
                            name TEXT NOT NULL,
                            point_type TEXT NOT NULL,
                            region_id INTEGER NOT NULL,
                            city_id INTEGER NOT NULL,
                            zipcode TEXT NOT NULL,
                            x DECIMAL(10,6) NOT NULL,
                            y DECIMAL(10,6) NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)

        # Re-insert sample data for consistent test state
        for zipcode, data in SAMPLE_TEST_DATA["zipcode"].items():
            # Determine region based on zipcode
            if zipcode < 20000:
                region = "northeast"
            elif zipcode < 80000:
                region = "southeast"
            else:
                region = "west"

            cur.execute(
                """
                INSERT INTO test_locations (zipcode, region, name, population)
                VALUES (%s, %s, %s, %s);
            """,
                (zipcode, region, data["name"], data["population"]),
            )

        # Re-insert sample spatial test data
        sample_businesses = [
            ("Pizza Palace", "restaurant", 1, 101, "10001", -74.0060, 40.7128, 4.2),
            ("Corner Pharmacy", "pharmacy", 1, 101, "10001", -74.0050, 40.7130, 3.8),
            ("Joe's Diner", "restaurant", 1, 102, "10002", -74.0070, 40.7125, 3.9),
            ("Health Plus", "pharmacy", 2, 201, "90210", -118.2437, 34.0522, 4.1),
            ("Sunset Bistro", "restaurant", 2, 201, "90210", -118.2440, 34.0520, 4.5),
            ("Metro Drugs", "pharmacy", 2, 202, "90211", -118.2430, 34.0525, 3.7),
            # Add business types expected by spatial queries
            ("Downtown Market", "supermarket", 1, 101, "10001", -74.0055, 40.7129, 4.0),
            ("City Cafe", "cafe", 1, 101, "10001", -74.0058, 40.7127, 4.3),
            ("First Bank", "bank", 1, 101, "10001", -74.0052, 40.7131, 3.9),
            ("West Market", "supermarket", 2, 201, "90210", -118.2440, 34.0523, 4.1),
            ("Sunset Cafe", "cafe", 2, 201, "90210", -118.2438, 34.0521, 4.2),
            ("Pacific Bank", "bank", 2, 201, "90210", -118.2435, 34.0524, 4.0),
        ]

        for name, btype, region_id, city_id, zipcode, x, y, rating in sample_businesses:
            cur.execute(
                """
                INSERT INTO test_businesses (name, business_type, region_id, city_id, zipcode, x, y, rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
                (name, btype, region_id, city_id, zipcode, x, y, rating),
            )

        # Re-insert sample spatial points data
        sample_spatial_points = [
            ("Central Park", "park", 1, 101, "10001", -74.0059, 40.7131),
            ("Library Square", "library", 1, 101, "10001", -74.0055, 40.7125),
            ("School District 1", "school", 1, 102, "10002", -74.0065, 40.7120),
            ("Hospital Center", "hospital", 2, 201, "90210", -118.2435, 34.0525),
            ("Beach Park", "park", 2, 201, "90210", -118.2445, 34.0515),
            ("Metro Station", "transport", 2, 202, "90211", -118.2425, 34.0530),
        ]

        for name, ptype, region_id, city_id, zipcode, x, y in sample_spatial_points:
            cur.execute(
                """
                INSERT INTO test_spatial_points (name, point_type, region_id, city_id, zipcode, x, y)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
                (name, ptype, region_id, city_id, zipcode, x, y),
            )

    yield conn

    # Cleanup after test
    # Reset queue handler singleton to avoid connection leaks
    try:
        from partitioncache.queue import reset_queue_handler

        reset_queue_handler()
    except Exception:
        pass

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
    if cache_backend.startswith("postgresql"):
        # Set PostgreSQL database connection variables if not already set
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

        # Set backend-specific table prefixes (only if not already set by CI)
        if cache_backend == "postgresql_array" and not os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX"):
            os.environ["PG_ARRAY_CACHE_TABLE_PREFIX"] = "integration_array_cache"
        elif cache_backend == "postgresql_bit":
            if not os.getenv("PG_BIT_CACHE_TABLE_PREFIX"):
                os.environ["PG_BIT_CACHE_TABLE_PREFIX"] = "integration_bit_cache"
            if not os.getenv("PG_BIT_CACHE_BITSIZE"):
                os.environ["PG_BIT_CACHE_BITSIZE"] = "200000"
        elif cache_backend == "postgresql_roaringbit" and not os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX"):
            os.environ["PG_ROARINGBIT_CACHE_TABLE_PREFIX"] = "integration_roaring_cache"

    elif cache_backend.startswith("redis"):
        # Set Redis connection variables if not already set, using separate databases for isolation
        if not os.getenv("REDIS_CACHE_DB"):
            os.environ["REDIS_CACHE_DB"] = "0"
        if not os.getenv("REDIS_BIT_DB"):
            os.environ["REDIS_BIT_DB"] = "1"
        if not os.getenv("REDIS_SET_DB"):
            os.environ["REDIS_SET_DB"] = "2"  # Use different database for redis_set
        if not os.getenv("REDIS_BIT_BITSIZE"):
            os.environ["REDIS_BIT_BITSIZE"] = "200000"

    elif cache_backend == "rocksdb_set":
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
        # Cleanup - Force complete cleanup for test isolation
        try:
            # Special cleanup for Redis backends - use bulk clear method
            if cache_backend.startswith("redis") and hasattr(cache_handler, "clear_all_cache_data"):
                deleted_count = cache_handler.clear_all_cache_data()
                logger.info(f"Cleared {deleted_count} Redis cache keys")
            else:
                # Clear ALL test data completely (not limited to 50 keys)
                for partition_key, _ in TEST_PARTITION_KEYS:
                    try:
                        # Check if partition exists before attempting cleanup
                        if hasattr(cache_handler, "_get_partition_datatype"):
                            # For PostgreSQL backends, check if partition exists
                            if cache_handler._get_partition_datatype(partition_key) is None:
                                continue  # Skip non-existent partitions

                        # Get all keys and delete them all
                        keys = cache_handler.get_all_keys(partition_key)
                        if keys:
                            # Delete all keys without limit for proper test isolation
                            for key in list(keys):
                                try:
                                    cache_handler.delete(key, partition_key)
                                except Exception:
                                    pass  # Continue deleting other keys

                            # Double-check deletion worked (only if partition still exists)
                            try:
                                remaining_keys = cache_handler.get_all_keys(partition_key)
                                if remaining_keys:
                                    logger.warning(f"Failed to delete all keys for {partition_key}: {len(remaining_keys)} remaining")
                            except Exception:
                                # Partition might have been deleted during cleanup - this is OK
                                pass
                    except Exception as e:
                        # Only log warnings for unexpected errors, not missing tables
                        if "does not exist" not in str(e).lower() and "relation" not in str(e).lower():
                            logger.warning(f"Cache cleanup failed for {partition_key}: {e}")

            # Properly close cache handler
            if hasattr(cache_handler, "close"):
                cache_handler.close()
        except Exception as e:
            logger.warning(f"Cache handler cleanup failed: {e}")

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
                elif cache_backend == "rocksdict" and hasattr(cache_handler, "db") and hasattr(cache_handler.db, "name"):
                    # For rocksdict, clean up the temp directory created for testing
                    db_path = getattr(cache_handler.db, "name", None)
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


@pytest.fixture(scope="session")
def postgresql_queue_functions(db_connection):
    """
    Session-scoped fixture that ensures PostgreSQL queue processor functions are loaded.

    This fixture:
    1. Checks if PostgreSQL queue provider is configured
    2. Loads the queue processor SQL functions into the database
    3. Only runs once per test session
    4. Skips gracefully if PostgreSQL is not available
    """
    # Check if PostgreSQL queue provider is configured
    queue_provider = os.getenv("QUERY_QUEUE_PROVIDER", "").lower()
    if queue_provider != "postgresql":
        pytest.skip("PostgreSQL queue functions not needed for non-PostgreSQL queue provider")

    if db_connection is None:
        pytest.skip("PostgreSQL database connection not available")

    try:
        # Import the setup function
        from partitioncache.cli.setup_postgresql_queue_processor import setup_database_objects
        from partitioncache.queue import get_queue_table_prefix_from_env

        # Load the SQL functions into the database
        setup_database_objects(db_connection)

        logger.info("PostgreSQL queue processor functions loaded successfully")
        return True

    except ImportError as e:
        pytest.skip(f"Could not import PostgreSQL queue processor setup: {e}")
    except Exception as e:
        logger.error(f"Failed to load PostgreSQL queue processor functions: {e}")
        pytest.fail(f"PostgreSQL queue function setup failed: {e}")


@pytest.fixture
def postgresql_queue_processor(postgresql_queue_functions, db_session):
    """
    Function-scoped fixture that provides a configured PostgreSQL queue processor.

    Depends on postgresql_queue_functions to ensure SQL functions are loaded.
    Sets up processor tables and configuration for the current test.
    """
    from partitioncache.cache_handler import get_cache_table_prefix_from_env
    from partitioncache.queue import get_queue_table_prefix_from_env

    queue_prefix = get_queue_table_prefix_from_env()
    table_prefix = get_cache_table_prefix_from_env()

    # Initialize processor tables
    with db_session.cursor() as cur:
        cur.execute(f"SELECT partitioncache_initialize_processor_tables('{queue_prefix}')")

        # Add a test configuration for the processor
        config_table = f"{queue_prefix}_processor_config"
        cur.execute(
            f"""
            INSERT INTO {config_table} 
            (job_name, enabled, max_parallel_jobs, frequency_seconds, timeout_seconds, 
             table_prefix, queue_prefix, cache_backend)
            VALUES ('partitioncache_process_queue', true, 1, 60, 30, 
                    %s, %s, 'array')
            ON CONFLICT (job_name) DO UPDATE 
            SET timeout_seconds = 30, enabled = true, updated_at = NOW()
        """,
            (table_prefix, queue_prefix),
        )
        db_session.commit()

    return {"queue_prefix": queue_prefix, "table_prefix": table_prefix, "config_table": config_table}
