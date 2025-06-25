#!/usr/bin/env python3
"""
Test script to validate environment variables for PartitionCache.
"""

import os


def check_env_vars():
    """Check that all required environment variables are set."""

    # Cache database variables
    cache_vars = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_NAME": os.getenv("DB_NAME"),
    }

    # Queue variables
    queue_vars = {
        "QUERY_QUEUE_PROVIDER": os.getenv("QUERY_QUEUE_PROVIDER"),
        "PG_QUEUE_HOST": os.getenv("PG_QUEUE_HOST"),
        "PG_QUEUE_PORT": os.getenv("PG_QUEUE_PORT"),
        "PG_QUEUE_USER": os.getenv("PG_QUEUE_USER"),
        "PG_QUEUE_PASSWORD": os.getenv("PG_QUEUE_PASSWORD"),
        "PG_QUEUE_DB": os.getenv("PG_QUEUE_DB"),
    }

    # Cache backend specific
    cache_backend_vars = {
        "CACHE_BACKEND": os.getenv("CACHE_BACKEND"),
    }

    # Backend-specific variables (check based on CACHE_BACKEND)
    backend = os.getenv("CACHE_BACKEND", "")
    backend_specific_vars = {}

    if backend == "postgresql_array":
        backend_specific_vars = {
            "PG_ARRAY_CACHE_TABLE_PREFIX": os.getenv("PG_ARRAY_CACHE_TABLE_PREFIX"),
        }
    elif backend == "postgresql_bit":
        backend_specific_vars = {
            "PG_BIT_CACHE_TABLE_PREFIX": os.getenv("PG_BIT_CACHE_TABLE_PREFIX"),
            "PG_BIT_CACHE_BITSIZE": os.getenv("PG_BIT_CACHE_BITSIZE"),
        }
    elif backend == "postgresql_roaringbit":
        backend_specific_vars = {
            "PG_ROARINGBIT_CACHE_TABLE_PREFIX": os.getenv("PG_ROARINGBIT_CACHE_TABLE_PREFIX"),
        }
    elif backend == "redis_set":
        backend_specific_vars = {
            "REDIS_HOST": os.getenv("REDIS_HOST"),
            "REDIS_PORT": os.getenv("REDIS_PORT"),
            "REDIS_CACHE_DB": os.getenv("REDIS_CACHE_DB"),
        }
    elif backend == "redis_bit":
        backend_specific_vars = {
            "REDIS_HOST": os.getenv("REDIS_HOST"),
            "REDIS_PORT": os.getenv("REDIS_PORT"),
            "REDIS_BIT_DB": os.getenv("REDIS_BIT_DB"),
            "REDIS_BIT_BITSIZE": os.getenv("REDIS_BIT_BITSIZE"),
        }
    elif backend == "rocksdb_set":
        backend_specific_vars = {
            "ROCKSDB_PATH": os.getenv("ROCKSDB_PATH"),
        }
    elif backend == "rocksdb_bit":
        backend_specific_vars = {
            "ROCKSDB_BIT_PATH": os.getenv("ROCKSDB_BIT_PATH"),
            "ROCKSDB_BIT_BITSIZE": os.getenv("ROCKSDB_BIT_BITSIZE"),
        }
    elif backend == "rocksdict":
        backend_specific_vars = {
            "ROCKSDB_DICT_PATH": os.getenv("ROCKSDB_DICT_PATH"),
        }

    print("Environment Variable Check")
    print("=" * 50)

    print("\n🗄️ Cache Database Variables:")
    for var, value in cache_vars.items():
        status = "✅" if value else "❌"
        print(f"  {status} {var}: {value or 'NOT SET'}")

    print("\n📬 Queue Variables:")
    for var, value in queue_vars.items():
        status = "✅" if value else "❌"
        print(f"  {status} {var}: {value or 'NOT SET'}")

    print("\n🔧 Cache Backend Variables:")
    for var, value in cache_backend_vars.items():
        status = "✅" if value else "❌"
        print(f"  {status} {var}: {value or 'NOT SET'}")

    if backend_specific_vars:
        print(f"\n🎯 {backend.upper()} Backend-Specific Variables:")
        for var, value in backend_specific_vars.items():
            status = "✅" if value else "❌"
            print(f"  {status} {var}: {value or 'NOT SET'}")
    elif backend:
        print(f"\n🎯 {backend.upper()} Backend: No additional variables required")

    # Check for legacy variables that might cause confusion
    print("\n🔍 Legacy Variables (for reference):")
    legacy_vars = ["PG_HOST", "PG_PORT", "PG_USER", "PG_PASSWORD", "PG_DBNAME"]
    for var in legacy_vars:
        value = os.getenv(var)
        if value:
            print(f"  📝 {var}: {value}")

    # Validation
    missing_cache = [var for var, value in cache_vars.items() if not value]
    missing_queue = [var for var, value in queue_vars.items() if not value]
    missing_backend = [var for var, value in backend_specific_vars.items() if not value]

    if missing_cache:
        print(f"\n❌ Missing cache database variables: {', '.join(missing_cache)}")
        return False

    if missing_queue:
        print(f"\n❌ Missing queue variables: {', '.join(missing_queue)}")
        return False

    if missing_backend:
        print(f"\n❌ Missing {backend} backend variables: {', '.join(missing_backend)}")
        return False

    print("\n✅ All required environment variables are set!")
    return True


def test_partitioncache_setup():
    """Test if PartitionCache setup would work with current environment."""
    print("\n" + "=" * 50)
    print("Testing PartitionCache Setup")
    print("=" * 50)

    try:
        # Try to import and test basic setup
        import subprocess

        result = subprocess.run(["python", "-m", "partitioncache.cli.manage_cache", "setup", "all"], capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            print("✅ PartitionCache setup succeeded!")
            print("Output:", result.stdout)
        else:
            print("❌ PartitionCache setup failed!")
            print("Error:", result.stderr)
            return False

    except Exception as e:
        print(f"❌ Error testing setup: {e}")
        return False

    return True


if __name__ == "__main__":
    print("PartitionCache Environment Validation")
    print("=" * 60)

    # Check environment variables
    env_ok = check_env_vars()

    if env_ok:
        # Test actual setup
        setup_ok = test_partitioncache_setup()

        if setup_ok:
            print("\n🎉 Environment is correctly configured for PartitionCache!")
        else:
            print("\n⚠️ Environment variables are set but setup failed. Check logs above.")
    else:
        print("\n⚠️ Please set the missing environment variables before running PartitionCache.")

        print("\nFor local testing, you can set them like this:")
        print("export DB_HOST=localhost")
        print("export DB_PORT=5432")
        print("export DB_USER=your_user")
        print("export DB_PASSWORD=your_password")
        print("export DB_NAME=your_database")
        print("export QUERY_QUEUE_PROVIDER=postgresql")
        print("export PG_QUEUE_HOST=localhost")
        print("export PG_QUEUE_PORT=5432")
        print("export PG_QUEUE_USER=your_user")
        print("export PG_QUEUE_PASSWORD=your_password")
        print("export PG_QUEUE_DB=your_database")
        print("export CACHE_BACKEND=postgresql_array")
