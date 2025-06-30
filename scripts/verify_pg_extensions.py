#!/usr/bin/env python3
"""
PostgreSQL Extensions Verification Script

This script verifies that required PostgreSQL extensions (pg_cron and roaringbitmap)
are properly installed and configured for PartitionCache.

Usage:
    python scripts/verify_pg_extensions.py
    python scripts/verify_pg_extensions.py --connection-string "postgresql://user:pass@host:port/db"
"""

import argparse
import os
import sys

try:
    import psycopg
except ImportError:
    print("❌ psycopg not installed. Install with: pip install psycopg[binary]")
    sys.exit(1)


def get_connection_string() -> str:
    """Get PostgreSQL connection string from arguments or environment."""
    parser = argparse.ArgumentParser(description="Verify PostgreSQL extensions for PartitionCache")
    parser.add_argument("--connection-string", help="PostgreSQL connection string (default: from environment variables)")
    parser.add_argument("--quiet", "-q", action="store_true", help="Only output errors")

    args = parser.parse_args()

    if args.connection_string:
        return args.connection_string

    # Build from environment variables
    host = os.getenv("PG_HOST", os.getenv("DB_HOST", "localhost"))
    port = os.getenv("PG_PORT", os.getenv("DB_PORT", "5432"))
    user = os.getenv("PG_USER", os.getenv("DB_USER", "postgres"))
    password = os.getenv("PG_PASSWORD", os.getenv("DB_PASSWORD", ""))
    dbname = os.getenv("PG_DBNAME", os.getenv("DB_NAME", "postgres"))

    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    else:
        return f"postgresql://{user}@{host}:{port}/{dbname}"


def check_extension(cursor, extension_name: str, quiet: bool = False) -> bool:
    """Check if a PostgreSQL extension is installed."""
    try:
        cursor.execute("SELECT extname, extversion FROM pg_extension WHERE extname = %s", (extension_name,))
        result = cursor.fetchone()

        if result:
            if not quiet:
                print(f"✅ {extension_name} extension is installed (version: {result[1]})")
            return True
        else:
            print(f"❌ {extension_name} extension is NOT installed")
            return False

    except Exception as e:
        print(f"❌ Error checking {extension_name} extension: {e}")
        return False


def check_pg_cron_config(cursor, quiet: bool = False) -> bool:
    """Check pg_cron specific configuration."""
    try:
        # Check if pg_cron is in shared_preload_libraries
        cursor.execute("SHOW shared_preload_libraries")
        shared_libs = cursor.fetchone()[0]

        if "pg_cron" in shared_libs:
            if not quiet:
                print("✅ pg_cron is in shared_preload_libraries")
        else:
            print("⚠️  pg_cron is NOT in shared_preload_libraries")
            print("   Add 'pg_cron' to shared_preload_libraries in postgresql.conf and restart PostgreSQL")
            return False

        # Check cron database configuration
        try:
            cursor.execute("SHOW cron.database_name")
            cron_db = cursor.fetchone()[0]
            if not quiet:
                print(f"✅ pg_cron database configured: {cron_db}")
        except Exception:
            print("⚠️  cron.database_name not configured")
            print("   Add 'cron.database_name = your_db_name' to postgresql.conf")

        # Check if cron schema exists
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'cron'")
        if cursor.fetchone():
            if not quiet:
                print("✅ cron schema exists")
        else:
            print("❌ cron schema does not exist")
            print("   Run: CREATE EXTENSION IF NOT EXISTS pg_cron;")
            return False

        return True

    except Exception as e:
        print(f"❌ Error checking pg_cron configuration: {e}")
        return False


def test_roaringbitmap_functionality(cursor, quiet: bool = False) -> bool:
    """Test basic roaringbitmap functionality."""
    try:
        # Test basic roaringbitmap operations
        cursor.execute("SELECT rb_build(ARRAY[1,2,3,4,5])")
        result = cursor.fetchone()

        if result:
            if not quiet:
                print("✅ roaringbitmap basic functionality works")
            return True
        else:
            print("❌ roaringbitmap basic test failed")
            return False

    except Exception as e:
        print(f"❌ Error testing roaringbitmap functionality: {e}")
        return False


def test_pg_cron_functionality(cursor, quiet: bool = False) -> bool:
    """Test basic pg_cron functionality."""
    try:
        # Check if we can access cron.job table
        cursor.execute("SELECT count(*) FROM cron.job")
        count = cursor.fetchone()[0]

        if not quiet:
            print(f"✅ pg_cron job table accessible ({count} jobs configured)")
        return True

    except Exception as e:
        print(f"❌ Error testing pg_cron functionality: {e}")
        return False


def main():
    """Main verification function."""
    connection_string = get_connection_string()
    quiet = "--quiet" in sys.argv or "-q" in sys.argv

    if not quiet:
        print("🔍 Verifying PostgreSQL extensions for PartitionCache...")
        print(f"Connecting to: {connection_string.split('@')[1] if '@' in connection_string else connection_string}")
        print()

    try:
        # Connect to PostgreSQL
        conn = psycopg.connect(connection_string)
        conn.autocommit = True
        cursor = conn.cursor()

        if not quiet:
            # Get PostgreSQL version
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            print(f"PostgreSQL version: {version.split(',')[0]}")
            print()

        # Check extensions
        extensions_ok = True

        # Check pg_cron
        pg_cron_installed = check_extension(cursor, "pg_cron", quiet)
        if pg_cron_installed:
            pg_cron_config_ok = check_pg_cron_config(cursor, quiet)
            if pg_cron_config_ok:
                pg_cron_functional = test_pg_cron_functionality(cursor, quiet)
                extensions_ok = extensions_ok and pg_cron_functional
            else:
                extensions_ok = False
        else:
            extensions_ok = False

        if not quiet:
            print()

        # Check roaringbitmap
        roaringbitmap_installed = check_extension(cursor, "roaringbitmap", quiet)
        if roaringbitmap_installed:
            roaringbitmap_functional = test_roaringbitmap_functionality(cursor, quiet)
            extensions_ok = extensions_ok and roaringbitmap_functional
        else:
            extensions_ok = False

        cursor.close()
        conn.close()

        # Final result
        if not quiet:
            print()

        if extensions_ok:
            if not quiet:
                print("🎉 All PostgreSQL extensions are properly configured!")
                print("   PartitionCache is ready to use.")
            sys.exit(0)
        else:
            print("❌ Some extensions are missing or misconfigured.")
            print("   See CI_SETUP.md for detailed installation instructions.")
            sys.exit(1)

    except Exception as e:
        print(f"❌ Connection error: {e}")
        print("   Check your connection parameters and ensure PostgreSQL is running.")
        sys.exit(1)


if __name__ == "__main__":
    main()
