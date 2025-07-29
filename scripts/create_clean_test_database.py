#!/usr/bin/env python3
"""
Script to create a clean test database with all required extensions and tables.
Replaces template database functionality with direct database creation.
"""

import argparse
import os
import sys

import psycopg
from psycopg import sql


def create_clean_database(unique_db: str, conn_str: str) -> None:
    """Create a clean database without using templates."""
    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Drop database if it exists
            cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (unique_db,))
            if cur.fetchone():
                print(f"Dropping existing database: {unique_db}")
                cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(unique_db)))

            # Create clean database (no template needed)
            print(f"Creating clean database: {unique_db}")
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(unique_db)))
            print(f"Successfully created database: {unique_db}")


def setup_database_extensions_and_tables(unique_db: str, require_pg_cron: bool = False) -> None:
    """Set up extensions and tables in the newly created database."""
    # Connect to the new database and set up extensions and tables
    conn_str_postgres = f"postgresql://integration_user:integration_password@localhost:{os.getenv('PG_PORT', '5432')}/postgres"
    conn_str_target_db = f"postgresql://integration_user:integration_password@localhost:{os.getenv('PG_PORT', '5432')}/{unique_db}"

    # Connect to 'postgres' database to drop/create the target database
    with psycopg.connect(conn_str_postgres, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Drop database if it exists
            cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), (unique_db,))
            if cur.fetchone():
                print(f"Dropping existing database: {unique_db}")
                cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE);").format(sql.Identifier(unique_db)))

            # Create clean database
            print(f"Creating clean database: {unique_db}")
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(unique_db)))
            print(f"Successfully created database: {unique_db}")

    # Connect to the newly created database to set up extensions and tables
    with psycopg.connect(conn_str_target_db, autocommit=True) as conn:
        with conn.cursor() as cur:
            print("Setting up extensions...")
            # Create roaringbitmap extension (always available)
            cur.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap;")

            # Handle pg_cron extension based on requirements
            if require_pg_cron:
                # For tests that require pg_cron, verify it exists
                cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'")
                if cur.fetchone():
                    print("✅ pg_cron extension verified (required for this test)")
                    # Grant necessary permissions for pg_cron
                    cur.execute("GRANT USAGE ON SCHEMA cron TO integration_user;")
                    cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cron TO integration_user;")
                    cur.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA cron TO integration_user;")
                else:
                    print("❌ pg_cron extension REQUIRED but not found!")
                    print(f"Note: This database ({unique_db}) must be the same as cron.database_name in PostgreSQL config")
                    raise Exception(f"pg_cron extension required but not available in database {unique_db}")
            else:
                # Try to create pg_cron extension (may fail if not the designated cron database)
                try:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_cron;")
                    print("pg_cron extension created successfully")

                    # Grant necessary permissions for pg_cron only if extension was created
                    cur.execute("GRANT USAGE ON SCHEMA cron TO integration_user;")
                    cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA cron TO integration_user;")
                    cur.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA cron TO integration_user;")
                except Exception as e:
                    print(f"pg_cron extension not created (this may be expected): {e}")
                    print("Note: pg_cron can only be created in the database specified by cron.database_name")

            print("Creating test tables...")
            # Create test tables needed for integration tests
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

            cur.execute("""
                CREATE TABLE test_cron_results (
                    id SERIAL PRIMARY KEY,
                    message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

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

            # Create test_spatial_points table for spatial cache testing
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

            # Grant permissions on test tables
            cur.execute("GRANT ALL PRIVILEGES ON test_cron_results TO integration_user;")
            cur.execute("GRANT USAGE, SELECT ON SEQUENCE test_cron_results_id_seq TO integration_user;")
            cur.execute("GRANT ALL PRIVILEGES ON test_locations TO integration_user;")
            cur.execute("GRANT USAGE, SELECT ON SEQUENCE test_locations_id_seq TO integration_user;")
            cur.execute("GRANT ALL PRIVILEGES ON test_businesses TO integration_user;")
            cur.execute("GRANT USAGE, SELECT ON SEQUENCE test_businesses_id_seq TO integration_user;")
            cur.execute("GRANT ALL PRIVILEGES ON test_spatial_points TO integration_user;")
            cur.execute("GRANT USAGE, SELECT ON SEQUENCE test_spatial_points_id_seq TO integration_user;")

            # Install PartitionCache queue processor functions for manual testing
            print("Installing PartitionCache queue processor functions...")
            try:
                # Read and execute the PostgreSQL queue processor SQL file
                sql_file_path = os.path.join(os.path.dirname(__file__), "..", "src", "partitioncache", "queue_handler", "postgresql_queue_processor.sql")
                if os.path.exists(sql_file_path):
                    with open(sql_file_path) as f:
                        sql_content = f.read()

                    # Execute the entire SQL content (PostgreSQL can handle multiple statements)
                    cur.execute(sql_content)
                    print("Queue processor functions installed successfully")
                else:
                    print(f"SQL file not found at: {sql_file_path}")
                    # Try alternative path (in case we're in different directory structure)
                    alt_path = os.path.join(os.getcwd(), "src", "partitioncache", "queue_handler", "postgresql_queue_processor.sql")
                    if os.path.exists(alt_path):
                        with open(alt_path) as f:
                            sql_content = f.read()
                        cur.execute(sql_content)
                        print(f"Queue processor functions installed successfully from: {alt_path}")
                    else:
                        print(f"Alternative path also not found: {alt_path}")

            except Exception as e:
                print(f"Warning: Could not install queue processor functions: {e}")
                # This is not critical for most tests

            print("Database setup completed successfully")


def main():
    """Main function to create and setup database."""
    parser = argparse.ArgumentParser(description="Create clean test database with extensions")
    parser.add_argument("--require-pg-cron", action="store_true",
                        help="Require pg_cron extension to be available (fail if not found)")
    args = parser.parse_args()

    unique_db = os.environ.get("UNIQUE_DB_NAME")
    if not unique_db:
        print("ERROR: UNIQUE_DB_NAME environment variable not set")
        sys.exit(1)

    try:
        setup_database_extensions_and_tables(unique_db, require_pg_cron=args.require_pg_cron)
        print(f"Successfully configured database: {unique_db}")
    except Exception as e:
        print(f"Error creating database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
