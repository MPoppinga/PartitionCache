#!/usr/bin/env python3
"""
Script to create a clean test database with all required extensions and tables.
Replaces template database functionality with direct database creation.
"""

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


def setup_database_extensions_and_tables(unique_db: str) -> None:
    """Set up extensions and tables in the newly created database."""
    # Connect to the new database and set up extensions and tables
    db_conn_str = f"postgresql://integration_user:integration_password@localhost:5432/{unique_db}"
    with psycopg.connect(db_conn_str, autocommit=True) as conn:
        with conn.cursor() as cur:
            print("Setting up extensions...")
            # Create roaringbitmap extension (always available)
            cur.execute("CREATE EXTENSION IF NOT EXISTS roaringbitmap;")

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
                    x DECIMAL(10,6) NOT NULL,
                    y DECIMAL(10,6) NOT NULL,
                    rating DECIMAL(2,1) DEFAULT 3.0,
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

            print("Database setup completed successfully")


def main():
    """Main function to create and setup database."""
    unique_db = os.environ.get("UNIQUE_DB_NAME")
    if not unique_db:
        print("ERROR: UNIQUE_DB_NAME environment variable not set")
        sys.exit(1)

    conn_str = "postgresql://integration_user:integration_password@localhost:5432/postgres"

    try:
        create_clean_database(unique_db, conn_str)
        setup_database_extensions_and_tables(unique_db)
        print(f"Successfully created and configured database: {unique_db}")
    except Exception as e:
        print(f"Error creating database: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
