#!/bin/bash
set -e

# Start PostgreSQL in the background to perform initialization
gosu postgres postgres --single -c "config_file=/etc/postgresql/postgresql.conf" &
PG_PID=$!

# Wait for PostgreSQL to be ready
until pg_isready -h localhost -p 5432 -U "$POSTGRES_USER"; do
  sleep 1
done

# Enable extensions and set up the database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS pg_cron;
    CREATE EXTENSION IF NOT EXISTS roaringbitmap;
EOSQL

# Stop the background PostgreSQL process
kill $PG_PID
wait $PG_PID 