# PostgreSQL with pg_cron extension for integration testing
# Based on official PostgreSQL 16 image with pg_cron support
FROM postgres:16

# Install pg_cron extension
RUN apt-get update && apt-get install -y \
    postgresql-16-cron \
    && rm -rf /var/lib/apt/lists/*

# Configure PostgreSQL for pg_cron
# These settings are critical for pg_cron to function correctly
RUN echo "shared_preload_libraries = 'pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "cron.database_name = 'test_db'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "cron.host = '/var/run/postgresql'" >> /usr/share/postgresql/postgresql.conf.sample

# Create initialization script to set up pg_cron extension
RUN mkdir -p /docker-entrypoint-initdb.d
COPY init-pg-cron.sql /docker-entrypoint-initdb.d/

# Expose PostgreSQL port
EXPOSE 5432