# PostgreSQL with pg_cron and roaringbitmap extensions for integration testing
# Based on official PostgreSQL 16 image with both extensions
FROM postgres:16

# Install required extensions
RUN apt-get update && apt-get install -y \
    postgresql-16-cron \
    build-essential \
    git \
    postgresql-server-dev-16 \
    && rm -rf /var/lib/apt/lists/*

# Install roaringbitmap extension from source
RUN cd /tmp && \
    git clone https://github.com/ChenHuajun/pg_roaringbitmap.git && \
    cd pg_roaringbitmap && \
    make && \
    make install && \
    cd / && \
    rm -rf /tmp/pg_roaringbitmap

# Configure PostgreSQL for pg_cron
# These settings are critical for pg_cron to function correctly
RUN echo "shared_preload_libraries = 'pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "cron.database_name = 'partitioncache_integration'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "cron.host = '/var/run/postgresql'" >> /usr/share/postgresql/postgresql.conf.sample

# Create initialization script to set up both extensions
RUN mkdir -p /docker-entrypoint-initdb.d
COPY init-pg-cron.sql /docker-entrypoint-initdb.d/

# Expose PostgreSQL port
EXPOSE 5432