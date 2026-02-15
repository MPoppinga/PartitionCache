# PostgreSQL with pg_cron, roaringbitmap, PostGIS, and h3-pg extensions for integration testing
# Based on official PostgreSQL 16 image with all extensions
FROM postgres:16

# Install required extensions and build dependencies
RUN apt-get update && apt-get install -y \
    postgresql-16-cron \
    postgresql-16-postgis-3 \
    build-essential \
    git \
    cmake \
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

# Install h3-pg extension from source (requires cmake for bundled h3 library)
RUN cd /tmp && \
    git clone https://github.com/zachasme/h3-pg.git && \
    cd h3-pg && \
    make && \
    make install && \
    cd / && \
    rm -rf /tmp/h3-pg

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