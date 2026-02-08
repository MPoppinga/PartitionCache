FROM postgis/postgis:16-3.4

# 1. Install pg_cron and build dependencies.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        # pg_cron
        postgresql-16-cron \
        # Build dependencies
        build-essential \
        git \
        postgresql-server-dev-16 \
        libc6-dev \
        llvm-13 \
        clang-13 \
        && \
    rm -rf /var/lib/apt/lists/*

# 2. Build extensions from source.
RUN cd /tmp && \
    git clone https://github.com/ChenHuajun/pg_roaringbitmap.git && \
    cd pg_roaringbitmap && \
    make && \
    make install

# 3. Build h3-pg extension (requires cmake)
RUN apt-get update && \
    apt-get install -y --no-install-recommends cmake && \
    cd /tmp && \
    git clone https://github.com/zachasme/h3-pg.git && \
    cd h3-pg && \
    make && \
    make install && \
    rm -rf /tmp/h3-pg && \
    apt-get purge -y cmake && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# 4. Clean up build
RUN rm -rf /tmp/*

# Copy the initialization script to be run by the official postgres entrypoint.
COPY init-db.sh /docker-entrypoint-initdb.d/


