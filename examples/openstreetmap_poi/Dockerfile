FROM postgis/postgis:16-3.4

# Install build dependencies and pg_cron
RUN apt-get update && \
    apt-get install -y \
        postgresql-16-cron \
        build-essential \
        git \
        postgresql-server-dev-16 \
        libc6-dev \
        llvm-13 \
        clang-13 \
        && \
    rm -rf /var/lib/apt/lists/*

# Install pg_roaringbitmaps from source
RUN cd /tmp && \
    git clone https://github.com/ChenHuajun/pg_roaringbitmap.git && \
    cd pg_roaringbitmap && \
    make && \
    make install && \
    cd / && \
    rm -rf /tmp/pg_roaringbitmap

# Ensure the extensions are available in the default database
# (You still need to run CREATE EXTENSION pg_cron; and CREATE EXTENSION roaringbitmap; in your DB)