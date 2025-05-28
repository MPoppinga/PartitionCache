FROM postgis/postgis:16-3.4

# Install pg_cron
RUN apt-get update && \
    apt-get install -y postgresql-16-cron && \
    rm -rf /var/lib/apt/lists/*

# Ensure the extension is available in the default database
# (You still need to run CREATE EXTENSION pg_cron; in your DB)