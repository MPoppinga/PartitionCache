#!/usr/bin/env python3
"""
NYC Taxi Data Generator for PartitionCache.

Downloads 2009-2010 TLC Yellow Taxi Parquet data (which includes actual GPS coordinates)
and OpenStreetMap POIs, then loads them into PostgreSQL with PostGIS.

Note: TLC retroactively converted 2011+ Parquet files to zone-based format (PULocationID/
DOLocationID). Only 2009-2010 Parquet files on the CDN retain GPS coordinates.

Tables:
    - taxi_trips (fact table): ~14M rows per month
    - osm_pois (dimension): ~20K-50K NYC POIs

Usage:
    python generate_nyc_taxi_data.py --months 1 --year 2010 --month 1
    python generate_nyc_taxi_data.py --months 12 --year 2010 --month 1
"""

import argparse
import csv
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# NYC bounding box (latitude, longitude)
NYC_LAT_MIN = 40.477
NYC_LAT_MAX = 40.918
NYC_LON_MIN = -74.260
NYC_LON_MAX = -73.700

# Overpass API endpoint
OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"

# OSM POI type definitions: (osm_tag_key, osm_tag_value, query_type, poi_type, poi_category)
# query_type: "node" for standard node queries, "nwr" for node/way/relation (parks need centroids)
OSM_POI_TYPES = [
    ("amenity", "restaurant", "node", "restaurant", "food"),
    ("amenity", "hospital", "node", "hospital", "medical"),
    ("tourism", "hotel", "node", "hotel", "accommodation"),
    ("tourism", "museum", "node", "museum", "attraction"),
    ("tourism", "attraction", "node", "attraction", "attraction"),
    ("railway", "station", "node", "station", "transport"),
    ("amenity", "bar", "node", "bar", "nightlife"),
    ("leisure", "park", "nwr", "park", "recreation"),
    ("amenity", "theatre", "node", "theatre", "culture"),
    ("amenity", "university", "node", "university", "education"),
]

# Column name mappings for different TLC Parquet schemas.
# 2009 data uses different names than 2010 data.
SCHEMA_MAPPINGS = {
    # 2009 schema: Start_Lon/Start_Lat/End_Lon/End_Lat, VARCHAR datetimes
    "2009": {
        "pickup_datetime": "CAST(Trip_Pickup_DateTime AS TIMESTAMP)",
        "dropoff_datetime": "CAST(Trip_Dropoff_DateTime AS TIMESTAMP)",
        "pickup_longitude": "Start_Lon",
        "pickup_latitude": "Start_Lat",
        "dropoff_longitude": "End_Lon",
        "dropoff_latitude": "End_Lat",
        "passenger_count": "CAST(Passenger_Count AS INTEGER)",
        "trip_distance": "Trip_Distance",
        "fare_amount": "Fare_Amt",
        "tip_amount": "Tip_Amt",
        "tolls_amount": "Tolls_Amt",
        "total_amount": "Total_Amt",
        "payment_type": "CASE WHEN LOWER(Payment_Type) LIKE 'cre%' THEN 1 WHEN LOWER(Payment_Type) LIKE 'cas%' THEN 2 WHEN LOWER(Payment_Type) LIKE 'no%' THEN 3 WHEN LOWER(Payment_Type) LIKE 'dis%' THEN 4 ELSE 0 END",
        "rate_code_id": "CAST(Rate_Code AS INTEGER)",
    },
    # 2010 schema: pickup_longitude/latitude, VARCHAR datetimes
    "2010": {
        "pickup_datetime": "CAST(pickup_datetime AS TIMESTAMP)",
        "dropoff_datetime": "CAST(dropoff_datetime AS TIMESTAMP)",
        "pickup_longitude": "pickup_longitude",
        "pickup_latitude": "pickup_latitude",
        "dropoff_longitude": "dropoff_longitude",
        "dropoff_latitude": "dropoff_latitude",
        "passenger_count": "CAST(passenger_count AS INTEGER)",
        "trip_distance": "trip_distance",
        "fare_amount": "fare_amount",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "total_amount": "total_amount",
        "payment_type": "CASE WHEN LOWER(payment_type) LIKE 'cre%' THEN 1 WHEN LOWER(payment_type) LIKE 'cas%' THEN 2 WHEN LOWER(payment_type) LIKE 'no%' THEN 3 WHEN LOWER(payment_type) LIKE 'dis%' THEN 4 ELSE 0 END",
        "rate_code_id": "CASE WHEN rate_code SIMILAR TO '[0-9]+' THEN CAST(rate_code AS INTEGER) ELSE 1 END",
    },
}

# Parquet download URL template
PARQUET_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
)

# CSV columns exported from DuckDB (flat, no geometry yet)
CSV_COLUMNS = [
    "trip_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_longitude",
    "pickup_latitude",
    "dropoff_longitude",
    "dropoff_latitude",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "payment_type",
    "rate_code_id",
    "duration_seconds",
    "pickup_hour",
    "is_rush_hour",
    "is_weekend",
]

# Staging table DDL (flat lon/lat columns for CSV load)
STAGING_TABLE_DDL = """
    CREATE TABLE taxi_trips_staging (
        trip_id INTEGER PRIMARY KEY,
        pickup_datetime TIMESTAMP NOT NULL,
        dropoff_datetime TIMESTAMP NOT NULL,
        pickup_longitude DOUBLE PRECISION NOT NULL,
        pickup_latitude DOUBLE PRECISION NOT NULL,
        dropoff_longitude DOUBLE PRECISION NOT NULL,
        dropoff_latitude DOUBLE PRECISION NOT NULL,
        passenger_count INTEGER,
        trip_distance DOUBLE PRECISION,
        fare_amount DOUBLE PRECISION,
        tip_amount DOUBLE PRECISION,
        tolls_amount DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        payment_type INTEGER,
        rate_code_id INTEGER,
        duration_seconds INTEGER,
        pickup_hour INTEGER,
        is_rush_hour BOOLEAN,
        is_weekend BOOLEAN
    )
"""

# Final taxi_trips table DDL (with PostGIS geometry)
TAXI_TRIPS_DDL = """
    CREATE TABLE taxi_trips (
        trip_id INTEGER PRIMARY KEY,
        pickup_datetime TIMESTAMP NOT NULL,
        dropoff_datetime TIMESTAMP NOT NULL,
        pickup_geom GEOGRAPHY(Point, 4326) NOT NULL,
        dropoff_geom GEOGRAPHY(Point, 4326) NOT NULL,
        passenger_count INTEGER,
        trip_distance DOUBLE PRECISION,
        fare_amount DOUBLE PRECISION,
        tip_amount DOUBLE PRECISION,
        tolls_amount DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        payment_type INTEGER,
        rate_code_id INTEGER,
        duration_seconds INTEGER,
        pickup_hour INTEGER,
        is_rush_hour BOOLEAN,
        is_weekend BOOLEAN
    )
"""

# OSM POIs table DDL
OSM_POIS_DDL = """
    CREATE TABLE osm_pois (
        poi_id INTEGER PRIMARY KEY,
        name VARCHAR,
        poi_type VARCHAR,
        poi_category VARCHAR,
        geom GEOGRAPHY(Point, 4326) NOT NULL
    )
"""


def _download_parquet(year, month, dest_path):
    """Download a single TLC Yellow Taxi Parquet file."""
    url = PARQUET_URL_TEMPLATE.format(year=year, month=month)
    print(f"    Downloading {url} ...")

    try:
        urllib.request.urlretrieve(url, dest_path)
    except urllib.error.HTTPError as e:
        print(f"    ERROR: Failed to download {url}: {e}")
        print(f"    Pre-2016 data with GPS coordinates is available for 2009-2015.")
        raise
    except urllib.error.URLError as e:
        print(f"    ERROR: Network error downloading {url}: {e}")
        raise

    file_size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    print(f"    Downloaded {file_size_mb:.1f} MB")


def _detect_schema(duck_conn, parquet_path):
    """Detect which TLC Parquet schema this file uses (2009 vs 2010)."""
    cols = [
        r[0]
        for r in duck_conn.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{parquet_path}' LIMIT 1)"
        ).fetchall()
    ]
    if "Start_Lon" in cols:
        return "2009"
    elif "pickup_longitude" in cols:
        return "2010"
    else:
        raise ValueError(
            f"Parquet file has no GPS coordinate columns. "
            f"Found columns: {cols}. "
            f"Only 2009-2010 TLC data retains GPS coordinates on the CDN."
        )


def _process_parquet_to_csv(duck_conn, parquet_path, csv_path, trip_id_offset):
    """
    Read Parquet with DuckDB, filter bad records, compute derived columns,
    assign sequential trip_id, and export to CSV.

    Auto-detects schema (2009 vs 2010 column names).
    Returns the number of rows exported.
    """
    schema_key = _detect_schema(duck_conn, parquet_path)
    m = SCHEMA_MAPPINGS[schema_key]
    print(f"    Detected schema: {schema_key}")

    # Column expressions using the schema mapping
    pickup_dt = m["pickup_datetime"]
    dropoff_dt = m["dropoff_datetime"]
    pickup_lon = m["pickup_longitude"]
    pickup_lat = m["pickup_latitude"]
    dropoff_lon = m["dropoff_longitude"]
    dropoff_lat = m["dropoff_latitude"]

    # Build the DuckDB query with filtering and derived columns
    query = f"""
        COPY (
            SELECT
                ROW_NUMBER() OVER (ORDER BY {pickup_dt}) + {trip_id_offset} AS trip_id,
                {pickup_dt} AS pickup_datetime,
                {dropoff_dt} AS dropoff_datetime,
                {pickup_lon} AS pickup_longitude,
                {pickup_lat} AS pickup_latitude,
                {dropoff_lon} AS dropoff_longitude,
                {dropoff_lat} AS dropoff_latitude,
                {m["passenger_count"]} AS passenger_count,
                {m["trip_distance"]} AS trip_distance,
                {m["fare_amount"]} AS fare_amount,
                {m["tip_amount"]} AS tip_amount,
                {m["tolls_amount"]} AS tolls_amount,
                {m["total_amount"]} AS total_amount,
                {m["payment_type"]} AS payment_type,
                {m["rate_code_id"]} AS rate_code_id,
                CAST(
                    EXTRACT(EPOCH FROM ({dropoff_dt} - {pickup_dt}))
                    AS INTEGER
                ) AS duration_seconds,
                EXTRACT(HOUR FROM {pickup_dt}) AS pickup_hour,
                CASE
                    WHEN EXTRACT(DOW FROM {pickup_dt}) BETWEEN 1 AND 5
                         AND (
                             EXTRACT(HOUR FROM {pickup_dt}) BETWEEN 7 AND 9
                             OR EXTRACT(HOUR FROM {pickup_dt}) BETWEEN 16 AND 18
                         )
                    THEN TRUE
                    ELSE FALSE
                END AS is_rush_hour,
                CASE
                    WHEN EXTRACT(DOW FROM {pickup_dt}) IN (0, 6)
                    THEN TRUE
                    ELSE FALSE
                END AS is_weekend
            FROM read_parquet('{parquet_path}')
            WHERE
                -- Valid pickup coordinates within NYC bbox
                {pickup_lon} != 0
                AND {pickup_lat} != 0
                AND {pickup_lat} BETWEEN {NYC_LAT_MIN} AND {NYC_LAT_MAX}
                AND {pickup_lon} BETWEEN {NYC_LON_MIN} AND {NYC_LON_MAX}
                -- Valid dropoff coordinates within NYC bbox
                AND {dropoff_lon} != 0
                AND {dropoff_lat} != 0
                AND {dropoff_lat} BETWEEN {NYC_LAT_MIN} AND {NYC_LAT_MAX}
                AND {dropoff_lon} BETWEEN {NYC_LON_MIN} AND {NYC_LON_MAX}
                -- Valid trip metrics
                AND {m["trip_distance"]} > 0
                AND {m["fare_amount"]} >= 0
                -- Valid duration (> 0 seconds and < 24 hours)
                AND EXTRACT(EPOCH FROM ({dropoff_dt} - {pickup_dt})) > 0
                AND EXTRACT(EPOCH FROM ({dropoff_dt} - {pickup_dt})) < 86400
        ) TO '{csv_path}' (FORMAT CSV, HEADER)
    """

    duck_conn.execute(query)

    # Count exported rows
    row_count = duck_conn.execute(
        f"SELECT COUNT(*) FROM read_csv('{csv_path}', header=true)"
    ).fetchone()[0]

    return row_count


def _query_overpass(tag_key, tag_value, query_type, max_retries=3):
    """
    Query the Overpass API for OSM features of a given type within the NYC bbox.

    Returns a list of dicts with 'lat', 'lon', 'name' keys.
    """
    bbox = f"{NYC_LAT_MIN},{NYC_LON_MIN},{NYC_LAT_MAX},{NYC_LON_MAX}"

    if query_type == "nwr":
        # For parks and other area features, use nwr with out center
        overpass_query = f"""
[out:json][timeout:120];
nwr["{tag_key}"="{tag_value}"]({bbox});
out center;
"""
    else:
        overpass_query = f"""
[out:json][timeout:120];
node["{tag_key}"="{tag_value}"]({bbox});
out body;
"""

    for attempt in range(1, max_retries + 1):
        try:
            data = urllib.parse.urlencode({"data": overpass_query}).encode("utf-8")
            req = urllib.request.Request(
                OVERPASS_API_URL,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            with urllib.request.urlopen(req, timeout=180) as response:
                result = json.loads(response.read().decode("utf-8"))

            features = []
            for element in result.get("elements", []):
                # For nodes, use lat/lon directly
                # For ways/relations with out center, use center coordinates
                lat = element.get("lat") or element.get("center", {}).get("lat")
                lon = element.get("lon") or element.get("center", {}).get("lon")

                if lat is None or lon is None:
                    continue

                name = element.get("tags", {}).get("name", "")
                features.append({"lat": lat, "lon": lon, "name": name})

            return features

        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries:
                wait_time = 30 * attempt
                print(f"      Rate limited (429), waiting {wait_time}s before retry {attempt}/{max_retries}...")
                time.sleep(wait_time)
                continue
            elif e.code == 504 and attempt < max_retries:
                wait_time = 15 * attempt
                print(f"      Timeout (504), waiting {wait_time}s before retry {attempt}/{max_retries}...")
                time.sleep(wait_time)
                continue
            else:
                print(f"      ERROR: Overpass API returned HTTP {e.code}: {e.reason}")
                raise

        except urllib.error.URLError as e:
            if attempt < max_retries:
                wait_time = 10 * attempt
                print(f"      Network error, waiting {wait_time}s before retry {attempt}/{max_retries}...")
                time.sleep(wait_time)
                continue
            else:
                print(f"      ERROR: Network error querying Overpass API: {e}")
                raise

    return []


def _download_osm_pois(csv_path):
    """
    Download OSM POIs for NYC via Overpass API (per-type queries) and write to CSV.

    Returns the number of POIs written.
    """
    poi_id = 0

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["poi_id", "name", "poi_type", "poi_category", "longitude", "latitude"])

        for tag_key, tag_value, query_type, poi_type, poi_category in OSM_POI_TYPES:
            print(f"    Querying OSM: {tag_key}={tag_value} ({query_type}) ...")

            try:
                features = _query_overpass(tag_key, tag_value, query_type)
            except Exception as e:
                print(f"      WARNING: Failed to fetch {poi_type}, skipping: {e}")
                features = []

            for feature in features:
                poi_id += 1
                writer.writerow([
                    poi_id,
                    feature["name"] or "",
                    poi_type,
                    poi_category,
                    feature["lon"],
                    feature["lat"],
                ])

            print(f"      Found {len(features)} {poi_type} features")

            # Respectful delay between queries
            time.sleep(2)

    return poi_id


def _report_selectivity(conn):
    """Run sample queries to report selectivity estimates for key conditions."""
    print("\n  Selectivity estimates:")

    queries = [
        (
            "Trips > 45 min (2700s)",
            "SELECT COUNT(*) FILTER (WHERE duration_seconds > 2700)::float / COUNT(*) * 100 FROM taxi_trips",
        ),
        (
            "Trips > 10 miles",
            "SELECT COUNT(*) FILTER (WHERE trip_distance > 10)::float / COUNT(*) * 100 FROM taxi_trips",
        ),
        (
            "Rush hour trips",
            "SELECT COUNT(*) FILTER (WHERE is_rush_hour)::float / COUNT(*) * 100 FROM taxi_trips",
        ),
        (
            "Weekend trips",
            "SELECT COUNT(*) FILTER (WHERE is_weekend)::float / COUNT(*) * 100 FROM taxi_trips",
        ),
        (
            "Fare > $50",
            "SELECT COUNT(*) FILTER (WHERE fare_amount > 50)::float / COUNT(*) * 100 FROM taxi_trips",
        ),
        (
            "Tip > 20% of fare",
            "SELECT COUNT(*) FILTER (WHERE fare_amount > 0 AND tip_amount / fare_amount > 0.20)::float / COUNT(*) * 100 FROM taxi_trips",
        ),
        (
            "Night trips (22:00-05:00)",
            "SELECT COUNT(*) FILTER (WHERE pickup_hour >= 22 OR pickup_hour < 5)::float / COUNT(*) * 100 FROM taxi_trips",
        ),
    ]

    with conn.cursor() as cur:
        for label, query in queries:
            try:
                cur.execute(query)  # type: ignore[arg-type]
                row = cur.fetchone()
                pct = row[0] if row else None
                if pct is not None:
                    print(f"    {label + ':':35s} {pct:6.2f}%")
                else:
                    print(f"    {label + ':':35s} N/A")
            except Exception as e:
                print(f"    {label + ':':35s} ERROR ({e})")


def main():
    parser = argparse.ArgumentParser(
        description="Generate NYC Taxi benchmark data with PostGIS"
    )
    parser.add_argument(
        "--months", type=int, default=1,
        help="Number of months of data to download (default: 1)",
    )
    parser.add_argument(
        "--year", type=int, default=2010,
        help="Starting year (default: 2010, must be 2009-2010 for GPS data on CDN)",
    )
    parser.add_argument(
        "--month", type=int, default=1,
        help="Starting month (default: 1)",
    )
    parser.add_argument("--db-name", type=str, default=None, help="PostgreSQL database name")
    parser.add_argument("--db-host", type=str, default=None, help="PostgreSQL host")
    parser.add_argument("--db-port", type=int, default=None, help="PostgreSQL port")
    parser.add_argument("--db-user", type=str, default=None, help="PostgreSQL user")
    parser.add_argument("--db-password", type=str, default=None, help="PostgreSQL password")

    args = parser.parse_args()

    # Validate year range
    if args.year < 2009 or args.year > 2010:
        print("WARNING: Only 2009-2010 TLC Parquet files on the CDN retain GPS coordinates.")
        print("         2011+ files were retroactively converted to zone IDs (PULocationID/DOLocationID).")
        print("         Proceeding, but data loading will fail if GPS columns are missing.")
        print("         Continuing, but data may not contain valid coordinates.")

    try:
        import duckdb
        import psycopg
        from dotenv import load_dotenv
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install duckdb psycopg[binary] python-dotenv")
        sys.exit(1)

    # Load .env configuration
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

    db_host = args.db_host or os.getenv("DB_HOST", "localhost")
    db_port = args.db_port or int(os.getenv("DB_PORT", "5432"))
    db_user = args.db_user or os.getenv("DB_USER", "app_user")
    db_password = args.db_password or os.getenv("DB_PASSWORD", "")
    db_name = args.db_name or os.getenv("DB_NAME", "nyc_taxi_db")

    # Build list of (year, month) tuples to download
    months_to_download = []
    current_year = args.year
    current_month = args.month
    for _ in range(args.months):
        months_to_download.append((current_year, current_month))
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    months_str = ", ".join(f"{y}-{m:02d}" for y, m in months_to_download)
    print(f"NYC Taxi Data Generator")
    print(f"  Months: {months_str}")
    print(f"  Target: PostgreSQL {db_host}:{db_port}/{db_name}")
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        duck_conn = duckdb.connect(":memory:")

        # ==================================================================
        # Step 1: Download and process Parquet files
        # ==================================================================
        print("Step 1: Downloading and processing Parquet files")

        csv_paths = []
        total_trip_count = 0
        trip_id_offset = 0

        for year, month in months_to_download:
            parquet_filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
            parquet_path = os.path.join(tmpdir, parquet_filename)
            csv_filename = f"taxi_trips_{year}-{month:02d}.csv"
            csv_path = os.path.join(tmpdir, csv_filename)

            _download_parquet(year, month, parquet_path)

            print(f"    Processing {parquet_filename} ...")
            row_count = _process_parquet_to_csv(
                duck_conn, parquet_path, csv_path, trip_id_offset,
            )
            print(f"    Exported {row_count:,} valid trips to CSV")

            csv_paths.append(csv_path)
            trip_id_offset += row_count
            total_trip_count += row_count

            # Remove parquet file to save disk space
            os.remove(parquet_path)

        print(f"  Total valid trips across all months: {total_trip_count:,}")
        print()

        duck_conn.close()

        # ==================================================================
        # Step 2: Download OSM POIs
        # ==================================================================
        print("Step 2: Downloading OSM POIs via Overpass API")
        pois_csv_path = os.path.join(tmpdir, "osm_pois.csv")
        poi_count = _download_osm_pois(pois_csv_path)
        print(f"  Total POIs downloaded: {poi_count:,}")
        print()

        # ==================================================================
        # Step 3: Create PostgreSQL tables and load data
        # ==================================================================
        print("Step 3: Setting up PostgreSQL tables")

        pg_conn = psycopg.connect(
            host=db_host, port=db_port, user=db_user,
            password=db_password, dbname=db_name,
        )
        pg_conn.autocommit = True

        with pg_conn.cursor() as cur:
            # Enable PostGIS
            print("  Enabling PostGIS extension...")
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")

            # Drop existing tables
            print("  Dropping existing tables...")
            cur.execute("DROP TABLE IF EXISTS taxi_trips_staging CASCADE")
            cur.execute("DROP TABLE IF EXISTS taxi_trips CASCADE")
            cur.execute("DROP TABLE IF EXISTS osm_pois CASCADE")

            # Create staging table
            print("  Creating staging table...")
            cur.execute(STAGING_TABLE_DDL)

            # Create final taxi_trips table
            print("  Creating taxi_trips table...")
            cur.execute(TAXI_TRIPS_DDL)

            # Create osm_pois table
            print("  Creating osm_pois table...")
            cur.execute(OSM_POIS_DDL)

        print()

        # ==================================================================
        # Step 4: Load taxi trip data via staging table
        # ==================================================================
        print("Step 4: Loading taxi trip data into PostgreSQL")

        with pg_conn.cursor() as cur:
            for csv_path in csv_paths:
                csv_name = os.path.basename(csv_path)
                print(f"    Loading {csv_name} into staging table...")

                # Truncate staging for each file
                cur.execute("TRUNCATE taxi_trips_staging")

                with open(csv_path, "rb") as f:
                    with cur.copy(
                        "COPY taxi_trips_staging FROM STDIN WITH (FORMAT CSV, HEADER)"
                    ) as copy:
                        while data := f.read(65536):
                            copy.write(data)

                # Insert from staging into final table with geometry conversion
                print(f"    Converting coordinates to PostGIS geometries...")
                cur.execute("""
                    INSERT INTO taxi_trips (
                        trip_id, pickup_datetime, dropoff_datetime,
                        pickup_geom, dropoff_geom,
                        passenger_count, trip_distance,
                        fare_amount, tip_amount, tolls_amount, total_amount,
                        payment_type, rate_code_id,
                        duration_seconds, pickup_hour, is_rush_hour, is_weekend
                    )
                    SELECT
                        trip_id, pickup_datetime, dropoff_datetime,
                        ST_SetSRID(ST_MakePoint(pickup_longitude, pickup_latitude), 4326)::geography,
                        ST_SetSRID(ST_MakePoint(dropoff_longitude, dropoff_latitude), 4326)::geography,
                        passenger_count, trip_distance,
                        fare_amount, tip_amount, tolls_amount, total_amount,
                        payment_type, rate_code_id,
                        duration_seconds, pickup_hour, is_rush_hour, is_weekend
                    FROM taxi_trips_staging
                """)

                cur.execute("SELECT COUNT(*) FROM taxi_trips_staging")
                row = cur.fetchone()
                staged_count = row[0] if row else 0
                print(f"    Inserted {staged_count:,} trips with geometry")

            # Drop staging table
            print("  Dropping staging table...")
            cur.execute("DROP TABLE IF EXISTS taxi_trips_staging")

        print()

        # ==================================================================
        # Step 5: Load OSM POIs
        # ==================================================================
        print("Step 5: Loading OSM POIs into PostgreSQL")

        # Create a staging table for POIs too (need to convert lon/lat to geometry)
        with pg_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE osm_pois_staging (
                    poi_id INTEGER PRIMARY KEY,
                    name VARCHAR,
                    poi_type VARCHAR,
                    poi_category VARCHAR,
                    longitude DOUBLE PRECISION NOT NULL,
                    latitude DOUBLE PRECISION NOT NULL
                )
            """)

            with open(pois_csv_path, "rb") as f:
                with cur.copy(
                    "COPY osm_pois_staging FROM STDIN WITH (FORMAT CSV, HEADER)"
                ) as copy:
                    while data := f.read(65536):
                        copy.write(data)

            cur.execute("""
                INSERT INTO osm_pois (poi_id, name, poi_type, poi_category, geom)
                SELECT
                    poi_id, name, poi_type, poi_category,
                    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
                FROM osm_pois_staging
            """)

            cur.execute("SELECT COUNT(*) FROM osm_pois")
            row = cur.fetchone()
            loaded_poi_count = row[0] if row else 0
            print(f"  Loaded {loaded_poi_count:,} POIs")

            cur.execute("DROP TABLE IF EXISTS osm_pois_staging")

        print()

        # ==================================================================
        # Step 6: Create indexes
        # ==================================================================
        print("Step 6: Creating indexes")

        with pg_conn.cursor() as cur:
            indexes = [
                # Geography columns use plain GiST indexes (no cast needed)
                ("idx_trips_pickup_geog", "CREATE INDEX idx_trips_pickup_geog ON taxi_trips USING GIST (pickup_geom)"),
                ("idx_trips_dropoff_geog", "CREATE INDEX idx_trips_dropoff_geog ON taxi_trips USING GIST (dropoff_geom)"),
                ("idx_trips_duration", "CREATE INDEX idx_trips_duration ON taxi_trips (duration_seconds)"),
                ("idx_trips_distance", "CREATE INDEX idx_trips_distance ON taxi_trips (trip_distance)"),
                ("idx_trips_pickup_hour", "CREATE INDEX idx_trips_pickup_hour ON taxi_trips (pickup_hour)"),
                ("idx_pois_geog", "CREATE INDEX idx_pois_geog ON osm_pois USING GIST (geom)"),
                ("idx_pois_type", "CREATE INDEX idx_pois_type ON osm_pois (poi_type)"),
            ]

            for idx_name, idx_sql in indexes:
                print(f"    Creating {idx_name}...")
                cur.execute(idx_sql)  # type: ignore[arg-type]

        print()

        # ==================================================================
        # Step 7: Analyze tables
        # ==================================================================
        print("Step 7: Analyzing tables")

        with pg_conn.cursor() as cur:
            cur.execute("ANALYZE taxi_trips")
            cur.execute("ANALYZE osm_pois")

        print("  ANALYZE complete")
        print()

        # ==================================================================
        # Step 8: Report statistics
        # ==================================================================
        print("Step 8: Final statistics")

        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM taxi_trips")
            row = cur.fetchone()
            trip_count = row[0] if row else 0
            cur.execute("SELECT COUNT(*) FROM osm_pois")
            row = cur.fetchone()
            poi_count_final = row[0] if row else 0

        print(f"  taxi_trips: {trip_count:>12,} rows")
        print(f"  osm_pois:   {poi_count_final:>12,} rows")

        # POI breakdown by category
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT poi_type, COUNT(*) FROM osm_pois GROUP BY poi_type ORDER BY COUNT(*) DESC"
            )
            print("\n  POI breakdown by type:")
            for row in cur.fetchall():
                print(f"    {row[0] + ':':20s} {row[1]:>6,}")

        _report_selectivity(pg_conn)

    pg_conn.close()

    print(f"\nNYC Taxi dataset created successfully in PostgreSQL ({db_host}:{db_port}/{db_name})")


if __name__ == "__main__":
    main()
