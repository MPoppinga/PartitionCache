#!/usr/bin/env python3
"""
Test Data Setup Script for PartitionCache

This script sets up test datasets for integration testing. It can be used
in CI/CD pipelines or for local development testing.

Usage:
    python scripts/setup_test_data.py [--reset] [--size SIZE] [--dataset DATASET]

Options:
    --reset: Drop existing test data before creating new data
    --size: Size of dataset (small, medium, large) [default: small]
    --dataset: Which dataset to create (orders, tpch, osm, all) [default: orders]
"""

import argparse
import os
import random
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

# Add the parent directory to sys.path to import partitioncache
sys.path.append(str(Path(__file__).parent.parent))

try:
    import psycopg
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Please install with: pip install -e '.[testing,db]'")
    sys.exit(1)


class TestDataSetup:
    """Setup test datasets for PartitionCache integration tests."""

    def __init__(self, connection_string: str | None = None):
        """Initialize with database connection."""
        if connection_string:
            self.conn_str = connection_string
        else:
            # Load from environment
            load_dotenv()
            host = os.getenv("PG_HOST", "localhost")
            port = os.getenv("PG_PORT", "5432")
            user = os.getenv("PG_USER", "test_user")
            password = os.getenv("PG_PASSWORD", "test_password")
            dbname = os.getenv("PG_DBNAME", "test_db")
            self.conn_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        self.conn = None

    def connect(self):
        """Connect to the database."""
        try:
            self.conn = psycopg.connect(self.conn_str)
            self.conn.autocommit = True
            print("✓ Connected to database")
        except Exception as e:
            print(f"✗ Failed to connect to database: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def create_orders_dataset(self, size: str = "small", reset: bool = False):
        """Create orders test dataset."""
        print(f"Creating orders dataset (size: {size})...")

        sizes = {"small": 1000, "medium": 10000, "large": 100000}

        num_records = sizes.get(size, 1000)

        with self.conn.cursor() as cur:
            # Create table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_orders (
                    id SERIAL PRIMARY KEY,
                    customer_id INTEGER NOT NULL,
                    order_date DATE NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    region_id INTEGER NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Reset if requested
            if reset:
                cur.execute("TRUNCATE TABLE test_orders RESTART IDENTITY")
                print("  ✓ Reset existing data")

            # Insert test data
            print(f"  Inserting {num_records} orders...")
            batch_size = 1000

            for batch_start in range(0, num_records, batch_size):
                batch_end = min(batch_start + batch_size, num_records)
                batch_data = []

                for _i in range(batch_start, batch_end):
                    order_date = date(2023, 1, 1) + timedelta(days=random.randint(0, 364))
                    batch_data.append(
                        (
                            random.randint(1, num_records // 10),  # customer_id
                            order_date,
                            round(random.uniform(10.0, 1000.0), 2),  # amount
                            random.randint(1, 5),  # region_id
                            random.choice(["pending", "processing", "shipped", "delivered"]),
                        )
                    )

                cur.executemany(
                    """
                    INSERT INTO test_orders (customer_id, order_date, amount, region_id, status)
                    VALUES (%s, %s, %s, %s, %s)
                """,
                    batch_data,
                )

                if batch_end % 5000 == 0:
                    print(f"    Inserted {batch_end} records...")

            # Create indexes for partition keys
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_test_orders_customer_id 
                ON test_orders(customer_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_test_orders_region_id 
                ON test_orders(region_id)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_test_orders_date 
                ON test_orders(order_date)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_test_orders_status 
                ON test_orders(status)
            """)

            # Analyze for query optimization
            cur.execute("ANALYZE test_orders")

            print(f"  ✓ Created orders dataset with {num_records} records")

    def create_spatial_dataset(self, size: str = "small", reset: bool = False):
        """Create spatial points dataset for testing distance-based queries."""
        print(f"Creating spatial dataset (size: {size})...")

        sizes = {
            "small": 5000,  # 5K points
            "medium": 50000,  # 50K points
            "large": 500000,  # 500K points
        }

        num_points = sizes.get(size, 5000)

        with self.conn.cursor() as cur:
            # Create spatial points table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_spatial_points (
                    id SERIAL PRIMARY KEY,
                    x DECIMAL(10,6) NOT NULL,
                    y DECIMAL(10,6) NOT NULL,
                    point_type VARCHAR(50) NOT NULL,
                    name VARCHAR(100),
                    region_id INTEGER NOT NULL,
                    city_id INTEGER NOT NULL,
                    zipcode VARCHAR(10),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create business locations table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_businesses (
                    id SERIAL PRIMARY KEY,
                    x DECIMAL(10,6) NOT NULL,
                    y DECIMAL(10,6) NOT NULL,
                    business_type VARCHAR(50) NOT NULL,
                    name VARCHAR(100) NOT NULL,
                    region_id INTEGER NOT NULL,
                    city_id INTEGER NOT NULL,
                    zipcode VARCHAR(10),
                    rating DECIMAL(3,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            if reset:
                cur.execute("TRUNCATE TABLE test_spatial_points, test_businesses RESTART IDENTITY")
                print("  ✓ Reset existing spatial data")

            # Generate spatial points with realistic geographic distribution
            print(f"  Inserting {num_points} spatial points...")

            # Define regions (simplified city-like areas)
            regions = [
                {"id": 1, "name": "Downtown", "center_x": 52.520008, "center_y": 13.404954, "cities": [1, 2]},
                {"id": 2, "name": "Westside", "center_x": 52.500000, "center_y": 13.350000, "cities": [3, 4]},
                {"id": 3, "name": "Eastside", "center_x": 52.530000, "center_y": 13.450000, "cities": [5, 6]},
                {"id": 4, "name": "Northside", "center_x": 52.550000, "center_y": 13.400000, "cities": [7, 8]},
                {"id": 5, "name": "Southside", "center_x": 52.480000, "center_y": 13.380000, "cities": [9, 10]},
            ]

            point_types = ["restaurant", "cafe", "shop", "park", "school", "hospital", "bank", "hotel", "station", "landmark"]

            batch_size = 1000
            for batch_start in range(0, num_points, batch_size):
                batch_end = min(batch_start + batch_size, num_points)
                batch_data = []

                for _i in range(batch_start, batch_end):
                    # Choose random region
                    region = random.choice(regions)

                    # Add some randomness around region center (±0.02 degrees ≈ ±2km)
                    x = region["center_x"] + random.uniform(-0.02, 0.02)
                    y = region["center_y"] + random.uniform(-0.02, 0.02)

                    # Choose random city within region
                    city_id = random.choice(region["cities"])

                    # Generate zipcode based on city
                    zipcode = f"{10000 + city_id * 100 + random.randint(1, 99):05d}"

                    batch_data.append((x, y, random.choice(point_types), f"{random.choice(point_types).title()} {_i + 1}", region["id"], city_id, zipcode))

                cur.executemany(
                    """
                    INSERT INTO test_spatial_points (x, y, point_type, name, region_id, city_id, zipcode)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                    batch_data,
                )

                if batch_end % 5000 == 0:
                    print(f"    Inserted {batch_end} points...")

            # Generate businesses with similar distribution
            num_businesses = int(num_points * 0.3)  # 30% of points are businesses
            print(f"  Inserting {num_businesses} businesses...")

            business_types = ["pharmacy", "supermarket", "restaurant", "cafe", "bakery", "gas_station", "bank", "hotel", "bookstore", "electronics"]

            business_names = {
                "pharmacy": ["City Pharmacy", "Health Plus", "MediCare", "Quick Pharmacy"],
                "supermarket": ["FreshMart", "SuperSave", "GroceryWorld", "QuickShop"],
                "restaurant": ["Tasty Bites", "Golden Fork", "City Grill", "Food Corner"],
                "cafe": ["Coffee House", "Brew & Bean", "Cafe Central", "Morning Cup"],
                "bakery": ["Fresh Bread", "Golden Bakery", "Sweet Treats", "Daily Bread"],
            }

            for batch_start in range(0, num_businesses, batch_size):
                batch_end = min(batch_start + batch_size, num_businesses)
                batch_data = []

                for _i in range(batch_start, batch_end):
                    region = random.choice(regions)
                    x = region["center_x"] + random.uniform(-0.02, 0.02)
                    y = region["center_y"] + random.uniform(-0.02, 0.02)
                    city_id = random.choice(region["cities"])
                    zipcode = f"{10000 + city_id * 100 + random.randint(1, 99):05d}"

                    business_type = random.choice(business_types)
                    if business_type in business_names:
                        name = f"{random.choice(business_names[business_type])} {_i + 1}"
                    else:
                        name = f"{business_type.replace('_', ' ').title()} {_i + 1}"

                    batch_data.append(
                        (
                            x,
                            y,
                            business_type,
                            name,
                            region["id"],
                            city_id,
                            zipcode,
                            round(random.uniform(1.0, 5.0), 2),  # rating
                        )
                    )

                cur.executemany(
                    """
                    INSERT INTO test_businesses (x, y, business_type, name, region_id, city_id, zipcode, rating)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    batch_data,
                )

                if batch_end % 2000 == 0:
                    print(f"    Inserted {batch_end} businesses...")

            # Create indexes for spatial and partition queries
            print("  Creating indexes...")

            # Spatial indexes (for distance calculations)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_spatial_points_xy ON test_spatial_points(x, y)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_businesses_xy ON test_businesses(x, y)")

            # Partition key indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_spatial_points_region ON test_spatial_points(region_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_spatial_points_city ON test_spatial_points(city_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_spatial_points_zipcode ON test_spatial_points(zipcode)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_businesses_region ON test_businesses(region_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_businesses_city ON test_businesses(city_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_businesses_zipcode ON test_businesses(zipcode)")

            # Type indexes for filtering
            cur.execute("CREATE INDEX IF NOT EXISTS idx_spatial_points_type ON test_spatial_points(point_type)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_businesses_type ON test_businesses(business_type)")

            # Composite indexes for common query patterns
            cur.execute("CREATE INDEX IF NOT EXISTS idx_spatial_points_region_type ON test_spatial_points(region_id, point_type)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_businesses_region_type ON test_businesses(region_id, business_type)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_businesses_city_type ON test_businesses(city_id, business_type)")

            # Analyze tables
            cur.execute("ANALYZE test_spatial_points, test_businesses")

            print(f"  ✓ Created spatial dataset with {num_points} points and {num_businesses} businesses")

    def create_tpch_dataset(self, size: str = "small", reset: bool = False):
        """Create simplified TPC-H dataset."""
        print(f"Creating TPC-H dataset (size: {size})...")

        sizes = {
            "small": 0.1,  # 100MB
            "medium": 1.0,  # 1GB
            "large": 10.0,  # 10GB
        }

        scale_factor = sizes.get(size, 0.1)

        with self.conn.cursor() as cur:
            # Create Nation table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_nation (
                    n_nationkey INTEGER PRIMARY KEY,
                    n_name CHAR(25) NOT NULL,
                    n_regionkey INTEGER NOT NULL,
                    n_comment VARCHAR(152)
                )
            """)

            # Create Customers table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_customers (
                    c_custkey INTEGER PRIMARY KEY,
                    c_name VARCHAR(25) NOT NULL,
                    c_address VARCHAR(40) NOT NULL,
                    c_nationkey INTEGER NOT NULL,
                    c_phone CHAR(15) NOT NULL,
                    c_acctbal DECIMAL(15,2) NOT NULL,
                    c_mktsegment CHAR(10) NOT NULL,
                    c_comment VARCHAR(117)
                )
            """)

            # Create Orders table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_tpch_orders (
                    o_orderkey INTEGER PRIMARY KEY,
                    o_custkey INTEGER NOT NULL,
                    o_orderstatus CHAR(1) NOT NULL,
                    o_totalprice DECIMAL(15,2) NOT NULL,
                    o_orderdate DATE NOT NULL,
                    o_orderpriority CHAR(15) NOT NULL,
                    o_clerk CHAR(15) NOT NULL,
                    o_shippriority INTEGER NOT NULL,
                    o_comment VARCHAR(79)
                )
            """)

            if reset:
                cur.execute("TRUNCATE TABLE test_nation, test_customers, test_tpch_orders RESTART IDENTITY CASCADE")
                print("  ✓ Reset existing TPC-H data")

            # Insert Nations (fixed data)
            nations = [
                (0, "ALGERIA", 0, "Haggle blithely against the furiously regular deposits"),
                (1, "ARGENTINA", 1, "Al foxes promise slyly according to the regular accounts"),
                (2, "BRAZIL", 1, "Y alongside of the pending deposits"),
                (3, "CANADA", 1, "Eas hang ironic, silent packages"),
                (4, "EGYPT", 4, "Y above the carefully unusual theodolites"),
                (5, "ETHIOPIA", 0, "Ven packages wake quickly"),
                (6, "FRANCE", 3, "Refully final requests"),
                (7, "GERMANY", 3, "L platelets"),
                (8, "INDIA", 2, "Ss excuses cajole slyly across the packages"),
                (9, "INDONESIA", 2, "Slyly express asymptotes"),
                (10, "IRAN", 4, "Efully alongside of the slyly final dependencies"),
                (11, "IRAQ", 4, "Nic deposits boost atop the quickly final requests"),
                (12, "JAPAN", 2, "Ously. final, express gifts cajole a"),
                (13, "JORDAN", 4, "Ic deposits are blithely about the carefully regular pa"),
                (14, "KENYA", 0, "Pending excuses haggle furiously deposits"),
                (15, "MOROCCO", 0, "Rns. blithely bold courts among the closely regular packages"),
                (16, "MOZAMBIQUE", 0, "S. ironic, unusual asymptotes wake blithely r"),
                (17, "PERU", 1, "Platelets. blithely pending dependencies use fluffily"),
                (18, "CHINA", 2, "C dependencies. furiously express notornis sleep slyly regular accounts"),
                (19, "ROMANIA", 3, "Ular asymptotes are about the furious multipliers"),
                (20, "SAUDI ARABIA", 4, "Ts. silent requests haggle"),
                (21, "VIETNAM", 2, "Hely enticingly express accounts"),
                (22, "RUSSIA", 3, "Requests against the platelets use never according to the quickly regular pint"),
                (23, "UNITED KINGDOM", 3, "Ously final packages"),
                (24, "UNITED STATES", 1, "Y final packages. slow foxes cajole quickly"),
            ]

            cur.executemany(
                """
                INSERT INTO test_nation (n_nationkey, n_name, n_regionkey, n_comment) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (n_nationkey) DO NOTHING
            """,
                nations,
            )

            # Generate customers
            base_customers = int(150000 * scale_factor)
            # Constrain customer count to fit within bitarray size limits (e.g., 200000)
            # This prevents "out of range" errors in bit-based cache handlers
            num_customers = min(base_customers, 199999)  # Leave room for 0-based indexing
            print(f"  Inserting {num_customers} customers (constrained from {base_customers} for bit-cache compatibility)...")

            for i in range(1, num_customers + 1):
                nation_key = random.randint(0, 24)
                cur.execute(
                    """
                    INSERT INTO test_customers 
                    (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        i,
                        f"Customer#{i:09d}",
                        f"Address Line {i}",
                        nation_key,
                        f"{nation_key:02d}-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                        round(random.uniform(-999.99, 9999.99), 2),
                        random.choice(["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]),
                        f"Customer comment {i}",
                    ),
                )

                if i % 10000 == 0:
                    print(f"    Inserted {i} customers...")

            # Generate orders
            num_orders = int(1500000 * scale_factor)
            print(f"  Inserting {num_orders} orders...")

            for i in range(1, num_orders + 1):
                # Constrain customer keys to fit within bitarray size limits (e.g., 200000)
                # This prevents "out of range" errors in bit-based cache handlers
                max_cust_key = min(num_customers, 199999)  # Leave room for 0-based indexing
                cust_key = random.randint(1, max_cust_key)
                order_date = date(1992, 1, 1) + timedelta(days=random.randint(0, 2557))  # 1992-1998

                cur.execute(
                    """
                    INSERT INTO test_tpch_orders 
                    (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, 
                     o_orderpriority, o_clerk, o_shippriority, o_comment)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        i,
                        cust_key,
                        random.choice(["O", "F", "P"]),
                        round(random.uniform(857.71, 555285.16), 2),
                        order_date,
                        random.choice(["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]),
                        f"Clerk#{random.randint(1, 1000):03d}",
                        0,
                        f"Order comment {i}",
                    ),
                )

                if i % 50000 == 0:
                    print(f"    Inserted {i} orders...")

            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_test_customers_nation ON test_customers(c_nationkey)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_test_tpch_orders_customer ON test_tpch_orders(o_custkey)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_test_tpch_orders_date ON test_tpch_orders(o_orderdate)")

            # Analyze tables
            cur.execute("ANALYZE test_nation, test_customers, test_tpch_orders")

            print(f"  ✓ Created TPC-H dataset with scale factor {scale_factor}")

    def create_ssb_dataset(self, size: str = "small", reset: bool = False):
        """Create SSB (Star Schema Benchmark) dataset.

        Creates all 5 SSB tables: lineorder (fact), customer, supplier, part, date_dim.
        Uses the generate_ssb_data module for data generation.
        """
        from examples.ssb_benchmark.generate_ssb_data import (
            generate_customers,
            generate_date_dim,
            generate_lineorder,
            generate_parts,
            generate_suppliers,
        )

        sizes = {"small": 0.01, "medium": 0.1, "large": 1.0}
        scale_factor = sizes.get(size, 0.01)

        print(f"Creating SSB dataset (size: {size}, SF={scale_factor})...")

        random.seed(42)

        with self.conn.cursor() as cur:
            # Create tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS date_dim (
                    d_datekey INTEGER PRIMARY KEY, d_date VARCHAR(18),
                    d_dayofweek VARCHAR(9), d_month VARCHAR(9), d_year INTEGER,
                    d_yearmonthnum INTEGER, d_yearmonth VARCHAR(7),
                    d_daynuminweek INTEGER, d_daynuminmonth INTEGER, d_daynuminyear INTEGER,
                    d_monthnuminyear INTEGER, d_weeknuminyear INTEGER,
                    d_sellingseason VARCHAR(12), d_lastdayinweekfl INTEGER,
                    d_lastdayinmonthfl INTEGER, d_holidayfl INTEGER, d_weekdayfl INTEGER
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS customer (
                    c_custkey INTEGER PRIMARY KEY, c_name VARCHAR(25), c_address VARCHAR(25),
                    c_city VARCHAR(10), c_nation VARCHAR(15), c_region VARCHAR(12),
                    c_phone VARCHAR(15), c_mktsegment VARCHAR(10)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS supplier (
                    s_suppkey INTEGER PRIMARY KEY, s_name VARCHAR(25), s_address VARCHAR(25),
                    s_city VARCHAR(10), s_nation VARCHAR(15), s_region VARCHAR(12), s_phone VARCHAR(15)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS part (
                    p_partkey INTEGER PRIMARY KEY, p_name VARCHAR(22), p_mfgr VARCHAR(6),
                    p_category VARCHAR(7), p_brand VARCHAR(9), p_color VARCHAR(11),
                    p_type VARCHAR(25), p_size INTEGER, p_container VARCHAR(10)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lineorder (
                    lo_orderkey INTEGER, lo_linenumber INTEGER, lo_custkey INTEGER,
                    lo_partkey INTEGER, lo_suppkey INTEGER, lo_orderdate INTEGER,
                    lo_orderpriority VARCHAR(15), lo_shippriority INTEGER,
                    lo_quantity INTEGER, lo_extendedprice INTEGER, lo_ordtotalprice INTEGER,
                    lo_discount INTEGER, lo_revenue INTEGER, lo_supplycost INTEGER,
                    lo_tax INTEGER, lo_commitdate INTEGER, lo_shipmode VARCHAR(10)
                )
            """)

            if reset:
                cur.execute("TRUNCATE TABLE lineorder, customer, supplier, part, date_dim RESTART IDENTITY CASCADE")
                print("  ✓ Reset existing SSB data")

            # Generate and insert data
            print("  Generating date_dim...")
            date_rows = generate_date_dim()
            cur.executemany(
                "INSERT INTO date_dim VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                date_rows,
            )
            print(f"    Inserted {len(date_rows)} date rows")

            print("  Generating customers...")
            customer_rows = generate_customers(scale_factor)
            cur.executemany(
                "INSERT INTO customer VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                customer_rows,
            )
            print(f"    Inserted {len(customer_rows)} customers")

            print("  Generating suppliers...")
            supplier_rows = generate_suppliers(scale_factor)
            cur.executemany(
                "INSERT INTO supplier VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                supplier_rows,
            )
            print(f"    Inserted {len(supplier_rows)} suppliers")

            print("  Generating parts...")
            part_rows = generate_parts(scale_factor)
            cur.executemany(
                "INSERT INTO part VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                part_rows,
            )
            print(f"    Inserted {len(part_rows)} parts")

            print("  Generating lineorder...")
            date_keys = [row[0] for row in date_rows]
            lineorder_rows = generate_lineorder(
                scale_factor, len(customer_rows), len(supplier_rows), len(part_rows), date_keys,
            )

            batch_size = 5000
            for i in range(0, len(lineorder_rows), batch_size):
                batch = lineorder_rows[i:i + batch_size]
                cur.executemany(
                    "INSERT INTO lineorder VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    batch,
                )
                if (i + batch_size) % 50000 == 0 or i + batch_size >= len(lineorder_rows):
                    print(f"    Inserted {min(i + batch_size, len(lineorder_rows))} lineorder rows...")

            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_lo_custkey ON lineorder(lo_custkey)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_lo_suppkey ON lineorder(lo_suppkey)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_lo_partkey ON lineorder(lo_partkey)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_lo_orderdate ON lineorder(lo_orderdate)")

            cur.execute("ANALYZE date_dim, customer, supplier, part, lineorder")

            print(f"  ✓ Created SSB dataset with scale factor {scale_factor}")
            print(f"    lineorder: {len(lineorder_rows)}, customer: {len(customer_rows)}, "
                  f"supplier: {len(supplier_rows)}, part: {len(part_rows)}, date_dim: {len(date_rows)}")

    def get_dataset_stats(self) -> dict[str, Any]:
        """Get statistics about existing test datasets."""
        stats = {}

        with self.conn.cursor() as cur:
            # Check spatial dataset
            try:
                cur.execute("SELECT COUNT(*) FROM test_spatial_points")
                stats["spatial_points_count"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM test_businesses")
                stats["businesses_count"] = cur.fetchone()[0]
            except:
                stats["spatial_points_count"] = 0
                stats["businesses_count"] = 0

            # Check orders dataset
            try:
                cur.execute("SELECT COUNT(*) FROM test_orders")
                stats["orders_count"] = cur.fetchone()[0]
            except:
                stats["orders_count"] = 0

            # Check TPC-H dataset
            try:
                cur.execute("SELECT COUNT(*) FROM test_tpch_orders")
                stats["tpch_orders_count"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM test_customers")
                stats["tpch_customers_count"] = cur.fetchone()[0]
            except:
                stats["tpch_orders_count"] = 0
                stats["tpch_customers_count"] = 0

            # Check SSB dataset
            try:
                cur.execute("SELECT COUNT(*) FROM lineorder")
                stats["ssb_lineorder_count"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM customer")
                stats["ssb_customer_count"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM supplier")
                stats["ssb_supplier_count"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM part")
                stats["ssb_part_count"] = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM date_dim")
                stats["ssb_date_count"] = cur.fetchone()[0]
            except:
                stats["ssb_lineorder_count"] = 0
                stats["ssb_customer_count"] = 0
                stats["ssb_supplier_count"] = 0
                stats["ssb_part_count"] = 0
                stats["ssb_date_count"] = 0

        return stats

    def cleanup_test_data(self):
        """Remove all test data."""
        print("Cleaning up test data...")

        with self.conn.cursor() as cur:
            tables = ["test_spatial_points", "test_businesses", "test_orders", "test_tpch_orders", "test_customers", "test_nation",
                      "lineorder", "customer", "supplier", "part", "date_dim"]

            for table in tables:
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    print(f"  ✓ Dropped {table}")
                except Exception as e:
                    print(f"  ⚠ Could not drop {table}: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Setup test data for PartitionCache")
    parser.add_argument("--reset", action="store_true", help="Reset existing test data")
    parser.add_argument("--size", choices=["small", "medium", "large"], default="small", help="Size of dataset to create")
    parser.add_argument("--dataset", choices=["spatial", "orders", "tpch", "ssb", "all"], default="spatial", help="Which dataset to create")
    parser.add_argument("--cleanup", action="store_true", help="Remove all test data")
    parser.add_argument("--stats", action="store_true", help="Show dataset statistics")
    parser.add_argument("--connection-string", help="Database connection string")

    args = parser.parse_args()

    # Initialize setup
    setup = TestDataSetup(args.connection_string)

    try:
        setup.connect()

        if args.cleanup:
            setup.cleanup_test_data()
            return

        if args.stats:
            stats = setup.get_dataset_stats()
            print("\nDataset Statistics:")
            print(f"  Spatial Points: {stats['spatial_points_count']} records")
            print(f"  Businesses: {stats['businesses_count']} records")
            print(f"  Orders: {stats['orders_count']} records")
            print(f"  TPC-H Orders: {stats['tpch_orders_count']} records")
            print(f"  TPC-H Customers: {stats['tpch_customers_count']} records")
            print(f"  SSB Lineorder: {stats['ssb_lineorder_count']} records")
            print(f"  SSB Customer: {stats['ssb_customer_count']} records")
            print(f"  SSB Supplier: {stats['ssb_supplier_count']} records")
            print(f"  SSB Part: {stats['ssb_part_count']} records")
            print(f"  SSB Date: {stats['ssb_date_count']} records")
            return

        # Create datasets
        if args.dataset in ["orders", "all"]:
            setup.create_orders_dataset(args.size, args.reset)

        if args.dataset in ["spatial", "all"]:
            setup.create_spatial_dataset(args.size, args.reset)

        if args.dataset in ["tpch", "all"]:
            setup.create_tpch_dataset(args.size, args.reset)

        if args.dataset in ["ssb", "all"]:
            setup.create_ssb_dataset(args.size, args.reset)

        # Show final stats
        stats = setup.get_dataset_stats()
        print("\n✓ Test data setup complete!")
        print(f"  Spatial Points: {stats['spatial_points_count']} records")
        print(f"  Businesses: {stats['businesses_count']} records")
        print(f"  Orders: {stats['orders_count']} records")
        print(f"  TPC-H Orders: {stats['tpch_orders_count']} records")
        print(f"  TPC-H Customers: {stats['tpch_customers_count']} records")
        print(f"  SSB Lineorder: {stats['ssb_lineorder_count']} records")
        print(f"  SSB Customer: {stats['ssb_customer_count']} records")

    except Exception as e:
        print(f"✗ Error: {e}")
        return 1
    finally:
        setup.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
