#!/usr/bin/env python3
"""
SSB (Star Schema Benchmark) Data Generator for PartitionCache.

Generates all 5 SSB tables with realistic data distributions following the SSB specification.
Supports DuckDB and PostgreSQL backends.

Tables:
    - lineorder (fact table): ~60K rows at SF=0.01
    - customer (dimension): ~300 rows at SF=0.01
    - supplier (dimension): ~20 rows at SF=0.01
    - part (dimension): ~2K rows at SF=0.01
    - date_dim (dimension): 2,556 rows (fixed, 1992-01-01 to 1998-12-31)

Usage:
    python generate_ssb_data.py --scale-factor 0.01 --db-backend duckdb
    python generate_ssb_data.py --scale-factor 0.1 --db-backend postgresql
"""

import argparse
import os
import random
import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# --- SSB Hierarchy Constants ---

REGIONS = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]

NATIONS_BY_REGION = {
    "AFRICA": ["ALGERIA", "ETHIOPIA", "KENYA", "MOROCCO", "MOZAMBIQUE"],
    "AMERICA": ["ARGENTINA", "BRAZIL", "CANADA", "PERU", "UNITED STATES"],
    "ASIA": ["CHINA", "INDIA", "INDONESIA", "JAPAN", "VIETNAM"],
    "EUROPE": ["FRANCE", "GERMANY", "ROMANIA", "RUSSIA", "UNITED KINGDOM"],
    "MIDDLE EAST": ["EGYPT", "IRAN", "IRAQ", "JORDAN", "SAUDI ARABIA"],
}

# Build flat nation list with region mapping
ALL_NATIONS = []
NATION_TO_REGION = {}
for region, nations in NATIONS_BY_REGION.items():
    for nation in nations:
        ALL_NATIONS.append(nation)
        NATION_TO_REGION[nation] = region

# Cities per nation (SSB spec: 10 cities per nation at SF=1)
CITIES_PER_NATION = 10

# Part hierarchy
MFGR_COUNT = 5  # MFGR#1 to MFGR#5
CATEGORIES_PER_MFGR = 5  # 25 total categories
BRANDS_PER_CATEGORY = 40  # 1000 total brands at SF=1

CONTAINERS = ["SM CASE", "SM BOX", "SM PACK", "SM PKG", "SM JAR", "SM DRUM", "SM BAG", "SM CAN",
              "MED CASE", "MED BOX", "MED PACK", "MED PKG", "MED JAR", "MED DRUM", "MED BAG", "MED CAN",
              "LG CASE", "LG BOX", "LG PACK", "LG PKG", "LG JAR", "LG DRUM", "LG BAG", "LG CAN",
              "JUMBO CASE", "JUMBO BOX", "JUMBO PACK", "JUMBO PKG", "JUMBO JAR", "JUMBO DRUM", "JUMBO BAG", "JUMBO CAN",
              "WRAP CASE", "WRAP BOX", "WRAP PACK", "WRAP PKG", "WRAP JAR", "WRAP DRUM", "WRAP BAG", "WRAP CAN"]

TYPES = ["ECONOMY ANODIZED STEEL", "ECONOMY BURNISHED BRASS", "ECONOMY PLATED COPPER",
         "ECONOMY BRUSHED NICKEL", "ECONOMY POLISHED TIN",
         "STANDARD ANODIZED STEEL", "STANDARD BURNISHED BRASS", "STANDARD PLATED COPPER",
         "STANDARD BRUSHED NICKEL", "STANDARD POLISHED TIN",
         "PROMO ANODIZED STEEL", "PROMO BURNISHED BRASS", "PROMO PLATED COPPER",
         "PROMO BRUSHED NICKEL", "PROMO POLISHED TIN",
         "SMALL ANODIZED STEEL", "SMALL BURNISHED BRASS", "SMALL PLATED COPPER",
         "SMALL BRUSHED NICKEL", "SMALL POLISHED TIN",
         "LARGE ANODIZED STEEL", "LARGE BURNISHED BRASS", "LARGE PLATED COPPER",
         "LARGE BRUSHED NICKEL", "LARGE POLISHED TIN"]

SHIP_MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
SHIP_PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def generate_date_dim():
    """Generate the date dimension table (fixed: 1992-01-01 to 1998-12-31)."""
    rows = []
    start = date(1992, 1, 1)
    end = date(1998, 12, 31)
    current = start

    while current <= end:
        datekey = current.year * 10000 + current.month * 100 + current.day
        month_name = MONTHS[current.month - 1]
        year = current.year
        yearmonthnum = current.year * 100 + current.month
        yearmonth = f"{month_name}{year}"
        daynuminweek = current.isoweekday()  # 1=Mon, 7=Sun
        daynuminmonth = current.day
        daynuminyear = current.timetuple().tm_yday
        monthnuminyear = current.month
        weeknuminyear = current.isocalendar()[1]
        day_of_week = DAYS_OF_WEEK[current.weekday()]
        sellingseason = "Christmas" if current.month == 12 else ("Summer" if current.month in (6, 7, 8) else "Spring" if current.month in (3, 4, 5) else "Fall" if current.month in (9, 10, 11) else "Winter")
        lastdayinweekfl = 1 if current.weekday() == 6 else 0
        # Last day of month
        if current.month == 12:
            next_month_first = date(current.year + 1, 1, 1)
        else:
            next_month_first = date(current.year, current.month + 1, 1)
        lastdayinmonthfl = 1 if current + timedelta(days=1) == next_month_first else 0
        holidayfl = 1 if (current.month == 12 and current.day == 25) or (current.month == 1 and current.day == 1) else 0
        weekdayfl = 1 if current.weekday() < 5 else 0

        rows.append((
            datekey, str(current), day_of_week, month_name,
            year, yearmonthnum, yearmonth,
            daynuminweek, daynuminmonth, daynuminyear,
            monthnuminyear, weeknuminyear,
            sellingseason, lastdayinweekfl, lastdayinmonthfl,
            holidayfl, weekdayfl,
        ))
        current += timedelta(days=1)

    return rows


def generate_customers(scale_factor):
    """Generate customer dimension rows."""
    num_customers = max(int(30000 * scale_factor), 10)
    rows = []

    for i in range(1, num_customers + 1):
        nation = random.choice(ALL_NATIONS)
        region = NATION_TO_REGION[nation]
        # City: nation + city number (SSB spec)
        city_num = random.randint(0, min(CITIES_PER_NATION - 1, max(int(CITIES_PER_NATION * scale_factor) - 1, 1)))
        city = f"{nation}{city_num}"
        phone = f"{random.randint(10, 34)}-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        mktsegment = random.choice(["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"])

        rows.append((
            i,  # c_custkey
            f"Customer#{i:09d}",  # c_name
            f"Address {i}",  # c_address
            city,  # c_city
            nation,  # c_nation
            region,  # c_region
            phone,  # c_phone
            mktsegment,  # c_mktsegment
        ))

    return rows


def generate_suppliers(scale_factor):
    """Generate supplier dimension rows."""
    num_suppliers = max(int(2000 * scale_factor), 4)
    rows = []

    for i in range(1, num_suppliers + 1):
        nation = random.choice(ALL_NATIONS)
        region = NATION_TO_REGION[nation]
        city_num = random.randint(0, min(CITIES_PER_NATION - 1, max(int(CITIES_PER_NATION * scale_factor) - 1, 1)))
        city = f"{nation}{city_num}"
        phone = f"{random.randint(10, 34)}-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

        rows.append((
            i,  # s_suppkey
            f"Supplier#{i:09d}",  # s_name
            f"Address {i}",  # s_address
            city,  # s_city
            nation,  # s_nation
            region,  # s_region
            phone,  # s_phone
        ))

    return rows


def generate_parts(scale_factor):
    """Generate part dimension rows."""
    num_parts = max(int(200000 * scale_factor), 100)
    rows = []

    for i in range(1, num_parts + 1):
        mfgr_num = ((i - 1) % MFGR_COUNT) + 1
        mfgr = f"MFGR#{mfgr_num}"
        cat_num = ((i - 1) % (MFGR_COUNT * CATEGORIES_PER_MFGR)) + 1
        category = f"MFGR#{mfgr_num}{((cat_num - 1) % CATEGORIES_PER_MFGR) + 1}"
        brand_num = ((i - 1) % (MFGR_COUNT * CATEGORIES_PER_MFGR * BRANDS_PER_CATEGORY)) + 1
        brand = f"MFGR#{mfgr_num}{((cat_num - 1) % CATEGORIES_PER_MFGR) + 1}{((brand_num - 1) % BRANDS_PER_CATEGORY) + 1:02d}"
        color = random.choice(["almond", "antique", "aquamarine", "azure", "beige",
                                "bisque", "black", "blanched", "blue", "blush",
                                "brown", "burlywood", "burnished", "chartreuse", "chiffon",
                                "chocolate", "coral", "cornflower", "cornsilk", "cream",
                                "cyan", "dark", "deep", "dim", "dodger"])
        p_type = random.choice(TYPES)
        size = random.randint(1, 50)
        container = random.choice(CONTAINERS)

        rows.append((
            i,  # p_partkey
            f"Part {i}",  # p_name
            mfgr,  # p_mfgr
            category,  # p_category
            brand,  # p_brand
            color,  # p_color
            p_type,  # p_type
            size,  # p_size
            container,  # p_container
        ))

    return rows


def generate_lineorder(scale_factor, num_customers, num_suppliers, num_parts, date_keys):
    """Generate lineorder fact table rows."""
    num_orders = max(int(6000000 * scale_factor), 1000)
    rows = []

    # Each order has 1-7 lineitems
    orderkey = 0
    linenumber = 0
    generated = 0

    while generated < num_orders:
        orderkey += 1
        num_lines = random.randint(1, 7)

        custkey = random.randint(1, num_customers)
        orderdate = random.choice(date_keys)
        orderpriority = random.choice(SHIP_PRIORITIES)
        ordtotalprice = 0

        for line in range(1, num_lines + 1):
            if generated >= num_orders:
                break

            linenumber = line
            partkey = random.randint(1, num_parts)
            suppkey = random.randint(1, num_suppliers)

            quantity = random.randint(1, 50)
            extendedprice = quantity * random.randint(90, 20000)  # in cents
            discount = random.randint(0, 10)  # percentage
            revenue = extendedprice * (100 - discount) // 100
            max_cost = max(101, extendedprice // max(quantity, 1))
            supplycost = random.randint(100, max_cost) if extendedprice > 100 else random.randint(1, 100)
            tax = random.randint(0, 8)

            # Commit date: order date + random offset
            order_date_obj = date(orderdate // 10000, (orderdate % 10000) // 100, orderdate % 100)
            commit_offset = random.randint(1, 120)
            commit_date_obj = order_date_obj + timedelta(days=commit_offset)
            commitdate = commit_date_obj.year * 10000 + commit_date_obj.month * 100 + commit_date_obj.day

            shipmode = random.choice(SHIP_MODES)
            ordtotalprice += extendedprice

            rows.append((
                orderkey,  # lo_orderkey
                linenumber,  # lo_linenumber
                custkey,  # lo_custkey
                partkey,  # lo_partkey
                suppkey,  # lo_suppkey
                orderdate,  # lo_orderdate
                orderpriority,  # lo_orderpriority
                0,  # lo_shippriority
                quantity,  # lo_quantity
                extendedprice,  # lo_extendedprice
                ordtotalprice,  # lo_ordtotalprice
                discount,  # lo_discount
                revenue,  # lo_revenue
                supplycost,  # lo_supplycost
                tax,  # lo_tax
                commitdate,  # lo_commitdate
                shipmode,  # lo_shipmode
            ))
            generated += 1

    return rows


# --- DuckDB Backend ---

def create_duckdb_tables(db_path, scale_factor, seed=42):
    """Create SSB tables in DuckDB."""
    import duckdb

    random.seed(seed)

    print(f"Creating SSB dataset in DuckDB: {db_path} (SF={scale_factor})")

    conn = duckdb.connect(str(db_path))

    # Drop existing tables
    for table in ["lineorder", "customer", "supplier", "part", "date_dim"]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    # Create date_dim
    conn.execute("""
        CREATE TABLE date_dim (
            d_datekey INTEGER PRIMARY KEY,
            d_date VARCHAR(18),
            d_dayofweek VARCHAR(9),
            d_month VARCHAR(9),
            d_year INTEGER,
            d_yearmonthnum INTEGER,
            d_yearmonth VARCHAR(7),
            d_daynuminweek INTEGER,
            d_daynuminmonth INTEGER,
            d_daynuminyear INTEGER,
            d_monthnuminyear INTEGER,
            d_weeknuminyear INTEGER,
            d_sellingseason VARCHAR(12),
            d_lastdayinweekfl INTEGER,
            d_lastdayinmonthfl INTEGER,
            d_holidayfl INTEGER,
            d_weekdayfl INTEGER
        )
    """)

    # Create customer
    conn.execute("""
        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name VARCHAR(25),
            c_address VARCHAR(25),
            c_city VARCHAR(10),
            c_nation VARCHAR(15),
            c_region VARCHAR(12),
            c_phone VARCHAR(15),
            c_mktsegment VARCHAR(10)
        )
    """)

    # Create supplier
    conn.execute("""
        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name VARCHAR(25),
            s_address VARCHAR(25),
            s_city VARCHAR(10),
            s_nation VARCHAR(15),
            s_region VARCHAR(12),
            s_phone VARCHAR(15)
        )
    """)

    # Create part
    conn.execute("""
        CREATE TABLE part (
            p_partkey INTEGER PRIMARY KEY,
            p_name VARCHAR(22),
            p_mfgr VARCHAR(6),
            p_category VARCHAR(7),
            p_brand VARCHAR(9),
            p_color VARCHAR(11),
            p_type VARCHAR(25),
            p_size INTEGER,
            p_container VARCHAR(10)
        )
    """)

    # Create lineorder
    conn.execute("""
        CREATE TABLE lineorder (
            lo_orderkey INTEGER,
            lo_linenumber INTEGER,
            lo_custkey INTEGER,
            lo_partkey INTEGER,
            lo_suppkey INTEGER,
            lo_orderdate INTEGER,
            lo_orderpriority VARCHAR(15),
            lo_shippriority INTEGER,
            lo_quantity INTEGER,
            lo_extendedprice INTEGER,
            lo_ordtotalprice INTEGER,
            lo_discount INTEGER,
            lo_revenue INTEGER,
            lo_supplycost INTEGER,
            lo_tax INTEGER,
            lo_commitdate INTEGER,
            lo_shipmode VARCHAR(10)
        )
    """)

    # Generate and insert date_dim
    print("  Generating date_dim...")
    date_rows = generate_date_dim()
    conn.executemany(
        "INSERT INTO date_dim VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        date_rows,
    )
    print(f"    Inserted {len(date_rows)} date rows")

    # Generate and insert customers
    print("  Generating customers...")
    customer_rows = generate_customers(scale_factor)
    conn.executemany(
        "INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        customer_rows,
    )
    print(f"    Inserted {len(customer_rows)} customers")

    # Generate and insert suppliers
    print("  Generating suppliers...")
    supplier_rows = generate_suppliers(scale_factor)
    conn.executemany(
        "INSERT INTO supplier VALUES (?, ?, ?, ?, ?, ?, ?)",
        supplier_rows,
    )
    print(f"    Inserted {len(supplier_rows)} suppliers")

    # Generate and insert parts
    print("  Generating parts...")
    part_rows = generate_parts(scale_factor)
    conn.executemany(
        "INSERT INTO part VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        part_rows,
    )
    print(f"    Inserted {len(part_rows)} parts")

    # Generate and insert lineorder
    print("  Generating lineorder (this may take a moment)...")
    date_keys = [row[0] for row in date_rows]
    lineorder_rows = generate_lineorder(
        scale_factor, len(customer_rows), len(supplier_rows), len(part_rows), date_keys,
    )

    # Insert in batches
    batch_size = 10000
    for i in range(0, len(lineorder_rows), batch_size):
        batch = lineorder_rows[i:i + batch_size]
        conn.executemany(
            "INSERT INTO lineorder VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        if (i + batch_size) % 50000 == 0 or i + batch_size >= len(lineorder_rows):
            print(f"    Inserted {min(i + batch_size, len(lineorder_rows))} lineorder rows...")

    print(f"    Total: {len(lineorder_rows)} lineorder rows")

    # Create indexes for partition keys used in benchmark
    print("  Creating indexes...")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lo_custkey ON lineorder(lo_custkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lo_suppkey ON lineorder(lo_suppkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lo_partkey ON lineorder(lo_partkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lo_orderdate ON lineorder(lo_orderdate)")

    conn.close()

    print(f"\nSSB dataset created successfully in {db_path}")
    print(f"  date_dim:   {len(date_rows):>10,} rows")
    print(f"  customer:   {len(customer_rows):>10,} rows")
    print(f"  supplier:   {len(supplier_rows):>10,} rows")
    print(f"  part:       {len(part_rows):>10,} rows")
    print(f"  lineorder:  {len(lineorder_rows):>10,} rows")


# --- PostgreSQL Backend ---

def create_postgresql_tables(scale_factor, seed=42):
    """Create SSB tables in PostgreSQL."""
    try:
        import psycopg
        from dotenv import load_dotenv
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install psycopg[binary] python-dotenv")
        sys.exit(1)

    random.seed(seed)

    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_user = os.getenv("DB_USER", "app_user")
    db_password = os.getenv("DB_PASSWORD", "")
    db_name = os.getenv("DB_NAME", "ssb_db")

    print(f"Creating SSB dataset in PostgreSQL: {db_host}:{db_port}/{db_name} (SF={scale_factor})")

    conn = psycopg.connect(
        host=db_host, port=int(db_port), user=db_user, password=db_password, dbname=db_name,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        # Drop existing tables
        for table in ["lineorder", "customer", "supplier", "part", "date_dim"]:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

        # Create date_dim
        cur.execute("""
            CREATE TABLE date_dim (
                d_datekey INTEGER PRIMARY KEY,
                d_date VARCHAR(18),
                d_dayofweek VARCHAR(9),
                d_month VARCHAR(9),
                d_year INTEGER,
                d_yearmonthnum INTEGER,
                d_yearmonth VARCHAR(7),
                d_daynuminweek INTEGER,
                d_daynuminmonth INTEGER,
                d_daynuminyear INTEGER,
                d_monthnuminyear INTEGER,
                d_weeknuminyear INTEGER,
                d_sellingseason VARCHAR(12),
                d_lastdayinweekfl INTEGER,
                d_lastdayinmonthfl INTEGER,
                d_holidayfl INTEGER,
                d_weekdayfl INTEGER
            )
        """)

        # Create customer
        cur.execute("""
            CREATE TABLE customer (
                c_custkey INTEGER PRIMARY KEY,
                c_name VARCHAR(25),
                c_address VARCHAR(25),
                c_city VARCHAR(10),
                c_nation VARCHAR(15),
                c_region VARCHAR(12),
                c_phone VARCHAR(15),
                c_mktsegment VARCHAR(10)
            )
        """)

        # Create supplier
        cur.execute("""
            CREATE TABLE supplier (
                s_suppkey INTEGER PRIMARY KEY,
                s_name VARCHAR(25),
                s_address VARCHAR(25),
                s_city VARCHAR(10),
                s_nation VARCHAR(15),
                s_region VARCHAR(12),
                s_phone VARCHAR(15)
            )
        """)

        # Create part
        cur.execute("""
            CREATE TABLE part (
                p_partkey INTEGER PRIMARY KEY,
                p_name VARCHAR(22),
                p_mfgr VARCHAR(6),
                p_category VARCHAR(7),
                p_brand VARCHAR(9),
                p_color VARCHAR(11),
                p_type VARCHAR(25),
                p_size INTEGER,
                p_container VARCHAR(10)
            )
        """)

        # Create lineorder
        cur.execute("""
            CREATE TABLE lineorder (
                lo_orderkey INTEGER,
                lo_linenumber INTEGER,
                lo_custkey INTEGER,
                lo_partkey INTEGER,
                lo_suppkey INTEGER,
                lo_orderdate INTEGER,
                lo_orderpriority VARCHAR(15),
                lo_shippriority INTEGER,
                lo_quantity INTEGER,
                lo_extendedprice INTEGER,
                lo_ordtotalprice INTEGER,
                lo_discount INTEGER,
                lo_revenue INTEGER,
                lo_supplycost INTEGER,
                lo_tax INTEGER,
                lo_commitdate INTEGER,
                lo_shipmode VARCHAR(10)
            )
        """)

        # Generate and insert date_dim
        print("  Generating date_dim...")
        date_rows = generate_date_dim()
        cur.executemany(
            "INSERT INTO date_dim VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            date_rows,
        )
        print(f"    Inserted {len(date_rows)} date rows")

        # Generate and insert customers
        print("  Generating customers...")
        customer_rows = generate_customers(scale_factor)
        cur.executemany(
            "INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            customer_rows,
        )
        print(f"    Inserted {len(customer_rows)} customers")

        # Generate and insert suppliers
        print("  Generating suppliers...")
        supplier_rows = generate_suppliers(scale_factor)
        cur.executemany(
            "INSERT INTO supplier VALUES (%s, %s, %s, %s, %s, %s, %s)",
            supplier_rows,
        )
        print(f"    Inserted {len(supplier_rows)} suppliers")

        # Generate and insert parts
        print("  Generating parts...")
        part_rows = generate_parts(scale_factor)
        cur.executemany(
            "INSERT INTO part VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            part_rows,
        )
        print(f"    Inserted {len(part_rows)} parts")

        # Generate and insert lineorder
        print("  Generating lineorder (this may take a moment)...")
        date_keys = [row[0] for row in date_rows]
        lineorder_rows = generate_lineorder(
            scale_factor, len(customer_rows), len(supplier_rows), len(part_rows), date_keys,
        )

        batch_size = 5000
        for i in range(0, len(lineorder_rows), batch_size):
            batch = lineorder_rows[i:i + batch_size]
            cur.executemany(
                "INSERT INTO lineorder VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                batch,
            )
            if (i + batch_size) % 50000 == 0 or i + batch_size >= len(lineorder_rows):
                print(f"    Inserted {min(i + batch_size, len(lineorder_rows))} lineorder rows...")

        print(f"    Total: {len(lineorder_rows)} lineorder rows")

        # Create indexes
        print("  Creating indexes...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_lo_custkey ON lineorder(lo_custkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_lo_suppkey ON lineorder(lo_suppkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_lo_partkey ON lineorder(lo_partkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_lo_orderdate ON lineorder(lo_orderdate)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_c_region ON customer(c_region)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_c_nation ON customer(c_nation)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_s_region ON supplier(s_region)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_s_nation ON supplier(s_nation)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_p_category ON part(p_category)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_p_brand ON part(p_brand)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_d_year ON date_dim(d_year)")

        # Analyze
        cur.execute("ANALYZE date_dim, customer, supplier, part, lineorder")

    conn.close()

    print(f"\nSSB dataset created successfully in PostgreSQL")
    print(f"  date_dim:   {len(date_rows):>10,} rows")
    print(f"  customer:   {len(customer_rows):>10,} rows")
    print(f"  supplier:   {len(supplier_rows):>10,} rows")
    print(f"  part:       {len(part_rows):>10,} rows")
    print(f"  lineorder:  {len(lineorder_rows):>10,} rows")


def main():
    parser = argparse.ArgumentParser(description="Generate SSB benchmark data")
    parser.add_argument("--scale-factor", type=float, default=0.01,
                        choices=[0.01, 0.1, 1.0],
                        help="Scale factor (0.01, 0.1, 1.0)")
    parser.add_argument("--db-backend", choices=["duckdb", "postgresql"], default="duckdb",
                        help="Database backend")
    parser.add_argument("--db-path", type=str, default=None,
                        help="DuckDB file path (default: ssb_sf{SF}.duckdb)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")

    args = parser.parse_args()

    if args.db_backend == "duckdb":
        db_path = args.db_path or os.path.join(
            os.path.dirname(__file__), f"ssb_sf{args.scale_factor}.duckdb",
        )
        create_duckdb_tables(db_path, args.scale_factor, args.seed)

    elif args.db_backend == "postgresql":
        create_postgresql_tables(args.scale_factor, args.seed)


if __name__ == "__main__":
    main()
