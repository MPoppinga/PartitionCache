#!/usr/bin/env python3
"""
TPC-H Data Generator for PartitionCache.

Generates all 8 TPC-H tables with realistic data distributions following the TPC-H specification.
Supports DuckDB and PostgreSQL backends.

Tables:
    - region (fixed): 5 rows
    - nation (fixed): 25 rows
    - part: 2,000 rows at SF=0.01
    - supplier: 100 rows at SF=0.01
    - partsupp: 8,000 rows at SF=0.01
    - customer: 1,500 rows at SF=0.01
    - orders: 15,000 rows at SF=0.01
    - lineitem (fact table): ~60,000 rows at SF=0.01

Usage:
    python generate_tpch_data.py --scale-factor 0.01 --db-backend duckdb
    python generate_tpch_data.py --scale-factor 0.1 --db-backend postgresql
"""

import argparse
import os
import random
import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


# --- TPC-H Constants ---

REGIONS = [(0, "AFRICA"), (1, "AMERICA"), (2, "ASIA"), (3, "EUROPE"), (4, "MIDDLE EAST")]

NATIONS = [
    (0, "ALGERIA", 0), (1, "ARGENTINA", 1), (2, "BRAZIL", 1), (3, "CANADA", 1),
    (4, "EGYPT", 4), (5, "ETHIOPIA", 0), (6, "FRANCE", 3), (7, "GERMANY", 3),
    (8, "INDIA", 2), (9, "INDONESIA", 2), (10, "IRAN", 4), (11, "IRAQ", 4),
    (12, "JAPAN", 2), (13, "JORDAN", 4), (14, "KENYA", 0), (15, "CHINA", 2),
    (16, "MOZAMBIQUE", 0), (17, "PERU", 1), (18, "ROMANIA", 3), (19, "RUSSIA", 3),
    (20, "SAUDI ARABIA", 4), (21, "UNITED KINGDOM", 3), (22, "UNITED STATES", 1),
    (23, "VIETNAM", 2), (24, "MOROCCO", 0),
]

MKTSEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]
ORDERPRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
SHIPMODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
SHIPINSTRUCTS = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]

# Part type: SYLLABLE1 + SYLLABLE2 + SYLLABLE3
TYPE_SYLLABLE1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
TYPE_SYLLABLE2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
TYPE_SYLLABLE3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]

CONTAINERS = [f"{s} {t}" for s in ["SM", "MED", "LG", "JUMBO", "WRAP"]
              for t in ["CASE", "BOX", "PACK", "PKG", "JAR", "DRUM", "BAG", "CAN"]]

# Order date range (TPC-H spec)
ORDER_DATE_START = date(1992, 1, 1)
ORDER_DATE_END = date(1998, 8, 2)
ORDER_DATE_RANGE = (ORDER_DATE_END - ORDER_DATE_START).days


def random_phone():
    """Generate a TPC-H phone number: CC-AAA-BBB-CCCC."""
    cc = random.randint(10, 34)
    return f"{cc}-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


def random_part_type():
    """Generate a random part type from syllable combinations."""
    return f"{random.choice(TYPE_SYLLABLE1)} {random.choice(TYPE_SYLLABLE2)} {random.choice(TYPE_SYLLABLE3)}"


def generate_regions():
    """Generate region table (fixed 5 rows)."""
    return [(r_key, r_name, f"Region {r_name} comment") for r_key, r_name in REGIONS]


def generate_nations():
    """Generate nation table (fixed 25 rows)."""
    return [(n_key, n_name, n_rkey, f"Nation {n_name} comment") for n_key, n_name, n_rkey in NATIONS]


def generate_parts(scale_factor):
    """Generate part table."""
    num_parts = max(int(200000 * scale_factor), 100)
    rows = []

    for i in range(1, num_parts + 1):
        mfgr_num = ((i - 1) % 5) + 1
        brand_y = ((i - 1) % 5) + 1
        p_name = f"Part {i}"
        p_mfgr = f"Manufacturer#{mfgr_num}"
        p_brand = f"Brand#{mfgr_num}{brand_y}"
        p_type = random_part_type()
        p_size = random.randint(1, 50)
        p_container = random.choice(CONTAINERS)
        p_retailprice = round(90000 + ((i / 10) % 20001) + 100 * (i % 1000), 2)
        p_comment = f"Part {i} comment"

        rows.append((
            i, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment,
        ))

    return rows


def generate_suppliers(scale_factor):
    """Generate supplier table."""
    num_suppliers = max(int(10000 * scale_factor), 10)
    rows = []

    for i in range(1, num_suppliers + 1):
        s_nationkey = random.randint(0, 24)
        rows.append((
            i,
            f"Supplier#{i:09d}",
            f"Address {i}",
            s_nationkey,
            random_phone(),
            round(random.uniform(-999.99, 9999.99), 2),
            f"Supplier {i} comment",
        ))

    return rows


def generate_partsupp(num_parts, num_suppliers):
    """Generate partsupp table (4 suppliers per part)."""
    rows = []
    step = max(num_suppliers // 4, 1)

    for partkey in range(1, num_parts + 1):
        for j in range(4):
            suppkey = ((partkey - 1 + j * step) % num_suppliers) + 1
            rows.append((
                partkey,
                suppkey,
                random.randint(1, 9999),
                round(random.uniform(1.0, 1000.0), 2),
                f"PartSupp {partkey}-{suppkey} comment",
            ))

    return rows


def generate_customers(scale_factor):
    """Generate customer table."""
    num_customers = max(int(150000 * scale_factor), 100)
    rows = []

    for i in range(1, num_customers + 1):
        c_nationkey = random.randint(0, 24)
        rows.append((
            i,
            f"Customer#{i:09d}",
            f"Address {i}",
            c_nationkey,
            random_phone(),
            round(random.uniform(-999.99, 9999.99), 2),
            random.choice(MKTSEGMENTS),
            f"Customer {i} comment",
        ))

    return rows


def generate_orders_and_lineitems(scale_factor, num_customers, num_parts, num_suppliers):
    """Generate orders and lineitem tables together (lineitem depends on orders)."""
    num_orders = max(int(1500000 * scale_factor), 1000)

    order_rows = []
    lineitem_rows = []

    for orderkey in range(1, num_orders + 1):
        custkey = random.randint(1, num_customers)
        o_orderdate = ORDER_DATE_START + timedelta(days=random.randint(0, ORDER_DATE_RANGE))
        o_orderstatus = random.choice(["F", "O", "P"])
        o_totalprice = 0.0
        o_orderpriority = random.choice(ORDERPRIORITIES)
        o_clerk = f"Clerk#{random.randint(1, max(int(scale_factor * 1000), 1)):09d}"
        o_shippriority = 0
        o_comment = f"Order {orderkey} comment"

        # 1-7 line items per order
        num_lines = random.randint(1, 7)

        for linenumber in range(1, num_lines + 1):
            l_partkey = random.randint(1, num_parts)
            l_suppkey = random.randint(1, num_suppliers)
            l_quantity = random.randint(1, 50)
            l_extendedprice = round(l_quantity * random.uniform(900, 100000) / 100, 2)
            l_discount = round(random.randint(0, 10) / 100, 2)
            l_tax = round(random.randint(0, 8) / 100, 2)

            # Return flag and line status
            if o_orderdate > date(1995, 3, 15):
                l_returnflag = "N"
            else:
                l_returnflag = random.choice(["R", "A"])
            l_linestatus = "F" if o_orderdate < date(1995, 3, 15) else "O"

            l_shipdate = o_orderdate + timedelta(days=random.randint(1, 121))
            l_commitdate = o_orderdate + timedelta(days=random.randint(30, 90))
            l_receiptdate = l_shipdate + timedelta(days=random.randint(1, 30))

            o_totalprice += l_extendedprice * (1 - l_discount) * (1 + l_tax)

            lineitem_rows.append((
                orderkey,
                l_partkey,
                l_suppkey,
                linenumber,
                l_quantity,
                l_extendedprice,
                l_discount,
                l_tax,
                l_returnflag,
                l_linestatus,
                str(l_shipdate),
                str(l_commitdate),
                str(l_receiptdate),
                random.choice(SHIPINSTRUCTS),
                random.choice(SHIPMODES),
                f"Lineitem {orderkey}-{linenumber} comment",
            ))

        o_totalprice = round(o_totalprice, 2)
        order_rows.append((
            orderkey, custkey, o_orderstatus, o_totalprice, str(o_orderdate),
            o_orderpriority, o_clerk, o_shippriority, o_comment,
        ))

    return order_rows, lineitem_rows


# --- DuckDB Backend ---

def create_duckdb_tables(db_path, scale_factor, seed=42):
    """Create TPC-H tables in DuckDB."""
    import duckdb

    random.seed(seed)

    print(f"Creating TPC-H dataset in DuckDB: {db_path} (SF={scale_factor})")

    conn = duckdb.connect(str(db_path))

    # Drop existing tables
    for table in ["lineitem", "orders", "partsupp", "customer", "supplier", "part", "nation", "region"]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    # Create tables
    conn.execute("""
        CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name VARCHAR(25),
            r_comment VARCHAR(152)
        )
    """)

    conn.execute("""
        CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name VARCHAR(25),
            n_regionkey INTEGER,
            n_comment VARCHAR(152)
        )
    """)

    conn.execute("""
        CREATE TABLE part (
            p_partkey INTEGER PRIMARY KEY,
            p_name VARCHAR(55),
            p_mfgr VARCHAR(25),
            p_brand VARCHAR(10),
            p_type VARCHAR(25),
            p_size INTEGER,
            p_container VARCHAR(10),
            p_retailprice DECIMAL(15,2),
            p_comment VARCHAR(23)
        )
    """)

    conn.execute("""
        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name VARCHAR(25),
            s_address VARCHAR(40),
            s_nationkey INTEGER,
            s_phone VARCHAR(15),
            s_acctbal DECIMAL(15,2),
            s_comment VARCHAR(101)
        )
    """)

    conn.execute("""
        CREATE TABLE partsupp (
            ps_partkey INTEGER,
            ps_suppkey INTEGER,
            ps_availqty INTEGER,
            ps_supplycost DECIMAL(15,2),
            ps_comment VARCHAR(199),
            PRIMARY KEY (ps_partkey, ps_suppkey)
        )
    """)

    conn.execute("""
        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name VARCHAR(25),
            c_address VARCHAR(40),
            c_nationkey INTEGER,
            c_phone VARCHAR(15),
            c_acctbal DECIMAL(15,2),
            c_mktsegment VARCHAR(10),
            c_comment VARCHAR(117)
        )
    """)

    conn.execute("""
        CREATE TABLE orders (
            o_orderkey INTEGER PRIMARY KEY,
            o_custkey INTEGER,
            o_orderstatus CHAR(1),
            o_totalprice DECIMAL(15,2),
            o_orderdate DATE,
            o_orderpriority VARCHAR(15),
            o_clerk VARCHAR(15),
            o_shippriority INTEGER,
            o_comment VARCHAR(79)
        )
    """)

    conn.execute("""
        CREATE TABLE lineitem (
            l_orderkey INTEGER,
            l_partkey INTEGER,
            l_suppkey INTEGER,
            l_linenumber INTEGER,
            l_quantity DECIMAL(15,2),
            l_extendedprice DECIMAL(15,2),
            l_discount DECIMAL(15,2),
            l_tax DECIMAL(15,2),
            l_returnflag CHAR(1),
            l_linestatus CHAR(1),
            l_shipdate DATE,
            l_commitdate DATE,
            l_receiptdate DATE,
            l_shipinstruct VARCHAR(25),
            l_shipmode VARCHAR(10),
            l_comment VARCHAR(44),
            PRIMARY KEY (l_orderkey, l_linenumber)
        )
    """)

    # Generate and insert region
    print("  Generating region...")
    region_rows = generate_regions()
    conn.executemany("INSERT INTO region VALUES (?, ?, ?)", region_rows)
    print(f"    Inserted {len(region_rows)} region rows")

    # Generate and insert nation
    print("  Generating nation...")
    nation_rows = generate_nations()
    conn.executemany("INSERT INTO nation VALUES (?, ?, ?, ?)", nation_rows)
    print(f"    Inserted {len(nation_rows)} nation rows")

    # Generate and insert parts
    print("  Generating part...")
    part_rows = generate_parts(scale_factor)
    conn.executemany("INSERT INTO part VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", part_rows)
    print(f"    Inserted {len(part_rows)} parts")

    # Generate and insert suppliers
    print("  Generating supplier...")
    supplier_rows = generate_suppliers(scale_factor)
    conn.executemany("INSERT INTO supplier VALUES (?, ?, ?, ?, ?, ?, ?)", supplier_rows)
    print(f"    Inserted {len(supplier_rows)} suppliers")

    # Generate and insert partsupp
    print("  Generating partsupp...")
    partsupp_rows = generate_partsupp(len(part_rows), len(supplier_rows))
    batch_size = 10000
    for i in range(0, len(partsupp_rows), batch_size):
        batch = partsupp_rows[i:i + batch_size]
        conn.executemany("INSERT INTO partsupp VALUES (?, ?, ?, ?, ?)", batch)
    print(f"    Inserted {len(partsupp_rows)} partsupp rows")

    # Generate and insert customers
    print("  Generating customer...")
    customer_rows = generate_customers(scale_factor)
    conn.executemany("INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?, ?, ?)", customer_rows)
    print(f"    Inserted {len(customer_rows)} customers")

    # Generate and insert orders and lineitem
    print("  Generating orders and lineitem (this may take a moment)...")
    order_rows, lineitem_rows = generate_orders_and_lineitems(
        scale_factor, len(customer_rows), len(part_rows), len(supplier_rows),
    )

    batch_size = 10000
    for i in range(0, len(order_rows), batch_size):
        batch = order_rows[i:i + batch_size]
        conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", batch)
    print(f"    Inserted {len(order_rows)} orders")

    for i in range(0, len(lineitem_rows), batch_size):
        batch = lineitem_rows[i:i + batch_size]
        conn.executemany(
            "INSERT INTO lineitem VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
        if (i + batch_size) % 50000 == 0 or i + batch_size >= len(lineitem_rows):
            print(f"    Inserted {min(i + batch_size, len(lineitem_rows))} lineitem rows...")

    print(f"    Total: {len(lineitem_rows)} lineitem rows")

    # Create indexes
    print("  Creating indexes...")
    # Partition key columns on lineitem (fact table)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_l_orderkey ON lineitem(l_orderkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_l_partkey ON lineitem(l_partkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_l_suppkey ON lineitem(l_suppkey)")
    # Bridge/dimension lookup columns
    conn.execute("CREATE INDEX IF NOT EXISTS idx_o_custkey ON orders(o_custkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_o_orderdate ON orders(o_orderdate)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_s_nationkey ON supplier(s_nationkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_c_nationkey ON customer(c_nationkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_c_mktsegment ON customer(c_mktsegment)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_n_regionkey ON nation(n_regionkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_p_type ON part(p_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_p_mfgr ON part(p_mfgr)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_p_brand ON part(p_brand)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ps_partkey ON partsupp(ps_partkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ps_suppkey ON partsupp(ps_suppkey)")

    conn.close()

    print(f"\nTPC-H dataset created successfully in {db_path}")
    print(f"  region:     {len(region_rows):>10,} rows")
    print(f"  nation:     {len(nation_rows):>10,} rows")
    print(f"  part:       {len(part_rows):>10,} rows")
    print(f"  supplier:   {len(supplier_rows):>10,} rows")
    print(f"  partsupp:   {len(partsupp_rows):>10,} rows")
    print(f"  customer:   {len(customer_rows):>10,} rows")
    print(f"  orders:     {len(order_rows):>10,} rows")
    print(f"  lineitem:   {len(lineitem_rows):>10,} rows")


# --- PostgreSQL Backend ---

def create_postgresql_tables(scale_factor, seed=42):
    """Create TPC-H tables in PostgreSQL."""
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
    db_name = os.getenv("DB_NAME", "tpch_db")

    print(f"Creating TPC-H dataset in PostgreSQL: {db_host}:{db_port}/{db_name} (SF={scale_factor})")

    conn = psycopg.connect(
        host=db_host, port=int(db_port), user=db_user, password=db_password, dbname=db_name,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        # Drop existing tables
        for table in ["lineitem", "orders", "partsupp", "customer", "supplier", "part", "nation", "region"]:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

        # Create tables
        cur.execute("""
            CREATE TABLE region (
                r_regionkey INTEGER PRIMARY KEY,
                r_name VARCHAR(25),
                r_comment VARCHAR(152)
            )
        """)

        cur.execute("""
            CREATE TABLE nation (
                n_nationkey INTEGER PRIMARY KEY,
                n_name VARCHAR(25),
                n_regionkey INTEGER,
                n_comment VARCHAR(152)
            )
        """)

        cur.execute("""
            CREATE TABLE part (
                p_partkey INTEGER PRIMARY KEY,
                p_name VARCHAR(55),
                p_mfgr VARCHAR(25),
                p_brand VARCHAR(10),
                p_type VARCHAR(25),
                p_size INTEGER,
                p_container VARCHAR(10),
                p_retailprice DECIMAL(15,2),
                p_comment VARCHAR(23)
            )
        """)

        cur.execute("""
            CREATE TABLE supplier (
                s_suppkey INTEGER PRIMARY KEY,
                s_name VARCHAR(25),
                s_address VARCHAR(40),
                s_nationkey INTEGER,
                s_phone VARCHAR(15),
                s_acctbal DECIMAL(15,2),
                s_comment VARCHAR(101)
            )
        """)

        cur.execute("""
            CREATE TABLE partsupp (
                ps_partkey INTEGER,
                ps_suppkey INTEGER,
                ps_availqty INTEGER,
                ps_supplycost DECIMAL(15,2),
                ps_comment VARCHAR(199),
                PRIMARY KEY (ps_partkey, ps_suppkey)
            )
        """)

        cur.execute("""
            CREATE TABLE customer (
                c_custkey INTEGER PRIMARY KEY,
                c_name VARCHAR(25),
                c_address VARCHAR(40),
                c_nationkey INTEGER,
                c_phone VARCHAR(15),
                c_acctbal DECIMAL(15,2),
                c_mktsegment VARCHAR(10),
                c_comment VARCHAR(117)
            )
        """)

        cur.execute("""
            CREATE TABLE orders (
                o_orderkey INTEGER PRIMARY KEY,
                o_custkey INTEGER,
                o_orderstatus CHAR(1),
                o_totalprice DECIMAL(15,2),
                o_orderdate DATE,
                o_orderpriority VARCHAR(15),
                o_clerk VARCHAR(15),
                o_shippriority INTEGER,
                o_comment VARCHAR(79)
            )
        """)

        cur.execute("""
            CREATE TABLE lineitem (
                l_orderkey INTEGER,
                l_partkey INTEGER,
                l_suppkey INTEGER,
                l_linenumber INTEGER,
                l_quantity DECIMAL(15,2),
                l_extendedprice DECIMAL(15,2),
                l_discount DECIMAL(15,2),
                l_tax DECIMAL(15,2),
                l_returnflag CHAR(1),
                l_linestatus CHAR(1),
                l_shipdate DATE,
                l_commitdate DATE,
                l_receiptdate DATE,
                l_shipinstruct VARCHAR(25),
                l_shipmode VARCHAR(10),
                l_comment VARCHAR(44),
                PRIMARY KEY (l_orderkey, l_linenumber)
            )
        """)

        # Generate and insert region
        print("  Generating region...")
        region_rows = generate_regions()
        cur.executemany("INSERT INTO region VALUES (%s, %s, %s)", region_rows)
        print(f"    Inserted {len(region_rows)} region rows")

        # Generate and insert nation
        print("  Generating nation...")
        nation_rows = generate_nations()
        cur.executemany("INSERT INTO nation VALUES (%s, %s, %s, %s)", nation_rows)
        print(f"    Inserted {len(nation_rows)} nation rows")

        # Generate and insert parts
        print("  Generating part...")
        part_rows = generate_parts(scale_factor)
        cur.executemany("INSERT INTO part VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", part_rows)
        print(f"    Inserted {len(part_rows)} parts")

        # Generate and insert suppliers
        print("  Generating supplier...")
        supplier_rows = generate_suppliers(scale_factor)
        cur.executemany("INSERT INTO supplier VALUES (%s, %s, %s, %s, %s, %s, %s)", supplier_rows)
        print(f"    Inserted {len(supplier_rows)} suppliers")

        # Generate and insert partsupp
        print("  Generating partsupp...")
        partsupp_rows = generate_partsupp(len(part_rows), len(supplier_rows))
        batch_size = 5000
        for i in range(0, len(partsupp_rows), batch_size):
            batch = partsupp_rows[i:i + batch_size]
            cur.executemany("INSERT INTO partsupp VALUES (%s, %s, %s, %s, %s)", batch)
        print(f"    Inserted {len(partsupp_rows)} partsupp rows")

        # Generate and insert customers
        print("  Generating customer...")
        customer_rows = generate_customers(scale_factor)
        cur.executemany("INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", customer_rows)
        print(f"    Inserted {len(customer_rows)} customers")

        # Generate and insert orders and lineitem
        print("  Generating orders and lineitem (this may take a moment)...")
        order_rows, lineitem_rows = generate_orders_and_lineitems(
            scale_factor, len(customer_rows), len(part_rows), len(supplier_rows),
        )

        batch_size = 5000
        for i in range(0, len(order_rows), batch_size):
            batch = order_rows[i:i + batch_size]
            cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", batch)
        print(f"    Inserted {len(order_rows)} orders")

        for i in range(0, len(lineitem_rows), batch_size):
            batch = lineitem_rows[i:i + batch_size]
            cur.executemany(
                "INSERT INTO lineitem VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                batch,
            )
            if (i + batch_size) % 50000 == 0 or i + batch_size >= len(lineitem_rows):
                print(f"    Inserted {min(i + batch_size, len(lineitem_rows))} lineitem rows...")

        print(f"    Total: {len(lineitem_rows)} lineitem rows")

        # Create indexes
        print("  Creating indexes...")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_l_orderkey ON lineitem(l_orderkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_l_partkey ON lineitem(l_partkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_l_suppkey ON lineitem(l_suppkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_o_custkey ON orders(o_custkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_o_orderdate ON orders(o_orderdate)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_s_nationkey ON supplier(s_nationkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_c_nationkey ON customer(c_nationkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_c_mktsegment ON customer(c_mktsegment)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_n_regionkey ON nation(n_regionkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_p_type ON part(p_type)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_p_mfgr ON part(p_mfgr)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_p_brand ON part(p_brand)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ps_partkey ON partsupp(ps_partkey)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ps_suppkey ON partsupp(ps_suppkey)")

        # Analyze
        cur.execute("ANALYZE region, nation, part, supplier, partsupp, customer, orders, lineitem")

    conn.close()

    print(f"\nTPC-H dataset created successfully in PostgreSQL")
    print(f"  region:     {len(region_rows):>10,} rows")
    print(f"  nation:     {len(nation_rows):>10,} rows")
    print(f"  part:       {len(part_rows):>10,} rows")
    print(f"  supplier:   {len(supplier_rows):>10,} rows")
    print(f"  partsupp:   {len(partsupp_rows):>10,} rows")
    print(f"  customer:   {len(customer_rows):>10,} rows")
    print(f"  orders:     {len(order_rows):>10,} rows")
    print(f"  lineitem:   {len(lineitem_rows):>10,} rows")


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H benchmark data")
    parser.add_argument("--scale-factor", type=float, default=0.01,
                        choices=[0.01, 0.1, 1.0],
                        help="Scale factor (0.01, 0.1, 1.0)")
    parser.add_argument("--db-backend", choices=["duckdb", "postgresql"], default="duckdb",
                        help="Database backend")
    parser.add_argument("--db-path", type=str, default=None,
                        help="DuckDB file path (default: tpch_sf{SF}.duckdb)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")

    args = parser.parse_args()

    if args.db_backend == "duckdb":
        db_path = args.db_path or os.path.join(
            os.path.dirname(__file__), f"tpch_sf{args.scale_factor}.duckdb",
        )
        create_duckdb_tables(db_path, args.scale_factor, args.seed)

    elif args.db_backend == "postgresql":
        create_postgresql_tables(args.scale_factor, args.seed)


if __name__ == "__main__":
    main()
