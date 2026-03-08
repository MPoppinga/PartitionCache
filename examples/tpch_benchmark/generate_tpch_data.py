#!/usr/bin/env python3
"""
TPC-H Data Generator for PartitionCache.

Uses DuckDB's built-in TPC-H generator (``CALL dbgen(sf=X)``) to produce
spec-compliant data with correct distributions, grammar-based names, and
deterministic output matching the official ``dbgen`` tool.

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
import sys
import tempfile
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Tables in dependency order (children before parents for DROP, parents before children for INSERT)
TABLES_DROP_ORDER = ["lineitem", "orders", "partsupp", "customer", "supplier", "part", "nation", "region"]
TABLES_INSERT_ORDER = list(reversed(TABLES_DROP_ORDER))


def _report_row_counts(conn, is_duckdb=True):
    """Print row counts for all TPC-H tables."""
    for table in TABLES_INSERT_ORDER:
        if is_duckdb:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        else:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
        print(f"  {table + ':':14s} {count:>10,} rows")


# --- DuckDB Backend ---

def create_duckdb_tables(db_path, scale_factor, seed=42):
    """Create TPC-H tables in DuckDB using the built-in dbgen generator."""
    import duckdb

    print(f"Creating TPC-H dataset in DuckDB: {db_path} (SF={scale_factor})")

    conn = duckdb.connect(str(db_path))

    # Drop existing tables
    for table in TABLES_DROP_ORDER:
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    # Generate spec-compliant data via DuckDB's built-in TPC-H extension
    print("  Loading TPC-H extension and generating data...")
    conn.execute("INSTALL tpch; LOAD tpch")
    conn.execute(f"CALL dbgen(sf={scale_factor})")
    print("  Data generated successfully.")

    # Create indexes on partition key and dimension lookup columns
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

    print(f"\nTPC-H dataset created successfully in {db_path}")
    _report_row_counts(conn, is_duckdb=True)

    conn.close()


# --- PostgreSQL Backend ---

# DDL for PostgreSQL table creation (matches DuckDB dbgen schema)
_PG_DDL = {
    "region": """
        CREATE TABLE region (
            r_regionkey INTEGER PRIMARY KEY,
            r_name VARCHAR(25),
            r_comment VARCHAR(152)
        )""",
    "nation": """
        CREATE TABLE nation (
            n_nationkey INTEGER PRIMARY KEY,
            n_name VARCHAR(25),
            n_regionkey INTEGER,
            n_comment VARCHAR(152)
        )""",
    "part": """
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
        )""",
    "supplier": """
        CREATE TABLE supplier (
            s_suppkey INTEGER PRIMARY KEY,
            s_name VARCHAR(25),
            s_address VARCHAR(40),
            s_nationkey INTEGER,
            s_phone VARCHAR(15),
            s_acctbal DECIMAL(15,2),
            s_comment VARCHAR(101)
        )""",
    "partsupp": """
        CREATE TABLE partsupp (
            ps_partkey INTEGER,
            ps_suppkey INTEGER,
            ps_availqty INTEGER,
            ps_supplycost DECIMAL(15,2),
            ps_comment VARCHAR(199),
            PRIMARY KEY (ps_partkey, ps_suppkey)
        )""",
    "customer": """
        CREATE TABLE customer (
            c_custkey INTEGER PRIMARY KEY,
            c_name VARCHAR(25),
            c_address VARCHAR(40),
            c_nationkey INTEGER,
            c_phone VARCHAR(15),
            c_acctbal DECIMAL(15,2),
            c_mktsegment VARCHAR(10),
            c_comment VARCHAR(117)
        )""",
    "orders": """
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
        )""",
    "lineitem": """
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
        )""",
}


def create_postgresql_tables(scale_factor, seed=42):
    """
    Create TPC-H tables in PostgreSQL using DuckDB's built-in generator.

    Two-phase approach:
    1. Generate spec-compliant data in a temporary DuckDB file via ``CALL dbgen()``
    2. Export each table to CSV and bulk-load into PostgreSQL via ``COPY FROM``
    """
    try:
        import duckdb
        import psycopg
        from dotenv import load_dotenv
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install duckdb psycopg[binary] python-dotenv")
        sys.exit(1)

    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_user = os.getenv("DB_USER", "app_user")
    db_password = os.getenv("DB_PASSWORD", "")
    db_name = os.getenv("DB_NAME", "tpch_db")

    print(f"Creating TPC-H dataset in PostgreSQL: {db_host}:{db_port}/{db_name} (SF={scale_factor})")

    # Phase 1: Generate data in temporary DuckDB
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_db = os.path.join(tmpdir, "tpch_tmp.duckdb")
        print("  Phase 1: Generating data in temporary DuckDB...")
        duck_conn = duckdb.connect(tmp_db)
        duck_conn.execute("INSTALL tpch; LOAD tpch")
        duck_conn.execute(f"CALL dbgen(sf={scale_factor})")
        print("  Data generated successfully.")

        # Phase 2: Export to CSV and load into PostgreSQL
        print("  Phase 2: Loading data into PostgreSQL...")

        pg_conn = psycopg.connect(
            host=db_host, port=int(db_port), user=db_user, password=db_password, dbname=db_name,
        )
        pg_conn.autocommit = True

        with pg_conn.cursor() as cur:
            # Drop existing tables
            for table in TABLES_DROP_ORDER:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

            # Create tables
            for table in TABLES_INSERT_ORDER:
                cur.execute(_PG_DDL[table])

            # Export each table from DuckDB to CSV, then bulk-load into PostgreSQL
            for table in TABLES_INSERT_ORDER:
                print(f"    Loading {table}...")
                csv_path = os.path.join(tmpdir, f"{table}.csv")
                duck_conn.execute(
                    f"COPY {table} TO '{csv_path}' (FORMAT CSV, HEADER)"
                )

                with open(csv_path, "rb") as f:
                    with cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
                        while data := f.read(65536):
                            copy.write(data)

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

        duck_conn.close()

    print(f"\nTPC-H dataset created successfully in PostgreSQL")
    _report_row_counts(pg_conn, is_duckdb=False)
    pg_conn.close()


def main():
    parser = argparse.ArgumentParser(description="Generate TPC-H benchmark data")
    parser.add_argument("--scale-factor", type=float, default=0.01,
                        help="Scale factor (e.g. 0.01, 0.1, 1.0)")
    parser.add_argument("--db-backend", choices=["duckdb", "postgresql"], default="duckdb",
                        help="Database backend")
    parser.add_argument("--db-path", type=str, default=None,
                        help="DuckDB file path (default: tpch_sf{SF}.duckdb)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (no-op for DuckDB dbgen, kept for backward compatibility)")

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
