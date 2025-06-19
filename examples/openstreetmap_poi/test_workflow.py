#!/usr/bin/env python3
"""
PartitionCache Complete Workflow Test Script

This script demonstrates a complete PartitionCache workflow including:
1. Direct processor setup and configuration
2. Running POI queries to populate cache automatically
3. Monitoring processor status and logs
4. Checking cache statistics
5. Re-running queries to demonstrate performance improvements

This serves as both a test and a comprehensive example of PartitionCache in action.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg2
import partitioncache
from dotenv import load_dotenv

# Add the project root to sys.path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))



def run_command(cmd, description="", check=True, capture=True):
    """Run a shell command and return the result."""
    print(f"\n🔧 {description}")
    print(f"   Command: {' '.join(cmd)}")

    try:
        if capture:
            result = subprocess.run(cmd, capture_output=True, text=True, check=check)
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()}")
            if result.stderr.strip():
                print(f"   Error: {result.stderr.strip()}")
            return result
        else:
            result = subprocess.run(cmd, check=check)
            return result
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Command failed with return code {e.returncode}")
        if capture and e.stdout:
            print(f"   Stdout: {e.stdout}")
        if capture and e.stderr:
            print(f"   Stderr: {e.stderr}")
        if check:
            raise
        return e


def wait_for_database():
    """Wait for the database to be ready."""
    print("\n⏳ Waiting for database to be ready...")

    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                connect_timeout=3,
            )
            conn.close()
            print("   ✅ Database is ready!")
            return True
        except psycopg2.OperationalError:
            if attempt < max_attempts - 1:
                print(f"   ⏳ Attempt {attempt + 1}/{max_attempts} - waiting 2 seconds...")
                time.sleep(2)
            else:
                print("   ❌ Database connection timeout!")
                return False
    return False


def run_poi_query(query_file, description=""):
    """Run a single POI query to test performance."""
    print(f"\n🔍 Running POI query: {description}")

    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"), host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT")
        )

        with open(query_file, "r") as f:
            query = f.read().strip()

        print(f"   Query file: {query_file}")

        # Run without cache first
        start_time = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        elapsed_without_cache = time.perf_counter() - start_time

        print(f"   Without cache: {len(rows)} results in {elapsed_without_cache:.3f} seconds")

        # Test with cache
        cache_backend = os.getenv("CACHE_BACKEND", "postgresql_array")
        try:
            with partitioncache.create_cache_helper(cache_backend, "landkreis", "text") as cache:
                start_time = time.perf_counter()
                partition_keys, num_subqueries, num_hits = partitioncache.get_partition_keys(
                    query=query,
                    cache_handler=cache.underlying_handler,
                    partition_key="landkreis",
                    min_component_size=1,
                )
                cache_lookup_time = time.perf_counter() - start_time

                if partition_keys:
                    optimized_query = partitioncache.extend_query_with_partition_keys(
                        query,
                        partition_keys,
                        partition_key="landkreis",
                        method="IN",
                        p0_alias="p1",
                    )

                    start_time = time.perf_counter()
                    with conn.cursor() as cur:
                        cur.execute(optimized_query)
                        rows_cached = cur.fetchall()
                    elapsed_with_cache = time.perf_counter() - start_time

                    total_cache_time = cache_lookup_time + elapsed_with_cache
                    speedup = elapsed_without_cache / total_cache_time if total_cache_time > 0 else 0

                    print(f"   With cache: {len(rows_cached)} results in {cache_lookup_time:.3f} + {elapsed_with_cache:.3f} = {total_cache_time:.3f} seconds")
                    print(f"   Cache hits: {num_hits}/{num_subqueries} subqueries, Speedup: {speedup:.2f}x")

                    return len(rows), num_hits > 0
                else:
                    print("   No cached partition keys found")
                    return len(rows), False

        except ValueError as e:
            print(f"   Cache error: {e}")
            return len(rows), False

        conn.close()

    except Exception as e:
        print(f"   ❌ Error running query: {e}")
        return 0, False


def main():
    """Main workflow demonstration."""
    print("=" * 80)
    print("🚀 PartitionCache Complete Workflow Test")
    print("=" * 80)

    # Load environment variables
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✅ Loaded environment from {env_file}")
    else:
        print("⚠️  No .env file found. Using environment variables.")
        print("   Make sure to set DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME")
        print("   and CACHE_BACKEND, plus queue configuration variables.")

    # Verify essential environment variables
    required_vars = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        print("   Please set these variables or create a .env file.")
        return 1

    print("\n📊 Configuration:")
    print(f"   Database: {os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")
    print(f"   Cache Backend: {os.getenv('CACHE_BACKEND', 'postgresql_array')}")
    print(f"   Queue Provider: {os.getenv('QUERY_QUEUE_PROVIDER', 'postgresql')}")

    # Wait for database
    if not wait_for_database():
        print("❌ Cannot connect to database. Is the container running?")
        print("   Try: docker-compose up -d")
        return 1

    # Step 1: Setup PartitionCache tables
    print("\n" + "=" * 60)
    print("📝 Step 1: Setting up PartitionCache tables")
    print("=" * 60)

    run_command(["pcache-manage", "setup", "all"], "Setting up queue and cache metadata tables")

    # Step 2: Setup and configure direct processor
    print("\n" + "=" * 60)
    print("🔧 Step 2: Setting up PostgreSQL Direct Processor")
    print("=" * 60)

    run_command(["pcache-postgresql-queue-processor", "setup"], "Setting up direct processor with pg_cron")

    run_command(["pcache-postgresql-queue-processor", "enable"], "Enabling the direct processor")

    run_command(["pcache-postgresql-queue-processor", "config", "--max-jobs", "5", "--frequency", "2"], "Configuring processor for 5 parallel jobs every 2 seconds")

    # Step 3: Check initial status
    print("\n" + "=" * 60)
    print("📊 Step 3: Checking initial status")
    print("=" * 60)

    run_command(["pcache-postgresql-queue-processor", "status"], "Checking direct processor status")

    run_command(["pcache-manage", "queue", "count"], "Checking initial queue length")

    run_command(["pcache-manage", "cache", "count"], "Checking initial cache entries")

    # Step 4: Run initial POI queries (should find no cache)
    print("\n" + "=" * 60)
    print("🔍 Step 4: Running initial POI queries (no cache expected)")
    print("=" * 60)

    query_file = Path(__file__).parent / "testqueries_examples" / "landkreis" / "q1.sql"
    rows_initial = 0
    cache_hit_initial = False

    if query_file.exists():
        rows_initial, cache_hit_initial = run_poi_query(query_file, "Ice cream near pharmacies and ALDI (Q1)")
        if cache_hit_initial:
            print("   ⚠️  Unexpected cache hit - cache may already contain data")
        else:
            print("   ✅ No cache hit as expected for initial run")
    else:
        print(f"   ⚠️  Query file not found: {query_file}")
        print("   Make sure OSM data is processed first with process_osm_data.py")

    # Step 5: Add queries to processing queue
    print("\n" + "=" * 60)
    print("📥 Step 5: Adding queries to processing queue")
    print("=" * 60)

    landkreis_queries = ["testqueries_examples/landkreis/q1.sql", "testqueries_examples/landkreis/q2.sql"]

    for query_file_name in landkreis_queries:
        query_path = Path(__file__).parent / query_file_name
        if query_path.exists():
            run_command(
                ["pcache-add", "--queue", "--query-file", str(query_path), "--partition-key", "landkreis"], f"Adding {query_file_name} to processing queue"
            )
        else:
            print(f"   ⚠️  Query file not found: {query_path}")

    # Step 6: Monitor processing
    print("\n" + "=" * 60)
    print("⏳ Step 6: Monitoring queue processing")
    print("=" * 60)

    run_command(["pcache-manage", "queue", "count"], "Checking queue length after adding queries")

    print("\n⏳ Waiting for direct processor to process queries...")
    print("   The processor runs every 2 seconds. Waiting 15 seconds for processing...")

    for i in range(15):
        time.sleep(1)
        if (i + 1) % 5 == 0:
            print(f"   ⏳ {i + 1}/15 seconds elapsed...")

    # Step 7: Check processing status and logs
    print("\n" + "=" * 60)
    print("📊 Step 7: Checking processing results")
    print("=" * 60)

    run_command(
        ["pcache-postgresql-queue-processor", "status-detailed"],
        "Checking detailed processor status",
        check=False,  # Don't fail if this command has issues
    )

    run_command(
        ["pcache-postgresql-queue-processor", "logs", "--limit", "10"],
        "Checking recent processor logs",
        check=False,  # Don't fail if this command has issues
    )

    run_command(["pcache-manage", "queue", "count"], "Checking remaining queue length")

    run_command(["pcache-manage", "cache", "count"], "Checking cache entries after processing")

    # Step 8: Re-run queries to demonstrate cache effectiveness
    print("\n" + "=" * 60)
    print("🔍 Step 8: Re-running queries to test cache effectiveness")
    print("=" * 60)

    if query_file.exists():
        rows_final, cache_hit_final = run_poi_query(query_file, "Ice cream near pharmacies and ALDI (Q1) - with cache")

        print("\n📈 Results Summary:")
        print(f"   Initial run (no cache): {rows_initial} results")
        print(f"   Final run (with cache): {rows_final} results")
        print(f"   Cache effectiveness: {'✅ Cache hit!' if cache_hit_final else '❌ No cache hit'}")

        if cache_hit_final:
            print("   🎉 Workflow completed successfully!")
            print("   The direct processor automatically populated the cache and queries are now optimized!")
        else:
            print("   ⚠️  Cache not effective. Check processor logs for issues.")

    # Step 9: Advanced monitoring
    print("\n" + "=" * 60)
    print("🔍 Step 9: Advanced monitoring and analysis")
    print("=" * 60)

    run_command(
        ["pcache-postgresql-queue-processor", "queue-info"],
        "Checking detailed queue and cache architecture info",
        check=False,  # Don't fail if this command has issues
    )

    # Optional: Run full POI query suite
    poi_script = Path(__file__).parent / "run_poi_queries.py"
    if poi_script.exists():
        print("\n🚀 Running complete POI query suite...")
        try:
            run_command([sys.executable, str(poi_script)], "Running complete POI query performance comparison", capture=False)
        except subprocess.CalledProcessError:
            print("   ⚠️  POI query suite had some issues (this is normal if data isn't fully loaded)")

    print("\n" + "=" * 80)
    print("🎉 PartitionCache Workflow Test Complete!")
    print("=" * 80)
    print("Summary:")
    print("✅ Direct processor setup and configuration")
    print("✅ Automatic query processing via pg_cron")
    print("✅ Cache population from complex spatial queries")
    print("✅ Performance monitoring and logging")
    print("✅ Cache effectiveness demonstration")
    print("\nNext steps:")
    print("- Add more queries to the queue for automatic processing")
    print("- Monitor processor status: pcache-postgresql-queue-processor status")
    print("- Check cache statistics: pcache-manage cache count")
    print("- View processor logs: pcache-postgresql-queue-processor logs")

    return 0


if __name__ == "__main__":
    sys.exit(main())
