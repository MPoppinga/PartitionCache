# OpenStreetMap POI Example with PartitionCache

This example demonstrates PartitionCache's workflow using real OpenStreetMap Point of Interest (POI) data for Germany. It showcases multi-partition support with different datatypes and provides a complete end-to-end workflow.

## Overview

The example imports OSM data for Germany and extracts POIs with their associated:
- **Zipcode** (integer partition key): Postal codes for fine-grained geographic partitioning  
- **Landkreis** (text partition key): Administrative districts (admin_level=6) for broader regional partitioning

This demonstrates:
- PartitionCache's API with automatic datatype validation
- Multi-partition support with different datatypes (integer vs text)
- Performance comparisons between partition strategies
- Complete CLI workflow for cache management

## Quick Start

### 1. Start the PostGIS Database

From the `examples/openstreetmap_poi/` directory, run:

```bash
docker-compose up -d
```

This starts a PostgreSQL database with PostGIS enabled, accessible on port 55432.

### 2. Configure Environment Variables

Copy and configure the environment file:

```bash
cp .env.example .env
# Edit .env with your specific configuration if needed
```

### 3. Install Python Dependencies

From the project root:

```bash
pip install -r requirements.txt
```

### 4. Import OSM Data

Run the import script (this may take a while for large OSM files):

```bash
python process_osm_data.py
```

This script will:
- Download the Germany OSM file if not present
- Extract POIs (amenities, shops, tourism, leisure)
- Assign zipcode and administrative boundaries to each POI

### 5. Run Example Queries

Test queries with both partition strategies:

```bash
python run_poi_queries.py
```

This will:
- Run the same queries against both zipcode and administrative boundaries partition keys # TODO administaive boundaries still pending
- Show performance comparisons between strategies
- Demonstrate cache hit/miss scenarios
- Provide helpful hints for adding queries to cache

Expected output shows timing comparisons and cache effectiveness for each partition strategy.

## Working with the Cache

### Adding Queries to Cache

#### For Zipcode Partition (Integer)

```bash
# Add query directly to cache
pcache-add --direct \
  --query-file testqueries_examples/q1.sql \
  --partition-key zipcode \
  --partition-datatype integer \ # interger is the default
  --cache-backend postgresql_array \
  --env .env

# Add to queue for async processing
pcache-add --queue-original \
  --query-file testqueries_examples/q1.sql \
  --partition-key zipcode \
  --partition-datatype integer \ # interger is the default
  --env .env

# Add all test queries for zipcode
for query in testqueries_examples/*.sql; do
  pcache-add --direct \
    --query-file "$query" \
    --partition-key zipcode \
    --partition-datatype integer \ # interger is the default
    --cache-backend postgresql_array \
    --env .env
done
```

#### For Landkreis Partition (Text)

```bash
# Add query directly to cache
pcache-add --direct \
  --query-file testqueries_examples/q1.sql \
  --partition-key landkreis \
  --partition-datatype text \
  --cache-backend postgresql_array \
  --env .env

# Add to queue for async processing
pcache-add --queue-original \
  --query-file testqueries_examples/q1.sql \
  --partition-key landkreis \
  --partition-datatype text \
  --env .env

# Add all test queries for landkreis
for query in testqueries_examples/*.sql; do
  pcache-add --direct \
    --query-file "$query" \
    --partition-key landkreis \
    --partition-datatype text \
    --cache-backend postgresql_array \
    --env .env
done
```

### Reading from Cache

```bash
# Read zipcode partition keys for a query
pcache-get \
  --query-file testqueries_examples/q1.sql \
  --partition-key zipcode \
  --partition-datatype integer \
  --cache-backend postgresql_array \
  --env .env

# Read landkreis partition keys for a query
pcache-get \
  --query-file testqueries_examples/q1.sql \
  --partition-key landkreis \
  --partition-datatype text \
  --cache-backend postgresql_array \
  --env .env
```

### Queue Monitoring

Start the queue monitor to process queries asynchronously:

```bash
pcache-observer \
  --cache-backend postgresql_array \
  --db-backend postgresql \
  --env .env
```

### Cache Management

```bash
# Count cache entries
pcache-manage --count --cache postgresql_array --env .env

# Count queue entries
pcache-manage --count-queue --env .env

# Clear specific partition
pcache-manage --delete-partition \
  --cache postgresql_array \
  --partition-key zipcode \
  --env .env
```

## Example Queries

The example includes three test queries:

1. **q1.sql**: Find ice cream shops near pharmacies and ALDI supermarkets
2. **q2.sql**: Find pizza restaurants near hotels
3. **q3.sql**: Find banks near ATMs

Each query works with both partition strategies and demonstrates spatial proximity searches.

## Performance Comparison

The example demonstrates how different partition strategies affect query performance:

- **Zipcode partitioning**: Fine-grained, good for local searches
- **Landkreis partitioning**: Broader regions, good for administrative queries

Run `python run_poi_queries.py` to see performance comparisons between:
- Queries without cache
- Queries with zipcode partitioning
- Queries with landkreis partitioning

## Cache Backend Options

The example supports multiple cache backends:

```bash
# PostgreSQL Array (supports all datatypes)
CACHE_BACKEND=postgresql_array

# PostgreSQL Bit (integers only, memory efficient)
CACHE_BACKEND=postgresql_bit

# RocksDB (file-based, good for development)
CACHE_BACKEND=rocksdb

# Redis (in-memory, good for distributed setups)
CACHE_BACKEND=redis
```

## Troubleshooting

### Database Connection Issues
- Ensure PostgreSQL is running: `docker-compose ps`
- Check connection settings in `.env`

### Cache Compatibility Issues
- Use `pcache-add` without arguments to see datatype compatibility
- PostgreSQL Array supports all datatypes
- Bit-based handlers only support integers

### Performance Issues
- Ensure database indexes are created (done automatically)
- Consider using queue-based processing for large datasets
- Monitor cache hit rates in query output


