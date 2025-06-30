# Spatial Test Queries

This directory contains spatial test queries for PartitionCache integration testing. These queries demonstrate different partitioning strategies and spatial operations using simple x,y coordinates.

## Query Structure

### Region-Based Partitioning (`region_based/`)

Partition key: `region_id` (integer)
- **q1_nearby_businesses.sql**: Find restaurants near pharmacies within the same region
- **q2_business_clusters.sql**: Find clusters of different business types using multi-table joins
- **q3_points_near_businesses.sql**: Cross-table spatial joins between points and businesses

### City-Based Partitioning (`city_based/`)

Partition key: `city_id` (integer)
- **q1_city_business_analysis.sql**: Business distribution analysis with aggregations
- **q2_nearest_neighbors.sql**: Nearest neighbor search using window functions

### Zipcode-Based Partitioning (`zipcode_based/`)

Partition key: `zipcode` (text)
- **q1_zipcode_density.sql**: Point and business density calculations
- **q2_cross_zipcode_distances.sql**: Cross-partition spatial queries

## Spatial Data Model

### Tables

1. **test_spatial_points**
   - `id`: Primary key
   - `x, y`: Decimal coordinates (latitude/longitude style)
   - `point_type`: Type of point (restaurant, cafe, shop, park, etc.)
   - `name`: Human-readable name
   - `region_id, city_id, zipcode`: Partition keys
   - `created_at`: Timestamp

2. **test_businesses**
   - `id`: Primary key
   - `x, y`: Decimal coordinates
   - `business_type`: Type of business (pharmacy, supermarket, etc.)
   - `name`: Business name
   - `region_id, city_id, zipcode`: Partition keys
   - `rating`: Business rating (1.0-5.0)
   - `created_at`: Timestamp

### Geographic Distribution

The test data simulates a city-like area with:
- 5 regions (Downtown, Westside, Eastside, Northside, Southside)
- 10 cities (2 per region)
- Multiple zipcodes per city
- Realistic coordinate clustering around region centers

### Distance Calculations

Queries use standard SQL distance calculations:
```sql
SQRT(POWER(x1 - x2, 2) + POWER(y1 - y2, 2))
```

Approximate distance conversions:
- 0.001 decimal degrees ≈ 100 meters
- 0.005 decimal degrees ≈ 500 meters  
- 0.01 decimal degrees ≈ 1 kilometer

## Cache Optimization Strategies

### 1. Region-Based Partitioning
- **Best for**: Queries filtering by geographic regions
- **Cache effectiveness**: High for region-specific queries
- **Example**: "Find businesses in Downtown region"

### 2. City-Based Partitioning
- **Best for**: Queries analyzing city-level patterns
- **Cache effectiveness**: Medium to high for city-specific queries
- **Example**: "Analyze business distribution in City 1"

### 3. Zipcode-Based Partitioning
- **Best for**: Fine-grained geographic queries
- **Cache effectiveness**: High for zipcode-specific queries
- **Example**: "Find businesses in zipcode 10101"

## Query Complexity Levels

### Simple Spatial Queries
- Single table with spatial filter
- Basic distance calculations
- Good for testing basic cache functionality

### Medium Complexity
- Two-table joins with spatial constraints
- Aggregations with spatial grouping
- Tests cache effectiveness with joins

### Complex Spatial Queries
- Multi-table joins (3+ tables)
- Window functions with spatial ordering
- Cross-partition spatial queries
- Tests advanced cache optimization

## Performance Testing

The queries are designed to test:

1. **Cache Hit Rates**: How often cached partition keys are found
2. **Query Optimization**: Performance improvement with cache
3. **Cross-Partition Handling**: Queries spanning multiple partitions
4. **Scalability**: Performance with different data sizes

## Integration with PartitionCache

Each query is tested with:
- **Cache lookup**: Finding relevant partition keys
- **Query optimization**: Extending queries with IN clauses
- **Performance comparison**: Before/after cache timing
- **Result validation**: Ensuring cache doesn't change results

## Usage in CI/CD

These queries are automatically tested in the CI pipeline:
- **Small dataset**: 5,000 points, 1,500 businesses
- **Medium dataset**: 50,000 points, 15,000 businesses
- **Large dataset**: 500,000 points, 150,000 businesses

The tests verify that PartitionCache can effectively optimize spatial queries across different cache backends and partition strategies.