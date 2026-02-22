# Partition-Join Table Handling in PartitionCache

## Overview

Partition-join tables ("p0 tables") are special tables used solely for partition key joins in star-schema query patterns. These tables serve as the central hub connecting other tables through the partition key, without contributing any filtering conditions. PartitionCache automatically detects and optimizes these tables by excluding them from variant generation and then re-adding them to each variant, significantly reducing the total number of generated variants while maintaining query correctness.

## How Partition-Join Detection Works

The system uses a three-tier detection approach to identify partition-join tables:

### 1. Smart Detection (Default: Enabled)
- **Pattern**: Tables that join to ALL other tables AND have ONLY partition key conditions
- **Purpose**: Automatically detects star-schema patterns without naming conventions
- **Example**: A `region_mapping` table that connects all regional tables via `region_id`

### 2. Naming Convention
- **Pattern**: Tables with names starting with 'p0' (case-insensitive) AND no attribute conditions
- **Purpose**: Legacy support and explicit partition-join designation
- **Example**: `p0_city`, `p0_partition`, `p0_region`

### 3. Explicit Specification
- **Pattern**: Tables specified via `partition_join_table` parameter (matches by alias or table name)
- **Purpose**: Override automatic detection for specific use cases
- **Example**: `partition_join_table='region_map'` or `partition_join_table='rm'` to mark region_map table

**Note**: Only ONE partition-join table is used per query. If multiple are detected, the first (alphabetically) is used.

Example of a P0 table that WILL be excluded:
```sql
FROM users u, orders o, p0_city p0
WHERE u.city_id = p0.city_id 
  AND o.city_id = p0.city_id
  -- p0 has no other conditions
```

Example of a P0 table that will NOT be excluded:
```sql
FROM users u, orders o, p0_city p0  
WHERE u.city_id = p0.city_id
  AND o.city_id = p0.city_id
  AND p0.active = true  -- Has condition, so NOT excluded
```

## How Variant Generation Works

### Step-by-Step Process

The partition-join optimization process:

1. **Parse Query**: Extract tables, conditions, and join relationships
2. **Detect Partition-Join**: Identify partition-join table using the three-tier approach
3. **Build Graph**: Create connected component graph of non-partition-join tables
4. **Generate Base Variants**: Create combinations of non-partition-join tables
5. **Re-add Partition-Join**: Add the partition-join table to EVERY variant
6. **Optimize Joins**: Ensure all tables join to partition-join table on partition key

### Why This Works

The optimization is based on the mathematical property that partition-join tables don't affect the selectivity of queries when they only provide partition key equality:

```
Original: A ⋈ B ⋈ C ⋈ PartitionJoin (on partition key)
Equivalent: (A ⋈ B ⋈ C) ⋈ PartitionJoin (on partition key)
```

Since the partition-join table must be included for correctness but doesn't affect which rows match, we can:
1. Generate variants without it (reducing combinations)
2. Add it back to every variant (maintaining correctness)

### Example Transformation

Original query:
```sql
SELECT * FROM users u, orders o, p0_city p0
WHERE u.city_id = p0.city_id 
  AND o.city_id = p0.city_id
  AND u.age > 25
  AND o.total > 100
```

Generated variants (ONLY with P0 re-added):
```sql
-- Variant 1 (single table with P0)
SELECT DISTINCT t1.city_id FROM users AS t1, p0_city AS p1
WHERE t1.age > 25 
  AND t1.city_id = p1.city_id

-- Variant 2 (single table with P0)
SELECT DISTINCT t1.city_id FROM orders AS t1, p0_city AS p1
WHERE t1.total > 100
  AND t1.city_id = p1.city_id

-- Variant 3 (multi-table with P0)
SELECT DISTINCT t1.city_id FROM users AS t1, orders AS t2, p0_city AS p1
WHERE t1.age > 25 
  AND t2.total > 100
  AND t1.city_id = t2.city_id
  AND t1.city_id = p1.city_id
  AND t2.city_id = p1.city_id
```

If the P0 table has partition key conditions:
```sql
-- Original with P0 condition
SELECT * FROM users u, orders o, p0_city p0
WHERE u.city_id = p0.city_id
  AND o.city_id = p0.city_id
  AND p0.city_id IN (SELECT city_id FROM active_cities)
  AND u.age > 25

-- Generated variant with condition
SELECT DISTINCT t1.city_id FROM users AS t1, p0_city AS p1
WHERE t1.age > 25
  AND t1.city_id = p1.city_id
  AND p1.city_id IN (SELECT city_id FROM active_cities)

-- Generated variant without condition (optional)
SELECT DISTINCT t1.city_id FROM users AS t1, p0_city AS p1
WHERE t1.age > 25
  AND t1.city_id = p1.city_id
```

## Configuration Options

### CLI Parameters

```bash
pcache-add \
  --query "SELECT ..." \
  --partition-key region_id \
  --min-component-size 2 \        # Minimum tables in variants
  --max-component-size 4 \        # Maximum tables in variants
  --follow-graph \                # Use connected component graph
  --no-auto-detect-partition-join \    # Disable smart detection
  --partition-join-table rm \          # Explicit partition-join table (only one)
  --no-warn-partition-key          # Disable partition key warnings
```

### Environment Variables

- `PARTITION_CACHE_MIN_COMPONENT_SIZE`: Minimum tables in variants (default: 1)
- `PARTITION_CACHE_MAX_COMPONENT_SIZE`: Maximum tables in variants (default: no limit)
- `PARTITION_CACHE_FOLLOW_GRAPH`: Generate only connected subgraphs (default: true)
- `PARTITION_CACHE_NO_AUTO_DETECT_PARTITION_JOIN`: Disable smart detection (default: false). Also accepts legacy `PARTITION_CACHE_NO_AUTO_DETECT_STAR_JOIN`.
- `PARTITION_CACHE_PARTITION_JOIN_TABLE`: Single partition-join table alias or name. Also accepts legacy `PARTITION_CACHE_STAR_JOIN_TABLE`.
- `PARTITION_CACHE_NO_WARN_PARTITION_KEY`: Disable partition key warnings (default: false)

### API Parameters

The partition-join parameters are available in all major PartitionCache API functions:

#### `apply_cache_lazy()` - Recommended API
```python
import partitioncache

enhanced_query, stats = partitioncache.apply_cache_lazy(
    query="SELECT * FROM users u, orders o, p0_city p0 WHERE ...",
    cache_handler=cache.underlying_handler,
    partition_key="city_id",
    method="TMP_TABLE_IN",
    auto_detect_partition_join=True,      # Enable smart partition-join detection (default)
    partition_join_table="p0",            # Explicitly mark table by alias
    min_component_size=2,            # Min tables per variant (including partition-join)
    # ... other parameters
)
```

#### `get_partition_keys()` - Cache Lookup
```python
import partitioncache

partition_keys, subqueries, hits = partitioncache.get_partition_keys(
    query="SELECT * FROM users u, orders o, p0_city p0 WHERE ...",
    cache_handler=cache.underlying_handler,
    partition_key="city_id",
    auto_detect_partition_join=True,      # Enable smart partition-join detection (default)
    partition_join_table="p0_city",       # Explicitly mark table by name
    min_component_size=2,            # Min tables per variant
    # ... other parameters
)
```

#### `extend_query_with_partition_keys()` - Query Enhancement
```python
import partitioncache

optimized_query = partitioncache.extend_query_with_partition_keys(
    query="SELECT * FROM users u, orders o, p0_city p0 WHERE ...",
    partition_keys={1, 5, 10, 15, 20},
    partition_key="city_id",
    method="IN",
    p0_alias="p0",                   # Use detected partition-join table alias
)
```

All functions support the same partition-join parameters:
- `auto_detect_partition_join: bool = True` - Enable smart partition-join detection
- `partition_join_table: str | None = None` - Explicitly mark ONE table as partition-join (by alias or name)

## When to Use Partition-Join Optimization

### Good Use Cases

1. **Pure Join Tables**: When partition-join tables only connect partitions without filtering
2. **Star Schemas**: Central mapping table connecting dimension tables
3. **Performance Optimization**: Reduce variant count for large queries
4. **Region/Partition Mappings**: Tables that map entities to partitions

### When NOT to Use

1. **Tables with Filter Conditions**: If the table has non-partition-key conditions
2. **Complex Mappings**: When the table provides more than simple equality joins
3. **Time-based Joins**: Tables with temporal conditions
4. **Business Logic Tables**: Tables that contain actual business data vs. just mappings

## Implementation Details

### Table Ordering

Tables in variants are ordered deterministically by their aliases, ensuring stable hash generation regardless of original query structure.

### Partition Key Handling

When P0 tables are excluded, the system ensures partition key equality by:
- Adding explicit joins between all tables on the partition key
- Maintaining transitivity of partition key relationships

### Performance Considerations

Excluding P0 tables can significantly reduce the number of variants:
- Query with N tables and 1 P0 table: 2^N base variants → 2^(N-1) base variants
- Each base variant has the partition-join table re-added, maintaining correctness
- Benefit: Better query organization and potential for optimization
- Partition-join tables clearly identified for query optimization

### Connected Component Graph

The `follow_graph=True` option uses a connected component graph to generate only variants where tables are connected through non-partition-key relationships:
- Builds a graph where tables are nodes and multi-table predicates (distance conditions, non-equijoin conditions) are edges
- Generates only connected subgraphs as variants
- Reduces combinatorial explosion for queries with many independent table groups
- P0 tables typically have no edges in this graph (only partition key connections)

## Limitations

1. Only ONE partition-join table is allowed per query
2. Only pure-join partition-join tables are excluded (those without non-partition-key conditions)
3. Smart detection automatically applies when enabled (default)

## Best Practices

1. Name pure-join tables with 'p0_' prefix for automatic detection
2. Use `partition_join_table` parameter to explicitly specify ONE partition-join table
3. Enable `warn_no_partition_key` to identify potential issues (default)
4. Review logs for smart-detected partition-join tables
5. Test that query semantics are preserved with partition-join optimization
6. Use `follow_graph=True` (default) for complex queries with independent table groups