# Datatype Support in PartitionCache

PartitionCache now supports multiple datatypes for partition keys, allowing you to work with integer, float, text, and timestamp partition keys depending on your cache handler capabilities.

## Supported Datatypes

- **integer**: Standard integer values (Python `int`)
- **float**: Floating-point numbers (Python `float`) 
- **text**: String values (Python `str`)
- **timestamp**: Date and time values (Python `datetime`)

## Cache Handler Compatibility

Different cache handlers support different datatypes:

| Cache Handler | integer | float | text | timestamp |
|---------------|---------|-------|------|-----------|
| postgresql_array | ✓ | ✓ | ✓ | ✓ |
| postgresql_bit | ✓ | ✗ | ✗ | ✗ |
| redis_set | ✓ | ✗ | ✓ | ✗ |
| redis_bit | ✓ | ✗ | ✗ | ✗ |
| rocksdb_set | ✓ | ✗ | ✓ | ✗ |
| rocksdb_bit | ✓ | ✗ | ✗ | ✗ |
| rocksdict | ✓ | ✓ | ✓ | ✓ |

**Note**: Bit-based handlers (postgresql_bit, redis_bit, rocksdb_bit) only support integers due to their underlying storage mechanism.

## CLI Tool Usage

### Adding Queries to Cache

Use the `--partition-datatype` parameter to specify the datatype:

```bash
# Integer partition keys (default)
python -m partitioncache.cli.add_to_cache \
    --query "SELECT DISTINCT user_id FROM users WHERE age > 25" \
    --partition-key "user_id" \
    --partition-datatype "integer" \
    --cache-backend "postgresql_array" \
    --direct

# String partition keys
python -m partitioncache.cli.add_to_cache \
    --query "SELECT DISTINCT country FROM users WHERE active = true" \
    --partition-key "country" \
    --partition-datatype "text" \
    --cache-backend "postgresql_array" \
    --direct

# Float partition keys
python -m partitioncache.cli.add_to_cache \
    --query "SELECT DISTINCT price FROM products WHERE category = 'electronics'" \
    --partition-key "price" \
    --partition-datatype "float" \
    --cache-backend "rocksdict" \
    --direct

# Timestamp partition keys
python -m partitioncache.cli.add_to_cache \
    --query "SELECT DISTINCT created_at FROM orders WHERE status = 'completed'" \
    --partition-key "created_at" \
    --partition-datatype "timestamp" \
    --cache-backend "postgresql_array" \
    --direct
```

### Reading from Cache

Specify the datatype when reading from cache:

```bash
# Read integer partition keys
python -m partitioncache.cli.read_from_cache \
    --query "SELECT DISTINCT user_id FROM users WHERE age > 25" \
    --partition-key "user_id" \
    --partition-datatype "integer" \
    --cache-backend "postgresql_array"

# Read string partition keys
python -m partitioncache.cli.read_from_cache \
    --query "SELECT DISTINCT country FROM users WHERE active = true" \
    --partition-key "country" \
    --partition-datatype "text" \
    --cache-backend "postgresql_array"
```

### Queue Operations

The queue system automatically handles datatype information:

```bash
# Add to original query queue with datatype
python -m partitioncache.cli.add_to_cache \
    --query "SELECT DISTINCT user_id FROM users WHERE age > 25" \
    --partition-key "user_id" \
    --partition-datatype "integer" \
    --cache-backend "postgresql_array" \
    --queue-original

# Monitor queue (automatically processes datatype information)
python -m partitioncache.cli.monitor_cache_queue \
    --cache-backend "postgresql_array" \
    --db-backend "postgresql"
```

## Programming Interface

### Using Datatype Support in Code

```python
from partitioncache.cache_handler import get_cache_handler
from partitioncache.cache_handler.datatype_support import (
    DATATYPE_TO_PYTHON_TYPE,
    validate_datatype_compatibility
)

# Validate datatype compatibility
cache_type = "postgresql_array"
partition_datatype = "float"
settype = DATATYPE_TO_PYTHON_TYPE[partition_datatype]

try:
    validate_datatype_compatibility(cache_type, settype, "price")
    print("Datatype is compatible!")
except ValueError as e:
    print(f"Incompatible datatype: {e}")

# Use with cache handler
cache_handler = get_cache_handler(cache_type)
partition_key = "price"

# Store float values
float_values = {19.99, 29.99, 39.99}
cache_handler.set_set("query_hash", float_values, settype, partition_key)

# Retrieve values
result = cache_handler.get("query_hash", settype, partition_key)
print(f"Retrieved values: {result}")
```

### Queue Operations

```python
from partitioncache.queue import push_to_original_query_queue, pop_from_original_query_queue

# Push with datatype
query = "SELECT DISTINCT price FROM products WHERE category = 'electronics'"
partition_key = "price"
partition_datatype = "float"

success = push_to_original_query_queue(query, partition_key, partition_datatype)

# Pop returns datatype information
result = pop_from_original_query_queue()
if result:
    query, partition_key, partition_datatype = result
    print(f"Query: {query}")
    print(f"Partition key: {partition_key}")
    print(f"Datatype: {partition_datatype}")
```
## Error Handling

The system validates datatype compatibility at multiple levels:

1. **CLI Level**: Validates datatype compatibility before executing operations
2. **Cache Handler Level**: Validates datatype when storing/retrieving data
3. **Queue Level**: Preserves datatype information through the queue system

Common errors:

- `ValueError: Cache handler 'redis_bit' does not support datatype 'float'`: Use a compatible cache handler or change datatype
- `ValueError: Unsupported datatype: invalid_type`: Use one of the supported datatypes (integer, float, text, timestamp)
- `ValueError: Partition key 'user_id' already exists with datatype 'integer', cannot use datatype 'text'`: Partition keys must maintain consistent datatypes

## Best Practices

1. **Choose the Right Cache Handler**: Use handlers that support your required datatypes
2. **Consistent Datatypes**: Keep partition key datatypes consistent across your application
3. **Validate Early**: Use the validation functions to catch datatype issues early
4. **Document Datatypes**: Clearly document the expected datatypes for your partition keys
5. **Test Compatibility**: Test datatype compatibility before deploying to production 