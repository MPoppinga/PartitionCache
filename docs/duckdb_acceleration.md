# DuckDB Query Acceleration for PartitionCache

## Overview

The DuckDB Query Acceleration feature provides a high-performance in-memory query layer for `pcache-monitor` when using PostgreSQL as the database backend. This feature creates a DuckDB in-memory instance with PostgreSQL extension support, allowing for significantly faster analytical queries while maintaining full compatibility with existing cache backends.

## Key Features

- **In-Memory DuckDB Instance**: Creates a memory-resident analytical database for faster queries
- **PostgreSQL Extension Integration**: Uses DuckDB's native PostgreSQL extension for seamless data access
- **Table Preloading**: Configurable preloading of PostgreSQL tables into DuckDB for optimal performance
- **Transparent Acceleration**: Automatic query routing with fallback to PostgreSQL on errors
- **Performance Statistics**: Comprehensive metrics tracking and logging
- **Zero Configuration Impact**: Works with all existing cache handlers without modification

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌───────────────────┐
│   pcache-monitor │    │  DuckDB In-Memory │    │  PostgreSQL DB    │
│                 │    │    Accelerator    │    │                   │
│   Query Request │───▶│                  │───▶│  Fallback/Metadata│
│                 │    │  • Preloaded     │    │                   │
│   Cache Storage │───▶│    Tables        │    │  • Original Data  │
│                 │    │  • Fast Analytics│    │  • Schema Info    │
└─────────────────┘    └──────────────────┘    └───────────────────┘
```

## Usage

### Basic Usage

Enable DuckDB acceleration with the feature flag:

```bash
pcache-monitor --enable-duckdb-acceleration --db-backend postgresql
```

### With Table Preloading

Preload specific tables for maximum performance:

```bash
pcache-monitor \
  --enable-duckdb-acceleration \
  --preload-tables "users,orders,products" \
  --db-backend postgresql
```

### Advanced Configuration

Configure memory limits and threading:

```bash
pcache-monitor \
  --enable-duckdb-acceleration \
  --preload-tables "large_table1,large_table2" \
  --duckdb-memory-limit "4GB" \
  --duckdb-threads 8 \
  --db-backend postgresql
```

### Disable Statistics (for production)

Turn off performance statistics logging:

```bash
pcache-monitor \
  --enable-duckdb-acceleration \
  --disable-acceleration-stats \
  --db-backend postgresql
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `--enable-duckdb-acceleration` | flag | `false` | Enable DuckDB query acceleration |
| `--preload-tables` | string | `none` | Comma-separated list of tables to preload |
| `--duckdb-memory-limit` | string | `"2GB"` | Memory limit for DuckDB instance |
| `--duckdb-threads` | integer | `4` | Number of DuckDB processing threads |
| `--disable-acceleration-stats` | flag | `false` | Disable performance statistics logging |
| `--duckdb-database-path` | string | `:memory:` | DuckDB database file for the preloaded tables; tables already in the file are reused without reloading (see below) |
| `--force-reload-tables` | flag | `false` | Reload preloaded tables from PostgreSQL even if they are in the database file |
| `--duckdb-read-unloaded-tables` | flag | `false` | Let DuckDB read tables that were not preloaded directly from PostgreSQL instead of running the whole query on PostgreSQL |

### What can be preloaded

- Tables, views, materialized views and partitioned tables, each copied as a table.
- Names as in PostgreSQL: `orders`, mixed-case names created quoted (`MixedCase`), names that need quoting such as `my-table`, and schema-qualified names (`sales.orders`, kept in a DuckDB schema of the same name; `public.orders` and `orders` both go to DuckDB's default schema).
- A name is looked up as written (quoted) first, then case-folded: if PostgreSQL has both `"MyTable"` and `mytable`, `MyTable` loads `"MyTable"`. Names containing dots inside quotes are not supported.
- Several tables at once (comma-separated); a table that fails to load is skipped with a warning, the others are loaded. All tables share `--duckdb-memory-limit`.

### Consistency with PostgreSQL

Cache entries are computed from what DuckDB returns, so DuckDB must see the same data and compute the same results:

- **Snapshot:** preloaded tables are a copy taken at startup. Rows changed in PostgreSQL afterwards are not seen until the next start. With `--duckdb-database-path` set to a file, tables already in the file are reused across starts (logged as a warning); use `--force-reload-tables` or the default `:memory:` when the data changes.
- **Integer division** follows PostgreSQL (`7 / 2 = 3`, DuckDB's `integer_division` setting).
- **Time zone:** `timestamptz` is evaluated in PostgreSQL's session time zone (comparisons with literals, `extract`, `::date`, `date_trunc`). If DuckDB does not know the zone name, the accelerator stays off.
- **Other differences** that can change results: `round()` of doubles at .5 (DuckDB rounds half away from zero, PostgreSQL to even), and regular expressions (RE2 in DuckDB, POSIX in PostgreSQL).
- **Text ordering** is byte-wise in DuckDB; with a non-C collation in PostgreSQL, range comparisons on text (`name > 'M'`) can differ. Equality and `LIKE` are not affected.
- A query DuckDB cannot run (unknown table or function, PostgreSQL-only syntax) runs on PostgreSQL instead, through an autocommit connection. A DuckDB query that exceeds the timeout is interrupted and also runs on PostgreSQL.
- With `--duckdb-read-unloaded-tables`, names that are not preloaded are read from PostgreSQL's `public` schema through DuckDB's read-only attachment, live, while preloaded tables are the snapshot from startup; a query joining both mixes the two states. Schema-qualified names outside `public` that are not preloaded still run on PostgreSQL.

## Requirements

### Software Dependencies

- **DuckDB**: Version 2.0+; the `postgres` extension is downloaded by DuckDB on first use (network access needed once per DuckDB version, otherwise the accelerator stays off and queries run on PostgreSQL)
- **PostgreSQL**: Compatible with existing PartitionCache PostgreSQL requirements
- **Python**: Python 3.11+ with the `duckdb` package

### Installation

The DuckDB acceleration feature requires the `duckdb` Python package:

```bash
pip install "duckdb>=2.0"   # or: pip install -e ".[db]"
```

### PostgreSQL Extension (Optional)

For maximum compatibility, install the `pg_duckdb` extension in PostgreSQL:

```sql
-- In PostgreSQL as superuser
CREATE EXTENSION IF NOT EXISTS pg_duckdb;
```

## Performance Benefits

### Query Speed Improvements

- **Analytical Queries**: 5-50x faster for complex aggregations and joins
- **Large Table Scans**: 3-10x faster for full table scans on preloaded tables
- **Memory-Resident Data**: Sub-millisecond access for preloaded datasets

### Resource Efficiency

- **Reduced I/O**: Eliminates disk reads for preloaded tables
- **CPU Optimization**: DuckDB's vectorized execution engine
- **Memory Management**: Configurable memory limits prevent system overload

## Monitoring and Statistics

### Performance Metrics

When statistics are enabled (default), the accelerator tracks:

- **Query Counts**: Accelerated vs. fallback queries
- **Execution Times**: Average acceleration and fallback times
- **Acceleration Rate**: Percentage of queries successfully accelerated
- **Table Loading**: Number of tables preloaded and loading time
- **Error Tracking**: Connection errors and fallback events

### Sample Statistics Output

```
=== DuckDB Query Accelerator Statistics ===
Total queries executed: 1,247
Queries accelerated: 1,156 (92.7%)
Queries fallback: 91
Tables preloaded: 3 (took 2.34s)
Average acceleration time: 0.023s
Average fallback time: 0.156s
```

## Error Handling and Fallback

### Automatic Fallback

The accelerator provides seamless fallback to PostgreSQL in case of:

- DuckDB initialization failures
- Query execution errors in DuckDB
- Memory limit exceeded
- Connection issues

### Error Recovery

- **Graceful Degradation**: System continues operating with PostgreSQL
- **Error Logging**: Detailed error information for troubleshooting
- **Statistics Tracking**: Error counts included in performance metrics

## Best Practices

### Table Selection for Preloading

Choose tables for preloading based on:

1. **Query Frequency**: Tables accessed frequently by partition queries
2. **Table Size**: Medium-sized tables (1K-10M rows) benefit most
3. **Join Patterns**: Tables commonly joined in analytical queries
4. **Update Frequency**: Relatively static tables work best: the preloaded copy is a snapshot from startup

### Memory Configuration

Configure memory limits based on:

- **Available System Memory**: Reserve memory for other processes
- **Table Sizes**: Estimate memory needed for preloaded tables
- **Concurrent Processes**: Account for multiple monitor processes

### Production Deployment

For production environments:

1. **Disable Statistics**: Use `--disable-acceleration-stats` to reduce logging
2. **Monitor Memory**: Watch memory usage with system monitoring tools
3. **Test Preloading**: Validate table preloading with actual data
4. **Fallback Testing**: Verify fallback behavior under error conditions

## Troubleshooting

### Common Issues

#### Acceleration Not Enabled

**Symptoms**: No acceleration logs, queries running at normal speed

**Solutions**:
- Verify `--enable-duckdb-acceleration` flag is set
- Ensure `--db-backend postgresql` is specified
- Check that DuckDB package is installed

#### Table Preloading Failures

**Symptoms**: Warnings about tables not found or preloading failures

**Solutions**:
- Verify table names exist in PostgreSQL
- Check PostgreSQL connection parameters
- Ensure sufficient memory for table loading

#### Memory Issues

**Symptoms**: Out of memory errors, system slowdown

**Solutions**:
- Reduce `--duckdb-memory-limit` setting
- Limit number of preloaded tables
- Monitor system memory usage

#### Connection Errors

**Symptoms**: Frequent fallback to PostgreSQL, connection error logs

**Solutions**:
- Verify PostgreSQL connection parameters
- Check network connectivity
- Review PostgreSQL server logs

### Debugging Tips

1. **Enable Verbose Logging**: Use `-v` flag for detailed logs
2. **Check Statistics**: Monitor acceleration rate and error counts
3. **Test Without Preloading**: Try acceleration without table preloading first
4. **Memory Monitoring**: Use system tools to monitor memory usage

## Compatibility

### Cache Backend Compatibility

The DuckDB acceleration feature is compatible with all PartitionCache cache backends:

- ✅ `postgresql_array`
- ✅ `postgresql_bit` 
- ✅ `postgresql_roaringbit`
- ✅ `redis_set`
- ✅ `redis_bit`
- ✅ `rocksdb_set`
- ✅ `rocksdb_bit`
- ✅ `rocksdict`
- ✅ `duckdb_bit`

### Database Backend Support

| Backend | Acceleration Support | Notes |
|---------|---------------------|-------|
| PostgreSQL | ✅ Full Support | Primary target for acceleration |
| MySQL | ❌ Not Supported | PostgreSQL extension required |
| SQLite | ❌ Not Supported | PostgreSQL extension required |

## Implementation Details

### Query Routing Logic

1. **Acceleration Check**: Verify accelerator is initialized and enabled
2. **DuckDB Execution**: Attempt query execution in DuckDB
3. **Result Processing**: Convert results to compatible format
4. **Fallback Handling**: Switch to PostgreSQL on errors
5. **Statistics Update**: Track performance metrics

### Table Preloading Process

1. **Connection Setup**: Establish DuckDB-PostgreSQL connection
2. **Table Validation**: Verify tables exist in source database
3. **Data Transfer**: Copy table data using `postgres_query()` function
4. **Indexing**: Create optimal indexes for query performance
5. **Validation**: Verify row counts and data integrity

### Memory Management

- **Configurable Limits**: Respect user-specified memory limits
- **Automatic Cleanup**: Release resources on shutdown
- **Error Recovery**: Clean up on initialization failures
- **Statistics Tracking**: Monitor memory usage patterns

## Contributing

### Development Setup

1. Install development dependencies:
   ```bash
   pip install -e ".[testing]"
   pip install duckdb
   ```

2. Run tests:
   ```bash
   python -m pytest tests/ -k acceleration
   ```

### Testing Guidelines

- Test with various table sizes and query patterns
- Verify fallback behavior under error conditions
- Validate memory usage under different configurations
- Test compatibility with all cache backends

### Code Style

Follow existing PartitionCache code style:
- Type hints for all functions
- Comprehensive error handling
- Detailed logging and statistics
- Performance-conscious implementation