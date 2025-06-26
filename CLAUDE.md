# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Testing
```bash
python -m pytest                    # Run all tests
python -m pytest tests/            # Run tests in tests directory
python -m pytest -v               # Verbose test output
python -m pytest tests/test_apply_cache.py  # Run specific test file
```

### Code Quality
```bash
ruff check src/                    # Run linting (configured in pyproject.toml)
ruff format src/                   # Format code (optional)
mypy src/                          # Type checking (configured in pyproject.toml)
```

### Development Setup
```bash
pip install -e .                   # Install package in development mode
pip install -e ".[testing]"        # Install with testing dependencies
pip install -e ".[db]"            # Install with database dependencies
pip install -e ".[rocksdb]"       # Install with RocksDB support (if available)
```

### Environment Variables
PartitionCache requires different environment variables depending on the cache backend selected.

**Essential Variables (Required for all setups):**
```bash
# Core configuration
export CACHE_BACKEND=postgresql_array  # Choose your backend
export QUERY_QUEUE_PROVIDER=postgresql # Choose queue provider

# Database connection (for PostgreSQL backends)
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=your_username
export DB_PASSWORD=your_password
export DB_NAME=your_database

# Queue configuration
export PG_QUEUE_HOST=localhost
export PG_QUEUE_PORT=5432
export PG_QUEUE_USER=your_username
export PG_QUEUE_PASSWORD=your_password
export PG_QUEUE_DB=your_database
```

**Backend-Specific Variables:**

For `postgresql_bit`:
```bash
export PG_BIT_CACHE_TABLE_PREFIX=partitioncache_bit
export PG_BIT_CACHE_BITSIZE=10000
```

For `redis` or `redis_bit`:
```bash
export REDIS_HOST=localhost
export REDIS_PORT=6379
export REDIS_CACHE_DB=0  # or REDIS_BIT_DB=1 for redis_bit
export REDIS_BIT_BITSIZE=10000  # for redis_bit only
```

For RocksDB backends:
```bash
export ROCKSDB_PATH=/tmp/rocksdb  # or ROCKSDB_BIT_PATH, ROCKSDB_DICT_PATH
export ROCKSDB_BIT_BITSIZE=10000  # for rocksdb_bit only
```

**Quick Setup:**
```bash
cp .env.example .env          # Copy example configuration
# Edit .env with your values
python test_env_vars.py       # Validate configuration
pcache-manage setup all  # Setup tables
```

### CLI Tools
The package provides several CLI commands via entry points:
```bash
pcache-manage                      # Cache management operations
pcache-add                         # Add queries to cache
pcache-read                        # Read from cache
pcache-monitor                     # Monitor cache queue
pcache-postgresql-queue-processor  # PostgreSQL queue processor management
pcache-postgresql-eviction-manager # Cache eviction management
```

## Architecture Overview

PartitionCache is a middleware system for partition-based query optimization with the following core components:

### Layer Structure
```
Application Layer (CLI Tools, Python API, Helper Objects)
    ↓
Query Processing Layer (Query Processor, Fragment Generator, Cache Application Engine)
    ↓
Cache Storage Layer (PostgreSQL/Redis/RocksDB Handlers with Array/Bit/Set storage)
```

### Core Modules

**`src/partitioncache/`** - Main package with public API
- `__init__.py` - Main API exports (`create_cache_helper`, `apply_cache_lazy`, etc.)
- `apply_cache.py` - Core cache application logic with query extension
- `query_processor.py` - SQL query parsing, normalization, and hash generation
- `queue.py` - Queue operations for asynchronous processing

**`cache_handler/`** - Cache backend implementations
- `abstract.py` - Base classes for cache handlers
- `helper.py` - High-level `PartitionCacheHelper` wrapper
- Storage backends: `postgresql_array.py`, `postgresql_bit.py`, `redis_set.py`, `redis_bit.py`, `rocks_db_*.py`, `rocks_dict.py`

**`queue_handler/`** - Queue processing systems
- `postgresql.py` - Database-native queue processing
- `postgresql_queue_processor.sql` - SQL procedures for pg_cron integration

**`db_handler/`** - Database connection abstractions
- Support for PostgreSQL, MySQL, SQLite

**`cli/`** - Command-line interface tools

### Key Concepts

**Cache Handlers**: Backend-specific implementations (PostgreSQL arrays/bits, Redis sets/bits, RocksDB variants)
- Each handler supports different datatypes (integer, float, text, timestamp)
- Handlers implement `AbstractCacheHandler` with `get()`, `set_set()`, `exists()` methods

**Query Processing**: Uses sqlglot for SQL parsing and normalization
- Generates subquery fragments with `generate_all_hashes()`
- Creates query hashes for cache lookups
- Supports conjunctive (AND) condition optimization via set intersections

**Lazy Cache Application**: Modern API pattern with `apply_cache_lazy()`
- Single function call for cache lookup + query optimization
- Multiple integration methods: `IN_SUBQUERY`, `TMP_TABLE_IN`, `TMP_TABLE_JOIN`
- Avoids duplicate hash generation and provides comprehensive statistics

**Queue System**: Two-tier architecture for asynchronous processing
- `original_query_queue` → `query_fragment_queue`
- PostgreSQL Queue Processor with pg_cron integration (recommended for production)

## Development Guidelines

### Code Style
- Maximum line length: 160 characters
- Use type hints for all functions
- Follow PEP 8 naming conventions (snake_case, PascalCase, UPPER_CASE)
- Use absolute imports over relative imports

### Testing
- All new functionality requires pytest tests
- Use proper fixtures and mocking with pytest-mock
- Test configuration in `pyproject.toml` under `[tool.pytest.ini_options]`
- Always run full test suite after changes
- For integration tests make sure that the test environment/containers are fresh and do not contain any data from previous tests

### Queue Processing in Tests
- **Integration tests use manual processing by default**: Call `partitioncache_manual_process_queue()` instead of pg_cron
- **Avoids concurrency issues**: No job name conflicts in parallel CI runs
- **One dedicated pg_cron test**: `test_pg_cron_integration.py` validates production behavior with proper isolation
- **Production unchanged**: Real deployments still use pg_cron for automated processing

### Architecture Patterns
- Prefer updating existing code over creating special cases for legacy functions
- Use factory pattern for cache handler creation via `get_cache_handler()`
- Implement proper error handling with custom exception classes
- Use Google-style docstrings for all public APIs

### Cache Backend Selection
- `postgresql_array`: Production applications, full datatype support
- `postgresql_bit`: Memory-efficient integer storage  
- `redis_set`: High-throughput, distributed cache
- `rocksdb_set`: Full RocksDB backend (require conda-forge: `conda install -c conda-forge rocksdb`)
- `rocksdb_bit`: Full RocksDB backend (require conda-forge: `conda install -c conda-forge rocksdb`)
- `rocksdict`: Development, file-based storage (pip installable)

### Multi-Partition Support
The system supports multiple partition keys with different datatypes simultaneously, enabling sophisticated partitioning strategies across cities, regions, time periods, etc.
```

## Personal Development and Self-Improvement Notes

- **Self-Debugging Strategy**: If you made a mistake as you missed something or misunderstood, check if this could be a repeating issue and if so document it for yourself and/or in user documentation

- Use context if useful
- **Handling Contexts**: 
  * Use context7 if useful