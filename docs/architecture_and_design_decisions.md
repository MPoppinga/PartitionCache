# PartitionCache — Architecture & Design Decisions

A consolidated reference of all system components and the design decisions behind them, with machine-checkable code anchors.

## 0. Document Purpose & How to Use It

**Audience.** (1) The author, as a coverage and consistency reference for the dissertation: every component and every deliberate design decision of the proof of concept is described here, so dissertation text can be checked against this document ("did I describe everything relevant?", "does what I wrote match the code?"). (2) AI verification agents, which can mechanically check the claims in this document — and, transitively, dissertation claims mapped to them — against the repository.

**Anchor convention.** Code references use the form `path::Symbol` (e.g., `src/partitioncache/apply_cache.py::apply_cache_lazy`), never line numbers. A symbol anchor is verified by `grep -n "def <name>\|class <name>" <path>`; SQL anchors name the SQL function or the file. All anchors in this document were verified against the snapshot below at writing time.

**Design-decision numbering.** Deliberate architectural choices are documented as numbered blocks `DD-01` … `DD-28` (`####`-level headings, stable identifiers). Each block follows a fixed template: Status / Decision / Context / Alternatives considered / Rationale / Trade-offs / Code anchors / Related docs / Dissertation mapping. The **Dissertation mapping** field is intentionally left as a placeholder — filling it (chapter/section per DD) is the author's coverage-check mechanism. Status vocabulary: `implemented (core path)`, `implemented (optional)`, `experimental`, `deprecated`, `superseded by DD-xx`.

**Verification.** Appendix A indexes all design decisions. Appendix B is a machine-checkable claims register (one row per claim, with a restricted verification vocabulary). Appendix C maps detail topics to the existing per-topic documents in `docs/`, which this document links to rather than duplicates.

**Snapshot.** Written against commit `1cf40f6946ef493f4e93e174fc2733fd05b38982` (branch `data_warehouse_support`, 2026-06-10). The working tree at writing time contained the in-flight benchmark consolidation (modified files under `examples/benchmark/`, removal of the legacy per-dataset benchmark directories); this document describes the working-tree state, which is ahead of `main`.

---

## 1. Problem Statement & Core Idea

### 1.1 Problem

Many analytical and search workloads run over datasets that are logically partitioned by a domain attribute — cities, regions, time periods, network segments, document collections. In such workloads, similar complex queries are issued repeatedly, each query is expensive (multi-join, spatial, or pattern-matching predicates), and the result of any single query typically touches only a small subset of the partitions. A conventional DBMS nevertheless evaluates each query against the full search space, because it has no persistent, query-level knowledge of *which partitions can possibly contain results*.

Result caches do not solve this: minor variations in query parameters defeat exact-match result caching, and result caches return stale or no data when the query text differs even slightly. What is needed is a mechanism that prunes the search space for *new, previously unseen* queries based on knowledge gained from *previously executed* queries — i.e., a cache whose entries remain useful across query variations.

The project describes itself in `README.md` as: "Partition-based query optimization middleware for heavily partitioned datasets. PartitionCache automatically decomposes complex queries into variants, caches which partitions contain results for each variant, and uses this knowledge to dramatically reduce search space for future queries by skipping partitions that won't contain results." The package docstring (`src/partitioncache/__init__.py`) frames it as "a caching middleware for partition-based query optimization." The underlying approach is published as a research paper (DOI [10.18420/BTW2025-23](https://doi.org/10.18420/BTW2025-23)).

The stated applicability criteria (from `README.md`) are: complex analytical queries (execution time beyond a few milliseconds), read-only or append-only datasets, searches across logical partitions, sparse results (matches in only a subset of partitions, no aggregation across all partitions), and queries with multiple conjuncts (AND conditions).

### 1.2 Core idea: cache partition keys, not results

PartitionCache decomposes a conjunctive query into its atomic *fragments* and recomposes them into executable subquery *variants*, executes each variant once (asynchronously, via a queue), and stores — per variant — the **set of partition key identifiers** for which the variant yields results. At query time, a new query is decomposed into the same kind of variants; the cached key sets of all variants found in the cache are **intersected**, and the original query is rewritten with an additional restriction (e.g., `AND <partition_key> IN (...)` or a temporary-table join) so that the DBMS only evaluates the partitions in the intersection. The query itself still executes normally inside the restricted search space, so results are always exact.

#### DD-01: Cache partition keys of query variants instead of query results
- **Status:** implemented (core path)
- **Decision:** The cache stores, per query-variant hash, the set of partition key identifiers for which the variant produces results — never the result tuples themselves. Query acceleration is achieved by intersecting these key sets and restricting the original query to the resulting partitions.
- **Context:** Result caches require exact query repetition and large storage; repeated analytical queries over partitioned data vary in parameters but share conjunctive building blocks. A cache keyed on those building blocks, storing only compact partition-key sets, stays useful across query variations and is cheap to store and intersect.
- **Alternatives considered:** (a) full result caching (rejected: storage cost, zero reuse across query variants, staleness semantics); (b) materialized views per query pattern (rejected: requires upfront schema knowledge, DBMS-specific); (c) DBMS-internal partition pruning (insufficient: prunes only on predicates directly over the partition column, not on knowledge derived from executing complex query variants).
- **Rationale:** Partition-key sets are orders of magnitude smaller than results, are valid for any query containing the variant as a conjunctive sub-pattern, compose via set intersection for AND-combined variants, and degrade gracefully — a miss simply means no restriction is added.
- **Trade-offs / costs:** The cache yields no benefit for queries whose results span (nearly) all partitions or for aggregations across all partitions; cached key sets are supersets of the true partition set, so pruning is conservative rather than exact; population requires executing variants, which is itself expensive (hence the asynchronous queue path); append-only/read-mostly data is assumed, since updates can invalidate cached key sets.
- **Code anchors:**
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_cache` (stores `partition_key_identifiers` per hash)
  - `src/partitioncache/apply_cache.py::get_partition_keys` (hash generation + intersection of cached key sets)
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.get_intersected` (server-side/handler-side intersection)
  - `src/partitioncache/query_processor.py::generate_all_query_hash_pairs` (decomposition, recomposition, and additional-variant generation)
- **Related docs:** [README.md](../README.md), [api_reference.md](api_reference.md)
- **Dissertation mapping:** _to be filled by author_

### 1.3 Why decomposition into fragment variants raises hit rates

Two distinct full queries rarely match textually, but they frequently share conjunctive building blocks ("restaurants with rating > 4.0", "POIs within 50 m of a street"). Decomposing every query into atomic fragments and recomposing its connected conjunctive subqueries (`src/partitioncache/query_processor.py::generate_partial_queries`, driven by connected-subgraph enumeration over the query's join graph) turns the cache key space from "whole queries" into "shared fragment variants", so a variant cached while processing query A is a cache hit for any later query B that contains the same sub-pattern.

This is *correct by construction*: a fragment variant contains a subset of the full query's conjuncts and is therefore less restrictive than the full query. Consequently, the partition-key set of a variant is a **superset** of the partition-key set of the full query. Intersecting the cached key sets of several variants yields a set that still contains every partition that can possibly hold a result of the full query — no false negatives. False positives (partitions in the intersection that hold no joint result) are possible and merely cost unnecessary scanning within those partitions; they never affect result correctness. The intersection over the *available* (cached) variants is implemented in `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.get_intersected`: only hashes actually present in the cache participate, so partial cache coverage tightens the restriction monotonically without ever over-restricting.

Additional normalization further raises hit rates across near-identical queries: distance constraints are discretized into buckets (`src/partitioncache/query_processor.py::normalize_distance_conditions`, parameter `bucket_steps`), and additional-variant generation can add or remove constraints to generalize variants (`add_constraints`, `remove_constraints_all`, `remove_constraints_add` parameters of `generate_all_query_hash_pairs`).

### 1.4 What PartitionCache is NOT

- **Not a result cache.** No result tuples are stored; every query is executed against the database, only over a reduced search space.
- **Not a full query rewriter or optimizer replacement.** The rewrite is purely additive: an extra conjunct (or join against a temporary table of partition keys) is attached to the original query (`src/partitioncache/apply_cache.py::extend_query_with_partition_keys`); the DBMS optimizer still plans and executes the query.
- **Results stay exact.** Because cached variant key sets are supersets of the true partition set (Section 1.3), restricting execution to their intersection cannot remove valid results.
- **A cache miss degrades to the original query.** If no variant of a query is cached, `apply_cache_lazy` returns the (working) query unchanged with `enhanced: 0` in its statistics — the system never blocks or fails a query due to cache state (`src/partitioncache/apply_cache.py::apply_cache_lazy`).

### 1.5 Position in the dissertation

_To be filled by author: chapter/section mapping of the partition-key caching approach and its evaluation._

## 2. Terminology & Glossary

| Term | German term (for dissertation alignment) | Definition | Primary code anchor |
|---|---|---|---|
| Partition key | Partitionsschlüssel | The column/attribute that logically partitions the dataset (e.g., `city_id`, `region`). Every cache operation is namespaced by partition key; multiple partition keys with different datatypes can coexist. | `src/partitioncache/cache_handler/helper.py::PartitionCacheHelper` |
| Partition key identifier | Partitionsschlüsselwert | A concrete value of the partition key (e.g., city 42). Cache entries are sets of such identifiers. | `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_cache` (parameter `partition_key_identifiers`) |
| Fragment | Anfragefragment | An **atomic** building block of a query obtained by conjunctive decomposition: a table with its single-table attribute conditions, or a pairwise condition connecting two tables (join edge, distance condition). Fragments are not executable on their own; they are the units from which variants are recomposed. | `src/partitioncache/query_processor.py::extract_and_group_query_conditions` |
| Fragment variant | Fragmentvariante (rekomponierte Teilanfrage) | An executable conjunctive subquery **recomposed** from a connected subset of fragments (connected subgraph of the query's join graph). Internally also called "partial query". | `src/partitioncache/query_processor.py::generate_partial_queries` |
| Additional variant | Zusätzliche Variante | A further variant derived from a fragment variant by **extension**: distance bucketing (`bucket_steps`), constraint addition (`add_constraints`), attribute removal (`remove_constraints_all`/`remove_constraints_add`), or bounded k-condition removal (`max_conditions_removed`). | `src/partitioncache/query_processor.py::normalize_distance_conditions`, `::remove_k_conditions`, `::_apply_constraint_modifications` |
| Query variant | Anfragevariante | Collective term for fragment variants and additional variants — everything that is hashed and used as a cache key. The pipeline `decompose → fragments → recompose → fragment variants → extend → additional variants` is orchestrated by the entry point below. | `src/partitioncache/query_processor.py::generate_all_query_hash_pairs` |
| Query hash | Anfrage-Hash | SHA-1 digest of the normalized query-variant text; the cache key under which a variant's partition-key set is stored. | `src/partitioncache/query_processor.py::hash_query` |
| Cache handler | Cache-Handler (Cache-Backend) | Backend-specific implementation of the cache storage interface (PostgreSQL array/bit/roaringbitmap, Redis, RocksDB, DuckDB, PostGIS variants). | `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler` |
| Lazy intersection | Verzögerte Schnittmengenbildung | Returning the intersection as an SQL subquery string (evaluated inside the database at execution time) instead of materializing the key set in Python. | `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler_Lazy.get_intersected_lazy` |
| Integration method | Integrationsmethode | How the partition restriction is attached to the query: `IN_SUBQUERY`, `TMP_TABLE_IN`, or `TMP_TABLE_JOIN` (non-lazy additionally `IN`, `VALUES`). | `src/partitioncache/apply_cache.py::extend_query_with_partition_keys_lazy` |
| Partition-join table (p0) | Partitions-Join-Tabelle (p0-Tabelle) | A central table joined by all other tables on the partition key (star-schema-like); detected automatically or specified, and usable as the attachment point for cache restrictions. | `src/partitioncache/query_processor.py::detect_partition_join_table`; `src/partitioncache/apply_cache.py::rewrite_query_with_p0_table` |
| Negative cache entry / null marker | Negativeintrag (Null-Markierung) | Marker stating a query variant was processed but produced no usable key set (e.g., too large or failed), distinguishing "known empty/unusable" from "never computed". | `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_null` |
| Query status | Anfragestatus | Per-hash execution status metadata (`ok`, `timeout`, `failed`) stored alongside the query text; used to avoid re-enqueueing known-bad variants. | `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_query_status` |
| Original query queue | Warteschlange für Originalanfragen | First tier of the two-tier queue: whole queries pushed by the application, awaiting decomposition. | `src/partitioncache/queue.py::push_to_original_query_queue` |
| Query variant queue | Variantenwarteschlange (historisch: Fragment-Warteschlange) | Second tier: `(variant_query, hash)` pairs awaiting execution and cache insertion. For backward compatibility the **persisted and wire-level names keep the historical "fragment" naming** (table `{prefix}_query_fragment_queue`, dict key `query_fragment_queue`, function `push_to_query_fragment_queue`) so that no database or API migration is required; the preferred API name is `push_to_query_variant_queue`. | `src/partitioncache/queue.py::push_to_query_variant_queue` |
| bucket_steps | Distanz-Diskretisierung (Schrittweite) | Step size used to normalize numeric distance conditions into buckets so that near-identical distance predicates map to the same variant hash. | `src/partitioncache/query_processor.py::normalize_distance_conditions` |
| Datatype registration | Datentyp-Registrierung | Binding a partition key to a declared datatype in the backend's metadata, validated on every subsequent use to prevent type mismatches. | `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.register_partition_key` |
| Eviction strategy | Verdrängungsstrategie | Policy for automatic cache cleanup when entry counts exceed a threshold: `oldest` (by insertion time) or `largest` (by key-set size), executed via pg_cron. | `src/partitioncache/cli/postgresql_cache_eviction.py::main` |
| Table prefix | Tabellenpräfix | Backend-specific prefix for all cache tables of one logical cache instance, allowing multiple independent caches in one database (e.g., `PG_ARRAY_CACHE_TABLE_PREFIX`). | `src/partitioncache/cache_handler/environment_config.py::EnvironmentConfigManager.get_postgresql_array_config` |

## 3. System Architecture Overview

### 3.1 Layered architecture

PartitionCache is structured as a middleware library with clearly separated layers. Full diagrams are maintained in [architecture_diagrams.md](architecture_diagrams.md); the compact view is:

```
+---------------------------------------------------------------+
| Application layer                                             |
|   CLI tools (pcache-*)  |  Python API (partitioncache.*)      |
|   Facade: PartitionCacheHelper / create_cache_helper          |
+-------------------------------+-------------------------------+
                                |
+-------------------------------v-------------------------------+
| Query processing layer        query_processor.py              |
|   parsing/normalization (sqlglot), join-graph analysis        |
|   (networkx), fragment & variant generation, hashing (SHA-1)  |
+-------------------------------+-------------------------------+
                                |
+---------------v---------------+----------------v--------------+
| Cache application (read path) | Queue / population (write     |
|   apply_cache.py              |   path): queue.py,            |
|   key lookup, intersection,   |   queue_handler/* (PostgreSQL/|
|   query extension             |   Redis), pg_cron SQL procs,  |
|                               |   cli/monitor_cache_queue.py  |
+---------------+---------------+----------------+--------------+
                |                                |
+---------------v--------------------------------v--------------+
| Cache storage layer           cache_handler/*                 |
|   AbstractCacheHandler / AbstractCacheHandler_Lazy            |
|   PostgreSQL array|bit|roaringbit, Redis set|bit|roaringbit,  |
|   RocksDB/RocksDict, DuckDB bit, PostGIS bbox, H3             |
+---------------------------------------------------------------+
| DB access                     db_handler/*                    |
|   AbstractDBHandler: PostgreSQL, MySQL, SQLite, DuckDB        |
+---------------------------------------------------------------+
```

The factory functions tie the layers together: `src/partitioncache/cache_handler/__init__.py::get_cache_handler` instantiates the storage backend from its name, and `src/partitioncache/cache_handler/helper.py::create_partitioncache_helper` wraps it in the partition-key-bound facade `PartitionCacheHelper`. Queue providers are resolved analogously via `src/partitioncache/queue_handler/__init__.py::get_queue_handler`.

### 3.2 The two data paths

**Synchronous read path (query time).** The application calls `src/partitioncache/apply_cache.py::apply_cache_lazy` (or the non-lazy `apply_cache`) with the query, a cache handler, and the partition key. The function (1) generates all query-variant hashes via the query processor, (2) obtains the intersection of cached key sets — as an SQL subquery in the lazy variant (`get_intersected_lazy`) or as a materialized Python set in the non-lazy variant (`get_intersected`) — and (3) rewrites the query using the chosen integration method (`IN_SUBQUERY`, `TMP_TABLE_IN`, `TMP_TABLE_JOIN`). It returns the enhanced query plus statistics (`generated_variants`, `cache_hits`, `enhanced`, `p0_rewritten`). On zero hits, the original query is returned unchanged. This path never executes the user's query itself; it only rewrites it.

**Asynchronous population path (write path).** The application pushes whole queries to the original query queue (`src/partitioncache/queue.py::push_to_original_query_queue`). A processor — either the Python observer `src/partitioncache/cli/monitor_cache_queue.py::main` or the database-native pg_cron processor (see [postgresql_queue_processor.md](postgresql_queue_processor.md)) — decomposes and recomposes them into query variants, pushes those to the query variant queue (persisted as `query_fragment_queue`), executes each variant against the database, and stores the resulting partition-key set together with the variant text via `set_entry`/`set_entry_lazy` (`src/partitioncache/cli/monitor_cache_queue.py::run_and_store_query`). Failures and timeouts are recorded as query status (`set_query_status`) or null markers (`set_null`) so they are not retried indefinitely. Queue architecture details are documented in [queue_system.md](queue_system.md).

### 3.3 Public API surface

The public API is defined by `__all__` in `src/partitioncache/__init__.py`. The facade entry point is `create_cache_helper(cache_type, partition_key, datatype, **kwargs)`, which creates a singleton backend handler and returns a `PartitionCacheHelper` bound to one partition key and datatype. Exports grouped by purpose:

- **Handler creation / facade:** `create_cache_helper`, `create_partitioncache_helper`, `get_cache_handler`, `list_cache_types`, `PartitionCacheHelper`
- **Cache application (read path):** `apply_cache_lazy`, `apply_cache`, `get_partition_keys`, `get_partition_keys_lazy`, `extend_query_with_partition_keys`, `extend_query_with_partition_keys_lazy`
- **Spatial query extension (experimental spatial backends):** `extend_query_with_spatial_filter`, `extend_query_with_spatial_filter_lazy`, `extend_query_with_h3_cell_filter`, `extend_query_with_h3_cell_filter_lazy`, `extend_query_with_h3_cell_lookup`
- **Queue / population path:** `push_to_original_query_queue`, `push_to_query_variant_queue` (preferred name; alias of the original `push_to_query_fragment_queue`, both remain available), `get_queue_lengths`
- **Query processing:** `generate_all_query_hash_pairs`
- **Type aliases:** `DataType`, `DataSet`

The complete signatures are documented in [api_reference.md](api_reference.md); CLI equivalents in [cli_reference.md](cli_reference.md).

### 3.4 Configuration model

All runtime configuration is environment-variable driven and centralized in `src/partitioncache/cache_handler/environment_config.py::EnvironmentConfigManager`. Each backend has a dedicated static accessor (e.g., `get_postgresql_array_config`, `get_redis_config`, `get_rocksdb_config`, `get_postgis_bbox_config`) that reads its variables, converts types, and raises `ValueError` immediately when a required variable is missing (fail-fast). Backend-specific variables use per-backend prefixes (`PG_ARRAY_CACHE_*`, `PG_BIT_CACHE_*`, `REDIS_SET_*`, `REDIS_BIT_*`, `ROCKSDB_*`, `PG_BBOX_*`, ...) with fallbacks to generic variables where sensible (e.g., `REDIS_SET_HOST` falls back to `REDIS_HOST`; `PG_BBOX_HOST` falls back to `DB_HOST`). Backend selection itself is explicit at the API level (`get_cache_handler(cache_type)`); the CLI tools resolve it from `--cache-backend` or the `CACHE_BACKEND` environment variable (`src/partitioncache/cli/common_args.py::resolve_cache_backend`). The queue provider is resolved from `QUERY_QUEUE_PROVIDER` (default `postgresql`) in `src/partitioncache/queue_handler/__init__.py::get_queue_handler`. `EnvironmentConfigManager.validate_environment(cache_type)` allows tools to validate a configuration without instantiating a handler.

#### DD-14: Environment-variable-driven configuration with per-backend namespaces
- **Status:** implemented (core path)
- **Decision:** All connection and backend parameters are read from environment variables through a single `EnvironmentConfigManager`, with backend-specific variable prefixes and generic fallbacks, and fail-fast `ValueError`s for missing required variables.
- **Context:** Up to a dozen storage backends plus two queue providers each need connection parameters; scattering `os.getenv` calls across handlers duplicated validation logic and produced inconsistent error behavior. Deployments (Docker, CI, pg_cron-based processors) favor 12-factor-style environment configuration over config files.
- **Alternatives considered:** (a) configuration files (rejected as primary mechanism: harder to inject into containers and pg_cron jobs; `.env` files are still supported via `python-dotenv` as a loading convenience); (b) per-handler ad-hoc `os.getenv` (the prior state; rejected due to duplication — the module docstring states it exists "to eliminate duplication across cache handler implementations"); (c) programmatic-only configuration via constructor arguments (still possible, since handlers accept explicit kwargs; the manager is layered on top).
- **Rationale:** One audited place for validation and type conversion; per-backend namespaces let several backends coexist in one process/database; generic fallbacks (`DB_*`, `REDIS_*`) keep simple single-backend setups terse; fail-fast errors surface misconfiguration at startup rather than at first query.
- **Trade-offs / costs:** Large variable surface (documented per backend); fallback chains add lookup complexity; environment variables are process-global, so two differently-configured instances of the *same* backend type in one process require explicit constructor arguments instead.
- **Code anchors:**
  - `src/partitioncache/cache_handler/environment_config.py::EnvironmentConfigManager`
  - `src/partitioncache/cache_handler/__init__.py::get_cache_handler`
  - `src/partitioncache/queue_handler/__init__.py::get_queue_handler`
  - `src/partitioncache/cli/common_args.py::resolve_cache_backend`
- **Related docs:** [README.md](../README.md) (environment setup), [cli_reference.md](cli_reference.md)
- **Dissertation mapping:** _to be filled by author_

### 3.5 Packaging & optional dependencies

The package (`pyproject.toml`, version 0.7.0, Python >= 3.10, LGPL-3.0-or-later) keeps its mandatory dependency set small: `networkx` (join-graph analysis), `sqlglot` pinned to `>=25.0.0,<26.0.0` (SQL parsing; pinned below 26 to avoid breaking AST changes), `bitarray` and `pyroaring` (bit/roaring-bitmap key-set representations), `python-dotenv`, and `tqdm`. Database drivers are *not* core dependencies; they are grouped into extras: `db` (psycopg, rocksdict, redis, duckdb, mysql-connector-python), `rocksdb` (separate "because it is not available on all platforms"), `testing`, and `benchmark`. Six console scripts (`pcache-manage`, `pcache-add`, `pcache-read`, `pcache-monitor`, `pcache-postgresql-queue-processor`, `pcache-postgresql-eviction-manager`) are declared under `[project.scripts]`. SQL assets are shipped as package data (`**/*.sql`), which is how the pg_cron processor and eviction procedures reach the database.

#### DD-25: Optional-dependency extras keep the core installable without database drivers
- **Status:** implemented (core path)
- **Decision:** Only parsing/graph/bitset libraries are hard dependencies; every database driver lives in an extra (`db`, `rocksdb`, `benchmark`), and optional backends are imported lazily or guarded with `try/except ImportError` feature flags.
- **Context:** PartitionCache supports many backends (PostgreSQL, Redis, RocksDB, DuckDB, MySQL), but any single deployment uses one or two. RocksDB in particular has no universal wheels, and requiring all drivers would make installation fragile and heavy.
- **Alternatives considered:** (a) all drivers as core dependencies (rejected: install failures on platforms without RocksDB, unnecessary bloat); (b) separate distribution packages per backend (rejected: maintenance overhead disproportionate for a research middleware). Lazy in-function imports inside `get_cache_handler` and `list_cache_types` are the complementary implemented mechanism.
- **Rationale:** `pip install partitioncache` always succeeds and supports query processing and cache application against whichever backend's driver is present; import-time guards (`ROCKSDB_AVAILABLE` in `src/partitioncache/__init__.py`) degrade feature discovery gracefully instead of crashing.
- **Trade-offs / costs:** Missing-driver errors surface at handler creation time rather than install time; the extras matrix must be kept in sync with new backends; conditional imports add `type: ignore` noise and small testing burden for both presence/absence paths.
- **Code anchors:**
  - `src/partitioncache/cache_handler/__init__.py::get_cache_handler` (lazy per-backend imports)
  - `src/partitioncache/__init__.py::list_cache_types` (feature-flagged backend registry)
- **Related docs:** [cache_handlers.md](cache_handlers.md), [datatype_support.md](datatype_support.md)
- **Dissertation mapping:** _to be filled by author_

## 4. Query Processing Pipeline

The query processing pipeline transforms an incoming SQL query into a set of `(variant_query, hash)` pairs that serve as cache keys, following the terminology chain **decompose → fragments → recompose → fragment variants → extend → additional variants** (Section 2). It is implemented almost entirely in `src/partitioncache/query_processor.py` and is shared by both the cache-population path (queue/processor) and the cache-lookup path (`apply_cache`). The pipeline entry point is `src/partitioncache/query_processor.py::generate_all_query_hash_pairs`; the convenience wrapper `src/partitioncache/query_processor.py::generate_all_hashes` returns only the hashes.

The pipeline runs in five stages:

1. **Normalization and cleaning** (`clean_query`) — produce a canonical textual form of the query.
2. **Conjunctive decomposition and recomposition** (`extract_and_group_query_conditions`, `generate_tuples`, `generate_partial_queries`) — decompose the query into atomic fragments, then recompose connected subsets of them into executable fragment variants.
3. **Additional-variant generation** (`normalize_distance_conditions`, `remove_k_conditions`, `_apply_constraint_modifications`) — extend fragment variants with controlled relaxations as additional lookup keys.
4. **Partition-join handling** (`detect_partition_join_table`) — special-case the central partition-join table so it does not inflate the variant space.
5. **Hashing** (`hash_query`) — derive a content hash per query variant that serves as the cache key.

The correctness invariant maintained throughout is: *every generated query variant is a logical relaxation of the original query with respect to the partition key*. A relaxed query matches at least all partition keys that the original query matches, so each variant's cached key set is a superset of the original query's key set, and intersecting any number of cached variant results can never exclude a correct partition (no false negatives in the final query result; at worst the restriction is less selective).

### 4.1 Normalization and cleaning

`src/partitioncache/query_processor.py::clean_query` converts an arbitrary input query into a stable canonical form before any decomposition or hashing takes place. Because cache keys are content hashes of query-variant text (Section 4.5), two queries that are semantically equivalent for partition-key purposes must reach an identical textual form, otherwise they produce different hashes and the cache misses spuriously.

`clean_query` performs, in order:

1. **Comment and whitespace normalization**: single-line `--` comments are stripped (before whitespace collapsing, so a comment cannot swallow the rest of a flattened query), all whitespace runs are collapsed to single spaces, spacing around `=` is removed, and trailing semicolons are dropped.
2. **JOIN normalization**: `src/partitioncache/query_processor.py::normalize_joins_to_cross_join` rewrites explicit `JOIN ... ON` syntax into comma-joins with the `ON` predicates moved into the `WHERE` clause. This ensures `FROM a JOIN b ON cond` and `FROM a, b WHERE cond` decompose into identical fragments. Only joins at the outermost SELECT scope are rewritten (joins inside subqueries, `EXISTS`, or `IN` clauses are left intact). Outer joins (`LEFT`/`RIGHT`/`FULL`) are also converted, with a logged warning, because the rewrite drops NULL-preserving semantics; this is acceptable for variant generation because the rewrite only relaxes the query.
3. **CNF normalization and simplification**: the query is parsed with sqlglot and passed through `sqlglot.optimizer.normalize.normalize` (conjunctive normal form) and `sqlglot.optimizer.simplify.simplify` (which, among other things, orders comparison operands consistently). CNF is a prerequisite for the conjunctive decomposition in Section 4.2, which treats the `WHERE` clause as a flat list of AND-ed conditions.
4. **Clause removal**: `ORDER BY`, `LIMIT`, `GROUP BY`, and `HAVING` are removed from all SELECT scopes. These clauses do not affect *which* partition keys a query touches, only how results are presented or aggregated, so removing them lets queries differing only in these clauses share cache entries.
5. **SELECT replacement**: the expressions of the outermost SELECT are replaced with `*` (subqueries and CTEs keep their column lists). The projected columns are irrelevant for partition-key reachability, and replacing them avoids confusing downstream regex-based condition parsing.
6. **Identifier quoting removal**: double quotes and backticks are stripped (quoted identifiers are currently unsupported, noted as TODO in the source).

#### DD-03: Canonical query normalization before hashing (sqlglot-based)
- **Status:** implemented (core path)
- **Decision:** Normalize every query into a canonical textual form — JOINs rewritten to comma-joins with WHERE predicates, CNF via sqlglot's optimizer, presentation clauses removed, SELECT list replaced by `*` — before decomposition and hashing.
- **Context:** Cache keys are content hashes of query-variant text. Without normalization, trivially different spellings of the same query (JOIN vs. comma-join, condition order, ORDER BY/LIMIT differences, projected columns) would hash differently and the cache hit rate would collapse.
- **Alternatives considered:** (a) Hashing the raw query text — rejected, maximally fragile. (b) AST-based fingerprinting (hashing a canonical serialization of the parse tree) — would be more robust against formatting differences but requires defining and maintaining a canonical AST ordering for all expression types; sqlglot's `normalize`/`simplify`/`canonicalize` passes provide most of this with far less custom code. An optional extra canonicalization pass exists behind `canonicalize_queries=False` (default off) in `generate_all_query_hash_pairs`, using `sqlglot.optimizer.canonicalize`; it is also implemented but disabled by default for performance.
- **Rationale:** sqlglot provides parsing, CNF normalization, and simplification across SQL dialects as maintained library code. Normalizing to *text* (rather than an AST fingerprint) keeps query variants directly executable: the same string that is hashed is the string sent to the database to populate the cache.
- **Trade-offs / costs:** The normalization scope is bounded — semantically equivalent queries outside its reach (e.g., algebraically rewritten predicates, equivalent subquery formulations) still hash differently, producing false misses (never false hits). The library is pinned to `sqlglot>=25.0.0,<26.0.0` in `pyproject.toml` because normalization output (and therefore hashes) can change across sqlglot major versions; upgrading the pin can invalidate existing cache contents. Converting outer joins to inner-join semantics is a deliberate relaxation.
- **Code anchors:**
  - `src/partitioncache/query_processor.py::clean_query`
  - `src/partitioncache/query_processor.py::normalize_joins_to_cross_join`
- **Related docs:** [api_reference.md](api_reference.md)
- **Dissertation mapping:** _to be filled by author_

### 4.2 Conjunctive decomposition and recomposition

**Decomposition.** After cleaning, `src/partitioncache/query_processor.py::extract_and_group_query_conditions` decomposes the query into its atomic **fragments**: it parses the FROM list and the (CNF) WHERE clause, and sorts every conjunctive condition (extracted via `src/partitioncache/query_processor.py::extract_conjunctive_conditions`) into one of six categories:

- **Attribute conditions** (`attribute_conditions: dict[table_alias, list[condition]]`): single-table predicates such as `t.fare > 10`.
- **Distance conditions** (`distance_conditions: dict[(alias1, alias2), list[condition]]`): predicates referencing exactly two table aliases through a function call or a two-alias comparison (e.g., `ST_DWithin(a.geom, b.geom, 500)`, `DIST(a.g, b.g) BETWEEN 1 AND 4`). These define the edges of the table-join graph. This includes *attachment joins* — plain equalities `a.<partition_key> = b.<other_column>` joining a dimension table to a partition-key-bearing fact table (`src/partitioncache/query_processor.py::_is_partition_key_fk_join`); they are explicitly exempted from the partition-key-condition category below.
- **Partition-key joins** (`partition_key_joins`): equijoins of the form `a.<partition_key> = b.<partition_key>`, recognized by regex and tracked separately for partition-join-table detection (Section 4.4). They are *not* kept as ordinary conditions; equivalent equijoins are re-synthesized per fragment variant.
- **Partition-key conditions** (`partition_key_conditions`): predicates on the partition key itself (`IN (...)` subqueries, `BETWEEN`, comparisons), kept aside and combined combinatorially into fragment variants later.
- **OR conditions** (`or_conditions`): disjunctions, grouped by the tuple of referenced aliases; they are attached to a fragment variant only when all referenced aliases are present.
- **Other functions** (`other_functions`): remaining multi-alias or zero-alias function predicates, attached like OR conditions (and treated as connectivity-irrelevant by default; `other_functions_as_distance_conditions=True` in `generate_partial_queries` controls whether they are included in fragment variants).

**Recomposition.** `src/partitioncache/query_processor.py::generate_partial_queries` then recomposes connected subsets of these fragments into executable **fragment variants** (internally called "partial queries") by enumerating table subsets. The signature defaults are `min_component_size=1`, `follow_graph=True`, `keep_all_attributes=True`, `max_component_size=None` (treated as 15 inside the function). Subset enumeration is delegated to `src/partitioncache/query_processor.py::generate_tuples`:

- With `follow_graph=True` (default), a NetworkX graph is built whose nodes are table aliases and whose edges are the alias pairs from the distance conditions. `src/partitioncache/query_processor.py::all_connected_subgraphs` recursively enumerates all *connected* subgraphs with sizes in `[min_component_size, max_component_size]` (its inner helper `recursive_local_expand` performs the expansion). Only table subsets that are actually linked by multi-table predicates become fragment variants.
- With `follow_graph=False`, all `C(n, k)` combinations of aliases are generated regardless of connectivity.

The connectivity restriction is what tames the otherwise exponential subset space: an unconnected table subset would produce a fragment variant containing a Cartesian product with no joining predicate, which is both expensive to evaluate during cache population and useless as a cache key (its partition-key set degenerates toward "all partitions", adding no selectivity to the intersection). Restricting to connected subgraphs prunes exactly these degenerate variants while retaining every variant that contributes a meaningful restriction.

For each selected table subset, the fragment-variant query is assembled: aliases are renamed to canonical `t1..tn` (ordered by the sorted concatenation of each table's attribute conditions, so the renaming is deterministic and independent of the original alias names), attribute and distance conditions are remapped to the new aliases and sorted, partition-key equijoins are added between every pair of variant tables (unless `skip_partition_key_joins=True`, see Section 4.6, or the attachment-join restriction below applies), and the SELECT clause is built by `src/partitioncache/query_processor.py::_build_select_clause` as `SELECT DISTINCT t1.<partition_key>` (with `strip_select=True`, the default). Each fragment variant is finally re-parsed and passed through `sqlglot.optimizer.simplify.simplify` for a stable serialization. Fragment variants containing partition-key conditions (e.g., `IN` subqueries) are additionally expanded into all combinations of those conditions, and raw `INTERSECT` members of partition-key subqueries are emitted as standalone variants.

**Attachment joins (fact–dimension queries).** Data-warehouse-style queries join dimension tables through the partition key column of the fact table (`lo.lo_custkey = c.c_custkey` with partition key `lo_custkey`); the dimension side never carries the partition key column itself. When at least one such attachment join is present (and no partition-join table is detected), variant generation switches to a *pk-bearing-aware* mode, gated so that all other workloads produce byte-identical fragment variants: `src/partitioncache/query_processor.py::_detect_pk_bearing_aliases` computes the aliases that reference `alias.<partition_key>` in any condition; combinations without any pk-bearing alias are dropped (a dimension-only variant cannot produce partition keys); synthesized partition-key equijoins are added only between pk-bearing aliases (dimension tables connect through their original attachment join, which is part of the variant as an ordinary edge); the SELECT clause and remapped partition-key conditions target a pk-bearing alias. The resulting fragment variants remain relaxations of the original query, so the superset invariant of Section 1.3 is unaffected.

#### DD-02: Decompose queries into fragments and recompose connected fragment variants
- **Status:** implemented (core path)
- **Decision:** Decompose each (CNF-normalized) query into its atomic fragments, recompose the subqueries induced by connected subsets of its table-join graph — each restricted to the conjunctive conditions its tables carry — and use these fragment variants as cache keys.
- **Context:** Whole-query caching only helps on exact repeats. Real workloads repeat *sub-patterns* (the same pair of spatially related tables, the same filtered table) across many distinct queries. Caching at fragment-variant granularity lets a new query reuse cached partition-key sets of any of its sub-patterns.
- **Alternatives considered:** (a) Whole-query keys only — implemented trivially as the maximal fragment variant, but provides no partial reuse. (b) Unrestricted subset enumeration — implemented and selectable via `follow_graph=False`, but generates disconnected Cartesian-product variants. (c) Per-condition caching (size-1 fragment variants only) — subsumed by `min_component_size=1` combined with `max_component_size=1`.
- **Rationale:** Each fragment variant drops tables and conditions relative to the original query, i.e., it is a relaxation: its partition-key result set is a superset of the original query's. Therefore intersecting the cached sets of *any* subset of fragment variants is always sound — it can only over-approximate, never exclude, valid partitions. Connectivity (`follow_graph=True`) bounds the enumeration to subsets that carry real joining predicates, and `max_component_size` (default 15 inside `generate_partial_queries`) caps recursion depth.
- **Trade-offs / costs:** The number of connected subgraphs can still grow quickly for densely connected queries; `min_component_size`/`max_component_size` are the operator-facing controls. The condition classifier in `extract_and_group_query_conditions` is partly regex-based and assumes the comma-join form produced by `clean_query`; queries outside that shape (CTEs, nested subqueries in FROM) are not decomposed. Variant evaluation during cache population costs database time proportional to the number of variants.
- **Code anchors:**
  - `src/partitioncache/query_processor.py::extract_and_group_query_conditions`
  - `src/partitioncache/query_processor.py::generate_partial_queries`
  - `src/partitioncache/query_processor.py::generate_tuples`
  - `src/partitioncache/query_processor.py::all_connected_subgraphs`
- **Related docs:** [api_reference.md](api_reference.md), [complete_workflow_example.md](complete_workflow_example.md)
- **Dissertation mapping:** _to be filled by author_

### 4.3 Additional-variant generation

Fragment variants generated from the query as written only hit the cache when a future query contains the *same* sub-pattern with the *same* constants. Additional-variant generation deliberately widens the key set by **extending** fragment variants into relaxed **additional variants**. Additional variants are always **additional lookup keys, not replacements**: the exact fragment variants remain in the output set alongside the relaxed ones.

**Distance bucketing.** `src/partitioncache/query_processor.py::normalize_distance_conditions` (signature: `(original_query, bucket_steps=1.0, restrict_to_dist_functions=True)`) rewrites the numeric bounds of distance conditions onto a bucket grid of width `bucket_steps`:

- `x BETWEEN lo AND hi` → lower bound floored to the bucket grid, upper bound ceiled (e.g., `1.6–3.6` → `1–4` with `bucket_steps=1.0`, `0–4` with `bucket_steps=2.0`);
- `x < v` / `x <= v` → `v` ceiled to the next bucket boundary (kept if already on a boundary);
- `x > v` / `x >= v` → `v` floored to the bucket boundary.

All three rewrites move bounds *outward*, so the bucketed condition is a relaxation of the original — preserving the superset invariant from Section 4.2. Conditions with no numeric literal, with the literal on the wrong side, or with negative values are skipped (with warnings). With `restrict_to_dist_functions=True` only conditions matching the distance-function heuristic `src/partitioncache/query_processor.py::is_distance_function` are rewritten. A `bucket_steps <= 0` disables bucketing entirely. In `generate_all_query_hash_pairs`, variant generation runs twice — once on the cleaned query and once on its distance-normalized form — and the union of both variant sets is kept.

**Attribute-condition removal.** When `keep_all_attributes=False` is passed (the parameter is exposed as `fix_attributes` on `generate_all_hashes`), `src/partitioncache/query_processor.py::remove_k_conditions` (defaults: `max_removed=1`, `protected_patterns=None`) generates per-table variants with up to `max_removed` conditions removed (`C(n, k)` variants for `k = 1..min(max_removed, n-1)` per table), always retaining the original as the first element and always keeping at least one condition per table. `protected_patterns` is a list of case-insensitive substrings; conditions matching any pattern are never removal candidates — used to pin cheap, highly selective relational filters into every variant. `max_conditions_removed` on `generate_partial_queries`/`generate_all_query_hash_pairs` overrides `max_removed`.

**Constraint add/removal.** After fragment-variant generation, `src/partitioncache/query_processor.py::_apply_constraint_modifications` applies three operator-configurable transformations in a fixed order:

1. `remove_constraints_all: list[str]` — removes every condition referencing the listed attribute names from *all* variants (replacing them; used to generalize variants when an attribute is known to be cache-hostile). Implemented by `_remove_constraints_from_query`, which also prunes tables left without any connecting condition via `_remove_orphaned_tables` (NetworkX connectivity analysis from the anchor table).
2. `remove_constraints_add: list[str]` — like the above, but keeps the originals and *adds* the stripped variants as additional variants.
3. `add_constraints: dict[str, str]` — for variants containing a listed table, adds the configured predicate to the WHERE clause as an *additional* variant (original kept). Implemented by `_add_constraints_to_query`.

If `add_constraints` or `remove_constraints_add` produced new queries, distance bucketing is re-applied to the modified set so that constrained/stripped variants also get bucketed counterparts. The same knobs are exposed on the CLI and via environment variables (`PARTITION_CACHE_BUCKET_STEPS`, `PARTITION_CACHE_ADD_CONSTRAINTS`, `PARTITION_CACHE_REMOVE_CONSTRAINTS_ALL`, `PARTITION_CACHE_REMOVE_CONSTRAINTS_ADD`; see [cli_reference.md](cli_reference.md)).

#### DD-05: Controlled additional-variant generation (distance bucketing + constraint add/removal)
- **Status:** implemented (core path; constraint add/removal and condition removal are optional, off by default)
- **Decision:** In addition to exact fragment variants, emit systematically relaxed additional variants — distance bounds snapped outward to a bucket grid, optional per-table condition removal, and operator-configured constraint addition/removal — as extra cache keys.
- **Context:** Exact variant matching fails on workloads where queries repeat a structural pattern but vary numeric constants (especially distances in spatial/proximity queries). Without bucketing, `DIST(a,b) < 3.6` and `DIST(a,b) < 3.7` never share a cache entry.
- **Alternatives considered:** (a) Exact matching only — implemented as the degenerate case (`bucket_steps<=0`, `keep_all_attributes=True`, no constraint config). (b) Range-indexed cache lookup (storing constraints symbolically and answering containment queries) — not implemented; would require a fundamentally different key structure than content hashes.
- **Rationale:** Every additional-variant transformation is a relaxation (outward bucket snapping, condition removal), so additional-variant cache entries remain supersets of the exact query's partition set and are safe to intersect. Additional variants trade exactness of the match for hit rate: a bucketed entry computed once serves all queries whose bounds fall in the same bucket. Because they are additional keys, enabling them never reduces what the exact path would have matched.
- **Trade-offs / costs:** Each additional variant multiplies cache-population work and storage. Bucketed entries are less selective than exact ones (bounded by `bucket_steps`). `add_constraints` is the one transformation that is *not* a relaxation — it tightens variants — and is therefore only sound when the operator guarantees the constraint holds for the target workload (e.g., a schema invariant like `size = 4`); this responsibility is deliberately shifted to configuration. The bucketing parser is partially regex-based with documented robustness TODOs (flipped operands, negative values are skipped rather than handled).
- **Code anchors:**
  - `src/partitioncache/query_processor.py::normalize_distance_conditions`
  - `src/partitioncache/query_processor.py::remove_k_conditions`
  - `src/partitioncache/query_processor.py::_apply_constraint_modifications`
- **Related docs:** [cli_reference.md](cli_reference.md)
- **Dissertation mapping:** _to be filled by author_

### 4.4 Partition-join (p0) table handling

Star-shaped queries often contain one central table whose only role is to join all other tables on the partition key (historically named `p0` tables, now called *partition-join tables*). If such a table participated in subset enumeration like any other table, it would roughly double the variant count (every fragment variant with and without the hub) without adding selectivity, since it carries no filtering conditions.

`src/partitioncache/query_processor.py::detect_partition_join_table` identifies at most one such table per query through three tiers (also exposed standalone as `src/partitioncache/query_processor.py::detect_partition_join_from_query`):

1. **Explicit specification** via `partition_join_table` (matched first by alias, then by table name).
2. **Naming convention** (when `auto_detect_partition_join=True`, the default): tables whose name starts with `p0` (case-insensitive) and that carry no attribute conditions.
3. **Smart detection** (≥3 tables): a table that joins *all* other tables via the partition key and has only partition-key conditions itself.

`generate_partial_queries` then excludes the detected alias (and all distance conditions touching it) from subset enumeration and re-adds the table to **every** generated fragment variant with the canonical alias `p1`, together with `tX.<partition_key> = p1.<partition_key>` joins from each variant table and any conditions the hub itself carried; direct pairwise partition-key equijoins between the `tX` tables are removed in this case, the variant SELECT is rewritten to `SELECT DISTINCT p1.<partition_key>`, and no hub-less base variant is emitted. In spatial mode (`skip_partition_key_joins=True`), the re-added hub is connected through the original distance/spatial conditions instead of partition-key equijoins.

The terminology was migrated from "star-join" to "partition-join" (git commits `b1417f8` "Rename star_join → partition_join with backward-compatible deprecation wrappers" and `e0d9350` for documentation/CLI). Backward compatibility is preserved: `src/partitioncache/query_processor.py::handle_deprecated_kwargs` translates the legacy keyword arguments `star_join_table` and `auto_detect_star_join` to their `partition_join_*` equivalents with a `DeprecationWarning`; the callers (`generate_all_query_hash_pairs`, `generate_all_hashes`) raise a `TypeError` if both old and new names are passed.

#### DD-06: Special-case the central partition-join table
- **Status:** implemented (core path)
- **Decision:** Detect the central partition-key join table (explicitly, by `p0*` naming, or by structural "smart" detection), exclude it from subset enumeration, and re-add it to every emitted fragment variant as alias `p1`.
- **Context:** In star-schema-like queries the hub table joins everything on the partition key but contributes no filter. Treating it as an ordinary table doubles the variant space (hub present/absent per variant) and produces hub-less variants whose SELECT would have to come from an arbitrary spoke table.
- **Alternatives considered:** (a) No special handling (hub enumerated like any table) — the pre-existing behavior this replaced; selectable today by `auto_detect_partition_join=False` with no `partition_join_table` given. (b) Explicit-only configuration without auto-detection — implemented as a subset (tier 1).
- **Rationale:** Excluding the hub from enumeration and re-adding it to every variant halves the variant space while keeping all variants anchored to the partition key through a uniform `p1` alias, which also makes the variant SELECT clause deterministic (`SELECT DISTINCT p1.<partition_key>`). Three detection tiers cover explicit control, legacy naming, and convention-free schemas.
- **Trade-offs / costs:** Smart detection requires ≥3 tables and can misfire on tables that genuinely join everything yet were meant to be enumerated; the escape hatch is `auto_detect_partition_join=False` or explicit `partition_join_table`. Only one hub per query is supported (first candidate wins, logged when multiple are found). The `star_join_*` → `partition_join_*` rename keeps deprecation wrappers alive in four call sites, which is maintenance surface.
- **Code anchors:**
  - `src/partitioncache/query_processor.py::detect_partition_join_table`
  - `src/partitioncache/query_processor.py::detect_partition_join_from_query`
  - `src/partitioncache/query_processor.py::handle_deprecated_kwargs`
- **Related docs:** [p0_table_handling.md](p0_table_handling.md)
- **Dissertation mapping:** _to be filled by author_

### 4.5 Hashing

`src/partitioncache/query_processor.py::hash_query` is the entire identity function of the cache:

- The input is the query variant's **final SQL text** — i.e., the string produced after `clean_query`, variant assembly with canonical `t1..tn`/`p1` aliases, condition sorting, and the per-variant `sqlglot.optimizer.simplify.simplify` pass at the end of `generate_partial_queries`.
- The hash is **SHA-1** of the UTF-8 encoded text, returned as the full 40-character hex digest (`hashlib.sha1(query.encode()).hexdigest()`).

`generate_all_query_hash_pairs` returns `[(variant_text, hash)]` pairs; the text is what cache-population executes against the database and what the query-metadata interface (`set_query`/`get_query`) stores, while the hash is the lookup key used by all cache backends. An optional `canonicalize_queries=True` runs `sqlglot.optimizer.canonicalize.canonicalize` over each variant before hashing for additional normalization (default `False`, since variants produced by this pipeline are already in canonical form; the docstring notes the performance cost).

Hash fragility is a deliberate, one-sided design property: the normalization scope (Section 4.1 plus deterministic alias renaming and condition sorting during variant assembly) defines an equivalence class per hash. Semantically equivalent queries *outside* that scope — algebraically rewritten predicates, equivalent but structurally different subqueries, identifier-case differences not handled by sqlglot — hash differently and cause **false misses only**. A false miss costs performance (the partition restriction is not applied or is computed fresh), never correctness. False *hits* would require a SHA-1 collision between two distinct variant texts, which is not a practical concern in this non-adversarial setting (keys are generated internally, not attacker-supplied).

#### DD-04: Content-hash variant identity
- **Status:** implemented (core path)
- **Decision:** Identify each query variant by the SHA-1 hex digest of its canonical SQL text and use that digest as the cache key across all backends.
- **Context:** Cache keys must be stable across processes, backends, and time; derivable from the query alone with no central key registry; and uniform in size for use as table keys/column values in PostgreSQL, Redis, RocksDB, and DuckDB handlers.
- **Alternatives considered:** (a) Auto-increment/registry-assigned IDs — rejected: requires coordination and a lookup round-trip before every cache access. (b) Storing full variant text as the key — rejected: unbounded key size; full text is still preserved separately via the query-metadata interface. (c) Stronger hashes (SHA-256) — no security requirement justifies the longer keys; collision resistance of SHA-1 is sufficient for non-adversarial, internally generated inputs.
- **Rationale:** Content hashing makes key generation a pure function of the (normalized) query, so the population side and the lookup side compute identical keys independently. SHA-1's fixed 40-hex-character digest is compact and portable across all storage backends.
- **Trade-offs / costs:** Identity is exactly as good as the normalization in front of it (DD-03): any change to `clean_query`, variant assembly, or the pinned sqlglot version silently invalidates existing cache contents (old entries become unreachable, not wrong). Equivalent queries outside the normalization scope produce false misses. SHA-1 must not be relied upon against adversarial inputs.
- **Code anchors:**
  - `src/partitioncache/query_processor.py::hash_query`
  - `src/partitioncache/query_processor.py::generate_all_query_hash_pairs`
- **Related docs:** [api_reference.md](api_reference.md), [cache_handlers.md](cache_handlers.md)
- **Dissertation mapping:** _to be filled by author_

### 4.6 Spatial pre-processing (overview)

For spatial partition datatypes (H3, BBox) the pipeline runs in a modified mode; the full spatial design is covered in Section 9. The query-processor-level hooks are:

- **`geometry_column` parameter** (default `None` throughout `generate_partial_queries`, `generate_all_query_hash_pairs`, `generate_all_hashes`): when set, variant SELECT clauses select the geometry column instead of the partition key, and multi-alias variants are built by `src/partitioncache/query_processor.py::_build_spatial_grouped_query`, which selects each alias's geometry as a separate column so the spatial handlers can store grouped match sets.
- **`skip_partition_key_joins=True`**: suppresses the synthetic partition-key equijoins between variant tables, because spatial fragment variants are linked by distance predicates rather than key equality; the partition-join re-addition path likewise substitutes the original spatial conditions for equijoins (Section 4.4). `partition_key_source_table` (table name or alias) restricts enumeration to variants containing the table that actually carries the partition/geometry column and is auto-detected from the original SELECT clause when unset.
- **`src/partitioncache/query_processor.py::extract_distance_constraints`** parses `(alias1, alias2, distance)` triples from both `ST_DWithin(a.geom, b.geom, d)` calls and comparison-based distance expressions (`BETWEEN` upper bounds, `<`/`<=` thresholds; pure lower bounds are ignored).
- **`src/partitioncache/query_processor.py::compute_buffer_distance`** derives the buffer distance needed by spatial filters as the *weighted graph diameter* of the distance-constraint graph: nodes are aliases, edge weights are maximal pairwise distances, and the diameter (longest weighted shortest path, computed via Dijkstra over each connected component) bounds how far any matched geometry can lie from any other within one match group. For point-radius queries (a single alias with a fixed geometry), `_extract_point_radius_distances` provides the fallback maximum; with no distance constraints the function returns `0.0`.

These functions feed the spatial cache handlers and `apply_cache`'s spatial filter path described in Section 9.

## 5. Cache Application (Read Path)

The read path takes an incoming SQL query, derives the set of cache keys that could constrain it, retrieves (or references) the intersection of the cached partition-key sets, and rewrites the query so that the database only scans the partitions that can contribute results. All read-path logic lives in `src/partitioncache/apply_cache.py`; the storage-side contract it relies on is defined in `src/partitioncache/cache_handler/abstract.py`.

The read path is deliberately layered:

1. **Hash generation** — the query is decomposed and recomposed into query variants and hashed (Section 4); this step is shared with the write path so that read and write agree on cache identity.
2. **Cache lookup** — either an eager set intersection (`get_intersected`) or a lazy SQL representation of that intersection (`get_intersected_lazy`).
3. **Query extension** — the partition-key restriction is attached to the user query using one of several integration methods.

### 5.1 Orchestration

Two pairs of functions implement the read path at different abstraction levels.

**Lookup functions.** `src/partitioncache/apply_cache.py::get_partition_keys` generates all variant hashes via `generate_all_hashes` and passes them to `cache_handler.get_intersected(...)`. It returns a three-tuple: the intersected partition-key set (or `None` when nothing matched), the total number of generated variant hashes, and the number of hashes that actually hit the cache. Its counterpart `src/partitioncache/apply_cache.py::get_partition_keys_lazy` performs the same hash generation but calls `get_intersected_lazy`, returning a SQL subquery string instead of a materialized set; it raises `ValueError` ("Cache handler does not support lazy intersection") if the handler is not an `AbstractCacheHandler_Lazy` subclass. Both accept the full variant-generation parameter set (`min_component_size=2`, `canonicalize_queries=False`, `follow_graph=True`, `bucket_steps=1.0`, constraint addition/removal lists, etc.), so lookup uses exactly the same variant space as population.

**Single-call wrappers.** `src/partitioncache/apply_cache.py::apply_cache` (eager) and `src/partitioncache/apply_cache.py::apply_cache_lazy` (lazy) combine lookup, optional p0 rewrite (Section 5.3), and query extension into one call. Both return `tuple[str, dict[str, int]]`: the (possibly) enhanced query and a statistics dictionary with exactly four keys:

| Key | Meaning |
|---|---|
| `generated_variants` | Number of variant hashes generated from the input query |
| `cache_hits` | Number of those hashes found in the cache |
| `enhanced` | `1` if a cache restriction was attached, `0` if the query is returned unchanged |
| `p0_rewritten` | `1` if the p0 star-schema rewrite changed the query, `0` otherwise |

If the lookup yields no usable result (empty intersection or empty lazy subquery), the wrappers return the working query unchanged with `enhanced = 0`; callers therefore never need a separate "cache miss" code path. Both wrappers also branch into a spatial mode when `geometry_column` is set (Section 5.5). The wrappers are exported as part of the public API in `src/partitioncache/__init__.py`.

#### DD-07: Lazy in-database intersection as the primary read path
- **Status:** implemented (core path)
- **Decision:** The primary read path defers the intersection of cached partition-key sets into the target database: `get_intersected_lazy` returns a SQL subquery string that computes the intersection at execution time, and this subquery is embedded into the user query. The eager variant (`apply_cache` / `get_partition_keys`, which materializes the intersected set in Python via `get_intersected`) is retained as an alternative for backends without lazy support and for callers that need the key set itself.
- **Context:** Cached partition-key sets can be large (thousands to millions of identifiers). Materializing them client-side requires transferring the full intersection result out of the cache store, deserializing it in Python, and re-serializing it into SQL literals — three copies of data the database already holds.
- **Alternatives considered:** (a) Always-eager intersection with client-side set operations (original approach, still available); (b) server-side stored procedures that both intersect and execute the user query (rejected: couples cache logic to one DBMS and removes caller control over the final query); (c) shipping hashes to the database and intersecting inside the user query without a dedicated handler API (effectively what `get_intersected_lazy` formalizes).
- **Rationale:** When cache store and query database are the same system (the PostgreSQL backends), the lazy subquery — e.g. `SELECT unnest((<intersection SQL>)) AS <partition_key>` produced by `src/partitioncache/cache_handler/postgresql_array.py::PostgreSQLArrayCacheHandler.get_intersected_lazy` — keeps all data movement inside the database and lets the planner integrate the intersection into the overall plan. It also avoids generating very long literal `IN` lists. The capability is opt-in by type: only handlers deriving from `AbstractCacheHandler_Lazy` offer it, and `get_partition_keys_lazy` enforces this with an explicit `ValueError`.
- **Trade-offs / costs:** Lazy application requires that the cache tables be readable from the connection executing the user query, which ties the cache backend to the target database; Redis/RocksDB backends cannot participate and must use the eager path. The caller never sees the key set, so result-size-based heuristics (e.g. choosing `IN` vs. temp table based on cardinality) are not possible without an extra round trip. Eager application remains the only option for cross-system deployments.
- **Code anchors:**
  - `src/partitioncache/apply_cache.py::apply_cache_lazy`
  - `src/partitioncache/apply_cache.py::get_partition_keys_lazy`
  - `src/partitioncache/apply_cache.py::apply_cache`
  - `src/partitioncache/apply_cache.py::get_partition_keys`
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler_Lazy.get_intersected_lazy`
  - `src/partitioncache/cache_handler/postgresql_array.py::PostgreSQLArrayCacheHandler.get_intersected_lazy`
- **Related docs:** [api_reference.md](api_reference.md), [cache_handlers.md](cache_handlers.md)
- **Dissertation mapping:** _to be filled by author_

### 5.2 Integration methods

The restriction produced by the lookup is attached to the user query by `src/partitioncache/apply_cache.py::extend_query_with_partition_keys` (eager, takes a materialized key set) and `src/partitioncache/apply_cache.py::extend_query_with_partition_keys_lazy` (lazy, takes a subquery string). Both parse the query with sqlglot and — except for `TMP_TABLE_JOIN`, which injects an inner join into the `FROM`/`JOIN` list instead — append the restriction as a conjunct to the `WHERE` clause via the helper `src/partitioncache/apply_cache.py::_add_where_condition`. The target table alias is resolved by `src/partitioncache/apply_cache.py::find_p0_alias` (partition-join detection with fallback to the first table) unless `p0_alias` is passed explicitly.

**Eager methods** (`method: Literal["IN", "VALUES", "TMP_TABLE_JOIN", "TMP_TABLE_IN"] = "IN"`):

| Method | Mechanism | Typical use |
|---|---|---|
| `IN` | `alias.pk IN (v1, v2, ...)` literal list | Small key sets; zero setup cost; planner sees exact literals |
| `VALUES` | `alias.pk IN (VALUES (v1),(v2),...)` | Medium sets; some planners treat the VALUES list as a relation and can hash-join it |
| `TMP_TABLE_IN` | Temp table + `alias.pk IN (SELECT partition_key FROM tmp)` | Large sets; avoids oversized SQL text; statistics available after `ANALYZE` |
| `TMP_TABLE_JOIN` | Temp table inner-joined against the table(s) | Large sets where a join is cheaper than a semi-join; with `p0_alias=None` it joins every table in the `FROM`/`JOIN` list |

The temp-table setup (`src/partitioncache/apply_cache.py::_create_tmp_table_setup`) creates a randomly named table (`tmp_cache_keys_<n>`) with the partition key as `PRIMARY KEY` and `ON COMMIT DROP`, infers the column type via `src/partitioncache/apply_cache.py::_get_partition_key_sql_type` (`INT`; `BIGINT` when any value exceeds the 32-bit range, as with H3 cell identifiers; otherwise `TEXT`), and — when `analyze_tmp_table: bool = True` (the default) — additionally runs `ANALYZE` so the optimizer has accurate cardinality estimates for the restriction (no separate `CREATE INDEX` is emitted: the `PRIMARY KEY` already provides the B-tree, and a B-tree rather than a hash index is used deliberately for DuckDB compatibility). Temp-table methods return a multi-statement script (setup SQL prepended to the rewritten query) that **must execute in a single transaction**: `ON COMMIT DROP` reclaims the temp table at transaction end instead of letting it persist for the whole session (a leak under pooled/long-lived connections). Because the final `SELECT` is the last statement, callers read the result with `nextset()` walking rather than a plain `fetchall()` — `src/partitioncache/db_handler/postgres.py::fetch_final_result_set` (used by `PostgresDBHandler.execute`) does this. The lazy `CREATE TEMPORARY TABLE ... ON COMMIT DROP AS (<subquery>)` form and the spatial-filter temp tables follow the same `ON COMMIT DROP` + single-transaction contract.

**Lazy methods** (`method: Literal["IN_SUBQUERY", "TMP_TABLE_IN", "TMP_TABLE_JOIN"] = "IN_SUBQUERY"`): `IN_SUBQUERY` inlines the lazy intersection directly — `alias.pk IN (<lazy subquery>)` — producing a single statement and leaving the optimizer free to merge the subquery into the plan. The `TMP_TABLE_IN` and `TMP_TABLE_JOIN` variants first materialize the lazy subquery with `CREATE TEMPORARY TABLE ... AS (<subquery>)` (`src/partitioncache/apply_cache.py::_create_tmp_table_setup_from_subquery`, again with optional index and `ANALYZE`), which forces the intersection to be computed once and gives the planner a concrete row count — useful when the optimizer mis-estimates the inlined subquery or when the restriction is reused across several joins.

#### DD-08: Multiple query-integration methods instead of one fixed rewrite
- **Status:** implemented (core path)
- **Decision:** The partition-key restriction can be attached through several interchangeable methods (`IN`, `VALUES`, `TMP_TABLE_IN`, `TMP_TABLE_JOIN` eagerly; `IN_SUBQUERY`, `TMP_TABLE_IN`, `TMP_TABLE_JOIN` lazily), selected per call via the `method` parameter, rather than committing to a single rewrite shape.
- **Context:** The optimal way to inject a set restriction depends on the key-set cardinality, the DBMS planner, and the query shape. Literal `IN` lists are cheap for tens of keys but produce pathological SQL text and poor plans for millions; temp tables amortize well for large sets but add round trips and session-state requirements; inlined subqueries give the optimizer maximal freedom but risk repeated evaluation or bad estimates.
- **Alternatives considered:** (a) Always literal `IN` (simplest, fails at scale); (b) always temp table (adds setup latency and multi-statement scripts even for trivial sets); (c) automatic method selection based on measured set size (rejected for the core API: the lazy path never materializes the set, so size is unknown without an extra query; heuristics would also hide behavior from benchmark instrumentation).
- **Rationale:** Exposing the method as an explicit parameter keeps the rewrite predictable and benchmarkable — the dissertation experiments compare integration methods directly — while sane defaults (`IN` eager, `IN_SUBQUERY` lazy) cover the common case. The `analyze_tmp_table` option exists because index creation plus `ANALYZE` is what actually makes the temp-table variants win on PostgreSQL: without statistics, the planner assumes default cardinalities for temporary tables.
- **Trade-offs / costs:** The caller carries the burden of choosing well; a poor choice (e.g. literal `IN` with 10^6 keys) degrades performance rather than failing. Temp-table methods produce multi-statement SQL scripts that not every driver executes atomically — and because the temp tables use `ON COMMIT DROP`, they must be run in one transaction (splitting statements across separate autocommit transactions drops the table right after `CREATE`); callers must also walk to the final result set (`fetch_final_result_set`) instead of fetching the first. `TMP_TABLE_JOIN` changes the join graph and can interact with the optimizer differently from a `WHERE`-clause restriction.
- **Code anchors:**
  - `src/partitioncache/apply_cache.py::extend_query_with_partition_keys`
  - `src/partitioncache/apply_cache.py::extend_query_with_partition_keys_lazy`
  - `src/partitioncache/apply_cache.py::_create_tmp_table_setup`
  - `src/partitioncache/apply_cache.py::_create_tmp_table_setup_from_subquery`
  - `src/partitioncache/apply_cache.py::find_p0_alias`
- **Related docs:** [api_reference.md](api_reference.md)
- **Dissertation mapping:** _to be filled by author_

### 5.3 p0 rewrite at apply time

When `use_p0_table=True` (default `False`), `apply_cache` and `apply_cache_lazy` rewrite the working query with `src/partitioncache/apply_cache.py::rewrite_query_with_p0_table`. The rewrite converts pairwise partition-key equality joins (`t1.pk = t2.pk`) into a star schema around a central partition-list table: every table is joined to `p0` (`tN.pk = p0.pk`), where `p0` defaults to the materialized view `{partition_key}_mv` (overridable via `p0_table_name`) and the alias defaults to `"p0"`. This gives the optimizer a small, central table that enumerates all valid partition keys, improving join-order decisions for chains of partition-equijoined tables.

The ordering inside the wrappers is significant: variant hashes are generated from the **original** query first (step 1), and the p0 rewrite is applied afterwards (step 2). Cache identity is therefore based on the original query structure, and the same query addresses the same cache entries regardless of whether `use_p0_table` is enabled. When the rewrite actually changed the query, the statistics report `p0_rewritten = 1`, and the cache restriction is targeted at the p0 alias instead of a detected table alias. The rewrite is idempotent: if the materialized-view table already appears in the query, it is returned unchanged.

### 5.4 Negative caching and query status

PartitionCache records *why* a cache entry is absent or unusable, decoupled from the cached data itself, in two mechanisms defined on `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler`:

**Per-query status.** Each observed query hash has a row in the `<prefix>_queries` metadata table carrying a status. The Python-side handler DDL in `src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler` constrains the status to `('ok', 'timeout', 'failed')`, matching the contract documented on `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_query_status` and `get_query_status`. The SQL-side table setup used by the PostgreSQL queue processor (`src/partitioncache/cache_handler/postgresql_cache_handlers.sql`) extends this set with a fourth status, `'limit'`, which the in-database processor (`src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql`) assigns when a computed result reaches the configured `result_limit`; the oversized entry is removed from the cache but the query remains recorded as "known, too large". The Python queue monitor sets `'timeout'` on statement timeouts and `'failed'` on execution errors (`src/partitioncache/cli/monitor_cache_queue.py`).

**Null markers.** `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_null` stores a `NULL` value under a hash (implemented in the PostgreSQL backends as a row with `partition_keys = NULL`), and `is_null` detects it. Null-marked entries are invisible to the read path: `filter_existing_keys` in fast mode requires `partition_keys IS NOT NULL`, so they can never enter an intersection.

The consumer of status information is `exists(key, partition_key, check_query=True)` (and the set-valued `filter_existing_keys(..., check_query=True)`): with `check_query=False` (the default) only cache-entry existence is checked, which is what the read path uses; with `check_query=True` the query metadata is consulted first — no query record yields `False`, status `'ok'` additionally requires a cache entry, and status `'timeout'`/`'failed'` yields `True` *without* a cache check. The write pipeline uses this mode to treat failed and timed-out queries as already known, so they are not recomputed on every recurrence.

#### DD-09: Negative caching via per-query status decoupled from cache data
- **Status:** implemented (core path)
- **Decision:** Failure knowledge ("this variant timed out", "this variant's result exceeds the storable limit") is stored as per-query status metadata (`ok`/`timeout`/`failed`, plus `limit` in the SQL-side processor) and as `NULL` cache markers, separate from the partition-key data, and is surfaced through the `check_query` parameter of `exists` / `filter_existing_keys`.
- **Context:** Query variants are recomputed asynchronously whenever they are observed. Without negative caching, a variant that reliably times out would be re-enqueued and re-executed on every occurrence, consuming worker capacity indefinitely (a retry storm). At the same time, a timed-out variant must never contribute an (incomplete) key set to a read-path intersection.
- **Alternatives considered:** (a) No negative caching — re-execute failures every time (rejected: unbounded recomputation cost for systematically expensive variants); (b) storing an empty set for failed queries (rejected: an empty set is semantically a valid result that would zero out every intersection it joins); (c) TTL-based suppression in the queue only (rejected: loses the failure reason and does not survive queue truncation).
- **Rationale:** Keeping status in the queries metadata table separates "what we know about the query" from "what the query returned". The asymmetric semantics of `check_query` encode both requirements precisely: the population pipeline (`check_query=True`) sees failed/timed-out variants as *known* and skips them, while the read path (`check_query=False`, plus the `partition_keys IS NOT NULL` filter) sees them as *absent* and simply intersects fewer sets — correctness is preserved because intersecting fewer supersets still yields a superset of the true partition set.
- **Trade-offs / costs:** Error statuses are sticky: a variant that failed once is not retried automatically, so transient failures require explicit status reset or eviction to be recomputed. The status vocabulary differs slightly between the Python-side DDL (`ok`/`timeout`/`failed`) and the SQL-side processor DDL (additionally `limit`), which must be kept aligned when adding statuses. Per-key status checks in `filter_existing_keys(check_query=True)` execute one lookup per hash.
- **Code anchors:**
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_query_status`
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.exists`
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_null`
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.is_null`
  - `src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler.filter_existing_keys`
  - `src/partitioncache/cache_handler/postgresql_cache_handlers.sql` (status CHECK including `limit`)
- **Related docs:** [cache_handlers.md](cache_handlers.md), [postgresql_queue_processor.md](postgresql_queue_processor.md)
- **Dissertation mapping:** _to be filled by author_

### 5.5 Spatial filter application (overview)

When `geometry_column` is set, `apply_cache` and `apply_cache_lazy` switch to a spatial mode: hashes are generated with `skip_partition_key_joins=True`, cache hits are counted via `filter_existing_keys`, and instead of a partition-key restriction the query receives a geometric filter obtained from the spatial handler (`get_spatial_filter_lazy` / `get_spatial_filter`, or `get_h3_cell_filter` for pure cell-ID handlers, routed by the handler's `spatial_filter_type` attribute). If `buffer_distance` is not supplied, it is auto-derived from the query's distance constraints via `compute_buffer_distance`; a query without distance constraints raises `ValueError`.

`src/partitioncache/apply_cache.py::extend_query_with_spatial_filter_lazy` (and its WKB-literal counterpart `extend_query_with_spatial_filter`) supports four application strategies via `spatial_method`: `SUBDIVIDE_TMP_TABLE` (default — `ST_Subdivide` pieces into a temporary table with GiST index and `ANALYZE`; `subdivide_max_vertices=256`), `SUBDIVIDE_INLINE` (subdivision inside an `EXISTS` subquery, no temp table), `DUMP_TMP_TABLE` (`ST_Dump` pieces without subdivision), and `DUMP_CTE` (a `MATERIALIZED` CTE, no temp table or index). The predicates are SRID-aware: for SRID 4326 the geometry is transformed and cast to `geography` so that `ST_DWithin` distances are meter-true; for metric SRIDs the cast is skipped, keeping plain geometry predicates that can use existing GiST indexes. When the handler reports `spatial_filter_includes_buffer` (e.g. the BBox handler, whose stored filter is already buffered), `use_intersects=True` switches the predicate from `ST_DWithin` to `ST_Intersects`. The full treatment of spatial cache handlers, the H3 hybrid design, and the strategy trade-offs is given in Section 9 (see DD-22).

## 6. Cache Storage Layer

The cache storage layer (`src/partitioncache/cache_handler/`) is responsible for persisting, retrieving, and intersecting sets of partition key identifiers, keyed by query-variant hashes. All backends are instantiated through a single factory function, `src/partitioncache/cache_handler/__init__.py::get_cache_handler`, which maps a backend name string (e.g., `postgresql_array`) to a concrete handler class and its environment-derived configuration (`src/partitioncache/cache_handler/environment_config.py::EnvironmentConfigManager`). Detailed per-backend usage documentation is maintained in [cache_handlers.md](cache_handlers.md); this section documents the architecture and the design decisions behind it.

### 6.1 Handler Interface & Hierarchy

The core contract is defined by `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler`. Its abstract methods fall into five groups:

- **Read path:** `get` (one cached set per hash), `get_intersected` (intersection of multiple cached sets, returning the set plus the count of matched hashes), `exists`, `filter_existing_keys` (subset of hashes present in the cache, optionally cross-checked against query status), and `get_all_keys`.
- **Write path:** `set_cache` (store a set under a hash), `set_null` / `is_null` (explicit null markers for queries whose result could not be cached, e.g., timeouts or oversized result sets), and `delete`.
- **Query metadata:** `set_query`, `get_query`, `get_all_queries`, `set_query_status`, and `get_query_status` (status values `ok`, `timeout`, `failed`; see Section 6.3).
- **Partition and datatype management:** `register_partition_key`, `get_datatype`, `get_partition_keys`, and the class-level `get_supported_datatypes` / `supports_datatype` / `validate_datatype_compatibility`.
- **Lifecycle:** `get_instance` and `close`.

One method is deliberately *not* abstract: `AbstractCacheHandler.set_entry` is a template method that combines `set_cache` and `set_query` into a single consistent population step (existence check first, then data and metadata writes), so every backend inherits identical population semantics.

`src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler_Lazy` extends this contract for backends that can defer intersection to the storage engine itself: `get_intersected_lazy` returns a SQL representation of the intersection instead of materialized values, `set_cache_lazy` populates the cache by executing a SQL query inside the store (no values transit through Python), and `set_entry_lazy` is the corresponding non-abstract template method. The lazy read path is what `apply_cache_lazy` in the query-processing layer embeds as a subquery or temporary table (see [api_reference.md](api_reference.md)).

Between the root contract and the concrete handlers sit four per-family abstract classes that centralize connection handling, metadata storage, and the singleton lifecycle:

- `src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler` (extends `AbstractCacheHandler_Lazy`; owns the relational metadata model of Section 6.3),
- `src/partitioncache/cache_handler/redis_abstract.py::RedisAbstractCacheHandler`,
- `src/partitioncache/cache_handler/rocks_db_abstract.py::RocksDBAbstractCacheHandler`,
- `src/partitioncache/cache_handler/rocksdict_abstract.py::RocksDictAbstractCacheHandler`.

The Redis, RocksDB, and RocksDict families extend only `AbstractCacheHandler`: as external or embedded key-value stores they cannot return a SQL intersection representation that the application's database could execute, so they are non-lazy by construction.

#### DD-10: Single abstract handler contract with an optional lazy extension
- **Status:** implemented (core path)
- **Decision:** Define one abstract interface (`AbstractCacheHandler`) that every cache backend implements identically, plus a strict superset interface (`AbstractCacheHandler_Lazy`) for backends whose storage engine can compute intersections server-side and hand back a SQL representation.
- **Context:** PartitionCache must support heterogeneous stores (PostgreSQL, DuckDB, Redis, RocksDB/RocksDict) whose native capabilities differ substantially, while the query-processing layer (`apply_cache.py`) and CLI tools must remain backend-agnostic.
- **Alternatives considered:** (a) Capability flags on a single flat interface (runtime `hasattr`/feature checks); (b) separate unrelated interfaces per family; (c) lowest-common-denominator interface only (no lazy path at all).
- **Rationale:** The two-tier hierarchy keeps the common contract small and uniformly testable while making the lazy capability statically discoverable via `isinstance(handler, AbstractCacheHandler_Lazy)` — which is exactly how `helper.py::create_partitioncache_helper` selects the helper variant. Because every backend exposes byte-identical read/write/metadata semantics, the same query workload can be replayed against any backend, enabling controlled A/B comparison of storage representations — a property directly relevant for the experimental evaluation in the dissertation.
- **Trade-offs / costs:** The uniform contract forces set-of-scalars semantics onto stores with richer native types (roaring-bitmap handlers return `BitMap` objects and need `# type: ignore[override]` on `get`/`get_intersected`); non-SQL stores must implement intersection client-side; the abstract surface (~20 methods) raises the cost of adding a new backend.
- **Code anchors:**
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler`
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler_Lazy`
  - `src/partitioncache/cache_handler/helper.py::create_partitioncache_helper`
- **Related docs:** [cache_handlers.md](cache_handlers.md), [api_reference.md](api_reference.md)
- **Dissertation mapping:** _to be filled by author_

### 6.2 Backend Taxonomy & Trade-off Matrix

The factory `get_cache_handler` registers thirteen backend names (plus two backward-compatibility aliases, `redis` → `redis_set` and `rocksdb` → `rocksdb_set`):

| Backend | Storage representation | Datatypes | Lazy-capable | Locality | Notes |
|---|---|---|---|---|---|
| `postgresql_array` | Native PostgreSQL arrays, one row per hash | integer, float, text, timestamp | yes | in-DB | Intersection via custom `array_intersect_agg` aggregate; intarray `&` / `unnest`-INTERSECT fallback |
| `postgresql_bit` | Fixed-width `BIT(bitsize)` column | integer | yes | in-DB | Bitsize grows but never shrinks (`GREATEST` logic); generated `partition_keys_count` column |
| `postgresql_roaringbit` | `roaringbitmap` extension type | integer | yes | in-DB | Server-side `rb_and_agg`; compressed sparse bitmaps |
| `duckdb_bit` | Native DuckDB `BITSTRING` | integer | yes | in-process analytical DB | `bit_and` aggregate; file-based or in-memory; see [duckdb_acceleration.md](duckdb_acceleration.md) |
| `redis_set` | Redis sets | integer, text | no | external KV | Server-side `SINTER`, results materialized into Python |
| `redis_bit` | Redis bit strings | integer | no | external KV | Server-side `BITOP AND`; positions decoded client-side via `bitarray` |
| `redis_roaringbit` | Serialized `pyroaring` BitMap blobs | integer | no | external KV | Client-side bitmap AND after pipelined fetch |
| `rocksdb_set` | Embedded RocksDB, serialized sets | integer, text | no | embedded KV | Client-side Python set intersection |
| `rocksdb_bit` | Embedded RocksDB, `bitarray` values | integer | no | embedded KV | Client-side bitwise AND |
| `rocksdict` | Embedded RocksDict, natively serialized Python sets | integer, float, text, timestamp, geometry | no | embedded KV | Also supports grouped `list[frozenset[int]]` values with connected-component intersection |
| `rocksdict_roaringbit` | Embedded RocksDict, `pyroaring` BitMap blobs | integer | no | embedded KV | Client-side bitmap AND |
| `rocksdict_h3_grouped` | Grouped H3 cell-ID sets in RocksDict | geometry in practice (declares the full `rocksdict` set by inheritance) | no | embedded KV + PostgreSQL for population | Spatial handler — see Section 9 |
| `postgis_bbox` | PostGIS geometry (bounding-box grid) | geometry (spatial) | yes | in-DB | Spatial handler — see Section 9 |

Two orthogonal axes structure this design space.

**Representation axis.** Native arrays (`postgresql_array`) are the only representation supporting all scalar datatypes and serve as the reference implementation. Fixed-width bit strings (`postgresql_bit`, `duckdb_bit`, `redis_bit`, `rocksdb_bit`) encode integer identifiers as bit positions, making intersection a bitwise AND with size proportional to the configured bitsize rather than to set cardinality — efficient for dense integer domains, wasteful for sparse ones, and constrained to a fixed value range that can be grown but not shrunk. Roaring bitmaps (`postgresql_roaringbit`, `redis_roaringbit`, `rocksdict_roaringbit`) remove the fixed-size constraint via compressed, dynamically sized bitmaps, targeting sparse integer domains. Plain sets (`redis_set`, `rocksdb_set`, `rocksdict`) trade compactness for datatype generality. Spatial representations are covered in Section 9.

**Locality axis.** In-DB backends (PostgreSQL family, DuckDB) co-locate the cache with a SQL engine, which is the precondition for the lazy path: the intersection can be expressed as SQL and — when the cache lives in the *same* database as the application data — embedded directly into the user's query, so cached identifiers never leave the database. External and embedded key-value stores must always materialize the intersected set into Python and re-serialize it into the rewritten query. Note that server-side intersection and lazy capability are distinct properties: Redis intersects server-side (`SINTER`, `BITOP AND`) but is still non-lazy, because the result cannot be referenced from the application's SQL query.

#### DD-11: Multiple interchangeable storage representations as an experimental matrix
- **Status:** implemented (core path: `postgresql_array`; remaining backends optional)
- **Decision:** Implement the same cache contract over a matrix of storage representations (array, fixed-width bit, roaring bitmap, set, spatial) crossed with storage localities (in-DB PostgreSQL/DuckDB, external Redis, embedded RocksDB/RocksDict), rather than committing to a single representation.
- **Context:** The optimal representation depends on partition-key datatype, domain density, cardinality, and deployment topology; these trade-offs are an object of study, not a settled input.
- **Alternatives considered:** (a) A single PostgreSQL-array backend only; (b) representation chosen automatically per partition at runtime; (c) pluggable third-party cache libraries instead of purpose-built handlers.
- **Rationale:** Because all backends sit behind DD-10's uniform contract, identical workloads can be executed against each representation and compared directly. The representations span deliberate extremes: arrays support every datatype; fixed-width bit arrays are densest for bounded integer domains but have a fixed bitsize that the bootstrap logic only allows to grow (`GREATEST(bitsize, new)` in `postgresql_cache_handlers.sql`), never shrink; roaring bitmaps handle sparse, unbounded integer domains dynamically; sets impose no encoding at all. Lazy intersection exists exactly where the store offers server-side SQL aggregation (PostgreSQL `array_intersect_agg` / bit AND / `rb_and_agg`, DuckDB `bit_and` over `BITSTRING`).
- **Trade-offs / costs:** Thirteen backends multiply test surface and CI cost; bit backends are integer-only and require bitsize provisioning; roaring backends add extension (`roaringbitmap`) or library (`pyroaring`) dependencies; RocksDB requires conda-forge packaging. Feature asymmetry (lazy vs. non-lazy) must be handled by every caller via the helper facade.
- **Code anchors:**
  - `src/partitioncache/cache_handler/__init__.py::get_cache_handler`
  - `src/partitioncache/cache_handler/postgresql_array.py::PostgreSQLArrayCacheHandler`
  - `src/partitioncache/cache_handler/postgresql_bit.py::PostgreSQLBitCacheHandler`
  - `src/partitioncache/cache_handler/postgresql_roaringbit.py::PostgreSQLRoaringBitCacheHandler`
  - `src/partitioncache/cache_handler/duckdb_bit.py::DuckDBBitCacheHandler`
- **Related docs:** [cache_handlers.md](cache_handlers.md), [duckdb_acceleration.md](duckdb_acceleration.md), [datatype_support.md](datatype_support.md)
- **Dissertation mapping:** _to be filled by author_

### 6.3 Metadata Model

The PostgreSQL family persists three kinds of objects per table prefix (created in `postgresql_abstract.py::PostgreSQLAbstractCacheHandler._recreate_metadata_table` and the SQL bootstrap function `partitioncache_bootstrap_partition` in `src/partitioncache/cache_handler/postgresql_cache_handlers.sql`):

1. **`{prefix}_partition_metadata`** — one row per partition key with its declared `datatype` (CHECK-constrained to the handler's supported set) and creation timestamp. The bit handler extends this table with a `bitsize` column whose updates pass through the grow-only `GREATEST` logic.
2. **`{prefix}_queries`** — one row per `(query_hash, partition_key)` holding the full normalized query text, a `status` column (`CHECK (status IN ('ok', 'timeout', 'failed'))` in the Python DDL; the SQL bootstrap additionally allows `'limit'`, see Section 5.4), and a `last_seen` timestamp refreshed on every re-observation.
3. **`{prefix}_cache_{partition_key}`** — one cache table per partition key, keyed by `query_hash`, with a representation-specific `partition_keys` column (array, `BIT(n)`, `roaringbitmap`, or geometry). A NULL `partition_keys` value is the null marker written by `set_null`.

Key-value backends mirror this model with namespaced keys: `_partition_metadata:{partition_key}` for datatype (and bitsize) records and `cache:{partition_key}:{hash}` for entries, in both `redis_abstract.py` and `rocksdict_abstract.py`.

Storing query text and status *beside* the cache entries makes the cache self-describing: `pcache-manage` copy/export/import operations transfer query metadata together with cache data (via `get_all_queries` / `set_query` in `src/partitioncache/cli/manage_cache.py`); `exists(..., check_query=True)` can report a hash as "handled" even when no value is stored, because a `timeout`/`failed` status documents *why*; and `last_seen` enables age-based pruning via `postgresql_abstract.py::PostgreSQLAbstractCacheHandler.prune_old_queries` as well as the eviction strategies described in [cache_eviction.md](cache_eviction.md).

#### DD-12: Metadata-first design — query text and status stored beside cache entries
- **Status:** implemented (core path)
- **Decision:** Every cache population writes not only the identifier set but also the originating query text, a processing status, and a `last_seen` timestamp; the high-level `set_entry` / `set_entry_lazy` template methods make this pairing the default population path.
- **Context:** A hash-keyed cache without metadata is opaque: entries cannot be audited, migrated between backends, or distinguished between "never computed" and "computed but failed/timed out".
- **Alternatives considered:** (a) Hash-only cache with external bookkeeping in the queue system; (b) optional metadata enabled per deployment; (c) storing only a status flag without the query text.
- **Rationale:** Co-located metadata makes the cache portable (export/import/copy reproduce a complete cache including provenance), debuggable (any hash can be resolved back to its query), prunable (`last_seen`-based aging), and negative-result-aware (`timeout`/`failed` statuses plus `set_null` markers prevent repeated recomputation of known-bad variants — `exists(check_query=True)` short-circuits on error statuses).
- **Trade-offs / costs:** Every population performs an additional metadata write (two statements instead of one); query text storage grows with workload size; the `(query_hash, partition_key)` metadata is duplicated across backends that do not share the relational schema (Redis/RocksDict re-implement it on key namespaces).
- **Code anchors:**
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_entry`
  - `src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler._recreate_metadata_table`
  - `src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler.prune_old_queries`
- **Related docs:** [cache_eviction.md](cache_eviction.md), [manage_cache_cli.md](manage_cache_cli.md)
- **Dissertation mapping:** _to be filled by author_

### 6.4 Helper Facade

`src/partitioncache/cache_handler/helper.py::PartitionCacheHelper` wraps a generic handler together with one fixed `partition_key` and its datatype, removing the repetitive `partition_key=` argument from every call and adding defensive error handling (all facade methods catch exceptions and return neutral values). Its subclass `helper.py::LazyPartitionCacheHelper` additionally exposes `get_intersected_lazy`. The polymorphic factory `helper.py::create_partitioncache_helper` selects the lazy variant whenever the wrapped handler is an `AbstractCacheHandler_Lazy` instance; the public API entry point `src/partitioncache/__init__.py::create_cache_helper` combines backend construction (singleton mode) with helper creation in one call.

Validation happens eagerly at construction time: the helper reads the partition key's stored datatype from backend metadata, validates a requested datatype against the handler class's supported set (`validate_datatype_compatibility`), registers unknown partition keys (`register_partition_key`, forwarding kwargs such as `bitsize`), and raises `ValueError("Datatype mismatch: ...")` if the requested datatype conflicts with the stored one (see Section 8).

#### DD-15: Partition-bound helper facade over the generic handler interface
- **Status:** implemented (core path)
- **Decision:** Expose a partition-scoped facade (`PartitionCacheHelper` / `LazyPartitionCacheHelper`) created by a polymorphic factory, instead of having application code call the generic multi-partition handler interface directly.
- **Context:** The raw handler interface threads `partition_key` through every call and defers datatype errors to the first write; application code and the CLI tools need a misuse-resistant, fail-fast surface.
- **Alternatives considered:** (a) Direct handler usage with the partition key passed per call; (b) binding the partition key into the handler constructor itself (precluding multi-partition use of one connection); (c) `functools.partial`-style binding without validation.
- **Rationale:** The facade performs datatype registration and mismatch detection once, at construction, so configuration errors surface before any query is processed; it preserves the handler's multi-partition capability (several helpers can share one singleton handler); and the factory's `isinstance` dispatch transparently upgrades to the lazy API where available, so callers need no backend-specific branching.
- **Trade-offs / costs:** One more indirection layer; the facade's broad exception swallowing (log-and-return-default) favors robustness over fail-loud semantics, which can mask backend faults in experiments unless logs are monitored; datatype validation is postponed when neither caller nor metadata specifies a datatype.
- **Code anchors:**
  - `src/partitioncache/cache_handler/helper.py::PartitionCacheHelper`
  - `src/partitioncache/cache_handler/helper.py::LazyPartitionCacheHelper`
  - `src/partitioncache/cache_handler/helper.py::create_partitioncache_helper`
  - `src/partitioncache/__init__.py::create_cache_helper`
- **Related docs:** [api_reference.md](api_reference.md)
- **Dissertation mapping:** _to be filled by author_

### 6.5 Concurrency & Lifecycle

Each handler family implements a class-level singleton with reference counting. `PostgreSQLAbstractCacheHandler.get_instance` returns the shared `_instance` and increments `_refcount`; `close` decrements and only tears down the connection when the count reaches zero. The PostgreSQL variant additionally bypasses the singleton entirely when `threading.active_count() > 1`, returning a fresh connection per call, because psycopg connections are not safely shareable across threads. `RocksDBAbstractCacheHandler.get_instance` uses double-checked locking with a class `_lock` (RocksDB permits only one writer process per database directory); `RedisAbstractCacheHandler` and `RocksDictAbstractCacheHandler` follow the same `_instance`/`_refcount` pattern. The helper facade cooperates with this lifecycle: its context-manager `__exit__` skips `close()` for singleton-managed handlers.

Datatype lookups are cached per handler *instance* (`self._cached_datatype = {}` in `PostgreSQLAbstractCacheHandler.__init__`) rather than per class: different table prefixes may reuse the same partition key name with different datatypes, so a class-level cache would leak state across logically separate caches (a defect class previously observed in integration tests).

## 7. Queue System & Asynchronous Cache Population

PartitionCache populates its cache asynchronously: queries observed by an application are pushed to a queue and processed in the background, so cache misses never add latency to the foreground request. The queue subsystem consists of a provider-agnostic module-level API (`src/partitioncache/queue.py`), pluggable queue handlers selected by a factory (`src/partitioncache/queue_handler/__init__.py::get_queue_handler`, driven by `QUERY_QUEUE_PROVIDER`), and two alternative consumers: an external Python observer process (`pcache-monitor`) and a database-native pg_cron stored-procedure pipeline (`pcache-postgresql-queue-processor`). Operational detail is documented in [queue_system.md](queue_system.md) and [postgresql_queue_processor.md](postgresql_queue_processor.md); this section records the architecture and the decisions behind it.

### 7.1 Two-Tier Queue Architecture

The queue is split into two tiers with distinct payloads:

1. **Original query queue** (`{PG_QUEUE_TABLE_PREFIX}_original_query_queue`, default prefix `partitioncache_queue`): whole user queries awaiting decomposition/recomposition into query variants. Rows carry `(query, partition_key, partition_datatype, priority)` and are deduplicated by a `UNIQUE(query, partition_key)` constraint.
2. **Query fragment queue** (`{PG_QUEUE_TABLE_PREFIX}_query_fragment_queue`): pre-hashed query fragments awaiting execution against the source database. Rows carry `(query, hash, partition_key, partition_datatype, cache_backend, priority)` and are deduplicated by a `UNIQUE(hash, partition_key)` constraint — the same fragment hash submitted via different original queries occupies a single queue row.

Both table schemas, including the UNIQUE constraints, are created in `src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler` (`_initialize_tables`). On a duplicate push, the `ON CONFLICT` path increments the row's `priority` instead of inserting a second row, so frequently re-submitted work rises in the dequeue order rather than bloating the queue.

The module-level API in `src/partitioncache/queue.py` wraps a lazily created singleton handler and exposes, per tier: `push_to_original_query_queue`, `push_to_query_variant_queue` (preferred name; alias of the original `push_to_query_fragment_queue`), `pop_from_original_query_queue`, `pop_from_query_fragment_queue`, blocking variants (`pop_from_original_query_queue_blocking`, `pop_from_query_fragment_queue_blocking`), plus `get_queue_lengths`, `clear_original_query_queue`, `clear_query_fragment_queue`, and `clear_all_queues`. Applications interact only with this API; the provider is selected at runtime.

The decomposition step between the two tiers is performed by `src/partitioncache/query_processor.py::generate_all_query_hash_pairs` (Section 4), executed either by the observer process or by `pcache-add` before pushing directly to the variant queue.

#### DD-16: Two-tier queue separating decomposition from execution
- **Status:** implemented (core path)
- **Decision:** Use two separate queues — one for whole original queries awaiting decomposition, one for pre-hashed query variants awaiting execution — instead of a single work queue.
- **Context:** Producing cache entries involves two very different workloads: (a) parsing and normalizing SQL and generating fragment variants, which is CPU-bound Python (sqlglot-based), and (b) executing fragments against the source database and storing the resulting partition-key sets, which is database-bound and potentially long-running.
- **Alternatives considered:** A single queue holding original queries, with decomposition and execution fused in one consumer step; pushing variants synchronously from the application without an original-query tier.
- **Rationale:** Separating the tiers lets decomposition and execution scale independently and lets fragments deduplicate *across* original queries: two different user queries that share a conjunctive subquery produce one fragment row (enforced by `UNIQUE(hash, partition_key)`), so the expensive source-database execution happens once. Each tier has its own priority ordering, and duplicate submissions translate into priority increments rather than repeated work. The fragment tier also allows bypassing decomposition entirely (`pcache-add --queue` vs. `--queue-original`, and `push_to_query_variant_queue` / `push_to_query_fragment_queue` for pre-computed pairs).
- **Trade-offs / costs:** Two tables/keys to operate and monitor; an original query is not atomically linked to its variants (a crash between decomposition and variant push loses that query's variants until re-submission); end-to-end latency includes two queue hops.
- **Code anchors:**
  - `src/partitioncache/queue.py::push_to_original_query_queue`
  - `src/partitioncache/queue.py::push_to_query_fragment_queue`
  - `src/partitioncache/queue_handler/abstract.py::AbstractQueueHandler`
  - `src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler`
  - `src/partitioncache/cli/monitor_cache_queue.py::query_fragment_processor`
- **Related docs:** [queue_system.md](queue_system.md)
- **Dissertation mapping:** _to be filled by author_

### 7.2 Queue Providers

Two providers implement the queue contract defined in `src/partitioncache/queue_handler/abstract.py`:

**PostgreSQL provider** (`src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler`, the default). It extends `src/partitioncache/queue_handler/abstract.py::AbstractPriorityQueueHandler` and supports priorities end to end:

- *Priority dequeue.* `pop_from_*_queue` selects `ORDER BY priority DESC, created_at ASC ... LIMIT 1 FOR UPDATE SKIP LOCKED` inside a transaction and deletes the row on commit. `SKIP LOCKED` makes concurrent consumers skip rows another worker holds, giving lock-free work distribution without advisory locks.
- *Blocking pop without polling.* Table triggers (`trigger_notify_original_query_insert/_update`, `trigger_notify_query_fragment_insert/_update`) call `pg_notify` on the channels `original_query_available` and `query_fragment_available`. `pop_from_original_query_queue_blocking` and `pop_from_query_fragment_queue_blocking` open a dedicated `LISTEN` connection and wait on the connection's file descriptor via `select.select`, with a bounded safety re-check (≤ 5 s per iteration) as fallback if notifications are missed.
- *Non-blocking producers.* Push operations call the SQL helpers `src/partitioncache/queue_handler/postgresql_queue_helper.sql::non_blocking_original_queue_upsert` and `::non_blocking_fragment_queue_batch_upsert` (which in turn invokes the per-row `::non_blocking_fragment_queue_upsert`). These use `FOR UPDATE NOWAIT` so a producer never blocks behind a row that a consumer is currently processing; conflicts are reported as `skipped_locked`/`skipped_concurrent` instead of stalling. A plain `INSERT ... ON CONFLICT DO UPDATE` fallback exists if the functions are not deployed.

**Redis provider** (`src/partitioncache/queue_handler/redis.py::RedisQueueHandler`). It extends the non-priority `AbstractQueueHandler`: queues are plain Redis lists (`RPUSH` to append, `BLPOP` to pop) holding JSON payloads. Ordering is strictly FIFO; there is no priority support and no deduplication (no equivalent of the UNIQUE constraints). Blocking pops map directly to `BLPOP` with a timeout.

#### DD-17: Database-backed queue with LISTEN/NOTIFY rather than a dedicated message broker
- **Status:** implemented (core path)
- **Decision:** Implement the default queue as ordinary PostgreSQL tables with `FOR UPDATE SKIP LOCKED` dequeue and LISTEN/NOTIFY-based blocking pops, rather than depending on a dedicated message broker.
- **Context:** PartitionCache deployments already require a PostgreSQL database for the source data and (typically) the cache tables. The queue's consumers write into cache tables in the same database family.
- **Alternatives considered:** Redis lists (implemented as the second provider — simpler, distributed, but FIFO-only, no dedup, no priority); external brokers such as RabbitMQ or Kafka (not implemented).
- **Rationale:** Keeping the queue in PostgreSQL provides transactional consistency between dequeue and cache writes (a variant row is deleted in the same transactional context that processes it), exact-once-style deduplication via UNIQUE constraints, priority scheduling via a simple indexed `ORDER BY`, and zero additional infrastructure to install, secure, and monitor. LISTEN/NOTIFY gives broker-like blocking-consumer semantics without busy polling; `SKIP LOCKED` gives safe multi-consumer concurrency. The Redis provider remains available where a PostgreSQL queue database is undesirable.
- **Trade-offs / costs:** Queue throughput is bounded by PostgreSQL row-level operations (each pop is a transaction with DELETE), which is slower than an in-memory broker; NOTIFY payload delivery is best-effort within a connection's lifetime, hence the periodic safety re-check in the blocking loop; the Redis alternative loses priorities and deduplication, so the two providers are not feature-equivalent.
- **Code anchors:**
  - `src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler`
  - `src/partitioncache/queue_handler/redis.py::RedisQueueHandler`
  - `src/partitioncache/queue_handler/__init__.py::get_queue_handler`
  - `src/partitioncache/queue_handler/postgresql_queue_helper.sql::non_blocking_fragment_queue_batch_upsert`
- **Related docs:** [queue_system.md](queue_system.md)
- **Dissertation mapping:** _to be filled by author_

### 7.3 Database-Native Processing (pg_cron Stored-Procedure Pipeline)

For PostgreSQL-family cache backends, the variant queue can be consumed entirely inside the database, without any external worker process. The pipeline is set up by the `pcache-postgresql-queue-processor` CLI (subcommands: `setup`, `remove`, `enable`, `disable`, `update-config`, `status`, `status-detailed`, `queue-info`, `logs`, `check-permissions`, `manual-process`, `verify`) and consists of:

- **Configuration table with trigger-based job sync.** `src/partitioncache/queue_handler/postgresql_queue_processor_cron.sql::partitioncache_initialize_cron_config_table` creates `{queue_prefix}_processor_config` in the pg_cron database, holding per-job settings: `enabled`, `max_parallel_jobs` (default 5, capped at 20), `frequency_seconds`, `timeout_seconds`, `table_prefix`, `queue_prefix`, `cache_backend`, `target_database`, `result_limit`, `default_bitsize`, `job_owner`, and the resulting pg_cron `job_ids`. The trigger function `::partitioncache_sync_cron_job` (attached via `::partitioncache_create_cron_config_trigger`) reacts to INSERT/UPDATE/DELETE on this table: it unschedules old jobs and schedules `max_parallel_jobs` pg_cron jobs (`{job_name}_1 .. _N`) — so configuration changes are applied by ordinary SQL updates, and the config table is the single source of truth. Job names follow `partitioncache_process_queue_<database>[_<suffix>]` (`::partitioncache_construct_job_name`, mirrored in Python by `src/partitioncache/cli/postgresql_queue_processor.py::construct_processor_job_name`).
- **Cross-database execution.** Jobs are scheduled with `cron.schedule_in_database(...)`, so pg_cron may live in a dedicated database while the work executes in the target/cache database; see [pg_cron_cross_database_setup.md](pg_cron_cross_database_setup.md). Statement timeouts are enforced by setting `statement_timeout` on the job-owner role.
- **Dispatcher.** Each pg_cron tick runs `src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql::partitioncache_run_single_job_with_params`. It dequeues one variant with a single statement: `DELETE ... WHERE id = (SELECT ... WHERE NOT EXISTS (already in the queries table) ORDER BY priority DESC, id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING ...`. The `NOT EXISTS` predicate skips variants whose hash is already cached; `SKIP LOCKED` lets the N parallel jobs dequeue without contention. If the queue yields nothing, idle time is used to bulk-remove already-cached queue rows via `::partitioncache_cleanup_cached_queue_items`.
- **Active-jobs guard.** Before executing, the dispatcher inserts into `{queue_prefix}_active_jobs`, whose primary key `(query_hash, partition_key)` makes a concurrent execution of the same variant a unique-violation, which is logged as `skipped`. This prevents two parallel cron jobs from computing the same cache entry simultaneously.
- **Execution and logging.** `::_partitioncache_execute_job` runs the variant against the target database, performs datatype detection and partition bootstrap if needed, and writes the resulting partition keys into the backend-specific cache table. Every attempt is recorded in `{queue_prefix}_processor_log` with status `started`/`success`/`failed`/`timeout`/`skipped`, rows affected, and execution time; log retention is handled by `::partitioncache_cleanup_processor_logs` (scheduled by `postgresql_queue_processor_cron.sql::partitioncache_schedule_processor_log_cleanup`). Status introspection is provided by `src/partitioncache/queue_handler/postgresql_queue_processor_cron_status.sql::partitioncache_get_processor_status` and `::partitioncache_get_processor_status_detailed`.
- **Manual processing.** `::partitioncache_manual_process_queue` processes up to N items synchronously without pg_cron; integration tests use this path to avoid cron-job concurrency in CI.

The direct processor is restricted to backends whose cache tables live in the same PostgreSQL instance: `src/partitioncache/cli/postgresql_queue_processor.py::DIRECT_PROCESSOR_BACKENDS` maps exactly `postgresql_array`, `postgresql_bit`, `postgresql_roaringbit`, and `postgis_bbox` to their SQL backend keys and table-prefix environment variables; any other `CACHE_BACKEND` is rejected for this mode.

#### DD-18: Database-native queue processing via pg_cron stored procedures
- **Status:** implemented (core path)
- **Decision:** Provide a fully database-resident consumer for the variant queue — pg_cron jobs invoking PL/pgSQL functions — as the recommended production processing mode for PostgreSQL-family cache backends.
- **Context:** An external observer process must be deployed, supervised, restarted, and given network access to both queue and cache databases. For deployments where queue, cache, and source data are all PostgreSQL, this external component is the only non-database moving part.
- **Alternatives considered:** The external observer process `src/partitioncache/cli/monitor_cache_queue.py` (also implemented; remains required for non-PostgreSQL cache backends and for the Redis queue provider, see Section 7.4); an external job scheduler (cron + psql scripts) without in-database state.
- **Rationale:** With pg_cron, there is no external process to operate: scheduling, dequeue, execution, locking, and logging all live in the database, survive host restarts, and are configured through a single SQL table whose trigger synchronizes the pg_cron schedule. Dequeue is transactional (`DELETE ... FOR UPDATE SKIP LOCKED`), already-cached variants are skipped at dequeue time (`NOT EXISTS` against the queries table), and the active-jobs primary key serializes work per variant across the `max_parallel_jobs` concurrent jobs.
- **Trade-offs / costs:** Restricted to PostgreSQL-family backends (`DIRECT_PROCESSOR_BACKENDS`); requires the pg_cron extension and superuser-assisted setup (permission checks are automated in the CLI); variant execution consumes resources of the database server itself rather than a separate worker host; PL/pgSQL duplication of logic that also exists in the Python observer must be kept consistent; pg_cron's scheduling granularity bounds reaction latency to roughly `frequency_seconds`.
- **Code anchors:**
  - `src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql::partitioncache_run_single_job_with_params`
  - `src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql::_partitioncache_execute_job`
  - `src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql::partitioncache_manual_process_queue`
  - `src/partitioncache/queue_handler/postgresql_queue_processor_cron.sql::partitioncache_sync_cron_job`
  - `src/partitioncache/queue_handler/postgresql_queue_processor_cron.sql::partitioncache_initialize_cron_config_table`
  - `src/partitioncache/cli/postgresql_queue_processor.py::DIRECT_PROCESSOR_BACKENDS`
- **Related docs:** [postgresql_queue_processor.md](postgresql_queue_processor.md), [pg_cron_cross_database_setup.md](pg_cron_cross_database_setup.md)
- **Dissertation mapping:** _to be filled by author_

### 7.4 External Observer Alternative (`pcache-monitor`)

`src/partitioncache/cli/monitor_cache_queue.py` implements the process-based consumer, exposed as the `pcache-monitor` entry point. Its architecture has two stages mirroring the two queue tiers:

- A dedicated decomposition thread, `::query_fragment_processor`, blocks on the original query queue, decomposes each query with `generate_all_query_hash_pairs` (applying variant-generation options such as bucket steps and constraint addition/removal passed via CLI arguments), and pushes the resulting fragment/hash pairs to the fragment queue.
- An execution stage, `::fragment_executor`, runs a `concurrent.futures.ThreadPoolExecutor` with `--max-processes` workers. It pops fragments — using the provider's blocking pop with adaptive timeouts when capacity is available — and submits each to `::run_and_store_query`, which executes the fragment against the source database via the `db_handler` abstraction and stores the resulting partition keys through the configured cache handler. Optional features include cache-aware fragment optimization before execution (`::apply_cache_optimization`, which consults existing cache entries to restrict fragment search spaces) and a DuckDB-based query accelerator (Section 10).

The observer is **required** whenever the database-native processor cannot be used: Redis-, RocksDB-, and DuckDB-backed caches (any backend outside `DIRECT_PROCESSOR_BACKENDS`), the Redis queue provider, or environments where pg_cron cannot be installed. It is also useful in development because it runs anywhere Python runs. Its costs are the inverse of DD-18: a long-running process to supervise, separate database connections per worker thread, and at-least-once semantics across process crashes (a variant popped but not yet stored is lost from the queue, though dedup-by-hash means re-submission is cheap).

### 7.5 Cache Eviction

Cache growth is bounded by an optional, likewise database-native eviction manager, set up by the `pcache-postgresql-eviction-manager` CLI (subcommands: `setup`, `remove`, `enable`, `disable`, `update-config`, `status`, `logs`, `manual-run`, `verify`); see [cache_eviction.md](cache_eviction.md) for operations. The design mirrors the queue processor:

- A config table `{table_prefix}_eviction_config` in the pg_cron database (`src/partitioncache/cache_handler/postgresql_cache_eviction_cron.sql::partitioncache_initialize_eviction_cron_config_table`) stores `enabled`, `frequency_minutes` (default 60), `strategy`, `threshold` (default 1000), `target_database`, `job_owner`, and the pg_cron `job_id`. A trigger (`::partitioncache_sync_eviction_cron_job`) synchronizes a `cron.schedule_in_database` job on every config change.
- The scheduled job runs `src/partitioncache/cache_handler/postgresql_cache_eviction_cache.sql::partitioncache_run_eviction_job_with_params` in the cache database, which iterates all partitions from the metadata table and applies the configured strategy per partition, writing outcomes to `{table_prefix}_eviction_log` (cleanup via `::partitioncache_cleanup_eviction_logs`).
- Exactly two strategies are implemented (enforced by a `CHECK (strategy IN ('oldest','largest'))` constraint and the CLI `--strategy` choices): `oldest` (`::_partitioncache_evict_oldest_from_partition`) removes the entries with the oldest `last_seen` timestamp once a partition's entry count exceeds `threshold` — an LRU-style policy using the queries-table access metadata; `largest` (`::_partitioncache_evict_largest_from_partition`) removes the entries with the highest `partition_keys_count`, i.e., the least selective cached results, and requires a count column in the cache table. Both delete from the cache table and the queries metadata table together.

The threshold is an entry-count per partition, not a byte size; time-based expiry is achieved indirectly through the `oldest` strategy combined with `last_seen` maintenance.

#### DD-19: Database-native TTL/size-based eviction
- **Status:** implemented (optional)
- **Decision:** Implement cache eviction as a pg_cron-scheduled, per-partition stored-procedure job with count-threshold triggers and two strategies (`oldest`, `largest`), configured through a trigger-synchronized SQL config table.
- **Context:** Cached partition-key sets accumulate indefinitely as new query variants are processed; PostgreSQL-backed caches have no built-in expiry (unlike Redis TTLs). Cache entries differ widely in usefulness: stale entries are never re-read, and very large (unselective) entries provide little pruning benefit per byte.
- **Alternatives considered:** Relying on backend-native expiry (only available for Redis backends); eviction inside the application/observer process (would re-introduce an external dependency for otherwise database-only deployments); byte-size-based thresholds (harder to compute uniformly across array/bit/roaring representations).
- **Rationale:** Reusing the pg_cron + config-table + sync-trigger pattern from DD-18 keeps eviction operable with SQL alone and consistent with the processor's operational model (status, logs, manual-run, verify). The `oldest` strategy approximates LRU using the `last_seen` column of the queries metadata table; the `largest` strategy targets the entries whose removal frees the most storage while sacrificing the least pruning selectivity. Per-partition thresholds keep multi-partition setups balanced.
- **Trade-offs / costs:** Count-based thresholds only indirectly bound storage size; `largest` depends on a `*_count` column and fails gracefully (logged) where absent; eviction and cache writes are not coordinated, so an entry can be evicted immediately after creation under pressure; only PostgreSQL-family cache backends are covered — Redis/RocksDB backends must rely on their own mechanisms.
- **Code anchors:**
  - `src/partitioncache/cache_handler/postgresql_cache_eviction_cache.sql::partitioncache_run_eviction_job_with_params`
  - `src/partitioncache/cache_handler/postgresql_cache_eviction_cache.sql::_partitioncache_evict_oldest_from_partition`
  - `src/partitioncache/cache_handler/postgresql_cache_eviction_cache.sql::_partitioncache_evict_largest_from_partition`
  - `src/partitioncache/cache_handler/postgresql_cache_eviction_cron.sql::partitioncache_sync_eviction_cron_job`
  - `src/partitioncache/cli/postgresql_cache_eviction.py::handle_setup`
- **Related docs:** [cache_eviction.md](cache_eviction.md), [pg_cron_cross_database_setup.md](pg_cron_cross_database_setup.md)
- **Dissertation mapping:** _to be filled by author_

## 8. Multi-Partition & Datatype System

PartitionCache supports multiple partition keys with different datatypes *simultaneously* on a single backend instance. Each partition key receives its own storage namespace — a dedicated table `{prefix}_cache_{partition_key}` in the PostgreSQL family, or key namespaces `cache:{partition_key}:{hash}` in the Redis/RocksDict families — and one registry entry recording its datatype (`{prefix}_partition_metadata` row, or `_partition_metadata:{partition_key}` key). This allows, for example, an integer `zipcode` partition and a text `city_name` partition to coexist under one cache configuration.

**Canonical datatypes.** The system defines four scalar datatypes — `integer`, `float`, `text`, `timestamp` — plus the spatial `geometry` datatype (Section 9). `src/partitioncache/cache_handler/datatype_utils.py` provides the bidirectional mapping between Python types and datatype strings (`PYTHON_TYPE_TO_DATATYPE`: `int` → `integer`, `float` → `float`, `str` → `text`, `datetime` → `timestamp`) via `datatype_utils.py::get_datatype_from_settype` and `datatype_utils.py::get_python_type_from_datatype`.

**Registration and validation flow.**

1. `create_cache_helper(cache_type, partition_key, datatype)` builds the backend handler and the partition-bound helper.
2. On helper construction (`helper.py::PartitionCacheHelper.__init__`), the stored datatype is fetched via `get_datatype(partition_key)`.
   - If the caller supplied a datatype, it is first checked against the handler class's capability set (`AbstractCacheHandler.validate_datatype_compatibility`, backed by each handler's `get_supported_datatypes`).
   - If the partition key is unknown, it is registered via `register_partition_key(partition_key, datatype, **kwargs)`; backend implementations re-validate against their supported set and create the partition storage (e.g., `PostgreSQLAbstractCacheHandler.register_partition_key` → `_ensure_partition_table`; the bit handler resolves the `bitsize` kwarg against its default and applies the grow-only rule).
   - If the partition key already exists with a *different* datatype, construction fails immediately with `ValueError("Datatype mismatch: ...")` — misconfiguration is detected before any query is processed.
   - If no datatype was supplied and none is stored, validation is explicitly postponed (logged), and the datatype is later inferred on first write: `PostgreSQLArrayCacheHandler.set_cache` samples the incoming identifier set, derives the datatype, registers the partition, and rejects identifiers that contradict an existing registration.

**Datatype support matrix** (derived from the `get_supported_datatypes` implementation of each handler; see [datatype_support.md](datatype_support.md) for the user-facing version):

| Backend | integer | float | text | timestamp | geometry |
|---|---|---|---|---|---|
| `postgresql_array` | yes | yes | yes | yes | no |
| `postgresql_bit` | yes | no | no | no | no |
| `postgresql_roaringbit` | yes | no | no | no | no |
| `duckdb_bit` | yes | no | no | no | no |
| `redis_set` | yes | no | yes | no | no |
| `redis_bit` | yes | no | no | no | no |
| `redis_roaringbit` | yes | no | no | no | no |
| `rocksdb_set` | yes | no | yes | no | no |
| `rocksdb_bit` | yes | no | no | no | no |
| `rocksdict` | yes | yes | yes | yes | yes |
| `rocksdict_roaringbit` | yes | no | no | no | no |
| `rocksdict_h3_grouped` | (inherits `rocksdict`; used for geometry) | | | | yes |
| `postgis_bbox` | no | no | no | no | yes |

The pattern is systematic: bit-based and roaring-bitmap representations are integer-only because they encode identifiers as bit positions; set-based external stores support integer and text (string-serializable values with unambiguous round-tripping); only the array representation and the natively serializing `rocksdict` support the full scalar matrix; `geometry` is confined to the spatial handlers and `rocksdict` (which stores arbitrary picklable sets). Per-partition metadata CHECK constraints (PostgreSQL) and registration-time validation (all families) enforce these limits at write time rather than at read time.

#### DD-13: Per-backend datatype restrictions enforced at registration time
- **Status:** implemented (core path)
- **Decision:** Each handler class declares its supported datatype set (`get_supported_datatypes`); compatibility is validated when a partition key is registered or a helper is constructed, never silently coerced at read or write time.
- **Context:** The storage representations constrain what is encodable: bit positions can only represent non-negative integers; Redis/RocksDB sets need unambiguous string round-tripping; PostGIS columns hold geometries. Accepting any datatype on any backend would defer failures to obscure runtime errors deep in the storage layer.
- **Alternatives considered:** (a) Transparent coercion (e.g., hashing text keys to integers for bit backends) — rejected: lossy/collision-prone and irreversible, which would corrupt intersection semantics; (b) a uniform lowest-common-denominator datatype (integer everywhere) — rejected: would forfeit the array backend's generality, which several workloads (text `landkreis`, timestamp partitions) rely on.
- **Rationale:** Declaring capabilities per class makes the support matrix explicit and testable; fail-fast validation at registration (`validate_datatype_compatibility`, helper-construction mismatch check) plus database-level CHECK constraints (e.g., `CHECK (datatype = 'integer')` in the bit handler's metadata DDL) catch misconfiguration at setup time instead of corrupting caches at runtime.
- **Trade-offs / costs:** Operators must consult the support matrix when choosing a backend; switching a partition to a different datatype requires deleting and re-registering it; datatype inference on first write exists only where sampling is possible (array handler) and is deferred otherwise.
- **Code anchors:**
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.validate_datatype_compatibility`
  - `src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.get_supported_datatypes`
  - `src/partitioncache/cache_handler/datatype_utils.py::get_datatype_from_settype`
  - `src/partitioncache/cache_handler/postgresql_bit.py::PostgreSQLBitCacheHandler` (integer-only CHECK constraint)
- **Related docs:** [datatype_support.md](datatype_support.md), [cache_handlers.md](cache_handlers.md)
- **Dissertation mapping:** _to be filled by author_

## 9. Spatial Extension (experimental)

> **Status: experimental.** The spatial extension is a research subsystem built for the dissertation's spatial proof-of-concept workloads. Interfaces, storage formats, and application strategies have been revised repeatedly based on benchmark findings (see 9.5 and 9.6), and earlier designs (grid-cell BBox storage, the PostGIS H3 handler) have already been removed or replaced. None of it is part of the stable core path.

### 9.1 Motivation

The proof-of-concept's primary workload consists of multi-table spatial search queries: conjunctions of per-table attribute predicates connected by distance constraints (`ST_DWithin`) — e.g., "an ice-cream parlor within 300 m of a pharmacy and within 400 m of a supermarket". The core PartitionCache mechanism caches *sets of partition-key values* per query variant and intersects them. For spatial data this is a poor fit for two reasons:

1. **Partition keys do not capture spatial locality.** Administrative keys (zip code, district) quantize space coarsely and arbitrarily; a match pair straddling a zip-code boundary is invisible to per-key intersection.
2. **Distance constraints are not equi-join conditions.** The variant-generation machinery assumes the tables of a variant are connected via partition-key equality; spatial variants are connected via `ST_DWithin`, so the standard key-set intersection semantics ("same partition for all variants") does not directly apply.

The spatial extension therefore changes *what* is cached: instead of partition-key sets, it caches **geometric descriptions of each variant's result locations** (raw geometry collections, or H3 cell sets). At query time, the cached per-variant geometries are intersected (with a buffer derived from the query's distance constraints) to produce a **search-space restriction geometry** that is injected into the query as an additional spatial predicate. Variant generation for spatial datatypes selects geometry columns rather than partition keys (`src/partitioncache/query_processor.py::clean_query` with `geometry_column` set) and skips partition-key equijoin generation (`skip_partition_key_joins=True`), as wired up in the spatial branch of `src/partitioncache/apply_cache.py::apply_cache_lazy`.

Both spatial handlers share an abstract base, `src/partitioncache/cache_handler/postgis_spatial_abstract.py::PostGISSpatialAbstractCacheHandler`, which registers the unified `"geometry"` datatype, verifies the PostGIS extension, and declares the spatial-filter contract: `get_spatial_filter()` returns `tuple[bytes, int] | None` (WKB bytes plus SRID) for the non-lazy path, `get_spatial_filter_lazy()` returns a SQL expression string for the lazy path, and two properties control routing in `apply_cache.py`: `spatial_filter_type` (`"geometry"` vs. cell-ID-based) and `spatial_filter_includes_buffer` (whether the caller must use `ST_Intersects` instead of `ST_DWithin` to avoid double-buffering).

The buffer distance for intersection is either supplied explicitly or derived automatically by `src/partitioncache/query_processor.py::compute_buffer_distance`, which builds a weighted graph of the query's distance constraints (nodes = table aliases, edges = `ST_DWithin` distances) and returns the weighted graph diameter — the maximum distance over which two matched objects can be separated and still satisfy the conjunction. If no distance constraint is found and no buffer is given, `apply_cache_lazy` raises a `ValueError` rather than silently producing an unsound filter.

### 9.2 PostGIS BBox handler

`src/partitioncache/cache_handler/postgis_bbox.py::PostGISBBoxCacheHandler` stores, per variant hash, a single PostGIS geometry collection built by `ST_Collect(DISTINCT geom)` over the variant query's result geometries (`set_cache_lazy`). Despite the historical name "BBox", the current implementation stores **raw collected geometries** (typically MultiPoint for POI data) — *not* envelopes or grid cells. The earlier design snapped geometries to a fixed grid (`ST_SnapToGrid` + envelope cells, controlled by a `cell_size` parameter); that parameter is now deprecated with a `DeprecationWarning` and ignored ("cell_size parameter is deprecated and no longer used. Raw geometries are now stored directly..."). Multi-alias variants arrive with per-alias geometry columns (`geom_1`, `geom_2`, …, produced by `src/partitioncache/query_processor.py::_build_spatial_grouped_query`) and are flattened via `CROSS JOIN LATERAL (VALUES ...)` before collection.

The cache table has a `partition_keys geometry` column with a GiST index (`CREATE INDEX ... USING GIST (partition_keys)` in `_ensure_partition_table`). Intersection is geometric: `_get_intersected_sql` chains `ST_Intersection(A, B)` over the stored collections; the filter-producing form `_get_buffered_intersected_sql` buffers each variant's collection by `buffer_distance` *before* intersecting, yielding the "co-occurrence zone" — the area within `buffer_distance` of all variants simultaneously. Because the buffer is baked in, `spatial_filter_includes_buffer` returns `True`, and `apply_cache.py` consequently applies the filter with `ST_Intersects` rather than `ST_DWithin` (preventing the double-buffering bug documented in [plans/spatial_bbox_optimization_analysis.md](plans/spatial_bbox_optimization_analysis.md), section 5c). `get_spatial_filter()` executes this chain server-side and returns `(WKB bytes, srid)`; `get_spatial_filter_lazy()` returns the SQL expression for inline embedding.

#### DD-20: Cache bounding geometries (PostGIS BBox handler) instead of partition-key sets
- **Status:** experimental
- **Decision:** For spatial workloads, cache each variant's result *geometry collection* (`ST_Collect` of raw geometries) in a PostGIS table with a GiST index, and answer multi-variant lookups by buffered geometric intersection (`ST_Buffer` per variant, then chained `ST_Intersection`), returning the result as a query-restriction geometry (WKB+SRID or SQL expression).
- **Context:** Partition-key sets cannot express spatial co-occurrence across key boundaries (9.1). The PoC needed a cache representation whose intersection semantics match `ST_DWithin`-connected conjunctions.
- **Alternatives considered:** (a) Grid-cell envelopes (original implementation: `ST_SnapToGrid` + 500 m envelope cells via `cell_size`) — replaced because raw collection is simpler, faster to populate, and more precise; (b) envelope-level intersection (`ST_Envelope` per variant) — rejected because envelopes of nationally scattered variant results cover ~100 % of the data extent (see 9.5); (c) H3 cell sets (DD-21) — pursued in parallel as a discrete alternative.
- **Rationale:** Buffered intersection of the actual geometry collections produces the tightest sound search-space description (e.g., 80.8 km² actual area for a two-variant query whose envelope spans 263,638 km²; [plans/spatial_bbox_optimization_analysis.md](plans/spatial_bbox_optimization_analysis.md)). Sound supersets are preserved: every true result location lies within `buffer_distance` of every variant's geometry, hence inside the intersection of the buffered collections.
- **Trade-offs / costs:** `ST_Buffer` on thousands of points and chained `ST_Intersection` of large collections are expensive at apply time (the dominant cost in the 9.6 measurements); the GiST index on the *cache table* is largely irrelevant because lookups are by `query_hash` — the GiST value lies in the *target* table's index, which is only exploitable after subdivision (DD-22). The non-lazy path (WKB serialization + hex literal embedding) is substantially slower than the lazy path and is kept primarily for evaluation ([plans/2026-03-06-spatial-filter-optimization-design.md](plans/2026-03-06-spatial-filter-optimization-design.md), overhead source 1).
- **Code anchors:**
  - `src/partitioncache/cache_handler/postgis_bbox.py::PostGISBBoxCacheHandler`
  - `src/partitioncache/cache_handler/postgis_bbox.py::PostGISBBoxCacheHandler._get_buffered_intersected_sql`
  - `src/partitioncache/cache_handler/postgis_spatial_abstract.py::PostGISSpatialAbstractCacheHandler`
- **Related docs:** [plans/spatial_bbox_optimization_analysis.md](plans/spatial_bbox_optimization_analysis.md), [plans/2026-03-06-spatial-filter-optimization-design.md](plans/2026-03-06-spatial-filter-optimization-design.md)
- **Dissertation mapping:** _to be filled by author_

### 9.3 H3 grouped handler

`src/partitioncache/cache_handler/rocksdict_h3_grouped.py::RocksDictH3GroupedCacheHandler` is a thin spatial subclass of the RocksDict handler. Per variant hash, it stores a **list of match groups** — `list[frozenset[int]]`, where each frozenset holds the H3 cells (as int64, resolution configurable, default 9) of one matching row/row-combination of that variant. The storage and intersection logic lives polymorphically in `src/partitioncache/cache_handler/rocks_dict.py::RocksDictCacheHandler`: `set_cache` accepts `list[frozenset[int]]` (registering the `"geometry"` datatype), and `get_intersected` detects the grouped format and dispatches to `rocks_dict.py::_grouped_intersection`.

`_grouped_intersection` implements **connected-component intersection** via union-find (path compression + union by rank): two groups from different variants are connected if they share at least one cell; only components whose member groups **span all variants** survive (`len(frag_indices) == num_fragments`), and the result is the union of cells of surviving components. This is the discrete analogue of the buffered geometric intersection: a group represents "a match exists in these cells", and a co-occurrence requires evidence from every variant in a connected neighborhood.

Spatial buffering is realized as **k-ring expansion**: `rocks_dict.py::_grouped_kring_intersection` expands every cell with `grid_disk(cell, k)` (Python `h3` library), merges per variant, then intersects across variants. `get_h3_cell_filter` computes `k = ceil(buffer_distance / average_hexagon_edge_length(resolution))` and returns the surviving cell-ID set; the handler accordingly reports `spatial_filter_type = "h3_cell_ids"` and `spatial_filter_includes_buffer = True`.

PostgreSQL with the h3-pg extension is used **only at population time**: `RocksDictH3GroupedCacheHandler.geom_to_h3_cell` converts variant-result geometries (WKB/hex EWKB) to cell IDs via `h3_lat_lng_to_cell(ST_Centroid(...)::point, res)::bigint`, applying `ST_Transform(..., 4326)` first when the data SRID is not 4326 (h3-pg expects WGS84). At query/filter time no PostGIS is required. Multi-alias variants are generated with one geometry column per alias (`query_processor.py::_build_spatial_grouped_query`) so each alias's geometry can be converted independently and grouped per row.

#### DD-21: H3-grouped cell caching with connected-component intersection
- **Status:** experimental
- **Decision:** Cache spatial variant results as grouped H3 cell sets (`list[frozenset[int]]`, int64 cells) in RocksDict; intersect variants via union-find connected components that must span all variants; approximate the distance buffer by k-ring (`grid_disk`) expansion computed from `buffer_distance` and the hexagon edge length; expose the result as a cell-ID set (`spatial_filter_type = "h3_cell_ids"`).
- **Context:** A discrete, index-friendly alternative to geometric intersection (DD-20): cell IDs are compact integers, set operations are cheap in Python, and the apply-time predicate can be a B-tree-indexable cell membership test instead of geometry evaluation.
- **Alternatives considered:** (a) Flat cell sets per variant with plain set intersection — insufficient, because matches of different variants generally lie in *different* cells within the buffer distance, so flat intersection misses valid co-occurrences; the grouped/k-ring formulation fixes this. (b) The earlier **PostGIS H3 handler** (`postgis_h3.py`, cells in `BIGINT[]` columns intersected via SQL `INTERSECT`/`h3_grid_disk` and a hybrid geometry-reconstruction path) — removed in commit "Replace PostGIS H3 handler with RocksDict H3 grouped match sets" after benchmarks (9.5, 9.6) showed its filter either lacked selectivity or duplicated BBox's geometry path. (c) Storing per-pair distances instead of cells — not pursued (storage blow-up).
- **Rationale:** Grouped match sets preserve which cells belong to the *same* match, allowing the intersection to require co-occurrence evidence from every variant without geometric computation. K-ring expansion gives a sound (superset) buffer approximation. RocksDict storage avoids any PostGIS dependency at read time.
- **Trade-offs / costs:** The filter is a superset whose looseness grows with resolution coarseness and k (cell count grows quadratically with k for `grid_disk`); resolution 9 (~174 m hex edge, per [plans/2026-03-06-spatial-filter-optimization-design.md](plans/2026-03-06-spatial-filter-optimization-design.md)) is too coarse for 200–500 m buffers, so selectivity is limited. Effective apply-time use requires pre-computed cell IDs on the fact table (DD-23); the inline per-row `h3_lat_lng_to_cell` predicate cannot use existing GiST indexes. Population requires a PostgreSQL round-trip per geometry (`geom_to_h3_cell`).
- **Code anchors:**
  - `src/partitioncache/cache_handler/rocksdict_h3_grouped.py::RocksDictH3GroupedCacheHandler`
  - `src/partitioncache/cache_handler/rocksdict_h3_grouped.py::RocksDictH3GroupedCacheHandler.get_h3_cell_filter`
  - `src/partitioncache/cache_handler/rocks_dict.py::_grouped_intersection`
  - `src/partitioncache/cache_handler/rocks_dict.py::_grouped_kring_intersection`
  - `src/partitioncache/query_processor.py::_build_spatial_grouped_query`
- **Related docs:** [plans/2026-03-06-spatial-filter-optimization-design.md](plans/2026-03-06-spatial-filter-optimization-design.md), [plans/spatial_bbox_optimization_analysis.md](plans/spatial_bbox_optimization_analysis.md)
- **Dissertation mapping:** _to be filled by author_

### 9.4 Spatial filter application strategies

Once a filter geometry (or cell set) is obtained, it must be attached to the user query so PostgreSQL can exploit it. This is the responsibility of `src/partitioncache/apply_cache.py::extend_query_with_spatial_filter_lazy` (SQL-expression filter) and its non-lazy counterpart `apply_cache.py::extend_query_with_spatial_filter` (pre-computed WKB literal). Both support four `spatial_method` strategies (`Literal["SUBDIVIDE_INLINE", "SUBDIVIDE_TMP_TABLE", "DUMP_TMP_TABLE", "DUMP_CTE"]`):

- **`SUBDIVIDE_TMP_TABLE`** (default): materializes `ST_Subdivide((ST_Dump(filter)).geom, max_vertices)` into a temporary table, builds a GiST index on it, runs `ANALYZE`, and adds `EXISTS (SELECT 1 FROM tmp WHERE ST_Intersects/ST_DWithin(alias.geom, tmp.geom[, dist]))` to the WHERE clause. Subdivision breaks the (often nationally scattered) filter collection into many pieces with *tight bounding boxes*, which is what makes the target table's GiST index selective (see 9.5).
- **`SUBDIVIDE_INLINE`**: same subdivision, but as a set-returning function in an `EXISTS` subquery — no temp table, no index.
- **`DUMP_TMP_TABLE`**: `ST_Dump` into an indexed temp table without subdivision (preserves original pieces).
- **`DUMP_CTE`**: `ST_Dump` inside a `WITH ... AS MATERIALIZED` CTE — no temp table or index; the simplest deployment (single statement).

`subdivide_max_vertices` (default 256) tunes the piece granularity. The temp-table methods emit multi-statement SQL (`DROP TABLE IF EXISTS ...; CREATE TEMPORARY TABLE ... ON COMMIT DROP AS ...;` prefix with a randomized table name), which callers must execute as a script **in a single transaction** so `ON COMMIT DROP` reclaims the table at transaction end (reading the final result via `nextset()` walking — see `db_handler/postgres.py::fetch_final_result_set`).

**SRID handling** encodes a piece of design history: when `srid == 4326`, the geometry is cast to `geography` (`ST_Transform(alias.geom, 4326)::geography`) so `ST_DWithin` distances are in meters; for metric SRIDs (e.g., 25832 UTM) the cast is *skipped* and plain geometry operators are used. Two reasons, both discovered during PoC debugging: (1) the `::geography` cast prevents use of the geometry GiST index, and (2) large 4326 polygons cast to geography get great-circle edges that bow inward, yielding incorrect `ST_DWithin` results near polygon edges — in a metric SRID, edges are straight lines in projected space. The earlier geography-cast-everywhere behavior was measured at 238 ms vs. 0.77 ms (309×) for a simple point-radius query after the fix ([plans/spatial_bbox_optimization_analysis.md](plans/spatial_bbox_optimization_analysis.md), section 4).

For cell-ID filters, `apply_cache.py::extend_query_with_h3_cell_filter_lazy` / `extend_query_with_h3_cell_filter` create an indexed temp table of allowed cells and add `h3_lat_lng_to_cell(centroid, res)::bigint IN (SELECT cell FROM tmp)`; routing between geometry, `h3_cell`, and `h3_cell_ids` filters happens in the spatial branch of `apply_cache.py::apply_cache_lazy` (and `apply_cache` for the non-lazy path) based on the handler's `spatial_filter_type`.

#### DD-22: Subdivided-geometry filters to exploit GiST indexes
- **Status:** experimental
- **Decision:** Before injecting a cached filter geometry into a query, decompose it with `ST_Dump` + `ST_Subdivide` (bounded vertex count per piece) into a GiST-indexed temporary table, and express the filter as an `EXISTS` probe against that table; offer non-subdividing variants (`DUMP_TMP_TABLE`, `DUMP_CTE`) and an inline variant for comparison. Skip the `::geography` cast for metric SRIDs.
- **Context:** GiST indexes operate on bounding boxes. A filter built from nationally scattered variant results has a near-country-sized envelope even when its actual area is tiny (80.8 km² actual vs. 263,638 km² envelope, 73.8 % of Germany, for the q5 example), so embedding it directly returned 3,957,164 of 5,162,579 rows (76.7 %) from the GiST index and forced row-level geometry evaluation — measured at 22,970 ms vs. a 263 ms baseline (87× slower) before this decision ([plans/spatial_bbox_optimization_analysis.md](plans/spatial_bbox_optimization_analysis.md)).
- **Alternatives considered:** (a) Direct embedding of the filter geometry ("DIRECT" in the method-comparison artifact) — catastrophic for BBox (up to 2653.2977 s vs. 0.1994 s baseline on spatial_q10; three queries hit the statement timeout); (b) envelope intersection of variants — rejected, produces ~95–97 % of-country filters; (c) skipping the spatial filter when variant envelopes exceed a coverage threshold, and regional cache partitioning — proposed in the analysis doc, not implemented.
- **Rationale:** Subdivision restructures one pathological geometry into many small pieces with tight boxes; the analysis measured ~13 ms for `ST_Subdivide`-based matching of a 636-cell variant vs. 969 ms for envelope-based filtering. A temp table with its own GiST index plus `ANALYZE` additionally gives the planner usable cardinalities.
- **Trade-offs / costs:** Multi-statement SQL output complicates integration (not a single SELECT); per-query temp-table churn; subdivision cost is paid at every application; benchmarks (9.6) show that even subdivided BBox filters often do not pay off when the per-piece `ST_Buffer`/`ST_Intersection` chain that *produces* the filter is itself the bottleneck.
- **Code anchors:**
  - `src/partitioncache/apply_cache.py::extend_query_with_spatial_filter_lazy`
  - `src/partitioncache/apply_cache.py::extend_query_with_spatial_filter`
  - `src/partitioncache/query_processor.py::compute_buffer_distance`
- **Related docs:** [plans/spatial_bbox_optimization_analysis.md](plans/spatial_bbox_optimization_analysis.md) (sections 4, 5d), [plans/2026-03-06-spatial-filter-optimization-design.md](plans/2026-03-06-spatial-filter-optimization-design.md)
- **Dissertation mapping:** _to be filled by author_

### 9.5 Design history and rejected approaches

The plan documents under `docs/plans/` record the iteration history; the rejected designs are dissertation-relevant:

1. **Grid-cell BBox storage (rejected).** The original BBox handler snapped result geometries to a 500 m grid and stored `MultiPolygon` envelope cells. The refactor to raw `ST_Collect` geometries improved population speed but initially *worsened* query time (buffering 3,333 individual points is costlier than buffering 3,333 rectangles); the lesson recorded in [plans/spatial_bbox_optimization_analysis.md](plans/spatial_bbox_optimization_analysis.md) is that the storage format mattered far less than the intersection/application strategy.
2. **Envelope intersection (rejected).** Intersecting variant `ST_Envelope`s produced filters covering 95–97 % of Germany ("restaurant ∩ hotel envelopes (buffered): 97.3 % of Germany"); cell-area vs. envelope-area ratios differed by 25–125,000×. This motivated cell-level/raw-geometry intersection and, at apply time, subdivision (DD-22).
3. **`ST_Intersection` of huge collections (mitigated).** Direct `ST_Intersection` of nationwide multi-geometry collections timed out for several queries (BBox rows marked `ERROR (timeout)` in the 2026-03-01 benchmark table). The interim mitigation was envelope reduction before intersection; the current design instead buffers raw collections and relies on subdivision at apply time. The estimated cost of naïve application was "1094 points × 66,295 rows = 72.5 billion point comparisons" for one query.
4. **Pure H3 cell-ID membership filtering via PostGIS (rejected, handler removed).** The former `postgis_h3.py` handler filtered with `h3_lat_lng_to_cell(...) IN cells` directly. The 7-query comparison in [plans/2026-03-06-spatial-filter-optimization-design.md](plans/2026-03-06-spatial-filter-optimization-design.md) measured BBox lazy at **41.3×** (q_a1_double_like: 19.4 s → 0.47 s) and **38.6×** (q_d1) while H3 lazy reached at best **1.6×** and was ≤1.0× on four of seven queries. Recorded causes: the per-row H3 predicate gets no GiST leverage (a functional B-tree index, 52 MB for 5.16 M rows, brought "no measurable speedup because H3 filter selectivity at resolution 9 is too coarse"); k-ring expansion for 300–500 m buffers at ~174 m hex edge "covers too large an area" — expansion area grows quadratically in k, so buffers destroy selectivity; and the resolution paradox — comparable selectivity "would need resolution 12–14", which multiplies cell counts. A batch-lookup rewrite (single `GROUP BY disk_cell HAVING COUNT(DISTINCT query_hash) = N` instead of N `INTERSECT` subqueries) yielded only ~3 % because `h3_grid_disk` expansion itself was the bottleneck. The handler was subsequently replaced by the RocksDict grouped handler (DD-21); its hybrid variant (cells stored compactly, geometry reconstructed via `h3_cell_to_boundary` → `ST_Collect` → buffer for GiST filtering) was removed together with it, since it converged on the BBox geometry path.
5. **Pre-computed cell lookup as the surviving H3 strategy.** Because the inline per-row predicate was the weak point, the current design moves cell computation off the query path: a materialized view or a physical column of pre-computed cell IDs (DD-23), set up by `pcache-manage setup h3-cells` (`src/partitioncache/cli/manage_cache.py::setup_h3_cells`, default view name `{table}_h3_cells`).

#### DD-23: H3 cell lookup modes (inline / materialized view / precomputed column) for cell-membership benchmarking
- **Status:** experimental
- **Decision:** Implement three interchangeable apply-time modes for H3 cell-ID filters in `apply_cache.py::extend_query_with_h3_cell_lookup`: `"inline"` (compute `h3_lat_lng_to_cell` per row), `"mv"` (JOIN against a materialized view of pre-computed `(id, h3_cell_id)` pairs), and `"column"` (filter a pre-computed cell-ID column on the fact table); select the mode via the handler's `h3_cell_mode` (env `H3_CELL_MODE`). With `filter_all_tables=True` (default), the cell filter is applied to *every* alias of the same base table, so the planner gets cardinality estimates for all sides of cross-joins.
- **Context:** The rejected pure-inline H3 filtering (9.5, item 4) left open whether pre-computation could make cell membership competitive; a dedicated benchmark (`examples/benchmark/benchmark_h3_cell_lookup.py`, partition keys `bench_h3_inline` / `bench_h3_mv` / `bench_h3_col`) compares all three modes against eight BBox application variants and the baseline.
- **Alternatives considered:** Only-inline filtering (rejected per 9.5); only-MV (rejected as a hard schema dependency — inline remains the zero-setup fallback and the default).
- **Rationale:** Pre-computed cells convert the spatial filter into an integer B-tree/hash lookup, eliminating per-row H3 computation and transform costs; the measured results (9.6) confirm that the H3 modes win precisely on the queries where geometry-based filtering does not.
- **Trade-offs / costs:** `mv` and `column` modes require schema preparation (`pcache-manage setup h3-cells`) and maintenance on data change; H3 filters return supersets (the benchmark explicitly tracks `superset_size` and counts supersets as matches); resolution remains a global tuning parameter.
- **Code anchors:**
  - `src/partitioncache/apply_cache.py::extend_query_with_h3_cell_lookup`
  - `src/partitioncache/cli/manage_cache.py::setup_h3_cells`
  - `examples/benchmark/benchmark_h3_cell_lookup.py` (modes wired via `setup_h3_handler("inline"|"mv"|"column", ...)`)
- **Related docs:** `examples/benchmark/README.md`, [plans/2026-03-06-spatial-filter-optimization-design.md](plans/2026-03-06-spatial-filter-optimization-design.md)
- **Dissertation mapping:** _to be filled by author_

### 9.6 Measured outcomes

Two result artifacts are checked into the repository. Numbers below are quoted from these files (speedups from the JSON artifact rounded to two decimals).

**`examples/benchmark/spatial_method_comparison_results.txt`** (OSM POI benchmark, `postgresql_array` cache metadata, lazy API, 10 spatial queries, backends `spatial_h3 (postgis_h3)` — the since-removed handler — and `spatial_bbox (postgis_bbox)`, methods SUBDIVIDE_TMP_TABLE / SUBDIVIDE_INLINE / DIRECT):

| Backend / method | Avg speedup | All correct |
|---|---|---|
| postgis_h3 / SUBDIVIDE_TMP_TABLE | 1.16x | No |
| postgis_h3 / SUBDIVIDE_INLINE | 1.01x | No |
| postgis_h3 / DIRECT | 56.68x | No |
| postgis_bbox / SUBDIVIDE_TMP_TABLE | 0.25x | No |
| postgis_bbox / SUBDIVIDE_INLINE | 0.30x | No |
| postgis_bbox / DIRECT | 0.01x | No |

The H3/DIRECT outliers (spatial_q6: 404.24x, 2.1831 s → 0.0054 s; spatial_q3: 74.62x; spatial_q2: 57.01x) come with `match: False` — the filter dropped valid results (e.g., q6 returned 1 row vs. 47 baseline rows), so these speedups are not usable as-is and document a correctness failure of that configuration, not a win. BBox was correct on most queries (`match: True`) but slow (spatial_q2: 0.4650 s baseline vs. 41.0429 s cached under SUBDIVIDE_TMP_TABLE; spatial_q10 under DIRECT: 0.1994 s vs. 2653.2977 s; DIRECT q1–q3 hit statement timeouts).

**[plans/2026-03-06-spatial-filter-optimization-design.md](plans/2026-03-06-spatial-filter-optimization-design.md)** (7 hard-to-index queries, Germany-wide 5.16 M POIs): BBox lazy reached 41.3x (q_a1_double_like, 19.4 s → 0.47 s), 38.6x (q_d1_expression_filter), 4.9x (q_b2_self_join), 2.3x (q_b1_high_card); it lost on q_a2_regex (0.9x), q_b3_triple (0.5x), and q_c1_chain (0.6x). H3 lazy peaked at 1.6x. Conclusion recorded there: PartitionCache's spatial path "excels on queries with selective non-spatial filters … + moderate spatial joins" and "hurts on queries that already produce many results or have many cache fragments".

**`examples/benchmark/h3_cell_lookup_results.json`** (9 queries; baseline vs. 8 BBox application variants vs. H3 inline/MV/column; SRID 25832, resolution 9): the H3 cell-lookup modes invert the earlier BBox-vs-H3 picture on cell-friendly queries — spatial_q3: h3_inline 14.20x, h3_col 14.11x, h3_mv 10.46x, vs. best BBox variant 1.30x; spatial_q1: h3_col 4.18x, h3_inline 4.15x vs. BBox 0.71–0.78x; spatial_q2: h3_inline 5.19x (h3_mv 2.80x) vs. BBox ≤1.57x. Conversely, on the long-running cross-product query bench_qd1 (baseline 12.384 s) BBox wins (bbox_dump_tmp 19.98x, bbox_tmp256 19.97x) while the best H3 mode (h3_col) reaches 2.24x (h3_inline 0.94x). On bench_qc1 all eight BBox variants produced result mismatches (`match: false`) while all three H3 modes were correct (though ≤0.71x). Overall: neither representation dominates — notably, the zero-setup inline mode is the top H3 performer on the cell-friendly queries; selectivity of the cached variants and query shape decide the winner, which is the central open finding of the spatial extension.

## 10. Experimental: DuckDB Query Accelerator

> **Status: experimental.** The accelerator is an optional, opt-in component of `pcache-monitor` only. It does not affect cache lookups or query rewriting; it changes only *where variant queries are executed during cache population*. Its performance claims in [duckdb_acceleration.md](duckdb_acceleration.md) (5–50× for analytical queries) are documentation estimates; no benchmark artifact for the accelerator is checked into the repository.

**Purpose.** Cache population executes many SELECT variant queries against the production PostgreSQL database. For analytical variants over moderately sized, mostly static tables, this load can be offloaded to an embedded, in-process DuckDB instance holding a replica of the relevant tables, exploiting DuckDB's vectorized engine and avoiding load on the operational database.

**Architecture** (`src/partitioncache/query_accelerator.py::DuckDBQueryAccelerator`):

- *Initialization*: `initialize()` opens a DuckDB database (default persistent file `/tmp/partitioncache_accel.duckdb`; `:memory:` supported), sets `memory_limit` and `threads`, and installs/loads DuckDB's `postgres` extension. A separate psycopg connection to PostgreSQL is kept for metadata checks and fallback execution.
- *Table replication*: `preload_tables()` attaches PostgreSQL via `ATTACH ... AS postgres_db (TYPE POSTGRES)` and copies each configured table with `CREATE OR REPLACE TABLE <t> AS SELECT * FROM postgres_db.<t>`. Tables, views, and materialized views are supported (existence is checked in `information_schema.tables` and `pg_matviews`). For persistent DuckDB files, already-present tables are reused unless `force_reload_tables` is set. Table names pass through `query_accelerator.py::validate_table_name` (identifier whitelist regex, length cap) before being interpolated into SQL.
- *Execution with fallback*: `execute_query()` first attempts the query on DuckDB (thread-local cursor); on any exception — including dialect incompatibilities — or on timeout it falls back transparently to PostgreSQL via `_execute_fallback()`, which runs the identical SQL on the psycopg connection. Timeouts are implemented in `_execute_with_timeout()` by running the DuckDB query in a daemon worker thread, joining with the timeout, and calling `duckdb_conn.interrupt()` if exceeded. Results are normalized to a set (first column for single-column rows), matching the partition-key-set semantics expected by `set_cache`.
- *Statistics*: counters for accelerated/fallback/timeout queries, preload time, and average execution times (`get_statistics`, `log_statistics`), guarded by a small lock.

**Usage site.** The only integration point is `src/partitioncache/cli/monitor_cache_queue.py`: at startup the monitor calls `query_accelerator.py::create_query_accelerator` (factory returning `None` on initialization failure, in which case acceleration is disabled and the monitor proceeds normally), optionally preloads tables, and inside variant execution routes through `query_accelerator.execute_query(...)` only when `--enable-duckdb-acceleration` is set *and* `--db-backend postgresql`. Cache writes (`cache_handler.set_cache`) are unchanged. An unresolved `# TODO query_accelerator duckdb` marker remains at the db-handler construction site.

**Configuration** (CLI flags on `pcache-monitor`, defaults from code): `--enable-duckdb-acceleration` (off), `--preload-tables` (comma-separated), `--duckdb-memory-limit` (default `2GB`), `--duckdb-threads` (default 4), `--duckdb-database-path` (default `/tmp/partitioncache_accel.duckdb`), `--force-reload-tables`, `--disable-acceleration-stats`. The `duckdb` package is an optional dependency (pyproject extras `db` and `benchmark`).

**Limitations / current status.** PostgreSQL is the only supported source backend (MySQL/SQLite are explicitly unsupported in [duckdb_acceleration.md](duckdb_acceleration.md)). The replica is a point-in-time snapshot — data changed in PostgreSQL after preloading is invisible to accelerated queries until a forced reload, so the accelerator is only appropriate for relatively static tables; this staleness window is not bounded or monitored by the implementation. Queries referencing non-preloaded tables or PostGIS functions fail in DuckDB and silently take the fallback path (visible only in statistics/logs). Lazy cache population (server-side `INSERT ... SELECT`, required for spatial `geometry` datatypes) cannot use the accelerator at all, since the accelerator only returns result sets to the Python process.

#### DD-28: Optional DuckDB-based variant-execution acceleration
- **Status:** experimental
- **Decision:** Provide an opt-in execution layer for `pcache-monitor` that replicates configured PostgreSQL tables into an embedded DuckDB instance (via DuckDB's `postgres` extension `ATTACH` + `CREATE TABLE ... AS SELECT`) and executes variant queries there, with automatic, per-query fallback to PostgreSQL on any error or timeout. The component is created by a factory that degrades gracefully (returns `None`, acceleration disabled) if DuckDB cannot be initialized.
- **Context:** Cache population is read-heavy and embarrassingly parallel; running it against the operational PostgreSQL instance competes with user queries. An embedded analytical engine over a snapshot decouples population load from the production database.
- **Alternatives considered:** (a) Executing all variants on PostgreSQL (the default, and still the only path for lazy/spatial population); (b) a PostgreSQL read replica — heavier operational footprint, not embeddable in the monitor process; (c) the `pg_duckdb` PostgreSQL extension (mentioned as optional in [duckdb_acceleration.md](duckdb_acceleration.md)) — would keep execution inside PostgreSQL but requires superuser installation.
- **Rationale:** DuckDB's vectorized engine suits the analytical SELECT shape of variant queries; the in-process design needs no extra infrastructure; the unconditional fallback path means enabling the feature can degrade performance but not correctness or availability (failures only increment fallback counters).
- **Trade-offs / costs:** Snapshot staleness (results reflect preload time, not current data — a correctness caveat for caches over mutable data); memory cost of replicated tables bounded only by the configured limit; SQL dialect mismatches reduce the acceleration rate silently; results are pulled into Python as sets, so it cannot serve lazy (server-side) population and is incompatible with the spatial `geometry` datatype path; the feature is untested at production scale within this repository (no committed benchmark artifact).
- **Code anchors:**
  - `src/partitioncache/query_accelerator.py::DuckDBQueryAccelerator`
  - `src/partitioncache/query_accelerator.py::create_query_accelerator`
  - `src/partitioncache/query_accelerator.py::validate_table_name`
  - `src/partitioncache/cli/monitor_cache_queue.py` (flag wiring and `execute_query` call site)
- **Related docs:** [duckdb_acceleration.md](duckdb_acceleration.md)
- **Dissertation mapping:** _to be filled by author_

## 11. CLI & Operational Tooling

PartitionCache exposes its operational surface through six console-script entry points defined in `pyproject.toml` under `[project.scripts]`:

| Entry point | Target module |
|---|---|
| `pcache-manage` | `src/partitioncache/cli/manage_cache.py::main` |
| `pcache-add` | `src/partitioncache/cli/add_to_cache.py::main` |
| `pcache-read` | `src/partitioncache/cli/read_from_cache.py::main` |
| `pcache-monitor` | `src/partitioncache/cli/monitor_cache_queue.py::main` |
| `pcache-postgresql-queue-processor` | `src/partitioncache/cli/postgresql_queue_processor.py::main` |
| `pcache-postgresql-eviction-manager` | `src/partitioncache/cli/postgresql_cache_eviction.py::main` |

The full flag-by-flag documentation is maintained in [cli_reference.md](cli_reference.md); this section summarizes each tool's architectural role.

### 11.1 pcache-manage

Central administration tool with nested subcommands: `setup {all,queue,cache,h3-cells}`, `status {all,env,tables}`, `cache {count,overview,copy,export,import,delete}`, `queue {count,clear}`, and `maintenance {prune,evict,cleanup,partition}`. It is the single place for infrastructure bootstrap, environment validation, and cross-backend cache migration (`cache copy` / `export` / `import` preserve query metadata). The `setup h3-cells` subcommand pre-computes H3 cell IDs for the spatial filtering path. See also [manage_cache_cli.md](manage_cache_cli.md).

### 11.2 pcache-add

Submits a query (`--query` / `--query-file`) for cache population with three mutually exclusive execution modes: `--direct` (compute query variants and execute synchronously), `--queue` (push variants to the variant queue), and `--queue-original` (push the unmodified query to the original-query queue for server-side decomposition). `--no-recompose` bypasses decomposition/recomposition entirely. It carries the full variant-generation argument group (`--bucket-steps`, `--add-constraints`, `--remove-constraints-all`, `--remove-constraints-add`, `--min-component-size`, `--max-component-size`, `--follow-graph`, `--partition-join-table`, `--max-conditions-removed`) and the spatial group (`--geometry-column`; `--buffer-distance` is deliberately excluded here because buffer distance is a read-time concern, see `src/partitioncache/cli/common_args.py::add_spatial_args`).

### 11.3 pcache-read

Reads cached partition keys for a given query/partition key without touching the database under test. Output is format-controlled (`--output-format {list,json,lines}`, `--output-file`) so it can be consumed by scripts or piped into other tools.

### 11.4 pcache-monitor

Multi-process queue consumer for non-PostgreSQL-native deployments: `--max-processes` (default 12) bounds concurrency, `--disable-optimized-polling` falls back to simple polling, `--status-log-interval` controls idle logging, and `--log-query-times` emits per-variant timing CSV. It shares the variant-generation group with `pcache-add` (so observer-side variant generation uses identical normalization) and additionally exposes the experimental DuckDB acceleration flags (`--preload-tables`, `--duckdb-memory-limit`, `--duckdb-threads`; see Section 10).

### 11.5 pcache-postgresql-queue-processor

Manages the database-native processing path (pg_cron): `setup`, `remove`, `enable`, `disable`, `update-config`, `status`, `status-detailed`, `queue-info`, `logs`, `check-permissions`, `manual-process`, `verify`. The `manual-process` subcommand triggers one processing round without pg_cron, which is the mechanism the integration tests rely on (see Section 13). Details in [postgresql_queue_processor.md](postgresql_queue_processor.md) and [pg_cron_cross_database_setup.md](pg_cron_cross_database_setup.md).

### 11.6 pcache-postgresql-eviction-manager

Installs and manages the pg_cron-based eviction job with `--strategy {oldest,largest}` and the same lifecycle subcommands (`setup`/`remove`/`enable`/`disable`/`update-config`/`status`/`logs`/`manual-run`/`verify`). See [cache_eviction.md](cache_eviction.md).

#### DD-24: Operations exposed as six dedicated CLI entry points sharing common argument groups
- **Status:** implemented (core path)
- **Decision:** Split operational tooling into six purpose-specific console scripts instead of one monolithic CLI, and factor all recurring argument groups (database connection, cache backend selection, queue, spatial, variant generation, output, verbosity, environment loading) into `src/partitioncache/cli/common_args.py`.
- **Context:** The tools serve different operators (application developer populating a cache, DBA installing pg_cron jobs, CI harness reading cache state) with mostly disjoint argument surfaces, but variant-generation and connection arguments must be byte-identical across producers and consumers so that variant hashes match.
- **Alternatives considered:** (a) a single `pcache` umbrella command with deep subcommand nesting; (b) per-tool duplicated argparse definitions.
- **Rationale:** Separate entry points keep each tool's `--help` small and allow packaging-level discovery, while the shared `add_*_args` functions guarantee that, e.g., `--bucket-steps` defaults (env-var backed, `PARTITION_CACHE_BUCKET_STEPS`) behave identically in `pcache-add` and `pcache-monitor`. Argument groups also encode read/write asymmetries explicitly (`add_spatial_args(include_buffer_distance=False)` for population-only tools).
- **Trade-offs / costs:** Six binaries to document and keep consistent; `pcache-manage` still accumulates broad scope internally. Some duplication remains in tool-local flags.
- **Code anchors:**
  - `pyproject.toml` (`[project.scripts]`)
  - `src/partitioncache/cli/common_args.py::add_variant_generation_args`
  - `src/partitioncache/cli/common_args.py::add_spatial_args`
  - `src/partitioncache/cli/add_to_cache.py::main`
- **Related docs:** [cli_reference.md](cli_reference.md), [manage_cache_cli.md](manage_cache_cli.md)
- **Dissertation mapping:** _to be filled by author_

## 12. Evaluation & Benchmark Infrastructure

### 12.1 Unified config-driven runner

All workload benchmarks are driven by a single script, `examples/benchmark/run_benchmark.py`, parameterized by YAML configuration files in `examples/benchmark/config/`. The runner supports the modes `all`, `flight N`, `cross-dimension`, `cold-vs-warm`, `hierarchy`, `spatial`, `spatial-backend-comparison`, `spatial-method-comparison`, `backend-comparison`, `method-comparison`, and `api-comparison` (each backed by a `run_*` function in the script), plus orthogonal switches for `--db-backend` (DuckDB/PostgreSQL), `--cache-backend`, `--method` (`IN`, `VALUES`, `TMP_TABLE_IN`, `TMP_TABLE_JOIN`, `IN_SUBQUERY`), `--api` (`non-lazy`, `lazy`, `both`), `--spatial-method` (`SUBDIVIDE_INLINE`, `SUBDIVIDE_TMP_TABLE`), `--repeat`, and JSON `--output`. Per query, the runner measures repeated baseline and cache-enhanced executions (`time.perf_counter`, with min/max/stddev), captures EXPLAIN-derived plan facts (scan type, GiST usage, estimated vs. actual rows), and checks result-correctness (`match`) between baseline and enhanced runs.

Five workload configs exist:

- `examples/benchmark/config/ssb.yaml` — Star Schema Benchmark on the `lineorder` fact table with four integer partition keys (`lo_custkey`, `lo_suppkey`, `lo_partkey`, `lo_orderdate`), eight query flights, and hierarchy labels (region→nation→city, manufacturer→category→brand) for drill-down reuse experiments; runs on DuckDB and PostgreSQL.
- `examples/benchmark/config/tpch.yaml` — TPC-H on `lineitem` with partition keys `l_orderkey`, `l_partkey`, `l_suppkey`; stresses multi-key variant reuse on a normalized schema.
- `examples/benchmark/config/nyc_taxi.yaml` — NYC taxi trips plus OSM POIs on PostgreSQL/PostGIS with a single high-cardinality integer key (`trip_id`); stresses cache behavior when the partition key is near row granularity, including spatial join queries.
- `examples/benchmark/config/osm_poi.yaml` — OpenStreetMap POIs (Germany, SRID 25832) with mixed partition keys: integer (`zipcode`), text (`landkreis`), and two spatial keys (`spatial_h3` via `rocksdict_h3_grouped` at H3 resolution 9; `spatial_bbox` via `postgis_bbox`); the primary workload for the spatial handlers of Section 9.
- `examples/benchmark/config/wikipedia.yaml` — ~280K Simple-English Wikipedia articles with partition key `article_id`; its queries combine conventional filters with per-row LLM classification UDF calls (`wiki_llm_classify(...)` in `examples/wikipedia_benchmark/queries/original/q1_1.sql`, backed by pgai/Ollama or a mock function), testing whether cached partition keys can prune the candidate set before extremely expensive per-row predicates are evaluated.

#### DD-26: One config-driven benchmark runner instead of per-dataset scripts
- **Status:** implemented (optional)
- **Decision:** Replace the per-dataset benchmark scripts with a single runner whose dataset-specific behavior (fact table, partition keys, query flights, hierarchy labels, fragment filters, cache backends, population settings) is declared in YAML.
- **Context:** The repository previously carried separate benchmark drivers and duplicated, manually pre-adapted query copies (e.g., `examples/ssb_benchmark/queries/adapted/`, `examples/tpch_benchmark/queries/adapted/`). Logic such as timing, EXPLAIN capture, correctness checking, and mode orchestration was repeated and drifted between datasets. The consolidation was done on the `data_warehouse_support` branch (commit "Consolidate benchmarks into unified config-driven runner"); the redundant adapted query copies are removed in favor of runtime query adaptation controlled by the `query_processing` section of each config.
- **Alternatives considered:** (a) keep per-dataset scripts and extract a shared library; (b) an external benchmark harness.
- **Rationale:** A single measurement loop means every workload is measured identically (same repeat/median/stddev logic, same plan introspection, same correctness check), which is essential for cross-workload comparability in the evaluation chapter. Adding a workload now means adding a YAML file and queries.
- **Trade-offs / costs:** The runner is large (~2,200 lines) and the YAML schema is informally specified; dataset-specific quirks (e.g., OSM POI's `query_subdirectory_pattern`) leak into the config format. Benchmarks are examples, not part of the installed package or CI.
- **Code anchors:**
  - `examples/benchmark/run_benchmark.py::main`
  - `examples/benchmark/run_benchmark.py::run_single_query`
  - `examples/benchmark/config/` (five YAML files)
  - `examples/benchmark/README.md`
- **Related docs:** `examples/benchmark/README.md` (in-tree), [api_reference.md](api_reference.md)
- **Dissertation mapping:** _to be filled by author_

### 12.2 Research-question mapping

| Benchmark / mode | Research question | Output |
|---|---|---|
| `--mode all` / `flight N` (per workload config) | What end-to-end speedup does cache application yield per query and per partition key on SSB / TPC-H / NYC Taxi / OSM POI / Wikipedia? | Per-query baseline vs. cached timings, plan facts, match flag; optional JSON via `--output` |
| `--mode backend-comparison` | How do cache backends (e.g., `postgresql_array` vs. `postgresql_bit` vs. roaring variants) differ in population and lookup cost for identical workloads? | Per-backend timing table over representative queries |
| `--mode method-comparison` | Which query-extension method (`IN`, `VALUES`, `TMP_TABLE_IN`, `TMP_TABLE_JOIN`, `IN_SUBQUERY`) is preferable at which key-set size? | Per-method timing table |
| `--mode api-comparison` | What is the cost difference between the eager (`get_partition_keys` + extend) and lazy (`apply_cache_lazy`) API paths? | non-lazy vs. lazy timings per backend |
| `--mode cold-vs-warm` | How much of the benefit requires a warm cache; what is the first-execution penalty? | Cold vs. warm timings for a configured flight |
| `--mode hierarchy` | Do drill-down query sequences (region→nation→city etc.) reuse variants cached by broader queries? | Reuse/hit statistics along labeled hierarchies |
| `--mode cross-dimension` | Do variants cached for one dimension's flights accelerate later flights over other dimensions? | Two-phase hit/timing report |
| `--mode spatial`, `spatial-backend-comparison`, `spatial-method-comparison` | Which spatial cache backend (H3 vs. BBox) and which filter-construction method (`SUBDIVIDE_TMP_TABLE`, `SUBDIVIDE_INLINE`, `DIRECT`) is fastest and correct? | Per-backend/method comparison incl. GiST usage and match flags; snapshot in `examples/benchmark/spatial_method_comparison_results.txt` |
| `examples/benchmark/benchmark_h3_cell_lookup.py` (standalone) | How do BBox geometry filtering (8 configurations) and three H3 cell-lookup modes (inline expression index, materialized view, pre-computed column) compare on OSM POI queries? | `examples/benchmark/h3_cell_lookup_results.json` |
| Wikipedia LLM workload (`config/wikipedia.yaml`) | Can cached partition keys prune rows before per-row LLM UDF predicates (`wiki_llm_classify`), where the dominant cost is the predicate itself rather than I/O? | Same per-query metrics on the `wikipedia_articles` workload |

### 12.3 Measured headline results (snapshot artifacts)

The repository contains two result artifacts. Both are committed snapshots from single runs on a developer machine; they are not regenerated automatically by CI and should be treated as illustrative, not authoritative.

From `examples/benchmark/spatial_method_comparison_results.txt` (OSM POI, PostgreSQL, lazy API/`IN_SUBQUERY`, repeat 2):

- H3 with the `DIRECT` method averages **56.68x** speedup over ten spatial queries, dominated by outliers such as spatial_q6 (2.1831 s → 0.0054 s, **404.24x**) and spatial_q3 (**74.62x**); however, the summary marks "All Correct: No", i.e., several runs in this snapshot have baseline/cached row-count mismatches and the numbers predate later correctness fixes.
- BBox is net-negative in this snapshot (averages 0.25x for `SUBDIVIDE_TMP_TABLE`, 0.01x for `DIRECT`, where, e.g., spatial_q10 degrades from 0.1994 s to 2653.3 s), documenting why the non-subdivided BBox path was abandoned for these query shapes.

From `examples/benchmark/h3_cell_lookup_results.json` (standalone cell-lookup benchmark, 9 queries, ~500k POIs; curated summary in `examples/benchmark/README.md`):

- For selective proximity queries, H3 inline mode wins: spatial_q3 reaches **14.2x** (`h3_inline_speedup: 14.20220845018742`), spatial_q2 5.2x, spatial_q1 4.2x, while the best BBox configuration stays at 0.7–1.6x.
- For expression-filter queries the ranking inverts: on bench_qd1 BBox `SUBDIVIDE_TMP_TABLE` (max_vertices 256) reaches **~20x** (`bbox_tmp256_speedup: 19.970746252259126`, equally `DUMP_TMP_TABLE` 19.98x) because the GiST index on the temp table prunes rows before expensive per-row expressions, whereas H3 modes reach only 0.94–2.24x.
- Neither approach helps when the baseline already has effective index paths (bench_qa1, bench_qb1 ≤ 1.03x) or for long spatial chains (bench_qc1 ≤ 0.71x).

### 12.4 Threats to validity / what is not measured

- **Single node, single client.** The runner executes queries sequentially over one connection; concurrency effects, lock contention, and throughput under mixed load are not measured.
- **Scale.** Default scale factor for SSB/TPC-H is 0.01; the spatial datasets are regional extracts (~500k POIs). Behavior at warehouse scale is extrapolated, not measured.
- **Warm-system bias.** Except for the dedicated `cold-vs-warm` mode, measurements run against warm OS/database caches; reported speedups exclude the one-time cache-population cost unless explicitly listed (`Cache population` lines).
- **Snapshot artifacts.** The committed result files are point-in-time snapshots; the spatial method comparison snapshot explicitly records correctness mismatches (`match: False`, "All Correct: No"), so its averages must not be quoted without that caveat.
- **No staleness/eviction dynamics.** The benchmarks populate and immediately query; the effect of eviction policies or stale cache entries on long-running deployments is untested here.

## 13. Testing Strategy

The test suite is split into unit tests (`tests/pytest/`, 41 `test_*.py` files, no external services required) and integration tests (`tests/integration/`, 27 `test_*.py` files plus `conftest.py`/`utils.py`, requiring PostgreSQL/Redis). `tests/TEST_SPEC.md` documents the intended integration coverage per file (cache backend lifecycle, queue processor, CLI, performance, error recovery, end-to-end workflows). The integration setup workflow is described in [integration_test_guide.md](integration_test_guide.md).

CI consists of two workflows:

- `.github/workflows/tests.yml` ("Unit Tests"): a Python 3.10/3.11/3.12 matrix running `ruff check src/ --fix`, `mypy src/`, and `pytest tests/pytest/` with coverage upload.
- `.github/workflows/integration-tests.yml` ("Integration Tests"): first builds a custom PostgreSQL 16 image (`.github/docker/postgres-cron/Dockerfile`, published as `ghcr.io/mpoppinga/postgres-test-extensions`) containing pg_cron, PostGIS, the roaringbitmap extension, and h3-pg compiled from source; then fans out into a 12-entry backend matrix (PostgreSQL Array/Bit/RoaringBit, Redis Set/Bit, RocksDB, DuckDB Acceleration, Queue Processing, Spatial Cache, Spatial Cache RoaringBit, Maintenance Operations, Pipeline Integration) plus separate jobs for CLI tools, performance, pg_cron, Redis queue provider, and a summary gate. Every matrix job creates its own database via `UNIQUE_DB_NAME: partitioncache_<suffix>_<run_id>` and job-specific table prefixes (`ci_array_<suffix>`, ...), so parallel jobs and re-runs cannot observe each other's state.

Queue processing in tests is deliberately manual: integration tests invoke the SQL function `partitioncache_manual_process_queue(...)` (e.g., `tests/integration/test_manual_queue_processor.py`, `tests/integration/test_error_recovery.py`) instead of waiting for pg_cron ticks. Exactly one test module, `tests/integration/test_pg_cron_integration.py`, validates the production pg_cron path end-to-end with its own isolation (cross-database variants in `tests/integration/test_cross_database_pg_cron.py`).

#### DD-27: Per-backend isolated integration matrix in CI; manual queue processing in tests, one dedicated pg_cron test
- **Status:** implemented (core path)
- **Decision:** Run integration tests as a CI matrix with one job per backend family, each against a uniquely named database in a custom-built PostgreSQL image with all required extensions; drive queue processing in tests synchronously via `partitioncache_manual_process_queue()` and confine pg_cron-driven scheduling to one dedicated test module.
- **Context:** pg_cron jobs are named, database-global resources; concurrent CI runs sharing a database produced job-name collisions and timing-dependent flakiness. Several backends additionally need extensions (roaringbitmap, PostGIS, h3-pg) absent from stock images.
- **Alternatives considered:** (a) a single integration job covering all backends sequentially; (b) relying on pg_cron with sleeps in every queue test; (c) per-test Docker containers.
- **Rationale:** Per-job databases (`UNIQUE_DB_NAME` including the GitHub run ID) make isolation structural rather than disciplinary; manual processing makes queue tests deterministic and fast while the production code path (the SQL processing function) is still exercised — pg_cron only adds scheduling, which one focused test validates.
- **Trade-offs / costs:** The matrix multiplies CI time and configuration surface (12+ jobs, per-job env blocks); the pg_cron scheduling path gets comparatively thin coverage; the custom image must be rebuilt when extension versions change.
- **Code anchors:**
  - `.github/workflows/integration-tests.yml` (matrix, `UNIQUE_DB_NAME`)
  - `.github/docker/postgres-cron/Dockerfile`
  - `tests/integration/test_manual_queue_processor.py`
  - `tests/integration/test_pg_cron_integration.py`
- **Related docs:** [integration_test_guide.md](integration_test_guide.md), `tests/TEST_SPEC.md` (in-tree)
- **Dissertation mapping:** _to be filled by author_

## 14. Limitations & Known Trade-offs

- **Conjunctive decomposition only.** Decomposition splits the WHERE clause into AND-connected conditions; OR expressions are detected (`src/partitioncache/query_processor.py::extract_and_group_query_conditions`, `or_conditions`) but treated as atomic blocks attached to their full table group — they are never split into separately cacheable variants, so disjunctive queries see coarser reuse.
- **SELECT-only scope.** The processor operates on the `exp.Select` node of the parsed statement (`src/partitioncache/query_processor.py::clean_query`); DML/DDL and set operations are outside the supported query class.
- **Hash fragility beyond normalization scope.** Cache keys are hashes of normalized variant SQL; normalization covers condition ordering, alias renaming, join canonicalization (`normalize_joins_to_cross_join`), and distance bucketing (`normalize_distance_conditions`), but semantically equivalent queries that differ beyond these rewrites (e.g., `BETWEEN` vs. two inequalities) hash differently and cause false cache misses. False misses cost performance, never correctness.
- **Variant explosion is bounded, not eliminated.** The number of generated variants grows combinatorially with table count and removable conditions; it is capped via `min_component_size`/`max_component_size`, `follow_graph`, and `max_conditions_removed` (`src/partitioncache/query_processor.py::generate_all_query_hash_pairs`), trading reuse opportunities for bounded population cost.
- **Bit backends are integer-only with fixed bitsize.** `postgresql_bit`, `redis_bit`, and `duckdb_bit` represent partition keys as bit positions; non-integer datatypes are rejected (`CHECK (datatype = 'integer')` in `src/partitioncache/cache_handler/postgresql_bit.py`) and the bitsize must be provisioned up front (`PG_BIT_CACHE_BITSIZE` etc.), with expansion handled as a special case (`tests/pytest/test_bitsize_expansion_fix.py`).
- **Asynchronous population implies a staleness window.** In queue-based operation (`pcache-add --queue`, `pcache-monitor`, pg_cron processor) a query's variants become usable only after background processing; the first execution(s) of a new query pattern run uncached, and the cache benefits only repeated or overlapping workloads (see the `cold-vs-warm` results in Section 12).
- **Database-native processing is PostgreSQL-only.** The pg_cron-based queue processor and eviction manager exist solely as PostgreSQL SQL objects (`src/partitioncache/queue_handler/postgresql_queue_processor_cron.sql`, `src/partitioncache/cli/postgresql_cache_eviction.py`); other database backends must use the external `pcache-monitor` observer.
- **Spatial handlers and the DuckDB query accelerator are experimental.** The spatial path (Section 9) has documented configuration sensitivity (buffer distance, subdivision method) and snapshot results with recorded correctness mismatches; the DuckDB acceleration path (Section 10, [duckdb_acceleration.md](duckdb_acceleration.md)) is an opt-in optimization of `pcache-monitor` only.
- **Deprecated remnants.** Three deprecation shims are intentionally retained: the legacy `star_join_*` keyword arguments are translated to `partition_join_*` (`src/partitioncache/query_processor.py::handle_deprecated_kwargs`); the `cell_size` constructor parameter of the BBox handler is accepted but ignored with a warning (`src/partitioncache/cache_handler/postgis_bbox.py`); and the legacy backend names `redis`/`rocksdb` are aliased to `redis_set`/`rocksdb_set` in `src/partitioncache/cache_handler/__init__.py::get_cache_handler`.

---

## Appendix A. Design-Decision Index

| ID | Title | Status | Section |
|---|---|---|---|
| DD-01 | Cache partition keys of query variants instead of query results | implemented (core path) | 1.2 |
| DD-02 | Decompose queries into fragments and recompose connected fragment variants | implemented (core path) | 4.2 |
| DD-03 | Canonical query normalization before hashing (sqlglot-based) | implemented (core path) | 4.1 |
| DD-04 | Content-hash variant identity (SHA-1) | implemented (core path) | 4.5 |
| DD-05 | Controlled variant generation (distance bucketing + constraint add/removal) | implemented (core path; parts optional) | 4.3 |
| DD-06 | Special-case the central partition-join table | implemented (core path) | 4.4 |
| DD-07 | Lazy in-database intersection as the primary read path | implemented (core path) | 5.1 |
| DD-08 | Multiple query-integration methods instead of one fixed rewrite | implemented (core path) | 5.2 |
| DD-09 | Negative caching via per-query status decoupled from cache data | implemented (core path) | 5.4 |
| DD-10 | Single abstract handler contract with an optional lazy extension | implemented (core path) | 6.1 |
| DD-11 | Multiple interchangeable storage representations as an experimental matrix | implemented (core: `postgresql_array`) | 6.2 |
| DD-12 | Metadata-first design — query text and status stored beside cache entries | implemented (core path) | 6.3 |
| DD-13 | Per-backend datatype restrictions enforced at registration time | implemented (core path) | 8 |
| DD-14 | Environment-variable-driven configuration with per-backend namespaces | implemented (core path) | 3.4 |
| DD-15 | Partition-bound helper facade over the generic handler interface | implemented (core path) | 6.4 |
| DD-16 | Two-tier queue separating decomposition from execution | implemented (core path) | 7.1 |
| DD-17 | Database-backed queue with LISTEN/NOTIFY rather than a message broker | implemented (core path) | 7.2 |
| DD-18 | Database-native queue processing via pg_cron stored procedures | implemented (core path) | 7.3 |
| DD-19 | Database-native TTL/size-based eviction | implemented (optional) | 7.5 |
| DD-20 | Cache bounding geometries (PostGIS BBox handler) instead of key sets | experimental | 9.2 |
| DD-21 | H3-grouped cell caching with connected-component intersection | experimental | 9.3 |
| DD-22 | Subdivided-geometry filters to exploit GiST indexes | experimental | 9.4 |
| DD-23 | H3 cell lookup modes (inline / materialized view / precomputed column) | experimental | 9.5 |
| DD-24 | Operations exposed as six dedicated CLI entry points with shared argument groups | implemented (core path) | 11 |
| DD-25 | Optional-dependency extras keep the core installable without database drivers | implemented (core path) | 3.5 |
| DD-26 | One config-driven benchmark runner instead of per-dataset scripts | implemented (optional) | 12.1 |
| DD-27 | Per-backend isolated integration matrix in CI; manual queue processing in tests | implemented (core path) | 13 |
| DD-28 | Optional DuckDB-based variant-execution acceleration | experimental | 10 |

## Appendix B. Claims Register (machine-checkable)

**Verification protocol for agents.** Each row is one claim about the codebase. To verify a row, apply its `Verify-by` check against the file in `Anchor` (paths relative to the repository root): `symbol-exists` → `grep -n "def <name>\|class <name>" <file>` must match the symbol after `::` (for `Class.method`, check the method name); `grep:"<pattern>"` → `grep -F -- <pattern> <file>` must match at least once (use the `--` separator — some patterns start with `--`; unescape `\"` to `"`; for SQL files, patterns may contain doubled quotes as written); `file-exists` → the path must exist; `config-key:<key>` → the key must appear in the named config file. A failing row means document and code have diverged — flag it, do not silently accept the claim.

### B.1 Core idea, API surface, configuration (Sections 1–3)

| ID | Claim | Anchor | Verify-by | Expected |
|---|---|---|---|---|
| FACT-001 | README self-description is "Partition-based query optimization middleware for heavily partitioned datasets" | README.md | grep:"Partition-based query optimization middleware" | 1 match |
| FACT-002 | Package docstring frames project as caching middleware for partition-based query optimization | src/partitioncache/__init__.py | grep:"caching middleware for partition-based query optimization" | 1 match |
| FACT-003 | Approach published as research paper DOI 10.18420/BTW2025-23 | README.md | grep:"10.18420/BTW2025-23" | >=1 match |
| FACT-004 | create_cache_helper is the facade entry point defined in the package root | src/partitioncache/__init__.py::create_cache_helper | symbol-exists | def create_cache_helper |
| FACT-005 | Public API surface is declared via __all__ | src/partitioncache/__init__.py | grep:"__all__" | 1 match |
| FACT-006 | apply_cache_lazy integration methods are IN_SUBQUERY, TMP_TABLE_IN, TMP_TABLE_JOIN | src/partitioncache/apply_cache.py | grep:"Literal[\"IN_SUBQUERY\", \"TMP_TABLE_IN\", \"TMP_TABLE_JOIN\"]" | >=1 match |
| FACT-007 | Handler interface intersects cached key sets across variant hashes | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.get_intersected | symbol-exists | def get_intersected |
| FACT-008 | On zero cache hits apply_cache_lazy returns the working query unchanged | src/partitioncache/apply_cache.py | grep:"If no cache hits, return working query" | 2 matches (eager + lazy path) |
| FACT-009 | Two-tier queue exposes original_query_queue and query_fragment_queue lengths | src/partitioncache/queue.py | grep:"original_query_queue" | >=1 match |
| FACT-010 | Queue provider resolved from QUERY_QUEUE_PROVIDER with default postgresql | src/partitioncache/queue_handler/__init__.py | grep:"QUERY_QUEUE_PROVIDER\", \"postgresql\"" | 1 match |
| FACT-011 | sqlglot is pinned to >=25.0.0,<26.0.0 | pyproject.toml | grep:"sqlglot>=25.0.0,<26.0.0" | 1 match |
| FACT-012 | pyroaring is a core (non-extra) dependency | pyproject.toml | grep:"pyroaring" | 1 match |
| FACT-013 | db extra contains mysql-connector-python among database drivers | pyproject.toml | grep:"mysql-connector-python" | 1 match |
| FACT-014 | rocksdb extra is separated due to platform availability | pyproject.toml | grep:"not available on all platforms" | 1 match |
| FACT-015 | RocksDB backends are feature-flagged via ROCKSDB_AVAILABLE import guard | src/partitioncache/__init__.py | grep:"ROCKSDB_AVAILABLE" | >=1 match |
| FACT-016 | EnvironmentConfigManager fails fast with ValueError on missing variables | src/partitioncache/cache_handler/environment_config.py | grep:"environment variable not set" | >=1 match |
| FACT-017 | Backend-specific Redis variables fall back to generic REDIS_HOST | src/partitioncache/cache_handler/environment_config.py | grep:"REDIS_SET_HOST\") or os.getenv(\"REDIS_HOST" | 1 match |
| FACT-018 | Six pcache-* console scripts are declared in project.scripts | pyproject.toml | grep:"pcache-manage" | 1 match |
| FACT-019 | Query hashes are SHA-1 digests of normalized variant text | src/partitioncache/query_processor.py | grep:"hashlib.sha1" | 1 match |
| DD-01a | Cache stores partition key identifier sets per variant hash | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_cache | symbol-exists | def set_cache |
| DD-01b | Read path intersects cached variant key sets | src/partitioncache/apply_cache.py::get_partition_keys | symbol-exists | def get_partition_keys |
| DD-01c | Decomposition/recomposition produces (variant, hash) pairs | src/partitioncache/query_processor.py::generate_all_query_hash_pairs | symbol-exists | def generate_all_query_hash_pairs |
| DD-14a | Centralized env-var configuration manager exists | src/partitioncache/cache_handler/environment_config.py::EnvironmentConfigManager | symbol-exists | class EnvironmentConfigManager |
| DD-14b | Backend factory consumes env config per cache type | src/partitioncache/cache_handler/__init__.py::get_cache_handler | symbol-exists | def get_cache_handler |
| DD-14c | CLI resolves backend from CACHE_BACKEND env variable | src/partitioncache/cli/common_args.py::resolve_cache_backend | symbol-exists | def resolve_cache_backend |
| DD-25a | Backend drivers are imported lazily inside the factory | src/partitioncache/cache_handler/__init__.py::get_cache_handler | symbol-exists | def get_cache_handler |
| DD-25b | Feature-flagged backend registry tolerates missing RocksDB | src/partitioncache/__init__.py::list_cache_types | symbol-exists | def list_cache_types |

### B.2 Query processing pipeline (Section 4)

| ID | Claim | Anchor | Verify-by | Expected |
|---|---|---|---|---|
| FACT-020 | clean_query normalizes and cleans queries before variant generation | src/partitioncache/query_processor.py::clean_query | symbol-exists | function defined |
| FACT-021 | JOIN ON syntax is rewritten to comma-joins with WHERE predicates | src/partitioncache/query_processor.py::normalize_joins_to_cross_join | symbol-exists | function defined |
| FACT-022 | CNF normalization uses sqlglot's optimizer | src/partitioncache/query_processor.py | grep:"sqlglot.optimizer.normalize.normalize" | >=1 match in clean_query |
| FACT-023 | Outermost SELECT expressions are replaced with * | src/partitioncache/query_processor.py | grep:"exp.Star()" | 1 match |
| FACT-024 | sqlglot is version-pinned | pyproject.toml | grep:"sqlglot>=25.0.0,<26.0.0" | 1 match in dependencies |
| FACT-025 | Conditions are categorized into attribute/distance/partition-key/OR/other groups | src/partitioncache/query_processor.py::extract_and_group_query_conditions | symbol-exists | function defined |
| FACT-026 | Partition-key equijoins are tracked separately for hub detection | src/partitioncache/query_processor.py | grep:"partition_key_joins" | multiple matches |
| FACT-027 | Connected-subgraph enumeration implements follow_graph mode | src/partitioncache/query_processor.py::all_connected_subgraphs | symbol-exists | function defined |
| FACT-028 | follow_graph defaults to True | src/partitioncache/query_processor.py | grep:"follow_graph: bool = True" | 2 matches (generate_partial_queries, generate_all_query_hash_pairs) |
| FACT-029 | max_component_size defaults to 15 when unset | src/partitioncache/query_processor.py | grep:"max_component_size if max_component_size else 15" | 1 match |
| FACT-030 | remove_k_conditions removes at most max_removed=1 condition per table by default | src/partitioncache/query_processor.py | grep:"max_removed: int = 1" | 1 match |
| FACT-031 | Distance bucketing defaults to bucket_steps=1.0 | src/partitioncache/query_processor.py | grep:"bucket_steps: float = 1.0" | 3 matches |
| FACT-032 | Bucketing is restricted to distance-function conditions by default | src/partitioncache/query_processor.py | grep:"restrict_to_dist_functions=True" | >=1 match |
| FACT-033 | Constraint modifications are applied in fixed order after fragment-variant generation | src/partitioncache/query_processor.py::_apply_constraint_modifications | symbol-exists | function defined |
| FACT-034 | remove_constraints_all modifies all variants | src/partitioncache/query_processor.py | grep:"remove_constraints_all" | multiple matches |
| FACT-035 | p0 naming-convention detection is case-insensitive startswith | src/partitioncache/query_processor.py | grep:"is_p0_table" | 2 matches |
| FACT-036 | Deprecated star_join_* kwargs are translated to partition_join_* | src/partitioncache/query_processor.py | grep:"star_join_table" | >=2 matches incl. alias mapping |
| FACT-037 | Variant hashes are SHA-1 hex digests of variant text | src/partitioncache/query_processor.py | grep:"hashlib.sha1" | 1 match in hash_query |
| FACT-140 | Attachment joins (alias.pk = other_alias.other_column) are classified as join-graph edges, not partition-key conditions | src/partitioncache/query_processor.py::_is_partition_key_fk_join | symbol-exists | function defined |
| FACT-141 | Attachment-join queries restrict variants to combinations with a pk-bearing alias and limit synthesized pk equijoins to pk-bearing aliases | src/partitioncache/query_processor.py | grep:"restrict_to_pk_bearing" | multiple matches |
| FACT-038 | Optional canonicalization pass is off by default | src/partitioncache/query_processor.py | grep:"canonicalize_queries: bool = False" | 1 match |
| FACT-039 | Buffer distance is the weighted graph diameter computed via Dijkstra | src/partitioncache/query_processor.py | grep:"single_source_dijkstra_path_length" | 1 match |
| DD-02a | Fragments recomposed into connected conjunctive fragment variants | src/partitioncache/query_processor.py::generate_partial_queries | symbol-exists | function defined |
| DD-02b | Subset enumeration grouped by component size | src/partitioncache/query_processor.py::generate_tuples | symbol-exists | function defined |
| DD-03a | Canonical normalization before hashing | src/partitioncache/query_processor.py::clean_query | symbol-exists | function defined |
| DD-03b | JOIN-to-comma-join normalization | src/partitioncache/query_processor.py::normalize_joins_to_cross_join | symbol-exists | function defined |
| DD-04a | Content-hash variant identity | src/partitioncache/query_processor.py::hash_query | symbol-exists | function defined |
| DD-04b | Pipeline entry returning (query, hash) pairs | src/partitioncache/query_processor.py::generate_all_query_hash_pairs | symbol-exists | function defined |
| DD-05a | Distance bucketing variant generation | src/partitioncache/query_processor.py::normalize_distance_conditions | symbol-exists | function defined |
| DD-05b | Protected-pattern-aware condition removal | src/partitioncache/query_processor.py::remove_k_conditions | symbol-exists | function defined |
| DD-06a | Partition-join table detection (3 tiers) | src/partitioncache/query_processor.py::detect_partition_join_table | symbol-exists | function defined |
| DD-06b | Spatial mode skips partition-key equijoins | src/partitioncache/query_processor.py | grep:"skip_partition_key_joins: bool = False" | 3 matches |

### B.3 Cache application / read path (Section 5)

| ID | Claim | Anchor | Verify-by | Expected |
|---|---|---|---|---|
| FACT-040 | get_partition_keys passes all generated hashes to get_intersected | src/partitioncache/apply_cache.py::get_partition_keys | grep:"get_intersected(set(cache_entry_hashes), partition_key=partition_key)" | 1 match in apply_cache.py |
| FACT-041 | get_partition_keys_lazy raises ValueError for non-lazy handlers | src/partitioncache/apply_cache.py::get_partition_keys_lazy | grep:"Cache handler does not support lazy intersection" | 1 match in apply_cache.py |
| FACT-042 | Statistics dict has keys generated_variants, cache_hits, enhanced, p0_rewritten | src/partitioncache/apply_cache.py::apply_cache | grep:"\"generated_variants\": generated_variants, \"cache_hits\": used_hashes, \"enhanced\": 0, \"p0_rewritten\": p0_rewritten" | matches in apply_cache and apply_cache_lazy |
| FACT-043 | Eager integration methods are IN, VALUES, TMP_TABLE_JOIN, TMP_TABLE_IN with default IN | src/partitioncache/apply_cache.py::extend_query_with_partition_keys | grep:"method: Literal[\"IN\", \"VALUES\", \"TMP_TABLE_JOIN\", \"TMP_TABLE_IN\"] = \"IN\"" | matches in extend_query_with_partition_keys and apply_cache |
| FACT-044 | Lazy integration methods are IN_SUBQUERY, TMP_TABLE_IN, TMP_TABLE_JOIN with default IN_SUBQUERY | src/partitioncache/apply_cache.py::extend_query_with_partition_keys_lazy | grep:"method: Literal[\"IN_SUBQUERY\", \"TMP_TABLE_IN\", \"TMP_TABLE_JOIN\"] = \"IN_SUBQUERY\"" | matches in extend_query_with_partition_keys_lazy and apply_cache_lazy |
| FACT-045 | analyze_tmp_table defaults to True (ANALYZE for temp-table methods; the PRIMARY KEY provides the B-tree index, so no separate CREATE INDEX is emitted for the eager path) | src/partitioncache/apply_cache.py::extend_query_with_partition_keys | grep:"analyze_tmp_table: bool = True" | matches in apply_cache.py signatures |
| FACT-046 | Eager temp table is created with the partition key as PRIMARY KEY and typed column | src/partitioncache/apply_cache.py::_create_tmp_table_setup | grep:"CREATE TEMPORARY TABLE {table_name} (partition_key {partition_key_type} PRIMARY KEY)" | 1 match in apply_cache.py |
| FACT-047 | Hashes are generated from the original query before the p0 rewrite | src/partitioncache/apply_cache.py::apply_cache | grep:"Generate all query variants from ORIGINAL query (not p0-rewritten)" | 1 match in apply_cache.py |
| FACT-048 | p0 materialized-view table name defaults to {partition_key}_mv | src/partitioncache/apply_cache.py::rewrite_query_with_p0_table | grep:"mv_table_name = f\"{partition_key}_mv\"" | 1 match in apply_cache.py |
| FACT-049 | Python-side queries-table DDL constrains status to ok/timeout/failed | src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler | grep:"CHECK (status IN ('ok', 'timeout', 'failed'))" | matches in postgresql_abstract.py and sibling handlers |
| FACT-050 | SQL-side processor DDL additionally allows status 'limit' | src/partitioncache/cache_handler/postgresql_cache_handlers.sql | grep:"CHECK (status IN (''ok'', ''timeout'', ''failed'', ''limit''))" | 1 match in postgresql_cache_handlers.sql |
| FACT-051 | exists(check_query=True) returns True for timeout/failed without a cache check | src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler.exists | grep:"Query has error status -> True (no cache check)" | matches in abstract.py and postgresql_abstract.py |
| FACT-052 | filter_existing_keys fast mode excludes null-marked entries | src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler.filter_existing_keys | grep:"partition_keys IS NOT NULL" | matches in postgresql_abstract.py |
| FACT-053 | Queue processor marks oversized results with status 'limit' | src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql | grep:"DO UPDATE SET status = ''limit''" | 1 match in postgresql_queue_processor_cache.sql |
| FACT-054 | Spatial strategies are SUBDIVIDE_INLINE, SUBDIVIDE_TMP_TABLE, DUMP_TMP_TABLE, DUMP_CTE with default SUBDIVIDE_TMP_TABLE | src/partitioncache/apply_cache.py::extend_query_with_spatial_filter_lazy | grep:"spatial_method: Literal[\"SUBDIVIDE_INLINE\", \"SUBDIVIDE_TMP_TABLE\", \"DUMP_TMP_TABLE\", \"DUMP_CTE\"] = \"SUBDIVIDE_TMP_TABLE\"" | matches in apply_cache.py signatures |
| FACT-055 | SRID 4326 path transforms and casts to geography before distance predicates | src/partitioncache/apply_cache.py::extend_query_with_spatial_filter_lazy | grep:"ST_Transform({p0_alias}.{geometry_column}, 4326)::geography" | multiple matches in apply_cache.py |
| FACT-056 | use_intersects is driven by the handler attribute spatial_filter_includes_buffer | src/partitioncache/apply_cache.py::apply_cache_lazy | grep:"spatial_filter_includes_buffer" | matches in apply_cache.py and spatial handlers |
| FACT-057 | PostgreSQL array lazy intersection is exposed as an unnest subquery | src/partitioncache/cache_handler/postgresql_array.py::PostgreSQLArrayCacheHandler.get_intersected_lazy | grep:"SELECT unnest(({intersectsql})) as {partition_col}" | 1 match in postgresql_array.py |
| FACT-058 | Queue monitor sets status timeout on statement timeout | src/partitioncache/cli/monitor_cache_queue.py | grep:"set_query_status(query_hash, partition_key, \"timeout\")" | matches in monitor_cache_queue.py |
| FACT-059 | buffer_distance is auto-derived from query distance constraints when None | src/partitioncache/apply_cache.py::apply_cache_lazy | grep:"buffer_distance = compute_buffer_distance(query)" | matches in apply_cache and apply_cache_lazy |
| DD-07a | Lazy capability is a distinct handler type | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler_Lazy | symbol-exists | class AbstractCacheHandler_Lazy defined |
| DD-07b | Single-call lazy wrapper exists | src/partitioncache/apply_cache.py::apply_cache_lazy | symbol-exists | function apply_cache_lazy defined |
| DD-08a | Eager integration entry point exists | src/partitioncache/apply_cache.py::extend_query_with_partition_keys | symbol-exists | function defined |
| DD-08b | Lazy integration entry point exists | src/partitioncache/apply_cache.py::extend_query_with_partition_keys_lazy | symbol-exists | function defined |
| DD-09a | Per-query status API is part of the abstract handler contract | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_query_status | symbol-exists | abstract method defined |
| DD-09b | Status checking is opt-in via check_query parameter defaulting to False | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.exists | grep:"check_query: bool = False" | matches in abstract.py and handler implementations |

### B.4 Cache storage layer & datatype system (Sections 6, 8)

| ID | Claim | Anchor | Verify-by | Expected |
|---|---|---|---|---|
| FACT-060 | Core handler contract is defined by AbstractCacheHandler | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler | symbol-exists | class present |
| FACT-061 | Contract includes query status methods | src/partitioncache/cache_handler/abstract.py | grep:"def set_query_status" | 1+ match in abstract.py |
| FACT-062 | set_entry is a non-abstract template method combining set_cache and set_query | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.set_entry | grep:"success_data and success_query" | match in abstract.py |
| FACT-063 | Lazy extension defines get_intersected_lazy, set_cache_lazy, set_entry_lazy | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler_Lazy | grep:"def set_entry_lazy" | match in abstract.py |
| FACT-064 | Factory registers rocksdict_h3_grouped among 13 backend names | src/partitioncache/cache_handler/__init__.py::get_cache_handler | grep:"cache_type == \"rocksdict_h3_grouped\"" | match in cache_handler/__init__.py |
| FACT-065 | PostgreSQL family abstract is lazy-capable | src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler | grep:"class PostgreSQLAbstractCacheHandler(AbstractCacheHandler_Lazy)" | match |
| FACT-066 | Redis family abstract is non-lazy | src/partitioncache/cache_handler/redis_abstract.py::RedisAbstractCacheHandler | grep:"class RedisAbstractCacheHandler(AbstractCacheHandler)" | match |
| FACT-067 | RocksDict family abstract is non-lazy | src/partitioncache/cache_handler/rocksdict_abstract.py::RocksDictAbstractCacheHandler | grep:"class RocksDictAbstractCacheHandler(AbstractCacheHandler)" | match |
| FACT-068 | Array handler intersects via custom aggregate | src/partitioncache/cache_handler/postgresql_array.py::PostgreSQLArrayCacheHandler.get_intersected_sql | grep:"array_intersect_agg" | matches in postgresql_array.py |
| FACT-069 | Bit cache tables use fixed-width BIT columns | src/partitioncache/cache_handler/postgresql_cache_handlers.sql | grep:"partition_keys BIT(%s)" | match in SQL file |
| FACT-070 | Roaring handler intersects server-side via rb_and_agg | src/partitioncache/cache_handler/postgresql_roaringbit.py::PostgreSQLRoaringBitCacheHandler | grep:"rb_and_agg" | matches in postgresql_roaringbit.py |
| FACT-071 | DuckDB handler uses native bit_and aggregate over BITSTRING | src/partitioncache/cache_handler/duckdb_bit.py::DuckDBBitCacheHandler | grep:"bit_and(partition_keys)" | matches in duckdb_bit.py |
| FACT-072 | Redis set handler intersects server-side via SINTER | src/partitioncache/cache_handler/redis_set.py::RedisCacheHandler | grep:"sinter" | match in redis_set.py |
| FACT-073 | Redis bit handler intersects via BITOP AND | src/partitioncache/cache_handler/redis_bit.py::RedisBitCacheHandler | grep:"bitop(\"AND\"" | match in redis_bit.py |
| FACT-074 | RocksDict intersects client-side in Python | src/partitioncache/cache_handler/rocks_dict.py::RocksDictCacheHandler.get_intersected | grep:"result.intersection(v)" | match in rocks_dict.py |
| FACT-075 | Queries table tracks status with CHECK constraint and last_seen | src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler._recreate_metadata_table | grep:"status TEXT NOT NULL DEFAULT 'ok' CHECK" | match in postgresql_abstract.py |
| FACT-076 | Age-based pruning via prune_old_queries | src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler.prune_old_queries | grep:"def prune_old_queries" | match in postgresql_abstract.py |
| FACT-077 | CLI copy/export preserve query metadata via get_all_queries | src/partitioncache/cli/manage_cache.py | grep:"get_all_queries" | matches in manage_cache.py |
| FACT-078 | PostgreSQL singleton bypassed under multithreading | src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler.get_instance | grep:"threading.active_count() > 1" | match in postgresql_abstract.py |
| FACT-079 | Python-type to datatype mapping (int/float/str/datetime) | src/partitioncache/cache_handler/datatype_utils.py::get_datatype_from_settype | grep:"PYTHON_TYPE_TO_DATATYPE" | matches in datatype_utils.py |
| DD-10a | Uniform contract root class exists | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler | symbol-exists | class present |
| DD-10b | Lazy extension is a strict subclass of the root contract | src/partitioncache/cache_handler/abstract.py | grep:"class AbstractCacheHandler_Lazy(AbstractCacheHandler)" | match in abstract.py |
| DD-11a | Bit partition bitsize uses grow-only GREATEST logic | src/partitioncache/cache_handler/postgresql_cache_handlers.sql | grep:"GREATEST(bitsize" | match in SQL file |
| DD-11b | Partition tables bootstrapped via SQL function | src/partitioncache/cache_handler/postgresql_cache_handlers.sql | grep:"partitioncache_bootstrap_partition" | matches in SQL file |
| DD-12a | Metadata tables created alongside cache tables | src/partitioncache/cache_handler/postgresql_abstract.py::PostgreSQLAbstractCacheHandler._recreate_metadata_table | grep:"_partition_metadata" | matches in postgresql_abstract.py |
| DD-12b | Roaring handlers deserialize stored BitMap blobs | src/partitioncache/cache_handler/rocksdict_roaringbit.py::RocksDictRoaringBitCacheHandler | grep:"BitMap.deserialize" | match in rocksdict_roaringbit.py |
| DD-13a | Handler classes declare and validate supported datatypes | src/partitioncache/cache_handler/abstract.py::AbstractCacheHandler.validate_datatype_compatibility | symbol-exists | classmethod defined |
| DD-13b | Bit handler metadata DDL restricts datatype to integer | src/partitioncache/cache_handler/postgresql_bit.py | grep:"CHECK (datatype = 'integer')" | match in postgresql_bit.py |
| DD-15a | Polymorphic helper factory exists | src/partitioncache/cache_handler/helper.py::create_partitioncache_helper | symbol-exists | function present |
| DD-15b | Helper init raises on datatype conflict | src/partitioncache/cache_handler/helper.py::PartitionCacheHelper | grep:"Datatype mismatch" | match in helper.py |

### B.5 Queue system & eviction (Section 7)

| ID | Claim | Anchor | Verify-by | Expected |
|---|---|---|---|---|
| FACT-080 | Module-level API exposes push_to_original_query_queue | src/partitioncache/queue.py::push_to_original_query_queue | grep:"def push_to_original_query_queue" | match in src/partitioncache/queue.py |
| FACT-081 | Module-level API exposes push_to_query_variant_queue (alias push_to_query_fragment_queue) | src/partitioncache/queue.py::push_to_query_variant_queue | grep:"push_to_query_variant_queue = push_to_query_fragment_queue" | alias defined in src/partitioncache/queue.py |
| FACT-082 | Original query queue deduplicates via UNIQUE(query, partition_key) | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"UNIQUE(query, partition_key)" | match in postgresql.py table DDL |
| FACT-083 | Variant queue deduplicates via UNIQUE(hash, partition_key) | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"UNIQUE(hash, partition_key)" | match in postgresql.py table DDL |
| FACT-084 | PostgreSQL pop orders by priority DESC, then created_at ASC | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"ORDER BY priority DESC, created_at ASC" | match in postgresql.py |
| FACT-085 | Dequeue uses FOR UPDATE SKIP LOCKED for lock-free concurrency | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"FOR UPDATE SKIP LOCKED" | matches in postgresql.py and postgresql_queue_processor_cache.sql |
| FACT-086 | Blocking fragment pop listens on channel query_fragment_available | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"query_fragment_available" | matches in postgresql.py |
| FACT-087 | Insert/update triggers notify via notify_query_fragment_insert | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"notify_query_fragment_insert" | matches in postgresql.py |
| FACT-088 | Redis provider pops via BLPOP (FIFO lists) | src/partitioncache/queue_handler/redis.py::RedisQueueHandler | grep:"r.blpop" | matches in redis.py |
| FACT-089 | Redis handler implements the non-priority base class | src/partitioncache/queue_handler/redis.py::RedisQueueHandler | grep:"class RedisQueueHandler(AbstractQueueHandler)" | match in redis.py |
| FACT-090 | PostgreSQL handler implements the priority base class | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"class PostgreSQLQueueHandler(AbstractPriorityQueueHandler)" | match in postgresql.py |
| FACT-091 | Processor config table defaults max_parallel_jobs to 5 (capped) | src/partitioncache/queue_handler/postgresql_queue_processor_cron.sql::partitioncache_initialize_cron_config_table | grep:"max_parallel_jobs INTEGER NOT NULL DEFAULT 5" | match in postgresql_queue_processor_cron.sql |
| FACT-092 | Config-table trigger synchronizes pg_cron jobs | src/partitioncache/queue_handler/postgresql_queue_processor_cron.sql::partitioncache_sync_cron_job | grep:"partitioncache_sync_cron_job" | matches in postgresql_queue_processor_cron.sql |
| FACT-093 | Jobs are scheduled cross-database via cron.schedule_in_database | src/partitioncache/queue_handler/postgresql_queue_processor_cron.sql::partitioncache_sync_cron_job | grep:"cron.schedule_in_database" | matches in queue processor and eviction cron SQL |
| FACT-094 | Dispatcher dequeue skips already-cached variants via NOT EXISTS | src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql::partitioncache_run_single_job_with_params | grep:"WHERE NOT EXISTS" | match in postgresql_queue_processor_cache.sql |
| FACT-095 | Active-jobs PK (query_hash, partition_key) blocks concurrent same-variant runs | src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql | grep:"PRIMARY KEY (query_hash, partition_key)" | match in postgresql_queue_processor_cache.sql |
| FACT-096 | Direct processor restricted to PostgreSQL-family backends | src/partitioncache/cli/postgresql_queue_processor.py::DIRECT_PROCESSOR_BACKENDS | grep:"DIRECT_PROCESSOR_BACKENDS" | dict with postgresql_array, postgresql_bit, postgresql_roaringbit, postgis_bbox |
| FACT-097 | Manual (non-cron) processing function exists for tests | src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql::partitioncache_manual_process_queue | grep:"partitioncache_manual_process_queue" | matches in postgresql_queue_processor_cache.sql |
| FACT-098 | Eviction strategy 'largest' implemented per partition | src/partitioncache/cache_handler/postgresql_cache_eviction_cache.sql::_partitioncache_evict_largest_from_partition | grep:"_partitioncache_evict_largest_from_partition" | matches in postgresql_cache_eviction_cache.sql |
| FACT-099 | Eviction dispatcher iterates partitions with strategy+threshold params | src/partitioncache/cache_handler/postgresql_cache_eviction_cache.sql::partitioncache_run_eviction_job_with_params | grep:"partitioncache_run_eviction_job_with_params" | matches in eviction SQL and CLI |
| DD-16a | Dedicated original-query queue table tier exists | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"_original_query_queue" | matches across queue handler and SQL files |
| DD-16b | Dedicated query-fragment queue table tier exists | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"_query_fragment_queue" | matches across queue handler and SQL files |
| DD-17a | Blocking original pop uses LISTEN original_query_available | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"LISTEN original_query_available" | match in postgresql.py |
| DD-17b | Blocking pop waits on the LISTEN connection file descriptor | src/partitioncache/queue_handler/postgresql.py::PostgreSQLQueueHandler | grep:"listen_conn.fileno" | matches in postgresql.py |
| DD-18a | pg_cron dispatcher function processes one variant per tick | src/partitioncache/queue_handler/postgresql_queue_processor_cache.sql::partitioncache_run_single_job_with_params | grep:"partitioncache_run_single_job_with_params" | matches in cache SQL, cron SQL, and CLI |
| DD-18b | External observer alternative uses a thread-pool fragment executor | src/partitioncache/cli/monitor_cache_queue.py::fragment_executor | grep:"def fragment_executor" | match in monitor_cache_queue.py |
| DD-19a | Eviction strategy 'oldest' implemented per partition | src/partitioncache/cache_handler/postgresql_cache_eviction_cache.sql::_partitioncache_evict_oldest_from_partition | grep:"_partitioncache_evict_oldest_from_partition" | matches in postgresql_cache_eviction_cache.sql |
| DD-19b | Eviction config trigger synchronizes its pg_cron job | src/partitioncache/cache_handler/postgresql_cache_eviction_cron.sql::partitioncache_sync_eviction_cron_job | grep:"partitioncache_sync_eviction_cron_job" | matches in postgresql_cache_eviction_cron.sql |

### B.6 Spatial extension & query accelerator (Sections 9–10)

| ID | Claim | Anchor | Verify-by | Expected |
|---|---|---|---|---|
| FACT-100 | Spatial handlers share a PostGIS abstract base with a unified "geometry" datatype | src/partitioncache/cache_handler/postgis_spatial_abstract.py::PostGISSpatialAbstractCacheHandler | symbol-exists | class defined |
| FACT-101 | get_spatial_filter contract returns WKB bytes plus SRID | src/partitioncache/cache_handler/postgis_spatial_abstract.py | grep:"tuple[bytes, int]" | return type annotation present |
| FACT-102 | BBox set_cache_lazy aggregates variant geometries with ST_Collect | src/partitioncache/cache_handler/postgis_bbox.py | grep:"ST_Collect(DISTINCT" | collect aggregation in INSERT SQL |
| FACT-103 | BBox cache table carries a GiST index on the stored geometry | src/partitioncache/cache_handler/postgis_bbox.py | grep:"USING GIST (partition_keys)" | index DDL present |
| FACT-104 | The cell_size grid parameter is deprecated and unused | src/partitioncache/cache_handler/postgis_bbox.py | grep:"cell_size parameter is deprecated" | DeprecationWarning text present |
| FACT-105 | BBox bakes the buffer into the filter geometry (ST_Intersects at apply time) | src/partitioncache/cache_handler/postgis_bbox.py | grep:"spatial_filter_includes_buffer" | property overridden to True |
| FACT-106 | Grouped match-set intersection uses a union-find helper | src/partitioncache/cache_handler/rocks_dict.py::_grouped_intersection | symbol-exists | function defined |
| FACT-107 | K-ring buffer expansion has a dedicated grouped helper | src/partitioncache/cache_handler/rocks_dict.py::_grouped_kring_intersection | symbol-exists | function defined |
| FACT-108 | Only connected components spanning all variants survive intersection | src/partitioncache/cache_handler/rocks_dict.py | grep:"len(frag_indices) == num_fragments" | spanning check present |
| FACT-109 | H3 grouped handler advertises spatial_filter_type h3_cell_ids | src/partitioncache/cache_handler/rocksdict_h3_grouped.py | grep:"h3_cell_ids" | property returns h3_cell_ids |
| FACT-110 | H3 cell conversion uses PostgreSQL h3-pg at population time only | src/partitioncache/cache_handler/rocksdict_h3_grouped.py | grep:"h3_lat_lng_to_cell" | conversion SQL in geom_to_h3_cell |
| FACT-111 | Four spatial application methods incl. DUMP variants are implemented | src/partitioncache/apply_cache.py | grep:"DUMP_TMP_TABLE" | method literal present |
| FACT-112 | Geography cast is applied only for SRID 4326 | src/partitioncache/apply_cache.py | grep:"4326)::geography" | cast only in srid==4326 branches |
| FACT-113 | Buffer distance is auto-derived as weighted graph diameter of distance constraints | src/partitioncache/query_processor.py::compute_buffer_distance | symbol-exists | function defined |
| FACT-114 | Multi-alias spatial variants emit per-alias geometry columns for grouped match sets | src/partitioncache/query_processor.py::_build_spatial_grouped_query | symbol-exists | function defined |
| FACT-115 | H3 cell lookup supports inline, mv, and column modes | src/partitioncache/apply_cache.py | grep:"cell_mode" | cell_mode parameter dispatch present |
| FACT-116 | Spatial method comparison artifact is checked in | examples/benchmark/spatial_method_comparison_results.txt | file-exists | file present |
| FACT-117 | H3 cell lookup benchmark artifact is checked in | examples/benchmark/h3_cell_lookup_results.json | file-exists | file present |
| FACT-118 | DuckDB accelerator class exists | src/partitioncache/query_accelerator.py::DuckDBQueryAccelerator | symbol-exists | class defined |
| FACT-119 | Accelerator falls back to PostgreSQL on DuckDB failure or timeout | src/partitioncache/query_accelerator.py | grep:"def _execute_fallback" | fallback method defined |
| DD-20a | PostGIS BBox handler is implemented | src/partitioncache/cache_handler/postgis_bbox.py::PostGISBBoxCacheHandler | symbol-exists | class defined |
| DD-20b | Filter is built by buffered chained ST_Intersection | src/partitioncache/cache_handler/postgis_bbox.py | grep:"_get_buffered_intersected_sql" | buffered intersection builder present |
| DD-21a | RocksDict H3 grouped handler is implemented | src/partitioncache/cache_handler/rocksdict_h3_grouped.py::RocksDictH3GroupedCacheHandler | symbol-exists | class defined |
| DD-21b | Cell-ID filter with k-ring expansion is implemented | src/partitioncache/cache_handler/rocksdict_h3_grouped.py | grep:"def get_h3_cell_filter" | method defined |
| DD-22a | Lazy spatial filter application function exists | src/partitioncache/apply_cache.py::extend_query_with_spatial_filter_lazy | symbol-exists | function defined |
| DD-22b | Default method subdivides dumped pieces into an indexed temp table | src/partitioncache/apply_cache.py | grep:"ST_Subdivide((ST_Dump(" | subdivide-of-dump SQL present |
| DD-23a | H3 cell lookup mode dispatcher exists | src/partitioncache/apply_cache.py::extend_query_with_h3_cell_lookup | symbol-exists | function defined |
| DD-23b | Dedicated cell-lookup benchmark script exists | examples/benchmark/benchmark_h3_cell_lookup.py | file-exists | file present |
| DD-28a | Accelerator factory with graceful degradation exists | src/partitioncache/query_accelerator.py::create_query_accelerator | symbol-exists | function defined |
| DD-28b | Acceleration is opt-in via monitor CLI flag | src/partitioncache/cli/monitor_cache_queue.py | grep:"enable-duckdb-acceleration" | flag defined and gated |

### B.7 CLI, evaluation, testing, limitations (Sections 11–14)

| ID | Claim | Anchor | Verify-by | Expected |
|---|---|---|---|---|
| FACT-120 | pyproject.toml defines six pcache-* console-script entry points incl. the eviction manager | pyproject.toml | grep:"pcache-postgresql-eviction-manager" | match in [project.scripts] |
| FACT-121 | Shared variant-generation argument group exists in the CLI common args module | src/partitioncache/cli/common_args.py::add_variant_generation_args | symbol-exists | function defined |
| FACT-122 | pcache-add offers --queue-original as an execution mode | src/partitioncache/cli/add_to_cache.py | grep:"--queue-original" | argparse flag present |
| FACT-123 | Spatial CLI group includes --buffer-distance | src/partitioncache/cli/common_args.py | grep:"--buffer-distance" | argparse flag present |
| FACT-124 | pcache-read supports --output-format (list/json/lines) | src/partitioncache/cli/common_args.py | grep:"--output-format" | flag with choices list,json,lines |
| FACT-125 | pcache-monitor bounds concurrency via --max-processes | src/partitioncache/cli/monitor_cache_queue.py | grep:"--max-processes" | argparse flag present |
| FACT-126 | Queue processor CLI has a manual-process subcommand | src/partitioncache/cli/postgresql_queue_processor.py | grep:"manual-process" | subparser present |
| FACT-127 | Eviction manager supports oldest/largest strategies | src/partitioncache/cli/postgresql_cache_eviction.py | grep:"largest" | strategy choice present |
| FACT-128 | Unified runner exposes a spatial-method-comparison mode | examples/benchmark/run_benchmark.py | grep:"spatial-method-comparison" | mode listed in --mode help |
| FACT-129 | Five workload configs exist incl. the Wikipedia LLM workload | examples/benchmark/config/wikipedia.yaml | file-exists | file present |
| FACT-130 | Spatial snapshot records 56.68x average for H3 DIRECT | examples/benchmark/spatial_method_comparison_results.txt | grep:"56.68x" | value in per-backend averages |
| FACT-131 | bench_qd1 BBox SUBDIVIDE_TMP_TABLE(256) speedup ~20x recorded in JSON artifact | examples/benchmark/h3_cell_lookup_results.json | grep:"19.970746252259126" | exact value present |
| FACT-132 | spatial_q3 H3 inline speedup 14.2x recorded in JSON artifact | examples/benchmark/h3_cell_lookup_results.json | grep:"14.20220845018742" | exact value present |
| FACT-133 | Wikipedia benchmark queries call a per-row LLM classification UDF | examples/wikipedia_benchmark/queries/original/q1_1.sql | grep:"wiki_llm_classify" | UDF invoked in query |
| FACT-134 | Integration CI gives each matrix job a unique database name | .github/workflows/integration-tests.yml | grep:"UNIQUE_DB_NAME" | env var with run-id suffix |
| FACT-135 | Custom CI postgres image builds h3-pg (plus pg_cron, PostGIS, roaringbitmap) | .github/docker/postgres-cron/Dockerfile | grep:"h3-pg" | extension built from source |
| FACT-136 | Integration tests process queues manually via SQL function | tests/integration/test_manual_queue_processor.py | grep:"partitioncache_manual_process_queue" | function called in tests |
| FACT-137 | One dedicated pg_cron integration test module exists | tests/integration/test_pg_cron_integration.py | file-exists | file present |
| FACT-138 | OR conditions are grouped atomically, not split into variants | src/partitioncache/query_processor.py | grep:"or_conditions" | or_conditions handling present |
| FACT-139 | Deprecated star_join_* kwargs are translated via a shim | src/partitioncache/query_processor.py::handle_deprecated_kwargs | symbol-exists | function defined |
| DD-24a | Six CLI entry points share common argument groups | src/partitioncache/cli/common_args.py | file-exists | module present |
| DD-26a | One config-driven benchmark runner replaces per-dataset scripts | examples/benchmark/run_benchmark.py | file-exists | unified runner present |
| DD-27a | Per-backend isolated integration matrix in CI | .github/workflows/integration-tests.yml | grep:"database_suffix" | per-job suffix-based isolation |

## Appendix C. Pointer Map to Existing Documents

This document states decisions, rationale, and anchors; per-topic details are owned by the documents below. When updating one of these, check whether the corresponding section here (and the dissertation text mapped to it) needs an update too.

| Topic | Owning document | Covered here in |
|---|---|---|
| Visual architecture diagrams (mermaid) | [architecture_diagrams.md](architecture_diagrams.md) | Section 3 |
| Full Python API signatures | [api_reference.md](api_reference.md) | Sections 3.3, 4, 5 |
| Backend selection guide & feature matrix | [cache_handlers.md](cache_handlers.md) | Section 6 |
| Datatype compatibility matrix (user-facing) | [datatype_support.md](datatype_support.md) | Section 8 |
| Queue schemas, providers, operations | [queue_system.md](queue_system.md) | Section 7 |
| pg_cron processor setup & monitoring | [postgresql_queue_processor.md](postgresql_queue_processor.md) | Section 7.3 |
| Cross-database pg_cron configuration | [pg_cron_cross_database_setup.md](pg_cron_cross_database_setup.md) | Sections 7.3, 7.5 |
| Eviction setup & operations | [cache_eviction.md](cache_eviction.md) | Section 7.5 |
| Partition-join (p0) table usage | [p0_table_handling.md](p0_table_handling.md) | Sections 4.4, 5.3 |
| CLI flag-by-flag reference | [cli_reference.md](cli_reference.md) | Section 11 |
| pcache-manage usage guide | [manage_cache_cli.md](manage_cache_cli.md) | Section 11.1 |
| End-to-end workflow tutorial | [complete_workflow_example.md](complete_workflow_example.md) | Sections 3.2, 12 |
| Integration test setup | [integration_test_guide.md](integration_test_guide.md) | Section 13 |
| DuckDB accelerator user guide | [duckdb_acceleration.md](duckdb_acceleration.md) | Section 10 |
| Spatial design history & measurements | [plans/2026-03-06-spatial-filter-optimization-design.md](plans/2026-03-06-spatial-filter-optimization-design.md), [plans/spatial_bbox_optimization_analysis.md](plans/spatial_bbox_optimization_analysis.md) | Section 9 |
| EXISTS vs. comma-join variant analysis | [plans/exists_vs_comma_join_variants.md](plans/exists_vs_comma_join_variants.md) | Section 4 (background) |
| Benchmark runner usage & config schema | `examples/benchmark/README.md` (in-tree) | Section 12 |
| Integration test case specification | `tests/TEST_SPEC.md` (in-tree) | Section 13 |







