# Changelog

## 1.0.0

First tagged release. The version is in `partitioncache.__version__`.

### Requirements

- Python 3.11 or later.
- PostgreSQL 13 or later for the queue processor (its fragment timeout is set per statement of the job command);
  `pg_cron` for the queue processor and the eviction manager; `intarray` for `postgresql_array`; `roaringbitmap`
  for `postgresql_roaringbit`; PostGIS only for the spatial handlers.
- DuckDB 2.0 or later for `duckdb_bit` and the DuckDB acceleration.
- The database drivers are in the `db` extra, the H3 library in the `spatial` extra.

### Behaviour changes

Against `main` at 3b132dc (#76), the state the release review started from:

- **Fragment queue order.** New fragments get a priority by size (`10 - tables`), so the smaller fragments of all
  queued queries are popped first. On by default; `--no-size-priority` or `QUERY_QUEUE_SIZE_PRIORITY=false` gives
  the former order (#77).
- **JOIN restriction methods** join the key table only to tables of the FROM clause and the top-level joins of
  the restricted SELECT, not to tables of subqueries or other branches that reuse an alias (#86).
- **Settings in an `--env` file.** `PARTITION_CACHE_*` values that stand only in the env file now take effect; they
  were ignored before. Precedence: command-line flag, exported variable, env file, built-in default (#80).
- **Lazy insertion** is used only when the cache handler's connection and the data connection are the same server
  (loopback spellings and resolved addresses count as equal), port and database. Otherwise the fragment takes the
  non-lazy path and a warning says what to align; spatial datatypes, which have no other path, fail with it (#80).
- **Exit status.** `pcache-add`, `pcache-manage cache copy/import/export`, `maintenance prune`, `queue clear` and
  `pcache-postgresql-queue-processor check-permissions` exit 1 when something failed (#80, #82).
- **Queue API.** For an item pushed without a datatype the pops return `None` as `partition_datatype` (formerly `""`
  from PostgreSQL, `"integer"` from Redis). The PostgreSQL queue's pushes return `False` when a row could not be
  queued (#82).
- **`pcache-monitor --close`** exits only when both queues are empty, no decomposition is in flight and no worker
  runs (#82).
- **Spatial lookups with an automatic buffer distance.** `compute_buffer_distance` returns `None` unless every table
  is linked to p0 by top-level distance predicates; the lookup then uses only the fragments of the linked tables or
  runs without the cache (`uncached_reason: "spatial_unbounded"`). Only `ST_DWithin` and upper bounds on PostGIS
  distance calls (`ST_Distance` and relatives) count; a distance written with SQRT/POWER over coordinates or with a
  user function gets no automatic buffer, pass `buffer_distance` for such a query. The spatial rewrite needs
  `sql_dialect="postgres"` (#84).
- **Lookups with `skip_partition_key_joins`** and no temporal or spatial parameters use only the variants that select
  the restricted alias's key; their hashes differ from before (#86).
- **Set operations.** A query whose variants are not all variants of its first branch runs without the cache (#86).
- **Queries without variants.** These run without the cache, with a log line: a query with a WITH clause; a query
  in which a subquery defines a table name or alias that the alias renaming would capture; a query with a condition
  between the partition keys of two tables that are not key-joined (`k1.pk < k2.pk`, `a.pk = b.pk + 1`), unless
  `skip_partition_key_joins` is set; a quoted mixed-case table without an alias. Clauses of a subquery (ORDER BY,
  LIMIT, OFFSET, GROUP BY, HAVING) are removed only where that cannot drop rows, so variants of queries with
  negated or limited subqueries have other hashes (#87).
- **Queue processor.** The fragment timeout is set in the scheduled command, not with `ALTER ROLE`; this needs
  PostgreSQL 13 or later. `setup` and `remove` reset a role-wide `statement_timeout` left by an earlier setup when it
  still has the value that setup wrote. `partitioncache_manual_process_queue` takes
  `(p_count, p_job_name, p_queue_prefix)` and raises instead of guessing when several configurations qualify. The
  SQL processor refuses the `postgis_bbox` backend (#85).
- **PostgreSQL cache handlers.** `prune_old_queries(days_old)` prunes by its window (it pruned everything older than
  one day). The roaring bitmap handler refuses negative keys. A new partition key whose truncated cache table name is
  that of another registered key is refused (#83).

- **Bit caches.** The bit cache tables of one prefix share one bitsize trigger function; older caches are switched
  when a handler starts (#81).

Earlier changes that a user of an older untagged state meets (see the history for the full list):

- `partitioncache.query_processor.variant_signature` and `is_relaxation_of` are gone; population restricts a
  variant through the cache lookup of its query (#61).
- The JOIN restriction methods join the key table to the p0 table only; `join_every_table`
  (`--join-every-table` in `pcache-add` and `pcache-monitor`) joins every table as before (#62).
- `pcache-monitor` records a fragment that timed out, failed or hit `--limit` and skips it from then on (#42).
- Key lists and key tables are typed like the key column for timestamp, `char(n)` and `real` keys (#64, #68).
- With `skip_partition_key_joins` and no explicit source, the alias whose key the query selects is the key
  source; variants of self-joins got other hashes (#72).
- `set_entry_lazy` writes the query row only together with its cache row (#76).

### Fixed

- `pcache-monitor`: blocking pops of the PostgreSQL queue ignored their timeout and could hang the monitor; a queue
  item without a datatype was dropped; an original query whose fragments could not be pushed was lost (#82).
- PostgreSQL cache handlers: a failed read left the connection aborted, and later lookups reported "unknown";
  `set_entry` wrote the query row after the cache row had failed; materialised intersections returned an empty set
  when entries vanished during the call (#83).
- DuckDB acceleration: a multi-statement script returned the result of its last statement, so an empty set could be
  cached; NULL keys were kept (#78).
- Spatial lookups: the automatic buffer distance was too small for tables tied to p0 only by an attribute join,
  under NOT or OR, for a radius written as arithmetic and for a reversed comparison; the spatial rewrite re-rendered
  PostgreSQL syntax in a generic dialect (#84).
- `apply_cache`: the restriction of a set operation could land in another branch; JOIN methods joined the key table
  into subqueries and other branches that reuse the p0 alias; `use_p0_table` removed conditions that only contained
  a key equality (#86). A query whose subquery reuses the p0 alias gets no variants since #87, so through the
  lookup this part of the fix is reached for set operations only; it also holds for
  `extend_query_with_partition_keys` and `extend_query_with_partition_keys_lazy` called directly.
- Queue processor: `manual-process --table-prefix` was ignored; the automatic `GRANT` never worked (#85).
- Variant generation: `x.pk = y.pk + 1`, `x.pk = y.pk_old` and `(x.pk = y.pk OR ...)` counted as key joins;
  LIMIT, GROUP BY and HAVING were removed from negated subqueries; a CTE query was decomposed from the CTE body;
  table names containing FROM or WHERE were cut; with `skip_partition_key_joins` a key condition moved to another
  alias; `BETWEEN SYMMETRIC` on one table became a contradiction; mixed-case quoted names lost their quotes (#87).
- Long table prefixes: with a prefix over 44 characters, eviction evicted nothing and the status report did not
  list the cache; with a cache table name over 63 bytes the queue processor bootstrapped the partition again in
  every job (#89).
- Tests that reported failures as skips now fail; the H3 tests run in CI (#79).

### Upgrading an existing installation

The SQL functions live in the database. Reload them with `pcache-postgresql-queue-processor setup` (processor and
status report) and `pcache-postgresql-eviction-manager setup` (eviction); until then the scheduled jobs keep
running the old functions. A process of an older version that runs its setup on the same database reinstalls the
old ones. The queue-processor notes are in `docs/postgresql_queue_processor.md` under "Upgrading". Clear the caches
of queries whose variants changed (see "Queries without variants" and the `skip_partition_key_joins` entries
above): their old entries are no longer looked up, and for these shapes some were wrong.

### Known limitations

Requirements on the data and the queries:

- **NULL partition keys.** The partition key column must not be NULL in rows a query can return; the cache
  restriction removes such rows.
- **Float partition keys are experimental.** In `postgresql_array` they are stored as NUMERIC with 15 significant digits for double
  precision and 6 for real values; keys with more digits are stored rounded and their rows are missing from
  restricted queries (see `docs/datatype_support.md`). Use integer, text or timestamp keys where exactness matters.
- **Spatial handlers.** `postgis_bbox` and `rocksdict_h3_grouped` are sound for point geometries only, and
  `rocksdict_h3_grouped` converts the buffer to H3 rings assuming metres. Grid-cell keys have neither limit.
- **JOIN restriction methods** add the key table's column to the output of `SELECT *`.
- **Lookup defaults.** `apply_cache` and `apply_cache_lazy` default to `min_component_size=2`, population to 1. A
  query whose tables are joined only on the partition key has only single-table variants, so a lookup with the
  default finds nothing; pass `min_component_size=1`.

- **Shared keys are assumed.** Without `skip_partition_key_joins`, the variants of a multi-table query assume that
  all its tables carry the same partition key value (they are joined on it, or co-partitioned). Nothing checks this
  for tables that have no condition on the key at all.
- **Set operations** are decomposed from their first branch only; the other branches run unrestricted.
- **A column named like a table alias** is renamed with the alias in a condition between two tables
  (`x.y < y.y`); such a variant fails or is wrong. Use column names that differ from the aliases.

Workers:

- **Failed fragments are not retried.** A fragment that fails for a transient reason (lost connection, server
  restart, deadlock) gets the status `failed` like any other and is skipped from then on.
- **`pcache-monitor` acknowledges a fragment when it pops it.** A fragment in flight when the monitor is killed, or
  whose worker fails before the query runs, is not put back.
- **The PostgreSQL queue processor** processes the fragment queue only; original queries are decomposed by
  `pcache-monitor`. With `cron.use_background_workers = on` its fragment timeout is not enforced. It does not
  support `postgis_bbox`.
- **DuckDB acceleration** must not be used for fragments that compare text under a non-C collation, compare
  `char(n)` columns, or use a LIKE pattern whose backslash comes from data; they can produce key sets that are too
  small. A fragment that compares a `real` column with a decimal literal can differ by the keys whose value equals
  the literal rounded to single precision (DuckDB compares in single precision, PostgreSQL in double): `=` and one
  non-strict bound give more keys, `<>` and one strict bound fewer, so a key set that is too small is possible there
  as well. Casting the column to `double precision` in the fragment avoids the difference.
- **Lazy cache optimisation in `pcache-monitor`** reads the cache tables on the data connection. If the cache lives
  in another database that has cache tables of the same names, it reads those.

Installations and trust:

- **Queue contents are SQL.** A row in the fragment queue is executed with the rights of the job owner or of the
  monitor's database user. Give write access to the queue tables only to roles that may run that SQL.
- **Cache archives are pickle.** `pcache-manage cache import` and the RocksDict backends unpickle; load only archives
  and cache directories you trust.
- **Several installations in one cluster.** Queue-processor job names can collide between installations with similar
  prefixes, and `remove` deletes cron jobs by a name pattern that can match another installation's jobs.
- **RocksDict backends.** `rocksdict`, `rocksdict_roaringbit` and `rocksdict_h3_grouped` fall back to
  `ROCKSDB_DICT_PATH`; give each backend its own path, their value formats differ.
