# Design note: folding partition-key subqueries into the key table

Status: **prototype with tests, for decision**. Verdict: **works, with the limits listed below.**

## Problem

A query can carry its own restriction on the partition key as a subquery. A GeoMine example:

```sql
... AND pl.pocket_key IN (SELECT unnest(pocket_key) FROM interactiondb_search_view_molecule_property_pockets_num WHERE prop_key = 39 AND ...)
```

Here `pl` is the p0 alias. The key-table methods (`TMP_TABLE_*`, `UNLOGGED_TABLE_*`) put the cache keys into a key table with an index and statistics, but leave this subquery in the statement. PostgreSQL estimates the output of `unnest()` with a default instead of the true size.

The demo below shows the effect: 10 estimated rows, 100,000 actual. With `planner_collapse_limit` the planner pulls the subquery into the join order and drives the search from it.

GeoMine's C++ implementation avoids this. It builds the key table as `(cache keys) ∩ (metadata subqueries)` and removes the subqueries from the statement (`combineLazySubqueries`).

## The rewrite

With `fold_key_subqueries=True` and a key-table method, a condition is folded when all of the following hold:
- it is a **top-level conjunct of the outer WHERE**, so never under `OR` or `NOT`, and therefore never `NOT IN`;
- it has the form `<alias>.<key> IN (<subquery>)` or `<alias>.<key> = ANY (<subquery>)`;
- `<alias>` is the restricted alias (p0), or is tied to it by equalities `a.<key> = b.<key>` among the top-level conjuncts of the outer WHERE and the ON clauses of inner joins;
- the subquery can run on its own (see "Refused" below).

A folded condition leaves the statement. The key table keeps only the keys for which `key IN (<subquery>)` is TRUE:

| Path | Key table built as |
|---|---|
| eager `TMP_TABLE_*` | `INSERT ... VALUES (<cache keys>)`, then per subquery `DELETE FROM t WHERE (partition_key IN (<subquery>)) IS NOT TRUE`, then `ANALYZE` |
| eager `UNLOGGED_TABLE_*` and every lazy table method | `CREATE ... AS (SELECT k FROM (<cache keys>) ... WHERE k IN (<subquery 1>) AND k IN (<subquery 2>))`, then index and `ANALYZE` |

The restriction itself is unchanged: the key table is joined to p0, or the keys are applied as an IN on it.

## Why it is equivalent

Let φ(v) be `v IN S` in SQL's three-valued logic.
- **Before:** a row passes when the rest of the WHERE is TRUE, φ(alias.key) is TRUE, and p0.key ∈ K, where K are the cache keys.
- **After:** a row passes when the rest is TRUE and p0.key ∈ T, with T = {k ∈ K : φ(k) TRUE}.

The two are equal when:
1. **φ is the same function in both places.** The subquery is uncorrelated and deterministic, so it gives the same set when it runs in the key table's statement.
2. **alias.key = p0.key in every passing row.** This holds for the p0 alias itself, and through top-level key equalities, which are part of "the rest".
   - The ON of an outer join does not count, because a null-extended row has alias.key NULL.
   - For a key with `skip_partition_key_joins` there are no key equalities, so only conditions on p0 itself fold.
3. **The comparison `k IN S` has the semantics of `alias.key IN S`.** The key table's column has the key's type (see "Limits").

NULLs and duplicates need no special handling. T is filtered by φ's TRUE exactly as the row filter was, and a NULL key never passes either way. The key tables are already sets.

An empty T restricts the query to no rows, which is what the folded conditions together with K imply.

## Refused (the condition stays in the statement)

| Case | Why |
|---|---|
| `NOT IN`, the condition under `OR` / `NOT`, not a top-level conjunct | φ is not a necessary condition of the row |
| correlated subquery (a qualified column of an outer alias) | it has no value on its own |
| a CTE of the outer query referenced in the subquery | not defined in the key table's statement (it could be copied there in a later version) |
| volatile function (`random`, `now`, `clock_timestamp`, `nextval`, ...) | the subquery runs in another statement |
| `LIMIT` / `OFFSET` / `FETCH` / `TABLESAMPLE` | the rows the subquery picks need not be the same |
| more than one output column, or `*` | not an IN list |
| an unqualified column named like the key table's column: `partition_key` (eager TMP), `pcache_key` (the others) | it could bind to the key table instead of an outer table; any other unqualified column binds to the subquery's own tables first, as before, or the setup statement fails |
| alias not tied to p0 by key equalities, including ties only through an outer join | condition 2 above |
| methods without a key table (`IN`, `VALUES`, `IN_SUBQUERY`) | nothing to fold into; the option raises `ValueError` |

Allowed: set-returning functions such as `unnest` (the GeoMine case), aggregates, joins and set operations inside the subquery, and temporary objects of the session (the script runs in the same session).

## Limits

- **Snapshot.** Under READ COMMITTED the subquery is read in the snapshot of the setup statement, not of the main SELECT. That differs only if its tables change during the script.
- **Types and collations.** `k IN S` compares in the key table's column type.
  - Integer, `Decimal`/numeric, timestamp and text keys compare as before.
  - Eager float keys are refused by the key-table methods anyway (#63).
  - Lazy float keys have the cache's float8 type, with the same float4 limit as the lazy path today.
  - A text key column with a non-default or non-deterministic collation (or `char(n)`) compares in the key table's default collation; this is documented, not detected.
- **planner_collapse_limit** also applies to the setup statements. The main statement has fewer relations to collapse.
- **join_every_table:** every table is tied to p0 (its guard checks that), so conditions on any of them fold.
- **Eviction (#45):** the lazy fallback to "all keys" still gives K ⊇ result, so T stays exact.

## On a cache miss

Without cache hits there is no key table, and the query is unchanged. GeoMine, in contrast, applies the method to the metadata set alone.

Folding without hits would mean that PartitionCache rewrites queries it otherwise leaves alone: it would build a key table from the subqueries only. The planner problem exists without the cache too, so that is useful, but it is a separate decision. I propose it as a later option value, `fold_key_subqueries="always"`, after the comparison runs.

## API

- **`fold_key_subqueries: bool = False`** in `extend_query_with_partition_keys(_lazy)` and `apply_cache(_lazy)`. Already in the prototype.
- **In the follow-up PR:** `restrict_with_cache` / `populate_query_variants`, `pcache-monitor --fold-key-subqueries` and `pcache-add --fold-key-subqueries`.
- **Recommendation: default off.**
  - It moves the subquery's work into the setup, changes the plan shape, and removes user conditions from the statement. That should be measured and opted into.
  - It also keeps the earlier runs comparable.
  - After the comparison runs it could become the default for the key-table methods.
- The `IN` and `VALUES` methods leave such subqueries alone; with the option they raise `ValueError`.

## Cost and order of work

The setup reads the cache keys first (VALUES list or lazy cache subquery), then filters them with `k IN (<subquery>)`. PostgreSQL plans this as a hashed subplan or a semi join, and evaluates the subquery once, completely. For konrad that materialises the 2.1 M metadata keys once, as the inline subquery does today.

GeoMine uses `INTERSECT`, which also evaluates both sides completely. The IN filter keeps the condition's own semantics and lets the planner pick hash or merge. The key table is ANALYZEd after the filter; for the unlogged and lazy paths the index is also built after it. So the main statement sees one table with its measured size.

Demo, PostgreSQL 17 (synthetic, not a benchmark): 200,000 pockets, 600,000 points, a 100,000-element property array, 28,572 cache keys, `UNLOGGED_TABLE_IN`, `planner_collapse_limit=20`.

| | key-subquery node | plan |
|---|---|---|
| inline | `HashAggregate` over `ProjectSet unnest(...)`, **estimated 10 rows, actual 100,000** | nested loops driven from the subquery, serial |
| folded | no subquery; key table `Index Only Scan`, estimated 5,952 per worker, actual 4,762 × 3 workers | merge semi join, parallel |

## Prototype and tests

The prototype is `_fold_key_subqueries`, `_unfoldable_reason`, `_key_component` and `_filtered_keys_sql` in `src/partitioncache/apply_cache.py`. The tests are in `tests/pytest/test_fold_key_subqueries.py`.

**Same rows with and without folding, and the same as the query unrestricted:**
- on SQLite and DuckDB for `TMP_TABLE_IN` / `TMP_TABLE_JOIN`;
- on PostgreSQL for all four key-table methods, eager and lazy;
- for these queries:
  - (a) GeoMine-like, with a metadata subquery and with `unnest` (PostgreSQL), in both forms `IN` and `= ANY`;
  - (b) correlated: left alone;
  - (c) NULLs and duplicates in the subquery and the data;
  - (d) two foldable subqueries, one on an alias tied by key equality;
  - plus `NOT IN` and the `OR` context, both left alone.

**Shape:** a folded condition leaves the statement. Unit tests cover every refused case, the inner join versus outer join tie, and the option's default and error.

**PostgreSQL plan:** `unnest` is in the inline plan and gone from the folded one, and the key table's row estimate equals its true size.

Not yet done: the monitor / pcache-add / population pass-through, and the `"always"` mode.
