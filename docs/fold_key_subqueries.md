# Design note: folding partition-key subqueries into the key table

Status: **prototype with tests, reviewed by PR-Review, for decision**.

Verdict: **works, with these limits:**
- integer and decimal keys by default, text and timestamp keys only on the caller's word, float keys never;
- subqueries whose columns are all qualified;
- queries without bind parameters.

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
- the subquery can run on its own (see "Refused" below);
- the key type allows it (see "Key types").

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
3. **The comparison `k IN S` has the semantics of `alias.key IN S`.** It runs in the key table's column type, not the key column's. That is the same comparison only for some types (see "Key types").

NULLs and duplicates need no special handling. T is filtered by φ's TRUE exactly as the row filter was, and a NULL key never passes either way. The key tables are already sets.

An empty T restricts the query to no rows, which is what the folded conditions together with K imply.

## Refused (the condition stays in the statement)

| Case | Why |
|---|---|
| `NOT IN`, the condition under `OR` / `NOT`, not a top-level conjunct | φ is not a necessary condition of the row |
| correlated subquery (a qualified column of an outer alias) | it has no value on its own |
| a CTE of the outer query referenced in the subquery | not defined in the key table's statement (it could be copied there in a later version) |
| volatile function (`random`, `clock_timestamp`, `statement_timestamp`, `nextval`, ...) | the subquery runs in another statement. The list is best effort, so a user-defined volatile function is not recognised. A truly volatile subquery is nondeterministic unfolded too. `now()`, `current_timestamp` and the other transaction-stable functions are allowed, because the script is one transaction |
| `LIMIT` / `OFFSET` / `FETCH` / `TABLESAMPLE` | the rows the subquery picks need not be the same |
| more than one output column, or `*` | not an IN list |
| any **unqualified column** in the subquery | it may bind to an outer table (an implicitly correlated subquery, which runs today). Moved, it either fails (`UndefinedColumn`, a working query turned into an error) or binds to the key table. Without the schema this cannot be decided, so v1 is strict |
| a **bind parameter** (`%s`, `?`, `%(name)s`, `$n`) anywhere in the query | the folded subquery moves in front of the main statement, and with it the positions of its parameters: found in review, all rows lost |
| alias not tied to p0 by key equalities, including ties only through an outer join | condition 2 above |
| methods without a key table (`IN`, `VALUES`, `IN_SUBQUERY`) | nothing to fold into; the option raises `ValueError` |

Allowed: set-returning functions such as `unnest` (the GeoMine case), aggregates, joins and set operations inside the subquery, and temporary objects of the session (the script runs in the same session).

## Key types

In the key table, `k IN S` compares in the key table's type. A difference from the key column's comparison loses rows silently.

In review: a `char(5)` key with its padded values read back lost every row once folded. The query compares as bpchar, which ignores the padding; the key table compares as text.

So the mode decides which key types fold:

| `fold_key_subqueries` | folds for |
|---|---|
| `False` (default) | nothing |
| `True` | integer and decimal keys (the eager keys' Python types, or the cache's datatype on the lazy path) |
| `"any_key_type"` | also text and timestamp keys. The caller states that the key column compares like text or its timestamp type: no `char(n)`, `citext`, non-default or non-deterministic collation, or domain with its own equality, and matching time zones |

Float keys never fold, because a `real` column compares in double precision.

On the lazy path the keys are not known, so `extend_query_with_partition_keys_lazy` takes `partition_datatype`, which `apply_cache_lazy` reads from the cache handler. Without it, nothing folds.

## Limits

- **Snapshot.** Under READ COMMITTED the subquery is read in the snapshot of the setup statement, not of the main SELECT. That differs only if its tables change during the script.
- **Types:** see "Key types".
- **Older, independent gap found on the way.** The key-table methods and VALUES miss a `char(n)` key column whether or not anything is folded: the text key table compares the padded keys as text. Only IN matches. This is listed separately and not fixed here.
- **planner_collapse_limit** also applies to the setup statements. The main statement has fewer relations to collapse.
- **join_every_table:** every table is tied to p0 (its guard checks that), so conditions on any of them fold.
- **Eviction (#45):** the lazy fallback to "all keys" still gives K ⊇ result, so T stays exact.

- **Cost.** The folded subquery is evaluated completely in the setup, even when the main query would not have needed it (e.g. a cheap outer filter makes the result empty).
- **Population.** `restrict_with_cache` / `populate_query_variants` stay without folding in v1. Their variants are generated SQL whose `key IN (subquery)` conditions the generator handles itself; folding there needs its own design.
- **The konrad statement.** As quoted (`SELECT unnest(pocket_key) FROM ... WHERE prop_key = 39`), it has unqualified columns, so v1 leaves it inline. To fold it, the bench or the SQL generator must qualify the subquery's columns (`SELECT unnest(v.pocket_key) FROM view AS v WHERE v.prop_key = 39`). The alternative, a relaxed opt-in in which a misbinding makes the script fail but never loses rows silently, is for the author to decide.

## On a cache miss

Without cache hits there is no key table, and the query is unchanged. GeoMine, in contrast, applies the method to the metadata set alone.

Folding without hits would mean that PartitionCache rewrites queries it otherwise leaves alone: it would build a key table from the subqueries only. The planner problem exists without the cache too, so that is useful, but it is a separate decision. I propose it as a later option, after the comparison runs; it is not in the first version.

## API

- **`fold_key_subqueries: bool | "any_key_type" = False`** in `extend_query_with_partition_keys(_lazy)` and `apply_cache(_lazy)`, plus `partition_datatype` on the lazy extension. Already in the prototype.
- **In the follow-up PR:** `pcache-monitor --fold-key-subqueries` and `pcache-add --fold-key-subqueries`. Population stays without folding, see "Limits".
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
  - plus `NOT IN` and the `OR` context, both left alone;
- a `char(5)` key that is not folded by default.

**Shape:** a folded condition leaves the statement. Unit tests cover every refused case (including bind parameters and unqualified columns), the key-type gating eager and lazy, the inner join versus outer join tie, and the option's default and error.

**PostgreSQL plan:** `unnest` is in the inline plan and gone from the folded one, and the key table's row estimate equals its true size.

Not yet done: the monitor / pcache-add pass-through.
