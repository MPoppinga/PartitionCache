# Design note: folding partition-key subqueries into the key table

Status: **implemented as an opt-in (`fold_key_subqueries`, default off); the design was reviewed by PR-Review and approved by the author on 2026-11-15**. The key table's defining query was changed the same day to the INTERSECT form below, after C2's smoke found the first form's setup up to 50× slower (see "The key table's defining query").

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

A folded condition leaves the statement. The key table keeps only the keys for which `key IN (<subquery>)` is TRUE, K ∩ S1 ∩ S2 ... for the cache keys K and the subqueries Si:

| Path | Key table built as |
|---|---|
| PostgreSQL, every key-table method, eager and lazy | created in the keys' type, then `WITH pcache_k_<hex> AS MATERIALIZED (<cache keys without NULLs>) INSERT INTO t SELECT k FROM pcache_k_<hex> INTERSECT SELECT * FROM (<S1>) AS pcache_folded_1 INTERSECT ...`, then index and `ANALYZE` |
| SQLite, DuckDB: eager `TMP_TABLE_*` | `INSERT ... VALUES (<cache keys>)`, then per subquery `DELETE FROM t WHERE (partition_key IN (<subquery>)) IS NOT TRUE`, then `ANALYZE` |
| SQLite, DuckDB: lazy `TMP_TABLE_*` | `CREATE ... AS (SELECT k FROM (<cache keys>) ... WHERE k IN (<S1>) AND k IN (<S2>))` |

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

NULLs and duplicates: T is filtered by φ's TRUE exactly as the row filter was, and a NULL key never passes either way. The key tables are already sets. For the set-operation form see "The key table's defining query".

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
| an **unqualified column** that does not bind inside the subquery, or cannot be decided | see "Unqualified columns" below: with a column resolver it folds when it binds inside; without one it is refused, unless the caller states it with `fold_assume_inner_columns` |
| a **bind parameter** (`%s`, `?`, `%(name)s`, `$n`) anywhere in the query | the folded subquery moves in front of the main statement, and with it the positions of its parameters: found in review, all rows lost |
| alias not tied to p0 by key equalities, including ties only through an outer join | condition 2 above |
| methods without a key table (`IN`, `VALUES`, `IN_SUBQUERY`) | nothing to fold into; the option raises `ValueError` |
| SQL dialects other than PostgreSQL, DuckDB and SQLite | untested (MySQL has `INTERSECT` only from 8.0.31); the option raises `ValueError` |

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
- **Population.** `restrict_with_cache` / `populate_query_variants` fold when asked (`pcache-monitor` / `pcache-add --fold-key-subqueries`).
  - The variants are generated SQL. The generator keeps a `key IN (subquery)` condition with its subquery text, re-aliased to the variant's key alias, and also caches the subquery itself as a variant.
  - The equivalence argument above does not depend on where the query comes from. A restricted variant returns the same keys with and without folding, so the stored entry is the same (test: `TestPopulation`).
  - The subquery's own variant has no such condition, so nothing folds there.
- **The konrad statement and GeoMine's SQL** have unqualified columns (`SELECT unnest(pocket_key) FROM ... WHERE prop_key = 39`). Until 2026-11-15 they were never folded; C2/C2b measured alias-qualified subqueries (the bench harness qualified them), so their folded numbers are for a statement text GeoMine does not send. Unqualified columns are now resolved by scoping (next section).

## On a cache miss

Without cache hits there is no key table, and the query is unchanged. GeoMine, in contrast, applies the method to the metadata set alone.

Folding without hits would mean that PartitionCache rewrites queries it otherwise leaves alone. It would build a key table from the subqueries only. The planner problem exists without the cache too, so that is useful.

**Proposed as a separate PR: `fold_key_subqueries="always"`.** On a cache miss, `apply_cache(_lazy)` builds the key table from the foldable subqueries alone, `T = S1 ∩ S2 ∩ ...`, and restricts the query with it. With hits it behaves as `True`.
- **Key types.** With no cache keys, T has the subquery's own column type, so `pl.key IN (SELECT k FROM T)` compares exactly as `pl.key IN (S1)` did. The key-type gating is not needed for this case.
- **Dialects.** The table is built as `CREATE ... AS (SELECT s.v AS <key> FROM (S1) AS s(v) WHERE s.v IN (S2) ...)`, using a column alias list. That works in PostgreSQL and DuckDB, not in SQLite.
- **Size.** It changes the no-hit returns of both apply functions and the lazy extension's key source, about 40 lines plus tests. That is more than a flag, hence a PR of its own after this one.

## API

- **`fold_key_subqueries: bool | "any_key_type" = False`** in `extend_query_with_partition_keys(_lazy)` and `apply_cache(_lazy)`, plus `partition_datatype` on the lazy extension. Already in the prototype.
- **In the follow-up PR:** `pcache-monitor --fold-key-subqueries` and `pcache-add --fold-key-subqueries`. Population stays without folding, see "Limits".
- **Recommendation: default off.**
  - It moves the subquery's work into the setup, changes the plan shape, and removes user conditions from the statement. That should be measured and opted into.
  - It also keeps the earlier runs comparable.
  - After the comparison runs it could become the default for the key-table methods.
- The `IN` and `VALUES` methods leave such subqueries alone; with the option they raise `ValueError`.

## Unqualified columns

GeoMine writes its metadata subqueries with bare columns. On 09g's GeoMine queue, all 56 inline fragments have this form:

```sql
... t1.pocket_key IN (SELECT ..._pocket_ligands.pocket_key FROM ..._pocket_ligands
                      INTERSECT SELECT UNNEST(..._pockets_num.pocket_key) AS pocket_key FROM ..._pockets_num
                      WHERE prop_key = 84 AND prop_value <= 100 AND prop_value >= 1)
```

Moved out of the query, such a column must bind as it did in the query.

**The rule.** SQL binds an unqualified name to the innermost query level whose FROM items have a column of that name. Each unqualified column of a candidate subquery is walked from its own SELECT outward, up to the subquery's root (each branch of a set operation is such a SELECT).
- **Inside:** some level's FROM items have the column. It folds.
- **Outside:** no level has it, so the subquery is correlated. It stays in the statement.
- **Undecidable:** a FROM item on the way has unknown columns. It stays.

Names that are no FROM column end up as "outside or undecidable", which is the safe side:
- a SELECT-list alias in ORDER BY or GROUP BY;
- a system column such as `ctid` (the catalog lookup leaves out `attnum <= 0`);
- a whole-row reference;
- a keyword-like value that sqlglot parses as a column.

LATERAL inside the subquery is refused.

**Where the columns come from:**

| FROM item | columns |
|---|---|
| table, view, materialized view, foreign or temporary table (also schema-qualified or quoted) | `fold_column_resolver(<relation as written>)` |
| CTE defined in the subquery | the names of its SELECT list or its column alias list (an unnamed output, or `*`: unknown) |
| derived table | its column alias list, else its SELECT list's names (`*`: unknown) |
| function, `unnest`, `VALUES` | only its column alias list (`unnest(x) AS u(v)`) |

**The resolver.** `catalog_column_resolver(execute, sql_dialect)` reads the catalog, and caches each relation for the resolver's life:
- **PostgreSQL:** `pg_attribute` via `to_regclass` of the name as written, so quotes, schema and the search_path count.
- **SQLite:** `pragma_table_info`.
- **DuckDB:** `information_schema.columns`.

It must run on the connection that executes the query, before the script runs: there the search_path and the session's temporary tables (which shadow permanent ones) are those of the query. A resolver from another connection, or from a schema dictionary, is the caller's statement that the names resolve the same way.

Who builds one:
- `populate_query_variants` builds one per run from its db_handler.
- `pcache-monitor` builds one per fragment, on a connection of its own to the queried database.
- `extend_query_with_partition_keys(_lazy)` and `apply_cache(_lazy)` take it as `fold_column_resolver`. Without it, unqualified columns stay refused.

**The caller's word.** `fold_assume_inner_columns=True` (default False, API only, not in population, the monitor or pcache-add) takes every unqualified column as inside.
- **PostgreSQL:** every fold form puts the subquery as a derived table into an INTERSECT of the key table's statement, with no query level around it. A column that really bound outward then fails with "column does not exist"; it cannot bind elsewhere. The tests check this for the eager and lazy temporary and unlogged tables, and the typed staging table, with columns named `partition_key`, `pcache_key` and the key column's name.
- **SQLite and DuckDB:** they put the subquery into a statement over the key table (`DELETE ... WHERE partition_key IN (S)`, `pcache_folded.pcache_key IN (S)`), so those two names stay refused there.

## The key table's defining query

C2's smoke on merapi (2026-11-15) measured the first form, `SELECT k ... WHERE k IN (S)`, against GeoMine's `(K) INTERSECT (S)`. The final SELECTs were identical, but the CREATE took 1.1 s against 0.4 s (konrad_fast), and 8.8 s against 0.17 s (1xkk at 1e4 keys).

**What went wrong in the first form** (reproduced on PostgreSQL 17 with synthetic data):
1. `k IN (S)` is a semi join. PostgreSQL de-duplicates and hashes all of S, whose output is estimated at the default 200 groups.
2. The bit handler's lazy keys, `unnest(COALESCE(<array subquery>, ...))`, are estimated at 10 rows. The nested loop over S then costs the planner about the same as the hash plan (within 0.1 %). Forced locally, it takes 8.9 s: the 1xkk case.
3. The eager `TMP_TABLE_*` path filtered with `DELETE ... WHERE (k IN (S)) IS NOT TRUE`. That is a subplan, hashed only when the estimated S fits in hash_mem; otherwise S is scanned once per key (cancelled after 30 s).

**The form now.** `WITH pcache_k AS MATERIALIZED (<keys>) SELECT k FROM pcache_k INTERSECT SELECT * FROM (S1) AS pcache_folded_1 INTERSECT ...`
- A set operation has no join: no nested loop, no subplan. Whatever the estimates, the cost is linear in its inputs; without enough hash memory it becomes a sorted SetOp, O(n log n).
- HashSetOp hashes the input with fewer estimated groups and streams the other, and a tie keeps the left.
- The keys are a MATERIALIZED CTE. Without statistics they are estimated at the default number of groups, like an unnest. So they stay the hashed side unless a subquery's statistics show fewer groups, and then that subquery is the small side.
- A VALUES list read directly is estimated higher and lost to an unnest of millions of rows (1.4 s below; 0.36 s as a CTE).

Defining query alone, ms, medians of 3. S unnests 2.2 M keys ("large"), 26 K ("small"), or is two selective subqueries ("two small").

| keys | S | first form `k IN (S)` | `K EXCEPT (K EXCEPT S)` | INTERSECT, keys as a MATERIALIZED CTE (this form) |
|---|---|---|---|---|
| 1e4, generate_series | large | 3,055 | 393 | 358 |
| 1e4, the bit handler's lazy keys | large | 2,768 | 477 | 483 |
| 1e4, eager VALUES | large | 2,615 | 337 | 355 |
| 1e4, the bit handler's | small / two small | 161 / 171 | 159 / 174 | 165 / 172 |
| 2.1 M, all keys of the table | large | 3,534 | 2,937 | 3,692 |
| 2.1 M, all keys of the table | small / two small | 303 / 358 | 2,586 / 4,259 | 893 / 739 |

`(K) INTERSECT (S)` with an eager VALUES list read directly took 1,387 ms against the large S.

The EXCEPT pair hashes the keys in every case: good with few keys, slow with many keys and a selective subquery. The first form wins only in that last case, where its semi join hashes the small S (303 against 893 ms). **The choice is robustness, not the minimum in each row**: the set operation has no join or subplan that a misestimate turns into a nested loop or a per-key rescan, and its worst case is linear.

**Why it is the same set as `k IN (S)`.**
- **Chaining.** K ∩ S1 ∩ S2 is the AND of the folded conditions.
- **NULL.** Set operations treat NULL as equal to NULL, while `k IN (S)` is never TRUE for a NULL k. So the keys are taken without their NULLs: the lazy keys through `WHERE k IS NOT NULL`, while the eager keys are Python values and never NULL. Then the intersection holds no NULL, and a NULL of a subquery matches nothing.
- **Types.**
  - The set operation compares in the common type of the keys and the subquery's column; `k IN (S)` resolved `=` between the same two types. For the kinds that fold, both are the same equality:
    - int2/int4/int8: the cross-type operators equal the comparison as bigint;
    - integer with numeric: numeric;
    - text with varchar: text;
    - timestamp with timestamptz: both through the session time zone.
  - Incompatible types (integer with text) are an error in both forms.
  - With deterministic collations both compare bytewise (checked: a `"C"` column against a default-collation column gives the same rows). Non-deterministic collations are excluded by the `"any_key_type"` statement.
- **Column type of the key table.** The common type can be wider than the keys' (integer keys, a numeric subquery). A numeric key table would hide the index on the key column from the main statement. So the table is created in the keys' type first:
  - eager: the declared key type;
  - lazy: `CREATE ... AS (SELECT k FROM (<lazy keys>) ... LIMIT 0)`, which plans but does not run them (3 ms with the bit handler's keys; a test runs keys that fail when executed). The all-keys fallback of #45 is replaced in both statements;
  - the INSERT then assigns the kept keys, a subset of the keys, to that type.
- **Subqueries.**
  - Each Si is a derived table, `SELECT * FROM (<Si>) AS pcache_folded_i`. A set-returning target list (`SELECT unnest(...)`), a `UNION` or an `ORDER BY` inside Si stay inside it, and precedence cannot change.
  - A derived table sees nothing of the setup statement except its CTE. The CTE's name has a random suffix, so that a table of Si cannot bind to it.

**Other dialects.**
- SQLite applies the key column's affinity in `k IN (S)` but not in a set operation, where the integer 1 and the text '1' differ. So SQLite keeps the IN filter, and so does DuckDB.
- Neither has PostgreSQL's per-key subplan rescans: SQLite builds an index on the IN subquery once, DuckDB a hash join. Checked for DuckDB: 1e4 keys against 2.2 M subquery rows, 233 ms folded against 246 ms unfolded (a unit test runs 2,000 keys against 400,000 rows).
- No case falls back: the form depends on the dialect only.

**Cost.**
- **Materialising the keys' CTE** costs about nothing: with 1.05 M lazy keys, 375 ms against 393 ms for creating the unfolded key table; it spills 14 MB to temporary files.
- **Every folded subquery is evaluated completely in the setup**, once, as GeoMine's `INTERSECT` does.
- **The key table is ANALYZEd after filling**, so the main statement sees one table with its measured size.

**Versions.** Measured on PostgreSQL 17. The tests assert what holds across versions (CI runs 16): a `SetOp Intersect` node, and no SubPlan, semi join, nested loop or hash join in the setup's plans, with the subquery far above `work_mem`. Which input is hashed is planner behaviour and is not asserted.

**Demo of the planner effect,** PostgreSQL 17 (synthetic, not a benchmark): 200,000 pockets, 600,000 points, a 100,000-element property array, 28,572 cache keys, `UNLOGGED_TABLE_IN`, `planner_collapse_limit=20`.

| | key-subquery node | plan |
|---|---|---|
| inline | `HashAggregate` over `ProjectSet unnest(...)`, **estimated 10 rows, actual 100,000** | nested loops driven from the subquery, serial |
| folded | no subquery; key table `Index Only Scan`, estimated 5,952 per worker, actual 4,762 × 3 workers | merge semi join, parallel |

## Implementation and tests

The implementation is `_fold_key_subqueries`, `_unfoldable_reason`, `_key_component`, `_kept_keys_sql`, `_folded_fill_sql`, `_typed_like_keys_sql` (PostgreSQL) and `_filtered_keys_sql` (the other dialects) in `src/partitioncache/apply_cache.py`. The tests are in `tests/pytest/test_fold_key_subqueries.py`.

**Same rows with and without folding, and the same as the query unrestricted:**
- on SQLite and DuckDB for `TMP_TABLE_IN` / `TMP_TABLE_JOIN`;
- on PostgreSQL for all four key-table methods, eager and lazy;
- for these queries:
  - (a) GeoMine-like, with a metadata subquery and with `unnest` (PostgreSQL), in both forms `IN` and `= ANY`;
  - (b) correlated: left alone;
  - (c) NULLs and duplicates in the subquery and the data;
  - (d) two foldable subqueries, one on an alias tied by key equality;
  - (e) a subquery with a `UNION` inside, and NULLs among the lazy cache keys;
  - plus `NOT IN` and the `OR` context, both left alone;
- a `char(5)` key that is not folded by default.

**Shape:** a folded condition leaves the statement. Unit tests cover every refused case (including bind parameters and unqualified columns), the key-type gating eager and lazy, the inner join versus outer join tie, and the option's default and error.

**PostgreSQL plan:** `unnest` is in the inline plan and gone from the folded one, and the key table's row estimate equals its true size. The setup's plans hold a `SetOp Intersect` and no SubPlan, semi join, nested loop or hash join, with the subquery far above `work_mem` (the first form failed this). The key table has the unfolded table's column type for integer keys with a numeric subquery, holds no NULL, and a table named `pcache_k` inside a subquery does not bind to the keys' CTE.

**SQLite:** an integer key with a TEXT subquery column keeps its rows (the IN filter's affinity).

The monitor and pcache-add flags and folding in population (`restrict_with_cache`, `populate_query_variants`) are in the stacked PR.
