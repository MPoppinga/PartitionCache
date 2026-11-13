# Grid-cell spatial keys

A grid-cell key restricts spatial pattern queries (several aliases of one table linked by `ST_DWithin`)
to the cells where a match can lie. It is an **integer partition key**:

```
cell(p) = floor(x / s) << 16 | floor(y / s)        -- x, y in a metric SRID, s = cell size in its units
```

## Why it is sound

- Let D be the diameter of the query's `ST_DWithin` graph over **all** aliases of its outer FROM clause (the longest
  shortest path), counting only `ST_DWithin` conditions that every result row satisfies: top-level conjuncts of the
  outer WHERE and of inner-join ON clauses.
- The fragments hold the cells of every alias they join, including aliases of other tables. Every alias of a match lies
  within D of the points each cached fragment holds for that match.
- Two points within D differ by at most ceil(D/s) cells in each direction.
- So the lookup widens each fragment's cell set by the disk stencil of radius D and intersects the widened sets.
- The result contains the cell of every alias of every match.
- The bound is exact in the SRID that `ST_DWithin` measures in, with no approximation of circles or hexagons.

## Setup

1. Install the cell function in the database that runs the fragments and the queries:

   ```python
   from partitioncache.spatial_grid import CELL_FUNCTION_DDL
   cursor.execute(CELL_FUNCTION_DDL)   # public.partitioncache_grid_cell(geometry, float8) -> integer
   ```

   - The body is schema-qualified, because PostgreSQL 17+ runs `CREATE INDEX` and `ANALYZE` with
     `search_path = pg_catalog, pg_temp`.
   - It has no `SET search_path` clause. Such a clause would also satisfy PostgreSQL 17+, and the index would still match,
     but it stops the planner from inlining the function, so every row it is evaluated for pays a function call.
2. **Required:** check that the table fits the packing (0 ≤ x/s < 2^15 and 0 ≤ y/s < 2^16, plus room for the stencil):
   `spatial_grid.check_extent(cursor, "pois", "geom", s, max_distance)`.
   - A y value that overflows into the x bits gives a valid-looking cell of another place.
   - The lookup cannot detect it, and matches would be lost.
   - Skip this check only if the data's extent is known to fit.
   - Re-run it when rows outside the checked extent are added.
3. Create expression indexes for each cell size in use. The query expression must match them exactly:

   ```sql
   CREATE INDEX ON pois (partitioncache_grid_cell(geom, 200));
   CREATE INDEX ON pois (subtype, partitioncache_grid_cell(geom, 200));   -- a typed column first
   ANALYZE pois;
   ```

Use a sparse-set integer handler: `postgresql_array`, `postgresql_roaringbit`, `redis_set`, `rocksdict` or `rocksdb_set`.

## Population

```python
from partitioncache.query_processor import generate_all_query_hash_pairs
from partitioncache.spatial_grid import GRID_GENERATION_KWARGS

pairs = generate_all_query_hash_pairs(
    query=sql, partition_key="cell200", geometry_column="geom", grid_cell_size=200, **GRID_GENERATION_KWARGS
)
for fragment_sql, h in pairs:      # each fragment selects the distinct cells of its geometry columns
    cache.set_cache_lazy(h, fragment_sql, partition_key="cell200")   # or execute and set_cache
```

- The hash covers the cell projection, so entries of different cell sizes never collide.
- Use the same generation flags (`GRID_GENERATION_KWARGS`) for population and lookup.

## Lookup and rewrite

```python
from partitioncache.apply_cache import apply_cache

rewritten, stats = apply_cache(
    sql, cache, partition_key="cell200", geometry_column="geom", grid_cell_size=200, p0_alias="p1"
)   # grid_method defaults to "CELL_AUTO"
```

`grid_method` decides how the cell set enters the query:

| method | rewrite |
|---|---|
| `CELL_AUTO` (default) | `CELL_ARRAY_ANCHOR` if a non-anchor alias has no equality or IN predicate against literals, otherwise `CELL_STAR` |
| `CELL_STAR` | Per anchor distance, a temporary pair table `(c, nc)` of cells within reach. Every other alias joins it with `nb.c = cell(anchor) AND cell(alias) = nb.nc`, so it is restricted to the cells next to *its* anchor's cell. |
| `CELL_ARRAY` | `cell(alias) = ANY('{…}'::int[])` on every alias of the anchor's table |
| `CELL_ARRAY_ANCHOR` | the same, on the anchor only |

**Behaviour of the lookup:**
- The lookup is always eager: cached sets are fetched and intersected in Python. `apply_cache_lazy` routes to the same path.
- `CELL_STAR` returns several statements: temporary tables `ON COMMIT DROP`, then the query. Run them in one transaction.
- An empty intersection adds a `FALSE` condition.
- Cells outside the packing range make the query run uncached (`stats["uncached_reason"]`).

**Not offered for grid keys:**
- **Temporary-table and CTE forms of a flat key set:** the planner estimates their semi-join with a default group count and
  misplans badly.
- **A lazy in-SQL intersection:** it was slower than the eager lookup in every form tried.

## Limits

- `ST_DWithin` constraints between aliases on the configured geometry column define D. Other distance expressions are
  ignored.
- Every alias of the outer FROM clause, of any table, must be linked to the anchor by such conditions. Otherwise no
  sound restriction exists, and the query runs uncached (`stats["uncached_reason"]`). This happens, for example, when
  the only distance condition is under `NOT` or `OR`, in a subquery, or uses a `::geography` cast.
- Queries with a CTE, a derived table in FROM, a set operation or an outer join also run uncached. Only the anchor
  table's aliases in the outer FROM are restricted.
- So does a join with a table that has no geometry, e.g. a dimension table (`pois a, categories c WHERE a.cat = c.id`).
  Its alias is not linked by `ST_DWithin`, and no distance bounds how far its rows lie from the anchor's.
- The SRID must be metric: the same units as the `ST_DWithin` distances, with non-negative coordinates in range.
