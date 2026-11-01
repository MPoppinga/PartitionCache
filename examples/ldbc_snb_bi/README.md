# LDBC SNB Business Intelligence

PartitionCache benchmark built on the [LDBC Social Network Benchmark, Business
Intelligence workload](https://github.com/ldbc/ldbc_snb_bi).

This is **not** an audited LDBC run. Only 6 of the 20 reference queries are used, each
hand-flattened into a conjunctive select-project-join form, because PartitionCache
cannot decompose CTEs, rejects top-level outer joins, and treats un-flattened `EXISTS`
blocks as opaque. See `claudedocs/ldbc_snb_bi_evaluation.md` for the full triage and
the measured results.

## Setup

```bash
# 1. Fetch a data set (SF1 is 217 MB compressed, ~1.2 GB once loaded)
mkdir -p ~/ldbc-data && cd ~/ldbc-data
curl --silent --fail https://datasets.ldbcouncil.org/bi-pre-audit/bi-sf1-composite-merged-fk.tar.zst \
  | tar -x --use-compress-program=unzstd

# 2. Substitution parameters (LDBC's own curated bindings)
curl -sfLO https://datasets.ldbcouncil.org/bi-pre-audit/parameters-2022-10-01.zip
unzip -q parameters-2022-10-01.zip

# 3. Load into DuckDB and PostgreSQL
python examples/ldbc_snb_bi/generate_ldbc_data.py --data-dir ~/ldbc-data

# 4. Pick bindings that the loaded snapshot can actually answer, then emit queries
python examples/ldbc_snb_bi/select_bindings.py
python examples/ldbc_snb_bi/generate_queries.py --selected examples/ldbc_snb_bi/selected_bindings.json

# 5. Run
python examples/benchmark/run_benchmark.py \
  --config examples/benchmark/config/ldbc_snb_bi_message.yaml \
  --db-backend postgresql --cache-backend postgresql_bit --repeat 3
```

## Schema notes

The generator emits `Post` and `Comment` separately and `Place`/`Organisation`
polymorphically; the BI queries expect the merged schema. `generate_ldbc_data.py`
reproduces the reference derivations (`umbra/load.py`,
`umbra/dml/maintain-views.sql`, `umbra/dml/create-static-materialized-views.sql`):

* `Message` = `Post` + `Comment`, each comment inheriting `RootPostId`,
  `RootPostLanguage` and `ContainerForumId` from the post at the root of its reply
  chain. This is what keeps `ContainerForumId` non-NULL for comments — a NULL there
  would silently defeat any `ContainerForumId IN (...)` cache filter, because
  `NULL IN (...)` is `NULL`.
* `Message_hasTag_Tag`, `Person_likes_Message` = the Post and Comment variants unioned.
* `Person_knows_Person` is stored **symmetrically**, as the reference loader does.
* `City`/`Country` come from `Place`, `Company`/`University` from `Organisation`.

### Dense ids

All entity ids are remapped to dense integers. LDBC ids reach ~3.7e13, while
`postgresql_array` stores an `integer[]` (int4, max 2.1e9) — so the raw ids cannot be
cached at all, and the bit backends would demand absurd bitmaps. Ids are opaque
surrogates, so remapping is semantics-preserving; the loader asserts afterwards that
every partition-key candidate fits in int4, and referential integrity is preserved
across all 20+ referencing columns.

## Two configs

`q11` and `q18` never touch `Message`, so they cannot share a `fact_table` /
`p0_alias` with the rest:

| Config | fact table | partition keys | queries |
|---|---|---|---|
| `ldbc_snb_bi_message.yaml` | `message` | `containerforumid`, `creatorpersonid` | q3, q9, q16, q17 |
| `ldbc_snb_bi_social.yaml` | `person_knows_person` | `person1id` | q11, q18 |

A query is registered only for the partition keys it actually **joins on**. Without such
an attachment join (`<fact>.<pk> = <dim>.<col>`) the fragment generator synthesises a
pairwise `<a>.<pk> = <b>.<pk>` clique across tables that do not have the column, and
the fragments become nonsense.

## PostgreSQL requirements

* No extensions are needed for `postgresql_bit`.
* **`postgresql_array` requires the `intarray` extension** — it intersects cached sets
  with the `&` operator on `integer[]`, which core PostgreSQL does not provide. Without
  it every lookup fails with `operator does not exist: integer[] & integer[]`, is logged
  and swallowed, and the benchmark silently reports 0 cache hits.
* LDBC BI queries reach 12 relations, above `join_collapse_limit` (8) and
  `geqo_threshold` (12). Leaving the defaults sends the planner into GEQO and inflates
  the baseline enormously (q11: >300 s versus 0.5 s once raised) — which would
  manufacture a fake cache speedup. Raise all three to 24.
