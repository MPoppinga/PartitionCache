#!/usr/bin/env python3
"""Compare the variant hashes of two PartitionCache source trees over a corpus of statements.

For a change that must not alter a single hash (a performance change to variant generation, say): each tree computes,
for every statement, the population side (generate_all_query_hash_pairs) and the lookup side (generate_all_hashes)
with the given settings, in subprocesses with PYTHONHASHSEED=0. The script reports every statement whose hashes
differ and exits 1 if any does.

A corpus file is JSON: a list of groups ``{"name", "partition_key", "population": {...}, "lookup": {...},
"statements": [paths or globs], "csv_sql_column": "sql"}``. A statement path is a .sql file or a .csv(.gz) whose
``csv_sql_column`` holds one statement per row. Usage::

    python scripts/compare_variant_hashes.py --old-src <tree>/src --new-src <tree>/src corpus.json [--jobs 8]
"""

import argparse
import csv
import glob
import gzip
import io
import json
import os
import subprocess
import sys
import time
from pathlib import Path

WORKER = r"""
import json, sys, time
from concurrent.futures import ProcessPoolExecutor
from partitioncache.query_processor import generate_all_hashes, generate_all_query_hash_pairs

def one(job):
    sql, partition_key, population, lookup = job
    start = time.perf_counter()
    try:
        pairs = sorted(h for _, h in generate_all_query_hash_pairs(sql, partition_key, **population))
    except Exception as e:
        pairs = [f"ERROR {type(e).__name__}: {e}"]
    middle = time.perf_counter()
    try:
        hashes = sorted(generate_all_hashes(sql, partition_key, **lookup))
    except Exception as e:
        hashes = [f"ERROR {type(e).__name__}: {e}"]
    return pairs, hashes, middle - start, time.perf_counter() - middle

jobs = json.load(sys.stdin)
with ProcessPoolExecutor(int(sys.argv[1])) as pool:
    print(json.dumps(list(pool.map(one, [tuple(j) for j in jobs], chunksize=1))))
"""


def _statements(group: dict, base: Path) -> list[tuple[str, str]]:
    column = group.get("csv_sql_column", "sql")
    out: list[tuple[str, str]] = []
    for pattern in group["statements"]:
        for path in sorted(glob.glob(str(base / pattern))):
            if path.endswith(".sql"):
                out.append((Path(path).name, Path(path).read_text().strip().rstrip(";")))
                continue
            opener = gzip.open(path, "rt") if path.endswith(".gz") else open(path)
            with opener as handle:
                for i, row in enumerate(csv.DictReader(io.StringIO(handle.read()))):
                    out.append((f"{Path(path).name}#{i}", row[column]))
    seen: dict[str, str] = {}
    for label, sql in out:
        seen.setdefault(sql, label)  # a statement in several queue orders is compared once
    return [(label, sql) for sql, label in seen.items()]


def _run(src: str, jobs: list, workers: int) -> list:
    env = {**os.environ, "PYTHONPATH": src, "PYTHONHASHSEED": "0", "PYTHONDONTWRITEBYTECODE": "1"}
    done = subprocess.run([sys.executable, "-c", WORKER, str(workers)], input=json.dumps(jobs), env=env, capture_output=True, text=True)
    if done.returncode != 0:
        sys.exit(f"{src} failed:\n{done.stderr[-3000:]}")
    return json.loads(done.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("corpus", type=Path)
    parser.add_argument("--old-src", required=True)
    parser.add_argument("--new-src", required=True)
    parser.add_argument("--base", type=Path, default=None, help="directory the corpus paths are relative to (default: the corpus file's)")
    parser.add_argument("--jobs", type=int, default=8)
    args = parser.parse_args()
    groups = json.loads(args.corpus.read_text())
    base = args.base or args.corpus.parent
    differing = 0
    for group in groups:
        statements = _statements(group, base)
        jobs = [(sql, group["partition_key"], group.get("population", {}), group.get("lookup", {})) for _, sql in statements]
        timings = {}
        results = {}
        for side, src in (("old", args.old_src), ("new", args.new_src)):
            start = time.perf_counter()
            results[side] = _run(src, jobs, args.jobs)
            timings[side] = time.perf_counter() - start
        bad = [
            (label, kind)
            for (label, _), old, new in zip(statements, results["old"], results["new"], strict=True)
            for kind, index in (("population", 0), ("lookup", 1))
            if old[index] != new[index]
        ]
        differing += len(bad)
        population = sum(len(r[0]) for r in results["new"])
        lookup = sum(len(r[1]) for r in results["new"])
        lookup_old = sum(r[3] for r in results["old"])
        lookup_new = sum(r[3] for r in results["new"])
        print(
            f"{group['name']}: {len(statements)} statements, {population} population and {lookup} lookup hashes; "
            f"{len(bad)} differ; lookup time {lookup_old:.1f} s old, {lookup_new:.1f} s new (summed over statements); "
            f"wall {timings['old']:.0f} s / {timings['new']:.0f} s",
            flush=True,
        )
        for label, kind in bad[:20]:
            print(f"  DIFFERS {kind}: {label}")
    print("ALL IDENTICAL" if differing == 0 else f"{differing} DIFFERENCES")
    return 0 if differing == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
