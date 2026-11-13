#!/usr/bin/env python3
"""Pick usable substitution-parameter bindings for the loaded LDBC snapshot.

LDBC's curated parameters target the *full* BI data set, i.e. the initial snapshot
with all insert batches applied. Benchmarking against the initial snapshot alone
therefore leaves some bindings pointing past the end of the data (message
creationDate stops at 2012-11-29 at SF1, while parameters reach into December) or
at tags whose neighbourhood is empty. Such bindings produce empty result sets, and
an empty baseline makes any speedup measurement meaningless.

This script walks the official binding list in order and keeps the first N that
actually return rows on the loaded data. The bindings stay LDBC's own -- only the
ones the snapshot cannot answer are skipped, and which ones were skipped is
reported so the selection stays auditable.
"""

from __future__ import annotations

import argparse
import json
import textwrap
import time
from pathlib import Path

import psycopg
from generate_queries import TEMPLATES, read_bindings


def is_useful(rows: list[tuple]) -> bool:
    """Non-empty, and for single-value aggregates the value must be non-zero."""
    if not rows:
        return False
    if len(rows) == 1 and len(rows[0]) == 1:
        return bool(rows[0][0])
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--params-dir", default="/home/poppinga/ldbc-data/params/parameters/parameters-sf1")
    parser.add_argument("--dsn", default="host=127.0.0.1 port=55433 user=ldbc dbname=ldbc")
    parser.add_argument("--count", type=int, default=6, help="bindings to keep per template")
    parser.add_argument("--max-tries", type=int, default=40)
    parser.add_argument("--timeout", type=int, default=90, help="per-query timeout in seconds")
    parser.add_argument("--out", default=str(Path(__file__).parent / "selected_bindings.json"))
    args = parser.parse_args()

    params_dir = Path(args.params_dir)
    selected: dict[str, list[dict[str, str]]] = {}

    with psycopg.connect(args.dsn, autocommit=True) as con:
        con.execute(f"SET statement_timeout = '{args.timeout}s'")
        for name, spec in TEMPLATES.items():
            bindings = read_bindings(params_dir, spec["params"])
            keep: list[dict[str, str]] = []
            skipped = 0
            for binding in bindings[: args.max_tries]:
                if len(keep) >= args.count:
                    break
                sql = textwrap.dedent(spec["sql"]).strip().format(**binding)
                try:
                    start = time.perf_counter()
                    rows = con.execute(sql).fetchall()
                    elapsed = time.perf_counter() - start
                except Exception:
                    skipped += 1
                    continue
                if is_useful(rows):
                    binding = dict(binding, _rows=str(len(rows)), _ms=f"{elapsed*1000:.0f}")
                    keep.append(binding)
                else:
                    skipped += 1
            selected[name] = keep
            print(f"{name}: kept {len(keep)}, skipped {skipped} "
                  f"(times: {', '.join(b['_ms'] for b in keep)} ms)", flush=True)

    Path(args.out).write_text(json.dumps(selected, indent=1))
    print(f"written {args.out}")


if __name__ == "__main__":
    main()
