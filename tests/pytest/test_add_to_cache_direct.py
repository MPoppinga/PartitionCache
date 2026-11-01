"""pcache-add --direct stores every variant result, including empty ones, against a real SQLite database."""

import sqlite3
import sys
from unittest.mock import patch

from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler
from partitioncache.cli import add_to_cache
from partitioncache.query_processor import generate_all_query_hash_pairs

QUERY = "SELECT t.pk FROM t, d WHERE t.k = d.k AND d.x = 7"


def test_empty_variant_result_is_stored(tmp_path, monkeypatch):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    # Every table carries the partition key, as the variant generator assumes without partition_key_source_table
    conn.executescript("CREATE TABLE t (pk INTEGER, k INTEGER); CREATE TABLE d (pk INTEGER, k INTEGER, x INTEGER);")
    conn.executemany("INSERT INTO t VALUES (?, ?)", [(pk, pk % 3) for pk in range(10)])
    conn.executemany("INSERT INTO d VALUES (?, ?, ?)", [(pk, pk % 3, pk % 3) for pk in range(10)])  # no row has x = 7
    conn.commit()
    conn.close()

    cache_path = tmp_path / "cache"
    env_file = tmp_path / "empty.env"
    env_file.write_text("")
    monkeypatch.setenv("CACHE_BACKEND", "rocksdict")
    monkeypatch.setenv("ROCKSDB_DICT_PATH", str(cache_path))
    argv = ["pcache-add", "--direct", "--query", QUERY, "--partition-key", "pk", "--partition-datatype", "integer",
            "--db-backend", "sqlite", "--db-dir", str(db_path), "--env-file", str(env_file), "--min-component-size", "1"]
    with patch.object(sys, "argv", argv):
        add_to_cache.main()

    pairs = generate_all_query_hash_pairs(QUERY, "pk", min_component_size=1, keep_all_attributes=True, warn_no_partition_key=False)
    handler = RocksDictCacheHandler(str(cache_path), read_only=True)
    try:
        stored = {h: handler.get(h, "pk") for _, h in pairs}
    finally:
        handler.close()
    assert stored and all(value is not None for value in stored.values()), stored
    assert set() in stored.values()  # the variants with d.x = 7 have no rows
