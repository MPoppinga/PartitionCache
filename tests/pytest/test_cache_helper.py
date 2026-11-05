"""
Tests for PartitionCacheHelper methods that forward to the cache handler with the helper's partition key.
"""

import logging
from unittest.mock import Mock

import pytest

from partitioncache.cache_handler.helper import PartitionCacheHelper


@pytest.fixture
def rocksdict_handler(tmp_path):
    pytest.importorskip("rocksdict")
    from partitioncache.cache_handler.rocks_dict import RocksDictCacheHandler

    handler = RocksDictCacheHandler(str(tmp_path / "rocksdict"))
    yield handler
    handler.close()


class TestGetQuery:
    def test_returns_the_query_stored_with_the_entry(self, rocksdict_handler):
        helper = PartitionCacheHelper(rocksdict_handler, "city_id", "integer")
        helper.set_entry("h1", {1, 2}, "SELECT * FROM users WHERE status = 'active'")

        assert helper.get_query("h1") == "SELECT * FROM users WHERE status = 'active'"
        assert helper.get_query("missing") is None

    def test_uses_the_helpers_partition_key(self, rocksdict_handler):
        cities = PartitionCacheHelper(rocksdict_handler, "city_id", "integer")
        regions = PartitionCacheHelper(rocksdict_handler, "region_id", "integer")
        cities.set_query("h1", "SELECT city_id FROM cities")

        assert regions.get_query("h1") is None
        assert cities.get_query("h1") == "SELECT city_id FROM cities"

    def test_handler_error_is_logged_and_returns_none(self, caplog):
        handler = Mock()
        handler.get_datatype.return_value = "integer"
        handler.get_query.side_effect = RuntimeError("connection lost")
        helper = PartitionCacheHelper(handler, "city_id", None)

        with caplog.at_level(logging.ERROR, logger="PartitionCache"):
            assert helper.get_query("h1") is None
        handler.get_query.assert_called_once_with(key="h1", partition_key="city_id")
        assert "connection lost" in caplog.text
