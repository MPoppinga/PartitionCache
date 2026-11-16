"""Materialised intersections when cache entries vanish between the existence check and the intersection.

Eviction, delete or set_null by another process can remove an entry the handler has just found. An intersection over
no entry is "unknown" (None), never the empty set: an empty set removes every row from the restricted query. The
array handler's aggregate and the roaring handler skip a vanished entry; these paths returned set() instead.
"""

from unittest.mock import Mock

import pytest

from partitioncache.cache_handler.postgresql_array import PostgreSQLArrayCacheHandler
from partitioncache.cache_handler.postgresql_bit import PostgreSQLBitCacheHandler


def _handler(cls):
    h = cls.__new__(cls)
    h.db = Mock()
    h.cursor = Mock()
    h.tableprefix = "t"
    h._cached_datatype = {"k": "integer"}
    return h


@pytest.fixture
def bit_handler():
    return _handler(PostgreSQLBitCacheHandler)


@pytest.fixture
def array_handler():
    h = _handler(PostgreSQLArrayCacheHandler)
    h.array_intersect_agg_available = True
    return h


def test_bit_intersection_of_entries_deleted_after_the_check_is_unknown(bit_handler):
    bit_handler.cursor.fetchall.return_value = [("h1",), ("h2",)]  # both found by the existence check
    bit_handler.cursor.fetchone.return_value = (None,)  # BIT_AND over no row left
    assert bit_handler.get_intersected({"h1", "h2"}, "k") == (None, 0)


def test_bit_intersection_without_set_bits_is_empty(bit_handler):
    bit_handler.cursor.fetchall.return_value = [("h1",), ("h2",)]
    bit_handler.cursor.fetchone.return_value = ("0000",)
    assert bit_handler.get_intersected({"h1", "h2"}, "k") == (set(), 2)


def test_bit_dilated_intersection_of_entries_deleted_after_the_check_is_unknown(bit_handler):
    bit_handler.filter_existing_keys = Mock(return_value={"h1", "h2"})
    bit_handler.cursor.fetchone.return_value = (None,)  # the set-bits array of a NULL BIT_AND
    bit_handler.cursor.fetchall.return_value = []  # what unnest(COALESCE(NULL, NULL)) yields
    assert bit_handler.get_intersected_dilated({"h1", "h2"}, "k", dilation=1) == (None, 0)


def test_bit_dilated_intersection_reads_the_set_bits(bit_handler):
    bit_handler.filter_existing_keys = Mock(return_value={"h1", "h2"})
    bit_handler.cursor.fetchone.return_value = ([3, 4],)
    bit_handler.cursor.fetchall.return_value = [(3,), (4,)]
    assert bit_handler.get_intersected_dilated({"h1", "h2"}, "k", dilation=1) == ({3, 4}, 2)


def test_array_dilated_intersection_with_an_entry_deleted_after_the_check_is_unknown(array_handler):
    array_handler.filter_existing_keys = Mock(return_value={"h1", "h2"})
    array_handler.cursor.fetchone.return_value = (False, [])  # not every entry present any more
    array_handler.cursor.fetchall.return_value = []  # what the INTERSECT over a missing entry yields
    assert array_handler.get_intersected_dilated({"h1", "h2"}, "k", dilation=1) == (None, 0)


def test_array_dilated_intersection_of_present_entries(array_handler):
    array_handler.filter_existing_keys = Mock(return_value={"h1", "h2"})
    array_handler.cursor.fetchone.return_value = (True, [])
    array_handler.cursor.fetchall.return_value = []
    assert array_handler.get_intersected_dilated({"h1", "h2"}, "k", dilation=1) == (set(), 2)
