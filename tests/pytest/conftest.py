"""Shared pytest configuration for the unit tests."""

try:
    import rocksdict  # noqa: F401
except ImportError:
    # rocksdict has no wheel for every Python version CI tests (e.g. 3.15); these files import it at module level
    collect_ignore = [
        "test_add_to_cache_direct.py",
        "test_query_interface.py",
        "test_rocksdict_cache_handler.py",
        "test_rocksdict_h3_grouped.py",
        "test_rocksdict_roaringbit_cache_handler.py",
        "test_singleton_close_lifecycle.py",
    ]
