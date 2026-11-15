"""Shared pytest configuration for the unit tests."""

from partitioncache.cache_handler.thread_safety import ThreadSafeHandler

# No public handler call may return with a write pending: on a shared instance another thread's commit or rollback
# would act on it (cache_handler/thread_safety.py)
ThreadSafeHandler.check_pending_writes = True

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
