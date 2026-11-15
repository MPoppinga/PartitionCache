"""One lock per cache handler instance, held for every public method, so that threads can share an instance.

A PostgreSQL or DuckDB handler holds one connection (and one cursor) for all its calls. Threads that share an
instance interleaved their statements on it: a thread's commit or rollback acted on another thread's pending writes,
and a fetch could read another thread's result. ``_read_txn`` (PostgreSQL) rolls back the read transaction it opened,
judged by the connection's status when the read began; with another thread writing in between, that rollback
discarded the other thread's uncommitted INSERT, and its set_cache still returned True. DuckDB's connection crashed.

``ThreadSafeHandler`` gives each instance a re-entrant lock and wraps every public method of a class and its
subclasses, inherited ones included, so that a new method cannot miss it. A method runs, from its first statement to its
commit or rollback, while no other thread uses the instance. That holds only if no public method returns with a write
pending: the next thread's commit or rollback would act on it. ``check_pending_writes`` raises when an outermost call
returns with one; the tests turn it on. A read may leave its transaction open (``get`` does, as before): a later
commit or rollback of a transaction that has only read loses nothing.

Long calls hold the instance: set_cache_lazy runs its variant query under the lock, and other threads on the same
instance wait for it. pcache-monitor's worker threads each get an instance of their own and do not wait.
"""

import inspect
import threading
import types
from collections.abc import Callable
from functools import wraps
from typing import Any, ClassVar

_RLOCK_TYPE = type(threading.RLock())
# Guards the lazy creation of an instance lock for objects made without ThreadSafeHandler.__new__
_LAZY_LOCK = threading.Lock()


def _instance_lock(obj: Any) -> Any:
    """The instance's lock; created on first use for an object made without __new__ (object.__new__, a mock as self)."""
    lock = vars(obj).get("_pcache_lock")
    if not isinstance(lock, _RLOCK_TYPE):
        with _LAZY_LOCK:
            lock = vars(obj).get("_pcache_lock")
            if not isinstance(lock, _RLOCK_TYPE):
                lock = threading.RLock()
                vars(obj)["_pcache_lock"] = lock
                vars(obj)["_pcache_lock_depth"] = 0
    return lock


def _locked(method: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        with _instance_lock(self):
            state = vars(self)
            state["_pcache_lock_depth"] = state.get("_pcache_lock_depth", 0) + 1
            try:
                return method(self, *args, **kwargs)
            finally:
                state["_pcache_lock_depth"] -= 1
                checked = isinstance(self, ThreadSafeHandler) and getattr(type(self), "check_pending_writes", False)
                if checked and state["_pcache_lock_depth"] == 0 and self._has_pending_write():
                    raise AssertionError(
                        f"{type(self).__name__}.{method.__name__} returned with a pending write; another thread's commit or rollback would act on it"
                    )

    wrapper.__pcache_locked__ = True  # type: ignore[attr-defined]
    return wrapper


def _wrap_public_methods(cls: type) -> None:
    """Wrap every public function of ``cls``: its own, and those it inherits from a class without the lock
    (AbstractCacheHandler's set_entry, for one). Staticmethods, classmethods and properties are left alone."""
    for name in dir(cls):
        if name.startswith("_"):
            continue
        value = inspect.getattr_static(cls, name)
        if isinstance(value, types.FunctionType) and not getattr(value, "__pcache_locked__", False):
            setattr(cls, name, _locked(value))


class ThreadSafeHandler:
    """Mixin: a re-entrant lock per instance around every public method of the class and its subclasses."""

    # The tests set this to True: an outermost public call that returns with a write pending raises
    check_pending_writes: ClassVar[bool] = False

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        instance = super().__new__(cls)
        # Before __init__, which may already call public methods
        instance._pcache_lock = threading.RLock()
        instance._pcache_lock_depth = 0
        return instance

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        _wrap_public_methods(cls)

    def _has_pending_write(self) -> bool:
        """Whether the connection holds an uncommitted write; False where a handler cannot tell (DuckDB autocommits)."""
        return False
