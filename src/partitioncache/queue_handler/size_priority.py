"""Fragment queue priority by fragment size: the smaller fragments of all queued queries are popped first.

The fragment queue pops the highest priority first. With size priority, a new fragment gets the priority

    SIZE_PRIORITY_BASE - relations

where ``relations`` is the number of tables in its outer FROM. A push of a fragment that is already waiting adds 1,
as without size priority. A query that is pushed again pushes all its fragments again, its smaller sub-fragments
included, so they all gain 1 and keep their order; the fragments of a query pushed more often can pop before
smaller fragments of other queries. The priority column is a signed INTEGER, so fragments with more than
SIZE_PRIORITY_BASE relations get priorities below 0 and still pop after the smaller ones.

Without size priority every fragment is pushed with priority 1, and a push of a waiting fragment adds 1.
"""

import os

from partitioncache.query_processor import variant_size

SIZE_PRIORITY_ENV = "QUERY_QUEUE_SIZE_PRIORITY"
SIZE_PRIORITY_BASE = 10


def size_priority_enabled(size_priority: bool | None = None) -> bool:
    """Whether fragments are pushed with size priority: the explicit setting, else QUERY_QUEUE_SIZE_PRIORITY (default on)."""
    if size_priority is not None:
        return size_priority
    return os.getenv(SIZE_PRIORITY_ENV, "true").strip().lower() in ("true", "1", "yes")


def fragment_size_priority(query: str) -> int:
    """The priority a new fragment gets with size priority; 1 (as without it) for a statement that is not a single SELECT."""
    size = variant_size(query)
    if size is None:
        return 1
    return SIZE_PRIORITY_BASE - size[0]
