import abc


class AbstractDBHandler(abc.ABC):

    @abc.abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def execute(self, query) -> list[int]:
        """Execute a query and return the results."""
        raise NotImplementedError

    def estimate_cost(self, query: str) -> float | None:
        """Return the planner's total cost estimate for ``query`` without executing it.

        Backends without a scalar cost model (DuckDB, SQLite) return None, which ``apply_cache``'s
        ``min_query_cost`` gate treats as "apply the cache".
        """
        return None

    @abc.abstractmethod
    def close(self):
        pass
