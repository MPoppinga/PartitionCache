from typing import List
import abc

class AbstractDBHandler(abc.ABC):
    
    @abc.abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abc.abstractmethod
    def execute(self, query) -> List[int]:
        """Execute a query and return the results."""
        raise NotImplementedError

    @abc.abstractmethod
    def close(self):
        pass
