from typing import List


class AbstractDBHandler:
    def __init__(self):
        raise NotImplementedError

    def execute(self, query) -> List[int]:  # todo auf List[str] generalisieren
        raise NotImplementedError

    def close(self):
        pass
