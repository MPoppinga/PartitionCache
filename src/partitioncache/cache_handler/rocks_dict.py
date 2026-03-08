from collections import defaultdict
from datetime import datetime
from logging import getLogger

from partitioncache.cache_handler.rocksdict_abstract import RocksDictAbstractCacheHandler

logger = getLogger("PartitionCache")


def _grouped_intersection(fragment_groups: list[list[frozenset[int]]]) -> set[int]:
    """
    Intersect grouped match sets across fragments using connected components.

    Each fragment has a list of match groups (frozenset of H3 cell IDs).
    Two groups from different fragments are "connected" if they share any cell.
    Returns the union of cells from connected components spanning ALL fragments.

    Uses union-find for efficiency.
    """
    num_fragments = len(fragment_groups)
    if num_fragments == 0:
        return set()

    # Single fragment: return union of all cells
    if num_fragments == 1:
        result: set[int] = set()
        for group in fragment_groups[0]:
            result.update(group)
        return result

    # Assign unique ID to each group: (fragment_idx, cells)
    all_groups: list[tuple[int, frozenset[int]]] = []
    for frag_idx, groups in enumerate(fragment_groups):
        for group in groups:
            all_groups.append((frag_idx, group))

    n = len(all_groups)
    if n == 0:
        return set()

    # Build cell -> group indices mapping
    cell_to_groups: dict[int, list[int]] = defaultdict(list)
    for group_idx, (_, cells) in enumerate(all_groups):
        for cell in cells:
            cell_to_groups[cell].append(group_idx)

    # Union-Find with path compression
    parent = list(range(n))
    rank = [0] * n

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: int, y: int) -> None:
        rx, ry = find(x), find(y)
        if rx == ry:
            return
        if rank[rx] < rank[ry]:
            rx, ry = ry, rx
        parent[ry] = rx
        if rank[rx] == rank[ry]:
            rank[rx] += 1

    # Connect groups sharing cells
    for group_indices in cell_to_groups.values():
        for i in range(1, len(group_indices)):
            union(group_indices[0], group_indices[i])

    # Collect components: root -> (fragment indices, cells)
    components: dict[int, tuple[set[int], set[int]]] = {}
    for group_idx, (frag_idx, cells) in enumerate(all_groups):
        root = find(group_idx)
        if root not in components:
            components[root] = (set(), set())
        components[root][0].add(frag_idx)
        components[root][1].update(cells)

    # Keep components spanning all fragments
    surviving: set[int] = set()
    for frag_indices, cells in components.values():
        if len(frag_indices) == num_fragments:
            surviving.update(cells)

    return surviving


class RocksDictCacheHandler(RocksDictAbstractCacheHandler):
    """
    Handles access to a RocksDB cache using RocksDict with native serialization.
    This handler supports multiple partition keys with datatypes: integer, float, text, timestamp.

    Also supports grouped match sets (list[frozenset[int]]) for spatial H3 queries.
    When stored values are lists of frozensets, get_intersected() automatically uses
    connected-component intersection instead of flat set intersection.
    """

    @classmethod
    def get_supported_datatypes(cls) -> set[str]:
        """RocksDict supports all datatypes with native serialization."""
        return {"integer", "float", "text", "timestamp", "geometry"}

    def __repr__(self) -> str:
        return "rocksdict"

    def get(self, key: str, partition_key: str = "partition_key") -> set[int] | set[str] | set[float] | set[datetime] | list[frozenset[int]] | None:
        """Get value from partition-specific cache namespace."""
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None

        cache_key = self._get_cache_key(key, partition_key)
        result = self.db.get(cache_key)
        if result is None or result == "NULL":
            return None

        return result

    def get_intersected(
        self, keys: set[str], partition_key: str = "partition_key"
    ) -> tuple[set[int] | set[str] | set[float] | set[datetime] | None, int]:
        """Returns the intersection of all cached values for the given keys.

        Automatically detects the storage format:
        - set[T]: flat set intersection
        - list[frozenset[int]]: connected-component grouped intersection
        """
        datatype = self._get_partition_datatype(partition_key)
        if datatype is None:
            return None, 0

        values = []
        for key in keys:
            t = self.get(key, partition_key=partition_key)
            if t is not None:
                values.append(t)

        if not values:
            return None, 0

        count_match = len(values)

        # Detect grouped match sets (list[frozenset[int]])
        if isinstance(values[0], list):
            return _grouped_intersection(values), count_match  # type: ignore[arg-type]

        # Flat set intersection
        result: set = values[0]  # type: ignore[assignment]
        for v in values[1:]:
            result = result.intersection(v)
        return result, count_match

    def set_cache(
        self,
        key: str,
        partition_key_identifiers: set[int] | set[str] | set[float] | set[datetime] | list[frozenset[int]],
        partition_key: str = "partition_key",
    ) -> bool:
        """Store partition key identifiers in cache. RocksDict handles serialization.

        Accepts both flat sets (set[int], etc.) and grouped match sets (list[frozenset[int]]).
        """
        existing_datatype = self._get_partition_datatype(partition_key)

        # Grouped match sets: list[frozenset[int]]
        if isinstance(partition_key_identifiers, list):
            if existing_datatype is None:
                self._set_partition_datatype(partition_key, "geometry")
            try:
                cache_key = self._get_cache_key(key, partition_key)
                logger.info(f"saving {len(partition_key_identifiers)} match groups in cache {cache_key}")
                self.db[cache_key] = partition_key_identifiers
                return True
            except Exception:
                return False

        # Flat sets: existing logic
        if existing_datatype is not None:
            if existing_datatype not in ("integer", "float", "text", "timestamp", "geometry"):
                raise ValueError(f"Unsupported datatype in metadata: {existing_datatype}")
        else:
            sample = next(iter(partition_key_identifiers))
            if isinstance(sample, int):
                datatype = "integer"
            elif isinstance(sample, float):
                datatype = "float"
            elif isinstance(sample, str):
                datatype = "text"
            elif isinstance(sample, datetime):
                datatype = "timestamp"
            else:
                raise ValueError(f"Unsupported partition key identifier type: {type(sample)}")
            self._set_partition_datatype(partition_key, datatype)

        try:
            cache_key = self._get_cache_key(key, partition_key)
            logger.info(f"saving {len(partition_key_identifiers)} partition key identifiers in cache {cache_key}")
            self.db[cache_key] = partition_key_identifiers
            return True
        except Exception:
            return False

    def register_partition_key(self, partition_key: str, datatype: str, **kwargs) -> None:
        """Register a partition key with the cache handler."""
        if datatype not in self.get_supported_datatypes():
            raise ValueError(f"Unsupported datatype: {datatype}")
        self._set_partition_datatype(partition_key, datatype)
