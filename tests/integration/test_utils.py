"""
Utility functions for integration tests.
"""

def compare_cache_values(retrieved, expected):
    """
    Helper function to compare cache values across different backend types.
    
    Args:
        retrieved: Value returned from cache backend (could be set, BitMap, etc.)
        expected: Expected set value
    
    Returns:
        bool: True if values are equivalent
    """
    # Handle None/empty cases
    if retrieved is None and not expected:
        return True
    if retrieved is None:
        return False
    if not expected and not retrieved:
        return True

    # Handle BitMap objects from roaringbit backend
    try:
        from pyroaring import BitMap
        if isinstance(retrieved, BitMap):
            return set(retrieved) == expected
    except ImportError:
        pass

    # Handle regular sets and other iterable types
    if hasattr(retrieved, '__iter__') and not isinstance(retrieved, (str, bytes)):
        return set(retrieved) == expected

    # Handle single values
    if not hasattr(expected, '__iter__') or isinstance(expected, (str, bytes)):
        return retrieved == expected

    # Default comparison
    return retrieved == expected


def normalize_cache_result(value):
    """
    Normalize cache results to Python sets for consistent comparison.
    
    Args:
        value: Value from cache backend
        
    Returns:
        set: Normalized set representation
    """
    if value is None:
        return set()

    # Handle BitMap objects from roaringbit backend
    try:
        from pyroaring import BitMap
        if isinstance(value, BitMap):
            return set(value)
    except ImportError:
        pass

    # Handle iterable types (but not strings/bytes)
    if hasattr(value, '__iter__') and not isinstance(value, (str, bytes)):
        return set(value)

    # Handle single values
    return {value} if value else set()
