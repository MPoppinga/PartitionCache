"""Temporal dilation for bucketed partition keys.

A correlation query links two events by a bounded window ("a failed logon followed by a
successful one within an hour"). When the partition key is a time bucket, the two events can
fall in *adjacent* buckets, so intersecting the per-fragment bucket sets is unsound: the
anchor's bucket may be missing from the other fragment's set even though the pair is a real
match.

This is the one-dimensional counterpart of the spatial buffer. ``postgis_bbox`` buffers each
cached geometry *before* intersecting them; here each cached bucket set is dilated by ``k``
buckets before intersection, with ``k = ceil(window / bucket_width)``.

The order matters and is not negotiable. Dilating the *result* of an intersection is unsound,
because the undilated intersection may already be empty::

    dilate(A ∩ B)  ⊆  dilate(A) ∩ dilate(B)

Soundness argument: if two correlated events satisfy ``|t2 - t1| <= W`` then their buckets
satisfy ``|b2 - b1| <= ceil(W / width)``, so the anchor bucket ``b1`` is always contained in
the dilated set built from ``b2``.
"""

import math

__all__ = ["compute_dilation_buckets", "dilate_bucket_set"]


def compute_dilation_buckets(window: float, bucket_width: float) -> int:
    """Return the number of buckets a cached set must be dilated by.

    Args:
        window: Correlation window in the same unit as ``bucket_width`` (e.g. seconds).
            Typically obtained from :func:`partitioncache.query_processor.compute_buffer_distance`.
        bucket_width: Width of one partition bucket, in the same unit as ``window``.

    Returns:
        ``ceil(window / bucket_width)``, or 0 when the query has no window. A return value of
        0 means the plain intersection is already sound and no dilation is needed.

    Raises:
        ValueError: If ``bucket_width`` is not positive, or ``window`` is negative.

    Note:
        Dilation inflates every cached set by a factor of roughly ``2k + 1``. Choosing a
        bucket width at least as large as the widest correlation window keeps ``k`` at 1; a
        24h window over 1h buckets needs ``k = 24``, which inflates each set ~49x and
        destroys the selectivity the cache exists to provide.
    """
    if bucket_width <= 0:
        raise ValueError(f"bucket_width must be positive, got {bucket_width}")
    if window < 0:
        raise ValueError(f"window must be non-negative, got {window}")
    if window == 0:
        return 0
    return math.ceil(window / bucket_width)


def dilate_bucket_set(values: set[int], dilation: int) -> set[int]:
    """Expand a bucket set by ``dilation`` buckets in both directions.

    Args:
        values: Bucket identifiers from one cached fragment.
        dilation: Number of buckets to expand by. 0 returns the input unchanged.

    Returns:
        A new set containing every bucket within ``dilation`` of an input bucket.

    Raises:
        ValueError: If ``dilation`` is negative.
    """
    if dilation < 0:
        raise ValueError(f"dilation must be non-negative, got {dilation}")
    if dilation == 0 or not values:
        return set(values)
    offsets = range(-dilation, dilation + 1)
    return {value + offset for value in values for offset in offsets}
