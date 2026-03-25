"""Numeric histogram computation from a column profile's reservoir."""

from typing import Optional


def compute_histogram(
    reservoir: list,
    bins: int = 10,
    col_min: Optional[float] = None,
    col_max: Optional[float] = None,
) -> Optional[dict]:
    """Compute a histogram from a reservoir of numeric values.

    Returns a dict with 'bins' (count per bin) and 'edges' (bin boundaries),
    or None if the reservoir is empty.
    """
    if not reservoir:
        return None

    lo = col_min if col_min is not None else min(reservoir)
    hi = col_max if col_max is not None else max(reservoir)

    if lo == hi:
        return {"bins": [len(reservoir)], "edges": [lo, hi], "bin_count": 1}

    bin_width = (hi - lo) / bins
    counts = [0] * bins
    edges = [round(lo + i * bin_width, 6) for i in range(bins + 1)]

    for val in reservoir:
        idx = int((val - lo) / bin_width)
        if idx >= bins:
            idx = bins - 1
        counts[idx] += 1

    return {"bins": counts, "edges": edges, "bin_count": bins}
