"""Small statistics helpers for reporting results."""
from __future__ import annotations

import math


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple:
    """Wilson score interval for a proportion (default 95%).

    Used for recall on a handful of incidents: with 9 detected out of 10 the plain "90% +/- ..."
    interval is badly wrong (it can pass 100%), while Wilson stays inside [0, 1] and is honest about
    how little 10 incidents can tell. Returns (low, high); (0.0, 1.0) when n is 0.
    """
    if n <= 0:
        return (0.0, 1.0)
    if not 0 <= successes <= n:
        raise ValueError(f"successes must be between 0 and n, got {successes} of {n}")
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))
