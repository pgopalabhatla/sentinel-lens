"""Choosing `abs_floor` for the seasonal detector, per series.

`abs_floor` is the smallest change worth caring about, in the series' own unit. The detector only
takes the number; how to pick it lives here so it can be swapped without touching the detector.

Two ways, compared on train in scripts/score_noise_fixes.py:
    abs_floor_from_unit    a fixed amount for things that are counted in whole items
    abs_floor_from_scale   a fraction of the series' own level in a known-quiet calibration period

`abs_floor_from_min_change` converts the number a person can reason about ("ignore changes smaller
than X") into the floor the detector takes.

Nothing here reads the sample data: pass a unit string or a pandas Series in.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Units that count whole items. One item more or less is never an incident, so the wobble is at
# least 1. Per-second rates and ratios are left out: 1 msg/s or a ratio of 1.0 means something
# entirely different from one series to the next, so no single number fits them.
COUNT_UNITS = ("count", "queries", "tasks", "messages", "partitions", "queries/min", "tasks/min")


def abs_floor_from_min_change(min_change: float, threshold: float = 6.0) -> float:
    """The absolute floor that makes `min_change` the smallest change that can be hot.

    A reading is hot when |value - expected| > threshold x wobble, and wobble >= abs_floor, so a
    floor of f silently means "ignore changes up to threshold x f". Stating the setting as the
    change itself keeps that visible: min_change 6.0 with threshold 6 is a floor of 1.0.
    `threshold` must be the threshold the detector is run with.
    """
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    return min_change / threshold


def abs_floor_from_unit(unit: str, count_floor: float = 1.0) -> float:
    """`count_floor` for count-like units, 0.0 (no absolute floor) for everything else."""
    return count_floor if unit in COUNT_UNITS else 0.0


def abs_floor_from_scale(calibration: pd.Series, fraction: float = 0.05, quantile: float = 0.95) -> float:
    """`fraction` of the `quantile` of a quiet calibration period (e.g. a no-incident warm-up week).

    The 95th percentile is the series' normal busy level without being set by one outlier.
    Guard: a series that is 0 nearly all the time has a 95th percentile of 0, which would switch the
    floor off exactly where it is needed, so fall back to the period's maximum. If the series was
    never above 0 (or has no data) there is nothing to calibrate from and the floor stays 0.0.
    """
    values = calibration.astype(float).abs().dropna()
    if values.empty:
        return 0.0
    level = float(np.quantile(values, quantile))
    if level <= 0:
        level = float(values.max())
    return fraction * level if level > 0 else 0.0
