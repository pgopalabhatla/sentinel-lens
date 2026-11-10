"""Robust z-score shared by the baseline detectors (seasonal, job runs).

    expected = median(history)
    wobble   = max(1.4826 * MAD(history), floor * |expected|, abs_floor)
    z        = (value - expected) / wobble

Median and MAD instead of mean and standard deviation, so one earlier outlier in the history barely
moves the baseline. Which values count as "history" is the caller's decision; this only does the maths.
"""
from __future__ import annotations

import warnings

import numpy as np

MAD_TO_SIGMA = 1.4826  # makes MAD comparable to a standard deviation for normally distributed noise


def robust_z(value: np.ndarray, past: np.ndarray, floor: float = 0.05, abs_floor: float = 0.0,
             min_history: int = 1):
    """Score N values, each against its own column of history.

    value        shape (N,)
    past         shape (K, N): column j holds the history for value j, NaN where there is none
    min_history  values with fewer usable history entries than this get no score (NaN)

    Returns (expected, wobble, z, n_used), each of shape (N,). NaN history entries are skipped,
    never treated as 0.
    """
    n_used = np.sum(~np.isnan(past), axis=0)
    with warnings.catch_warnings():  # nanmedian warns when there is no history yet; handled below
        warnings.simplefilter("ignore", RuntimeWarning)
        expected = np.nanmedian(past, axis=0)
        mad = np.nanmedian(np.abs(past - expected), axis=0)
    expected[n_used < min_history] = np.nan

    wobble = np.maximum(np.maximum(MAD_TO_SIGMA * mad, floor * np.abs(expected)), abs_floor)
    diff = value - expected
    with np.errstate(all="ignore"):
        z = np.where(wobble > 0, diff / wobble, np.sign(diff) * np.inf)
    z = np.where(diff == 0, 0.0, z)  # exact match is never anomalous, even when wobble is 0
    z[np.isnan(expected) | np.isnan(value)] = np.nan
    return expected, wobble, z, n_used
