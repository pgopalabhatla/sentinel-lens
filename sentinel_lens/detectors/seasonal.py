"""Seasonal baseline detector: compare each reading with the same time of day over the previous days.

For a reading at time t, the history is the values at t - 1 day, t - 2 days, ..., t - 7 days.
    expected = median(history)
    wobble   = max(1.4826 * MAD(history), floor * |expected|)
    z        = (value - expected) / wobble

Only past values are used, so a score never depends on anything after its own timestamp.
The detector knows nothing about the sample data; it takes any pandas Series with a DatetimeIndex.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

MAD_TO_SIGMA = 1.4826  # makes MAD comparable to a standard deviation for normally distributed noise


def seasonal_scores(series: pd.Series, threshold: float = 6.0, floor: float = 0.05,
                    days: int = 7, min_days: int = 1) -> pd.DataFrame:
    """Score every reading of `series` against the same time of day on the previous `days` days.

    threshold  |z| above this marks a reading as hot (column `hot`)
    floor      wobble is at least this fraction of the expected value
    min_days   readings with fewer usable history days than this get no score (NaN). Default 1:
               batch pods exist ~30 min a day at shifting times, and requiring 3 days cost two
               Spark OOM incidents on train with no precision gain.
    """
    s = series.astype(float)
    # Same time of day on each previous day, aligned back onto this series' timestamps.
    history = np.stack([s.shift(freq=pd.Timedelta(days=k)).reindex(s.index).to_numpy()
                        for k in range(1, days + 1)])
    n_days = np.sum(~np.isnan(history), axis=0)
    with warnings.catch_warnings():  # nanmedian warns when there is no history yet; handled below
        warnings.simplefilter("ignore", RuntimeWarning)
        expected = np.nanmedian(history, axis=0)
        mad = np.nanmedian(np.abs(history - expected), axis=0)
    expected[n_days < min_days] = np.nan

    wobble = np.maximum(MAD_TO_SIGMA * mad, floor * np.abs(expected))
    value = s.to_numpy()
    diff = value - expected
    with np.errstate(all="ignore"):
        z = np.where(wobble > 0, diff / wobble, np.sign(diff) * np.inf)
    z = np.where(diff == 0, 0.0, z)  # exact match is never anomalous, even when wobble is 0
    z[np.isnan(expected) | np.isnan(value)] = np.nan

    direction = np.where(z > 0, "up", np.where(z < 0, "down", None))
    out = pd.DataFrame({"ts": s.index, "value": value, "expected": expected, "wobble": wobble,
                        "z": z, "direction": direction})
    out["hot"] = np.abs(out["z"]) > threshold
    out["explanation"] = [_explain(series.name, v, e, w, zz)
                          for v, e, w, zz in zip(value, expected, wobble, z)]
    return out


def _explain(name, value, expected, wobble, z) -> str | None:
    if np.isnan(z):
        return None
    metric, _, entity = str(name).partition("|")
    who = f"{metric} ({entity})" if entity else metric
    if expected > 0 and value >= 0:
        ratio = f"{value / expected:.1f}x expected"
    else:
        ratio = f"{value:.3g} vs expected {expected:.3g}"
    return (f"{who} is {ratio} for this time of day "
            f"({value:.3g} vs {expected:.3g} +/- {wobble:.2g}, z={z:+.1f})")
