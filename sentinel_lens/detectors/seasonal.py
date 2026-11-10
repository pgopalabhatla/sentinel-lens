"""Seasonal baseline detector: compare each reading with the same time of day on earlier days.

For a reading at time t, the history is the values at the same time of day on a set of earlier days
(which days depends on the `history` mode, see `seasonal_scores`).
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
HISTORY_MODES = ("previous_days", "same_weekday", "day_type")
DAY_TYPE_LOOKBACK_DAYS = 28


def _days_back(s: pd.Series, lags) -> np.ndarray:
    """Row i holds the value at (t - lags[i] days), aligned onto this series' timestamps."""
    return np.stack([s.shift(freq=pd.Timedelta(days=k)).reindex(s.index).to_numpy() for k in lags])


def _history(s: pd.Series, history: str, days: int, weeks: int, n_days: int) -> tuple[np.ndarray, str]:
    """(values to build the baseline from, one row per earlier day; words describing them)."""
    if history == "previous_days":
        return _days_back(s, range(1, days + 1)), f"the previous {days} days"
    if history == "same_weekday":
        return _days_back(s, [7 * k for k in range(1, weeks + 1)]), f"the same weekday in the previous {weeks} weeks"
    if history == "day_type":
        lags = np.arange(1, DAY_TYPE_LOOKBACK_DAYS + 1)
        values = _days_back(s, lags)
        dow = s.index.day_of_week.to_numpy()                       # Monday=0 ... Sunday=6 (UTC index)
        same_type = ((dow[None, :] - lags[:, None]) % 7 >= 5) == (dow[None, :] >= 5)
        usable = same_type & ~np.isnan(values)
        most_recent = usable & (np.cumsum(usable, axis=0) <= n_days)   # rows run from newest to oldest
        return (np.where(most_recent, values, np.nan),
                f"the last {n_days} days of the same type (weekday/weekend)")
    raise ValueError(f"unknown history mode {history!r}; choose one of {HISTORY_MODES}")


def seasonal_scores(series: pd.Series, threshold: float = 6.0, floor: float = 0.05,
                    days: int = 7, min_days: int = 1, history: str = "previous_days",
                    weeks: int = 4, n_days: int = 5) -> pd.DataFrame:
    """Score every reading of `series` against the same time of day on earlier days.

    threshold  |z| above this marks a reading as hot (column `hot`)
    floor      wobble is at least this fraction of the expected value
    history    which earlier days form the baseline:
               "previous_days"  each of the previous `days` days (default). Simple, but a weekend
                                reading is compared with a mostly-weekday history.
               "same_weekday"   the same weekday in each of the previous `weeks` weeks.
               "day_type"       the most recent `n_days` earlier days of the same type (Mon-Fri or
                                Sat-Sun) that have a reading, looking back at most 28 days.
    min_days   readings with fewer usable history values than this get no score (NaN). Default 1:
               batch pods exist ~30 min a day at shifting times, and requiring 3 days cost two
               Spark OOM incidents on train with no precision gain.

    Column `baseline_days` is how many history values each score rests on. Read it before trusting
    a score: with 1 value the "median" is that value and MAD is 0, with 2 it is their average, so
    one unusual earlier day moves the baseline directly and only the floor sets the wobble. The
    median starts to shrug off a single odd day from 3 values. `same_weekday` reaches 3 values only
    after 3 weeks of data.
    """
    s = series.astype(float)
    past, described = _history(s, history, days, weeks, n_days)
    n_used = np.sum(~np.isnan(past), axis=0)
    with warnings.catch_warnings():  # nanmedian warns when there is no history yet; handled below
        warnings.simplefilter("ignore", RuntimeWarning)
        expected = np.nanmedian(past, axis=0)
        mad = np.nanmedian(np.abs(past - expected), axis=0)
    expected[n_used < min_days] = np.nan

    wobble = np.maximum(MAD_TO_SIGMA * mad, floor * np.abs(expected))
    value = s.to_numpy()
    diff = value - expected
    with np.errstate(all="ignore"):
        z = np.where(wobble > 0, diff / wobble, np.sign(diff) * np.inf)
    z = np.where(diff == 0, 0.0, z)  # exact match is never anomalous, even when wobble is 0
    z[np.isnan(expected) | np.isnan(value)] = np.nan

    direction = np.where(z > 0, "up", np.where(z < 0, "down", None))
    out = pd.DataFrame({"ts": s.index, "value": value, "expected": expected, "wobble": wobble,
                        "z": z, "direction": direction, "baseline_days": n_used})
    out["hot"] = np.abs(out["z"]) > threshold
    out["explanation"] = [_explain(series.name, v, e, w, zz, n, described)
                          for v, e, w, zz, n in zip(value, expected, wobble, z, n_used)]
    return out


def _explain(name, value, expected, wobble, z, n_used, described) -> str | None:
    if np.isnan(z):
        return None
    metric, _, entity = str(name).partition("|")
    who = f"{metric} ({entity})" if entity else metric
    if expected > 0 and value >= 0:
        ratio = f"{value / expected:.1f}x expected"
    else:
        ratio = f"{value:.3g} vs expected {expected:.3g}"
    return (f"{who} is {ratio} for this time of day "
            f"({value:.3g} vs {expected:.3g} +/- {wobble:.2g}, z={z:+.1f}; "
            f"baseline: {n_used} value{'s' if n_used != 1 else ''} from {described})")
