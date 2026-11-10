"""Saturation forecast: is this resource on a steady climb that will hit its limit soon?

A baseline detector asks "is this unusual?". For memory, disk or lag the better question is "when does
it run out?": a leak is a slow, straight climb that looks normal at every single reading until the
pod is killed. At each step a straight line is fitted to the recent readings:

    slope, intercept  least squares over the last `window_min` minutes
    fitted_now        the line's value at the current reading (not the last noisy reading itself)
    time_to_full      (limit - fitted_now) / slope

A reading is hot only if ALL of these hold:
    slope > 0                           it is rising
    R^2 >= min_r2                       and the rise really is a line (a sawtooth or a step is not)
    fitted_now >= min_usage * limit     and it is already using a meaningful share of the limit
    time_to_full < horizon_h            and at this rate the limit is reached within the horizon

Only the current and earlier readings are used. The detector knows nothing about the sample data: it
takes any evenly spaced pandas Series with a DatetimeIndex and the limit as a number.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from sentinel_lens.detectors.persistence import persistent_alerts

SCORE_COLUMNS = ["ts", "value", "fitted", "slope_per_h", "r2", "usage", "time_to_full_h", "points", "hot",
                 "explanation"]
GIB, MB = 1024 ** 3, 1024 ** 2


def _check_limit(limit, name) -> float:
    if limit is None or not np.isfinite(limit) or limit <= 0:
        raise ValueError(f"saturation forecast needs a positive limit for {name!r}, got {limit!r}")
    return float(limit)


def saturation_scores(series: pd.Series, limit: float, window_min: float = 60, horizon_h: float = 4.0,
                      min_r2: float = 0.9, min_usage: float = 0.5, min_coverage: float = 0.8,
                      label: str = "memory", unit: str = "bytes") -> pd.DataFrame:
    """One row per reading: the line fitted to the last `window_min` minutes and whether it is hot.

    limit         the value at which the resource is full, in the series' unit. Required: a missing,
                  zero or negative limit raises ValueError instead of being guessed.
    window_min    how far back the line is fitted. 60 min of 5-minute readings is 12 points.
    horizon_h     alert only if the limit would be reached within this many hours
    min_r2        how straight the climb must be (1.0 = perfectly on the line)
    min_usage     ignore climbs while the fitted value is below this share of the limit
    min_coverage  the window must hold at least this share of its readings, and the current reading
                  itself, or there is no fit. Without this a pod that has existed for 15 minutes
                  would be "forecast" from 3 points, and every start-up ramp is a perfect line.
    label, unit   words for the explanation; unit "bytes" is shown as MB/h and GiB.

    Columns: ts, value, fitted, slope_per_h, r2, usage (fitted / limit), time_to_full_h (NaN unless
    rising), points (readings in the window), hot, explanation (hot readings only).
    """
    limit = _check_limit(limit, series.name)
    s = series.astype(float)
    if len(s) < 2:
        return _no_scores()
    steps = np.diff(s.index.asi8)
    if not (steps == steps[0]).all():
        raise ValueError("saturation forecast needs an evenly spaced series (resample it first)")
    step_h = pd.Timedelta(int(steps[0]), unit=s.index.unit).total_seconds() / 3600
    n = max(int(round(window_min / 60 / step_h)), 2)            # readings per window
    need = max(int(np.ceil(min_coverage * n)), 2)

    # Sliding windows ending at each reading; the first n-1 readings are padded with NaN in front.
    y = np.concatenate([np.full(n - 1, np.nan), s.to_numpy()])
    win = np.lib.stride_tricks.sliding_window_view(y, n)        # row i = the window ending at reading i
    have = ~np.isnan(win)
    x = (np.arange(n) - (n - 1)) * step_h                       # hours relative to "now" (0 = current reading)
    yz = np.where(have, win, 0.0)
    cnt = have.sum(axis=1)
    sx, sy = (have * x).sum(axis=1), yz.sum(axis=1)
    sxx, sxy = (have * x * x).sum(axis=1), (yz * x).sum(axis=1)
    denom = cnt * sxx - sx ** 2
    ok = (cnt >= need) & have[:, -1] & (denom > 0)
    with np.errstate(all="ignore"):
        slope = np.where(ok, (cnt * sxy - sx * sy) / denom, np.nan)
        intercept = np.where(ok, (sy - slope * sx) / cnt, np.nan)      # x = 0 is now, so this is fitted_now
        resid = np.where(have, win - (intercept[:, None] + slope[:, None] * x), 0.0)
        ss_res = (resid ** 2).sum(axis=1)
        ss_tot = (np.where(have, win - (sy / cnt)[:, None], 0.0) ** 2).sum(axis=1)
        r2 = np.where(ok & (ss_tot > 0), 1 - ss_res / ss_tot, np.where(ok, 0.0, np.nan))   # flat line: R^2 0
        usage = intercept / limit
        ttf = np.where(slope > 0, (limit - intercept) / slope, np.nan)
    hot = ok & (slope > 0) & (r2 >= min_r2) & (usage >= min_usage) & (ttf < horizon_h)

    out = pd.DataFrame({"ts": s.index, "value": s.to_numpy(), "fitted": intercept, "slope_per_h": slope,
                        "r2": r2, "usage": usage, "time_to_full_h": ttf, "points": cnt, "hot": hot})
    out["explanation"] = [_explain(label, unit, sl, limit, t, r) if h else None
                          for h, sl, t, r in zip(hot, slope, ttf, r2)]
    return out[SCORE_COLUMNS]


def saturation_alerts(series: pd.Series, limit: float, window_min: float = 60, horizon_h: float = 4.0,
                      min_r2: float = 0.9, min_usage: float = 0.5, k: int = 3, n: int = 5,
                      **kwargs) -> pd.DataFrame:
    """Alerts from `saturation_scores` after the usual persistence check (3 of the last 5 readings hot).

    Columns: ts, value, fitted, slope_per_h, r2, usage, time_to_full_h, direction ("up"), explanation.
    An alert reading that is not hot itself (the third hot reading was one or two steps earlier)
    carries the explanation of the most recent hot reading.
    """
    scores = saturation_scores(series, limit, window_min=window_min, horizon_h=horizon_h, min_r2=min_r2,
                               min_usage=min_usage, **kwargs)
    if scores.empty:
        return scores.drop(columns=["hot", "points"]).assign(direction=None)
    scores = scores.set_index("ts")
    scores["explanation"] = scores.explanation.ffill()
    alerts = scores.loc[persistent_alerts(scores.hot, k=k, n=n)].drop(columns=["hot", "points"])
    alerts.insert(len(alerts.columns) - 1, "direction", "up")
    return alerts.reset_index()


def _no_scores() -> pd.DataFrame:
    return pd.DataFrame(columns=SCORE_COLUMNS).astype({"hot": bool})


def _explain(label, unit, slope, limit, ttf, r2) -> str:
    if unit == "bytes":
        rate, full = f"~{slope / MB:,.0f} MB/h", f"{limit / GIB:.1f} GiB"
    else:
        rate, full = f"~{slope:.3g} {unit}/h", f"{limit:g} {unit}"
    return f"{label} rising {rate}; projected to reach its {full} limit in ~{ttf:.1f} h (R2={r2:.2f})"
