"""Change point detector: has the level of a series shifted and stayed shifted?

The seasonal detector asks, reading by reading, "is this one far from normal?" (|z| > 6). A latency or
a job duration that moves up by two or three wobbles and stays there never crosses that line, yet it
is a real change. CUSUM (cumulative sum) catches it by adding up small deviations that all point the
same way:

    s_up   = max(0, s_up   + clip(z) - k)      evidence that the level has moved up
    s_down = max(0, s_down - clip(z) - k)      evidence that it has moved down
    alert when either total passes h; that side then restarts from 0

    z     the seasonal z-score of the reading (how many wobbles from expected), from `seasonal_scores`
    k     the slack, in wobbles: deviations smaller than k count against the total, so ordinary noise
          keeps pulling it back to 0. 0.5 means "tuned to notice shifts of about 1 wobble or more".
    h     the decision line for the running total. With k = 0.5 and h = 10: a shift of 2.5 wobbles
          gains about 2 per reading and alerts after about 5 readings; a shift of 1 wobble gains 0.5
          and takes about 20.
    clip  each reading counts for at most this many wobbles. With clip = 3 one reading adds at most
          2.5, so a single spike, however large (even an infinite z), can never reach h on its own:
          that is the seasonal detector's job, not this one's.

There is no separate persistence step: the running total is the persistence. Readings without a
z-score (no history yet, or the series is absent) are skipped and leave the totals as they were.

The detector knows nothing about the sample data; it takes any pandas Series of z-scores.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SCORE_COLUMNS = ["ts", "z", "s_up", "s_down", "alert", "total", "shift", "readings"]
ALERT_COLUMNS = ["ts", "direction", "total", "shift", "readings", "z", "explanation"]


def change_point_scores(z: pd.Series, k: float = 0.5, h: float = 10.0, clip: float = 3.0) -> pd.DataFrame:
    """The two running totals at every reading.

    Columns: ts, z, s_up and s_down (the totals after this reading; 0 on the side that just alerted),
    alert ("up", "down" or None), and for alert readings: total (the value that passed h), shift (the
    average clipped z since that total last left 0: roughly how many wobbles the level moved) and
    readings (how many readings that took).
    """
    if k < 0 or h <= 0 or clip <= 0:
        raise ValueError("k must be >= 0, h and clip must be > 0")
    values = z.to_numpy(dtype=float)
    n = len(values)
    s_up, s_down = np.zeros(n), np.zeros(n)
    alert = np.full(n, None, dtype=object)
    total, shift, readings = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    up = down = 0.0
    n_up = n_down = 0              # readings since each total last was 0
    for i, v in enumerate(values):
        if np.isnan(v):            # no z-score: skip, the totals stay as they were
            s_up[i], s_down[i] = up, down
            continue
        c = min(clip, max(-clip, v))
        up, n_up = (up + c - k, n_up + 1) if up + c - k > 0 else (0.0, 0)
        down, n_down = (down - c - k, n_down + 1) if down - c - k > 0 else (0.0, 0)
        if up > h:
            alert[i], total[i], shift[i], readings[i] = "up", up, up / n_up + k, n_up
            up, n_up = 0.0, 0
        elif down > h:
            alert[i], total[i], shift[i], readings[i] = "down", down, down / n_down + k, n_down
            down, n_down = 0.0, 0
        s_up[i], s_down[i] = up, down
    return pd.DataFrame({"ts": z.index, "z": values, "s_up": s_up, "s_down": s_down, "alert": alert,
                         "total": total, "shift": shift, "readings": readings})[SCORE_COLUMNS]


def change_point_alerts(z: pd.Series, k: float = 0.5, h: float = 10.0, clip: float = 3.0) -> pd.DataFrame:
    """One row for every reading where a running total passes `h`.

    Columns: ts, direction ("up"/"down"), total (the running total that passed h), shift (about how
    many wobbles the level moved), readings (how long the evidence took to add up), z, explanation.
    """
    scores = change_point_scores(z, k=k, h=h, clip=clip)
    hit = scores[scores.alert.notna()]
    metric, _, entity = str(z.name).partition("|") if z.name is not None else ("", "", "")
    who = f"{metric} ({entity}): " if entity else (f"{metric}: " if metric else "")
    out = pd.DataFrame({"ts": hit.ts.to_numpy(), "direction": hit.alert.to_numpy(), "total": hit.total.to_numpy(),
                        "shift": hit["shift"].to_numpy(), "readings": hit.readings.to_numpy().astype(int),
                        "z": hit.z.to_numpy()})
    out["explanation"] = [f"{who}level shifted {d} ~{s:.1f} wobbles over {n} readings; running total passed {h:g}"
                          for d, s, n in zip(out.direction, out["shift"], out.readings)]
    return out[ALERT_COLUMNS]
