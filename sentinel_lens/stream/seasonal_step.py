"""The seasonal baseline, one 5-minute bucket at a time.

The batch detector (`seasonal_scores`) scores a whole series at once. Here only the newest bucket is
scored, against the same history, by calling the batch detector's own parts in the same order:

    seasonal._history   which earlier days form the baseline (previous_days / same_weekday / day_type)
    robust.robust_z     expected, wobble, z
    seasonal._explain   the sentence
    alerts.alerts_from_seasonal   3-of-5 persistence, and which reading the alert describes

Nothing here contains a formula of its own. What is new is only the bookkeeping:

- History is a rolling table of 5-minute means, 28 days long (the furthest any history mode looks back).
- The history of a reading never includes the reading itself: it is the same time of day on EARLIER
  days. So the baseline rows for every bucket of the next 24 hours can be worked out ahead, from
  values that are already known, by handing `_history` the table with the coming day left empty.
  That is done once a day per series ("a chunk"); scoring a bucket is then one `robust_z` call.
  A chunk longer than one day would need values that do not exist yet, so one day is the limit.
- The last 5 scored buckets are kept per series for the persistence check.

Settings that the batch pipeline leaves at their defaults (threshold, 5% floor, min_days, n_days,
k of n) are read from the batch functions' signatures, so they cannot drift.
"""
from __future__ import annotations

import inspect
from collections import deque

import numpy as np
import pandas as pd

from sentinel_lens.alerts import Alert, alerts_from_seasonal
from sentinel_lens.detectors import seasonal as batch
from sentinel_lens.detectors.robust import robust_z


def defaults_of(fn) -> dict:
    return {k: v.default for k, v in inspect.signature(fn).parameters.items()
            if v.default is not inspect.Parameter.empty}


SCORE = defaults_of(batch.seasonal_scores)        # threshold, floor, days, min_days, weeks, n_days
PERSIST = defaults_of(alerts_from_seasonal)       # k, n (and the threshold the score is made with)
LOOKBACK_DAYS = max(batch.DAY_TYPE_LOOKBACK_DAYS, SCORE["days"], 7 * SCORE["weeks"])
ONE_DAY = pd.Timedelta("1D")


class SeasonalStep:
    """Rolling state for a set of series that run on the seasonal baseline.

    series_ids  the series, in the column order `score` will be given values in
    detectors   the detector label of each ("seasonal" or "seasonal fallback")
    histories   the history mode of each (from the router)
    abs_floors  the absolute floor of each (from the router)
    step        bucket length; must divide a day
    """

    def __init__(self, series_ids, detectors, histories, abs_floors, step="5min"):
        self.series_ids = list(series_ids)
        self.detectors = list(detectors)
        self.histories = list(histories)
        self.abs_floors = [float(f) for f in abs_floors]
        self.step = pd.Timedelta(step)
        if ONE_DAY % self.step != pd.Timedelta(0):
            raise ValueError(f"step {step!r} must divide a day")
        self.chunk = int(ONE_DAY / self.step)                 # buckets scored per history build
        self.lookback = LOOKBACK_DAYS * self.chunk            # buckets of history kept
        n = len(self.series_ids)
        self._table = np.full((self.lookback + self.chunk, n), np.nan)
        self._chunk_start = None          # time of the first bucket of the current chunk
        self._slot = 0                    # next bucket to score, counted from _chunk_start
        self._groups = []                 # (columns, abs_floor, past[K, chunk, len(columns)])
        self._described = [""] * n
        self._window = deque(maxlen=PERSIST["n"])             # the last n scored buckets

    # ------------------------------------------------------------------------------- loading
    def history_index(self, first_bucket) -> pd.DatetimeIndex:
        """The bucket times `load` needs when `first_bucket` is the first one that will be scored."""
        return pd.date_range(end=first_bucket - self.step, periods=self.lookback, freq=self.step)

    def load(self, history: pd.DataFrame) -> None:
        """Start from a table of closed buckets: index = `history_index(first bucket to score)`,
        one column per series (NaN where there is no reading)."""
        if len(history) != self.lookback or list(history.columns) != self.series_ids:
            raise ValueError("history must have history_index() as its index and one column per series")
        self._table[:self.lookback] = history.to_numpy(dtype=float)
        self._table[self.lookback:] = np.nan
        self._chunk_start = history.index[-1] + self.step
        self._slot = 0
        self._window.clear()
        self._build_baselines()

    @property
    def next_bucket(self) -> pd.Timestamp:
        return self._chunk_start + self._slot * self.step

    def _roll(self) -> None:
        """The chunk is used up: move the table one day on and build the next day's baselines."""
        self._table[:self.lookback] = self._table[self.chunk:].copy()
        self._table[self.lookback:] = np.nan
        self._chunk_start = self._chunk_start + self.chunk * self.step
        self._slot = 0
        self._build_baselines()

    def _build_baselines(self) -> None:
        """Baseline rows for every bucket of the chunk, from the batch detector's own `_history`."""
        grid = pd.date_range(self._chunk_start - self.lookback * self.step,
                             periods=self.lookback + self.chunk, freq=self.step)
        per_series = []
        for j, mode in enumerate(self.histories):
            past, described = batch._history(pd.Series(self._table[:, j], index=grid), mode,
                                             SCORE["days"], SCORE["weeks"], SCORE["n_days"])
            per_series.append(past[:, self.lookback:])        # only the columns of the coming chunk
            self._described[j] = described
        # series that share a history mode and a floor are scored in one robust_z call
        by_setting: dict = {}
        for j, (mode, floor) in enumerate(zip(self.histories, self.abs_floors)):
            by_setting.setdefault((mode, floor), []).append(j)
        self._groups = [(np.array(cols), floor, np.stack([per_series[j] for j in cols], axis=2))
                        for (_, floor), cols in by_setting.items()]

    # ------------------------------------------------------------------------------- scoring
    def score(self, ts: pd.Timestamp, values: np.ndarray) -> list[Alert]:
        """Score the bucket that starts at `ts` (it must be the next one) and return the alerts that
        fire AT this bucket: series for which k of the last n buckets, this one included, are hot."""
        if self._chunk_start is None:
            raise RuntimeError("load() history first")
        if self._slot == self.chunk:
            self._roll()
        if ts != self.next_bucket:
            raise ValueError(f"expected the bucket at {self.next_bucket}, got {ts}")
        values = np.asarray(values, dtype=float)
        self._table[self.lookback + self._slot] = values

        n = len(self.series_ids)
        expected, wobble, z = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
        n_used = np.zeros(n, dtype=int)
        for cols, abs_floor, past in self._groups:
            e, w, zz, used = robust_z(values[cols], past[:, self._slot, :], floor=SCORE["floor"],
                                      abs_floor=abs_floor, min_history=SCORE["min_days"])
            expected[cols], wobble[cols], z[cols], n_used[cols] = e, w, zz, used
        hot = np.abs(z) > SCORE["threshold"]                  # as in seasonal_scores
        self._window.append({"ts": ts, "value": values, "expected": expected, "wobble": wobble,
                             "z": z, "n_used": n_used, "hot": hot})
        self._slot += 1

        # No alert is possible for a series with no hot bucket in the window; for the others the
        # batch adapter decides (persistence, worst reading, score).
        any_hot = np.any([row["hot"] for row in self._window], axis=0)
        alerts = []
        for j in np.flatnonzero(any_hot):
            fired = alerts_from_seasonal(self._scores(j), target=self.series_ids[j], detector=self.detectors[j])
            alerts += [a for a in fired if a.time == ts]
        return alerts

    def _scores(self, j: int) -> pd.DataFrame:
        """The last n buckets of one series, in the shape `seasonal_scores` returns."""
        rows = list(self._window)
        z = np.array([r["z"][j] for r in rows])
        direction = np.where(z > 0, "up", np.where(z < 0, "down", None))     # as in seasonal_scores
        explanation = [batch._explain(self.series_ids[j], r["value"][j], r["expected"][j], r["wobble"][j],
                                      r["z"][j], r["n_used"][j], self._described[j]) for r in rows]
        return pd.DataFrame({"ts": pd.DatetimeIndex([r["ts"] for r in rows]), "z": z, "direction": direction,
                             "hot": np.array([r["hot"][j] for r in rows]), "explanation": explanation})
