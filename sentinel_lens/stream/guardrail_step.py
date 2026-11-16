"""Guardrail rules, one minute at a time.

The batch rules are used as they are, on the smallest series that holds their answer:
    counter_increase_alerts   on [previous reading, this reading]
    above_zero_alerts         on [this reading]
so the only state is the previous reading of each counter.

A counter whose reading is unchanged (the same value, or missing both times) is skipped without
asking the rule: an increase needs a change. Every change, in either direction or from/to missing,
goes to the batch rule, which decides.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from sentinel_lens.alerts import Alert, alerts_from_above_zero, alerts_from_counter
from sentinel_lens.detectors.guardrails import above_zero_alerts, counter_increase_alerts


class GuardrailStep:
    def __init__(self, counter_ids, zero_ids):
        self.counter_ids = list(counter_ids)
        self.zero_ids = list(zero_ids)
        self._previous = None             # the counters' values one reading ago
        self._previous_ts = None

    def load(self, ts, counter_values) -> None:
        """The last counter readings before the stream starts (None if there are none)."""
        self._previous_ts = ts
        self._previous = None if counter_values is None else np.asarray(counter_values, dtype=float)

    def step(self, ts: pd.Timestamp, counter_values: np.ndarray, zero_values: np.ndarray) -> list[Alert]:
        alerts = []
        now = np.asarray(counter_values, dtype=float)
        if self._previous is not None:
            before = self._previous
            unchanged = (now == before) | (np.isnan(now) & np.isnan(before))
            for j in np.flatnonzero(~unchanged):
                sid = self.counter_ids[j]
                pair = pd.Series([before[j], now[j]], index=pd.DatetimeIndex([self._previous_ts, ts]), name=sid)
                alerts += alerts_from_counter(counter_increase_alerts(pair), target=sid)
        self._previous, self._previous_ts = now.copy(), ts
        for sid, value in zip(self.zero_ids, np.asarray(zero_values, dtype=float)):
            one = pd.Series([value], index=pd.DatetimeIndex([ts]), name=sid)
            alerts += alerts_from_above_zero(above_zero_alerts(one), target=sid)
        return [a for a in alerts if a.time == ts]
