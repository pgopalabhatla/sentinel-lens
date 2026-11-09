"""Hard guardrails: rules for things that are always bad. No baseline, no persistence.

A restart or a failed checkpoint is a discrete event, not a noisy measurement, so there is nothing to
average and nothing to wait for: one occurrence is the alert. That is also why these run on the raw
1-minute readings instead of 5-minute averages.

The functions take any pandas Series with a DatetimeIndex and know nothing about the sample data.
"""
from __future__ import annotations

import pandas as pd

# Cumulative counters whose healthy state is flat. Counters that rise in normal operation
# (requests served, bytes sent) must NOT go through counter_increase_alerts.
FLAT_COUNTER_METRICS = (
    "kube_pod_container_status_restarts_total",
    "flink_job_restarts_total",
    "flink_checkpoint_failures_total",
)
# Gauges that must be exactly 0 when healthy.
ZERO_GAUGE_METRICS = ("kafka_under_replicated_partitions",)


def _who(name) -> tuple[str, str]:
    metric, _, entity = str(name).partition("|")
    return metric, entity


def counter_increase_alerts(series: pd.Series) -> pd.DataFrame:
    """One row wherever a cumulative counter is higher than the reading directly before it.

    NaN means the pod does not exist. A reading is only compared with the one immediately before it,
    so the first reading after a gap is never an alert and NaN is never treated as 0.
    A decrease is a counter reset (the process restarted its count), not an alert.
    """
    s = series.astype(float)
    previous = s.shift(1)
    increase = s - previous                    # NaN whenever either side is missing
    rose = (increase > 0).to_numpy()           # NaN > 0 is False, so gaps drop out here
    metric, entity = _who(series.name)
    out = pd.DataFrame({"ts": s.index[rose], "value": s.to_numpy()[rose],
                        "previous": previous.to_numpy()[rose], "increase": increase.to_numpy()[rose]})
    out["explanation"] = [f"{metric} for {entity} rose from {p:g} to {v:g}"
                          for p, v in zip(out.previous, out.value)]
    return out


def above_zero_alerts(series: pd.Series) -> pd.DataFrame:
    """One row for every reading above 0, for gauges that must be 0 when healthy."""
    s = series.astype(float)
    bad = (s > 0).to_numpy()                   # NaN > 0 is False: a missing reading is not an alert
    metric, entity = _who(series.name)
    out = pd.DataFrame({"ts": s.index[bad], "value": s.to_numpy()[bad]})
    out["explanation"] = [f"{metric} for {entity} is {v:g}, should be 0" for v in out.value]
    return out
