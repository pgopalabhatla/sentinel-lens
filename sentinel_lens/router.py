"""Router: decide which rule scores each series, from the catalog.

The catalog says what kind of signal a series is (`default_detector`); the router turns that into the
rule we actually have today. Pass the catalog DataFrame in: nothing here reads files.

Order matters: the metric-name rules are checked first because the catalog files both
"restart counter" and "heartbeat age" under the same `guardrail` label, and they need different rules.
"""
from __future__ import annotations

import pandas as pd

from sentinel_lens.detectors.guardrails import FLAT_COUNTER_METRICS, ZERO_GAUGE_METRICS

# Catalog detectors that are not built yet. Their series run on the seasonal baseline for now and are
# marked fallback=True so results can be reported separately. Remove an entry when its detector exists:
#   saturation_forecast, change_point   planned detectors
#   guardrail                           guardrail gauges with no hard rule yet (e.g. heartbeat age);
#                                       counters and must-be-zero gauges are caught earlier by name
SEASONAL_FALLBACK = ("saturation_forecast", "change_point", "guardrail")

RULES = ("counter_rule", "above_zero", "seasonal")


def route_series(catalog: pd.DataFrame) -> pd.DataFrame:
    """One row per catalog series: series_id, rule, fallback.

    Needs catalog columns series_id, metric, default_detector. An unknown default_detector raises
    instead of being guessed, so a new kind of series cannot silently go unmonitored.
    """
    rows = []
    for sid, metric, default in zip(catalog.series_id, catalog.metric, catalog.default_detector):
        if metric in FLAT_COUNTER_METRICS:
            rule, fallback = "counter_rule", False
        elif metric in ZERO_GAUGE_METRICS:
            rule, fallback = "above_zero", False
        elif default == "seasonal_baseline":
            rule, fallback = "seasonal", False
        elif default in SEASONAL_FALLBACK:
            rule, fallback = "seasonal", True
        else:
            raise ValueError(f"no rule for {sid}: unknown default_detector {default!r}")
        rows.append({"series_id": sid, "rule": rule, "fallback": fallback})
    return pd.DataFrame(rows, columns=["series_id", "rule", "fallback"])
