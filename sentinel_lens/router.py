"""Router: decide which rule scores each series, and with which settings, from the catalog.

The catalog says what kind of signal a series is (`default_detector`); the router turns that into the
rule we actually have today. Pass the catalog DataFrame in: nothing here reads files.

Order matters: the metric-name rules are checked first because the catalog files both
"restart counter" and "heartbeat age" under the same `guardrail` label, and they need different rules.
"""
from __future__ import annotations

import pandas as pd

from sentinel_lens.calibration import COUNT_UNITS, abs_floor_from_min_change
from sentinel_lens.detectors.guardrails import FLAT_COUNTER_METRICS, ZERO_GAUGE_METRICS

# Catalog detectors that are not built yet. Their series run on the seasonal baseline for now and are
# marked fallback=True so results can be reported separately. Remove an entry when its detector exists:
#   saturation_forecast, change_point   planned detectors
#   guardrail                           guardrail gauges with no hard rule yet (e.g. heartbeat age);
#                                       counters and must-be-zero gauges are caught earlier by name
SEASONAL_FALLBACK = ("saturation_forecast", "change_point", "guardrail")

RULES = ("counter_rule", "above_zero", "seasonal")

# Seasonal settings chosen on train (scripts/score_noise_fixes.py, docs/log.md "Nov 9, seasonal noise fixes"):
# false alarms 785 -> 208 with recall unchanged and no incident detected later.
#   day_type history       weekends are compared with weekends, weekdays with weekdays
#   count min change 6.0   a series counted in whole items must move by more than 6 items to alert
#                          (an absolute floor of 1.0 at threshold 6: the same setting as before,
#                          stated as the change it ignores instead of the floor that causes it)
# The first build's behaviour is route_series(catalog, history="previous_days", count_min_change=0.0).
DEFAULT_HISTORY = "day_type"
DEFAULT_COUNT_MIN_CHANGE = 6.0
SEASONAL_THRESHOLD = 6.0     # the threshold seasonal_scores runs with; min changes are converted with it

COLUMNS = ["series_id", "rule", "fallback", "history", "abs_floor"]


def route_series(catalog: pd.DataFrame, history: str = DEFAULT_HISTORY,
                 count_min_change: float = DEFAULT_COUNT_MIN_CHANGE,
                 threshold: float = SEASONAL_THRESHOLD) -> pd.DataFrame:
    """One row per catalog series: series_id, rule, fallback, history, abs_floor.

    `history` and `abs_floor` are the settings to pass to `seasonal_scores`; they are None and 0.0
    for series on other rules. Needs catalog columns series_id, metric, unit, default_detector.

    count_min_change  smallest change that may alert on a count-like series (COUNT_UNITS); 0 = off
    threshold         the threshold seasonal_scores will be run with: abs_floor = min change / threshold
    An unknown default_detector raises instead of being guessed, so a new kind of series cannot
    silently go unmonitored.
    """
    rows = []
    for sid, metric, unit, default in zip(catalog.series_id, catalog.metric, catalog.unit,
                                          catalog.default_detector):
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
        seasonal = rule == "seasonal"
        min_change = count_min_change if unit in COUNT_UNITS else 0.0
        rows.append({"series_id": sid, "rule": rule, "fallback": fallback,
                     "history": history if seasonal else None,
                     "abs_floor": abs_floor_from_min_change(min_change, threshold) if seasonal else 0.0})
    return pd.DataFrame(rows, columns=COLUMNS)
