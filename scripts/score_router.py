"""Score the routed system on the TRAIN split, next to the naive detector and seasonal-everywhere.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/score_router.py

Routed system:
    seasonal / seasonal fallback   5-minute averages, 3-of-5 persistence; history mode and absolute
                                   floor come from the router (day_type, 1.0 for count-like units)
    counter_rule, above_zero       native 1-minute readings, no persistence
    start_late, sla_breach,        DAG runs vs. the schedule, target "dag:<dag_id>"
    failed_run

Targets follow sentinel_sim/evaluate.py: a series_id ("metric|entity") is mapped to its entity through
the catalog; anything without "|" is taken as an entity itself, e.g. "dag:daily_orders".

Data is read only through DataSource with the clock at the end of train, so the test week is invisible.
"""
import warnings

import numpy as np
import pandas as pd

from scripts.score_seasonal import (DATA, START, TRAIN_END, ds, m, naive_counter_and_job_detections,
                                    naive_gauge_detections, seasonal_detections)
from sentinel_lens.detectors.guardrails import above_zero_alerts, counter_increase_alerts
from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.schedule import failed_run_alerts, schedule_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_lens.router import route_series
from sentinel_sim.evaluate import evaluate_detections

RULE_ORDER = ["counter_rule", "above_zero", "seasonal", "seasonal fallback",
              "start_late", "sla_breach", "failed_run"]


FIRST_BUILD = {"history": "previous_days", "count_min_change": 0.0, "metric_min_change": {}}   # seasonal settings before the noise fixes


def routed_detections(**route_settings):
    """Alerts of the routed system. `route_settings` go to route_series; none = the router's defaults."""
    routes = route_series(ds.series(layer=[1, 2, 3]), **route_settings)
    parts = []

    # seasonal and seasonal fallback: 5-minute steps, 3-of-5, history and absolute floor from the router
    seasonal = routes[routes.rule == "seasonal"]
    m5 = ds.query(list(seasonal.series_id), start=START, end=TRAIN_END, step="5min")
    for sid, fallback, history, abs_floor in zip(seasonal.series_id, seasonal.fallback, seasonal.history,
                                                 seasonal.abs_floor):
        hot = seasonal_scores(m5[sid], history=history, abs_floor=abs_floor).set_index("ts").hot
        parts.append(pd.DataFrame({"ts": persistent_alerts(hot), "target": sid,
                                   "rule": "seasonal fallback" if fallback else "seasonal"}))

    # guardrails: native 1-minute readings, every hit is an alert
    for rule, detect in [("counter_rule", counter_increase_alerts), ("above_zero", above_zero_alerts)]:
        sids = list(routes[routes.rule == rule].series_id)
        m1 = ds.query(sids, start=START, end=TRAIN_END)
        for sid in sids:
            parts.append(pd.DataFrame({"ts": detect(m1[sid]).ts, "target": sid, "rule": rule}))

    # DAG runs
    runs = ds.job_runs(start=START, end=TRAIN_END)
    expected = ds.expected_runs(start=START, end=TRAIN_END)
    parts.append(schedule_alerts(expected, runs, now=ds.now)[["ts", "target", "rule"]])
    parts.append(failed_run_alerts(runs)[["ts", "target", "rule"]])

    return pd.concat([p for p in parts if len(p)], ignore_index=True), routes


def score(name, dets):
    r = evaluate_detections(dets, DATA, split="train")
    row = {"detector": name, "recall": r["recall"], "precision": r["precision"], "alerts": r["alerts"],
           "false_pos": r["false_positives"], "median_ttd_min": r["median_ttd_min"]}
    return row, r


if __name__ == "__main__":
    with np.errstate(all="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)
        naive_g, extra, seas = naive_gauge_detections(), naive_counter_and_job_detections(), seasonal_detections()
        routed_first, _ = routed_detections(**FIRST_BUILD)
        routed, routes = routed_detections()

    rows, per, results = [], {}, {}
    for name, dets in [("naive (quickstart, full)", pd.concat([naive_g, extra])),
                       ("naive (gauges only)", naive_g),
                       ("seasonal everywhere", seas),
                       ("routed system (previous_days, no abs floor)", routed_first),
                       ("routed system", routed)]:
        row, res = score(name, dets)
        rows.append(row)
        per[name] = res["per_incident"].set_index("incident_id")
        results[name] = res

    print(f"TRAIN split ({len(routes)} series routed: "
          + ", ".join(f"{n} {r}" for r, n in routes.rule.value_counts().items())
          + f"; {int(routes.fallback.sum())} of the seasonal ones are fallbacks)\n")
    print(pd.DataFrame(rows).to_string(index=False))

    print("\nPer incident, time to detect in minutes (blank = missed):")
    base = per["naive (gauges only)"][["scenario", "actionable"]]
    table = base.join(pd.DataFrame({n: p.ttd_min for n, p in per.items()}))
    print(table.round(1).to_string(na_rep=""))

    # Alerts are counted after the scorer's dedup (one per target per 30 minutes).
    alerts = results["routed system"]["alerts_table"]
    by_rule = (alerts.groupby(["rule", "class"]).size().unstack(fill_value=0)
               .reindex(RULE_ORDER, fill_value=0)
               .reindex(columns=["true", "expected", "false_positive"], fill_value=0))
    by_rule["alerts"] = by_rule.sum(axis=1)
    print("\nRouted system, alerts by rule (after 30-minute dedup per target):")
    print(by_rule[["alerts", "true", "expected", "false_positive"]].to_string())
