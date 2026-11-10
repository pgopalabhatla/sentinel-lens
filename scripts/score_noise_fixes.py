"""Compare the seasonal noise fixes on the TRAIN split.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/score_noise_fixes.py

Everything is the routed system from scripts/score_router.py; only the seasonal rule changes:
    history     previous_days (current) / same_weekday / day_type
    abs_floor   off (current) / from the unit / from the warm-up week's scale
Counter, above-zero and schedule alerts are identical in every row.

The absolute floor from scale is calibrated on the no-incident warm-up week (Sep 28 - Oct 4) only.
Data is read only through DataSource with the clock at the end of train, so the test week is invisible.
"""
import warnings

import numpy as np
import pandas as pd

from scripts.score_seasonal import DATA, START, TRAIN_END, ds
from sentinel_lens.calibration import abs_floor_from_scale, abs_floor_from_unit
from sentinel_lens.detectors.guardrails import above_zero_alerts, counter_increase_alerts
from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.schedule import failed_run_alerts, schedule_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_lens.router import route_series
from sentinel_sim.evaluate import evaluate_detections

WARMUP_END = "2026-10-05"
# (b) is shown at two fractions: 0.05 is "a small fraction" of the busy level; 0.2 is the value with
# the best train precision in a sweep of 0.01-0.5, shown so the trade-off is visible.
SCALE_FRACTIONS = (0.05, 0.2)
# The 10 noisiest series from scripts/diagnose_noise.py, in that order.
DIAGNOSED = ["trino_failed_queries_per_min|trino-lakehouse", "trino_queued_queries|trino-lakehouse",
             "container_cpu_usage_cores|trino-worker-1", "container_cpu_usage_cores|trino-worker-0",
             "trino_resource_group_queued|adhoc", "container_cpu_usage_cores|trino-coordinator",
             "kafka_messages_in_per_sec|orders", "kafka_consumer_records_consumed_per_sec|orders-etl",
             "spark_streaming_input_rows_per_sec|orders-stream",
             "trino_completed_queries_per_min|trino-lakehouse"]

catalog = ds.series(layer=[1, 2, 3])
routes = route_series(catalog)
seasonal_routes = routes[routes.rule == "seasonal"]
m5 = ds.query(list(seasonal_routes.series_id), start=START, end=TRAIN_END, step="5min")


def other_rule_detections() -> pd.DataFrame:
    """Counter, above-zero and DAG alerts: the part of the routed system this task does not touch."""
    parts = []
    for rule, detect in [("counter_rule", counter_increase_alerts), ("above_zero", above_zero_alerts)]:
        sids = list(routes[routes.rule == rule].series_id)
        m1 = ds.query(sids, start=START, end=TRAIN_END)
        parts += [pd.DataFrame({"ts": detect(m1[sid]).ts, "target": sid, "rule": rule}) for sid in sids]
    runs = ds.job_runs(start=START, end=TRAIN_END)
    expected = ds.expected_runs(start=START, end=TRAIN_END)
    parts.append(schedule_alerts(expected, runs, now=ds.now)[["ts", "target", "rule"]])
    parts.append(failed_run_alerts(runs)[["ts", "target", "rule"]])
    return pd.concat([p for p in parts if len(p)], ignore_index=True)


def unit_floors() -> dict:
    unit = catalog.set_index("series_id").unit
    return {sid: abs_floor_from_unit(unit[sid]) for sid in seasonal_routes.series_id}


def scale_floors(fraction: float) -> dict:
    warmup = m5.loc[:pd.Timestamp(WARMUP_END, tz="UTC") - pd.Timedelta("1ns")]
    return {sid: abs_floor_from_scale(warmup[sid], fraction=fraction) for sid in seasonal_routes.series_id}


def seasonal_detections(history: str = "previous_days", abs_floors: dict | None = None) -> pd.DataFrame:
    parts = []
    for sid, fallback in zip(seasonal_routes.series_id, seasonal_routes.fallback):
        floor = (abs_floors or {}).get(sid, 0.0)
        hot = seasonal_scores(m5[sid], history=history, abs_floor=floor).set_index("ts").hot
        parts.append(pd.DataFrame({"ts": persistent_alerts(hot), "target": sid,
                                   "rule": "seasonal fallback" if fallback else "seasonal"}))
    return pd.concat(parts, ignore_index=True)


def score(name: str, dets: pd.DataFrame):
    # the scorer needs a unique index, hence reset_index
    r = evaluate_detections(dets.reset_index(drop=True), DATA, split="train")
    pi = r["per_incident"]
    row = {"configuration": name, "recall": r["recall"], "precision": r["precision"], "alerts": r["alerts"],
           "false_pos": r["false_positives"], "median_ttd_min": round(r["median_ttd_min"], 1),
           "slowest_ttd_min": round(pi[pi.actionable].ttd_min.max(), 1)}
    return row, r


def false_alarms_per_series(result: dict) -> pd.Series:
    a = result["alerts_table"]
    return a[a["class"] == "false_positive"].groupby("target").size()


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    others = other_rule_detections()
    floors = {"a: unit": unit_floors()}
    floors.update({f"b: {f:g} x warm-up p95": scale_floors(f) for f in SCALE_FRACTIONS})

    rows, results = [], {}

    def run(name, history, abs_floors):
        row, res = score(name, pd.concat([seasonal_detections(history, abs_floors), others]))
        rows.append(row)
        results[name] = res
        return row

    current = run("current", "previous_days", None)

    def beats_current(row):
        return row["recall"] >= current["recall"] and row["false_pos"] < current["false_pos"]

    history_rows = {h: run(f"+ {h}", h, None) for h in ("same_weekday", "day_type")}
    for label, f in floors.items():
        run(f"+ floor ({label})", "previous_days", f)

    # "best history mode + abs_floor together": only if a history mode beats the current one
    winners = {h: r for h, r in history_rows.items() if beats_current(r)}
    best_history = min(winners, key=lambda h: winners[h]["false_pos"]) if winners else None
    if best_history:
        for label, f in floors.items():
            run(f"+ {best_history} + floor ({label})", best_history, f)

    print(f"TRAIN split, routed system with different seasonal settings ({len(seasonal_routes)} seasonal "
          "series; counter, above-zero and schedule rules are the same in every row)\n")
    print(pd.DataFrame(rows).to_string(index=False))

    print("\nHistory modes vs. the current previous_days:")
    for h, r in history_rows.items():
        verdict = "beats it" if beats_current(r) else "does NOT beat it"
        print(f"  {h}: {verdict} (recall {r['recall']}, false alarms {r['false_pos']} vs {current['false_pos']})")
    if not winners:
        print("  Neither history mode beats the current one on train.")

    names = list(results)
    key = {n: chr(ord("A") + i) for i, n in enumerate(names)}
    print("\nColumns in the two tables below: " + "   ".join(f"{key[n]} = {n}" for n in names))

    print("\nPer incident, time to detect in minutes (blank = missed):")
    per = {n: r["per_incident"].set_index("incident_id") for n, r in results.items()}
    base = per["current"][["scenario", "actionable"]]
    print(base.join(pd.DataFrame({key[n]: p.ttd_min for n, p in per.items()})).round(1).to_string(na_rep=""))

    print("\nFalse alarms on the 10 series from the diagnostic (after 30-minute dedup):")
    table = pd.DataFrame({key[n]: false_alarms_per_series(r).reindex(DIAGNOSED).fillna(0).astype(int)
                          for n, r in results.items()})
    table.loc["all other series"] = [results[n]["false_positives"] - table[key[n]].sum() for n in names]
    print(table.to_string())
