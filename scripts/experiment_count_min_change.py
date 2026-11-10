"""Report-only experiment on the TRAIN split: what if count-like series needed a smaller change to alert?

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/experiment_count_min_change.py

count_min_change is the smallest change (on a 5-minute average) that can alert on a series counted
in whole items. The router default is 6. This script tries 1, 2, 3 and 6 with everything else at the
router defaults (day_type history, throttled-ratio minimum change 0.18) and prints the result.
It changes no default. Nothing here should be read as a tuned choice: it is a table to decide from.

Data is read only through DataSource with the clock at the end of train, so the test week is invisible.
"""
import warnings

import pandas as pd

from scripts.score_router import routed_detections
from scripts.score_seasonal import DATA, START, TRAIN_END, ds
from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_lens.router import DEFAULT_COUNT_MIN_CHANGE
from sentinel_sim.evaluate import evaluate_detections

VALUES = (1.0, 2.0, 3.0, 6.0)
WATCHED = ["trino_failed_queries_per_min|trino-lakehouse", "airflow_queued_tasks|airflow-main",
           "airflow_task_completions_per_min|airflow-main"]
TRAIN_START = pd.Timestamp("2026-10-05", tz="UTC")


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    rows, per, watched = [], {}, []
    m5 = ds.query(WATCHED, start=START, end=TRAIN_END, step="5min")
    for v in VALUES:
        dets, routes = routed_detections(count_min_change=v)
        res = evaluate_detections(dets.reset_index(drop=True), DATA, split="train")
        label = f"{v:g}" + (" (default)" if v == DEFAULT_COUNT_MIN_CHANGE else "")
        pi = res["per_incident"]
        rows.append({"count_min_change": label, "recall": res["recall"], "precision": res["precision"],
                     "alerts": res["alerts"], "false_pos": res["false_positives"],
                     "median_ttd_min": round(res["median_ttd_min"], 1),
                     "slowest_ttd_min": round(pi[pi.actionable].ttd_min.max(), 1)})
        per[label] = pi.set_index("incident_id")

        table = res["alerts_table"]
        settings = routes.set_index("series_id")
        for sid in WATCHED:
            out = seasonal_scores(m5[sid], history=settings.history[sid], abs_floor=settings.abs_floor[sid])
            out = out.set_index("ts")
            out = out[out.index >= TRAIN_START]
            raw = persistent_alerts(out.hot)
            scored = table[table.target == sid]
            train_max = m5[sid][m5.index >= TRAIN_START].max()
            watched.append({"count_min_change": label, "series_id": sid, "train_max": round(train_max, 2),
                            "max_rise_over_expected": round((out.value - out.expected).max(), 2),
                            "can_alert": "yes" if len(raw) else "no",
                            "hot_readings": int(out.hot.sum()), "raw_alerts": len(raw),
                            "scored_alerts": len(scored),
                            "true": int((scored["class"] == "true").sum()),
                            "false_pos": int((scored["class"] == "false_positive").sum())})

    print("TRAIN split, routed system with different count_min_change (report only; default stays "
          f"{DEFAULT_COUNT_MIN_CHANGE:g})\n")
    print(pd.DataFrame(rows).to_string(index=False))

    print("\nPer incident, time to detect in minutes by count_min_change (blank = missed):")
    base = per[next(iter(per))][["scenario", "actionable"]]
    print(base.join(pd.DataFrame({n: p.ttd_min for n, p in per.items()})).round(1).to_string(na_rep=""))

    print("\nCan these three series alert? (can_alert = at least one persistent alert on train;\n"
          "max_rise_over_expected = largest value - expected on a 5-minute reading, to compare with the\n"
          "minimum change; raw_alerts are before the scorer's 30-minute dedup, scored_alerts after)")
    print(pd.DataFrame(watched).to_string(index=False))
