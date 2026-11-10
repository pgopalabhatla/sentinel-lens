"""Score the job run baseline on the TRAIN split (Oct 5-18), alone and added to the routed system.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/score_job_runs.py

The detector runs with its defaults (threshold 6, floor 5%, min_runs 5); nothing is tuned here.
Runs from the warm-up week (from Sep 28) are passed in as history, alerts are scored on train only.
Data is read only through DataSource with the clock at the end of train, so the test week is invisible.
"""
import warnings

import numpy as np
import pandas as pd

from scripts.score_router import routed_detections
from scripts.score_seasonal import DATA, START, TRAIN_END, ds
from sentinel_lens.detectors.job_runs import job_run_alerts, job_run_scores
from sentinel_sim.evaluate import evaluate_detections

TRAIN_START = pd.Timestamp("2026-10-05", tz="UTC")
# Routed system without this detector, at the current router defaults (docs/log.md, "Nov 10, router
# settings"). Also pinned by tests/test_train_regression.py; update both together.
PREVIOUS = {"recall": 0.8, "precision": 0.608, "alerts": 270, "false_pos": 100, "median_ttd_min": 9.0}


def score(dets: pd.DataFrame) -> dict:
    return evaluate_detections(dets.reset_index(drop=True), DATA, split="train")   # scorer needs a unique index


def summary(name: str, r: dict) -> dict:
    return {"system": name, "recall": r["recall"], "precision": r["precision"], "alerts": r["alerts"],
            "false_pos": r["false_positives"], "median_ttd_min": round(r["median_ttd_min"], 1)}


def duration_vs_input_rows(flagged_ends: set) -> pd.DataFrame:
    """Diagnostic only: does duration follow input size on healthy runs? Healthy = finished successful
    scheduled runs that the job run baseline did not flag."""
    rows = []
    for dag in sorted(ds.expected_runs(start=START, end=TRAIN_END).dag_id.unique()):
        h = ds.job_history(dag, n=10_000)
        h = h[(h.state == "success") & ~pd.to_datetime(h.end, utc=True).isin(flagged_ends)]
        with_rows = h.dropna(subset=["input_rows"])
        row = {"dag_id": dag, "healthy_runs": len(h), "with_input_rows": len(with_rows)}
        if len(with_rows) >= 3:
            x, y = with_rows.input_rows.astype(float), with_rows.duration_min.astype(float)
            fit = np.polyfit(x, y, 1)
            row.update({"pearson_r": round(x.corr(y), 2), "spearman_r": round(x.corr(y, method="spearman"), 2),
                        "r_squared": round(x.corr(y) ** 2, 2),
                        "min_per_million_rows": round(fit[0] * 1e6, 2),
                        "input_rows_min": int(x.min()), "input_rows_max": int(x.max())})
        rows.append(row)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    runs = ds.job_runs(start=START, end=TRAIN_END)
    all_alerts = job_run_alerts(runs)
    scores = job_run_scores(runs)
    alerts = all_alerts[all_alerts.ts >= TRAIN_START].reset_index(drop=True)

    print("JOB RUN BASELINE on the TRAIN split (threshold 6, floor 5%, min_runs 5: defaults, not tuned)\n")
    print(f"Runs scored: {int(scores.z.notna().sum())} finished successful runs since {START}; "
          f"{all_alerts.attrs['skipped_cold_start']} skipped as cold start (fewer than 5 earlier runs), "
          f"{int((scores.z.isna() & (scores.ts >= TRAIN_START)).sum())} of those in the train period.")
    print(f"Alerts before train (warm-up week, not scored): {int((all_alerts.ts < TRAIN_START).sum())}")

    alone = score(alerts[["ts", "target", "rule"]])
    print("\n1. Per incident, did the job run baseline alone detect it?")
    pi = alone["per_incident"].set_index("incident_id")[["scenario", "actionable", "detected", "ttd_min"]]
    print(pi.round(1).to_string(na_rep=""))

    print("\n2. Alerts it raised (class is the scorer's verdict):")
    klass = alone["alerts_table"].set_index("ts")["class"]
    shown = alerts.assign(**{"class": alerts.ts.map(klass).fillna("deduplicated")})
    numeric = ["duration_min", "expected", "wobble", "z"]
    shown[numeric] = shown[numeric].round(2)
    print(shown[["ts", "dag_id", "duration_min", "expected", "wobble", "z", "direction", "baseline_runs", "class"]]
          .to_string(index=False))
    print()
    for text in alerts.explanation:
        print("   " + text)

    print("\n3. Alerts per DAG:")
    per_dag = (shown.groupby(["dag_id", "class"]).size().unstack(fill_value=0)
               .reindex(columns=["true", "expected", "false_positive"], fill_value=0))
    print(per_dag.to_string())

    print("\n4. Added to the routed system (router defaults, counters, schedule watcher):")
    routed, _ = routed_detections()
    before = score(routed)
    after = score(pd.concat([routed, alerts[["ts", "target", "rule"]]]))
    print(pd.DataFrame([{"system": "previous numbers (log)", **PREVIOUS},
                        summary("routed system, re-run now", before),
                        summary("routed system + job run baseline", after)]).to_string(index=False))

    b = before["per_incident"].set_index("incident_id")
    a = after["per_incident"].set_index("incident_id")
    both = b[["scenario"]].assign(before=b.ttd_min, after=a.ttd_min)
    print("\n   Per incident, time to detect in minutes (blank = missed):")
    print(both.round(1).to_string(na_rep=""))
    was_detected = b.detected
    changed = both[was_detected & ~np.isclose(both.before.astype(float), both.after.astype(float), equal_nan=True)]
    if changed.empty:
        print(f"\n   Check: all {int(was_detected.sum())} previously detected incidents have an unchanged "
              "time to detect.")
    else:
        print("\n   CHECK FAILED: time to detect changed for previously detected incidents:")
        print(changed.to_string())
    newly = both[~was_detected & a.detected]
    print("   Newly detected: " + (", ".join(f"{i} ({r.scenario}, {r.after:.1f} min)" for i, r in newly.iterrows())
                                  or "none"))

    print("\n5. Diagnostic only: duration vs. input_rows on healthy runs (no normalization is built)")
    print("   input_rows comes from DataSource.job_history; the job_runs table itself has no such column.")
    print(duration_vs_input_rows(set(all_alerts.ts)).to_string(index=False, na_rep="-"))
