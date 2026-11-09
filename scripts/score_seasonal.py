"""Score the seasonal baseline + persistence detector on the TRAIN split, next to the quickstart's
naive detector.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/score_seasonal.py

Data is read only through DataSource with the clock at the end of train, so the test week is invisible.
"""
from pathlib import Path

import numpy as np
import pandas as pd

from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_sim.evaluate import evaluate_detections
from sentinel_sim.replay import DataSource

DATA = Path(__file__).resolve().parents[1] / "data"
START, TRAIN_END = "2026-09-28", "2026-10-19"   # warm-up + train; test starts Oct 19
COUNTERS = ("kube_pod_container_status_restarts_total", "flink_job_restarts_total",
            "flink_checkpoint_failures_total")

ds = DataSource(DATA).at(TRAIN_END)
cat = ds.series(layer=[1, 2, 3])
gauges = [s for s in cat.series_id if not s.startswith(("kube_pod", "flink_job_restarts", "flink_checkpoint_failures"))]
m = ds.query(gauges, start=START, end=TRAIN_END, step="5min")


def seasonal_detections(threshold=6.0, floor=0.05):
    dets = []
    for sid in m.columns:
        out = seasonal_scores(m[sid], threshold=threshold, floor=floor).set_index("ts")
        dets += [{"ts": ts, "target": sid} for ts in persistent_alerts(out.hot)]
    return pd.DataFrame(dets, columns=["ts", "target"])


def naive_gauge_detections():
    """The quickstart's z-score part, unchanged: no floor, threshold 6, 3-of-5."""
    lagged = np.stack([m.shift(288 * k).values for k in range(1, 8)])
    med = np.nanmedian(lagged, axis=0)
    mad = np.nanmedian(np.abs(lagged - med), axis=0) * 1.4826 + 1e-9
    z = pd.DataFrame((m.values - med) / mad, index=m.index, columns=m.columns)
    hot = (z.abs() > 6).astype(int).rolling(5).sum() >= 3
    return pd.DataFrame([{"ts": ts, "target": c} for c in hot.columns for ts in hot.index[hot[c].values]],
                        columns=["ts", "target"])


def naive_counter_and_job_detections():
    """The rest of the quickstart detector: any counter increase, failed or SLA-missed runs."""
    dets = []
    for sid in ds.series(metric=list(COUNTERS)).series_id:
        s = ds.query(sid, start=START, end=TRAIN_END)[sid]
        dets += [{"ts": ts, "target": sid} for ts in s.index[s.diff() > 0]]
    jr = ds.job_runs(window="30D")
    dets += [{"ts": r.end, "target": f"dag:{r.dag_id}"} for r in jr.itertuples() if r.state == "failed" or r.sla_missed]
    return pd.DataFrame(dets, columns=["ts", "target"])


def score(name, dets):
    r = evaluate_detections(dets, DATA, split="train")
    return {"detector": name, "recall": r["recall"], "precision": r["precision"],
            "false_pos": r["false_positives"], "median_ttd_min": r["median_ttd_min"]}, r["per_incident"]


if __name__ == "__main__":
    with np.errstate(all="ignore"):
        import warnings
        warnings.simplefilter("ignore", RuntimeWarning)
        naive_g, extra, seas = naive_gauge_detections(), naive_counter_and_job_detections(), seasonal_detections()

    rows, per = [], {}
    for name, dets in [("naive (quickstart, full)", pd.concat([naive_g, extra])),
                       ("naive (gauges only)", naive_g),
                       ("seasonal + 3-of-5 (gauges only)", seas)]:
        row, pi = score(name, dets)
        rows.append(row)
        per[name] = pi.set_index("incident_id")

    print(f"TRAIN split ({len(gauges)} gauge series, 5-minute steps)\n")
    print(pd.DataFrame(rows).to_string(index=False))

    print("\nPer incident, time to detect in minutes (blank = missed):")
    base = per["naive (gauges only)"][["scenario", "actionable"]]
    table = base.join(pd.DataFrame({n: p.ttd_min for n, p in per.items()}))
    print(table.to_string(na_rep=""))
