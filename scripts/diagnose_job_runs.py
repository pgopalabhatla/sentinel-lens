"""Why does the job run baseline raise false alarms on hourly_quality_check? Read-only, TRAIN split.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/diagnose_job_runs.py

For every job-run alert the scorer counts as a false alarm on train, prints the run's duration, the
history it was compared with (median, MAD), the wobble and which term set it, and z. Nothing is
changed or tuned; no fix is chosen here.

The job run detector has two wobble terms: 1.4826 x MAD and the 5% relative floor. It has no
absolute floor (unlike the seasonal detector's abs_floor).
"""
import pandas as pd

from scripts.score_seasonal import DATA, START, TRAIN_END, ds
from sentinel_lens.detectors.job_runs import job_run_alerts, job_run_scores
from sentinel_lens.detectors.robust import MAD_TO_SIGMA
from sentinel_sim.evaluate import evaluate_detections

TRAIN_START = pd.Timestamp("2026-10-05", tz="UTC")
THRESHOLD, FLOOR = 6.0, 0.05          # job_run_alerts defaults

if __name__ == "__main__":
    runs = ds.job_runs(start=START, end=TRAIN_END)
    alerts = job_run_alerts(runs)
    alerts = alerts[alerts.ts >= TRAIN_START].reset_index(drop=True)
    table = evaluate_detections(alerts[["ts", "target", "rule"]], DATA, split="train")["alerts_table"]
    false_ts = set(table[table["class"] == "false_positive"].ts)
    false = alerts[alerts.ts.isin(false_ts)]

    # the same history the detector used: this DAG's successful runs that had ended before this run ended
    ok = runs[(runs.state == "success") & runs.end.notna()].copy()
    ok["end"] = pd.to_datetime(ok.end, utc=True)
    ok["duration_min"] = (ok.end - pd.to_datetime(ok.start, utc=True)).dt.total_seconds() / 60

    print(f"Job run baseline on train: {len(alerts)} alerts, {len(false)} false alarms "
          f"({', '.join(sorted(set(false.dag_id)))})\n")
    rows = []
    for a in false.itertuples():
        hist = ok[(ok.dag_id == a.dag_id) & (ok.end < a.ts)].duration_min
        median = hist.median()
        mad = (hist - median).abs().median()
        from_mad, from_floor = MAD_TO_SIGMA * mad, FLOOR * abs(median)
        assert abs(max(from_mad, from_floor) - a.wobble) < 1e-9 and abs(median - a.expected) < 1e-9
        rows.append({
            "run_end": f"{a.ts:%m-%d %H:%M}", "dag_id": a.dag_id, "duration_min": round(a.duration_min, 2),
            "earlier_runs": len(hist), "history_median": round(median, 2), "history_MAD": round(mad, 3),
            "1.4826xMAD": round(from_mad, 3), "5%_floor": round(from_floor, 3), "wobble": round(a.wobble, 3),
            "wobble_set_by": "MAD" if from_mad >= from_floor else "5% floor",
            "wobble_seconds": round(a.wobble * 60, 1), "excess_min": round(a.duration_min - median, 2),
            "min_to_be_hot": round(THRESHOLD * a.wobble, 2), "z": round(a.z, 1)})
    print(pd.DataFrame(rows).to_string(index=False))
    print("\nwobble_seconds: the wobble in seconds. excess_min: duration minus history median.\n"
          "min_to_be_hot: how far above the median a run must be to alert (threshold x wobble).")

    for dag in sorted(set(false.dag_id)):
        d = ok[ok.dag_id == dag].duration_min
        s = job_run_scores(runs)
        s = s[(s.dag_id == dag) & s.z.notna()]
        q = d.quantile([0.5, 0.9, 0.95, 0.99])
        print(f"\n{dag}: all {len(d)} finished successful runs since {START} (warm-up + train)")
        print(f"  duration in minutes: median {q[0.5]:.2f}, p90 {q[0.9]:.2f}, p95 {q[0.95]:.2f}, p99 {q[0.99]:.2f}, "
              f"max {d.max():.2f}")
        print(f"  runs longer than 3 min: {int((d > 3).sum())}; longer than 4 min: {int((d > 4).sum())}")
        print(f"  hot runs (|z| > {THRESHOLD:g}): {int(s.hot.sum())} of {len(s)} scored "
              f"({int((s.hot & (s.ts < TRAIN_START)).sum())} in the warm-up week, "
              f"{int((s.hot & (s.ts >= TRAIN_START)).sum())} in train)")
        print(f"  wobble over the scored runs: {s.wobble.min() * 60:.0f} to {s.wobble.max() * 60:.0f} seconds")
        hot = s[s.hot]
        print("  the hot runs: " + ", ".join(f"{t:%m-%d %H:%M} {v:.1f} min (z={z:.0f})"
                                           for t, v, z in zip(hot.ts, hot.duration_min, hot.z)))
