"""Job run baseline: compare each run's duration with that DAG's own earlier runs.

A run can finish successfully and inside its SLA and still be a problem: twice its usual duration
usually means skewed data, a bad plan or a starved executor. The schedule watcher cannot see that
(nothing was late, nothing failed), and no metric has a baseline for "how long does this job take".

Same maths as the seasonal detector (sentinel_lens/detectors/robust.py), with "history" meaning the
DAG's earlier finished successful runs:
    expected = median(earlier durations)
    wobble   = max(1.4826 * MAD, floor * expected)
    z        = (duration - expected) / wobble

One run is one observation, so there is no persistence step: a hot run is an alert at its end time.

Input is a plain DataFrame, one row per run (the shape Airflow's API gives you); nothing here reads
the sample data.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from sentinel_lens.detectors.robust import robust_z

SCORE_COLUMNS = ["ts", "target", "dag_id", "duration_min", "expected", "wobble", "z", "direction",
                 "baseline_runs", "hot", "explanation"]


def job_run_scores(runs: pd.DataFrame, threshold: float = 6.0, floor: float = 0.05,
                   min_runs: int = 5) -> pd.DataFrame:
    """One row per finished successful run, scored against the same DAG's earlier ones.

    runs       columns dag_id, start, end, state (times as strings, Timestamps or None)
    threshold  |z| above this marks the run as hot
    floor      wobble is at least this fraction of the expected duration
    min_runs   a run with fewer earlier runs than this gets no score (z is NaN, hot is False)

    Which runs count:
    - Only state == "success" with an end time. Failed runs have their own alert (failed_run in the
      schedule watcher) and say nothing about normal duration; a run in progress has no duration yet.
    - "Earlier" means the other run had already ENDED before this run ended, i.e. it was known at
      this run's alert time. The run itself and anything later are never in its baseline.
    - All earlier runs are used, not a window. A slow run therefore joins later baselines; the
      median absorbs a few of those, but a DAG that is slow for good shifts its baseline over time.
    - Manual runs count like scheduled ones: the same job doing the same work.

    Column `baseline_runs` is how many earlier runs each score rests on.
    """
    if runs is None or runs.empty:
        return _no_scores()
    r = runs[(runs.state == "success") & runs.end.notna()].copy()
    r["start"] = pd.to_datetime(r.start, utc=True)
    r["end"] = pd.to_datetime(r.end, utc=True)
    r["duration_min"] = (r.end - r.start).dt.total_seconds() / 60

    parts = []
    for dag, g in r.groupby("dag_id", sort=True):
        g = g.sort_values("end", kind="stable")
        duration = g.duration_min.to_numpy(dtype=float)
        end = g.end.to_numpy()
        # past[k, j] = duration of run k if it had ended before run j ended, else NaN
        known = end[:, None] < end[None, :]
        past = np.where(known, duration[:, None], np.nan)
        expected, wobble, z, n_used = robust_z(duration, past, floor=floor, min_history=min_runs)
        parts.append(pd.DataFrame({
            "ts": g.end.to_numpy(), "target": f"dag:{dag}", "dag_id": dag, "duration_min": duration,
            "expected": expected, "wobble": wobble, "z": z,
            "direction": np.where(z > 0, "slower", np.where(z < 0, "faster", None)),
            "baseline_runs": n_used, "hot": np.abs(z) > threshold,
            "explanation": [_explain(dag, d, e, zz) for d, e, zz in zip(duration, expected, z)]}))
    if not parts:
        return _no_scores()
    out = pd.concat(parts, ignore_index=True)
    out["ts"] = pd.to_datetime(out.ts, utc=True)
    return out.sort_values("ts", kind="stable").reset_index(drop=True)[SCORE_COLUMNS]


def job_run_alerts(runs: pd.DataFrame, threshold: float = 6.0, floor: float = 0.05,
                   min_runs: int = 5) -> pd.DataFrame:
    """The hot runs from `job_run_scores`: one alert per run, at the run's end time.

    Columns: ts, target ("dag:<dag_id>"), rule, dag_id, duration_min, expected, wobble, z, direction
    (slower/faster), baseline_runs, explanation.
    `alerts.attrs["skipped_cold_start"]` is the number of finished successful runs that could not be
    scored because their DAG had fewer than `min_runs` earlier runs.
    """
    scores = job_run_scores(runs, threshold=threshold, floor=floor, min_runs=min_runs)
    alerts = scores[scores.hot].drop(columns="hot").reset_index(drop=True)
    alerts.insert(2, "rule", "job_run_baseline")
    alerts.attrs["skipped_cold_start"] = int(scores.z.isna().sum())
    return alerts


def _no_scores() -> pd.DataFrame:
    return pd.DataFrame(columns=SCORE_COLUMNS).astype({"hot": bool})


def _explain(dag, duration, expected, z) -> str | None:
    if np.isnan(z):
        return None
    return f"{dag} took {duration:.1f} min; its earlier runs took ~{expected:.1f} min (z={z:.1f})"
