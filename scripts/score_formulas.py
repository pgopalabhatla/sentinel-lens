"""Report-only experiment on the TRAIN split: which score formula ranks true alerts above false alarms?

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/score_formulas.py

The alerts are the current system's (routed system with the router defaults + job run baseline),
built through the adapters in sentinel_lens/alerts.py. Which alerts fire is NOT changed: the script
first checks that they are exactly the alerts of scripts/score_router.py + the job run baseline and
that the scorer gives the same result. Only the 0-1 score attached to each alert differs.

Four formulas, each a function of r = measure / alert line (see sentinel_lens/alerts.py for the
measure and line of each detector):
    (1) linear       min(1, r/2)
    (2) exponential  1 - exp(-(r - 1))
    (3) log          min(1, ln(r) / ln(4))
    (4) percentile   share of the warm-up week's measures, for that detector, that are smaller

Reported per formula, pooled and per detector:
    AUC             chance that a random true alert scores higher than a random false alarm (ties
                    count half). 0.5 = the score says nothing, 1.0 = perfect. 'expected' alerts
                    (inside a non-actionable incident) are left out.
    top-10 precision  each day, keep the 10 highest-scored alerts; precision = true / (true + false)
                    over all days. Ties at the cut are shared evenly (the expected value of a random
                    tie-break), so the result does not depend on row order.
    mean score      of true alerts and of false alarms.

Alerts are scored after the scorer's 30-minute dedup: one alert per target per 30 minutes, the first
one, with the score it had when it fired. Nothing is tuned; no constant other than the four formulas'
own. Data is read only through DataSource with the clock at the end of train.
"""
import warnings

import numpy as np
import pandas as pd

from scripts.score_router import routed_detections
from scripts.score_seasonal import DATA, START, TRAIN_END, ds
from sentinel_lens.alerts import (alerts_from_above_zero, alerts_from_counter, alerts_from_job_runs,
                                  alerts_from_schedule, alerts_from_seasonal, exponential_score, linear_score,
                                  log_score, percentile_formula, to_frame)
from sentinel_lens.detectors.guardrails import above_zero_alerts, counter_increase_alerts
from sentinel_lens.detectors.job_runs import job_run_alerts, job_run_scores
from sentinel_lens.detectors.schedule import failed_run_alerts, schedule_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_lens.router import route_series
from sentinel_sim.evaluate import evaluate_detections

TRAIN_START = pd.Timestamp("2026-10-05", tz="UTC")
EXPECTED_RESULT = {"recall": 1.0, "precision": 0.604, "alerts": 275, "false_positives": 103}
TOP_N = 10


def current_alerts():
    """(alerts of the current system as Alert records, warm-up measures per detector)."""
    routes = route_series(ds.series(layer=[1, 2, 3]))
    alerts, reference = [], {}

    def remember(detector, values):
        reference.setdefault(detector, []).extend(np.asarray(values, dtype=float).tolist())

    seasonal = routes[routes.rule == "seasonal"]
    m5 = ds.query(list(seasonal.series_id), start=START, end=TRAIN_END, step="5min")
    for sid, fallback, history, abs_floor in zip(seasonal.series_id, seasonal.fallback, seasonal.history,
                                                 seasonal.abs_floor):
        detector = "seasonal fallback" if fallback else "seasonal"
        scores = seasonal_scores(m5[sid], history=history, abs_floor=abs_floor)
        alerts += alerts_from_seasonal(scores, target=sid, detector=detector)
        remember(detector, scores.z[(scores.ts < TRAIN_START) & scores.z.notna()].abs())

    for rule, detect, adapt, column in [("counter_rule", counter_increase_alerts, alerts_from_counter, None),
                                        ("above_zero", above_zero_alerts, alerts_from_above_zero, "value")]:
        sids = list(routes[routes.rule == rule].series_id)
        m1 = ds.query(sids, start=START, end=TRAIN_END)
        for sid in sids:
            alerts += adapt(detect(m1[sid]), target=sid)
            warm = m1[sid][m1.index < TRAIN_START]
            remember(rule, (warm.diff().clip(lower=0) if column is None else warm).dropna())

    runs = ds.job_runs(start=START, end=TRAIN_END)
    expected = ds.expected_runs(start=START, end=TRAIN_END)
    sched = schedule_alerts(expected, runs, now=ds.now)
    failed = failed_run_alerts(runs)
    alerts += alerts_from_schedule(pd.concat([sched, failed]))
    alerts += alerts_from_job_runs(job_run_alerts(runs))

    # yes/no rules: one 0 or 1 per opportunity in the warm-up week
    warm_expected = int((expected.scheduled_for < TRAIN_START).sum())
    for rule in ("start_late", "sla_breach"):
        fired = int(((sched.rule == rule) & (sched.ts < TRAIN_START)).sum())
        remember(rule, [1.0] * fired + [0.0] * (warm_expected - fired))
    ends = pd.to_datetime(runs.end, utc=True)
    warm_runs = runs[ends.notna() & (ends < TRAIN_START)]
    remember("failed_run", (warm_runs.state == "failed").astype(float))
    js = job_run_scores(runs)
    remember("job_run_baseline", js.z[(js.ts < TRAIN_START) & js.z.notna()].abs())
    return alerts, reference


def auc(scores: pd.Series, is_true: pd.Series) -> float:
    """Chance that a random true alert outscores a random false alarm; ties count half."""
    n_pos, n_neg = int(is_true.sum()), int((~is_true).sum())
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    ranks = scores.rank(method="average")
    return float((ranks[is_true].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def top_n_precision(table: pd.DataFrame, score: str, n: int = TOP_N) -> float:
    """Precision of the n highest-scored alerts per day; ties at the cut are shared evenly."""
    true = false = 0.0
    for _, day in table.groupby(table.ts.dt.floor("D")):
        left = float(n)
        for _, tied in sorted(day.groupby(score), key=lambda kv: -kv[0]):
            weight = min(1.0, left / len(tied))
            true += weight * (tied["class"] == "true").sum()
            false += weight * (tied["class"] == "false_positive").sum()
            left -= weight * len(tied)
            if left <= 1e-12:
                break
    return true / (true + false) if true + false else float("nan")


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    alerts, reference = current_alerts()
    frame = to_frame(alerts)

    # --- the alerts must be exactly the current system's
    old, _ = routed_detections()
    old = pd.concat([old, job_run_alerts(ds.job_runs(start=START, end=TRAIN_END))[["ts", "target", "rule"]]])
    same = (set(zip(frame.ts, frame.target, frame.rule)) == set(zip(old.ts, old.target, old.rule))
            and len(frame) == len(old))
    res = evaluate_detections(frame, DATA, split="train")
    got = {k: (int(res[k]) if k in ('alerts', 'false_positives') else float(res[k])) for k in EXPECTED_RESULT}
    print(f"Alerts through the adapters: {len(frame)} before dedup; identical to the current pipeline's: {same}")
    print(f"Scorer result: {got}; unchanged: {got == EXPECTED_RESULT}")
    assert same and got == EXPECTED_RESULT, "the adapters changed which alerts fire"

    table = res["alerts_table"].copy()
    formulas = {"(1) linear": linear_score, "(2) exponential": exponential_score, "(3) log": log_score,
                "(4) percentile": percentile_formula(reference)}
    for name, f in formulas.items():
        table[name] = [f(m, line, d) for m, line, d in zip(table.measure, table.line, table.rule)]
    table["r"] = table.measure / table.line
    judged = table[table["class"] != "expected"]
    is_true = judged["class"] == "true"
    per_day = table.groupby(table.ts.dt.floor("D")).size()
    print(f"\nAfter the 30-minute dedup: {len(table)} alerts on {len(per_day)} days "
          f"(median {per_day.median():.0f} a day, {int((per_day > TOP_N).sum())} days with more than {TOP_N}): "
          f"{int(is_true.sum())} true, {int((~is_true).sum())} false, {len(table) - len(judged)} expected.")
    print(f"Precision of all alerts (what a score that says nothing would give the top {TOP_N}): "
          f"{is_true.mean():.3f}")

    print("\n1. Pooled over all detectors")
    rows = []
    for name in formulas:
        rows.append({"formula": name, "AUC": round(auc(judged[name], is_true), 3),
                     f"top{TOP_N}_per_day_precision": round(top_n_precision(table, name), 3),
                     "mean_score_true": round(judged[name][is_true].mean(), 3),
                     "mean_score_false": round(judged[name][~is_true].mean(), 3),
                     "share_at_1.0": f"{(table[name] >= 1.0).mean():.0%}",
                     "share_at_0.0": f"{(table[name] <= 0.0).mean():.0%}"})
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n2. Per detector: what each detector's alerts look like")
    by = table.groupby("rule")
    shape = pd.DataFrame({"alerts": by.size(), "true": by["class"].apply(lambda c: (c == "true").sum()),
                          "false": by["class"].apply(lambda c: (c == "false_positive").sum()),
                          "expected": by["class"].apply(lambda c: (c == "expected").sum()),
                          "r_median": by.r.median().round(2), "r_max": by.r.max().round(1),
                          "warmup_reference_size": pd.Series({d: len(v) for d, v in reference.items()})})
    shape = shape.dropna(subset=["alerts"]).astype({"alerts": int, "true": int, "false": int, "expected": int})
    print(shape.sort_values("alerts", ascending=False).to_string())

    order = list(shape.sort_values("alerts", ascending=False).index)
    for title, fn in [("3. Per detector: AUC (blank = needs both true and false alerts)",
                       lambda t, name: auc(t[t["class"] != "expected"][name], t[t["class"] != "expected"]["class"] == "true")),
                      (f"4. Per detector: precision of the top {TOP_N} alerts per day (of that detector alone)",
                       lambda t, name: top_n_precision(t, name)),
                      ("5. Per detector: mean score of true alerts", lambda t, name: t[t["class"] == "true"][name].mean()),
                      ("6. Per detector: mean score of false alarms",
                       lambda t, name: t[t["class"] == "false_positive"][name].mean())]:
        print(f"\n{title}")
        out = pd.DataFrame({name: {d: fn(table[table.rule == d], name) for d in order} for name in formulas})
        print(out.round(3).to_string(na_rep=""))
