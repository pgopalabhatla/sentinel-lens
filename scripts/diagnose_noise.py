"""Why are the noisiest series noisy? Read-only diagnostic on the TRAIN split.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/diagnose_noise.py

Takes the routed system with its first-build seasonal settings (previous_days history, no absolute
floor: the state before docs/tasks/task-fix-seasonal-noise.md), picks the 10 series with the most
false alarms from the seasonal rule (fallback series included, since they run the same rule), and
prints what the seasonal detector saw on each. Nothing is changed or tuned here: the detector runs
with its default parameters and the numbers are only printed.

"Readings" are the scored 5-minute readings in the train period (Oct 5-18). "Hot" means |z| > threshold
before persistence. When expected is 0 the floor is 0 too, so those readings usually also count as
"wobble at floor".

Suspects, checked in this order (the cut-offs are for labelling only, not detector parameters):
    zero expected      at least half of the hot readings have expected == 0. floor x 0 = 0, so the
                       wobble is 0 and any non-zero value gets an unbounded z.
    weekend baseline   at least 60% of the hot readings are on Sat/Sun (which are 2/7 = 29% of readings).
                       The median of the previous 7 days is 5 weekdays + 2 weekend days, so "expected"
                       is the weekday level and a normal quiet weekend looks like a drop.
    steep daily ramp   at least half of the hot readings sit in 3 hours of the day (3/24 = 12.5% if even).
                       On a steep ramp a small shift in timing is a large change in value.
    none               none of the above.
"""
import warnings

import numpy as np
import pandas as pd

from scripts.score_router import FIRST_BUILD, routed_detections
from scripts.score_seasonal import DATA, START, TRAIN_END, ds
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_sim.evaluate import evaluate_detections

TOP_N = 10
FLOOR = 0.05                      # seasonal_scores default, repeated here only to recognise floored wobble
TRAIN_START = pd.Timestamp("2026-10-05", tz="UTC")
SEASONAL_RULES = ["seasonal", "seasonal fallback"]


def diagnose(sid: str) -> dict:
    s = ds.query(sid, start=START, end=TRAIN_END, step="5min")[sid]
    out = seasonal_scores(s).set_index("ts")
    out = out[(out.index >= TRAIN_START) & out.z.notna()]
    hot = out[out.hot]
    weekend = hot.index.day_of_week >= 5                      # Monday=0 ... Saturday=5, Sunday=6
    hours = hot.index.hour.value_counts().head(3)
    return {
        "readings": len(out), "hot": len(hot),
        "zero_expected": (out.expected == 0).mean(),
        "at_floor": np.isclose(out.wobble, FLOOR * out.expected.abs()).mean(),
        "hot_weekday": int((~weekend).sum()), "hot_weekend": int(weekend.sum()),
        "top_hours": hours,
        # used only for the one-sentence verdict
        "hot_zero_expected": (hot.expected == 0).mean() if len(hot) else 0.0,
        "hot_down": (hot.direction == "down").mean() if len(hot) else 0.0,
        "hot_value": hot.value.median(), "hot_expected": hot.expected.median(),
    }


def verdict(d: dict) -> str:
    hot = max(d["hot"], 1)
    weekend_share = d["hot_weekend"] / hot
    top3_share = d["top_hours"].sum() / hot
    if d["hot_zero_expected"] >= 0.5:
        return (f"zero expected: {d['hot_zero_expected']:.0%} of its hot readings have expected == 0, where "
                f"the 5% floor is 5% of nothing, so a typical hot value of {d['hot_value']:.3g} gets a huge z.")
    if weekend_share >= 0.6:
        side = "below" if d["hot_down"] >= 0.5 else "above"
        return (f"weekend baseline: {weekend_share:.0%} of its hot readings are on Sat/Sun and "
                f"{max(d['hot_down'], 1 - d['hot_down']):.0%} are {side} expected (typically {d['hot_value']:.3g} "
                f"vs {d['hot_expected']:.3g}), because the 7-day median is mostly weekdays.")
    if top3_share >= 0.5:
        return (f"steep daily ramp: {top3_share:.0%} of its hot readings fall in just 3 hours of the day, "
                f"with weekends at only {weekend_share:.0%}.")
    return (f"none: only {d['hot_zero_expected']:.0%} of hot readings have zero expected, {weekend_share:.0%} "
            f"are on weekends and the top 3 hours hold {top3_share:.0%}, so no single suspect explains it.")


if __name__ == "__main__":
    with np.errstate(all="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)
        dets, _ = routed_detections(**FIRST_BUILD)   # the system this diagnostic was written about
    alerts = evaluate_detections(dets, DATA, split="train")["alerts_table"]
    false = alerts[(alerts["class"] == "false_positive") & alerts.rule.isin(SEASONAL_RULES)]
    per_series = false.groupby("target").size().sort_values(ascending=False)
    top = per_series.head(TOP_N)
    default = ds.series().set_index("series_id").default_detector

    print(f"TRAIN split: {len(false)} false alarms from the seasonal rule across {len(per_series)} series; "
          f"the top {TOP_N} account for {top.sum()} ({top.sum() / len(false):.0%}).\n")

    verdicts = []
    for rank, (sid, n_false) in enumerate(top.items(), 1):
        d = diagnose(sid)
        hours = ", ".join(f"{h:02d}:00 ({n})" for h, n in d["top_hours"].items())
        print(f"{rank}. {sid}")
        print(f"   false alarms            {n_false}")
        print(f"   default_detector        {default[sid]}")
        print(f"   expected == 0           {d['zero_expected']:.1%} of {d['readings']} readings")
        print(f"   wobble at the floor     {d['at_floor']:.1%} of readings")
        print(f"   hot readings            {d['hot_weekday']} weekday, {d['hot_weekend']} weekend")
        print(f"   top 3 hours (UTC)       {hours}")
        print()
        verdicts.append((rank, sid, verdict(d)))

    print("Best explanation per series:")
    for rank, sid, text in verdicts:
        print(f"{rank}. {sid} - {text}")
