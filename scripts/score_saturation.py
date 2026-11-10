"""Score the saturation forecast on the TRAIN split, three ways.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/score_saturation.py

    (a) current setup     routed system (router defaults) + job run baseline. The 25 container memory
                          series are on the seasonal fallback.
    (b) forecast only     the memory series are scored by the saturation forecast INSTEAD of seasonal
                          (route_series(saturation=True, limits=...)).
    (c) forecast as well  the memory series keep seasonal AND get the forecast.

The forecast runs with its defaults (window 60 min, horizon 4 h, R2 0.9, min usage 0.5); nothing is
tuned. The sweep at the end is report-only. Limits are memory_limit_bytes per pod from the topology
(DataSource.pods()). Data is read only through DataSource with the clock at the end of train, so the
test week is invisible.
"""
import itertools
import warnings

import pandas as pd

from scripts.score_router import routed_detections
from scripts.score_seasonal import DATA, START, TRAIN_END, ds
from sentinel_lens.detectors.job_runs import job_run_alerts
from sentinel_lens.detectors.saturation import saturation_alerts
from sentinel_lens.router import SATURATION_METRICS
from sentinel_sim.evaluate import evaluate_detections

TRAIN_START = pd.Timestamp("2026-10-05", tz="UTC")
HIGHLIGHT = ["INC-007", "INC-001", "INC-010"]
SWEEP = {"window_min": (30, 60, 120), "horizon_h": (2.0, 4.0, 8.0), "min_usage": (0.3, 0.5, 0.7)}
DEFAULTS = {"window_min": 60, "horizon_h": 4.0, "min_usage": 0.5}
GIB = 1024 ** 3


def score(dets: pd.DataFrame) -> dict:
    return evaluate_detections(dets.reset_index(drop=True), DATA, split="train")


def summary(name: str, r: dict) -> dict:
    pi = r["per_incident"].set_index("incident_id")
    return {"setup": name, "recall": r["recall"], "precision": r["precision"], "alerts": r["alerts"],
            "false_pos": r["false_positives"], "median_ttd_min": round(r["median_ttd_min"], 1),
            **{i: (round(pi.ttd_min[i], 1) if pi.detected[i] else None) for i in HIGHLIGHT}}


def memory_limits() -> dict:
    pods = ds.pods()
    return dict(zip(pods.pod, pods.memory_limit_bytes))


def is_memory(target: pd.Series) -> pd.Series:
    return target.str.split("|").str[0].isin(SATURATION_METRICS)


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    limits = memory_limits()
    runs = ds.job_runs(start=START, end=TRAIN_END)
    jobs = job_run_alerts(runs)[["ts", "target", "rule"]]

    current, _ = routed_detections()
    forecast_routed, routes = routed_detections(saturation=True, limits=limits)
    memory_seasonal = current[is_memory(current.target)]
    forecast = forecast_routed[forecast_routed.rule == "saturation"]

    setups = {"(a) current setup": pd.concat([current, jobs]),
              "(b) memory on forecast only": pd.concat([forecast_routed, jobs]),
              "(c) forecast in addition to seasonal": pd.concat([forecast_routed, memory_seasonal, jobs])}
    results = {n: score(d) for n, d in setups.items()}

    n_mem = int((routes.rule == "saturation").sum())
    print(f"SATURATION FORECAST on the TRAIN split ({n_mem} container memory series; window 60 min, horizon 4 h, "
          "R2 >= 0.9, usage >= 0.5 of the limit: defaults, not tuned)\n")
    print(pd.DataFrame([summary(n, r) for n, r in results.items()]).to_string(index=False, na_rep="missed"))

    print("\nPer incident, time to detect in minutes (blank = missed):")
    per = {n: r["per_incident"].set_index("incident_id") for n, r in results.items()}
    a = per["(a) current setup"]
    table = a[["scenario", "actionable"]].join(pd.DataFrame({n[:3]: p.ttd_min for n, p in per.items()}))
    table.insert(2, "highlight", ["<--" if i in HIGHLIGHT else "" for i in table.index])
    print(table.round(1).to_string(na_rep=""))

    print("\nAgainst (a), incident by incident:")
    for name in list(setups)[1:]:
        p = per[name]
        lost = [i for i in a.index if a.detected[i] and not p.detected[i]]
        later = [f"{i} ({a.ttd_min[i]:.1f} -> {p.ttd_min[i]:.1f} min)" for i in a.index
                 if a.detected[i] and p.detected[i] and p.ttd_min[i] > a.ttd_min[i] + 1e-9]
        sooner = [f"{i} ({a.ttd_min[i]:.1f} -> {p.ttd_min[i]:.1f} min)" for i in a.index
                  if a.detected[i] and p.detected[i] and p.ttd_min[i] < a.ttd_min[i] - 1e-9]
        print(f"  {name}: lost {', '.join(lost) or 'none'}; later {', '.join(later) or 'none'}; "
              f"sooner {', '.join(sooner) or 'none'}")

    # what the memory series contribute under each rule, scored on their own
    print("\nThe memory series on their own (alerts after the scorer's 30-minute dedup):")
    rows = []
    for name, dets in [("seasonal fallback on memory", memory_seasonal), ("saturation forecast on memory", forecast)]:
        r = score(dets)
        cls = r["alerts_table"]["class"].value_counts()
        pi = r["per_incident"].set_index("incident_id")
        rows.append({"rule": name, "alerts": r["alerts"], "true": int(cls.get("true", 0)),
                     "expected": int(cls.get("expected", 0)), "false_pos": int(cls.get("false_positive", 0)),
                     "incidents_detected": ", ".join(f"{i} ({pi.ttd_min[i]:.0f} min)" for i in pi.index[pi.detected])
                     or "none"})
    print(pd.DataFrame(rows).to_string(index=False))

    print("\nEvery forecast alert on train (before dedup; class is the scorer's verdict, '-' = deduplicated):")
    m5 = ds.query(list(routes[routes.rule == "saturation"].series_id), start=START, end=TRAIN_END, step="5min")
    klass = score(forecast)["alerts_table"].set_index(["ts", "target"])["class"]
    detail = []
    for sid, limit in zip(routes.series_id[routes.rule == "saturation"], routes.limit[routes.rule == "saturation"]):
        al = saturation_alerts(m5[sid], limit)
        al = al[al.ts >= TRAIN_START]
        for r in al.itertuples():
            detail.append({"ts": f"{r.ts:%m-%d %H:%M}", "pod": sid.split("|")[1], "limit_GiB": limit / GIB,
                           "usage": round(r.usage, 2), "MB_per_h": round(r.slope_per_h / 1024 ** 2),
                           "hours_to_full": round(r.time_to_full_h, 1), "r2": round(r.r2, 2),
                           "class": klass.get((r.ts, sid), "-")})
    detail = pd.DataFrame(detail)
    if len(detail):
        runs_of = detail.groupby(["pod", detail.ts.str[:5]]).agg(
            first=("ts", "first"), last=("ts", "last"), alert_readings=("ts", "size"),
            usage_from=("usage", "first"), usage_to=("usage", "last"), MB_per_h=("MB_per_h", "median"),
            classes=("class", lambda c: ", ".join(sorted(set(c) - {"-"})))).reset_index(level=1, drop=True)
        print(runs_of.to_string())
    else:
        print("  none")

    # ------------------------------------------------------------------ report-only sweep
    print("\nReport-only sweep (defaults marked *). 'forecast' columns score the forecast alone;\n"
          "'(b)' and '(c)' are the whole system with the memory series on forecast only / forecast as well.")
    base_b = pd.concat([current[~is_memory(current.target)], jobs])
    base_c = pd.concat([current, jobs])
    sat = routes[routes.rule == "saturation"]
    rows = []
    for window, horizon, usage in itertools.product(*SWEEP.values()):
        parts = [pd.DataFrame({"ts": saturation_alerts(m5[sid], limit, window_min=window, horizon_h=horizon,
                                                       min_usage=usage).ts, "target": sid, "rule": "saturation"})
                 for sid, limit in zip(sat.series_id, sat.limit)]
        f = pd.concat(parts, ignore_index=True)
        alone, b, c = score(f), score(pd.concat([base_b, f])), score(pd.concat([base_c, f]))
        fpi, bpi, cpi = (r["per_incident"].set_index("incident_id") for r in (alone, b, c))
        cls = alone["alerts_table"]["class"].value_counts() if alone["alerts"] else {}
        is_default = {"window_min": window, "horizon_h": horizon, "min_usage": usage} == DEFAULTS
        rows.append({"": "*" if is_default else "", "window_min": window, "horizon_h": horizon, "min_usage": usage,
                     "forecast_alerts": alone["alerts"], "forecast_true": int(cls.get("true", 0)),
                     "forecast_FP": int(cls.get("false_positive", 0)),
                     "forecast_INC-007": round(fpi.ttd_min["INC-007"], 0) if fpi.detected["INC-007"] else None,
                     "(b)_recall": b["recall"], "(b)_precision": b["precision"], "(b)_FP": b["false_positives"],
                     "(b)_INC-007": round(bpi.ttd_min["INC-007"], 0) if bpi.detected["INC-007"] else None,
                     "(c)_recall": c["recall"], "(c)_precision": c["precision"], "(c)_FP": c["false_positives"],
                     "(c)_INC-007": round(cpi.ttd_min["INC-007"], 0) if cpi.detected["INC-007"] else None})
    print(pd.DataFrame(rows).to_string(index=False, na_rep="missed"))
