"""Diagnosis of the routed system on the TRAIN split. Read-only: no detector or scoring is changed.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/diagnose_router.py

1. Reproduce the routed train result.
2. Where do the container_cpu_throttled_ratio false alarms come from?
3. What does the count floor cost? (smallest alertable change; incidents lost, measured)
4. Injection test: which absolute wobble floors still detect a synthetic throttling jump?
5. Which metrics have unit "ratio"?

Data is read only through DataSource with the clock at the end of train, so the test week is invisible.
Section 2 needs incident windows and affected entities, so this script reads the labels; only
split == "train" incidents are kept. That is fine for a diagnosis script and would not be for detector
code. Section 4 works on an in-memory copy of one series; nothing under data/ is written.
What-if settings in sections 3 and 4 are for measurement only; none of them is adopted here.
"""
import warnings

import numpy as np
import pandas as pd

from scripts.score_router import routed_detections
from scripts.score_seasonal import DATA, START, TRAIN_END, ds
from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_lens.router import DEFAULT_COUNT_MIN_CHANGE, route_series
from sentinel_sim.evaluate import evaluate_detections
from sentinel_sim.replay import load_labels

EXPECTED = {"recall": 0.8, "precision": 0.427, "alerts": 378, "false_positives": 208}
THROTTLE = "container_cpu_throttled_ratio"
THRESHOLD = 6.0                                    # seasonal_scores default
TRAIN_START = pd.Timestamp("2026-10-05", tz="UTC")
THIN_WEEKEND = (pd.Timestamp("2026-10-10", tz="UTC"), pd.Timestamp("2026-10-12", tz="UTC"))
GRACE = pd.Timedelta("5min")                       # the scorer's own window: start - 5 min .. end

# Section 4
INJECT_SERIES = f"{THROTTLE}|kafka-0"
INJECT_START = pd.Timestamp("2026-10-14T12:00:00Z")    # a Wednesday, 3 hours
INJECT_HOURS, INJECT_VALUE = 3, 0.45
WOBBLE_FLOORS = (0.0, 0.01, 0.02, 0.03, 0.05, 0.08)

catalog = ds.series(layer=[1, 2, 3])
meta = catalog.set_index("series_id")
train_incidents = [i for i in load_labels(DATA)[0] if i["split"] == "train"]


def score(dets: pd.DataFrame) -> dict:
    return evaluate_detections(dets.reset_index(drop=True), DATA, split="train")


def heading(text: str):
    print(f"\n{'=' * 100}\n{text}\n{'=' * 100}")


def in_train(df: pd.DataFrame) -> pd.DataFrame:
    return df[df.index >= TRAIN_START]


# ---------------------------------------------------------------------------------------------- 1
def reproduce():
    heading("1. Routed system on train (router defaults)")
    dets, routes = routed_detections()
    res = score(dets)
    rows = [{"metric": k, "expected": v, "measured": res[k], "match": res[k] == v} for k, v in EXPECTED.items()]
    print(pd.DataFrame(rows).to_string(index=False))
    print(f"median time to detect: {res['median_ttd_min']:.1f} min")
    return dets, routes, res


# ---------------------------------------------------------------------------------------------- 2
def throttle_false_alarms(res: dict, routes: pd.DataFrame):
    heading(f"2. {THROTTLE}: where do the false alarms come from?")
    a = res["alerts_table"]
    fa = a[(a["class"] == "false_positive") & a.target.str.startswith(THROTTLE + "|")].copy()
    fa["entity"] = "pod:" + fa.target.str.split("|").str[1]
    fa["weekend_oct_10_11"] = (fa.ts >= THIN_WEEKEND[0]) & (fa.ts < THIN_WEEKEND[1])

    def incidents_at(ts):
        return [i for i in train_incidents
                if pd.Timestamp(i["start"]) - GRACE <= ts <= pd.Timestamp(i["end"])]

    fa["incidents"] = [",".join(i["incident_id"] for i in incidents_at(ts)) for ts in fa.ts]
    fa["in_incident_window"] = fa.incidents != ""
    # by construction these are false alarms, so the entity is in none of those incidents' lists;
    # checked explicitly anyway
    fa["entity_affected"] = [any(e in i["affected_entities"] for i in incidents_at(ts))
                             for ts, e in zip(fa.ts, fa.entity)]

    # how many earlier same-type days the baseline had at each alert
    settings = routes.set_index("series_id")
    days = {}
    for sid in fa.target.unique():
        s = ds.query(sid, start=START, end=TRAIN_END, step="5min")[sid]
        out = seasonal_scores(s, history=settings.history[sid], abs_floor=settings.abs_floor[sid])
        days[sid] = out.set_index("ts").baseline_days
    fa["baseline_days"] = [int(days[sid][ts]) for sid, ts in zip(fa.target, fa.ts)]

    print(f"False alarms on {THROTTLE} (after the scorer's 30-minute dedup): {len(fa)} "
          f"across {fa.target.nunique()} pods\n")
    cross = pd.crosstab(fa.weekend_oct_10_11, fa.in_incident_window, margins=True)
    cross.index.name, cross.columns.name = "on Oct 10-11", "inside an incident window"
    print("Overlap between the two conditions:")
    print(cross.to_string())

    fa["group"] = np.select(
        [fa.weekend_oct_10_11 & ~fa.in_incident_window, fa.weekend_oct_10_11 & fa.in_incident_window,
         fa.in_incident_window],
        ["(a) Oct 10-11 weekend only", "(a)+(b) Oct 10-11 AND inside an incident window",
         "(b) inside an incident window only"], default="(c) other")
    split = fa.groupby("group").agg(false_alarms=("ts", "size"), pods=("target", "nunique"),
                                    median_baseline_days=("baseline_days", "median"))
    print("\nSplit into exclusive groups:")
    print(split.astype({"false_alarms": int, "pods": int}).to_string())
    n = split.false_alarms
    a_only, both, b_only, other = (int(n.get(k, 0)) for k in
                                   ["(a) Oct 10-11 weekend only", "(a)+(b) Oct 10-11 AND inside an incident window",
                                    "(b) inside an incident window only", "(c) other"])
    print("\nAgainst the by-hand numbers (54 / 56 / 29):")
    print(pd.DataFrame([
        {"group": "(a) on Oct 10-11", "by_hand": 54, "counting_overlap_in_b": a_only, "counting_overlap_in_a": a_only + both},
        {"group": "(b) inside an incident window, entity not affected", "by_hand": 56,
         "counting_overlap_in_b": b_only + both, "counting_overlap_in_a": b_only},
        {"group": "(c) other", "by_hand": 29, "counting_overlap_in_b": other, "counting_overlap_in_a": other},
        {"group": "total", "by_hand": 139, "counting_overlap_in_b": len(fa), "counting_overlap_in_a": len(fa)},
    ]).to_string(index=False))
    print(f"\nEntity listed in affected_entities of a concurrent incident: {int(fa.entity_affected.sum())} "
          "(must be 0 for false alarms)")

    b = fa[fa.in_incident_window]
    print("\nAll false alarms inside an incident window, by incident:")
    print(b.groupby("incidents").agg(false_alarms=("ts", "size"), on_oct_10_11=("weekend_oct_10_11", "sum"),
                                     pods=("target", "nunique"),
                                     pod_list=("entity", lambda x: ", ".join(sorted(set(x.str[4:])))))
          .to_string())
    affected = {i["incident_id"]: ", ".join(i["affected_entities"]) for i in train_incidents}
    for inc in sorted(set(b.incidents)):
        print(f"  {inc} ({next(i['scenario'] for i in train_incidents if i['incident_id'] == inc)}) "
              f"affected entities: {affected[inc]}")
    c = fa[fa.group.str.startswith("(c)")]
    print("\n(c) by day and by pod:")
    print(c.groupby(c.ts.dt.strftime("%m-%d %a")).size().rename("false_alarms").to_frame().T.to_string(index=False))
    print(c.groupby("entity").size().sort_values(ascending=False).rename("false_alarms").to_frame().T
          .to_string(index=False))
    print("\nbaseline_days at the alert, by group (rows) and number of earlier same-type days (columns):")
    print(pd.crosstab(fa.group, fa.baseline_days).to_string())
    return fa


# ---------------------------------------------------------------------------------------------- 3
def seasonal_alerts(routes: pd.DataFrame, floor: float) -> pd.DataFrame:
    """The seasonal part of the routed system with a what-if relative floor (measurement only)."""
    seasonal = routes[routes.rule == "seasonal"]
    m5 = ds.query(list(seasonal.series_id), start=START, end=TRAIN_END, step="5min")
    parts = []
    for sid, fallback, history, abs_floor in zip(seasonal.series_id, seasonal.fallback, seasonal.history,
                                                 seasonal.abs_floor):
        hot = seasonal_scores(m5[sid], history=history, abs_floor=abs_floor, floor=floor).set_index("ts").hot
        parts.append(pd.DataFrame({"ts": persistent_alerts(hot), "target": sid,
                                   "rule": "seasonal fallback" if fallback else "seasonal"}))
    return pd.concat(parts, ignore_index=True)


def floor_semantics(default_dets: pd.DataFrame, default_res: dict):
    heading("3. Floor semantics: what does the count floor (1.0, i.e. count_min_change 6.0) cost?")
    print(f"A reading is hot when |value - expected| > threshold x wobble, and wobble >= abs_floor.\n"
          f"With threshold {THRESHOLD:g} and an absolute floor of {DEFAULT_COUNT_MIN_CHANGE / THRESHOLD:g}, a count-like series "
          f"must move by more than {DEFAULT_COUNT_MIN_CHANGE:g} units\n(on a 5-minute average, in 3 of 5 readings) before it can "
          "alert, whatever its history looks like.\n")

    with_floor = route_series(catalog).set_index("series_id")
    count_series = with_floor[(with_floor.rule == "seasonal") & (with_floor.abs_floor > 0)]
    rows = []
    for sid in count_series.index:
        s = ds.query(sid, start=START, end=TRAIN_END, step="5min")[sid]
        on = in_train(seasonal_scores(s, history="day_type", abs_floor=count_series.abs_floor[sid]).set_index("ts"))
        off = in_train(seasonal_scores(s, history="day_type", abs_floor=0.0).set_index("ts"))
        scored = on[on.z.notna()]
        rows.append({
            "series_id": sid, "unit": meta.unit[sid], "abs_floor": count_series.abs_floor[sid],
            "min_alertable_change": THRESHOLD * count_series.abs_floor[sid],
            "min_change_typical": round(THRESHOLD * scored.wobble.median(), 2),
            "abs_floor_binding": f"{(scored.wobble == count_series.abs_floor[sid]).mean():.0%}",
            "train_median": round(s[s.index >= TRAIN_START].median(), 2),
            "train_max": round(s[s.index >= TRAIN_START].max(), 2),
            "hot_floor_off": int(off.hot.sum()), "hot_floor_on": int(on.hot.sum()),
            "alerts_floor_off": len(persistent_alerts(off.hot)), "alerts_floor_on": len(persistent_alerts(on.hot))})
    print("Count-like series on the seasonal rule (min_alertable_change = threshold x abs_floor;\n"
          "min_change_typical = threshold x the median wobble actually used; abs_floor_binding = share of\n"
          "train readings where the absolute floor, not MAD or the 5% floor, set the wobble;\n"
          "hot/alerts are raw counts before the scorer's dedup):")
    print(pd.DataFrame(rows).to_string(index=False))

    # incidents: measured with the floor on and off, everything else identical
    off_dets, _ = routed_detections(count_min_change=0.0)
    off_res = score(off_dets)
    routes = route_series(catalog)
    others = default_dets[~default_dets.rule.isin(["seasonal", "seasonal fallback"])]
    rel_off = score(pd.concat([seasonal_alerts(routes, floor=0.0), others]))
    no_floor_routes = route_series(catalog, count_min_change=0.0)
    both_off = score(pd.concat([seasonal_alerts(no_floor_routes, floor=0.0), others]))

    variants = {"router defaults (count floor 1.0, 5% floor)": default_res,
                "count floor off": off_res,
                "5% floor off (what-if)": rel_off,
                "both floors off (what-if)": both_off}
    print("\nSystem result under each floor setting (history day_type, threshold 6, 3-of-5 throughout):")
    print(pd.DataFrame([{"setting": n, "recall": r["recall"], "precision": r["precision"], "alerts": r["alerts"],
                         "false_pos": r["false_positives"]} for n, r in variants.items()]).to_string(index=False))

    per = {n: r["per_incident"].set_index("incident_id") for n, r in variants.items()}
    base = per["router defaults (count floor 1.0, 5% floor)"][["scenario", "actionable"]]
    key = {n: chr(ord("A") + i) for i, n in enumerate(variants)}
    print("\nPer incident, time to detect in minutes (blank = missed):   "
          + "   ".join(f"{key[n]} = {n}" for n in variants))
    table = base.join(pd.DataFrame({key[n]: p.ttd_min for n, p in per.items()}))
    print(table.round(1).to_string(na_rep=""))

    on_p, off_p = per["router defaults (count floor 1.0, 5% floor)"], per["count floor off"]
    lost = off_p.index[off_p.detected & ~on_p.detected]
    slower = off_p.index[off_p.detected & on_p.detected & (on_p.ttd_min > off_p.ttd_min + 1e-9)]
    print(f"\nMeasured: incidents detected with the count floor off but missed with it on: "
          f"{', '.join(lost) or 'none'}")
    print(f"Measured: incidents detected later because of the count floor: {', '.join(slower) or 'none'}")
    for inc in ("INC-004", "INC-011"):
        state = {key[n]: ("detected" if p.detected[inc] else "missed") for n, p in per.items()}
        print(f"  {inc}: " + ", ".join(f"{k} {v}" for k, v in state.items()))

    # why: the key-signal series of the two skew incidents, under each floor
    for inc_id in slower:
        inc = next(i for i in train_incidents if i["incident_id"] == inc_id)
        lo, hi = pd.Timestamp(inc["start"]) - GRACE, pd.Timestamp(inc["end"])
        for label, r in [("floor on", default_res), ("floor off", off_res)]:
            tbl = r["alerts_table"]
            hits = tbl[(tbl["class"] == "true") & (tbl.ts >= lo) & (tbl.ts <= hi)].sort_values("ts")
            first = hits.iloc[0]
            print(f"  {inc_id} first alert, {label}: {first.ts:%m-%d %H:%M} on {first.target} "
                  f"(incident starts {pd.Timestamp(inc['start']):%m-%d %H:%M})")

    print("\nKey-signal series of INC-004 and INC-011 inside the incident window (start - 5 min .. end).\n"
          "Per floor setting (columns as above): max |z|, hot readings / scored readings, persistent alerts.\n"
          "readings = 5-minute readings the series has in the window at all.")
    rows = []
    settings = {"A": (routes, 0.05), "B": (no_floor_routes, 0.05), "C": (routes, 0.0), "D": (no_floor_routes, 0.0)}
    for inc in [i for i in train_incidents if i["incident_id"] in ("INC-004", "INC-011")]:
        lo, hi = pd.Timestamp(inc["start"]) - GRACE, pd.Timestamp(inc["end"])
        for sid in sorted({k["series_id"] for k in inc["key_signals"]}):
            if sid not in meta.index:
                continue
            row = {"incident": inc["incident_id"], "series_id": sid, "unit": meta.unit[sid],
                   "rule": routes.set_index("series_id").rule[sid]}
            if row["rule"] == "seasonal":
                s = ds.query(sid, start=START, end=TRAIN_END, step="5min")[sid]
                for k, (r, floor) in settings.items():
                    r = r.set_index("series_id")
                    out = seasonal_scores(s, history=r.history[sid], abs_floor=r.abs_floor[sid], floor=floor)
                    out = out.set_index("ts")
                    win = out[(out.index >= lo) & (out.index <= hi)]
                    alerts = persistent_alerts(out.hot)
                    n_alerts = int(((alerts >= lo) & (alerts <= hi)).sum())
                    zmax = win.z.abs().max()
                    row[f"{k} max|z|"] = "-" if pd.isna(zmax) else ("inf" if np.isinf(zmax) else f"{zmax:.1f}")
                    row[f"{k} hot/scored"] = f"{int(win.hot.sum())}/{int(win.z.notna().sum())}"
                    row[f"{k} alerts"] = n_alerts
                row["readings"] = int(s[(s.index >= lo) & (s.index <= hi)].notna().sum())
            rows.append(row)
    print(pd.DataFrame(rows).fillna("-").to_string(index=False))
    targets = {i["incident_id"]: i["affected_entities"] for i in train_incidents
               if i["incident_id"] in ("INC-004", "INC-011")}
    for inc, ents in targets.items():
        print(f"  {inc} affected entities: {', '.join(ents)}")


# ---------------------------------------------------------------------------------------------- 4
def injection_test():
    heading("4. Injection test: a synthetic throttling jump on a copy of one series")
    end = INJECT_START + pd.Timedelta(hours=INJECT_HOURS)
    original = ds.query(INJECT_SERIES, start=START, end=TRAIN_END)[INJECT_SERIES]     # 1-minute readings
    injected = original.copy()                                                         # the COPY
    window = (injected.index >= INJECT_START) & (injected.index < end)
    before = original[window]
    injected[window] = INJECT_VALUE
    assert not injected.equals(original) and original[window].max() < INJECT_VALUE
    s5 = injected.resample("5min").mean()                                              # as the pipeline does
    print(f"Series {INJECT_SERIES}, {INJECT_START:%a %b %d %H:%M} to {end:%H:%M} UTC set to {INJECT_VALUE} "
          f"({int(window.sum())} one-minute readings).\n"
          f"Original values in that window: median {before.median():.4f}, max {before.max():.4f}. "
          f"Jump = {INJECT_VALUE - before.median():.3f}.\n"
          "Detector: seasonal_scores(history='day_type', threshold 6, 5% floor) + 3-of-5, with different "
          "absolute wobble floors.\n")
    rows = []
    for f in WOBBLE_FLOORS:
        out = seasonal_scores(s5, history="day_type", abs_floor=f).set_index("ts")
        alerts = persistent_alerts(out.hot)
        inside = alerts[(alerts >= INJECT_START) & (alerts < end)]
        win = out[(out.index >= INJECT_START) & (out.index < end)]
        outside = alerts[(alerts >= TRAIN_START) & ~((alerts >= INJECT_START) & (alerts < end + pd.Timedelta("20min")))]
        rows.append({"abs_floor": f, "min_alertable_change": round(THRESHOLD * f, 3),
                     "wobble_in_window": round(win.wobble.median(), 4), "z_in_window": round(win.z.median(), 1),
                     "hot_readings": f"{int(win.hot.sum())} of {len(win)}",
                     "detected": "yes" if len(inside) else "NO",
                     "minutes_to_alert": (inside[0] - INJECT_START).total_seconds() / 60 if len(inside) else None,
                     "alerts_elsewhere_in_train": len(outside)})
    print(pd.DataFrame(rows).to_string(index=False, na_rep="-"))
    print("\nminutes_to_alert is from the start of the jump to the third hot 5-minute reading.\n"
          "alerts_elsewhere_in_train: raw persistent alerts on this series outside the injected window "
          "(not scored, context only).")


# ---------------------------------------------------------------------------------------------- 5
def ratio_metrics(routes: pd.DataFrame, res: dict):
    heading('5. Metrics whose unit is "ratio"')
    r = catalog[catalog.unit == "ratio"].merge(routes, on="series_id")
    a = res["alerts_table"]
    fa = a[a["class"] == "false_positive"].groupby("target").size()
    r["false_alarms"] = r.series_id.map(fa).fillna(0).astype(int)
    warm = ds.query(list(r.series_id), start=START, end=str(TRAIN_START.date()), step="5min")
    r["warmup_median"] = r.series_id.map(warm.median())
    table = r.groupby(["metric", "default_detector", "canonical_event"]).agg(
        series=("series_id", "size"), rule=("rule", "first"), fallback=("fallback", "first"),
        abs_floor=("abs_floor", "first"), warmup_median=("warmup_median", "median"),
        train_false_alarms=("false_alarms", "sum")).reset_index()
    table["warmup_median"] = table.warmup_median.round(4)
    print(table.to_string(index=False))
    print("\nwarmup_median: median over the metric's series of each series' warm-up week median (5-minute steps).")


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    dets, routes, res = reproduce()
    throttle_false_alarms(res, routes)
    floor_semantics(dets, res)
    injection_test()
    ratio_metrics(routes, res)
