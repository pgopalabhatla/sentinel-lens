"""POST-HOC analysis of the test week. NOT the pre-registered result.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/posthoc_test_week.py

The pre-registered result is docs/results/test-week.md and stays as it is. This script was written
AFTER that result was seen, to explain it. It is read-only: it changes no detector, router or config
code, tunes nothing and does not invoke the frozen runner. It recomputes the same alerts with the
frozen runner's own functions (imported, not re-run) and first checks that they reproduce the numbers
in the pre-registered result, so everything below describes exactly those alerts.

    a. INC-015: what systems 1 and 2 alerted on, and which rule stopped each of those series in system 3
    b. INC-013: when the first alert came, and (EXPLORATORY) when the saturation forecast would have
    c. time to detect of system 5 on the same footing as systems 1-4; the INC-014 raw alert
    d. false-alert counts for systems 1-5

Test-week labels are read here (incident windows, key signals); that is allowed only because the
evaluation is over. Nothing learned here goes back into the frozen system.
"""
import warnings

import numpy as np
import pandas as pd

from scripts.run_test_week import (DATA, HISTORY_START, SPLITS, final_alerts, first_build_detections, in_window, judge,
                                   naive_detections, page_measures, score)
from sentinel_lens import feedback, router
from sentinel_lens.alerts import to_frame
from sentinel_lens.calibration import COUNT_UNITS
from sentinel_lens.correlator import build_topology, correlate
from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.saturation import saturation_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_sim.evaluate import evaluate_detections
from sentinel_sim.replay import DataSource, load_labels

SPLIT = "test"
END = SPLITS[SPLIT][1]
GRACE = pd.Timedelta("5min")                       # the scorer's own window: start - 5 min .. end
PRE_REGISTERED = {"precision_1": 0.256, "precision_2": 0.294, "precision_3": 0.742, "precision_5": 0.79,
                  "pages_4": 26, "false_pages_4": 10, "pages_5": 24, "false_pages_5": 8}


def heading(text):
    print(f"\n{'=' * 110}\n{text}\n{'=' * 110}")


def minutes(ts, start) -> float:
    return round((ts - start).total_seconds() / 60, 1)


def matches(ts, target, incident_id) -> bool:
    """Does the scorer count an alert (ts, target) for this labeled incident? Asked of the scorer itself."""
    pi = evaluate_detections(pd.DataFrame([{"ts": ts, "target": target}]), DATA, split=SPLIT, dedup="0min")["per_incident"]
    return bool(pi.set_index("incident_id").detected[incident_id])


def matching_alerts(dets: pd.DataFrame, inc: dict) -> pd.DataFrame:
    lo, hi = pd.Timestamp(inc["start"]) - GRACE, pd.Timestamp(inc["end"])
    d = dets[(pd.to_datetime(dets.ts, utc=True) >= lo) & (pd.to_datetime(dets.ts, utc=True) <= hi)]
    keep = [matches(r.ts, r.target, inc["incident_id"]) for r in d.itertuples()]
    return d[keep].sort_values(["ts", "target"])


def naive_detail(ds, sid: str) -> pd.DataFrame:
    """System 1's own numbers for one gauge: value, expected (median of the previous 7 days), wobble, z."""
    m = ds.query([sid], start=HISTORY_START, end=END, step="5min")
    lagged = np.stack([m.shift(288 * k).values for k in range(1, 8)])
    med = np.nanmedian(lagged, axis=0)
    mad = np.nanmedian(np.abs(lagged - med), axis=0) * 1.4826 + 1e-9
    return pd.DataFrame({"value": m[sid].values, "expected": med[:, 0], "wobble": mad[:, 0],
                         "z": ((m.values - med) / mad)[:, 0]}, index=m.index)


def window_summary(scores: pd.DataFrame, lo, hi) -> dict:
    scores = scores.set_index("ts")
    win = scores[(scores.index >= lo) & (scores.index <= hi)]
    alerts = persistent_alerts(scores.hot)
    n_alerts = int(((alerts >= lo) & (alerts <= hi)).sum())
    zmax = win.z.abs().max()
    return {"scored": int(win.z.notna().sum()), "hot": int(win.hot.sum()),
            "max|z|": "-" if pd.isna(zmax) else ("inf" if np.isinf(zmax) else round(float(zmax), 1)),
            "alerts": n_alerts, "win": win}


# =========================================================================================== a
def section_a(ds, inc, dets1, dets2, routes, catalog):
    heading("a. INC-015 (trino_saturation): detected by systems 1 and 2, missed by systems 3-5")
    start, end = pd.Timestamp(inc["start"]), pd.Timestamp(inc["end"])
    lo = start - GRACE
    print(f"Labeled window: {start:%m-%d %H:%M} to {end:%m-%d %H:%M} UTC ({minutes(end, start):.0f} min). "
          f"Affected entities: {', '.join(inc['affected_entities'])}.")
    print("Key-signal series in the label: " + ", ".join(sorted({k["series_id"] for k in inc["key_signals"]})))

    series = set()
    for name, dets, detail in [("System 1 (naive)", dets1, "naive"), ("System 2 (first build)", dets2, "first")]:
        hit = matching_alerts(dets, inc)
        rows = []
        for sid, g in hit.groupby("target"):
            ts = pd.to_datetime(g.ts, utc=True).min()
            row = {"series": sid, "first_alert": f"{ts:%m-%d %H:%M}", "min_from_start": minutes(ts, start),
                   "raw_alerts": len(g)}
            if "|" in sid and not sid.split("|")[0].endswith("_total"):
                if detail == "naive":
                    d = naive_detail(ds, sid)
                else:
                    d = seasonal_scores(ds.query([sid], start=HISTORY_START, end=END, step="5min")[sid], threshold=6.0,
                                        floor=0.05, days=7, min_days=1, history="previous_days", abs_floor=0.0).set_index("ts")
                at = d.loc[ts]
                win = d[(d.index >= lo) & (d.index <= end)]
                peak = win.loc[win.z.abs().idxmax()]
                row.update({"z_at_alert": round(float(at.z), 1), "value": round(float(at.value), 3),
                            "expected": round(float(at.expected), 3), "wobble": round(float(at.wobble), 4),
                            "peak_z": round(float(peak.z), 1), "peak_value": round(float(peak.value), 3)})
                series.add(sid)
            else:
                row["z_at_alert"] = "(counter or run rule, no z)"
            rows.append(row)
        print(f"\n{name}: {len(hit)} raw alerts matching INC-015 on {hit.target.nunique()} series")
        print(pd.DataFrame(rows).to_string(index=False, na_rep="-") if rows else "  none")

    print("\nHow the final router treats each of those series:")
    r = routes.set_index("series_id")
    meta = catalog.set_index("series_id")
    rows = []
    for sid in sorted(series):
        metric = sid.split("|")[0]
        why_floor = ("per-metric override" if metric in router.METRIC_MIN_CHANGE else
                     ("count-like unit rule" if meta.unit[sid] in COUNT_UNITS else "none"))
        rows.append({"series": sid, "unit": meta.unit[sid], "catalog_detector": meta.default_detector[sid],
                     "rule": r.rule[sid], "fallback": bool(r.fallback[sid]), "history": r.history[sid],
                     "min_change": r.min_change[sid], "abs_floor": r.abs_floor[sid], "min_change_from": why_floor})
    print(pd.DataFrame(rows).to_string(index=False))

    print("\nWhich rule stops each series in system 3? The same seasonal detector, four settings, inside the\n"
          "incident window (start - 5 min .. end). A = first build (previous_days history, no absolute floor);\n"
          "B = day_type history only; C = the router's absolute floor only; D = final system (both).\n"
          "largest_rise = the largest value - expected under the final history, to compare with min_change.")
    rows, verdicts = [], []
    for sid in sorted(series):
        s = ds.query([sid], start=HISTORY_START, end=END, step="5min")[sid]
        floor = float(r.abs_floor[sid])
        variants = {"A": ("previous_days", 0.0), "B": ("day_type", 0.0), "C": ("previous_days", floor), "D": ("day_type", floor)}
        res = {k: window_summary(seasonal_scores(s, history=h, abs_floor=f), lo, end) for k, (h, f) in variants.items()}
        win = res["D"]["win"]
        scored = win[win.z.notna()]
        binding = ("absolute floor" if floor > 0 and np.isclose(scored.wobble, floor).mean() > 0.5 else
                   ("5% floor" if np.isclose(scored.wobble, 0.05 * scored.expected.abs()).mean() > 0.5 else "MAD"))
        row = {"series": sid, "min_change": r.min_change[sid],
               "largest_rise": round(float((scored.value - scored.expected).max()), 2) if len(scored) else None,
               "wobble_set_by_(D)": binding}
        for k in "ABCD":
            row[f"{k}: hot/scored"] = f"{res[k]['hot']}/{res[k]['scored']}"
            row[f"{k}: max|z|"] = res[k]["max|z|"]
            row[f"{k}: alerts"] = res[k]["alerts"]
        rows.append(row)
        a_, b_, c_, d_ = (res[k]["alerts"] > 0 for k in "ABCD")
        if d_:
            verdict = "alerts in the final system too (but not on an entity the label lists)"
        elif not a_:
            verdict = "does not alert under the first-build settings in this window either"
        elif b_ and not c_:
            verdict = f"stopped by the minimum change ({r.min_change[sid]:g} units; absolute floor {floor:g}): day_type history alone still alerts"
        elif c_ and not b_:
            verdict = "stopped by the day_type history: the absolute floor alone still alerts"
        elif not b_ and not c_:
            verdict = "stopped by either change alone: day_type history, and separately the minimum change"
        else:
            verdict = "stopped only by the two together: each alone still alerts"
        verdicts.append((sid, verdict))
    with pd.option_context("display.width", 300):
        print(pd.DataFrame(rows).to_string(index=False, na_rep="-"))
    print("\nVerdict per series:")
    for sid, v in verdicts:
        print(f"  {sid}: {v}")


# =========================================================================================== b
def section_b(ds, inc, alerts, dets1, class_matched, routes):
    heading("b. INC-013 (memory_leak): first alert vs. incident start; saturation forecast (EXPLORATORY)")
    start, end = pd.Timestamp(inc["start"]), pd.Timestamp(inc["end"])
    print(f"Labeled window: {start:%m-%d %H:%M} to {end:%m-%d %H:%M} UTC ({minutes(end, start) / 60:.1f} h). "
          f"Affected entities: {', '.join(inc['affected_entities'])}.")
    _, matched_of = class_matched
    mine = sorted((a for a in alerts if inc["incident_id"] in matched_of[a]), key=lambda a: (a.time, a.target))
    first = pd.DataFrame([{"series": t, "detector": g[0].detector, "first_alert": f"{g[0].time:%m-%d %H:%M}",
                           "min_from_start": minutes(g[0].time, start), "raw_alerts": len(g),
                           "explanation": g[0].explanation[:95]}
                          for t, g in ((t, [a for a in mine if a.target == t]) for t in sorted({a.target for a in mine}))])
    print(f"\nFinal system (3): {len(mine)} raw alerts match the incident. First alert per series:")
    with pd.option_context("display.width", 300, "display.max_colwidth", 100):
        print(first.sort_values("min_from_start").to_string(index=False))
    hit1 = matching_alerts(dets1, inc)
    f1 = hit1.assign(ts=pd.to_datetime(hit1.ts, utc=True)).groupby("target").ts.min().sort_values()
    print("\nSystem 1 (naive), first alert per series, for comparison:")
    print(pd.DataFrame({"first_alert": f1.dt.strftime("%m-%d %H:%M"), "min_from_start": [minutes(t, start) for t in f1]})
          .head(6).to_string())

    print("\nEXPLORATORY ONLY: the saturation forecast is OFF in the frozen system. With its defaults (window 60 min,\n"
          "horizon 4 h, R2 >= 0.9, usage >= 0.5) on the memory series of the affected pods, limits from the topology:")
    limits = dict(zip(ds.pods().pod, ds.pods().memory_limit_bytes))
    rows, seasonal_rows = [], []
    for ent in inc["affected_entities"]:
        if not ent.startswith("pod:"):
            continue
        pod = ent.split(":", 1)[1]
        sid = f"container_memory_working_set_bytes|{pod}"
        s = ds.query([sid], start=HISTORY_START, end=END, step="5min")[sid]
        sat = saturation_alerts(s, limits[pod])
        sat = sat[(sat.ts >= start - GRACE) & (sat.ts <= end)]
        seasonal_first = min((a.time for a in mine if a.target == sid), default=None)
        usage = s[(s.index >= start) & (s.index <= end)] / limits[pod]
        r = routes.set_index("series_id").loc[sid]
        sc = seasonal_scores(s, history=r.history, abs_floor=r.abs_floor).set_index("ts")
        win = sc[(sc.index >= start) & (sc.index <= end) & sc.z.notna()]
        first_hot = win.index[win.hot][0] if win.hot.any() else None
        at = win.loc[first_hot] if first_hot is not None else None
        seasonal_rows.append({
            "memory_series": sid, "first_hot_reading_min": minutes(first_hot, start) if first_hot is not None else None,
            "value/expected_then": round(float(at.value / at.expected), 3) if at is not None else None,
            "wobble/expected_then": round(float(at.wobble / at.expected), 3) if at is not None else None,
            "wobble_set_by": ("5% floor" if at is not None and np.isclose(at.wobble, 0.05 * abs(at.expected)) else "MAD"),
            "rise_needed_to_be_hot": f"{6 * float(at.wobble / at.expected):.0%}" if at is not None else None,
            "first_reading_10%_above_expected_min": minutes(win.index[win.value > 1.1 * win.expected][0], start)
            if (win.value > 1.1 * win.expected).any() else None})
        rows.append({"memory_series": sid, "limit_GiB": limits[pod] / 1024 ** 3,
                     "usage_at_start": round(float(usage.iloc[0]), 2), "max_usage": round(float(usage.max()), 2),
                     "seasonal_first_alert_min": minutes(seasonal_first, start) if seasonal_first is not None else None,
                     "forecast_first_alert_min": minutes(sat.ts.iloc[0], start) if len(sat) else None,
                     "forecast_alerts": len(sat),
                     "forecast_says": sat.explanation.iloc[0] if len(sat) else "never alerts in the window"})
    with pd.option_context("display.width", 300, "display.max_colwidth", 100):
        print(pd.DataFrame(rows).to_string(index=False, na_rep="-"))
        print("\nWhat the final system's seasonal detector saw on those memory series (frozen settings, threshold 6):")
        print(pd.DataFrame(seasonal_rows).to_string(index=False, na_rep="-"))


# =========================================================================================== c
def section_c(inc14, alerts, demoted, labeled, res3, res5, p4, p5, class_matched):
    heading("c. Time to detect of system 5 on the same footing as systems 1-4; the INC-014 raw alert")
    print("The pre-registered table timed systems 1-4 with the scorer after its 30-minute dedup, but system 5 on raw\n"
          "alerts (the moment its page pages). 'dedup' columns below use the scorer's timing for both; 'raw' columns\n"
          "use the page timing for both. System 5 = the alerts that are not demoted.")
    per = labeled[["scenario", "actionable"]].copy()
    per["(3)=(4) dedup"] = res3["per_incident"].set_index("incident_id").ttd_min.round(1)
    per["(5) dedup"] = res5["per_incident"].set_index("incident_id").ttd_min.round(1)
    per["(4) raw"] = p4["per_incident"].ttd_min.astype(float).round(1)
    per["(5) raw, as pre-registered"] = p5["per_incident"].ttd_min.astype(float).round(1)
    print(per.to_string(na_rep=""))
    act = per[per.actionable]
    summary = pd.DataFrame({c: {"detected": int(act[c].notna().sum()), "median_ttd_min": round(act[c].median(), 1),
                                "max_ttd_min": round(act[c].max(), 1)} for c in per.columns[2:]}).T
    print("\nOver the actionable incidents:")
    print(summary.to_string())
    diff = per[(per["(3)=(4) dedup"] - per["(5) dedup"]).abs() > 1e-9]
    print(f"\nIncidents where system 5 differs from system 3 on the same (dedup) timing: "
          f"{', '.join(f'{i} ({a:.1f} -> {b:.1f})' for i, a, b in zip(diff.index, diff['(3)=(4) dedup'], diff['(5) dedup'])) or 'none'}")

    start = pd.Timestamp(inc14["start"])
    class_of, matched_of = class_matched
    mine = sorted((a for a in alerts if "INC-014" in matched_of[a]), key=lambda a: (a.time, a.target))
    print(f"\nINC-014 ({inc14['scenario']}): labeled start {start:%m-%d %H:%M:%S} UTC. Earliest raw alerts that match it:")
    rows = [{"time": f"{a.time:%m-%d %H:%M}", "min_from_start": minutes(a.time, start), "series": a.target,
             "detector": a.detector, "demoted_in_(5)": "yes" if a in demoted else "", "explanation": a.explanation[:80]}
            for a in mine[:8]]
    with pd.option_context("display.width", 300, "display.max_colwidth", 90):
        print(pd.DataFrame(rows).to_string(index=False))
    kept = set(zip(res3["alerts_table"].ts, res3["alerts_table"].target))
    print("\nWhy the scorer times INC-014 at 19.0 min: it keeps one alert per target per 30 minutes, and these series were\n"
          "already alerting before the labeled start. Raw alerts from 60 min before to 25 min after the start; * = kept by\n"
          "the scorer's dedup; the scorer's window for the incident opens at -5 min:")
    for target in sorted({a.target for a in mine[:8]}):
        near = sorted((a for a in alerts if a.target == target
                       and start - pd.Timedelta("60min") <= a.time <= start + pd.Timedelta("25min")), key=lambda a: a.time)
        marks = " ".join(f"{minutes(a.time, start):+.0f}{'*' if (a.time, a.target) in kept else ''}" for a in near)
        print(f"  {target}\n      minutes from start: {marks}")
    before = [a for a in alerts if a.target in {x.target for x in mine[:8]} and a.time < start - GRACE
              and a.time >= start - pd.Timedelta("60min")]
    print(f"  Raw alerts on these series in the hour before the scorer's window opens: {len(before)} "
          f"(scorer class: {pd.Series([class_of[a] for a in before]).value_counts().to_dict() if before else {}})")


# =========================================================================================== d
def section_d(res1, res2, res3, res5, p4, p5):
    heading("d. False-alert counts per system (alerts after the scorer's 30-minute dedup; pages from the correlator)")
    rows = []
    for name, res, p in [("(1) naive full baseline", res1, None), ("(2) seasonal everywhere (first build)", res2, None),
                         ("(3) final alerts", res3, None), ("(4) final alerts + correlator pages", res3, p4),
                         ("(5) final + correlator + frozen feedback", res5, p5)]:
        cls = res["alerts_table"]["class"].value_counts()
        rows.append({"system": name, "alerts": int(res["alerts"]), "true_alerts": int(cls.get("true", 0)),
                     "expected_alerts": int(cls.get("expected", 0)), "false_alerts": int(res["false_positives"]),
                     "false_alerts_per_day": round(res["false_positives"] / 7, 1),
                     "pages": p["pages"] if p else None, "false_pages": p["FALSE_pages"] if p else None})
    print(pd.DataFrame(rows, dtype=object).to_string(index=False, na_rep="-"))
    f1, f2, f3 = (int(r["false_positives"]) for r in (res1, res2, res3))
    print(f"\nPredicted ordering on false volume, 5 <= 4 < 3 < 2 < 1 (systems 1-3 count alerts, 4-5 count pages):\n"
          f"  {p5['FALSE_pages']} <= {p4['FALSE_pages']} < {f3} < {f2} < {f1}: "
          f"{'holds' if p5['FALSE_pages'] <= p4['FALSE_pages'] < f3 < f2 < f1 else 'DOES NOT hold'}")


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    print("POST-HOC, not the pre-registered result. Written after docs/results/test-week.md was seen; read-only.")
    ds = DataSource(DATA).at(END)
    catalog = ds.series()
    canonical_event_of = dict(zip(catalog.series_id, catalog.canonical_event))
    routes = router.route_series(ds.series(layer=[1, 2, 3]))
    incidents = {i["incident_id"]: i for i in load_labels(DATA)[0] if i["split"] == SPLIT}

    # the same alerts as the pre-registered run, built with the frozen runner's functions
    train_ds = DataSource(DATA).at(SPLITS["train"][1])
    train_alerts = in_window(final_alerts(train_ds, SPLITS["train"][1]), "train")
    _, _, _, train_labels, _ = judge(train_alerts, "train")
    frozen = feedback.demoted_groups(feedback.replay(train_alerts, train_labels, canonical_event_of).groups)
    dets1, dets2 = naive_detections(ds, END), first_build_detections(ds, END)
    res1, res2 = score(dets1, SPLIT), score(dets2, SPLIT)
    alerts = in_window(final_alerts(ds, END), SPLIT)
    class_of, matched_of, labeled, _, res3 = judge(alerts, SPLIT)
    demoted = feedback.apply_frozen(alerts, frozen, canonical_event_of)
    res5 = score(to_frame([a for a in alerts if a not in demoted]), SPLIT)
    pages = correlate(alerts, build_topology(ds.relationships(), catalog))
    p4 = page_measures(pages, class_of, matched_of, labeled, frozenset(), 7)
    p5 = page_measures(pages, class_of, matched_of, labeled, demoted, 7)

    got = {"precision_1": float(res1["precision"]), "precision_2": float(res2["precision"]),
           "precision_3": float(res3["precision"]), "precision_5": float(res5["precision"]),
           "pages_4": p4["pages"], "false_pages_4": p4["FALSE_pages"], "pages_5": p5["pages"], "false_pages_5": p5["FALSE_pages"]}
    print(f"\nSame alerts as the pre-registered run (precision of systems 1, 2, 3, 5; pages and false pages of 4, 5): "
          f"{got == PRE_REGISTERED}")
    assert got == PRE_REGISTERED, f"not the alerts of the pre-registered run: {got}"

    section_a(ds, incidents["INC-015"], dets1, dets2, routes, catalog)
    section_b(ds, incidents["INC-013"], alerts, dets1, (class_of, matched_of), routes)
    section_c(incidents["INC-014"], alerts, demoted, labeled, res3, res5, p4, p5, (class_of, matched_of))
    section_d(res1, res2, res3, res5, p4, p5)
    print("\nPOST-HOC. Nothing here replaces docs/results/test-week.md, and nothing was changed or re-tuned.")
