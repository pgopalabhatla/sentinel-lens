"""Score the change point detector on the TRAIN split, three ways.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/score_change_point.py

The change point detector (CUSUM on the seasonal z-score) is tried on the 24 series the catalog marks
`change_point` (JVM GC time, checkpoint duration, produce latency, batch duration, query latency):

    (a) current system         routed system (router defaults) + job run baseline; those 24 series are
                               on the seasonal fallback
    (b) change point only      on those 24 series the seasonal fallback is REPLACED by change point
    (c) both                   those 24 series keep the seasonal fallback AND get change point
                               (this is what route_series(change_point=True) means)

Defaults k = 0.5, h = 10, clip = 3 are stated in sentinel_lens/detectors/change_point.py and are not
tuned here; the sweep at the end is report-only. Pages come from the correlator with its defaults; no
feedback is applied, so every page pages.

TRAIN only: data is read through DataSource with the clock at the end of train, and only train
labels are used. The test week is not touched.
"""
import itertools
import warnings

import pandas as pd

from scripts.score_formulas import current_alerts
from scripts.score_seasonal import DATA, START, TRAIN_END, ds
from sentinel_lens.alerts import alerts_from_change_point, to_frame
from sentinel_lens.correlator import build_topology, correlate
from sentinel_lens.detectors.change_point import change_point_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_lens.router import route_series
from sentinel_sim.evaluate import evaluate_detections

TRAIN_START, TRAIN_END_TS = pd.Timestamp("2026-10-05", tz="UTC"), pd.Timestamp(TRAIN_END, tz="UTC")
DEFAULTS = {"k": 0.5, "h": 10.0, "clip": 3.0}
SWEEP = {"k": (0.25, 0.5, 1.0), "h": (5.0, 8.0, 10.0, 12.0), "clip": (2.0, 3.0, 5.0)}
EXPECTED_A = {"recall": 1.0, "precision": 0.604, "alerts": 275, "false_positives": 103, "pages": 46, "FALSE_pages": 27}


class Judge:
    """The scorer's verdict on raw alerts, cached by (time, target): class, and labeled incidents matched."""

    def __init__(self):
        self.klass, self.matched = {}, {}

    def add(self, alerts):
        new = sorted({(a.time, a.target) for a in alerts} - set(self.klass))
        if not new:
            return
        frame = pd.DataFrame(new, columns=["ts", "target"])
        res = evaluate_detections(frame, DATA, split="train", dedup="0min")
        classes = res["alerts_table"]["class"].reindex(range(len(new)))
        for i, key in enumerate(new):
            self.klass[key] = classes[i]
            if classes[i] == "false_positive":
                self.matched[key] = frozenset()
            else:
                pi = evaluate_detections(frame.iloc[[i]], DATA, split="train", dedup="0min")["per_incident"]
                self.matched[key] = frozenset(pi.incident_id[pi.detected])

    def of(self, a):
        return self.klass[(a.time, a.target)]


def measure(alerts: list, judge: Judge, topology) -> dict:
    """Alert-level numbers from the scorer as usual, page-level numbers from the correlator."""
    res = evaluate_detections(to_frame(alerts), DATA, split="train")
    judge.add(alerts)
    verdicts = []
    for inc in correlate(alerts, topology):
        classes = {judge.of(a) for a in inc.alerts}
        verdicts.append("TRUE" if "true" in classes else ("expected" if "expected" in classes else "FALSE"))
    pi = res["per_incident"].set_index("incident_id")
    return {"recall": float(res["recall"]), "precision": float(res["precision"]), "alerts": int(res["alerts"]),
            "false_positives": int(res["false_positives"]), "median_ttd_min": round(float(res["median_ttd_min"]), 1),
            "pages": len(verdicts), "TRUE_pages": verdicts.count("TRUE"), "FALSE_pages": verdicts.count("FALSE"),
            "per_incident": pi}


def compare(a: dict, other: dict) -> tuple:
    """(lost, later, sooner) incidents of `other` against the current system."""
    pa, po = a["per_incident"], other["per_incident"]
    lost = [i for i in pa.index if pa.detected[i] and not po.detected[i]]
    both = [i for i in pa.index if pa.detected[i] and po.detected[i]]
    later = [f"{i} ({pa.ttd_min[i]:.1f}->{po.ttd_min[i]:.1f})" for i in both if po.ttd_min[i] > pa.ttd_min[i] + 1e-9]
    sooner = [f"{i} ({pa.ttd_min[i]:.1f}->{po.ttd_min[i]:.1f})" for i in both if po.ttd_min[i] < pa.ttd_min[i] - 1e-9]
    return lost, later, sooner


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    catalog = ds.series()
    topology = build_topology(ds.relationships(), catalog)
    routes = route_series(ds.series(layer=[1, 2, 3]), change_point=True)
    cp_routes = routes[routes.change_point]
    cp_series = set(cp_routes.series_id)

    base = [a for a in current_alerts()[0] if TRAIN_START <= a.time < TRAIN_END_TS]
    fallback_on_cp = [a for a in base if a.detector == "seasonal fallback" and a.target in cp_series]
    without_fallback = [a for a in base if not (a.detector == "seasonal fallback" and a.target in cp_series)]

    # the z-score each of the 24 series already gets from the seasonal detector, with the router's settings
    m5 = ds.query(list(cp_routes.series_id), start=START, end=TRAIN_END, step="5min")
    z = {}
    for sid, history, abs_floor in zip(cp_routes.series_id, cp_routes.history, cp_routes.abs_floor):
        s = seasonal_scores(m5[sid], history=history, abs_floor=abs_floor).set_index("ts").z
        s.name = sid
        z[sid] = s

    def cp_alerts(k, h, clip) -> list:
        out = []
        for sid in cp_routes.series_id:
            out += alerts_from_change_point(change_point_alerts(z[sid], k=k, h=h, clip=clip), target=sid, h=h)
        return [a for a in out if TRAIN_START <= a.time < TRAIN_END_TS]

    judge = Judge()
    a = measure(base, judge, topology)
    got = {k: a[k] for k in EXPECTED_A}
    assert got == EXPECTED_A, f"the current system changed: {got}"
    cp = cp_alerts(**DEFAULTS)
    b = measure(without_fallback + cp, judge, topology)
    c = measure(base + cp, judge, topology)

    metrics = cat_metrics = sorted({s.split("|")[0] for s in cp_series})
    print(f"CHANGE POINT on the TRAIN split (k {DEFAULTS['k']}, h {DEFAULTS['h']:g}, clip {DEFAULTS['clip']:g}: defaults, "
          "not tuned)\n")
    print(f"Series the catalog marks change_point: {len(cp_series)} ({', '.join(cat_metrics)}).")
    print(f"Raw alerts on those series in the train window: seasonal fallback {len(fallback_on_cp)}, "
          f"change point {len(cp)}.\n")

    cols = ["recall", "precision", "alerts", "false_positives", "median_ttd_min", "pages", "TRUE_pages", "FALSE_pages"]
    print("1. The three systems (alerts after the scorer's 30-minute dedup; pages from the correlator, no feedback)")
    print(pd.DataFrame([{"system": n, **{k: m[k] for k in cols}} for n, m in
                        [("(a) current system", a), ("(b) change point only on the 24", b),
                         ("(c) both on the 24", c)]]).to_string(index=False))

    print("\n2. Per incident, time to detect in minutes (blank = missed)")
    per = a["per_incident"][["scenario", "actionable"]].copy()
    for name, m in [("(a)", a), ("(b)", b), ("(c)", c)]:
        per[name] = m["per_incident"].ttd_min.round(1)
    print(per.to_string(na_rep=""))
    for name, m in [("(b)", b), ("(c)", c)]:
        lost, later, sooner = compare(a, m)
        print(f"   {name} against (a): lost {', '.join(lost) or 'none'}; later {', '.join(later) or 'none'}; "
              f"sooner {', '.join(sooner) or 'none'}")

    print("\n3. The two detectors on the 24 series, raw alerts by the scorer's class")
    rows = []
    for name, group in [("seasonal fallback", fallback_on_cp), ("change point", cp)]:
        classes = pd.Series([judge.of(x) for x in group]).value_counts() if group else {}
        hit = sorted(set().union(*[judge.matched[(x.time, x.target)] for x in group])) if group else []
        rows.append({"detector": name, "raw_alerts": len(group), "true": int(classes.get("true", 0)),
                     "expected": int(classes.get("expected", 0)), "false": int(classes.get("false_positive", 0)),
                     "incidents_matched": ", ".join(hit) or "none"})
    print(pd.DataFrame(rows).to_string(index=False))

    print(f"\n4. Every change point alert on train ({len(cp)})")
    listing = pd.DataFrame([{"ts": f"{x.time:%m-%d %H:%M}", "series": x.target, "direction": x.direction,
                             "total": round(x.measure, 1), "class": judge.of(x),
                             "matches": ", ".join(sorted(judge.matched[(x.time, x.target)])),
                             "explanation": x.explanation.split(": ", 1)[-1]}
                            for x in sorted(cp, key=lambda x: (x.time, x.target))])
    if len(listing):
        with pd.option_context("display.max_rows", 500, "display.width", 250, "display.max_colwidth", 90):
            print(listing.to_string(index=False))
        by_metric = listing.assign(metric=listing.series.str.split("|").str[0]).groupby(["metric", "class"]).size() \
            .unstack(fill_value=0).reindex(columns=["true", "expected", "false_positive"], fill_value=0)
        print("\n   By metric:")
        print(by_metric.to_string())
        print(f"\n   True change point alerts: {int((listing['class'] == 'true').sum())} of {len(listing)}; "
              f"direction up {int((listing.direction == 'up').sum())}, down {int((listing.direction == 'down').sum())}.")

    # ------------------------------------------------------------------ why: is z unit noise?
    print("\n4b. What the detector assumes vs. what the z-scores look like (train readings, clipped at +/-3)\n"
          "   CUSUM with k = 0.5, h = 10 expects quiet readings to be independent noise of about 1 wobble.")
    rows = [{"metric": "what the defaults assume: N(0, 1)", "series": "-", "std": 1.0, "share_|z|>1": "32%",
             "share_|z|>=3": "0.3%", "lag1_autocorr": 0.0, "median_run_same_sign": 1, "longest_run_same_sign": "-"}]
    for metric in metrics:
        zs = [z[sid][z[sid].index >= TRAIN_START].dropna().clip(-3, 3) for sid in sorted(cp_series) if sid.startswith(metric + "|")]
        pooled = pd.concat(zs)
        runs = []
        for s in zs:
            sign = (s > 0).astype(int)
            runs += sign.groupby((sign != sign.shift()).cumsum()).size().tolist()
        auto = [s.autocorr(1) for s in zs if len(s) > 10 and s.std() > 0]
        rows.append({"metric": metric, "series": len(zs), "std": round(pooled.std(), 2),
                     "share_|z|>1": f"{(pooled.abs() > 1).mean():.0%}", "share_|z|>=3": f"{(pooled.abs() >= 3).mean():.1%}",
                     "lag1_autocorr": round(float(pd.Series(auto).median()), 2),
                     "median_run_same_sign": int(pd.Series(runs).median()), "longest_run_same_sign": int(max(runs))})
    print(pd.DataFrame(rows).to_string(index=False))

    # ------------------------------------------------------------------ report-only sweep
    print("\n5. Sweep (report only; default marked *). cp_* = raw change point alerts by class; (b) and (c) as above.\n"
          f"   Current system (a): recall {a['recall']}, precision {a['precision']}, {a['false_positives']} false alerts, "
          f"{a['pages']} pages, {a['FALSE_pages']} false pages.")
    rows = []
    for k, h, clip in itertools.product(*SWEEP.values()):
        alerts_ = cp_alerts(k, h, clip)
        mb = measure(without_fallback + alerts_, judge, topology)
        mc = measure(base + alerts_, judge, topology)
        classes = pd.Series([judge.of(x) for x in alerts_]).value_counts() if alerts_ else {}
        row = {"": "*" if {"k": k, "h": h, "clip": clip} == DEFAULTS else "", "k": k, "h": h, "clip": clip,
               "cp_raw": len(alerts_), "cp_true": int(classes.get("true", 0)),
               "cp_false": int(classes.get("false_positive", 0))}
        for name, m in [("b", mb), ("c", mc)]:
            lost, later, _ = compare(a, m)
            row.update({f"({name})recall": m["recall"], f"({name})prec": m["precision"],
                        f"({name})FP": m["false_positives"], f"({name})pages": m["pages"],
                        f"({name})false_pages": m["FALSE_pages"],
                        f"({name})lost/later": (", ".join(lost + [x.split(' ')[0] + ' later' for x in later]) or "none")})
        rows.append(row)
    with pd.option_context("display.width", 300, "display.max_colwidth", 60):
        print(pd.DataFrame(rows).to_string(index=False))
