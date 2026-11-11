"""Score the feedback loop on the TRAIN split with an online replay.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/score_feedback.py

Setup
    alerts   every raw alert of the current system in the train window (routed system + job run
             baseline), grouped into pages by the correlator with its defaults. Feedback never
             changes which alerts fire or how they are grouped.
    labels   simulated engineer labels = the existing scorer's own verdicts, with its usual 30-minute
             dedup: one label per target per 30 minutes, 'true' or 'false_positive'. 'expected' is
             not a label. The scorer is untouched.
    replay   alerts are processed in time order. A label becomes available `label_delay_min` (60)
             after its alert and only affects later alerts. An alert whose group is demoted at that
             moment stays attached to its page, but a page only PAGES if it has at least one alert
             that is not demoted.

Defaults n_min = 10, tau = 0.25, label_delay = 60 min are stated in sentinel_lens/feedback.py and not
tuned here; the sweep at the end is report-only.

How a labeled incident is counted: it is detected if a PAGING page contains an alert that matches it.
Its time to detect is the moment that page pages (its first alert that is not demoted), but not
before the page's first alert matching the incident. Without demotions this is today's first-alert
time to detect.

Data is read only through DataSource with the clock at the end of train; train labels only.
"""
import itertools
import warnings

import numpy as np
import pandas as pd

from scripts.score_formulas import current_alerts
from scripts.score_incidents import TRAIN_END_TS, TRAIN_START, judge_alerts, judge_pages
from scripts.score_seasonal import DATA, ds
from sentinel_lens.alerts import to_frame
from sentinel_lens.correlator import build_topology, correlate
from sentinel_lens.feedback import FALSE, LABEL_DELAY_MIN, N_MIN, TAU, TRUE, pages, paging_time, replay
from sentinel_sim.evaluate import evaluate_detections

DAYS = 14
SWEEP = {"n_min": (5, 10, 20), "tau": (0.20, 0.25, 0.33), "label_delay_min": (30, 60, 120)}
DEFAULTS = {"n_min": N_MIN, "tau": TAU, "label_delay_min": LABEL_DELAY_MIN}


def measure(incidents, verdicts, matched_of, labeled, demoted: frozenset) -> dict:
    """Everything reported for one set of demoted alerts."""
    paging = [i for i in incidents if pages(i, demoted)]
    v = pd.Series([verdicts[i.id] for i in paging]).value_counts() if paging else pd.Series(dtype=int)
    true, false = int(v.get("TRUE", 0)), int(v.get("FALSE", 0))
    per = {}
    for lid, info in labeled.iterrows():
        hits = []
        for inc in paging:
            matching = [a.time for a in inc.alerts if lid in matched_of[a]]
            if matching:
                hits.append(max(paging_time(inc, demoted), min(matching)))
        per[lid] = {"pages": len(hits),
                    "ttd_min": (min(hits) - info.start).total_seconds() / 60 if hits else None}
    per = pd.DataFrame(per).T
    actionable = labeled.index[labeled.actionable]
    detected = per.pages > 0
    return {"pages": len(paging), "TRUE": true, "expected": int(v.get("expected", 0)), "FALSE": false,
            "false_pages_per_day": round(false / DAYS, 2),
            "page_precision": round(true / (true + false), 3) if true + false else None,
            "recall": f"{int(detected[actionable].sum())} of {len(actionable)}",
            "pages_per_labeled_incident": round(float(per.pages[detected].mean()), 2),
            "lost": [i for i in labeled.index if not detected[i]], "per_incident": per}


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    everything, _ = current_alerts()
    alerts = [a for a in everything if TRAIN_START <= a.time < TRAIN_END_TS]
    catalog = ds.series()
    canonical_event_of = dict(zip(catalog.series_id, catalog.canonical_event))
    incidents = correlate(alerts, build_topology(ds.relationships(), catalog))

    # the scorer's verdict on every raw alert (for judging pages) and which labeled incident it matches
    raw_class, matched, raw_res = judge_alerts(alerts)
    class_of, matched_of = dict(zip(alerts, raw_class)), dict(zip(alerts, matched))
    verdicts = dict(zip(*[judge_pages(incidents, class_of, matched_of)[c] for c in ("id", "verdict")]))

    # the labels: the scorer's own output, one per target per 30 minutes
    frame = to_frame(alerts)
    scored = evaluate_detections(frame, DATA, split="train")
    labels = {alerts[i]: c for i, c in scored["alerts_table"]["class"].items() if c in (TRUE, FALSE)}
    every_alert_labels = {a: c for a, c in class_of.items() if c in (TRUE, FALSE)}

    # labeled incidents: start time recovered from the scorer's first-alert time to detect
    labeled = raw_res["per_incident"].set_index("incident_id")[["scenario", "actionable", "ttd_min"]].copy()
    first_match = {lid: min(a.time for a in alerts if lid in matched_of[a]) for lid in labeled.index}
    labeled["start"] = [first_match[lid] - pd.Timedelta(minutes=labeled.ttd_min[lid]) for lid in labeled.index]

    def run(label_set=labels, **settings):
        result = replay(alerts, label_set, canonical_event_of, **{**DEFAULTS, **settings})
        return result, measure(incidents, verdicts, matched_of, labeled, result.demoted)

    before = measure(incidents, verdicts, matched_of, labeled, frozenset())
    result, after = run()

    n_true = sum(c == TRUE for c in labels.values())
    print("FEEDBACK LOOP on the TRAIN split, online replay "
          f"(n_min {N_MIN}, tau {TAU}, label delay {LABEL_DELAY_MIN} min: defaults, not tuned)\n")
    print(f"Raw alerts: {len(alerts)}, in {len(incidents)} pages. Labels (scorer verdicts, one per target per 30 min): "
          f"{len(labels)} ({n_true} true, {len(labels) - n_true} false). Groups: {len(result.groups)}.")
    dem_true = sum(class_of[a] == "true" for a in result.demoted)
    print(f"Alerts demoted when they fired: {len(result.demoted)} of {len(alerts)} "
          f"({dem_true} of them the scorer calls true, "
          f"{sum(class_of[a] == 'false_positive' for a in result.demoted)} false).\n")

    print("1. Before vs after feedback")
    cols = ["pages", "TRUE", "expected", "FALSE", "false_pages_per_day", "page_precision", "recall",
            "pages_per_labeled_incident"]
    print(pd.DataFrame([{"": "before (every page pages)", **{c: before[c] for c in cols}},
                        {"": "after feedback", **{c: after[c] for c in cols}}]).to_string(index=False))

    print("\n2. Per labeled incident: time to detect at the first PAGING alert, and paging pages")
    per = labeled[["scenario", "actionable"]].copy()
    per["ttd_before"] = before["per_incident"].ttd_min.astype(float).round(1)
    per["ttd_after"] = after["per_incident"].ttd_min.astype(float).round(1)
    per["pages_before"] = before["per_incident"].pages.astype(int)
    per["pages_after"] = after["per_incident"].pages.astype(int)
    per["change"] = ["NO LONGER PAGES" if pa == 0 else ("later" if ta > tb + 1e-9 else "")
                     for tb, ta, pa in zip(per.ttd_before, per.ttd_after, per.pages_after)]
    print(per.to_string(na_rep="-"))
    same_as_today = np.allclose(per.ttd_before, labeled.ttd_min.round(1), equal_nan=True)
    print(f"\n   'before' equals today's first-alert time to detect for every incident: {same_as_today}")

    def false_pages_by_week(demoted):
        false = [i for i in incidents if verdicts[i.id] == "FALSE" and pages(i, demoted)]
        week2 = pd.Timestamp("2026-10-12", tz="UTC")
        return sum(i.start < week2 for i in false), sum(i.start >= week2 for i in false)

    b1, b2 = false_pages_by_week(frozenset())
    a1, a2 = false_pages_by_week(result.demoted)
    print(f"\n   FALSE pages by week (the loop starts with no labels): week 1 (Oct 5-11) {b1} -> {a1}, "
          f"week 2 (Oct 12-18) {b2} -> {a2}")

    lost = [i for i in after["lost"]]
    print(f"\n3. Incidents that no longer page: {', '.join(f'{i} ({labeled.scenario[i]})' for i in lost) or 'none'}")
    stopped = [i for i in incidents if not pages(i, result.demoted)]
    sv = pd.Series([verdicts[i.id] for i in stopped]).value_counts() if stopped else {}
    print(f"   Pages that stopped paging: {len(stopped)} ({int(sv.get('FALSE', 0))} FALSE, {int(sv.get('TRUE', 0))} TRUE, "
          f"{int(sv.get('expected', 0))} expected)")
    for inc in stopped:
        if verdicts[inc.id] != "FALSE":
            labs = sorted(set().union(*[matched_of[a] for a in inc.alerts]))
            print(f"     {verdicts[inc.id]}: {inc.start:%m-%d %H:%M}, {len(inc.alerts)} alerts, {', '.join(inc.entities)} "
                  f"(matches {', '.join(labs)})")

    print("\n4. Every group at the end of train (estimate = (true + 1) / (labeled + 2))")
    rows = [{"detector": g.key[0], "event_or_entity": g.key[1], "alerts": g.alerts, "true": g.true, "false": g.false,
             "labeled": g.labeled, "estimate": round(g.estimate, 3),
             "demoted_at_end": "yes" if result.demoted_now(g.key) else "",
             "first_demoted": f"{g.first_demoted:%m-%d %H:%M}" if g.first_demoted is not None else "",
             "alerts_demoted": g.demoted_alerts} for g in result.groups]
    print(pd.DataFrame(rows).sort_values(["estimate", "labeled"], ascending=[True, False]).to_string(index=False))

    # ------------------------------------------------------------------ report-only sweep
    def sweep_row(mark, m, res, **settings):
        later = [i for i in labeled.index if m["per_incident"].pages[i] > 0
                 and m["per_incident"].ttd_min[i] > before["per_incident"].ttd_min[i] + 1e-9]
        return {"": mark, **settings, "pages": m["pages"], "FALSE": m["FALSE"],
                "false_per_day": m["false_pages_per_day"], "recall": m["recall"],
                "no_longer_page": ", ".join(m["lost"]) or "none", "later": ", ".join(later) or "none",
                "groups_ever_demoted": sum(g.first_demoted is not None for g in res.groups),
                "alerts_demoted": len(res.demoted)}

    print("\n5. Sweep (report only; default marked *). 'before' has "
          f"{before['pages']} pages, {before['FALSE']} FALSE, recall {before['recall']}.")
    rows = []
    for n_min, tau, delay in itertools.product(*SWEEP.values()):
        settings = {"n_min": n_min, "tau": tau, "label_delay_min": delay}
        res, m = run(**settings)
        rows.append(sweep_row("*" if settings == DEFAULTS else "", m, res, **settings))
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n6. Sensitivity (report only): a label on EVERY raw alert instead of one per target per 30 min")
    res, m = run(label_set=every_alert_labels)
    print(f"   labels: {len(every_alert_labels)}")
    print(pd.DataFrame([sweep_row("", m, res, **DEFAULTS)]).to_string(index=False))
