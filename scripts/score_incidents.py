"""Score the correlator on the TRAIN split: how many pages, and are they the right ones?

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/score_incidents.py

Input: every raw alert of the current system in the train window (routed system + job run baseline,
through the adapters), grouped by sentinel_lens.correlator.correlate.

How a predicted incident (a "page") is judged, using the existing scorer unchanged:
    The scorer classifies single alerts (true / expected / false_positive). It is run once on the raw
    alerts with dedup="0min", an existing argument, so every raw alert gets its class by exactly the
    scorer's own matching rule. Which labeled incident an alert matches is also asked of the scorer
    (one call per non-false alert). A page is then
        TRUE      if it contains at least one alert classed 'true'
        expected  if it has none but contains an 'expected' one (a non-actionable incident)
        FALSE     otherwise
    Precision = TRUE / (TRUE + FALSE), expected left out, as the scorer does for alerts.

Reported: pages vs. alerts; page precision; recall and first-alert time to detect per labeled
incident; fragmentation (pages per labeled incident); merging (pages covering several labeled
incidents). The ablation and the sweep are report-only: defaults are gap 15 min, 1 hop, not tuned.
Data is read only through DataSource with the clock at the end of train; train labels only.
"""
import warnings

import numpy as np
import pandas as pd

from scripts.score_formulas import current_alerts
from scripts.score_seasonal import DATA, ds
from sentinel_lens.alerts import to_frame
from sentinel_lens.correlator import build_topology, correlate
from sentinel_sim.evaluate import evaluate_detections

TRAIN_START, TRAIN_END_TS = pd.Timestamp("2026-10-05", tz="UTC"), pd.Timestamp("2026-10-19", tz="UTC")
TODAY = {"recall": 1.0, "precision": 0.604, "alerts": 275, "false_positives": 103}
DEFAULT = {"gap_min": 15, "max_hops": 1}
SWEEP_GAPS, SWEEP_HOPS = (10, 15, 30, 60), (0, 1, 2)


def judge_alerts(alerts: list):
    """(class of every raw alert, labeled incidents each alert matches), both from the scorer."""
    frame = to_frame(alerts)
    res = evaluate_detections(frame, DATA, split="train", dedup="0min")
    assert res["alerts"] == len(alerts), "the scorer dropped raw alerts"
    klass = res["alerts_table"]["class"].reindex(range(len(alerts))).tolist()
    matched = []
    for i, c in enumerate(klass):
        if c == "false_positive":
            matched.append(frozenset())
            continue
        pi = evaluate_detections(frame.iloc[[i]], DATA, split="train", dedup="0min")["per_incident"]
        matched.append(frozenset(pi.incident_id[pi.detected]))
    return klass, matched, res


def judge_pages(incidents, klass_of, matched_of) -> pd.DataFrame:
    rows = []
    for inc in incidents:
        classes = [klass_of[a] for a in inc.alerts]
        labeled = sorted(set().union(*[matched_of[a] for a in inc.alerts]))
        verdict = "TRUE" if "true" in classes else ("expected" if "expected" in classes else "FALSE")
        rows.append({"id": inc.id, "start": inc.start, "end": inc.end, "verdict": verdict, "alerts": len(inc.alerts),
                     "true_alerts": classes.count("true"), "n_targets": inc.n_targets,
                     "n_entities": len(inc.entities), "n_detectors": inc.n_detectors,
                     "max_severity": inc.max_severity, "labeled": labeled, "title": inc.title})
    return pd.DataFrame(rows)


def summarize(name, pages: pd.DataFrame, labeled_info: pd.DataFrame) -> dict:
    true, false = int((pages.verdict == "TRUE").sum()), int((pages.verdict == "FALSE").sum())
    covering = {lid: int(pages.labeled.apply(lambda ls: lid in ls).sum()) for lid in labeled_info.index}
    actionable = labeled_info.index[labeled_info.actionable]
    frag = [covering[i] for i in labeled_info.index if covering[i] > 0]
    return {"setting": name, "pages": len(pages), "TRUE": true, "expected": int((pages.verdict == "expected").sum()),
            "FALSE": false, "precision": round(true / (true + false), 3) if true + false else None,
            "recall": round(float(np.mean([covering[i] > 0 for i in actionable])), 3),
            "pages_per_labeled_mean": round(float(np.mean(frag)), 1), "pages_per_labeled_max": max(frag),
            "pages_covering_2+": int((pages.labeled.apply(len) > 1).sum()),
            "largest_page_alerts": int(pages.alerts.max()), "single_alert_pages": int((pages.alerts == 1).sum())}


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    everything, _ = current_alerts()
    alerts = [a for a in everything if TRAIN_START <= a.time < TRAIN_END_TS]
    topology = build_topology(ds.relationships(), ds.series())

    today = evaluate_detections(to_frame(everything), DATA, split="train")
    got = {k: (int(today[k]) if k in ("alerts", "false_positives") else float(today[k])) for k in TODAY}
    assert got == TODAY, f"the alerts changed: {got}"
    klass, matched, raw_res = judge_alerts(alerts)
    klass_of, matched_of = dict(zip(alerts, klass)), dict(zip(alerts, matched))
    labeled_info = today["per_incident"].set_index("incident_id")

    print("CORRELATOR on the TRAIN split\n")
    print(f"Raw alerts through the adapters since Sep 28: {len(everything)}; in the train window: {len(alerts)} "
          f"(the other {len(everything) - len(alerts)} are in the warm-up week and are not scored).")
    counts = pd.Series(klass).value_counts()
    print(f"Raw train alerts by scorer class: {counts.get('true', 0)} true, {counts.get('expected', 0)} expected, "
          f"{counts.get('false_positive', 0)} false.")
    print(f"Today (one alert per target per 30 min): {got}\n")

    # ------------------------------------------------------------------ defaults
    incidents = correlate(alerts, topology, **DEFAULT)
    pages = judge_pages(incidents, klass_of, matched_of)
    row = summarize("gap 15 min, 1 hop (default)", pages, labeled_info)
    print(f"1. Pages with the defaults (gap {DEFAULT['gap_min']} min, {DEFAULT['max_hops']} hop)")
    print(f"   {len(alerts)} raw alerts -> {got['alerts']} alerts after the 30-minute dedup -> {len(pages)} pages "
          f"({len(pages) / 14:.1f} a day over 14 days)")
    print(f"   pages: {row['TRUE']} TRUE, {row['expected']} expected, {row['FALSE']} FALSE; "
          f"page precision {row['precision']} (alert precision today {got['precision']})")
    sizes = pages.groupby("verdict").agg(pages=("alerts", "size"), alerts=("alerts", "sum"),
                                         median_alerts=("alerts", "median"), max_alerts=("alerts", "max"),
                                         single_alert=("alerts", lambda a: int((a == 1).sum())),
                                         median_targets=("n_targets", "median"),
                                         median_detectors=("n_detectors", "median"))
    print(sizes.to_string())

    print("\n2. Per labeled incident (defaults). ttd_today = first alert, from today's scorer run;\n"
          "   ttd_first_alert = first matching raw alert in any page (must be equal); pages = predicted\n"
          "   incidents containing an alert that matches it (fragmentation)")
    raw_pi = raw_res["per_incident"].set_index("incident_id")
    per = labeled_info[["scenario", "actionable"]].copy()
    per["detected"] = [bool(pages.labeled.apply(lambda ls, i=i: i in ls).any()) for i in per.index]
    per["ttd_today"] = labeled_info.ttd_min.round(1)
    per["ttd_first_alert"] = raw_pi.ttd_min.round(1)
    per["pages"] = [int(pages.labeled.apply(lambda ls, i=i: i in ls).sum()) for i in per.index]
    per["alerts_in_those_pages"] = [int(pages.alerts[pages.labeled.apply(lambda ls, i=i: i in ls)].sum())
                                    for i in per.index]
    print(per.to_string())
    same_ttd = np.allclose(per.ttd_today.astype(float), per.ttd_first_alert.astype(float), equal_nan=True)
    actionable = per[per.actionable]
    print(f"\n   Recall on actionable incidents: {int(actionable.detected.sum())} of {len(actionable)}; "
          f"first-alert time to detect equal to today's for every incident: {same_ttd}")

    merged = pages[pages.labeled.apply(len) > 1]
    print(f"\n3. Merging: pages that cover more than one labeled incident (defaults): {len(merged)}")
    for r in merged.itertuples():
        print(f"   {r.id}: {', '.join(r.labeled)}; {r.alerts} alerts, {r.start:%m-%d %H:%M} to {r.end:%m-%d %H:%M}")

    false = pages[pages.verdict == "FALSE"]
    print(f"\n4. FALSE pages (defaults): {len(false)}; by entity of the first alert (top 8)")
    print(false.id.str.split("|").str[1].value_counts().head(8).to_string())

    # ------------------------------------------------------------------ ablation and sweep
    def run(name, **kw):
        return summarize(name, judge_pages(correlate(alerts, topology, **kw), klass_of, matched_of), labeled_info)

    print("\n5. Ablation (report only): what each half of the rule does")
    print(pd.DataFrame([run("time only (gap 15 min, any entity)", gap_min=15, max_hops=None),
                        run("topology only (1 hop, any time)", gap_min=None, max_hops=1),
                        run("both (gap 15 min, 1 hop) = default", **DEFAULT)]).to_string(index=False))

    print("\n6. Sweep (report only; default marked *)")
    rows = []
    for gap in SWEEP_GAPS:
        for hops in SWEEP_HOPS:
            r = run(f"gap {gap} min, {hops} hop{'s' if hops != 1 else ''}", gap_min=gap, max_hops=hops)
            rows.append({"": "*" if (gap, hops) == (DEFAULT["gap_min"], DEFAULT["max_hops"]) else "", **r})
    print(pd.DataFrame(rows).to_string(index=False))
