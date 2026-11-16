"""POST-HOC, read-only: what the test week looks like LIVE, from the streaming engine.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/posthoc_live_timing.py

Written after docs/results/test-week.md was seen. Nothing is re-tuned and the frozen runner is not run
again; its own functions rebuild the same alerts and pages, and this script checks that they reproduce
the pre-registered numbers before printing anything. Test-week labels are read here; that is allowed
only because the evaluation is over.

The streaming engine (sentinel_stream/) emits the same alerts and ends with the same pages as the
batch pipeline (tests/test_stream_test_week.py). What differs is WHEN things are known:

a. Time to detect. Batch stamps a seasonal alert with the start of its 5-minute bucket; the engine
   can only know it when the bucket is complete, 4 minutes later.
b. Paging. Batch counts the pages that exist at the end of the week. Live, the engineer is paged
   when a page first holds an alert that is not demoted (decided once per engine step, after all of
   that minute's alerts have been grouped), and two pages that have both paged can later be merged
   into one.
"""
import warnings
from dataclasses import replace

import numpy as np
import pandas as pd

from scripts.run_test_week import DATA, HISTORY_START, SPLITS, final_alerts, in_window, judge, page_measures
from sentinel_lens import feedback
from sentinel_lens.correlator import build_topology, correlate
from sentinel_sim.replay import DataSource, load_labels
from sentinel_stream import FROZEN_DEMOTED_GROUPS, AlertEvent, PageEvent, StreamEngine, replay

SPLIT = "test"
PRE_REGISTERED = {"recall": "9 of 10", "median_ttd_min": 10.0, "max_ttd_min": 405.0, "pages": 24,
                  "FALSE_pages": 8, "false_pages_per_day": 1.14}        # system 5, docs/results/test-week.md


def heading(text):
    print("\n" + "=" * 110 + f"\n{text}\n" + "=" * 110)


def verdict(alerts, class_of) -> str:
    """The runner's rule for a page: TRUE if any of its alerts is classed 'true', else expected if any
    is classed 'expected', else FALSE."""
    classes = {class_of[a] for a in alerts}
    return "TRUE" if "true" in classes else ("expected" if "expected" in classes else "FALSE")


def first_paged_match(events, matched_of) -> dict:
    """{labeled incident: the first stream time at which a page that has paged holds an alert matching
    it}. Follows the page events in order, so it uses the pages as they were at each moment."""
    pages, first = {}, {}
    for e in events:
        if not isinstance(e, PageEvent):
            continue
        page = pages.setdefault(e.page, {"paged": False, "incidents": set()})
        if e.kind == "paged":
            page["paged"] = True
        else:
            for k in e.absorbed:
                other = pages.pop(k)
                page["paged"] |= other["paged"]
                page["incidents"] |= other["incidents"]
            page["incidents"] |= matched_of[e.alert]
        if page["paged"]:
            for lid in page["incidents"]:
                first.setdefault(lid, e.emitted_at)
    return first


def page_outs(events) -> list:
    """One record per 'paged' event: the page then, and what became of it."""
    content, outs = {}, {}
    for e in events:
        if not isinstance(e, PageEvent):
            continue
        if e.kind == "paged":
            outs[e.page] = {"key": e.page, "paged_at": e.emitted_at, "first_alert_time": content[e.page][0].time,
                            "id_then": e.page_id, "alerts_then": list(content[e.page]), "trigger": e.alert,
                            "absorbed_at": None, "absorbed_into": None, "absorbed_by": None}
            continue
        alerts = content.setdefault(e.page, [])
        for k in e.absorbed:
            if k in outs:
                outs[k].update(absorbed_at=e.emitted_at, absorbed_into=e.page, absorbed_by=e.alert,
                               alerts_end=list(content[k]))
            alerts += content.pop(k)
        alerts.append(e.alert)
    for key, out in outs.items():
        out.setdefault("alerts_end", list(content[key]) if key in content else [])
    return sorted(outs.values(), key=lambda o: (o["paged_at"], o["key"]))


def main():
    warnings.simplefilter("ignore", RuntimeWarning)
    pd.set_option("display.width", 250)
    lo, hi = SPLITS[SPLIT]
    days = (pd.Timestamp(hi) - pd.Timestamp(lo)).days
    ds = DataSource(DATA).at(hi)
    catalog = ds.series()
    canonical_event_of = dict(zip(catalog.series_id, catalog.canonical_event))
    topology = build_topology(ds.relationships(), catalog)

    # ---- batch side, with the frozen runner's own functions
    alerts = in_window(final_alerts(ds, hi), SPLIT)
    class_of, matched_of, labeled, _, _ = judge(alerts, SPLIT)
    incidents = correlate(alerts, topology)
    demoted = feedback.apply_frozen(alerts, FROZEN_DEMOTED_GROUPS, canonical_event_of)
    p5 = page_measures(incidents, class_of, matched_of, labeled, demoted, days)
    same_as_pre_registered = all(p5[k] == v for k, v in PRE_REGISTERED.items())

    # ---- streaming side
    engine = StreamEngine.warm_start(ds, lo, history_start=HISTORY_START)
    events = list(replay(engine, ds, lo, hi, history_start=HISTORY_START))
    emitted_at = {e.alert: e.emitted_at for e in events if isinstance(e, AlertEvent)}
    same_alerts = set(emitted_at) == set(alerts) and engine.pages() == incidents and engine.demoted == demoted

    print("POST-HOC, not the pre-registered result. Written after docs/results/test-week.md was seen; read-only.")
    print(f"\nBatch side reproduces the pre-registered system 5 numbers {PRE_REGISTERED}: {same_as_pre_registered}")
    print(f"Streaming engine: same {len(alerts)} alerts, same {len(incidents)} pages, same {len(demoted)} demoted alerts as batch: {same_alerts}")
    if not (same_as_pre_registered and same_alerts):
        raise SystemExit("STOP: the inputs of this analysis are not the pre-registered ones")

    labels = {i["incident_id"]: i for i in load_labels(DATA)[0] if i["split"] == SPLIT}
    actionable = list(labeled.index[labeled.actionable])

    # =========================================================================================== a
    heading("a. Time to detect, system 5: when the engine could know (emission time) vs. the batch time")
    lag = pd.Series({a: (emitted_at[a] - a.time).total_seconds() / 60 for a in alerts})
    seasonal = np.array([a.detector.startswith("seasonal") for a in alerts])
    print(f"Emission time - alert time: {int(seasonal.sum())} seasonal alerts, all {sorted(set(lag[seasonal]))} min; "
          f"{int((~seasonal).sum())} other alerts, all {sorted(set(lag[~seasonal]))} min.")
    print("Batch: an incident is detected by a paging page that holds a matching alert, timed at the later of the page's\n"
          "paging time and that alert's time (alert times as batch stamps them). Live: the first stream time at which a page\n"
          "that has paged holds a matching alert, following the pages as they actually were at each moment. Same alerts,\n"
          "same matching to incidents as the pre-registered run.\n")
    live_first = first_paged_match(events, matched_of)
    rows = []
    for lid in labeled.index:
        start, end = pd.Timestamp(labels[lid]["start"]), pd.Timestamp(labels[lid]["end"])
        batch_ttd = p5["per_incident"].ttd_min[lid]
        t = live_first.get(lid)
        live_ttd = (t - start).total_seconds() / 60 if t is not None else np.nan
        rows.append({"incident_id": lid, "scenario": labeled.scenario[lid], "actionable": labeled.actionable[lid],
                     "batch_ttd_min": float(batch_ttd), "live_ttd_min": live_ttd, "later_by_min": live_ttd - float(batch_ttd),
                     "live_first_page_at": "" if t is None else f"{t:%m-%d %H:%M}",
                     "incident_end": f"{end:%m-%d %H:%M}",
                     "paged_before_incident_end": "" if t is None else bool(t <= end)})
    table = pd.DataFrame(rows).set_index("incident_id")
    ttd_cols = ["batch_ttd_min", "live_ttd_min", "later_by_min"]
    print(table.assign(**{c: table[c].round(1) for c in ttd_cols}).to_string(na_rep=""))     # rounded as the runner does
    act = table.loc[actionable]
    summary = pd.DataFrame([
        {"system 5": "batch time (pre-registered)", "detected": f"{int(act.batch_ttd_min.notna().sum())} of {len(act)}",
         "median_ttd_min": round(act.batch_ttd_min.median(), 1), "max_ttd_min": round(act.batch_ttd_min.max(), 1)},
        {"system 5": "live (emission time)", "detected": f"{int(act.live_ttd_min.notna().sum())} of {len(act)}",
         "median_ttd_min": round(act.live_ttd_min.median(), 1), "max_ttd_min": round(act.live_ttd_min.max(), 1)}])
    print("\nOver the actionable incidents:")
    print(summary.to_string(index=False))

    # stricter status check: let the unmodified scorer match on EMISSION times, i.e. an alert counts
    # for an incident only if it is KNOWN between the incident's start - 5 min and its end
    shifted = {a: replace(a, time=emitted_at[a]) for a in alerts}
    _, matched_live, labeled_live, _, _ = judge(list(shifted.values()), SPLIT)
    matched_by_emission = {a: matched_live[shifted[a]] for a in alerts}
    strict_first = first_paged_match(events, matched_by_emission)
    status = pd.DataFrame({"scenario": labeled.scenario, "actionable": labeled.actionable,
                           "batch_detected_(5)": [not pd.isna(p5["per_incident"].ttd_min[i]) for i in labeled.index],
                           "live_detected_(5)": [i in live_first for i in labeled.index],
                           "live_detected_(5)_matching_on_emission_time": [i in strict_first for i in labeled.index],
                           "alerts_(3)_detected_batch": labeled.detected,
                           "alerts_(3)_detected_matching_on_emission_time": labeled_live.detected})
    changed = status[(status.iloc[:, 2] != status.iloc[:, 3]) | (status.iloc[:, 2] != status.iloc[:, 4])
                     | (status.iloc[:, 5] != status.iloc[:, 6])]
    print("\nDetected / missed, three ways (the last two columns are the alert level, before pages and feedback):")
    print(status.to_string())
    print(f"\nIncidents whose detected/missed status changes: {', '.join(changed.index) or 'none'}")
    n_moved = sum(matched_by_emission[a] != matched_of[a] for a in alerts)
    print(f"Alerts whose set of matched incidents changes when matched on emission time: {n_moved} of {len(alerts)}")

    # =========================================================================================== b
    heading("b. Live paging: every page-out, classed with the runner's rule")
    outs = page_outs(events)
    final_by_key = {k: p for k, p in engine.pager._pages.items()}
    print("A page-out is a 'paged' event: at the end of an engine step (all alerts of that minute grouped) a page holds an\n"
          "alert that is not demoted and has not paged before. Verdict = the runner's rule\n"
          "(TRUE if the page holds an alert the scorer classes 'true', else expected, else FALSE), applied to the page's\n"
          "own alerts at the end of its life: at the end of the week, or at the moment it was absorbed into another page.\n"
          "'verdict_at_page_out' applies the same rule to the alerts the page held when it paged.\n")
    rows = []
    for n, o in enumerate(outs, 1):
        covers = sorted(set().union(*[matched_of[a] for a in o["alerts_end"]])) if o["alerts_end"] else []
        fate = ("kept to the end of the week" if o["absorbed_at"] is None
                else f"absorbed {o['absorbed_at']:%m-%d %H:%M} into page-out {[x['key'] for x in outs].index(o['absorbed_into']) + 1}"
                if o["absorbed_into"] in [x["key"] for x in outs] else f"absorbed {o['absorbed_at']:%m-%d %H:%M}")
        rows.append({"n": n, "paged_at": f"{o['paged_at']:%m-%d %H:%M}", "page_first_alert": f"{o['first_alert_time']:%m-%d %H:%M}",
                     "entity_of_first_alert": o["id_then"].split("|", 1)[1],
                     "alerts_at_page_out": len(o["alerts_then"]), "alerts_at_end": len(o["alerts_end"]),
                     "verdict_at_page_out": verdict(o["alerts_then"], class_of), "verdict": verdict(o["alerts_end"], class_of),
                     "labeled_incidents": ", ".join(covers), "fate": fate})
    out_table = pd.DataFrame(rows)
    print(out_table.to_string(index=False))

    kept = out_table[out_table.fate.str.startswith("kept")]
    absorbed = out_table[~out_table.fate.str.startswith("kept")]
    counts = out_table.verdict.value_counts()
    batch_paging = [p for p in incidents if feedback.pages(p, demoted)]
    batch_counts = pd.Series([verdict(p.alerts, class_of) for p in batch_paging]).value_counts()
    # each kept page-out is one of the batch paging pages, with the same verdict
    kept_keys = [o["key"] for o in outs if o["absorbed_at"] is None]
    kept_is_batch = sorted(final_by_key[k].id for k in kept_keys) == sorted(p.id for p in batch_paging)
    summary = pd.DataFrame([
        {"": "batch, system 5 (pre-registered)", "pages": len(batch_paging), "TRUE": int(batch_counts.get("TRUE", 0)),
         "expected": int(batch_counts.get("expected", 0)), "FALSE": int(batch_counts.get("FALSE", 0)),
         "false_pages_per_day": round(int(batch_counts.get("FALSE", 0)) / days, 2)},
        {"": "live page-outs", "pages": len(out_table), "TRUE": int(counts.get("TRUE", 0)),
         "expected": int(counts.get("expected", 0)), "FALSE": int(counts.get("FALSE", 0)),
         "false_pages_per_day": round(int(counts.get("FALSE", 0)) / days, 2)},
        {"": "  of which kept to the end of the week", "pages": len(kept), "TRUE": int((kept.verdict == "TRUE").sum()),
         "expected": int((kept.verdict == "expected").sum()), "FALSE": int((kept.verdict == "FALSE").sum()),
         "false_pages_per_day": round(int((kept.verdict == "FALSE").sum()) / days, 2)},
        {"": "  of which absorbed by a merge after paging", "pages": len(absorbed), "TRUE": int((absorbed.verdict == "TRUE").sum()),
         "expected": int((absorbed.verdict == "expected").sum()), "FALSE": int((absorbed.verdict == "FALSE").sum()),
         "false_pages_per_day": round(int((absorbed.verdict == "FALSE").sum()) / days, 2)}])
    print("\nSummary:")
    print(summary.to_string(index=False))
    print(f"The page-outs kept to the end are exactly the {len(batch_paging)} batch paging pages: {kept_is_batch}")
    at_out = out_table.verdict_at_page_out.value_counts()
    print(f"By the alerts held at the moment of paging instead: TRUE {int(at_out.get('TRUE', 0))}, "
          f"expected {int(at_out.get('expected', 0))}, FALSE {int(at_out.get('FALSE', 0))}.")

    print("\nThe page-outs that were later absorbed by a merge:")
    rows = []
    for n, o in enumerate(outs, 1):
        if o["absorbed_at"] is None:
            continue
        by = o["absorbed_by"]
        rows.append({"n": n, "paged_at": f"{o['paged_at']:%m-%d %H:%M}", "entity_of_first_alert": o["id_then"].split("|", 1)[1],
                     "own_alerts": len(o["alerts_end"]), "verdict": verdict(o["alerts_end"], class_of),
                     "labeled_incidents": ", ".join(sorted(set().union(*[matched_of[a] for a in o["alerts_end"]]))),
                     "absorbed_at": f"{o['absorbed_at']:%m-%d %H:%M}",
                     "minutes_on_its_own": round((o["absorbed_at"] - o["paged_at"]).total_seconds() / 60, 1),
                     "into_page": final_page_id(final_by_key, outs, o), "linking_alert": f"{by.target} [{by.detector}]"})
    print(pd.DataFrame(rows).to_string(index=False))

    same_step = [r for r in rows if r["minutes_on_its_own"] == 0]
    print(f"\nPage-outs absorbed in the same minute they paged: {len(same_step)} (paging is decided once per step, so a page that "
          "opens and\nmerges inside one step never pages on its own).")

    print("\nPOST-HOC. Nothing here replaces docs/results/test-week.md, and nothing was changed or re-tuned.")


def final_page_id(final_by_key, outs, o) -> str:
    """The id of the end-of-week page an absorbed page-out ended up in (following later merges)."""
    key = o["absorbed_into"]
    by_key = {x["key"]: x for x in outs}
    while key not in final_by_key and key in by_key and by_key[key]["absorbed_into"] is not None:
        key = by_key[key]["absorbed_into"]
    return final_by_key[key].id if key in final_by_key else "?"


if __name__ == "__main__":
    main()
