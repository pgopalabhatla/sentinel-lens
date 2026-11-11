"""The pre-registered evaluation runner. Read docs/test-protocol.md before using it.

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/run_test_week.py                          # DRY RUN on the TRAIN split (default)
    python scripts/run_test_week.py --write-config           # write docs/config-frozen.md (before the freeze)
    python scripts/run_test_week.py --split test --run-once  # THE test run: once, after the freeze

The dry run exercises exactly the code path of the test run on the train split, with the clock at the
end of train, and checks that it reproduces the train numbers already in docs/log.md. It reads no
test-week data.

The test run is guarded: it needs --run-once, the tag `pre-test-freeze` with code identical to it,
filled-in predictions in docs/test-protocol.md, and no existing docs/results/test-week.md. Its output
is saved verbatim to that file. Any later run must pass --post-hoc "<reason>" and is saved to a
separate, clearly marked file; it never replaces the first result.

Five systems (docs/test-protocol.md):
    (1) naive full baseline          the quickstart's detector
    (2) seasonal everywhere          the first build: previous_days history, 5% floor, gauges only
    (3) final alerts                 routed system (router defaults) + job run baseline
    (4) final alerts + pages         (3) grouped by the correlator
    (5) final + pages + frozen feedback   PRIMARY. Groups demoted by all train labels stay demoted
                                          for the whole evaluated period; nothing is learned in it.
"""
import argparse
import hashlib
import inspect
import subprocess
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from sentinel_lens import alerts as alerts_mod
from sentinel_lens import feedback, router
from sentinel_lens.alerts import (alerts_from_above_zero, alerts_from_counter, alerts_from_job_runs,
                                  alerts_from_schedule, alerts_from_seasonal, to_frame)
from sentinel_lens.correlator import build_topology, correlate
from sentinel_lens.detectors import guardrails, schedule
from sentinel_lens.detectors.guardrails import above_zero_alerts, counter_increase_alerts
from sentinel_lens.detectors.job_runs import job_run_alerts
from sentinel_lens.detectors.persistence import persistent_alerts
from sentinel_lens.detectors.schedule import failed_run_alerts, schedule_alerts
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_lens.stats import wilson_interval
from sentinel_lens.topology_extra import EXTRA_EDGES
from sentinel_sim.evaluate import evaluate_detections
from sentinel_sim.replay import DataSource

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
PROTOCOL, CONFIG = ROOT / "docs/test-protocol.md", ROOT / "docs/config-frozen.md"
RESULTS_DIR = ROOT / "docs/results"
FREEZE_TAG = "pre-test-freeze"
FROZEN_PATHS = ["sentinel_lens", "sentinel_sim", "scripts/run_test_week.py", "docs/config-frozen.md"]
PREDICTION_MARKER = "NOT FILLED IN"

HISTORY_START = "2026-09-28"                       # detectors see everything from here up to the clock
SPLITS = {"train": ("2026-10-05", "2026-10-19"), "test": ("2026-10-19", "2026-10-26")}
NOT_GAUGES = ("kube_pod", "flink_job_restarts", "flink_checkpoint_failures")     # the quickstart's own filter
# Train numbers already reported in docs/log.md; the dry run must reproduce them.
TRAIN_CHECK = {"(1)": (1.0, 0.117, 1488, 1301), "(2)": (0.7, 0.150, 935, 783), "(3)": (1.0, 0.604, 275, 103),
               "pages": (46, 27), "online": (44, 25)}


# ----------------------------------------------------------------------------------- detections
def naive_detections(ds, end) -> pd.DataFrame:
    """System (1): examples/quickstart.py, unchanged in substance: robust z vs. the same time on the
    previous 7 days (no floor), |z| > 6 in 3 of 5; any counter increase; failed or SLA-missed runs."""
    cat = ds.series(layer=[1, 2, 3])
    cols = [s for s in cat.series_id if not s.startswith(NOT_GAUGES)]
    m = ds.query(cols, start=HISTORY_START, end=end, step="5min")
    lagged = np.stack([m.shift(288 * k).values for k in range(1, 8)])
    med = np.nanmedian(lagged, axis=0)
    mad = np.nanmedian(np.abs(lagged - med), axis=0) * 1.4826 + 1e-9
    z = pd.DataFrame((m.values - med) / mad, index=m.index, columns=m.columns)
    hot = (z.abs() > 6).astype(int).rolling(5).sum() >= 3
    dets = [{"ts": ts, "target": c} for c in hot.columns for ts in hot.index[hot[c].values]]
    for sid in ds.series(metric=list(guardrails.FLAT_COUNTER_METRICS)).series_id:
        s = ds.query(sid, start=HISTORY_START, end=end)[sid]
        dets += [{"ts": ts, "target": sid} for ts in s.index[s.diff() > 0]]
    jr = ds.job_runs(start=HISTORY_START, end=end)
    dets += [{"ts": r.end, "target": f"dag:{r.dag_id}"} for r in jr.itertuples() if r.state == "failed" or r.sla_missed]
    return pd.DataFrame(dets, columns=["ts", "target"])


def first_build_detections(ds, end) -> pd.DataFrame:
    """System (2): the first build. Seasonal baseline on every gauge with its first settings
    (previous 7 days, 5% floor, no absolute floor, threshold 6) and 3-of-5. No other rule."""
    cat = ds.series(layer=[1, 2, 3])
    cols = [s for s in cat.series_id if not s.startswith(NOT_GAUGES)]
    m = ds.query(cols, start=HISTORY_START, end=end, step="5min")
    dets = []
    for sid in cols:
        out = seasonal_scores(m[sid], threshold=6.0, floor=0.05, days=7, min_days=1, history="previous_days",
                              abs_floor=0.0).set_index("ts")
        dets += [{"ts": ts, "target": sid} for ts in persistent_alerts(out.hot, k=3, n=5)]
    return pd.DataFrame(dets, columns=["ts", "target"])


def final_alerts(ds, end) -> list:
    """Systems (3)-(5): every raw alert of the final system since HISTORY_START, as Alert records.
    Router defaults, seasonal + 3-of-5 at 5-minute steps, guardrails at 1-minute steps, schedule
    watcher, job run baseline. The saturation forecast is off (router default)."""
    routes = router.route_series(ds.series(layer=[1, 2, 3]))
    out = []
    seasonal = routes[routes.rule == "seasonal"]
    m5 = ds.query(list(seasonal.series_id), start=HISTORY_START, end=end, step="5min")
    for sid, fallback, history, abs_floor in zip(seasonal.series_id, seasonal.fallback, seasonal.history,
                                                 seasonal.abs_floor):
        scores = seasonal_scores(m5[sid], history=history, abs_floor=abs_floor)
        out += alerts_from_seasonal(scores, target=sid, detector="seasonal fallback" if fallback else "seasonal")
    for rule, detect, adapt in [("counter_rule", counter_increase_alerts, alerts_from_counter),
                                ("above_zero", above_zero_alerts, alerts_from_above_zero)]:
        sids = list(routes[routes.rule == rule].series_id)
        m1 = ds.query(sids, start=HISTORY_START, end=end)
        for sid in sids:
            out += adapt(detect(m1[sid]), target=sid)
    runs = ds.job_runs(start=HISTORY_START, end=end)
    expected = ds.expected_runs(start=HISTORY_START, end=end)
    out += alerts_from_schedule(pd.concat([schedule_alerts(expected, runs, now=ds.now), failed_run_alerts(runs)]))
    out += alerts_from_job_runs(job_run_alerts(runs))
    return out


# -------------------------------------------------------------------------------------- scoring
def score(dets: pd.DataFrame, split: str, **kw) -> dict:
    return evaluate_detections(dets.reset_index(drop=True), DATA, split=split, **kw)


def in_window(alerts: list, split: str) -> list:
    lo, hi = (pd.Timestamp(t, tz="UTC") for t in SPLITS[split])
    return [a for a in alerts if lo <= a.time < hi]


def judge(alerts: list, split: str):
    """From the unmodified scorer: (class of every raw alert, labeled incidents each alert matches,
    labeled-incident table with its start time, labels as the scorer gives them after its dedup)."""
    frame = to_frame(alerts)
    raw = score(frame, split, dedup="0min")
    assert raw["alerts"] == len(alerts), "the scorer dropped raw alerts"
    klass = raw["alerts_table"]["class"].reindex(range(len(alerts))).tolist()
    matched = []
    for i, c in enumerate(klass):
        if c == "false_positive":
            matched.append(frozenset())
        else:
            pi = score(frame.iloc[[i]], split, dedup="0min")["per_incident"]
            matched.append(frozenset(pi.incident_id[pi.detected]))
    labeled = raw["per_incident"].set_index("incident_id")[["scenario", "actionable", "detected", "ttd_min"]].copy()
    starts = {}
    for lid in labeled.index:
        times = [a.time for a, m in zip(alerts, matched) if lid in m]
        starts[lid] = min(times) - pd.Timedelta(minutes=labeled.ttd_min[lid]) if times else pd.NaT
    labeled["start"] = pd.Series(starts)
    deduped = score(frame, split)
    labels = {alerts[i]: c for i, c in deduped["alerts_table"]["class"].items()
              if c in (feedback.TRUE, feedback.FALSE)}
    return dict(zip(alerts, klass)), dict(zip(alerts, matched)), labeled, labels, deduped


def alert_row(name: str, res: dict, days: int) -> dict:
    pi = res["per_incident"]
    act = pi[pi.actionable]
    k, n = int(act.detected.sum()), len(act)
    lo, hi = wilson_interval(k, n)
    return {"system": name, "alerts_per_day": round(res["alerts"] / days, 1),
            "alert_precision": res["precision"], "recall": f"{k} of {n}", "recall_95%_CI": f"{lo:.2f}-{hi:.2f}",
            "median_ttd_min": None if act.ttd_min.dropna().empty else round(act.ttd_min.median(), 1),
            "max_ttd_min": None if act.ttd_min.dropna().empty else round(act.ttd_min.max(), 1)}


def page_measures(incidents, class_of, matched_of, labeled, demoted: frozenset, days: int) -> dict:
    """Page-level numbers for one set of demoted alerts (empty set = system 4)."""
    paging = [i for i in incidents if feedback.pages(i, demoted)]
    verdict = {}
    for inc in paging:
        classes = {class_of[a] for a in inc.alerts}
        verdict[inc.id] = "TRUE" if "true" in classes else ("expected" if "expected" in classes else "FALSE")
    covers = {inc.id: set().union(*[matched_of[a] for a in inc.alerts]) for inc in paging}
    per = {}
    for lid, info in labeled.iterrows():
        hits = []
        for inc in paging:
            matching = [a.time for a in inc.alerts if lid in matched_of[a]]
            if matching:
                hits.append(max(feedback.paging_time(inc, demoted), min(matching)))
        per[lid] = {"pages": len(hits), "ttd_min": (min(hits) - info.start).total_seconds() / 60 if hits else np.nan}
    per = pd.DataFrame(per).T
    act = labeled.index[labeled.actionable]
    k, n = int((per.pages[act] > 0).sum()), len(act)
    lo, hi = wilson_interval(k, n)
    counts = pd.Series(list(verdict.values())).value_counts() if verdict else pd.Series(dtype=int)
    ttd = per.ttd_min[act].astype(float).dropna()
    hit = per.pages > 0
    return {"recall": f"{k} of {n}", "recall_95%_CI": f"{lo:.2f}-{hi:.2f}",
            "median_ttd_min": None if ttd.empty else round(ttd.median(), 1),
            "max_ttd_min": None if ttd.empty else round(ttd.max(), 1),
            "pages": len(paging), "TRUE_pages": int(counts.get("TRUE", 0)), "FALSE_pages": int(counts.get("FALSE", 0)),
            "false_pages_per_day": round(int(counts.get("FALSE", 0)) / days, 2),
            "pages_per_labeled_incident": round(float(per.pages[hit].mean()), 2) if hit.any() else None,
            "merged_incidents": sum(len(c) > 1 for c in covers.values()), "per_incident": per,
            "lost": [i for i in act if per.pages[i] == 0]}


# --------------------------------------------------------------------------------------- config
def defaults_of(fn) -> dict:
    return {k: v.default for k, v in inspect.signature(fn).parameters.items() if v.default is not inspect.Parameter.empty}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def config_markdown(train_groups, frozen: frozenset) -> str:
    """Every parameter of the evaluated systems, read from the code itself so it cannot drift."""
    cat = DataSource(DATA).at(SPLITS["train"][1]).series(layer=[1, 2, 3])
    routes = router.route_series(cat).merge(cat[["series_id", "metric", "unit", "default_detector"]], on="series_id")
    per_metric = routes.groupby("metric").agg(series=("series_id", "size"), unit=("unit", "first"),
                                              catalog_detector=("default_detector", "first"), rule=("rule", "first"),
                                              fallback=("fallback", "first"), history=("history", "first"),
                                              min_change=("min_change", "first"), abs_floor=("abs_floor", "first"))
    L = ["# Frozen configuration", "",
         "Generated by `python scripts/run_test_week.py --write-config` from the code itself. The test run",
         "regenerates this text and reports whether it still matches. Do not edit by hand.", "",
         "## Data and windows", "",
         "- Dataset: `python -m sentinel_sim.generate --out data --seed 7 --days 28` (data/ is not in git)",
         f"- History available to every detector: from {HISTORY_START} up to the clock; nothing after the clock is read",
         f"- Train window: {SPLITS['train'][0]} to {SPLITS['train'][1]} (end exclusive), clock at {SPLITS['train'][1]}",
         f"- Test window: {SPLITS['test'][0]} to {SPLITS['test'][1]} (end exclusive), clock at {SPLITS['test'][1]}",
         "- Data files (sha256, first 16 hex): "
         + ", ".join(f"{p} {sha(DATA / p)}" for p in ["metrics/metrics.csv.gz", "jobs/dag_runs.jsonl", "catalog/series.csv",
                                                      "catalog/topology.json", "labels/incidents.json"]), "",
         "## Scorer (sentinel_sim/evaluate.py, unmodified)", "",
         f"- `evaluate_detections` defaults: {defaults_of(evaluate_detections)}",
         "- A page is TRUE if it contains an alert the scorer classes 'true' (run with dedup='0min' so every raw alert is classed)", "",
         "## System (1) naive full baseline", "",
         "- Gauges (every layer 1-3 series except restart/failure counters), 5-minute means; robust z against the same",
         "  time on the previous 7 days (median, 1.4826 x MAD + 1e-9, no floor); |z| > 6 in 3 of the last 5 readings",
         "- Any increase of " + ", ".join(guardrails.FLAT_COUNTER_METRICS) + " at 1-minute readings",
         "- DAG runs that failed or missed their SLA, at the run's end", "",
         "## System (2) seasonal everywhere (first build)", "",
         "- Same gauges; `seasonal_scores(threshold=6.0, floor=0.05, days=7, min_days=1, history='previous_days', abs_floor=0.0)`;",
         "  3-of-5 persistence; no counter, schedule or job-run rules", "",
         "## Systems (3)-(5) final system", "",
         f"- `seasonal_scores` defaults: {defaults_of(seasonal_scores)}; history and abs_floor come from the router; step 5 min",
         f"- Persistence: {defaults_of(persistent_alerts)}",
         f"- Router: history {router.DEFAULT_HISTORY!r}, count_min_change {router.DEFAULT_COUNT_MIN_CHANGE}, "
         f"threshold {router.SEASONAL_THRESHOLD}, METRIC_MIN_CHANGE {router.METRIC_MIN_CHANGE}, saturation off",
         f"- Seasonal fallback for catalog detectors: {router.SEASONAL_FALLBACK}",
         f"- Counter rule metrics: {guardrails.FLAT_COUNTER_METRICS}; above-zero metrics: {guardrails.ZERO_GAUGE_METRICS}; 1-minute readings, no persistence",
         f"- Schedule watcher: grace {schedule.DEFAULT_GRACE_MIN} min for every DAG; SLA minutes from the topology; failed runs alert at their end",
         f"- Job run baseline: {defaults_of(job_run_alerts)}",
         "- Saturation forecast: built but OFF (not routed)", "",
         "Routing per metric (router defaults):", "", "```", per_metric.to_string(), "```", "",
         "## Alert score", "",
         f"- Default formula: severity. Guardrail detectors {alerts_mod.GUARDRAIL_DETECTORS} score 1.0; all others",
         "  0.5 + 0.5 * (1 - exp(-(r - 1))), r = measure / alert line, kept in [0.5, 1). The score does not decide what fires or pages.", "",
         "## Correlator", "",
         f"- `correlate` defaults: {defaults_of(correlate)}; applied to the raw alerts inside the evaluated window",
         "- Topology: the relationships table plus these tracked extra edges (sentinel_lens/topology_extra.py):"]
    L += [f"  - {e[0]}:{e[1]} --{e[2]}--> {e[3]}:{e[4]}" for e in EXTRA_EDGES]
    L += ["", "## Feedback", "",
          f"- n_min {feedback.N_MIN}, tau {feedback.TAU}; estimate = (true + 1) / (labeled + 2)",
          "- Labels: the scorer's verdicts on train alerts after its 30-minute dedup ('true' / 'false_positive'; 'expected' is not a label)",
          "- FROZEN state for system (5): learned from ALL train labels (clock at the end of train), then applied unchanged",
          "  to the whole evaluated period from its first alert. Nothing is learned from the evaluated period.",
          f"- Demoted groups carried into the test week: {sorted(frozen)}",
          f"- Exploratory online variant only: label delay {feedback.LABEL_DELAY_MIN} min, labels keep arriving during the test week", "",
          "State of every group at the end of train:", "", "```"]
    rows = [{"detector": g.key[0], "event_or_entity": g.key[1], "true": g.true, "false": g.false, "labeled": g.labeled,
             "estimate": round(g.estimate, 3), "demoted": "yes" if g.key in frozen else ""} for g in train_groups]
    L += [pd.DataFrame(rows).sort_values(["estimate", "detector", "event_or_entity"]).to_string(index=False), "```", ""]
    return "\n".join(L)


# ------------------------------------------------------------------------------------------ git
def git(*args) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def freeze_status() -> dict:
    tag = git("rev-parse", "--verify", "--quiet", f"{FREEZE_TAG}^{{commit}}")
    same = bool(tag) and subprocess.run(["git", "diff", "--quiet", FREEZE_TAG, "--", *FROZEN_PATHS], cwd=ROOT).returncode == 0
    return {"commit": git("rev-parse", "HEAD"), "tag_commit": tag or None, "frozen_code_identical_to_tag": same,
            "working_tree_clean": git("status", "--porcelain", "--", *FROZEN_PATHS) == ""}


# ----------------------------------------------------------------------------------------- main
def evaluate_split(split: str) -> tuple:
    """Run all five systems on one split. Returns (report text, things to check)."""
    out = []
    say = out.append
    lo, hi = SPLITS[split]
    days = (pd.Timestamp(hi) - pd.Timestamp(lo)).days
    ds = DataSource(DATA).at(hi)
    catalog = ds.series()
    canonical_event_of = dict(zip(catalog.series_id, catalog.canonical_event))
    topology = build_topology(ds.relationships(), catalog)

    # frozen feedback: always learned on train, with the clock at the end of train
    train_ds = DataSource(DATA).at(SPLITS["train"][1])
    train_alerts = in_window(final_alerts(train_ds, SPLITS["train"][1]), "train")
    _, _, _, train_labels, _ = judge(train_alerts, "train")
    learned = feedback.replay(train_alerts, train_labels, canonical_event_of)
    frozen = feedback.demoted_groups(learned.groups)

    res1 = score(naive_detections(ds, hi), split)
    res2 = score(first_build_detections(ds, hi), split)
    alerts = train_alerts if split == "train" else in_window(final_alerts(ds, hi), split)
    class_of, matched_of, labeled, labels, res3 = judge(alerts, split)
    incidents = correlate(alerts, topology)
    demoted = feedback.apply_frozen(alerts, frozen, canonical_event_of)
    res5_alerts = score(to_frame([a for a in alerts if a not in demoted]), split)
    p4 = page_measures(incidents, class_of, matched_of, labeled, frozenset(), days)
    p5 = page_measures(incidents, class_of, matched_of, labeled, demoted, days)

    page_cols = ["pages", "false_pages_per_day", "pages_per_labeled_incident", "merged_incidents"]
    rows = [alert_row("(1) naive full baseline", res1, days), alert_row("(2) seasonal everywhere (first build)", res2, days),
            alert_row("(3) final alerts", res3, days),
            {**alert_row("(4) final alerts + correlator pages", res3, days), **{c: p4[c] for c in page_cols}},
            {**alert_row("(5) final + correlator + frozen feedback  [PRIMARY]", res5_alerts, days),
             **{c: p5[c] for c in ["recall", "recall_95%_CI", "median_ttd_min", "max_ttd_min"] + page_cols}}]
    n_act = int(labeled.actionable.sum())
    say(f"Split: {split} ({lo} to {hi}, {days} days). Labeled incidents: {len(labeled)}, of which actionable: {n_act}.")
    say(f"Raw alerts of the final system in the window: {len(alerts)}; demoted by the frozen feedback: {len(demoted)}.")
    say(f"Frozen demoted groups (learned from all train labels): {sorted(frozen)}\n")
    say("Summary. Recall = actionable incidents detected, with a Wilson 95% interval. For (1)-(4) an incident is detected by")
    say("its first matching alert; for (5) by a PAGING page that matches it, timed at the moment that page pages.")
    say("Per-alert precision and alerts per day are after the scorer's 30-minute dedup; for (5), of alerts not demoted.\n")
    say(pd.DataFrame(rows, dtype=object).to_string(index=False, na_rep="-"))

    say("\nPages (systems 4 and 5):")
    say(pd.DataFrame([{"system": "(4)", **{k: p4[k] for k in ["pages", "TRUE_pages", "FALSE_pages", "false_pages_per_day",
                                                              "pages_per_labeled_incident", "merged_incidents"]}},
                      {"system": "(5) PRIMARY", **{k: p5[k] for k in ["pages", "TRUE_pages", "FALSE_pages", "false_pages_per_day",
                                                                      "pages_per_labeled_incident", "merged_incidents"]}}])
        .to_string(index=False, na_rep="-"))
    say(f"Actionable incidents that do not page in (5): {', '.join(p5['lost']) or 'none'}")

    say("\nPer labeled incident: time to detect in minutes (blank = missed) and number of pages")
    per = labeled[["scenario", "actionable"]].copy()
    for name, res in [("ttd_(1)", res1), ("ttd_(2)", res2), ("ttd_(3)=(4)", res3)]:
        per[name] = res["per_incident"].set_index("incident_id").ttd_min.round(1)
    per["ttd_(5)"] = p5["per_incident"].ttd_min.astype(float).round(1)
    per["pages_(4)"] = p4["per_incident"].pages.astype(int)
    per["pages_(5)"] = p5["per_incident"].pages.astype(int)
    say(per.to_string(na_rep=""))

    # exploratory: online feedback that keeps learning, labels arriving label_delay minutes after each alert
    if split == "train":
        online = feedback.replay(alerts, labels, canonical_event_of)
        online_demoted = online.demoted
    else:
        both = feedback.replay(train_alerts + alerts, {**train_labels, **labels}, canonical_event_of)
        online_demoted = frozenset(a for a in both.demoted if a in set(alerts))
    px = page_measures(incidents, class_of, matched_of, labeled, online_demoted, days)
    say("\nEXPLORATORY, not the primary result: online feedback that keeps learning during the period")
    say(pd.DataFrame([{"system": "(5x) online-continuing feedback", **{k: px[k] for k in
        ["recall", "recall_95%_CI", "median_ttd_min", "max_ttd_min", "pages", "FALSE_pages", "false_pages_per_day",
         "pages_per_labeled_incident", "merged_incidents"]}}]).to_string(index=False, na_rep="-"))
    say(f"Actionable incidents that do not page in (5x): {', '.join(px['lost']) or 'none'}")

    check = {"(1)": res1, "(2)": res2, "(3)": res3, "p4": p4, "px": px, "learned": learned, "frozen": frozen,
             "alerts": alerts}
    return "\n".join(out), check


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--split", choices=["train", "test"], default="train")
    ap.add_argument("--run-once", action="store_true", help="required for --split test")
    ap.add_argument("--post-hoc", metavar="REASON", help="a later test run; saved separately and marked post-hoc")
    ap.add_argument("--write-config", action="store_true", help="write docs/config-frozen.md from the code (train only)")
    args = ap.parse_args()
    warnings.simplefilter("ignore", RuntimeWarning)

    if args.split == "test":
        first = RESULTS_DIR / "test-week.md"
        status = freeze_status()
        problems = []
        if not args.run_once:
            problems.append("pass --run-once: the test week is evaluated once")
        if PREDICTION_MARKER in PROTOCOL.read_text():
            problems.append(f"fill in the predictions in {PROTOCOL.relative_to(ROOT)} and commit them first")
        if first.exists() and not args.post_hoc:
            problems.append(f"{first.relative_to(ROOT)} already exists; a further run needs --post-hoc \"<reason>\"")
        if not first.exists() and args.post_hoc:
            problems.append("--post-hoc is only for runs after the first result exists")
        if not first.exists() and not (status["tag_commit"] and status["frozen_code_identical_to_tag"]
                                       and status["working_tree_clean"]):
            problems.append(f"the first run needs tag {FREEZE_TAG} with identical, committed code and config: {status}")
        if problems:
            sys.exit("Refusing to run the test week:\n  - " + "\n  - ".join(problems))

    body, check = evaluate_split(args.split)
    config_now = config_markdown(check["learned"].groups, check["frozen"])

    if args.write_config:
        if args.split != "train":
            sys.exit("--write-config only works with the train split")
        CONFIG.write_text(config_now)
        print(f"wrote {CONFIG.relative_to(ROOT)}")
        return

    config_matches = CONFIG.exists() and CONFIG.read_text() == config_now
    if args.split == "train":
        print("DRY RUN on the TRAIN split: same code path as the test run, no test-week data is read.\n")
        print(body)
        got = {k: (float(check[k]["recall"]), float(check[k]["precision"]), int(check[k]["alerts"]),
                   int(check[k]["false_positives"])) for k in ("(1)", "(2)", "(3)")}
        got["pages"] = (check["p4"]["pages"], check["p4"]["FALSE_pages"])
        got["online"] = (check["px"]["pages"], check["px"]["FALSE_pages"])
        from scripts.score_formulas import current_alerts            # the pipeline every earlier result used
        same_alerts = set(check["alerts"]) == set(in_window(current_alerts()[0], "train"))
        print("\nChecks against the train numbers in docs/log.md:")
        for k, want in TRAIN_CHECK.items():
            print(f"  {k:7s} expected {want}  got {got[k]}  {'ok' if got[k] == want else 'MISMATCH'}")
        print(f"  alerts identical to the pipeline used for every earlier result: {same_alerts}")
        print(f"  docs/config-frozen.md matches the code: {config_matches}")
        print(f"  freeze status: {freeze_status()}")
        ok = all(got[k] == want for k, want in TRAIN_CHECK.items()) and same_alerts
        sys.exit(0 if ok else "DRY RUN FAILED: the runner does not reproduce the train results")

    # ---- the test run: save verbatim
    status = freeze_status()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    first = RESULTS_DIR / "test-week.md"
    if args.post_hoc:
        n = 1
        while (RESULTS_DIR / f"test-week-posthoc-{n}.md").exists():
            n += 1
        path = RESULTS_DIR / f"test-week-posthoc-{n}.md"
        title = f"# POST-HOC test-week run {n} (NOT the pre-registered result)\n\nReason given: {args.post_hoc}\n"
    else:
        path, title = first, "# Test-week result (the pre-registered run)\n"
    header = [title, f"- Run at (UTC): {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S}",
              f"- Commit: {status['commit']}", f"- Tag {FREEZE_TAG}: {status['tag_commit']}",
              f"- Frozen code and config identical to the tag: {status['frozen_code_identical_to_tag']}",
              f"- Working tree clean for the frozen paths: {status['working_tree_clean']}",
              f"- docs/config-frozen.md matches the code: {config_matches}",
              "- Protocol: docs/test-protocol.md. Output below is verbatim from `scripts/run_test_week.py`.", "", "```"]
    text = "\n".join(header) + "\n" + body + "\n```\n"
    path.write_text(text)
    print(text)
    print(f"saved to {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
