"""Export live week 2 to the small, committed demo folder dashboard/demo/live-week-2/, so the dashboard
runs with no broker and no data_live_2/.

    python scripts/posthoc/lw2_export_topics.py    # once, needs the broker (topics -> runs/live-week-2/topics/)
    python dashboard/export_demo.py                 # needs runs/live-week-2/topics/ and data_live_2/

What is written (all from the recorded run; nothing is re-run):
    answer_key.json   the publisher's answer key, verbatim (the attacker panel only)
    pages.json        the consumer's page records (opened / alert_added / merged / paged)
    alerts.json       every alert the engine emitted, with its demoted flag
    triage_r1.json    triage run 1: notes, follow-ups and link records, trimmed to what the panel shows
    series.csv.gz     for every series that appears in a page: 5-minute means over the 7-day stream, and
                      for seasonal series the expected value and wobble, recomputed with the frozen
                      seasonal_scores and the router's per-series settings (history, abs_floor)
    meta.json         stream bounds, counts, and how the files were made

The expected/wobble columns are a recomputation, not a recording: the engine does not publish them. The
export checks them against the numbers printed in the engine's own alert explanations and reports the
worst disagreement (tests/test_dashboard.py asserts it).
"""
from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "posthoc"))
from lw2_common import DATA, load, metric_series  # noqa: E402

OUT = ROOT / "dashboard" / "demo" / "live-week-2"
START, END = pd.Timestamp("2026-10-26T00:00Z"), pd.Timestamp("2026-11-02T00:00Z")
NOTE_FIELDS = ("type", "page", "page_id", "paged_at", "as_of", "seconds", "needs_human", "confidence", "cause",
               "impact", "evidence", "next_step", "affected_entities", "guard", "follow_up", "follow_up_of",
               "tools_used", "absorbed", "at", "notes", "link_id")


def main() -> None:
    from sentinel_lens.detectors.seasonal import seasonal_scores
    from sentinel_lens.router import route_series
    from sentinel_sim.replay import DataSource

    OUT.mkdir(parents=True, exist_ok=True)
    pages, alerts, key = load("pages"), load("alerts"), load("answer_key")
    triage = [{k: r[k] for k in NOTE_FIELDS if k in r} for r in load("triage-r1")]
    for name, obj in (("answer_key", key), ("pages", pages), ("alerts", alerts), ("triage_r1", triage)):
        (OUT / f"{name}.json").write_text(json.dumps(obj, default=str))

    routes = route_series(DataSource(DATA).series()).set_index("series_id")
    series_ids = sorted({r["alert"]["target"] for r in pages if r["kind"] != "paged"})
    telemetry = load("telemetry")
    frames = []
    for sid in series_ids:
        s = metric_series(sid, telemetry)
        route = routes.loc[sid]
        if route["rule"] == "seasonal":
            sc = seasonal_scores(s, history=route["history"], abs_floor=float(route["abs_floor"])).set_index("ts")
            df = sc[["value", "expected", "wobble"]]
        else:                                           # guardrails: a rule on the value, no baseline
            df = pd.DataFrame({"value": s, "expected": float("nan"), "wobble": float("nan")})
        df = df[(df.index >= START) & (df.index < END)].copy()
        df["series_id"], df["rule"] = sid, route["rule"]
        frames.append(df.rename_axis("ts").reset_index())
    series = pd.concat(frames, ignore_index=True)
    with gzip.open(OUT / "series.csv.gz", "wt") as f:
        series.to_csv(f, index=False, float_format="%.6g")

    worst = check_against_alerts(series, alerts)
    meta = {"stream_start": str(START), "stream_end": str(END), "bucket_minutes": 5,
            "series": len(series_ids), "page_records": len(pages), "alerts": len(alerts),
            "page_outs": sum(r["kind"] == "paged" for r in pages), "triage_run": 1,
            "expected_vs_alert_explanations_worst_relative_diff": worst,
            "made_by": "dashboard/export_demo.py from runs/live-week-2/topics/ (lw2_export_topics.py) and data_live_2/"}
    (OUT / "meta.json").write_text(json.dumps(meta, indent=1))
    size = sum(p.stat().st_size for p in OUT.iterdir())
    print(json.dumps(meta, indent=1))
    print(f"{OUT.relative_to(ROOT)}: {len(list(OUT.iterdir()))} files, {size / 1024:.0f} KiB")


def check_against_alerts(series: pd.DataFrame, alerts: list) -> dict | None:
    """Worst relative difference between the exported (value, expected, wobble) and the numbers the
    engine printed in its seasonal alert explanations ("... (value vs expected +/- wobble, z=...").

    A seasonal alert is stamped at the bucket that completed the 3-of-5 persistence window, but its
    explanation describes the most extreme hot reading IN that window (sentinel_lens.alerts.
    alerts_from_seasonal), so the printed numbers are compared with the best-matching bucket of the 5
    ending at the alert's time. Numbers are printed to 3 significant digits."""
    import re
    pat = re.compile(r"\(([-\d.e+]+) vs ([-\d.e+]+) \+/- ([-\d.e+]+), z=")
    by_sid = {sid: g.set_index("ts") for sid, g in series.groupby("series_id")}
    worst = None

    def rel(a, b):
        return abs(a - b) / max(abs(b), 1e-9)

    for r in alerts:
        a = r["alert"]
        m = pat.search(a.get("explanation") or "")
        if not m or a["detector"] not in ("seasonal", "seasonal fallback") or a["target"] not in by_sid:
            continue
        printed = [float(x) for x in m.groups()]
        t = pd.Timestamp(a["time"])
        window = by_sid[a["target"]].loc[t - pd.Timedelta("20min"):t]
        if window.empty:
            continue
        # the bucket the explanation describes: the best match on all three numbers (each scaled by
        # its print rounding: value and expected .3g, wobble .2g)
        best = min(window.itertuples(), key=lambda w: max(rel(w.value, printed[0]) / 5e-3, rel(w.expected, printed[1]) / 5e-3,
                                                            rel(w.wobble, printed[2]) / 5e-2))
        d = {"value": rel(best.value, printed[0]), "expected": rel(best.expected, printed[1]),
             "wobble": rel(best.wobble, printed[2]), "n": 1}
        worst = d if worst is None else {k: (worst[k] + 1 if k == "n" else max(worst[k], d[k])) for k in d}
    return worst


if __name__ == "__main__":
    main()
