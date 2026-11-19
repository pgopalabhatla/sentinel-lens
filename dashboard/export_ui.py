"""One UI-ready export for every dashboard variant, made from the committed demo folder only.

    python dashboard/export_ui.py        # dashboard/demo/live-week-2/ -> dashboard/demo/live-week-2/ui/

Writes two files:
    demo.json    the records the panels are built from (page records, triage run 1, the change log, the
                 answer key), each still carrying its own time, plus things that never change with the
                 clock (the paging alert's one-line reason, series labels, display settings). Nothing in
                 it is pre-filtered for a clock: the "nothing from the future" rule is applied at view
                 time, by dashboard/data.py (Python) or its port dashboard/shared/visibility.js (JS).
    series.json  every exported series on its regular 5-minute grid: {t0_ms, step_ms, value, expected,
                 wobble}, null for a missing number. Same numbers as series.csv.gz.

Deterministic: the same input gives the same bytes.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import data as D  # noqa: E402

OUT = D.DEMO / "ui"
HOT_Z = 6.0                     # the frozen seasonal threshold (docs/config-frozen.md)
SETTINGS = {"hot_z": HOT_Z, "lookback_hours": 12, "tick_ms": 400,
            "speeds_min": [5, 15, 60, 360], "default_speed_min": 15}


def _num(x):
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else float(x)


def build(dm: D.Demo) -> tuple[dict, dict]:
    paging = D._paging_alerts(dm)
    series = {}
    index = {}
    for sid, g in dm.series.groupby("series_id", sort=True):
        g = g.sort_values("ts")
        ts = [D.ms(x) for x in g["ts"]]
        step = D.ms(dm.start + dm.bucket) - D.ms(dm.start)
        assert all(b - a == step for a, b in zip(ts, ts[1:])), f"{sid}: not on a regular grid"
        series[sid] = {"t0_ms": ts[0], "step_ms": step, "value": [_num(v) for v in g["value"]],
                       "expected": [_num(v) for v in g["expected"]], "wobble": [_num(v) for v in g["wobble"]]}
        index[sid] = {"rule": g["rule"].iloc[0], "label": D.label(sid), "entity": sid.split("|")[-1]}
    demo = {
        "meta": {**dm.meta, "start_ms": D.ms(dm.start), "end_ms": D.ms(dm.end),
                 "history_start_ms": D.ms(dm.meta.get("history_start", dm.start)),
                 "bucket_ms": D.ms(dm.start + dm.bucket) - D.ms(dm.start)},
        "settings": SETTINGS,
        "page_records": dm.pages,
        "triage": dm.triage,
        "changes": dm.changes,
        "answer_key": dm.answer_key,
        "paging_reasons": {str(k): D.reason(a) for k, a in sorted(paging.items())},
        "paging_targets": {str(k): a["target"] for k, a in sorted(paging.items())},
        "series_index": index,
    }
    return demo, series


def main() -> None:
    demo, series = build(D.load())
    OUT.mkdir(parents=True, exist_ok=True)
    for name, obj in (("demo", demo), ("series", series)):
        (OUT / f"{name}.json").write_text(json.dumps(obj, separators=(",", ":"), sort_keys=True, allow_nan=False))
    for f in sorted(OUT.iterdir()):
        print(f"{f.relative_to(D.DEMO.parents[2])}: {f.stat().st_size / 1024:.0f} KiB")


if __name__ == "__main__":
    main()
