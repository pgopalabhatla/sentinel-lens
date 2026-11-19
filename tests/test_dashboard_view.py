"""dashboard/shared/view.js: the selected page's chart window, series ranking, status line and cause label.
Pure functions of one page from uiState(demo, t) and the clock, run here through Node (skipped without it).
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
VIEW = ROOT / "dashboard" / "shared" / "view.js"
H, M = 3600_000, 60_000


def run(expr: str):
    """Evaluate a JS expression with view.js imported as V; return its JSON value."""
    node = shutil.which("node")
    if not node:
        pytest.skip("needs Node")
    js = f"import * as V from {json.dumps(VIEW.as_uri())};\nprocess.stdout.write(JSON.stringify({expr}));"
    out = subprocess.run([node, "--input-type=module", "-e", js], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def page(paged_at, alerts, target="a|x"):
    return {"page": 1, "paged_at": paged_at, "paging_target": target, "alerts": alerts}


def alert(target, emitted_at, score=1.0, measure=8.0, line=6.0):
    return {"target": target, "time": emitted_at - 4 * M, "emitted_at": emitted_at, "score": score, "measure": measure, "line": line}


P = 100 * H                                   # a paging time


def test_window_is_anchored_on_the_page_and_capped_at_the_clock():
    p = page(P, [alert("a|x", P)])
    assert run(f"V.chartWindow({json.dumps(p)}, {P + 30 * M})") == {"from": P - 3 * H, "to": P + 30 * M, "nowInside": True}
    assert run(f"V.chartWindow({json.dumps(p)}, {P + 2 * H})") == {"from": P - 3 * H, "to": P + 2 * H, "nowInside": True}
    # a day later the window still shows the page, and the clock is past it
    assert run(f"V.chartWindow({json.dumps(p)}, {P + 24 * H})") == {"from": P - 3 * H, "to": P + 2 * H, "nowInside": False}
    assert run(f"V.chartWindow({json.dumps(p)}, {P})") == {"from": P - 3 * H, "to": P, "nowInside": True}


def test_series_ranked_by_score_then_count_then_excess_then_paging_series():
    p = page(P, [alert("b|x", P, score=0.9), alert("b|x", P + 5 * M, score=0.9), alert("b|x", P + 10 * M, score=0.9),
                 alert("c|x", P, score=1.0, measure=7), alert("d|x", P, score=1.0, measure=30),
                 alert("e|x", P, score=1.0, measure=30), alert("a|x", P, score=1.0, measure=30)], target="a|x")
    ranked = run(f"V.rankSeries({json.dumps(p)}, {P + 10 * M})")
    assert [r["sid"] for r in ranked] == ["a|x", "d|x", "e|x", "c|x", "b|x"]       # ties: paging series first, then name
    assert [r["count"] for r in ranked] == [1, 1, 1, 1, 3]
    # firing: an alert emitted in the last 10 minutes of the clock
    assert {r["sid"]: r["firing"] for r in ranked} == {"a|x": True, "d|x": True, "e|x": True, "c|x": True, "b|x": True}
    late = run(f"V.rankSeries({json.dumps(p)}, {P + 20 * M + 1})")
    assert {r["sid"]: r["firing"] for r in late} == {"a|x": False, "d|x": False, "e|x": False, "c|x": False, "b|x": False}


def test_an_infinite_z_ranks_above_a_finite_one():
    p = page(P, [alert("a|x", P, measure=20), alert("b|x", P, measure=None)])
    assert [r["sid"] for r in run(f"V.rankSeries({json.dumps(p)}, {P})")] == ["b|x", "a|x"]


def test_status_line_from_the_newest_alert():
    p = page(P, [alert("a|x", P), alert("a|x", P + 25 * M)])
    assert run(f"V.pageStatus({json.dumps(p)}, {P + 35 * M})") == {"firing": True, "lastAlertMs": P + 25 * M}
    assert run(f"V.pageStatus({json.dumps(p)}, {P + 35 * M + 1})") == {"firing": False, "lastAlertMs": P + 25 * M}


def test_cause_label():
    assert run('V.causeLabel("unknown")') == {"label": "Likely cause (unconfirmed)", "tag": None}
    assert run("V.causeLabel(null)") == {"label": "Likely cause (unconfirmed)", "tag": None}
    assert run('V.causeLabel("change")') == {"label": "Cause", "tag": "change"}
