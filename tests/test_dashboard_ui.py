"""The UI export (dashboard/export_ui.py) and the one "nothing from the future" rule shared by every
dashboard variant: dashboard/data.py's ui_state, and its JavaScript port dashboard/shared/visibility.js,
which must give exactly the same output at the same clocks. The JS test needs Node; skipped without it.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dashboard"))
import data as D  # noqa: E402
import export_ui  # noqa: E402

UI = D.DEMO / "ui"
CLOCKS = ["2026-10-26T00:00Z", "2026-10-26T00:30Z", "2026-10-27T20:20Z", "2026-10-28T13:40Z",
          "2026-10-29T00:20Z", "2026-10-31T05:00Z", "2026-11-02T00:00Z"]       # as tests/test_dashboard.py


@pytest.fixture(scope="module")
def dm():
    return D.load()


def test_committed_ui_export_is_current(dm):
    """demo.json and series.json are what export_ui.py makes from the committed demo folder."""
    demo, series = export_ui.build(dm)
    dump = lambda o: json.dumps(o, separators=(",", ":"), sort_keys=True, allow_nan=False)  # noqa: E731
    assert (UI / "demo.json").read_text() == dump(demo), "run python dashboard/export_ui.py"
    assert (UI / "series.json").read_text() == dump(series), "run python dashboard/export_ui.py"


def test_every_alert_has_a_plain_reason(dm):
    for r in dm.alerts:
        why = D.reason(r["alert"])
        assert not why.endswith((" up", " down")), (why, r["alert"]["explanation"])
    assert D.reason(next(r["alert"] for r in dm.pages if r["kind"] == "paged" and r["page"] == 3)) == \
        "trino-worker-0 CPU 6.3x expected"


@pytest.mark.parametrize("clock", CLOCKS)
def test_ui_state_has_nothing_from_the_future(dm, clock):
    t = D.ms(clock)
    st = D.ui_state(dm, clock)
    assert st["page_count"] == len(st["pages"])
    for p in st["pages"]:
        assert p["paged_at"] <= t and all(a["emitted_at"] <= t for a in p["alerts"])
        assert p["verdict_at"] is None or p["verdict_at"] <= t
        assert all(n["ready_at"] <= t for n in st["notes"][str(p["page"])])
        for sid in p["series"]:
            assert all(row[0] + D.ms(dm.start + dm.bucket) - D.ms(dm.start) <= t for row in D.series_points(dm, sid, clock))
    assert all(c["streamed_at_ms"] <= t for c in st["changes"])
    assert [p["paged_at"] for p in st["pages"]] == sorted((p["paged_at"] for p in st["pages"]), reverse=True)


def test_a_change_is_visible_only_once_streamed(dm):
    c = next(c for c in dm.changes if c["change_id"] == "chg-0007")
    streamed = pd.Timestamp(c["streamed_at"])
    assert pd.Timestamp(c["ts"]) < streamed
    ids = lambda t: {x["change_id"] for x in D.ui_state(dm, t)["changes"]}  # noqa: E731
    assert "chg-0007" not in ids(streamed - pd.Timedelta(seconds=1)) and "chg-0007" in ids(streamed)


def test_page_14_and_17_notes_are_their_own(dm):
    """The data behind the chg-0007 regression: only page 14's notes name chg-0007."""
    st = D.ui_state(dm, dm.end)
    assert any("chg-0007" in json.dumps(n) for n in st["notes"]["14"])
    assert not any("chg-0007" in json.dumps(n) for n in st["notes"]["17"])


def _python_view(dm, clock):
    ui = D.ui_state(dm, clock)
    pts = {sid: D.series_points(dm, sid, clock) for p in ui["pages"] for sid in p["series"]}
    return {"ui": ui, "series": pts, "attacker": D.attacker_view(dm, clock)}


def test_js_port_equals_python_at_the_same_clocks(dm):
    node = shutil.which("node")
    if not node:
        pytest.skip("needs Node (the JavaScript dashboards' visibility.js port)")
    out = subprocess.run([node, str(ROOT / "dashboard" / "shared" / "dump_state.mjs"), str(UI), *CLOCKS],
                         capture_output=True, text=True, check=True).stdout
    js = json.loads(out)
    for clock in CLOCKS:
        py = json.loads(json.dumps(_python_view(dm, clock)))       # same JSON round trip as the JS side
        assert js[clock] == py, clock
    assert js[CLOCKS[-1]]["ui"]["page_count"] == 13 and js[CLOCKS[0]]["ui"]["page_count"] == 0
