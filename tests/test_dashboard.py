"""The demo dashboard (dashboard/): the exported files load, nothing from the future of the clock leaks
into the system's panels, the page-outs agree with the live scorer's, and every panel renders.

The render test needs requirements-dashboard.txt (Streamlit); it is skipped without it. The rest needs
only the committed demo files.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dashboard"))
import data as D  # noqa: E402

CLOCKS = ["2026-10-26T00:00Z", "2026-10-26T00:30Z", "2026-10-27T20:20Z", "2026-10-28T13:40Z",
          "2026-10-29T00:20Z", "2026-10-31T05:00Z", "2026-11-02T00:00Z"]


@pytest.fixture(scope="module")
def dm():
    return D.load()


def test_export_loads_and_is_consistent(dm):
    assert dm.meta["page_outs"] == sum(r["kind"] == "paged" for r in dm.pages) == 13
    assert dm.series["series_id"].nunique() == dm.meta["series"]
    paged_series = {r["alert"]["target"] for r in dm.pages if r["kind"] != "paged"}
    assert paged_series == set(dm.series["series_id"])
    assert dm.series["ts"].min() >= dm.start and dm.series["ts"].max() < dm.end
    emitted = [D._ts(r["emitted_at"]) for r in dm.pages]
    assert emitted == sorted(emitted), "page_outs() relies on page records being in emission order"


def test_expected_and_wobble_match_the_engines_alert_texts(dm):
    """The chart's expected/wobble are recomputed; the export compared them with the numbers the engine
    printed in all its seasonal alerts. They must agree to the print rounding (.3g value and expected,
    .2g wobble)."""
    worst = dm.meta["expected_vs_alert_explanations_worst_relative_diff"]
    assert worst["n"] > 500
    assert worst["value"] <= 0.006 and worst["expected"] <= 0.006 and worst["wobble"] <= 0.05


@pytest.mark.parametrize("clock", CLOCKS)
def test_nothing_from_the_future(dm, clock):
    t = pd.Timestamp(clock)
    for p in D.page_outs(dm, t):
        assert p["paged_at"] <= t
        assert all(a["emitted_at"] <= t for a in p["alerts"])
        for n in D.notes_for(dm, p["page"], t):
            assert n["ready_at"] <= t
        for sid in {a["target"] for a in p["alerts"]}:
            s = D.series_until(dm, sid, t)
            assert (s["ts"] + dm.bucket <= t).all()
    for i in D.active_injections(dm, t):                 # attacker view: current only
        assert i["start"] <= t < i["stop"]


def test_page_outs_agree_with_the_live_scorer(dm):
    """At the end of the week: the same 13 page-outs as sentinel_stream.live_score, each paged with the
    same number of alerts."""
    from sentinel_stream import live_score
    ref, _ = live_score.page_outs(dm.pages)
    ours = D.page_outs(dm, dm.end)
    assert sorted((o["time"], o["page"], len(o["alerts"])) for o in ref) == \
        sorted((p["paged_at"], p["page"], p["paged_with"]) for p in ours)


def test_a_note_appears_only_once_written(dm):
    note = next(r for r in dm.triage if r["type"] == "note")
    ready = D._ts(note["as_of"]) + pd.Timedelta(seconds=note["seconds"])
    assert not [r for r in D.notes_for(dm, note["page"], ready - pd.Timedelta(seconds=1)) if r["type"] == "note"]
    assert [r for r in D.notes_for(dm, note["page"], ready) if r["type"] == "note"]


@pytest.mark.parametrize("clock", [datetime(2026, 10, 26, 0, 0), datetime(2026, 10, 26, 0, 30),
                                   datetime(2026, 10, 28, 13, 40), datetime(2026, 10, 29, 0, 20),
                                   datetime(2026, 11, 2, 0, 0)])
def test_app_renders_every_panel(clock):
    AppTest = pytest.importorskip("streamlit.testing.v1", reason="needs requirements-dashboard.txt "
                                  "(use .venv-dashboard, see README: Run the demo)").AppTest
    at = AppTest.from_file(str(ROOT / "dashboard" / "app.py"), default_timeout=60)
    at.session_state["clock"] = clock
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    heads = [h.value for h in at.subheader]
    assert {"Pages as they fire", "Active injections", "Triage note", "Series"} <= set(heads)
    assert any("Attacker view: hidden from the system" in w.value for w in at.warning)
    if clock > datetime(2026, 10, 26, 0, 15):            # the first page-out is at 00:15
        assert at.dataframe, "the pages table"
        assert at.get("arrow_vega_lite_chart"), "the series chart"
        md = " ".join(m.value for m in at.markdown)
        assert "needs_human" in md and "**Cause**" in md and "**Impact**" in md
