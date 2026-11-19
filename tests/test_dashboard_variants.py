"""The three dashboard variants of dashboard-v2 (docs/dashboard-compare/README.md).

Default run (.venv): variant B's single-page build. With requirements-dashboard.txt (.venv-dashboard):
variant A's chart spec and the app itself through Streamlit's AppTest. `pytest -m ui` runs the
Playwright comparison (dashboard/e2e/compare.mjs) on all three variants in a real browser; it needs
Node, `npm install` in dashboard/e2e and dashboard/c_react, and .venv-dashboard, and takes ~10 min.
"""
from __future__ import annotations

import html
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DASH = ROOT / "dashboard"
sys.path.insert(0, str(DASH))
sys.path.insert(0, str(DASH / "a_streamlit"))
sys.path.insert(0, str(DASH / "b_static"))
import data as D  # noqa: E402

END = "2026-11-02T00:00Z"


@pytest.fixture(scope="module")
def dm():
    return D.load()


# ------------------------------------------------------------------ B: one page, nothing local left
def test_b_single_page_inlines_everything_local():
    import make_single
    page = make_single.build()
    assert 'href="../' not in page and 'src="app.js"' not in page
    data = re.search(r'<script type="application/json" id="demo-data">(.*?)</script>', page, re.S).group(1)
    blob = json.loads(data.replace("<\\/", "</"))
    assert set(blob) == {"demo", "series"} and blob["demo"]["meta"]["page_outs"] == 13
    assert "export function uiState" not in page and "function uiState" in page     # the module, inlined
    assert len(page) < 16 * 2**20                                                     # a single-page limit


# ------------------------------------------------------------------ A: the chart spec
def test_a_chart_is_utc_24h_over_the_window_with_named_markers(dm):
    pytest.importorskip("altair", reason="needs requirements-dashboard.txt (.venv-dashboard)")
    from chart import series_chart
    t = D.ms("2026-10-30T00:30Z")
    ui = D.ui_state(dm, "2026-10-30T00:30Z")
    page = ui["pages"][0]
    spec = series_chart(dm, ui, page, page["paging_target"], t, reveal=False).to_dict()
    x = spec["layer"][0]["encoding"]["x"]
    assert x["scale"]["type"] == "utc" and x["axis"]["format"] == "%H:%M" and x["axis"]["labelFlush"] is False
    # ISO strings, not numbers: Streamlit's Vega-Lite draws nothing for a numeric domain on a utc scale
    assert x["scale"]["domain"] == ["2026-10-29T12:30:00+00:00", "2026-10-30T00:30:00+00:00"]
    names = {layer.get("name") for layer in spec["layer"]}
    assert {"alertmark", "pagemark", "changemark"} <= names                       # chg-0007 is in the window
    assert spec["usermeta"]["embedOptions"]["renderer"] == "svg"


# ------------------------------------------------------------------ A: the app, without a browser
def _app(**query):
    AppTest = pytest.importorskip("streamlit.testing.v1", reason="needs requirements-dashboard.txt "
                                  "(.venv-dashboard)").AppTest
    at = AppTest.from_file(str(DASH / "a_streamlit" / "app.py"), default_timeout=60)
    for k, v in query.items():
        at.query_params[k] = v
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    return at


def _detail(at) -> str:
    bodies = [h.proto.body for h in at.get("html") if 'data-testid="page-detail"' in h.proto.body]
    assert len(bodies) == 1, f"{len(bodies)} page details"
    return bodies[0]


@pytest.mark.parametrize("clock", ["2026-10-26T00:00Z", "2026-10-26T04:30Z", "2026-10-30T00:30Z", END])
def test_a_renders_what_ui_state_says(dm, clock):
    at = _app(t=clock)
    st = D.ui_state(dm, clock)
    md = " ".join(m.value for m in at.markdown)
    assert f'data-testid="page-count">{st["page_count"]}<' in md
    assert [int(x) for x in re.findall(r'data-testid="page-card" data-page="(\d+)"', md)] == [p["page"] for p in st["pages"]]
    if st["pages"]:
        assert f'data-page="{st["pages"][0]["page"]}"' in _detail(at)
        assert at.get("arrow_vega_lite_chart"), "the series chart"


def test_a_page_14_then_17_shows_only_page_17(dm):
    at = _app(t=END)
    at.button(key="open-14").click().run()
    assert "chg-0007" in _detail(at) and 'data-page="14"' in _detail(at)
    at.button(key="open-17").click().run()
    detail = _detail(at)
    assert 'data-page="17"' in detail and "chg-0007" not in detail
    notes = D.ui_state(dm, END)["notes"]["17"]
    assert all(html.escape(n["cause"]["description"]) in detail for n in notes if n["type"] != "link")


# ------------------------------------------------------------------ all three, in a browser
@pytest.mark.ui
def test_playwright_comparison_passes_for_every_variant():
    if not shutil.which("node") or not (DASH / "e2e" / "node_modules").exists():
        pytest.skip("needs Node and `npm install` in dashboard/e2e")
    subprocess.run(["node", "compare.mjs", "a", "b", "c"], cwd=DASH / "e2e", check=True, timeout=1800)
    for v in "abc":
        res = json.loads((ROOT / "docs" / "dashboard-compare" / v / "results.json").read_text())
        assert all(res["summary"].values()), (v, res["summary"])
