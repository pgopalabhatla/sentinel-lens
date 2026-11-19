"""The dashboard page (dashboard/index.html + app.js) and its one-file build (dashboard/dist/).

Default run: the one-file build inlines everything local and the committed file is current. `pytest -m ui`
runs the Playwright checks (dashboard/e2e/check.mjs) in a real browser; it needs Node and `npm install`
in dashboard/e2e, and takes about 2 minutes.
"""
from __future__ import annotations

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
import make_single  # noqa: E402


def test_single_file_inlines_everything_local():
    page = make_single.build()
    assert 'href="shared/' not in page and 'src="app.js"' not in page
    data = re.search(r'<script type="application/json" id="demo-data">(.*?)</script>', page, re.S).group(1)
    blob = json.loads(data.replace("<\\/", "</"))
    assert set(blob) == {"demo", "series"} and blob["demo"]["meta"]["page_outs"] == 13
    assert "export function uiState" not in page and "function uiState" in page     # the module, inlined
    assert len(page) < 16 * 2**20


def test_committed_single_file_is_current():
    assert make_single.OUT.read_text() == make_single.build(), "run python dashboard/make_single.py"


@pytest.mark.ui
def test_playwright_checks_pass():
    if not shutil.which("node") or not (DASH / "e2e" / "node_modules").exists():
        pytest.skip("needs Node and `npm install` in dashboard/e2e")
    subprocess.run(["node", "check.mjs"], cwd=DASH / "e2e", check=True, timeout=900)
    res = json.loads((ROOT / "docs" / "dashboard" / "results.json").read_text())
    assert all(res["summary"].values()), res["summary"]
