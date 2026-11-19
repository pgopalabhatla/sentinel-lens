"""Variant B as ONE self-contained HTML file, for publishing as a single page.

    python dashboard/b_static/make_single.py      # -> dashboard/b_static/dist/sentinel-lens-week2.html

No bundler: it concatenates the same files the served page loads (theme.css, shared/format.js,
shared/visibility.js, app.js) and inlines demo.json + series.json as a JSON script tag. ECharts and the
fonts still come from their CDNs. The served page (index.html) is unchanged and stays the source.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
DASH = HERE.parent
OUT = HERE / "dist" / "sentinel-lens-week2.html"


def module(path: Path) -> str:
    """An ES module's code with its imports dropped and `export` removed, to share one script scope."""
    src = path.read_text()
    src = re.sub(r"^import .*?;\n", "", src, flags=re.M | re.S)
    return re.sub(r"^export (?=(const|function|async function) )", "", src, flags=re.M)


def build() -> str:
    page = (HERE / "index.html").read_text()
    css = (DASH / "shared" / "theme.css").read_text()
    ui = DASH / "demo" / "live-week-2" / "ui"
    data = {"demo": json.loads((ui / "demo.json").read_text()), "series": json.loads((ui / "series.json").read_text())}
    blob = json.dumps(data, separators=(",", ":"), allow_nan=False).replace("</", "<\\/")
    code = "\n".join(module(p) for p in (DASH / "shared" / "format.js", DASH / "shared" / "visibility.js", HERE / "app.js"))
    page = page.replace('<link rel="stylesheet" href="../shared/theme.css">', f"<style>\n{css}\n</style>")
    page = page.replace('<script type="module" src="app.js"></script>',
                        f'<script type="application/json" id="demo-data">{blob}</script>\n<script type="module">\n{code}\n</script>')
    assert 'href="../' not in page and 'src="app.js"' not in page and not re.search(r"^import ", code, re.M), \
        "a reference to a local file survived"
    return page


def main() -> None:
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(build())
    print(f"{OUT.relative_to(DASH.parent)}: {OUT.stat().st_size / 2**20:.1f} MiB")


if __name__ == "__main__":
    main()
