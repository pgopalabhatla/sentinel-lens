"""Variant A's chart, as a pure function of the data (no Streamlit), so it can be built and checked alone.

The x axis is Vega's "utc" scale over [clock - 12 h, clock] with a 24-hour format: labels are UTC whatever
the browser's time zone. Marker layers are named (alertmark, pagemark, changemark, injectmark); Vega puts
the name in each mark group's class, which the Playwright checks read. Rendered as SVG (usermeta).
"""
from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import data as D  # noqa: E402
from export_ui import SETTINGS  # noqa: E402

H_MS = 3600_000


def series_chart(dm: D.Demo, ui: dict, page: dict, sid: str, t: int, reveal: bool) -> alt.LayerChart:
    """The selected page's series over [t - 12 h, t] on a UTC time scale (see the module docstring)."""
    lookback = SETTINGS["lookback_hours"]
    frm = t - lookback * H_MS
    # the domain as ISO strings: Streamlit's Vega-Lite draws nothing for a numeric domain on a utc scale
    dom = [pd.Timestamp(frm, unit="ms", tz="UTC").isoformat(), pd.Timestamp(t, unit="ms", tz="UTC").isoformat()]
    info_rule = D.rule_of(dm, sid)
    seasonal = info_rule == "seasonal"
    alerts = [a for a in page["alerts"] if a["target"] == sid and a["time"] >= frm]
    down = bool(alerts) and all(a["direction"] == "down" for a in alerts)
    # times as epoch ms (float; int64 draws the same, checked in a scratch app)
    rows =pd.DataFrame(D.series_points(dm, sid, D._ts(t * 1_000_000), lookback), columns=["ts", "value", "expected", "wobble"]).astype(float)
    if seasonal and not rows.empty:
        rows = rows.assign(lo=rows.expected - rows.wobble, hi=rows.expected + rows.wobble,
                           hot=rows.expected + (-1 if down else 1) * SETTINGS["hot_z"] * rows.wobble)
    x = alt.X("ts:T", title=None, scale=alt.Scale(type="utc", domain=dom),
              axis=alt.Axis(format="%H:%M", labelFont="IBM Plex Mono", labelColor="#aba190", tickCount=12,
                            gridColor="#3b352c", gridOpacity=0.5, domainColor="#5a5244", labelOverlap=True, labelFlush=False))
    y = alt.Y("value:Q", title=None, scale=alt.Scale(zero=False),
              axis=alt.Axis(labelFont="IBM Plex Mono", labelColor="#aba190", gridColor="#3b352c", gridOpacity=0.5, domainColor="#5a5244"))
    layers = []
    if reveal:
        inj = [i for i in D.attacker_view(dm, D._ts(t * 1_000_000))["all"] if i["stop"] > frm and i["start"] < t and sid in i["series"]]
        if inj:
            df = pd.DataFrame([{"x1": float(max(i["start"], frm)), "x2": float(min(i["stop"], t)), "id": i["id"]} for i in inj])
            layers.append(alt.Chart(df, name="injectmark").mark_rect(color="#c46f55", opacity=0.16, stroke="#c46f55", strokeDash=[4, 3])
                          .encode(x=alt.X("x1:T", scale=alt.Scale(type="utc", domain=dom)), x2="x2:T"))
    base = alt.Chart(rows)
    if seasonal and not rows.empty:
        layers += [base.mark_area(color="#aba190", opacity=0.20).encode(x=x, y="lo:Q", y2="hi:Q"),
                   base.mark_line(color="#aba190", strokeDash=[5, 4], strokeWidth=1.5).encode(x=x, y="expected:Q"),
                   base.mark_line(color="#f0a030", strokeDash=[2, 3], strokeWidth=1.5).encode(x=x, y="hot:Q")]
    layers.append(base.mark_line(color="#ece4d4", strokeWidth=1.8).encode(x=x, y=y, tooltip=[
        alt.Tooltip("ts:T", format="%a %d %b %H:%M", formatType="utc", title="UTC"), "value:Q", "expected:Q", "wobble:Q"]))
    marks = [{"ts": float(a["time"]), "kind": "alert"} for a in alerts]
    if marks:
        layers.append(alt.Chart(pd.DataFrame(marks), name="alertmark").mark_rule(color="#f0a030", strokeDash=[4, 3], opacity=0.75)
                      .encode(x=alt.X("ts:T", scale=alt.Scale(type="utc", domain=dom))))
    if page["paged_at"] >= frm:
        pm = pd.DataFrame([{"ts": float(page["paged_at"]), "label": f"page {page['page']}"}])
        layers.append(alt.Chart(pm, name="pagemark").mark_rule(color="#f0a030", strokeWidth=3)
                      .encode(x=alt.X("ts:T", scale=alt.Scale(type="utc", domain=dom))))
        layers.append(alt.Chart(pm).mark_text(color="#f0a030", font="IBM Plex Mono", fontSize=11, align="right", dx=-6, dy=8, baseline="top")
                      .encode(x=alt.X("ts:T", scale=alt.Scale(type="utc", domain=dom)), y=alt.value(0), text="label:N"))
    ch = [c for c in ui["changes"] if c["ts_ms"] >= frm]
    if ch:
        cm = pd.DataFrame([{"ts": float(c["ts_ms"]), "label": c["change_id"]} for c in ch])
        layers.append(alt.Chart(cm, name="changemark").mark_rule(color="#6fa59a", strokeWidth=1.5)
                      .encode(x=alt.X("ts:T", scale=alt.Scale(type="utc", domain=dom))))
        layers.append(alt.Chart(cm).mark_text(color="#6fa59a", font="IBM Plex Mono", fontSize=11, align="left", dx=4, baseline="bottom")
                      .encode(x=alt.X("ts:T", scale=alt.Scale(type="utc", domain=dom)), y=alt.value(296), text="label:N"))
    return (alt.layer(*layers).properties(height=300, padding={"left": 12, "right": 16, "top": 8, "bottom": 4}, usermeta={"embedOptions": {"renderer": "svg", "actions": False}})
            .configure(background="transparent").configure_view(stroke=None))
