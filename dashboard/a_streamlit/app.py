"""Variant A: the live week 2 replay in Streamlit, restyled to the shared design spec.

    cd dashboard/a_streamlit && ../../.venv-dashboard/bin/streamlit run app.py     # http://127.0.0.1:8601

Same data as the other variants (dashboard/demo/live-week-2/), same rule: everything the system's panels
show comes from dashboard/data.py's ui_state(t) and series_points(t). Differences from dashboard/app.py
(v1), which stays as it was:
- Stale content between pages: the selected page is drawn into a placeholder (st.empty) holding a
  container keyed by the page number, as ONE HTML block. Switching pages replaces that block; no element
  of the previous page can survive into the next one's tree (v1 showed page 14's chg-0007 under page 17).
- UTC axis: the chart's x scale is Vega's "utc" type with a 24-hour format, over [clock - 12 h, clock].
  v1 used a plain temporal axis, which Vega draws in the browser's local time zone.
- Look: dashboard/shared/theme.css, injected, plus .streamlit/config.toml for Streamlit's own widgets.
"""
from __future__ import annotations

import html
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
import data as D  # noqa: E402
from chart import series_chart  # noqa: E402
from export_ui import SETTINGS  # noqa: E402

H_MS = 3600_000
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

st.set_page_config(page_title="Sentinel Lens replay", layout="wide", initial_sidebar_state="collapsed")


@st.cache_resource
def demo() -> D.Demo:
    return D.load(os.environ.get("SENTINEL_DEMO_DIR") or D.DEMO)


def css() -> str:                                       # not cached: two small files, always current
    return (HERE.parent / "shared" / "theme.css").read_text() + (HERE / "streamlit.css").read_text()


def utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def hhmm(ms: int) -> str:
    return f"{utc(ms):%H:%M}"


def day(ms: int) -> str:
    d = utc(ms)
    return f"{DAYS[d.weekday()]} {d:%d %b}"


def verdict_text(v):
    return f"needs human: {v}" if v else "triage: investigating"


def verdict_class(v):
    return f"sl-v-{v}" if v else "sl-v-none"


def speed_label(m: int) -> str:
    return f"{m} min / tick" if m < 60 else f"{m // 60} h / tick"


e = html.escape
dm = demo()
START, END, BUCKET = D.ms(dm.start), D.ms(dm.end), D.ms(dm.start + dm.bucket) - D.ms(dm.start)
ss = st.session_state

# ------------------------------------------------------------------ state, first from the URL contract
if "t" not in ss:
    q = st.query_params
    try:
        t0 = D.ms(q["t"]) if "t" in q else START
    except ValueError:
        t0 = START
    ss.t = min(END, max(START, t0))
    ss.page = int(q["page"]) if q.get("page", "").isdigit() else None
    ss.series = q.get("series")
    ss.reveal = q.get("reveal") == "1"
    ss.speed = int(q["speed"]) if q.get("speed", "").isdigit() else SETTINGS["default_speed_min"]
    ss.playing = False
    ss.shown = None

st.markdown(f"<style>{css()}</style>", unsafe_allow_html=True)


def set_t(ms: int):
    ss.t = min(END, max(START, int(ms)))


def on_play():
    ss.playing = not ss.playing


def on_next():
    later = [D.ms(r["emitted_at"]) for r in dm.pages if r["kind"] == "paged" and D.ms(r["emitted_at"]) > ss.t]
    ss.playing = False
    if later:
        ss.page = None
        set_t(later[0])


def on_scrub():
    ss.playing = False
    set_t(D.ms(pd.Timestamp(ss.scrub, tz="UTC")))


def on_card(n: int):
    ss.page, ss.series = n, None


def on_series():
    picked = ss.get(f"series-pick-{ss.page_shown}")
    ss.series = ss.get("sid_by_label", {}).get(picked)


t = ss.t
ui = D.ui_state(dm, utc(t))
pages = ui["pages"]
if ss.page is not None and not any(p["page"] == ss.page for p in pages):
    ss.page = None
page = next((p for p in pages if p["page"] == ss.page), pages[0] if pages else None)
ss.page_shown = page["page"] if page else None

# ------------------------------------------------------------------ top strip
with st.container(key="strip", horizontal=True, vertical_alignment="center", gap="small"):
    st.markdown(f'<div class="sl-strip-inline"><span class="sl-brand">Sentinel Lens</span>'
                f'<span class="sl-clock" data-testid="clock" data-ms="{t}">{day(t)} {hhmm(t)}<small>UTC</small></span></div>',
                unsafe_allow_html=True, width="content")
    st.button("Pause" if ss.playing else "Play", key="play", on_click=on_play, type="primary" if ss.playing else "secondary")
    st.button("Next page", key="next-page", on_click=on_next)
    st.selectbox("Speed", SETTINGS["speeds_min"], key="speed", format_func=speed_label, label_visibility="collapsed", width=190)
    st.toggle("Reveal answer key", key="reveal")
    st.markdown(f'<div class="sl-count"><b data-testid="page-count">{ui["page_count"]}</b><span>page-outs so far</span></div>',
                unsafe_allow_html=True, width="content")

ss.scrub = utc(t).replace(tzinfo=None)
st.slider("Simulated time (UTC)", min_value=utc(START).replace(tzinfo=None), max_value=utc(END).replace(tzinfo=None),
          step=timedelta(minutes=5), key="scrub", on_change=on_scrub, format="ddd DD MMM HH:mm", label_visibility="collapsed")

# ------------------------------------------------------------------ timeline (left) and selected page (right)
left, right = st.columns([1, 2.7], gap="small", border=False)
prev = ss.shown
ss.shown = {p["page"] for p in pages}
with left.container(key="timeline"):
    st.markdown('<p class="sl-eyebrow">Pages, newest first</p>', unsafe_allow_html=True)
    if not pages:
        st.markdown('<p class="sl-empty">No page-out yet.</p>', unsafe_allow_html=True)
    for p in pages:
        fresh = prev is not None and p["page"] not in prev
        selected = page is not None and p["page"] == page["page"]
        merged = f' · merged into page {p["merged_into"]}' if p["merged_into"] else ""
        # the container's key is its CSS class (st-key-...): "-new" carries the slide-in, "-sel" the selection
        with st.container(key=f"card-{p['page']}" + ("-new" if fresh else "") + ("-sel" if selected else "")):
            st.markdown(
                f'<div class="sl-card{" is-selected" if selected else ""}" data-testid="page-card" data-page="{p["page"]}" '
                f'data-paged-at-ms="{p["paged_at"]}"><span class="sl-card-time">{day(p["paged_at"])} {hhmm(p["paged_at"])} UTC '
                f'· page {p["page"]}</span><span class="sl-card-reason" data-testid="card-reason">{e(p["reason"])}</span>'
                f'<span class="sl-pill {verdict_class(p["verdict"])}">{e(verdict_text(p["verdict"]))}</span>'
                f'<span class="sl-card-meta">{len(p["alerts"])} alerts{merged}</span></div>', unsafe_allow_html=True)
            st.button(f"Open page {p['page']}", key=f"open-{p['page']}", on_click=on_card, args=(p["page"],))


def note_card(r: dict, title: str) -> str:
    ev = r.get("evidence") or []
    cause, impact = r.get("cause") or {}, r.get("impact") or {}
    guard = (f'<p class="sl-advisory">Guard: converted from "{e(str(r["guard"].get("converted_from")))}": '
             f'{e(str(r["guard"].get("reason")))}</p>') if r.get("guard") else ""
    items = "".join(f"<li><code>{e(str(x.get('tool')))}</code>{e(str(x.get('finding')))}</li>" for x in ev)
    when = utc(r["ready_at"])
    return (f'<article class="sl-note" data-testid="note" data-type="{r["type"]}"><div class="sl-h3" role="heading" aria-level="3">{title}<span class="sl-when">written '
            f'{day(r["ready_at"])} {when:%H:%M:%S} UTC</span></div><p><span class="sl-pill {verdict_class(r.get("needs_human"))}">'
            f'{e(verdict_text(r.get("needs_human")))}</span><span class="sl-advisory">confidence {e(str(r.get("confidence")))} '
            f'· advisory only</span></p>{guard}'
            f'<div class="sl-field" data-testid="cause"><b>Cause · {e(cause.get("category") or "-")}</b><p>{e(cause.get("description") or "-")}</p></div>'
            f'<div class="sl-field" data-testid="impact"><b>Impact · {e(", ".join(impact.get("degraded") or []) or "none")}</b>'
            f'<p>{e(impact.get("description") or "-")}</p></div>'
            f'<details class="sl-evidence" data-testid="evidence"><summary>Evidence ({len(ev)})</summary><ul>{items}</ul></details>'
            f'<div class="sl-field" data-testid="next-step" style="margin-top:10px"><b>Next step</b><p>{e(r.get("next_step") or "-")}</p></div>'
            f'</article>')


def detail_html(p: dict, notes: list) -> str:
    written = [r for r in notes if r["type"] != "link"]
    src = ("from the follow-up" if written[-1]["type"] == "follow_up" else "from the first note") if written else "no note written yet"
    body = "".join(
        f'<p class="sl-link">{hhmm(r["ready_at"])} UTC: page {e(", ".join(map(str, r.get("absorbed") or [])))} merged into this page after it paged.</p>'
        if r["type"] == "link" else note_card(r, "Follow-up" if r["type"] == "follow_up" else "Triage note") for r in notes)
    merged = f' · merged into page {p["merged_into"]}' if p["merged_into"] else ""
    return (f'<div data-testid="page-detail" data-page="{p["page"]}"><p class="sl-eyebrow">Page {p["page"]} · paged '
            f'{day(p["paged_at"])} {hhmm(p["paged_at"])} UTC · {len(p["alerts"])} alerts now{merged}</p>'
            f'<div class="sl-headline" role="heading" aria-level="1" data-testid="reason-headline">{e(p["reason"])}</div>'
            f'<div class="sl-badge {verdict_class(p["verdict"])}" data-testid="verdict">{e(verdict_text(p["verdict"]))}</div>'
            f'<span class="sl-advisory" style="margin-left:10px">{src}</span>'
            f'{body or "<p class=sl-empty>The triage agent has not written a note yet.</p>"}</div>')


with right.container(key="detail"):
    slot = st.empty()                                   # cleared and refilled on every run
    if page is None:
        slot.markdown('<p class="sl-eyebrow">Selected page</p><p class="sl-empty">No page-out yet. Press play or Next page.</p>',
                      unsafe_allow_html=True)
    else:
        with slot.container(key=f"detail-{page['page']}"):   # keyed by page (a CSS hook; it does NOT remount)
            # st.html sets the block's inner HTML, so another page replaces it whole. With st.markdown, React
            # reused the <details> elements, and page 14's opened evidence was still open on page 17.
            st.html(detail_html(page, ui["notes"][str(page["page"])]))


# ------------------------------------------------------------------ chart of the page's series


with st.container(key="chartwrap"):
    if page is None:
        st.markdown('<div class="sl-chart-head"><div class="sl-h2" role="heading" aria-level="2">Series of the selected page</div></div>'
                    '<div data-testid="chart" data-series=""></div>', unsafe_allow_html=True)
    else:
        sids = page["series"]
        sid = ss.series if ss.series in sids else (page["paging_target"] if page["paging_target"] in sids else sids[0])
        lab = lambda s: f"{s.split('|')[-1]} {D.label(s)}"  # noqa: E731
        seasonal = D.rule_of(dm, sid) == "seasonal"
        down = any(a["target"] == sid for a in page["alerts"]) and all(a["direction"] == "down" for a in page["alerts"] if a["target"] == sid)
        legend = ['<span><i style="border-color:var(--chart-value)"></i>5-min mean</span>']
        if seasonal:
            legend += ['<span><i style="border-color:var(--chart-expected);border-top-style:dashed"></i>expected</span>',
                       '<span><i style="border-color:var(--chart-band);border-top-width:8px"></i>wobble band (expected +/- 1 wobble)</span>',
                       f'<span><i style="border-color:var(--chart-hot);border-top-style:dotted"></i>hot line (expected {"-" if down else "+"} {SETTINGS["hot_z"]} x wobble)</span>']
        legend += ['<span><i style="border-color:var(--chart-hot);border-top-style:dashed"></i>alert</span>',
                   '<span><i style="border-color:var(--chart-hot);border-top-width:3px"></i>page fired</span>',
                   '<span><i style="border-color:var(--chart-change)"></i>change</span>']
        if ss.reveal:
            legend.append('<span><i style="border-color:var(--key);border-top-width:8px"></i>injection (answer key)</span>')
        st.markdown(f'<div class="sl-chart-head"><div class="sl-h2" role="heading" aria-level="2">{e(lab(sid))}</div></div>', unsafe_allow_html=True)
        if len(sids) > 1:
            # a horizontal radio styled as chips (streamlit.css). Not st.pills: Streamlit's AppTest cannot run a
            # script twice with a pills widget on the page (its ButtonGroup state fails to serialise)
            by_label = {lab(s): s for s in sids}
            ss.sid_by_label = by_label
            st.radio("Series", list(by_label), index=list(by_label).index(lab(sid)), key=f"series-pick-{page['page']}",
                     on_change=on_series, horizontal=True, label_visibility="collapsed")
        st.markdown(f'<div class="sl-legend">{"".join(legend)}</div>', unsafe_allow_html=True)
        with st.container(key=f"chart-{page['page']}"):
            st.markdown(f'<div data-testid="chart-meta" data-series="{e(sid)}"></div>', unsafe_allow_html=True)
            st.altair_chart(series_chart(dm, ui, page, sid, t, ss.reveal), width="stretch", theme=None)

# ------------------------------------------------------------------ answer key (hidden from the system)
if ss.reveal:
    view = D.attacker_view(dm, utc(t))
    active = {i["id"] for i in view["active"]}
    rows = "".join(
        f'<tr class="{"is-active" if i["id"] in active else ""}"><td>{e(i["id"])}{" (active)" if i["id"] in active else ""}</td>'
        f'<td>{day(i["start"])} {hhmm(i["start"])}</td><td>{hhmm(i["stop"])}</td>'
        f'<td>{e(i["scenario"] if i["kind"] == "named" else "generic")}</td><td>{e(i["severity"])}</td><td>{e(i["intent"])}</td></tr>'
        for i in view["all"])
    st.markdown(
        '<div class="sl-key" data-testid="answer-key"><div class="sl-h2" role="heading" aria-level="2">Answer key: hidden from the system</div><p class="sl-advisory">'
        "The attacker's injections for the whole week, including after the clock. Nothing here reaches the detector, the pages "
        "or the triage agent. Rows active at the clock are bright; on the chart, an injection that touched the shown series is shaded."
        '</p><table><thead><tr><th>id</th><th>start</th><th>stop</th><th>kind</th><th>severity</th><th>intent</th></tr></thead>'
        f"<tbody>{rows}</tbody></table></div>", unsafe_allow_html=True)

st.markdown('<div data-testid="ready"></div>', unsafe_allow_html=True)

# ------------------------------------------------------------------ URL and play
if not ss.playing:
    want = {"t": f"{utc(t):%Y-%m-%dT%H:%MZ}"}
    if ss.page is not None:
        want["page"] = str(ss.page)
    if ss.series:
        want["series"] = ss.series
    if ss.reveal:
        want["reveal"] = "1"
    if dict(st.query_params) != want:
        st.query_params.from_dict(want)
else:
    if t >= END:
        ss.playing = False
        st.rerun()
    time.sleep(SETTINGS["tick_ms"] / 1000)
    set_t(t + ss.speed * 60_000)
    if ss.t >= END:
        ss.playing = False
    st.rerun()
