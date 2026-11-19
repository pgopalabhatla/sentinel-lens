"""Sentinel Lens: live week 2, replayed.

    pip install -r requirements.txt -r requirements-dashboard.txt
    streamlit run dashboard/app.py

Reads only the exported demo files (dashboard/demo/live-week-2/, made by dashboard/export_demo.py):
no broker, no data_live_2/, no API calls. A slider and a play button move a simulated clock over the
7-day stream. Every panel on the system's side shows only what existed at that clock (dashboard/data.py
has the rule). The attacker panel is the exception, and is labelled so: it shows the answer key, which
the system never sees.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import timedelta
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
import data as D  # noqa: E402

HOT_Z = 6.0                                           # the frozen seasonal threshold (docs/config-frozen.md)
SPEEDS = {"5 min / tick": 5, "15 min / tick": 15, "1 h / tick": 60}
ANSWER_BADGE = {"yes": "🔴 yes", "no": "🟢 no", "unsure": "🟡 unsure"}

st.set_page_config(page_title="Sentinel Lens: live week 2", layout="wide")


@st.cache_resource
def demo() -> D.Demo:
    return D.load(os.environ.get("SENTINEL_DEMO_DIR") or D.DEMO)


def naive(t: pd.Timestamp):
    return t.tz_convert("UTC").tz_localize(None).to_pydatetime()


dm = demo()
ss = st.session_state
ss.setdefault("clock", naive(dm.start))
ss.setdefault("playing", False)
ss.setdefault("speed", "15 min / tick")

# ------------------------------------------------------------------------------------------ the clock
st.title("Sentinel Lens: live week 2, replayed")
st.caption("Frozen detector, frozen triage agent (run 1 of 3) and a recorded attacker campaign, on a quiet world "
           "the system was never tuned on. Simulated time, UTC. Each panel shows only what existed at the clock, "
           "except the attacker's.")


def on_slide():
    ss.clock = ss.t_slider
    ss.playing = False


def next_page_out():
    now = pd.Timestamp(ss.clock, tz="UTC")
    later = [_ for _ in dm.pages if _["kind"] == "paged" and D._ts(_["emitted_at"]) > now]
    if later:
        ss.clock = naive(D._ts(later[0]["emitted_at"]) + timedelta(minutes=1))
    ss.playing = False


c1, c2, c3, c4 = st.columns([6, 1, 1.4, 1.6])
ss.t_slider = ss.clock
c1.slider("Simulated time (UTC)", min_value=naive(dm.start), max_value=naive(dm.end), step=timedelta(minutes=5),
          format="ddd MMM D, HH:mm", key="t_slider", on_change=on_slide)
if c2.button("⏸ Pause" if ss.playing else "▶ Play", width="stretch"):
    ss.playing = not ss.playing
    st.rerun()
c3.button("⏭ Next page-out", on_click=next_page_out, width="stretch")
c4.selectbox("Speed", list(SPEEDS), key="speed", label_visibility="collapsed")

t = pd.Timestamp(ss.clock, tz="UTC")
outs = D.page_outs(dm, t)
st.markdown(f"**Clock: {t:%a %b %d, %H:%M} UTC** · page-outs so far: **{len(outs)}**")

# ------------------------------------------------------------------------------------------ top row
left, right = st.columns([3, 2])

with left:
    st.subheader("Pages as they fire")
    if not outs:
        st.info("No page-out yet.")
        selected = None
    else:
        rows = []
        for p in outs:
            notes = [n for n in D.notes_for(dm, p["page"], t) if n["type"] in ("note", "follow_up")]
            answer = notes[-1]["needs_human"] if notes else None
            first = p["alerts"][0] if p["alerts"] else {}
            rows.append({"page": p["page"], "paged at": f"{p['paged_at']:%a %H:%M}",
                         "first alert": f"{first.get('target', '-')} ({first.get('detector', '-')})",
                         "alerts now": len(p["alerts"]),
                         "triage": ANSWER_BADGE.get(answer, "…investigating") if answer else "…investigating",
                         "merged into": f"page {p['merged_into']}" if p["merged_into"] else ""})
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
        choices = [p["page"] for p in outs]
        if ss.get("page_choice") not in choices:          # e.g. the clock moved back before that page
            ss.pop("page_choice", None)
        selected = st.selectbox("Selected page (newest first)", choices, format_func=lambda k: f"page {k}",
                                key="page_choice")

with right:
    st.subheader("Active injections")
    st.warning("**Attacker view: hidden from the system.** This panel reads the answer key. Nothing in it "
               "reaches the detector, the pages or the triage agent.", icon="🕶️")
    active = D.active_injections(dm, t)
    if not active:
        st.write("No injection active at this time.")
    for i in active:
        kind = "generic" if i["scenario"] == "custom" else i["scenario"]
        st.markdown(f"**{i['id']}** · {kind} · *{i['severity']}* · {i['start']:%a %H:%M}–{i['stop']:%H:%M}  \n"
                    f"{i['intent']}  \n`{', '.join(i['entities'])}`")
    with st.expander("Answer key: the whole week (INCLUDES THE FUTURE of the clock)"):
        st.caption("Attacker view: hidden from the system. Shown for the demo only.")
        st.dataframe(pd.DataFrame([{"id": i["id"], "start": f"{i['start']:%a %b %d %H:%M}", "stop": f"{i['stop']:%H:%M}",
                                    "kind": "generic" if i["scenario"] == "custom" else "named",
                                    "scenario": i["scenario"], "severity": i["severity"], "intent": i["intent"]}
                                   for i in D.injections(dm)]), hide_index=True, width="stretch")

# ------------------------------------------------------------------------------------------ the selected page
page = next((p for p in outs if p["page"] == selected), None)
note_col, chart_col = st.columns([2, 3])

with note_col:
    st.subheader("Triage note")
    if page is None:
        st.write("Select a page.")
    else:
        recs = D.notes_for(dm, page["page"], t)
        if not [r for r in recs if r["type"] in ("note", "follow_up")]:
            st.info(f"Page {page['page']}: the agent is investigating.")
        for r in recs:
            if r["type"] == "link":
                st.caption(f"{r['ready_at']:%H:%M}: page(s) {r.get('absorbed')} merged into this one after it paged.")
                continue
            label = "Follow-up (+10 min)" if r["type"] == "follow_up" else "First note"
            st.markdown(f"#### {label} · {r['ready_at']:%a %H:%M}")
            st.markdown(f"**needs_human: {ANSWER_BADGE.get(r['needs_human'], r['needs_human'])}** "
                        f"· confidence {r.get('confidence')} · *advisory only*")
            if r.get("guard"):
                st.caption(f"Guard: converted from “{r['guard'].get('converted_from')}”: {r['guard'].get('reason')}")
            cause, impact = r.get("cause") or {}, r.get("impact") or {}
            st.markdown(f"**Cause** ({cause.get('category', '-')}): {cause.get('description', '-')}")
            st.markdown(f"**Impact** ({', '.join(impact.get('degraded') or []) or 'none'}): {impact.get('description', '-')}")
            ev = r.get("evidence") or []
            if ev:
                st.markdown("**Evidence**")
                st.dataframe(pd.DataFrame([{"tool": e.get("tool"), "finding": e.get("finding")} for e in ev]),
                             hide_index=True, width="stretch")
            if r.get("next_step"):
                st.markdown(f"**Next step:** {r['next_step']}")

with chart_col:
    st.subheader("Series")
    if page is None or not page["alerts"]:
        st.write("Select a page.")
    else:
        sids = list(dict.fromkeys(a["target"] for a in page["alerts"]))
        sid = st.selectbox("Series in this page", sids, key=f"sid_{page['page']}")
        rule = D.rule_of(dm, sid)
        s = D.series_until(dm, sid, t)
        marks = pd.DataFrame([{"ts": a["time"], "detector": a["detector"]} for a in page["alerts"] if a["target"] == sid])
        if s.empty:
            st.write("No closed 5-minute bucket yet.")
        else:
            base = alt.Chart(s).encode(x=alt.X("ts:T", title=None))
            layers = [base.mark_line(color="#4c78a8").encode(y=alt.Y("value:Q", title=sid.split("|")[0]),
                                                              tooltip=["ts:T", "value:Q", "expected:Q", "wobble:Q"])]
            if rule == "seasonal":
                s = s.assign(lo=s.expected - s.wobble, hi=s.expected + s.wobble, hot=s.expected + HOT_Z * s.wobble)
                base = alt.Chart(s).encode(x="ts:T")
                layers = [base.mark_area(opacity=0.25, color="#9e9e9e").encode(y="lo:Q", y2="hi:Q"),
                          base.mark_line(strokeDash=[4, 3], color="#616161").encode(y="expected:Q"),
                          base.mark_line(strokeDash=[1, 3], color="#d62728").encode(y="hot:Q")] + layers
            if not marks.empty:
                marks["ts"] = pd.to_datetime(marks["ts"], utc=True)
                marks = marks[marks["ts"] >= s["ts"].min()]
                layers.append(alt.Chart(marks).mark_rule(color="#d62728", opacity=0.5).encode(x="ts:T", tooltip=["ts:T", "detector:N"]))
            st.altair_chart(alt.layer(*layers).properties(height=320), width="stretch")
            if rule == "seasonal":
                st.caption("Blue: 5-minute mean. Grey dashed: expected (same time on the last 5 days of the same type). "
                           "Grey band: ± wobble. Red dotted: hot line, expected + 6 × wobble. Red rules: this page's alerts "
                           "on the series. Expected and wobble are recomputed with the frozen settings; the export checks "
                           "them against the engine's own alert texts.")
            else:
                st.caption(f"Guardrail series ({rule}): alerts on the raw value (an increase, or above zero), no baseline. "
                           "Red rules: this page's alerts on the series.")

# ------------------------------------------------------------------------------------------ play
if ss.playing:
    if t >= dm.end:
        ss.playing = False
    else:
        time.sleep(0.4)
        ss.clock = naive(min(t + pd.Timedelta(minutes=SPEEDS[ss.speed]), dm.end))
        st.rerun()
