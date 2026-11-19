"""What the dashboard knows at a given simulated time `t`, from the exported demo files.

The rule the whole dashboard follows: **nothing from after `t` is shown**, except in the attacker panel's
labelled answer-key view. Every function here that serves the system's side takes `t` and returns only
records that exist at `t`:

    a page record          exists from its `emitted_at`
    a page-out             from its `paged` record's `emitted_at`
    a triage note          from as_of + seconds (when the agent finished; the scorer's timing)
    a 5-minute reading     from the END of its bucket (ts + 5 min): its mean is not known before
    an injection (attacker view, hidden from the system)   active while start <= t < stop

Kept free of Streamlit so the no-future rule can be tested directly (tests/test_dashboard.py).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

DEMO = Path(__file__).resolve().parent / "demo" / "live-week-2"


def _ts(x) -> pd.Timestamp:
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


@dataclass
class Demo:
    meta: dict
    answer_key: list
    pages: list
    alerts: list
    triage: list
    series: pd.DataFrame

    @property
    def start(self) -> pd.Timestamp:
        return _ts(self.meta["stream_start"])

    @property
    def end(self) -> pd.Timestamp:
        return _ts(self.meta["stream_end"])

    @property
    def bucket(self) -> pd.Timedelta:
        return pd.Timedelta(minutes=self.meta["bucket_minutes"])


def load(folder: str | Path = DEMO) -> Demo:
    folder = Path(folder)
    if not (folder / "meta.json").exists():
        raise FileNotFoundError(f"{folder} has no demo export: run python dashboard/export_demo.py")
    read = lambda name: json.loads((folder / f"{name}.json").read_text())  # noqa: E731
    series = pd.read_csv(folder / "series.csv.gz", parse_dates=["ts"])
    series["ts"] = pd.to_datetime(series["ts"], utc=True)
    return Demo(read("meta"), read("answer_key"), read("pages"), read("alerts"), read("triage_r1"), series)


# ------------------------------------------------------------------ attacker view (hidden from the system)
def injections(demo: Demo) -> list[dict]:
    """Every injection of the answer key, with its actual stop. ANSWER KEY: the whole week."""
    out = {}
    for r in demo.answer_key:
        if r["event"] == "started":
            out[r["id"]] = {**r, "start": _ts(r["start"]), "stop": _ts(r["stop"])}
        elif r["event"] == "stopped" and r["id"] in out:
            out[r["id"]]["stop"] = _ts(r["stop"])
    return sorted(out.values(), key=lambda i: i["start"])


def active_injections(demo: Demo, t) -> list[dict]:
    t = _ts(t)
    return [i for i in injections(demo) if i["start"] <= t < i["stop"]]


# ------------------------------------------------------------------ the system's side: nothing after t
def page_outs(demo: Demo, t) -> list[dict]:
    """The page-outs that have fired by `t`, each with the alerts its page holds AT `t` (a page keeps
    growing after it pages; merges move another page's alerts into it). Newest first."""
    t = _ts(t)
    held, paged, alias = {}, {}, {}
    for r in demo.pages:
        at = _ts(r["emitted_at"])
        if at > t:
            break                                       # records are in emission order
        key = r["page"]
        if r["kind"] == "paged":
            paged[key] = {"page": key, "page_id": r["page_id"], "paged_at": at,
                          "paged_with": len(held.get(key, []))}
            continue
        held.setdefault(key, []).append({**r["alert"], "emitted_at": at})
        for absorbed in r.get("absorbed", []):
            held[key] += held.pop(absorbed, [])
            alias[absorbed] = key
    out = []
    for key, p in paged.items():
        home = key
        while home in alias:                            # a page that paged and was merged later
            home = alias[home]
        alerts = sorted(held.get(home, []), key=lambda a: a["emitted_at"])
        out.append({**p, "merged_into": home if home != key else None, "alerts": alerts})
    return sorted(out, key=lambda p: p["paged_at"], reverse=True)


def notes_for(demo: Demo, page: int, t) -> list[dict]:
    """Triage run 1's note and follow-up for a page, each only once it was written (as_of + seconds),
    oldest first; plus link records (a later page merged into this one) once they happened."""
    t = _ts(t)
    out = []
    for r in demo.triage:
        if r.get("page") != page:
            continue
        if r["type"] in ("note", "follow_up"):
            ready = _ts(r["as_of"]) + pd.Timedelta(seconds=r.get("seconds") or 0)
        else:
            ready = _ts(r["at"])
        if ready <= t:
            out.append({**r, "ready_at": ready})
    return sorted(out, key=lambda r: r["ready_at"])


def series_until(demo: Demo, series_id: str, t, lookback="12h") -> pd.DataFrame:
    """The 5-minute readings of one series whose bucket has CLOSED by `t`, over the last `lookback`."""
    t = _ts(t)
    s = demo.series[demo.series["series_id"] == series_id]
    closed = s["ts"] + demo.bucket <= t
    recent = s["ts"] >= t - pd.Timedelta(lookback)
    return s[closed & recent].copy()


def rule_of(demo: Demo, series_id: str) -> str | None:
    s = demo.series.loc[demo.series["series_id"] == series_id, "rule"]
    return None if s.empty else s.iloc[0]
