"""What the dashboard knows at a given simulated time `t`, from the exported demo files.

The rule the whole dashboard follows: **nothing from after `t` is shown**, except in the attacker panel's
labelled answer-key view. Every function here that serves the system's side takes `t` and returns only
records that exist at `t`:

    a page record          exists from its `emitted_at`
    a page-out             from its `paged` record's `emitted_at`
    a triage note          from as_of + seconds (when the agent finished; the scorer's timing), to the ms
    a change (deploy etc.) from its stream record's time (`streamed_at`, a minute after the change)
    a 5-minute reading     from the END of its bucket (ts + 5 min): its mean is not known before
    an injection (attacker view, hidden from the system)   active while start <= t < stop

Kept free of Streamlit so the no-future rule can be tested directly (tests/test_dashboard.py).

`ui_state(demo, t)` is everything the system's panels show at `t`, as plain JSON. The JavaScript
dashboards (dashboard/b_static, dashboard/c_react) use a port of it, dashboard/shared/visibility.js,
and tests/test_dashboard_ui.py checks that the port gives exactly the same output at the same clocks.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
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
    changes: list = field(default_factory=list)

    @property
    def start(self) -> pd.Timestamp:
        return _ts(self.meta["stream_start"])

    @property
    def end(self) -> pd.Timestamp:
        return _ts(self.meta["stream_end"])

    @property
    def bucket(self) -> pd.Timedelta:
        return pd.Timedelta(minutes=self.meta["bucket_minutes"])


def _finite(x):
    """JSON has no Infinity or NaN, and browsers refuse them: a non-finite number (an alert's z when the
    wobble is 0) becomes None. The alert's explanation text still says "z=+inf"."""
    if isinstance(x, float) and not math.isfinite(x):
        return None
    if isinstance(x, list):
        return [_finite(v) for v in x]
    if isinstance(x, dict):
        return {k: _finite(v) for k, v in x.items()}
    return x


def load(folder: str | Path = DEMO) -> Demo:
    folder = Path(folder)
    if not (folder / "meta.json").exists():
        raise FileNotFoundError(f"{folder} has no demo export: run python dashboard/export_demo.py")
    read = lambda name: _finite(json.loads((folder / f"{name}.json").read_text()))  # noqa: E731
    series = pd.read_csv(folder / "series.csv.gz", parse_dates=["ts"])
    series["ts"] = pd.to_datetime(series["ts"], utc=True)
    changes = read("changes") if (folder / "changes.json").exists() else []
    return Demo(read("meta"), read("answer_key"), read("pages"), read("alerts"), read("triage_r1"), series, changes)


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


def ready_at(r: dict) -> pd.Timestamp:
    """When a triage record exists: a note or follow-up once written (as_of + seconds, rounded to the
    millisecond so the JavaScript port agrees exactly), a link record at its own time."""
    if r["type"] in ("note", "follow_up"):
        return _ts(r["as_of"]) + pd.Timedelta(milliseconds=round((r.get("seconds") or 0) * 1000))
    return _ts(r["at"])


def notes_for(demo: Demo, page: int, t) -> list[dict]:
    """Triage run 1's note and follow-up for a page, each only once it was written (as_of + seconds),
    oldest first; plus link records (a later page merged into this one) once they happened."""
    t = _ts(t)
    out = []
    for r in demo.triage:
        if r.get("page") != page:
            continue
        ready = ready_at(r)
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


def changes_until(demo: Demo, t) -> list[dict]:
    """The change log as the system had it at `t`: changes whose stream record had arrived, oldest first."""
    t = _ts(t)
    return [c for c in demo.changes if _ts(c["streamed_at"]) <= t]


# ------------------------------------------------------------------ one line per page, in plain words
LABELS = {
    "airflow_scheduler_heartbeat_age_sec": "scheduler heartbeat age", "container_cpu_throttled_ratio": "CPU throttling",
    "container_cpu_usage_cores": "CPU", "container_memory_working_set_bytes": "memory",
    "flink_backpressure_ratio": "backpressure", "flink_checkpoint_failures_total": "checkpoint failures",
    "flink_last_checkpoint_duration_ms": "checkpoint duration", "flink_records_out_per_sec": "records out",
    "jvm_gc_time_ms_per_sec": "GC time", "jvm_heap_used_ratio": "heap",
    "kafka_consumer_records_consumed_per_sec": "consume rate", "kafka_consumergroup_lag": "consumer lag",
    "kafka_failed_produce_requests_per_sec": "failed produce requests", "kafka_isr_shrinks_per_sec": "ISR shrinks",
    "kafka_messages_in_per_sec": "in-rate", "kafka_produce_request_p99_ms": "produce p99",
    "kafka_under_replicated_partitions": "under-replicated partitions",
    "kube_pod_container_status_restarts_total": "restarts", "spark_streaming_batch_duration_sec": "batch duration",
    "spark_streaming_input_rows_per_sec": "input rows", "spark_streaming_scheduling_delay_sec": "scheduling delay",
    "trino_query_p95_sec": "query p95", "trino_queued_queries": "queued queries",
    "trino_resource_group_queued": "queued queries in resource group",
}
_RATIO = re.compile(r" is ([\d.]+)x expected")
_VS_ZERO = re.compile(r" is ([-\d.e+]+) vs expected 0\b")
_SHOULD = re.compile(r" is ([-\d.e+]+), should be ([-\d.e+]+)")
_ROSE = re.compile(r" rose from ([-\d.e+]+) to ([-\d.e+]+)")


def label(series_id: str) -> str:
    metric = series_id.split("|")[0]
    return LABELS.get(metric, metric.replace("_", " "))


def reason(alert: dict) -> str:
    """One plain line from an alert, e.g. "trino-worker-0 CPU 6.3x expected". Read from the engine's own
    explanation text; every alert of the week has a match (tests/test_dashboard_ui.py)."""
    sid, text = alert["target"], alert.get("explanation") or ""
    entity, what = sid.split("|")[-1], label(sid)
    if m := _RATIO.search(text):
        return f"{entity} {what} {m.group(1)}x expected"
    if m := _VS_ZERO.search(text):
        return f"{entity} {what} at {m.group(1)}, expected 0"
    if m := _SHOULD.search(text):
        return f"{entity} {what} at {m.group(1)}, should be {m.group(2)}"
    if m := _ROSE.search(text):
        return f"{entity} {what} rose from {m.group(1)} to {m.group(2)}"
    return f"{entity} {what} {'up' if alert.get('direction') == 'up' else 'down'}"


# ------------------------------------------------------------------ the UI's view, as plain JSON
def ms(t) -> int:
    """A time as integer epoch milliseconds (UTC): the one time format of ui_state and the JS port."""
    return int(_ts(t).value // 1_000_000)


def _paging_alerts(demo: Demo) -> dict:
    return {r["page"]: r["alert"] for r in demo.pages if r["kind"] == "paged"}


def ui_state(demo: Demo, t) -> dict:
    """Everything the system's panels show at `t`, built only from page_outs, notes_for and changes_until
    (so only from records that exist at `t`). Times are epoch ms. Pages newest first."""
    t = _ts(t)
    paging = _paging_alerts(demo)
    pages, notes = [], {}
    for p in sorted(page_outs(demo, t), key=lambda p: (ms(p["paged_at"]), p["page"]), reverse=True):
        recs = notes_for(demo, p["page"], t)
        written = [r for r in recs if r["type"] in ("note", "follow_up")]
        alerts = [{**a, "time": ms(a["time"]), "emitted_at": ms(a["emitted_at"])} for a in p["alerts"]]
        pages.append({"page": p["page"], "page_id": p["page_id"], "paged_at": ms(p["paged_at"]),
                      "paged_with": p["paged_with"], "merged_into": p["merged_into"],
                      "reason": reason(paging[p["page"]]), "paging_target": paging[p["page"]]["target"],
                      "series": list(dict.fromkeys(a["target"] for a in alerts)), "alerts": alerts,
                      "verdict": written[-1]["needs_human"] if written else None,
                      "verdict_at": ms(written[-1]["ready_at"]) if written else None})
        notes[str(p["page"])] = [{**r, "ready_at": ms(r["ready_at"])} for r in recs]
    return {"clock": ms(t), "page_count": len(pages), "pages": pages, "notes": notes,
            "changes": [{**c, "ts_ms": ms(c["ts"]), "streamed_at_ms": ms(c["streamed_at"])}
                        for c in changes_until(demo, t)]}


def series_points(demo: Demo, series_id: str, t, lookback_hours: int = 12) -> list[list]:
    """series_until as [ts_ms, value, expected, wobble] rows (None for a missing number)."""
    s = series_until(demo, series_id, t, lookback=f"{lookback_hours}h")
    num = lambda x: None if x is None or (isinstance(x, float) and math.isnan(x)) else float(x)  # noqa: E731
    return [[ms(r.ts), num(r.value), num(r.expected), num(r.wobble)] for r in s.itertuples()]


def attacker_view(demo: Demo, t) -> dict:
    """ANSWER KEY, hidden from the system: the injections active at `t` and the whole week's list."""
    def row(i):
        return {"id": i["id"], "scenario": i["scenario"], "kind": "generic" if i["scenario"] == "custom" else "named",
                "severity": i["severity"], "intent": i["intent"], "entities": i["entities"],
                "series": i.get("series", []), "start": ms(i["start"]), "stop": ms(i["stop"])}
    return {"active": [row(i) for i in active_injections(demo, t)], "all": [row(i) for i in injections(demo)]}
