"""Read-only context for ONE page, cut off at the moment it paged.

The triage agent (investigator.py) and the MCP server (mcp_server.py) both call the functions here;
there is one implementation and one list of tools (`TOOLS`), each with its JSON schema.

Where each tool reads from
--------------------------
    get_page             the `pages` topic (and `alerts`, for which alerts are demoted)    the stream
    query_metric         the `telemetry` topic: the readings as they were STREAMED         the stream
    baseline_comparison  the seasonal detector's own 5-minute means and history: the       the stream (before it
                         history store before the stream started, the stream after         started: history store)
    kafka_lag            the `telemetry` topic (lag, consume rate, the topic's in-rate)    the stream
    dag_runs             the `telemetry` topic's run messages; the schedule from config    the stream
    related_entities     the entity graph (catalog + relationships + topology_extra)       configuration
    recent_logs          stream v1: the log store (the data directory, DataSource)         history store
                         stream v2: the streamed `logs` batches                            the stream (before it
    k8s_events           likewise, Kubernetes events                                       started: history store)
    recent_changes       likewise, the change log

Metrics come from the stream and never from the data directory's metrics file: whatever the
publisher sent, changed or left out is what the tools see. On stream version 1, logs, events and
changes are not on the stream at all, so those three read the data directory, which stands in for
Loki, the Kubernetes API and a deploy log. On version 2 they are on the stream (wire.py) and the tools
read them from there; for times before the stream started they read the data directory, as a log
store would hold what came before. Every result says where it came from (`source`). Which version a
Context reads is given when it is built (`stream_version`), never guessed.

Causality
---------
A `Context` is built for one `paged` record of the `pages` topic. `now` is that record's time. Every
tool sees:
    readings and run messages stamped at or before `now` (the engine stamps a minute's outputs with
        the minute's own time, so the minute stamped `now` had been read when the page went out);
    page records up to and including that `paged` record (topic order), alert records emitted at or
        before `now`;
    the data directory through `DataSource.at(now)`, which returns nothing at or after `now`.
Nothing in this module can reach past `now`: the cut is made once, when the Context is built.

This module reads three topics' records (handed to it; it opens no topic itself) and the data
directory. It knows nothing about how failures are produced in the live stream and must not
(tests/test_live_isolation.py).
"""
from __future__ import annotations

import difflib
import json
import math
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd

from sentinel_lens import router
from sentinel_lens.correlator import Topology, build_topology
from sentinel_lens.detectors.seasonal import seasonal_scores
from sentinel_lens.topology_extra import EXTRA_EDGES
from sentinel_stream import wire
from sentinel_stream.events import utc
from sentinel_stream.seasonal_step import LOOKBACK_DAYS, SCORE

ONE_MIN, BUCKET = pd.Timedelta("1min"), pd.Timedelta("5min")
STREAM, HISTORY, CONFIG = "stream", "history store (data directory)", "configuration (catalog and topology)"
STREAM_THEN_HISTORY = "stream (before the stream started: history store)"
LOG_COLUMNS = ["ts", "pod", "level", "logger", "message"]
EVENT_COLUMNS = ["ts", "type", "reason", "object", "object_kind", "namespace", "message", "count"]
MAX_RESULT_CHARS = 16000          # a tool result larger than this is a bug in the tool, not something to send


def _r(x) -> float | None:
    """4 significant digits: enough to read a trend, few tokens."""
    if x is None or (isinstance(x, float) and not math.isfinite(x)):
        return None
    return float(f"{float(x):.4g}")


def _t(ts) -> str:
    return utc(ts).strftime("%Y-%m-%dT%H:%MZ")


def _ts_s(ts) -> str:
    return utc(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


# ------------------------------------------------------------------------------- what was streamed
class StreamStore:
    """The records of the three topics the triage side reads, in topic order. Grows as they arrive
    (the dispatcher adds records as it reads them); `Context.at_page` takes a cut of it."""

    def __init__(self):
        self._ts: list = []
        self._rows: list = []
        self.runs: list = []          # dag_run messages
        self.pages: list = []         # page records
        self.alerts: list = []        # alert records
        self.evidence: dict = {kind: [] for kind in wire.EVIDENCE}     # stream v2: records of the batches
        self._frame = None
        self._evidence_frames: dict = {}

    @classmethod
    def from_records(cls, telemetry=(), pages=(), alerts=()) -> "StreamStore":
        store = cls()
        for m in telemetry:
            store.add_telemetry(m)
        for r in pages:
            store.add_page(r)
        for r in alerts:
            store.add_alert(r)
        return store

    def add_telemetry(self, message: dict) -> None:
        if message["type"] == wire.METRICS:
            self._ts.append(wire.event_time(message))
            self._rows.append(message["values"])
            self._frame = None
        elif message["type"] == wire.DAG_RUN:
            self.runs.append(message)
        elif wire.is_evidence(message):
            self.evidence[message["type"]].extend(wire.records_of(message))
            self._evidence_frames.pop(message["type"], None)

    def evidence_frame(self, kind: str) -> pd.DataFrame:
        """The streamed logs or k8s events as a table with the columns DataSource gives them."""
        if kind not in self._evidence_frames:
            records = self.evidence[kind]
            if kind == wire.LOGS:
                frame = pd.DataFrame.from_records(records, columns=LOG_COLUMNS)
            else:
                frame = pd.DataFrame.from_records(
                    [{**{k: r[k] for k in ("ts", "type", "reason", "message", "count")}, "object": r["involved_object"]["name"],
                      "object_kind": r["involved_object"]["kind"], "namespace": r["involved_object"]["namespace"]}
                     for r in records], columns=EVENT_COLUMNS)
            frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
            self._evidence_frames[kind] = frame.drop_duplicates().sort_values("ts", kind="stable").reset_index(drop=True)
        return self._evidence_frames[kind]

    def add_page(self, record: dict) -> None:
        self.pages.append(record)

    def add_alert(self, record: dict) -> None:
        self.alerts.append(record)

    def metrics(self) -> pd.DataFrame:
        """One row per streamed minute, one column per series (NaN: no reading that minute)."""
        if self._frame is None:
            if self._rows:
                frame = pd.DataFrame.from_records(self._rows, index=pd.DatetimeIndex(self._ts, name="ts"))
                frame = frame[~frame.index.duplicated(keep="first")].sort_index()   # a message sent twice counts once
            else:
                frame = pd.DataFrame(index=pd.DatetimeIndex([], tz="UTC", name="ts"))
            self._frame = frame
        return self._frame

    def paged(self) -> list:
        """Indices (into `pages`) of the `paged` records: one per page-out."""
        return [i for i, r in enumerate(self.pages) if r["kind"] == "paged"]


class StreamEvidence:
    """Stream v2's logs, k8s events and changes as a Context sees them: what was streamed before `now`,
    and before the stream started what the history store holds. The same three methods (and
    arguments) as DataSource, so the tools do not care which they read."""

    def __init__(self, store: StreamStore, history, now, stream_start):
        cut = lambda df: df[df.ts < now].copy()                   # a batch stamped T <= now holds records before T
        self._logs, self._events = cut(store.evidence_frame(wire.LOGS)), cut(store.evidence_frame(wire.K8S_EVENTS))
        seen, self._changes = set(), []
        for c in store.evidence[wire.CHANGES]:
            if utc(c["ts"]) < now and c["change_id"] not in seen:
                seen.add(c["change_id"])
                self._changes.append(c)
        self.history, self.start = history, stream_start

    def _split(self, start, end) -> tuple:
        """(the part of [start, end) before the stream, read from history; or None), the streamed part's start."""
        if self.start is None:
            return (start, end), end
        return ((start, min(end, self.start)) if start < self.start else None), max(start, self.start)

    def logs(self, pod=None, start=None, end=None, level=None, contains=None, limit=None) -> pd.DataFrame:
        early, s = self._split(utc(start), utc(end))
        parts = [self.history.logs(pod=pod, start=early[0], end=early[1], level=level, contains=contains, limit=None)[LOG_COLUMNS]] \
            if early else []
        df = self._logs
        m = (df.ts >= s) & (df.ts < utc(end))
        if pod:
            m &= df.pod == pod
        if level:
            m &= df.level.isin([level] if isinstance(level, str) else level)
        if contains:
            m &= df.message.str.contains(contains, regex=False)
        out = pd.concat(parts + [df[m]], ignore_index=True) if parts else df[m]
        return out.tail(limit) if limit else out

    def k8s_events(self, pod=None, start=None, end=None, reason=None) -> pd.DataFrame:
        early, s = self._split(utc(start), utc(end))
        parts = [self.history.k8s_events(pod=pod, start=early[0], end=early[1], reason=reason).reindex(columns=EVENT_COLUMNS)] \
            if early else []
        df = self._events
        m = (df.ts >= s) & (df.ts < utc(end))
        if pod:
            m &= df.object == pod
        if reason:
            m &= df.reason.isin([reason] if isinstance(reason, str) else reason)
        return pd.concat(parts + [df[m]], ignore_index=True) if parts else df[m]

    def changes(self, start=None, end=None, target=None) -> list:
        early, s = self._split(utc(start), utc(end))
        before = self.history.changes(start=early[0], end=early[1], target=target) if early else []
        return before + [c for c in self._changes if s <= utc(c["ts"]) < utc(end) and (target is None or target in c["target"])]


# ----------------------------------------------------------------------------------- configuration
@dataclass
class Catalog:
    """What does not change with time: the series catalog, the entity graph, pods, DAG schedules."""
    topology: Topology
    relation: dict                    # frozenset({a, b}) -> "a <relation> b"
    unit_of: dict
    series_of: dict                   # entity -> [series_id]
    pods: frozenset
    dags: dict
    entities: frozenset
    routes: dict = field(default_factory=dict)     # series_id -> (rule, history mode, abs floor), the router's table

    @classmethod
    def from_ds(cls, ds) -> "Catalog":
        rel, catalog = ds.relationships(), ds.series()
        topology = build_topology(rel, catalog)
        relation = {}
        for ck, c, how, pk, p in zip(rel.child_kind, rel.child, rel.relation, rel.parent_kind, rel.parent):
            relation[frozenset({f"{ck}:{c}", f"{pk}:{p}"})] = f"{ck}:{c} {how} {pk}:{p}"
        for e in EXTRA_EDGES:
            relation.setdefault(frozenset({f"{e[0]}:{e[1]}", f"{e[3]}:{e[4]}"}), f"{e[0]}:{e[1]} {e[2]} {e[3]}:{e[4]}")
        series_of: dict = {}
        for sid, ent in topology.entity_of.items():
            series_of.setdefault(ent, []).append(sid)
        entities = frozenset(series_of) | frozenset(topology.neighbours)
        dags = {k: {"schedule": v["schedule"], "sla_minutes": v["sla_minutes"]} for k, v in ds._s.topology["dags"].items()}
        entities |= {f"dag:{d}" for d in dags}
        table = router.route_series(ds.series(layer=[1, 2, 3]))            # as the engine routes them (engine.warm_start)
        routes = {s: (rule, h if isinstance(h, str) else None, float(f) if f == f else 0.0)
                  for s, rule, h, f in zip(table.series_id, table.rule, table.history, table.abs_floor)}
        return cls(topology, relation, dict(zip(catalog.series_id, catalog.unit)),
                   {k: sorted(v) for k, v in series_of.items()}, frozenset(ds.pods().pod), dags, entities, routes)

    def resolve_entity(self, name: str) -> tuple:
        """("kind:name", None) or (None, error). Takes "kind:name", a series id, or a bare name."""
        if "|" in name:
            if name in self.topology.entity_of:
                return self.topology.entity_of[name], None
            return None, f"unknown series {name!r}" + _close(name, self.unit_of)
        if ":" in name:
            return (name, None) if name in self.entities else (None, f"unknown entity {name!r}" + _close(name, self.entities))
        found = sorted(e for e in self.entities if e.split(":", 1)[1] == name)
        if len(found) == 1:
            return found[0], None
        if found:
            return None, f"{name!r} is ambiguous: give one of {found}"
        return None, f"unknown entity {name!r}" + _close(name, {e.split(':', 1)[1] for e in self.entities})


def _close(name: str, options) -> str:
    near = difflib.get_close_matches(name, list(options), n=5, cutoff=0.5)
    return f"; did you mean one of {near}?" if near else ""


# --------------------------------------------------------------------------------- one page, at now
@dataclass
class Context:
    now: pd.Timestamp
    page: int                         # the page's number on the topic
    page_id: str                      # its id when it paged
    metrics: pd.DataFrame             # streamed minutes stamped <= now
    runs: list                        # streamed run messages stamped <= now
    page_records: list                # page records up to and including the paged one
    alert_records: list               # alert records emitted <= now
    history: object                   # DataSource.at(now): schedule, and metrics from before the stream
    catalog: Catalog
    stream_start: pd.Timestamp | None
    calls: list = field(default_factory=list)   # (tool, args) of every call made through `call`
    evidence: object = None           # logs, k8s events, changes: `history` (stream v1) or a StreamEvidence (v2)
    evidence_source: str = HISTORY

    @classmethod
    def at_page(cls, store: StreamStore, ds, index: int, catalog: Catalog | None = None, stream_version: int = 1) -> "Context":
        """The context of the page-out at `store.pages[index]` (a `paged` record), as it was then.
        stream_version 2: logs, events and changes are read from the stream (module docstring)."""
        if stream_version not in wire.STREAM_VERSIONS:
            raise ValueError(f"stream_version must be one of {wire.STREAM_VERSIONS}")
        record = store.pages[index]
        if record["kind"] != "paged":
            raise ValueError(f"pages[{index}] is a {record['kind']!r} record, not a page-out")
        now = utc(record["emitted_at"])
        frame = store.metrics()
        history = ds.at(now)
        stream_start = frame.index[0] if len(frame.index) else None
        evidence = history if stream_version == 1 else StreamEvidence(store, history, now, stream_start)
        return cls(now=now, page=int(record["page"]), page_id=record["page_id"],
                   metrics=frame.loc[:now].copy(), runs=[m for m in store.runs if wire.event_time(m) <= now],
                   page_records=list(store.pages[:index + 1]),
                   alert_records=[r for r in store.alerts if utc(r["emitted_at"]) <= now],
                   history=history, catalog=catalog or Catalog.from_ds(ds), stream_start=stream_start,
                   evidence=evidence, evidence_source=HISTORY if stream_version == 1 else STREAM_THEN_HISTORY)


# ------------------------------------------------------------------------------------------- tools
@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    input_schema: dict
    fn: Callable


def _schema(props: dict, required=()) -> dict:
    return {"type": "object", "properties": props, "required": list(required), "additionalProperties": False}


def _int(desc, lo, hi, default):
    return {"type": "integer", "minimum": lo, "maximum": hi, "default": default, "description": desc}


def _str(desc, enum=None):
    return {"type": "string", "description": desc, **({"enum": list(enum)} if enum else {})}


def get_page(ctx: Context) -> dict:
    held, ids, merged = {}, {}, {}
    for r in ctx.page_records:
        key = r["page"]
        ids[key] = r["page_id"]
        if r["kind"] == "paged":
            continue
        held.setdefault(key, []).append(r["alert"])
        for k in r.get("absorbed", []):
            held[key] += held.pop(k, [])
            merged.setdefault(key, []).append(k)
    alerts = sorted(held.get(ctx.page, []), key=lambda a: (a["time"], a["target"]))
    demoted = {(r["alert"]["time"], r["alert"]["target"], r["alert"]["detector"]): r["demoted"] for r in ctx.alert_records}
    by_target: dict = {}
    for a in alerts:
        g = by_target.setdefault((a["target"], a["detector"]), {"target": a["target"], "detector": a["detector"],
                                                               "entity": ctx.catalog.topology.entity(a["target"]),
                                                               "n": 0, "first": a["time"], "max_score": 0.0})
        g.update(n=g["n"] + 1, last=a["time"], max_score=max(g["max_score"], a["score"]), direction=a["direction"],
                 last_explanation=a["explanation"],
                 demoted=demoted.get((a["time"], a["target"], a["detector"]), False))
    shown = 30
    return {"source": STREAM, "page": ctx.page, "page_id": ids.get(ctx.page, ctx.page_id), "paged_at": _ts_s(ctx.now),
            "n_alerts": len(alerts), "merged_pages": merged.get(ctx.page, []),
            "entities": dict(Counter(ctx.catalog.topology.entity(a["target"]) for a in alerts).most_common()),
            "by_target": [{**g, "first": _ts_s(g["first"]), "last": _ts_s(g["last"]), "max_score": _r(g["max_score"])}
                          for g in sorted(by_target.values(), key=lambda g: g["first"])],
            "alerts": [{"time": _ts_s(a["time"]), "target": a["target"], "detector": a["detector"], "score": _r(a["score"]),
                        "direction": a["direction"], "explanation": a["explanation"],
                        "demoted": demoted.get((a["time"], a["target"], a["detector"]), False)} for a in alerts[:shown]],
            "alerts_not_shown": max(0, len(alerts) - shown),
            "note": "demoted: the alert's feedback group was demoted, so on its own it would not page anyone"}


def _summary(ctx: Context, sid: str, window_min: int, points: int) -> dict:
    start = ctx.now - pd.Timedelta(minutes=window_min)
    col = ctx.metrics[sid] if sid in ctx.metrics.columns else pd.Series(dtype=float)
    s = col.loc[col.index > start].dropna()
    covered = 0 if ctx.stream_start is None else max(0, int((ctx.now - max(start + ONE_MIN, ctx.stream_start)) / ONE_MIN) + 1)
    out = {"series_id": sid, "unit": ctx.catalog.unit_of.get(sid, ""), "entity": ctx.catalog.topology.entity(sid),
           "window": f"{_t(start + ONE_MIN)} .. {_t(ctx.now)}", "readings": int(len(s)),
           "minutes_without_reading": max(0, covered - int(len(s)))}
    if s.empty:
        out["note"] = "no reading in this window (the series was not sent: pod not running, or nothing to report)"
        return out
    q = s.quantile([0.05, 0.5, 0.95])
    hours = (s.index - s.index[0]).total_seconds() / 3600
    slope = float(np.polyfit(hours, s.to_numpy(dtype=float), 1)[0]) if len(s) > 2 and hours[-1] > 0 else 0.0
    out.update(first=_r(s.iloc[0]), last=_r(s.iloc[-1]), min=_r(s.min()), max=_r(s.max()), mean=_r(s.mean()),
               p05=_r(q.iloc[0]), median=_r(q.iloc[1]), p95=_r(q.iloc[2]), slope_per_hour=_r(slope),
               max_at=_ts_s(s.idxmax()), min_at=_ts_s(s.idxmin()))
    before = col.loc[(col.index > start - pd.Timedelta(minutes=window_min)) & (col.index <= start)].dropna()
    day = col.loc[(col.index > start - pd.Timedelta("1D")) & (col.index <= ctx.now - pd.Timedelta("1D"))].dropna()
    out["previous_window_mean"] = _r(before.mean()) if len(before) else None
    out["same_window_24h_earlier_mean"] = _r(day.mean()) if len(day) else None
    if not len(day) and ctx.stream_start is not None and start - pd.Timedelta("1D") < ctx.stream_start:
        out["note"] = f"the stream starts at {_t(ctx.stream_start)}; nothing earlier was streamed"
    step = max(1, math.ceil(window_min / points))
    ds = s.resample(f"{step}min", origin=start + ONE_MIN, label="left").mean().dropna()
    out["points_every_min"] = step
    out["points"] = [[_t(t), _r(v)] for t, v in ds.items()]
    return out


def query_metric(ctx: Context, series_id: str, window_minutes: int = 180, points: int = 48) -> dict:
    if series_id not in ctx.catalog.unit_of:
        return {"error": f"unknown series {series_id!r}" + _close(series_id, ctx.catalog.unit_of)}
    return {"source": STREAM, **_summary(ctx, series_id, window_minutes, points)}


def related_entities(ctx: Context, entity: str, hops: int = 1) -> dict:
    ent, err = ctx.catalog.resolve_entity(entity)
    if err:
        return {"error": err}
    topo, levels, seen, frontier = ctx.catalog.topology, [], {ent}, {ent}
    for _ in range(hops):
        frontier = {n for e in frontier for n in topo.neighbours.get(e, ())} - seen
        seen |= frontier
        levels.append(sorted(frontier))
    out = []
    for h, level in enumerate(levels, 1):
        for n in level:
            row = {"entity": n, "hops": h, "series": ctx.catalog.series_of.get(n, [])}
            if h == 1:
                row["relation"] = ctx.catalog.relation.get(frozenset({ent, n}), "related")
            out.append(row)
    return {"source": CONFIG, "entity": ent, "series": ctx.catalog.series_of.get(ent, []), "related": out}


def _pod(ctx: Context, pod: str) -> str | None:
    pod = pod.split(":", 1)[1] if pod.startswith("pod:") else pod
    return pod if pod in ctx.catalog.pods else None


def recent_logs(ctx: Context, pod: str, window_minutes: int = 30, level: str | None = None, contains: str | None = None,
                limit: int = 20) -> dict:
    name = _pod(ctx, pod)
    if name is None:
        return {"error": f"unknown pod {pod!r}" + _close(pod, ctx.catalog.pods)}
    df = _evidence(ctx).logs(pod=name, start=ctx.now - pd.Timedelta(minutes=window_minutes), end=ctx.now, level=level,
                             contains=contains, limit=None)
    templates = Counter(re.sub(r"\d+(?:\.\d+)?", "#", m) for m in df.message)
    return {"source": ctx.evidence_source, "pod": name, "window": f"{_t(ctx.now - pd.Timedelta(minutes=window_minutes))} .. {_t(ctx.now)}",
            "lines": int(len(df)), "by_level": dict(Counter(df.level)),
            "top_templates": [{"template": t[:200], "n": n} for t, n in templates.most_common(8)],
            "latest": [{"ts": _ts_s(r.ts), "level": r.level, "logger": r.logger, "message": r.message[:300]}
                       for r in df.tail(limit).itertuples()]}


def k8s_events(ctx: Context, pod: str | None = None, window_minutes: int = 120, reason: str | None = None) -> dict:
    name = None
    if pod is not None:
        name = _pod(ctx, pod)
        if name is None:
            return {"error": f"unknown pod {pod!r}" + _close(pod, ctx.catalog.pods)}
    df = _evidence(ctx).k8s_events(pod=name, start=ctx.now - pd.Timedelta(minutes=window_minutes), end=ctx.now, reason=reason)
    shown = 40
    return {"source": ctx.evidence_source, "pod": name, "window": f"{_t(ctx.now - pd.Timedelta(minutes=window_minutes))} .. {_t(ctx.now)}",
            "events": int(len(df)), "by_reason": dict(Counter(df.reason)), "by_type": dict(Counter(df.type)),
            "latest": [{"ts": _ts_s(r.ts), "object": r.object, "type": r.type, "reason": r.reason, "message": r.message[:200],
                        "count": int(r.count)} for r in df.tail(shown).itertuples()],
            "not_shown": max(0, len(df) - shown)}


def recent_changes(ctx: Context, window_hours: int = 24, target: str | None = None) -> dict:
    found = _evidence(ctx).changes(start=ctx.now - pd.Timedelta(hours=window_hours), end=ctx.now, target=target)
    return {"source": ctx.evidence_source, "window": f"{_t(ctx.now - pd.Timedelta(hours=window_hours))} .. {_t(ctx.now)}",
            "changes": len(found), "latest": found[-30:]}


def _evidence(ctx: Context):
    return ctx.evidence if ctx.evidence is not None else ctx.history


def _minutes(ctx: Context, sid: str, start, end) -> pd.Series:
    """One series' minute readings in [start, end): from the history store before the stream started,
    from the stream after. Never the data directory's readings for the streamed period: those are not
    what was sent."""
    split = end if ctx.stream_start is None else min(end, ctx.stream_start)
    parts = []
    if start < split:
        early = ctx.history.query([sid], start=start, end=split)
        if sid in early.columns:
            parts.append(early[sid])
    if ctx.stream_start is not None and sid in ctx.metrics.columns:
        col = ctx.metrics[sid]
        parts.append(col[(col.index >= max(start, ctx.stream_start)) & (col.index < end)])
    if not parts:
        return pd.Series(dtype=float)
    s = pd.concat(parts).astype(float)
    return s[~s.index.duplicated(keep="last")].sort_index()


def baseline_comparison(ctx: Context, series_id: str, window_minutes: int = 60) -> dict:
    """The seasonal detector's view of one series: its 5-minute means against the same time of day on
    comparable earlier days, with the detector's own settings and history mode for the series."""
    if series_id not in ctx.catalog.unit_of:
        return {"error": f"unknown series {series_id!r}" + _close(series_id, ctx.catalog.unit_of)}
    rule, mode, abs_floor = ctx.catalog.routes.get(series_id, (None, None, 0.0))
    seasonal = rule == "seasonal"
    mode = mode or router.DEFAULT_HISTORY
    last = (ctx.now + ONE_MIN).floor(BUCKET) - BUCKET                    # the newest bucket whose 5 minutes were all read
    n = max(1, window_minutes // 5)
    first = last - (n - 1) * BUCKET
    grid = pd.date_range(first - pd.Timedelta(days=LOOKBACK_DAYS), last, freq=BUCKET)
    minutes = _minutes(ctx, series_id, grid[0], last + BUCKET)
    means = minutes.resample(BUCKET).mean().reindex(grid) if len(minutes) else pd.Series(np.nan, index=grid)
    means.name = series_id
    scores = seasonal_scores(means, **{**SCORE, "history": mode, "abs_floor": abs_floor}).set_index("ts").loc[first:last]
    rows = [{"bucket": _t(t), "value": _r(r.value), "expected": _r(r.expected), "wobble": _r(r.wobble),
             "z": _r(r.z), "hot": bool(r.hot), "baseline_days": int(r.baseline_days)} for t, r in scores.iterrows()]
    out = {"source": STREAM_THEN_HISTORY, "series_id": series_id, "unit": ctx.catalog.unit_of.get(series_id, ""),
           "entity": ctx.catalog.topology.entity(series_id), "detector": rule or "not routed", "history_mode": mode,
           "hot_when_abs_z_above": SCORE["threshold"], "buckets": rows,
           "latest_explanation": next((e for e in scores.explanation[::-1] if isinstance(e, str)), None),
           "how_to_read": "value: the mean of 5 minutes; expected: the median of the same 5 minutes of the day on comparable "
                          "earlier days; wobble: their robust spread (at least 5% of expected); z = (value - expected) / wobble. "
                          "Normal daily and weekly patterns are in `expected`."}
    if not seasonal:
        out["note"] = (f"this series is not scored by the seasonal baseline (its rule: {rule or 'none'}); the comparison uses "
                       f"the default history mode" + ("; it is a cumulative counter, so compare increases, not values"
                                                      if series_id.split("|")[0].endswith("_total") else ""))
    return out


def dag_runs(ctx: Context, dag_id: str | None = None, window_hours: int = 24) -> dict:
    if dag_id is not None and dag_id not in ctx.catalog.dags:
        return {"error": f"unknown DAG {dag_id!r}; there are {sorted(ctx.catalog.dags)}"}
    start = ctx.now - pd.Timedelta(hours=window_hours)
    runs: dict = {}
    for m in ctx.runs:
        if dag_id is not None and m["dag_id"] != dag_id:
            continue
        run = runs.setdefault((m["dag_id"], m["run_id"]), {"dag_id": m["dag_id"], "run_id": m["run_id"]})
        if m["event"] == "start":
            run.update(run_type=m["run_type"], scheduled_for=m["scheduled_for"], started=m["ts"])
        else:
            run.update(finished=m["ts"], state=m["state"])
    rows = []
    for run in runs.values():
        last = run.get("finished") or run.get("started")
        if last is None or utc(last) <= start:
            continue
        row = {"dag_id": run["dag_id"], "run_id": run["run_id"], "run_type": run.get("run_type"),
               "scheduled_for": _ts_s(run["scheduled_for"]) if run.get("scheduled_for") else None,
               "started": _ts_s(run["started"]) if run.get("started") else None,
               "finished": _ts_s(run["finished"]) if run.get("finished") else None,
               "state": run.get("state", "running")}
        if run.get("started") and run.get("finished"):
            row["duration_min"] = _r((utc(run["finished"]) - utc(run["started"])) / ONE_MIN)
        if run.get("started") and run.get("scheduled_for"):
            row["lateness_min"] = _r((utc(run["started"]) - utc(run["scheduled_for"])) / ONE_MIN)
        rows.append(row)
    rows.sort(key=lambda r: r["started"] or r["finished"])
    started_for = {(r["dag_id"], r["scheduled_for"]) for r in rows if r["scheduled_for"]}
    expected = ctx.history.expected_runs(start=start, end=ctx.now + pd.Timedelta("1ns"))
    due = []
    for e in expected.itertuples() if len(expected) else ():
        if dag_id is not None and e.dag_id != dag_id:
            continue
        t = utc(e.scheduled_for)
        if ctx.stream_start is not None and t < ctx.stream_start:
            status = "before the stream started"
        else:
            status = "started" if (e.dag_id, _ts_s(t)) in started_for else "no start streamed yet"
        due.append({"dag_id": e.dag_id, "scheduled_for": _ts_s(t), "minutes_ago": _r((ctx.now - t) / ONE_MIN),
                    "sla_minutes": int(e.sla_minutes), "status": status})
    return {"source": STREAM, "schedules": {k: v for k, v in ctx.catalog.dags.items() if dag_id in (None, k)},
            "window": f"{_t(start)} .. {_t(ctx.now)}", "runs": rows[-40:], "runs_not_shown": max(0, len(rows) - 40),
            "scheduled": due[-40:]}


def kafka_lag(ctx: Context, group: str = "orders-etl", window_minutes: int = 120) -> dict:
    ent = f"consumer_group:{group.split(':', 1)[-1]}"
    if ent not in ctx.catalog.series_of:
        groups = sorted(e.split(":", 1)[1] for e in ctx.catalog.series_of if e.startswith("consumer_group:"))
        return {"error": f"no lag series for {group!r}; groups with series: {groups}"}
    topo = ctx.catalog.topology
    topics = sorted(n for n in topo.neighbours.get(ent, ()) if n.startswith("topic:"))
    members = sorted(n.split(":", 1)[1] for n in topo.neighbours.get(ent, ()) if n.startswith("pod:"))
    series = ctx.catalog.series_of.get(ent, []) + [s for t in topics for s in ctx.catalog.series_of.get(t, [])]
    last5 = ctx.metrics.loc[ctx.metrics.index > ctx.now - pd.Timedelta("5min")]
    alive = {p: int(last5[f"container_cpu_usage_cores|{p}"].notna().sum()) if f"container_cpu_usage_cores|{p}" in last5 else 0
             for p in members}
    return {"source": STREAM, "group": ent, "topics": topics,
            "member_pods_minutes_with_reading_in_last_5": alive,
            "series": [_summary(ctx, s, window_minutes, 24) for s in series]}


TOOLS = [
    Tool("get_page", "The page under investigation as it was when it paged: its alerts (target series or entity, "
         "detector, score, direction, plain-language explanation), grouped by target, and the entities involved.",
         _schema({}), get_page),
    Tool("query_metric", "Summary statistics and a downsampled curve of one metric series over the last "
         "`window_minutes` before the page, from the readings as they were streamed: first/last/min/max/mean/median, "
         "slope per hour, minutes without a reading, the mean of the window before and of the same window 24 hours "
         "earlier. Series ids look like '<metric>|<entity>'.",
         _schema({"series_id": _str("e.g. 'kafka_consumergroup_lag|orders-etl'"),
                  "window_minutes": _int("how far back to look", 10, 10080, 180),
                  "points": _int("how many downsampled points to return", 6, 120, 48)}, ["series_id"]), query_metric),
    Tool("baseline_comparison", "How one series compares with what the seasonal detector expects for the same time of "
         "day on comparable earlier days, from the detector's own 5-minute means and history up to the page: per "
         "5-minute bucket the value, the expected value, the wobble (robust spread) and z. Use it to tell whether a "
         "reading, traffic included, is unusual for this time of day or just the normal daily pattern.",
         _schema({"series_id": _str("e.g. 'kafka_messages_in_per_sec|orders'"),
                  "window_minutes": _int("how many minutes of the latest 5-minute buckets to show", 5, 360, 60)},
                 ["series_id"]), baseline_comparison),
    Tool("related_entities", "Entities connected to one entity in the platform's entity graph (pods, consumer "
         "groups, topics, clusters, DAGs, Spark and Flink apps), with how they are related and the series each one has.",
         _schema({"entity": _str("'kind:name' (e.g. 'pod:kafka-1'), a series id, or a bare name"),
                  "hops": _int("1 = direct neighbours, 2 = also their neighbours", 1, 2, 1)}, ["entity"]), related_entities),
    Tool("recent_logs", "Log lines of one pod before the page: counts by level, the most frequent message templates "
         "and the latest lines.",
         _schema({"pod": _str("pod name, e.g. 'orders-etl-consumer-0'"),
                  "window_minutes": _int("how far back to look", 1, 360, 30),
                  "level": _str("only this level", ["DEBUG", "INFO", "WARN", "ERROR"]),
                  "contains": _str("only lines containing this text"),
                  "limit": _int("how many of the latest lines to return", 0, 60, 20)}, ["pod"]), recent_logs),
    Tool("k8s_events", "Kubernetes events (scheduling, pulls, starts, kills, probe failures, OOM kills, back-off) "
         "before the page, for one pod or for all.",
         _schema({"pod": _str("pod name; leave out for all pods"),
                  "window_minutes": _int("how far back to look", 1, 1440, 120),
                  "reason": _str("only this reason, e.g. 'OOMKilled', 'BackOff', 'Unhealthy'")}), k8s_events),
    Tool("recent_changes", "Deploys, configuration changes, scaling, DAG pauses and maintenance before the page.",
         _schema({"window_hours": _int("how far back to look", 1, 168, 24),
                  "target": _str("only changes whose target contains this text")}), recent_changes),
    Tool("dag_runs", "Airflow DAG runs as they were streamed (start, finish, state, duration, lateness) and the "
         "scheduled run times in the window, each marked started or not, with the DAG's schedule and SLA.",
         _schema({"dag_id": _str("one DAG; leave out for all"),
                  "window_hours": _int("how far back to look", 1, 168, 24)}), dag_runs),
    Tool("kafka_lag", "A consumer group's lag and consume rate, the in-rate of the topics it reads, and whether its "
         "member pods are sending readings, from the streamed readings.",
         _schema({"group": _str("consumer group name, e.g. 'orders-etl'"),
                  "window_minutes": _int("how far back to look", 10, 1440, 120)}), kafka_lag),
]
TOOL_BY_NAME = {t.name: t for t in TOOLS}


def definitions() -> list:
    """The tools as name / description / JSON schema, the form both the Messages API and MCP take."""
    return [{"name": t.name, "description": t.description, "input_schema": t.input_schema} for t in TOOLS]


def check_args(schema: dict, args) -> str | None:
    """None if `args` fit the schema (the subset of JSON Schema used above), else why not."""
    if not isinstance(args, dict):
        return "arguments must be an object"
    props = schema["properties"]
    for k in schema["required"]:
        if k not in args:
            return f"missing required argument {k!r}"
    for k, v in args.items():
        if k not in props:
            return f"unknown argument {k!r}; arguments are {sorted(props)}"
        p = props[k]
        if p["type"] == "integer":
            if isinstance(v, float) and v.is_integer():
                v = int(v)
            if not isinstance(v, int) or isinstance(v, bool):
                return f"{k} must be an integer"
            if not p["minimum"] <= v <= p["maximum"]:
                return f"{k} must be between {p['minimum']} and {p['maximum']}"
        elif p["type"] == "string":
            if not isinstance(v, str):
                return f"{k} must be a string"
            if "enum" in p and v not in p["enum"]:
                return f"{k} must be one of {p['enum']}"
    return None


def call(ctx: Context, name: str, args: dict | None = None) -> tuple:
    """Run one tool. Returns (result as a dict, is_error). Never raises for a bad call: the error goes
    back to the caller as the result, so an agent can correct itself."""
    args = dict(args or {})
    ctx.calls.append((name, args))
    tool = TOOL_BY_NAME.get(name)
    if tool is None:
        return {"error": f"unknown tool {name!r}; tools are {sorted(TOOL_BY_NAME)}"}, True
    why = check_args(tool.input_schema, args)
    if why:
        return {"error": why}, True
    args = {k: int(v) if isinstance(v, float) and tool.input_schema["properties"][k]["type"] == "integer" else v
            for k, v in args.items()}
    result = tool.fn(ctx, **args)
    if "error" in result:
        return result, True
    if len(dumps(result)) > MAX_RESULT_CHARS:
        return {"error": f"result too large ({len(dumps(result))} characters); ask for a shorter window or fewer points"}, True
    return result, False


def dumps(result: dict) -> str:
    return json.dumps(result, separators=(",", ":"), default=str)
