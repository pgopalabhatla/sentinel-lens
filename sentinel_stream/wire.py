"""What is written to Kafka: the JSON form of the engine's inputs and outputs.

Topic `telemetry` (one partition), every message has a `type`:

    {"type": "metrics", "ts": ..., "values": {series_id: value, ...}}        one minute of readings;
                                                                             a series with no reading is left out
    {"type": "dag_run", "event": "start",  "ts": ..., "dag_id", "run_id", "run_type", "scheduled_for"}
    {"type": "dag_run", "event": "finish", "ts": ..., "dag_id", "run_id", "state"}
    {"type": "watermark", "ts": ...}       the publisher has sent everything stamped before `ts`

Topic `alerts`: one AlertEvent per message.  Topic `pages`: one PageEvent per message.

`ts` is EVENT time: when the thing happened in the monitored platform, not when the message was
sent. The publisher also puts it in the Kafka message timestamp, so "the first message at or after
time T" can be looked up by the broker.

Position. Messages on `telemetry` are in one fixed order, the order `sentinel_stream.replay` feeds
the in-process engine: by event time, and at the same time a watermark first, then run starts, then
run finishes, then the minute of metrics. `position(message)` is that sort key. The consumer keeps the
position of the last message it processed and skips anything at or before it; event time alone is
not enough, because two messages can carry the same time (two runs ending in the same second, or a
run starting exactly on the minute).

Numbers go through `repr`, so a float read back is the same float: the engine fed from Kafka sees
bit-for-bit the values the in-process engine sees. `dumps` is canonical (sorted keys, no spaces), so
two equal records are equal as bytes.
"""
from __future__ import annotations

import json
import math

import pandas as pd

from sentinel_lens.alerts import Alert
from sentinel_stream.events import AlertEvent, PageEvent, RunFinished, RunStarted, utc

METRICS, DAG_RUN, WATERMARK = "metrics", "dag_run", "watermark"
_RANK = {WATERMARK: 0, "start": 1, "finish": 2, METRICS: 3}


def iso(ts) -> str:
    """UTC, ISO 8601, lossless (no rounding to seconds)."""
    return utc(ts).isoformat()


def millis(ts) -> int:
    """Event time as a Kafka message timestamp (milliseconds since 1970)."""
    return int(utc(ts).value // 1_000_000)


def dumps(record: dict) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":")).encode()


def loads(raw: bytes) -> dict:
    return json.loads(raw)


# ------------------------------------------------------------------------------- telemetry: write
def metrics_message(ts, values) -> dict:
    """`values`: a pandas Series or a dict {series_id: value}. Missing readings (NaN) are left out."""
    items = values.items()
    return {"type": METRICS, "ts": iso(ts),
            "values": {str(k): float(v) for k, v in items if v is not None and not math.isnan(v)}}


def run_message(event) -> dict:
    if isinstance(event, RunStarted):
        return {"type": DAG_RUN, "event": "start", "ts": iso(event.time), "dag_id": event.dag_id,
                "run_id": event.run_id, "run_type": event.run_type, "scheduled_for": iso(event.scheduled_for)}
    if isinstance(event, RunFinished):
        return {"type": DAG_RUN, "event": "finish", "ts": iso(event.time), "dag_id": event.dag_id,
                "run_id": event.run_id, "state": event.state}
    raise TypeError(f"not a run event: {event!r}")


def watermark_message(ts) -> dict:
    return {"type": WATERMARK, "ts": iso(ts)}


# -------------------------------------------------------------------------------- telemetry: read
def event_time(message: dict) -> pd.Timestamp:
    return utc(message["ts"])


def position(message: dict) -> tuple:
    """The message's place in the stream: (event time, kind, dag_id, run_id). See the module docstring."""
    kind = message["event"] if message["type"] == DAG_RUN else message["type"]
    return (event_time(message), _RANK[kind], message.get("dag_id", ""), message.get("run_id", ""))


def position_before(ts) -> tuple:
    """A position just before every message stamped `ts`: after it, messages stamped before `ts`
    count as processed and messages stamped `ts` or later do not."""
    return (utc(ts), -1, "", "")


def run_event(message: dict):
    if message["event"] == "start":
        return RunStarted(utc(message["ts"]), message["dag_id"], message["run_id"], message["run_type"],
                          utc(message["scheduled_for"]))
    if message["event"] == "finish":
        return RunFinished(utc(message["ts"]), message["dag_id"], message["run_id"], message["state"])
    raise ValueError(f"unknown dag_run event {message['event']!r}")


# ------------------------------------------------------------------------------------- outputs
def _alert(a: Alert) -> dict:
    return {"time": iso(a.time), "target": a.target, "detector": a.detector, "score": float(a.score),
            "direction": a.direction, "explanation": a.explanation, "measure": float(a.measure),
            "line": float(a.line)}


def _alert_from(d: dict) -> Alert:
    return Alert(time=utc(d["time"]), target=d["target"], detector=d["detector"], score=d["score"],
                 direction=d["direction"], explanation=d["explanation"], measure=d["measure"], line=d["line"])


def alert_record(e: AlertEvent) -> dict:
    return {"emitted_at": iso(e.emitted_at), "demoted": bool(e.demoted), "alert": _alert(e.alert)}


def alert_event(record: dict) -> AlertEvent:
    return AlertEvent(utc(record["emitted_at"]), _alert_from(record["alert"]), record["demoted"])


def page_record(e: PageEvent) -> dict:
    return {"kind": e.kind, "emitted_at": iso(e.emitted_at), "page": int(e.page), "page_id": e.page_id,
            "n_alerts": int(e.n_alerts), "absorbed": [int(k) for k in e.absorbed], "alert": _alert(e.alert)}


def page_event(record: dict) -> PageEvent:
    return PageEvent(record["kind"], utc(record["emitted_at"]), record["page"], record["page_id"],
                     _alert_from(record["alert"]), record["n_alerts"], tuple(record["absorbed"]))


def output_record(event) -> tuple:
    """An engine output -> ("alerts" | "pages", record)."""
    if isinstance(event, AlertEvent):
        return "alerts", alert_record(event)
    if isinstance(event, PageEvent):
        return "pages", page_record(event)
    raise TypeError(f"not an engine output: {event!r}")
