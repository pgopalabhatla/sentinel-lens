"""The Kafka consumer's logic without a broker: wire format, skipping, crash and restart.

`sentinel_stream.consumer.run` is a loop around three Kafka calls (poll, produce, commit). Everything
that decides WHAT is written lives in `Processor` and `recover`, which know nothing about Kafka. Here
the three topics and the committed offset are plain Python lists (`Log`) and `consume()` below is the
same loop as `consumer.run`, so the rules can be tested in the default run, in seconds, and a crash
can be placed at an exact message and stage. tests/test_kafka_stream.py repeats the important ones
through the real broker.

Data: twelve hours of the TRAIN split (Oct 7 00:00-12:00), which hold a Spark job that fails, pod
restarts, seasonal alerts and six pages. No labels are read and nothing is tuned.
"""
import math
from pathlib import Path

import pandas as pd
import pytest

from sentinel_lens.alerts import Alert
from sentinel_stream import AlertEvent, PageEvent, RunFinished, RunStarted, StreamEngine, replay, wire
from sentinel_stream.consumer import Processor, ReplayDiverged, recover
from sentinel_stream.publisher import messages

DATA = Path(__file__).resolve().parents[1] / "data"
needs_data = pytest.mark.skipif(not (DATA / "metrics").exists(), reason="dataset not generated")
pytestmark = pytest.mark.filterwarnings("ignore::RuntimeWarning")

HISTORY_START = "2026-09-28"
START, END = "2026-10-07 00:00", "2026-10-07 12:00"


def ts(x) -> pd.Timestamp:
    return pd.Timestamp(x, tz="UTC")


def through_json(record: dict) -> dict:
    """What a record looks like after a trip through Kafka."""
    return wire.loads(wire.dumps(record))


# ------------------------------------------------------------------------------------ wire format
def an_alert(**kw) -> Alert:
    base = dict(time=ts("2026-10-07 02:15"), target="jvm_heap_used_ratio|daily-orders-exec-2", detector="seasonal",
                score=0.8123456789012345, direction="up", explanation="heap at 91%, expected 55% +/- 8%",
                measure=13.700000000000001, line=6.0)
    return Alert(**{**base, **kw})


def test_alert_and_page_events_survive_the_trip_unchanged():
    alert = AlertEvent(ts("2026-10-07 02:19"), an_alert(), demoted=True)
    page = PageEvent("merged", ts("2026-10-07 02:19"), 3, "2026-10-07 02:15:00+00:00|pod:x", an_alert(), 7, absorbed=(4, 6))
    assert wire.alert_event(through_json(wire.alert_record(alert))) == alert
    assert wire.page_event(through_json(wire.page_record(page))) == page
    assert wire.output_record(alert)[0] == "alerts" and wire.output_record(page)[0] == "pages"


def test_floats_come_back_bit_for_bit_also_an_infinite_measure():
    for measure in (0.1 + 0.2, 1e-320, 123456789.12345679, math.inf):
        back = wire.alert_event(through_json(wire.alert_record(AlertEvent(ts("2026-10-07"), an_alert(measure=measure), False))))
        assert back.alert.measure == measure
    values = {"a|x": 0.1 + 0.2, "b|x": float("nan"), "c|x": 3.0}
    message = through_json(wire.metrics_message(ts("2026-10-07 00:01"), pd.Series(values)))
    assert message == {"type": "metrics", "ts": "2026-10-07T00:01:00+00:00", "values": {"a|x": 0.1 + 0.2, "c|x": 3.0}}


def test_run_events_survive_the_trip_unchanged():
    started = RunStarted(ts("2026-10-07 02:00:13"), "daily_orders", "scheduled__2026-10-07", "scheduled", ts("2026-10-07 02:00"))
    finished = RunFinished(ts("2026-10-07 02:41:07"), "daily_orders", "scheduled__2026-10-07", "failed")
    for event in (started, finished):
        assert wire.run_event(through_json(wire.run_message(event))) == event


def test_position_orders_messages_with_the_same_time_stamp():
    """At one time stamp: watermark, run starts, run finishes, then the minute of metrics."""
    t = ts("2026-10-07 02:00")
    at_t = [wire.watermark_message(t),
            wire.run_message(RunStarted(t, "a", "r1", "scheduled", t)),
            wire.run_message(RunStarted(t, "b", "r1", "scheduled", t)),
            wire.run_message(RunFinished(t, "a", "r0", "success")),
            wire.metrics_message(t, {"m|x": 1.0})]
    later = wire.run_message(RunStarted(t + pd.Timedelta("1s"), "a", "r2", "manual", t))
    positions = [wire.position(m) for m in at_t + [later]]
    assert positions == sorted(positions) and len(set(positions)) == len(positions)
    assert wire.position_before(t) < positions[0]
    assert wire.position(wire.metrics_message(t - pd.Timedelta("1min"), {})) < wire.position_before(t)


# --------------------------------------------------------------------------- an in-memory "broker"
class Log:
    """The three topics and the consumer group's committed offset, as lists."""

    def __init__(self, telemetry):
        self.telemetry = [through_json(m) for m in telemetry]
        self.alerts, self.pages = [], []
        self.committed = None

    def write(self, records):
        for topic, record in records:
            getattr(self, topic).append(through_json(record))

    def outputs(self):
        return [wire.dumps(r) for r in self.alerts], [wire.dumps(r) for r in self.pages]


def consume(log: Log, ds, crash=None) -> Processor:
    """`consumer.run` over a Log. crash = (index of a telemetry message, stage): stop dead at that
    stage of that message, as a killed process would.

        before-outputs   processed by the engine, nothing sent
        mid-outputs      the first of its outputs sent, the rest not
        before-commit    all outputs sent, offset not committed
    """
    last = log.telemetry[log.committed - 1] if log.committed else None
    processor = recover(ds, START, log.alerts, log.pages, last, HISTORY_START)
    clock = processor.position[0]
    i = next((n for n, m in enumerate(log.telemetry) if wire.event_time(m) >= clock), len(log.telemetry))
    while i < len(log.telemetry):
        records = processor.handle(log.telemetry[i])
        if crash and crash[0] == i:
            log.write({"before-outputs": [], "mid-outputs": records[:1], "before-commit": records}[crash[1]])
            return processor
        log.write(records)
        log.committed = i + 1
        i += 1
    return processor


@pytest.fixture(scope="module")
def ds():
    from sentinel_sim.replay import DataSource
    return DataSource(DATA).at(END)


@pytest.fixture(scope="module")
def telemetry(ds):
    return list(messages(ds, START, END, HISTORY_START))


@pytest.fixture(scope="module")
def uninterrupted(ds, telemetry):
    """One run with no crash, and for every telemetry message the number of outputs it caused."""
    log = Log(telemetry)
    processor = recover(ds, START, history_start=HISTORY_START)
    n_outputs = []
    for i, m in enumerate(log.telemetry):
        records = processor.handle(m)
        n_outputs.append(len(records))
        log.write(records)
        log.committed = i + 1
    return log, n_outputs


# ------------------------------------------------------------------------- publisher and equivalence
@needs_data
def test_publisher_sends_the_stream_in_position_order_and_ends_with_a_watermark(telemetry):
    positions = [wire.position(m) for m in telemetry]
    assert positions == sorted(positions) and len(set(positions)) == len(positions)
    kinds = [m["type"] for m in telemetry]
    assert kinds.count("metrics") == 12 * 60 and kinds.count("dag_run") >= 16 and kinds[-1] == "watermark"
    assert telemetry[-1] == {"type": "watermark", "ts": "2026-10-07T12:00:00+00:00"}
    assert all(len(m["values"]) > 100 for m in telemetry if m["type"] == "metrics")


@needs_data
def test_through_the_wire_format_the_engine_says_what_it_says_in_process(ds, uninterrupted):
    engine = StreamEngine.warm_start(ds, START, history_start=HISTORY_START)
    events = list(replay(engine, ds, START, END, history_start=HISTORY_START))
    want_alerts = [wire.dumps(wire.alert_record(e)) for e in events if isinstance(e, AlertEvent)]
    want_pages = [wire.dumps(wire.page_record(e)) for e in events if isinstance(e, PageEvent)]
    log, _ = uninterrupted
    assert log.outputs() == (want_alerts, want_pages)
    # the slice is worth testing on: several detectors, several pages, some of them paging
    assert len(want_alerts) >= 20 and len({e.alert.detector for e in events if isinstance(e, AlertEvent)}) >= 3
    assert sum(e.kind == "paged" for e in events if isinstance(e, PageEvent)) >= 3


# ------------------------------------------------------------------------------- crash and restart
@needs_data
def test_crashes_at_every_stage_leave_the_same_output_and_no_duplicates(ds, telemetry, uninterrupted):
    """Kill the consumer five times on the way: before the outputs of a message are sent, between
    two of them, between the last one and the commit (twice, once on the very first message with
    outputs), and at a message without outputs. Restart each time from what is in the 'topics'."""
    want, n_outputs = uninterrupted
    busy = [i for i, n in enumerate(n_outputs) if n >= 2]          # messages with at least 2 outputs
    quiet = next(i for i in range(busy[2], len(n_outputs)) if n_outputs[i] == 0)
    crashes = sorted([(busy[0], "before-commit"), (busy[1], "before-outputs"), (busy[len(busy) // 2], "mid-outputs"),
                      (quiet, "before-commit"), (busy[-1], "before-commit")])
    assert len({i for i, _ in crashes}) == 5

    log = Log(telemetry)
    for crash in crashes:
        consume(log, ds, crash)
        assert log.committed in (None, crash[0])                   # the crashed message was never committed
    final = consume(log, ds)
    assert log.committed == len(telemetry) and final.pending_said() == 0
    assert log.outputs() == want.outputs()
    for topic in log.outputs():
        assert len(set(topic)) == len(topic)                       # no record twice
    assert final.not_resent >= n_outputs[busy[-1]]                 # the last crash's outputs were held back


@needs_data
def test_messages_sent_twice_by_the_publisher_are_skipped(ds, telemetry, uninterrupted):
    """The publisher is restarted from an earlier time and sends 200 messages again."""
    want, _ = uninterrupted
    log = Log(telemetry[:300] + telemetry[100:])
    processor = consume(log, ds)
    assert processor.skipped == 200 and processor.processed == len(telemetry)
    assert log.outputs() == want.outputs()


@needs_data
def test_restart_after_the_end_writes_nothing_more(ds, telemetry, uninterrupted):
    want, _ = uninterrupted
    log = Log(telemetry)
    consume(log, ds)
    again = consume(log, ds)                                       # resumes at the watermark, the last message
    assert log.outputs() == want.outputs() and again.processed == 1


@needs_data
def test_a_restart_that_would_say_something_else_stops_instead_of_writing(ds, telemetry, uninterrupted):
    _, n_outputs = uninterrupted
    crash_at = next(i for i, n in enumerate(n_outputs) if n >= 2)
    log = Log(telemetry)
    consume(log, ds, (crash_at, "before-commit"))
    log.alerts[-1]["alert"]["score"] = 0.123                       # the topic no longer holds what the engine said
    before = log.outputs()
    with pytest.raises(ReplayDiverged, match="already holds"):
        consume(log, ds)
    assert log.outputs() == before
