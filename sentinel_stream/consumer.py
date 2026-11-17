"""Consumer: reads `telemetry`, runs the streaming engine, writes `alerts` and `pages`.

    python -m sentinel_stream.consumer --data data_live --start 2026-10-26

The engine is the in-process one, unchanged (engine.py). This module only adds what a process
between two Kafka topics needs: where to start, when to commit, and what to do after a crash.

One message at a time
---------------------
    1. read one message from `telemetry`
    2. skip it if it is at or before the last processed position (see wire.position)
    3. give it to the engine; the engine returns the alerts and page events it causes
    4. send them to `alerts` and `pages` and WAIT until the broker has acknowledged every one
    5. commit the message's offset

The commit comes last, so a crash can never lose a message: the worst case is that a message is
read again (at-least-once delivery). Reading it again must then not send its outputs again.

Where the state lives
---------------------
The consumer keeps nothing on disk. After a crash, three things bring it back:

    committed offset   which telemetry messages are fully done (steps 1-5)
    `alerts`, `pages`  everything the consumer has already said
    the data dir       the telemetry itself, as history (it stands for Prometheus and Airflow, which
                       a real setup would query: they keep what the scraper and the scheduler saw)

Restart
-------
    a. Read `alerts` and `pages` to their ends.
    b. The clock to resume at, `c`, is the event time of the last committed message (on the very
       first start: `--start`). Everything stamped before `c` is done and its outputs are in the
       topics, because the stream is in event-time order.
    c. Warm-start the engine at `c`: detector state from the data dir's history before `c`, open
       pages rebuilt from the alerts in `alerts` that were emitted before `c`.
    d. Go back to the first telemetry message stamped `c` or later and continue from there.

Step d re-reads a few messages that were already processed: the last committed one, any other with
the same time stamp, and whatever was processed but not committed when the consumer died. The engine
is deterministic, so it says exactly what it said the first time. Those outputs are already in the
topics, so each output the engine produces is first compared with the next record already there
(emitted at `c` or later): if they are equal it is not sent again; if they differ the consumer stops
with an error instead of writing a second, different history. Only when the already-written records
are used up does the consumer send again. So no alert and no page event is written twice, whether
the crash came before the outputs were sent, between two of them, or between the last one and the
commit.

Two more sources of duplicates, handled elsewhere: a retried send (the producer is idempotent, the
broker drops the second copy), and the publisher sending a message twice, e.g. after being restarted
(step 2 skips it).

Limits: one consumer. Two running at once would both write. Outputs are compared in order, which is
right only because every topic has one partition. And a restart needs the history store to hold the
telemetry up to `c` (true for the data dir; for Prometheus, true once the scrape has landed).
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
from collections import deque
from dataclasses import dataclass, field

from sentinel_stream import wire
from sentinel_stream.engine import StreamEngine
from sentinel_stream.events import utc

EXIT_INJECTED_CRASH = 77
CRASH_STAGES = ("mid-outputs", "before-commit")


class ReplayDiverged(RuntimeError):
    """After a restart the engine said something different from what is already in the topics."""


class Processor:
    """Everything the consumer does that is not Kafka I/O: skip, feed the engine, and hold back the
    outputs that are already written."""

    def __init__(self, engine: StreamEngine, position: tuple, said_alerts=(), said_pages=()):
        self.engine = engine
        self.position = position                      # of the last message processed (wire.position)
        self._said = {"alerts": deque(wire.dumps(r) for r in said_alerts),
                      "pages": deque(wire.dumps(r) for r in said_pages)}
        self.processed = self.skipped = self.not_resent = 0

    def handle(self, message: dict) -> list:
        """One telemetry message -> [(topic, record)] to send, in order. Empty if the message was
        skipped or caused nothing new."""
        pos = wire.position(message)
        if pos <= self.position:
            self.skipped += 1
            return []
        kind, ts = message["type"], wire.event_time(message)
        if kind == wire.METRICS:
            events = self.engine.on_minute(ts, message["values"])
        elif kind == wire.DAG_RUN:
            events = self.engine.on_run_event(wire.run_event(message))
        elif kind == wire.WATERMARK:
            events = self.engine.advance(ts)
        else:
            raise ValueError(f"unknown telemetry message type {kind!r}")
        self.position = pos
        self.processed += 1
        return [out for out in map(wire.output_record, events) if not self._already_said(*out)]

    def _already_said(self, topic: str, record: dict) -> bool:
        said = self._said[topic]
        if not said:
            return False
        raw = wire.dumps(record)
        if raw != said[0]:
            raise ReplayDiverged(f"{topic}: the engine now says\n  {raw.decode()}\nbut the topic already holds\n  "
                                 f"{said[0].decode()}\nRefusing to write a second history.")
        said.popleft()
        self.not_resent += 1
        return True

    def pending_said(self) -> int:
        """Already-written outputs the engine has not said again yet (0 once the replay has caught up)."""
        return sum(len(q) for q in self._said.values())


def recover(ds, start, alerts_log=(), pages_log=(), last_committed: dict | None = None,
            history_start=None) -> Processor:
    """A Processor that continues where the previous one stopped (module docstring, "Restart").

    ds              replay API over the data dir; only history before the resume clock is read
    start           where the stream starts, used when nothing has been committed yet
    alerts_log      every record in `alerts`, in order;  pages_log: every record in `pages`
    last_committed  the last telemetry message whose offset was committed, or None
    """
    clock = utc(start) if last_committed is None else wire.event_time(last_committed)
    before = [r for r in alerts_log if utc(r["emitted_at"]) < clock]
    engine = StreamEngine.warm_start(ds, clock, history_start=history_start,
                                     alert_log=[wire.alert_event(r) for r in before])
    return Processor(engine, wire.position_before(clock),
                     said_alerts=[r for r in alerts_log if utc(r["emitted_at"]) >= clock],
                     said_pages=[r for r in pages_log if utc(r["emitted_at"]) >= clock])


# ------------------------------------------------------------------------------------- Kafka
@dataclass
class Config:
    data: str
    start: str
    bootstrap: str = ""
    telemetry: str = "telemetry"
    alerts: str = "alerts"
    pages: str = "pages"
    group: str = "sentinel-consumer"
    history_start: str | None = None
    exit_at_watermark: bool = False       # stop at the next watermark not committed before (tests, one-off replays)
    crash_at: tuple | None = None         # fault injection for the tests: (stage, event time)
    log: object = field(default=lambda line: print(line, file=sys.stderr, flush=True))


def run(config: Config, stop=lambda: False) -> Processor:
    from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker

    log = config.log
    bootstrap = config.bootstrap or broker.DEFAULT_BOOTSTRAP
    topic_of = {"alerts": config.alerts, "pages": config.pages}
    broker.ensure_topics(bootstrap, [config.telemetry, config.alerts, config.pages])

    consumer = Consumer(broker.client_config(bootstrap, **{
        "group.id": config.group,
        "enable.auto.commit": False,          # offsets are committed by hand, after the outputs
        "auto.offset.reset": "error"}))       # never guess a position: it is always set below
    # idempotent producer: the broker drops a retried send, and sends stay in order
    producer = Producer(broker.client_config(bootstrap, **{"enable.idempotence": True}))
    failed = []

    def delivered(err, msg):
        if err is not None:
            failed.append(err)

    try:
        # ---- restart steps a-d (also the first start: then nothing is committed and the logs are empty)
        tp = TopicPartition(config.telemetry, broker.PARTITION)
        committed = consumer.committed([tp], timeout=10)[0].offset
        last = None
        first_new = max(committed or 0, 0)    # offsets below this were committed before this start
        if committed is not None and committed > 0:
            found = broker.read_topic(bootstrap, config.telemetry, committed - 1, committed)
            last = found[0] if found else None
        alerts_log = broker.read_topic(bootstrap, config.alerts)
        pages_log = broker.read_topic(bootstrap, config.pages)
        processor = recover(DataSource(config.data), config.start, alerts_log, pages_log, last, config.history_start)
        clock = processor.position[0]
        # The end is read BEFORE the lookup. If the lookup then finds nothing stamped `clock` or later,
        # everything below that end is older than `clock`, so starting there loses nothing even when
        # the publisher is writing at this very moment (read the other way round, messages that
        # arrive between the two calls would be jumped over).
        end = consumer.get_watermark_offsets(tp, timeout=10, cached=False)[1]
        offset = broker.first_offset_at_or_after(consumer, config.telemetry, clock)
        if offset is None:
            offset = end
        consumer.assign([TopicPartition(config.telemetry, broker.PARTITION, offset)])
        log(f"engine warm-started at {clock:%Y-%m-%d %H:%M:%S} from {config.data}; "
            f"{len(alerts_log)} alerts and {len(pages_log)} page events already written, "
            f"{processor.pending_said()} of them at or after that time; reading {config.telemetry!r} from offset {offset}"
            + ("" if last is None else f" (committed: {committed})"))

        hour = None
        while not stop():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            message = wire.loads(msg.value())                                   # 1
            records = processor.handle(message)                                 # 2, 3
            crash = _crash_stage(config.crash_at, message, records)
            for n, (kind, record) in enumerate(records):                        # 4
                producer.produce(topic_of[kind], value=wire.dumps(record), key=_key(kind, record),
                                 timestamp=wire.millis(record["emitted_at"]), on_delivery=delivered)
                if crash == "mid-outputs" and n == 0:
                    producer.flush()
                    os._exit(EXIT_INJECTED_CRASH)
                if kind == "pages" and record["kind"] == "paged":
                    a = record["alert"]
                    print(f"PAGE {record['emitted_at'][:19]} page {record['page']} ({record['n_alerts']} alerts) "
                          f"{a['target']}: {a['explanation']}", flush=True)
            if records:
                producer.flush()              # returns when every send above is acknowledged or failed
                if failed:
                    raise RuntimeError(f"the broker rejected an output: {failed[0]}")
            if crash == "before-commit":
                os._exit(EXIT_INJECTED_CRASH)
            consumer.commit(offsets=[TopicPartition(config.telemetry, broker.PARTITION, msg.offset() + 1)],
                            asynchronous=False)                                 # 5
            ts = wire.event_time(message)
            if ts.floor("h") != hour:
                hour = ts.floor("h")
                log(f"at {ts:%Y-%m-%d %H:%M}: {processor.processed} messages processed, {processor.skipped} skipped, "
                    f"{processor.not_resent} outputs not sent again")
            if config.exit_at_watermark and message["type"] == wire.WATERMARK and msg.offset() >= first_new:
                break
        log(f"stopped: {processor.processed} messages processed, {processor.skipped} skipped, "
            f"{processor.not_resent} outputs not sent again")
        return processor
    finally:
        producer.flush()
        consumer.close()


def _key(kind: str, record: dict) -> str:
    return record["alert"]["target"] if kind == "alerts" else str(record["page"])


def _crash_stage(crash_at, message: dict, records: list) -> str | None:
    """Fault injection: die at the first message stamped at or after the given time that has enough
    outputs for the stage to mean something (2 for mid-outputs, 1 for before-commit)."""
    if crash_at is None:
        return None
    stage, when = crash_at
    enough = len(records) >= (2 if stage == "mid-outputs" else 1)
    return stage if enough and wire.event_time(message) >= utc(when) else None


def main(argv=None) -> int:
    from sentinel_stream import broker

    ap = argparse.ArgumentParser(description="Consume telemetry, run the engine, publish alerts and pages.")
    ap.add_argument("--data", required=True, help="data directory whose history warms the engine up (e.g. data_live)")
    ap.add_argument("--start", required=True, help="event time the stream starts at (UTC), e.g. 2026-10-26")
    ap.add_argument("--bootstrap", default=broker.DEFAULT_BOOTSTRAP)
    ap.add_argument("--telemetry-topic", default="telemetry")
    ap.add_argument("--alerts-topic", default="alerts")
    ap.add_argument("--pages-topic", default="pages")
    ap.add_argument("--group", default="sentinel-consumer", help="consumer group that holds the committed offset")
    ap.add_argument("--history-start", default=None, help="do not read history before this time")
    ap.add_argument("--exit-at-watermark", action="store_true", help="stop at the publisher's next watermark (one that was not committed before this start)")
    ap.add_argument("--crash-at", nargs=2, metavar=("STAGE", "TIME"), default=None,
                    help=f"fault injection for the tests: exit hard at STAGE ({' | '.join(CRASH_STAGES)}) "
                         "of the first message at or after TIME that has outputs")
    args = ap.parse_args(argv)
    if args.crash_at and args.crash_at[0] not in CRASH_STAGES:
        raise SystemExit(f"--crash-at STAGE must be one of {CRASH_STAGES}")

    stopping = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopping.append(1))
    run(Config(args.data, args.start, args.bootstrap, args.telemetry_topic, args.alerts_topic, args.pages_topic,
               args.group, args.history_start, args.exit_at_watermark,
               tuple(args.crash_at) if args.crash_at else None), stop=lambda: bool(stopping))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
