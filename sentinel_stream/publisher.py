"""Publisher: plays a data directory into the `telemetry` topic, as if the platform were running now.

    python -m sentinel_stream.publisher --data data_live --start 2026-10-26 --speed 60

It stands in for what would be a metrics scraper and an Airflow listener in a real setup. From
`--start` it sends, in event-time order, one `metrics` message per minute (every series' reading) and
a `dag_run` message when a run starts or finishes, and at the end one `watermark` message saying
"everything before this time has been sent" (wire.py has the formats).

Speed. `--speed 60` sends one simulated minute per real second (a week takes 2.8 hours); `--speed 1`
is real time; `--speed max` does not wait at all (tests). A message stamped t is sent when
(t - start) / speed seconds have passed since the publisher started, so run events fall between the
minutes where they belong.

Injection. The publisher is also where failures are injected into the live stream: it reads
commands from the topic `control`, applies the active scenarios to the readings it sends from the
injection time on, and records what it injected on `answer_key` (injector.py, scenarios_live.py).
A command is either a named scenario or a list of primitive effects; effects that would break a
series' physical limits are rejected and reported on `answer_key`. With no command the stream is the
data as it is.

Order is the one thing that must hold: the topic has one partition and one publisher writes to it
with an idempotent producer (no reordering or duplicates from retries), so the consumer reads the
messages in exactly the order `messages()` yields them, which is the order the in-process replay
(`sentinel_stream.replay`) feeds the engine.
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Iterator

from sentinel_stream import wire
from sentinel_stream.events import utc
from sentinel_stream.replay import run_events


def messages(ds, start, end=None, history_start=None) -> Iterator[dict]:
    """Every telemetry message stamped start <= t < end, in stream order, then the watermark at
    `end`. Default end: the end of the data."""
    start = utc(start)
    end = ds.now if end is None else min(utc(end), ds.now)
    events = run_events(ds, start, end, history_start)
    i = 0
    for ts, row in ds.at(end).stream(start, end):
        while i < len(events) and events[i].time <= ts:
            yield wire.run_message(events[i])
            i += 1
        yield wire.metrics_message(ts, row)
    for event in events[i:]:
        yield wire.run_message(event)
    yield wire.watermark_message(end)


def publish(ds, start, end=None, speed=None, bootstrap=None, topic="telemetry", history_start=None,
            log=None, control=None, answer_key=None, scenarios=None) -> int:
    """Send `messages(ds, start, end)` to `topic`. speed=None: as fast as possible. Returns the
    number of messages sent, after every one of them has been acknowledged by the broker.

    With `control` and `answer_key` (topic names) the publisher also takes injection commands
    (injector.py): it applies the active scenarios to each message before sending it and writes
    what it injected to `answer_key`. Commands already on `control` when the publisher starts are
    used only if they carry a time `at` that is not before `start` (a schedule loaded in advance);
    commands without a time count only if they arrive while it runs. With no command, the messages
    sent are exactly `messages(ds, start, end)`.
    """
    from confluent_kafka import Consumer, Producer, TopicPartition

    from sentinel_stream import broker

    bootstrap = bootstrap or broker.DEFAULT_BOOTSTRAP
    log = log or (lambda line: None)
    start = utc(start)
    end_ts = ds.now if end is None else min(utc(end), ds.now)
    failed = []
    producer = Producer(broker.client_config(bootstrap, **{"enable.idempotence": True, "compression.type": "lz4"}))

    def delivered(err, msg):
        if err is not None:
            failed.append(err)

    injector = reader = None
    if control or answer_key:
        if not (control and answer_key):
            raise ValueError("injection needs both a control topic and an answer-key topic")
        from sentinel_stream.injector import Injector, published_record
        from sentinel_stream.scenarios_live import SCENARIOS

        broker.ensure_topics(bootstrap, [topic, control, answer_key])
        injector = Injector.for_data(ds, scenarios or SCENARIOS)

        def take(cmd, scheduled_only=False):
            if scheduled_only and (cmd.get("at") is None or utc(cmd["at"]) < start):
                log(f"control: ignored (sent before this publisher started, not scheduled for its stream): {cmd}")
                return
            try:
                for r in injector.command(cmd):
                    log(f"control: effect rejected ({r['reason']}): {r['effect']}")
                log(f"control: accepted {cmd}")
            except ValueError as e:              # nothing of it can be carried out: say so where the caller can read it
                log(f"control: refused {cmd}: {e}")
                record = {"event": "rejected", "id": str(cmd.get("id", "")), "reasons": [str(e)]}
                producer.produce(answer_key, value=wire.dumps(record), key=record["id"], on_delivery=delivered)

        seen = broker.end_offset(bootstrap, control)
        for cmd in broker.read_topic(bootstrap, control, end=seen):
            take(cmd, scheduled_only=True)
        reader = Consumer(broker.client_config(bootstrap, **{"group.id": f"publisher-{start.value}-{id(producer)}",
                                                             "enable.auto.commit": False}))
        reader.assign([TopicPartition(control, broker.PARTITION, seen)])
    else:
        broker.ensure_topics(bootstrap, [topic])

    def check():
        if failed:
            raise RuntimeError(f"the broker rejected a message: {failed[0]}")

    began = time.monotonic()
    sent, hour = 0, None
    try:
        for message in messages(ds, start, end, history_start):
            ts = wire.event_time(message)
            if speed is not None:
                wait = (ts - start).total_seconds() / speed - (time.monotonic() - began)
                if wait > 0:
                    producer.flush()                    # nothing waits in the buffer while we sleep
                    time.sleep(wait)
            if injector is not None:
                while (cmd := reader.poll(0)) is not None:
                    if not cmd.error():
                        take(wire.loads(cmd.value()))
                message, records = injector.apply(message)
                for record in records:
                    producer.produce(answer_key, value=wire.dumps(record), key=record["id"], on_delivery=delivered)
                    log(f"answer key: {record}")
                if records:
                    producer.flush()                    # the answer key is durable before the readings it describes
                if message is None:                     # a run that does not happen while its DAG is paused
                    continue
            while True:
                try:
                    producer.produce(topic, value=wire.dumps(message), key=message["type"], timestamp=wire.millis(ts),
                                     on_delivery=delivered)
                    break
                except BufferError:                     # local queue full (only at max speed): let it drain
                    producer.poll(0.1)
            producer.poll(0)
            sent += 1
            check()
            if ts.floor("h") != hour:
                hour = ts.floor("h")
                log(f"published up to {ts:%Y-%m-%d %H:%M} ({sent} messages)")
        if injector is not None:
            producer.produce(answer_key, value=wire.dumps(published_record(start, end_ts)), key="published",
                             on_delivery=delivered)
        producer.flush()
        check()
        return sent
    finally:
        if reader is not None:
            reader.close()


def main(argv=None) -> int:
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker

    ap = argparse.ArgumentParser(description="Publish a data directory to the telemetry topic.")
    ap.add_argument("--data", required=True, help="data directory (e.g. data_live)")
    ap.add_argument("--start", required=True, help="first event time to send (UTC), e.g. 2026-10-26")
    ap.add_argument("--end", default=None, help="stop before this event time (default: the end of the data)")
    ap.add_argument("--speed", default="60",
                    help="simulated seconds per real second: 60 = one simulated minute per second, "
                         "1 = real time, max = no waiting (default 60)")
    ap.add_argument("--bootstrap", default=broker.DEFAULT_BOOTSTRAP)
    ap.add_argument("--topic", default="telemetry")
    ap.add_argument("--history-start", default=None, help="ignore DAG runs that started before this time")
    ap.add_argument("--control-topic", default="control", help="where inject / stop commands arrive (injector.py)")
    ap.add_argument("--answer-key-topic", default="answer_key", help="where the publisher records what it injected")
    ap.add_argument("--no-injection", action="store_true", help="do not read commands; send the data as it is")
    args = ap.parse_args(argv)
    speed = None if args.speed == "max" else float(args.speed)
    if speed is not None and speed <= 0:
        raise SystemExit("--speed must be positive, or 'max'")

    def log(line):
        print(line, file=sys.stderr, flush=True)

    inject = not args.no_injection
    sent = publish(DataSource(args.data), args.start, args.end, speed, args.bootstrap, args.topic,
                   args.history_start, log, control=args.control_topic if inject else None,
                   answer_key=args.answer_key_topic if inject else None)
    log(f"done: {sent} messages on {args.topic!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
