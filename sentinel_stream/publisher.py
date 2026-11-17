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
            log=None) -> int:
    """Send `messages(ds, start, end)` to `topic`. speed=None: as fast as possible. Returns the
    number of messages sent, after every one of them has been acknowledged by the broker."""
    from confluent_kafka import Producer

    from sentinel_stream import broker

    bootstrap = bootstrap or broker.DEFAULT_BOOTSTRAP
    broker.ensure_topics(bootstrap, [topic])
    failed = []
    producer = Producer(broker.client_config(bootstrap, **{"enable.idempotence": True, "compression.type": "lz4"}))

    def delivered(err, msg):
        if err is not None:
            failed.append(err)

    start = utc(start)
    began = time.monotonic()
    sent, hour = 0, None
    for message in messages(ds, start, end, history_start):
        ts = wire.event_time(message)
        if speed is not None:
            wait = (ts - start).total_seconds() / speed - (time.monotonic() - began)
            if wait > 0:
                producer.flush()                    # nothing waits in the buffer while we sleep
                time.sleep(wait)
        while True:
            try:
                producer.produce(topic, value=wire.dumps(message), key=message["type"], timestamp=wire.millis(ts),
                                 on_delivery=delivered)
                break
            except BufferError:                     # local queue full (only at max speed): let it drain
                producer.poll(0.1)
        producer.poll(0)
        sent += 1
        if failed:
            raise RuntimeError(f"the broker rejected a message: {failed[0]}")
        if log and ts.floor("h") != hour:
            hour = ts.floor("h")
            log(f"published up to {ts:%Y-%m-%d %H:%M} ({sent} messages)")
    producer.flush()
    if failed:
        raise RuntimeError(f"the broker rejected a message: {failed[0]}")
    return sent


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
    args = ap.parse_args(argv)
    speed = None if args.speed == "max" else float(args.speed)
    if speed is not None and speed <= 0:
        raise SystemExit("--speed must be positive, or 'max'")

    def log(line):
        print(line, file=sys.stderr, flush=True)

    sent = publish(DataSource(args.data), args.start, args.end, speed, args.bootstrap, args.topic,
                   args.history_start, log)
    log(f"done: {sent} messages on {args.topic!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
