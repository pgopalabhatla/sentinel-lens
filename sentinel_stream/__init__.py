"""In-process streaming engine: the final detection system, one minute at a time.

See engine.py for how to use it and events.py for what goes in and comes out.

The same engine behind Kafka (README, "Run the live stream"):

    wire.py        the JSON messages on the topics, and a message's position in the stream
    publisher.py   data dir -> topic `telemetry`, at a chosen speed            (CLI)
    consumer.py    `telemetry` -> engine -> topics `alerts` and `pages`;        (CLI)
                   commit after output, restart without duplicates
    broker.py      the few Kafka calls they share

Only broker.py and the two CLIs' Kafka loops need the confluent-kafka package (requirements-kafka.txt);
importing this package does not.
"""
from sentinel_stream.engine import FROZEN_DEMOTED_GROUPS, StreamEngine
from sentinel_stream.events import AlertEvent, PageEvent, RunFinished, RunStarted
from sentinel_stream.replay import replay, run_events

__all__ = ["StreamEngine", "FROZEN_DEMOTED_GROUPS", "AlertEvent", "PageEvent", "RunStarted", "RunFinished",
           "replay", "run_events"]
