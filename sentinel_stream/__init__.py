"""In-process streaming engine: the final detection system, one minute at a time.

See engine.py for how to use it and events.py for what goes in and comes out.

The same engine behind Kafka (README, "Run the live stream"):

    wire.py        the JSON messages on the topics, and a message's position in the stream
    publisher.py   data dir -> topic `telemetry`, at a chosen speed            (CLI)
    consumer.py    `telemetry` -> engine -> topics `alerts` and `pages`;        (CLI)
                   commit after output, restart without duplicates
    broker.py      the few Kafka calls they share
    quiet_world.py the world they stream: the simulator with no incidents, of any length  (CLI)

Only broker.py and the two CLIs' Kafka loops need the confluent-kafka package (requirements-kafka.txt);
importing this package does not.

Failures injected into the live stream (README, "Inject a failure"; docs/live-protocol.md):

    scenarios_live.py   seven failure scenarios as lists of effects on named series
    injector.py         applies them to what the publisher sends, and records what was injected  (CLI)
    evidence_live.py    stream v2: the logs, Kubernetes events and changes each failure writes
    live_score.py       were the injections paged for, and how many pages were for nothing       (CLI)

The injection side is kept apart from the detection side: nothing the consumer loads imports these
modules or knows their topics (tests/test_live_isolation.py).

Stream version 2 (`publisher --stream-version 2`) also carries logs, Kubernetes events and changes as
minute batches (wire.py); version 1, the one live-week-1 was run on, does not.
"""
from sentinel_stream.engine import FROZEN_DEMOTED_GROUPS, StreamEngine
from sentinel_stream.events import AlertEvent, PageEvent, RunFinished, RunStarted
from sentinel_stream.replay import replay, run_events

__all__ = ["StreamEngine", "FROZEN_DEMOTED_GROUPS", "AlertEvent", "PageEvent", "RunStarted", "RunFinished",
           "replay", "run_events"]
