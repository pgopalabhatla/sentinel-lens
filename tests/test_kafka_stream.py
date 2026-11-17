"""The streaming engine behind a real broker: publisher -> `telemetry` -> consumer -> `alerts`, `pages`.

    docker compose up -d
    pytest -m kafka            # about 3 minutes; skipped when no broker answers on 127.0.0.1:19092

1. Equivalence   The TEST week (frozen data/), published at max speed and consumed by a separate
                 consumer process, gives exactly the in-process engine's alerts and page events.
2. Crash         The consumer is killed three times on the way through the week (kill -9 at an
                 arbitrary moment; between sending a message's outputs and committing its offset;
                 between two outputs of one message) and restarted each time. The topics end up
                 identical to the uninterrupted run, with no record twice.
3. Duplicates    The publisher sends part of the stream a second time; nothing is written twice.

As in tests/test_stream_test_week.py: the test week was evaluated once, no labels are read and
nothing is tuned here. If a test fails, the difference is the finding.

Every test uses its own topics and consumer group (a random prefix), deleted afterwards, so the
tests do not touch the `telemetry`, `alerts` and `pages` topics of the live demo.
"""
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("confluent_kafka")

from sentinel_stream import AlertEvent, PageEvent, StreamEngine, replay, wire  # noqa: E402
from sentinel_stream import broker  # noqa: E402
from sentinel_stream.consumer import EXIT_INJECTED_CRASH  # noqa: E402
from sentinel_stream.publisher import publish  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
BOOTSTRAP = broker.DEFAULT_BOOTSTRAP
pytestmark = [pytest.mark.kafka,
              pytest.mark.skipif(not (DATA / "metrics").exists(), reason="dataset not generated"),
              pytest.mark.filterwarnings("ignore::RuntimeWarning")]

FROZEN_RESULT = {"alerts": 1445, "page_outs": 29}        # docs/log.md: the test week, live
KILL_AFTER_ALERTS = 400                                  # kill -9 once this many alerts are in the topic
CRASH_BEFORE_COMMIT = "2026-10-22 17:49"                 # a minute in which a page is sent
CRASH_MID_OUTPUTS = "2026-10-24 12:00"                   # the first later message with two or more outputs


def utc(x) -> pd.Timestamp:
    return pd.Timestamp(x) if pd.Timestamp(x).tzinfo else pd.Timestamp(x, tz="UTC")


# ------------------------------------------------------------------------------------- fixtures
@pytest.fixture(scope="module")
def kafka():
    """A broker, and a prefix for this test run's topics and groups. Everything with the prefix is
    deleted at the end."""
    if not broker.reachable(BOOTSTRAP):
        pytest.skip(f"no Kafka broker at {BOOTSTRAP} (start one: docker compose up -d)")
    prefix = f"test-{uuid.uuid4().hex[:8]}-"
    yield prefix
    from confluent_kafka.admin import AdminClient
    topics = AdminClient(broker.client_config(BOOTSTRAP)).list_topics(timeout=10).topics
    broker.delete_topics(BOOTSTRAP, [t for t in topics if t.startswith(prefix)])


@pytest.fixture(scope="module")
def week():
    from scripts.run_test_week import HISTORY_START, SPLITS
    from sentinel_sim.replay import DataSource

    lo, hi = SPLITS["test"]
    return {"ds": DataSource(DATA).at(hi), "lo": lo, "hi": hi, "history_start": HISTORY_START}


@pytest.fixture(scope="module")
def in_process(week):
    """The in-process engine on the test week: its alerts and page events, as wire records."""
    engine = StreamEngine.warm_start(week["ds"], week["lo"], history_start=week["history_start"])
    events = list(replay(engine, week["ds"], week["lo"], week["hi"], history_start=week["history_start"]))
    return ([wire.dumps(wire.alert_record(e)) for e in events if isinstance(e, AlertEvent)],
            [wire.dumps(wire.page_record(e)) for e in events if isinstance(e, PageEvent)])


@pytest.fixture(scope="module")
def telemetry(kafka, week):
    """The test week on a telemetry topic, published once at max speed and shared by the tests."""
    topic = kafka + "telemetry"
    publish(week["ds"], week["lo"], week["hi"], speed=None, bootstrap=BOOTSTRAP, topic=topic,
            history_start=week["history_start"])
    return topic


class ConsumerRun:
    """One consumer (its own output topics and group) that can be started, killed and restarted."""

    def __init__(self, name: str, telemetry: str, start: str, history_start: str, logs: Path, data: Path = DATA):
        self.telemetry, self.alerts, self.pages, self.group = telemetry, f"{name}-alerts", f"{name}-pages", name
        self.args = ["--data", str(data), "--start", start, "--history-start", history_start, "--bootstrap", BOOTSTRAP,
                     "--telemetry-topic", telemetry, "--alerts-topic", self.alerts, "--pages-topic", self.pages,
                     "--group", name, "--exit-at-watermark"]
        self.log_path = logs / f"{name}.log"
        broker.ensure_topics(BOOTSTRAP, [self.alerts, self.pages])

    def start(self, *extra) -> subprocess.Popen:
        with open(self.log_path, "a") as log:
            return subprocess.Popen([sys.executable, "-W", "ignore", "-m", "sentinel_stream.consumer", *self.args, *extra],
                                    cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT)}, stdout=log, stderr=log)

    def finish(self, *extra, timeout=600) -> int:
        return self.start(*extra).wait(timeout=timeout)

    def log(self) -> str:
        return self.log_path.read_text()

    def outputs(self) -> tuple:
        return ([wire.dumps(r) for r in broker.read_topic(BOOTSTRAP, self.alerts)],
                [wire.dumps(r) for r in broker.read_topic(BOOTSTRAP, self.pages)])

    def committed(self) -> int:
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer(broker.client_config(BOOTSTRAP, **{"group.id": self.group, "enable.auto.commit": False}))
        try:
            return c.committed([TopicPartition(self.telemetry, broker.PARTITION)], timeout=10)[0].offset
        finally:
            c.close()

    def telemetry_message(self, offset: int) -> dict:
        return broker.read_topic(BOOTSTRAP, self.telemetry, offset, offset + 1)[0]

    def emitted_after(self, t) -> int:
        """How many records in `alerts` and `pages` were emitted after event time `t`."""
        return sum(utc(wire.loads(raw)["emitted_at"]) > t for topic in self.outputs() for raw in topic)


@pytest.fixture(scope="module")
def uninterrupted(kafka, telemetry, week, tmp_path_factory):
    run = ConsumerRun(kafka + "plain", telemetry, week["lo"], week["history_start"], tmp_path_factory.mktemp("logs"))
    assert run.finish() == 0, run.log()
    return run


# ---------------------------------------------------------------------------------- 1. equivalence
def test_through_kafka_the_test_week_gives_the_in_process_alerts_and_page_events(uninterrupted, in_process):
    alerts, pages = uninterrupted.outputs()
    want_alerts, want_pages = in_process
    assert alerts == want_alerts
    assert pages == want_pages
    assert len(alerts) == FROZEN_RESULT["alerts"]
    assert sum(wire.loads(p)["kind"] == "paged" for p in pages) == FROZEN_RESULT["page_outs"]


def test_every_offset_is_committed_and_message_time_stamps_are_event_time(uninterrupted, telemetry, week):
    end = broker.end_offset(BOOTSTRAP, telemetry)
    assert uninterrupted.committed() == end
    assert uninterrupted.telemetry_message(end - 1) == wire.watermark_message(week["hi"])
    # the broker finds a message by event time, which is what a restart relies on
    from confluent_kafka import Consumer
    c = Consumer(broker.client_config(BOOTSTRAP, **{"group.id": uninterrupted.group + "-probe"}))
    try:
        offset = broker.first_offset_at_or_after(c, telemetry, "2026-10-22 17:49")
        assert broker.first_offset_at_or_after(c, telemetry, "2027-01-01") is None
    finally:
        c.close()
    message = uninterrupted.telemetry_message(offset)
    assert wire.event_time(message) == utc("2026-10-22 17:49")
    assert wire.event_time(uninterrupted.telemetry_message(offset - 1)) < utc("2026-10-22 17:49")


# --------------------------------------------------------------------------------------- 2. crash
def test_killed_three_times_mid_week_the_topics_equal_the_uninterrupted_run(kafka, telemetry, week, uninterrupted,
                                                                             tmp_path):
    run = ConsumerRun(kafka + "killed", telemetry, week["lo"], week["history_start"], tmp_path)

    # (1) kill -9 at an arbitrary moment: whatever it was doing, it gets no chance to finish it
    process = run.start()
    deadline = time.monotonic() + 300
    while broker.end_offset(BOOTSTRAP, run.alerts) < KILL_AFTER_ALERTS:
        assert process.poll() is None and time.monotonic() < deadline, run.log()
        time.sleep(0.05)
    process.kill()
    process.wait()
    killed_at = run.committed()
    assert 0 < killed_at < broker.end_offset(BOOTSTRAP, telemetry)
    assert utc(week["lo"]) < wire.event_time(run.telemetry_message(killed_at)) < utc(CRASH_BEFORE_COMMIT)

    # (2) restart; die between sending a message's outputs and committing its offset
    assert run.finish("--crash-at", "before-commit", CRASH_BEFORE_COMMIT) == EXIT_INJECTED_CRASH, run.log()
    k = run.committed()
    assert k > killed_at
    crashed_on, last_committed = run.telemetry_message(k), run.telemetry_message(k - 1)
    assert wire.event_time(crashed_on) >= utc(CRASH_BEFORE_COMMIT)
    # its outputs are in the topics although its offset is not committed: the at-least-once case
    assert run.emitted_after(wire.event_time(last_committed)) >= 1

    # (3) restart; die after the first of a message's outputs
    assert run.finish("--crash-at", "mid-outputs", CRASH_MID_OUTPUTS) == EXIT_INJECTED_CRASH, run.log()
    k2 = run.committed()
    assert k2 > k and wire.event_time(run.telemetry_message(k2)) >= utc(CRASH_MID_OUTPUTS)
    assert run.emitted_after(wire.event_time(run.telemetry_message(k2 - 1))) == 1

    # (4) restart and run to the end of the week
    assert run.finish() == 0, run.log()
    assert run.committed() == broker.end_offset(BOOTSTRAP, telemetry)

    alerts, pages = run.outputs()
    want_alerts, want_pages = uninterrupted.outputs()
    assert len(set(alerts)) == len(alerts) and len(set(pages)) == len(pages)      # no record twice
    assert alerts == want_alerts
    assert pages == want_pages
    assert run.log().count("engine warm-started at") == 4
    assert "outputs not sent again" in run.log()


# ---------------------------------------------------------------------------------- 3. duplicates
def test_a_stream_sent_twice_by_the_publisher_is_processed_once(kafka, tmp_path):
    """Twelve hours of the train split. The publisher sends the first eight hours, is 'restarted' two
    hours back and sends from there to the end, so two hours are on the topic twice. The consumer
    skips every message at or before the last one it processed."""
    from sentinel_sim.replay import DataSource
    from sentinel_stream.consumer import recover
    from sentinel_stream.publisher import messages

    start, again_from, stop, end, history_start = ("2026-10-07 00:00", "2026-10-07 06:00", "2026-10-07 08:00",
                                                   "2026-10-07 12:00", "2026-09-28")
    ds = DataSource(DATA).at(end)
    topic = kafka + "twice-telemetry"
    first = publish(ds, start, stop, speed=None, bootstrap=BOOTSTRAP, topic=topic, history_start=history_start)
    second = publish(ds, again_from, end, speed=None, bootstrap=BOOTSTRAP, topic=topic, history_start=history_start)
    assert broker.end_offset(BOOTSTRAP, topic) == first + second
    sent_twice = sum(wire.event_time(m) < utc(stop) for m in messages(ds, again_from, end, history_start))
    assert sent_twice > 120

    run = ConsumerRun(kafka + "twice", topic, start, history_start, tmp_path)
    assert run.finish() == 0, run.log()            # stops at the first watermark (08:00)
    assert run.committed() == first
    assert run.finish() == 0, run.log()            # restarted: reads on through the second copy to the end
    assert run.committed() == first + second
    assert f"{sent_twice} skipped" in run.log()

    processor = recover(ds, start, history_start=history_start)
    want = [out for m in messages(ds, start, end, history_start) for out in processor.handle(m)]
    assert len(want) > 20
    alerts, pages = run.outputs()
    assert len(set(alerts)) == len(alerts) and len(set(pages)) == len(pages)
    assert (alerts, pages) == ([wire.dumps(r) for t, r in want if t == "alerts"],
                               [wire.dumps(r) for t, r in want if t == "pages"])
