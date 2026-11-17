"""The consumer and the engine must never see the answer.

The injection side has two topics of its own (sentinel_stream/injector.py): the publisher takes its
commands from one and writes what it injected to the other. If the detection side could read either,
a "detection" would prove nothing. Two checks:

1. Static (default run). No module the consumer process loads mentions those topics or imports the
   injection side, and the consumer has no setting through which a topic other than its three could
   be given.
2. Through the broker (marker `kafka`). With both injection topics present and filled, every Kafka
   call the consumer makes is recorded: the partition it is assigned is `telemetry` only, and it
   reads from, looks up, commits to and writes to nothing but telemetry, alerts and pages.
"""
import dataclasses
import re
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INJECTION_SIDE = {"sentinel_stream.injector", "sentinel_stream.scenarios_live", "sentinel_stream.live_score",
                  "sentinel_stream.publisher"}
FORBIDDEN = re.compile(r"answer[_\- ]?key|\bcontrol\b", re.IGNORECASE)


def consumer_side_modules() -> dict:
    """{module name: source file} of every sentinel_* module loaded once the consumer has started
    (its imports at the top and the ones inside `run`), found by a fresh Python process."""
    code = ("import sys, sentinel_stream.consumer, sentinel_sim.replay\n"
            "try:\n    import sentinel_stream.broker\nexcept ImportError:\n    pass\n"
            "print('\\n'.join(f'{n}\\t{m.__file__}' for n, m in sorted(sys.modules.items()) "
            "if n.startswith('sentinel_') and getattr(m, '__file__', None)))")
    out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, check=True).stdout
    return dict(line.split("\t") for line in out.splitlines())


def test_the_names_the_test_looks_for_are_the_injection_topics():
    from sentinel_stream import injector
    assert FORBIDDEN.fullmatch(injector.CONTROL_TOPIC) and FORBIDDEN.fullmatch(injector.ANSWER_KEY_TOPIC)
    assert FORBIDDEN.search((ROOT / "sentinel_stream/publisher.py").read_text())        # the check can find them


def test_no_consumer_side_module_mentions_the_injection_topics_or_imports_the_injection_side():
    modules = consumer_side_modules()
    assert {"sentinel_stream.consumer", "sentinel_stream.engine", "sentinel_stream.pages", "sentinel_stream.wire",
            "sentinel_lens.correlator", "sentinel_sim.replay"} <= set(modules)
    assert not INJECTION_SIDE & set(modules), sorted(INJECTION_SIDE & set(modules))
    files = set(modules.values()) | {str(ROOT / "sentinel_stream/broker.py")}
    for path in sorted(files):
        hits = [f"{Path(path).name}:{n}: {line.strip()}" for n, line in enumerate(Path(path).read_text().splitlines(), 1)
                if FORBIDDEN.search(line)]
        assert not hits, "\n".join(hits)


def test_the_consumer_can_only_be_given_its_three_topics():
    from sentinel_stream import consumer
    fields = {f.name for f in dataclasses.fields(consumer.Config)}
    assert {"telemetry", "alerts", "pages"} <= fields
    assert not [f for f in fields if FORBIDDEN.search(f)]
    with pytest.raises(SystemExit):
        consumer.main(["--data", "x", "--start", "2026-10-26", "--answer-key-topic", "answer_key"])
    with pytest.raises(SystemExit):
        consumer.main(["--data", "x", "--start", "2026-10-26", "--control-topic", "control"])


# ------------------------------------------------------------------------------ through the broker
@pytest.mark.kafka
@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_through_the_broker_the_consumer_is_assigned_telemetry_only(monkeypatch):
    confluent_kafka = pytest.importorskip("confluent_kafka")
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker, consumer, injector
    from sentinel_stream.publisher import publish
    from sentinel_stream.scenarios_live import REHEARSAL

    if not (ROOT / "data" / "metrics").exists():
        pytest.skip("dataset not generated")
    if not broker.reachable(broker.DEFAULT_BOOTSTRAP):
        pytest.skip(f"no Kafka broker at {broker.DEFAULT_BOOTSTRAP} (start one: docker compose up -d)")
    bootstrap = broker.DEFAULT_BOOTSTRAP
    prefix = f"test-{uuid.uuid4().hex[:8]}-"
    topic = {k: prefix + k for k in ("telemetry", "control", "answer_key", "alerts", "pages")}
    start, end = "2026-10-07 05:00", "2026-10-07 07:00"
    try:
        # the injection side does its work first: both of its topics exist and hold records
        injector.send(bootstrap, topic["control"], [{"cmd": "inject", "id": "iso-1", "scenario": REHEARSAL.name,
                                                     "target": REHEARSAL.targets[0], "params": {"duration_min": 20.0},
                                                     "at": "2026-10-07T06:00:00+00:00"}])
        publish(DataSource(ROOT / "data"), start, end, bootstrap=bootstrap, topic=topic["telemetry"], history_start="2026-09-28",
                control=topic["control"], answer_key=topic["answer_key"], scenarios={REHEARSAL.name: REHEARSAL})
        assert len(broker.read_topic(bootstrap, topic["answer_key"])) == 3 and len(broker.read_topic(bootstrap, topic["control"])) == 1

        calls = []                                              # (group.id, method, topic) for every call that names a topic

        class SpyConsumer(confluent_kafka.Consumer):
            def __init__(self, conf):
                super().__init__(conf)
                self._group = conf.get("group.id")

            def _note(self, method, partitions):
                calls.extend((self._group, method, p.topic) for p in partitions)

            def assign(self, partitions):
                self._note("assign", partitions)
                return super().assign(partitions)

            def subscribe(self, topics, *args, **kwargs):
                calls.extend((self._group, "subscribe", t) for t in topics)
                return super().subscribe(topics, *args, **kwargs)

            def committed(self, partitions, *args, **kwargs):
                self._note("committed", partitions)
                return super().committed(partitions, *args, **kwargs)

            def offsets_for_times(self, partitions, *args, **kwargs):
                self._note("offsets_for_times", partitions)
                return super().offsets_for_times(partitions, *args, **kwargs)

            def get_watermark_offsets(self, partition, *args, **kwargs):
                self._note("get_watermark_offsets", [partition])
                return super().get_watermark_offsets(partition, *args, **kwargs)

            def commit(self, *args, **kwargs):
                self._note("commit", kwargs.get("offsets") or [])
                return super().commit(*args, **kwargs)

        class SpyProducer(confluent_kafka.Producer):
            def produce(self, topic, *args, **kwargs):
                calls.append(("producer", "produce", topic))
                return super().produce(topic, *args, **kwargs)

        monkeypatch.setattr(confluent_kafka, "Consumer", SpyConsumer)       # what consumer.run imports
        monkeypatch.setattr(confluent_kafka, "Producer", SpyProducer)
        monkeypatch.setattr(broker, "Consumer", SpyConsumer)                # what broker.read_topic uses
        group = prefix + "consumer"
        processor = consumer.run(consumer.Config(str(ROOT / "data"), start, bootstrap, topic["telemetry"], topic["alerts"],
                                                 topic["pages"], group, "2026-09-28", exit_at_watermark=True,
                                                 log=lambda line: None))
        assert processor.processed > 120

        assigned = {t for g, method, t in calls if g == group and method in ("assign", "subscribe")}
        assert assigned == {topic["telemetry"]}
        assert not [c for c in calls if c[1] == "subscribe"]
        assert {t for _, _, t in calls} == {topic["telemetry"], topic["alerts"], topic["pages"]}
        assert {t for g, method, t in calls if method == "produce"} == {topic["alerts"], topic["pages"]}
        assert {t for g, method, t in calls if method == "commit"} == {topic["telemetry"]}
    finally:
        monkeypatch.undo()
        from confluent_kafka.admin import AdminClient
        topics = AdminClient(broker.client_config(bootstrap)).list_topics(timeout=10).topics
        broker.delete_topics(bootstrap, [t for t in topics if t.startswith(prefix)])
