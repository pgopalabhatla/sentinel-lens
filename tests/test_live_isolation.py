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

The triage side (context_tools, investigator, dispatcher, mcp_server) is held to the same two checks:
nothing it loads is on the injection side or names those topics, its settings cannot name them, and
through the broker the dispatcher reads only telemetry, alerts, pages and triage and writes only triage.
`triage_score` reads the answer key to evaluate the notes, so it is on the injection side.

The attacker (sentinel_stream/attacker.py) is held to the opposite: it is on the injection side and must
be blind to the detection side. Statically: its source and settings name none of alerts, pages, triage,
the answer key or the engine; it imports nothing from the detection or triage side; its settings and CLI
can name no topic but `control`. Through the broker (`kafka`): in live mode its only Kafka calls write
to `control`, and it creates no consumer at all, so it reads no topic.
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
                  "sentinel_stream.publisher", "sentinel_stream.triage_score", "sentinel_stream.evidence_live"}
FORBIDDEN = re.compile(r"answer[_\- ]?key|\bcontrol\b", re.IGNORECASE)
TRIAGE_SIDE = ["sentinel_stream.context_tools", "sentinel_stream.investigator", "sentinel_stream.dispatcher",
               "sentinel_stream.mcp_server"]


def loaded_modules(imports: list) -> dict:
    """{module name: source file} of every sentinel_* module loaded after importing `imports` (and the
    broker, which the Kafka code paths import inside functions), found by a fresh Python process."""
    code = (f"import sys, {', '.join(imports)}\n"
            "try:\n    import sentinel_stream.broker\nexcept ImportError:\n    pass\n"
            "print('\\n'.join(f'{n}\\t{m.__file__}' for n, m in sorted(sys.modules.items()) "
            "if n.startswith('sentinel_') and getattr(m, '__file__', None)))")
    out = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, check=True).stdout
    return dict(line.split("\t") for line in out.splitlines())


def consumer_side_modules() -> dict:
    """Every sentinel_* module loaded once the consumer has started (its imports at the top and the
    ones inside `run`)."""
    return loaded_modules(["sentinel_stream.consumer", "sentinel_sim.replay"])


def forbidden_lines(paths) -> list:
    return [f"{Path(p).name}:{n}: {line.strip()}" for p in sorted(paths)
            for n, line in enumerate(Path(p).read_text().splitlines(), 1) if FORBIDDEN.search(line)]


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


def test_no_triage_side_module_mentions_the_injection_topics_or_imports_the_injection_side():
    modules = loaded_modules(TRIAGE_SIDE + ["sentinel_sim.replay"])
    assert set(TRIAGE_SIDE) <= set(modules)
    assert not INJECTION_SIDE & set(modules), sorted(INJECTION_SIDE & set(modules))
    files = set(modules.values()) | {str(ROOT / "sentinel_stream/broker.py"), str(ROOT / "sentinel_stream/triage.yaml")}
    hits = forbidden_lines(files)
    assert not hits, "\n".join(hits)


def test_the_triage_side_can_only_be_given_its_own_topics():
    from sentinel_stream import dispatcher, mcp_server
    fields = {f.name for f in dataclasses.fields(dispatcher.Topics)}
    assert fields == {"telemetry", "alerts", "pages", "triage"}
    for main, args in ((dispatcher.main, ["--data", "x"]), (mcp_server.main, ["--data", "x", "--page", "1"])):
        for flag in ("--answer-key-topic", "--control-topic"):
            with pytest.raises(SystemExit):
                main(args + [flag, "x"])


# ------------------------------------------------------------------------------ through the broker
@pytest.mark.kafka
@pytest.mark.filterwarnings("ignore::RuntimeWarning")
@pytest.mark.parametrize("stream_version", [1, 2])
def test_through_the_broker_the_consumer_is_assigned_telemetry_only(monkeypatch, stream_version):
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
                control=topic["control"], answer_key=topic["answer_key"], scenarios={REHEARSAL.name: REHEARSAL},
                stream_version=stream_version)
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


@pytest.mark.kafka
@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_through_the_broker_the_dispatcher_touches_only_its_four_topics(monkeypatch, tmp_path):
    confluent_kafka = pytest.importorskip("confluent_kafka")
    from sentinel_sim.replay import DataSource
    from sentinel_stream import broker, dispatcher, injector, wire
    from tests import triage_world as world

    if not broker.reachable(broker.DEFAULT_BOOTSTRAP):
        pytest.skip(f"no Kafka broker at {broker.DEFAULT_BOOTSTRAP} (start one: docker compose up -d)")
    bootstrap = broker.DEFAULT_BOOTSTRAP
    prefix = f"test-{uuid.uuid4().hex[:8]}-"
    topic = {k: prefix + k for k in ("telemetry", "control", "answer_key", "alerts", "pages", "triage")}
    pages, alerts = world.pages_and_alerts()
    try:
        broker.ensure_topics(bootstrap, list(topic.values()))
        injector.send(bootstrap, topic["control"], [{"cmd": "stop", "id": "x"}])          # both injection topics hold records
        injector.send(bootstrap, topic["answer_key"], [{"event": "published", "start": "2026-01-05", "end": "2026-01-06"}])
        producer = confluent_kafka.Producer(broker.client_config(bootstrap))
        for name, records in (("telemetry", world.telemetry()), ("alerts", alerts), ("pages", pages)):
            for r in records:
                producer.produce(topic[name], value=wire.dumps(r))
        producer.flush()

        calls = []

        class SpyConsumer(confluent_kafka.Consumer):
            def assign(self, partitions):
                calls.extend(("assign", p.topic) for p in partitions)
                return super().assign(partitions)

            def subscribe(self, topics, *args, **kwargs):
                calls.extend(("subscribe", t) for t in topics)
                return super().subscribe(topics, *args, **kwargs)

            def get_watermark_offsets(self, partition, *args, **kwargs):
                calls.append(("get_watermark_offsets", partition.topic))
                return super().get_watermark_offsets(partition, *args, **kwargs)

        class SpyProducer(confluent_kafka.Producer):
            def produce(self, topic, *args, **kwargs):
                calls.append(("produce", topic))
                return super().produce(topic, *args, **kwargs)

        monkeypatch.setattr(confluent_kafka, "Producer", SpyProducer)
        monkeypatch.setattr(broker, "Consumer", SpyConsumer)
        topics = dispatcher.Topics(topic["telemetry"], topic["alerts"], topic["pages"], topic["triage"])
        seen = []

        def agent(ctx):
            seen.append(ctx.page)
            return {"page_id": ctx.page_id, "verdict": "unsure", "confidence": 0.0, "tool_calls": 0}

        d = dispatcher.Dispatcher(dispatcher.KafkaTopics(bootstrap, topics), DataSource(world.write_data_dir(tmp_path)),
                                  agent, topics, log=lambda line: None).run(until_end=True)
        assert sorted(seen) == [1, 2, 3] and d.investigated == 3
        touched = {t for _, t in calls}
        assert touched == {topic["telemetry"], topic["alerts"], topic["pages"], topic["triage"]}
        assert {t for method, t in calls if method == "produce"} == {topic["triage"]}
        assert not [c for c in calls if c[0] == "subscribe"]
        assert len(broker.read_topic(bootstrap, topic["triage"])) == 4                  # 3 notes and 1 link
    finally:
        monkeypatch.undo()
        from confluent_kafka.admin import AdminClient
        existing = AdminClient(broker.client_config(bootstrap)).list_topics(timeout=10).topics
        broker.delete_topics(bootstrap, [t for t in existing if t.startswith(prefix)])


# ------------------------------------------------------------------------------------ the attacker
ATTACKER_FORBIDDEN = re.compile(r"\balerts?\b|\bpages?\b|\btriage\b|answer[_\- ]?key|StreamEngine|warm_start|labels/", re.IGNORECASE)
DETECTION_AND_TRIAGE = {"sentinel_stream.consumer", "sentinel_stream.context_tools", "sentinel_stream.dispatcher",
                        "sentinel_stream.investigator", "sentinel_stream.live_score", "sentinel_stream.triage_score",
                        "sentinel_stream.mcp_server", "sentinel_stream.engine"}


def test_the_attacker_names_and_imports_nothing_of_the_detection_or_triage_side():
    import ast
    files = [ROOT / "sentinel_stream/attacker.py", ROOT / "sentinel_stream/attacker.yaml"]
    hits = [f"{p.name}:{n}: {line.strip()}" for p in files for n, line in enumerate(p.read_text().splitlines(), 1)
            if ATTACKER_FORBIDDEN.search(line)]
    assert not hits, "\n".join(hits)
    tree = ast.parse((ROOT / "sentinel_stream/attacker.py").read_text())
    imported = {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module} | \
               {f"{n.module}.{a.name}" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module for a in n.names} | \
               {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert not imported & DETECTION_AND_TRIAGE, sorted(imported & DETECTION_AND_TRIAGE)
    # nothing it loads (its imports, the broker, and the replay API it reads history through) is on those sides,
    # except the engine's code, which the sentinel_stream package itself imports; the attacker never builds one
    loaded = set(loaded_modules(["sentinel_stream.attacker", "sentinel_sim.replay"]))
    assert not loaded & (DETECTION_AND_TRIAGE - {"sentinel_stream.engine"}), sorted(loaded & DETECTION_AND_TRIAGE)


def test_the_attacker_can_only_be_given_the_control_topic():
    from sentinel_stream import attacker
    fields = {f.name for f in dataclasses.fields(attacker.Config)} | {f.name for f in dataclasses.fields(attacker.Budget)}
    assert not [f for f in fields if re.search(r"topic|alert|page|triage|answer", f)]
    for flag in ("--alerts-topic", "--pages-topic", "--triage-topic", "--answer-key-topic", "--telemetry-topic"):
        with pytest.raises(SystemExit):
            attacker.main(["live", "--data", "x", "--start", "2026-10-26", flag, "x"])
    assert [d["name"] for d in attacker.definitions()] == ["list_platform", "describe_normal", "list_scenarios", "inject_scenario",
                                                           "inject_effects", "list_active", "stop"]


@pytest.mark.kafka
@pytest.mark.filterwarnings("ignore::RuntimeWarning")
def test_through_the_broker_the_attacker_writes_to_control_only_and_reads_nothing(monkeypatch):
    confluent_kafka = pytest.importorskip("confluent_kafka")
    from sentinel_sim.replay import DataSource
    from sentinel_stream import attacker as A
    from sentinel_stream import broker, injector
    from tests.test_triage import FakeLLM, text, tool_use

    if not (ROOT / "data" / "metrics").exists():
        pytest.skip("dataset not generated")
    if not broker.reachable(broker.DEFAULT_BOOTSTRAP):
        pytest.skip(f"no Kafka broker at {broker.DEFAULT_BOOTSTRAP} (start one: docker compose up -d)")
    bootstrap = broker.DEFAULT_BOOTSTRAP
    prefix = f"test-{uuid.uuid4().hex[:8]}-"
    topic = {k: prefix + k for k in ("telemetry", "control", "answer_key", "alerts", "pages", "triage")}
    calls, consumers = [], []
    try:
        broker.ensure_topics(bootstrap, list(topic.values()))                    # every other topic exists and could be read

        class SpyConsumer(confluent_kafka.Consumer):
            def __init__(self, conf):
                consumers.append(conf)
                super().__init__(conf)

        class SpyProducer(confluent_kafka.Producer):
            def produce(self, t, *args, **kwargs):
                calls.append(t)
                return super().produce(t, *args, **kwargs)

        monkeypatch.setattr(confluent_kafka, "Consumer", SpyConsumer)
        monkeypatch.setattr(confluent_kafka, "Producer", SpyProducer)
        monkeypatch.setattr(broker, "Consumer", SpyConsumer)
        cfg = A.Config(model="fake", effort=None, wake_every_minutes=60)
        att = A.Attacker(DataSource(ROOT / "data"), cfg, send=lambda cmd: injector.send(bootstrap, topic["control"], [cmd]))
        llm = FakeLLM([[tool_use("inject_scenario", {"scenario": "trino_heavy_query", "intent": "a heavy query",
                                                     "severity": "noticeable"}, "a")], [text("one")], [text("none")]])
        A.run_live(att, A.Agent(att, create=llm), "2026-10-07 05:00", speed=1e9, end="2026-10-07 07:00", sleep=lambda s: None)
        assert set(calls) == {topic["control"]} and len(calls) == 1 and consumers == []
        sent = broker.read_topic(bootstrap, topic["control"])
        assert sent == att.commands and sent[0]["id"] == "atk-001"
    finally:
        monkeypatch.undo()
        from confluent_kafka.admin import AdminClient
        existing = AdminClient(broker.client_config(bootstrap)).list_topics(timeout=10).topics
        broker.delete_topics(bootstrap, [t for t in existing if t.startswith(prefix)])
