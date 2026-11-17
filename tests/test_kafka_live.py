"""Injection through the real broker (marker `kafka`; skipped when no broker answers).

1. Rehearsal     The whole pipeline of the scripted run (control -> publisher -> telemetry and answer
                 key -> consumer -> pages -> live scorer) with the dummy scenario, one restart of
                 airflow-worker-0, on twelve hours of the TRAIN split. It checks the machinery; the
                 seven real scenarios are not run through the engine here (docs/live-protocol.md).
2. Nothing       With no command, the injecting publisher and the consumer give exactly the quiet
                 week's result: the in-process engine's 14 alerts and 18 page events, 4 of them pages.
"""
import uuid
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("confluent_kafka")

from scripts import run_live_week  # noqa: E402
from sentinel_stream import AlertEvent, PageEvent, StreamEngine, broker, replay, wire  # noqa: E402
from sentinel_stream.scenarios_live import REHEARSAL  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = broker.DEFAULT_BOOTSTRAP
pytestmark = [pytest.mark.kafka, pytest.mark.filterwarnings("ignore::RuntimeWarning")]

QUIET_WEEK = {"alerts": 14, "page_events": 18, "page_outs": 4}          # docs/log.md, "the engine behind Kafka"


@pytest.fixture
def prefix():
    if not broker.reachable(BOOTSTRAP):
        pytest.skip(f"no Kafka broker at {BOOTSTRAP} (start one: docker compose up -d)")
    prefix = f"test-{uuid.uuid4().hex[:8]}-"
    yield prefix
    from confluent_kafka.admin import AdminClient
    topics = AdminClient(broker.client_config(BOOTSTRAP)).list_topics(timeout=10).topics
    broker.delete_topics(BOOTSTRAP, [t for t in topics if t.startswith(prefix)])


def in_process(data, start, end, history_start=None) -> tuple:
    from sentinel_sim.replay import DataSource
    ds = DataSource(data).at(end)
    engine = StreamEngine.warm_start(ds, start, history_start=history_start)
    events = list(replay(engine, ds, start, end, history_start=history_start))
    return ([wire.dumps(wire.alert_record(e)) for e in events if isinstance(e, AlertEvent)],
            [wire.dumps(wire.page_record(e)) for e in events if isinstance(e, PageEvent)])


def test_rehearsal_one_dummy_injection_through_the_whole_pipeline(prefix):
    if not (ROOT / "data" / "metrics").exists():
        pytest.skip("dataset not generated")
    start, end, history_start = "2026-10-07 00:00", "2026-10-07 12:00", "2026-09-28"
    at = pd.Timestamp("2026-10-07 06:00", tz="UTC")
    commands = [{"cmd": "inject", "id": "rehearsal-01", "scenario": REHEARSAL.name, "target": "airflow-worker-0",
                 "params": {"duration_min": 20.0}, "at": at.isoformat()}]
    out = run_live_week.run(ROOT / "data", start, end, commands, prefix, scenarios={REHEARSAL.name: REHEARSAL},
                            history_start=history_start, log=lambda line: None)

    # the answer key: what was injected, when, on what
    started, stopped, published = out["answer_key"]
    counter = "kube_pod_container_status_restarts_total|airflow-worker-0"
    assert (started["event"], started["id"], started["scenario"], started["target"]) == ("started", "rehearsal-01", REHEARSAL.name, "airflow-worker-0")
    assert started["series"] == [counter] and started["entities"] == ["pod:airflow-worker-0"]
    assert pd.Timestamp(started["start"]) == at and pd.Timestamp(started["stop"]) == at + pd.Timedelta("20min")
    assert stopped == {"event": "stopped", "id": "rehearsal-01", "reason": "duration", "stop": (at + pd.Timedelta("20min")).isoformat()}
    assert (published["event"], pd.Timestamp(published["start"]), pd.Timestamp(published["end"])) == (
        "published", pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC"))

    # the telemetry: the counter is one higher from the injection minute on, and nothing else differs
    from sentinel_sim.replay import DataSource
    from sentinel_stream.publisher import messages
    plain = list(messages(DataSource(ROOT / "data").at(end), start, end, history_start))
    sent = broker.read_topic(BOOTSTRAP, out["topics"]["telemetry"])
    assert len(sent) == len(plain) == out["telemetry_messages"]
    for was, now in zip(plain, sent):
        if was["type"] == "metrics" and wire.event_time(was) >= at:
            assert now["values"][counter] == was["values"][counter] + 1
            assert {k: v for k, v in now["values"].items() if k != counter} == {k: v for k, v in was["values"].items() if k != counter}
        else:
            assert now == wire.loads(wire.dumps(was))

    # the consumer saw one more restart than without the injection, paged for it, and the scorer says so
    plain_alerts, plain_pages = in_process(ROOT / "data", start, end, history_start)
    assert out["alerts"] == len(plain_alerts) + 1
    r = out["result"]
    assert (r["n_injections"], r["n_detected"], r["days"]) == (1, 1, 0.5)
    row = r["injections"][0]
    assert row["time_to_detect_min"] == 0 and row["paged_by"] == f"{counter} (counter_rule)"
    plain_page_outs = sum(wire.loads(p)["kind"] == "paged" for p in plain_pages)
    assert r["page_outs"] == plain_page_outs + 1 and r["false_page_outs"] == plain_page_outs      # the slice's own pages match nothing
    text = run_live_week.report(out, commands, run_live_week.quiet_page_outs(ROOT / "data", start, end, history_start), ["rehearsal"])
    assert "| rehearsal-01 | rehearsal_restart | airflow-worker-0 | Oct 07 06:00 | Oct 07 06:20 |" in text
    assert f"of the {plain_page_outs} false page-outs here, {plain_page_outs} are at the time of one of those {plain_page_outs}" in text


def test_injection_of_nothing_reproduces_the_quiet_week_exactly(prefix):
    if not (ROOT / "data_live" / "metrics").exists():
        pytest.skip("data_live/ not generated")
    out = run_live_week.run(ROOT / "data_live", run_live_week.START, run_live_week.END, [], prefix, log=lambda line: None)
    want_alerts, want_pages = in_process(ROOT / "data_live", run_live_week.START, run_live_week.END)
    assert [wire.dumps(r) for r in broker.read_topic(BOOTSTRAP, out["topics"]["alerts"])] == want_alerts
    assert [wire.dumps(r) for r in out["pages"]] == want_pages
    assert {"alerts": len(want_alerts), "page_events": len(want_pages),
            "page_outs": sum(r["kind"] == "paged" for r in out["pages"])} == QUIET_WEEK
    assert [a["event"] for a in out["answer_key"]] == ["published"]
    r = out["result"]
    assert (r["n_injections"], r["page_outs"], r["false_page_outs"], r["days"]) == (0, 4, 4, 7.0)
    assert r["false_pages_per_day"] == pytest.approx(4 / 7)
