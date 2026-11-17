"""Stream v2: the logs, Kubernetes events and changes an injected failure writes (evidence_live.py).

On the synthetic stream of tests/test_live_scenarios.py, with a background of ordinary log lines in
every minute's batch. For every named scenario:
  - the readings and runs sent are exactly stream v1's (evidence never changes a reading);
  - nothing changes in any batch before the injection starts, and what is added is stamped from the
    start on (and not long after the stop);
  - what is added has the data directory's own fields and names no scenario, injection id or parameter.
Then what is particular: the crash loop's silence and back-offs, OOM kills, the paused DAG's missing
run, generic injections (metrics only) and the answer key's evidence field.
"""
import json

import pandas as pd
import pytest

from sentinel_stream import evidence_live, wire
from sentinel_stream.events import RunFinished, RunStarted
from sentinel_stream.injector import Injector
from sentinel_stream.publisher import sendable
from sentinel_stream.scenarios_live import SCENARIOS
from tests.test_live_scenarios import BASE, ENTITY_OF, INJECT_AT, PODS, MiB, at

ID = "inj-evidence-test"
LOG_FIELDS = {"ts", "pod", "level", "logger", "message"}
EVENT_FIELDS = {"ts", "type", "reason", "involved_object", "message", "count"}
CHANGE_FIELDS = {"change_id", "ts", "kind", "target", "details", "author", "description"}
FIELDS = {wire.LOGS: LOG_FIELDS, wire.K8S_EVENTS: EVENT_FIELDS, wire.CHANGES: CHANGE_FIELDS}
NAMESPACES = {p: "ns-" + p.split("-")[0] for p in PODS}
TARGET = {"dag_paused": "daily_orders"}


def iso_s(t) -> str:
    return pd.Timestamp(t).strftime("%Y-%m-%dT%H:%M:%SZ")


def background_logs(minute: int) -> list:
    """Ordinary lines stamped in minute `minute` (they go in the batch stamped minute + 1)."""
    t = at(minute)
    return [{"ts": iso_s(t + pd.Timedelta("7s")), "pod": "orders-producer", "level": "INFO", "logger": "orders_producer.sender",
             "message": f"Produced batch of {500 + minute} records to orders in 12 ms"},
            {"ts": iso_s(t + pd.Timedelta("31s")), "pod": "kafka-0", "level": "INFO", "logger": "kafka.log.LocalLog",
             "message": f"[Log partition=orders-1] Rolled new log segment at offset {minute * 1000} in 40 ms."}]


def v2_stream(n_minutes: int, runs=(), extra_logs=(), extra_events=()) -> list:
    """Metrics every minute, the run messages, and one logs / events / changes batch per minute stamp."""
    logs = [r for m in range(n_minutes) for r in background_logs(m)] + list(extra_logs)
    out = [wire.metrics_message(at(m), BASE) for m in range(n_minutes)] + list(runs)
    for m in range(1, n_minutes + 1):
        lo, hi = at(m - 1), at(m)
        def inside(r):
            return lo <= pd.Timestamp(r["ts"]) < hi
        out += [wire.evidence_message(wire.LOGS, hi, sorted((r for r in logs if inside(r)), key=lambda r: r["ts"])),
                wire.evidence_message(wire.K8S_EVENTS, hi, [r for r in extra_events if inside(r)]),
                wire.evidence_message(wire.CHANGES, hi, [])]
    return sorted(out, key=wire.position)


def make_injector(evidence=True) -> Injector:
    return Injector(list(BASE), ENTITY_OF, PODS, SCENARIOS, evidence=evidence, namespace_of=NAMESPACES)


def command(name: str, **params) -> dict:
    return {"cmd": "inject", "id": ID, "scenario": name, "target": TARGET.get(name, SCENARIOS[name].targets[0]),
            "params": params, "at": at(INJECT_AT).isoformat()}


def play(inj: Injector, messages: list) -> tuple:
    sent, records = [], []
    for m in messages:
        out, new = inj.apply(m)
        records += new
        if sendable(out):
            sent.append(out)
    return sent, records


def batches(messages: list) -> dict:
    return {(m["type"], m["ts"]): wire.records_of(m) for m in messages if wire.is_evidence(m)}


def diff(base: list, injected: list) -> tuple:
    """(records added, records removed), each [(kind, record)]."""
    a, b = batches(base), batches(injected)
    added = [(k[0], r) for k, recs in b.items() for r in recs if r not in a.get(k, [])]
    removed = [(k[0], r) for k, recs in a.items() for r in recs if r not in b.get(k, [])]
    return added, removed


def no_injection(messages: list) -> list:
    return [m for m in messages if sendable(m)]


DURATION = {"consumer_memory_leak": 300, "flink_state_leak": 300, "consumer_slow_deploy": 150, "broker_gc_pressure": 120,
            "trino_heavy_query": 40, "producer_crashloop": 35, "dag_paused": 240}


@pytest.fixture(scope="module")
def stream_6h():
    return v2_stream(INJECT_AT + 330)


@pytest.fixture(scope="module")
def played(stream_6h):
    """{scenario: (sent v2, answer-key records, sent v1 of the same commands)}"""
    out = {}
    for name in SCENARIOS:
        inj = make_injector()
        inj.command(command(name, duration_min=DURATION[name]))
        sent, records = play(inj, stream_6h)
        v1 = make_injector(evidence=False)
        v1.command(command(name, duration_min=DURATION[name]))
        sent_v1, _ = play(v1, [m for m in stream_6h if not wire.is_evidence(m)])
        out[name] = (sent, records, sent_v1)
    return out


# ------------------------------------------------------------------------------------ every scenario
@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_the_readings_and_runs_of_stream_v2_are_those_of_stream_v1(played, name):
    sent, _, sent_v1 = played[name]
    assert [m for m in sent if not wire.is_evidence(m)] == sent_v1


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_evidence_appears_only_with_its_injection(stream_6h, played, name):
    sent, records, _ = played[name]
    start, stop = at(INJECT_AT), at(INJECT_AT + DURATION[name])
    base = no_injection(stream_6h)

    def before(messages):              # stamped before the start, and the batch stamped at it (it holds earlier records)
        return [m for m in messages if wire.event_time(m) < start or (wire.is_evidence(m) and wire.event_time(m) == start)]
    assert before(sent) == before(base)                            # nothing before the injection is touched
    added, removed = diff(base, sent)
    assert added, "a named scenario writes something"
    for _, r in added:
        assert start <= pd.Timestamp(r["ts"]) <= stop + pd.Timedelta("1min")
    for _, r in removed:
        assert pd.Timestamp(r["ts"]) >= start
    declared = set(records[0]["evidence"])
    assert {k for k, _ in added} <= declared and "metrics" in declared
    assert wire.CHANGES in {k for k, _ in added} or name == "trino_heavy_query"


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_evidence_has_the_stores_own_fields_and_never_names_what_only_the_answer_key_knows(stream_6h, played, name):
    sent, records, _ = played[name]
    added, _ = diff(no_injection(stream_6h), sent)
    forbidden = [ID, "inj-", "inject", "answer", "scenario", "magnitude", "duration_min", "lead_min", *SCENARIOS,
                 *(k for s in SCENARIOS.values() for k in s.params)]
    for kind, r in added:
        assert set(r) == FIELDS[kind]
        text = json.dumps(r).lower()
        assert not [w for w in forbidden if w.lower() in text], (kind, r)
        if kind == wire.K8S_EVENTS:
            assert set(r["involved_object"]) == {"kind", "name", "namespace"}
    # and the messages around them are the stream's ordinary ones
    assert {m["type"] for m in sent} <= {wire.METRICS, wire.DAG_RUN, wire.LOGS, wire.K8S_EVENTS, wire.CHANGES}


def test_without_a_command_stream_v2_is_sent_as_it_is_less_the_empty_batches(stream_6h):
    sent, records = play(make_injector(), stream_6h)
    assert sent == no_injection(stream_6h) and records == []


# ----------------------------------------------------------------------------- what is particular
def test_crash_loop_silences_the_pod_and_backs_off_once_per_restart(stream_6h, played):
    sent, _, _ = played["producer_crashloop"]
    start, stop = at(INJECT_AT), at(INJECT_AT + DURATION["producer_crashloop"])
    lines = [r for m in sent if m["type"] == wire.LOGS for r in m["lines"] if r["pod"] == "orders-producer"]
    during = [r for r in lines if start <= pd.Timestamp(r["ts"]) < stop]
    assert during and all(r["level"] in ("WARN", "ERROR") for r in during)          # no "Produced batch" while it is down
    assert any("Produced batch" in r["message"] for r in lines if pd.Timestamp(r["ts"]) >= stop)      # back afterwards
    restarts = [m["values"]["kube_pod_container_status_restarts_total|orders-producer"] for m in sent if m["type"] == wire.METRICS]
    backoffs = [r for m in sent if m["type"] == wire.K8S_EVENTS for r in m["events"] if r["reason"] == "BackOff"]
    assert len(backoffs) == restarts[-1] - restarts[0] == 7                 # one at once, then 12 an hour for 35 minutes
    assert all(r["involved_object"] == {"kind": "Pod", "name": "orders-producer", "namespace": "ns-orders"} for r in backoffs)
    changes = [r for m in sent if m["type"] == wire.CHANGES for r in m["changes"]]
    assert [c["kind"] for c in changes] == ["deploy", "deploy"]
    assert changes[0]["ts"] == "2026-03-02T00:30:00Z" and changes[1]["ts"] == "2026-03-02T01:05:00Z"


def test_an_oom_kill_writes_an_oomkilled_event_at_the_restart():
    base = dict(BASE)
    base["container_memory_working_set_bytes|orders-etl-consumer-0"] = 900 * MiB
    # 124 MiB of room at 80 MiB/hour: killed 92 minutes in; the stream ends before the second kill
    stream = [m if m["type"] != wire.METRICS else wire.metrics_message(wire.event_time(m), base) for m in v2_stream(INJECT_AT + 150)]
    inj = make_injector()
    inj.command(command("consumer_memory_leak", rate_pod0_mib_per_hour=80, rate_pod1_mib_per_hour=40, duration_min=240))
    sent, _ = play(inj, stream)
    restarts = "kube_pod_container_status_restarts_total|orders-etl-consumer-0"
    metrics = [m for m in sent if m["type"] == wire.METRICS]
    kills = [wire.event_time(m) for prev, m in zip(metrics, metrics[1:]) if m["values"][restarts] > prev["values"][restarts]]
    ooms = [r for m in sent if m["type"] == wire.K8S_EVENTS for r in m["events"] if r["reason"] == "OOMKilled"]
    assert len(kills) == len(ooms) == 1
    assert kills[0] <= pd.Timestamp(ooms[0]["ts"]) < kills[0] + pd.Timedelta("1min")
    assert ooms[0]["involved_object"]["name"] == "orders-etl-consumer-0" and ooms[0]["type"] == "Warning"
    cache = [r for m in sent if m["type"] == wire.LOGS for r in m["lines"] if r["logger"] == "orders_etl.cache"]
    assert cache and all("eviction" in r["message"] for r in cache)


def test_a_paused_dag_leaves_no_trace_of_the_run_that_did_not_happen():
    s, e = INJECT_AT + 11, INJECT_AT + 50
    run = [wire.run_message(RunStarted(at(s), "daily_orders", "scheduled__due", "scheduled", at(s - 1))),
           wire.run_message(RunFinished(at(e), "daily_orders", "scheduled__due", "success"))]
    other = [wire.run_message(RunStarted(at(s + 2), "customer_agg", "scheduled__other", "scheduled", at(s + 1)))]

    def run_lines(dag, run_id, pod, t0):
        return [{"ts": iso_s(at(t0) + pd.Timedelta("8s")), "pod": "airflow-scheduler", "level": "INFO", "logger": "airflow.jobs",
                 "message": f"Sending TaskInstanceKey(dag_id='{dag}', task_id='spark_submit', run_id='{run_id}', try_number=1)"},
                {"ts": iso_s(at(t0 + 1)), "pod": pod, "level": "INFO", "logger": "org.apache.spark.SparkContext",
                 "message": "Submitted application spark-1 attempt 1"}]

    def scheduled(pod, t0):
        return {"ts": iso_s(at(t0) + pd.Timedelta("20s")), "type": "Normal", "reason": "Scheduled", "count": 1,
                "involved_object": {"kind": "Pod", "name": pod, "namespace": "spark"}, "message": f"Successfully assigned spark/{pod}"}

    logs = run_lines("daily_orders", "scheduled__due", "daily-orders-driver", s) + run_lines("customer_agg", "scheduled__other", "customer-agg-driver", s + 2)
    events = [scheduled("daily-orders-driver", s), scheduled("customer-agg-driver", s + 2),
              {**scheduled("daily-orders-driver", e), "reason": "Killing", "ts": iso_s(at(e) + pd.Timedelta("30s"))}]
    stream = v2_stream(INJECT_AT + 300, run + other, logs, events)
    inj = make_injector()
    inj.command(command("dag_paused", duration_min=240, lead_min=10))
    sent, records = play(inj, stream)
    added, removed = diff(no_injection(stream), sent)
    assert sorted((k, r.get("message", r.get("reason"))) for k, r in removed) == sorted(
        [(wire.LOGS, logs[0]["message"]), (wire.LOGS, logs[1]["message"]),
         (wire.K8S_EVENTS, "Successfully assigned spark/daily-orders-driver"), (wire.K8S_EVENTS, events[2]["message"])])
    assert [(k, r["kind"], r["target"]) for k, r in added] == [(wire.CHANGES, "dag_pause", "daily_orders"),
                                                               (wire.CHANGES, "dag_unpause", "daily_orders")]
    assert not [m for m in sent if m["type"] == wire.DAG_RUN and m["dag_id"] == "daily_orders"]
    assert records[0]["evidence"] == ["metrics", "dag_run", "logs", "k8s_events", "changes"]


def test_a_generic_injection_is_metrics_only_and_the_answer_key_says_so(stream_6h):
    inj = make_injector()
    inj.command({"cmd": "inject", "id": ID, "at": at(INJECT_AT).isoformat(),
                 "effects": [{"series_id": "kube_pod_container_status_restarts_total|orders-producer", "op": "add",
                              "shape": "ramp", "magnitude": 6, "duration_min": 30},
                             {"series_id": "jvm_heap_used_ratio|kafka-0", "op": "add", "shape": "flat", "magnitude": 0.3,
                              "duration_min": 60}]})
    sent, records = play(inj, stream_6h)
    assert batches(sent) == batches(no_injection(stream_6h))          # no log, event or change: a novel failure
    started = records[0]
    assert started["scenario"] == "custom" and started["evidence"] == ["metrics"] and started["stream_version"] == 2


def test_stream_v1_answer_key_and_published_record_are_unchanged():
    from sentinel_stream.injector import published_record
    inj = make_injector(evidence=False)
    inj.command(command("trino_heavy_query", duration_min=40))
    _, records = play(inj, [m for m in v2_stream(INJECT_AT + 50) if not wire.is_evidence(m)])
    assert "evidence" not in records[0] and "stream_version" not in records[0]
    assert published_record(at(0), at(10)) == {"event": "published", "start": at(0).isoformat(), "end": at(10).isoformat()}
    assert published_record(at(0), at(10), 2)["stream_version"] == 2


def test_every_named_scenario_has_evidence_and_the_rehearsal_has_none():
    assert set(evidence_live.BY_SCENARIO) == set(SCENARIOS)


def test_a_batch_comes_before_everything_else_stamped_at_its_time():
    t = at(10)
    order = [wire.evidence_message(wire.LOGS, t, []), wire.evidence_message(wire.K8S_EVENTS, t, []),
             wire.evidence_message(wire.CHANGES, t, []), wire.watermark_message(t),
             wire.run_message(RunStarted(t, "a", "r1", "scheduled", t)), wire.run_message(RunFinished(t, "a", "r0", "success")),
             wire.metrics_message(t, {})]
    positions = [wire.position(m) for m in order]
    assert positions == sorted(positions) and len(set(positions)) == len(positions)
    assert wire.position_before(t) < positions[0]
