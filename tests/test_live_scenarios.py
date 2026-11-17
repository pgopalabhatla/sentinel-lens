"""Live scenarios and the injector, on a synthetic stream (no data directory, no broker, no engine).

For every scenario: its effect is on its target series from the injection minute, on nothing else,
and on nothing before. Then what is particular to each scenario. These tests look only at the
readings the publisher would send; whether the detectors catch a scenario is not tested here, on
purpose (docs/live-protocol.md).
"""
from pathlib import Path

import pandas as pd
import pytest

from sentinel_stream import wire
from sentinel_stream.events import RunFinished, RunStarted
from sentinel_stream.injector import Injector, throttled_for
from sentinel_stream.scenarios_live import GiB, MiB, REHEARSAL, SCENARIOS, Effect, Scenario

ROOT = Path(__file__).resolve().parents[1]
T0 = pd.Timestamp("2026-03-02 00:00", tz="UTC")
INJECT_AT = 30                                   # minutes after T0

PODS = {"kafka-0": 2.0, "kafka-1": 2.0, "kafka-2": 2.0, "orders-producer": 0.5, "orders-etl-consumer-0": 1.0,
        "orders-etl-consumer-1": 1.0, "flink-taskmanager-0": 2.0, "flink-taskmanager-1": 2.0, "trino-worker-0": 4.0,
        "trino-worker-1": 4.0, "daily-orders-driver": 1.0, "daily-orders-exec-1": 2.0, "customer-agg-driver": 1.0,
        "customer-agg-exec-1": 2.0, "airflow-worker-0": 2.0}
POD_READINGS = {"container_cpu_usage_cores": 0.4, "container_cpu_throttled_ratio": 0.004, "jvm_heap_used_ratio": 0.5,
                "container_memory_working_set_bytes": 400 * MiB, "jvm_gc_time_ms_per_sec": 15.0,
                "kube_pod_container_status_restarts_total": 3.0}
OTHER = {"kafka_consumergroup_lag|orders-etl": 200.0, "kafka_consumer_records_consumed_per_sec|orders-etl": 800.0,
         "kafka_messages_in_per_sec|orders": 800.0, "spark_streaming_input_rows_per_sec|orders-stream": 796.0,
         "kafka_produce_request_p99_ms|kafka-ingest": 15.0, "kafka_failed_produce_requests_per_sec|kafka-ingest": 0.0,
         "kafka_under_replicated_partitions|kafka-ingest": 0.0, "trino_queued_queries|trino-lakehouse": 0.0,
         "trino_resource_group_queued|adhoc": 0.0, "trino_query_p95_sec|trino-lakehouse": 4.0,
         "trino_completed_queries_per_min|trino-lakehouse": 20.0, "trino_failed_queries_per_min|trino-lakehouse": 0.0,
         "flink_job_restarts_total|clickstream-enrich": 0.0, "flink_records_out_per_sec|clickstream-enrich": 500.0,
         "flink_backpressure_ratio|clickstream-enrich": 0.06, "airflow_queued_tasks|airflow-main": 0.0}
BASE = {**{f"{metric}|{pod}": value for pod in PODS for metric, value in POD_READINGS.items()}, **OTHER}
BASE.update({f"container_memory_working_set_bytes|flink-taskmanager-{n}": 2.5 * GiB for n in (0, 1)})
ENTITY_OF = {s: ("pod:" if s.split("|")[1] in PODS else "thing:") + s.split("|")[1] for s in BASE}
COUNTERS = {s for s in BASE if "_total|" in s}


def at(minute: float) -> pd.Timestamp:
    return T0 + pd.Timedelta(minutes=minute)


def stream(n_minutes: int, base: dict = BASE) -> list:
    return [wire.metrics_message(at(m), base) for m in range(n_minutes)]


def injector(scenarios=SCENARIOS) -> Injector:
    return Injector(list(BASE), ENTITY_OF, PODS, scenarios)


def inject(name: str, target=None, minute=INJECT_AT, id_="inj-test", scenarios=SCENARIOS, **params) -> tuple:
    inj = injector(scenarios)
    spec = scenarios[name]
    inj.command({"cmd": "inject", "id": id_, "scenario": name, "target": target or spec.targets[0], "params": params,
                 "at": at(minute).isoformat()})
    return inj, spec


def play(inj: Injector, messages: list) -> tuple:
    out, records = [], []
    for m in messages:
        sent, new = inj.apply(m)
        out.append(sent)
        records += new
    return out, records


def value(message: dict, sid: str):
    return message["values"].get(sid)


def changed_series(base: dict, message: dict) -> set:
    return {s for s in base if message["values"].get(s) != base[s]}


# ----------------------------------------------------------------- every scenario: where and when
@pytest.mark.parametrize("name,target", [(s.name, t) for s in SCENARIOS.values() for t in s.targets])
def test_effect_is_on_the_target_series_from_the_injection_minute_and_nowhere_before(name, target):
    spec = SCENARIOS[name]
    duration = spec.params["duration_min"][0]
    inj, _ = inject(name, target, duration_min=duration)
    messages = stream(INJECT_AT + int(duration) + 90)
    out, records = play(inj, messages)

    started = next(r for r in records if r["event"] == "started")
    assert (started["scenario"], started["target"]) == (name, target)
    assert pd.Timestamp(started["start"]) == at(INJECT_AT) and pd.Timestamp(started["stop"]) == at(INJECT_AT + duration)
    assert [r["event"] for r in records] == ["started", "stopped"] and records[1]["reason"] == "duration"
    targets = set(started["series"])
    assert started["entities"] == sorted({ENTITY_OF[s] for s in targets} | ({f"dag:{target}"} if spec.pauses_dag else set()))

    # nothing before the injection minute: the very same messages
    assert all(sent is original for sent, original in zip(out[:INJECT_AT], messages[:INJECT_AT]))
    # nothing outside the target series, ever
    touched = set().union(*(changed_series(BASE, m) for m in out))
    assert touched <= targets, sorted(touched - targets)
    if targets:
        # on the target series from the injection minute itself ...
        assert changed_series(BASE, out[INJECT_AT]), "nothing changed at the injection minute"
        # ... and every series the scenario names is changed at some point, apart from throttling,
        # which only moves when the CPU crosses 72% of the pod's limit
        never = {s for s in targets - touched if not s.startswith("container_cpu_throttled_ratio|")}
        assert not never, sorted(never)
    # an hour after the stop and its tails, only counters still differ: a counter never goes back
    assert changed_series(BASE, out[-1]) <= COUNTERS
    for sid in COUNTERS:
        steps = [v for m in out if (v := value(m, sid)) is not None]      # a paused job's pods send nothing
        assert steps == sorted(steps) and all(float(v).is_integer() for v in steps)


def test_no_command_means_the_stream_is_sent_as_it_is():
    inj = injector()
    messages = stream(120) + [wire.watermark_message(at(120))]
    out, records = play(inj, messages)
    assert records == [] and all(sent is original for sent, original in zip(out, messages))


def test_the_same_commands_give_the_same_stream():
    runs = []
    for _ in range(2):
        inj, _ = inject("broker_gc_pressure", "kafka-2", magnitude=0.9, duration_min=120)
        runs.append(play(inj, stream(240)))
    assert runs[0] == runs[1]
    other, _ = inject("broker_gc_pressure", "kafka-2", id_="another-id", magnitude=0.9, duration_min=120)
    assert play(other, stream(240))[0] != runs[0][0]            # the jitter is seeded by the injection's id


# ------------------------------------------------------------------------------ each scenario
def test_consumer_memory_leak_grows_at_its_rate_and_goes_back_at_the_stop():
    inj, _ = inject("consumer_memory_leak", rate_pod0_mib_per_hour=48, rate_pod1_mib_per_hour=61, duration_min=240)
    out, _ = play(inj, stream(330))
    mem0, mem1 = (f"container_memory_working_set_bytes|orders-etl-consumer-{n}" for n in (0, 1))
    assert value(out[INJECT_AT + 119], mem0) == pytest.approx(400 * MiB + 2 * 48 * MiB)
    assert value(out[INJECT_AT + 239], mem1) == pytest.approx(400 * MiB + 4 * 61 * MiB)
    grow = [value(m, mem0) for m in out[INJECT_AT:INJECT_AT + 240]]
    assert grow == sorted(grow) and grow[0] > 400 * MiB
    assert value(out[INJECT_AT + 240], mem0) == 400 * MiB       # the stop minute is already clean
    assert all(value(m, "kube_pod_container_status_restarts_total|orders-etl-consumer-0") == 3 for m in out)   # no OOM kill


def test_a_leak_that_reaches_the_memory_limit_is_oom_killed_and_starts_again():
    base = {**BASE, "container_memory_working_set_bytes|orders-etl-consumer-0": 900 * MiB}
    inj, _ = inject("consumer_memory_leak", rate_pod0_mib_per_hour=80, rate_pod1_mib_per_hour=40, duration_min=240)
    out, _ = play(inj, stream(330, base))
    mem, restarts = "container_memory_working_set_bytes|orders-etl-consumer-0", "kube_pod_container_status_restarts_total|orders-etl-consumer-0"
    kills = [m for m in range(1, len(out)) if value(out[m], restarts) > value(out[m - 1], restarts)]
    assert len(kills) == 2                                      # 124 MiB of room at 80 MiB/hour: about every 93 minutes
    assert kills[0] - INJECT_AT == 92 and kills[1] - kills[0] == 93
    for k in kills:
        assert value(out[k - 1], mem) > 0.99 * GiB and value(out[k], mem) == 900 * MiB       # killed: back to the base reading
        assert value(out[k + 1], mem) == pytest.approx(900 * MiB + 80 * MiB / 60)             # and growing again
    assert all(value(m, mem) < 1 * GiB for m in out)
    assert value(out[-1], restarts) == 5                        # the two restarts stay
    assert all(value(m, "kube_pod_container_status_restarts_total|orders-etl-consumer-1") == 3 for m in out)


def test_flink_state_leak_grows_both_taskmanagers_and_leaves_the_heap_alone():
    inj, _ = inject("flink_state_leak", rate_tm0_mib_per_hour=55, rate_tm1_mib_per_hour=85, duration_min=300)
    out, _ = play(inj, stream(400))
    assert value(out[INJECT_AT + 299], "container_memory_working_set_bytes|flink-taskmanager-1") == pytest.approx(2.5 * GiB + 5 * 85 * MiB)
    assert value(out[INJECT_AT + 299], "container_memory_working_set_bytes|flink-taskmanager-0") == pytest.approx(2.5 * GiB + 5 * 55 * MiB)
    assert all(value(m, "jvm_heap_used_ratio|flink-taskmanager-0") == 0.5 for m in out)
    assert all(value(m, "flink_job_restarts_total|clickstream-enrich") == 0 for m in out)     # far from the 4 GiB limit


def test_flink_oom_kill_restarts_the_job_and_dents_its_output():
    base = {**BASE, "container_memory_working_set_bytes|flink-taskmanager-0": 3.9 * GiB}
    inj, _ = inject("flink_state_leak", rate_tm0_mib_per_hour=100, rate_tm1_mib_per_hour=50, duration_min=240)
    out, _ = play(inj, stream(330, base))
    job, pod = "flink_job_restarts_total|clickstream-enrich", "kube_pod_container_status_restarts_total|flink-taskmanager-0"
    kill = next(m for m in range(1, len(out)) if value(out[m], job) > value(out[m - 1], job))
    assert value(out[kill], pod) == 4 and value(out[kill], job) == 1
    assert min(value(m, "flink_records_out_per_sec|clickstream-enrich") for m in out[kill:kill + 6]) < 100
    assert value(out[kill + 6], "flink_records_out_per_sec|clickstream-enrich") == 500
    assert value(out[kill + 4], "flink_backpressure_ratio|clickstream-enrich") == 0.06        # starts 5 minutes later
    assert value(out[kill + 5], "flink_backpressure_ratio|clickstream-enrich") == pytest.approx(0.56, abs=0.15)


def test_consumer_slow_deploy_lag_ramps_and_drains_after_the_rollback():
    inj, _ = inject("consumer_slow_deploy", lag_per_hour=480_000, magnitude=1.0, duration_min=180)
    out, _ = play(inj, stream(300))
    lag, consumed, cpu = "kafka_consumergroup_lag|orders-etl", "kafka_consumer_records_consumed_per_sec|orders-etl", "container_cpu_usage_cores|orders-etl-consumer-0"
    assert value(out[INJECT_AT + 59], lag) == pytest.approx(200 + 480_000, rel=0.25)
    assert value(out[INJECT_AT + 179], lag) == pytest.approx(200 + 3 * 480_000, rel=0.25)
    assert value(out[INJECT_AT + 90], consumed) == pytest.approx(800 * 0.85, rel=0.05)
    assert 0.7 < value(out[INJECT_AT + 90], cpu) <= 0.97
    for n in (0, 1):                                            # the rollout restarts both pods, once
        restarts = [value(m, f"kube_pod_container_status_restarts_total|orders-etl-consumer-{n}") for m in out]
        assert restarts[INJECT_AT - 1] == 3 and restarts[INJECT_AT] == 4 and restarts[-1] == 4
    stop = INJECT_AT + 180
    peak = value(out[stop - 1], lag)
    assert value(out[stop], lag) == pytest.approx(peak, rel=0.25)                   # the backlog is still there
    assert value(out[stop + 15], lag) < 0.2 * peak and value(out[stop + 29], lag) < 0.03 * peak
    assert value(out[stop + 10], consumed) == pytest.approx(800 * 1.85, rel=0.05)   # draining
    assert value(out[stop + 30], lag) == 200 and value(out[stop + 30], consumed) == 800


def test_magnitude_scales_the_size_of_an_effect():
    def queued(magnitude):
        inj, _ = inject("trino_heavy_query", magnitude=magnitude, duration_min=60)
        out, _ = play(inj, stream(120))
        return max(value(m, "trino_queued_queries|trino-lakehouse") for m in out)
    assert queued(1.0) == pytest.approx(22, rel=0.3) and queued(0.5) == pytest.approx(11, rel=0.3)


def test_trino_heavy_query_hits_both_workers_and_the_queues():
    inj, _ = inject("trino_heavy_query", magnitude=1.0, duration_min=40)
    out, _ = play(inj, stream(120))
    def typical(sid):                       # the middle 20 minutes, where the effect is at full size; jitter averaged out
        return float(pd.Series([value(m, sid) for m in out[INJECT_AT + 10:INJECT_AT + 30]]).median())
    for w in ("trino-worker-0", "trino-worker-1"):
        assert typical(f"jvm_heap_used_ratio|{w}") == pytest.approx(0.97, abs=0.03)          # 0.5 + 0.5, clipped
        assert typical(f"container_cpu_usage_cores|{w}") == pytest.approx(2.6, rel=0.1)
        assert typical(f"jvm_gc_time_ms_per_sec|{w}") == pytest.approx(195, rel=0.1)
    assert typical("trino_resource_group_queued|adhoc") == pytest.approx(14, rel=0.1)
    assert typical("trino_query_p95_sec|trino-lakehouse") == pytest.approx(36, rel=0.1)
    assert typical("trino_completed_queries_per_min|trino-lakehouse") == pytest.approx(7, rel=0.15)
    assert typical("trino_failed_queries_per_min|trino-lakehouse") == pytest.approx(4, rel=0.1)
    assert value(out[INJECT_AT + 2], "trino_queued_queries|trino-lakehouse") == 0             # queues fill 3 minutes in
    assert value(out[INJECT_AT + 40], "trino_queued_queries|trino-lakehouse") == 0


def test_broker_gc_pressure_is_on_one_broker_and_partitions_flap():
    inj, _ = inject("broker_gc_pressure", "kafka-2", magnitude=1.0, duration_min=150)
    out, _ = play(inj, stream(240))
    mid = out[INJECT_AT + 75]
    assert value(mid, "jvm_heap_used_ratio|kafka-2") == pytest.approx(0.92, abs=0.07)
    assert value(mid, "jvm_gc_time_ms_per_sec|kafka-2") == pytest.approx(535, rel=0.25)
    assert value(mid, "kafka_produce_request_p99_ms|kafka-ingest") == pytest.approx(915, rel=0.25)
    assert value(mid, "jvm_heap_used_ratio|kafka-0") == 0.5 and value(mid, "jvm_heap_used_ratio|kafka-1") == 0.5
    urp = [value(m, "kafka_under_replicated_partitions|kafka-ingest") for m in out]
    assert set(urp) == {0.0, 12.0}
    assert all(v == 0 for v in urp[:INJECT_AT + 12]) and urp[INJECT_AT + 12] == 12        # first drop-out 12 minutes in
    assert all(v == 0 for v in urp[INJECT_AT + 145:])                                     # back in sync 5 minutes before the end
    flips = sum(a != b for a, b in zip(urp, urp[1:]))
    assert flips >= 8                                                                     # several in/out cycles


def test_throttling_follows_cpu_with_the_generators_formula():
    inj, _ = inject("broker_gc_pressure", "kafka-1", magnitude=1.2, duration_min=120)
    base = {**BASE, "container_cpu_usage_cores|kafka-1": 0.9}          # 45% of 2 cores; x 2.2 goes past 72%
    out, _ = play(inj, stream(200, base))
    m = out[INJECT_AT + 60]
    cpu = value(m, "container_cpu_usage_cores|kafka-1")
    assert cpu > 0.72 * 2.0
    assert value(m, "container_cpu_throttled_ratio|kafka-1") == pytest.approx(0.004 + throttled_for(cpu, 2.0))
    assert value(m, "container_cpu_throttled_ratio|kafka-0") == 0.004
    assert throttled_for(1.44, 2.0) == 0 and throttled_for(2.0, 2.0) == pytest.approx(0.55)


def test_producer_crashloop_restarts_in_a_loop_and_orders_stop():
    inj, _ = inject("producer_crashloop", duration_min=35)
    out, _ = play(inj, stream(150))
    restarts = [value(m, "kube_pod_container_status_restarts_total|orders-producer") for m in out]
    assert restarts[INJECT_AT - 1] == 3 and restarts[INJECT_AT] == 4            # the first restart at once
    assert restarts[INJECT_AT + 34] == 3 + 1 + 6 and restarts[-1] == 10         # then 12 an hour; they stay
    during = out[INJECT_AT + 1:INJECT_AT + 35]
    for sid in ("kafka_messages_in_per_sec|orders", "kafka_consumer_records_consumed_per_sec|orders-etl",
                "kafka_consumergroup_lag|orders-etl", "spark_streaming_input_rows_per_sec|orders-stream"):
        assert all(value(m, sid) == 0 for m in during)
        assert value(out[INJECT_AT + 45], sid) == pytest.approx(BASE[sid] * 1.6, rel=0.05)      # the queue drains
        assert value(out[INJECT_AT + 55], sid) == BASE[sid]
    assert all("container_cpu_usage_cores|orders-producer" not in m["values"] for m in during)  # no reading: not running
    assert all("container_cpu_throttled_ratio|orders-producer" not in m["values"] for m in during)
    assert all(value(m, "container_cpu_usage_cores|orders-etl-consumer-0") == pytest.approx(0.04, abs=0.03) for m in during)
    assert value(out[INJECT_AT + 35], "container_cpu_usage_cores|orders-producer") == 0.4


def run_messages(dag: str, run_id: str, start_min: float, end_min: float, scheduled_min: float) -> list:
    return [wire.run_message(RunStarted(at(start_min), dag, run_id, "scheduled", at(scheduled_min))),
            wire.run_message(RunFinished(at(end_min), dag, run_id, "success"))]


def test_dag_paused_drops_the_runs_that_would_start_during_the_pause():
    inj, _ = inject("dag_paused", "daily_orders", duration_min=240, lead_min=10)
    before = run_messages("daily_orders", "started-before", INJECT_AT - 5, INJECT_AT + 20, INJECT_AT - 6)
    during = run_messages("daily_orders", "due-during", INJECT_AT + 11, INJECT_AT + 50, INJECT_AT + 10)
    other = run_messages("customer_agg", "other-dag", INJECT_AT + 30, INJECT_AT + 60, INJECT_AT + 29)
    after = run_messages("daily_orders", "after", INJECT_AT + 250, INJECT_AT + 290, INJECT_AT + 249)
    messages = sorted(stream(INJECT_AT + 330) + before + during + other + after, key=wire.position)
    out, records = play(inj, messages)
    sent_runs = [(m["run_id"], m["event"]) for m in out if m is not None and m["type"] == "dag_run"]
    assert sent_runs == [("started-before", "start"), ("started-before", "finish"), ("other-dag", "start"),
                         ("other-dag", "finish"), ("after", "start"), ("after", "finish")]
    assert sum(m is None for m in out) == 2                              # the start and the finish of the run that was due
    assert "dag:daily_orders" in records[0]["entities"]
    # the job's batch-only pods have no readings while the DAG is paused, and the other job's pods do
    paused = [m for m in out if m is not None and m["type"] == "metrics" and at(INJECT_AT) <= wire.event_time(m) < at(INJECT_AT + 240)]
    assert all("container_cpu_usage_cores|daily-orders-exec-1" not in m["values"] for m in paused)
    assert all(m["values"]["container_cpu_usage_cores|customer-agg-exec-1"] == 0.4 for m in paused)


def test_pausing_a_dag_without_pods_only_drops_its_runs():
    inj, _ = inject("dag_paused", "hourly_quality_check", duration_min=240, lead_min=10)
    runs = [m for h in range(8) for m in run_messages("hourly_quality_check", f"h{h}", 5 + 60 * h + 0.3, 5 + 60 * h + 2, 5 + 60 * h)]
    out, records = play(inj, sorted(stream(8 * 60) + runs, key=wire.position))
    sent = [m["run_id"] for m in out if m is not None and m["type"] == "dag_run" and m["event"] == "start"]
    assert sent == ["h0", "h5", "h6", "h7"]                              # paused from 00:30 to 04:30: h1..h4 are missing
    assert records[0]["series"] == [] and records[0]["entities"] == ["dag:hourly_quality_check"]
    assert all(m is None or m["type"] != "metrics" or m["values"] == BASE for m in out)


# ------------------------------------------------------------------------------------- commands
def test_a_stop_command_ends_an_injection_early_and_the_answer_key_says_so():
    inj, _ = inject("trino_heavy_query", magnitude=1.0, duration_min=60)
    inj.command({"cmd": "stop", "id": "inj-test", "at": at(INJECT_AT + 20).isoformat()})
    out, records = play(inj, stream(120))
    assert [(r["event"], r.get("reason")) for r in records] == [("started", None), ("stopped", "stop command")]
    assert pd.Timestamp(records[0]["stop"]) == at(INJECT_AT + 60) and pd.Timestamp(records[1]["stop"]) == at(INJECT_AT + 20)
    assert changed_series(BASE, out[INJECT_AT + 19]) and not changed_series(BASE, out[INJECT_AT + 20])


def test_a_command_without_a_time_starts_at_the_next_message():
    inj = injector()
    messages = stream(100)
    out = [inj.apply(m) for m in messages[:40]]
    inj.command({"cmd": "inject", "id": "now", "scenario": "producer_crashloop", "target": "orders-producer", "params": {"duration_min": 20}})
    sent, records = inj.apply(messages[40])
    assert pd.Timestamp(records[0]["start"]) == at(40) and changed_series(BASE, sent)
    assert all(not r for _, r in out) and inj.active() == ["now"]


def test_commands_that_cannot_be_carried_out_are_refused():
    inj = injector()
    ok = {"cmd": "inject", "id": "a", "scenario": "trino_heavy_query", "target": "trino-lakehouse", "params": {"magnitude": 1.0}}
    inj.command(ok)
    for bad, why in [({**ok, "id": "a"}, "already used"), ({**ok, "id": "b", "scenario": "meteor"}, "unknown scenario"),
                     ({**ok, "id": "c", "target": "trino-other"}, "target must be one of"),
                     ({**ok, "id": "d", "params": {"magnitude": 5.0}}, "outside its range"),
                     ({**ok, "id": "e", "params": {"size": 1.0}}, "unknown parameter"),
                     ({"cmd": "stop", "id": "never-seen"}, "unknown injection"), ({"cmd": "explode"}, "unknown command")]:
        with pytest.raises(ValueError, match=why):
            inj.command(bad)


def test_a_scenario_naming_a_series_that_does_not_exist_is_refused():
    typo = Scenario("typo", "(none)", "", ("x",), {"duration_min": (1, 10)}, (Effect("no_such_metric|{target}", "add", 1),))
    with pytest.raises(ValueError, match="not in the catalog"):
        injector({"typo": typo}).command({"cmd": "inject", "id": "t", "scenario": "typo", "target": "x", "params": {}})


def test_missing_parameters_take_the_middle_of_their_range():
    spec = SCENARIOS["broker_gc_pressure"]
    assert spec.check("kafka-0", {"magnitude": 0.5}) == {"magnitude": 0.5, "duration_min": 135.0}
    assert all("duration_min" in s.params and s.approximations for s in SCENARIOS.values())
    assert len(SCENARIOS) == 7 and REHEARSAL.name not in SCENARIOS


# ------------------------------------------------------------------------- against the real catalog
@pytest.mark.parametrize("data", ["data", "data_live"])
def test_every_scenario_names_only_series_that_exist_in_the_catalog(data):
    if not (ROOT / data / "catalog").exists():
        pytest.skip(f"{data}/ not generated")
    from sentinel_sim.replay import DataSource
    ds = DataSource(ROOT / data)
    inj = Injector.for_data(ds, {**SCENARIOS, REHEARSAL.name: REHEARSAL})
    n = 0
    for spec in inj.scenarios.values():
        for target in spec.targets:
            n += 1
            inj.command({"cmd": "inject", "id": f"i{n}", "scenario": spec.name, "target": target, "params": {}})      # resolves or raises
    pods = [s for s in ds.series().series_id if s.endswith("|daily-orders-exec-1")]
    assert len(pods) >= 4 and inj.cpu_limit_of["trino-worker-0"] == 4.0 and inj.entity_of["kafka_consumergroup_lag|orders-etl"] == "consumer_group:orders-etl"


def test_injection_of_nothing_sends_the_quiet_week_exactly_as_it_is():
    """Message for message: with no command the injecting publisher's stream IS the data."""
    if not (ROOT / "data_live" / "catalog").exists():
        pytest.skip("data_live/ not generated")
    from sentinel_sim.replay import DataSource
    from sentinel_stream.publisher import messages
    ds = DataSource(ROOT / "data_live")
    inj = Injector.for_data(ds)
    n = 0
    for message in messages(ds, "2026-10-26", "2026-10-28"):
        sent, records = inj.apply(message)
        assert sent is message and records == []
        n += 1
    assert n > 2 * 1440
