"""Generic injections: a list of primitive effects instead of a named scenario.

On the synthetic stream of test_live_scenarios.py (no data directory, no broker, no engine). What is
tested: the three shapes are exact; each effect has its own start and duration; effects that break a
series' physical limits are rejected and reported; the answer key records exactly what was applied,
with the caller's `intent` and `severity`; and the scorer reports by severity without ever matching
on it.
"""
import math

import pandas as pd
import pytest

from sentinel_lens.correlator import Topology
from sentinel_stream import live_score, wire
from sentinel_stream.injector import Injector
from sentinel_stream.scenarios_live import GiB, MiB, SCENARIOS
from tests.test_live_scenarios import BASE, ENTITY_OF, PODS, at, changed_series, play, stream, value

UNIT_OF_METRIC = {"container_cpu_usage_cores": "cores", "container_cpu_throttled_ratio": "ratio", "jvm_heap_used_ratio": "ratio",
                  "container_memory_working_set_bytes": "bytes", "jvm_gc_time_ms_per_sec": "ms/s",
                  "kube_pod_container_status_restarts_total": "count", "flink_job_restarts_total": "count",
                  "kafka_under_replicated_partitions": "partitions", "trino_queued_queries": "queries",
                  "trino_resource_group_queued": "queries", "airflow_queued_tasks": "tasks",
                  "kafka_produce_request_p99_ms": "ms", "trino_query_p95_sec": "s", "flink_backpressure_ratio": "ratio"}
UNIT_OF = {s: UNIT_OF_METRIC.get(s.split("|")[0], "msg/s") for s in BASE}
MEMORY_LIMIT = {pod: (1 * GiB if "consumer" in pod else 4 * GiB) for pod in PODS}

HEAP, MEM, CPU = "jvm_heap_used_ratio|kafka-0", "container_memory_working_set_bytes|orders-etl-consumer-0", "container_cpu_usage_cores|kafka-0"
QUEUED, P99, RESTARTS = "trino_queued_queries|trino-lakehouse", "kafka_produce_request_p99_ms|kafka-ingest", "kube_pod_container_status_restarts_total|kafka-0"
LAG = "kafka_consumergroup_lag|orders-etl"
START = 30


def injector() -> Injector:
    return Injector(list(BASE), ENTITY_OF, PODS, SCENARIOS, UNIT_OF, MEMORY_LIMIT)


def effect(series_id, op, shape, magnitude, duration_min, start=None) -> dict:
    return {"series_id": series_id, "op": op, "shape": shape, "magnitude": magnitude, "duration_min": duration_min,
            **({} if start is None else {"start": at(start).isoformat()})}


def run(effects, n=150, base=BASE, minute=START, **extra):
    inj = injector()
    rejected = inj.command({"cmd": "inject", "id": "c1", "effects": effects, "at": at(minute).isoformat(), **extra})
    out, records = play(inj, stream(n) if isinstance(base, dict) else base)
    return out, records, rejected


def series(out, sid) -> list:
    return [value(m, sid) for m in out]


# --------------------------------------------------------------------------------------- shapes
def test_flat_holds_the_magnitude_for_the_duration_and_touches_nothing_else():
    out, records, _ = run([effect(P99, "add", "flat", 250, 40)])
    p99 = series(out, P99)
    assert p99[:START] == [15.0] * START and p99[START:START + 40] == [265.0] * 40 and p99[START + 40:] == [15.0] * (150 - START - 40)
    assert all(changed_series(BASE, m) <= {P99} for m in out)
    assert all(sent["values"] == BASE for sent in out[:START])


def test_ramp_grows_linearly_to_the_magnitude_at_the_end():
    out, _, _ = run([effect(LAG, "add", "ramp", 6000, 60)])
    lag = series(out, LAG)
    assert lag[START - 1] == 200 and lag[START] == pytest.approx(200 + 100) and lag[START + 29] == pytest.approx(200 + 3000)
    assert lag[START + 59] == pytest.approx(200 + 6000) and lag[START + 60] == 200
    steps = [b - a for a, b in zip(lag[START:START + 59], lag[START + 1:START + 60])]
    assert all(s == pytest.approx(100) for s in steps)


def test_decay_starts_at_the_magnitude_and_falls_off():
    out, _, _ = run([effect(P99, "add", "decay", 400, 50)])
    p99 = series(out, P99)
    assert p99[START] == 415 and p99[START + 25] == pytest.approx(15 + 400 * math.exp(-2)) and p99[START + 50] == 15
    assert p99[START:START + 50] == sorted(p99[START:START + 50], reverse=True)


def test_mult_flat_ramp_and_decay():
    out, _, _ = run([effect("kafka_messages_in_per_sec|orders", "mult", "flat", 0.25, 20),
                     effect(LAG, "mult", "ramp", 5, 40), effect(P99, "mult", "decay", 3, 40)])
    assert series(out, "kafka_messages_in_per_sec|orders")[START:START + 20] == [200.0] * 20
    lag, p99 = series(out, LAG), series(out, P99)
    assert lag[START + 39] == pytest.approx(1000) and lag[START + 19] == pytest.approx(200 * (1 + 4 * 20 / 40))
    assert p99[START] == pytest.approx(45) and p99[START + 39] < 16 and p99[START + 40] == 15


def test_each_effect_has_its_own_start_and_duration():
    out, records, _ = run([effect(P99, "add", "flat", 100, 20), effect(HEAP, "add", "flat", 0.2, 30, start=START + 45)], n=200)
    assert series(out, P99)[START:START + 20] == [115.0] * 20 and series(out, P99)[START + 20] == 15
    heap = series(out, HEAP)
    assert heap[START + 44] == 0.5 and heap[START + 45:START + 75] == [0.7] * 30 and heap[START + 75] == 0.5
    started, stopped = records
    assert pd.Timestamp(started["start"]) == at(START) and pd.Timestamp(started["stop"]) == at(START + 75)      # earliest start, latest stop
    assert started["effects"] == [
        {"series_id": P99, "op": "add", "shape": "flat", "magnitude": 100.0, "start": at(START).isoformat(), "stop": at(START + 20).isoformat()},
        {"series_id": HEAP, "op": "add", "shape": "flat", "magnitude": 0.2, "start": at(START + 45).isoformat(), "stop": at(START + 75).isoformat()}]
    assert stopped == {"event": "stopped", "id": "c1", "stop": at(START + 75).isoformat(), "reason": "duration"}


# ------------------------------------------------------------------------------- the answer key
def test_the_answer_key_records_what_was_applied_with_intent_and_severity():
    out, records, _ = run([effect(CPU, "mult", "flat", 2.0, 30)], intent="one broker twice as busy", severity="subtle")
    started = records[0]
    assert (started["event"], started["id"], started["scenario"], started["target"], started["params"]) == ("started", "c1", "custom", "", {})
    assert (started["intent"], started["severity"]) == ("one broker twice as busy", "subtle")
    assert started["series"] == ["container_cpu_throttled_ratio|kafka-0", CPU]          # throttling follows CPU, so it is listed
    assert started["entities"] == ["pod:kafka-0"] and started["rejected"] == []
    assert series(out, CPU)[START] == 0.8


def test_a_named_scenario_can_carry_intent_and_severity_too_and_they_are_optional():
    inj = injector()
    inj.command({"cmd": "inject", "id": "n1", "scenario": "trino_heavy_query", "target": "trino-lakehouse", "params": {},
                 "at": at(START).isoformat(), "severity": "noticeable", "intent": "a big ad-hoc query"})
    inj.command({"cmd": "inject", "id": "n2", "scenario": "producer_crashloop", "target": "orders-producer", "params": {},
                 "at": at(START + 60).isoformat()})
    _, records = play(inj, stream(160))
    first, second = [r for r in records if r["event"] == "started"]
    assert (first["severity"], first["intent"]) == ("noticeable", "a big ad-hoc query")
    assert "severity" not in second and "intent" not in second
    with pytest.raises(ValueError, match="severity must be one of"):
        inj.command({"cmd": "inject", "id": "n3", "effects": [effect(P99, "add", "flat", 1, 5)], "severity": "huge"})


# ------------------------------------------------------------------------------------ rejection
@pytest.mark.parametrize("bad,why", [
    (effect("no_such|series", "add", "flat", 1, 10), "not in the catalog"),
    (effect(P99, "set", "flat", 1, 10), "op must be add or mult"),
    (effect(P99, "add", "square", 1, 10), "shape must be flat, ramp or decay"),
    (effect(P99, "add", "flat", "a lot", 10), "must be numbers"),
    (effect(P99, "add", "flat", float("nan"), 10), "must be finite"),
    (effect(P99, "add", "flat", 5, 0), "greater than 0"),
    (effect(P99, "mult", "flat", -2, 10), "factor cannot be negative"),
    (effect(HEAP, "add", "flat", 1.5, 10), "cannot stay inside 0 .. 1"),                       # a ratio
    (effect(QUEUED, "add", "flat", 2.5, 10), "must be a whole number"),                         # a count of queries
    (effect(RESTARTS, "mult", "flat", 2, 10), "counter only takes add"),
    (effect(RESTARTS, "add", "decay", 2, 10), "cannot go back"),
    (effect(RESTARTS, "add", "flat", -1, 10), "cannot go back"),
    (effect(RESTARTS, "add", "flat", 1.5, 10), "whole positive number"),
    ("not an effect", "must be an object"),
])
def test_effects_that_can_never_be_valid_are_rejected_at_once(bad, why):
    inj = injector()
    with pytest.raises(ValueError, match="no valid effect.*" + why):
        inj.command({"cmd": "inject", "id": "bad", "effects": [bad]})
    rejected = inj.command({"cmd": "inject", "id": "mixed", "effects": [bad, effect(P99, "add", "flat", 100, 10)], "at": at(START).isoformat()})
    assert len(rejected) == 1 and why.split(".*")[-1] in rejected[0]["reason"] and rejected[0]["effect"] == bad
    out, records = play(inj, stream(60))
    assert records[0]["rejected"] == rejected and len(records[0]["effects"]) == 1          # the valid one is applied, the other reported
    assert series(out, P99)[START] == 115


def test_commands_that_are_not_an_injection_are_refused():
    inj = injector()
    for cmd, why in [({"cmd": "inject", "id": "a", "effects": []}, "at least one effect"),
                     ({"cmd": "inject", "id": "b", "effects": "heap up"}, "must be a list"),
                     ({"cmd": "inject", "id": "c", "scenario": "trino_heavy_query", "effects": [effect(P99, "add", "flat", 1, 5)]}, "not both")]:
        with pytest.raises(ValueError, match=why):
            inj.command(cmd)
    assert play(inj, stream(30))[1] == []


@pytest.mark.parametrize("bad,why", [
    (effect(HEAP, "add", "flat", 0.6, 20), "the reading is 0.5; + 0.6 would be 1.1, above its limit of 1"),
    (effect(HEAP, "mult", "ramp", 2.5, 20), "x 2.5 would be 1.25, above its limit of 1"),
    (effect(MEM, "add", "ramp", 700 * MiB, 20), "above its limit of 1.074e+09"),                 # 400 MiB + 700 MiB > the pod's 1 GiB
    (effect(CPU, "add", "flat", 1.7, 20), "would be 2.1, above its limit of 2"),                 # the pod has 2 cores
    (effect(QUEUED, "add", "flat", -3, 20), "would be -3, below its limit of 0"),
    (effect(P99, "add", "decay", -20, 20), "would be -5, below its limit of 0"),                 # a latency
    (effect("jvm_gc_time_ms_per_sec|kafka-0", "add", "flat", 990, 20), "above its limit of 1000"),
])
def test_effects_that_would_break_a_physical_limit_on_the_current_reading_are_rejected(bad, why):
    out, records, at_once = run([bad, effect(P99, "add", "flat", 100, 10)])
    assert at_once == []                                         # nothing wrong with it until it meets the reading
    started = records[0]
    assert len(started["rejected"]) == 1 and why in started["rejected"][0]["reason"]
    assert started["rejected"][0]["effect"]["series_id"] == bad["series_id"]
    assert [e["series_id"] for e in started["effects"]] == [P99] and started["series"] == [P99]
    assert all(changed_series(BASE, m) <= {P99} for m in out)    # the rejected effect is not applied at all

    out, records, _ = run([bad])                                 # on its own: nothing starts, and the answer key says why
    assert [r["event"] for r in records] == ["rejected"] and why in records[0]["reasons"][0] and records[0]["id"] == "c1"
    assert all(sent["values"] == BASE for sent in out)


def test_the_same_amounts_inside_the_limits_are_accepted():
    out, records, _ = run([effect(HEAP, "add", "flat", 0.45, 20), effect(MEM, "add", "ramp", 600 * MiB, 20),
                           effect(CPU, "add", "flat", 1.5, 20), effect(QUEUED, "add", "flat", 7, 20)])
    assert records[0]["rejected"] == [] and len(records[0]["effects"]) == 4 and "minutes_held_at_a_limit" not in records[1]
    assert value(out[START + 19], MEM) == pytest.approx(1000 * MiB) and value(out[START + 5], QUEUED) == 7


def test_a_reading_that_moves_later_is_held_at_the_limit_and_the_answer_key_counts_the_minutes():
    """Heap is 0.5 when + 0.4 is accepted, and rises to 0.8 twenty minutes in: 1.2 is not a heap ratio."""
    messages = [wire.metrics_message(at(m), {**BASE, HEAP: 0.5 if m < START + 20 else 0.8}) for m in range(120)]
    out, records, _ = run([effect(HEAP, "add", "flat", 0.4, 60)], base=messages)
    heap = series(out, HEAP)
    assert heap[START:START + 20] == [0.9] * 20 and heap[START + 20:START + 60] == [1.0] * 40 and heap[START + 60] == 0.8
    assert records[0]["rejected"] == [] and records[1]["minutes_held_at_a_limit"] == {HEAP: 40}


def test_counts_stay_whole_numbers():
    out, _, _ = run([effect(QUEUED, "add", "ramp", 9, 30), effect("kafka_under_replicated_partitions|kafka-ingest", "add", "decay", 12, 30)])
    queued, urp = series(out, QUEUED), series(out, "kafka_under_replicated_partitions|kafka-ingest")
    assert all(float(v).is_integer() and v >= 0 for v in queued + urp)
    assert queued[START + 29] == 9 and max(queued) == 9 and urp[START] == 12 and urp[START + 29] == 0
    assert queued[START:START + 30] == sorted(queued[START:START + 30])


def test_a_counter_takes_whole_steps_that_stay():
    out, records, _ = run([effect(RESTARTS, "add", "ramp", 4, 40), effect("kube_pod_container_status_restarts_total|kafka-1", "add", "flat", 2, 10)])
    ramp, flat = series(out, RESTARTS), series(out, "kube_pod_container_status_restarts_total|kafka-1")
    assert ramp[START - 1] == 3 and ramp[START + 9] == 4 and ramp[START + 39] == 7 and ramp[-1] == 7      # one step every 10 minutes, kept
    assert ramp == sorted(ramp) and all(float(v).is_integer() for v in ramp)
    assert flat[START - 1] == 3 and flat[START] == 5 and flat[-1] == 5
    assert records[0]["rejected"] == []


def test_a_stop_command_ends_a_generic_injection_early():
    inj = injector()
    inj.command({"cmd": "inject", "id": "c1", "effects": [effect(P99, "add", "flat", 100, 60)], "at": at(START).isoformat()})
    inj.command({"cmd": "stop", "id": "c1", "at": at(START + 15).isoformat()})
    out, records = play(inj, stream(100))
    assert series(out, P99)[START + 14] == 115 and series(out, P99)[START + 15] == 15
    assert records[1] == {"event": "stopped", "id": "c1", "stop": at(START + 15).isoformat(), "reason": "stop command"}


def test_generic_injections_are_exact_and_repeatable():
    first = run([effect(LAG, "add", "ramp", 6000, 60), effect(HEAP, "mult", "decay", 1.5, 30)])
    assert first[:2] == run([effect(LAG, "add", "ramp", 6000, 60), effect(HEAP, "mult", "decay", 1.5, 30)])[:2]


def test_limits_come_from_the_real_catalog_and_pod_limits():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    data = next((root / d for d in ("data_live", "data") if (root / d / "catalog").exists()), None)
    if data is None:
        pytest.skip("no data directory generated")
    from sentinel_sim.replay import DataSource
    inj = Injector.for_data(DataSource(data))
    assert inj.limits_of("jvm_heap_used_ratio|kafka-1") == (0.0, 1.0, False)
    assert inj.limits_of("container_cpu_throttled_ratio|trino-worker-0") == (0.0, 1.0, False)
    assert inj.limits_of("container_memory_working_set_bytes|orders-etl-consumer-0") == (0.0, 1.0 * GiB, False)
    assert inj.limits_of("container_cpu_usage_cores|trino-worker-1") == (0.0, 4.0, False)
    assert inj.limits_of("jvm_gc_time_ms_per_sec|kafka-0") == (0.0, 1000.0, False)
    for whole in ("trino_queued_queries|trino-lakehouse", "kafka_under_replicated_partitions|kafka-ingest", "airflow_queued_tasks|airflow-main"):
        assert inj.limits_of(whole) == (0.0, math.inf, True)
    for free in ("kafka_produce_request_p99_ms|kafka-ingest", "kafka_consumergroup_lag|orders-etl", "trino_query_p95_sec|trino-lakehouse"):
        assert inj.limits_of(free) == (0.0, math.inf, False)
    counters = [s for s in inj.series_ids if inj.is_counter(s)]
    assert counters and all("_total|" in s for s in counters) and len(counters) == sum("_total|" in s for s in inj.series_ids)
    assert set(inj.unit_of.values()) >= {"ratio", "bytes", "cores", "count", "queries", "ms"}


# ---------------------------------------------------------------------------------------- scorer
TOPOLOGY = Topology(entity_of={P99: ENTITY_OF[P99], HEAP: ENTITY_OF[HEAP], "cpu|far": "pod:far"},
                    neighbours={ENTITY_OF[P99]: {ENTITY_OF[HEAP]}, ENTITY_OF[HEAP]: {ENTITY_OF[P99]}})      # the broker and its cluster


def paged(page, minute, target) -> list:
    alert = {"time": at(minute).isoformat(), "target": target, "detector": "seasonal", "score": 0.8, "direction": "up",
             "explanation": "x", "measure": 7.0, "line": 6.0}
    return [{"kind": kind, "emitted_at": at(minute).isoformat(), "page": page, "page_id": f"p{page}", "n_alerts": 1, "absorbed": [],
             "alert": alert} for kind in ("opened", "paged")]


def answer_key(notes: list) -> list:
    """Three generic injections an hour apart, each with what its caller said about it, plus one rejected command."""
    inj = injector()
    for n, note in enumerate(notes):
        inj.command({"cmd": "inject", "id": f"c{n}", "effects": [effect(P99 if n != 1 else HEAP, "add", "flat", 100 if n != 1 else 0.3, 30)],
                     "at": at(60 * n + 10).isoformat(), **note})
    inj.command({"cmd": "inject", "id": "never", "effects": [effect(HEAP, "add", "flat", 0.9, 30)], "at": at(5).isoformat(), "severity": "subtle"})
    _, records = play(inj, stream(240))
    return records + [{"event": "published", "start": at(0).isoformat(), "end": at(1440).isoformat()}]


def test_the_scorer_reports_by_severity_and_never_matches_on_it():
    pages = paged(1, 15, P99) + paged(2, 80, "cpu|far") + paged(3, 140, HEAP)           # c0 paged, c1 not (a far page), c2 paged
    said = [{"severity": "noticeable", "intent": "produce latency x 7"}, {"severity": "subtle", "intent": "heap up a little"}, {}]
    r = live_score.score(answer_key(said), pages, TOPOLOGY)
    assert r["n_injections"] == 3 and [i["detected"] for i in r["injections"]] == [True, False, True]      # the rejected command is no injection
    assert r["by_severity"] == {"noticeable": {"injected": 1, "detected": 1, "times": [5.0]},
                                "subtle": {"injected": 1, "detected": 0, "times": []},
                                "not given": {"injected": 1, "detected": 1, "times": [10.0]}}
    assert [(i["severity"], i["intent"]) for i in r["injections"]] == [("noticeable", "produce latency x 7"), ("subtle", "heap up a little"), (None, None)]
    assert r["by_scenario"] == {"custom": {"injected": 3, "detected": 2, "times": [5.0, 10.0]}}
    text = live_score.markdown(r)
    assert "| subtle | 1 | 0 | - |" in text and "| not given | 1 | 1 | 10 |" in text
    assert "| c1 | custom | - | Mar 02 01:10 | Mar 02 01:40 | add flat 0.3 on jvm_heap_used_ratio\\|kafka-0 for 30 min | NO |" in text
    assert text.rstrip().splitlines()[-1].startswith("| Mar 02 01:20 |") and "| subtle | heap up a little |" in text

    # whatever the caller says about severity or intent, the verdicts are the same
    def verdicts(result):
        return [(i["id"], i["detected"], i["time_to_detect_min"], i["paged_by"], i["matching_page_outs"]) for i in result["injections"]], \
               result["false_page_outs"], result["n_detected"]
    for other in ([{}, {}, {}], [{"severity": "subtle"}] * 3, [{"severity": "noticeable", "intent": "pod:far"}, {"intent": "cpu|far"}, {"severity": "subtle"}]):
        assert verdicts(live_score.score(answer_key(other), pages, TOPOLOGY)) == verdicts(r)
