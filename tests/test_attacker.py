"""The attacker agent (sentinel_stream/attacker.py) with a scripted fake model: budget enforcement,
rejection of invalid effects, the record, and the replay of a recorded campaign. Blindness is in
tests/test_live_isolation.py. Data: the evaluated dataset's train split (Oct 7), only as a stream to
inject into; no labels are read."""
import json
from pathlib import Path

import pandas as pd
import pytest

from sentinel_stream import attacker as A
from sentinel_stream import wire
from tests.test_triage import FakeLLM, text, tool_use

DATA = Path(__file__).resolve().parents[1] / "data"
needs_data = pytest.mark.skipif(not (DATA / "metrics").exists(), reason="dataset not generated")
pytestmark = [needs_data, pytest.mark.filterwarnings("ignore::RuntimeWarning")]
START, END = "2026-10-07 00:00", "2026-10-07 12:00"


@pytest.fixture(scope="module")
def ds():
    from sentinel_sim.replay import DataSource
    return DataSource(DATA)


def config(**kw):
    return A.Config(model="fake", effort=None, prices_per_mtok={"fake": {"input": 2.0, "output": 10.0, "cache_read": 0.1,
                                                                          "cache_write": 2.5}}, **kw)


def scenario(name="trino_heavy_query", start_in=0, severity="noticeable", target=None, **params):
    out = {"scenario": name, "params": params, "intent": f"a {name} for the test", "severity": severity,
           "start_in_minutes": start_in}
    return {**out, "target": target} if target else out


GOOD = {"series_id": "kafka_produce_request_p99_ms|kafka-ingest", "op": "mult", "shape": "ramp", "magnitude": 3.0, "duration_min": 40}
TOO_BIG = {"series_id": "jvm_heap_used_ratio|kafka-0", "op": "add", "shape": "flat", "magnitude": 0.95, "duration_min": 30}
UNKNOWN = {"series_id": "made_up|nowhere", "op": "add", "shape": "flat", "magnitude": 1, "duration_min": 10}
COUNTER_MULT = {"series_id": "kube_pod_container_status_restarts_total|kafka-0", "op": "mult", "shape": "flat", "magnitude": 2,
                "duration_min": 10}


NOVELTY = "a failure none of the named scenarios covers, for the test"
NO_QUOTA = A.Budget(max_named_share=1.0)                    # tests about another rule start with a named scenario
NO_GAP = A.Budget(min_gap_minutes=0, max_named_share=1.0)


def describe(*series, id_="d"):
    return tool_use("describe_normal", {"series_ids": list(series)}, id_)


def touched(ds, name="trino_heavy_query", target=None):
    probe = A.Attacker(ds, config())
    return probe.scenario_series(name, target or A.SCENARIOS[name].targets[0])


def effects(*items, intent="slow produce", severity="subtle", **more):
    return {"effects": list(items), "intent": intent, "severity": severity, "novelty": NOVELTY, **more}


def campaign(ds, script, start=START, end=END, every=120, **kw):
    attacker = A.Attacker(ds, config(wake_every_minutes=every, **kw), window=(start, end))
    agent = A.Agent(attacker, create=FakeLLM(script))
    return attacker, A.run_campaign(attacker, agent, start, end)


# ------------------------------------------------------------------------------------------ budget
def test_at_most_three_a_day_and_two_active_at_once_are_enforced_in_code(ds):
    # one wake that tries four short scenarios in a row, and a third overlapping injection
    script = [[describe(*touched(ds))],
              [tool_use("inject_scenario", scenario(duration_min=25), "a")],
              [tool_use("inject_scenario", scenario(start_in=5, duration_min=25), "b")],
              [tool_use("inject_scenario", scenario(start_in=10, duration_min=25), "c")],       # 3 active at 00:10: refused
              [tool_use("inject_scenario", scenario(start_in=60, duration_min=25), "d")],       # third of the day
              [tool_use("inject_scenario", scenario(start_in=120, duration_min=25), "e")],      # fourth of the day: refused
              [text("tried five")]]
    attacker, record = campaign(ds, script, every=720, budget=NO_GAP)
    assert [c["id"] for c in record["commands"]] == ["atk-001", "atk-002", "atk-003"]
    reasons = [r["reasons"][0] for r in record["rejections"]]
    assert len(reasons) == 2 and "at most 2 injections may be active" in reasons[0] and "at most 3 a day" in reasons[1]
    calls = record["wakes"][0]["tool_calls"]
    assert [c["is_error"] for c in calls] == [False, False, False, True, False, True]
    assert "rejected: budget" in calls[3]["result"]["error"]


def test_injection_starts_must_be_three_hours_apart(ds):
    script = [[describe(*touched(ds))],
              [tool_use("inject_scenario", scenario(duration_min=25), "a")],
              [tool_use("inject_scenario", scenario(start_in=179, duration_min=25), "b")],      # 2 h 59 min later: refused
              [tool_use("inject_scenario", scenario(start_in=180, duration_min=25), "c")],      # 3 h later: accepted
              [text("spaced")]]
    attacker, record = campaign(ds, script, every=720, budget=NO_QUOTA)
    assert [c["at"][11:16] for c in record["commands"]] == ["00:00", "03:00"]
    assert "at least 3 h apart" in record["rejections"][0]["reasons"][0] and "atk-001" in record["rejections"][0]["reasons"][0]
    assert attacker.config.budget.min_gap_minutes == 180


def test_only_series_described_in_the_same_wake_may_be_touched(ds):
    series = touched(ds)
    script = [[tool_use("inject_scenario", scenario(), "a")],                                    # nothing described
              [describe(*series[:-1])], [tool_use("inject_scenario", scenario(), "b")],          # one series short
              [describe(GOOD["series_id"])],
              [tool_use("inject_effects", effects(GOOD, {**GOOD, "series_id": "kafka_failed_produce_requests_per_sec|kafka-ingest",
                                                         "op": "add", "magnitude": 0.5}), "c")],    # second effect undescribed
              [text("partly")],
              [tool_use("inject_effects", effects(GOOD, start_in_minutes=0), "e")], [text("next wake")]]   # wake 2: not described there
    attacker, record = campaign(ds, script, end="2026-10-07 06:00", every=180, budget=NO_GAP)
    w0, w1 = record["wakes"]
    errors = [c["result"].get("error", "") for c in w0["tool_calls"] if c["tool"].startswith("inject")]
    assert "11 of the series" in errors[0] and "1 of the series" in errors[1] and series[-1] in errors[1]
    partly = w0["tool_calls"][-1]["result"]
    assert partly["accepted"] and "describe_normal was not called" in partly["rejected_effects"][0]
    assert [e["series_id"] for e in record["commands"][0]["effects"]] == [GOOD["series_id"]]
    assert "describe_normal was not called" in w1["tool_calls"][0]["result"]["error"]                # reset at every wake
    assert len(record["commands"]) == 1 and len(record["rejections"]) == 4


def test_a_generic_incident_needs_novelty_and_its_overlap_with_the_scenarios_is_recorded(ds):
    trino = [x for x in touched(ds) if x.startswith(("trino_queued", "trino_query_p95"))]
    script = [[describe(*trino)],
              [tool_use("inject_effects", {**effects(*({"series_id": x, "op": "mult", "shape": "flat", "magnitude": 2.0,
                                                        "duration_min": 30} for x in trino)), "novelty": " "}, "a")],
              [tool_use("inject_effects", effects(*({"series_id": x, "op": "mult", "shape": "flat", "magnitude": 2.0,
                                                     "duration_min": 30} for x in trino)), "b")],
              [text("one")]]
    attacker, record = campaign(ds, script, every=720)
    calls = record["wakes"][0]["tool_calls"]
    assert calls[1]["is_error"] and "novelty" in calls[1]["result"]["error"]
    assert "overlap" not in calls[2]["result"]                                                     # recorded, not shown
    inj, = record["injections"]
    assert inj["novelty"] == NOVELTY
    assert inj["overlap"]["trino_heavy_query"] == {"shared": 2, "of_incident": 2, "jaccard": round(2 / 11, 3)}
    assert all(v["shared"] == 0 for k, v in inj["overlap"].items() if k != "trino_heavy_query")
    assert "inject_effects" in [d["name"] for d in A.definitions()] and \
        "novelty" in next(d for d in A.definitions() if d["name"] == "inject_effects")["input_schema"]["required"]


def test_named_scenarios_may_make_up_at_most_60_percent_of_the_campaign(ds):
    # named first: 1 of 1 is refused; a generic, then a named one (1 of 2) passes; a second named (2 of 3) is refused
    script = [[describe(*touched(ds), GOOD["series_id"])],
              [tool_use("inject_scenario", scenario(duration_min=25), "a")],
              [tool_use("inject_effects", effects(GOOD), "b")],
              [tool_use("inject_scenario", scenario(start_in=60, duration_min=25), "c")],
              [tool_use("list_active", {}, "d")],
              [tool_use("inject_scenario", scenario(start_in=120, duration_min=25), "e")],
              [text("one of each")]]
    attacker, record = campaign(ds, script, every=720, budget=A.Budget(min_gap_minutes=0))
    assert attacker.config.budget.max_named_share == 0.6
    assert [c.get("scenario", "generic") for c in record["commands"]] == ["generic", "trino_heavy_query"]
    first, second = (r["reasons"][0] for r in record["rejections"])
    assert "at most 60%" in first and "0 of the 0 so far" in first and "Inject a generic incident (inject_effects)" in first
    assert "1 of the 2 so far" in second and "would make 2 of 3" in second
    active = record["wakes"][0]["tool_calls"][4]["result"]
    assert (active["named_so_far"], active["injections_so_far"], active["next_may_be_named"]) == (1, 2, False)
    assert "the next one may NOT be a named scenario" in record["wakes"][0]["messages"][0]["content"]   # 0 so far


def test_the_named_share_is_a_running_cap_with_60_percent_itself_allowed(ds):
    attacker = A.Attacker(ds, config())
    attacker.now = pd.Timestamp(START, tz="UTC")
    wake = A.Agent(attacker).wake_text(attacker.now, False)
    assert "the next one may NOT be a named scenario" in wake                  # 1 of 1 would be 100%
    for kinds, allowed in ((["generic"], True), (["generic", "scenario"], False), (["generic", "scenario", "generic"], True),
                           (["generic", "scenario", "generic", "scenario"], True),                  # 3 of 5 = 60%: allowed
                           (["generic", "scenario", "generic", "scenario", "scenario"], False)):    # 4 of 6: refused
        attacker.injections = {f"x{n}": {"kind": k} for n, k in enumerate(kinds)}
        assert (attacker.check_named_share() is None) is allowed, kinds
    prompt = A.system_prompt(attacker.config)
    assert "at most 60% of the campaign's injections" in prompt and "{named_share}" not in prompt


def test_an_injection_outside_the_window_or_in_the_past_is_refused(ds):
    attacker = A.Attacker(ds, config(budget=NO_QUOTA), window=(START, END))
    attacker.now = pd.Timestamp(END, tz="UTC") - pd.Timedelta("30min")
    attacker.described = set(touched(ds))
    out = attacker.inject_scenario(**scenario(start_in=60))
    assert "outside the campaign window" in out["error"] and attacker.commands == []


# --------------------------------------------------------------------------------- invalid effects
def test_invalid_effects_are_rejected_with_the_injectors_reasons_and_only_valid_ones_are_sent(ds):
    script = [[describe(TOO_BIG["series_id"], COUNTER_MULT["series_id"], GOOD["series_id"])],
              [tool_use("inject_effects", effects(TOO_BIG, UNKNOWN, COUNTER_MULT, GOOD), "a")],
              [tool_use("inject_effects", effects(TOO_BIG, UNKNOWN, intent="all bad"), "b")],
              [tool_use("inject_effects", {"effects": [GOOD], "intent": "", "severity": "loud"}, "c")],
              [text("done")]]
    attacker, record = campaign(ds, script, every=720)
    _, first, second, third = record["wakes"][0]["tool_calls"]
    assert not first["is_error"] and len(first["result"]["rejected_effects"]) == 3
    joined = " ".join(first["result"]["rejected_effects"])
    assert "above its limit of 1" in joined and "not in the catalog" in joined and "a counter only takes add" in joined
    assert second["is_error"] and "rejected" in second["result"]["error"] and third["is_error"]
    assert all(w in third["result"]["error"] for w in ("intent", "severity", "novelty"))
    sent, = record["commands"]
    assert [e["series_id"] for e in sent["effects"]] == [GOOD["series_id"]]
    assert len(record["rejections"]) == 3 and record["rejections"][0]["partly_accepted"]


def test_stop_ends_only_its_own_running_injection(ds):
    script = [[describe(*touched(ds))], [tool_use("inject_scenario", scenario(duration_min=50), "a")], [text("one")],
              [tool_use("stop", {"injection_id": "atk-001"}, "b")], [tool_use("stop", {"injection_id": "atk-001"}, "c")],
              [tool_use("stop", {"injection_id": "nope"}, "d")], [text("stopped")]]
    attacker, record = campaign(ds, script, end="2026-10-07 01:00", every=30, budget=NO_QUOTA)
    assert [c["cmd"] for c in record["commands"]] == ["inject", "stop"]
    assert record["commands"][1] == {"cmd": "stop", "id": "atk-001", "at": "2026-10-07T00:30:00+00:00"}
    second = record["wakes"][1]["tool_calls"]
    assert [c["is_error"] for c in second] == [False, True, True]


# ------------------------------------------------------------------------------------------ record
def test_every_wake_records_its_calls_commands_and_reasoning(ds):
    script = [[tool_use("list_scenarios", {}, "a"), tool_use("describe_normal", {"series_id": GOOD["series_id"]}, "b")],
              [tool_use("inject_effects", effects(GOOD, intent="a slow broker disk"), "c")],
              [text("Injected a slow-disk incident on the produce path.")], [text("Nothing this time: one is running.")]]
    attacker, record = campaign(ds, script, end="2026-10-07 04:00")
    w0, w1 = record["wakes"]
    assert [c["tool"] for c in w0["tool_calls"]] == ["list_scenarios", "describe_normal", "inject_effects"]
    assert w0["note"].startswith("Injected") and w1["note"].startswith("Nothing") and len(w0["commands"]) == 1
    assert "It is now 2026-10-07 00:00 UTC" in w0["messages"][0]["content"] and "Today so far: 1" in w1["messages"][0]["content"]
    assert record["cost_usd"] > 0 and record["injections"][0]["layers"] == [2]
    normal = w0["tool_calls"][1]["result"]
    assert normal["readings"] > 1000 and pd.Timestamp(normal["last_at"]) < pd.Timestamp(START, tz="UTC")   # nothing from now on


def test_a_wake_whose_tool_budget_runs_out_still_ends_with_a_note(ds):
    attacker2 = A.Attacker(ds, config(budget=A.Budget(max_tool_calls_per_wake=2)), window=(START, END))
    llm = FakeLLM([[tool_use("list_active", {}, f"x{i}")] for i in range(2)] + [[text("out of calls")]])   # tools off: text
    w = A.Agent(attacker2, create=llm).wake(pd.Timestamp(START, tz="UTC"))
    assert len(w["tool_calls"]) == 2 and w["stop"] == "a per-wake budget ran out" and w["note"] == "out of calls"
    assert llm.requests[-1]["tool_choice"] == {"type": "none"}


# ------------------------------------------------------------------------------------------ replay
@pytest.fixture(scope="module")
def recorded(ds, tmp_path_factory):
    failed = "kafka_failed_produce_requests_per_sec|kafka-ingest"
    script = [[describe(*touched(ds, "producer_crashloop"), GOOD["series_id"], failed)],
              [tool_use("inject_scenario", scenario("producer_crashloop", duration_min=30), "a")],
              [tool_use("inject_effects", effects(GOOD, {**GOOD, "series_id": failed, "op": "add", "shape": "flat", "magnitude": 0.5,
                                                         "start_in_minutes": 10},
                                                  intent="a broker's disk slows down", start_in_minutes=15), "b")],
              [text("two")], [text("nothing")],
              [describe(*touched(ds, "broker_gc_pressure", "kafka-2"))],
              [tool_use("inject_scenario", scenario("broker_gc_pressure", target="kafka-2", severity="subtle", magnitude=0.6,
                                                    duration_min=150), "c")], [text("gc")],
              [tool_use("stop", {"injection_id": "atk-003"}, "d")], [text("stopped early")]]
    _, record = campaign(ds, script, end="2026-10-07 08:00", budget=NO_GAP)
    record_path, control = A.save(record, tmp_path_factory.mktemp("campaign"))
    return record, record_path, control


def _bytes(messages):
    return [wire.dumps(m) for m in messages]


def test_a_recorded_campaign_replays_to_a_byte_identical_stream_and_record(ds, recorded):
    record, record_path, control = recorded
    assert [c["cmd"] for c in record["commands"]] == ["inject", "inject", "inject", "stop"]
    loaded = A.load_commands(control)
    assert loaded == json.loads(json.dumps(record["commands"]))
    sent1, key1 = A.replay(ds, START, END, record["commands"])
    sent2, key2 = A.replay(ds, START, END, loaded)                     # from the file, without the LLM
    assert _bytes(sent1) == _bytes(sent2) and _bytes(key1) == _bytes(key2)
    started = [k for k in key1 if k["event"] == "started"]
    assert [k["id"] for k in started] == ["atk-001", "atk-002", "atk-003"]
    assert [k["intent"] for k in started][1] == "a broker's disk slows down" and started[2]["severity"] == "subtle"
    assert [k["evidence"] for k in started] == [["metrics", "logs", "k8s_events", "changes"], ["metrics"], ["metrics", "logs", "changes"]]
    stopped = {k["id"]: k for k in key1 if k["event"] == "stopped"}
    assert stopped["atk-003"]["reason"] == "stop command" and stopped["atk-003"]["stop"] == "2026-10-07T06:00:00+00:00"
    assert not [k for k in key1 if k["event"] == "rejected"]
    # the injections changed the stream, and only from their start on
    plain, _ = A.replay(ds, START, END, [])
    first_diff = next(i for i, (a, b) in enumerate(zip(_bytes(plain), _bytes(sent1))) if a != b)
    assert wire.event_time(sent1[first_diff]) >= pd.Timestamp(START, tz="UTC")


def test_the_attacker_tools_are_the_seven_and_errors_never_raise(ds):
    assert [d["name"] for d in A.definitions()] == ["list_platform", "describe_normal", "list_scenarios", "inject_scenario",
                                                    "inject_effects", "list_active", "stop"]
    attacker = A.Attacker(ds, config())
    attacker.now = pd.Timestamp(START, tz="UTC")
    for name, args in (("nope", {}), ("describe_normal", {"series_id": "x|y"}), ("inject_scenario", {"wrong": 1}),
                       ("inject_scenario", scenario("not_a_scenario")), ("stop", {"injection_id": "x"})):
        result, is_error = A.call(attacker, name, args)
        assert is_error and "error" in result, (name, result)
    platform, _ = A.call(attacker, "list_platform")
    assert "pod:kafka-0" in platform["entities"] and platform["relationships"]


@pytest.mark.kafka
def test_through_the_publisher_the_recorded_control_stream_gives_what_replay_gives(ds, recorded):
    """The recorded commands put on a control topic before the publisher starts (a schedule loaded in
    advance): the telemetry and the injection record it writes are byte for byte replay()'s."""
    import uuid

    from sentinel_stream import broker, injector
    from sentinel_stream.publisher import publish
    if not broker.reachable(broker.DEFAULT_BOOTSTRAP):
        pytest.skip("no Kafka broker")
    record, _, control = recorded
    b, prefix = broker.DEFAULT_BOOTSTRAP, f"test-{uuid.uuid4().hex[:8]}-"
    topic = {k: prefix + k for k in ("telemetry", "control", "record")}
    try:
        injector.send(b, topic["control"], A.load_commands(control))
        publish(ds, START, END, bootstrap=b, topic=topic["telemetry"], control=topic["control"], answer_key=topic["record"],
                stream_version=2)
        sent, key = A.replay(ds, START, END, A.load_commands(control))
        assert [wire.dumps(m) for m in broker.read_topic(b, topic["telemetry"])] == _bytes(sent)
        assert [wire.dumps(m) for m in broker.read_topic(b, topic["record"])] == _bytes(key)
    finally:
        from confluent_kafka.admin import AdminClient
        existing = AdminClient(broker.client_config(b)).list_topics(timeout=10).topics
        broker.delete_topics(b, [t for t in existing if t.startswith(prefix)])


def test_the_mcp_attacker_group_serves_the_same_seven_tools_and_injects_through_the_same_check(ds):
    pytest.importorskip("mcp")
    import anyio
    from mcp import Client

    sent = []
    attacker = A.Attacker(ds, config(budget=NO_QUOTA), send=sent.append)
    attacker.now = pd.Timestamp(START, tz="UTC")

    async def session():
        async with Client(A.build_mcp_server(attacker)) as client:
            tools = (await client.list_tools()).tools
            await client.call_tool("describe_normal", {"series_ids": touched(ds) + [TOO_BIG["series_id"]]})
            bad = await client.call_tool("inject_effects", effects(TOO_BIG))
            good = await client.call_tool("inject_scenario", scenario())
            return tools, bad, good

    tools, bad, good = anyio.run(session)
    assert [t.name for t in tools] == [d["name"] for d in A.definitions()]
    assert [t.input_schema for t in tools] == [d["input_schema"] for d in A.definitions()]
    assert bad.is_error and "above its limit" in bad.content[0].text
    assert not good.is_error and [c["id"] for c in sent] == ["atk-001"]


@pytest.mark.llm
def test_one_real_wake_stays_inside_the_tools_and_the_budget(ds):
    import os
    pytest.importorskip("anthropic")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY is not set")
    cfg = A.Config.load(runs_dir="")
    attacker = A.Attacker(ds, cfg, window=(START, END))
    w = A.Agent(attacker).wake(pd.Timestamp(START, tz="UTC"))
    assert w["stop"] is None or "budget" in w["stop"]
    assert all(c["tool"] in {d["name"] for d in A.definitions()} for c in w["tool_calls"])
    assert len(attacker.commands) <= cfg.budget.max_per_day and w["note"] and w["cost_usd"] > 0
    for cmd in attacker.commands:                                     # what was sent replays without a refusal
        assert cmd["intent"] and cmd["severity"] in ("noticeable", "subtle")
    _, key = A.replay(ds, START, END, attacker.commands)
    assert not [k for k in key if k["event"] == "rejected"]
