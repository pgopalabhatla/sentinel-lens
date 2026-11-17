"""The triage agent and its dispatcher, and the MCP server's tools against the investigator's. No API
is called here: the model is a scripted fake (`FakeLLM`). Real-API tests: test_triage_llm.py (marker `llm`)."""
import json
from types import SimpleNamespace

import pytest

from sentinel_stream import context_tools as ct
from sentinel_stream.dispatcher import Dispatcher, MemoryTopics
from sentinel_stream.investigator import Budget, Config, Investigator, parse_note, validate_note
from tests import triage_world as world
from tests.test_context_tools import ctx_of, ds, store  # noqa: F401  (fixtures)


# --------------------------------------------------------------------------------- the fake model
class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def tool_use(name, args=None, id_=None):
    return {"type": "tool_use", "id": id_ or f"toolu_{name}", "name": name, "input": args or {}}


def text(s):
    return {"type": "text", "text": s}


class FakeLLM:
    """Answers each request with the next item of `script` (a list of content-block lists, or a
    callable request -> content). Records every request. `seconds` per request move the clock."""

    def __init__(self, script, clock=None, seconds=1.0, usage=(1000, 100)):
        self.script, self.clock, self.seconds, self.usage = list(script), clock, seconds, usage
        self.requests = []

    def __call__(self, **kwargs):
        self.requests.append({**kwargs, "messages": json.loads(json.dumps(kwargs["messages"], default=_plain)),
                              "at": self.clock.t if self.clock else None})
        item = self.script.pop(0) if self.script else [text("{}")]
        if isinstance(item, Exception):
            raise item
        if self.clock is not None:
            took = self.seconds.pop(0) if isinstance(self.seconds, list) else self.seconds
            if took > kwargs["timeout"]:                     # as the SDK does: give up at the timeout
                self.clock.t += kwargs["timeout"]
                late = TimeoutError("request timed out")
                late.is_timeout = True
                raise late
            self.clock.t += took
        content = item(kwargs) if callable(item) else item
        stop = "tool_use" if any(b["type"] == "tool_use" for b in content) else "end_turn"
        return SimpleNamespace(content=content, stop_reason=stop, model=kwargs["model"],
                               usage=SimpleNamespace(input_tokens=self.usage[0], output_tokens=self.usage[1],
                                                     cache_read_input_tokens=0, cache_creation_input_tokens=0))


def _plain(o):
    return o if isinstance(o, dict) else vars(o)


def note_json(page_id, answer="yes", evidence=("c1",), entities=("consumer_group:grp",), **extra):
    degraded = ["lag"] if evidence else ["none"]
    return json.dumps({"page_id": page_id, "needs_human": answer, "confidence": 0.8,
                       "cause": {"category": "capacity", "description": "lag grows"},
                       "impact": {"degraded": degraded, "description": "lag is growing", "evidence": list(evidence[:1])},
                       "affected_entities": list(entities), "evidence": [{"tool_call_id": c, "finding": "seen"} for c in evidence],
                       "next_step": "check the consumer", **extra})


def investigator(llm, tmp_path, clock=None, **budget):
    cfg = Config(model="fake-model", effort=None, fallbacks=None, budget=Budget(**budget), runs_dir=str(tmp_path / "runs"),
                 prices_per_mtok={"fake-model": {"input": 2.0, "output": 10.0, "cache_read": 0.1, "cache_write": 2.5}})
    return Investigator(cfg, create=llm, clock=clock or Clock(), run_name="t", sleep=lambda s: None)


def test_a_valid_note_after_some_tool_calls(store, ds, tmp_path):
    ctx = ctx_of(store, ds, 1)
    llm = FakeLLM([[tool_use("get_page")], [tool_use("query_metric", {"series_id": world.LAG})],
                   [text(note_json(ctx.page_id, evidence=("c1", "c2")))]])
    note = investigator(llm, tmp_path).investigate(ctx)
    assert note["valid"] and note["needs_human"] == "yes" and note["tool_calls"] == 2 and note["advisory"]
    assert note["cause"] == {"category": "capacity", "description": "lag grows"} and note["impact"]["degraded"] == ["lag"]
    assert note["paged_at"] == note["as_of"] and not note["follow_up"]
    assert [e["tool"] for e in note["evidence"]] == ["get_page", "query_metric"]
    assert note["tokens"]["total"] == 3 * 1100 and note["cost_usd_estimate"] == pytest.approx(3 * (1000 * 2 + 100 * 10) / 1e6)
    assert all(r["tool_choice"] == {"type": "auto"} for r in llm.requests)
    result = llm.requests[1]["messages"][-1]["content"][0]
    assert result["type"] == "tool_result" and json.loads(result["content"])["call_id"] == "c1"


def test_the_tool_call_budget_is_never_exceeded(store, ds, tmp_path):
    ctx = ctx_of(store, ds, 1)
    always = lambda req: [tool_use("get_page", id_=f"a{len(req['messages'])}"), tool_use("dag_runs", id_=f"b{len(req['messages'])}")]
    llm = FakeLLM([always] * 8 + [lambda req: [text(note_json(ctx.page_id))]])
    note = investigator(llm, tmp_path, max_tool_calls=15).investigate(ctx)
    assert note["tool_calls"] == 15 and note["budget_exhausted"] == "tool_calls" and note["valid"]
    last = llm.requests[-1]
    assert last["tool_choice"] == {"type": "none"}
    assert "budget is used up" in json.dumps(last["messages"][-1])
    errors = [b for m in llm.requests[-1]["messages"] if m["role"] == "user" and isinstance(m["content"], list)
              for b in m["content"] if b.get("type") == "tool_result" and b.get("is_error")]
    assert len(errors) == 1                              # 8 x 2 asked, 15 run, the 16th answered with an error


def test_the_wall_time_budget_switches_tools_off_then_stops(store, ds, tmp_path):
    ctx = ctx_of(store, ds, 1)
    clock = Clock()
    script = [[tool_use("get_page", id_=f"t{i}")] for i in range(4)] + [[text(note_json(ctx.page_id))]]
    llm = FakeLLM(script, clock=clock, seconds=[30.0, 30.0, 30.0, 30.0, 10.0])
    note = investigator(llm, tmp_path, clock=clock, max_seconds=120, final_reserve_seconds=25).investigate(ctx)
    # tools on at 0, 30, 60 (timeouts 95, 65, 35); at 90 the timeout is 5 s, the request needs 30 and is cut at 95;
    # tools go off and the note is asked for with the 25 s kept back, and comes at 105
    assert [(r["at"], r["timeout"], r["tool_choice"]["type"]) for r in llm.requests] == \
        [(0.0, 95.0, "auto"), (30.0, 65.0, "auto"), (60.0, 35.0, "auto"), (90.0, 5.0, "auto"), (95.0, 25.0, "none")]
    assert note["valid"] and note["budget_exhausted"] == "time" and note["seconds"] == 105.0 and note["tool_calls"] == 3


def test_no_request_runs_past_the_deadline(store, ds, tmp_path):
    ctx = ctx_of(store, ds, 1)
    clock = Clock()
    llm = FakeLLM([[tool_use("get_page", id_=f"t{i}")] for i in range(20)], clock=clock, seconds=50.0)
    note = investigator(llm, tmp_path, clock=clock, max_seconds=120, final_reserve_seconds=25).investigate(ctx)
    assert all(r["at"] + r["timeout"] <= 120 for r in llm.requests) and clock.t <= 120
    assert not note["valid"] and note["needs_human"] == "unsure" and note["budget_exhausted"] == "time"


def test_the_token_cap_stops_the_investigation(store, ds, tmp_path):
    ctx = ctx_of(store, ds, 1)
    llm = FakeLLM([[tool_use("get_page", id_=f"t{i}")] for i in range(20)], usage=(50_000, 1_000))
    note = investigator(llm, tmp_path, max_tokens=200_000).investigate(ctx)
    assert note["tokens"]["total"] <= 200_000
    assert note["budget_exhausted"] == "tokens" and not note["valid"] and note["needs_human"] == "unsure"


def test_an_invalid_note_is_sent_back_once_with_the_reasons(store, ds, tmp_path):
    ctx = ctx_of(store, ds, 1)
    llm = FakeLLM([[tool_use("get_page")], [text("Looks real to me.")],
                   [text("```json\n" + note_json(ctx.page_id) + "\n```")]])
    note = investigator(llm, tmp_path).investigate(ctx)
    assert note["valid"] and note["attempts"] == 2 and note["invalid_answers"] == [["the answer holds no JSON object"]]
    retry = llm.requests[-1]
    assert retry["tool_choice"] == {"type": "none"} and "not valid" in retry["messages"][-1]["content"]


def test_two_invalid_notes_give_an_unsure_note_and_no_third_try(store, ds, tmp_path):
    ctx = ctx_of(store, ds, 1)
    llm = FakeLLM([[tool_use("get_page")], [text(note_json(ctx.page_id, evidence=("c7",)))],
                   [text(note_json(ctx.page_id, answer="probably"))], [text(note_json(ctx.page_id))]])
    note = investigator(llm, tmp_path).investigate(ctx)
    assert len(llm.requests) == 3 and not note["valid"] and note["needs_human"] == "unsure" and note["confidence"] == 0
    assert "not made" in note["invalid_answers"][0][0] and "needs_human" in note["invalid_answers"][1][0]


def test_a_refused_or_failing_request_ends_with_an_unsure_note(store, ds, tmp_path):
    ctx = ctx_of(store, ds, 1)
    flaky = RuntimeError("overloaded")
    flaky.retryable = True
    llm = FakeLLM([flaky, [tool_use("get_page")], [text(note_json(ctx.page_id))]])
    assert investigator(llm, tmp_path).investigate(ctx)["valid"]                   # retried once
    llm = FakeLLM([ValueError("bad request")])
    note = investigator(llm, tmp_path).investigate(ctx)
    assert not note["valid"] and "API request failed" in note["stop"]


@pytest.mark.parametrize("change, fragment", [
    ({"needs_human": "maybe"}, "needs_human"), ({"needs_human": True}, "needs_human"), ({"verdict": "real"}, "unknown fields"),
    ({"confidence": 1.5}, "confidence"), ({"confidence": True}, "confidence"),
    ({"cause": "a deploy"}, "cause must be"), ({"cause": {"category": "bad luck", "description": "x"}}, "cause.category"),
    ({"cause": {"category": "change", "description": ""}}, "cause.description"),
    ({"cause": {"category": "change"}}, "cause must be"),
    ({"impact": {"degraded": ["lag"], "description": "x"}}, "impact must be"),
    ({"impact": {"degraded": [], "description": "x", "evidence": []}}, "impact.degraded"),
    ({"impact": {"degraded": ["throughput"], "description": "x", "evidence": ["c1"]}}, "impact.degraded"),
    ({"impact": {"degraded": ["none", "lag"], "description": "x", "evidence": ["c1"]}}, "cannot go with"),
    ({"impact": {"degraded": ["lag"], "description": "x", "evidence": []}}, "must cite"),
    ({"impact": {"degraded": ["lag"], "description": "x", "evidence": ["c8"]}}, "not made"),
    ({"impact": {"degraded": ["lag"], "description": " ", "evidence": ["c1"]}}, "impact.description"),
    ({"page_id": "other"}, "page_id"), ({"affected_entities": ["pod:ghost"]}, "entity graph"),
    ({"affected_entities": "pod:p-a"}, "list of strings"), ({"evidence": [{"tool_call_id": "c9", "finding": "x"}]}, "not made"),
    ({"evidence": [{"tool_call_id": "c1"}]}, "evidence must be"), ({"evidence": []}, "at least one"),
    ({"next_step": " "}, "next_step"), ({"extra": 1}, "unknown fields")])
def test_validate_note_rejects(change, fragment):
    note = {**json.loads(note_json("P", evidence=("c1",), entities=("pod:p-a",))), **change}
    errors = validate_note(note, "P", {"c1"}, {"pod:p-a"})
    assert errors and any(fragment in e for e in errors), errors


def test_validate_note_accepts_unsure_without_evidence_and_parse_finds_the_object():
    note = json.loads(note_json("P", answer="unsure", evidence=(), entities=()))
    assert validate_note(note, "P", set(), set()) == []
    assert parse_note('Here it is: {"a": 1} done')[0] == {"a": 1}
    assert parse_note("no json")[0] is None and parse_note("[1, 2]")[0] is None


def test_the_transcript_is_saved_and_holds_no_api_key(store, ds, tmp_path, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-THIS-MUST-NOT-APPEAR")
    ctx = ctx_of(store, ds, 1)
    llm = FakeLLM([[tool_use("get_page")], [text(note_json(ctx.page_id))]])
    note = investigator(llm, tmp_path).investigate(ctx)
    saved = (tmp_path / "runs/t/page-0001.json").read_text()
    assert note["transcript"].endswith("page-0001.json") and "THIS-MUST-NOT-APPEAR" not in saved
    t = json.loads(saved)
    assert t["tool_calls"][0]["tool"] == "get_page" and t["note"]["needs_human"] == "yes" and len(t["messages"]) == 4


# ------------------------------------------------------------------------------------ dispatcher
def topics_with(n_pages=None):
    pages, alerts = world.pages_and_alerts()
    return MemoryTopics(telemetry=world.telemetry(), alerts=alerts, pages=pages[:n_pages] if n_pages else pages)


class FakeAgent:
    def __init__(self, fail_on=()):
        self.seen, self.fail_on = [], set(fail_on)

    def __call__(self, ctx):
        page = ct.get_page(ctx)
        self.seen.append((ctx.page, ctx.now, page["n_alerts"]))
        if ctx.page in self.fail_on:
            raise RuntimeError("crash")
        return {"page_id": ctx.page_id, "needs_human": "no", "confidence": 0.5, "tool_calls": 0, "page": ctx.page}


def notes(io):
    return [r for r in io.topics["triage"] if r["type"] == "note"]


def links(io):
    return [r for r in io.topics.get("triage", []) if r["type"] == "link"]


def test_dispatcher_investigates_every_page_out_once_as_it_was_then(ds):
    io, agent = topics_with(), FakeAgent()
    Dispatcher(io, ds, agent, log=lambda line: None).run(until_end=True)
    assert sorted(agent.seen) == [(1, world.at("01:00"), 1), (2, world.at("02:00"), 1), (3, world.at("03:00"), 1)]
    assert sorted(n["page"] for n in notes(io)) == [1, 2, 3]
    assert [(l["page"], l["absorbed"], l["notes"]) for l in links(io)] == \
        [(1, [2], ["2026-01-05T01:00:00Z|consumer_group:grp", "2026-01-05T02:00:00Z|pod:p-a"])]


def test_dispatcher_restart_does_not_investigate_or_write_again(ds):
    io = topics_with()
    Dispatcher(io, ds, FakeAgent(), log=lambda line: None).run(until_end=True)
    before = list(io.topics["triage"])
    again = FakeAgent()
    Dispatcher(io, ds, again, log=lambda line: None).run(until_end=True)
    assert again.seen == [] and io.topics["triage"] == before


def test_dispatcher_after_a_crash_redoes_only_the_missing_note(ds):
    io = topics_with()
    Dispatcher(io, ds, FakeAgent(fail_on={2}), log=lambda line: None).run(until_end=True)
    assert sorted(n["page"] for n in notes(io)) == [1, 3]
    second = FakeAgent()
    Dispatcher(io, ds, second, log=lambda line: None).run(until_end=True)
    assert [s[0] for s in second.seen] == [2] and sorted(n["page"] for n in notes(io)) == [1, 2, 3]
    assert len(links(io)) == 1


def test_dispatcher_follows_a_growing_topic(ds):
    pages, alerts = world.pages_and_alerts()
    io = MemoryTopics(telemetry=world.telemetry(), alerts=alerts, pages=pages[:4])
    first = FakeAgent()
    Dispatcher(io, ds, first, log=lambda line: None).run(until_end=True)
    assert sorted(s[0] for s in first.seen) == [1, 2] and links(io) == []
    io.topics["pages"] += pages[4:]
    second = FakeAgent()
    Dispatcher(io, ds, second, log=lambda line: None).run(until_end=True)
    assert [s[0] for s in second.seen] == [3] and len(links(io)) == 1          # the merge is linked, not re-investigated


def test_dispatcher_runs_at_most_three_at_once(ds):
    import threading
    import time
    pages, alerts = world.pages_and_alerts()
    many = []
    for k in range(10):                                      # ten pages that page at the same minute
        p = dict(pages[1], page=10 + k, page_id=f"P{k}")
        many += [dict(pages[0], page=10 + k, page_id=f"P{k}"), p]
    io = MemoryTopics(telemetry=world.telemetry(), alerts=alerts, pages=many)
    running, peak, lock = [0], [0], threading.Lock()

    def agent(ctx):
        with lock:
            running[0] += 1
            peak[0] = max(peak[0], running[0])
        time.sleep(0.05)
        with lock:
            running[0] -= 1
        return {"page_id": ctx.page_id, "needs_human": "no", "confidence": 0.5, "tool_calls": 0}

    Dispatcher(io, ds, agent, concurrency=3, log=lambda line: None).run(until_end=True)
    assert len(notes(io)) == 10 and peak[0] == 3


# --------------------------------------------------------------------------- MCP = investigator
def test_the_mcp_server_and_the_investigator_offer_the_same_tools(store, ds, tmp_path):
    pytest.importorskip("mcp")
    import anyio
    from mcp import Client

    from sentinel_stream import mcp_server

    ctx = ctx_of(store, ds, 1)

    async def listed():
        async with Client(mcp_server.build_server(ctx)) as client:
            tools = (await client.list_tools()).tools
            called = await client.call_tool("query_metric", {"series_id": world.LAG, "window_minutes": 30})
            return tools, called

    tools, called = anyio.run(listed)
    llm = FakeLLM([[text(note_json(ctx.page_id, answer="unsure", evidence=(), entities=()))]])
    investigator(llm, tmp_path).investigate(ctx)
    sent = llm.requests[0]["tools"]
    assert [t.name for t in tools] == [t["name"] for t in sent] == [t.name for t in ct.TOOLS]
    assert [t.input_schema for t in tools] == [t["input_schema"] for t in sent]
    assert [t.description for t in tools] == [t["description"] for t in sent]
    direct, _ = ct.call(ctx, "query_metric", {"series_id": world.LAG, "window_minutes": 30})
    assert json.loads(called.content[0].text) == json.loads(ct.dumps(direct)) and not called.is_error


# ----------------------------------------------------------------------------- scoring the notes
def test_triage_score_tables_and_target_naming(ds):
    from sentinel_lens.correlator import build_topology
    from sentinel_stream import triage_score

    pages, _ = world.pages_and_alerts()
    answer_key = [{"event": "started", "id": "inj-1", "scenario": "consumer_slow_deploy", "target": "grp", "params": {"x": 1.0},
                   "series": [world.LAG], "entities": ["consumer_group:grp"], "start": world.iso(world.at("00:50")),
                   "stop": world.iso(world.at("01:20"))},
                  {"event": "published", "start": world.iso(world.DAY), "end": world.iso(world.at("23:59"))}]
    ids = {p["page"]: p["page_id"] for p in pages if p["kind"] == "paged"}

    def note(page, verdict, ents):
        return {"type": "note", "page_id": ids[page], "verdict": verdict, "confidence": 0.7, "suspected_cause": "c",
                "affected_entities": ents, "evidence": [{"tool_call_id": "c1", "finding": "f", "tool": "query_metric"}],
                "valid": True, "cost_usd_estimate": 0.01, "seconds": 10.0, "tool_calls": 3, "budget_exhausted": None,
                "tokens": {"total": 100}}

    notes = [note(1, "noise", ["pod:p-a"]), note(2, "real", ["pod:p-a"])]          # page 3 has no note
    s = triage_score.score(answer_key, pages, notes, build_topology(ds.relationships(), ds.series()))
    assert [(r["page"], r["truth"], r["verdict"]) for r in s["rows"]] == [(1, "real", "noise"), (2, "noise", "real"), (3, "noise", None)]
    assert s["table"] == {"real": {"real": 0, "noise": 1, "unsure": 0, "no note": 0},
                          "noise": {"real": 1, "noise": 0, "unsure": 0, "no note": 1}}
    assert [r["page"] for r in s["real_called_noise"]] == [1] and [r["page"] for r in s["noise_called_real"]] == [2]
    assert s["answer_key_entity_named"] == 0 and s["target_named"] == 0                # p-a is 1 hop away, not the target
    assert "Real called noise: 1 of 1" in triage_score.markdown(s)
    assert triage_score.names_target(["pod:orders-etl-consumer-0"], "orders-etl-consumer")
    assert triage_score.names_target(["flink_job:clickstream-enrich"], "clickstream-enrich")
    assert not triage_score.names_target(["pod:orders-etl-consumer-0"], "orders-etl")
