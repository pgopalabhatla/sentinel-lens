"""Note schema v3: the follow-up of an "unsure" note and the needs_human scoring.

The follow-up is a fresh investigation of the same page at paging time + N minutes: its tools must see
everything up to that later time and nothing after it, and a page merged in the meantime is followed
into the page that holds it. The dispatcher writes it as a linked `follow_up` record, once.
"""
import json

import pandas as pd
import pytest

from sentinel_stream import context_tools as ct
from sentinel_stream import triage_score
from sentinel_stream.dispatcher import Dispatcher, MemoryTopics
from sentinel_stream.investigator import request_text
from tests import triage_world as world
from tests.test_context_tools import ALL_CALLS, ISO, ds, store  # noqa: F401  (fixtures)
from tests.test_triage import FakeLLM, investigator, note_json, text, tool_use


def index_of(store, page: int) -> int:
    return next(i for i in store.paged() if store.pages[i]["page"] == page)


# -------------------------------------------------------------------------- the context, 10 min later
@pytest.mark.parametrize("page", [1, 2, 3])
def test_a_follow_up_sees_nothing_after_paging_plus_10_minutes(store, ds, page):
    paged = ct.Context.at_page(store, ds, index_of(store, page))
    later = ct.Context.at_page(store, ds, index_of(store, page), at=paged.now + pd.Timedelta("10min"))
    assert later.now == paged.now + pd.Timedelta("10min") and later.paged_at == paged.now
    for name, args in ALL_CALLS + [("baseline_comparison", {"series_id": world.LAG})]:
        result, is_error = ct.call(later, name, args)
        assert not is_error, (name, result)
        late = [s for s in ISO.findall(ct.dumps(result)) if pd.Timestamp(s.replace("Z", "+00:00")) > later.now]
        assert not late, (name, late)


def test_a_follow_up_sees_what_happened_in_the_ten_minutes(store, ds):
    later = ct.Context.at_page(store, ds, index_of(store, 1), at=world.at("01:10"))             # paged at 01:00
    lag, _ = ct.call(later, "query_metric", {"series_id": world.LAG, "window_minutes": 30})
    assert lag["last"] == 70.0                                                                  # the 01:10 reading, not later
    runs, _ = ct.call(later, "dag_runs", {"window_hours": 2})
    assert [(r["scheduled_for"][11:16], r["state"]) for r in runs["runs"]][-1] == ("01:05", "success")   # started 01:05, done 01:07
    logs, _ = ct.call(later, "recent_logs", {"pod": "p-a", "window_minutes": 30})
    assert logs["latest"][-1]["ts"] == "2026-01-05T01:00:07Z"                                   # 01:10:07 is after 01:10
    page, _ = ct.call(later, "get_page")
    assert page["paged_at"] == "2026-01-05T01:00:00Z" and page["as_of"] == "2026-01-05T01:10:00Z"


def test_a_follow_up_follows_a_page_into_the_page_that_absorbed_it(store, ds):
    i = index_of(store, 2)                                                                     # paged 02:00; merged into 1 at 02:10
    before = ct.call(ct.Context.at_page(store, ds, i, at=world.at("02:09")), "get_page")[0]
    after_ctx = ct.Context.at_page(store, ds, i, at=world.at("02:10"))
    after = ct.call(after_ctx, "get_page")[0]
    assert before["page"] == 2 and before["n_alerts"] == 1 and "paged_as_page" not in before
    assert after_ctx.page == 1 and after["page"] == 1 and after["paged_as_page"] == 2 and after["n_alerts"] == 4
    assert after["page_id"] == store.pages[i]["page_id"] and after["merged_pages"] == [2]


def test_a_follow_up_cannot_be_cut_before_the_page_went_out(store, ds):
    with pytest.raises(ValueError, match="before the page went out"):
        ct.Context.at_page(store, ds, index_of(store, 2), at=world.at("01:59"))


def test_the_follow_up_agent_is_told_the_times_and_the_first_note(store, ds, tmp_path):
    ctx = ct.Context.at_page(store, ds, index_of(store, 1), at=world.at("01:10"))
    first = {**json.loads(note_json(ctx.page_id, answer="unsure", evidence=(), entities=())), "tool_calls": 3}
    llm = FakeLLM([[tool_use("get_page")], [text(note_json(ctx.page_id))]])
    note = investigator(llm, tmp_path).investigate(ctx, previous=first)
    asked = llm.requests[0]["messages"][0]["content"]
    assert "paged at 2026-01-05 01:00" in asked and "now 2026-01-05 01:10" in asked and "10 minutes later" in asked
    assert '"needs_human": "unsure"' in asked and "tool_calls" not in asked
    assert note["follow_up"] and note["paged_at"] == world.iso(world.at("01:00")) and note["as_of"] == world.iso(world.at("01:10"))
    assert note["transcript"].endswith("page-0001-follow-up.json")
    assert request_text(ct.Context.at_page(store, ds, index_of(store, 1))).endswith("Investigate it.")


# ---------------------------------------------------------------------------------- the dispatcher
class Agent:
    """Says `answers[page]` at paging time and `later[page]` on a follow-up; records what it was given."""

    def __init__(self, answers, later=None):
        self.answers, self.later, self.calls = answers, later or {}, []

    def __call__(self, ctx, previous=None):
        self.calls.append((ctx.original_page, ctx.now, previous is not None, ctx.metrics.index.max()))
        answer = (self.later if previous else self.answers).get(ctx.original_page, "yes")
        return {"page_id": ctx.page_id, "needs_human": answer, "confidence": 0.5, "tool_calls": 1, "page": ctx.original_page,
                "paged_at": ctx.paged_at.isoformat(), "as_of": ctx.now.isoformat(), "seconds": 6.0}


def topics():
    pages, alerts = world.pages_and_alerts()
    return MemoryTopics(telemetry=world.telemetry(), alerts=alerts, pages=pages)


def records(io, kind):
    return [r for r in io.topics.get("triage", []) if r["type"] == kind]


def test_an_unsure_note_gets_one_follow_up_ten_minutes_later_written_as_a_linked_record(ds):
    io, agent = topics(), Agent({1: "unsure", 2: "yes", 3: "unsure"}, later={1: "yes", 3: "unsure"})
    d = Dispatcher(io, ds, agent, follow_up_minutes=10, log=lambda line: None).run(until_end=True)
    follow = sorted((c[0], c[1]) for c in agent.calls if c[2])
    assert follow == [(1, world.at("01:10")), (3, world.at("03:10"))]                   # page 2 said yes: none
    assert all(c[3] <= c[1] for c in agent.calls)                                      # no reading after the cut
    ups = records(io, "follow_up")
    assert sorted((r["follow_up_of"], r["needs_human"]) for r in ups) == [
        ("2026-01-05T01:00:00Z|consumer_group:grp", "yes"), ("2026-01-05T03:00:00Z|pod:p-b", "unsure")]
    assert len(records(io, "note")) == 3 and d.followed_up == 2                          # an unsure follow-up is not followed up


def test_no_follow_up_without_the_setting_and_none_past_the_end_of_the_stream(ds):
    io, agent = topics(), Agent({1: "unsure"})
    Dispatcher(io, ds, agent, log=lambda line: None).run(until_end=True)
    assert not [c for c in agent.calls if c[2]] and records(io, "follow_up") == []
    io, agent = topics(), Agent({3: "unsure"})
    d = Dispatcher(io, ds, agent, follow_up_minutes=24 * 60, log=lambda line: None).run(until_end=True)
    assert records(io, "follow_up") == [] and d.follow_ups_past_end == 1


def test_a_restart_does_the_missing_follow_up_and_nothing_twice(ds):
    io = topics()
    first = Agent({1: "unsure"})
    Dispatcher(io, ds, first, log=lambda line: None).run(until_end=True)                  # follow-ups off: none written
    assert records(io, "follow_up") == []
    second = Agent({}, later={1: "no"})
    Dispatcher(io, ds, second, follow_up_minutes=10, log=lambda line: None).run(until_end=True)
    assert [(c[0], c[1], c[2]) for c in second.calls] == [(1, world.at("01:10"), True)]   # notes are not redone
    third = Agent({})
    Dispatcher(io, ds, third, follow_up_minutes=10, log=lambda line: None).run(until_end=True)
    assert third.calls == [] and len(records(io, "follow_up")) == 1


def test_a_live_dispatcher_waits_until_the_stream_has_passed_the_follow_up_time(ds):
    pages, alerts = world.pages_and_alerts()
    upto = [m for m in world.telemetry() if pd.Timestamp(m["ts"]) <= world.at("01:10")]   # the stream has reached 01:10
    io = MemoryTopics(telemetry=upto, alerts=alerts, pages=pages[:2])
    agent = Agent({1: "unsure"})
    d = Dispatcher(io, ds, agent, follow_up_minutes=10, log=lambda line: None)
    d.step()
    d.wait()
    d.step()
    d.wait()
    assert not [c for c in agent.calls if c[2]]                                          # 01:10 + 1 min grace not passed yet
    io.topics["telemetry"] += [m for m in world.telemetry() if world.at("01:10") < pd.Timestamp(m["ts"]) <= world.at("01:12")]
    d.step()
    d.wait()
    assert [(c[1], c[2]) for c in agent.calls if c[2]] == [(world.at("01:10"), True)]
    assert max(c[3] for c in agent.calls if c[2]) == world.at("01:10")                   # read to 01:12, cut at 01:10
    d._pool.shutdown()


# ------------------------------------------------------------------------------------------- scoring
def test_needs_human_outcomes_follow_ups_and_target_naming(ds):
    from sentinel_lens.correlator import build_topology
    pages, _ = world.pages_and_alerts()
    answer_key = [{"event": "started", "id": "inj-1", "scenario": "x", "target": "grp", "params": {},
                   "series": [world.LAG], "entities": ["consumer_group:grp"], "start": world.iso(world.at("00:50")),
                   "stop": world.iso(world.at("01:20"))},
                  {"event": "started", "id": "inj-2", "scenario": "y", "target": "p-b", "params": {},
                   "series": [world.MEM_B], "entities": ["pod:p-b"], "start": world.iso(world.at("02:50")),
                   "stop": world.iso(world.at("03:20"))},
                  {"event": "published", "start": world.iso(world.DAY), "end": world.iso(world.at("23:59"))}]
    ids = {p["page"]: p["page_id"] for p in pages if p["kind"] == "paged"}            # pages 1 and 3 true, 2 false

    def note(page, answer, cause, seconds=60.0, **more):
        return {"type": "note", "page_id": ids[page], "needs_human": answer, "confidence": 0.6,
                "cause": {"category": "change", "description": cause}, "impact": {"degraded": ["lag"], "description": "d", "evidence": []},
                "evidence": [], "seconds": seconds, "cost_usd_estimate": 0.02, "tool_calls": 4,
                "paged_at": pages[[p["page_id"] for p in pages].index(ids[page])]["emitted_at"], **more}

    notes = [note(1, "unsure", "something on the consumers"), note(2, "yes", "a deploy of p-a"), note(3, "no", "p-b restarted on purpose")]
    follow = [{**note(1, "yes", "the consumer group grp is behind", seconds=120.0), "type": "follow_up",
               "follow_up_of": ids[1], "as_of": world.iso(world.at("01:10"))}]
    s = triage_score.score_needs_human(answer_key, pages, notes, follow, build_topology(ds.relationships(), ds.series()))
    rows = {r["page"]: r for r in s["rows"]}
    assert (rows[1]["first"], rows[1]["follow_up"], rows[1]["final"]) == ("unsure", "yes", "yes")
    assert (rows[1]["first_outcome"], rows[1]["final_outcome"]) == ("deferred", "correct")
    assert rows[2]["final_outcome"] == "wasted" and rows[3]["final_outcome"] == "dangerous"
    assert s["first"]["real"]["deferred"] == 1 and s["final"]["real"] == {"correct": 1, "dangerous": 1, "wasted": 0,
                                                                         "deferred": 0, "no note": 0}
    assert [r["page"] for r in s["dangerous"]] == [3] and [r["page"] for r in s["wasted"]] == [2]
    assert rows[1]["minutes_to_first_answer"] == 1.0 and rows[1]["minutes_to_final_answer"] == 12.0      # 10 min + 2 min
    assert rows[3]["minutes_to_final_answer"] == 1.0
    assert rows[1]["cause_names_target"] and not rows[1]["first_cause_names_target"] and rows[3]["cause_names_target"]
    assert s["cause_names_target"] == 2 and s["first_cause_names_target"] == 1 and s["follow_ups"] == 1
    assert rows[1]["cost"] == pytest.approx(0.04)
    md = triage_score.markdown_needs_human(s)
    assert "**Dangerous answers**" in md and "| real (2) | 1 | 1 | - | 0 | 0 |" in md and "| noise (1) | 0 | - | 1 | 0 | 0 |" in md


def test_old_verdict_notes_map_onto_needs_human():
    assert [triage_score.answer_of({"verdict": v}) for v in ("real", "noise", "unsure")] == ["yes", "no", "unsure"]
    assert triage_score.answer_of(None) is None and triage_score.outcome("noise", None) == "no note"
    assert triage_score.cause_text({"suspected_cause": "x"}) == "x"


@pytest.mark.parametrize("text, target, named", [
    ("orders-etl-consumer-0 is OOM-killed", "orders-etl-consumer", True), ("the orders-etl group lags", "orders-etl", True),
    ("orders-etl-consumer is slow", "orders-etl", False), ("kafka-1 GC pauses", "kafka-1", True), ("kafka-12", "kafka-1", False),
    ("hourly_quality_check was paused", "hourly_quality_check", True), ("dag:customer_agg missed", "customer_agg", True),
    ("nothing named", "trino-lakehouse", False), ("", "x", False)])
def test_text_names_target(text, target, named):
    assert triage_score.text_names_target(text, target) is named


# ------------------------------------------------------------------------------------------ the guard
def _no(category="capacity", degraded=("none",)):
    return {"page_id": "P", "needs_human": "no", "confidence": 0.7, "cause": {"category": category, "description": "d"},
            "impact": {"degraded": list(degraded), "description": "d", "evidence": []}}


@pytest.mark.parametrize("note, converted, fragment", [
    (_no(), False, None), (_no("change"), False, None),
    (_no("unknown"), True, '"unknown"'), (_no(degraded=("lag",)), True, "['lag']"),
    (_no("unknown", ("errors", "latency")), True, "unknown"),
    ({**_no("unknown"), "needs_human": "yes"}, False, None), ({**_no("unknown"), "needs_human": "unsure"}, False, None)])
def test_the_guard_accepts_no_only_with_a_known_cause_and_no_impact(note, converted, fragment):
    from sentinel_stream.investigator import guard
    out, why = guard(note)
    assert (why is not None) is converted
    if converted:
        assert out["needs_human"] == "unsure" and fragment in why and note["needs_human"] == "no"     # the input is not changed
    else:
        assert out == note


def test_a_guarded_no_is_recorded_as_converted_and_gets_the_follow_up(ds, tmp_path):
    pages, alerts = world.pages_and_alerts()
    io = MemoryTopics(telemetry=world.telemetry(), alerts=alerts, pages=pages[:2])           # one page-out, 01:00
    no_unknown = note_json(pages[1]["page_id"], answer="no", evidence=("c1",), entities=(),
                           cause={"category": "unknown", "description": "a restart, cause not found"},
                           impact={"degraded": ["none"], "description": "nothing degraded", "evidence": ["c1"]})
    llm = FakeLLM([[tool_use("get_page")], [text(no_unknown)],                             # at 01:00: "no", unknown cause
                   [tool_use("get_page")], [text(note_json(pages[1]["page_id"]))]])      # at 01:10: yes
    agent = investigator(llm, tmp_path)
    Dispatcher(io, ds, agent.investigate, concurrency=1, follow_up_minutes=10, log=lambda line: None).run(until_end=True)
    first, = records(io, "note")
    later, = records(io, "follow_up")
    assert first["valid"] and first["needs_human"] == "unsure" and first["cause"]["category"] == "unknown"
    assert first["guard"]["converted_from"] == "no" and '"unknown"' in first["guard"]["reason"]
    assert later["needs_human"] == "yes" and later["guard"] is None and later["as_of"] == world.iso(world.at("01:10"))
