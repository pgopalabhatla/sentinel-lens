"""The investigator against the real API (marker `llm`; skipped without ANTHROPIC_API_KEY).

    pytest -m llm

One investigation of one page of the synthetic world (tests/triage_world.py), with the model in
sentinel_stream/triage.yaml. It costs a few cents. What is checked is the plumbing, not the verdict:
a valid note comes back within the budgets, every piece of evidence cites a call that was made, and
the transcript holds the whole exchange.
"""
import json
import os

import pytest

from sentinel_sim.replay import DataSource
from sentinel_stream import context_tools as ct
from tests import triage_world as world

pytestmark = pytest.mark.llm


@pytest.fixture(scope="module", autouse=True)
def needs_key():
    pytest.importorskip("anthropic")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY is not set")


def test_a_real_investigation_returns_a_valid_note_within_budget(tmp_path):
    from sentinel_stream.investigator import Config, Investigator

    pages, alerts = world.pages_and_alerts()
    store = ct.StreamStore.from_records(world.telemetry(), pages, alerts)
    ds = DataSource(world.write_data_dir(tmp_path / "world"))
    ctx = ct.Context.at_page(store, ds, store.paged()[0])
    config = Config.load(runs_dir=str(tmp_path / "runs"))
    note = Investigator(config, run_name="llm-test").investigate(ctx)

    assert note["valid"], note
    assert note["verdict"] in ("real", "noise", "unsure") and 0 <= note["confidence"] <= 1
    assert note["tool_calls"] <= config.budget.max_tool_calls and note["seconds"] <= config.budget.max_seconds
    assert note["tokens"]["total"] <= config.budget.max_tokens and note["cost_usd_estimate"] is not None
    assert "get_page" in note["tools_used"]
    made = {c["call_id"] for c in json.loads(open(note["transcript"]).read())["tool_calls"]}
    assert {e["tool_call_id"] for e in note["evidence"]} <= made
