"""The schedule generator of the scripted live week (scripts/run_live_week.py).

It needs data_live/ only for the DAG schedule (configuration); skipped when data_live is missing.
No reading and no engine is involved.
"""
from pathlib import Path

import pandas as pd
import pytest

from scripts.run_live_week import END, GAP_MIN, LEAD_IN_MIN, PROTOCOL, SEED, START, TAIL_MIN, schedule, schedule_markdown
from sentinel_stream.injector import Injector
from sentinel_stream.scenarios_live import SCENARIOS

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(not (ROOT / "data_live" / "catalog").exists(), reason="data_live/ not generated")


@pytest.fixture(scope="module")
def ds():
    from sentinel_sim.replay import DataSource
    return DataSource(ROOT / "data_live")


def spans(commands) -> list:
    out = []
    for c in commands:
        start = pd.Timestamp(c["at"])
        out.append((start, start + pd.Timedelta(minutes=c["params"]["duration_min"])))
    return out


def test_the_schedule_is_the_same_every_time_and_differs_by_seed(ds):
    assert schedule(ds) == schedule(ds) == schedule(ds, seed=SEED)
    assert schedule(ds, seed=SEED + 1) != schedule(ds)


def test_every_scenario_twice_with_parameters_inside_the_declared_ranges(ds):
    commands = schedule(ds)
    assert len(commands) == 14 and [c["id"] for c in commands] == [f"inj-{n:02d}" for n in range(1, 15)]
    assert sorted(c["scenario"] for c in commands) == sorted(list(SCENARIOS) * 2)
    for c in commands:
        spec = SCENARIOS[c["scenario"]]
        assert spec.check(c["target"], c["params"]) == c["params"]          # complete and inside the ranges
        assert c["params"]["duration_min"] == int(c["params"]["duration_min"])


def test_injections_do_not_overlap_and_fit_into_the_week(ds):
    s = spans(schedule(ds))
    week_start, week_end = pd.Timestamp(START, tz="UTC"), pd.Timestamp(END, tz="UTC")
    lead_in = (s[0][0] - week_start) / pd.Timedelta("1min")
    assert LEAD_IN_MIN[0] <= lead_in <= LEAD_IN_MIN[1]
    assert all(b[0] - a[1] >= pd.Timedelta(minutes=GAP_MIN) for a, b in zip(s, s[1:]))
    assert s[-1][1] + pd.Timedelta(minutes=TAIL_MIN) <= week_end
    assert all(start == start.floor("min") for start, _ in s)


def test_a_dag_pause_starts_its_lead_time_before_a_scheduled_run(ds):
    expected = ds.expected_runs(start=START, end=END)
    pauses = [c for c in schedule(ds) if c["scenario"] == "dag_paused"]
    assert len(pauses) == 2
    for c in pauses:
        due = pd.Timestamp(c["at"]) + pd.Timedelta(minutes=c["params"]["lead_min"])
        runs = pd.to_datetime(expected[expected.dag_id == c["target"]].scheduled_for, utc=True)
        assert (runs == due).any()
        assert c["params"]["duration_min"] > c["params"]["lead_min"] + 10          # the pause outlasts the run's grace time


def test_the_injector_accepts_the_whole_schedule(ds):
    inj = Injector.for_data(ds)
    for c in schedule(ds):
        inj.command(c)


def test_the_schedule_is_the_one_written_in_the_protocol(ds):
    if not PROTOCOL.exists():
        pytest.skip("docs/live-protocol.md not written yet")
    assert schedule_markdown(schedule(ds)) in PROTOCOL.read_text()


def test_the_scheduled_week_is_injected_as_scheduled_and_untouched_outside_the_injections(ds):
    """The whole scheduled week through the injector (no broker, no engine): every injection starts
    and stops when the schedule says, and a reading outside every injection (and its tail) differs
    from the data only if it is a counter."""
    from sentinel_stream import wire
    from sentinel_stream.publisher import messages

    commands = schedule(ds)
    inj = Injector.for_data(ds)
    for c in commands:
        inj.command(c)
    windows = [(a, b + pd.Timedelta("31min")) for a, b in spans(commands)]
    records, dropped, changed_inside = [], 0, 0
    for message in messages(ds, START, END):
        sent, new = inj.apply(message)
        records += new
        if sent is None:
            dropped += 1
            continue
        if sent is message or message["type"] != "metrics":
            continue
        ts = wire.event_time(message)
        differs = {s for s in message["values"] if sent["values"].get(s) != message["values"][s]}
        if any(a <= ts < b for a, b in windows):
            changed_inside += bool(differs)
        else:
            assert all("_total|" in s for s in differs), (ts, sorted(differs))
    started = [r for r in records if r["event"] == "started"]
    stopped = [r for r in records if r["event"] == "stopped"]
    assert [(r["id"], r["start"], r["stop"]) for r in started] == [(c["id"], a.isoformat(), b.isoformat())
                                                                  for c, (a, b) in zip(commands, spans(commands))]
    assert [(r["id"], r["reason"]) for r in stopped] == [(c["id"], "duration") for c in commands]
    assert all(r["entities"] for r in started)
    assert dropped % 2 == 0 and dropped >= 2 * 9          # whole runs: the hourly check for 9 hours, and customer_agg once
    assert changed_inside > 2000                          # the injections do change the stream
