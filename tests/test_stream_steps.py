"""The streaming steps against the batch detectors, on synthetic data.

Each step is fed one reading / one event at a time and must produce exactly the alerts the batch
pipeline produces when it is given everything at once.
"""
import ast
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from sentinel_lens import feedback
from sentinel_lens.alerts import (Alert, alerts_from_above_zero, alerts_from_counter, alerts_from_job_runs,
                                  alerts_from_schedule, alerts_from_seasonal)
from sentinel_lens.correlator import build_topology, correlate
from sentinel_lens.detectors.guardrails import above_zero_alerts, counter_increase_alerts
from sentinel_lens.detectors.job_runs import job_run_alerts
from sentinel_lens.detectors.schedule import failed_run_alerts, schedule_alerts
from sentinel_lens.detectors.seasonal import HISTORY_MODES, seasonal_scores
from sentinel_stream import FROZEN_DEMOTED_GROUPS, RunFinished, RunStarted
from sentinel_stream.guardrail_step import GuardrailStep
from sentinel_stream.pages import Pager
from sentinel_stream.runs_step import RunsStep
from sentinel_stream.seasonal_step import PERSIST, SeasonalStep
from tests.helpers import STEPS_PER_DAY, weekly_series

ROOT = Path(__file__).resolve().parents[1]


def key(a: Alert):
    return (a.time, a.target, a.detector)


def ts(x) -> pd.Timestamp:
    return pd.Timestamp(x, tz="UTC")


# ------------------------------------------------------------------------------------- seasonal
def synthetic_metrics() -> pd.DataFrame:
    """Six weeks of 5-minute readings, one column per history mode. The last two weeks hold a step
    (2 hours high), a single-reading spike, a slow drift, a stretch with no readings and a weekend."""
    cols = {}
    for n, mode in enumerate(HISTORY_MODES):
        s = weekly_series(weeks=6, seed=n, name=f"metric_{mode}|entity").copy()
        day = 30 * STEPS_PER_DAY
        s.iloc[day + 100:day + 124] += 80                                   # step: 2 hours
        s.iloc[day + 200] += 300                                            # spike: one reading
        s.iloc[day + 2 * STEPS_PER_DAY:day + 3 * STEPS_PER_DAY] += np.linspace(0, 60, STEPS_PER_DAY)   # drift
        s.iloc[day + 4 * STEPS_PER_DAY + 30:day + 4 * STEPS_PER_DAY + 90] = np.nan                     # no readings
        s.iloc[day + 6 * STEPS_PER_DAY + 50:day + 6 * STEPS_PER_DAY + 70] -= 60                        # a drop
        cols[s.name] = s
    return pd.DataFrame(cols)


def test_seasonal_step_matches_batch_for_every_history_mode():
    m = synthetic_metrics()
    floors = [0.0, 0.0, 2.0]
    batch = []
    for sid, mode, floor in zip(m.columns, HISTORY_MODES, floors):
        batch += alerts_from_seasonal(seasonal_scores(m[sid], history=mode, abs_floor=floor), target=sid)

    step = SeasonalStep(m.columns, ["seasonal"] * 3, HISTORY_MODES, floors)
    first = m.index[28 * STEPS_PER_DAY + 100]                               # not midnight: chunks start off-grid
    step.load(m.reindex(step.history_index(first)))
    live = m.loc[first:]
    stream = []
    for t, row in zip(live.index, live.to_numpy()):
        stream += step.score(t, row)

    settled = live.index[PERSIST["n"] - 1]          # from here the persistence window is full
    want = sorted([a for a in batch if a.time >= settled], key=key)
    got = sorted([a for a in stream if a.time >= settled], key=key)
    assert got == want
    for sid in m.columns:                           # the comparison is not empty for any mode
        assert any(a.target == sid for a in want), sid


def test_seasonal_step_refuses_a_bucket_out_of_order():
    m = synthetic_metrics().iloc[:, :1]
    step = SeasonalStep(m.columns, ["seasonal"], ["day_type"], [0.0])
    first = m.index[28 * STEPS_PER_DAY]
    step.load(m.reindex(step.history_index(first)))
    with pytest.raises(ValueError):
        step.score(first + pd.Timedelta("5min"), m.loc[first].to_numpy())


# ------------------------------------------------------------------------------------ guardrails
def test_guardrail_step_matches_batch():
    idx = pd.date_range("2026-01-01", periods=40, freq="1min", tz="UTC")
    counter = pd.Series([0.0] * 5 + [1] * 5 + [3] * 5 + [np.nan] * 5 + [4] * 5 + [0] * 5 + [1] * 10,
                        index=idx, name="restarts_total|pod-a")      # rises, a gap, a reset, a rise
    quiet = pd.Series(2.0, index=idx, name="restarts_total|pod-b")
    gauge = pd.Series([0.0] * 10 + [2, 1, 0, np.nan, 3] + [0.0] * 25, index=idx, name="under_replicated|kafka")
    batch = (alerts_from_counter(counter_increase_alerts(counter), target=counter.name)
             + alerts_from_counter(counter_increase_alerts(quiet), target=quiet.name)
             + alerts_from_above_zero(above_zero_alerts(gauge), target=gauge.name))

    step = GuardrailStep([counter.name, quiet.name], [gauge.name])
    stream = []
    for t in idx:
        stream += step.step(t, np.array([counter[t], quiet[t]]), np.array([gauge[t]]))
    assert sorted(stream, key=key) == sorted(batch, key=key)
    assert len(batch) == 3 + 3                      # 0->1, 1->3, 0->1 on the counter; three readings above zero


def test_guardrail_step_started_from_history_compares_with_the_last_reading_before():
    step = GuardrailStep(["restarts_total|pod-a"], [])
    step.load(ts("2026-01-01 00:00"), np.array([4.0]))
    fired = step.step(ts("2026-01-01 00:01"), np.array([5.0]), np.array([]))
    assert [a.explanation for a in fired] == ["restarts_total for pod-a rose from 4 to 5"]


# ------------------------------------------------------------------------------------------ runs
def synthetic_runs():
    """A nightly DAG (02:00, SLA 60 min) over 12 days: normal runs of about 20 minutes, then a run
    that never happens, one that starts late, one that is slow and breaks its SLA, one that fails."""
    days = pd.date_range("2026-01-01", periods=12, freq="1D", tz="UTC")
    expected = pd.DataFrame({"dag_id": "nightly", "scheduled_for": days + pd.Timedelta(hours=2), "sla_minutes": 60})
    runs = []
    for n, sched in enumerate(expected.scheduled_for):
        start, minutes, state = sched + pd.Timedelta(seconds=10 + n), 20 + 0.3 * (n % 3), "success"
        if n == 8:
            continue                                                    # never runs
        if n == 9:
            start = sched + pd.Timedelta(minutes=30)                    # starts late
        if n == 10:
            minutes = 80                                                # slow, and past its SLA
        if n == 11:
            minutes, state = 15, "failed"
        runs.append({"dag_id": "nightly", "run_id": f"scheduled__{n}", "run_type": "scheduled",
                     "scheduled_for": sched, "start": start, "end": start + pd.Timedelta(minutes=minutes),
                     "state": state})
    manual = days[5] + pd.Timedelta(hours=12)
    runs.append({"dag_id": "nightly", "run_id": "manual__1", "run_type": "manual", "scheduled_for": manual,
                 "start": manual, "end": manual + pd.Timedelta(minutes=21), "state": "success"})
    return expected, pd.DataFrame(runs)


def test_runs_step_matches_batch():
    expected, runs = synthetic_runs()
    end = ts("2026-01-13")
    batch = (alerts_from_schedule(pd.concat([schedule_alerts(expected, runs, now=end), failed_run_alerts(runs)]))
             + alerts_from_job_runs(job_run_alerts(runs)))

    step = RunsStep(lambda a, b: expected[(expected.scheduled_for >= a) & (expected.scheduled_for < b)])
    step.load(None, ts("2026-01-01"))
    events = [RunStarted(r.start, r.dag_id, r.run_id, r.run_type, r.scheduled_for) for r in runs.itertuples()]
    events += [RunFinished(r.end, r.dag_id, r.run_id, r.state) for r in runs.itertuples()]
    stream = []
    for ev in sorted(events, key=lambda e: e.time):
        for _, alerts in step.due(ev.time, inclusive=False):
            stream += alerts
        stream += step.started(ev) if isinstance(ev, RunStarted) else step.finished(ev)
    for _, alerts in step.due(end, inclusive=False):
        stream += alerts

    assert sorted(stream, key=key) == sorted(batch, key=key)
    assert sorted(a.detector for a in batch) == ["failed_run", "job_run_baseline", "sla_breach",
                                                 "start_late", "start_late"]


def test_runs_step_started_from_history_gives_the_same_alerts_afterwards():
    expected, runs = synthetic_runs()
    restart, end = ts("2026-01-11 02:30"), ts("2026-01-13")      # the slow run is in progress
    batch = (alerts_from_schedule(pd.concat([schedule_alerts(expected, runs, now=end), failed_run_alerts(runs)]))
             + alerts_from_job_runs(job_run_alerts(runs)))

    step = RunsStep(lambda a, b: expected[(expected.scheduled_for >= a) & (expected.scheduled_for < b)])
    known = runs[runs.start < restart].copy()
    known["end"] = known.end.where(known.end < restart, None)    # what a DataSource at `restart` shows
    step.load(known, restart)
    events = [RunStarted(r.start, r.dag_id, r.run_id, r.run_type, r.scheduled_for)
              for r in runs.itertuples() if r.start >= restart]
    events += [RunFinished(r.end, r.dag_id, r.run_id, r.state) for r in runs.itertuples() if r.end >= restart]
    stream = []
    for ev in sorted(events, key=lambda e: e.time):
        for _, alerts in step.due(ev.time, inclusive=False):
            stream += alerts
        stream += step.started(ev) if isinstance(ev, RunStarted) else step.finished(ev)
    for _, alerts in step.due(end, inclusive=False):
        stream += alerts
    assert sorted(stream, key=key) == sorted([a for a in batch if a.time >= restart], key=key)
    assert {a.detector for a in stream} == {"sla_breach", "job_run_baseline", "failed_run"}


# ----------------------------------------------------------------------------------------- pages
def small_topology():
    """a - b - c in a chain, d on its own."""
    relationships = pd.DataFrame({"child_kind": ["pod", "pod"], "child": ["a", "c"],
                                  "parent_kind": ["group", "group"], "parent": ["b", "b"]})
    catalog = pd.DataFrame({"series_id": ["m|a", "m|b", "m|c", "m|d"], "entity_kind": ["pod", "group", "pod", "pod"],
                            "entity": ["a", "b", "c", "d"]})
    return build_topology(relationships, catalog, extra_edges=())


def random_alerts(n=80, seed=3):
    rng = np.random.default_rng(seed)
    start = ts("2026-01-01")
    out = []
    for i in range(n):
        target = ["m|a", "m|b", "m|c", "m|d"][rng.integers(4)]
        detector = ["seasonal", "counter_rule"][rng.integers(2)]
        out.append(Alert(time=start + pd.Timedelta(minutes=int(rng.integers(0, 360))), target=target,
                         detector=detector, score=0.7, direction="up", explanation=f"alert {i}", measure=7.0, line=6.0))
    return out


def test_pager_gives_the_batch_pages_after_every_alert_whatever_the_arrival_order():
    topology = small_topology()
    alerts = random_alerts()
    rng = np.random.default_rng(0)
    # arrival = alert time + 0..4 minutes, so alerts arrive out of time order as seasonal ones do
    arrivals = sorted(((a.time + pd.Timedelta(minutes=int(rng.integers(0, 5))), i, a) for i, a in enumerate(alerts)),
                      key=lambda x: x[:2])
    pager = Pager(topology)
    seen = []
    events = []
    for arrived, _, alert in arrivals:
        events += pager.add(alert, arrived) + pager.settle(arrived)       # one alert per step
        seen.append(alert)
        assert pager.pages() == correlate(seen, topology)
    kinds = [e.kind for e in events]
    absorbed = sum(len(e.absorbed) for e in events)
    assert absorbed > 0                                             # the random set does exercise merges
    assert kinds.count("opened") - absorbed == len(pager.pages())   # every page was opened once or merged away
    assert kinds.count("paged") == kinds.count("opened")            # nothing is demoted: each paged when it opened


def test_pager_pages_only_when_an_alert_that_is_not_demoted_arrives():
    topology = small_topology()
    events_of = {"m|a": "cpu_throttling", "m|b": "backlog", "m|c": "cpu_throttling", "m|d": ""}
    pager = Pager(topology, demoted_groups={("seasonal", "cpu_throttling")}, canonical_event_of=events_of)
    t0 = ts("2026-01-01 10:00")

    def alert(minute, target):
        return Alert(time=t0 + pd.Timedelta(minutes=minute), target=target, detector="seasonal", score=0.6,
                     direction="up", explanation="x", measure=7.0, line=6.0)

    def step(a, at):
        return pager.add(a, at) + pager.settle(at)

    first = step(alert(0, "m|a"), t0)                                       # demoted: a page, nobody paged
    assert [e.kind for e in first] == ["opened"]
    second = step(alert(5, "m|b"), t0 + pd.Timedelta(minutes=9))            # related and not demoted: pages now
    assert [e.kind for e in second] == ["alert_added", "paged"]
    assert second[-1].alert == alert(5, "m|b")                              # the page's first alert that is not demoted
    third = step(alert(10, "m|b"), t0 + pd.Timedelta(minutes=14))           # already paged: no second page
    assert [e.kind for e in third] == ["alert_added"]

    page, = pager.pages()
    demoted = feedback.apply_frozen(page.alerts, frozenset({("seasonal", "cpu_throttling")}), events_of)
    assert pager.demoted == set(demoted)
    assert feedback.paging_time(page, demoted) == t0 + pd.Timedelta(minutes=5)      # the alert's own time ...
    assert pager.paged_at() == {page.id: t0 + pd.Timedelta(minutes=9)}              # ... and when it arrived


def test_pager_pages_once_for_a_step_whose_alerts_end_up_in_one_page():
    """a and c are two hops apart, so their alerts open two pages; b, between them, joins the two.
    When all three arrive in the same step that is one page-out, not two followed by a merge."""
    topology = small_topology()
    t0 = ts("2026-01-01 10:00")

    def alert(target):
        return Alert(time=t0, target=target, detector="seasonal", score=0.6, direction="up",
                     explanation="x", measure=7.0, line=6.0)

    together = Pager(topology)
    events = [e for target in ("m|a", "m|c", "m|b") for e in together.add(alert(target), t0)]
    events += together.settle(t0)
    assert [e.kind for e in events] == ["opened", "opened", "merged", "paged"]
    assert len(together.pages()) == 1

    # the same three alerts in three steps: both pages page before the third alert joins them
    apart = Pager(topology)
    events = []
    for minute, target in enumerate(("m|a", "m|c", "m|b")):
        at = t0 + pd.Timedelta(minutes=minute)
        events += apart.add(alert(target), at) + apart.settle(at)
    assert [e.kind for e in events] == ["opened", "paged", "opened", "paged", "merged"]
    assert apart.pages() == together.pages()                   # the pages are the same either way


def test_frozen_demoted_groups_are_the_ones_in_the_frozen_config():
    text = (ROOT / "docs/config-frozen.md").read_text()
    listed = re.search(r"Demoted groups carried into the test week: (\[.*\])", text).group(1)
    assert FROZEN_DEMOTED_GROUPS == frozenset(ast.literal_eval(listed))
