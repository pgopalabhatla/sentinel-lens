"""Schedule watcher and job run baseline, from DAG-run events.

The batch rules are used as they are; this module only keeps what they need and asks at the right time.

    schedule_alerts      asked once per deadline (scheduled time + grace, scheduled time + SLA), with
                         the one expected run and the runs known for it, and `now` = the deadline.
                         The batch rule already decides each deadline "only from what was known at
                         that moment", so asking at the moment gives the same answer.
    failed_run_alerts    asked when a run finishes, with that run.
    job_run_alerts       asked when a run finishes, with every finished run of the same DAG. The
                         batch rule scores a run against runs that ended before it, so the newest
                         run's row is the same whether later runs exist yet or not.

Expected run times come from a function (dag schedule -> times), fetched one day ahead. A schedule
is configuration, not telemetry: knowing that a job is due at 02:00 is not seeing the future.
"""
from __future__ import annotations

import heapq
from typing import Callable

import pandas as pd

from sentinel_lens.alerts import Alert, alerts_from_job_runs, alerts_from_schedule
from sentinel_lens.detectors.job_runs import job_run_alerts
from sentinel_lens.detectors.schedule import DEFAULT_GRACE_MIN, failed_run_alerts, schedule_alerts
from sentinel_stream.events import RunFinished, RunStarted, utc

RUN_COLUMNS = ["dag_id", "run_id", "run_type", "scheduled_for", "start", "end", "state"]
FETCH_AHEAD = pd.Timedelta("1D")
# How far back to look for expected runs whose deadlines are still ahead when starting from history.
# Must be at least the longest grace or SLA of any DAG.
MAX_DEADLINE = pd.Timedelta("2D")


class RunsStep:
    """expected_runs  f(start, end) -> DataFrame(dag_id, scheduled_for, sla_minutes) for start <= scheduled_for < end
    grace_min      as in schedule_alerts: a float, or {dag_id: minutes}"""

    def __init__(self, expected_runs: Callable, grace_min=DEFAULT_GRACE_MIN):
        self._expected_runs = expected_runs
        self.grace_min = grace_min
        self._runs: dict = {}             # (dag_id, run_id) -> run record (RUN_COLUMNS)
        self._by_schedule: dict = {}      # (dag_id, scheduled_for) -> run records
        self._finished: dict = {}         # dag_id -> finished run records, in the order they finished
        self._job_alerts_at: dict = {}    # (dag_id, end) -> job-run alerts already given out
        self._timers: list = []           # heap of (deadline, n, dag_id, scheduled_for, sla_minutes)
        self._n = 0
        self._fetched_to = None
        self._not_before = None           # deadlines before this fired before the stream started

    # ------------------------------------------------------------------------------- loading
    def load(self, runs: pd.DataFrame, now) -> None:
        """Start at `now` knowing these runs (the columns of `DataSource.job_runs`): every run that
        started before `now`; one that had not ended before `now` counts as still running."""
        now = utc(now)
        for r in (runs.itertuples() if runs is not None and len(runs) else ()):
            self._start(RunStarted(utc(r.start), r.dag_id, r.run_id, r.run_type, utc(r.scheduled_for)))
            if r.end is not None and not pd.isna(r.end) and utc(r.end) < now:
                self._finish(RunFinished(utc(r.end), r.dag_id, r.run_id, r.state))
        self._not_before = now
        self._fetched_to = now - MAX_DEADLINE

    # -------------------------------------------------------------------------------- events
    def _start(self, ev: RunStarted) -> dict:
        run = {"dag_id": ev.dag_id, "run_id": ev.run_id, "run_type": ev.run_type,
               "scheduled_for": ev.scheduled_for, "start": ev.time, "end": None, "state": "running"}
        self._runs[(ev.dag_id, ev.run_id)] = run
        self._by_schedule.setdefault((ev.dag_id, ev.scheduled_for), []).append(run)
        return run

    def _finish(self, ev: RunFinished) -> dict:
        run = self._runs.get((ev.dag_id, ev.run_id))
        if run is None:
            raise KeyError(f"run {ev.run_id} of {ev.dag_id} finished but its start was never seen")
        run["end"], run["state"] = ev.time, ev.state
        self._finished.setdefault(ev.dag_id, []).append(run)
        return run

    def started(self, ev: RunStarted) -> list[Alert]:
        self._start(ev)
        return []

    def finished(self, ev: RunFinished) -> list[Alert]:
        run = self._finish(ev)
        alerts = alerts_from_schedule(failed_run_alerts(pd.DataFrame([run], columns=RUN_COLUMNS)))
        # job run baseline: the batch rule on this DAG's finished runs; keep what it says AT this
        # run's end and has not been given out yet (two runs of a DAG can end in the same second)
        scored = job_run_alerts(pd.DataFrame(self._finished[ev.dag_id], columns=RUN_COLUMNS))
        at_end = alerts_from_job_runs(scored[scored.ts == ev.time])
        given = self._job_alerts_at.get((ev.dag_id, ev.time), 0)
        self._job_alerts_at[(ev.dag_id, ev.time)] = len(at_end)
        return alerts + at_end[given:]

    # ------------------------------------------------------------------------------ deadlines
    def _fetch(self, upto: pd.Timestamp) -> None:
        """Make sure every expected run scheduled before `upto` has its deadlines on the timer heap."""
        while self._fetched_to < upto:
            end = self._fetched_to + FETCH_AHEAD
            expected = self._expected_runs(self._fetched_to, end)
            for dag, sched, sla in zip(expected.dag_id, pd.to_datetime(expected.scheduled_for, utc=True),
                                       expected.sla_minutes):
                grace = self.grace_min.get(dag, DEFAULT_GRACE_MIN) if isinstance(self.grace_min, dict) else self.grace_min
                for deadline in sorted({sched + pd.Timedelta(minutes=float(grace)),
                                        sched + pd.Timedelta(minutes=float(sla))}):
                    if deadline >= self._not_before:
                        self._n += 1
                        heapq.heappush(self._timers, (deadline, self._n, dag, sched, sla))
            self._fetched_to = end

    def due(self, now: pd.Timestamp, inclusive: bool) -> list[tuple]:
        """[(deadline, alerts)] for every deadline before `now` (or at it, if inclusive), in order."""
        if self._fetched_to is None:
            self.load(None, now)
        self._fetch(now + pd.Timedelta("1ns") if inclusive else now)
        out = []
        while self._timers and (self._timers[0][0] < now or (inclusive and self._timers[0][0] == now)):
            deadline, _, dag, sched, sla = heapq.heappop(self._timers)
            expected = pd.DataFrame([{"dag_id": dag, "scheduled_for": sched, "sla_minutes": sla}])
            runs = pd.DataFrame(self._by_schedule.get((dag, sched), []), columns=RUN_COLUMNS)
            rows = schedule_alerts(expected, runs, now=deadline, grace_min=self.grace_min)
            out.append((deadline, alerts_from_schedule(rows[rows.ts == deadline])))
        return out
