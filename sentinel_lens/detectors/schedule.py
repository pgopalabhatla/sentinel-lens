"""Schedule watcher: "should a run have happened by now?"

A paused or stuck DAG produces no metric at all, so no baseline can see it. The only signal is the
absence of a run that the schedule promised. Two deadlines per expected run:

    start_late   at scheduled_for + grace         no scheduled run has started
    sla_breach   at scheduled_for + sla_minutes   a run started in time for the SLA but has not finished

Each alert is stamped with its deadline and decided only from what was known at that moment, so the
result for a given deadline is the same whether you ask one minute or one week later.

Inputs are plain DataFrames (the shapes Airflow's API gives you); nothing here reads the sample data.
"""
from __future__ import annotations

import pandas as pd

# Why 10 minutes: on warm-up + train every healthy scheduled run of every DAG started within 0.47 min
# of its schedule (525 runs), and the one unhealthy run was 460 min late. Anything from 1 to 400 min
# separates the two perfectly, so the data does not pick the number. 10 min is ~20x the worst healthy
# start, which leaves room for a busy scheduler, and costs only 10 min on an outage that otherwise
# surfaces hours later. No per-DAG override is needed on train; the dict form exists for DAGs with
# sensors or long queues.
DEFAULT_GRACE_MIN = 10.0

COLUMNS = ["ts", "target", "rule", "explanation"]


def _utc(x):
    return pd.to_datetime(x, utc=True)


def _hm(ts) -> str:
    return ts.strftime("%H:%M")


def _scheduled_runs(runs: pd.DataFrame) -> dict:
    """(dag_id, scheduled_for) -> (start, end) for non-manual runs. A manual run is someone pressing
    the button; it does not satisfy the schedule."""
    if runs is None or runs.empty:
        return {}
    r = runs[runs.run_type != "manual"]
    out = {}
    for dag, sched, start, end in zip(r.dag_id, _utc(r.scheduled_for), _utc(r.start), _utc(r.end)):
        key = (dag, sched)
        if key not in out or start < out[key][0]:      # duplicates: keep the earliest start
            out[key] = (start, end)
    return out


def schedule_alerts(expected: pd.DataFrame, runs: pd.DataFrame, now,
                    grace_min: float | dict = DEFAULT_GRACE_MIN) -> pd.DataFrame:
    """Late-start and SLA alerts for every expected run whose deadline has passed by `now`.

    expected   dag_id, scheduled_for, sla_minutes
    runs       dag_id, run_type, scheduled_for, start, end, state (times as strings, Timestamps or None)
    grace_min  minutes allowed between the schedule and the start; a float, or {dag_id: minutes}
               (DAGs missing from the dict get DEFAULT_GRACE_MIN)
    """
    now = _utc(now)
    by_key = _scheduled_runs(runs)
    rows = []
    for dag, sched, sla in zip(expected.dag_id, _utc(expected.scheduled_for), expected.sla_minutes):
        grace = grace_min.get(dag, DEFAULT_GRACE_MIN) if isinstance(grace_min, dict) else grace_min
        start, end = by_key.get((dag, sched), (pd.NaT, pd.NaT))
        start_by = sched + pd.Timedelta(minutes=float(grace))
        finish_by = sched + pd.Timedelta(minutes=float(sla))

        # start_late: at start_by, had a run started? (NaT <= x is False, so "no run" counts as no.)
        if start_by <= now and not (start <= start_by):
            rows.append({"ts": start_by, "target": f"dag:{dag}", "rule": "start_late",
                         "explanation": f"{dag} was due at {_hm(sched)}, no run had started by "
                                        f"{_hm(start_by)} (grace {float(grace):g} min)"})

        # sla_breach: at finish_by, a run was in progress but not done. If nothing had started by
        # then, start_late has already reported it; a second alert would say nothing new.
        if finish_by <= now and start <= finish_by and not (end <= finish_by):
            rows.append({"ts": finish_by, "target": f"dag:{dag}", "rule": "sla_breach",
                         "explanation": f"{dag} was due at {_hm(sched)}, its run had not finished by "
                                        f"{_hm(finish_by)} (SLA {float(sla):g} min)"})
    out = pd.DataFrame(rows, columns=COLUMNS)
    return out.sort_values("ts", kind="stable").reset_index(drop=True)


def failed_run_alerts(runs: pd.DataFrame) -> pd.DataFrame:
    """One alert per run with state == "failed", at the moment it ended. Manual runs count too:
    a failure is a failure whoever started it."""
    if runs is None or runs.empty:
        return pd.DataFrame(columns=COLUMNS)
    failed = runs[runs.state == "failed"]
    rows = [{"ts": end, "target": f"dag:{dag}", "rule": "failed_run",
             "explanation": f"{dag} run due at {_hm(sched)} failed at {_hm(end)}"}
            for dag, sched, end in zip(failed.dag_id, _utc(failed.scheduled_for), _utc(failed.end))]
    out = pd.DataFrame(rows, columns=COLUMNS)
    return out.sort_values("ts", kind="stable").reset_index(drop=True)
