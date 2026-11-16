"""Drive the engine from the replay API: `DataSource.stream()` minutes merged with DAG-run events.

The dataset stores each DAG run as one finished record. `run_events` turns the records into the two
events a live system would see (a start at the run's start time, a finish at its end time), and
`replay` feeds them to the engine in time order between the minutes. The driver knows the whole
period in advance, as a message queue would hold it; the engine gets each item only at its own time.
"""
from __future__ import annotations

from typing import Iterator

import pandas as pd

from sentinel_lens.stream.engine import StreamEngine
from sentinel_lens.stream.events import RunFinished, RunStarted, utc


def run_events(ds, start, end, history_start=None) -> list:
    """RunStarted / RunFinished for start <= event time < end, in time order, from `ds.job_runs`
    with the clock at `end` (so a run still going at `end` has no finish event)."""
    start, end = utc(start), utc(end)
    view = ds.at(end)
    runs = view.job_runs(end=end, window="3650D") if history_start is None else view.job_runs(start=history_start, end=end)
    events = []
    for r in runs.itertuples():
        began = utc(r.start)
        if start <= began < end:
            events.append(RunStarted(began, r.dag_id, r.run_id, r.run_type, utc(r.scheduled_for)))
        if r.end is not None and not pd.isna(r.end) and start <= utc(r.end) < end:
            events.append(RunFinished(utc(r.end), r.dag_id, r.run_id, r.state))
    return sorted(events, key=lambda e: (e.time, isinstance(e, RunFinished), e.dag_id, e.run_id))


def replay(engine: StreamEngine, ds, start, end, history_start=None) -> Iterator:
    """Feed the engine everything stamped start <= t < end and yield its output events in order.
    Afterwards the engine stands at clock `end`."""
    start, end = utc(start), utc(end)
    events = run_events(ds, start, end, history_start)
    i = 0
    for ts, row in ds.at(end).stream(start, end):
        while i < len(events) and events[i].time <= ts:
            yield from engine.on_run_event(events[i])
            i += 1
        yield from engine.on_minute(ts, row)
    for event in events[i:]:
        yield from engine.on_run_event(event)
    yield from engine.advance(end)
