"""The streaming engine: the final detection system, one minute at a time, in one process.

    engine = StreamEngine.warm_start(ds, at="2026-10-19")      # state from history, nothing after `at`
    for event in engine.on_minute(ts, values): ...             # AlertEvent / PageEvent as they happen
    for event in engine.on_run_event(RunStarted(...)): ...
    engine.advance(now)                                        # let deadlines before `now` fire

Inputs must arrive in time order. At one timestamp the order is: run events, then deadlines, then
the minute of metrics. After everything before a time `c` has been fed and `advance(c)` called, the
engine has emitted exactly what it can know at clock `c` (the same convention as `DataSource.at(c)`:
readings stamped before `c` are in, `c` itself is not).

What each part does is in its own module; this one only routes inputs to them:

    seasonal_step    5-minute means, seasonal baseline as routed, 3-of-5 persistence
    guardrail_step   counter increases and must-be-zero gauges, every minute
    runs_step        schedule watcher, failed runs, job run baseline, from DAG-run events
    pages            incremental correlator and frozen feedback demotions

The engine reads no files and no labels. `warm_start` needs an object with the replay API's methods
(series, relationships, query, job_runs, expected_runs, at); everything else is plain values.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from sentinel_lens import router
from sentinel_lens.correlator import Topology, build_topology
from sentinel_lens.stream.events import AlertEvent, RunFinished, RunStarted, utc
from sentinel_lens.stream.guardrail_step import GuardrailStep
from sentinel_lens.stream.pages import Pager
from sentinel_lens.stream.runs_step import RunsStep
from sentinel_lens.stream.seasonal_step import PERSIST, SeasonalStep

# The feedback state frozen at the end of train (docs/config-frozen.md, "Demoted groups carried into
# the test week"). It is configuration here: the engine never reads labels. A test checks it against
# the frozen config file.
FROZEN_DEMOTED_GROUPS = frozenset({("seasonal", "cpu_throttling"), ("seasonal fallback", "jvm_gc")})

SAMPLE_EVERY = pd.Timedelta("1min")       # the input cadence: one reading per series per minute
SUPPORTED_RULES = ("seasonal", "counter_rule", "above_zero")


def _order(alert):
    return (alert.time, alert.target, alert.detector)


class StreamEngine:
    """routes              the router's table (series_id, rule, fallback, history, abs_floor)
    topology            the correlator's Topology
    canonical_event_of  {series_id: canonical event}, for the feedback groups
    expected_runs       f(start, end) -> expected DAG runs (see runs_step)
    demoted_groups      feedback groups that are demoted for the whole run (frozen feedback)
    """

    def __init__(self, routes: pd.DataFrame, topology: Topology, canonical_event_of: dict, expected_runs,
                 demoted_groups=FROZEN_DEMOTED_GROUPS, step="5min"):
        unknown = sorted(set(routes.rule) - set(SUPPORTED_RULES))
        if unknown:
            raise ValueError(f"the streaming engine has no step for rule(s) {unknown}")
        s = routes[routes.rule == "seasonal"]
        self.seasonal = SeasonalStep(s.series_id, ["seasonal fallback" if f else "seasonal" for f in s.fallback],
                                     s.history, s.abs_floor, step=step)
        self.guardrails = GuardrailStep(routes[routes.rule == "counter_rule"].series_id,
                                        routes[routes.rule == "above_zero"].series_id)
        self.runs = RunsStep(expected_runs)
        self.pager = Pager(topology, demoted_groups, canonical_event_of)
        self.step = pd.Timedelta(step)
        self.now = None                   # everything stamped before this has been processed
        self._bucket_start = None         # the open 5-minute bucket and its minutes so far
        self._bucket_ts, self._bucket_rows = [], []
        self._columns = None              # how to pick this engine's series out of an input row
        self._positions = None

    # ------------------------------------------------------------------------------- loading
    @classmethod
    def warm_start(cls, ds, at, history_start=None, alert_log=(), demoted_groups=FROZEN_DEMOTED_GROUPS,
                   step="5min") -> "StreamEngine":
        """An engine whose state is what it would be had it been running until `at`.

        ds             replay API; only `ds.at(at)` is read, so nothing at or after `at` is seen
        history_start  do not read history before this (the batch pipeline's HISTORY_START). Default:
                       as far back as the detectors can use (28 days of metrics, every earlier run).
        alert_log      AlertEvents this engine emitted before `at`, if it is being restarted. Detector
                       state comes back from the metric and run history; pages cannot (an alert is
                       not in that history), so they are rebuilt from the alerts already emitted.
        """
        at = utc(at)
        view = ds.at(at)
        catalog = view.series()
        routes = router.route_series(view.series(layer=[1, 2, 3]))
        engine = cls(routes, build_topology(view.relationships(), catalog),
                     dict(zip(catalog.series_id, catalog.canonical_event)),
                     expected_runs=lambda start, end: ds.at(end).expected_runs(start=start, end=end),
                     demoted_groups=demoted_groups, step=step)
        engine._load(view, at, None if history_start is None else utc(history_start))
        for event in alert_log:
            engine.pager.add(event.alert, event.emitted_at)
        return engine

    def _load(self, view, at: pd.Timestamp, history_start) -> None:
        def table(ids, start, end, **kw):
            start = start if history_start is None else max(start, history_start)
            if not ids or start >= end:
                return pd.DataFrame(columns=ids, dtype=float)
            return view.query(ids, start=start, end=end, **kw)

        # seasonal: closed buckets as history, then the last n-1 closed buckets scored without
        # emitting anything (they fill the persistence window), then the minutes of the open bucket
        sids = self.seasonal.series_ids
        bucket = at.floor(self.step)
        first_scored = bucket - (PERSIST["n"] - 1) * self.step
        index = self.seasonal.history_index(first_scored)
        self.seasonal.load(table(sids, index[0], first_scored, step=self.step).reindex(index))
        recent = pd.date_range(first_scored, bucket - self.step, freq=self.step)
        closed = table(sids, first_scored, bucket, step=self.step).reindex(recent)
        for ts, row in zip(closed.index, closed.to_numpy(dtype=float)):
            self.seasonal.score(ts, row)
        self._bucket_start = bucket
        open_minutes = table(sids, bucket, at)
        self._bucket_ts = list(open_minutes.index)
        self._bucket_rows = list(open_minutes.to_numpy(dtype=float))

        # guardrails: the previous reading of each counter
        last = table(self.guardrails.counter_ids, at - SAMPLE_EVERY, at)
        if len(last):
            self.guardrails.load(last.index[-1], last.to_numpy(dtype=float)[-1])

        # runs: everything that started before `at`
        if history_start is None:
            runs = view.job_runs(end=at, window="3650D")
        else:
            runs = view.job_runs(start=history_start, end=at)
        self.runs.load(runs, at)
        self.now = at

    # -------------------------------------------------------------------------------- inputs
    def on_minute(self, ts, values) -> list:
        """One minute of readings: `values` is {series_id: value} or a pandas Series indexed by
        series_id. Series the engine watches that are missing from it count as no reading (NaN)."""
        ts = utc(ts)
        self._check_order(ts)
        out = self._deadlines(ts, inclusive=True)
        row = self._vector(values)
        n_s, n_c = len(self.seasonal.series_ids), len(self.guardrails.counter_ids)
        alerts = self.guardrails.step(ts, row[n_s:n_s + n_c], row[n_s + n_c:])

        bucket = ts.floor(self.step)
        if self._bucket_start is None:
            self._bucket_start = bucket
        while self._bucket_start < bucket:                    # a gap in the input: close what is open
            alerts += self._close_bucket()
        self._bucket_ts.append(ts)
        self._bucket_rows.append(row[:n_s])
        if ts + SAMPLE_EVERY >= bucket + self.step:           # that was the bucket's last minute
            alerts += self._close_bucket()
        self.now = ts + SAMPLE_EVERY
        return out + self._emit(alerts, ts)

    def on_run_event(self, event) -> list:
        """A RunStarted or RunFinished, at the time it happened."""
        self._check_order(event.time)
        out = self._deadlines(event.time, inclusive=False)
        if isinstance(event, RunStarted):
            alerts = self.runs.started(event)
        elif isinstance(event, RunFinished):
            alerts = self.runs.finished(event)
        else:
            raise TypeError(f"not a run event: {event!r}")
        self.now = max(self.now, event.time) if self.now is not None else event.time
        return out + self._emit(alerts, event.time)

    def advance(self, now) -> list:
        """Nothing new arrived, but time passed: fire the deadlines before `now`."""
        now = utc(now)
        self._check_order(now)
        out = self._deadlines(now, inclusive=False)
        self.now = now
        return out

    # ------------------------------------------------------------------------------ internals
    def _check_order(self, ts) -> None:
        if self.now is not None and ts < self.now - SAMPLE_EVERY:
            raise ValueError(f"input at {ts} is older than the engine's clock {self.now}")

    def _vector(self, values) -> np.ndarray:
        """The engine's series (seasonal, then counters, then must-be-zero) out of one input row."""
        wanted = self.seasonal.series_ids + self.guardrails.counter_ids + self.guardrails.zero_ids
        if isinstance(values, pd.Series):
            if self._columns is None or (values.index is not self._columns and not values.index.equals(self._columns)):
                self._columns, self._positions = values.index, values.index.get_indexer(wanted)
            raw = values.to_numpy(dtype=float)
            return np.where(self._positions >= 0, raw[self._positions], np.nan)
        return np.array([values.get(sid, np.nan) for sid in wanted], dtype=float)

    def _close_bucket(self) -> list:
        """Mean of the bucket's minutes (missing readings skipped, as pandas resampling does it in
        the batch pipeline's `query(step="5min")`), scored by the seasonal step."""
        n = len(self.seasonal.series_ids)
        start = self._bucket_start
        if self._bucket_rows:
            block = pd.DataFrame(np.vstack(self._bucket_rows), index=pd.DatetimeIndex(self._bucket_ts))
            means = block.resample(self.step).mean().to_numpy(dtype=float)[0]
        else:
            means = np.full(n, np.nan)
        self._bucket_start, self._bucket_ts, self._bucket_rows = start + self.step, [], []
        return self.seasonal.score(start, means) if n else []

    def _deadlines(self, now, inclusive: bool) -> list:
        out = []
        for deadline, alerts in self.runs.due(now, inclusive):
            out += self._emit(alerts, deadline)
        return out

    def _emit(self, alerts, emitted_at) -> list:
        out = []
        for alert in sorted(alerts, key=_order):
            out.append(AlertEvent(emitted_at, alert, self.pager.is_demoted(alert)))
            out += self.pager.add(alert, emitted_at)
        return out

    # --------------------------------------------------------------------------------- views
    def pages(self) -> list:
        return self.pager.pages()

    def paging_pages(self) -> list:
        return self.pager.paging()

    @property
    def demoted(self) -> frozenset:
        return frozenset(self.pager.demoted)
