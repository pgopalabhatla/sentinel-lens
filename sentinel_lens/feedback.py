"""Feedback loop: learn from engineer labels which kinds of alert are almost never real.

An engineer marks alerts as real or false. Labels are kept per GROUP, a kind of alert:
    series alerts   (detector, canonical event)   e.g. ("seasonal", "cpu_throttling")
                    (detector, metric) when the catalog gives the series no canonical event
    DAG alerts      (detector, entity)            e.g. ("job_run_baseline", "dag:hourly_quality_check")
A group is coarse on purpose: one pod's labels then help every other pod with the same kind of
signal, and a group collects enough labels to say something.

For each group:
    estimate = (true + 1) / (labeled + 2)
the share of its alerts that are real, starting from "one real, one false" before any label
(Laplace's rule). With no labels the estimate is 0.5; it only moves as far as the labels push it.

A group is DEMOTED when
    labeled >= n_min (10)   there is enough evidence to judge it, and
    estimate < tau (0.25)   fewer than one in four of its alerts is real.
With these defaults a group needs at least 10 labels of which at most 1 is real. Both numbers are
round, stated in advance and not tuned on any result.

A demoted alert is not deleted: it stays attached to its page as context. But a page only pages an
engineer if it contains at least one alert that is not demoted.

Everything here is pure and deterministic: no files, no clock, no randomness. The replay takes the
alerts and their labels and decides, for each alert, from the labels that were already available
when it fired. A label arrives `label_delay_min` after its alert and only affects later alerts.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

import pandas as pd

from sentinel_lens.alerts import Alert

N_MIN = 10          # labels a group needs before it can be demoted
TAU = 0.25          # demote when fewer than one in four of its alerts is real
LABEL_DELAY_MIN = 60
TRUE, FALSE = "true", "false_positive"      # the scorer's words; anything else is not a label


def estimate(true: int, false: int) -> float:
    """Estimated share of a group's alerts that are real: (true + 1) / (labeled + 2)."""
    return (true + 1) / (true + false + 2)


def is_demoted(true: int, false: int, n_min: int = N_MIN, tau: float = TAU) -> bool:
    """Enough labels (>= n_min) and too few of them real (estimate < tau)."""
    return (true + false) >= n_min and estimate(true, false) < tau


def group_key(alert: Alert, canonical_event_of: Mapping[str, str]) -> tuple:
    """(detector, canonical event) for a series alert, (detector, entity) for anything else.

    canonical_event_of  {series_id: canonical event}, from the series catalog. A series alert whose
                        series is missing from it raises, so a new series cannot silently fall into
                        no group. A series that is listed with an empty canonical event gets its
                        metric name instead, so unrelated metrics are not lumped into one blank group.
    """
    if "|" not in alert.target:
        return (alert.detector, alert.target)
    if alert.target not in canonical_event_of:
        raise KeyError(f"series {alert.target!r} has no canonical event in the catalog")
    event = canonical_event_of[alert.target]
    if not isinstance(event, str) or not event.strip():
        event = alert.target.split("|")[0]
    return (alert.detector, event)


@dataclass(frozen=True)
class GroupState:
    key: tuple
    true: int
    false: int
    alerts: int                      # alerts seen in this group, labeled or not
    demoted_alerts: int              # how many of them fired while the group was demoted
    first_demoted: pd.Timestamp | None   # time of the first alert that was demoted, if any

    @property
    def labeled(self) -> int:
        return self.true + self.false

    @property
    def estimate(self) -> float:
        return estimate(self.true, self.false)


@dataclass(frozen=True)
class Replay:
    demoted: frozenset               # the alerts that were demoted when they fired
    groups: tuple                    # final GroupState of every group, sorted by key
    n_min: int
    tau: float

    def demoted_now(self, key: tuple) -> bool:
        """Would a new alert of this group be demoted after all labels are in?"""
        g = next(g for g in self.groups if g.key == key)
        return is_demoted(g.true, g.false, self.n_min, self.tau)


def replay(alerts: Iterable[Alert], labels: Mapping[Alert, str], canonical_event_of: Mapping[str, str],
           n_min: int = N_MIN, tau: float = TAU, label_delay_min: float = LABEL_DELAY_MIN) -> Replay:
    """Go through the alerts in time order and decide for each whether its group is demoted by then.

    labels           {alert: "true" or "false_positive"}; alerts that are missing, or have any other
                     value (e.g. "expected"), are simply not labeled
    label_delay_min  a label is available this many minutes after its alert. An alert is judged
                     only with labels whose availability time is at or before the alert's own time,
                     so a label never affects its own alert or anything earlier.

    The result does not depend on the order the alerts are passed in.
    """
    ordered = sorted(alerts, key=lambda a: (a.time, a.target, a.detector))
    delay = pd.Timedelta(minutes=label_delay_min)
    pending = [(a.time + delay, a.time, labels[a], group_key(a, canonical_event_of))
               for a in ordered if labels.get(a) in (TRUE, FALSE)]        # already in arrival order
    counts: dict = {}
    seen: dict = {}
    demoted_count: dict = {}
    first_demoted: dict = {}
    demoted = set()
    arrived = 0
    for a in ordered:
        # labels that have arrived by now, from alerts strictly before this one (so even with no
        # delay a label cannot affect its own alert)
        while arrived < len(pending) and pending[arrived][0] <= a.time and pending[arrived][1] < a.time:
            _, _, label, key = pending[arrived]
            t, f = counts.get(key, (0, 0))
            counts[key] = (t + 1, f) if label == TRUE else (t, f + 1)
            arrived += 1
        key = group_key(a, canonical_event_of)
        seen[key] = seen.get(key, 0) + 1
        if is_demoted(*counts.get(key, (0, 0)), n_min=n_min, tau=tau):
            demoted.add(a)
            demoted_count[key] = demoted_count.get(key, 0) + 1
            first_demoted.setdefault(key, a.time)
    for _, _, label, key in pending[arrived:]:                               # labels that arrive after the last alert
        t, f = counts.get(key, (0, 0))
        counts[key] = (t + 1, f) if label == TRUE else (t, f + 1)
    groups = tuple(GroupState(key=k, true=counts.get(k, (0, 0))[0], false=counts.get(k, (0, 0))[1], alerts=seen[k],
                              demoted_alerts=demoted_count.get(k, 0), first_demoted=first_demoted.get(k))
                   for k in sorted(seen))
    return Replay(demoted=frozenset(demoted), groups=groups, n_min=n_min, tau=tau)


def demoted_groups(groups: Iterable[GroupState], n_min: int = N_MIN, tau: float = TAU) -> frozenset:
    """The keys of the groups that are demoted given their final label counts: the frozen state to
    carry into a later period."""
    return frozenset(g.key for g in groups if is_demoted(g.true, g.false, n_min=n_min, tau=tau))


def apply_frozen(alerts: Iterable[Alert], frozen_groups: frozenset, canonical_event_of: Mapping[str, str]) -> frozenset:
    """The alerts that are demoted when a frozen set of demoted groups is applied unchanged: every
    alert whose group is in the set, from the first alert on. Nothing is learned from these alerts."""
    return frozenset(a for a in alerts if group_key(a, canonical_event_of) in frozen_groups)


def pages(incident, demoted: frozenset) -> bool:
    """A page pages an engineer only if at least one of its alerts is not demoted."""
    return any(a not in demoted for a in incident.alerts)


def paging_time(incident, demoted: frozenset):
    """When the page first pages: the time of its first alert that is not demoted (None if it never does)."""
    return next((a.time for a in incident.alerts if a not in demoted), None)
