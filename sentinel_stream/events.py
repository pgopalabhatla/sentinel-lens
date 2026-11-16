"""What goes into the streaming engine and what comes out of it.

In:   one minute of metric values (see StreamEngine.on_minute) and DAG-run events as they happen.
Out:  AlertEvent for every alert that fires, PageEvent for everything that happens to a page.

Every output carries two times, and they are not the same thing:
    alert.time   the time the alert is ABOUT. Identical to the batch pipeline's alert time.
    emitted_at   the stream time at which the engine could first know it.
For a seasonal alert `alert.time` is the START of its 5-minute bucket (that is how the batch pipeline
labels a 5-minute mean), but the bucket is only complete when its last minute has arrived, so
`emitted_at` is 4 minutes later. For every other detector the two are equal.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from sentinel_lens.alerts import Alert


def utc(x) -> pd.Timestamp:
    t = pd.Timestamp(x)
    return t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")


# ------------------------------------------------------------------------------------------ in
@dataclass(frozen=True)
class RunStarted:
    """A DAG run started (what Airflow's API shows the moment a run appears)."""
    time: pd.Timestamp           # when it started
    dag_id: str
    run_id: str
    run_type: str                # "scheduled" or "manual"
    scheduled_for: pd.Timestamp


@dataclass(frozen=True)
class RunFinished:
    """A DAG run ended, with its final state ("success" or "failed")."""
    time: pd.Timestamp           # when it ended
    dag_id: str
    run_id: str
    state: str


# ----------------------------------------------------------------------------------------- out
@dataclass(frozen=True)
class AlertEvent:
    emitted_at: pd.Timestamp
    alert: Alert
    demoted: bool                # its feedback group is demoted: it joins a page but cannot page anyone


PAGE_EVENT_KINDS = ("opened", "alert_added", "merged", "paged")


@dataclass(frozen=True)
class PageEvent:
    """Something happened to a page.

    opened        a new page, started by `alert`
    alert_added   `alert` joined an existing page
    merged        `alert` linked two or more pages; `absorbed` are the keys that no longer exist
    paged         an engineer is paged now: at the end of a step the page holds an alert that is not
                  demoted and has not paged before. Decided once per step, after all of the step's
                  alerts have been grouped; `alert` is the page's earliest alert that is not demoted

    `page` is the engine's own key for the page and never changes while the page lives (a merge keeps
    the oldest key). `page_id` is the batch correlator's id ("<first alert time>|<its entity>"); it
    changes when an alert with an earlier time joins.
    """
    kind: str
    emitted_at: pd.Timestamp
    page: int
    page_id: str
    alert: Alert
    n_alerts: int
    absorbed: tuple = ()
