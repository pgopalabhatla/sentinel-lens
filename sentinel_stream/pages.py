"""Incremental correlator: pages that grow as alerts arrive, plus the frozen feedback demotions.

The linking rule is not written again here. When an alert arrives, the batch `correlate` is run on
that alert together with every page it could possibly touch, and the result replaces those pages:

    candidates = pages whose time span, widened by the gap on both sides, contains the alert's time

Two alerts can only link if they are within the gap of each other, so any page the new alert links
to is a candidate; pages that are candidates but turn out not to be linked come back unchanged
(they were complete groups before, and the new alert is the only new thing). The topology half of
the rule is left entirely to `correlate`.

Alerts do not arrive in time order: a seasonal alert about 16:15 arrives at 16:19, after a restart
alert about 16:17. `correlate` does not care about order, so pages end up the same as in batch.

Feedback: an alert whose group (`feedback.group_key`) is in the frozen demoted set is demoted. It
joins its page, but a page pages only when it holds an alert that is not demoted.

Paging is decided once per step, not once per alert. One minute often brings several alerts of the
same problem (a pod, its consumer group and its topic in the same 5-minute bucket). Taken one by one
they would open two pages, page for both, and merge them a moment later, all inside the same minute:
two pages sent for what is one page before anyone could have read the first. So `add` only groups,
and `settle`, called after all alerts of the step are in, pages for every page that holds an alert
that is not demoted and has not paged yet.

Cost note: each new alert re-groups the whole candidate pages, so a page with thousands of alerts
gets slow. Fine for this dataset; a larger one would only need the alerts inside the gap.
"""
from __future__ import annotations

import inspect

import pandas as pd

from sentinel_lens import feedback
from sentinel_lens.alerts import Alert
from sentinel_lens.correlator import Incident, Topology, correlate
from sentinel_stream.events import PageEvent

_CORRELATE = {k: v.default for k, v in inspect.signature(correlate).parameters.items()
              if v.default is not inspect.Parameter.empty}                    # gap_min 15, max_hops 1


class Pager:
    def __init__(self, topology: Topology, demoted_groups=frozenset(), canonical_event_of=None,
                 gap_min=_CORRELATE["gap_min"], max_hops=_CORRELATE["max_hops"]):
        self.topology = topology
        self.demoted_groups = frozenset(demoted_groups)
        self.canonical_event_of = canonical_event_of or {}
        self.gap_min, self.max_hops = gap_min, max_hops
        self._gap = None if gap_min is None else pd.Timedelta(minutes=gap_min)
        self._pages: dict = {}            # key -> Incident (the batch correlator's own record)
        self._paged_at: dict = {}         # key -> when the engine paged for it
        self._next_key = 1
        self._touched: set = set()        # pages changed since the last settle()
        self.demoted: set = set()         # the alerts that are demoted

    def is_demoted(self, alert: Alert) -> bool:
        return bool(self.demoted_groups) and feedback.group_key(alert, self.canonical_event_of) in self.demoted_groups

    def add(self, alert: Alert, emitted_at: pd.Timestamp) -> list[PageEvent]:
        """Group one alert: it opens a page, joins one, or merges several. Never pages; see `settle`."""
        if self.is_demoted(alert):
            self.demoted.add(alert)
        candidates = [k for k, p in self._pages.items()
                      if self._gap is None or p.start - self._gap <= alert.time <= p.end + self._gap]
        pool = [a for k in candidates for a in self._pages[k].alerts] + [alert]
        regrouped = correlate(pool, self.topology, gap_min=self.gap_min, max_hops=self.max_hops)
        page = next(p for p in regrouped if alert in p.alerts)
        members = set(page.alerts)
        joined = sorted(k for k in candidates if self._pages[k].alerts[0] in members)

        if not joined:
            key, kind, absorbed = self._next_key, "opened", ()
            self._next_key += 1
        else:
            key, absorbed = joined[0], tuple(joined[1:])
            kind = "merged" if absorbed else "alert_added"
        paged_before = [self._paged_at.pop(k) for k in joined if k in self._paged_at]
        for k in absorbed:
            del self._pages[k]
        self._pages[key] = page
        self._touched = (self._touched - set(absorbed)) | {key}
        if paged_before:
            self._paged_at[key] = min(paged_before)
        return [PageEvent(kind, emitted_at, key, page.id, alert, len(page.alerts), absorbed)]

    def settle(self, emitted_at: pd.Timestamp) -> list[PageEvent]:
        """End of a step: page for every page changed in it that holds an alert that is not demoted
        and has not paged before. The event carries the page's earliest such alert."""
        events = []
        for key in sorted(self._touched):
            page = self._pages[key]
            if key not in self._paged_at and feedback.pages(page, self.demoted):
                self._paged_at[key] = emitted_at
                first = next(a for a in page.alerts if a not in self.demoted)
                events.append(PageEvent("paged", emitted_at, key, page.id, first, len(page.alerts)))
        self._touched.clear()
        return events

    # --------------------------------------------------------------------------------- views
    def pages(self) -> list[Incident]:
        """Every page so far, ordered like the batch correlator's result."""
        return sorted(self._pages.values(), key=lambda p: (p.start, p.id))

    def paging(self) -> list[Incident]:
        """The pages that page an engineer (at least one alert that is not demoted)."""
        return [p for p in self.pages() if feedback.pages(p, self.demoted)]

    def paged_at(self) -> dict:
        """{page id: stream time at which the engine paged for it}."""
        return {self._pages[k].id: t for k, t in self._paged_at.items()}
