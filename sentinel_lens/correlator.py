"""Correlator: group related alerts into incidents, so one problem is one page.

One real problem raises many alerts: a consumer falls behind, so its lag, its CPU, its throttling
and the topic it reads all alert within minutes. An engineer should be paged once for that.

Two alerts are linked when BOTH hold:
    time      they are at most `gap_min` minutes apart
    topology  their entities are at most `max_hops` steps apart in the entity graph
              (0 = the same entity, 1 = directly related, e.g. a pod and its consumer group)

An incident is a connected component of those links (single linkage: A-B and B-C puts A, B and C
together even if A and C are more than `gap_min` apart, which is how a long incident stays one
incident as long as its alerts keep coming).

Nothing here reads files. The topology is built from two plain tables: the relationships
(child_kind, child, parent_kind, parent) and the series catalog (series_id, entity_kind, entity),
plus the platform's extra edges from sentinel_lens/topology_extra.py (relationships that are true of
the platform but missing from the relationships table).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from sentinel_lens.alerts import Alert
from sentinel_lens.topology_extra import EXTRA_EDGES


# ------------------------------------------------------------------------------------- topology
@dataclass(frozen=True)
class Topology:
    """The entity graph, plus which entity each series belongs to.

    entity_of   {series_id: "kind:name"}, e.g. "kafka_consumergroup_lag|orders-etl" -> "consumer_group:orders-etl"
    neighbours  {"kind:name": entities one step away}; undirected
    """
    entity_of: dict
    neighbours: dict

    def entity(self, target: str) -> str:
        """The entity an alert target belongs to, the way the scorer maps it: a series_id goes through
        the catalog; anything without "|" already is an entity ("dag:daily_orders")."""
        if "|" not in target:
            return target
        if target not in self.entity_of:
            raise KeyError(f"series {target!r} is not in the catalog, so its entity is unknown")
        return self.entity_of[target]

    def within(self, entity: str, max_hops: int) -> frozenset:
        """Every entity at most `max_hops` steps from `entity`, itself included."""
        seen, frontier = {entity}, {entity}
        for _ in range(max_hops):
            frontier = {n for e in frontier for n in self.neighbours.get(e, ())} - seen
            if not frontier:
                break
            seen |= frontier
        return frozenset(seen)


def build_topology(relationships: pd.DataFrame, catalog: pd.DataFrame, extra_edges=EXTRA_EDGES) -> Topology:
    """Topology from the relationships table and the series catalog, with `extra_edges` merged in.

    extra_edges  rows of (child_kind, child, relation, parent_kind, parent, ...) added to the
                 relationships. Default: the tracked list in sentinel_lens/topology_extra.py.
                 Pass () for the relationships exactly as given.

    Every relationship row is one undirected edge between "child_kind:child" and
    "parent_kind:parent", whatever the relation is called (consumes, member_of, runs, ...). Pod
    membership is part of that table (member_of, broker_of, runs), so no separate pod table is needed.
    Entities that appear in no relationship are simply unconnected: their alerts only group with
    alerts on the same entity.
    """
    neighbours: dict = {}
    pairs = list(zip(relationships.child_kind, relationships.child, relationships.parent_kind, relationships.parent))
    pairs += [(e[0], e[1], e[3], e[4]) for e in extra_edges]
    for ck, c, pk, p in pairs:
        a, b = f"{ck}:{c}", f"{pk}:{p}"
        neighbours.setdefault(a, set()).add(b)
        neighbours.setdefault(b, set()).add(a)
    entity_of = {sid: f"{kind}:{name}" for sid, kind, name in zip(catalog.series_id, catalog.entity_kind,
                                                                  catalog.entity)}
    return Topology(entity_of=entity_of, neighbours={k: frozenset(v) for k, v in neighbours.items()})


# ------------------------------------------------------------------------------------- incident
@dataclass(frozen=True)
class Incident:
    id: str                  # "<start time>|<entity of the first alert>": stable while the first alert is
    start: pd.Timestamp      # time of the earliest alert
    end: pd.Timestamp        # time of the latest alert
    alerts: tuple            # the Alert records, ordered by (time, target, detector)
    targets: tuple           # distinct alert targets, sorted
    entities: tuple          # distinct entities, sorted
    detectors: tuple         # distinct detector labels, sorted
    max_severity: float      # the highest alert score (severity, not probability)
    n_detectors: int         # distinct kinds of detector; "seasonal fallback" counts as "seasonal"
    n_targets: int
    title: str               # the explanation of the earliest alert


def _order(a: Alert):
    return (a.time, a.target, a.detector)


def _incident(alerts: list, entities: list) -> Incident:
    first = alerts[0]
    detectors = tuple(sorted({a.detector for a in alerts}))
    targets = tuple(sorted({a.target for a in alerts}))
    return Incident(id=f"{first.time:%Y-%m-%dT%H:%M:%SZ}|{entities[0]}", start=first.time, end=alerts[-1].time,
                    alerts=tuple(alerts), targets=targets, entities=tuple(sorted(set(entities))),
                    detectors=detectors, max_severity=max(a.score for a in alerts),
                    n_detectors=len({d.removesuffix(" fallback") for d in detectors}), n_targets=len(targets),
                    title=first.explanation)


# ------------------------------------------------------------------------------------ correlate
def correlate(alerts: Iterable[Alert], topology: Topology, gap_min: float | None = 15,
              max_hops: int | None = 1) -> list[Incident]:
    """Group alerts into incidents.

    alerts    raw Alert records (every alert, not one per target per 30 minutes)
    gap_min   two alerts can link only if they are at most this many minutes apart.
              None = ignore time (group by topology only).
    max_hops  ... and only if their entities are at most this many steps apart. 0 = same entity only.
              None = ignore topology (group by time only, even across unconnected entities).

    Returns incidents ordered by (start, id). The result depends only on the set of alerts, not on
    the order they are passed in.
    """
    alerts = sorted(alerts, key=_order)
    entities = [topology.entity(a.target) for a in alerts]
    gap = None if gap_min is None else pd.Timedelta(minutes=gap_min)

    parent = list(range(len(alerts)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def link_in_time(indices: list):
        """Alerts (already in time order) that may all link to each other: join neighbours in time."""
        for a, b in zip(indices, indices[1:]):
            if gap is None or alerts[b].time - alerts[a].time <= gap:
                ra, rb = find(a), find(b)
                if ra != rb:
                    parent[max(ra, rb)] = min(ra, rb)

    by_entity: dict = {}
    for i, e in enumerate(entities):
        by_entity.setdefault(e, []).append(i)          # indices stay in time order

    if max_hops is None:
        link_in_time(list(range(len(alerts))))
    else:
        names = sorted(by_entity)
        for n, e1 in enumerate(names):
            reach = topology.within(e1, max_hops)
            link_in_time(by_entity[e1])
            for e2 in names[n + 1:]:
                if e2 in reach:
                    # every pair across (or inside) these two entities may link if close in time,
                    # so joining neighbours in their merged time order finds the same groups
                    link_in_time(sorted(by_entity[e1] + by_entity[e2]))

    groups: dict = {}
    for i in range(len(alerts)):
        groups.setdefault(find(i), []).append(i)
    incidents = [_incident([alerts[i] for i in members], [entities[i] for i in members])
                 for members in groups.values()]
    return sorted(incidents, key=lambda inc: (inc.start, inc.id))
