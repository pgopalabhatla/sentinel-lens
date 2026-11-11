"""Why do labeled incidents split into several pages, and what would two possible remedies do?

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/diagnose_fragmentation.py

REPORT ONLY, TRAIN split only. The correlator and its defaults (gap 15 min, 1 hop) are not changed;
everything tried here is built in this script on top of them.

1. For every labeled incident that the default correlator splits into more than one page, each pair
   of its pages is explained:
       (a) no topology path   the two pages' entities are not connected at all in the entity graph
       (b) quiet gap          linkable entities (same entity or 1 hop apart) exist in both pages, but
                              their alerts are more than 15 minutes apart
       (c) other              a path exists but is longer than the hop limit
2. Experiment A: extra topology edges. Only relationships that are true of the platform itself are
   added, each with its justification from the platform description, not from the scorer. A second
   variant joins all entities that share a catalog source_id.
3. Experiment B: a recurrence rule. A page that starts with an alert on a target of an earlier page,
   within R minutes of that page's last alert, is attached to it (R = 60, 120, 180).

IMPORTANT: A and B were chosen AFTER looking at how the train incidents fragment. Any improvement
they show on train is therefore optimistic: they are fitted to these twelve incidents by construction.
They are hypotheses to test on data they were not designed on, not results.

Pages are judged exactly as in scripts/score_incidents.py (the existing scorer, untouched). Data is
read only through DataSource with the clock at the end of train; train labels only.
"""
import warnings

import numpy as np
import pandas as pd

from scripts.score_formulas import current_alerts
from scripts.score_incidents import DEFAULT, TRAIN_END_TS, TRAIN_START, judge_alerts, judge_pages, summarize
from scripts.score_seasonal import DATA, ds
from sentinel_lens.alerts import to_frame
from sentinel_lens.correlator import Incident, build_topology, correlate
from sentinel_lens.topology_extra import EXTRA_EDGES
from sentinel_sim.evaluate import evaluate_detections

GAP_MIN, MAX_HOPS = DEFAULT["gap_min"], DEFAULT["max_hops"]
RECURRENCE_MIN = (60, 120, 180)

# Experiment A: the six edges tried here were adopted afterwards and now live in the tracked list
# sentinel_lens/topology_extra.py: (child_kind, child, relation, parent_kind, parent, justification).
# This report still starts from the topology as given, so its numbers stay reproducible.
REAL_EDGES = list(EXTRA_EDGES)
EDGE_COLUMNS = ["child_kind", "child", "relation", "parent_kind", "parent"]


# ------------------------------------------------------------------------------------ helpers
def shortest_path(topology, a: str, b: str):
    """Shortest path between two entities as a list of nodes, or None if they are not connected."""
    if a == b:
        return [a]
    parent, frontier = {a: None}, [a]
    while frontier:
        nxt = []
        for node in frontier:
            for n in sorted(topology.neighbours.get(node, ())):
                if n not in parent:
                    parent[n] = node
                    if n == b:
                        path = [b]
                        while parent[path[-1]] is not None:
                            path.append(parent[path[-1]])
                        return path[::-1]
                    nxt.append(n)
        frontier = nxt
    return None


def min_gap_min(times_a, times_b) -> float:
    """Smallest time distance in minutes between any alert of one set and any alert of the other."""
    a = np.array([t.value for t in times_a], dtype="int64")[:, None]
    b = np.array([t.value for t in times_b], dtype="int64")[None, :]
    return float(np.abs(a - b).min()) / 60e9


def explain_pair(topology, p, q) -> dict:
    ents_p = {topology.entity(a.target) for a in p.alerts}
    ents_q = {topology.entity(a.target) for a in q.alerts}
    paths = [(shortest_path(topology, e1, e2), e1, e2) for e1 in sorted(ents_p) for e2 in sorted(ents_q)]
    connected = [(len(path) - 1, path) for path, _, _ in paths if path is not None]
    gap_any = min_gap_min([a.time for a in p.alerts], [a.time for a in q.alerts])
    row = {"page_1": f"{p.start:%m-%d %H:%M}-{p.end:%H:%M}", "page_2": f"{q.start:%m-%d %H:%M}-{q.end:%H:%M}",
           "closest_alerts_min": round(gap_any, 1)}
    if not connected:
        row.update(hops="no path", reason="(a) no topology path",
                   detail=f"no path between {{{', '.join(sorted(ents_p))}}} and {{{', '.join(sorted(ents_q))}}}")
        return row
    hops, path = min(connected, key=lambda c: (c[0], c[1]))
    row["hops"] = hops
    if hops > MAX_HOPS:
        also = "" if gap_any <= GAP_MIN else f"; their alerts are also {gap_any:.0f} min apart"
        row.update(reason="(c) other: hop limit", detail=f"nearest entities are {hops} hops apart: "
                   + " - ".join(path) + also)
        return row
    # linkable entities exist: the split is in time. Gap between alerts on linkable entity pairs only.
    linkable = [(e1, e2) for path, e1, e2 in paths if path is not None and len(path) - 1 <= MAX_HOPS]
    gaps = [min_gap_min([a.time for a in p.alerts if topology.entity(a.target) == e1],
                        [a.time for a in q.alerts if topology.entity(a.target) == e2]) for e1, e2 in linkable]
    row.update(reason="(b) quiet gap", detail=f"no alert for {min(gaps):.0f} min on "
               + (" / ".join(sorted({e for pair in linkable for e in pair}))) + f" (limit {GAP_MIN} min)")
    return row


def merged_incident(group: list, topology) -> Incident:
    """One Incident from several (for the recurrence experiment); same fields as the correlator makes."""
    alerts = sorted({a for inc in group for a in inc.alerts}, key=lambda a: (a.time, a.target, a.detector))
    entities = [topology.entity(a.target) for a in alerts]
    detectors = tuple(sorted({a.detector for a in alerts}))
    targets = tuple(sorted({a.target for a in alerts}))
    return Incident(id=f"{alerts[0].time:%Y-%m-%dT%H:%M:%SZ}|{entities[0]}", start=alerts[0].time, end=alerts[-1].time,
                    alerts=tuple(alerts), targets=targets, entities=tuple(sorted(set(entities))), detectors=detectors,
                    max_severity=max(a.score for a in alerts),
                    n_detectors=len({d.removesuffix(" fallback") for d in detectors}), n_targets=len(targets),
                    title=alerts[0].explanation)


def with_recurrence(incidents: list, topology, window_min: float) -> list:
    """Attach a page to an earlier one if it starts with an alert on one of that page's targets within
    `window_min` of the earlier page's last alert. Pages are taken in start order; chains are followed."""
    window = pd.Timedelta(minutes=window_min)
    groups = []                                   # each: {"members": [...], "end": ts, "targets": set}
    for inc in sorted(incidents, key=lambda i: (i.start, i.id)):
        first_targets = {a.target for a in inc.alerts if a.time == inc.start}
        home = next((g for g in groups if first_targets & g["targets"] and inc.start - g["end"] <= window), None)
        if home is None:
            groups.append({"members": [inc], "end": inc.end, "targets": set(inc.targets)})
        else:
            home["members"].append(inc)
            home["end"] = max(home["end"], inc.end)
            home["targets"] |= set(inc.targets)
    return sorted((merged_incident(g["members"], topology) for g in groups), key=lambda i: (i.start, i.id))


def clique_edges(catalog: pd.DataFrame) -> pd.DataFrame:
    """An edge between every two entities that share a catalog source_id."""
    rows = []
    ents = catalog.assign(kind=catalog.entity_kind, name=catalog.entity)[["source_id", "kind", "name"]].drop_duplicates()
    for _, g in ents.groupby("source_id"):
        items = sorted(zip(g.kind, g.name))
        rows += [(k1, n1, "same_source", k2, n2) for i, (k1, n1) in enumerate(items) for k2, n2 in items[i + 1:]]
    return pd.DataFrame(rows, columns=EDGE_COLUMNS)


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    everything, _ = current_alerts()
    alerts = [a for a in everything if TRAIN_START <= a.time < TRAIN_END_TS]
    relationships, catalog = ds.relationships(), ds.series()
    topology = build_topology(relationships, catalog, extra_edges=())      # the topology as given: this report's baseline
    klass, matched, _ = judge_alerts(alerts)
    klass_of, matched_of = dict(zip(alerts, klass)), dict(zip(alerts, matched))
    labeled_info = evaluate_detections(to_frame(everything), DATA, split="train")["per_incident"].set_index("incident_id")

    def score(name, incidents):
        return summarize(name, judge_pages(incidents, klass_of, matched_of), labeled_info)

    def show(rows):
        cols = ["setting", "pages", "TRUE", "FALSE", "precision", "recall", "pages_per_labeled_mean",
                "pages_per_labeled_max", "pages_covering_2+", "largest_page_alerts"]
        print(pd.DataFrame(rows)[cols].to_string(index=False))

    base = correlate(alerts, topology, **DEFAULT)
    pages = judge_pages(base, klass_of, matched_of)
    by_id = {i.id: i for i in base}
    baseline = score("default (gap 15 min, 1 hop)", base)

    print("FRAGMENTATION DIAGNOSIS on the TRAIN split. Report only: the correlator defaults are unchanged.\n")
    print("NOTE: experiments A and B below were chosen AFTER seeing how these train incidents fragment,\n"
          "so their train numbers are optimistic by construction.\n")

    # ------------------------------------------------------------------ 1. why each split
    print("1. Labeled incidents that split into more than one page, and why (every pair of their pages)")
    split_summary = []
    for lid in labeled_info.index:
        mine = sorted((by_id[i] for i in pages.id[pages.labeled.apply(lambda ls, lid=lid: lid in ls)]),
                      key=lambda i: (i.start, i.id))
        if len(mine) < 2:
            continue
        print(f"\n   {lid} ({labeled_info.scenario[lid]}): {len(mine)} pages")
        for n, inc in enumerate(mine, 1):
            print(f"     page {n}: {inc.start:%m-%d %H:%M} to {inc.end:%m-%d %H:%M}, {len(inc.alerts)} alerts, "
                  f"{', '.join(inc.entities)}")
        rows = [{"pair": f"{i + 1}-{j + 1}", **explain_pair(topology, mine[i], mine[j])}
                for i in range(len(mine)) for j in range(i + 1, len(mine))]
        table = pd.DataFrame(rows)
        with pd.option_context("display.max_colwidth", 200, "display.width", 250):
            print(table[["pair", "closest_alerts_min", "hops", "reason", "detail"]].to_string(index=False))
        split_summary.append({"incident": lid, "pages": len(mine),
                              **{r: int((table.reason == r).sum()) for r in
                                 ["(a) no topology path", "(b) quiet gap", "(c) other: hop limit"]}})
    print("\n   Summary (number of page pairs by reason):")
    print(pd.DataFrame(split_summary).to_string(index=False))

    # ------------------------------------------------------------------ 2. experiment A
    print("\n2. Experiment A: extra topology edges (chosen after seeing train; report only)")
    print("   Edges added, each a real platform relationship:")
    for n, (ck, c, rel, pk, p, why) in enumerate(REAL_EDGES, 1):
        print(f"   A{n}. {ck}:{c} --{rel}--> {pk}:{p}\n       why: {why}")
    extra = pd.DataFrame([e[:5] for e in REAL_EDGES], columns=EDGE_COLUMNS)
    rows = [baseline]
    for n in range(len(REAL_EDGES)):
        if n in (3, 4, 5) and n != 3:
            continue                                                  # the three DAG edges are tried together
        chosen = extra.iloc[[3, 4, 5]] if n == 3 else extra.iloc[[n]]
        label = "A4-A6 only (DAGs - Airflow)" if n == 3 else f"A{n + 1} only ({chosen.child.iloc[0]} - {chosen.parent.iloc[0]})"
        topo = build_topology(pd.concat([relationships[EDGE_COLUMNS], chosen]), catalog, extra_edges=())
        rows.append(score(label, correlate(alerts, topo, **DEFAULT)))
    topo_a = build_topology(pd.concat([relationships[EDGE_COLUMNS], extra]), catalog, extra_edges=())
    rows.append(score("A1-A6 together (all real edges)", correlate(alerts, topo_a, **DEFAULT)))
    cliques = clique_edges(catalog)
    topo_src = build_topology(pd.concat([relationships[EDGE_COLUMNS], cliques]), catalog, extra_edges=())
    rows.append(score(f"same catalog source_id joined ({len(cliques)} edges)", correlate(alerts, topo_src, **DEFAULT)))
    show(rows)
    print("   Entities that share a source_id: "
          + "; ".join(f"{s}: {n}" for s, n in catalog.assign(e=catalog.entity_kind + ':' + catalog.entity)
                      .groupby("source_id").e.nunique().items()))

    a_pages = judge_pages(correlate(alerts, topo_a, **DEFAULT), klass_of, matched_of)
    print("\n   Pages per labeled incident, default -> all real edges -> same source_id:")
    src_pages = judge_pages(correlate(alerts, topo_src, **DEFAULT), klass_of, matched_of)
    frag = pd.DataFrame({name: {lid: int(p.labeled.apply(lambda ls, lid=lid: lid in ls).sum()) for lid in labeled_info.index}
                         for name, p in [("default", pages), ("real_edges", a_pages), ("same_source", src_pages)]})
    print(frag[frag.default > 1].T.to_string())

    # ------------------------------------------------------------------ 3. experiment B
    print("\n3. Experiment B: recurrence rule on top of the default pages (chosen after seeing train; report only)")
    rows = [baseline]
    b_frag = {"default": frag.default}
    for r in RECURRENCE_MIN:
        merged = with_recurrence(base, topology, r)
        rows.append(score(f"B: re-attach within {r} min", merged))
        bp = judge_pages(merged, klass_of, matched_of)
        b_frag[f"B_{r}"] = pd.Series({lid: int(bp.labeled.apply(lambda ls, lid=lid: lid in ls).sum())
                                      for lid in labeled_info.index})
    show(rows)
    print("\n   Pages per labeled incident:")
    bf = pd.DataFrame(b_frag)
    print(bf[bf.default > 1].T.to_string())

    print("\n4. A and B together (all real edges, then recurrence; report only)")
    base_a = correlate(alerts, topo_a, **DEFAULT)
    show([baseline] + [score(f"A1-A6 + B within {r} min", with_recurrence(base_a, topo_a, r)) for r in RECURRENCE_MIN])

    print("\nReminder: A and B were designed after looking at the train fragmentation above. The defaults\n"
          "(gap 15 min, 1 hop, the topology as given, no recurrence) are unchanged.")
