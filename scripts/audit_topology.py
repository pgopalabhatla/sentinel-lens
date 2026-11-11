"""Audit the entity graph: which entities that alert, or are in the catalog, have no relationships?

    source .venv/bin/activate && export PYTHONPATH=.
    python scripts/audit_topology.py

Prints, for the topology as given and with the tracked extra edges (sentinel_lens/topology_extra.py):
entities without any relationship, and the connected components of the graph. An alert on an entity
with no relationship can only be grouped with alerts on that same entity, and two components can
never share a page, so both lists are worth reading whenever the platform changes.

Read-only, through DataSource with the clock at the end of train. No labels are read.
"""
import warnings

from scripts.score_formulas import current_alerts
from scripts.score_seasonal import START, TRAIN_END, ds
from sentinel_lens.correlator import build_topology
from sentinel_lens.topology_extra import EXTRA_EDGES


def components(topology, entities) -> list:
    seen, out = set(), []
    for node in sorted(set(topology.neighbours) | set(entities)):
        if node not in seen:
            comp = topology.within(node, 10_000)
            seen |= comp
            out.append(sorted(comp))
    return out


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)
    relationships, catalog = ds.relationships(), ds.series()
    given = build_topology(relationships, catalog, extra_edges=())
    merged = build_topology(relationships, catalog)

    in_catalog = set(catalog.entity_kind + ":" + catalog.entity)
    alerts, _ = current_alerts()
    in_alerts = {given.entity(a.target) for a in alerts}
    dags = {f"dag:{d}" for d in ds.expected_runs(start=START, end=TRAIN_END).dag_id.unique()}
    entities = in_catalog | in_alerts | dags
    print(f"Entities: {len(in_catalog)} in the catalog, {len(in_alerts)} with alerts since {START}, "
          f"{len(dags)} scheduled DAGs; {len(entities)} distinct. Relationships in the topology: {len(relationships)}.\n")

    print("Platform description (DataSource.sources()):")
    for s in ds.sources():
        print(f"  {s['source_id']}: {s['description']}")

    for name, topo in [("Topology as given", given), (f"With the {len(EXTRA_EDGES)} tracked extra edges", merged)]:
        lonely = sorted(e for e in entities if e not in topo.neighbours)
        comps = components(topo, entities)
        print(f"\n{name}")
        print(f"  entities with no relationship: {', '.join(lonely) or 'none'}")
        print(f"  connected components: {len(comps)}")
        for c in sorted(comps, key=lambda c: (-len(c), c)):
            print(f"    {len(c):2d}  {', '.join(c)}")

    print("\nExtra edges and why each is true of the platform:")
    for ck, c, rel, pk, p, why in EXTRA_EDGES:
        print(f"  {ck}:{c} --{rel}--> {pk}:{p}\n      {why}")
    unused = sorted(set(given.neighbours) - entities)
    print(f"\nIn the relationships but neither in the catalog nor alerting: {', '.join(unused)}")
