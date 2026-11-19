"""POST-HOC, section 5 of docs/results/live-week-2-posthoc.md: how loud the generic incidents were.

For each generic injection: its effects, the guardrail-triggering ones (effects on a series a guardrail
rule watches), the alerts on or 1 hop from its targets inside [start, stop + 30 min] by detector, the
first alert that is not demoted, and - a COUNTERFACTUAL - the first one left once the alerts on the
guardrail-triggering series are removed. Effects are applied per series, so removing one removes exactly
the alerts on its series; and any page holding a non-demoted alert pages at the end of its step
(sentinel_stream/pages.py), so that alert's emission time is the page-out time.

    python scripts/posthoc/lw2_s5_generic.py
"""
from __future__ import annotations

from collections import Counter

import pandas as pd

from lw2_common import GUARD_METRICS, injections, load, topology


def main() -> None:
    alerts, topo = load("alerts"), topology()
    for inj in injections():
        if inj["scenario"] != "custom":
            continue
        near = frozenset().union(*(topo.within(e, 1) for e in inj["entities"]))
        lo, hi = inj["start"], inj["stop"] + pd.Timedelta("30min")
        guard = [e["series_id"] for e in inj["effects"] if e["series_id"].split("|")[0] in GUARD_METRICS]
        rows = sorted((pd.Timestamp(r["emitted_at"]), r["alert"]["target"], r["alert"]["detector"], r["demoted"],
                       r["alert"]["explanation"]) for r in alerts
                      if lo <= pd.Timestamp(r["alert"]["time"]) <= hi and topo.entity(r["alert"]["target"]) in near)
        live = [x for x in rows if not x[3]]
        rest = [x for x in live if x[1] not in guard]
        eff = {e["series_id"] for e in inj["effects"]}
        hit = {x[1] for x in live}
        print(f"\n== {inj['id']} ({inj['severity']}): {len(inj['effects'])} effects; guardrail-triggering: {guard or 'none'}")
        print(f"   alerts near the targets by detector: {dict(Counter(x[2] for x in rows))}; demoted {sum(x[3] for x in rows)}")
        if live:
            f = live[0]
            print(f"   first non-demoted: {f[0]} ({(f[0] - inj['start']) / pd.Timedelta('1min'):.0f} min) {f[1]} ({f[2]})")
        if rest:
            f = rest[0]
            print(f"   COUNTERFACTUAL without guardrail series: {f[0]} ({(f[0] - inj['start']) / pd.Timedelta('1min'):.0f} min) "
                  f"{f[1]} ({f[2]}); {len(rest)} alerts on {len({x[1] for x in rest})} series left")
            print(f"      {f[4]}")
        else:
            print("   COUNTERFACTUAL without guardrail series: nothing left, it would not have paged")
        print(f"   effect series that alerted ({len(eff & hit)} of {len(eff)}): {sorted(eff & hit)}")
        print(f"   effect series that never alerted: {sorted(eff - hit)}")


if __name__ == "__main__":
    main()
