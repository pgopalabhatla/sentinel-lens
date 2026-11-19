"""POST-HOC, section 2 of docs/results/live-week-2-posthoc.md: atk-004 (dag_paused on customer_agg).

Was any run of the DAG due in the pause, did its batch pods report anything to drop, what did the
stream carry for it, and what did the attacker say when it sent the injection.

    python scripts/posthoc/lw2_s2_dag_pause.py
"""
from __future__ import annotations

import json

import pandas as pd

from lw2_common import DATA, campaign_record, injections, load

DAG, POD_PREFIX = "customer_agg", "customer-agg"


def main() -> None:
    from sentinel_sim.replay import DataSource
    inj = next(i for i in injections() if i["id"] == "atk-004")
    start, stop = inj["start"], inj["stop"]
    print(f"atk-004: {inj['scenario']} on {inj['target']}, {start} - {stop}, params {inj['params']}")

    topo = json.loads((DATA / "catalog" / "topology.json").read_text())
    print("schedule:", topo["dags"][DAG])
    ds = DataSource(DATA, now="2026-11-02T00:00Z")
    day0, day1 = start.floor("D"), stop.floor("D") + pd.Timedelta("1D")
    exp = ds.expected_runs(start=day0, end=day1 + pd.Timedelta("1D"))
    exp = exp[exp["dag_id"] == DAG]
    print("expected runs around the pause:", [str(t) for t in exp["scheduled_for"]])
    print("expected runs inside the pause:", [str(t) for t in exp["scheduled_for"] if start <= t < stop] or "none")

    ids = [s for s in ds.series()["series_id"] if POD_PREFIX in s]
    q = ds.query(ids, start=day0, end=day1 + pd.Timedelta("6h"))
    per_hour = q.notna().sum(axis=1).groupby(q.index.floor("h")).sum()
    print(f"quiet-world readings of the {len(ids)} {POD_PREFIX} series, by hour ({day0:%b %d} to {day1:%b %d} 06:00):")
    print(per_hour[per_hour > 0].to_string())
    in_pause = q[(q.index >= start) & (q.index < stop)]
    print("readings inside the pause:", int(in_pause.notna().sum().sum()))

    print("\non the stream (dag runs and changes of the DAG):")
    for r in load("telemetry"):
        if r["type"] == "dag_run" and r["dag_id"] == DAG and day0 <= pd.Timestamp(r["ts"]) < day1 + pd.Timedelta("1D"):
            print(f"  dag_run {r['event']:6s} {r['run_id']} at {r['ts']} {r.get('state', '')}")
        if r["type"] == "changes":
            for c in r["changes"]:
                if c["target"] == DAG:
                    print(f"  change {c['kind']} ts {c['ts']} published {r['ts']}: {c['description']}")

    print("\nthe attacker's note for the wake that sent it:")
    for w in campaign_record()["wakes"]:
        if any(c.get("id") == "atk-004" for c in (w.get("commands") or [])) or w["now"] == start.strftime("%Y-%m-%dT%H:%M:%SZ"):
            print(" ", w["now"], w.get("note"))


if __name__ == "__main__":
    main()
