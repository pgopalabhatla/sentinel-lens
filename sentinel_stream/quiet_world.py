"""A quiet world for the live stream: the simulator's platform with no incidents, of any length.

    python -m sentinel_stream.quiet_world --days 35 --seed 11 --out data_live

The same platform, the same daily and weekly rhythm and the same background noise as the evaluated
dataset (log lines, single-reading blips, readiness flaps, the odd retried task), but the scenario
calendar is skipped, so there are no incidents and no changes, and the incident labels are empty.

It lives here and not in sentinel_sim/ so that sentinel_sim/ stays identical to the `pre-test-freeze`
tag: it calls the generator's own functions in the generator's own order, leaving out only
`register_all` (the scenario calendar), and then corrects the two outputs that belong to the calendar:
topology.json gets no splits, and anomaly_windows.csv is written as an empty table with its header.

It refuses to write over a dataset that has splits (the evaluated `data/` is one), because the
generator's `write` deletes the output directory first.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from sentinel_sim import core
from sentinel_sim.generate import noise_logs, outliers, write

ANOMALY_WINDOW_COLUMNS = ["incident_id", "split", "actionable", "series_id", "start", "end", "direction"]
MIN_DAYS = 8          # single-reading blips are placed after the first 7 days, so there must be an 8th


def _is_evaluated_dataset(out: Path) -> bool:
    topo = out / "catalog/topology.json"
    if not topo.exists():
        return False
    try:
        return bool(json.loads(topo.read_text()).get("splits"))
    except (ValueError, AttributeError):
        return True                    # unreadable: do not guess, do not delete


def generate(out: Path, seed: int, days: int):
    if days < MIN_DAYS:
        raise SystemExit(f"--days must be at least {MIN_DAYS}.")
    out = Path(out)
    if _is_evaluated_dataset(out):
        raise SystemExit(f"{out} holds a dataset with splits (an evaluated dataset); not overwriting it.")

    # sentinel_sim.generate.main without register_all(ctx), so there are no post-steps either
    ctx = core.Ctx(seed=seed, days=days)
    core.baseline(ctx)
    core.build_runs(ctx)
    core.runs_to_series(ctx)
    core.kafka_physics(ctx)
    noise_logs(ctx)
    blips = outliers(ctx)
    core.finalize(ctx)
    df = write(ctx, out, blips)

    # the warm-up / train / test splits belong to the scenario calendar, so there are none
    topo_path = out / "catalog/topology.json"
    topo = json.loads(topo_path.read_text())
    topo["splits"] = {}
    topo_path.write_text(json.dumps(topo, indent=2, default=str))
    # with no incidents there are no rows to take the columns from, so the header is given
    pd.DataFrame([], columns=ANOMALY_WINDOW_COLUMNS).to_csv(out / "labels/anomaly_windows.csv", index=False)
    return ctx, df


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", required=True)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--days", type=int, default=35)
    args = ap.parse_args(argv)
    ctx, df = generate(Path(args.out), args.seed, args.days)
    print(f"series={df.shape[1]} minutes={df.shape[0]} logs={len(ctx.logs)} events={len(ctx.events)} "
          f"changes={len(ctx.changes)} dag_runs={len(ctx.dag_runs)} spark_apps={len(ctx.spark_apps)} "
          f"incidents={len(ctx.incidents)}")


if __name__ == "__main__":
    main()
