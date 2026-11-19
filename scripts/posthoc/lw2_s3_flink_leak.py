"""POST-HOC, section 3 of docs/results/live-week-2-posthoc.md: atk-010 (flink_state_leak).

Recomputes the seasonal scores of both taskmanagers' memory with the frozen detector function and the
frozen settings for this metric (docs/config-frozen.md: seasonal fallback, history 'day_type',
floor 0.05, abs_floor 0, threshold 6, 5-minute means), hour by hour over the leak; which term set the
wobble; and, as a COUNTERFACTUAL only, the same with floor 0 (MAD alone).

    python scripts/posthoc/lw2_s3_flink_leak.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from lw2_common import DATA, MiB, injections, load, metric_series

FROZEN = dict(threshold=6.0, floor=0.05, history="day_type", abs_floor=0.0)


def main() -> None:
    from sentinel_lens.detectors.seasonal import seasonal_scores
    from sentinel_sim.replay import DataSource
    inj = next(i for i in injections() if i["id"] == "atk-010")
    start, stop = inj["start"], inj["stop"]
    print(f"atk-010: {start} - {stop}, params {inj['params']}")
    telemetry, alerts = load("telemetry"), load("alerts")
    ds = DataSource(DATA)
    for sid in inj["series"]:
        pod = sid.split("|")[1]
        s = metric_series(sid, telemetry)
        sc = seasonal_scores(s, **FROZEN).set_index("ts")
        w = sc[(sc.index >= start) & (sc.index <= stop)]
        rel = 0.05 * w["expected"].abs()
        peak = w.loc[w["z"].idxmax()]
        print(f"\n== {pod}: {len(w)} readings; baseline_days {sorted(set(w['baseline_days']))}")
        print(f"5% floor sets the wobble in {np.isclose(w['wobble'], rel).mean():.0%} of readings; "
              f"wobble {w['wobble'].min() / MiB:.0f}-{w['wobble'].max() / MiB:.0f} MiB")
        print(f"hot readings: {int(w['hot'].sum())}; peak z {peak['z']:+.2f} at {w['z'].idxmax()}: value {peak['value'] / MiB:.0f}, "
              f"expected {peak['expected'] / MiB:.0f}, excess {(peak['value'] - peak['expected']) / MiB:.0f} MiB; "
              f"|z| > 6 needs excess > {6 * peak['wobble'] / MiB:.0f} MiB")
        rate = inj["params"][f"rate_tm{pod[-1]}_mib_per_hour"]
        print(f"hours of leak at {rate:g} MiB/h to reach that excess: {6 * peak['wobble'] / MiB / rate:.1f}")
        print("| hour (UTC) | value | expected | wobble | z max |")
        hourly = w.resample("1h").agg({"value": "mean", "expected": "mean", "wobble": "mean", "z": "max"})
        for t, r in hourly.iterrows():
            print(f"| {t:%H:%M} | {r['value'] / MiB:.0f} | {r['expected'] / MiB:.0f} | {r['wobble'] / MiB:.0f} | {r['z']:+.2f} |")
        n_alerts = sum(1 for a in alerts if a["alert"]["target"] == sid
                       and start <= pd.Timestamp(a["alert"]["time"]) <= stop + pd.Timedelta("30min"))
        print(f"engine alerts on this series in [start, stop + 30 min]: {n_alerts}")
        spec = ds.pod_spec(pod)
        print(f"memory limit {spec['memory_limit_bytes'] / MiB:.0f} MiB; peak value is {w['value'].max() / spec['memory_limit_bytes']:.0%} of it")
        # COUNTERFACTUAL: no relative floor
        cf = seasonal_scores(s, **{**FROZEN, "floor": 0.0}).set_index("ts")
        cw = cf[(cf.index >= start) & (cf.index <= stop)]
        first = cw.index[cw["hot"]].min() if cw["hot"].any() else None
        print(f"COUNTERFACTUAL floor=0: wobble median {cw['wobble'].median() / MiB:.0f} MiB; "
              f"hot {int(cw['hot'].sum())} of {len(cw)}; first hot {first}")


if __name__ == "__main__":
    main()
