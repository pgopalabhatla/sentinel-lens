"""Smoke test of the dataset: replay API, agent-style tools, and a deliberately naive detector
scored by the evaluator. The naive detector is a floor to beat, not a design to copy.

    python examples/quickstart.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sentinel_sim.evaluate import evaluate_detections  # noqa: E402
from sentinel_sim.replay import DataSource  # noqa: E402

DATA = Path(__file__).resolve().parents[1] / "data"

# --- 1. what an agent sees 10 minutes into the consumer-lag incident (INC-003)
ds = DataSource(DATA).at("2026-10-09T15:00:00Z")
print("query_metric:", ds.query_metric("kafka_consumergroup_lag|orders-etl", window="3h"))
print("kafka_group:", ds.kafka_group("orders-etl"))
print("changes (last 6h):", [(c["ts"], c["kind"], c["target"], c["description"]) for c in ds.changes(window="6h")])
print(ds.logs(pod="orders-etl-consumer-0", window="15min", level="WARN").message.head(3).tolist())

# --- 2. naive detector: rolling robust z-score vs same minute over the previous 7 days, 3-of-5 persistence
full = DataSource(DATA)
cat = full.series(layer=[1, 2, 3])
cols = [s for s in cat.series_id if not s.startswith(("kube_pod", "flink_job_restarts", "flink_checkpoint_failures"))]
m = full.query(cols, start="2026-09-28", end="2026-10-26", step="5min")
lagged = np.stack([m.shift(288 * k).values for k in range(1, 8)])        # same time, previous 7 days
med = np.nanmedian(lagged, axis=0)
mad = np.nanmedian(np.abs(lagged - med), axis=0) * 1.4826 + 1e-9
z = pd.DataFrame((m.values - med) / mad, index=m.index, columns=m.columns)
hot = (z.abs() > 6).astype(int).rolling(5).sum() >= 3
dets = [{"ts": ts, "target": col} for col in hot.columns for ts in hot.index[hot[col].values]]
# counters: any increase is a detection
for sid in full.series(metric=["kube_pod_container_status_restarts_total", "flink_job_restarts_total",
                               "flink_checkpoint_failures_total"]).series_id:
    s = full.query(sid, start="2026-09-28", end="2026-10-26")[sid]
    dets += [{"ts": ts, "target": sid} for ts in s.index[s.diff() > 0]]
# jobs: failed or SLA-missed runs
jr = full.job_runs(window="30D")
dets += [{"ts": r.end, "target": f"dag:{r.dag_id}"} for r in jr.itertuples() if r.state == "failed" or r.sla_missed]

res = evaluate_detections(pd.DataFrame(dets), DATA, split="test")
print({k: v for k, v in res.items() if k not in ("per_incident", "alerts_table")})
print(res["per_incident"].to_string(index=False))
