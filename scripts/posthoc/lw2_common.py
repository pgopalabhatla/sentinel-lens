"""Shared paths and loaders for the live-week-2 post-hoc scripts (docs/results/live-week-2-posthoc.md).

POST-HOC: everything under scripts/posthoc/ was written after the live week 2 result was seen. These
scripts only read: the exported topics, runs/live-week-2/ and data_live_2/ (through DataSource). They
never write to docs/results/, the broker or data_live_2/.

Order:
    python scripts/posthoc/lw2_export_topics.py        # once, needs the broker: topics -> runs/live-week-2/topics/
    python scripts/posthoc/lw2_s1_predictions.py       # section 1, and so on up to lw2_s6_late_starts.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

RUN = ROOT / "runs" / "live-week-2"                 # gitignored; written by run_live_week_2.py
TOPICS = RUN / "topics"                             # gitignored; written by lw2_export_topics.py
DATA = ROOT / "data_live_2"
TRIAGE_RUNS = (1, 2, 3)
GUARD_METRICS = ("kube_pod_container_status_restarts_total", "flink_job_restarts_total",
                 "flink_checkpoint_failures_total", "kafka_under_replicated_partitions")   # docs/config-frozen.md
MiB = 2 ** 20


def load(name: str):
    """One exported topic: answer_key, alerts, pages, control, telemetry, triage-r1..3."""
    path = TOPICS / f"{name}.json"
    if not path.exists():
        raise SystemExit(f"{path} is missing: run scripts/posthoc/lw2_export_topics.py first (needs the broker)")
    return json.loads(path.read_text())


def campaign_record() -> dict:
    return json.loads((RUN / "campaign" / "record.json").read_text())


def topology():
    from sentinel_lens.correlator import build_topology
    from sentinel_sim.replay import DataSource
    ds = DataSource(DATA)
    return build_topology(ds.relationships(), ds.series())


def injections() -> list:
    from sentinel_stream import live_score
    return live_score.injections(load("answer_key"))[0]


def metric_series(series_id: str, telemetry: list):
    """One series as the engine saw it: the quiet world's history before the stream, then the replayed
    (injected) readings, as 5-minute means (the frozen step)."""
    import pandas as pd
    from sentinel_sim.replay import DataSource
    live = pd.Series({pd.Timestamp(r["ts"]): r["values"][series_id] for r in telemetry
                      if r.get("type") == "metrics" and series_id in r.get("values", {})}, dtype=float)
    hist = DataSource(DATA, now=live.index.min()).query(series_id, start="2026-09-28T00:00Z", end=live.index.min())[series_id]
    s = pd.concat([hist[hist.index < live.index.min()], live]).sort_index()
    s.index = pd.to_datetime(s.index, utc=True)
    s = s.resample("5min").mean()
    s.name = series_id
    return s
