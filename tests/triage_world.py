"""A tiny synthetic world for the triage tests: a data directory and the records of the three topics.

One day (2026-01-05). A producer pod writes to topic `tp`; consumer group `grp` (pod `p-a`) reads it;
DAG `d1` runs hourly at :05. Everything exists BEFORE and AFTER the page-outs, so a test can check that
nothing after a page's paging time is visible: readings, run messages, logs, events and changes.

Page records (topic order):
    0  opened  page 1  01:00  lag|grp
    1  paged   page 1  01:00
    2  opened  page 2  02:00  cpu of p-a
    3  paged   page 2  02:00
    4  alert_added page 1  02:05  lag|grp (after page 1 paged)
    5  merged  page 1  02:10  absorbs page 2
    6  opened  page 3  03:00  memory of p-b
    7  paged   page 3  03:00
    8  alert_added page 3  03:30
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import numpy as np
import pandas as pd

DAY = pd.Timestamp("2026-01-05", tz="UTC")
LAG, CONSUMED, IN_RATE = "kafka_consumergroup_lag|grp", "kafka_consumer_records_consumed_per_sec|grp", "kafka_messages_in_per_sec|tp"
CPU_A, MEM_B, RESTARTS_B = "container_cpu_usage_cores|p-a", "container_memory_working_set_bytes|p-b", "kube_pod_container_status_restarts_total|p-b"
SERIES = [(LAG, "kafka_consumergroup_lag", "grp", "consumer_group", "records"),
          (CONSUMED, "kafka_consumer_records_consumed_per_sec", "grp", "consumer_group", "msg/s"),
          (IN_RATE, "kafka_messages_in_per_sec", "tp", "topic", "msg/s"),
          (CPU_A, "container_cpu_usage_cores", "p-a", "pod", "cores"),
          (MEM_B, "container_memory_working_set_bytes", "p-b", "pod", "bytes"),
          (RESTARTS_B, "kube_pod_container_status_restarts_total", "p-b", "pod", "count")]


def iso(t) -> str:
    return pd.Timestamp(t).isoformat()


def at(hhmm: str) -> pd.Timestamp:
    h, m = hhmm.split(":")
    return DAY + pd.Timedelta(hours=int(h), minutes=int(m))


def write_data_dir(root: Path) -> Path:
    root = Path(root)
    for sub in ("catalog", "metrics", "logs", "k8s", "changes", "jobs", "labels"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"series_id": s, "metric": m, "entity": e, "entity_kind": k, "layer": "x", "canonical_event": "",
                   "source_id": "src", "source_type": "x", "unit": u, "labels": "", "default_detector": ""}
                  for s, m, e, k, u in SERIES]).to_csv(root / "catalog/series.csv", index=False)
    topology = {
        "time_range": {"start": iso(DAY), "end": iso(DAY + pd.Timedelta("1D")), "step": "1m"},
        "sources": [], "images": {}, "spark_base_config": {}, "splits": {},
        "pods": [{"pod": p, "namespace": "ns", "role": "r", "source_id": "src", "cpu_limit_cores": 2.0,
                  "memory_limit_bytes": 2 ** 31, "batch_only": False} for p in ("p-a", "p-b")],
        "relationships": [{"child_kind": "consumer_group", "child": "grp", "relation": "consumes", "parent_kind": "topic", "parent": "tp"},
                          {"child_kind": "pod", "child": "p-a", "relation": "member_of", "parent_kind": "consumer_group", "parent": "grp"},
                          {"child_kind": "pod", "child": "p-b", "relation": "produces_to", "parent_kind": "topic", "parent": "tp"}],
        "dags": {"d1": {"schedule": "5 * * * *", "tasks": ["t"], "spark_job": None, "sla_minutes": 20, "retries": 1}},
    }
    (root / "catalog/topology.json").write_text(json.dumps(topology))
    minutes = pd.date_range(DAY, periods=24 * 60, freq="1min")
    pd.DataFrame({s: np.arange(len(minutes), dtype=float) for s, *_ in SERIES}, index=minutes).to_csv(
        root / "metrics/metrics.csv.gz", compression="gzip")
    with gzip.open(root / "logs/logs.jsonl.gz", "wt") as f:
        for t in pd.date_range(DAY, periods=24 * 6, freq="10min"):
            f.write(json.dumps({"ts": iso(t + pd.Timedelta("7s")), "pod": "p-a", "level": "INFO", "logger": "app",
                                "message": f"processed batch {t.hour * 60 + t.minute} in 12 ms"}) + "\n")
    with open(root / "k8s/events.jsonl", "w") as f:
        for hhmm, reason in (("00:30", "Started"), ("01:50", "Unhealthy"), ("02:30", "BackOff"), ("05:00", "Killing")):
            f.write(json.dumps({"ts": iso(at(hhmm)), "type": "Normal", "reason": reason, "message": f"{reason} p-a", "count": 1,
                                "involved_object": {"kind": "Pod", "name": "p-a", "namespace": "ns"}}) + "\n")
    with open(root / "changes/changes.jsonl", "w") as f:
        for hhmm in ("00:45", "02:30"):
            f.write(json.dumps({"ts": iso(at(hhmm)), "kind": "deploy", "target": "p-a", "details": {"image": ["a", "b"]}}) + "\n")
    for name in ("jobs/dag_runs.jsonl", "jobs/spark_apps.jsonl"):
        (root / name).write_text("")
    return root


def telemetry() -> list:
    """Every minute of the day (values = minutes since midnight, p-b's memory missing 02:50-02:59),
    and the d1 runs: :05 start, :07 finish, except the 02:05 run, which never starts."""
    out = []
    for i, t in enumerate(pd.date_range(DAY, periods=24 * 60, freq="1min")):
        if t.minute == 5 and t.hour != 2:
            run = f"scheduled__{t:%Y-%m-%dT%H:%M}"
            out.append({"type": "dag_run", "event": "start", "ts": iso(t), "dag_id": "d1", "run_id": run,
                        "run_type": "scheduled", "scheduled_for": iso(t)})
        if t.minute == 7 and t.hour != 2:
            out.append({"type": "dag_run", "event": "finish", "ts": iso(t), "dag_id": "d1",
                        "run_id": f"scheduled__{t - pd.Timedelta('2min'):%Y-%m-%dT%H:%M}", "state": "success"})
        values = {s: float(i) for s, *_ in SERIES}
        if at("02:50") <= t < at("03:00"):
            del values[MEM_B]
        out.append({"type": "metrics", "ts": iso(t), "values": values})
    return out


def _alert(hhmm, target, score=0.9, detector="seasonal_mad"):
    return {"time": iso(at(hhmm)), "target": target, "detector": detector, "score": score, "direction": "up",
            "explanation": f"{target} high at {hhmm}", "measure": 1.0, "line": 0.5}


def _page(kind, hhmm, page, page_id, alert, n, absorbed=()):
    return {"kind": kind, "emitted_at": iso(at(hhmm)), "page": page, "page_id": page_id, "n_alerts": n,
            "absorbed": list(absorbed), "alert": alert}


def pages_and_alerts() -> tuple:
    a1, a2, a3, a4 = _alert("01:00", LAG), _alert("02:00", CPU_A), _alert("02:05", LAG), _alert("02:10", IN_RATE)
    b1, b2 = _alert("03:00", MEM_B), _alert("03:30", MEM_B)
    p1, p2, p3 = "2026-01-05T01:00:00Z|consumer_group:grp", "2026-01-05T02:00:00Z|pod:p-a", "2026-01-05T03:00:00Z|pod:p-b"
    pages = [_page("opened", "01:00", 1, p1, a1, 1), _page("paged", "01:00", 1, p1, a1, 1),
             _page("opened", "02:00", 2, p2, a2, 1), _page("paged", "02:00", 2, p2, a2, 1),
             _page("alert_added", "02:05", 1, p1, a3, 2), _page("merged", "02:10", 1, p1, a4, 4, absorbed=[2]),
             _page("opened", "03:00", 3, p3, b1, 1), _page("paged", "03:00", 3, p3, b1, 1),
             _page("alert_added", "03:30", 3, p3, b2, 2)]
    alerts = [{"emitted_at": a["time"], "demoted": False, "alert": a} for a in (a1, a2, a3, a4, b1, b2)]
    return pages, alerts
