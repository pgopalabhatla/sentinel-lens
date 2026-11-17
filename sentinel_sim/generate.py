"""Generate the Sentinel Lens synthetic dataset.

    python -m sentinel_sim.generate --out data --seed 7 --days 28
"""
from __future__ import annotations

import argparse
import gzip
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from . import core
from .scenarios import register_all
from .topology import (BASE_IMAGES, CANONICAL_EVENTS, DAGS, POD, PODS, RELATIONSHIPS, SOURCES,
                       SPARK_BASE_CONFIG)

NOISE_TEMPLATES = {
    "broker": [("INFO", "[Log partition=orders-{p}, dir=/var/lib/kafka/data] Rolled new log segment at offset {o} in {ms} ms.", "kafka.log.LocalLog"),
               ("INFO", "[ProducerStateManager partition=clicks-{p}] Wrote producer snapshot at offset {o} with 0 producer ids in {ms} ms.", "kafka.log.ProducerStateManager"),
               ("INFO", "[GroupCoordinator {b}]: Member orders-etl-{h} in group orders-etl has left group through explicit `LeaveGroup` request", "kafka.coordinator.group.GroupCoordinator"),
               ("INFO", "[LocalLog partition=__consumer_offsets-{p}, dir=/var/lib/kafka/data] Deleting segment files LogSegment(baseOffset={o}, size=104857600)", "kafka.log.LocalLog")],
    "producer": [("INFO", "Produced batch of {n} records to orders in {ms} ms", "orders_producer.sender"),
                 ("WARN", "Got error produce response with correlation id {o} on topic-partition orders-{p}, retrying (2 attempts left). Error: NOT_LEADER_OR_FOLLOWER", "org.apache.kafka.clients.producer.internals.Sender")],
    "consumer": [("INFO", "Committed offsets for orders-{p} at offset {o}", "kafka.consumer.coordinator"),
                 ("INFO", "Processed {n} records in {ms} ms", "orders_etl.loop")],
    "jobmanager": [("INFO", "Completed checkpoint {o} for job clickstream-enrich ({kb} KB, checkpointDuration={ms} ms, finalizationTime=4 ms).", "org.apache.flink.runtime.checkpoint.CheckpointCoordinator")],
    "taskmanager": [("INFO", "Closing part file s3://lake-clickstream/enriched/part-{o}.parquet ({kb} KB)", "org.apache.flink.connector.file.sink.writer.FileWriterBucket")],
    "driver": [("INFO", "Streaming query made progress: batchId={o}, numInputRows={n}, processedRowsPerSecond={ms}.0", "org.apache.spark.sql.execution.streaming.MicroBatchExecution")],
    "executor": [("INFO", "Finished task {p}.0 in stage {o}.0 (TID {o}). 4091 bytes result sent to driver", "org.apache.spark.executor.Executor")],
    "coordinator": [("INFO", "Query 20261{p}_{o}_q finished: state=FINISHED user=bi_reader elapsed={ms}ms rows={n}", "io.trino.event.QueryMonitor"),
                    ("WARN", "Spilling to disk for query 20261{p}_{o}_q (revocable memory 512MB)", "io.trino.operator.Driver")],
    "worker": [("INFO", "Memory pool: reserved={kb}MB of 4096MB", "io.trino.memory.MemoryPool")],
    "scheduler": [("INFO", "Heartbeat recorded for SchedulerJob (healthy)", "airflow.jobs.job")],
}
RATE = {"broker": 0.6, "producer": 0.5, "consumer": 0.6, "jobmanager": 0.33, "taskmanager": 0.4, "driver": 0.5,
        "executor": 0.15, "coordinator": 0.6, "worker": 0.15, "scheduler": 0.2}


def noise_logs(ctx):
    rng = ctx.rng
    for p in PODS:
        name, src, role, *_, batch = p
        if batch:
            continue
        tpls = NOISE_TEMPLATES.get(role if not (src == "airflow-main" and role == "worker") else "scheduler")
        n = rng.poisson(RATE.get(role, 0.3), ctx.N)
        idx = np.repeat(np.arange(ctx.N), n)
        for t in idx:
            k = rng.random()
            lvl, msg, lg = tpls[0] if (len(tpls) == 1 or k < 0.9) else tpls[1 + int(rng.integers(0, len(tpls) - 1))]
            if role == "producer" and lvl == "WARN" and rng.random() < 0.85:
                lvl, msg, lg = tpls[0]
            if np.isnan(ctx.s[f"container_cpu_usage_cores|{name}"][t]):
                continue
            ctx.log(t + rng.random(), name, lvl, msg.format(
                p=int(rng.integers(0, 6)), o=int(rng.integers(1000, 99999999)), ms=int(rng.integers(2, 900)),
                n=int(rng.integers(50, 5000)), b=int(rng.integers(0, 3)), h=f"{int(rng.integers(0, 1 << 20)):05x}",
                kb=int(rng.integers(100, 4000))), lg)
    # benign readiness flaps (noise for event-based detectors)
    for _ in range(14):
        t = rng.uniform(0, ctx.N)
        ctx.event(t, "trino-coordinator", "Unhealthy", "Readiness probe failed: HTTP probe failed with statuscode: 503",
                  "Warning")


def outliers(ctx, n=40):
    """Single-sample blips that a detector with a persistence check should ignore."""
    rng = ctx.rng
    sids = [k for k in ctx.s if not k.startswith(("kube_pod", "flink_job_restarts", "flink_checkpoint_failures",
                                                   "kafka_under_rep"))]
    out = []
    for _ in range(n):
        sid = sids[int(rng.integers(0, len(sids)))]
        t = int(rng.integers(7 * 1440, ctx.N))
        if np.isnan(ctx.s[sid][t]):
            continue
        ctx.s[sid][t] *= rng.uniform(2.5, 5)
        out.append({"series_id": sid, "ts": ctx.iso(t)})
    return out


def _clean(rec):
    return {k: v for k, v in rec.items() if not k.startswith("_")}


def write(ctx, out: Path, blips):
    if out.exists():
        shutil.rmtree(out)
    for d in ("catalog/mappings", "metrics", "logs", "k8s", "jobs", "changes", "labels"):
        (out / d).mkdir(parents=True, exist_ok=True)

    # ---- metrics (wide, 1-minute)
    idx = pd.date_range(ctx.t0, periods=ctx.N, freq="1min")
    cols = [r["series_id"] for r in ctx.catalog]
    df = pd.DataFrame({c: ctx.s[c] for c in cols}, index=idx)
    df.index.name = "ts"
    df.to_csv(out / "metrics/metrics.csv.gz", float_format="%.5g", compression="gzip",
              date_format="%Y-%m-%dT%H:%M:%SZ")

    # ---- catalog
    pd.DataFrame(ctx.catalog).to_csv(out / "catalog/series.csv", index=False)
    with open(out / "catalog/canonical_events.yaml", "w") as f:
        yaml.safe_dump({k: {"layer": v[0], "default_detector": v[1], "healthy_when": v[2], "description": v[3]}
                        for k, v in CANONICAL_EVENTS.items()}, f, sort_keys=False)
    by_type: dict = {}
    for r in ctx.catalog:
        if r["layer"] == 1:
            continue
        by_type.setdefault(r["source_type"], []).append(
            {"canonical_event": r["canonical_event"] or None, "layer": r["layer"], "metric": r["metric"],
             "entity_kind": r["entity_kind"], "entity": r["entity"], "unit": r["unit"],
             "promql": f'{r["metric"]}{{{r["labels"]}}}', "default_detector": r["default_detector"]})
    for t, rows in by_type.items():
        with open(out / f"catalog/mappings/{t}.yaml", "w") as f:
            yaml.safe_dump({"source_type": t, "events": rows}, f, sort_keys=False)
    topo = {"sources": SOURCES,
            "pods": [{"pod": p[0], "source_id": p[1], "role": p[2], "namespace": core._ns(p[0]),
                      "cpu_limit_cores": p[3], "memory_limit_bytes": int(p[4]), "jvm": p[6],
                      "batch_only": p[7]} for p in PODS],
            "relationships": [dict(zip(["child_kind", "child", "relation", "parent_kind", "parent"], r))
                              for r in RELATIONSHIPS],
            "dags": DAGS, "images": BASE_IMAGES, "spark_base_config": SPARK_BASE_CONFIG,
            "time_range": {"start": ctx.iso(0), "end": ctx.iso(ctx.N), "step": "1m"},
            "splits": {"warmup": [ctx.iso(0), ctx.iso(7 * 1440)], "train": [ctx.iso(7 * 1440), ctx.iso(21 * 1440)],
                       "test": [ctx.iso(21 * 1440), ctx.iso(ctx.N)]}}
    (out / "catalog/topology.json").write_text(json.dumps(topo, indent=2, default=str))

    # ---- logs / events / changes
    ctx.logs.sort(key=lambda r: r["_t"])
    with gzip.open(out / "logs/logs.jsonl.gz", "wt") as f:
        for r in ctx.logs:
            if r["_t"] < ctx.N:
                f.write(json.dumps(_clean(r)) + "\n")
    ctx.events.sort(key=lambda r: r["_t"])
    with open(out / "k8s/events.jsonl", "w") as f:
        for r in ctx.events:
            if r["_t"] < ctx.N:
                f.write(json.dumps(_clean(r)) + "\n")
    ctx.changes.sort(key=lambda r: r["_t"])
    with open(out / "changes/changes.jsonl", "w") as f:
        for r in ctx.changes:
            f.write(json.dumps(_clean(r)) + "\n")

    # ---- jobs
    ctx.dag_runs.sort(key=lambda r: r["_s"])
    with open(out / "jobs/dag_runs.jsonl", "w") as f:
        for r in ctx.dag_runs:
            rr = _clean(r)
            rr["tasks"] = [_clean(t) for t in r["tasks"]]
            f.write(json.dumps(rr) + "\n")
    with open(out / "jobs/spark_apps.jsonl", "w") as f:
        for a in sorted(ctx.spark_apps, key=lambda a: a["_t0"]):
            f.write(json.dumps(_clean(a)) + "\n")
    apps = {a["app_id"]: a for a in ctx.spark_apps}
    flat = []
    for r in ctx.dag_runs:
        app_ids = [t["spark_app_id"] for t in r["tasks"] if t.get("spark_app_id")]
        flat.append({"dag_id": r["dag_id"], "run_id": r["run_id"], "run_type": r["run_type"],
                     "scheduled_for": r["scheduled_for"], "start": r["start"], "end": r["end"],
                     "duration_min": round(r["_e"] - r["_s"], 2), "lateness_min": round(r["_s"] - r["_sched"], 2),
                     "state": r["state"], "sla_missed": r["sla_missed"], "tries": max(t["try_number"] for t in r["tasks"]),
                     "spark_app_ids": ";".join(app_ids),
                     "input_rows": apps[app_ids[-1]]["input_rows"] if app_ids else None})
    pd.DataFrame(flat).to_csv(out / "jobs/job_runs.csv", index=False)

    # ---- labels
    inc = sorted(ctx.incidents, key=lambda i: i["start"])
    for k, i in enumerate(inc, 1):
        i["incident_id"] = f"INC-{k:03d}"
    (out / "labels/incidents.json").write_text(json.dumps(inc, indent=2))
    rows = [{"incident_id": i["incident_id"], "split": i["split"], "actionable": i["actionable"], **s}
            for i in inc for s in i["key_signals"]]
    pd.DataFrame(rows).to_csv(out / "labels/anomaly_windows.csv", index=False)
    pd.DataFrame(blips).to_csv(out / "labels/noise_blips.csv", index=False)
    return df


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--days", type=int, default=28)
    args = ap.parse_args(argv)
    if args.days != 28:
        raise SystemExit("Scenario calendar assumes 28 days (warm-up 7, train 14, test 7).")

    ctx = core.Ctx(seed=args.seed, days=args.days)
    core.baseline(ctx)
    posts = register_all(ctx)
    core.build_runs(ctx)
    core.runs_to_series(ctx)
    core.kafka_physics(ctx)
    for p in posts:
        p()
    noise_logs(ctx)
    blips = outliers(ctx)
    core.finalize(ctx)
    df = write(ctx, Path(args.out), blips)
    print(f"series={df.shape[1]} minutes={df.shape[0]} logs={len(ctx.logs)} events={len(ctx.events)} "
          f"changes={len(ctx.changes)} dag_runs={len(ctx.dag_runs)} spark_apps={len(ctx.spark_apps)} "
          f"incidents={len(ctx.incidents)}")


if __name__ == "__main__":
    main()
