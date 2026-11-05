"""Generation context and generic platform behaviour (the "physics").

Scenarios only change *causes* (a config change, a deploy, more input rows, a slower
consumer). The generic code here turns causes into symptoms, so the same mechanism
produces metrics, logs, events and job records consistently.
"""
from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd
from scipy.signal import lfilter

from .topology import (BASE_IMAGES, DAGS, GiB, MiB, POD, PODS, SPARK_BASE_CONFIG,
                       build_series_catalog)


def _parse_mem(v: str) -> float:
    return float(v[:-1]) * (1 if v.endswith("g") else 1 / 1024)


class Ctx:
    def __init__(self, seed: int = 7, days: int = 28, start: str = "2026-09-28"):
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        self.days = days
        self.N = days * 1440
        self.t0 = pd.Timestamp(start, tz="UTC")
        self.m = np.arange(self.N)
        self.hour = (self.m % 1440) / 60.0
        self.dow = (self.m // 1440 + self.t0.dayofweek) % 7  # 0 = Monday
        self.catalog = build_series_catalog()
        self.s: dict[str, np.ndarray] = {r["series_id"]: np.full(self.N, np.nan) for r in self.catalog}
        self.restart_inc = {p[0]: np.zeros(self.N) for p in PODS}
        self.logs: list[dict] = []
        self.events: list[dict] = []
        self.changes: list[dict] = []
        self.incidents: list[dict] = []
        self.dag_runs: list[dict] = []
        self.spark_apps: list[dict] = []
        self.overrides: dict = {}            # (dag, day) -> dict
        self.hourly_delay = np.zeros(self.N)  # extra start delay (minutes) for hourly DAG
        self.produce_mult = np.ones(self.N)   # multiplies orders produce rate
        self.consumer_replicas = np.full(self.N, 2)        # orders-etl replicas
        self.per_pod_capacity = np.full(self.N, 1100.0)    # msg/s each replica can process
        self.cap_mult = np.ones(self.N)                    # brief pauses (restarts, rebalances)
        self._app_counter = 0

    # ------------------------------------------------------------ time helpers
    def M(self, day, h=0, mm=0, s=0) -> float:
        return day * 1440 + h * 60 + mm + s / 60

    def iso(self, minute: float) -> str:
        return (self.t0 + pd.Timedelta(seconds=round(minute * 60))).strftime("%Y-%m-%dT%H:%M:%SZ")

    def sl(self, a, b) -> slice:
        return slice(max(0, int(a)), min(self.N, int(math.ceil(b))))

    # ------------------------------------------------------------ noise
    def ar(self, sigma=1.0, phi=0.9):
        w = self.rng.normal(0, sigma * math.sqrt(1 - phi ** 2), self.N)
        return lfilter([1.0], [1.0, -phi], w)

    # ------------------------------------------------------------ records
    def log(self, t, pod, level, msg, logger=""):
        self.logs.append({"ts": self.iso(t), "_t": t, "pod": pod, "level": level, "logger": logger, "message": msg})

    def event(self, t, pod, reason, message, type_="Normal", kind="Pod", count=1):
        self.events.append({"ts": self.iso(t), "_t": t, "type": type_, "reason": reason,
                            "involved_object": {"kind": kind, "name": pod, "namespace": _ns(pod)},
                            "message": message, "count": count})

    def change(self, t, kind, target, details, author, description):
        cid = f"chg-{len(self.changes) + 1:04d}"
        self.changes.append({"change_id": cid, "ts": self.iso(t), "_t": t, "kind": kind, "target": target,
                             "details": details, "author": author, "description": description})
        return cid

    def restart(self, pod, t):
        self.restart_inc[pod][min(self.N - 1, int(t))] += 1

    def down(self, pod, a, b):
        for metric in ("container_cpu_usage_cores", "container_memory_working_set_bytes",
                       "jvm_heap_used_ratio", "jvm_gc_time_ms_per_sec"):
            sid = f"{metric}|{pod}"
            if sid in self.s:
                self.s[sid][self.sl(a, b)] = np.nan

    def incident(self, **kw):
        kw.setdefault("novel_in_test", False)
        kw["start"], kw["end"] = self.iso(kw.pop("a")), self.iso(kw.pop("b"))
        kw["key_signals"] = [{"series_id": sid, "start": self.iso(a), "end": self.iso(b), "direction": d}
                             for sid, a, b, d in kw.get("key_signals", [])]
        self.incidents.append(kw)

    # ------------------------------------------------------------ config state
    def spark_config(self, job, t):
        cfg = dict(SPARK_BASE_CONFIG[job])
        for c in sorted(self.changes, key=lambda c: c["_t"]):
            if c["kind"] == "spark_config" and c["target"] == job and c["_t"] <= t:
                for k, (_, new) in c["details"].items():
                    cfg[k] = new
        return cfg

    def dag_paused(self, dag, t):
        state = False
        for c in sorted(self.changes, key=lambda c: c["_t"]):
            if c["target"] == dag and c["_t"] <= t and c["kind"] in ("dag_pause", "dag_unpause"):
                state = c["kind"] == "dag_pause"
        return state


def _ns(pod):
    if pod in POD:
        return {"kafka-ingest": "kafka", "orders-apps": "apps", "flink-clickstream": "flink",
                "spark-orders-stream": "spark", "spark-batch": "spark", "trino-lakehouse": "trino",
                "airflow-main": "airflow"}[POD[pod]["source_id"]]
    return "default"


# ====================================================================== baseline
def traffic(ctx: Ctx, peak_hour=19.0, weekend=0.7, floor=0.45):
    d = 0.5 + 0.5 * np.cos(2 * np.pi * (ctx.hour - peak_hour) / 24)
    f = floor + (1 - floor) * d
    return f * np.where(ctx.dow >= 5, weekend, 1.0)


def business(ctx: Ctx):
    h = ctx.hour
    g = np.exp(-(((h - 20.0 + 12) % 24 - 12) / 3.5) ** 2)
    return 0.05 + g * np.where(ctx.dow >= 5, 0.2, 1.0)


def baseline(ctx: Ctx):
    s, rng = ctx.s, ctx.rng
    f_orders = traffic(ctx)
    f_clicks = traffic(ctx, peak_hour=20, weekend=0.85, floor=0.35)
    g = business(ctx)
    ctx.f_orders, ctx.f_clicks, ctx.g_trino = f_orders, f_clicks, g

    # --- pods: memory + cpu (non-batch); batch pods are filled from spark runs
    for p in PODS:
        name, src, role, cpu_lim, mem_lim, base_mem, jvm, batch = p
        if batch:
            continue
        load = {"broker": f_orders * 0.7 + f_clicks * 0.3, "producer": f_orders, "consumer": f_orders,
                "jobmanager": 0.3 + 0 * f_clicks, "taskmanager": f_clicks, "driver": 0.4 + 0.3 * f_orders,
                "executor": f_orders, "coordinator": g, "worker": g, "scheduler": 0.5 + 0 * g}[role]
        if src == "airflow-main" and role == "worker":
            load = 0.1 + 0 * g
        cpu_scale = {"broker": 0.75, "producer": 0.18, "consumer": 0.42, "jobmanager": 0.12,
                     "taskmanager": 0.95, "driver": 0.2, "executor": 0.85, "coordinator": 0.6,
                     "worker": 2.4 if src == "trino-lakehouse" else 0.1, "scheduler": 0.25}[role]
        cpu = (0.05 * cpu_lim + cpu_scale * load) * (1 + ctx.ar(0.06, 0.8))
        s[f"container_cpu_usage_cores|{name}"] = np.clip(cpu, 0.005, cpu_lim)
        mem = base_mem * (1 + 0.03 * np.sin(2 * np.pi * ctx.m / (1440 * 3.3) + rng.uniform(0, 6))
                          + ctx.ar(0.012, 0.98))
        if src == "trino-lakehouse" and role == "worker":
            mem = mem * (0.9 + 0.2 * g)
        s[f"container_memory_working_set_bytes|{name}"] = np.minimum(mem, mem_lim * 0.97)
        if jvm:
            period = rng.uniform(18, 40)
            saw = (ctx.m % period) / period
            trino = src == "trino-lakehouse"
            heapbase = 0.3 + 0.18 * g if trino else 0.38 + 0.08 * load
            amp = 0.15 if trino else 0.22
            s[f"jvm_heap_used_ratio|{name}"] = np.clip(heapbase + amp * saw + ctx.ar(0.02, 0.5), 0.1, 0.99)
            s[f"jvm_gc_time_ms_per_sec|{name}"] = np.clip(8 + 14 * load + ctx.ar(3, 0.6), 0.5, None)

    # --- kafka
    produce = 1200 * f_orders * (1 + ctx.ar(0.03, 0.85))
    ctx.produce_base = produce
    s["kafka_messages_in_per_sec|clicks"] = 900 * f_clicks * (1 + ctx.ar(0.04, 0.85))
    s["kafka_produce_request_p99_ms|kafka-ingest"] = np.clip(11 + 7 * f_orders + ctx.ar(1.5, 0.7), 4, None)
    s["kafka_failed_produce_requests_per_sec|kafka-ingest"] = rng.poisson(0.01, ctx.N) / 60.0
    s["kafka_under_replicated_partitions|kafka-ingest"] = np.zeros(ctx.N)
    s["kafka_isr_shrinks_per_sec|kafka-ingest"] = rng.poisson(0.002, ctx.N) / 60.0

    # --- spark streaming (reads orders independently)
    s["spark_streaming_scheduling_delay_sec|orders-stream"] = np.clip(0.25 + ctx.ar(0.08, 0.6), 0.02, None)
    s["spark_streaming_batch_duration_sec|orders-stream"] = np.clip(4 + 3 * f_orders + ctx.ar(0.4, 0.7), 1, None)

    # --- flink
    s["flink_records_out_per_sec|clickstream-enrich"] = 880 * f_clicks * (1 + ctx.ar(0.03, 0.8))
    s["flink_backpressure_ratio|clickstream-enrich"] = np.clip(0.04 + 0.04 * f_clicks + ctx.ar(0.015, 0.7), 0, 1)
    s["flink_last_checkpoint_duration_ms|clickstream-enrich"] = np.clip(
        2300 + 900 * f_clicks + ctx.ar(250, 0.6), 800, None)
    s["flink_job_restarts_total|clickstream-enrich"] = np.zeros(ctx.N)
    s["flink_checkpoint_failures_total|clickstream-enrich"] = np.zeros(ctx.N)

    # --- trino
    s["trino_completed_queries_per_min|trino-lakehouse"] = rng.poisson(2 + 28 * g)
    s["trino_queued_queries|trino-lakehouse"] = rng.poisson(0.25 * g)
    s["trino_query_p95_sec|trino-lakehouse"] = np.clip(3.5 + 2.5 * g + ctx.ar(0.6, 0.6), 0.8, None)
    s["trino_failed_queries_per_min|trino-lakehouse"] = rng.poisson(0.04 + 0.25 * g)
    s["trino_resource_group_queued|adhoc"] = rng.poisson(0.12 * g)

    # --- airflow (queued/completions filled from runs)
    s["airflow_scheduler_heartbeat_age_sec|airflow-main"] = np.clip(3 + ctx.ar(0.8, 0.5), 0.5, None)
    s["airflow_queued_tasks|airflow-main"] = np.zeros(ctx.N)
    s["airflow_task_completions_per_min|airflow-main"] = np.zeros(ctx.N)

    # existing restart counts so counters do not all start at 0
    ctx.restart_base = {p[0]: int(rng.integers(0, 4)) if not p[7] else 0 for p in PODS}


# ====================================================================== airflow + spark
def _app_id(ctx):
    ctx._app_counter += 1
    return f"spark-{int(ctx.rng.integers(0x10000000, 0xffffffff)):08x}{ctx._app_counter:04d}"


def simulate_spark(ctx: Ctx, job: str, t: float, attempt: int, dag_run_id: str, ov: dict):
    """Simulate one Spark batch application starting at minute t. Returns app record."""
    rng = ctx.rng
    cfg = ctx.spark_config(job, t)
    day = int(t // 1440)
    if job == "daily-orders":
        base_rows, base_min, base_need = 18e6, 22.0, 2.3
    else:
        base_rows, base_min, base_need = 9e6, 12.0, 1.6
    weekday = 0.85 if (day + ctx.t0.dayofweek) % 7 >= 5 else 1.0
    rows = base_rows * (1 + 0.003 * day) * weekday * rng.normal(1, 0.03) * ov.get("rows_mult", 1.0)
    parts = int(cfg["spark.sql.shuffle.partitions"])
    heap = _parse_mem(cfg["spark.executor.memory"])
    limit = (heap + _parse_mem(cfg["spark.executor.memoryOverhead"])) * GiB
    need = base_need * (rows / base_rows) * (200 / parts) ** 0.6
    dur = base_min * (rows / base_rows) ** 0.95 * rng.normal(1, 0.04)
    skew = ov.get("skew")
    if skew:
        dur *= skew
    oom = need > heap
    if oom:
        mode = "container_oom" if (rows / base_rows > 1.5 or parts < 100) else "heap_oom"
        fail_at = 0.55 * base_min * (rows / base_rows) ** 0.95
        end = t + 0.7 + fail_at
    else:
        mode, end = None, t + 0.7 + dur
    app = {"app_id": _app_id(ctx), "job": job, "attempt": attempt, "dag_run_id": dag_run_id,
           "submitted": ctx.iso(t), "start": ctx.iso(t + 0.7), "end": ctx.iso(end),
           "_t0": t + 0.7, "_t1": end, "duration_s": round((end - t - 0.7) * 60),
           "state": "FAILED" if oom else "SUCCEEDED", "config": cfg,
           "input_rows": int(rows), "input_bytes": int(rows * 410), "output_rows": 0 if oom else int(rows * 0.97),
           "error": None, "_need": need, "_limit": limit, "_mode": mode, "_skew": skew}
    # stages
    n_join = parts
    med = (dur * 60 * 0.45) / (n_join / 6) * 1000  # ms per join task
    stages = [
        {"stage_id": 0, "name": "scan parquet raw.orders", "num_tasks": 96, "status": "COMPLETE"},
        {"stage_id": 1, "name": "Exchange hashpartitioning(customer_id)", "num_tasks": 96, "status": "COMPLETE"},
        {"stage_id": 2, "name": "SortMergeJoin orders x customers", "num_tasks": n_join, "status": "COMPLETE"},
        {"stage_id": 3, "name": "write iceberg", "num_tasks": 50, "status": "COMPLETE"},
    ]
    for st in stages:
        k = st["num_tasks"]
        m_ = med * (0.6 if st["stage_id"] != 2 else 1.0) * (200 / parts if st["stage_id"] == 2 else 1)
        st.update(task_ms_median=int(m_), task_ms_p75=int(m_ * 1.25), task_ms_max=int(m_ * rng.uniform(1.6, 2.4)),
                  shuffle_read_bytes=int(rows * 120 if st["stage_id"] >= 2 else 0),
                  spill_bytes=0, failed_tasks=0)
    if parts < 100:
        stages[2]["spill_bytes"] = int(rows * 95)
    if skew:
        stages[2]["task_ms_max"] = int(stages[2]["task_ms_median"] * rng.uniform(16, 22))
        stages[2]["spill_bytes"] = int(rows * 180)
        stages[2]["skew_note"] = "1 task processed 31% of stage input rows"
    if oom:
        stages[2]["status"] = "FAILED"
        stages[2]["failed_tasks"] = 4
        stages[3]["status"] = "SKIPPED"
        tid = int(rng.integers(600, 999))
        if mode == "container_oom":
            app["error"] = (f"Job aborted due to stage failure: Task 37 in stage 2.0 failed 4 times, most recent "
                            f"failure: Lost task 37.3 in stage 2.0 (TID {tid}) (10.42.1.17 executor 2): "
                            f"ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: "
                            f"The executor with id 2 exited with exit code 137(SIGKILL, possible container OOM).")
        else:
            app["error"] = (f"Job aborted due to stage failure: Task 112 in stage 2.0 failed 4 times, most recent "
                            f"failure: Lost task 112.3 in stage 2.0 (TID {tid}) (10.42.2.9 executor 2): "
                            f"ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: "
                            f"Remote RPC client disassociated. Likely due to containers exceeding thresholds, "
                            f"or network issues. Check driver logs for WARN messages.")
    app["stages"] = stages
    execs = []
    for i in (1, 2, 3):
        pod = f"{job}-exec-{i}"
        peak = min(limit * 0.99 if (oom and i == 2) else limit * 0.97, (need * 0.92 + 0.85) * GiB)
        if oom:
            peak = limit * (0.99 if i == 2 else 0.93)
        execs.append({"executor_id": str(i), "pod": pod, "peak_memory_bytes": int(peak),
                      "gc_time_s": round(dur * 60 * (0.04 if not (oom or skew) else 0.18) * rng.uniform(0.8, 1.2)),
                      "tasks": int((96 * 2 + n_join + 50) / 3), "failed_tasks": 4 if (oom and i == 2) else 0,
                      "shuffle_spill_bytes": stages[2]["spill_bytes"] if (skew and i == 3) else
                      (stages[2]["spill_bytes"] // 3 if parts < 100 else 0),
                      "lost": bool(oom and i == 2),
                      "exit_code": (137 if mode == "container_oom" else 52) if (oom and i == 2) else None})
    app["executors"] = execs
    ctx.spark_apps.append(app)
    return app


def build_runs(ctx: Ctx):
    rng = ctx.rng
    for day in range(ctx.days):
        # ---------------- hourly quality check
        for h in range(24):
            sched = ctx.M(day, h, 5)
            if sched >= ctx.N - 5:
                continue
            delay = ctx.hourly_delay[int(sched)]
            q = sched + rng.uniform(2, 12) / 60 + delay
            st = q + rng.uniform(3, 15) / 60 + (delay * 0.1)
            tries = 2 if rng.random() < 0.012 else 1
            tasks, t = [], st
            for tr in range(1, tries + 1):
                d = rng.normal(95, 15) / 60
                tasks.append({"task_id": "run_checks", "try_number": tr, "state": "failed" if tr < tries else "success",
                              "queued_at": ctx.iso(q if tr == 1 else t), "start": ctx.iso(t), "end": ctx.iso(t + d),
                              "_q": q if tr == 1 else t, "_s": t, "_e": t + d,
                              "duration_s": round(d * 60), "operator": "PythonOperator"})
                t = t + d + (2 if tr < tries else 0)
            _add_dag_run(ctx, "hourly_quality_check", sched, q, st, t, "success", tasks, "scheduled")
        # ---------------- daily dags
        for dag, (hh, mm) in (("daily_orders", (2, 0)), ("customer_agg", (3, 30))):
            sched = ctx.M(day, hh, mm)
            ov = ctx.overrides.get((dag, day), {})
            if ctx.dag_paused(dag, sched):
                unpause = next(c["_t"] for c in sorted(ctx.changes, key=lambda c: c["_t"])
                               if c["target"] == dag and c["kind"] == "dag_unpause" and c["_t"] > sched)
                _daily_run(ctx, dag, day, sched, unpause + 0.4, ov, "scheduled")
                continue
            _daily_run(ctx, dag, day, sched, sched + rng.uniform(5, 20) / 60, ov, "scheduled")
            if "manual_rerun_at" in ov:
                _daily_run(ctx, dag, day, ov["manual_rerun_at"], ov["manual_rerun_at"] + 0.2,
                           {k: v for k, v in ov.items() if k not in ("rows_mult", "skew", "sensor_min")}, "manual")


def _daily_run(ctx, dag, day, sched, q, ov, run_type):
    rng = ctx.rng
    st = q + rng.uniform(3, 10) / 60
    run_id = f"{run_type}__{ctx.iso(sched)}"
    tasks, t = [], st
    if dag == "daily_orders":
        d = ov.get("sensor_min", rng.uniform(1, 8)) if run_type == "scheduled" else 0.3
        tasks.append(dict(task_id="wait_for_upstream", try_number=1, state="success", _q=q, _s=t, _e=t + d,
                          operator="S3KeySensor"))
        t += d + rng.uniform(2, 6) / 60
    job = DAGS[dag]["spark_job"]
    final = "success"
    for tr in (1, 2):
        app = simulate_spark(ctx, job, t + 0.1, tr, run_id, ov)
        e = app["_t1"] + 0.5
        ok = app["state"] == "SUCCEEDED"
        tasks.append(dict(task_id="spark_submit", try_number=tr, state="success" if ok else ("up_for_retry" if tr == 1 else "failed"),
                          _q=t if tr == 1 else t, _s=t, _e=e, operator="SparkKubernetesOperator", spark_app_id=app["app_id"]))
        t = e
        if ok:
            break
        if tr == 1:
            t += 5
        else:
            final = "failed"
    nxt = ["validate"] if dag == "daily_orders" else ["publish"]
    for tid in nxt:
        if final == "failed":
            tasks.append(dict(task_id=tid, try_number=0, state="upstream_failed", _q=t, _s=t, _e=t, operator="PythonOperator"))
        else:
            d = rng.uniform(1, 1.6)
            tasks.append(dict(task_id=tid, try_number=1, state="success", _q=t, _s=t + 0.05, _e=t + 0.05 + d,
                              operator="PythonOperator"))
            t += 0.05 + d
    for tk in tasks:
        tk.update(queued_at=ctx.iso(tk["_q"]), start=ctx.iso(tk["_s"]), end=ctx.iso(tk["_e"]),
                  duration_s=round((tk["_e"] - tk["_s"]) * 60))
    _add_dag_run(ctx, dag, sched, q, st, t, final, tasks, run_type)


def _add_dag_run(ctx, dag, sched, q, st, end, state, tasks, run_type):
    sla = DAGS[dag]["sla_minutes"]
    ctx.dag_runs.append({"dag_id": dag, "run_id": f"{run_type}__{ctx.iso(sched)}", "run_type": run_type,
                         "scheduled_for": ctx.iso(sched), "queued_at": ctx.iso(q), "start": ctx.iso(st),
                         "end": ctx.iso(end), "state": state, "sla_minutes": sla,
                         "sla_missed": bool(run_type == "scheduled" and (end - sched) > sla),
                         "_sched": sched, "_s": st, "_e": end, "tasks": tasks})


def runs_to_series(ctx: Ctx):
    """Fill batch pod series, Airflow queue/completion series, and spark logs/events from runs."""
    s, rng = ctx.s, ctx.rng
    q = np.zeros(ctx.N)
    comp = np.zeros(ctx.N)
    wcpu = s["container_cpu_usage_cores|airflow-worker-0"]
    for r in ctx.dag_runs:
        for tk in r["tasks"]:
            if tk["state"] == "upstream_failed":
                continue
            q[ctx.sl(tk["_q"], max(tk["_s"], tk["_q"] + 0.01))] += 1
            if tk["_e"] < ctx.N:
                comp[int(tk["_e"])] += 1
            wcpu[ctx.sl(tk["_s"], tk["_e"])] += 0.35 if tk["operator"] == "PythonOperator" else 0.04
            _airflow_task_logs(ctx, r, tk)
    s["airflow_queued_tasks|airflow-main"] = q
    s["airflow_task_completions_per_min|airflow-main"] = comp

    for app in ctx.spark_apps:
        _spark_app_series(ctx, app)


def _airflow_task_logs(ctx, r, tk):
    key = (f"TaskInstanceKey(dag_id='{r['dag_id']}', task_id='{tk['task_id']}', run_id='{r['run_id']}', "
           f"try_number={tk['try_number']}, map_index=-1)")
    ctx.log(tk["_q"], "airflow-scheduler", "INFO", f"Sending {key} to LocalExecutor with priority 1 and queue default",
            "airflow.jobs.scheduler_job_runner")
    w = "airflow-worker-0"
    if tk["operator"] == "S3KeySensor":
        ds = r["scheduled_for"][:10]
        n = max(1, int((tk["_e"] - tk["_s"]) / 5))
        for i in range(n):
            ctx.log(tk["_s"] + i * 5, w, "INFO", f"Poking for key : s3://lake/raw/orders/dt={ds}/_SUCCESS",
                    "airflow.task.S3KeySensor")
        ctx.log(tk["_e"], w, "INFO", "Success criteria met. Exiting.", "airflow.task.S3KeySensor")
    if tk["operator"] == "SparkKubernetesOperator":
        ctx.log(tk["_s"] + 0.05, w, "INFO", f"Creating sparkApplication {tk.get('spark_app_id')}",
                "airflow.task.SparkKubernetesOperator")
    lvl = "ERROR" if tk["state"] in ("failed", "up_for_retry") else "INFO"
    msg = (f"Marking task as {tk['state'].upper()}. dag_id={r['dag_id']}, task_id={tk['task_id']}, "
           f"run_id={r['run_id']}")
    if tk["state"] in ("failed", "up_for_retry") and tk["operator"] == "SparkKubernetesOperator":
        ctx.log(tk["_e"] - 0.02, w, "ERROR", f"SparkApplication {tk.get('spark_app_id')} failed: state FAILED",
                "airflow.task.SparkKubernetesOperator")
    if tk["state"] in ("failed",) and tk["operator"] == "PythonOperator":
        ctx.log(tk["_e"] - 0.01, w, "ERROR", "Data quality check failed: orders_freshness_minutes=74 > 60 (retrying)",
                "airflow.task.run_checks")
    ctx.log(tk["_e"], w, lvl, msg, "airflow.task")


def _spark_app_series(ctx: Ctx, app):
    s, rng = ctx.s, ctx.rng
    job, a, b = app["job"], app["_t0"], app["_t1"]
    sl = ctx.sl(a, b)
    n = sl.stop - sl.start
    if n <= 0:
        return
    x = np.linspace(0, 1, n)
    drv = f"{job}-driver"
    s[f"container_cpu_usage_cores|{drv}"][sl] = 0.3 + rng.normal(0, 0.03, n)
    s[f"container_memory_working_set_bytes|{drv}"][sl] = 1.2 * GiB * (1 + rng.normal(0, 0.01, n))
    s[f"jvm_heap_used_ratio|{drv}"][sl] = 0.45 + rng.normal(0, 0.03, n)
    s[f"jvm_gc_time_ms_per_sec|{drv}"][sl] = np.abs(10 + rng.normal(0, 3, n))
    for ex in app["executors"]:
        pod = ex["pod"]
        lim = POD[pod]["mem_limit"]
        peak = ex["peak_memory_bytes"]
        prof = np.minimum(1, x / 0.6)
        mem = 0.6 * GiB + (peak - 0.6 * GiB) * prof + rng.normal(0, 0.01 * GiB, n)
        cpu = 1.75 + rng.normal(0, 0.12, n)
        heap = 0.35 + 0.45 * prof + rng.normal(0, 0.03, n)
        gc = 25 + 40 * prof + rng.normal(0, 8, n)
        if app["_skew"]:
            normal_frac = 1 / app["_skew"]
            idle = x > normal_frac
            if ex["executor_id"] != "3":
                cpu = np.where(idle, 0.15 + rng.normal(0, 0.03, n), cpu)
                mem = np.where(idle, mem * 0.7, mem)
            else:
                heap = np.where(idle, 0.9 + rng.normal(0, 0.02, n), heap)
                gc = np.where(idle, 260 + rng.normal(0, 40, n), gc)
                mem = np.where(idle, lim * 0.9 + rng.normal(0, 0.01 * GiB, n), mem)
        if app["_mode"]:
            gc = gc + 120 * x ** 2
            heap = np.minimum(0.99, heap + 0.2 * x ** 3)
            if ex["executor_id"] == "2":
                mem = 0.6 * GiB + (peak - 0.6 * GiB) * x ** 1.3
                ctx.restart(pod, b - 0.3)
        s[f"container_cpu_usage_cores|{pod}"][sl] = np.clip(cpu, 0.02, 2)
        s[f"container_memory_working_set_bytes|{pod}"][sl] = np.clip(mem, 0.2 * GiB, lim)
        s[f"jvm_heap_used_ratio|{pod}"][sl] = np.clip(heap, 0.05, 0.995)
        s[f"jvm_gc_time_ms_per_sec|{pod}"][sl] = np.clip(gc, 1, None)
    _spark_logs_events(ctx, app)


def _spark_logs_events(ctx, app):
    rng = ctx.rng
    job, a, b = app["job"], app["_t0"], app["_t1"]
    drv = f"{job}-driver"
    pods = [drv] + [e["pod"] for e in app["executors"]]
    for i, p in enumerate(pods):
        t = a - 0.6 + i * 0.05
        ctx.event(t, p, "Scheduled", f"Successfully assigned spark/{p} to ip-10-0-{2 + i % 3}-{17 + i}.ec2.internal")
        ctx.event(t + 0.1, p, "Pulled", "Container image \"registry.local/spark-jobs:3.5.4-r12\" already present on machine")
        ctx.event(t + 0.12, p, "Started", f"Started container {'spark-kubernetes-driver' if p == drv else 'spark-kubernetes-executor'}")
    ctx.log(a, drv, "INFO", f"Submitted application {app['app_id']} ({job}) attempt {app['attempt']}",
            "org.apache.spark.SparkContext")
    ctx.log(a + 0.2, drv, "INFO", f"Reading {app['input_rows']:,} rows from raw.orders", "org.apache.spark.sql.execution")
    dur = b - a
    for st in app["stages"][:-1] if app["state"] == "FAILED" else app["stages"]:
        if st["status"] == "SKIPPED":
            continue
        frac = {0: 0.15, 1: 0.3, 2: 0.85, 3: 0.98}[st["stage_id"]]
        tt = a + dur * frac
        if st["status"] == "COMPLETE":
            ctx.log(tt, drv, "INFO", f"ResultStage {st['stage_id']} ({st['name']}) finished in "
                    f"{dur * 60 * 0.2:.1f} s", "org.apache.spark.scheduler.DAGScheduler")
    for ex in app["executors"]:
        for k in range(int(dur)):
            if rng.random() < 0.6:
                tid = int(rng.integers(1, 900))
                ctx.log(a + k + rng.random(), ex["pod"], "INFO",
                        f"Finished task {tid % 200}.0 in stage {min(3, int(4 * k / max(dur, 1)))}.0 (TID {tid}). "
                        f"{int(rng.integers(2000, 6000))} bytes result sent to driver",
                        "org.apache.spark.executor.Executor")
    # skew
    if app["_skew"]:
        ex3 = f"{job}-exec-3"
        for k in range(int(dur / app["_skew"]), int(dur), 3):
            ctx.log(a + k, ex3, "INFO", f"Thread 77 spilling sort data of {rng.uniform(1.2, 2.1):.1f} GiB to disk "
                    f"({k // 3 + 1} times so far)", "org.apache.spark.util.collection.ExternalSorter")
        ctx.log(a + dur * 0.5, drv, "WARN", "Stage 2 has 1 running task(s) for > 10x median task duration "
                "(task 143.0, 2.4 GiB shuffle read vs median 64 MiB)", "org.apache.spark.scheduler.TaskSetManager")
    # failures
    if app["_mode"]:
        ex2 = f"{job}-exec-2"
        if app["_mode"] == "heap_oom":
            ctx.log(b - 0.8, ex2, "WARN", "GC overhead: 41% of time spent in GC over last 60 s",
                    "org.apache.spark.executor.Executor")
            ctx.log(b - 0.4, ex2, "ERROR", "Exception in task 112.0 in stage 2.0 (TID 671)\njava.lang.OutOfMemoryError: "
                    "Java heap space\n\tat org.apache.spark.sql.catalyst.expressions.UnsafeRow.copy(UnsafeRow.java:479)\n"
                    "\tat org.apache.spark.sql.execution.joins.SortMergeJoinScanner.bufferMatchingRows",
                    "org.apache.spark.executor.Executor")
            ctx.log(b - 0.38, ex2, "ERROR", "Uncaught exception in thread Executor task launch worker for task 112.0 "
                    "in stage 2.0 (TID 671): java.lang.OutOfMemoryError: Java heap space",
                    "org.apache.spark.util.SparkUncaughtExceptionHandler")
            ctx.event(b - 0.35, ex2, "BackOff", "Container spark-kubernetes-executor exited with code 52", "Warning")
            ctx.log(b - 0.3, f"{job}-driver", "ERROR", "Lost executor 2 on 10.42.2.9: The executor with id 2 exited "
                    "with exit code 52(JVM OOM).", "org.apache.spark.scheduler.cluster.k8s.KubernetesClusterSchedulerBackend")
        else:
            ctx.log(b - 0.9, ex2, "INFO", f"Thread 81 spilling sort data of 1.9 GiB to disk (1 time so far)",
                    "org.apache.spark.util.collection.ExternalSorter")
            ctx.event(b - 0.35, ex2, "OOMKilled", f"Container spark-kubernetes-executor in pod {ex2} was OOMKilled "
                      f"(exit code 137, memory limit {app['_limit'] / GiB:.0f}Gi)", "Warning")
            ctx.log(b - 0.3, f"{job}-driver", "ERROR", "Lost executor 2 on 10.42.1.17: The executor with id 2 exited "
                    "with exit code 137(SIGKILL, possible container OOM).",
                    "org.apache.spark.scheduler.cluster.k8s.KubernetesClusterSchedulerBackend")
        ctx.log(b - 0.05, f"{job}-driver", "ERROR", app["error"], "org.apache.spark.scheduler.DAGScheduler")
    else:
        ctx.log(b - 0.05, drv, "INFO", f"Job 0 finished: wrote {app['output_rows']:,} rows to lake.orders_enriched",
                "org.apache.spark.scheduler.DAGScheduler")
    for p in pods:
        ctx.event(b + 0.1, p, "Killing", f"Stopping container {'spark-kubernetes-driver' if p == drv else 'spark-kubernetes-executor'}")


# ====================================================================== kafka lag physics
def kafka_physics(ctx: Ctx):
    s = ctx.s
    produce = ctx.produce_base * ctx.produce_mult
    s["kafka_messages_in_per_sec|orders"] = produce
    cap = ctx.consumer_replicas * ctx.per_pod_capacity * ctx.cap_mult
    lag = 0.0
    lag_arr = np.zeros(ctx.N)
    consumed = np.zeros(ctx.N)
    for i in range(ctx.N):
        c = min(cap[i], produce[i] + lag / 60.0)
        lag = max(0.0, lag + (produce[i] - c) * 60.0)
        lag_arr[i] = lag
        consumed[i] = c
    inflight = produce * 0.22 * (1 + ctx.ar(0.15, 0.5))
    s["kafka_consumergroup_lag|orders-etl"] = np.maximum(0, lag_arr + inflight)
    s["kafka_consumer_records_consumed_per_sec|orders-etl"] = consumed * (1 + ctx.ar(0.01, 0.5))
    # spark streaming reads orders too (own group, plenty of capacity)
    s["spark_streaming_input_rows_per_sec|orders-stream"] = produce * 0.995 * (1 + ctx.ar(0.02, 0.6))
    # consumer pods: cpu follows consumed share, saturating near the per-pod capacity
    for i, pod in enumerate(("orders-etl-consumer-0", "orders-etl-consumer-1")):
        sid = f"container_cpu_usage_cores|{pod}"
        alive = ~np.isnan(s[sid])
        share = np.where(ctx.consumer_replicas == 2, 0.5, np.where(i == 0, 1.0, 0.0))
        util = consumed * share / np.maximum(1, ctx.per_pod_capacity)
        cpu = (0.05 + 0.9 * util) * (1 + ctx.ar(0.04, 0.7))
        s[sid] = np.where(alive, np.clip(cpu, 0.01, 1.0), np.nan)


def finalize(ctx: Ctx):
    s = ctx.s
    for p in PODS:
        name, cpu_lim = p[0], p[3]
        cpu = s[f"container_cpu_usage_cores|{name}"]
        util = cpu / cpu_lim
        thr = np.clip((util - 0.72) / 0.28, 0, 1) * 0.55 + np.abs(ctx.rng.normal(0, 0.004, ctx.N))
        s[f"container_cpu_throttled_ratio|{name}"] = np.where(np.isnan(cpu), np.nan, thr)
        cnt = ctx.restart_base[name] + np.cumsum(ctx.restart_inc[name])
        s[f"kube_pod_container_status_restarts_total|{name}"] = cnt if not p[7] else np.where(np.isnan(cpu), np.nan, cnt)
    # counters stay monotone
    for sid in ("flink_job_restarts_total|clickstream-enrich", "flink_checkpoint_failures_total|clickstream-enrich"):
        s[sid] = np.maximum.accumulate(s[sid])
