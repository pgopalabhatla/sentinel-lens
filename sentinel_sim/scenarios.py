"""Failure scenarios with ground truth.

Each scenario function runs in two phases:
  * register (the function body): record causes - changes, run overrides, capacity
    changes - before the generic physics runs.
  * post (the returned closure): add symptoms the physics does not model generically
    (logs, events, metric effects) and write the ground-truth incident label.

Split: days 0-6 warm-up (no incidents), days 7-20 train, days 21-27 test.
Test contains new variants of train scenarios plus two novel types.
"""
from __future__ import annotations

import numpy as np

from .topology import GiB, MiB

TRAIN, TEST = "train", "test"


def _runs(ctx, dag, day, run_type=None):
    lo, hi = ctx.M(day), ctx.M(day + 1)
    return [r for r in ctx.dag_runs if r["dag_id"] == dag and lo <= r["_sched"] < hi
            and (run_type is None or r["run_type"] == run_type)]


def _apps(ctx, run):
    ids = {t.get("spark_app_id") for t in run["tasks"]}
    return [a for a in ctx.spark_apps if a["app_id"] in ids]


def _bump(ctx, sid, a, b, add=None, mult=None, shape="flat", rise=0.15, jitter=0.07):
    """Apply an additive/multiplicative effect over [a,b) with soft ramp in/out."""
    sl = ctx.sl(a, b)
    n = sl.stop - sl.start
    if n <= 0:
        return
    x = np.linspace(0, 1, n)
    if shape == "flat":
        w = np.clip(np.minimum(x / rise, (1 - x) / rise), 0, 1)
    elif shape == "ramp":          # grows until the end
        w = x
    elif shape == "decay":         # jumps then decays
        w = np.exp(-4 * x)
    else:
        w = np.ones(n)
    w = w * (1 + ctx.rng.normal(0, jitter, n))
    arr = ctx.s[sid]
    if add is not None:
        arr[sl] = arr[sl] + add * w
    if mult is not None:
        arr[sl] = arr[sl] * (1 + (mult - 1) * w)


def _cap(ctx, sid, cap):
    """Clip at a ceiling with a little jitter, so saturated series are not perfectly flat."""
    arr = ctx.s[sid]
    ctx.s[sid] = np.minimum(arr, cap * (1 - np.abs(ctx.rng.normal(0, 0.008, arr.size))))


def _logs_every(ctx, a, b, every, pod, level, fn, logger=""):
    t = a
    k = 0
    while t < b:
        ctx.log(t + ctx.rng.uniform(0, every * 0.5), pod, level, fn(k), logger)
        t += every
        k += 1


# =============================================================== S1 spark OOM
def spark_oom(ctx, day, split, cause, change_t=None, revert_t=None, rerun_t=None):
    job, dag = "daily-orders", "daily_orders"
    cid = None
    if cause == "executor_memory_reduced":
        cid = ctx.change(change_t, "spark_config", job, {"spark.executor.memory": ["3g", "2g"]}, "j.chen",
                         "Reduce daily-orders executor memory to cut cluster cost (PR #412)")
    elif cause == "shuffle_partitions_reduced":
        cid = ctx.change(change_t, "spark_config", job, {"spark.sql.shuffle.partitions": [200, 50]}, "a.patel",
                         "Fewer shuffle partitions to reduce small files in lake.orders_enriched (PR #455)")
    else:
        ctx.overrides[(dag, day)] = {"rows_mult": 2.4}
    if revert_t:
        key = list(ctx.changes[-1]["details"].items())[0]
        ctx.change(revert_t, "spark_config", job, {key[0]: [key[1][1], key[1][0]]}, "oncall-sre",
                   f"Revert {cid}: daily-orders failed overnight")
    if rerun_t:
        ctx.overrides.setdefault((dag, day), {})["manual_rerun_at"] = rerun_t

    def post():
        run = _runs(ctx, dag, day, "scheduled")[0]
        apps = _apps(ctx, run)
        a, b = apps[0]["_t0"], run["_e"]
        sig = []
        for app in apps:
            first = ((f"container_memory_working_set_bytes|{job}-exec-2", app["_t0"], app["_t1"], "up")
                     if app["_mode"] == "container_oom" else
                     (f"jvm_heap_used_ratio|{job}-exec-2", app["_t1"] - 6, app["_t1"], "up"))
            sig += [first, (f"jvm_gc_time_ms_per_sec|{job}-exec-2", app["_t0"], app["_t1"], "up"),
                    (f"kube_pod_container_status_restarts_total|{job}-exec-2", app["_t1"] - 1, app["_t1"] + 1, "up")]
        mode = apps[0]["_mode"]
        rc = {"executor_memory_reduced": ("resource_config",
              "spark.executor.memory was lowered from 3g to 2g; the SortMergeJoin stage needs ~2.4 GiB of heap per "
              "executor, so executor 2 hit java.lang.OutOfMemoryError (exit 52) and all 4 task attempts failed."),
              "shuffle_partitions_reduced": ("resource_config",
              "spark.sql.shuffle.partitions was lowered from 200 to 50, making each join partition ~4x larger; "
              "executor memory demand exceeded the 4Gi container limit and executor 2 was OOMKilled (exit 137)."),
              "input_spike": ("input_data_volume",
              "Input rows were ~2.4x normal (upstream reprocessed 3 days into one partition); join partitions "
              "outgrew executor memory and executor 2 was OOMKilled (exit 137) on both attempts.")}[cause]
        fix = {"executor_memory_reduced": ["Revert spark.executor.memory to 3g", "Rerun the failed DAG run"],
               "shuffle_partitions_reduced": ["Revert spark.sql.shuffle.partitions to 200 (or enable AQE coalescing "
                                              "instead to reduce small files)", "Rerun the failed DAG run"],
               "input_spike": ["Rerun with more executors or spark.executor.memory 6g for this partition",
                               "Enable AQE (spark.sql.adaptive.enabled=true) so partition sizes adapt to input",
                               "Ask upstream to backfill into the correct dt partitions"]}[cause]
        ctx.incident(incident_id=None, scenario="spark_executor_oom", variant=cause, split=split,
                     title="daily_orders failed: Spark executor out of memory", a=a, b=b, actionable=True,
                     severity="high", root_cause_category=rc[0], root_cause=rc[1], trigger_change_id=cid,
                     affected_entities=["dag:daily_orders", f"spark_job:{job}", f"pod:{job}-exec-2"],
                     key_signals=sig, dag_run_ids=[run["run_id"]], spark_app_ids=[x["app_id"] for x in apps],
                     expected_evidence=[f"spark app error ExecutorLostFailure exit code {137 if mode == 'container_oom' else 52}",
                                        "executor 2 memory at limit before failure",
                                        "change " + cid if cid else "input_rows ~2.4x the job history median"],
                     suggested_fix=fix)
    return post


# =============================================================== S2 data skew
def spark_skew(ctx, day, split, dag, factor):
    job = "customer-agg" if dag == "customer_agg" else "daily-orders"
    ctx.overrides[(dag, day)] = {"skew": factor}

    def post():
        run = _runs(ctx, dag, day, "scheduled")[0]
        app = _apps(ctx, run)[0]
        a, b = app["_t0"], app["_t1"]
        mid = a + (b - a) / factor
        ctx.incident(incident_id=None, scenario="spark_data_skew", variant=dag, split=split,
                     title=f"{dag} ran {factor:.1f}x longer than usual", a=a, b=run["_e"], actionable=True,
                     severity="medium", root_cause_category="data_skew",
                     root_cause=("Join key customer_id is skewed (one value - guest checkouts - holds ~31% of rows). "
                                 "One task in stage 2 ran ~18x the median and spilled heavily on executor 3 while "
                                 "executors 1 and 2 sat idle."),
                     trigger_change_id=None, affected_entities=[f"dag:{dag}", f"spark_job:{job}", f"pod:{job}-exec-3"],
                     key_signals=[(f"jvm_gc_time_ms_per_sec|{job}-exec-3", mid, b, "up"),
                                  (f"jvm_heap_used_ratio|{job}-exec-3", mid, b, "up"),
                                  (f"container_cpu_usage_cores|{job}-exec-1", mid, b, "down"),
                                  (f"container_cpu_usage_cores|{job}-exec-2", mid, b, "down")],
                     dag_run_ids=[run["run_id"]], spark_app_ids=[app["app_id"]],
                     expected_evidence=["run duration far above job history p90",
                                        "stage 2 task_ms_max ~18x task_ms_median", "spill logs on exec-3 only"],
                     suggested_fix=["Enable AQE skew join (spark.sql.adaptive.enabled and "
                                    "spark.sql.adaptive.skewJoin.enabled = true)",
                                    "Or salt the hot customer_id key / handle guest rows separately"])
    return post


# =============================================================== S3 consumer lag
def consumer_slow_deploy(ctx, day, split):
    a, b = ctx.M(day, 14, 0), ctx.M(day, 17, 30)
    cid = ctx.change(a, "deploy", "orders-etl-consumer", {"image": ["orders-etl-consumer:1.7.3", "orders-etl-consumer:1.8.0"]},
                     "r.gomez", "Add per-order fraud score enrichment (calls fraud-api synchronously)")
    ctx.change(b, "deploy", "orders-etl-consumer", {"image": ["orders-etl-consumer:1.8.0", "orders-etl-consumer:1.7.3"]},
               "oncall-sre", "Rollback orders-etl-consumer to 1.7.3 (lag)")
    ctx.per_pod_capacity[ctx.sl(a, b)] = 470.0
    for p in ("orders-etl-consumer-0", "orders-etl-consumer-1"):
        ctx.event(a + 0.2, p, "Killing", "Stopping container consumer")
        ctx.event(a + 0.6, p, "Started", "Started container consumer")
        ctx.restart(p, a + 0.5)

    def post():
        lag = ctx.s["kafka_consumergroup_lag|orders-etl"]
        end = b + np.argmax(lag[int(b):int(b) + 600] < 2000)
        for p in ("orders-etl-consumer-0", "orders-etl-consumer-1"):
            _logs_every(ctx, a + 1, b, 1.5, p, "WARN",
                        lambda k: f"fraud-api call slow: {int(ctx.rng.uniform(380, 900))} ms (order batch of 50)",
                        "orders_etl.enrich")
        ctx.incident(incident_id=None, scenario="kafka_consumer_lag", variant="slow_deploy", split=split,
                     title="orders-etl consumer lag growing", a=a, b=end, actionable=True, severity="high",
                     root_cause_category="bad_deploy_code",
                     root_cause=("orders-etl-consumer 1.8.0 added a synchronous fraud-api call per order; per-replica "
                                 "throughput fell from ~1100 to ~470 msg/s, below the produce rate, so lag grew "
                                 "until rollback."),
                     trigger_change_id=cid, affected_entities=["consumer_group:orders-etl", "topic:orders"],
                     key_signals=[("kafka_consumergroup_lag|orders-etl", a + 5, end, "up"),
                                  ("kafka_consumer_records_consumed_per_sec|orders-etl", a, b, "down"),
                                  ("container_cpu_usage_cores|orders-etl-consumer-0", a, b, "up"),
                                  ("container_cpu_usage_cores|orders-etl-consumer-1", a, b, "up")],
                     expected_evidence=["lag grows linearly while produce rate is normal", "deploy " + cid + " at onset",
                                        "fraud-api slow WARN logs"],
                     suggested_fix=["Roll back to 1.7.3", "Make the enrichment async/batched or scale replicas "
                                    "(orders has 6 partitions, so up to 6 replicas)"])
    return post


def consumer_scaled_down(ctx, day, split):
    a, b = ctx.M(day, 15, 0), ctx.M(day + 1, 0, 30)
    cid = ctx.change(a, "scale", "orders-etl-consumer", {"replicas": [2, 1]}, "k.lee",
                     "Scale down orders-etl-consumer to save cost (traffic looked low this morning)")
    ctx.change(b, "scale", "orders-etl-consumer", {"replicas": [1, 2]}, "oncall-sre", "Restore 2 replicas")
    ctx.consumer_replicas[ctx.sl(a, b)] = 1
    ctx.down("orders-etl-consumer-1", a, b)
    ctx.event(a + 0.1, "orders-etl-consumer-1", "Killing", "Stopping container consumer")
    ctx.log(a + 0.3, "orders-etl-consumer-0", "INFO", "Revoked partitions: [orders-3, orders-4, orders-5]; "
            "assigned partitions: [orders-0, orders-1, orders-2, orders-3, orders-4, orders-5]", "kafka.consumer.coordinator")

    def post():
        lag = ctx.s["kafka_consumergroup_lag|orders-etl"]
        onset = a + np.argmax(lag[int(a):int(b)] > 20000)
        end = b + np.argmax(lag[int(b):int(b) + 600] < 2000)
        ctx.incident(incident_id=None, scenario="kafka_consumer_lag", variant="replicas_reduced", split=split,
                     title="orders-etl consumer lag growing", a=onset, b=end, actionable=True, severity="high",
                     root_cause_category="capacity_reduction",
                     root_cause=("orders-etl-consumer was scaled from 2 to 1 replica; one replica handles ~1100 msg/s, "
                                 "so lag grew once evening traffic exceeded that."),
                     trigger_change_id=cid, affected_entities=["consumer_group:orders-etl", "topic:orders"],
                     key_signals=[("kafka_consumergroup_lag|orders-etl", onset, end, "up"),
                                  ("container_cpu_usage_cores|orders-etl-consumer-0", onset, b, "up"),
                                  ("container_cpu_throttled_ratio|orders-etl-consumer-0", onset, b, "up")],
                     expected_evidence=["scale change " + cid, "only one consumer pod alive, at CPU limit"],
                     suggested_fix=["Restore 2 replicas", "Add an HPA on consumer lag with minReplicas=2"])
    return post


# =============================================================== S4 crashloop
def producer_crashloop(ctx, day, split):
    a, b = ctx.M(day, 16, 20), ctx.M(day, 16, 55)
    cid = ctx.change(a, "deploy", "orders-producer", {"image": ["orders-producer:2.2.4", "orders-producer:2.3.0"],
                     "env.KAFKA_BOOTSTRAP": ["kafka-ingest-kafka-bootstrap.kafka.svc:9092",
                                             "kafka-ingest-kafka-bootstrp.kafka.svc:9092"]},
                     "d.wu", "Move producer config to env vars")
    ctx.change(b, "deploy", "orders-producer", {"image": ["orders-producer:2.3.0", "orders-producer:2.2.4"]},
               "oncall-sre", "Rollback orders-producer")
    ctx.produce_mult[ctx.sl(a + 0.5, b)] = 0.0
    ctx.produce_mult[ctx.sl(b, b + 20)] = 1.6   # upstream queue drains
    pod = "orders-producer"
    t, back = a + 0.4, 10 / 60
    while t < b:
        ctx.log(t, pod, "WARN", "Couldn't resolve server kafka-ingest-kafka-bootstrp.kafka.svc:9092 from "
                "bootstrap.servers as DNS resolution failed for kafka-ingest-kafka-bootstrp.kafka.svc",
                "org.apache.kafka.clients.ClientUtils")
        ctx.log(t + 0.01, pod, "ERROR", "Exception in thread \"main\" org.apache.kafka.common.KafkaException: "
                "Failed to construct kafka producer\nCaused by: org.apache.kafka.common.config.ConfigException: "
                "No resolvable bootstrap urls given in bootstrap.servers", "orders_producer.main")
        ctx.restart(pod, t + 0.05)
        ctx.event(t + 0.06, pod, "BackOff", f"Back-off restarting failed container producer in pod {pod}", "Warning")
        t += back + 0.1
        back = min(back * 2, 5)

    def post():
        sl = ctx.sl(a + 0.5, b)
        ctx.s[f"container_cpu_usage_cores|{pod}"][sl] = np.where(ctx.rng.random(sl.stop - sl.start) < 0.3, 0.01, np.nan)
        ctx.s[f"container_memory_working_set_bytes|{pod}"][sl] = np.where(
            np.isnan(ctx.s[f"container_cpu_usage_cores|{pod}"][sl]), np.nan, 40 * MiB)
        ctx.incident(incident_id=None, scenario="crashloop_bad_config", variant="producer", split=split,
                     title="orders-producer CrashLoopBackOff, orders ingestion stopped", a=a, b=b + 20,
                     actionable=True, severity="high", root_cause_category="bad_deploy_config",
                     root_cause=("orders-producer 2.3.0 moved config to env vars and KAFKA_BOOTSTRAP has a typo "
                                 "('bootstrp'); DNS lookup fails, the producer exits on startup and crash-loops. "
                                 "orders messages_in dropped to zero until rollback."),
                     trigger_change_id=cid, affected_entities=["pod:orders-producer", "topic:orders"],
                     key_signals=[("kafka_messages_in_per_sec|orders", a, b, "down"),
                                  (f"kube_pod_container_status_restarts_total|{pod}", a, b, "up"),
                                  ("spark_streaming_input_rows_per_sec|orders-stream", a, b, "down"),
                                  ("kafka_consumer_records_consumed_per_sec|orders-etl", a, b, "down")],
                     expected_evidence=["BackOff events", "ConfigException: No resolvable bootstrap urls",
                                        "env var diff in " + cid],
                     suggested_fix=["Roll back, fix KAFKA_BOOTSTRAP to kafka-ingest-kafka-bootstrap.kafka.svc:9092",
                                    "Add a startup config check in CI"])
    return post


def trino_worker_crashloop(ctx, day, split):
    a, b = ctx.M(day, 17, 30), ctx.M(day, 18, 40)
    cid = ctx.change(a, "config", "trino-worker", {"config.properties query.max-memory-per-node": ["4GB", "9GB"]},
                     "s.nair", "Raise per-node query memory for large finance joins (rolling restart)")
    ctx.change(b, "config", "trino-worker", {"config.properties query.max-memory-per-node": ["9GB", "4GB"]},
               "oncall-sre", "Revert query.max-memory-per-node")
    pod = "trino-worker-1"
    ctx.down(pod, a + 0.5, b + 1)
    t, back = a + 0.5, 10 / 60
    while t < b:
        ctx.log(t, pod, "ERROR", "Invalid memory configuration. The sum of max query memory per node (9GB) and "
                "heap headroom (2.4GB) cannot be larger than the available heap memory (6GB)",
                "io.trino.server.Server")
        ctx.log(t + 0.01, pod, "ERROR", "Guice creation errors: 1) Error in custom provider, "
                "java.lang.IllegalArgumentException: Invalid memory configuration", "io.trino.server.Server")
        ctx.restart(pod, t + 0.02)
        ctx.event(t + 0.03, pod, "BackOff", f"Back-off restarting failed container trino in pod {pod}", "Warning")
        t += back + 0.3
        back = min(back * 2, 5)

    def post():
        s = ctx.s
        _bump(ctx, "trino_completed_queries_per_min|trino-lakehouse", a, b, mult=0.55, rise=0.03)
        _bump(ctx, "trino_queued_queries|trino-lakehouse", a, b, add=9, rise=0.1)
        _bump(ctx, "trino_query_p95_sec|trino-lakehouse", a, b, mult=2.6, rise=0.1)
        _bump(ctx, "jvm_heap_used_ratio|trino-worker-0", a, b, add=0.22, rise=0.1)
        _bump(ctx, "container_cpu_usage_cores|trino-worker-0", a, b, mult=1.7, rise=0.1)
        s["container_cpu_usage_cores|trino-worker-0"] = np.minimum(s["container_cpu_usage_cores|trino-worker-0"], 4)
        _cap(ctx, "jvm_heap_used_ratio|trino-worker-0", 0.99)
        ctx.log(a + 1, "trino-coordinator", "WARN", "Node trino-worker-1 (10.42.3.22) is not responding, "
                "removing it from the active node set", "io.trino.node.DiscoveryNodeManager")
        ctx.incident(incident_id=None, scenario="crashloop_bad_config", variant="trino_worker", split=split,
                     title="trino-worker-1 crash-looping, query capacity halved", a=a, b=b, actionable=True,
                     severity="high", root_cause_category="bad_deploy_config",
                     root_cause=("query.max-memory-per-node was raised to 9GB, which with 2.4GB heap headroom exceeds the "
                                 "6GB worker heap; Trino refuses to start, so trino-worker-1 crash-loops after the "
                                 "rolling restart and the cluster runs on one worker."),
                     trigger_change_id=cid, affected_entities=["pod:trino-worker-1", "cluster:trino-lakehouse"],
                     key_signals=[(f"kube_pod_container_status_restarts_total|{pod}", a, b, "up"),
                                  ("trino_queued_queries|trino-lakehouse", a, b, "up"),
                                  ("trino_query_p95_sec|trino-lakehouse", a, b, "up"),
                                  ("trino_completed_queries_per_min|trino-lakehouse", a, b, "down")],
                     expected_evidence=["'Invalid memory configuration' startup error", "config change " + cid],
                     suggested_fix=["Revert query.max-memory-per-node to 4GB",
                                    "If more is needed, raise -Xmx and the container memory limit together"])
    return post


# =============================================================== S5 memory leak
def consumer_memory_leak(ctx, day, split):
    a, b = ctx.M(day, 6, 0), ctx.M(day + 1, 9, 0)
    cid = ctx.change(a, "deploy", "orders-etl-consumer", {"image": ["orders-etl-consumer:1.7.3", "orders-etl-consumer:1.8.1"]},
                     "r.gomez", "Cache customer profiles in-process to cut DB lookups")
    ctx.change(b, "deploy", "orders-etl-consumer", {"image": ["orders-etl-consumer:1.8.1", "orders-etl-consumer:1.7.3"]},
               "oncall-sre", "Rollback consumer, memory leak")
    kills = []
    for pod, rate in (("orders-etl-consumer-0", 48), ("orders-etl-consumer-1", 61)):  # MiB/hour
        sid = f"container_memory_working_set_bytes|{pod}"
        t = a + 1
        while t < b:
            n = np.arange(int(b) - int(t))
            grow = 400 * MiB + rate * MiB * n / 60
            hit = np.argmax(grow >= 1 * GiB) if (grow >= 1 * GiB).any() else len(n)
            seg = ctx.sl(t, t + hit)
            ctx.s[sid][seg] = grow[:hit] * (1 + ctx.rng.normal(0, 0.004, hit))
            if hit < len(n):
                tk = t + hit
                kills.append((pod, tk))
                ctx.restart(pod, tk)
                ctx.event(tk, pod, "OOMKilled", f"Container consumer in pod {pod} was OOMKilled (exit code 137, "
                          "memory limit 1Gi)", "Warning")
                ctx.event(tk + 0.3, pod, "Started", "Started container consumer")
                ctx.cap_mult[ctx.sl(tk, tk + 2)] = 0.5
                t = tk + 1
            else:
                break
        _logs_every(ctx, a + 30, b, 30, pod, "INFO",
                    lambda k: f"customer profile cache size={(k + 1) * 9100} entries (no eviction policy)",
                    "orders_etl.cache")

    def post():
        ctx.incident(incident_id=None, scenario="memory_leak", variant="consumer", split=split,
                     title="orders-etl-consumer memory climbing, OOMKilled repeatedly", a=a + 60, b=b,
                     actionable=True, severity="medium", root_cause_category="memory_leak",
                     root_cause=("orders-etl-consumer 1.8.1 added an in-process customer profile cache with no eviction; "
                                 "memory grows ~50-60 MiB/hour per pod until the 1Gi limit, then the pod is OOMKilled "
                                 "and the cycle repeats."),
                     trigger_change_id=cid, affected_entities=["pod:orders-etl-consumer-0", "pod:orders-etl-consumer-1"],
                     key_signals=[("container_memory_working_set_bytes|orders-etl-consumer-0", a + 60, b, "up"),
                                  ("container_memory_working_set_bytes|orders-etl-consumer-1", a + 60, b, "up")] +
                                 [(f"kube_pod_container_status_restarts_total|{p}", t - 1, t + 2, "up") for p, t in kills],
                     expected_evidence=["linear memory growth starting after deploy " + cid, "OOMKilled events",
                                        "cache size logs increasing"],
                     suggested_fix=["Roll back to 1.7.3", "Bound the cache (LRU with max entries / TTL)"])
    return post


def flink_state_leak(ctx, day, split):
    a = ctx.M(day - 1, 22, 0)
    b = ctx.M(day + 1, 8, 0)
    cid = ctx.change(a, "flink_job_update", "clickstream-enrich", {"jar": ["clickstream-enrich-3.1.0.jar",
                     "clickstream-enrich-3.2.0.jar"]}, "t.ivanova", "Add sessionization operator (keyed state per user)")
    ctx.change(b, "flink_job_update", "clickstream-enrich", {"jar": ["clickstream-enrich-3.2.0.jar",
               "clickstream-enrich-3.2.1.jar"]}, "t.ivanova", "Add state TTL (30 min) to sessionization state")
    kills = []
    for pod, rate in (("flink-taskmanager-0", 55), ("flink-taskmanager-1", 85)):
        sid = f"container_memory_working_set_bytes|{pod}"
        t = a + 5
        while t < b:
            n = np.arange(int(b) - int(t))
            grow = 2.4 * GiB + rate * MiB * n / 60
            lim = 4 * GiB
            hit = np.argmax(grow >= lim) if (grow >= lim).any() else len(n)
            seg = ctx.sl(t, t + hit)
            ctx.s[sid][seg] = grow[:hit] * (1 + ctx.rng.normal(0, 0.003, hit))
            if hit < len(n):
                tk = t + hit
                kills.append((pod, tk))
                ctx.restart(pod, tk)
                ctx.event(tk, pod, "OOMKilled", f"Container flink-main-container in pod {pod} was OOMKilled "
                          "(exit code 137, memory limit 4Gi)", "Warning")
                r = ctx.s["flink_job_restarts_total|clickstream-enrich"]
                r[int(tk):] += 1
                _bump(ctx, "flink_records_out_per_sec|clickstream-enrich", tk, tk + 6, mult=0.05, rise=0.05)
                _bump(ctx, "flink_backpressure_ratio|clickstream-enrich", tk + 5, tk + 25, add=0.5, shape="decay")
                ctx.log(tk + 0.2, "flink-jobmanager", "INFO", f"Task 'Sessionize -> Sink: MinIO' switched from RUNNING "
                        f"to FAILED. org.apache.flink.runtime.jobmaster.JobMasterException: TaskManager with id "
                        f"{pod}:6122 is no longer reachable.", "org.apache.flink.runtime.executiongraph.ExecutionGraph")
                ctx.log(tk + 0.3, "flink-jobmanager", "INFO", "Restarting job clickstream-enrich "
                        "(restart strategy: exponential-delay)", "org.apache.flink.runtime.jobmaster.JobMaster")
                ctx.log(tk + 1.5, "flink-jobmanager", "INFO", f"Restoring job clickstream-enrich from Checkpoint "
                        f"{int(9000 + tk / 2)}", "org.apache.flink.runtime.checkpoint.CheckpointCoordinator")
                t = tk + 2
            else:
                break
        _logs_every(ctx, a + 60, b, 60, pod, "INFO",
                    lambda k: f"RocksDB estimate-live-data-size={0.4 + k * 0.09:.2f} GB for operator "
                              "'Sessionize' (state.backend.rocksdb.memory.managed=false)",
                    "org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor")

    def post():
        ctx.incident(incident_id=None, scenario="memory_leak", variant="flink_state", split=split,
                     title="Flink TaskManager memory climbing, job restarting on OOMKill", a=a + 120, b=b,
                     actionable=True, severity="high", root_cause_category="memory_leak",
                     root_cause=("clickstream-enrich 3.2.0 added keyed session state without a TTL; RocksDB native "
                                 "memory grows outside the JVM heap (heap stays flat) until the TaskManager container "
                                 "is OOMKilled and the job restarts from checkpoint."),
                     trigger_change_id=cid, affected_entities=["flink_job:clickstream-enrich",
                                                               "pod:flink-taskmanager-1", "pod:flink-taskmanager-0"],
                     key_signals=[("container_memory_working_set_bytes|flink-taskmanager-1", a + 120, b, "up"),
                                  ("container_memory_working_set_bytes|flink-taskmanager-0", a + 180, b, "up")] +
                                 [("flink_job_restarts_total|clickstream-enrich", t - 1, t + 3, "up") for _, t in kills],
                     expected_evidence=["container memory rising while jvm_heap_used_ratio is flat (off-heap growth)",
                                        "flink job update " + cid, "RocksDB live data size logs growing"],
                     suggested_fix=["Add StateTtlConfig to session state (done in 3.2.1)",
                                    "Enable managed RocksDB memory (state.backend.rocksdb.memory.managed=true)"],
                     novel_in_test=False)
    return post


# =============================================================== S6 schedule
def dag_paused(ctx, day, split):
    p, u = ctx.M(day - 1, 23, 10), ctx.M(day, 9, 40)
    cid = ctx.change(p, "dag_pause", "daily_orders", {"is_paused": [False, True]}, "m.ortiz",
                     "Pause daily_orders during warehouse table migration")
    ctx.change(u, "dag_unpause", "daily_orders", {"is_paused": [True, False]}, "m.ortiz", "Unpause daily_orders")

    def post():
        sched = ctx.M(day, 2, 0)
        run = _runs(ctx, "daily_orders", day)[0]
        ctx.incident(incident_id=None, scenario="dag_late_or_missing", variant="paused", split=split,
                     title="daily_orders did not run at 02:00", a=sched + 15, b=run["_e"], actionable=True,
                     severity="high", root_cause_category="scheduler_paused",
                     root_cause=("daily_orders was paused for a migration at 23:10 the night before and not unpaused "
                                 "until 09:40, so the 02:00 run was skipped and only ran as a catch-up 7.7 h late; "
                                 "customer_agg at 03:30 used stale orders."),
                     trigger_change_id=cid, affected_entities=["dag:daily_orders", "dag:customer_agg"],
                     key_signals=[], dag_run_ids=[run["run_id"]],
                     expected_evidence=["no daily_orders run at the scheduled time", "dag_pause change " + cid],
                     suggested_fix=["Unpause the DAG", "Alert when a production DAG stays paused past its next run"])
    return post


def upstream_late(ctx, day, split):
    ctx.overrides[("daily_orders", day)] = {"sensor_min": 141.0}

    def post():
        run = _runs(ctx, "daily_orders", day)[0]
        sensor = run["tasks"][0]
        ctx.incident(incident_id=None, scenario="dag_late_or_missing", variant="upstream_late", split=split,
                     title="daily_orders missed its 90-minute SLA", a=run["_sched"] + 20, b=run["_e"],
                     actionable=True, severity="medium", root_cause_category="upstream_delay",
                     root_cause=("The upstream orders export landed its _SUCCESS marker ~2h20m late; the "
                                 "wait_for_upstream sensor poked for 141 minutes, so spark_submit started late and "
                                 "the run missed its SLA. Nothing in the platform was broken."),
                     trigger_change_id=None, affected_entities=["dag:daily_orders"], key_signals=[],
                     dag_run_ids=[run["run_id"]],
                     expected_evidence=["wait_for_upstream ran 141 min vs usual 1-8 min", "repeated 'Poking for key' logs",
                                        "spark app itself had normal duration"],
                     suggested_fix=["Contact the upstream export owner", "Add an SLA on the upstream export"])
    return post


# =============================================================== S7 expected bursts
def traffic_burst(ctx, day, split, h, dur, mult, annotated):
    a, b = ctx.M(day, h, 0), ctx.M(day, h, 0) + dur
    cid = None
    if annotated:
        cid = ctx.change(a - 30, "annotation", "orders", {}, "marketing-ops",
                         "Fall Sale email campaign sends at 18:00 UTC, expect ~4-5x order traffic for 45 min")
    sl = ctx.sl(a, b)
    n = sl.stop - sl.start
    ctx.produce_mult[sl] = 1 + (mult - 1) * np.clip(np.minimum(np.linspace(0, 1, n) / 0.1, (1 - np.linspace(0, 1, n)) / 0.3), 0, 1)

    def post():
        lag = ctx.s["kafka_consumergroup_lag|orders-etl"]
        end = b + np.argmax(lag[int(b):int(b) + 600] < 2000)
        for p in ("kafka-0", "kafka-1", "kafka-2"):
            _bump(ctx, f"container_cpu_usage_cores|{p}", a, b, mult=1.9)
        _bump(ctx, "kafka_produce_request_p99_ms|kafka-ingest", a, b, mult=1.8)
        ctx.incident(incident_id=None, scenario="traffic_burst", variant="annotated" if annotated else "unannotated",
                     split=split, title="orders traffic burst, lag rose then drained", a=a, b=end, actionable=False,
                     severity="low", root_cause_category="expected_traffic",
                     root_cause=(f"A marketing campaign drove order traffic ~{mult:.1f}x for {dur} minutes. Consumers ran "
                                 "at capacity and lag drained on its own afterwards. Expected; no action needed."),
                     trigger_change_id=cid, affected_entities=["topic:orders", "consumer_group:orders-etl"],
                     key_signals=[("kafka_messages_in_per_sec|orders", a, b, "up"),
                                  ("kafka_consumergroup_lag|orders-etl", a + 3, end, "up")],
                     expected_evidence=["produce rate itself spiked (cause is load, not consumer slowness)",
                                        "lag drains after traffic returns to normal"],
                     suggested_fix=["None. Label as expected; optionally pre-scale consumers before known campaigns"])
    return post


def broker_maintenance(ctx, day, split):
    a = ctx.M(day, 3, 10)
    cid = ctx.change(a - 2, "node_maintenance", "ip-10-0-3-41.ec2.internal", {"action": "drain"}, "infra-bot",
                     "Drain node ip-10-0-3-41 for kernel patching (hosts kafka-2)")
    pod = "kafka-2"
    ctx.down(pod, a, a + 3)
    ctx.restart(pod, a + 2.5)
    ctx.event(a, pod, "Killing", "Stopping container kafka")
    ctx.event(a + 2, pod, "Scheduled", "Successfully assigned kafka/kafka-2 to ip-10-0-3-58.ec2.internal")
    ctx.event(a + 2.5, pod, "Started", "Started container kafka")
    ctx.event(a + 3, pod, "Unhealthy", "Readiness probe failed: broker not yet caught up", "Warning")

    def post():
        s = ctx.s
        urp = s["kafka_under_replicated_partitions|kafka-ingest"]
        sl = ctx.sl(a, a + 12)
        urp[sl] = np.round(16 * np.clip(1 - np.linspace(0, 1, sl.stop - sl.start) ** 1.5, 0, 1))
        _bump(ctx, "kafka_isr_shrinks_per_sec|kafka-ingest", a, a + 3, add=0.25, shape="decay")
        _bump(ctx, "kafka_produce_request_p99_ms|kafka-ingest", a, a + 6, add=70, shape="decay")
        _bump(ctx, "kafka_failed_produce_requests_per_sec|kafka-ingest", a, a + 3, add=2.5, shape="decay")
        mem = s[f"container_memory_working_set_bytes|{pod}"]
        mem[ctx.sl(a + 3, a + 120)] *= np.linspace(0.55, 1.0, int(a + 120) - int(a + 3))
        for p in ("kafka-0", "kafka-1"):
            ctx.log(a + 0.2, p, "INFO", f"[Partition orders-{2 if p == 'kafka-0' else 5} broker={p[-1]}] Shrinking ISR "
                    f"from 0,1,2 to {'0,1'}. Leader: (highWatermark: 88123412, endOffset: 88123477). Out of sync "
                    "replicas: (brokerId: 2, endOffset: 88123401)", "kafka.cluster.Partition")
            ctx.log(a + 9, p, "INFO", f"[Partition orders-{2 if p == 'kafka-0' else 5} broker={p[-1]}] ISR updated to "
                    "0,1,2", "kafka.cluster.Partition")
        ctx.log(a + 2.7, pod, "INFO", "[KafkaServer id=2] started", "kafka.server.KafkaServer")
        ctx.log(a + 0.3, "orders-producer", "WARN", "Got error produce response with correlation id 991827 on "
                "topic-partition orders-2, retrying (2 attempts left). Error: NOT_LEADER_OR_FOLLOWER",
                "org.apache.kafka.clients.producer.internals.Sender")
        ctx.incident(incident_id=None, scenario="broker_restart", variant="planned_maintenance", split=split,
                     title="kafka-2 restarted, partitions under-replicated briefly", a=a, b=a + 12, actionable=False,
                     severity="low", root_cause_category="planned_maintenance",
                     root_cause=("The node hosting kafka-2 was drained for kernel patching. The broker moved to another "
                                 "node, partitions were under-replicated for ~10 minutes while it caught up, then "
                                 "recovered. Expected."),
                     trigger_change_id=cid, affected_entities=["pod:kafka-2", "cluster:kafka-ingest"],
                     key_signals=[("kafka_under_replicated_partitions|kafka-ingest", a, a + 12, "up"),
                                  (f"kube_pod_container_status_restarts_total|{pod}", a, a + 4, "up"),
                                  ("kafka_produce_request_p99_ms|kafka-ingest", a, a + 6, "up")],
                     expected_evidence=["node drain change " + cid, "URP returns to 0 within minutes"],
                     suggested_fix=["None. Optionally suppress URP alerts during planned node drains"])
    return post


# =============================================================== novel: broker GC pressure
def broker_gc_pressure(ctx, day, split):
    a, b = ctx.M(day, 12, 40), ctx.M(day, 15, 10)
    cid = ctx.change(a, "deploy", "finance-reporting-consumer", {"replicas": [0, 4], "auto.offset.reset": "earliest",
                     "fetch.max.bytes": "524288000"}, "b.okafor",
                     "Launch finance reporting consumer (initial backfill of orders from offset 0)")
    ctx.change(b, "kafka_quota", "finance-reporting-consumer", {"consumer_byte_rate": [None, "20MB/s"]},
               "oncall-sre", "Throttle finance-reporting-consumer with a client quota")

    def post():
        s = ctx.s
        pod = "kafka-1"
        _bump(ctx, f"jvm_heap_used_ratio|{pod}", a, b, add=0.42, rise=0.08)
        _cap(ctx, f"jvm_heap_used_ratio|{pod}", 0.985)
        _bump(ctx, f"jvm_gc_time_ms_per_sec|{pod}", a + 5, b, add=520, rise=0.1)
        _bump(ctx, f"container_cpu_usage_cores|{pod}", a, b, mult=2.0)
        _bump(ctx, "kafka_produce_request_p99_ms|kafka-ingest", a + 8, b, add=900, rise=0.1)
        _bump(ctx, "kafka_failed_produce_requests_per_sec|kafka-ingest", a + 10, b, add=1.8, rise=0.1)
        urp = s["kafka_under_replicated_partitions|kafka-ingest"]
        isr = s["kafka_isr_shrinks_per_sec|kafka-ingest"]
        t = a + 12
        while t < b - 5:   # kafka-1 flaps out of ISR during long GC pauses
            d = ctx.rng.uniform(3, 9)
            urp[ctx.sl(t, t + d)] = np.maximum(urp[ctx.sl(t, t + d)], 12)
            isr[int(t)] += 0.2
            ctx.log(t, "kafka-0", "INFO", "[Partition orders-1 broker=0] Shrinking ISR from 0,1,2 to 0,2. Out of sync "
                    "replicas: (brokerId: 1, endOffset: 91233001, lastCaughtUpTimeMs: 31544)", "kafka.cluster.Partition")
            ctx.log(t + 0.1, pod, "WARN", f"[ReplicaFetcher replicaId=1, leaderId=0, fetcherId=0] Error sending fetch "
                    "request (sessionId=812763314) to node 0: java.net.SocketTimeoutException", "kafka.server.ReplicaFetcherThread")
            ctx.log(t + 0.05, pod, "WARN", f"[GC pause (G1 Evacuation Pause) (young) {ctx.rng.uniform(1.8, 4.2):.2f}s] "
                    "heap 3584M->3410M(3584M)", "gc")
            t += d + ctx.rng.uniform(4, 12)
        _logs_every(ctx, a + 2, b, 4, pod, "INFO",
                    lambda k: f"[KafkaApi-1] Fetch request from client finance-reporting-{k % 4} for orders "
                              f"offset {k * 250000} returned 500 MB (fetch.max.bytes=524288000)",
                    "kafka.server.KafkaApis")
        ctx.incident(incident_id=None, scenario="broker_gc_pressure", variant="large_fetch_backfill", split=split,
                     title="kafka-1 long GC pauses, ISR flapping, produce latency up", a=a + 5, b=b, actionable=True,
                     severity="high", root_cause_category="noisy_neighbor_client",
                     root_cause=("A new finance-reporting consumer started a full backfill from offset 0 with "
                                 "fetch.max.bytes=500MB, mostly against partitions led by kafka-1. Huge fetch responses "
                                 "filled the broker heap, causing multi-second GC pauses; kafka-1 repeatedly fell out "
                                 "of ISR and produce p99 latency rose ~50x."),
                     trigger_change_id=cid, affected_entities=["pod:kafka-1", "cluster:kafka-ingest"],
                     key_signals=[(f"jvm_heap_used_ratio|{pod}", a, b, "up"), (f"jvm_gc_time_ms_per_sec|{pod}", a + 5, b, "up"),
                                  ("kafka_produce_request_p99_ms|kafka-ingest", a + 8, b, "up"),
                                  ("kafka_under_replicated_partitions|kafka-ingest", a + 12, b, "up")],
                     expected_evidence=["deploy " + cid + " of a new consumer just before onset",
                                        "fetch logs with 500 MB responses", "GC pause logs on kafka-1"],
                     suggested_fix=["Apply a client quota to finance-reporting-consumer",
                                    "Lower its fetch.max.bytes / max.partition.fetch.bytes"],
                     novel_in_test=True)
    return post


# =============================================================== S9 trino saturation
def trino_heavy_query(ctx, day, split):
    a, b = ctx.M(day, 20, 0), ctx.M(day, 20, 42)
    qid = f"2026{day:02d}_200012_00417_x8k2m"

    def post():
        s = ctx.s
        for w in ("trino-worker-0", "trino-worker-1"):
            _bump(ctx, f"jvm_heap_used_ratio|{w}", a, b, add=0.5, rise=0.08)
            _cap(ctx, f"jvm_heap_used_ratio|{w}", 0.97)
            _bump(ctx, f"jvm_gc_time_ms_per_sec|{w}", a, b, add=180)
            _bump(ctx, f"container_cpu_usage_cores|{w}", a, b, add=2.2)
            s[f"container_cpu_usage_cores|{w}"] = np.minimum(s[f"container_cpu_usage_cores|{w}"], 4)
        _bump(ctx, "trino_queued_queries|trino-lakehouse", a + 3, b, add=22)
        _bump(ctx, "trino_resource_group_queued|adhoc", a + 3, b, add=14)
        _bump(ctx, "trino_query_p95_sec|trino-lakehouse", a + 2, b, mult=9)
        _bump(ctx, "trino_completed_queries_per_min|trino-lakehouse", a + 2, b, mult=0.35)
        _bump(ctx, "trino_failed_queries_per_min|trino-lakehouse", a + 5, b, add=4)
        ctx.log(a, "trino-coordinator", "INFO", f"Query {qid} created: user=analyst.rhea source=trino-cli "
                "resource_group=global.adhoc sql=SELECT o.*, c.* FROM lake.orders_enriched o, lake.customers c "
                "WHERE o.order_ts > date '2026-01-01'", "io.trino.event.QueryMonitor")
        _logs_every(ctx, a + 6, b, 1.2, "trino-coordinator", "ERROR",
                    lambda k: f"Query 2026{day:02d}_20{k:02d}_0{k}_q{k} failed: EXCEEDED_LOCAL_MEMORY_LIMIT: Query "
                              "exceeded per-node memory limit of 4GB [Allocated: 4.00GB, Delta: 11.2MB, Top Consumers: "
                              "{HashBuilderOperator=3.71GB}]", "io.trino.event.QueryMonitor")
        ctx.log(b, "trino-coordinator", "INFO", f"Query {qid} failed: USER_CANCELED Query was canceled by oncall-sre",
                "io.trino.event.QueryMonitor")
        ctx.incident(incident_id=None, scenario="trino_saturation", variant="cross_join", split=split,
                     title="Trino queries queuing and failing on memory", a=a + 2, b=b, actionable=True,
                     severity="medium", root_cause_category="noisy_neighbor_query",
                     root_cause=(f"Ad-hoc query {qid} by analyst.rhea is an accidental cross join (no join condition) "
                                 "between orders_enriched and customers. Its hash build consumed most worker memory, so "
                                 "other queries queued or failed with EXCEEDED_LOCAL_MEMORY_LIMIT until it was killed."),
                     trigger_change_id=None, affected_entities=["cluster:trino-lakehouse", "resource_group:adhoc"],
                     key_signals=[("jvm_heap_used_ratio|trino-worker-0", a, b, "up"),
                                  ("jvm_heap_used_ratio|trino-worker-1", a, b, "up"),
                                  ("trino_queued_queries|trino-lakehouse", a + 3, b, "up"),
                                  ("trino_query_p95_sec|trino-lakehouse", a + 2, b, "up"),
                                  ("trino_failed_queries_per_min|trino-lakehouse", a + 5, b, "up")],
                     expected_evidence=[f"query {qid} with a comma join and no WHERE join condition",
                                        "EXCEEDED_LOCAL_MEMORY_LIMIT errors, HashBuilderOperator top consumer"],
                     suggested_fix=["Kill the query", "Set query_max_memory and a hard concurrency limit on the adhoc "
                                    "resource group"])
    return post


def trino_dashboard_storm(ctx, day, split):
    a, b = ctx.M(day, 21, 0), ctx.M(day, 21, 35)
    cid = ctx.change(a - 120, "bi_schedule", "superset:finance_daily_dash", {"refresh": ["off", "every 30 min"],
                     "charts": 42}, "finance-analytics", "Enable auto-refresh on Finance Daily dashboard")

    def post():
        s = ctx.s
        for k in (0, 30):
            if a + k < b:
                _bump(ctx, "trino_queued_queries|trino-lakehouse", a + k, a + k + 14, add=36, shape="decay")
                _bump(ctx, "trino_completed_queries_per_min|trino-lakehouse", a + k, a + k + 14, add=35, shape="decay")
                _bump(ctx, "trino_query_p95_sec|trino-lakehouse", a + k, a + k + 14, mult=5, shape="decay")
        for w in ("trino-worker-0", "trino-worker-1"):
            _bump(ctx, f"container_cpu_usage_cores|{w}", a, b, add=1.6)
            s[f"container_cpu_usage_cores|{w}"] = np.minimum(s[f"container_cpu_usage_cores|{w}"], 4)
            _bump(ctx, f"jvm_heap_used_ratio|{w}", a, b, add=0.25)
        _logs_every(ctx, a, b, 0.6, "trino-coordinator", "INFO",
                    lambda k: f"Query 2026{day:02d}_21{k:03d}_q created: user=svc_superset source=Apache Superset "
                              f"resource_group=global.adhoc chart_id={1100 + k % 42}", "io.trino.event.QueryMonitor")
        ctx.incident(incident_id=None, scenario="trino_saturation", variant="dashboard_storm", split=split,
                     title="Trino queue spikes every 30 minutes", a=a, b=b, actionable=True, severity="medium",
                     root_cause_category="noisy_neighbor_query",
                     root_cause=("Auto-refresh was enabled on the Finance Daily Superset dashboard (42 charts every 30 "
                                 "minutes). Each refresh fires ~42 concurrent queries as svc_superset into the adhoc "
                                 "resource group, saturating workers and queueing other users."),
                     trigger_change_id=cid, affected_entities=["cluster:trino-lakehouse", "resource_group:adhoc"],
                     key_signals=[("trino_queued_queries|trino-lakehouse", a, b, "up"),
                                  ("trino_query_p95_sec|trino-lakehouse", a, b, "up")],
                     expected_evidence=["bursts of svc_superset queries at :00 and :30", "BI schedule change " + cid],
                     suggested_fix=["Move svc_superset into its own resource group with a concurrency limit",
                                    "Cache the dashboard or stagger refreshes"])
    return post


# =============================================================== S10 flink checkpoints
def flink_sink_slow(ctx, day, split, h):
    a, b = ctx.M(day, h, 0), ctx.M(day, h, 85)

    def post():
        s = ctx.s
        cp = s["flink_last_checkpoint_duration_ms|clickstream-enrich"]
        sl = ctx.sl(a, b)
        n = sl.stop - sl.start
        x = np.linspace(0, 1, n)
        cp[sl] = np.minimum(60000, cp[sl] * (1 + 30 * np.clip(x / 0.35, 0, 1) * np.clip((1 - x) / 0.15, 0, 1)))
        fails = s["flink_checkpoint_failures_total|clickstream-enrich"]
        first_fail = a + 0.35 * (b - a)
        tf = first_fail
        k = 0
        while tf < b - 12:
            fails[int(tf):] += 1
            k += 1
            ctx.log(tf, "flink-jobmanager", "WARN", f"Checkpoint {int(9300 + tf / 3)} of job clickstream-enrich "
                    "expired before completing.", "org.apache.flink.runtime.checkpoint.CheckpointCoordinator")
            if k % 3 == 0:
                ctx.log(tf + 0.1, "flink-jobmanager", "ERROR", "Triggering job failover: Exceeded checkpoint tolerable "
                        "failure threshold (3).", "org.apache.flink.runtime.checkpoint.CheckpointFailureManager")
                s["flink_job_restarts_total|clickstream-enrich"][int(tf + 0.5):] += 1
            tf += 2.0
        _bump(ctx, "flink_backpressure_ratio|clickstream-enrich", a + 10, b, add=0.8, rise=0.1)
        s["flink_backpressure_ratio|clickstream-enrich"] = np.minimum(s["flink_backpressure_ratio|clickstream-enrich"], 1)
        _bump(ctx, "flink_records_out_per_sec|clickstream-enrich", a + 10, b, mult=0.55)
        for tm in ("flink-taskmanager-0", "flink-taskmanager-1"):
            _bump(ctx, f"container_cpu_usage_cores|{tm}", a + 10, b, mult=0.5)
            _logs_every(ctx, a + 5, b - 10, 3, tm, "WARN",
                        lambda k: "Retrying PUT s3://lake-clickstream/enriched/part-" + f"{k:05d}" +
                                  ".parquet after: software.amazon.awssdk.core.exception.SdkClientException: Unable to "
                                  "execute HTTP request: Read timed out (attempt 3/10)",
                        "org.apache.flink.fs.s3.common.writer.S3RecoverableFsDataOutputStream")
        ctx.incident(incident_id=None, scenario="flink_checkpoint_failures", variant="sink_latency", split=split,
                     title="Flink checkpoints timing out, job restarting", a=a + 10, b=b, actionable=True,
                     severity="medium", root_cause_category="external_dependency",
                     root_cause=("Writes to MinIO (s3://lake-clickstream) slowed down (S3 PUT read timeouts). The file "
                                 "sink could not flush, checkpoints grew to the 60 s timeout and expired, backpressure "
                                 "rose, and after 3 failed checkpoints the job failed over. Flink itself was healthy."),
                     trigger_change_id=None, affected_entities=["flink_job:clickstream-enrich"],
                     key_signals=[("flink_last_checkpoint_duration_ms|clickstream-enrich", a + 5, b, "up"),
                                  ("flink_backpressure_ratio|clickstream-enrich", a + 10, b, "up"),
                                  ("flink_checkpoint_failures_total|clickstream-enrich", first_fail, b, "up"),
                                  ("flink_records_out_per_sec|clickstream-enrich", a + 10, b, "down")],
                     expected_evidence=["S3 'Read timed out' retries on TaskManagers", "TaskManager CPU dropped (waiting, "
                                        "not busy)", "no recent Flink deploy"],
                     suggested_fix=["Check MinIO health / disk latency", "Raise checkpoint timeout and "
                                    "tolerable-failed-checkpoints during storage incidents"])
    return post


# =============================================================== novel: airflow scheduler stall
def airflow_db_pool(ctx, day, split):
    a, b = ctx.M(day, 12, 50), ctx.M(day, 14, 20)
    cid = ctx.change(a, "config", "airflow", {"core.parallelism": [16, 64], "core.max_active_tasks_per_dag": [8, 32]},
                     "p.singh", "Increase Airflow parallelism for upcoming backfills")
    ctx.change(b, "config", "airflow", {"core.parallelism": [64, 16], "core.max_active_tasks_per_dag": [32, 8]},
               "oncall-sre", "Revert parallelism, scheduler starved of DB connections")
    sl = ctx.sl(a + 5, b)
    ctx.hourly_delay[sl] = np.minimum(np.arange(sl.stop - sl.start) * 0.9, 70)

    def post():
        s = ctx.s
        hb = s["airflow_scheduler_heartbeat_age_sec|airflow-main"]
        sl2 = ctx.sl(a + 5, b)
        n = sl2.stop - sl2.start
        hb[sl2] = 3 + 40 * np.abs(np.sin(np.linspace(0, 9, n))) * np.linspace(0.6, 1, n) + ctx.rng.normal(0, 4, n) ** 2
        _bump(ctx, "container_cpu_usage_cores|airflow-scheduler", a + 5, b, mult=0.4)
        _logs_every(ctx, a + 6, b, 2.5, "airflow-scheduler", "ERROR",
                    lambda k: "sqlalchemy.exc.TimeoutError: QueuePool limit of size 5 overflow 10 reached, connection "
                              "timed out, timeout 30.00 (Background on this error at: https://sqlalche.me/e/20/3o7r)",
                    "airflow.jobs.scheduler_job_runner")
        _logs_every(ctx, a + 7, b, 6, "airflow-scheduler", "WARN",
                    lambda k: f"Scheduler heartbeat delayed: last heartbeat {int(ctx.rng.uniform(35, 140))}s ago",
                    "airflow.jobs.job")
        runs = [r for r in ctx.dag_runs if r["dag_id"] == "hourly_quality_check" and a <= r["_sched"] < b]
        ctx.incident(incident_id=None, scenario="scheduler_stall", variant="db_pool_exhaustion", split=split,
                     title="Airflow scheduler stalling, hourly runs starting late", a=a + 5, b=b, actionable=True,
                     severity="medium", root_cause_category="db_pool_exhaustion",
                     root_cause=("Airflow parallelism was raised 16 -> 64 without raising sql_alchemy_pool_size; "
                                 "workers hold metadata-DB connections and the scheduler times out waiting for the "
                                 "pool (QueuePool limit of size 5 overflow 10), so heartbeats lag and runs start late."),
                     trigger_change_id=cid, affected_entities=["cluster:airflow-main", "dag:hourly_quality_check"],
                     key_signals=[("airflow_scheduler_heartbeat_age_sec|airflow-main", a + 5, b, "up")],
                     dag_run_ids=[r["run_id"] for r in runs],
                     expected_evidence=["QueuePool TimeoutError logs", "config change " + cid,
                                        "hourly_quality_check lateness rising"],
                     suggested_fix=["Revert parallelism or raise database.sql_alchemy_pool_size / max_overflow",
                                    "Put PgBouncer in front of the metadata DB"],
                     novel_in_test=True)
    return post


# =============================================================== benign changes (red herrings)
def benign_changes(ctx):
    ctx.change(ctx.M(8, 17, 5), "deploy", "trino-coordinator", {"image": ["trinodb/trino:476", "trinodb/trino:477"]},
               "s.nair", "Upgrade Trino coordinator to 477")
    ctx.change(ctx.M(11, 13, 40), "deploy", "orders-stream", {"image": ["spark-jobs:3.5.4-r12", "spark-jobs:3.5.4-r13"]},
               "a.patel", "orders-stream: add order_channel column")
    ctx.change(ctx.M(17, 19, 20), "config", "kafka-ingest", {"log.retention.hours": [168, 120]}, "infra-bot",
               "Reduce Kafka retention to 5 days")
    ctx.change(ctx.M(22, 20, 10), "deploy", "airflow", {"image": ["apache/airflow:3.0.6", "apache/airflow:3.0.6-r2"]},
               "p.singh", "Airflow image rebuild with security patches")
    ctx.change(ctx.M(24, 10, 30), "deploy", "orders-etl-consumer", {"image": ["orders-etl-consumer:1.7.3",
               "orders-etl-consumer:1.7.4"]}, "r.gomez", "Log format change only")
    ctx.change(ctx.M(26, 2, 50), "spark_config", "customer-agg", {"spark.sql.adaptive.enabled": [False, True]},
               "a.patel", "Enable AQE on customer-agg")


# =============================================================== registry
def register_all(ctx):
    benign_changes(ctx)
    posts = [
        # ---- train (days 7-20)
        spark_oom(ctx, 9, TRAIN, "executor_memory_reduced", change_t=ctx.M(8, 21, 40), revert_t=ctx.M(9, 9, 50),
                  rerun_t=ctx.M(9, 10, 5)),
        traffic_burst(ctx, 10, TRAIN, 18, 45, 4.5, annotated=True),
        consumer_slow_deploy(ctx, 11, TRAIN),
        spark_skew(ctx, 12, TRAIN, "customer_agg", 2.6),
        dag_paused(ctx, 13, TRAIN),
        producer_crashloop(ctx, 14, TRAIN),
        consumer_memory_leak(ctx, 15, TRAIN),
        broker_maintenance(ctx, 16, TRAIN),
        trino_heavy_query(ctx, 17, TRAIN),
        spark_oom(ctx, 18, TRAIN, "input_spike"),
        spark_skew(ctx, 19, TRAIN, "customer_agg", 2.4),
        flink_sink_slow(ctx, 20, TRAIN, 5),
        # ---- test (days 21-27)
        flink_state_leak(ctx, 21, TEST),
        consumer_scaled_down(ctx, 22, TEST),
        trino_dashboard_storm(ctx, 22, TEST),
        spark_oom(ctx, 23, TEST, "shuffle_partitions_reduced", change_t=ctx.M(22, 16, 15), revert_t=ctx.M(23, 11, 0),
                  rerun_t=ctx.M(23, 11, 15)),
        trino_worker_crashloop(ctx, 24, TEST),
        traffic_burst(ctx, 24, TEST, 19, 40, 4.0, annotated=False),
        spark_skew(ctx, 25, TEST, "daily_orders", 2.2),
        airflow_db_pool(ctx, 25, TEST),
        flink_sink_slow(ctx, 26, TEST, 4),
        upstream_late(ctx, 26, TEST),
        broker_gc_pressure(ctx, 27, TEST),
    ]
    return posts
