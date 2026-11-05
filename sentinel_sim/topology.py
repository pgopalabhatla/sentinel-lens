"""Static description of the simulated data platform: sources, pods, series catalog.

Everything Sentinel Lens would normally discover from Kubernetes + Prometheus is
declared here, so the generator and the replay API agree on names.
"""
from __future__ import annotations

GiB = 1024 ** 3
MiB = 1024 ** 2

# ---------------------------------------------------------------- sources
SOURCES = [
    {"source_id": "kafka-ingest", "type": "kafka", "namespace": "kafka", "environment": "prod",
     "description": "Strimzi Kafka cluster 'ingest', 3 brokers, topics orders (6 partitions) and clicks (6)"},
    {"source_id": "spark-orders-stream", "type": "spark", "namespace": "spark", "environment": "prod",
     "description": "Spark Structured Streaming app orders-stream (Kafka orders -> Iceberg)"},
    {"source_id": "spark-batch", "type": "spark", "namespace": "spark", "environment": "prod",
     "description": "Spark batch apps daily-orders and customer-agg, submitted by Airflow"},
    {"source_id": "flink-clickstream", "type": "flink", "namespace": "flink", "environment": "prod",
     "description": "Flink job clickstream-enrich (Kafka clicks -> MinIO), 1 JobManager, 2 TaskManagers"},
    {"source_id": "trino-lakehouse", "type": "trino", "namespace": "trino", "environment": "prod",
     "description": "Trino cluster over the Iceberg lakehouse, 1 coordinator, 2 workers"},
    {"source_id": "airflow-main", "type": "airflow", "namespace": "airflow", "environment": "prod",
     "description": "Airflow (LocalExecutor). DAGs: daily_orders, customer_agg, hourly_quality_check"},
    {"source_id": "orders-apps", "type": "app", "namespace": "apps", "environment": "prod",
     "description": "orders-producer and orders-etl-consumer (plain Kafka clients)"},
]

# ---------------------------------------------------------------- pods
# batch=True pods only exist while a Spark batch app runs (values are NaN otherwise)
PODS = [
    # name, source_id, role, cpu_limit, mem_limit_bytes, base_mem_bytes, jvm, batch
    ("kafka-0", "kafka-ingest", "broker", 2, 4 * GiB, 2.8 * GiB, True, False),
    ("kafka-1", "kafka-ingest", "broker", 2, 4 * GiB, 2.8 * GiB, True, False),
    ("kafka-2", "kafka-ingest", "broker", 2, 4 * GiB, 2.8 * GiB, True, False),
    ("orders-producer", "orders-apps", "producer", 0.5, 512 * MiB, 180 * MiB, False, False),
    ("orders-etl-consumer-0", "orders-apps", "consumer", 1, 1 * GiB, 400 * MiB, False, False),
    ("orders-etl-consumer-1", "orders-apps", "consumer", 1, 1 * GiB, 400 * MiB, False, False),
    ("flink-jobmanager", "flink-clickstream", "jobmanager", 1, 1.5 * GiB, 0.9 * GiB, True, False),
    ("flink-taskmanager-0", "flink-clickstream", "taskmanager", 2, 4 * GiB, 2.4 * GiB, True, False),
    ("flink-taskmanager-1", "flink-clickstream", "taskmanager", 2, 4 * GiB, 2.4 * GiB, True, False),
    ("orders-stream-driver", "spark-orders-stream", "driver", 1, 2 * GiB, 1.1 * GiB, True, False),
    ("orders-stream-exec-1", "spark-orders-stream", "executor", 2, 4 * GiB, 2.2 * GiB, True, False),
    ("orders-stream-exec-2", "spark-orders-stream", "executor", 2, 4 * GiB, 2.2 * GiB, True, False),
    ("daily-orders-driver", "spark-batch", "driver", 1, 2 * GiB, 1.2 * GiB, True, True),
    ("daily-orders-exec-1", "spark-batch", "executor", 2, 4 * GiB, 0.6 * GiB, True, True),
    ("daily-orders-exec-2", "spark-batch", "executor", 2, 4 * GiB, 0.6 * GiB, True, True),
    ("daily-orders-exec-3", "spark-batch", "executor", 2, 4 * GiB, 0.6 * GiB, True, True),
    ("customer-agg-driver", "spark-batch", "driver", 1, 2 * GiB, 1.2 * GiB, True, True),
    ("customer-agg-exec-1", "spark-batch", "executor", 2, 4 * GiB, 0.6 * GiB, True, True),
    ("customer-agg-exec-2", "spark-batch", "executor", 2, 4 * GiB, 0.6 * GiB, True, True),
    ("customer-agg-exec-3", "spark-batch", "executor", 2, 4 * GiB, 0.6 * GiB, True, True),
    ("trino-coordinator", "trino-lakehouse", "coordinator", 2, 4 * GiB, 2.6 * GiB, True, False),
    ("trino-worker-0", "trino-lakehouse", "worker", 4, 8 * GiB, 5.0 * GiB, True, False),
    ("trino-worker-1", "trino-lakehouse", "worker", 4, 8 * GiB, 5.0 * GiB, True, False),
    ("airflow-scheduler", "airflow-main", "scheduler", 1, 1 * GiB, 520 * MiB, False, False),
    ("airflow-worker-0", "airflow-main", "worker", 2, 2 * GiB, 700 * MiB, False, False),
]
POD = {p[0]: dict(zip(["pod", "source_id", "role", "cpu_limit", "mem_limit", "base_mem", "jvm", "batch"], p))
       for p in PODS}

# Entity relationships for the correlator (child -> parent edges)
RELATIONSHIPS = [
    # (child_kind, child, relation, parent_kind, parent)
    ("consumer_group", "orders-etl", "consumes", "topic", "orders"),
    ("consumer_group", "orders-stream", "consumes", "topic", "orders"),
    ("consumer_group", "clickstream-enrich", "consumes", "topic", "clicks"),
    ("topic", "orders", "hosted_on", "cluster", "kafka-ingest"),
    ("topic", "clicks", "hosted_on", "cluster", "kafka-ingest"),
    ("pod", "orders-producer", "produces_to", "topic", "orders"),
    ("pod", "orders-etl-consumer-0", "member_of", "consumer_group", "orders-etl"),
    ("pod", "orders-etl-consumer-1", "member_of", "consumer_group", "orders-etl"),
    ("pod", "kafka-0", "broker_of", "cluster", "kafka-ingest"),
    ("pod", "kafka-1", "broker_of", "cluster", "kafka-ingest"),
    ("pod", "kafka-2", "broker_of", "cluster", "kafka-ingest"),
    ("pod", "orders-stream-driver", "runs", "spark_app", "orders-stream"),
    ("pod", "orders-stream-exec-1", "runs", "spark_app", "orders-stream"),
    ("pod", "orders-stream-exec-2", "runs", "spark_app", "orders-stream"),
    ("pod", "daily-orders-driver", "runs", "spark_job", "daily-orders"),
    ("pod", "daily-orders-exec-1", "runs", "spark_job", "daily-orders"),
    ("pod", "daily-orders-exec-2", "runs", "spark_job", "daily-orders"),
    ("pod", "daily-orders-exec-3", "runs", "spark_job", "daily-orders"),
    ("pod", "customer-agg-driver", "runs", "spark_job", "customer-agg"),
    ("pod", "customer-agg-exec-1", "runs", "spark_job", "customer-agg"),
    ("pod", "customer-agg-exec-2", "runs", "spark_job", "customer-agg"),
    ("pod", "customer-agg-exec-3", "runs", "spark_job", "customer-agg"),
    ("spark_job", "daily-orders", "submitted_by", "dag", "daily_orders"),
    ("spark_job", "customer-agg", "submitted_by", "dag", "customer_agg"),
    ("dag", "customer_agg", "depends_on", "dag", "daily_orders"),
    ("pod", "flink-jobmanager", "runs", "flink_job", "clickstream-enrich"),
    ("pod", "flink-taskmanager-0", "runs", "flink_job", "clickstream-enrich"),
    ("pod", "flink-taskmanager-1", "runs", "flink_job", "clickstream-enrich"),
    ("pod", "trino-coordinator", "member_of", "cluster", "trino-lakehouse"),
    ("pod", "trino-worker-0", "member_of", "cluster", "trino-lakehouse"),
    ("pod", "trino-worker-1", "member_of", "cluster", "trino-lakehouse"),
    ("pod", "airflow-scheduler", "member_of", "cluster", "airflow-main"),
    ("pod", "airflow-worker-0", "member_of", "cluster", "airflow-main"),
]

# ---------------------------------------------------------------- canonical events
CANONICAL_EVENTS = {
    # name: (layer, default_detector, healthy_when, description)
    "cpu_usage": (1, "seasonal_baseline", "below_limit", "Container CPU in cores"),
    "memory_usage": (1, "saturation_forecast", "below_limit", "Container working-set memory"),
    "cpu_throttling": (1, "seasonal_baseline", "low", "Fraction of CFS periods throttled"),
    "restarts": (1, "guardrail", "flat", "Container restart counter (use increase())"),
    "throughput": (2, "seasonal_baseline", "near_baseline", "Work completed per unit time"),
    "backlog": (2, "saturation_forecast", "not_growing", "Work waiting to be processed"),
    "latency": (2, "change_point", "low", "How long work takes (percentile)"),
    "error_rate": (2, "seasonal_baseline", "low", "Failed work per unit time"),
    "jvm_heap": (2, "saturation_forecast", "below_limit", "JVM heap used / max"),
    "jvm_gc": (2, "change_point", "low", "Time spent in GC per second"),
    "run_duration": (2, "change_point", "near_baseline", "Duration of a unit of scheduled/streaming work"),
    "run_outcome": (2, "job_run_baseline", "success", "Success/failure of runs (job_runs.csv + restart counters)"),
    "run_lateness": (2, "schedule_watcher", "on_time", "Actual vs scheduled start (job_runs.csv)"),
}


def build_series_catalog():
    """Return list of dicts describing every metric series in metrics.csv.gz."""
    rows = []

    def add(metric, entity, layer, canonical, source_id, entity_kind, unit, labels, detector=None):
        sid = f"{metric}|{entity}"
        rows.append(dict(series_id=sid, metric=metric, entity=entity, entity_kind=entity_kind,
                         layer=layer, canonical_event=canonical or "", source_id=source_id,
                         source_type=next(s["type"] for s in SOURCES if s["source_id"] == source_id),
                         unit=unit, labels=labels,
                         default_detector=detector or (CANONICAL_EVENTS[canonical][1] if canonical else "")))

    # layer 1: workload, every pod
    for p in PODS:
        name, src = p[0], p[1]
        lbl = f'namespace="{next(s["namespace"] for s in SOURCES if s["source_id"] == src)}",pod="{name}"'
        add("container_cpu_usage_cores", name, 1, "cpu_usage", src, "pod", "cores", lbl)
        add("container_memory_working_set_bytes", name, 1, "memory_usage", src, "pod", "bytes", lbl)
        add("container_cpu_throttled_ratio", name, 1, "cpu_throttling", src, "pod", "ratio", lbl)
        add("kube_pod_container_status_restarts_total", name, 1, "restarts", src, "pod", "count", lbl)
        if p[6]:
            add("jvm_heap_used_ratio", name, 2, "jvm_heap", src, "pod", "ratio", lbl)
            add("jvm_gc_time_ms_per_sec", name, 2, "jvm_gc", src, "pod", "ms/s", lbl)

    K = "kafka-ingest"
    add("kafka_messages_in_per_sec", "orders", 2, "throughput", K, "topic", "msg/s", 'topic="orders"')
    add("kafka_messages_in_per_sec", "clicks", 2, "throughput", K, "topic", "msg/s", 'topic="clicks"')
    add("kafka_consumergroup_lag", "orders-etl", 2, "backlog", K, "consumer_group", "messages",
        'consumergroup="orders-etl",topic="orders"')
    add("kafka_produce_request_p99_ms", "kafka-ingest", 2, "latency", K, "cluster", "ms", 'cluster="ingest"')
    add("kafka_failed_produce_requests_per_sec", "kafka-ingest", 2, "error_rate", K, "cluster", "req/s",
        'cluster="ingest"')
    add("kafka_under_replicated_partitions", "kafka-ingest", 3, None, K, "cluster", "partitions",
        'cluster="ingest"', "guardrail")
    add("kafka_isr_shrinks_per_sec", "kafka-ingest", 3, None, K, "cluster", "events/s", 'cluster="ingest"',
        "seasonal_baseline")
    add("kafka_consumer_records_consumed_per_sec", "orders-etl", 3, None, K, "consumer_group", "msg/s",
        'consumergroup="orders-etl"', "seasonal_baseline")

    S = "spark-orders-stream"
    add("spark_streaming_input_rows_per_sec", "orders-stream", 2, "throughput", S, "spark_app", "rows/s",
        'app="orders-stream"')
    add("spark_streaming_scheduling_delay_sec", "orders-stream", 2, "backlog", S, "spark_app", "s",
        'app="orders-stream"')
    add("spark_streaming_batch_duration_sec", "orders-stream", 2, "run_duration", S, "spark_app", "s",
        'app="orders-stream"')

    F = "flink-clickstream"
    add("flink_records_out_per_sec", "clickstream-enrich", 2, "throughput", F, "flink_job", "records/s",
        'job="clickstream-enrich"')
    add("flink_backpressure_ratio", "clickstream-enrich", 2, "backlog", F, "flink_job", "ratio",
        'job="clickstream-enrich"')
    add("flink_last_checkpoint_duration_ms", "clickstream-enrich", 2, "run_duration", F, "flink_job", "ms",
        'job="clickstream-enrich"')
    add("flink_job_restarts_total", "clickstream-enrich", 2, "run_outcome", F, "flink_job", "count",
        'job="clickstream-enrich"', "guardrail")
    add("flink_checkpoint_failures_total", "clickstream-enrich", 3, None, F, "flink_job", "count",
        'job="clickstream-enrich"', "guardrail")

    T = "trino-lakehouse"
    add("trino_completed_queries_per_min", "trino-lakehouse", 2, "throughput", T, "cluster", "queries/min",
        'cluster="lakehouse"')
    add("trino_queued_queries", "trino-lakehouse", 2, "backlog", T, "cluster", "queries", 'cluster="lakehouse"')
    add("trino_query_p95_sec", "trino-lakehouse", 2, "latency", T, "cluster", "s", 'cluster="lakehouse"')
    add("trino_failed_queries_per_min", "trino-lakehouse", 2, "error_rate", T, "cluster", "queries/min",
        'cluster="lakehouse"')
    add("trino_resource_group_queued", "adhoc", 3, None, T, "resource_group", "queries",
        'resource_group="global.adhoc"', "saturation_forecast")

    A = "airflow-main"
    add("airflow_queued_tasks", "airflow-main", 2, "backlog", A, "cluster", "tasks", 'pool="default_pool"')
    add("airflow_task_completions_per_min", "airflow-main", 2, "throughput", A, "cluster", "tasks/min", "")
    add("airflow_scheduler_heartbeat_age_sec", "airflow-main", 3, None, A, "cluster", "s", "", "guardrail")
    return rows


# initial pod specs / app config (changes.jsonl modifies these over time)
BASE_IMAGES = {
    "orders-producer": "registry.local/orders-producer:2.2.4",
    "orders-etl-consumer-0": "registry.local/orders-etl-consumer:1.7.3",
    "orders-etl-consumer-1": "registry.local/orders-etl-consumer:1.7.3",
    "kafka-0": "quay.io/strimzi/kafka:0.45.0-kafka-3.9.0",
    "kafka-1": "quay.io/strimzi/kafka:0.45.0-kafka-3.9.0",
    "kafka-2": "quay.io/strimzi/kafka:0.45.0-kafka-3.9.0",
    "flink-jobmanager": "registry.local/clickstream-enrich:3.1.0",
    "flink-taskmanager-0": "registry.local/clickstream-enrich:3.1.0",
    "flink-taskmanager-1": "registry.local/clickstream-enrich:3.1.0",
    "trino-coordinator": "trinodb/trino:476",
    "trino-worker-0": "trinodb/trino:476",
    "trino-worker-1": "trinodb/trino:476",
    "airflow-scheduler": "apache/airflow:3.0.6",
    "airflow-worker-0": "apache/airflow:3.0.6",
}

SPARK_BASE_CONFIG = {
    "daily-orders": {"spark.executor.memory": "3g", "spark.executor.memoryOverhead": "1g",
                     "spark.executor.instances": 3, "spark.executor.cores": 2,
                     "spark.sql.shuffle.partitions": 200, "spark.sql.adaptive.enabled": False,
                     "spark.sql.adaptive.skewJoin.enabled": False},
    "customer-agg": {"spark.executor.memory": "3g", "spark.executor.memoryOverhead": "1g",
                     "spark.executor.instances": 3, "spark.executor.cores": 2,
                     "spark.sql.shuffle.partitions": 200, "spark.sql.adaptive.enabled": False,
                     "spark.sql.adaptive.skewJoin.enabled": False},
}

DAGS = {
    "daily_orders": {"schedule": "0 2 * * *", "tasks": ["wait_for_upstream", "spark_submit", "validate"],
                     "spark_job": "daily-orders", "sla_minutes": 90, "retries": 1},
    "customer_agg": {"schedule": "30 3 * * *", "tasks": ["spark_submit", "publish"],
                     "spark_job": "customer-agg", "sla_minutes": 60, "retries": 1},
    "hourly_quality_check": {"schedule": "5 * * * *", "tasks": ["run_checks"], "spark_job": None,
                             "sla_minutes": 20, "retries": 2},
}
