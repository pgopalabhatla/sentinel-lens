# Sentinel Lens synthetic platform data

Simulated inputs for building Sentinel Lens's anomaly detection and RCA agent **without running
Kafka, Spark, Airflow, Flink, Trino, Prometheus or Kubernetes**. It is 28 days of 1-minute telemetry
from a simulated data platform, plus everything the agent's tools would return (logs, k8s events,
pod specs, Airflow DAG runs, Spark History Server details, change log) and ground-truth labels for
23 injected incidents.

The data is generated from causes, not painted on. A scenario sets a cause (for example lowering
`spark.sql.shuffle.partitions`, deploying a slow consumer, or pausing a DAG). Shared simulation code
then turns that cause into consistent symptoms across metrics, logs, events and job records, the
same way a real platform would.

## Quick start

```bash
pip install pandas numpy scipy pyyaml matplotlib   # Python 3.11+
python examples/quickstart.py                       # replay API tour + naive detector scored on test
python -m sentinel_sim.generate --out data --seed 7 # regenerate (about 25 s, deterministic)
```

```python
from sentinel_sim.replay import DataSource
ds = DataSource("data").at("2026-10-09T15:00:00Z")   # the clock: nothing after this is visible
ds.series(layer=2, canonical_event="backlog")       # catalog lookup
ds.query("kafka_consumergroup_lag|orders-etl", window="3h")
ds.query_metric("kafka_consumergroup_lag|orders-etl", window="3h")   # compact summary for the LLM
ds.logs(pod="orders-etl-consumer-0", window="15min", level="WARN")
ds.changes(window="6h")
```

## Timeline and splits

| Period | Dates (UTC) | Use |
| --- | --- | --- |
| Warm-up | Sep 28 - Oct 4 (days 0-6) | No incidents. Detectors build baselines here. |
| Train | Oct 5 - Oct 18 (days 7-20) | 12 incidents. Tune detectors, write signatures, seed agent memory with feedback. |
| Test | Oct 19 - Oct 25 (days 21-27) | 11 incidents: new variants of train scenarios plus 2 types never seen in train. |

**Keep the test set honest.** Don't read the test entries in `labels/incidents.json` until you
run the final evaluation. The novel test types measure whether the LLM agent adds value beyond
signatures and memory.

Traffic has realistic daily and weekly seasonality: orders peak around 19:00 UTC, weekends run at
about 70%, and Trino load follows Pacific business hours. The noise is deliberate:
- 40 single-sample outliers (`labels/noise_blips.csv`) test your persistence checks.
- Batch Spark executors normally run near their CPU limit, so throttling during batch runs is normal.
- Hourly Airflow checks occasionally fail once and succeed on retry.
- Trino readiness probes sometimes fail.
- Six harmless changes act as red herrings for RCA.

## The simulated platform

| Source | What runs | Notes |
| --- | --- | --- |
| `kafka-ingest` | 3 Strimzi brokers, topics `orders` and `clicks` (6 partitions each) | |
| `orders-apps` | `orders-producer`, `orders-etl-consumer-0/1` | Each consumer replica handles about 1,100 msg/s |
| `spark-orders-stream` | Structured Streaming app, driver + 2 executors | |
| `spark-batch` | `daily-orders` (02:00) and `customer-agg` (03:30), submitted by Airflow | Pods exist only while a run is active (NaN otherwise) |
| `flink-clickstream` | Job `clickstream-enrich`, JobManager + 2 TaskManagers | Writes to MinIO |
| `trino-lakehouse` | Coordinator + 2 workers | |
| `airflow-main` | Scheduler + worker. DAGs: `daily_orders`, `customer_agg`, `hourly_quality_check` | |

`catalog/topology.json` holds the sources, pods with their limits, the entity relationship graph
(for the correlator), DAG schedules and SLAs, and the base Spark config.

## Files

| Path | What it is | The real system it replaces |
| --- | --- | --- |
| `metrics/metrics.csv.gz` | Wide table: `ts` + 164 series, 1-minute steps, 40,320 rows | Prometheus |
| `catalog/series.csv` | One row per series: `series_id`, metric, entity, entity_kind, **layer**, **canonical_event**, source, unit, PromQL labels, default detector | Source packs / discovery |
| `catalog/canonical_events.yaml` | The 13 canonical events (layers 1-2), default detector and healthy direction | Design doc v2 |
| `catalog/mappings/<type>.yaml` | Per-product mapping of metrics to canonical events, as PromQL | Mapping files |
| `logs/logs.jsonl.gz` | 274k lines: `ts, pod, level, logger, message` | Loki / `kubectl logs` |
| `k8s/events.jsonl` | Scheduled, Started, Killing, BackOff, Unhealthy, OOMKilled | Kubernetes API |
| `changes/changes.jsonl` | Deploys, config changes, scaling, DAG pause/unpause, node drains, annotations | Deploy log / GitOps |
| `jobs/dag_runs.jsonl` | Airflow runs with task instances (tries, states, times, Spark app ids) | Airflow REST API |
| `jobs/spark_apps.jsonl` | Spark apps: config, input rows, stages with task-time stats and spill, executors with peak memory and exit codes, error | Spark History Server |
| `jobs/job_runs.csv` | One flat row per DAG run: duration, lateness, state, SLA miss, input rows | Convenience |
| `labels/incidents.json` | Ground truth per incident (see below) | Evaluation only |
| `labels/anomaly_windows.csv` | Per-series anomaly windows for each incident | Evaluation only |

### Series ids and layers

`series_id` = `<metric>|<entity>`, for example `container_memory_working_set_bytes|kafka-1` or
`kafka_consumergroup_lag|orders-etl`.

- **Layer 1 (workload)**, every pod: CPU cores, working-set memory bytes, CPU throttled ratio,
  restart counter.
- **Layer 2 (canonical)**: throughput, backlog, latency, error_rate, jvm_heap, jvm_gc,
  run_duration, run_outcome. `run_outcome` and `run_lateness` for batch jobs come from
  `job_runs`, not from series.
- **Layer 3 (product-specific)**: Kafka under-replicated partitions and ISR shrinks, consumer
  consumed rate, Flink checkpoint failures, Trino resource group queue, Airflow scheduler
  heartbeat age.

Counters (`*_total`) are cumulative; detect on increases. Rates are already per-second or
per-minute, as the unit says.

## Replay API (`sentinel_sim/replay.py`)

Method names match the RCA agent's tools in the design doc. Every call is capped at `now`, so a
detector replaying history or an agent investigating at time T can never see the future.

| Method | Agent tool / use |
| --- | --- |
| `query(series, start, end, window, step)` | Raw points for detectors |
| `query_metric(series_id, window)` | `query_metric`: summary stats, slope, same window yesterday |
| `stream(start, end, step)` | Minute-by-minute replay for online detection |
| `logs(pod, window, level, contains)` | `get_log_templates` input (run Drain3 on these) |
| `k8s_events(pod, window, reason, type_)` | `get_k8s_events` |
| `pod_spec(pod, at)` | `get_pod_spec`: limits, image and env after changes, Spark config, restarts |
| `spark_app(app_id)` | `get_spark_app` |
| `job_runs(dag_id, window)` / `job_history(dag_id, n)` | `get_job_history` |
| `expected_runs(window)` | Schedule watcher: when each DAG should have run |
| `kafka_group(group)` | `get_kafka_group` |
| `changes(window, target)` | Recent deploys and config changes |
| `series(**filters)`, `relationships()`, `pods()`, `sources()` | Catalog and entity graph |

Later, to run on a real cluster, implement the same methods against Prometheus, the Kubernetes
API, Loki, Airflow and the Spark History Server. Detectors and the agent don't change.

## Ground truth (`labels/incidents.json`)

Each incident has: `incident_id`, `scenario`, `variant`, `split`, `start` / `end`, `actionable`
(false for expected bursts and planned maintenance, where alerting is fine but should be learnable
as "expected"), `severity`, `root_cause_category`, `root_cause` (the reference explanation),
`trigger_change_id`, `affected_entities`, `key_signals` (series + window + direction),
`dag_run_ids` / `spark_app_ids`, `expected_evidence` (what a good RCA should cite),
`suggested_fix`, and `novel_in_test`.

### Train incidents (Oct 5 - 18)

| Id | Scenario | What happened |
| --- | --- | --- |
| INC-001 | spark_executor_oom | Executor memory lowered 3g to 2g; `daily_orders` fails with JVM heap OOM (exit 52) |
| INC-002 | traffic_burst (expected) | Annotated marketing campaign, orders 4.5x, lag drains on its own |
| INC-003 | kafka_consumer_lag | Consumer deploy adds a synchronous API call; lag grows until rollback |
| INC-004 | spark_data_skew | `customer_agg` 2.6x slower; one straggler task, spill on exec-3 |
| INC-005 | dag_late_or_missing | `daily_orders` paused overnight, 02:00 run skipped |
| INC-006 | crashloop_bad_config | Producer deploy with a typo in the bootstrap address; ingestion stops |
| INC-007 | memory_leak | Consumer cache without eviction; sawtooth memory and OOMKilled |
| INC-008 | broker_restart (expected) | Node drain for patching; URP for about 10 minutes |
| INC-009 | trino_saturation | Accidental cross join by an analyst fills worker memory |
| INC-010 | spark_executor_oom | Input rows 2.4x normal; container OOMKilled (exit 137) |
| INC-011 | spark_data_skew | `customer_agg` skew again |
| INC-012 | flink_checkpoint_failures | MinIO write timeouts; checkpoints expire, job fails over |

The test week has 11 incidents: variants of the train scenarios with different causes, products or
signals, plus 2 novel types.

## Scoring (`sentinel_sim/evaluate.py`)

```python
from sentinel_sim.evaluate import evaluate_detections, evaluate_rca
res = evaluate_detections(detections_df, "data", split="test")   # columns: ts, target
res["recall"], res["precision"], res["median_ttd_min"], res["per_incident"]
evaluate_rca({"INC-013": {"root_cause_category": "memory_leak", "trigger_change_id": "chg-00NN"}}, "data")
```

- An incident counts as detected if any alert hits one of its key-signal series or affected
  entities between its start (minus 5 minutes) and its end.
- Alerts are deduplicated per target within 30 minutes. Each is then classed as `true`, `expected`
  (fired on a non-actionable incident) or `false_positive`.
- `target` can be a `series_id` or an entity such as `dag:daily_orders` (for job alerts).
- RCA scoring is exact match on category and triggering change. Grade free-text explanations
  against `root_cause` by hand or with an LLM judge.

**Baseline to beat:** `examples/quickstart.py` runs a naive detector (robust z-score vs the same
time on the previous 7 days, 3-of-5 persistence, any counter increase, failed or late runs). On
test it scores **recall 1.0, precision 0.26** (641 false positives), median time-to-detect 10 min,
and over 3 hours on the slow memory leak. Raising precision without losing recall, and detecting
slow problems sooner, is the detection half of the project.

## Simplifications to know

- Batch Spark pods use stable names (`daily-orders-exec-2`) instead of per-run names; a restart of
  that name stands for "executor replaced".
- `OOMKilled` appears as an event row. On a real cluster it comes from the container's terminated
  state, which a pod watcher would turn into an event.
- `kafka_group()` splits total lag across partitions using fixed weights.
- Logs are realistic in form but sparse (about 1 line per 2 minutes per pod outside incidents).
