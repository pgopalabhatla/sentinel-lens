# live-week-1: the scripted live week

Written by `scripts/run_live_week.py --run-once` and not edited afterwards. Protocol: [docs/live-protocol.md](../live-protocol.md).

- code: commit `1f2b32740827f24db5c17e47f71df075126ce3a3`
- world: `data_live` (`--no-incidents --days 35 --seed 11`), stream 2026-10-26 to 2026-11-02 (UTC), engine warm-started on the 28 days before
- schedule seed: 1; topics: `live-week-1-*`

- telemetry messages published: 10425
- alerts written by the consumer: 1356; page events: 1385; page-outs: 29
- injections commanded: 14; started according to the answer key: 14; stopped: 14 (14 by duration)
- the same week with nothing injected: 4 page-outs (all false by construction)
- of the 4 false page-outs here, 4 are at the time of one of those 4; 0 are not

## Overall

| injections | detected | median time to detect (min) | max time to detect (min) | page-outs | matching an injection | FALSE | false pages per day |
|---|---|---|---|---|---|---|---|
| 14 | 12 | 12 | 132 | 29 | 25 | 4 | 0.57 |

Length of the run: 7 days.

## By scenario

| scenario | injected | detected | times to detect (min) |
|---|---|---|---|
| broker_gc_pressure | 2 | 2 | 12, 12 |
| consumer_memory_leak | 2 | 2 | 116, 132 |
| consumer_slow_deploy | 2 | 2 | 0, 0 |
| dag_paused | 2 | 2 | 52, 70 |
| flink_state_leak | 2 | 0 | - |
| producer_crashloop | 2 | 2 | 0, 0 |
| trino_heavy_query | 2 | 2 | 17, 12 |

## Per injection

| id | scenario | target | start (UTC) | stop (UTC) | parameters | detected | time to detect (min) | first paged by | matching page-outs | alerts near target |
|---|---|---|---|---|---|---|---|---|---|---|
| inj-01 | broker_gc_pressure | kafka-1 | Oct 26 02:50 | Oct 26 05:38 | duration_min=168, magnitude=0.519 | yes | 12 | kafka_under_replicated_partitions\|kafka-ingest (above_zero) | 1 | 198 |
| inj-02 | producer_crashloop | orders-producer | Oct 26 08:07 | Oct 26 08:57 | duration_min=50 | yes | 0 | kube_pod_container_status_restarts_total\|orders-producer (counter_rule) | 1 | 90 |
| inj-03 | dag_paused | hourly_quality_check | Oct 26 17:23 | Oct 27 02:42 | duration_min=559, lead_min=102 | yes | 52 | dag:hourly_quality_check (start_late) | 9 | 9 |
| inj-04 | flink_state_leak | clickstream-enrich | Oct 27 10:13 | Oct 27 16:12 | duration_min=359, rate_tm0_mib_per_hour=89.4, rate_tm1_mib_per_hour=65.2 | NO | - | - | 0 | 0 |
| inj-05 | trino_heavy_query | trino-lakehouse | Oct 27 18:57 | Oct 27 19:57 | duration_min=60, magnitude=0.817 | yes | 17 | jvm_gc_time_ms_per_sec\|trino-worker-0 (seasonal fallback) | 1 | 125 |
| inj-06 | consumer_slow_deploy | orders-etl | Oct 28 05:39 | Oct 28 07:55 | duration_min=136, lag_per_hour=482000, magnitude=0.642 | yes | 0 | kube_pod_container_status_restarts_total\|orders-etl-consumer-0 (counter_rule) | 2 | 100 |
| inj-07 | consumer_slow_deploy | orders-etl | Oct 28 20:09 | Oct 28 22:16 | duration_min=127, lag_per_hour=384000, magnitude=1.03 | yes | 0 | kube_pod_container_status_restarts_total\|orders-etl-consumer-0 (counter_rule) | 2 | 152 |
| inj-08 | flink_state_leak | clickstream-enrich | Oct 29 07:23 | Oct 29 13:04 | duration_min=341, rate_tm0_mib_per_hour=74.3, rate_tm1_mib_per_hour=99 | NO | - | - | 0 | 0 |
| inj-09 | broker_gc_pressure | kafka-2 | Oct 29 18:11 | Oct 29 21:08 | duration_min=177, magnitude=1.01 | yes | 12 | kafka_under_replicated_partitions\|kafka-ingest (above_zero) | 1 | 258 |
| inj-10 | consumer_memory_leak | orders-etl-consumer | Oct 30 09:13 | Oct 30 17:35 | duration_min=502, rate_pod0_mib_per_hour=61.6, rate_pod1_mib_per_hour=51.1 | yes | 116 | container_memory_working_set_bytes\|orders-etl-consumer-0 (seasonal fallback) | 2 | 153 |
| inj-11 | trino_heavy_query | trino-lakehouse | Oct 31 01:42 | Oct 31 02:12 | duration_min=30, magnitude=1.18 | yes | 12 | jvm_gc_time_ms_per_sec\|trino-worker-1 (seasonal fallback) | 2 | 58 |
| inj-12 | producer_crashloop | orders-producer | Oct 31 10:20 | Oct 31 10:57 | duration_min=37 | yes | 0 | kube_pod_container_status_restarts_total\|orders-producer (counter_rule) | 1 | 74 |
| inj-13 | dag_paused | customer_agg | Nov 01 02:30 | Nov 01 07:15 | duration_min=285, lead_min=60 | yes | 70 | dag:customer_agg (start_late) | 1 | 1 |
| inj-14 | consumer_memory_leak | orders-etl-consumer | Nov 01 11:02 | Nov 01 17:12 | duration_min=370, rate_pod0_mib_per_hour=64.9, rate_pod1_mib_per_hour=71.1 | yes | 132 | container_memory_working_set_bytes\|orders-etl-consumer-0 (seasonal fallback) | 2 | 102 |

## False page-outs

| time (UTC) | page | alerts in the page when it paged |
|---|---|---|
| Oct 28 21:29 | 20 | spark_streaming_scheduling_delay_sec\|orders-stream (seasonal fallback) |
| Oct 31 01:10 | 25 | dag:hourly_quality_check (job_run_baseline) |
| Nov 01 18:10 | 35 | dag:hourly_quality_check (job_run_baseline) |
| Nov 01 20:54 | 36 | spark_streaming_scheduling_delay_sec\|orders-stream (seasonal fallback) |

## The same week with nothing injected: page-outs

| time (UTC) | first alert of the page |
|---|---|
| Oct 28 21:29 | spark_streaming_scheduling_delay_sec\|orders-stream |
| Oct 31 01:10 | dag:hourly_quality_check |
| Nov 01 18:10 | dag:hourly_quality_check |
| Nov 01 20:54 | spark_streaming_scheduling_delay_sec\|orders-stream |

## Answer key, as written by the publisher

```json
{"entities": ["cluster:kafka-ingest", "pod:kafka-1"], "event": "started", "id": "inj-01", "params": {"duration_min": 168.0, "magnitude": 0.519}, "scenario": "broker_gc_pressure", "series": ["container_cpu_throttled_ratio|kafka-1", "container_cpu_usage_cores|kafka-1", "jvm_gc_time_ms_per_sec|kafka-1", "jvm_heap_used_ratio|kafka-1", "kafka_failed_produce_requests_per_sec|kafka-ingest", "kafka_produce_request_p99_ms|kafka-ingest", "kafka_under_replicated_partitions|kafka-ingest"], "start": "2026-10-26T02:50:00+00:00", "stop": "2026-10-26T05:38:00+00:00", "target": "kafka-1"}
{"event": "stopped", "id": "inj-01", "reason": "duration", "stop": "2026-10-26T05:38:00+00:00"}
{"entities": ["consumer_group:orders-etl", "pod:orders-etl-consumer-0", "pod:orders-etl-consumer-1", "pod:orders-producer", "spark_app:orders-stream", "topic:orders"], "event": "started", "id": "inj-02", "params": {"duration_min": 50.0}, "scenario": "producer_crashloop", "series": ["container_cpu_throttled_ratio|orders-etl-consumer-0", "container_cpu_throttled_ratio|orders-etl-consumer-1", "container_cpu_throttled_ratio|orders-producer", "container_cpu_usage_cores|orders-etl-consumer-0", "container_cpu_usage_cores|orders-etl-consumer-1", "container_cpu_usage_cores|orders-producer", "container_memory_working_set_bytes|orders-producer", "kafka_consumer_records_consumed_per_sec|orders-etl", "kafka_consumergroup_lag|orders-etl", "kafka_messages_in_per_sec|orders", "kube_pod_container_status_restarts_total|orders-producer", "spark_streaming_input_rows_per_sec|orders-stream"], "start": "2026-10-26T08:07:00+00:00", "stop": "2026-10-26T08:57:00+00:00", "target": "orders-producer"}
{"event": "stopped", "id": "inj-02", "reason": "duration", "stop": "2026-10-26T08:57:00+00:00"}
{"entities": ["dag:hourly_quality_check"], "event": "started", "id": "inj-03", "params": {"duration_min": 559.0, "lead_min": 102.0}, "scenario": "dag_paused", "series": [], "start": "2026-10-26T17:23:00+00:00", "stop": "2026-10-27T02:42:00+00:00", "target": "hourly_quality_check"}
{"event": "stopped", "id": "inj-03", "reason": "duration", "stop": "2026-10-27T02:42:00+00:00"}
{"entities": ["pod:flink-taskmanager-0", "pod:flink-taskmanager-1"], "event": "started", "id": "inj-04", "params": {"duration_min": 359.0, "rate_tm0_mib_per_hour": 89.4, "rate_tm1_mib_per_hour": 65.2}, "scenario": "flink_state_leak", "series": ["container_memory_working_set_bytes|flink-taskmanager-0", "container_memory_working_set_bytes|flink-taskmanager-1"], "start": "2026-10-27T10:13:00+00:00", "stop": "2026-10-27T16:12:00+00:00", "target": "clickstream-enrich"}
{"event": "stopped", "id": "inj-04", "reason": "duration", "stop": "2026-10-27T16:12:00+00:00"}
{"entities": ["cluster:trino-lakehouse", "pod:trino-worker-0", "pod:trino-worker-1", "resource_group:adhoc"], "event": "started", "id": "inj-05", "params": {"duration_min": 60.0, "magnitude": 0.817}, "scenario": "trino_heavy_query", "series": ["container_cpu_throttled_ratio|trino-worker-0", "container_cpu_throttled_ratio|trino-worker-1", "container_cpu_usage_cores|trino-worker-0", "container_cpu_usage_cores|trino-worker-1", "jvm_gc_time_ms_per_sec|trino-worker-0", "jvm_gc_time_ms_per_sec|trino-worker-1", "jvm_heap_used_ratio|trino-worker-0", "jvm_heap_used_ratio|trino-worker-1", "trino_completed_queries_per_min|trino-lakehouse", "trino_failed_queries_per_min|trino-lakehouse", "trino_query_p95_sec|trino-lakehouse", "trino_queued_queries|trino-lakehouse", "trino_resource_group_queued|adhoc"], "start": "2026-10-27T18:57:00+00:00", "stop": "2026-10-27T19:57:00+00:00", "target": "trino-lakehouse"}
{"event": "stopped", "id": "inj-05", "reason": "duration", "stop": "2026-10-27T19:57:00+00:00"}
{"entities": ["consumer_group:orders-etl", "pod:orders-etl-consumer-0", "pod:orders-etl-consumer-1"], "event": "started", "id": "inj-06", "params": {"duration_min": 136.0, "lag_per_hour": 482000.0, "magnitude": 0.642}, "scenario": "consumer_slow_deploy", "series": ["container_cpu_throttled_ratio|orders-etl-consumer-0", "container_cpu_throttled_ratio|orders-etl-consumer-1", "container_cpu_usage_cores|orders-etl-consumer-0", "container_cpu_usage_cores|orders-etl-consumer-1", "kafka_consumer_records_consumed_per_sec|orders-etl", "kafka_consumergroup_lag|orders-etl", "kube_pod_container_status_restarts_total|orders-etl-consumer-0", "kube_pod_container_status_restarts_total|orders-etl-consumer-1"], "start": "2026-10-28T05:39:00+00:00", "stop": "2026-10-28T07:55:00+00:00", "target": "orders-etl"}
{"event": "stopped", "id": "inj-06", "reason": "duration", "stop": "2026-10-28T07:55:00+00:00"}
{"entities": ["consumer_group:orders-etl", "pod:orders-etl-consumer-0", "pod:orders-etl-consumer-1"], "event": "started", "id": "inj-07", "params": {"duration_min": 127.0, "lag_per_hour": 384000.0, "magnitude": 1.03}, "scenario": "consumer_slow_deploy", "series": ["container_cpu_throttled_ratio|orders-etl-consumer-0", "container_cpu_throttled_ratio|orders-etl-consumer-1", "container_cpu_usage_cores|orders-etl-consumer-0", "container_cpu_usage_cores|orders-etl-consumer-1", "kafka_consumer_records_consumed_per_sec|orders-etl", "kafka_consumergroup_lag|orders-etl", "kube_pod_container_status_restarts_total|orders-etl-consumer-0", "kube_pod_container_status_restarts_total|orders-etl-consumer-1"], "start": "2026-10-28T20:09:00+00:00", "stop": "2026-10-28T22:16:00+00:00", "target": "orders-etl"}
{"event": "stopped", "id": "inj-07", "reason": "duration", "stop": "2026-10-28T22:16:00+00:00"}
{"entities": ["pod:flink-taskmanager-0", "pod:flink-taskmanager-1"], "event": "started", "id": "inj-08", "params": {"duration_min": 341.0, "rate_tm0_mib_per_hour": 74.3, "rate_tm1_mib_per_hour": 99.0}, "scenario": "flink_state_leak", "series": ["container_memory_working_set_bytes|flink-taskmanager-0", "container_memory_working_set_bytes|flink-taskmanager-1"], "start": "2026-10-29T07:23:00+00:00", "stop": "2026-10-29T13:04:00+00:00", "target": "clickstream-enrich"}
{"event": "stopped", "id": "inj-08", "reason": "duration", "stop": "2026-10-29T13:04:00+00:00"}
{"entities": ["cluster:kafka-ingest", "pod:kafka-2"], "event": "started", "id": "inj-09", "params": {"duration_min": 177.0, "magnitude": 1.01}, "scenario": "broker_gc_pressure", "series": ["container_cpu_throttled_ratio|kafka-2", "container_cpu_usage_cores|kafka-2", "jvm_gc_time_ms_per_sec|kafka-2", "jvm_heap_used_ratio|kafka-2", "kafka_failed_produce_requests_per_sec|kafka-ingest", "kafka_produce_request_p99_ms|kafka-ingest", "kafka_under_replicated_partitions|kafka-ingest"], "start": "2026-10-29T18:11:00+00:00", "stop": "2026-10-29T21:08:00+00:00", "target": "kafka-2"}
{"event": "stopped", "id": "inj-09", "reason": "duration", "stop": "2026-10-29T21:08:00+00:00"}
{"entities": ["pod:orders-etl-consumer-0", "pod:orders-etl-consumer-1"], "event": "started", "id": "inj-10", "params": {"duration_min": 502.0, "rate_pod0_mib_per_hour": 61.6, "rate_pod1_mib_per_hour": 51.1}, "scenario": "consumer_memory_leak", "series": ["container_memory_working_set_bytes|orders-etl-consumer-0", "container_memory_working_set_bytes|orders-etl-consumer-1"], "start": "2026-10-30T09:13:00+00:00", "stop": "2026-10-30T17:35:00+00:00", "target": "orders-etl-consumer"}
{"event": "stopped", "id": "inj-10", "reason": "duration", "stop": "2026-10-30T17:35:00+00:00"}
{"entities": ["cluster:trino-lakehouse", "pod:trino-worker-0", "pod:trino-worker-1", "resource_group:adhoc"], "event": "started", "id": "inj-11", "params": {"duration_min": 30.0, "magnitude": 1.18}, "scenario": "trino_heavy_query", "series": ["container_cpu_throttled_ratio|trino-worker-0", "container_cpu_throttled_ratio|trino-worker-1", "container_cpu_usage_cores|trino-worker-0", "container_cpu_usage_cores|trino-worker-1", "jvm_gc_time_ms_per_sec|trino-worker-0", "jvm_gc_time_ms_per_sec|trino-worker-1", "jvm_heap_used_ratio|trino-worker-0", "jvm_heap_used_ratio|trino-worker-1", "trino_completed_queries_per_min|trino-lakehouse", "trino_failed_queries_per_min|trino-lakehouse", "trino_query_p95_sec|trino-lakehouse", "trino_queued_queries|trino-lakehouse", "trino_resource_group_queued|adhoc"], "start": "2026-10-31T01:42:00+00:00", "stop": "2026-10-31T02:12:00+00:00", "target": "trino-lakehouse"}
{"event": "stopped", "id": "inj-11", "reason": "duration", "stop": "2026-10-31T02:12:00+00:00"}
{"entities": ["consumer_group:orders-etl", "pod:orders-etl-consumer-0", "pod:orders-etl-consumer-1", "pod:orders-producer", "spark_app:orders-stream", "topic:orders"], "event": "started", "id": "inj-12", "params": {"duration_min": 37.0}, "scenario": "producer_crashloop", "series": ["container_cpu_throttled_ratio|orders-etl-consumer-0", "container_cpu_throttled_ratio|orders-etl-consumer-1", "container_cpu_throttled_ratio|orders-producer", "container_cpu_usage_cores|orders-etl-consumer-0", "container_cpu_usage_cores|orders-etl-consumer-1", "container_cpu_usage_cores|orders-producer", "container_memory_working_set_bytes|orders-producer", "kafka_consumer_records_consumed_per_sec|orders-etl", "kafka_consumergroup_lag|orders-etl", "kafka_messages_in_per_sec|orders", "kube_pod_container_status_restarts_total|orders-producer", "spark_streaming_input_rows_per_sec|orders-stream"], "start": "2026-10-31T10:20:00+00:00", "stop": "2026-10-31T10:57:00+00:00", "target": "orders-producer"}
{"event": "stopped", "id": "inj-12", "reason": "duration", "stop": "2026-10-31T10:57:00+00:00"}
{"entities": ["dag:customer_agg", "pod:customer-agg-driver", "pod:customer-agg-exec-1", "pod:customer-agg-exec-2", "pod:customer-agg-exec-3"], "event": "started", "id": "inj-13", "params": {"duration_min": 285.0, "lead_min": 60.0}, "scenario": "dag_paused", "series": ["container_cpu_throttled_ratio|customer-agg-driver", "container_cpu_throttled_ratio|customer-agg-exec-1", "container_cpu_throttled_ratio|customer-agg-exec-2", "container_cpu_throttled_ratio|customer-agg-exec-3", "container_cpu_usage_cores|customer-agg-driver", "container_cpu_usage_cores|customer-agg-exec-1", "container_cpu_usage_cores|customer-agg-exec-2", "container_cpu_usage_cores|customer-agg-exec-3", "container_memory_working_set_bytes|customer-agg-driver", "container_memory_working_set_bytes|customer-agg-exec-1", "container_memory_working_set_bytes|customer-agg-exec-2", "container_memory_working_set_bytes|customer-agg-exec-3", "jvm_gc_time_ms_per_sec|customer-agg-driver", "jvm_gc_time_ms_per_sec|customer-agg-exec-1", "jvm_gc_time_ms_per_sec|customer-agg-exec-2", "jvm_gc_time_ms_per_sec|customer-agg-exec-3", "jvm_heap_used_ratio|customer-agg-driver", "jvm_heap_used_ratio|customer-agg-exec-1", "jvm_heap_used_ratio|customer-agg-exec-2", "jvm_heap_used_ratio|customer-agg-exec-3", "kube_pod_container_status_restarts_total|customer-agg-driver", "kube_pod_container_status_restarts_total|customer-agg-exec-1", "kube_pod_container_status_restarts_total|customer-agg-exec-2", "kube_pod_container_status_restarts_total|customer-agg-exec-3"], "start": "2026-11-01T02:30:00+00:00", "stop": "2026-11-01T07:15:00+00:00", "target": "customer_agg"}
{"event": "stopped", "id": "inj-13", "reason": "duration", "stop": "2026-11-01T07:15:00+00:00"}
{"entities": ["pod:orders-etl-consumer-0", "pod:orders-etl-consumer-1"], "event": "started", "id": "inj-14", "params": {"duration_min": 370.0, "rate_pod0_mib_per_hour": 64.9, "rate_pod1_mib_per_hour": 71.1}, "scenario": "consumer_memory_leak", "series": ["container_memory_working_set_bytes|orders-etl-consumer-0", "container_memory_working_set_bytes|orders-etl-consumer-1"], "start": "2026-11-01T11:02:00+00:00", "stop": "2026-11-01T17:12:00+00:00", "target": "orders-etl-consumer"}
{"event": "stopped", "id": "inj-14", "reason": "duration", "stop": "2026-11-01T17:12:00+00:00"}
{"end": "2026-11-02T00:00:00+00:00", "event": "published", "start": "2026-10-26T00:00:00+00:00"}
```
