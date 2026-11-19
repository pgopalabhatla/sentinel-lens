# live-week-2: frozen detector, triage agent and attacker on a new world

Written by `scripts/run_live_week_2.py --run-once` and not edited afterwards. Protocol: [docs/live-week-2-protocol.md](../live-week-2-protocol.md).

- code: commit `bc1619eb0b5cd9e9d61134d5a12d8d4051a12480`; protocol last committed in `bc1619e`
- frozen: `pre-test-freeze` (770fab8), `triage-freeze-v1` (6640b14), `attacker-freeze-v2` (500558b); every frozen file identical to its tag
- world: `python -m sentinel_stream.quiet_world --days 35 --seed 13 --out data_live_2`, 18 files, combined SHA-256 `08a352e00fe1a45bd2825c3ca821e8adec864b645a8d38db4714e51cebe230c5`; stream 2026-10-26 to 2026-11-02 (UTC), version 2; engine warm-started on the 28 days before
- topics: `live-week-2-*`; telemetry messages 20,569; alerts 841; page-outs 13
- attempts: 2026-11-19 18:57 UTC: first attempt
- **total cost: USD 4.54** (campaign 2.61 + triage 0.81 + 0.59 + 0.53)

## The attacker campaign

Frozen attacker (`attacker-freeze-v2`): `claude-sonnet-5-5`, effort `medium`, woken every 60 simulated minutes (168 wakes). Budget in code: at most 3 a day, 2 active, starts 3 h apart, named scenarios at most 60% of the campaign's injections. Recorded once; control stream: [live-week-2.control.jsonl](live-week-2.control.jsonl).

- injections: **11**; named 6, generic 5; noticeable 5, subtle 6
- the attacker's own rejections: 1 (partly accepted 1)
- cost: USD 2.608 (estimate); tokens {'input': 1100281, 'output': 40718, 'cache_read': 0, 'cache_write': 0}
- tool calls: 49 (describe_normal 21, list_active 6, list_scenarios 6, inject_scenario 6, list_platform 5, inject_effects 5)

Injections per simulated day (by start):

| day | injections |
|---|---|
| Mon Oct 26 | 2 |
| Tue Oct 27 | 2 |
| Wed Oct 28 | 2 |
| Thu Oct 29 | 2 |
| Fri Oct 30 | 1 |
| Sat Oct 31 | 1 |
| Sun Nov 01 | 1 |

| id | start (UTC) | stop | kind | scenario / effects | severity | started on the stream | intent | novelty |
|---|---|---|---|---|---|---|---|---|
| atk-001 | 10-26T00:00 | 10-26T01:50 | generic | 6 effects on cluster:kafka-ingest, consumer_group:orders-etl, pod:kafka-1 | subtle | yes | A degrading disk/slow follower on kafka-1 causes slow log flushes, replicas dropping out of ISR, slower produce requests and growing consumer lag. | Partial single-broker storage degradation with ISR flapping; not a named broker crash, CPU saturation or consumer-stall scenario. |
| atk-002 | 10-26T04:00 | 10-26T04:40 | scenario | trino_heavy_query on trino-lakehouse | noticeable | yes | An analyst runs a huge unbounded ad-hoc cross join off-hours that saturates both Trino workers; other queries queue and some fail. | - |
| atk-003 | 10-27T00:10 | 10-27T02:10 | generic | 7 effects on flink_job:clickstream-enrich, pod:flink-taskmanager-0 | subtle | yes | A state/heap leak in a Flink taskmanager (unbounded keyed state in the enrichment join) drives heap up and GC thrash, causing backpressure, slow checkpoints and eventually checkpoint timeouts and reduced throughput. | Gradual JVM heap/state leak on one Flink taskmanager with GC thrash cascading to backpressure and checkpoint failures; not a named scenario of broker, Trino, or pod-restart type. |
| atk-004 | 10-27T14:00 | 10-27T19:00 | scenario | dag_paused on customer_agg | subtle | yes | Someone pauses the customer_agg DAG during maintenance and forgets it; scheduled Spark runs do not happen and the executors stay idle. | - |
| atk-005 | 10-28T01:00 | 10-28T03:00 | scenario | broker_gc_pressure on kafka-2 | subtle | yes | A client with huge fetch batches fills kafka-2's heap: long GC pauses, slower produce, and repeated ISR drops. | - |
| atk-006 | 10-28T13:05 | 10-28T14:55 | generic | 5 effects on cluster:airflow-main, pod:airflow-scheduler | noticeable | yes | Airflow scheduler degrades from a bloated metadata DB / DAG-parse storm: loop saturates CPU, heartbeat goes stale, tasks pile up in queued state. | Scheduler resource saturation and stale heartbeat without a pause or outage; not covered by DAG-pause scenario. |
| atk-007 | 10-29T00:00 | 10-29T02:30 | scenario | consumer_slow_deploy on orders-etl | noticeable | yes | A new orders-etl-consumer release is slower per record than the produce rate; lag builds until rollback, then drains. | - |
| atk-008 | 10-29T17:00 | 10-29T19:30 | generic | 7 effects on pod:orders-stream-exec-2, spark_app:orders-stream | subtle | yes | A Spark Structured Streaming executor (orders-stream-exec-2) slowly leaks memory (unbounded state/broadcast cache); heap and GC climb, batches slow, scheduling delay builds, and the pod nears its memory limit and gets OOMKilled. | Gradual executor memory leak ending in OOMKill on a streaming Spark app; named scenarios cover broker GC, Flink heap leak, paused DAGs, etc., not a Spark streaming executor leak with downstream batch delay. |
| atk-009 | 10-30T00:10 | 10-30T00:45 | scenario | producer_crashloop on orders-producer | noticeable | yes | A bad config rollout makes orders-producer crash on startup; it loops in restarts and no orders arrive until fixed. | - |
| atk-010 | 10-31T02:10 | 10-31T09:10 | scenario | flink_state_leak on clickstream-enrich | subtle | yes | A Flink job update keeps keyed state forever, so both TaskManagers' memory creeps up slowly over hours. | - |
| atk-011 | 11-01T00:00 | 11-01T02:00 | generic | 9 effects on cluster:trino-lakehouse, pod:trino-worker-0, resource_group:adhoc | noticeable | yes | A runaway ad-hoc cross-join query lands on trino-worker-0, eating heap and CPU; GC thrashes, other queries slow and queue in the adhoc resource group, and some fail on memory limits. | Trino memory-hungry query skew on a single worker with resource-group queueing; not covered by named scenarios, which target Kafka, Spark, Airflow and Flink. |

Generic incidents: series shared with each named scenario's series, `shared / the incident's (Jaccard)` (reported, not enforced):

| incident | consumer_memory_leak | flink_state_leak | consumer_slow_deploy | broker_gc_pressure | trino_heavy_query | producer_crashloop | dag_paused |
|---|---|---|---|---|---|---|---|
| atk-001 | 0/6 (0) | 0/6 (0) | 1/6 (0.091) | 4/6 (0.286) | 0/6 (0) | 1/6 (0.071) | 0/6 (0) |
| atk-003 | 0/7 (0) | 0/7 (0) | 0/7 (0) | 0/7 (0) | 0/7 (0) | 0/7 (0) | 0/7 (0) |
| atk-006 | 0/5 (0) | 0/5 (0) | 0/5 (0) | 0/5 (0) | 0/5 (0) | 0/5 (0) | 0/5 (0) |
| atk-008 | 0/7 (0) | 0/7 (0) | 0/7 (0) | 0/7 (0) | 0/7 (0) | 1/7 (0.067) | 0/7 (0) |
| atk-011 | 0/9 (0) | 0/9 (0) | 0/9 (0) | 0/9 (0) | 8/9 (0.667) | 0/9 (0) | 0/9 (0) |

## Detection

- inject commands in the recording: 11; started on the stream (counted): 11; refused entirely by the publisher (not counted): 0; started with some effects refused: 0
- false page-outs: **2** in 7 days = **0.29 a day** (exact Poisson 95%: 0.03-1.03)
- the same 7 days with nothing injected: 2 page-outs; of the 2 false page-outs here, 2 are at the time of one of those

| injections | n | detected | Wilson 95% | median time to detect (min) | max (min) |
|---|---|---|---|---|---|
| all | 11 | 9 | 0.52-0.95 | 15 | 49 |
| named | 6 | 4 | 0.30-0.90 | 6 | 14 |
| generic | 5 | 5 | 0.57-1.00 | 19 | 49 |
| noticeable | 5 | 5 | 0.57-1.00 | 14 | 19 |
| subtle | 6 | 4 | 0.30-0.90 | 20 | 49 |
| named x noticeable | 3 | 3 | 0.44-1.00 | 0 | 14 |
| named x subtle | 3 | 1 | 0.06-0.79 | 12 | 12 |
| generic x noticeable | 2 | 2 | 0.34-1.00 | 19 | 19 |
| generic x subtle | 3 | 3 | 0.44-1.00 | 24 | 49 |

### The scorer's tables (`sentinel_stream/live_score.py`)

## Overall

| injections | detected | median time to detect (min) | max time to detect (min) | page-outs | matching an injection | FALSE | false pages per day |
|---|---|---|---|---|---|---|---|
| 11 | 9 | 15 | 49 | 13 | 11 | 2 | 0.29 |

Length of the run: 7 days.

## By scenario

| scenario | injected | detected | times to detect (min) |
|---|---|---|---|
| broker_gc_pressure | 1 | 1 | 12 |
| consumer_slow_deploy | 1 | 1 | 0 |
| custom | 5 | 5 | 15, 24, 19, 49, 19 |
| dag_paused | 1 | 0 | - |
| flink_state_leak | 1 | 0 | - |
| producer_crashloop | 1 | 1 | 0 |
| trino_heavy_query | 1 | 1 | 14 |

## By severity (as given by whoever injected; reported, not used for matching)

| severity | injected | detected | times to detect (min) |
|---|---|---|---|
| noticeable | 5 | 5 | 14, 19, 0, 0, 19 |
| subtle | 6 | 4 | 15, 24, 12, 49 |

## Per injection

| id | scenario | target | start (UTC) | stop (UTC) | parameters | detected | time to detect (min) | first paged by | matching page-outs | alerts near target | severity | intent |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| atk-001 | custom | - | Oct 26 00:00 | Oct 26 01:50 | mult ramp 3.5 on kafka_produce_request_p99_ms\|kafka-ingest for 90 min; add flat 0.15 on kafka_isr_shrinks_per_sec\|kafka-ingest for 80 min; add flat 2 on kafka_under_replicated_partitions\|kafka-ingest for 75 min; add ramp 0.1 on kafka_failed_produce_requests_per_sec\|kafka-ingest for 60 min; mult ramp 2 on jvm_gc_time_ms_per_sec\|kafka-1 for 90 min; add ramp 600 on kafka_consumergroup_lag\|orders-etl for 90 min | yes | 15 | kafka_under_replicated_partitions\|kafka-ingest (above_zero) | 2 | 142 | subtle | A degrading disk/slow follower on kafka-1 causes slow log flushes, replicas dropping out of ISR, slower produce requests and growing consumer lag. |
| atk-002 | trino_heavy_query | trino-lakehouse | Oct 26 04:00 | Oct 26 04:40 | duration_min=40, magnitude=0.8 | yes | 14 | container_cpu_usage_cores\|trino-worker-0 (seasonal) | 1 | 66 | noticeable | An analyst runs a huge unbounded ad-hoc cross join off-hours that saturates both Trino workers; other queries queue and some fail. |
| atk-003 | custom | - | Oct 27 00:10 | Oct 27 02:10 | add ramp 0.33 on jvm_heap_used_ratio\|flink-taskmanager-0 for 120 min; add ramp 220 on jvm_gc_time_ms_per_sec\|flink-taskmanager-0 for 120 min; add ramp 0.5 on container_cpu_usage_cores\|flink-taskmanager-0 for 120 min; add ramp 0.4 on flink_backpressure_ratio\|clickstream-enrich for 120 min; add ramp 14000 on flink_last_checkpoint_duration_ms\|clickstream-enrich for 120 min; mult ramp 0.6 on flink_records_out_per_sec\|clickstream-enrich for 120 min; add ramp 4 on flink_checkpoint_failures_total\|clickstream-enrich for 60 min | yes | 24 | jvm_gc_time_ms_per_sec\|flink-taskmanager-0 (seasonal fallback) | 1 | 91 | subtle | A state/heap leak in a Flink taskmanager (unbounded keyed state in the enrichment join) drives heap up and GC thrash, causing backpressure, slow checkpoints and eventually checkpoint timeouts and reduced throughput. |
| atk-004 | dag_paused | customer_agg | Oct 27 14:00 | Oct 27 19:00 | duration_min=300, lead_min=30 | NO | - | - | 0 | 0 | subtle | Someone pauses the customer_agg DAG during maintenance and forgets it; scheduled Spark runs do not happen and the executors stay idle. |
| atk-005 | broker_gc_pressure | kafka-2 | Oct 28 01:00 | Oct 28 03:00 | duration_min=120, magnitude=0.6 | yes | 12 | kafka_under_replicated_partitions\|kafka-ingest (above_zero) | 1 | 138 | subtle | A client with huge fetch batches fills kafka-2's heap: long GC pauses, slower produce, and repeated ISR drops. |
| atk-006 | custom | - | Oct 28 13:05 | Oct 28 14:55 | add ramp 45 on airflow_scheduler_heartbeat_age_sec\|airflow-main for 90 min; add ramp 0.7 on container_cpu_usage_cores\|airflow-scheduler for 90 min; add ramp 0.5 on container_cpu_throttled_ratio\|airflow-scheduler for 90 min; add ramp 2.5e+08 on container_memory_working_set_bytes\|airflow-scheduler for 90 min; add ramp 6 on airflow_queued_tasks\|airflow-main for 90 min | yes | 19 | airflow_scheduler_heartbeat_age_sec\|airflow-main (seasonal fallback) | 1 | 52 | noticeable | Airflow scheduler degrades from a bloated metadata DB / DAG-parse storm: loop saturates CPU, heartbeat goes stale, tasks pile up in queued state. |
| atk-007 | consumer_slow_deploy | orders-etl | Oct 29 00:00 | Oct 29 02:30 | duration_min=150, lag_per_hour=500000, magnitude=0.8 | yes | 0 | kube_pod_container_status_restarts_total\|orders-etl-consumer-0 (counter_rule) | 2 | 108 | noticeable | A new orders-etl-consumer release is slower per record than the produce rate; lag builds until rollback, then drains. |
| atk-008 | custom | - | Oct 29 17:00 | Oct 29 19:30 | add ramp 1.75e+09 on container_memory_working_set_bytes\|orders-stream-exec-2 for 120 min; add ramp 0.33 on jvm_heap_used_ratio\|orders-stream-exec-2 for 120 min; add ramp 260 on jvm_gc_time_ms_per_sec\|orders-stream-exec-2 for 120 min; add ramp 0.5 on container_cpu_usage_cores\|orders-stream-exec-2 for 120 min; add ramp 5 on spark_streaming_batch_duration_sec\|orders-stream for 120 min; add ramp 8 on spark_streaming_scheduling_delay_sec\|orders-stream for 120 min; mult ramp 0.8 on spark_streaming_input_rows_per_sec\|orders-stream for 90 min | yes | 49 | jvm_gc_time_ms_per_sec\|orders-stream-exec-2 (seasonal fallback) | 1 | 86 | subtle | A Spark Structured Streaming executor (orders-stream-exec-2) slowly leaks memory (unbounded state/broadcast cache); heap and GC climb, batches slow, scheduling delay builds, and the pod nears its memory limit and gets OOMKilled. |
| atk-009 | producer_crashloop | orders-producer | Oct 30 00:10 | Oct 30 00:45 | duration_min=35 | yes | 0 | kube_pod_container_status_restarts_total\|orders-producer (counter_rule) | 1 | 66 | noticeable | A bad config rollout makes orders-producer crash on startup; it loops in restarts and no orders arrive until fixed. |
| atk-010 | flink_state_leak | clickstream-enrich | Oct 31 02:10 | Oct 31 09:10 | duration_min=420, rate_tm0_mib_per_hour=70, rate_tm1_mib_per_hour=90 | NO | - | - | 0 | 0 | subtle | A Flink job update keeps keyed state forever, so both TaskManagers' memory creeps up slowly over hours. |
| atk-011 | custom | - | Nov 01 00:00 | Nov 01 02:00 | add ramp 0.42 on jvm_heap_used_ratio\|trino-worker-0 for 90 min; add ramp 160 on jvm_gc_time_ms_per_sec\|trino-worker-0 for 90 min; add ramp 2.2 on container_cpu_usage_cores\|trino-worker-0 for 90 min; add ramp 2.2e+09 on container_memory_working_set_bytes\|trino-worker-0 for 90 min; add ramp 35 on trino_query_p95_sec\|trino-lakehouse for 90 min; add ramp 9 on trino_queued_queries\|trino-lakehouse for 80 min; add ramp 8 on trino_resource_group_queued\|adhoc for 80 min; add ramp 3 on trino_failed_queries_per_min\|trino-lakehouse for 60 min; mult flat 0.5 on trino_completed_queries_per_min\|trino-lakehouse for 80 min | yes | 19 | container_cpu_usage_cores\|trino-worker-0 (seasonal) | 1 | 80 | noticeable | A runaway ad-hoc cross-join query lands on trino-worker-0, eating heap and CPU; GC thrashes, other queries slow and queue in the adhoc resource group, and some fail on memory limits. |

## False page-outs

| time (UTC) | page | alerts in the page when it paged |
|---|---|---|
| Oct 27 20:14 | 7 | kafka_failed_produce_requests_per_sec\|kafka-ingest (seasonal) |
| Oct 28 19:59 | 10 | kafka_failed_produce_requests_per_sec\|kafka-ingest (seasonal) |

### The same 7 days with nothing injected: page-outs

| time (UTC) | first alert of the page |
|---|---|
| Oct 27 20:14 | kafka_failed_produce_requests_per_sec\|kafka-ingest |
| Oct 28 19:59 | kafka_failed_produce_requests_per_sec\|kafka-ingest |

## Triage

The frozen agent (`triage-freeze-v1`), 3 independent runs on the same 13 page-outs (11 true, 2 false under the live rule). Final answer = the follow-up's when there was one.

| | run 1 | run 2 | run 3 | min-max | sum |
|---|---|---|---|---|---|
| true pages (11): correct | 10 | 10 | 10 | 10-10 | 30 |
| true pages (11): **DANGEROUS** | 0 | 0 | 0 | 0-0 | 0 |
| true pages (11): deferred | 1 | 1 | 1 | 1-1 | 3 |
| true pages (11): no note | 0 | 0 | 0 | 0-0 | 0 |
| false pages (2): correct | 0 | 0 | 0 | 0-0 | 0 |
| false pages (2): wasted | 0 | 0 | 0 | 0-0 | 0 |
| false pages (2): deferred | 2 | 2 | 2 | 2-2 | 6 |
| false pages (2): no note | 0 | 0 | 0 | 0-0 | 0 |
| true pages: share correct (Wilson 95%) | 0.91 (0.62-0.98) | 0.91 (0.62-0.98) | 0.91 (0.62-0.98) | | |
| cause names the target, named (of 5 true pages) | 0 | 0 | 0 | 0-0 | |
| cause names any entity of the injection, generic (lenient: a generic incident has several targets) (of 6 true pages) | 5 | 3 | 5 | 3-5 | |
| follow-ups | 6 | 6 | 6 | 6-6 | 18 |
| notes the guard converted | 5 | 4 | 3 | 3-5 | 12 |
| minutes to the final answer, median | 0.5 | 0.6 | 0.5 | 0.5-0.6 |  |
| minutes to the final answer, max | 10.4 | 10.2 | 10.3 | 10.2-10.4 |  |
| cost, USD (estimate, follow-ups included) | 0.81 | 0.59 | 0.53 | 0.53-0.81 | 1.93 |
| wall time, s | 116 | 106 | 102 | 102-116 | 324 |

Agreement: 13 of 13 page-outs got the same final answer in all 3 runs.

### Every dangerous final answer

None, in any run.

### Per page-out

| time (UTC) | page | truth | injection | kind | run 1 final (outcome) | run 2 final (outcome) | run 3 final (outcome) |
|---|---|---|---|---|---|---|---|
| Oct 26 00:15 | 1 | real | atk-001 | generic | yes (correct) | yes (correct) | yes (correct) |
| Oct 26 00:44 | 2 | real | atk-001 | generic | unsure (deferred) | unsure (deferred) | unsure (deferred) |
| Oct 26 04:14 | 3 | real | atk-002 | named | yes (correct) | yes (correct) | yes (correct) |
| Oct 27 00:34 | 5 | real | atk-003 | generic | yes (correct) | yes (correct) | yes (correct) |
| Oct 27 20:14 | 7 | noise | - | - | unsure (deferred) | unsure (deferred) | unsure (deferred) |
| Oct 28 01:12 | 8 | real | atk-005 | named | yes (correct) | yes (correct) | yes (correct) |
| Oct 28 13:24 | 9 | real | atk-006 | generic | yes (correct) | yes (correct) | yes (correct) |
| Oct 28 19:59 | 10 | noise | - | - | unsure (deferred) | unsure (deferred) | unsure (deferred) |
| Oct 29 00:00 | 11 | real | atk-007 | named | yes (correct) | yes (correct) | yes (correct) |
| Oct 29 00:00 | 12 | real | atk-007 | named | yes (correct) | yes (correct) | yes (correct) |
| Oct 29 17:49 | 13 | real | atk-008 | generic | yes (correct) | yes (correct) | yes (correct) |
| Oct 30 00:10 | 14 | real | atk-009 | named | yes (correct) | yes (correct) | yes (correct) |
| Nov 01 00:19 | 17 | real | atk-011 | generic | yes (correct) | yes (correct) | yes (correct) |

### Run 1 in full

## Run 1: Outcomes

A true page: needs_human yes = correct, no = **dangerous**, unsure = deferred. A false page: no = correct, yes = wasted, unsure = deferred. Final = the follow-up's answer when there was one.

First answer (at paging time):

| truth | correct | DANGEROUS | wasted | deferred | no note |
|---|---|---|---|---|---|
| real (11) | 7 | 0 | - | 4 | 0 |
| noise (2) | 0 | - | 0 | 2 | 0 |

Final answer:

| truth | correct | DANGEROUS | wasted | deferred | no note |
|---|---|---|---|---|---|
| real (11) | 10 | 0 | - | 1 | 0 |
| noise (2) | 0 | - | 0 | 2 | 0 |

Follow-ups: 6. Dangerous (final): 0. Wasted (final): 0.

Cause names the injected target: 0 of 11 true page-outs (final answer; first answer: 1).

Minutes from paging to the final answer: median 0.5, max 10.4; for the pages that had a follow-up: median 10.2, max 10.4.

| time (UTC) | page | truth | injection | first | follow-up | final | outcome | cause | names target | impact | min to final |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Oct 26 00:15 | 1 | real | atk-001 | yes | - | yes | correct | unknown | no | latency | 0.2 |
| Oct 26 00:44 | 2 | real | atk-001 | unsure | unsure | unsure | deferred | unknown | no | lag | 10.2 |
| Oct 26 04:14 | 3 | real | atk-002 | yes | - | yes | correct | unknown | no | latency | 0.1 |
| Oct 27 00:34 | 5 | real | atk-003 | yes | - | yes | correct | unknown | no | latency | 0.3 |
| Oct 27 20:14 | 7 | noise | - | unsure | unsure | unsure | deferred | unknown | - | none | 10.1 |
| Oct 28 01:12 | 8 | real | atk-005 | yes | - | yes | correct | change | no | latency, errors | 0.4 |
| Oct 28 13:24 | 9 | real | atk-006 | unsure | yes | yes | correct | unknown | no | latency | 10.1 |
| Oct 28 19:59 | 10 | noise | - | unsure | unsure | unsure | deferred | unknown | - | none | 10.3 |
| Oct 29 00:00 | 11 | real | atk-007 | yes | - | yes | correct | failure | no | lag | 0.2 |
| Oct 29 00:00 | 12 | real | atk-007 | yes | - | yes | correct | failure | no | lag | 0.5 |
| Oct 29 17:49 | 13 | real | atk-008 | yes | - | yes | correct | unknown | no | latency | 0.3 |
| Oct 30 00:10 | 14 | real | atk-009 | unsure | yes | yes | correct | change | no | errors, lag | 10.2 |
| Nov 01 00:19 | 17 | real | atk-011 | unsure | yes | yes | correct | failure | no | latency | 10.4 |

### Run 2 in full

## Run 2: Outcomes

A true page: needs_human yes = correct, no = **dangerous**, unsure = deferred. A false page: no = correct, yes = wasted, unsure = deferred. Final = the follow-up's answer when there was one.

First answer (at paging time):

| truth | correct | DANGEROUS | wasted | deferred | no note |
|---|---|---|---|---|---|
| real (11) | 7 | 0 | - | 4 | 0 |
| noise (2) | 0 | - | 0 | 2 | 0 |

Final answer:

| truth | correct | DANGEROUS | wasted | deferred | no note |
|---|---|---|---|---|---|
| real (11) | 10 | 0 | - | 1 | 0 |
| noise (2) | 0 | - | 0 | 2 | 0 |

Follow-ups: 6. Dangerous (final): 0. Wasted (final): 0.

Cause names the injected target: 0 of 11 true page-outs (final answer; first answer: 1).

Minutes from paging to the final answer: median 0.6, max 10.2; for the pages that had a follow-up: median 10.2, max 10.2.

| time (UTC) | page | truth | injection | first | follow-up | final | outcome | cause | names target | impact | min to final |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Oct 26 00:15 | 1 | real | atk-001 | yes | - | yes | correct | unknown | no | latency | 0.3 |
| Oct 26 00:44 | 2 | real | atk-001 | unsure | unsure | unsure | deferred | unknown | no | lag | 10.2 |
| Oct 26 04:14 | 3 | real | atk-002 | yes | - | yes | correct | unknown | no | latency | 0.1 |
| Oct 27 00:34 | 5 | real | atk-003 | yes | - | yes | correct | failure | no | latency | 0.2 |
| Oct 27 20:14 | 7 | noise | - | unsure | unsure | unsure | deferred | unknown | - | none | 10.2 |
| Oct 28 01:12 | 8 | real | atk-005 | yes | - | yes | correct | change | no | latency, errors | 0.4 |
| Oct 28 13:24 | 9 | real | atk-006 | unsure | yes | yes | correct | unknown | no | none | 10.2 |
| Oct 28 19:59 | 10 | noise | - | unsure | unsure | unsure | deferred | unknown | - | none | 10.2 |
| Oct 29 00:00 | 11 | real | atk-007 | yes | - | yes | correct | failure | no | lag | 0.2 |
| Oct 29 00:00 | 12 | real | atk-007 | yes | - | yes | correct | failure | no | lag | 0.6 |
| Oct 29 17:49 | 13 | real | atk-008 | yes | - | yes | correct | unknown | no | latency | 0.2 |
| Oct 30 00:10 | 14 | real | atk-009 | unsure | yes | yes | correct | change | no | errors, lag | 10.2 |
| Nov 01 00:19 | 17 | real | atk-011 | unsure | yes | yes | correct | unknown | no | latency | 10.2 |

### Run 3 in full

## Run 3: Outcomes

A true page: needs_human yes = correct, no = **dangerous**, unsure = deferred. A false page: no = correct, yes = wasted, unsure = deferred. Final = the follow-up's answer when there was one.

First answer (at paging time):

| truth | correct | DANGEROUS | wasted | deferred | no note |
|---|---|---|---|---|---|
| real (11) | 7 | 0 | - | 4 | 0 |
| noise (2) | 0 | - | 0 | 2 | 0 |

Final answer:

| truth | correct | DANGEROUS | wasted | deferred | no note |
|---|---|---|---|---|---|
| real (11) | 10 | 0 | - | 1 | 0 |
| noise (2) | 0 | - | 0 | 2 | 0 |

Follow-ups: 6. Dangerous (final): 0. Wasted (final): 0.

Cause names the injected target: 0 of 11 true page-outs (final answer; first answer: 1).

Minutes from paging to the final answer: median 0.5, max 10.3; for the pages that had a follow-up: median 10.2, max 10.3.

| time (UTC) | page | truth | injection | first | follow-up | final | outcome | cause | names target | impact | min to final |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Oct 26 00:15 | 1 | real | atk-001 | yes | - | yes | correct | unknown | no | latency | 0.2 |
| Oct 26 00:44 | 2 | real | atk-001 | unsure | unsure | unsure | deferred | unknown | no | lag | 10.1 |
| Oct 26 04:14 | 3 | real | atk-002 | yes | - | yes | correct | unknown | no | latency | 0.1 |
| Oct 27 00:34 | 5 | real | atk-003 | yes | - | yes | correct | unknown | no | latency | 0.2 |
| Oct 27 20:14 | 7 | noise | - | unsure | unsure | unsure | deferred | unknown | - | none | 10.1 |
| Oct 28 01:12 | 8 | real | atk-005 | yes | - | yes | correct | change | no | latency, errors | 0.5 |
| Oct 28 13:24 | 9 | real | atk-006 | unsure | yes | yes | correct | unknown | no | latency | 10.3 |
| Oct 28 19:59 | 10 | noise | - | unsure | unsure | unsure | deferred | unknown | - | none | 10.2 |
| Oct 29 00:00 | 11 | real | atk-007 | yes | - | yes | correct | failure | no | lag | 0.1 |
| Oct 29 00:00 | 12 | real | atk-007 | yes | - | yes | correct | failure | no | lag | 0.5 |
| Oct 29 17:49 | 13 | real | atk-008 | yes | - | yes | correct | failure | no | latency | 0.2 |
| Oct 30 00:10 | 14 | real | atk-009 | unsure | yes | yes | correct | change | no | errors | 10.1 |
| Nov 01 00:19 | 17 | real | atk-011 | unsure | yes | yes | correct | unknown | no | latency | 10.2 |

## Answer key, as written by the publisher

```json
{"effects": [{"magnitude": 3.5, "op": "mult", "series_id": "kafka_produce_request_p99_ms|kafka-ingest", "shape": "ramp", "start": "2026-10-26T00:00:00+00:00", "stop": "2026-10-26T01:30:00+00:00"}, {"magnitude": 0.15, "op": "add", "series_id": "kafka_isr_shrinks_per_sec|kafka-ingest", "shape": "flat", "start": "2026-10-26T00:10:00+00:00", "stop": "2026-10-26T01:30:00+00:00"}, {"magnitude": 2.0, "op": "add", "series_id": "kafka_under_replicated_partitions|kafka-ingest", "shape": "flat", "start": "2026-10-26T00:15:00+00:00", "stop": "2026-10-26T01:30:00+00:00"}, {"magnitude": 0.1, "op": "add", "series_id": "kafka_failed_produce_requests_per_sec|kafka-ingest", "shape": "ramp", "start": "2026-10-26T00:30:00+00:00", "stop": "2026-10-26T01:30:00+00:00"}, {"magnitude": 2.0, "op": "mult", "series_id": "jvm_gc_time_ms_per_sec|kafka-1", "shape": "ramp", "start": "2026-10-26T00:00:00+00:00", "stop": "2026-10-26T01:30:00+00:00"}, {"magnitude": 600.0, "op": "add", "series_id": "kafka_consumergroup_lag|orders-etl", "shape": "ramp", "start": "2026-10-26T00:20:00+00:00", "stop": "2026-10-26T01:50:00+00:00"}], "entities": ["cluster:kafka-ingest", "consumer_group:orders-etl", "pod:kafka-1"], "event": "started", "evidence": ["metrics"], "id": "atk-001", "intent": "A degrading disk/slow follower on kafka-1 causes slow log flushes, replicas dropping out of ISR, slower produce requests and growing consumer lag.", "params": {}, "rejected": [], "scenario": "custom", "series": ["jvm_gc_time_ms_per_sec|kafka-1", "kafka_consumergroup_lag|orders-etl", "kafka_failed_produce_requests_per_sec|kafka-ingest", "kafka_isr_shrinks_per_sec|kafka-ingest", "kafka_produce_request_p99_ms|kafka-ingest", "kafka_under_replicated_partitions|kafka-ingest"], "severity": "subtle", "start": "2026-10-26T00:00:00+00:00", "stop": "2026-10-26T01:50:00+00:00", "stream_version": 2, "target": ""}
{"event": "stopped", "id": "atk-001", "reason": "duration", "stop": "2026-10-26T01:50:00+00:00"}
{"entities": ["cluster:trino-lakehouse", "pod:trino-worker-0", "pod:trino-worker-1", "resource_group:adhoc"], "event": "started", "evidence": ["metrics", "logs"], "id": "atk-002", "intent": "An analyst runs a huge unbounded ad-hoc cross join off-hours that saturates both Trino workers; other queries queue and some fail.", "params": {"duration_min": 40.0, "magnitude": 0.8}, "scenario": "trino_heavy_query", "series": ["container_cpu_throttled_ratio|trino-worker-0", "container_cpu_throttled_ratio|trino-worker-1", "container_cpu_usage_cores|trino-worker-0", "container_cpu_usage_cores|trino-worker-1", "jvm_gc_time_ms_per_sec|trino-worker-0", "jvm_gc_time_ms_per_sec|trino-worker-1", "jvm_heap_used_ratio|trino-worker-0", "jvm_heap_used_ratio|trino-worker-1", "trino_completed_queries_per_min|trino-lakehouse", "trino_failed_queries_per_min|trino-lakehouse", "trino_query_p95_sec|trino-lakehouse", "trino_queued_queries|trino-lakehouse", "trino_resource_group_queued|adhoc"], "severity": "noticeable", "start": "2026-10-26T04:00:00+00:00", "stop": "2026-10-26T04:40:00+00:00", "stream_version": 2, "target": "trino-lakehouse"}
{"event": "stopped", "id": "atk-002", "reason": "duration", "stop": "2026-10-26T04:40:00+00:00"}
{"effects": [{"magnitude": 0.33, "op": "add", "series_id": "jvm_heap_used_ratio|flink-taskmanager-0", "shape": "ramp", "start": "2026-10-27T00:10:00+00:00", "stop": "2026-10-27T02:10:00+00:00"}, {"magnitude": 220.0, "op": "add", "series_id": "jvm_gc_time_ms_per_sec|flink-taskmanager-0", "shape": "ramp", "start": "2026-10-27T00:10:00+00:00", "stop": "2026-10-27T02:10:00+00:00"}, {"magnitude": 0.5, "op": "add", "series_id": "container_cpu_usage_cores|flink-taskmanager-0", "shape": "ramp", "start": "2026-10-27T00:10:00+00:00", "stop": "2026-10-27T02:10:00+00:00"}, {"magnitude": 0.4, "op": "add", "series_id": "flink_backpressure_ratio|clickstream-enrich", "shape": "ramp", "start": "2026-10-27T00:10:00+00:00", "stop": "2026-10-27T02:10:00+00:00"}, {"magnitude": 14000.0, "op": "add", "series_id": "flink_last_checkpoint_duration_ms|clickstream-enrich", "shape": "ramp", "start": "2026-10-27T00:10:00+00:00", "stop": "2026-10-27T02:10:00+00:00"}, {"magnitude": 0.6, "op": "mult", "series_id": "flink_records_out_per_sec|clickstream-enrich", "shape": "ramp", "start": "2026-10-27T00:10:00+00:00", "stop": "2026-10-27T02:10:00+00:00"}, {"magnitude": 4.0, "op": "add", "series_id": "flink_checkpoint_failures_total|clickstream-enrich", "shape": "ramp", "start": "2026-10-27T01:10:00+00:00", "stop": "2026-10-27T02:10:00+00:00"}], "entities": ["flink_job:clickstream-enrich", "pod:flink-taskmanager-0"], "event": "started", "evidence": ["metrics"], "id": "atk-003", "intent": "A state/heap leak in a Flink taskmanager (unbounded keyed state in the enrichment join) drives heap up and GC thrash, causing backpressure, slow checkpoints and eventually checkpoint timeouts and reduced throughput.", "params": {}, "rejected": [], "scenario": "custom", "series": ["container_cpu_throttled_ratio|flink-taskmanager-0", "container_cpu_usage_cores|flink-taskmanager-0", "flink_backpressure_ratio|clickstream-enrich", "flink_checkpoint_failures_total|clickstream-enrich", "flink_last_checkpoint_duration_ms|clickstream-enrich", "flink_records_out_per_sec|clickstream-enrich", "jvm_gc_time_ms_per_sec|flink-taskmanager-0", "jvm_heap_used_ratio|flink-taskmanager-0"], "severity": "subtle", "start": "2026-10-27T00:10:00+00:00", "stop": "2026-10-27T02:10:00+00:00", "stream_version": 2, "target": ""}
{"event": "stopped", "id": "atk-003", "reason": "duration", "stop": "2026-10-27T02:10:00+00:00"}
{"entities": ["dag:customer_agg", "pod:customer-agg-driver", "pod:customer-agg-exec-1", "pod:customer-agg-exec-2", "pod:customer-agg-exec-3"], "event": "started", "evidence": ["metrics", "dag_run", "logs", "k8s_events", "changes"], "id": "atk-004", "intent": "Someone pauses the customer_agg DAG during maintenance and forgets it; scheduled Spark runs do not happen and the executors stay idle.", "params": {"duration_min": 300.0, "lead_min": 30.0}, "scenario": "dag_paused", "series": ["container_cpu_throttled_ratio|customer-agg-driver", "container_cpu_throttled_ratio|customer-agg-exec-1", "container_cpu_throttled_ratio|customer-agg-exec-2", "container_cpu_throttled_ratio|customer-agg-exec-3", "container_cpu_usage_cores|customer-agg-driver", "container_cpu_usage_cores|customer-agg-exec-1", "container_cpu_usage_cores|customer-agg-exec-2", "container_cpu_usage_cores|customer-agg-exec-3", "container_memory_working_set_bytes|customer-agg-driver", "container_memory_working_set_bytes|customer-agg-exec-1", "container_memory_working_set_bytes|customer-agg-exec-2", "container_memory_working_set_bytes|customer-agg-exec-3", "jvm_gc_time_ms_per_sec|customer-agg-driver", "jvm_gc_time_ms_per_sec|customer-agg-exec-1", "jvm_gc_time_ms_per_sec|customer-agg-exec-2", "jvm_gc_time_ms_per_sec|customer-agg-exec-3", "jvm_heap_used_ratio|customer-agg-driver", "jvm_heap_used_ratio|customer-agg-exec-1", "jvm_heap_used_ratio|customer-agg-exec-2", "jvm_heap_used_ratio|customer-agg-exec-3", "kube_pod_container_status_restarts_total|customer-agg-driver", "kube_pod_container_status_restarts_total|customer-agg-exec-1", "kube_pod_container_status_restarts_total|customer-agg-exec-2", "kube_pod_container_status_restarts_total|customer-agg-exec-3"], "severity": "subtle", "start": "2026-10-27T14:00:00+00:00", "stop": "2026-10-27T19:00:00+00:00", "stream_version": 2, "target": "customer_agg"}
{"event": "stopped", "id": "atk-004", "reason": "duration", "stop": "2026-10-27T19:00:00+00:00"}
{"entities": ["cluster:kafka-ingest", "pod:kafka-2"], "event": "started", "evidence": ["metrics", "logs", "changes"], "id": "atk-005", "intent": "A client with huge fetch batches fills kafka-2's heap: long GC pauses, slower produce, and repeated ISR drops.", "params": {"duration_min": 120.0, "magnitude": 0.6}, "scenario": "broker_gc_pressure", "series": ["container_cpu_throttled_ratio|kafka-2", "container_cpu_usage_cores|kafka-2", "jvm_gc_time_ms_per_sec|kafka-2", "jvm_heap_used_ratio|kafka-2", "kafka_failed_produce_requests_per_sec|kafka-ingest", "kafka_produce_request_p99_ms|kafka-ingest", "kafka_under_replicated_partitions|kafka-ingest"], "severity": "subtle", "start": "2026-10-28T01:00:00+00:00", "stop": "2026-10-28T03:00:00+00:00", "stream_version": 2, "target": "kafka-2"}
{"event": "stopped", "id": "atk-005", "reason": "duration", "stop": "2026-10-28T03:00:00+00:00"}
{"effects": [{"magnitude": 45.0, "op": "add", "series_id": "airflow_scheduler_heartbeat_age_sec|airflow-main", "shape": "ramp", "start": "2026-10-28T13:05:00+00:00", "stop": "2026-10-28T14:35:00+00:00"}, {"magnitude": 0.7, "op": "add", "series_id": "container_cpu_usage_cores|airflow-scheduler", "shape": "ramp", "start": "2026-10-28T13:05:00+00:00", "stop": "2026-10-28T14:35:00+00:00"}, {"magnitude": 0.5, "op": "add", "series_id": "container_cpu_throttled_ratio|airflow-scheduler", "shape": "ramp", "start": "2026-10-28T13:05:00+00:00", "stop": "2026-10-28T14:35:00+00:00"}, {"magnitude": 250000000.0, "op": "add", "series_id": "container_memory_working_set_bytes|airflow-scheduler", "shape": "ramp", "start": "2026-10-28T13:05:00+00:00", "stop": "2026-10-28T14:35:00+00:00"}, {"magnitude": 6.0, "op": "add", "series_id": "airflow_queued_tasks|airflow-main", "shape": "ramp", "start": "2026-10-28T13:25:00+00:00", "stop": "2026-10-28T14:55:00+00:00"}], "entities": ["cluster:airflow-main", "pod:airflow-scheduler"], "event": "started", "evidence": ["metrics"], "id": "atk-006", "intent": "Airflow scheduler degrades from a bloated metadata DB / DAG-parse storm: loop saturates CPU, heartbeat goes stale, tasks pile up in queued state.", "params": {}, "rejected": [], "scenario": "custom", "series": ["airflow_queued_tasks|airflow-main", "airflow_scheduler_heartbeat_age_sec|airflow-main", "container_cpu_throttled_ratio|airflow-scheduler", "container_cpu_usage_cores|airflow-scheduler", "container_memory_working_set_bytes|airflow-scheduler"], "severity": "noticeable", "start": "2026-10-28T13:05:00+00:00", "stop": "2026-10-28T14:55:00+00:00", "stream_version": 2, "target": ""}
{"event": "stopped", "id": "atk-006", "reason": "duration", "stop": "2026-10-28T14:55:00+00:00"}
{"entities": ["consumer_group:orders-etl", "pod:orders-etl-consumer-0", "pod:orders-etl-consumer-1"], "event": "started", "evidence": ["metrics", "logs", "k8s_events", "changes"], "id": "atk-007", "intent": "A new orders-etl-consumer release is slower per record than the produce rate; lag builds until rollback, then drains.", "params": {"duration_min": 150.0, "lag_per_hour": 500000.0, "magnitude": 0.8}, "scenario": "consumer_slow_deploy", "series": ["container_cpu_throttled_ratio|orders-etl-consumer-0", "container_cpu_throttled_ratio|orders-etl-consumer-1", "container_cpu_usage_cores|orders-etl-consumer-0", "container_cpu_usage_cores|orders-etl-consumer-1", "kafka_consumer_records_consumed_per_sec|orders-etl", "kafka_consumergroup_lag|orders-etl", "kube_pod_container_status_restarts_total|orders-etl-consumer-0", "kube_pod_container_status_restarts_total|orders-etl-consumer-1"], "severity": "noticeable", "start": "2026-10-29T00:00:00+00:00", "stop": "2026-10-29T02:30:00+00:00", "stream_version": 2, "target": "orders-etl"}
{"event": "stopped", "id": "atk-007", "reason": "duration", "stop": "2026-10-29T02:30:00+00:00"}
{"effects": [{"magnitude": 1750000000.0, "op": "add", "series_id": "container_memory_working_set_bytes|orders-stream-exec-2", "shape": "ramp", "start": "2026-10-29T17:00:00+00:00", "stop": "2026-10-29T19:00:00+00:00"}, {"magnitude": 0.33, "op": "add", "series_id": "jvm_heap_used_ratio|orders-stream-exec-2", "shape": "ramp", "start": "2026-10-29T17:00:00+00:00", "stop": "2026-10-29T19:00:00+00:00"}, {"magnitude": 260.0, "op": "add", "series_id": "jvm_gc_time_ms_per_sec|orders-stream-exec-2", "shape": "ramp", "start": "2026-10-29T17:00:00+00:00", "stop": "2026-10-29T19:00:00+00:00"}, {"magnitude": 0.5, "op": "add", "series_id": "container_cpu_usage_cores|orders-stream-exec-2", "shape": "ramp", "start": "2026-10-29T17:00:00+00:00", "stop": "2026-10-29T19:00:00+00:00"}, {"magnitude": 5.0, "op": "add", "series_id": "spark_streaming_batch_duration_sec|orders-stream", "shape": "ramp", "start": "2026-10-29T17:00:00+00:00", "stop": "2026-10-29T19:00:00+00:00"}, {"magnitude": 8.0, "op": "add", "series_id": "spark_streaming_scheduling_delay_sec|orders-stream", "shape": "ramp", "start": "2026-10-29T17:30:00+00:00", "stop": "2026-10-29T19:30:00+00:00"}, {"magnitude": 0.8, "op": "mult", "series_id": "spark_streaming_input_rows_per_sec|orders-stream", "shape": "ramp", "start": "2026-10-29T17:30:00+00:00", "stop": "2026-10-29T19:00:00+00:00"}], "entities": ["pod:orders-stream-exec-2", "spark_app:orders-stream"], "event": "started", "evidence": ["metrics"], "id": "atk-008", "intent": "A Spark Structured Streaming executor (orders-stream-exec-2) slowly leaks memory (unbounded state/broadcast cache); heap and GC climb, batches slow, scheduling delay builds, and the pod nears its memory limit and gets OOMKilled.", "params": {}, "rejected": [], "scenario": "custom", "series": ["container_cpu_throttled_ratio|orders-stream-exec-2", "container_cpu_usage_cores|orders-stream-exec-2", "container_memory_working_set_bytes|orders-stream-exec-2", "jvm_gc_time_ms_per_sec|orders-stream-exec-2", "jvm_heap_used_ratio|orders-stream-exec-2", "spark_streaming_batch_duration_sec|orders-stream", "spark_streaming_input_rows_per_sec|orders-stream", "spark_streaming_scheduling_delay_sec|orders-stream"], "severity": "subtle", "start": "2026-10-29T17:00:00+00:00", "stop": "2026-10-29T19:30:00+00:00", "stream_version": 2, "target": ""}
{"event": "stopped", "id": "atk-008", "reason": "duration", "stop": "2026-10-29T19:30:00+00:00"}
{"entities": ["consumer_group:orders-etl", "pod:orders-etl-consumer-0", "pod:orders-etl-consumer-1", "pod:orders-producer", "spark_app:orders-stream", "topic:orders"], "event": "started", "evidence": ["metrics", "logs", "k8s_events", "changes"], "id": "atk-009", "intent": "A bad config rollout makes orders-producer crash on startup; it loops in restarts and no orders arrive until fixed.", "params": {"duration_min": 35.0}, "scenario": "producer_crashloop", "series": ["container_cpu_throttled_ratio|orders-etl-consumer-0", "container_cpu_throttled_ratio|orders-etl-consumer-1", "container_cpu_throttled_ratio|orders-producer", "container_cpu_usage_cores|orders-etl-consumer-0", "container_cpu_usage_cores|orders-etl-consumer-1", "container_cpu_usage_cores|orders-producer", "container_memory_working_set_bytes|orders-producer", "kafka_consumer_records_consumed_per_sec|orders-etl", "kafka_consumergroup_lag|orders-etl", "kafka_messages_in_per_sec|orders", "kube_pod_container_status_restarts_total|orders-producer", "spark_streaming_input_rows_per_sec|orders-stream"], "severity": "noticeable", "start": "2026-10-30T00:10:00+00:00", "stop": "2026-10-30T00:45:00+00:00", "stream_version": 2, "target": "orders-producer"}
{"event": "stopped", "id": "atk-009", "reason": "duration", "stop": "2026-10-30T00:45:00+00:00"}
{"entities": ["pod:flink-taskmanager-0", "pod:flink-taskmanager-1"], "event": "started", "evidence": ["metrics", "logs", "k8s_events", "changes"], "id": "atk-010", "intent": "A Flink job update keeps keyed state forever, so both TaskManagers' memory creeps up slowly over hours.", "params": {"duration_min": 420.0, "rate_tm0_mib_per_hour": 70.0, "rate_tm1_mib_per_hour": 90.0}, "scenario": "flink_state_leak", "series": ["container_memory_working_set_bytes|flink-taskmanager-0", "container_memory_working_set_bytes|flink-taskmanager-1"], "severity": "subtle", "start": "2026-10-31T02:10:00+00:00", "stop": "2026-10-31T09:10:00+00:00", "stream_version": 2, "target": "clickstream-enrich"}
{"event": "stopped", "id": "atk-010", "reason": "duration", "stop": "2026-10-31T09:10:00+00:00"}
{"effects": [{"magnitude": 0.42, "op": "add", "series_id": "jvm_heap_used_ratio|trino-worker-0", "shape": "ramp", "start": "2026-11-01T00:00:00+00:00", "stop": "2026-11-01T01:30:00+00:00"}, {"magnitude": 160.0, "op": "add", "series_id": "jvm_gc_time_ms_per_sec|trino-worker-0", "shape": "ramp", "start": "2026-11-01T00:00:00+00:00", "stop": "2026-11-01T01:30:00+00:00"}, {"magnitude": 2.2, "op": "add", "series_id": "container_cpu_usage_cores|trino-worker-0", "shape": "ramp", "start": "2026-11-01T00:00:00+00:00", "stop": "2026-11-01T01:30:00+00:00"}, {"magnitude": 2200000000.0, "op": "add", "series_id": "container_memory_working_set_bytes|trino-worker-0", "shape": "ramp", "start": "2026-11-01T00:00:00+00:00", "stop": "2026-11-01T01:30:00+00:00"}, {"magnitude": 35.0, "op": "add", "series_id": "trino_query_p95_sec|trino-lakehouse", "shape": "ramp", "start": "2026-11-01T00:15:00+00:00", "stop": "2026-11-01T01:45:00+00:00"}, {"magnitude": 9.0, "op": "add", "series_id": "trino_queued_queries|trino-lakehouse", "shape": "ramp", "start": "2026-11-01T00:30:00+00:00", "stop": "2026-11-01T01:50:00+00:00"}, {"magnitude": 8.0, "op": "add", "series_id": "trino_resource_group_queued|adhoc", "shape": "ramp", "start": "2026-11-01T00:30:00+00:00", "stop": "2026-11-01T01:50:00+00:00"}, {"magnitude": 3.0, "op": "add", "series_id": "trino_failed_queries_per_min|trino-lakehouse", "shape": "ramp", "start": "2026-11-01T01:00:00+00:00", "stop": "2026-11-01T02:00:00+00:00"}, {"magnitude": 0.5, "op": "mult", "series_id": "trino_completed_queries_per_min|trino-lakehouse", "shape": "flat", "start": "2026-11-01T00:40:00+00:00", "stop": "2026-11-01T02:00:00+00:00"}], "entities": ["cluster:trino-lakehouse", "pod:trino-worker-0", "resource_group:adhoc"], "event": "started", "evidence": ["metrics"], "id": "atk-011", "intent": "A runaway ad-hoc cross-join query lands on trino-worker-0, eating heap and CPU; GC thrashes, other queries slow and queue in the adhoc resource group, and some fail on memory limits.", "params": {}, "rejected": [], "scenario": "custom", "series": ["container_cpu_throttled_ratio|trino-worker-0", "container_cpu_usage_cores|trino-worker-0", "container_memory_working_set_bytes|trino-worker-0", "jvm_gc_time_ms_per_sec|trino-worker-0", "jvm_heap_used_ratio|trino-worker-0", "trino_completed_queries_per_min|trino-lakehouse", "trino_failed_queries_per_min|trino-lakehouse", "trino_query_p95_sec|trino-lakehouse", "trino_queued_queries|trino-lakehouse", "trino_resource_group_queued|adhoc"], "severity": "noticeable", "start": "2026-11-01T00:00:00+00:00", "stop": "2026-11-01T02:00:00+00:00", "stream_version": 2, "target": ""}
{"event": "stopped", "id": "atk-011", "reason": "duration", "stop": "2026-11-01T02:00:00+00:00"}
{"end": "2026-11-02T00:00:00+00:00", "event": "published", "start": "2026-10-26T00:00:00+00:00", "stream_version": 2}
```
