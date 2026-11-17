# Triage agents on live-week-1, stream v2 (DEVELOPMENT SET)

**This is a development-set result, not a held-out one, and not a final result.** The week, its schedule and the first triage run's mistakes (docs/results/triage-dev-week-1.md) were read before the stream-v2 evidence, the two verdict rules and the `baseline_comparison` tool were written. It shows whether the changes do what they were meant to on the cases they were written for; a held-out number needs a further world with its own protocol.

- written by `scripts/triage_dev_week_v2.py` at commit `7dbf314`
- the same 14 injections as live-week-1 (schedule seed 1, checked against docs/live-protocol.md), replayed on stream v2: topics `live-week-1-v2-*`. live-week-1 itself (stream v1) and its result file are unchanged
- agent: `claude-sonnet-5-5`, effort `medium`; budget per page 15 tool calls, 120 s, 200,000 tokens; one fresh agent per page-out, 3 at a time; notes are advisory
- this invocation: arm C investigated 29 page-outs in 112 s of wall time
- this invocation: arm B investigated 29 page-outs in 125 s of wall time

Three arms:

| arm | agent | stream | notes topic |
|---|---|---|---|
| A | as in the first run (no rules, 8 tools) | v1: metrics and runs only; logs / events / changes from `data_live` | `live-week-1-triage` |
| B | two verdict rules + `baseline_comparison` | v1 (same page-outs as A) | `live-week-1-triage-agent-v2` |
| C | two verdict rules + `baseline_comparison` | v2: logs / events / changes on the stream, with the failures' own | `live-week-1-v2-triage` |

A to B changes the agent only; B to C changes the evidence only.

## Detection: stream v1 against stream v2

| | stream v1 (live-week-1) | stream v2 |
|---|---|---|
| injections detected | 12 | 12 |
| median time to detect (min) | 12 | 12 |
| max time to detect (min) | 132 | 132 |
| page-outs | 29 | 29 |
| matching an injection | 25 | 25 |
| false page-outs | 4 | 4 |
| false pages per day | 0.57 | 0.57 |

Per injection: identical (detected, time to detect, first paged by, matching page-outs).
The `alerts` topics of the two runs are identical record for record: True; the `pages` topics: True.
The engine does not read the new message types, and the readings and runs on stream v2 are byte-for-byte those of stream v1 (tests/test_live_evidence.py), so no difference is expected.

What stream v2 carried: 10,080 `metrics` messages, 344 `dag_run` messages, 10,069 `logs` messages, 50 `k8s_events` messages, 24 `changes` messages, 1 `watermark` messages; 69,198 log lines, 244 Kubernetes events, 24 changes.

# Arm C: agent v2 on stream v2

## Verdict against truth

Truth: a page-out is real when it matches an injection under the live-week rule, noise otherwise.

| truth \ verdict | real | noise | unsure | no note |
|---|---|---|---|---|
| real (25) | 12 | 10 | 3 | 0 |
| noise (4) | 0 | 1 | 3 | 0 |

**Real called noise: 10 of 25 real page-outs.**

| time (UTC) | page | injection | scenario | confidence | suspected cause | evidence from |
|---|---|---|---|---|---|---|
| Oct 26 18:15 | 5 | inj-03 | dag_paused | 0.9 | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The 18:05 run was therefore not started. Before the pause, every hourly run since 07:05 started within about 0.5 minutes of schedule and succeeded. | dag_runs, get_page, recent_changes |
| Oct 26 19:15 | 6 | inj-03 | dag_paused | 0.9 | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The 18:05 and 19:05 runs did not start because of the pause, so the start_late alert is expected. | dag_runs, recent_changes |
| Oct 26 20:15 | 7 | inj-03 | dag_paused | 0.9 | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The last run was 17:05, and the 18:05, 19:05 and 20:05 runs didn't start because the DAG is paused. The start_late alert is firing on the pause, not on a scheduler fault. | dag_runs, recent_changes |
| Oct 26 21:15 | 8 | inj-03 | dag_paused | 0.9 | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The last run was at 17:05, and the runs scheduled from 18:05 on never started because the DAG is paused. The start_late alert is therefore expected. | dag_runs, recent_changes |
| Oct 26 22:15 | 9 | inj-03 | dag_paused | 0.85 | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. No runs have started since the 17:05 run, which fits the pause. The start_late alert fired because of the pause, not because the scheduler failed. | dag_runs, get_page, recent_changes |
| Oct 26 23:15 | 10 | inj-03 | dag_paused | 0.85 | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The 23:05 run didn't start because the DAG is paused. No runs have started since 18:05, which matches the pause. | dag_runs, get_page, recent_changes |
| Oct 27 00:15 | 11 | inj-03 | dag_paused | 0.85 | The DAG was deliberately paused at 17:23 UTC on 10-26 (chg-0005, m.ortiz) for a warehouse table migration. No runs have started since the 17:05 run, which matches the pause, so the start_late alert is expected. | dag_runs, recent_changes |
| Oct 27 01:15 | 12 | inj-03 | dag_paused | 0.85 | hourly_quality_check was deliberately paused by m.ortiz at 17:23 UTC on 10-26 for a warehouse table migration. The missing runs are expected, and the start_late alert fired because of the pause. No run has started since the pause. | dag_runs, get_page, recent_changes |
| Oct 27 02:15 | 13 | inj-03 | dag_paused | 0.85 | The DAG was deliberately paused by m.ortiz at 17:23 UTC on 10-26 for a warehouse table migration. No runs have started since, which is expected for a paused DAG, so the start_late alert is not a fault. | dag_runs, recent_changes |
| Nov 01 03:40 | 32 | inj-13 | dag_paused | 0.9 | customer_agg was deliberately paused at 02:30 UTC by m.ortiz for a warehouse table migration (chg-0021). The 03:30 run was not started because the DAG is paused, not because the scheduler failed. | dag_runs, get_page, recent_changes |

Noise called real: 0 of 4 noise page-outs.

## Does the note name the injected target?

Over the real page-outs. (a) any of the entities the answer key lists for the matched injection (the owners of the series it changed, plus the DAG for a pause); (b) the scenario's named target itself (an entity with that name, or a pod named `<target>-<n>`).

| real page-outs | (a) names an answer-key entity | (b) names the target |
|---|---|---|
| 25 | 25 | 23 |

## Cost and time

| | total | per page: mean | median | max |
|---|---|---|---|---|
| cost (USD, estimate) | 0.974 | 0.0336 | 0.0327 | 0.0568 |
| time (s, one investigation) | 325 | 11.2 | 10.6 | 21.8 |
| tool calls | 182 | 6.3 | - | 12 |

Tokens over all investigations: 898,108. Notes: 29 of 29 page-outs, 29 valid. Budget used up: 0 by tool calls, 0 by time, 0 by tokens.

## Per page-out

| time (UTC) | page | truth | injection | verdict | conf. | affected entities | (a) | (b) | tool calls | s | USD | evidence from |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Oct 26 03:02 | 1 | real | inj-01 | real | 0.8 | cluster:kafka-ingest, topic:orders, pod:kafka-0 | yes | no | 10 | 21.78 | 0.05678 | query_metric, recent_changes |
| Oct 26 08:07 | 2 | real | inj-02 | unsure | 0.5 | pod:orders-producer, topic:orders | yes | yes | 12 | 19.07 | 0.05185 | get_page, k8s_events, query_metric, recent_changes, recent_logs |
| Oct 26 18:15 | 5 | real | inj-03 | noise | 0.9 | dag:hourly_quality_check | yes | yes | 4 | 9.06 | 0.02652 | dag_runs, get_page, recent_changes |
| Oct 26 19:15 | 6 | real | inj-03 | noise | 0.9 | dag:hourly_quality_check | yes | yes | 4 | 6.06 | 0.0208 | dag_runs, recent_changes |
| Oct 26 20:15 | 7 | real | inj-03 | noise | 0.9 | dag:hourly_quality_check | yes | yes | 7 | 13.92 | 0.03272 | dag_runs, recent_changes |
| Oct 26 21:15 | 8 | real | inj-03 | noise | 0.9 | dag:hourly_quality_check | yes | yes | 5 | 6.53 | 0.02556 | dag_runs, recent_changes |
| Oct 26 22:15 | 9 | real | inj-03 | noise | 0.85 | dag:hourly_quality_check | yes | yes | 4 | 6.59 | 0.02469 | dag_runs, get_page, recent_changes |
| Oct 26 23:15 | 10 | real | inj-03 | noise | 0.85 | dag:hourly_quality_check | yes | yes | 5 | 7.98 | 0.02272 | dag_runs, get_page, recent_changes |
| Oct 27 00:15 | 11 | real | inj-03 | noise | 0.85 | dag:hourly_quality_check | yes | yes | 3 | 5.27 | 0.02031 | dag_runs, recent_changes |
| Oct 27 01:15 | 12 | real | inj-03 | noise | 0.85 | dag:hourly_quality_check | yes | yes | 8 | 13.95 | 0.02649 | dag_runs, get_page, recent_changes |
| Oct 27 02:15 | 13 | real | inj-03 | noise | 0.85 | dag:hourly_quality_check | yes | yes | 5 | 10.43 | 0.02237 | dag_runs, recent_changes |
| Oct 27 19:14 | 14 | real | inj-05 | real | 0.85 | pod:trino-worker-0, pod:trino-worker-1, cluster:trino-lakehouse | yes | yes | 4 | 9.73 | 0.03195 | get_page, k8s_events, query_metric, recent_changes |
| Oct 28 05:39 | 16 | real | inj-06 | real | 0.7 | pod:orders-etl-consumer-0, consumer_group:orders-etl, topic:orders | yes | yes | 7 | 12.71 | 0.03905 | get_page, k8s_events, kafka_lag, query_metric, recent_changes |
| Oct 28 05:39 | 17 | real | inj-06 | real | 0.7 | pod:orders-etl-consumer-1, consumer_group:orders-etl, topic:orders | yes | yes | 5 | 8.71 | 0.02752 | get_page, k8s_events, kafka_lag, recent_changes |
| Oct 28 20:09 | 18 | real | inj-07 | real | 0.7 | pod:orders-etl-consumer-0, consumer_group:orders-etl, topic:orders | yes | yes | 8 | 12.35 | 0.04421 | get_page, k8s_events, kafka_lag, query_metric, recent_changes |
| Oct 28 20:09 | 19 | real | inj-07 | real | 0.6 | pod:orders-etl-consumer-1, consumer_group:orders-etl, topic:orders | yes | yes | 7 | 11.09 | 0.03975 | k8s_events, kafka_lag, query_metric, recent_changes, recent_logs |
| Oct 28 21:29 | 20 | noise | - | noise | 0.65 | spark_app:orders-stream | - | - | 3 | 8.47 | 0.02425 | get_page, query_metric, recent_changes |
| Oct 29 18:23 | 22 | real | inj-09 | real | 0.8 | cluster:kafka-ingest, topic:orders | yes | no | 8 | 18.76 | 0.04792 | k8s_events, query_metric, recent_changes |
| Oct 30 11:09 | 23 | real | inj-10 | unsure | 0.5 | pod:orders-etl-consumer-0, consumer_group:orders-etl | yes | yes | 6 | 12.09 | 0.03487 | k8s_events, kafka_lag, query_metric, recent_changes, recent_logs |
| Oct 30 11:44 | 24 | real | inj-10 | real | 0.8 | pod:orders-etl-consumer-1, pod:orders-etl-consumer-0, consumer_group:orders-etl | yes | yes | 7 | 11.17 | 0.04183 | kafka_lag, query_metric, recent_changes, recent_logs |
| Oct 31 01:10 | 25 | noise | - | unsure | 0.55 | dag:hourly_quality_check, cluster:airflow-main | - | - | 10 | 12.77 | 0.04396 | dag_runs, k8s_events, query_metric, recent_changes |
| Oct 31 01:54 | 26 | real | inj-11 | real | 0.85 | pod:trino-worker-1, cluster:trino-lakehouse | yes | yes | 8 | 12.38 | 0.03761 | get_page, k8s_events, query_metric, recent_changes |
| Oct 31 01:54 | 27 | real | inj-11 | real | 0.85 | pod:trino-worker-0, cluster:trino-lakehouse | yes | yes | 8 | 14.17 | 0.04034 | get_page, k8s_events, query_metric, recent_changes, recent_logs |
| Oct 31 10:20 | 28 | real | inj-12 | unsure | 0.5 | pod:orders-producer | yes | yes | 5 | 10.49 | 0.02913 | get_page, k8s_events, query_metric, recent_changes, recent_logs |
| Nov 01 03:40 | 32 | real | inj-13 | noise | 0.9 | dag:customer_agg | yes | yes | 3 | 6.17 | 0.0192 | dag_runs, get_page, recent_changes |
| Nov 01 13:14 | 33 | real | inj-14 | real | 0.7 | pod:orders-etl-consumer-0, pod:orders-etl-consumer-1, consumer_group:orders-etl | yes | yes | 7 | 10.6 | 0.03677 | k8s_events, kafka_lag, query_metric, recent_changes |
| Nov 01 13:14 | 34 | real | inj-14 | real | 0.7 | pod:orders-etl-consumer-1, pod:orders-etl-consumer-0, consumer_group:orders-etl | yes | yes | 6 | 10.45 | 0.03565 | k8s_events, kafka_lag, query_metric, recent_changes |
| Nov 01 18:10 | 35 | noise | - | unsure | 0.5 | dag:hourly_quality_check | - | - | 8 | 11.61 | 0.04065 | dag_runs, k8s_events, query_metric, recent_changes |
| Nov 01 20:54 | 36 | noise | - | unsure | 0.6 | spark_app:orders-stream | - | - | 5 | 10.24 | 0.02818 | get_page, kafka_lag, query_metric, recent_changes |

## Suspected cause next to what was injected (to read, not scored)

### Mon Oct 26 03:02 - page 1 - verdict real (0.8)

- Injected: **inj-01: broker_gc_pressure** on `kafka-1` (duration_min=168, magnitude=0.519). A client fetching huge batches fills one broker's heap: long GC pauses, slow produce requests, the broker falls out of the in-sync replicas again and again.
- First alert of the page: `kafka_under_replicated_partitions|kafka-ingest`
- Note's suspected cause: Under-replicated partitions rose from 0 to 12 at about 02:59. Produce p99 latency rose from about 16 ms to 165 ms, and failed produce requests started. This began about 10 minutes after a new finance-reporting-consumer was deployed (chg-0001, 02:50). It backfills orders from offset 0 with a 500 MB fetch.max.bytes. The backfill is the likely cause: it probably overloads the brokers that hold its replicas, so followers fall behind. This is correlation, not proven.
- Note's affected entities: `cluster:kafka-ingest`, `topic:orders`, `pod:kafka-0`

### Mon Oct 26 08:07 - page 2 - verdict unsure (0.5)

- Injected: **inj-02: producer_crashloop** on `orders-producer` (duration_min=50). A bad config makes orders-producer crash on startup: it restarts in a loop and no orders arrive.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-producer`
- Note's suspected cause: orders-producer's restart counter went from 3 to 4, which is the first restart in the streamed history. The pod is healthy afterwards and producing normally. I found no cause (no k8s events, no OOM, no errors) and no benign explanation (no deploy or change touching the producer). The producer logged 5 NOT_LEADER_OR_FOLLOWER warnings in the last 2 hours, which could point to a broker leadership change, but I did not confirm that.
- Note's affected entities: `pod:orders-producer`, `topic:orders`

### Mon Oct 26 18:15 - page 5 - verdict noise (0.9)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The 18:05 run was therefore not started. Before the pause, every hourly run since 07:05 started within about 0.5 minutes of schedule and succeeded.
- Note's affected entities: `dag:hourly_quality_check`

### Mon Oct 26 19:15 - page 6 - verdict noise (0.9)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The 18:05 and 19:05 runs did not start because of the pause, so the start_late alert is expected.
- Note's affected entities: `dag:hourly_quality_check`

### Mon Oct 26 20:15 - page 7 - verdict noise (0.9)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The last run was 17:05, and the 18:05, 19:05 and 20:05 runs didn't start because the DAG is paused. The start_late alert is firing on the pause, not on a scheduler fault.
- Note's affected entities: `dag:hourly_quality_check`

### Mon Oct 26 21:15 - page 8 - verdict noise (0.9)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The last run was at 17:05, and the runs scheduled from 18:05 on never started because the DAG is paused. The start_late alert is therefore expected.
- Note's affected entities: `dag:hourly_quality_check`

### Mon Oct 26 22:15 - page 9 - verdict noise (0.85)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. No runs have started since the 17:05 run, which fits the pause. The start_late alert fired because of the pause, not because the scheduler failed.
- Note's affected entities: `dag:hourly_quality_check`

### Mon Oct 26 23:15 - page 10 - verdict noise (0.85)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The 23:05 run didn't start because the DAG is paused. No runs have started since 18:05, which matches the pause.
- Note's affected entities: `dag:hourly_quality_check`

### Tue Oct 27 00:15 - page 11 - verdict noise (0.85)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG was deliberately paused at 17:23 UTC on 10-26 (chg-0005, m.ortiz) for a warehouse table migration. No runs have started since the 17:05 run, which matches the pause, so the start_late alert is expected.
- Note's affected entities: `dag:hourly_quality_check`

### Tue Oct 27 01:15 - page 12 - verdict noise (0.85)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: hourly_quality_check was deliberately paused by m.ortiz at 17:23 UTC on 10-26 for a warehouse table migration. The missing runs are expected, and the start_late alert fired because of the pause. No run has started since the pause.
- Note's affected entities: `dag:hourly_quality_check`

### Tue Oct 27 02:15 - page 13 - verdict noise (0.85)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG was deliberately paused by m.ortiz at 17:23 UTC on 10-26 for a warehouse table migration. No runs have started since, which is expected for a paused DAG, so the start_late alert is not a fault.
- Note's affected entities: `dag:hourly_quality_check`

### Tue Oct 27 19:14 - page 14 - verdict real (0.85)

- Injected: **inj-05: trino_heavy_query** on `trino-lakehouse` (duration_min=60, magnitude=0.817). One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.
- First alert of the page: `jvm_gc_time_ms_per_sec|trino-worker-0`
- Note's suspected cause: Both Trino workers are under heap pressure. Heap on worker-1 is about 0.95 and GC time is about 8x normal on both workers. CPU is throttled at about 55%, query p95 is 41s against a normal 6s, and 18 queries are queued. This looks like a heavy query or workload exhausting worker memory. I did not identify the query.
- Note's affected entities: `pod:trino-worker-0`, `pod:trino-worker-1`, `cluster:trino-lakehouse`

### Wed Oct 28 05:39 - page 16 - verdict real (0.7)

- Injected: **inj-06: consumer_slow_deploy** on `orders-etl` (duration_min=136, lag_per_hour=482000, magnitude=0.642). A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-etl-consumer-0`
- Note's suspected cause: orders-etl-consumer-0 restarted at 05:39. This is its first restart in 24 hours. Consumer lag for orders-etl jumped from about 120 to 7671 in the same minute, which fits a consumer that stopped consuming. Topic traffic is normal, so load doesn't explain it. I found no benign explanation: there are no deploys, and the logs show no errors. The restart cause is not visible in the logs or events.
- Note's affected entities: `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`, `topic:orders`

### Wed Oct 28 05:39 - page 17 - verdict real (0.7)

- Injected: **inj-06: consumer_slow_deploy** on `orders-etl` (duration_min=136, lag_per_hour=482000, magnitude=0.642). A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-etl-consumer-1`
- Note's suspected cause: orders-etl-consumer-1 restarted (restart counter went from 1 to 2). At the same time, consumer group lag jumped from about 130 to 7671 messages. Topic traffic is steady and slightly below normal, so the lag is not caused by load. The restart is the likely cause, probably a crash or a rebalance. I found no OOM, Kubernetes events, error logs or deploys to confirm why.
- Note's affected entities: `pod:orders-etl-consumer-1`, `consumer_group:orders-etl`, `topic:orders`

### Wed Oct 28 20:09 - page 18 - verdict real (0.7)

- Injected: **inj-07: consumer_slow_deploy** on `orders-etl` (duration_min=127, lag_per_hour=384000, magnitude=1.03). A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-etl-consumer-0`
- Note's suspected cause: orders-etl-consumer-0 restarted (restart count 4 to 5). At the same minute, orders-etl consumer lag jumped from about 270 to 6320 messages. Topic traffic is normal, so the lag is not a load spike. The restart appears to have caused a rebalance or a stall. The cause of the restart is not visible: there are no k8s events and no WARN or ERROR logs. A fraud-api enrichment deploy (1.8.0) was rolled back at 07:55, which probably does not explain a restart 12 hours later.
- Note's affected entities: `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`, `topic:orders`

### Wed Oct 28 20:09 - page 19 - verdict real (0.6)

- Injected: **inj-07: consumer_slow_deploy** on `orders-etl` (duration_min=127, lag_per_hour=384000, magnitude=1.03). A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-etl-consumer-1`
- Note's suspected cause: orders-etl-consumer-1 restarted at about 20:08. This was its first restart in more than 10 hours. At the same time, orders-etl consumer-group lag jumped from about 260 to 1475 and then to 6320. Topic traffic is flat at about 1150 msg/s, so the lag is not caused by load. The logs show no error or termination reason, and there are no k8s events, so the cause of the restart is unknown.
- Note's affected entities: `pod:orders-etl-consumer-1`, `consumer_group:orders-etl`, `topic:orders`

### Wed Oct 28 21:29 - page 20 - verdict noise (0.65)

- Injected: **nothing injected** (a page-out of the quiet world)
- First alert of the page: `spark_streaming_scheduling_delay_sec|orders-stream`
- Note's suspected cause: The scheduling delay fell below its seasonal expectation, which is an improvement and not a fault. The absolute values are tiny (0.15 s against 0.36 s expected). The reading sits inside the series' own recent range of 0.02 to 0.43 s. The z-score of -9.8 comes from a very narrow seasonal baseline (±0.021), not from a real change.
- Note's affected entities: `spark_app:orders-stream`

### Thu Oct 29 18:23 - page 22 - verdict real (0.8)

- Injected: **inj-09: broker_gc_pressure** on `kafka-2` (duration_min=177, magnitude=1.01). A client fetching huge batches fills one broker's heap: long GC pauses, slow produce requests, the broker falls out of the in-sync replicas again and again.
- First alert of the page: `kafka_under_replicated_partitions|kafka-ingest`
- Note's suspected cause: The finance-reporting-consumer was launched at 18:11 and is backfilling the orders topic from offset 0 with a 500 MB fetch.max.bytes. Broker load from that backfill is the most likely reason replication is falling behind. Under-replicated partitions went from 0 to 12, and producers are already affected. I did not check broker-level CPU, disk or network, so the link to the backfill is inferred from timing only.
- Note's affected entities: `cluster:kafka-ingest`, `topic:orders`

### Fri Oct 30 11:09 - page 23 - verdict unsure (0.5)

- Injected: **inj-10: consumer_memory_leak** on `orders-etl-consumer` (duration_min=502, rate_pod0_mib_per_hour=61.6, rate_pod1_mib_per_hour=51.1). A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- First alert of the page: `container_memory_working_set_bytes|orders-etl-consumer-0`
- Note's suspected cause: Memory on orders-etl-consumer-0 has climbed steadily since the 09:13 deploy of orders-etl-consumer 1.8.1. That release adds an in-process customer-profile cache. It is either a cache warming up or an unbounded cache that will lead to an OOM. The data doesn't separate the two. Topic traffic is also up about 20% (about 600 to 730 msg/s), which may add to the growth. I did not check that traffic against its time-of-day baseline.
- Note's affected entities: `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`

### Fri Oct 30 11:44 - page 24 - verdict real (0.8)

- Injected: **inj-10: consumer_memory_leak** on `orders-etl-consumer` (duration_min=502, rate_pod0_mib_per_hour=61.6, rate_pod1_mib_per_hour=51.1). A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- First alert of the page: `container_memory_working_set_bytes|orders-etl-consumer-1`
- Note's suspected cause: The orders-etl-consumer 1.8.1 deploy at 09:13 added an in-process customer profile cache with no eviction policy. Memory on both consumer pods has climbed steadily since then and has not levelled off. The cause is the unbounded cache, not traffic: topic traffic is normal and lag is flat.
- Note's affected entities: `pod:orders-etl-consumer-1`, `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`

### Sat Oct 31 01:10 - page 25 - verdict unsure (0.55)

- Injected: **nothing injected** (a page-out of the quiet world)
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: A single run of hourly_quality_check took 5.2 min against a usual ~1.6 min. It succeeded, started on time, and finished well inside the 20 min SLA. I found no cause and no benign explanation: no related change, no k8s events, and no Airflow queue or scheduler backlog. It could be a one-off slow run or the start of a slowdown in the checks' upstream data or queries. One run isn't enough to tell which.
- Note's affected entities: `dag:hourly_quality_check`, `cluster:airflow-main`

### Sat Oct 31 01:54 - page 26 - verdict real (0.85)

- Injected: **inj-11: trino_heavy_query** on `trino-lakehouse` (duration_min=30, magnitude=1.18). One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.
- First alert of the page: `jvm_gc_time_ms_per_sec|trino-worker-1`
- Note's suspected cause: trino-worker-1 has a heap of about 96% used and is spending a large share of its time in GC (about 216 ms/s, 27x expected). Cluster queries are now failing and slow. This looks like memory exhaustion on the worker, probably from a heavy query or a leak. I found no deploy or maintenance that explains it.
- Note's affected entities: `pod:trino-worker-1`, `cluster:trino-lakehouse`

### Sat Oct 31 01:54 - page 27 - verdict real (0.85)

- Injected: **inj-11: trino_heavy_query** on `trino-lakehouse` (duration_min=30, magnitude=1.18). One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.
- First alert of the page: `container_cpu_usage_cores|trino-worker-0`
- Note's suspected cause: trino-worker-0 has been under memory pressure since about 01:45. Heap is at 0.93–0.97, GC time is 24x normal and CPU is 8x normal. The cluster is hurting too: failed queries went from 0 to about 5 per minute, and p95 query time went from about 3s to 48s. This looks like a heavy or runaway query, or a memory leak, exhausting the worker heap. I found no deploy or maintenance that explains it. The only changes in the last 24h were orders-etl-consumer deploys, which are unrelated.
- Note's affected entities: `pod:trino-worker-0`, `cluster:trino-lakehouse`

### Sat Oct 31 10:20 - page 28 - verdict unsure (0.5)

- Injected: **inj-12: producer_crashloop** on `orders-producer` (duration_min=37). A bad config makes orders-producer crash on startup: it restarts in a loop and no orders arrive.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-producer`
- Note's suspected cause: The orders-producer restart counter rose from 13 to 14, a single restart after 24 hours with no change. I found no cause and no benign explanation. The producer was healthy up to 10:17, apart from a few retriable NOT_LEADER_OR_FOLLOWER warnings.
- Note's affected entities: `pod:orders-producer`

### Sun Nov 01 03:40 - page 32 - verdict noise (0.9)

- Injected: **inj-13: dag_paused** on `customer_agg` (duration_min=285, lead_min=60). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:customer_agg`
- Note's suspected cause: customer_agg was deliberately paused at 02:30 UTC by m.ortiz for a warehouse table migration (chg-0021). The 03:30 run was not started because the DAG is paused, not because the scheduler failed.
- Note's affected entities: `dag:customer_agg`

### Sun Nov 01 13:14 - page 33 - verdict real (0.7)

- Injected: **inj-14: consumer_memory_leak** on `orders-etl-consumer` (duration_min=370, rate_pod0_mib_per_hour=64.9, rate_pod1_mib_per_hour=71.1). A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- First alert of the page: `container_memory_working_set_bytes|orders-etl-consumer-0`
- Note's suspected cause: Memory on both orders-etl consumers has climbed steadily since the 11:02 deploy of image 1.8.1, which added an in-process customer profile cache. The cache looks unbounded or leaking, so the pods will probably hit their memory limit and be OOM-killed. Rising topic traffic is a minor factor at most, because memory was flat until the deploy.
- Note's affected entities: `pod:orders-etl-consumer-0`, `pod:orders-etl-consumer-1`, `consumer_group:orders-etl`

### Sun Nov 01 13:14 - page 34 - verdict real (0.7)

- Injected: **inj-14: consumer_memory_leak** on `orders-etl-consumer` (duration_min=370, rate_pod0_mib_per_hour=64.9, rate_pod1_mib_per_hour=71.1). A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- First alert of the page: `container_memory_working_set_bytes|orders-etl-consumer-1`
- Note's suspected cause: Memory on both orders-etl consumer pods has climbed steadily since the 11:02 deploy of orders-etl-consumer 1.8.1, which added an in-process customer profile cache. The growth looks like an unbounded cache or a leak. Traffic rose only about 25%, and the climb began at the deploy time, so traffic doesn't explain it.
- Note's affected entities: `pod:orders-etl-consumer-1`, `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`

### Sun Nov 01 18:10 - page 35 - verdict unsure (0.5)

- Injected: **nothing injected** (a page-out of the quiet world)
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: A single hourly_quality_check run took 5.6 min against a usual ~1.6 min. It finished with state success and is well inside the 20 min SLA. I found no cause for the slowdown and no corroborating signal, and no planned change explains it.
- Note's affected entities: `dag:hourly_quality_check`

### Sun Nov 01 20:54 - page 36 - verdict unsure (0.6)

- Injected: **nothing injected** (a page-out of the quiet world)
- First alert of the page: `spark_streaming_scheduling_delay_sec|orders-stream`
- Note's suspected cause: The scheduling delay on orders-stream is 0.29s against an expected 0.20s. The alert fires only because the seasonal baseline is very narrow (±0.01). The absolute delay is tiny and within the series' own range over the last 6 hours. I found no failure signal and no benign cause such as a deploy or scheduled job, so I can't call it noise. It looks like a harmless blip, but that is not confirmed.
- Note's affected entities: `spark_app:orders-stream`


# v1 against v2

## Verdict against truth, per arm

| truth | verdict | A: agent v1, stream v1 (the first run) | B: agent v2, stream v1 (ablation) | C: agent v2, stream v2 |
|---|---|---|---|---|
| real | real | 18 | 20 | 12 |
| real | noise | 4 | 0 | 10 |
| real | unsure | 3 | 5 | 3 |
| real | no note | 0 | 0 | 0 |
| noise | real | 0 | 0 | 0 |
| noise | noise | 4 | 1 | 1 |
| noise | unsure | 0 | 3 | 3 |
| noise | no note | 0 | 0 | 0 |

| | arm A | arm B | arm C |
|---|---|---|---|
| real called noise | 4 of 25 | 0 of 25 | 10 of 25 |
| noise called real | 0 of 4 | 0 of 4 | 0 of 4 |
| real called real | 18 of 25 | 20 of 25 | 12 of 25 |
| (a) names an answer-key entity | 25 of 25 | 25 of 25 | 25 of 25 |
| (b) names the target | 23 of 25 | 25 of 25 | 23 of 25 |
| valid notes | 29 of 29 | 29 of 29 | 29 of 29 |
| cost (USD, estimate) | 1.06 | 0.80 | 0.97 |
| tool calls per page (mean) | 7.1 | 7.0 | 6.3 |
| seconds per page (mean) | 12.3 | 12.6 | 11.2 |
| budget used up (tool calls / time / tokens) | 2 / 0 / 0 | 1 / 0 / 0 | 0 / 0 / 0 |

## By evidence type (arm C)

From the v2 answer key: what the matched injection writes to besides the readings. Every injection of this schedule is a named scenario, so the `metrics only` group (generic injections) is empty here; it is reported so that a schedule with generic injections can be split the same way.

| evidence of the injection | real page-outs | real | noise | unsure |
|---|---|---|---|---|
| metrics + dag_run, logs, k8s_events, changes | 10 | 0 | 10 | 0 |
| metrics + logs | 3 | 3 | 0 | 0 |
| metrics + logs, changes | 2 | 2 | 0 | 0 |
| metrics + logs, k8s_events, changes | 10 | 7 | 0 | 3 |
| metrics only | 0 | 0 | 0 | 0 |

## Per page-out

A page-out is the same in all arms when its page id (paging time and entity) is the same.

| time (UTC) | page | truth | injection | scenario | A | B | C | C cites |
|---|---|---|---|---|---|---|---|---|
| Oct 26 03:02 | 1 | real | inj-01 | broker_gc_pressure | real (0.8) | real (0.6) | real (0.8) | query_metric, recent_changes |
| Oct 26 08:07 | 2 | real | inj-02 | producer_crashloop | noise (0.65) | unsure (0.5) | unsure (0.5) | get_page, k8s_events, query_metric, recent_changes, recent_logs |
| Oct 26 18:15 | 5 | real | inj-03 | dag_paused | real (0.6) | real (0.6) | noise (0.9) | dag_runs, get_page, recent_changes |
| Oct 26 19:15 | 6 | real | inj-03 | dag_paused | real (0.8) | real (0.7) | noise (0.9) | dag_runs, recent_changes |
| Oct 26 20:15 | 7 | real | inj-03 | dag_paused | unsure (0.55) | real (0.65) | noise (0.9) | dag_runs, recent_changes |
| Oct 26 21:15 | 8 | real | inj-03 | dag_paused | real (0.75) | real (0.8) | noise (0.9) | dag_runs, recent_changes |
| Oct 26 22:15 | 9 | real | inj-03 | dag_paused | real (0.75) | real (0.75) | noise (0.85) | dag_runs, get_page, recent_changes |
| Oct 26 23:15 | 10 | real | inj-03 | dag_paused | real (0.75) | unsure (0.5) | noise (0.85) | dag_runs, get_page, recent_changes |
| Oct 27 00:15 | 11 | real | inj-03 | dag_paused | real (0.75) | real (0.75) | noise (0.85) | dag_runs, recent_changes |
| Oct 27 01:15 | 12 | real | inj-03 | dag_paused | noise (0.85) | real (0.6) | noise (0.85) | dag_runs, get_page, recent_changes |
| Oct 27 02:15 | 13 | real | inj-03 | dag_paused | real (0.8) | real (0.8) | noise (0.85) | dag_runs, recent_changes |
| Oct 27 19:14 | 14 | real | inj-05 | trino_heavy_query | real (0.85) | real (0.8) | real (0.85) | get_page, k8s_events, query_metric, recent_changes |
| Oct 28 05:39 | 16 | real | inj-06 | consumer_slow_deploy | real (0.6) | real (0.7) | real (0.7) | get_page, k8s_events, kafka_lag, query_metric, recent_changes |
| Oct 28 05:39 | 17 | real | inj-06 | consumer_slow_deploy | real (0.7) | real (0.7) | real (0.7) | get_page, k8s_events, kafka_lag, recent_changes |
| Oct 28 20:09 | 18 | real | inj-07 | consumer_slow_deploy | real (0.6) | real (0.7) | real (0.7) | get_page, k8s_events, kafka_lag, query_metric, recent_changes |
| Oct 28 20:09 | 19 | real | inj-07 | consumer_slow_deploy | real (0.7) | real (0.65) | real (0.6) | k8s_events, kafka_lag, query_metric, recent_changes, recent_logs |
| Oct 28 21:29 | 20 | noise | - | - | noise (0.92) | noise (0.6) | noise (0.65) | get_page, query_metric, recent_changes |
| Oct 29 18:23 | 22 | real | inj-09 | broker_gc_pressure | real (0.8) | real (0.7) | real (0.8) | k8s_events, query_metric, recent_changes |
| Oct 30 11:09 | 23 | real | inj-10 | consumer_memory_leak | noise (0.65) | unsure (0.5) | unsure (0.5) | k8s_events, kafka_lag, query_metric, recent_changes, recent_logs |
| Oct 30 11:44 | 24 | real | inj-10 | consumer_memory_leak | real (0.6) | real (0.6) | real (0.8) | kafka_lag, query_metric, recent_changes, recent_logs |
| Oct 31 01:10 | 25 | noise | - | - | noise (0.7) | unsure (0.55) | unsure (0.55) | dag_runs, k8s_events, query_metric, recent_changes |
| Oct 31 01:54 | 26 | real | inj-11 | trino_heavy_query | real (0.75) | real (0.88) | real (0.85) | get_page, k8s_events, query_metric, recent_changes |
| Oct 31 01:54 | 27 | real | inj-11 | trino_heavy_query | real (0.72) | real (0.85) | real (0.85) | get_page, k8s_events, query_metric, recent_changes, recent_logs |
| Oct 31 10:20 | 28 | real | inj-12 | producer_crashloop | unsure (0.5) | unsure (0.5) | unsure (0.5) | get_page, k8s_events, query_metric, recent_changes, recent_logs |
| Nov 01 03:40 | 32 | real | inj-13 | dag_paused | real (0.65) | real (0.6) | noise (0.9) | dag_runs, get_page, recent_changes |
| Nov 01 13:14 | 33 | real | inj-14 | consumer_memory_leak | unsure (0.5) | real (0.6) | real (0.7) | k8s_events, kafka_lag, query_metric, recent_changes |
| Nov 01 13:14 | 34 | real | inj-14 | consumer_memory_leak | noise (0.6) | unsure (0.5) | real (0.7) | k8s_events, kafka_lag, query_metric, recent_changes |
| Nov 01 18:10 | 35 | noise | - | - | noise (0.7) | unsure (0.5) | unsure (0.5) | dag_runs, k8s_events, query_metric, recent_changes |
| Nov 01 20:54 | 36 | noise | - | - | noise (0.9) | unsure (0.6) | unsure (0.6) | get_page, kafka_lag, query_metric, recent_changes |

Real page-outs whose verdict differs between A and C: 13.

## The first run's mistakes, again

The four real page-outs arm A called noise, and what B and C say:

| time (UTC) | page | scenario | A | B | C | C's suspected cause |
|---|---|---|---|---|---|---|
| Oct 26 08:07 | 2 | producer_crashloop | noise (0.65) | unsure (0.5) | unsure (0.5) | orders-producer's restart counter went from 3 to 4, which is the first restart in the streamed history. The pod is healthy afterwards and producing normally. I found no cause (no k8s events, no OOM, no errors) and no benign explanation (no deploy or change touching the producer). The producer logged 5 NOT_LEADER_OR_FOLLOWER warnings in the last 2 hours, which could point to a broker leadership change, but I did not confirm that. |
| Oct 27 01:15 | 12 | dag_paused | noise (0.85) | real (0.6) | noise (0.85) | hourly_quality_check was deliberately paused by m.ortiz at 17:23 UTC on 10-26 for a warehouse table migration. The missing runs are expected, and the start_late alert fired because of the pause. No run has started since the pause. |
| Oct 30 11:09 | 23 | consumer_memory_leak | noise (0.65) | unsure (0.5) | unsure (0.5) | Memory on orders-etl-consumer-0 has climbed steadily since the 09:13 deploy of orders-etl-consumer 1.8.1. That release adds an in-process customer-profile cache. It is either a cache warming up or an unbounded cache that will lead to an OOM. The data doesn't separate the two. Topic traffic is also up about 20% (about 600 to 730 msg/s), which may add to the growth. I did not check that traffic against its time-of-day baseline. |
| Nov 01 13:14 | 34 | consumer_memory_leak | noise (0.6) | unsure (0.5) | real (0.7) | Memory on both orders-etl consumer pods has climbed steadily since the 11:02 deploy of orders-etl-consumer 1.8.1, which added an in-process customer profile cache. The growth looks like an unbounded cache or a leak. Traffic rose only about 25%, and the climb began at the deploy time, so traffic doesn't explain it. |
