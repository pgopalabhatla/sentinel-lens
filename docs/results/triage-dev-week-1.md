# Triage agents on live-week-1 (DEVELOPMENT SET)

**This is a development-set result, not a held-out one.** The live week had been run, scored and read before the agent existed, and the agent's prompt and tools were written in the same session. It says whether the pipeline works and what kind of mistakes it makes; a held-out number needs a further world.

- written by `scripts/triage_dev_week.py` at commit `5890cfd`; notes from the topic `live-week-1-triage`
- agent: `claude-sonnet-5-5`, effort `medium`, server-side fallback `default`; budget per page 15 tool calls, 120 s, 200,000 tokens; one fresh agent per page-out, 3 at a time
- 29 page-outs; notes on the topic: 29; merge links: 5; this invocation investigated 29 page-outs in 123 s of wall time
- the notes are advisory: nothing in the system reads them

What the agent could see (see docs/log.md, Nov 17, the evidence check): the injected failures change **readings only** (and, for a paused DAG, remove its run messages). Logs, Kubernetes events and changes are not on the stream; the tools read them from `data_live`, the quiet world, where they show normal operation during every injection (no OOMKilled, BackOff or CrashLoopBackOff event, no ERROR line, no deploy record; the paused `customer_agg` run's Spark pods even appear as scheduled and started). So for injected failures these three tools can only return nothing or contradicting evidence. The column `evidence from` shows which tools a note cites.

## Verdict against truth

Truth: a page-out is real when it matches an injection under the live-week rule, noise otherwise.

| truth \ verdict | real | noise | unsure | no note |
|---|---|---|---|---|
| real (25) | 18 | 4 | 3 | 0 |
| noise (4) | 0 | 4 | 0 | 0 |

**Real called noise: 4 of 25 real page-outs.**

| time (UTC) | page | injection | scenario | confidence | suspected cause | evidence from |
|---|---|---|---|---|---|---|
| Oct 26 08:07 | 2 | inj-02 | producer_crashloop | 0.65 | A single restart (counter 3 to 4) was the only change in 24 hours. The producer is sending batches normally and has only two transient NOT_LEADER_OR_FOLLOWER retry warnings, which look like a brief partition leader move. The alert's feedback group was also demoted. I did not find the restart's cause: there are no k8s events, and I did not check recent changes or the Kafka brokers. | get_page, k8s_events, query_metric, recent_logs |
| Oct 27 01:15 | 12 | inj-03 | dag_paused | 0.85 | The DAG appears to be running on time. The streamed DAG-run feed has no run records after 17:05, so the start_late alert looks like a gap in that feed and not a missed run. The scheduler log shows the 01:05 run_checks task was sent to the executor at 01:05:09, and Airflow recorded a task completion at 01:06, as it did at 00:07. | dag_runs, get_page, query_metric, recent_changes, recent_logs |
| Oct 30 11:09 | 23 | inj-10 | consumer_memory_leak | 0.65 | Memory on orders-etl-consumer-0 is rising with a traffic ramp on the orders topic. Inflow went from about 570 to 727 msg/s since 09:10, and memory went from about 425 MB to 556 MB over the same period. The consumer is keeping up, and the seasonal baseline alert did not account for the ramp. | get_page, k8s_events, kafka_lag, query_metric |
| Nov 01 13:14 | 34 | inj-14 | consumer_memory_leak | 0.6 | Memory on both orders-etl consumers has risen from about 415MB to about 570MB since about 11:00. This matches a rise in the orders topic in-rate from about 520 to about 650 msg/s. It looks like load-driven buffer growth, not a leak. Consumer lag is flat at about 110-130 messages, and there are no restarts, OOM kills, warnings or changes. | k8s_events, kafka_lag, query_metric, recent_logs |

Noise called real: 0 of 4 noise page-outs.

## Does the note name the injected target?

Over the real page-outs. (a) any of the entities the answer key lists for the matched injection (the owners of the series it changed, plus the DAG for a pause); (b) the scenario's named target itself (an entity with that name, or a pod named `<target>-<n>`).

| real page-outs | (a) names an answer-key entity | (b) names the target |
|---|---|---|
| 25 | 25 | 23 |

## Cost and time

| | total | per page: mean | median | max |
|---|---|---|---|---|
| cost (USD, estimate) | 1.058 | 0.0365 | 0.0339 | 0.0674 |
| time (s, one investigation) | 356 | 12.3 | 11.5 | 25.8 |
| tool calls | 205 | 7.1 | - | 15 |

Tokens over all investigations: 1,155,364. Notes: 29 of 29 page-outs, 29 valid. Budget used up: 2 by tool calls, 0 by time, 0 by tokens.

## Per page-out

| time (UTC) | page | truth | injection | verdict | conf. | affected entities | (a) | (b) | tool calls | s | USD | evidence from |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Oct 26 03:02 | 1 | real | inj-01 | real | 0.8 | cluster:kafka-ingest, pod:kafka-1, topic:orders, topic:clicks | yes | yes | 10 | 13.73 | 0.04284 | query_metric, recent_changes |
| Oct 26 08:07 | 2 | real | inj-02 | noise | 0.65 | pod:orders-producer | yes | yes | 4 | 10.49 | 0.0239 | get_page, k8s_events, query_metric, recent_logs |
| Oct 26 18:15 | 5 | real | inj-03 | real | 0.6 | dag:hourly_quality_check, cluster:airflow-main | yes | yes | 8 | 15.89 | 0.0395 | dag_runs, k8s_events, query_metric, recent_changes |
| Oct 26 19:15 | 6 | real | inj-03 | real | 0.8 | dag:hourly_quality_check | yes | yes | 6 | 8.59 | 0.02796 | dag_runs, get_page, k8s_events, recent_changes |
| Oct 26 20:15 | 7 | real | inj-03 | unsure | 0.55 | dag:hourly_quality_check, cluster:airflow-main | yes | yes | 8 | 17.89 | 0.05003 | dag_runs, query_metric, recent_changes |
| Oct 26 21:15 | 8 | real | inj-03 | real | 0.75 | dag:hourly_quality_check, cluster:airflow-main | yes | yes | 6 | 11.49 | 0.03318 | dag_runs, query_metric, recent_changes |
| Oct 26 22:15 | 9 | real | inj-03 | real | 0.75 | dag:hourly_quality_check | yes | yes | 4 | 7.73 | 0.02701 | dag_runs, get_page, recent_changes |
| Oct 26 23:15 | 10 | real | inj-03 | real | 0.75 | dag:hourly_quality_check, cluster:airflow-main | yes | yes | 6 | 11.59 | 0.03398 | dag_runs, get_page, query_metric, recent_changes |
| Oct 27 00:15 | 11 | real | inj-03 | real | 0.75 | dag:hourly_quality_check, cluster:airflow-main | yes | yes | 7 | 11.99 | 0.03498 | dag_runs, k8s_events, query_metric, recent_changes |
| Oct 27 01:15 | 12 | real | inj-03 | noise | 0.85 | dag:hourly_quality_check, cluster:airflow-main | yes | yes | 11 | 19.25 | 0.05561 | dag_runs, get_page, query_metric, recent_changes, recent_logs |
| Oct 27 02:15 | 13 | real | inj-03 | real | 0.8 | dag:hourly_quality_check | yes | yes | 4 | 7.62 | 0.02623 | dag_runs, get_page, recent_changes |
| Oct 27 19:14 | 14 | real | inj-05 | real | 0.85 | pod:trino-worker-0, pod:trino-worker-1, cluster:trino-lakehouse | yes | yes | 5 | 9.64 | 0.03208 | get_page, query_metric, recent_changes |
| Oct 28 05:39 | 16 | real | inj-06 | real | 0.6 | pod:orders-etl-consumer-0, consumer_group:orders-etl, topic:orders | yes | yes | 11 | 14.4 | 0.04665 | k8s_events, query_metric, recent_changes, recent_logs |
| Oct 28 05:39 | 17 | real | inj-06 | real | 0.7 | pod:orders-etl-consumer-1, consumer_group:orders-etl, topic:orders | yes | yes | 10 | 13.34 | 0.04438 | k8s_events, kafka_lag, query_metric, recent_changes, recent_logs |
| Oct 28 20:09 | 18 | real | inj-07 | real | 0.6 | pod:orders-etl-consumer-0, consumer_group:orders-etl, topic:orders | yes | yes | 9 | 13.02 | 0.04279 | get_page, k8s_events, query_metric, recent_logs |
| Oct 28 20:09 | 19 | real | inj-07 | real | 0.7 | pod:orders-etl-consumer-1, consumer_group:orders-etl, topic:orders | yes | yes | 15 | 25.8 | 0.0674 | get_page, k8s_events, kafka_lag, query_metric, recent_logs |
| Oct 28 21:29 | 20 | noise | - | noise | 0.92 | spark_app:orders-stream | - | - | 8 | 17.2 | 0.03394 | get_page, query_metric |
| Oct 29 18:23 | 22 | real | inj-09 | real | 0.8 | cluster:kafka-ingest, pod:kafka-0, pod:kafka-1, pod:kafka-2, topic:orders, topic:clicks | yes | yes | 11 | 13.53 | 0.04317 | query_metric, recent_changes |
| Oct 30 11:09 | 23 | real | inj-10 | noise | 0.65 | pod:orders-etl-consumer-0, consumer_group:orders-etl, topic:orders | yes | yes | 4 | 9.24 | 0.02768 | get_page, k8s_events, kafka_lag, query_metric |
| Oct 30 11:44 | 24 | real | inj-10 | real | 0.6 | pod:orders-etl-consumer-1, pod:orders-etl-consumer-0, consumer_group:orders-etl | yes | yes | 15 | 19.53 | 0.0664 | k8s_events, kafka_lag, query_metric, recent_changes, recent_logs |
| Oct 31 01:10 | 25 | noise | - | noise | 0.7 | dag:hourly_quality_check | - | - | 3 | 5.6 | 0.02178 | dag_runs, get_page, recent_changes |
| Oct 31 01:54 | 26 | real | inj-11 | real | 0.75 | pod:trino-worker-1 | yes | no | 5 | 10.42 | 0.02722 | get_page, k8s_events, query_metric, recent_changes, recent_logs |
| Oct 31 01:54 | 27 | real | inj-11 | real | 0.72 | pod:trino-worker-0 | yes | no | 6 | 9.95 | 0.03026 | get_page, k8s_events, query_metric, recent_logs |
| Oct 31 10:20 | 28 | real | inj-12 | unsure | 0.5 | pod:orders-producer | yes | yes | 5 | 9.12 | 0.02786 | get_page, k8s_events, query_metric, recent_changes, recent_logs |
| Nov 01 03:40 | 32 | real | inj-13 | real | 0.65 | dag:customer_agg | yes | yes | 4 | 7.49 | 0.02263 | dag_runs, get_page, recent_changes |
| Nov 01 13:14 | 33 | real | inj-14 | unsure | 0.5 | pod:orders-etl-consumer-0, pod:orders-etl-consumer-1, consumer_group:orders-etl | yes | yes | 6 | 10.46 | 0.03401 | k8s_events, kafka_lag, query_metric, recent_changes |
| Nov 01 13:14 | 34 | real | inj-14 | noise | 0.6 | pod:orders-etl-consumer-1, pod:orders-etl-consumer-0, consumer_group:orders-etl | yes | yes | 7 | 15.96 | 0.04349 | k8s_events, kafka_lag, query_metric, recent_logs |
| Nov 01 18:10 | 35 | noise | - | noise | 0.7 | dag:hourly_quality_check | - | - | 3 | 6.21 | 0.02823 | dag_runs, get_page, recent_changes |
| Nov 01 20:54 | 36 | noise | - | noise | 0.9 | spark_app:orders-stream | - | - | 4 | 8.76 | 0.0228 | get_page, query_metric |

## Suspected cause next to what was injected (to read, not scored)

### Mon Oct 26 03:02 - page 1 - verdict real (0.8)

- Injected: **inj-01: broker_gc_pressure** on `kafka-1` (duration_min=168, magnitude=0.519). A client fetching huge batches fills one broker's heap: long GC pauses, slow produce requests, the broker falls out of the in-sync replicas again and again.
- First alert of the page: `kafka_under_replicated_partitions|kafka-ingest`
- Note's suspected cause: Under-replicated partitions on kafka-ingest jumped from 0 to 12 in about 5 minutes. At the same time produce p99 latency rose about 10x, failed produce requests started, and kafka-1 GC time rose about 10x. This looks like a broker degrading under GC pressure, with followers falling behind. I did not check kafka-0 or kafka-2.
- Note's affected entities: `cluster:kafka-ingest`, `pod:kafka-1`, `topic:orders`, `topic:clicks`

### Mon Oct 26 08:07 - page 2 - verdict noise (0.65)

- Injected: **inj-02: producer_crashloop** on `orders-producer` (duration_min=50). A bad config makes orders-producer crash on startup: it restarts in a loop and no orders arrive.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-producer`
- Note's suspected cause: A single restart (counter 3 to 4) was the only change in 24 hours. The producer is sending batches normally and has only two transient NOT_LEADER_OR_FOLLOWER retry warnings, which look like a brief partition leader move. The alert's feedback group was also demoted. I did not find the restart's cause: there are no k8s events, and I did not check recent changes or the Kafka brokers.
- Note's affected entities: `pod:orders-producer`

### Mon Oct 26 18:15 - page 5 - verdict real (0.6)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The 18:05 run of hourly_quality_check has not started 10 minutes after it was due. The previous 11 hourly runs all started within 30 seconds of schedule. The Airflow scheduler looks healthy and the queue is empty, so the cause is probably specific to this DAG (paused, import or parse error, or the run was not created) rather than a scheduler outage. I found no logged cause, so this is an inference.
- Note's affected entities: `dag:hourly_quality_check`, `cluster:airflow-main`

### Mon Oct 26 19:15 - page 6 - verdict real (0.8)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: hourly_quality_check has stopped being scheduled. The 18:05 and 19:05 runs never started, after 10 straight on-time runs (about 20 seconds late each). The cause is unknown. No changes or pod events explain it. The scheduler may be stalled, or the DAG may have been paused or broken outside the change log.
- Note's affected entities: `dag:hourly_quality_check`

### Mon Oct 26 20:15 - page 7 - verdict unsure (0.55)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG-run stream shows no hourly_quality_check start since 17:05, so the 18:05, 19:05 and 20:05 runs look missing. But the Airflow scheduler heartbeat is healthy, and task completions still appear at 18:06, 19:06 and 20:06, about a minute after each scheduled time. That fits the DAG actually running while its run events are not reaching the stream. I can't rule out a real problem, because the task completions could come from something else.
- Note's affected entities: `dag:hourly_quality_check`, `cluster:airflow-main`

### Mon Oct 26 21:15 - page 8 - verdict real (0.75)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: hourly_quality_check has not started since the 17:05 run. The 18:05, 19:05, 20:05 and 21:05 runs are all missing, so this is a sustained outage and not a single late start. The Airflow scheduler heartbeat is normal, which suggests the problem is specific to this DAG. It may be paused, or a DAG parse error or similar may have stopped it from scheduling. I could not confirm which.
- Note's affected entities: `dag:hourly_quality_check`, `cluster:airflow-main`

### Mon Oct 26 22:15 - page 9 - verdict real (0.75)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: hourly_quality_check has not started since the 17:05 run. Five scheduled runs (18:05 to 22:05) are missing, which points to the scheduler not triggering it or the DAG being paused or broken. Before 17:05 it ran every hour on time, each run taking about 1.5 minutes. No change or pause is recorded, so the cause is unknown. A gap in the telemetry stream is possible but unlikely.
- Note's affected entities: `dag:hourly_quality_check`

### Mon Oct 26 23:15 - page 10 - verdict real (0.75)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: hourly_quality_check has not started since the 17:05 run, so six consecutive hourly runs (18:05 to 23:05) are missing. The Airflow scheduler heartbeat is healthy, so the scheduler is alive and this DAG is not being scheduled. A silent pause, a DAG parse failure or a similar DAG-level problem is likely, but I did not confirm which. The telemetry could also have dropped this DAG's run events, which I can't rule out.
- Note's affected entities: `dag:hourly_quality_check`, `cluster:airflow-main`

### Tue Oct 27 00:15 - page 11 - verdict real (0.75)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: hourly_quality_check has not started since the 17:05 run on 10/26. Seven hourly runs (18:05 through 00:05) have no start. The Airflow scheduler heartbeat is healthy and no change was recorded, so the DAG was probably paused or disabled without a logged change, or its runs are being dropped. A gap in the telemetry stream is also possible.
- Note's affected entities: `dag:hourly_quality_check`, `cluster:airflow-main`

### Tue Oct 27 01:15 - page 12 - verdict noise (0.85)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: The DAG appears to be running on time. The streamed DAG-run feed has no run records after 17:05, so the start_late alert looks like a gap in that feed and not a missed run. The scheduler log shows the 01:05 run_checks task was sent to the executor at 01:05:09, and Airflow recorded a task completion at 01:06, as it did at 00:07.
- Note's affected entities: `dag:hourly_quality_check`, `cluster:airflow-main`

### Tue Oct 27 02:15 - page 13 - verdict real (0.8)

- Injected: **inj-03: dag_paused** on `hourly_quality_check` (duration_min=559, lead_min=102). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: hourly_quality_check has not started since the 17:05 run on 10-26. That is 9 straight missed hourly runs. The scheduler is working, because daily_orders started on time at 02:00. So the problem is specific to this DAG, for example it was paused, a DAG parse error, or a changed schedule or dependency. No change was recorded for it.
- Note's affected entities: `dag:hourly_quality_check`

### Tue Oct 27 19:14 - page 14 - verdict real (0.85)

- Injected: **inj-05: trino_heavy_query** on `trino-lakehouse` (duration_min=60, magnitude=0.817). One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.
- First alert of the page: `jvm_gc_time_ms_per_sec|trino-worker-0`
- Note's suspected cause: Both Trino workers have been under memory pressure since about 18:55. Heap use is near 95%, GC time is about 8x normal, and CPU is throttled at about 55%. Queries are slowing and queuing, which looks like a heavy query or load surge. No deploy or Kubernetes event explains it.
- Note's affected entities: `pod:trino-worker-0`, `pod:trino-worker-1`, `cluster:trino-lakehouse`

### Wed Oct 28 05:39 - page 16 - verdict real (0.6)

- Injected: **inj-06: consumer_slow_deploy** on `orders-etl` (duration_min=136, lag_per_hour=482000, magnitude=0.642). A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-etl-consumer-0`
- Note's suspected cause: orders-etl-consumer-0 restarted (restart count went from 3 to 4, the first restart in 12h). In the same minute, consumer group lag jumped from about 125 to 7671 messages. That fits a rebalance or restart pause, so the group probably stopped consuming for a short time. The cause of the restart is unknown: there are no K8s events, no error logs and no recent changes. It could still recover on its own.
- Note's affected entities: `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`, `topic:orders`

### Wed Oct 28 05:39 - page 17 - verdict real (0.7)

- Injected: **inj-06: consumer_slow_deploy** on `orders-etl` (duration_min=136, lag_per_hour=482000, magnitude=0.642). A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-etl-consumer-1`
- Note's suspected cause: orders-etl-consumer-1 restarted at about 05:39. This is its first restart in 24 hours, and consumer-group lag jumped from about 120 to 7671 messages in the same minute. That fits a consumer restart and rebalance, with the pod's partitions not being consumed in the meantime. The cause of the restart is not visible. Kubernetes events and ERROR logs are empty, and there were no recent changes.
- Note's affected entities: `pod:orders-etl-consumer-1`, `consumer_group:orders-etl`, `topic:orders`

### Wed Oct 28 20:09 - page 18 - verdict real (0.6)

- Injected: **inj-07: consumer_slow_deploy** on `orders-etl` (duration_min=127, lag_per_hour=384000, magnitude=1.03). A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-etl-consumer-0`
- Note's suspected cause: orders-etl-consumer-0 restarted for the second time in about 15 hours (4 to 5), and consumer-group lag jumped from about 270 to 6320 in the same minute. That fits a consumer that stopped consuming around the restart. The restart cause is unknown: there are no k8s events, no error logs and no recent changes. It is the first lag spike and the throughput drop is small so far, so it may be a brief rebalance that recovers.
- Note's affected entities: `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`, `topic:orders`

### Wed Oct 28 20:09 - page 19 - verdict real (0.7)

- Injected: **inj-07: consumer_slow_deploy** on `orders-etl` (duration_min=127, lag_per_hour=384000, magnitude=1.03). A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-etl-consumer-1`
- Note's suspected cause: orders-etl-consumer-1 restarted (restart count 2 to 3). At the same minute, group lag jumped from about 270 to 6320 messages. That fits a consumer restart followed by a rebalance or a backlog. I found no k8s events, logs or changes that explain the restart. The topic in-rate and consume rate are both steady, and memory is flat at about 420MB, so I see no OOM or resource cause.
- Note's affected entities: `pod:orders-etl-consumer-1`, `consumer_group:orders-etl`, `topic:orders`

### Wed Oct 28 21:29 - page 20 - verdict noise (0.92)

- Injected: **nothing injected** (a page-out of the quiet world)
- First alert of the page: `spark_streaming_scheduling_delay_sec|orders-stream`
- Note's suspected cause: The alert fired because scheduling delay was lower than the seasonal baseline. Lower delay is harmless. The metric is about 0.2s all evening and the stream is healthy, so the baseline was probably built from too few days (5).
- Note's affected entities: `spark_app:orders-stream`

### Thu Oct 29 18:23 - page 22 - verdict real (0.8)

- Injected: **inj-09: broker_gc_pressure** on `kafka-2` (duration_min=177, magnitude=1.01). A client fetching huge batches fills one broker's heap: long GC pauses, slow produce requests, the broker falls out of the in-sync replicas again and again.
- First alert of the page: `kafka_under_replicated_partitions|kafka-ingest`
- Note's suspected cause: Under-replicated partitions on kafka-ingest jumped from 0 to 12 in about 3 minutes, which has not happened in the last 3 hours. Produce p99 latency and failed produce requests rose at the same time, so a broker or replication problem is likely. I did not find the cause: no change or k8s event is recorded, memory is flat on the one broker I checked (kafka-1), and kafka-0 CPU throttling is normal.
- Note's affected entities: `cluster:kafka-ingest`, `pod:kafka-0`, `pod:kafka-1`, `pod:kafka-2`, `topic:orders`, `topic:clicks`

### Fri Oct 30 11:09 - page 23 - verdict noise (0.65)

- Injected: **inj-10: consumer_memory_leak** on `orders-etl-consumer` (duration_min=502, rate_pod0_mib_per_hour=61.6, rate_pod1_mib_per_hour=51.1). A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- First alert of the page: `container_memory_working_set_bytes|orders-etl-consumer-0`
- Note's suspected cause: Memory on orders-etl-consumer-0 is rising with a traffic ramp on the orders topic. Inflow went from about 570 to 727 msg/s since 09:10, and memory went from about 425 MB to 556 MB over the same period. The consumer is keeping up, and the seasonal baseline alert did not account for the ramp.
- Note's affected entities: `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`, `topic:orders`

### Fri Oct 30 11:44 - page 24 - verdict real (0.6)

- Injected: **inj-10: consumer_memory_leak** on `orders-etl-consumer` (duration_min=502, rate_pod0_mib_per_hour=61.6, rate_pod1_mib_per_hour=51.1). A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- First alert of the page: `container_memory_working_set_bytes|orders-etl-consumer-1`
- Note's suspected cause: Memory on both orders-etl consumer pods has risen steadily since about 09:00. It has not plateaued after 2.5 hours, which looks like a leak or unbounded buffer growth rather than a spike. Consumer-1 went from about 410MB to 550MB, and consumer-0 from about 420MB to 603MB. Nothing in the logs, events or changes explains it, and consumer lag and throughput are healthy so far. The memory limit is unknown, so I can't say how close to an OOM kill they are.
- Note's affected entities: `pod:orders-etl-consumer-1`, `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`

### Sat Oct 31 01:10 - page 25 - verdict noise (0.7)

- Injected: **nothing injected** (a page-out of the quiet world)
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: A single run of hourly_quality_check took 5.2 min against a baseline of about 1.6 min. It succeeded and finished well inside the 20-minute SLA. The 11 earlier runs in the 12-hour window all took 1.2–2.2 min. There were no config changes or deploys. I did not check task-level logs or upstream data volume.
- Note's affected entities: `dag:hourly_quality_check`

### Sat Oct 31 01:54 - page 26 - verdict real (0.75)

- Injected: **inj-11: trino_heavy_query** on `trino-lakehouse` (duration_min=30, magnitude=1.18). One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.
- First alert of the page: `jvm_gc_time_ms_per_sec|trino-worker-1`
- Note's suspected cause: trino-worker-1 is under sustained JVM heap pressure. Heap went from about 0.4 to 0.96 and stayed there from 01:45 to 01:54. GC time is about 27x normal and CPU is about 8.5x normal, which looks like GC thrashing. The cause is unclear: no deploy or config change, and the logs don't explain it.
- Note's affected entities: `pod:trino-worker-1`

### Sat Oct 31 01:54 - page 27 - verdict real (0.72)

- Injected: **inj-11: trino_heavy_query** on `trino-lakehouse` (duration_min=30, magnitude=1.18). One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.
- First alert of the page: `container_cpu_usage_cores|trino-worker-0`
- Note's suspected cause: trino-worker-0 has been under heap pressure since about 01:40. Heap is at about 0.92–0.97 and GC time is about 220 ms/s, up from about 9. CPU is also about 8x normal. This looks like a heavy query or a memory leak causing GC thrashing. Nothing in the changes or K8s events explains it.
- Note's affected entities: `pod:trino-worker-0`

### Sat Oct 31 10:20 - page 28 - verdict unsure (0.5)

- Injected: **inj-12: producer_crashloop** on `orders-producer` (duration_min=37). A bad config makes orders-producer crash on startup: it restarts in a loop and no orders arrive.
- First alert of the page: `kube_pod_container_status_restarts_total|orders-producer`
- Note's suspected cause: The orders-producer restart counter went from 13 to 14, which is one restart after 12 hours with no change. The producer was still logging successful batches up to 10:17, and the only warnings were occasional NOT_LEADER_OR_FOLLOWER retries, which are usually harmless leader changes. There are no k8s events or changes to explain the restart, so I can't tell whether it was a one-off or the start of a restart loop. The 13 earlier restarts suggest it has restarted before.
- Note's affected entities: `pod:orders-producer`

### Sun Nov 01 03:40 - page 32 - verdict real (0.65)

- Injected: **inj-13: dag_paused** on `customer_agg` (duration_min=285, lead_min=60). Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- First alert of the page: `dag:customer_agg`
- Note's suspected cause: customer_agg's 03:30 run has not started 10 minutes after its due time. It normally starts within about 20 seconds. The scheduler is alive, because hourly_quality_check started on time at 03:05, so the problem looks specific to this DAG. Possible causes are a pause, a blocked pool or slot, or a scheduling problem. No recent change explains it, so the cause is not confirmed.
- Note's affected entities: `dag:customer_agg`

### Sun Nov 01 13:14 - page 33 - verdict unsure (0.5)

- Injected: **inj-14: consumer_memory_leak** on `orders-etl-consumer` (duration_min=370, rate_pod0_mib_per_hour=64.9, rate_pod1_mib_per_hour=71.1). A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- First alert of the page: `container_memory_working_set_bytes|orders-etl-consumer-0`
- Note's suspected cause: Memory on both orders-etl consumers has climbed steadily since about 11:00 and shows no plateau yet. It is about 1.35x baseline, from 410MB to 556MB. The orders topic inflow rose over the same period, from about 520 to 650 msg/s, which may explain the growth. A slow leak or buffer growth cannot be ruled out. Consumer lag is normal, and there are no restarts, OOM kills or changes. I could not see the memory limit.
- Note's affected entities: `pod:orders-etl-consumer-0`, `pod:orders-etl-consumer-1`, `consumer_group:orders-etl`

### Sun Nov 01 13:14 - page 34 - verdict noise (0.6)

- Injected: **inj-14: consumer_memory_leak** on `orders-etl-consumer` (duration_min=370, rate_pod0_mib_per_hour=64.9, rate_pod1_mib_per_hour=71.1). A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- First alert of the page: `container_memory_working_set_bytes|orders-etl-consumer-1`
- Note's suspected cause: Memory on both orders-etl consumers has risen from about 415MB to about 570MB since about 11:00. This matches a rise in the orders topic in-rate from about 520 to about 650 msg/s. It looks like load-driven buffer growth, not a leak. Consumer lag is flat at about 110-130 messages, and there are no restarts, OOM kills, warnings or changes.
- Note's affected entities: `pod:orders-etl-consumer-1`, `pod:orders-etl-consumer-0`, `consumer_group:orders-etl`

### Sun Nov 01 18:10 - page 35 - verdict noise (0.7)

- Injected: **nothing injected** (a page-out of the quiet world)
- First alert of the page: `dag:hourly_quality_check`
- Note's suspected cause: A single run of hourly_quality_check took 5.6 min against a typical ~1.6 min. It finished successfully, well inside the 20 min SLA. There were no config changes or deploys, and earlier runs were normal, so this looks like a one-off slow run.
- Note's affected entities: `dag:hourly_quality_check`

### Sun Nov 01 20:54 - page 36 - verdict noise (0.9)

- Injected: **nothing injected** (a page-out of the quiet world)
- First alert of the page: `spark_streaming_scheduling_delay_sec|orders-stream`
- Note's suspected cause: The seasonal-fallback detector fired on a scheduling delay of about 0.29 s. That is 1.5x a very tight baseline built from only 5 days of data. The metric itself is normal: over the last 6 hours it stayed between 0.02 and 0.54 s with a mean of 0.24 s, and it has no trend.
- Note's affected entities: `spark_app:orders-stream`

