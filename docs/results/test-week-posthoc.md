# POST-HOC, not the pre-registered result

The pre-registered result is `docs/results/test-week.md`. It is unchanged and is the only result to quote.

This file was written on 2026-11-11, AFTER that result was seen, to explain it. It comes from
`python scripts/posthoc_test_week.py`, a read-only script on branch `posthoc-test`. No detector, router or
config code was changed, nothing was re-tuned and the frozen runner was not run again. The script rebuilds the
same alerts with the frozen runner's own functions and checks that they reproduce the pre-registered numbers
before printing anything. Test-week labels are read here; that is allowed only because the evaluation is over.

## What it shows

**a. INC-015 (trino_saturation), missed by systems 3-5.**
- Systems 1 and 2 matched it on two series: `trino_failed_queries_per_min` (first alert at 0 min) and
  `trino_queued_queries` (first alert at 15 min). On both, expected was 0 and the wobble was 0.
- The 0-minute detection was a reading of 0.2 failed queries per minute against an expected 0. That series
  never rose more than 0.6 above expected during the incident.
- `trino_queued_queries` did rise, by up to 21.8 queries. In the final system both series have a minimum
  change of 6 units (the count-like unit rule, absolute floor 1.0).
- Which rule stopped them: for `trino_queued_queries`, the minimum change. With it, only 2 of the 9 readings
  in the window are hot, and persistence needs 3 of 5; without it (day_type history alone) the series alerts.
  For `trino_failed_queries_per_min`, either change alone stops it: day_type history, and separately the
  minimum change.
- The label's other key signal, `trino_query_p95_sec`, matched no alert in systems 1 or 2.

**b. INC-013 (memory_leak), detected after 405 minutes.**
- The incident is labeled from 00:00 on Oct 19, the first minute of the test week, on the two Flink
  taskmanagers, whose memory starts at 63-64% of its limit and reaches 100%.
- The final system's first alert is the seasonal fallback on `flink-taskmanager-1` memory at 405 min, when
  memory was 1.3 times expected. The wobble there is set by the 5% floor, so a reading must be 30% above
  expected to be hot. Memory was already 10% above expected at 55 min.
- System 1, which has no floor, alerted on the same series at 190 min.
- EXPLORATORY: the saturation forecast, which is off in the frozen system, would first have alerted at
  800 min on that series and at 1475 min on `flink-taskmanager-0`. It would not have been faster.

**c. Time to detect, like for like.**
- The pre-registered table timed systems 1-4 after the scorer's 30-minute dedup but system 5 on raw alerts.
  On the same footing system 5 is identical to system 3 for every incident: median 13.0 min and maximum 405
  with the scorer's timing, median 10.0 and maximum 405 on raw alerts. Feedback changed no time to detect.
  The 13.0 against 10.0 in the pre-registered table is this difference in timing, not an effect of feedback.
- INC-014: the first raw alerts matching it are at -1 min (16:15 against a labeled start of 16:16), on
  consumer-0 throttling (demoted in system 5), consumer-0 CPU and the consumer lag. The scorer shows 19 min
  because it keeps one alert per target per 30 minutes and these series were already alerting: 24 raw alerts
  in the hour before its window opens, all classed false.

**d. False alerts per system.** 641, 426, 67 for systems 1-3; 10 false pages for system 4 (67 false alerts);
8 false pages for system 5 (45 false alerts). The predicted ordering 5 <= 4 < 3 < 2 < 1 holds.

## Script output, verbatim

```
POST-HOC, not the pre-registered result. Written after docs/results/test-week.md was seen; read-only.

Same alerts as the pre-registered run (precision of systems 1, 2, 3, 5; pages and false pages of 4, 5): True

==============================================================================================================
a. INC-015 (trino_saturation): detected by systems 1 and 2, missed by systems 3-5
==============================================================================================================
Labeled window: 10-20 21:00 to 10-20 21:35 UTC (35 min). Affected entities: cluster:trino-lakehouse, resource_group:adhoc.
Key-signal series in the label: trino_query_p95_sec|trino-lakehouse, trino_queued_queries|trino-lakehouse

System 1 (naive): 9 raw alerts matching INC-015 on 2 series
                                      series first_alert  min_from_start  raw_alerts  z_at_alert  value  expected  wobble       peak_z  peak_value
trino_failed_queries_per_min|trino-lakehouse 10-20 21:00             0.0           6 200000000.0    0.2       0.0     0.0 6.000000e+08         0.8
        trino_queued_queries|trino-lakehouse 10-20 21:15            15.0           3 400000000.0    0.4       0.0     0.0 2.180000e+10        22.2

System 2 (first build): 9 raw alerts matching INC-015 on 2 series
                                      series first_alert  min_from_start  raw_alerts  z_at_alert  value  expected  wobble  peak_z  peak_value
trino_failed_queries_per_min|trino-lakehouse 10-20 21:00             0.0           6         inf    0.2       0.0     0.0     inf         0.2
        trino_queued_queries|trino-lakehouse 10-20 21:15            15.0           3         inf    0.4       0.0     0.0     inf        20.6

How the final router treats each of those series:
                                      series        unit    catalog_detector     rule  fallback  history  min_change  abs_floor      min_change_from
trino_failed_queries_per_min|trino-lakehouse queries/min   seasonal_baseline seasonal     False day_type         6.0        1.0 count-like unit rule
        trino_queued_queries|trino-lakehouse     queries saturation_forecast seasonal      True day_type         6.0        1.0 count-like unit rule

Which rule stops each series in system 3? The same seasonal detector, four settings, inside the
incident window (start - 5 min .. end). A = first build (previous_days history, no absolute floor);
B = day_type history only; C = the router's absolute floor only; D = final system (both).
largest_rise = the largest value - expected under the final history, to compare with min_change.
                                      series  min_change  largest_rise wobble_set_by_(D) A: hot/scored A: max|z|  A: alerts B: hot/scored B: max|z|  B: alerts C: hot/scored  C: max|z|  C: alerts D: hot/scored  D: max|z|  D: alerts
trino_failed_queries_per_min|trino-lakehouse         6.0           0.6    absolute floor           4/9       inf          6           3/9       inf          0           0/9        0.6          0           0/9        0.6          0
        trino_queued_queries|trino-lakehouse         6.0          21.8    absolute floor           5/9       inf          3           5/9       inf          2           2/9       21.8          0           2/9       21.8          0

Verdict per series:
  trino_failed_queries_per_min|trino-lakehouse: stopped by either change alone: day_type history, and separately the minimum change
  trino_queued_queries|trino-lakehouse: stopped by the minimum change (6 units; absolute floor 1): day_type history alone still alerts

==============================================================================================================
b. INC-013 (memory_leak): first alert vs. incident start; saturation forecast (EXPLORATORY)
==============================================================================================================
Labeled window: 10-19 00:00 to 10-20 08:00 UTC (32.0 h). Affected entities: flink_job:clickstream-enrich, pod:flink-taskmanager-1, pod:flink-taskmanager-0.

Final system (3): 404 raw alerts match the incident. First alert per series:
                                                      series          detector first_alert  min_from_start  raw_alerts                                                                                     explanation
      container_memory_working_set_bytes|flink-taskmanager-1 seasonal fallback 10-19 06:45           405.0         188 container_memory_working_set_bytes (flink-taskmanager-1) is 1.3x expected for this time of day 
      container_memory_working_set_bytes|flink-taskmanager-0 seasonal fallback 10-19 10:35           635.0         205 container_memory_working_set_bytes (flink-taskmanager-0) is 1.3x expected for this time of day 
                 flink_job_restarts_total|clickstream-enrich      counter_rule 10-19 17:22          1042.0           2                                flink_job_restarts_total for clickstream-enrich rose from 7 to 8
kube_pod_container_status_restarts_total|flink-taskmanager-1      counter_rule 10-19 17:22          1042.0           1               kube_pod_container_status_restarts_total for flink-taskmanager-1 rose from 1 to 2
                 flink_backpressure_ratio|clickstream-enrich seasonal fallback 10-19 17:35          1055.0           7 flink_backpressure_ratio (clickstream-enrich) is 3.4x expected for this time of day (0.284 vs 0
kube_pod_container_status_restarts_total|flink-taskmanager-0      counter_rule 10-20 03:53          1673.0           1               kube_pod_container_status_restarts_total for flink-taskmanager-0 rose from 3 to 4

System 1 (naive), first alert per series, for comparison:
                                                              first_alert  min_from_start
target                                                                                   
container_memory_working_set_bytes|flink-taskmanager-1        10-19 03:10           190.0
container_memory_working_set_bytes|flink-taskmanager-0        10-19 05:30           330.0
flink_job_restarts_total|clickstream-enrich                   10-19 17:22          1042.0
kube_pod_container_status_restarts_total|flink-taskmanager-1  10-19 17:22          1042.0
flink_backpressure_ratio|clickstream-enrich                   10-19 17:35          1055.0
kube_pod_container_status_restarts_total|flink-taskmanager-0  10-20 03:53          1673.0

EXPLORATORY ONLY: the saturation forecast is OFF in the frozen system. With its defaults (window 60 min,
horizon 4 h, R2 >= 0.9, usage >= 0.5) on the memory series of the affected pods, limits from the topology:
                                         memory_series  limit_GiB  usage_at_start  max_usage  seasonal_first_alert_min  forecast_first_alert_min  forecast_alerts                                                                    forecast_says
container_memory_working_set_bytes|flink-taskmanager-1        4.0            0.64        1.0                     405.0                     800.0               50 memory rising ~95 MB/h; projected to reach its 4.0 GiB limit in ~3.5 h (R2=0.97)
container_memory_working_set_bytes|flink-taskmanager-0        4.0            0.63        1.0                     635.0                    1475.0               32 memory rising ~64 MB/h; projected to reach its 4.0 GiB limit in ~2.7 h (R2=0.94)

What the final system's seasonal detector saw on those memory series (frozen settings, threshold 6):
                                         memory_series  first_hot_reading_min  value/expected_then  wobble/expected_then wobble_set_by rise_needed_to_be_hot  first_reading_10%_above_expected_min
container_memory_working_set_bytes|flink-taskmanager-1                  395.0                1.306                  0.05      5% floor                   30%                                  55.0
container_memory_working_set_bytes|flink-taskmanager-0                  625.0                1.302                  0.05      5% floor                   30%                                 190.0

==============================================================================================================
c. Time to detect of system 5 on the same footing as systems 1-4; the INC-014 raw alert
==============================================================================================================
The pre-registered table timed systems 1-4 with the scorer after its 30-minute dedup, but system 5 on raw
alerts (the moment its page pages). 'dedup' columns below use the scorer's timing for both; 'raw' columns
use the page timing for both. System 5 = the alerts that are not demoted.
                              scenario  actionable  (3)=(4) dedup  (5) dedup  (4) raw  (5) raw, as pre-registered
incident_id                                                                                                      
INC-013                    memory_leak        True          405.0      405.0    405.0                       405.0
INC-014             kafka_consumer_lag        True           19.0       19.0     -1.0                        -1.0
INC-015               trino_saturation        True                                                               
INC-016             spark_executor_oom        True           13.0       13.0     13.0                        13.0
INC-017           crashloop_bad_config        True            0.0        0.0      0.0                         0.0
INC-018                  traffic_burst       False           10.0       10.0     10.0                        10.0
INC-019                spark_data_skew        True           30.6       30.6     30.6                        30.6
INC-020                scheduler_stall        True           10.0       10.0     10.0                        10.0
INC-021            dag_late_or_missing        True           70.0       70.0     70.0                        70.0
INC-022      flink_checkpoint_failures        True            0.0        0.0      0.0                         0.0
INC-023             broker_gc_pressure        True            7.0        7.0      7.0                         7.0

Over the actionable incidents:
                            detected  median_ttd_min  max_ttd_min
(3)=(4) dedup                    9.0            13.0        405.0
(5) dedup                        9.0            13.0        405.0
(4) raw                          9.0            10.0        405.0
(5) raw, as pre-registered       9.0            10.0        405.0

Incidents where system 5 differs from system 3 on the same (dedup) timing: none

INC-014 (kafka_consumer_lag): labeled start 10-20 16:16:00 UTC. Earliest raw alerts that match it:
       time  min_from_start                                              series          detector demoted_in_(5)                                                                      explanation
10-20 16:15            -1.0 container_cpu_throttled_ratio|orders-etl-consumer-0          seasonal            yes container_cpu_throttled_ratio (orders-etl-consumer-0) is 190.4x expected for thi
10-20 16:15            -1.0     container_cpu_usage_cores|orders-etl-consumer-0          seasonal                container_cpu_usage_cores (orders-etl-consumer-0) is 1.9x expected for this time
10-20 16:15            -1.0                  kafka_consumergroup_lag|orders-etl seasonal fallback                kafka_consumergroup_lag (orders-etl) is 89.6x expected for this time of day (2.4
10-20 16:20             4.0 container_cpu_throttled_ratio|orders-etl-consumer-0          seasonal            yes container_cpu_throttled_ratio (orders-etl-consumer-0) is 190.4x expected for thi
10-20 16:20             4.0     container_cpu_usage_cores|orders-etl-consumer-0          seasonal                container_cpu_usage_cores (orders-etl-consumer-0) is 1.9x expected for this time
10-20 16:20             4.0                  kafka_consumergroup_lag|orders-etl seasonal fallback                kafka_consumergroup_lag (orders-etl) is 89.6x expected for this time of day (2.4
10-20 16:25             9.0 container_cpu_throttled_ratio|orders-etl-consumer-0          seasonal            yes container_cpu_throttled_ratio (orders-etl-consumer-0) is 133.7x expected for thi
10-20 16:25             9.0     container_cpu_usage_cores|orders-etl-consumer-0          seasonal                container_cpu_usage_cores (orders-etl-consumer-0) is 1.9x expected for this time

Why the scorer times INC-014 at 19.0 min: it keeps one alert per target per 30 minutes, and these series were
already alerting before the labeled start. Raw alerts from 60 min before to 25 min after the start; * = kept by
the scorer's dedup; the scorer's window for the incident opens at -5 min:
  container_cpu_throttled_ratio|orders-etl-consumer-0
      minutes from start: -56 -51 -46 -41 -36* -31 -26 -21 -16 -11 -6* -1 +4 +9 +14 +19 +24*
  container_cpu_usage_cores|orders-etl-consumer-0
      minutes from start: -56 -51 -46 -41 -36* -31 -26 -21 -16 -11 -6* -1 +4 +9 +14 +19 +24*
  kafka_consumergroup_lag|orders-etl
      minutes from start: -11* -6 -1 +4 +9 +14 +19* +24
  Raw alerts on these series in the hour before the scorer's window opens: 24 (scorer class: {'false_positive': 24})

==============================================================================================================
d. False-alert counts per system (alerts after the scorer's 30-minute dedup; pages from the correlator)
==============================================================================================================
                                  system alerts true_alerts expected_alerts false_alerts false_alerts_per_day pages false_pages
                 (1) naive full baseline    872         221              10          641                 91.6  None        None
   (2) seasonal everywhere (first build)    613         177              10          426                 60.9  None        None
                        (3) final alerts    270         193              10           67                  9.6  None        None
     (4) final alerts + correlator pages    270         193              10           67                  9.6    26          10
(5) final + correlator + frozen feedback    224         169              10           45                  6.4    24           8

Predicted ordering on false volume, 5 <= 4 < 3 < 2 < 1 (systems 1-3 count alerts, 4-5 count pages):
  8 <= 10 < 67 < 426 < 641: holds

POST-HOC. Nothing here replaces docs/results/test-week.md, and nothing was changed or re-tuned.
```
