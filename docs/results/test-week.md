# Test-week result (the pre-registered run)

- Run at (UTC): 2026-11-11 18:52:18
- Commit: ee06888940d5d29ab51ed5182b99f0e3b41aff24
- Tag pre-test-freeze: 770fab8db3795af42d48a1147c355ea67ce23341
- Frozen code and config identical to the tag: True
- Working tree clean for the frozen paths: True
- docs/config-frozen.md matches the code: True
- Protocol: docs/test-protocol.md. Output below is verbatim from `scripts/run_test_week.py`.

```
Split: test (2026-10-19 to 2026-10-26, 7 days). Labeled incidents: 11, of which actionable: 10.
Raw alerts of the final system in the window: 1445; demoted by the frozen feedback: 244.
Frozen demoted groups (learned from all train labels): [('seasonal', 'cpu_throttling'), ('seasonal fallback', 'jvm_gc')]

Summary. Recall = actionable incidents detected, with a Wilson 95% interval. For (1)-(4) an incident is detected by
its first matching alert; for (5) by a PAGING page that matches it, timed at the moment that page pages.
Per-alert precision and alerts per day are after the scorer's 30-minute dedup; for (5), of alerts not demoted.

                                             system alerts_per_day alert_precision   recall recall_95%_CI median_ttd_min max_ttd_min pages false_pages_per_day pages_per_labeled_incident merged_incidents
                            (1) naive full baseline          124.6           0.256 10 of 10     0.72-1.00           10.5       190.0     -                   -                          -                -
              (2) seasonal everywhere (first build)           87.6           0.294  9 of 10     0.60-0.98           10.0       405.0     -                   -                          -                -
                                   (3) final alerts           38.6           0.742  9 of 10     0.60-0.98           13.0       405.0     -                   -                          -                -
                (4) final alerts + correlator pages           38.6           0.742  9 of 10     0.60-0.98           13.0       405.0    26                1.43                        1.6                0
(5) final + correlator + frozen feedback  [PRIMARY]           32.0            0.79  9 of 10     0.60-0.98           10.0       405.0    24                1.14                        1.6                0

Pages (systems 4 and 5):
     system  pages  TRUE_pages  FALSE_pages  false_pages_per_day  pages_per_labeled_incident  merged_incidents
        (4)     26          15           10                 1.43                         1.6                 0
(5) PRIMARY     24          15            8                 1.14                         1.6                 0
Actionable incidents that do not page in (5): INC-015

Per labeled incident: time to detect in minutes (blank = missed) and number of pages
                              scenario  actionable  ttd_(1)  ttd_(2)  ttd_(3)=(4)  ttd_(5)  pages_(4)  pages_(5)
incident_id                                                                                                     
INC-013                    memory_leak        True    190.0    405.0        405.0    405.0          2          2
INC-014             kafka_consumer_lag        True     24.0     24.0         19.0     -1.0          1          1
INC-015               trino_saturation        True      0.0      0.0                                0          0
INC-016             spark_executor_oom        True     11.0     11.0         13.0     13.0          2          2
INC-017           crashloop_bad_config        True      0.0      0.0          0.0      0.0          1          1
INC-018                  traffic_burst       False     10.0     10.0         10.0     10.0          1          1
INC-019                spark_data_skew        True     25.6     30.6         30.6     30.6          4          4
INC-020                scheduler_stall        True     10.0     10.0         10.0     10.0          1          1
INC-021            dag_late_or_missing        True    144.8                  70.0     70.0          2          2
INC-022      flink_checkpoint_failures        True      0.0      0.0          0.0      0.0          1          1
INC-023             broker_gc_pressure        True     10.0     10.0          7.0      7.0          1          1

EXPLORATORY, not the primary result: online feedback that keeps learning during the period
                         system  recall recall_95%_CI  median_ttd_min  max_ttd_min  pages  FALSE_pages  false_pages_per_day  pages_per_labeled_incident  merged_incidents
(5x) online-continuing feedback 9 of 10     0.60-0.98            10.0        405.0     24            8                 1.14                         1.6                 0
Actionable incidents that do not page in (5x): INC-015
```
