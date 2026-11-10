# Weekly log

## Nov 4

Seasonal + 3-of-5 on train: recall 0.7, precision 0.150, 783 false alarms (naive gauges only: 0.9 / 0.107 / 1299). The 5% floor costs INC-004 and INC-011. min_days=3 had cost INC-001 and INC-010, so the default is now 1. 624 of the false alarms come from seasonal-assigned series and 148 from saturation-assigned series.

## Nov 9

Guardrail rules, schedule watcher and router, scored on train (`python scripts/score_router.py`). Seasonal parameters unchanged (floor 5%, threshold 6, 3-of-5).

| detector | recall | precision | alerts | false alarms | median TTD (min) |
|---|---|---|---|---|---|
| naive (quickstart, full) | 1.0 | 0.117 | 1488 | 1301 | 9.5 |
| naive (gauges only) | 0.9 | 0.107 | 1469 | 1299 | 10.0 |
| seasonal everywhere | 0.7 | 0.150 | 935 | 783 | 10.0 |
| routed system | 0.8 | 0.164 | 954 | 785 | 9.0 |

Routed system, alerts by rule after the scorer's 30-minute dedup:

| rule | alerts | true | expected | false alarms |
|---|---|---|---|---|
| counter_rule | 16 | 13 | 1 | 2 |
| above_zero | 1 | 0 | 1 | 0 |
| seasonal | 659 | 28 | 7 | 624 |
| seasonal fallback | 275 | 110 | 6 | 159 |
| start_late | 1 | 1 | 0 | 0 |
| sla_breach | 0 | 0 | 0 | 0 |
| failed_run | 2 | 2 | 0 | 0 |

- Counter rule: any rise in a restart or checkpoint-failure counter at 1-minute readings is an alert with no persistence, because a restart is an event, not a noisy measurement. It cut INC-001 from 23.8 to 10.8 min and INC-006 from 10 to 0 min for 2 false alarms.
- Above-zero rule: under-replicated partitions > 0 alerts on the first reading. Its only train alert is the planned broker restart (INC-008, not actionable).
- Schedule watcher: alert at scheduled time + 10 min grace if no run has started, and at the SLA deadline if a started run has not finished. It is the only thing that sees INC-005 (paused DAG), 10 minutes after the missed 02:00 run instead of 471 minutes later when the naive detector saw the late run finish. Healthy runs all start within 0.47 min, so 10 min is about 20x the worst healthy start.
- Failed-run rule: a run with state failed alerts at its end (2 alerts, both true).
- Router: the catalog's `default_detector` picks the rule; saturation, change-point and heartbeat series fall back to seasonal (76 of 136) until their detectors exist.

Still open: recall is 0.8, not 1.0. INC-004 and INC-011 (data skew) are still missed because of the 5% floor, and INC-007 (memory leak) still takes 75 min. Precision barely moved (0.150 to 0.164) because the new rules add 19 alerts and the 783 seasonal false alarms are untouched: 624 from seasonal series, 159 from fallback series.

Caveat on INC-005: its time to detect shows as -5 min because the label starts the incident after our 02:10 alert. The scorer accepts alerts up to 5 minutes before the labelled start, so the alert only just counts.

## Nov 9, seasonal noise fixes

The diagnostic (`scripts/diagnose_noise.py`) showed 693 of the 783 seasonal false alarms come from 10 series with two causes: a weekend reading compared with a mostly-weekday history, and an expected value of 0 where the 5% floor is 0. Two fixes, compared on train (`python scripts/score_noise_fixes.py`). Counter, above-zero and schedule rules are the same in every row; threshold 6, 5% floor and 3-of-5 are unchanged.

| seasonal settings | recall | precision | alerts | false alarms | median TTD (min) | slowest TTD (min) |
|---|---|---|---|---|---|---|
| before (previous_days, no abs floor) | 0.8 | 0.164 | 954 | 785 | 9.0 | 75 |
| + same_weekday | 0.8 | 0.026 | 6038 | 5866 | 6.5 | 80 |
| + day_type | 0.8 | 0.215 | 749 | 576 | 5.0 | 75 |
| + abs floor (a: 1.0 for count units) | 0.8 | 0.253 | 620 | 452 | 9.0 | 75 |
| + abs floor (b: 0.05 x warm-up p95) | 0.8 | 0.202 | 761 | 595 | 9.0 | 75 |
| + abs floor (b: 0.2 x warm-up p95) | 0.8 | 0.470 | 124 | 61 | 10.4 | 445 |
| **+ day_type + abs floor (a)** (chosen) | 0.8 | 0.427 | 378 | 208 | 9.0 | 75 |
| + day_type + abs floor (b: 0.05) | 0.8 | 0.240 | 660 | 490 | 5.0 | 75 |
| + day_type + abs floor (b: 0.2) | 0.8 | 0.500 | 119 | 55 | 9.0 | 445 |

Chosen: day_type history plus an absolute floor of 1.0 for count-like units. False alarms 785 to 208, precision 0.164 to 0.427, recall still 0.8, and every incident is detected at exactly the same minute as before. These are now the router's defaults; `route_series(catalog, history="previous_days", count_floor=0.0)` gives the old behaviour.

- History mode: `day_type` (the last 5 days of the same type, weekday or weekend) beats `previous_days`: the seven weekend-baseline series drop from 361 false alarms to 9. `same_weekday` does not: it is far worse (5866 false alarms), because with one warm-up week a train reading has only 1 or 2 earlier same-weekday values, so the "median" is a single earlier day and MAD is 0. It may work with months of history; it cannot be judged on this dataset.
- Absolute floor (a), from the unit: wobble is at least 1.0 for series counted in whole items (queries, tasks, messages, per-minute counts). The three zero-expected Trino series drop from 332 false alarms to 2. Reason to defend: 0.2 of a queued query is not an incident.
- Absolute floor (b), from the warm-up week's scale: rejected. At a small fraction (0.05 of the 95th percentile) it does nothing for the zero-expected series, whose 95th percentile is 0.2-0.4. At 0.2 it has the fewest false alarms of all (55) but only by making every series less sensitive: the memory leak (INC-007) is found after 445 minutes instead of 75. Recall stays 0.8 on paper, so by the letter of the selection rule this row wins; I rejected it because a six-hour delay on a leak is a real loss the recall number hides, and (a) is also the simpler rule (no calibration step).

Still open:
- Recall is still 0.8: INC-004 and INC-011 (data skew) are missed by every configuration here.
- 208 false alarms remain. 139 are `container_cpu_throttled_ratio`: a ratio that normally sits near 0.003-0.005, so tiny absolute moves are large relative ones. Its unit is "ratio", so floor (a) does not cover it.
- day_type raises false alarms outside the top 10 (92 to about 200). 81 of the 208 fall on the first train weekend (Oct 10-11), when only 2-3 earlier weekend days exist; `baseline_days` shows this.
- With day_type alone, INC-009 shows a time to detect of -2 min: an alert just before the labelled start. It goes back to 8 min once the count-unit floor is on, so that "faster" detection was most likely a false alarm that happened to land in the window (not checked further).

## Nov 9, job run baseline

New detector `sentinel_lens/detectors/job_runs.py`: each finished successful DAG run is compared with that DAG's own earlier finished successful runs (median, 1.4826 x MAD, 5% floor, threshold 6: the same maths as the seasonal detector, now shared in `detectors/robust.py`). One run is one observation, so there is no persistence; the alert is at the run's end. A run needs 5 earlier runs to be scored. Defaults only, nothing tuned. Scored on train with `python scripts/score_job_runs.py`.

| system | recall | precision | alerts | false alarms | median TTD (min) |
|---|---|---|---|---|---|
| routed system (before) | 0.8 | 0.427 | 378 | 208 | 9.0 |
| routed system + job run baseline | 1.0 | 0.427 | 383 | 211 | 10.4 |

- It catches the two data-skew incidents nothing else saw: INC-004 (customer_agg took 28.6 min against ~14.5, z=15.4, found 27.8 min after the start) and INC-011 (26.3 min against ~15.0, z=13.8, found after 25.5 min). Both runs succeeded inside the 60 min SLA, which is why the schedule watcher was blind to them. Train recall is now 1.0.
- All 10 previously detected incidents keep exactly the same time to detect. The median rises from 9.0 to 10.4 only because two new detections of about 26 min join the list; nothing got slower.
- Cost: 3 false alarms, all on hourly_quality_check, which normally takes 1.6 min and occasionally 4.6-5.2 min. With hundreds of earlier runs the wobble is tight (0.22-0.25 min), so a 3-minute slip scores z=12-16. Three more such alerts fell in the warm-up week. Not tuned away: a likely fix is an absolute floor in minutes, as for the count-like series, but that is a separate decision.
- A detection can only come when the run ends, so time to detect equals the run's duration. Flagging a run while it is still going past its expected duration would be faster; not built.
- Cold start: 15 runs skipped (the first 5 of each DAG), all in the warm-up week, none in train.

Diagnostic for input-size normalization (healthy runs only, nothing built): customer_agg duration follows input_rows closely (Pearson r = 0.87, about 1.5 min per million rows, 19 runs); daily_orders weakly (r = 0.52, 19 runs); hourly_quality_check has no input_rows. Both skew incidents happened on low-input days (7.2M and 7.8M rows against a usual 9-9.6M), so normalizing would have made them stand out more, not less; without it they were still caught at z > 13.

## Nov 10, router diagnosis

Diagnosis only, no detector or scoring change (`python scripts/diagnose_router.py`, train split, branch `router-diagnosis` off main, so without the job run baseline). The routed result reproduces exactly: recall 0.8, precision 0.427, 378 alerts, 208 false alarms.

Where the 139 `container_cpu_throttled_ratio` false alarms come from (17 pods):

| group | false alarms |
|---|---|
| on the Oct 10-11 weekend only (baseline of 2-3 earlier weekend days) | 54 |
| on Oct 10-11 and inside an incident window (all INC-005) | 14 |
| inside an incident window only, pod not in affected_entities | 42 |
| other | 29 |

The by-hand split of 54 / 56 / 29 is confirmed, with one detail: 14 alarms are in both of the first two groups, so "on Oct 10-11" is 68 in total and "inside an incident window" is 56 in total. Every alarm outside Oct 10-11 had a full 5-day baseline except one. The largest incident-window groups are INC-002 (17, traffic burst) and INC-003 (16, consumer lag, both on the orders-etl-consumer pods): the labels list the consumer group and topic, not the pods, so the scorer counts throttling on those pods as false.

Floor semantics, measured. A reading is hot when |value - expected| > threshold x wobble, so count_floor 1.0 means a count-like series must move by more than 6 units on a 5-minute average.

| floor setting | recall | precision | alerts | false alarms |
|---|---|---|---|---|
| router defaults (count floor 1.0, 5% floor) | 0.8 | 0.427 | 378 | 208 |
| count floor off | 0.8 | 0.215 | 749 | 576 |
| 5% floor off (what-if) | 0.9 | 0.115 | 1574 | 1380 |
| both floors off (what-if) | 0.9 | 0.094 | 1958 | 1761 |

- The count floor loses no train incident. INC-004 and INC-011 are missed with it on and with it off; none of their key-signal series has a count-like unit (they are CPU cores, GC ms/s and heap ratio on customer-agg executors).
- INC-011 is lost to the 5% relative floor: with it, 1 of 5 readings is hot on each key series; without it, 3 of 5 and the incident is detected at 14 min. INC-004 is missed under every floor setting: the pods have 6 readings in the window, only 3 can be scored (it is Saturday Oct 10, thin weekend history) and 1 is hot, so 3-of-5 never fires. The job run baseline is what catches both.
- The only incident the count floor changes is INC-009: 8 min with it, -2 min without. The earlier alert is on trino_failed_queries_per_min at 20:00, two minutes before the labelled start.
- Cost that the recall number hides: with a floor of 6 units, trino_failed_queries_per_min (train max 4.6), airflow_queued_tasks (0.6) and airflow_task_completions_per_min (0.4) cannot alert upward at any value seen on train.

Injection test (copy of `container_cpu_throttled_ratio|kafka-0`, 0.003 to 0.45 for 3 hours on Wed Oct 14): detected in 10 min with an absolute wobble floor of 0, 0.01, 0.02, 0.03 and 0.05; not detected at 0.08, where the smallest alertable change is 0.48 and the jump is 0.447. Raw alerts elsewhere on that series in train: 16, 6, 5, 0, 0, 0.

Ratio-unit metrics: container_cpu_throttled_ratio (25 series, seasonal_baseline, cpu_throttling, warm-up median 0.003, 139 false alarms), flink_backpressure_ratio (1, saturation_forecast, backlog, 0.066, 1) and jvm_heap_used_ratio (20, saturation_forecast, jvm_heap, 0.54, 1). One floor for the unit "ratio" would not fit all three: they sit two orders of magnitude apart.

## Nov 10, router settings

Router settings only; `seasonal.py` and the scorer are untouched (branch `router-settings`, on top of `router-diagnosis`). Train split.

- Regression test (`tests/test_train_regression.py`, skipped when `data/` is missing): the routed train result is pinned, so a refactor cannot move it unnoticed.
- The count floor is now stated as the change it ignores: `count_min_change = 6.0`, converted with `abs_floor_from_min_change(min_change, threshold) = min_change / threshold`. Same behaviour as `count_floor = 1.0`; the pinned result did not move (0.8 / 0.427 / 378 / 208).
- Per-metric minimum change: `METRIC_MIN_CHANGE = {"container_cpu_throttled_ratio": 0.18}`, chosen by judgment, not tuned: healthy batch executors run near 0.3 throttled (warm-up medians 0.29-0.33 on all six batch executors), so a rise under about 0.2 is not informative. `route_series` returns a `min_change` column.

| router setting | recall | precision | alerts | false alarms | median TTD (min) |
|---|---|---|---|---|---|
| before (no per-metric minimum change) | 0.8 | 0.427 | 378 | 208 | 9.0 |
| throttled ratio min change 0.18 (new default) | 0.8 | 0.608 | 270 | 100 | 9.0 |

Every incident is detected at exactly the same minute as before; none is later. Throttling false alarms drop from 139 to 31, and 26 of those 31 are on the two orders-etl-consumer pods on Oct 8-9, during the traffic burst and consumer-lag incidents whose labels list the consumer group, not the pods.

- The injection test is now permanent (`tests/test_throttling_injection.py`, synthetic series at 0.003 with small noise): a jump to 0.45 or to 0.25 for 3 hours alerts at the third reading, a blip or a sustained rise to 0.01 does not, and the same 0.01 rise does alert when the floor is removed.

Report-only experiment, default unchanged (`python scripts/experiment_count_min_change.py`):

| count_min_change | recall | precision | alerts | false alarms | median TTD (min) |
|---|---|---|---|---|---|
| 1 | 0.8 | 0.602 | 274 | 103 | 9.0 |
| 2 | 0.8 | 0.609 | 271 | 100 | 9.0 |
| 3 | 0.8 | 0.609 | 271 | 100 | 9.0 |
| 6 (default) | 0.8 | 0.608 | 270 | 100 | 9.0 |

Per-incident times to detect are identical at all four values. At 1, 2 or 3, trino_failed_queries_per_min can alert again (its largest rise over expected on train is 4.4): it raises one scored alert, a true one, and no false alarm. At 1 three false alarms appear on other count-like series. airflow_queued_tasks and airflow_task_completions_per_min cannot alert at any of the four values: their largest rise on train is 0.4. So on train the choice between 2, 3 and 6 is worth one true alert and nothing else; it does not decide itself from this data.

Not done yet, by decision: cold-start guard for thin baselines, severity.

## Nov 10, combined result and job run follow-ups

Combined train result after merging the job run baseline and the router settings into main (`python scripts/score_job_runs.py`):

| system | recall | precision | alerts | false alarms | median TTD (min) |
|---|---|---|---|---|---|
| routed system (router defaults) | 0.8 | 0.608 | 270 | 100 | 9.0 |
| routed system + job run baseline | 1.0 | 0.604 | 275 | 103 | 10.4 |

The job run baseline adds 5 alerts: 2 true and 3 false. INC-004 is detected 27.8 min after its start and INC-011 after 25.5 min, both at the end of the slow customer_agg run. The other 10 incidents keep exactly the same time to detect; the median rises to 10.4 only because these two join the list. The "previous numbers" row in the script now shows the current router default (0.8 / 0.608 / 270 / 100).

The 3 hourly_quality_check false alarms (`python scripts/diagnose_job_runs.py`, diagnosis only, no fix chosen):

| run end | duration (min) | earlier runs | history median | MAD | 1.4826 x MAD | 5% floor | wobble | set by | z |
|---|---|---|---|---|---|---|---|---|---|
| Oct 8 03:10 | 4.72 | 243 | 1.60 | 0.167 | 0.247 | 0.08 | 0.247 | MAD | 12.6 |
| Oct 9 13:09 | 4.58 | 277 | 1.60 | 0.167 | 0.247 | 0.08 | 0.247 | MAD | 12.1 |
| Oct 14 10:10 | 5.15 | 394 | 1.60 | 0.150 | 0.222 | 0.08 | 0.222 | MAD | 16.0 |

- The wobble is tiny in minutes: 13 to 15 seconds, set by the MAD each time (the 5% floor would be 5 seconds; the job run detector has no absolute floor). A run only 1.3 to 1.5 min over the median is already hot.
- These are not borderline runs, though: each took about 3 min longer than usual, three times the median. Over warm-up and train, 4 of 504 runs took longer than 4 min (p99 is 2.3 min), so this DAG has an occasional slow run about once every five days that the labels do not count as an incident.
- For scale only: an absolute floor would have to ignore excesses of up to about 3.6 min to silence all three.

Does `--seed` change the incident schedule? (`python scripts/compare_seeds.py`, seeds 7 and 8 generated into a temporary directory; `data/` untouched and confirmed equal to a fresh seed-7 run.) No: the schedule is fixed and only the noise changes.
- Same in both seeds: all 23 incident ids; for the 12 train incidents the scenario, the day, the affected entities and the key-signal series; all 16 changes before the test week (identical, including timestamps); all 547 DAG run ids with the same state and SLA result; catalog and topology.
- Different: metric values (37% of cells equal, 7 of 164 series identical, all of them counters or under-replicated partitions), log and event contents, DAG run start times (by seconds) and durations (median 0.25 min, at most 5.9 min). Seven of the 12 train incident windows move a little: starts by at most 1.9 min, ends by up to 8 min; INC-006, 007, 008, 009 and 012 are identical.
- Test week, counts only: 5 of 11 incidents differ, in start, end, key-signal windows and Spark app ids, the same kinds of fields as on train.
- Consequence: another seed is a noise replicate of the same incidents, not an independent set of incidents. It can show how stable a result is against noise; it cannot serve as fresh incidents to validate on.

## Nov 10, saturation forecast

New detector `sentinel_lens/detectors/saturation.py` (branch `saturation-forecast`): at each 5-minute reading, fit a least-squares line to the last 60 minutes; hot only if the slope is positive, R2 >= 0.9, the fitted value is at least 50% of the limit, and the limit would be reached within 4 hours; then 3-of-5. Limits are `memory_limit_bytes` per pod from the topology. The router can send the 25 `container_memory_working_set_bytes` series to it with `route_series(saturation=True, limits=...)`; it is off by default, so the router defaults and the pinned train result are unchanged. Defaults only, nothing tuned. Train, `python scripts/score_saturation.py`:

| setup | recall | precision | alerts | false alarms | median TTD (min) | INC-007 | INC-001 | INC-010 |
|---|---|---|---|---|---|---|---|---|
| (a) current: routed + job run baseline, memory on seasonal fallback | 1.0 | 0.604 | 275 | 103 | 10.4 | 75 | 10.8 | 24.3 |
| (b) memory on forecast only | 1.0 | 0.493 | 221 | 105 | 10.4 | 330 | 10.8 | 24.3 |
| (c) forecast in addition to seasonal | 1.0 | 0.597 | 278 | 106 | 10.4 | 75 | 10.8 | 24.3 |

The forecast does not improve detection on train, and that is the result, not a tuning problem.
- No incident is lost in (b) or (c). In (b) INC-007 is found after 330 min instead of 75; nothing else moves. In (c) nothing moves at all: the forecast adds 3 alerts and all 3 are false alarms.
- Why INC-007 is late: the leak is slow, 48-61 MB/h on a 1 GiB limit, about 5-6% of the limit per hour, so the climb from the usual 39% takes 10-13 hours. "Full within 4 hours" is only true above roughly 76-81% usage, which is 5.5 hours into the incident. The seasonal baseline flags the same pods after 75 min because 45% is already unusual for them. The forecast answers "when does it run out", not "is this unusual", and for a slow leak the second question fires first.
- INC-001 and INC-010 (executor OOM) are invisible to the forecast: batch executors exist for about 30 minutes, so they never fill a 60-minute window. Their healthy runs also climb from 0.7 to 3.1 GiB in 20 minutes every night, a perfect line towards the limit, which is why the detector requires a mostly full window (`min_coverage=0.8`, an addition to the given design). Both incidents are still caught by the restart counter and the seasonal rules.
- The 3 false alarms are one episode: kafka-2 climbing at about 650 MB/h from 56% to 72% of its 4 GiB limit on Oct 14 04:15-05:30, the hour after the planned broker restart (INC-008).
- On their own, the memory series give 91 alerts (89 true, 1 false) on the seasonal fallback and 37 alerts (34 true, 3 false) on the forecast.

Report-only sweep (window 30/60/120 min, horizon 2/4/8 h, min usage 0.3/0.5/0.7): recall stays 1.0 everywhere. No setting finds INC-007 sooner than seasonal's 75 min. Horizon is the only parameter that matters: 2 h finds it at about 445-450 min, 4 h at 320-330 min, 8 h at 75-85 min (with min usage 0.3 or 0.5; 260-265 min at 0.7) but with 6 to 44 forecast false alarms. Window and min usage barely change anything at 2 h and 4 h.

Where this leaves it: the forecast stays off by default. Its alerts are few and almost all true, and each one carries something the seasonal alert does not ("projected to reach its 1.0 GiB limit in ~3.6 h"), so its natural use is as urgency on top of an existing anomaly rather than as the first detector. Not decided here.

## Nov 10, alert record and score formulas

Every alert is now one record (`sentinel_lens/alerts.py`, branch `alert-record`): time, target, detector, score (0-1), direction, explanation, plus the measure and alert line the score was made from. Adapters turn each detector's output into these records (seasonal, counter_rule, above_zero, start_late / sla_breach / failed_run, job run baseline, saturation), and `to_frame` gives the scorer its ts / target / rule. The score formula is pluggable: `formula(measure, line, detector)`.

- Measure and line per detector: |z| against the threshold (seasonal, job runs); horizon / time-to-full against 1 (saturation); the increase or the value against 1 (counters, above-zero); 1 against 1 for the yes/no schedule rules.
- With persistence the alert fires where 3 of the last 5 readings are hot, which need not be a hot reading itself, so the alert carries the largest measure among the hot readings in that window, with that reading's direction and explanation.
- Which alerts fire is unchanged: through the adapters the current system gives the same 2186 raw alerts and the same train result (recall 1.0, precision 0.604, 275 alerts, 103 false alarms).

Report-only experiment (`python scripts/score_formulas.py`, train, alerts after the scorer's dedup: 157 true, 103 false, 15 expected left out). r = measure / line.

| formula | pooled AUC | precision of top 10 per day | mean score, true | mean score, false | alerts at exactly 1.0 |
|---|---|---|---|---|---|
| (1) min(1, r/2) | 0.482 | 0.540 | 0.913 | 0.939 | 75% |
| (2) 1 - exp(-(r-1)) | 0.558 | 0.604 | 0.721 | 0.737 | 7% |
| (3) min(1, ln r / ln 4) | 0.560 | 0.605 | 0.678 | 0.662 | 22% |
| (4) percentile vs. warm-up week | 0.670 | 0.632 | 0.997 | 0.997 | 17% |

For reference: precision of all 275 alerts is 0.604, which is what a score that says nothing gives the top 10.

Per detector AUC (only detectors with both true and false alerts):

| detector | alerts (true / false) | (1) | (2) | (3) | (4) |
|---|---|---|---|---|---|
| seasonal fallback | 149 (112 / 31) | 0.605 | 0.630 | 0.632 | 0.621 |
| seasonal | 101 (27 / 67) | 0.524 | 0.723 | 0.725 | 0.695 |
| counter_rule | 16 (13 / 2) | 0.538 | 0.538 | 0.538 | 0.500 |
| job_run_baseline | 5 (2 / 3) | 0.500 | 0.667 | 0.667 | 0.833 |

What this says:
- No formula makes the score a good ranking on train. Top-10-a-day precision is at or below the unranked 0.604 for (1)-(3) and 0.632 for (4). "How far over the line" is only weakly related to "is it a real incident": the median alert is already at 3 times its line, true or false.
- (1) is the worst: it reaches 1.0 at twice the line, so 75% of alerts tie at 1.0 and false alarms score slightly higher on average than true ones.
- Inside one detector all four formulas rise with the same measure, so they rank alerts the same way and differ only through ties at 0 and 1. (2) and (3) are practically identical there; the seasonal rule is where they help most (0.72).
- The pooled differences come mostly from where the yes/no rules land. A restart or a failed run sits exactly on its line (r = 1): (2) and (3) give it 0, (1) gives 0.5, (4) gives 1.0 because nothing like it happened in the warm-up week. Those alerts are mostly true (16 of 18), so (4) wins pooled by putting them on top, not by ranking the statistical alerts better.
- (4) is useless as a number to show: true and false alarms both average 0.997, because almost any alert is beyond 99% of a quiet week's readings. Its warm-up reference is also thin for the job run baseline (167 runs) and rests on a week in which day_type has little history.
- job_run_baseline (5 alerts) and counter_rule (2 false) are too few for their AUC to mean anything.

Nothing is chosen. `DEFAULT_FORMULA` is still (1), put there as a placeholder before this experiment; it is the weakest of the four here and should be replaced once a formula is decided. An option the experiment points to but does not test: give the yes/no guardrails a fixed score instead of forcing them through r.

## Nov 10, score decision

Decision: the default score is severity. Hard guardrails (counter_rule, above_zero, failed_run, sla_breach) score exactly 1.0; every other detector scores 0.5 + 0.5 x (1 - exp(-(r - 1))) with r = measure / alert line, kept in [0.5, 1): 0.50 at the line, 0.82 at twice the line, 0.93 at three times, never 1.0.

Reason, in one line: no formula could tell true alerts from false alarms on train, so the score should not pretend to be a probability; it states how far past the line an alert is, puts the always-bad guardrails on top, and never gives 0 to an alert that has fired.

- It is documented in `sentinel_lens/alerts.py` as severity, not the probability of being real.
- The exponential shape is formula (2) from the experiment, the one with the fewest ties, moved up into [0.5, 1) so that an alert at its line scores 0.5 instead of 0.
- start_late is not in the guardrail list, so it always scores 0.5 (it fires exactly at its line).
- Which alerts fire is unchanged: train is still recall 1.0, precision 0.604, 275 alerts, 103 false alarms. On train only the 19 guardrail alerts score exactly 1.0; seasonal alerts have a median of 0.93.
- The four experimental formulas and `scripts/score_formulas.py` are kept as they were.
