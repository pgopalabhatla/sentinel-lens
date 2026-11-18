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

## Nov 10, correlator

`start_late` is now a guardrail (fixed score 1.0); which alerts fire and the train result are unchanged.

New `sentinel_lens/correlator.py` (branch `correlator`): two alerts are linked if they are at most 15 minutes apart AND their entities are at most 1 hop apart in the entity graph (the 33 relationships in the topology; an alert target is mapped to its entity the way the scorer does it). An incident is a connected component of those links. Each incident carries its alerts, targets, entities, detectors, max severity and a title (the explanation of its earliest alert). Defaults only, nothing tuned. Train, `python scripts/score_incidents.py`; the scorer is untouched (a page is TRUE if it contains an alert the scorer classes 'true').

Correction to an earlier number: 2,186 raw alerts is the count since Sep 28. In the train window there are 1,357 (832 true, 78 expected, 447 false); the other 829 are in the warm-up week.

| | count | true | expected | false | precision |
|---|---|---|---|---|---|
| raw alerts (train) | 1357 | 832 | 78 | 447 | 0.650 |
| alerts after the scorer's 30-minute dedup (today) | 275 | 157 | 15 | 103 | 0.604 |
| pages from the correlator (gap 15 min, 1 hop) | 49 | 19 | 2 | 28 | 0.404 |

- 49 pages in 14 days, 3.5 a day, instead of 275 alerts. Recall is 10 of 10 actionable incidents, and the first-alert time to detect of every labeled incident is identical to today's (the correlator drops nothing).
- Page precision (0.404) is lower than alert precision (0.604), and that is the correlator working, not failing: a real incident's alerts collapse far more than false alarms do. 1,047 alerts become 19 TRUE pages (median 12 alerts a page), while 71 alerts become 28 FALSE pages (median 2, 11 of them a single alert). In absolute terms an engineer sees 28 false pages instead of 103 false alerts.
- Merging: none. No page covers more than one labeled incident.
- Fragmentation: 1.8 pages per labeled incident on average. INC-007 (memory leak) is 5 pages: the two consumer pods are 2 hops apart (pod - consumer group - pod) and each pod's sawtooth has quiet stretches longer than 15 minutes. INC-010 is 3 and INC-001 is 2: the executor pod and its DAG are 2 hops apart (pod - spark job - dag). INC-009 is 2: resource_group:adhoc has no relationship at all in the topology. INC-006 is 2: spark_app:orders-stream is not linked to the topic it reads.
- Topology gaps found: resource_group:adhoc and dag:hourly_quality_check appear in no relationship, so their alerts can only group with themselves.

Ablation (report only):

| rule | pages | TRUE | FALSE | precision | pages per labeled incident | pages covering 2+ labeled | largest page (alerts) |
|---|---|---|---|---|---|---|---|
| time only (15 min, any entity) | 27 | 10 | 16 | 0.385 | 1.0 | 1 | 531 |
| topology only (1 hop, any time) | 11 | 7 | 4 | 0.636 | 1.3 | 3 | 1069 |
| both (default) | 49 | 19 | 28 | 0.404 | 1.8 | 0 | 248 |

Time alone merges unrelated things that happen together; topology alone glues different days into one page. Only both together never merge two labeled incidents, at the price of more fragmentation.

Sweep (report only; gap 10/15/30/60 min x hops 0/1/2): recall is 1.0 and merging is 0 everywhere. The gap hardly matters (10, 15 and 30 min give identical pages at 0 and 1 hop; 60 min removes 1-2 pages), because alerts inside an incident come every 5 minutes and separate episodes are hours apart. Hops matter: 0 hops gives 67 pages (2.4 per labeled incident), 1 hop 49 (1.8), 2 hops 38-39 (1.2) with precision 0.35-0.36 and a largest page of 511 alerts.

## Nov 10, why incidents fragment (report only)

`python scripts/diagnose_fragmentation.py`, train only. The correlator defaults (gap 15 min, 1 hop, topology as given) are unchanged. Experiments A and B were chosen AFTER seeing how the train incidents fragment, so their train numbers are optimistic by construction; they are hypotheses, not results.

Why the five split incidents split (every pair of their pages):

| incident | pages | (a) no topology path | (b) quiet gap | (c) path exists but is 2 hops |
|---|---|---|---|---|
| INC-001 | 2 | 0 | 0 | 1 |
| INC-006 | 2 | 1 | 0 | 0 |
| INC-007 | 5 | 0 | 4 | 6 |
| INC-009 | 2 | 1 | 0 | 0 |
| INC-010 | 3 | 0 | 1 | 2 |

- (a) Missing edges: `spark_app:orders-stream` has no path to `topic:orders` or anything around it (INC-006), and `resource_group:adhoc` has no path to `cluster:trino-lakehouse` (INC-009). In both cases the two pages' alerts overlap in time.
- (b) Quiet gaps: INC-007 has 130-140 min without an alert on the same consumer pod after each restart (the leak starts again from a normal level); INC-010 has 31 min between two alerts on daily-orders-exec-2.
- (c) Hop limit: the two consumer pods are 2 hops apart (pod - consumer group - pod; INC-007), and an executor pod is 2 hops from its DAG (pod - spark job - dag; INC-001, INC-010). These are not missing edges.

Experiment A, extra edges that are true of the platform (justified from the source descriptions and the catalog, not from the scorer): A1 resource_group:adhoc - cluster:trino-lakehouse; A2 spark_app:orders-stream - topic:orders; A3 flink_job:clickstream-enrich - topic:clicks; A4-A6 the three DAGs - cluster:airflow-main. Experiment B, recurrence: a page that starts with an alert on a target of an earlier page, within R minutes of that page's last alert, is attached to it.

| setting | pages | TRUE | FALSE | page precision | pages per labeled incident (mean / max) | pages covering 2+ labeled | largest page (alerts) |
|---|---|---|---|---|---|---|---|
| default | 49 | 19 | 28 | 0.404 | 1.8 / 5 | 0 | 248 |
| A1 only | 48 | 18 | 28 | 0.391 | 1.7 / 5 | 0 | 248 |
| A2 only | 47 | 18 | 27 | 0.400 | 1.7 / 5 | 0 | 248 |
| A3 only, or A4-A6 only | 49 | 19 | 28 | 0.404 | 1.8 / 5 | 0 | 248 |
| A1-A6 together | 46 | 17 | 27 | 0.386 | 1.6 / 5 | 0 | 248 |
| entities sharing a catalog source_id joined (77 edges) | 40 | 14 | 24 | 0.368 | 1.3 / 3 | 0 | 511 |
| B, 60 min | 48 | 18 | 28 | 0.391 | 1.7 / 5 | 0 | 248 |
| B, 120 min | 47 | 18 | 27 | 0.400 | 1.7 / 5 | 0 | 248 |
| B, 180 min | 44 | 15 | 27 | 0.357 | 1.4 / 2 | 0 | 260 |
| A1-A6 + B 180 min | 41 | 13 | 26 | 0.333 | 1.2 / 2 | 0 | 260 |

Recall is 1.0 and no page covers two labeled incidents in any row.
- A (real edges) repairs exactly the two missing-edge splits (INC-006 and INC-009 become 1 page each) and nothing else; A3 and A4-A6 change nothing on train.
- Joining by source_id makes INC-007 one page (both consumer pods are in orders-apps) but its largest page doubles to 511 alerts and it ties the two unrelated batch jobs together; it does not fix INC-006.
- B at 60 min only closes INC-010's 31-min gap. Only 180 min closes INC-007's 130-140 min gaps (5 pages to 2), which is a window fitted to this one incident.
- Neither A nor B touches the 2-hop splits, the most common reason.
- Page precision goes DOWN as fragmentation is repaired (0.404 to 0.333), because the repaired pages are TRUE ones being merged; the number of FALSE pages barely moves (28 to 26). Page precision is therefore the wrong number to judge these changes by; pages per labeled incident and false pages are.

Nothing is adopted.

## Nov 10, topology edges adopted

Decision: six relationships that are true of the platform but missing from the generated topology are now part of the default entity graph. They live in a tracked file, `sentinel_lens/topology_extra.py` (the topology itself is in `data/`, regenerated and not in git), each with a one-line justification, and `build_topology` merges them (`extra_edges=()` gives the topology as given).

Reason: the edges are structural facts about the platform, each supported by the platform's own description (source descriptions and the catalog), not by the scorer. They were chosen after seeing train: the gaps were noticed because train incidents fragmented across them. That is the honest order of events; what makes them acceptable is that each can be checked against the platform description without looking at any label.

Audit (`python scripts/audit_topology.py`): of the 37 entities that are in the catalog, alert, or are scheduled DAGs, two had no relationship at all (`resource_group:adhoc`, `dag:hourly_quality_check`), and four more were cut off from what the description says they depend on. The graph had 8 connected components; it now has 3 (Kafka with its streaming consumers, Airflow with its batch jobs, Trino) and no entity without a relationship.

| edge | support in the platform description |
|---|---|
| resource_group:adhoc - cluster:trino-lakehouse | a resource group is a query queue inside its cluster; the catalog files its series under source trino-lakehouse |
| spark_app:orders-stream - topic:orders | "Spark Structured Streaming app orders-stream (Kafka orders -> Iceberg)" |
| flink_job:clickstream-enrich - topic:clicks | "Flink job clickstream-enrich (Kafka clicks -> MinIO)" |
| dag:daily_orders, dag:customer_agg, dag:hourly_quality_check - cluster:airflow-main | "Airflow (LocalExecutor). DAGs: daily_orders, customer_agg, hourly_quality_check" |

Considered and not added, because the description does not state them: spark_app:orders-stream - consumer_group:orders-stream (only the names match); Trino - the Spark apps through the Iceberg tables (an inference, and there is no table entity); anything based on a shared catalog source_id.

NOT adopted: the recurrence rule (experiment B) and source_id joins. `gap_min=15` and `max_hops=1` are unchanged.

Train, correlator defaults (`python scripts/score_incidents.py`):

| | pages | TRUE | expected | FALSE | page precision | pages per labeled incident (mean / max) | pages covering 2+ labeled | largest page (alerts) |
|---|---|---|---|---|---|---|---|---|
| topology as given | 49 | 19 | 2 | 28 | 0.404 | 1.8 / 5 | 0 | 248 |
| with the six edges (default now) | 46 | 17 | 2 | 27 | 0.386 | 1.6 / 5 | 0 | 248 |

Recall is still 10 of 10 and every first-alert time to detect is unchanged; the alerts themselves are untouched (1.0 / 0.604 / 275 / 103). INC-006 and INC-009 are now one page each. INC-007 (5 pages), INC-010 (3) and INC-001 (2) still split, through the 2-hop limit and quiet gaps, which these edges do not address. Page precision falls only because two pairs of TRUE pages merged. A train regression test pins the 46 pages.

## Nov 11, feedback loop

New `sentinel_lens/feedback.py` (branch `feedback-loop`), pure and deterministic. Labels are counted per group: (detector, canonical event) for series alerts, (detector, entity) for DAG alerts. estimate = (true + 1) / (labeled + 2). A group is demoted when it has at least 10 labels and its estimate is below 0.25, i.e. at most 1 real alert in 10. A demoted alert stays attached to its page, but a page only pages if it has at least one alert that is not demoted. Defaults are stated in advance and not tuned.

Online replay on train (`python scripts/score_feedback.py`): alerts in time order; labels are the scorer's own verdicts (one per target per 30 minutes; 260 labels: 157 true, 103 false; 'expected' is not a label); a label arrives 60 minutes after its alert and only affects later alerts. Which alerts fire and how they are grouped is unchanged.

| | pages | TRUE | expected | FALSE | false pages per day | page precision | recall (actionable) | pages per labeled incident |
|---|---|---|---|---|---|---|---|---|
| before | 46 | 17 | 2 | 27 | 1.93 | 0.386 | 10 of 10 | 1.58 |
| after feedback | 44 | 17 | 2 | 25 | 1.79 | 0.405 | 10 of 10 | 1.58 |

- The effect on train is small: 2 fewer false pages. No incident stops paging. One is paged later: INC-010 at 30.3 min instead of 24.3, because its first alert is in a demoted group. All other times to detect are unchanged.
- Why so small: the loop starts with no labels and needs 10 per group. By week: false pages go 20 to 20 in week 1 (Oct 5-11) and 7 to 5 in week 2. The first group is demoted on Oct 9 at 14:10.
- 205 of the 1,357 raw alerts are demoted when they fire (154 the scorer calls false, 51 true), but most sit in pages that still page through another alert, so demotion mostly removes context, not pages.
- Groups demoted at the end of train: (seasonal, cpu_throttling), 0 true / 31 false, estimate 0.03; (seasonal fallback, jvm_gc), 2 / 15, estimate 0.16. (seasonal, cpu_usage) was demoted for a while from Oct 8 and came back as true labels arrived (17 / 32, estimate 0.35). (job_run_baseline, dag:hourly_quality_check) has 0 / 3: all false, but too few labels to act on.
- Six catalog series have no canonical event; they are grouped by metric name instead of falling into one blank group.

Report-only sweep (n_min 5/10/20, tau 0.20/0.25/0.33, label delay 30/60/120 min): recall is 10 of 10 in all 27 settings and no incident stops paging. n_min decides everything: 20 does nothing (46 pages, 27 false); 10 gives 44 pages and 25 false at every tau and delay; 5 gives 39-44 pages and 20-25 false. INC-010 is 6 minutes later whenever anything is demoted. Labeling every raw alert instead of one per 30 minutes (1,279 labels) gives 42 pages and 23 false.

Limits to keep in mind: the simulation assumes demoted alerts still get labeled (otherwise a demoted group could never come back), and two weeks is short for a rule that needs 10 labels per group. What the loop has learned by the end of train has not been applied to anything yet.

## Nov 11, test protocol frozen (the test week has NOT been run)

The detection system is complete for the evaluation: seasonal baseline, persistence, counter and above-zero rules, schedule watcher, job run baseline, router, alert record, correlator, feedback loop. Before running the test week the protocol is written down so the result cannot be shaped afterwards.

- `docs/test-protocol.md`: five systems (naive baseline, first build, final alerts, final + pages, final + pages + frozen feedback; the last is primary), the metrics, the rules (run once, results saved verbatim, anything later is post-hoc) and what will not be done after seeing the numbers. It has a section for the author's predictions, to be filled in before the run.
- `docs/config-frozen.md`: every parameter and default, generated from the code, and the frozen feedback state: groups demoted by all train labels are (seasonal, cpu_throttling) and (seasonal fallback, jvm_gc).
- `scripts/run_test_week.py`: the runner. Its dry run on train goes through the same code path as the test run and reproduces the logged train numbers for systems 1-4 and the online feedback (1.0 / 0.117 / 1488 / 1301; 0.7 / 0.150 / 935 / 783; 1.0 / 0.604 / 275 / 103; 46 pages with 27 false; 44 pages with 25 false). It refuses to run the test week without the tag `pre-test-freeze`, identical committed code, filled-in predictions and `--run-once`, and it never overwrites a first result.
- Frozen feedback applied to train itself gives 35 pages and 16 false. That is in-sample twice over and is not a result; it only shows the code path works.

Disclosure: On Nov 10, during router investigation, a grep over data/labels printed one test-split row (INC-014: container_cpu_throttled_ratio on orders-etl-consumer-0, direction up). No test values were seen. This happened BEFORE the 0.18 throttling minimum change and the injection test were chosen. 0.18 was justified from warm-up/train distributions and domain reasoning, but the decision to keep throttling detectable instead of suppressing it was informed by knowing a test throttling incident exists. INC-014 is therefore reported separately, and system 5's recall is reported with and without it.

## Nov 11, test week result and post-hoc

The test week was run once, from the frozen code at tag `pre-test-freeze`, after the predictions were committed.

- Pre-registered result, verbatim from the runner: [docs/results/test-week.md](results/test-week.md). This is the only result to quote.
- Post-hoc analysis, written after the result was seen and marked as such: [docs/results/test-week-posthoc.md](results/test-week-posthoc.md). Read-only; nothing was re-run or re-tuned.

Primary system (final alerts + correlator pages + frozen feedback): 9 of 10 actionable incidents detected (Wilson 95% interval 0.60-0.98), per-alert precision 0.79, 24 pages in 7 days of which 8 false (1.14 a day), no merged incidents, 1.6 pages per labeled incident. Before vs. after feedback: per-alert precision 0.742 to 0.79, false pages 10 to 8, no incident lost to feedback. Against the naive baseline: false alerts 641 to 45, at the price of one incident (INC-015) that the baseline caught and the final system missed. INC-014, the one test label seen beforehand, pages; without it recall is 8 of 9.

Corrections the post-hoc makes to how the pre-registered table reads: the median time to detect of the primary system (10.0 min) was timed on raw alerts while the other systems were timed after the scorer's dedup (13.0 min); on the same footing they are identical, so feedback changed no time to detect. The maximum, 405 min, is the same either way.

Two limitations, found on the test week and NOT fixed on it:

1. Zero-baseline, low-count series. INC-015 (Trino saturation) was missed. Its signal was on `trino_queued_queries`, which sits at 0 and rose by up to 21.8 queries; with the 6-unit minimum change for count-like series only 2 of 9 readings were hot and persistence needs 3 of 5. The same floor is what removed hundreds of false alarms on train, so this is a real trade-off, not a bug.
2. Slow leaks under the 5% floor. INC-013 (memory leak) was detected after 405 min. The floor means memory must be 30% above expected to be hot; it was 10% above after 55 min. The saturation forecast would not have helped (first alert at 800 min).

Future work: any change aimed at these two is to be designed and evaluated on a second generated world (a dataset with a different incident schedule, since another seed of this one only changes the noise), not on this test week. The test week has been seen; tuning on it now would make its numbers meaningless.

## Nov 11, change point detector (not adopted)

Change point detector (CUSUM on seasonal z, k 0.5, h 10, clip 3): built and tested on train before the test run, not adopted: 517 alerts, 5 true; cause: quiet-period z is wider (sd 1.1-1.6) and autocorrelated (0.2-0.33), unlike the unit-noise assumption. Code and full log entry on the pushed branch `change-point`.

## Nov 16, write-up

README rewritten, design doc added, old README moved to docs/dataset.md, three exploration scripts removed (`look_graph.py`, `plot_baseline.py`, `plot_lag.py`; nothing imported them, history keeps them).

## Nov 16, streaming engine

New package `sentinel_lens/stream/` (branch `streaming-engine`): the final system, one minute at a time, in one process. No Kafka yet. No existing detector, router, correlator, feedback or frozen-config file was touched; the runner's dry run still reproduces the train numbers.

Input: one minute of metric values at a time (`on_minute`) plus DAG-run start and finish events (`on_run_event`), e.g. from `DataSource.stream()` (`sentinel_lens/stream/replay.py`). Output: `AlertEvent` for each alert as it fires and `PageEvent` (opened / alert_added / merged / paged). `StreamEngine.warm_start(ds, at)` builds the state from history and reads nothing at or after `at`.

How it stays equal to batch: no formula is written twice. Each step calls the batch function on the smallest input that holds its answer.

| step | batch code it calls | state it keeps |
|---|---|---|
| seasonal | `seasonal._history` once a day per series (the baseline of a reading only uses earlier days, so the next 24 hours of baselines can be built ahead); then `robust_z` per 5-minute bucket; `alerts_from_seasonal` for 3-of-5 | 28 days of 5-minute means; the last 5 scored buckets |
| guardrails | `counter_increase_alerts` on [previous, current], `above_zero_alerts` on [current] | the previous reading of each counter |
| schedule | `schedule_alerts` once per deadline with `now` = the deadline; `failed_run_alerts` when a run finishes | known runs; a timer per deadline |
| job run baseline | `job_run_alerts` on the DAG's finished runs when a run finishes | finished runs per DAG |
| pages | `correlate` on the new alert plus every page within 15 minutes of it; `feedback.group_key` for the frozen demotions | open pages; which have paged |

Tests (`tests/test_stream_steps.py` on synthetic data, `tests/test_stream_test_week.py` on the test week; no labels read, nothing tuned):

1. Equivalence: the test week streamed through the engine, warm-started on Sep 28 - Oct 18, gives the same 1,445 alerts as `final_alerts()` in `scripts/run_test_week.py` (time, target, detector, direction, score, and also explanation, measure, line), the same 26 pages, the same 244 demoted alerts and the same 24 paging pages with the same paging times. Nothing differed.
2. Causality: an engine given the stream only up to a cut (Oct 19 06:52, Oct 20 02:07, Oct 20 16:20) emits exactly the events the full run had emitted by then, and its pages are the batch pages of the alerts it has seen.
3. Restart: stopped at Oct 20 02:07 (mid-bucket, the leak's page open with 321 alerts, the nightly job running), a new engine built from history and continued to the end of the week gives the same events as the uninterrupted run. Detector state comes back from the DataSource; pages are rebuilt from the alerts already emitted (an alert is not in the metric history, so the alert log has to be kept).

Two things the streaming run shows that the batch numbers hide. Neither is changed here; both are about the frozen result's times, not about which alerts fire.

- A seasonal alert's time is the START of its 5-minute bucket, because that is how `query(step="5min")` labels a mean. The bucket is complete 4 minutes later. So every seasonal alert is known 4 minutes after the time batch gives it (1,322 of the 1,445 alerts; the test asserts exactly 4 minutes for seasonal and 0 for the other 123). Of the 24 paging pages, 14 are paged 4 minutes after their batch paging time and 10 at the same time. Any time to detect in docs/results/test-week.md that rests on a seasonal alert is therefore 4 minutes better than a live system could do. My reading: this is also why INC-014 shows -1 min there (an alert stamped 16:15 built from minutes up to 16:19, against a start of 16:16).
- Batch counts pages at the end of the week. Live, a page that has already paged can later be joined to another by a new alert: 5 merges absorbed 7 pages that had already paged. The engine pages 31 times for what ends as 24 pages.

Speed (`python scripts/measure_stream.py`, test week, 136 seasonal series, 27 counters, 1 must-be-zero gauge): warm start 1.1 s; 2.84 ms per simulated minute on average (about 21,000x real time).

| minutes | n | mean ms | median ms | p95 ms | max ms |
|---|---|---|---|---|---|
| all | 10080 | 2.84 | 0.42 | 4.51 | 960 |
| ordinary | 8064 | 1.88 | 0.41 | 1.64 | 90 |
| closes a 5-minute bucket | 2009 | 3.48 | 3.03 | 7.51 | 18 |
| builds the next day's baselines | 7 | 926 | 920 | 958 | 960 |

The slow ordinary minutes are those in which a DAG run finishes (the batch job-run rule re-scores the DAG's whole history, about 50 ms).

Limits: one process, state in memory; a restart needs the alert log for pages; each new alert re-groups whole candidate pages, which will get slow for pages with thousands of alerts; the saturation rule (off in the frozen system) has no streaming step and the engine refuses routes that ask for it. Adding files under `sentinel_lens/` makes the runner's "frozen code identical to the tag" flag False for any later run, although no frozen file changed (`git diff pre-test-freeze -- sentinel_lens` shows only the new `stream/` files). The full test suite now takes about 100 s instead of 26 s; the three test-week tests are about 75 s of that.

## Nov 16, streaming engine moved; live timing (post-hoc)

- The streaming engine now lives in a top-level package, `sentinel_stream/` (it was `sentinel_lens/stream/` in the entry above). `sentinel_lens/` is again identical to tag `pre-test-freeze`, and the runner's freeze status reads `frozen_code_identical_to_tag: True, working_tree_clean: True`. Imports only; no behaviour change.
- The 12 tests that replay the test week are marked `slow` and skipped by default. `pytest`: 199 tests, 28 s. `pytest -m slow`: 12 tests, 73 s. `pytest -m "slow or not slow"`: 211 tests, 110 s.
- Post-hoc, read-only (`python scripts/posthoc_live_timing.py`; section "Live timing (post-hoc, streaming engine)" in [docs/results/test-week-posthoc.md](results/test-week-posthoc.md#live-timing-post-hoc-streaming-engine)). The pre-registered result is unchanged and still the one to quote; these are the live versions of two of its numbers.

| system 5, test week | pre-registered (batch) | live (streaming engine) |
|---|---|---|
| incidents detected | 9 of 10 | 9 of 10, the same ones |
| median time to detect (min) | 10.0 | 13.0 |
| maximum time to detect (min) | 405 | 409 |
| pages sent | 24 | 31 |
| of which FALSE | 8 | 13 |
| false pages per day | 1.14 | 1.86 |

- Time: every seasonal alert is known 4 minutes after its batch time (the 5-minute bucket has to finish). 5 of the 9 detected incidents are 4 minutes later, 4 are unchanged (first paged by a counter, a must-be-zero gauge or the schedule watcher). INC-014's -1 min becomes 3 min. No incident changes between detected and missed, also when the scorer matches on the time an alert is known.
- Pages: 24 of the 31 page-outs are the pre-registered pages with the same verdicts. The other 7 paged and were later absorbed by a merge: 2 TRUE (both the memory leak INC-013) and 5 FALSE (all inside the windows of INC-017 and the expected burst INC-018, on entities those labels do not list).
- Two of those 5 FALSE page-outs were absorbed in the same minute they paged, because the engine pages alert by alert inside one step. Waiting for the end of the step before paging would give 29 page-outs, 11 FALSE, 1.57 a day. Not changed here: it is a change to the engine's behaviour, to be decided and tested on its own, not slipped into a post-hoc analysis.

## Nov 16, paging once per step

Decision: the streaming engine decides paging once per step, after all alerts of that minute have been grouped into pages, instead of alert by alert (`Pager.add` only groups; `Pager.settle` pages; `sentinel_stream/pages.py`).

Reason: one minute often brings several alerts of the same problem. Taken one by one, two of them can open two pages, page for both, and be merged by a third a moment later, all inside the same engine step. That sends two pages for what is one page before anyone could have read the first. Nothing about which alerts fire or how they are grouped changes: the alerts and the final pages are still identical to batch (the equivalence, causality and restart tests pass unchanged), and time to detect is unchanged (the step's pages are sent at the same stream time).

| live, system 5, test week | alert by alert (before) | once per step (now) | pre-registered (batch) |
|---|---|---|---|
| pages sent | 31 | 29 | 24 |
| TRUE / expected / FALSE | 17 / 1 / 13 | 17 / 1 / 11 | 15 / 1 / 8 |
| false pages per day | 1.86 | 1.57 | 1.14 |
| absorbed by a merge after paging | 7 (2 TRUE, 5 FALSE) | 5 (2 TRUE, 3 FALSE) | - |
| median / maximum time to detect (min) | 13.0 / 409 | 13.0 / 409 | 10.0 / 405 |

- The two page-outs that disappear are trino-worker-0 (Oct 22 17:49) and orders-etl-consumer-1 (Oct 22 19:14), both FALSE, both absorbed in the minute they paged.
- The 5 that remain were on their own for 5 to 358 minutes before a later alert joined them to another page; an engineer would have been paged for each. Waiting longer before paging would remove some of them at the price of time to detect. Not explored.
- Honest order of events: this change was prompted by the post-hoc analysis of the test week (the entry above). It is justified by the argument, not by the number, and 1.57 is a description of the engine as it is now, not a test of the change. The pre-registered result (24 pages, 8 false, 1.14 a day) is untouched and is still the one to quote.
- The post-hoc section "Live timing (post-hoc, streaming engine)" and the README correction now carry the once-per-step numbers; the section says what its first version reported. A unit test covers the rule on a synthetic case (three alerts that end in one page: one page-out in one step, two when spread over three steps), and the pinned test-week count is now 29 page-outs, 5 absorbed.

## Nov 16, the engine behind Kafka

Branch `kafka-stream`. The streaming engine now also runs between Kafka topics: a publisher plays a data directory into `telemetry`, a consumer runs the engine on it and writes `alerts` and `pages`. `sentinel_lens/` is untouched (still identical to `pre-test-freeze`) and so are the engine's own files; everything here is new files around it. No labels read, nothing tuned, the pre-registered result is unchanged.

Pieces:

- **Broker**: `docker-compose.yml`, one Redpanda node, image pinned to `redpandadata/redpanda:v26.1.20`, port 127.0.0.1:19092. Redpanda instead of Apache Kafka because it is one container with nothing to set up and speaks the Kafka protocol, so all the code is plain Kafka code. Three topics, ONE partition each, kept forever (`retention.ms=-1`).
- **Client**: `confluent-kafka` 2.16.1 (`requirements-kafka.txt`). Checked on this venv's Python 3.14: confluent-kafka, kafka-python and aiokafka all install from a wheel. confluent-kafka because it wraps librdkafka (the reference client), its calls are synchronous so the consumer loop reads top to bottom (aiokafka would need asyncio), and it has the three things the consumer needs: commit of an explicit offset, message timestamps set by the producer, offset lookup by time. kafka-python (pure Python) would also have worked.
- **Quiet world**: `python -m sentinel_sim.generate --no-incidents --days 35 --seed 11 --out data_live` (gitignored). The option skips the whole scenario calendar: no incidents, no changes (also not the six harmless ones, they are entries of the same calendar), incident labels written empty with their headers, no splits in topology.json. Background noise stays: log lines, the 40 single-reading blips (still listed in `labels/noise_blips.csv`), readiness flaps, the odd retried task. `--days` other than 28 is only allowed with it, minimum 8. Default generation is byte-identical: a test regenerates seed 7 and compares the content hash of all 18 files with hashes taken before the option existed, which are also the hashes of the `data/` the test week ran on (the two .gz files are hashed after decompression, because a gzip header stores the write time).
- **Wire format** (`sentinel_stream/wire.py`): JSON. `telemetry` carries `metrics` (one minute: `{ts, values{series_id: value}}`, missing readings left out), `dag_run` (start / finish) and `watermark`. `ts` is event time and is also the Kafka message timestamp. Floats go through `repr`, so the engine behind Kafka sees bit-for-bit the values the in-process engine sees.
- **Publisher** (`sentinel_stream/publisher.py`): sends a message stamped t when (t - start) / speed seconds have passed; `--speed 60` is one simulated minute per second, `--speed max` does not wait.
- **Consumer** (`sentinel_stream/consumer.py`): for each message: skip it if it is at or before the last processed position; give it to the engine; send the engine's alerts and page events and wait until the broker has acknowledged all of them; only then commit the offset. A crash can therefore repeat a message but never lose one (at-least-once).

Restart, the part to be able to explain. The consumer keeps nothing on disk. (a) It reads `alerts` and `pages` to the end. (b) The resume clock `c` is the event time of the last committed message. (c) The engine is warm-started at `c`: detector state from the data dir's history before `c` (it stands for Prometheus and Airflow), open pages rebuilt from the alerts emitted before `c`, which is the engine's existing `alert_log` restart. (d) It goes back to the first message stamped `c` or later. That re-feeds a few messages already processed (the last committed one, and whatever was processed but not committed). The engine is deterministic, so it says the same again; each output is compared with the next record already on the topic and is not sent if equal. If it differs the consumer stops with an error instead of writing a second history. This one rule covers a crash before the outputs, between two outputs of a message, and between the last output and the commit.

Why `c` is the last committed message's time and not the next message's: the engine fires schedule deadlines while processing the next input, and `warm_start(c)` assumes every deadline before `c` has fired. That is only guaranteed up to the last message that was fully processed.

Tests:

| | what | result |
|---|---|---|
| default run, no broker (`tests/test_stream_consumer.py`, 10 tests, 12 hours of TRAIN, Oct 7) | wire round trips; the consumer's logic over in-memory topics equals the in-process engine; 5 crashes at 3 different stages; a stream sent twice; a tampered topic stops the restart | pass |
| `pytest -m kafka` (`tests/test_kafka_stream.py`, 4 tests, needs the broker) | the TEST week through Kafka at max speed, consumer in its own process | exactly the in-process engine's 1,445 alerts and 1,474 page events (29 of them `paged`), same order, same bytes |
| | the consumer killed three times in the week and restarted: `kill -9` when 400 alerts were written (in the run looked at, it had reached Oct 20 07:38; this moment differs from run to run); between a message's outputs and its commit at Oct 22 17:49 (on restart the engine said 6 records again that were already written, and none was sent); after the first of a message's outputs, just after Oct 25 12:51 (1 record not sent again) | topics identical to the uninterrupted run, no record twice |
| | part of a stream published twice (publisher restarted two hours back) | the second copy is skipped, output unchanged |

The quiet world, streamed (Oct 26 - Nov 1, engine warm-started on Sep 28 - Oct 25, frozen configuration and frozen demotions): 14 alerts, 7 pages, 4 of which page. The other 3 pages hold only `jvm_gc` alerts (7 of the 14), which the frozen feedback demoted at the end of train, so they page nobody. With no incidents every page is false by construction: 0.57 false pages a day, against 1.57 a day live on the test week. Two are `spark_streaming_scheduling_delay_sec` on orders-stream (z = -9.8 and +9.0 on a series whose usual spread is 0.01-0.02 s) and two are `hourly_quality_check` runs that took 5.2 and 5.6 minutes instead of 1.6 (a retried task). This is a description, not an evaluation, and nothing was changed because of it. It is one seed of one quiet week.

Where this differs from the task as it was written, and why:

1. A third message type, `watermark` ("everything before this time has been sent"), sent by the publisher at the end of its range. Without it the last deadlines before the end would never fire (the in-process replay ends with `engine.advance(end)`), and a test could not know when the consumer is done.
2. "Skip messages at or before the last processed event time" is done on a POSITION, not on the time alone: (event time, kind, dag, run). Two messages can carry the same time (two runs ending in the same second; a run starting exactly on a minute), and skipping on time alone would drop the second.
3. Skipping alone is not enough after a crash, because a message can be half done (some outputs written, no commit). So the restart re-feeds from `c` and compares, as described above.
4. The consumer reads the partition directly (`assign`) and uses the consumer group only to store its offset. With `subscribe`, a restart after `kill -9` waits for the dead member's session to time out. The price: nothing stops a second consumer from being started by mistake, and two would both write.
5. The offset is committed synchronously after EVERY message (measured: 0.5 ms each, about 6 s over the test week). Committing every N messages would be faster and still correct, but this is the rule as stated and it is cheap enough.

Limits: one consumer, one partition per topic (the comparison on restart relies on output order); a restart needs the history store to already hold the telemetry up to `c`; `alerts` is the consumer's memory and must not expire; the JSON uses Python's `Infinity` for an infinite z-score, which a strict JSON reader in another language would reject; `--exit-at-watermark` stops at the next watermark that was not committed before, so a finished run started again with it waits for more.

A slip during this work, for the record: a shell command I ran while writing the generator test also executed `python -m sentinel_sim.generate --out data --seed 7`, which rewrote `data/` with the modified generator. The content hashes of all 18 files were compared with the ones recorded beforehand and are identical, so the dataset is the same; only the files' timestamps changed.

Test runs on this branch: `pytest` 213 passed in 54 s (it was 199 in 28 s; the 10 broker-free consumer tests take about 19 s and the quiet-world generation about 5 s); `pytest -m slow` 13 passed in 95 s (the 12 test-week tests plus the seed-7 hash test, about 20 s); `pytest -m kafka` 4 passed in 2 min 12 s with the broker running. The Kafka tests use their own topics with a random prefix and delete them afterwards.

Freeze flag: on this branch the runner's dry run reads `frozen_code_identical_to_tag: False`. The only difference from the tag inside the frozen paths is `sentinel_sim/generate.py` (the `--no-incidents` option; `git diff pre-test-freeze -- sentinel_lens sentinel_sim` shows that one file). `sentinel_lens/`, the runner and the frozen config are identical to the tag, and the seed-7 hash test shows the generator still writes the evaluated dataset byte for byte. If the flag should read True again, the option can move into a separate module outside `sentinel_sim/` that calls the same generator functions. Not done here: the task asked for a generator option, and whether the flag matters more is the author's call.

## Nov 16, live injection and the scripted live week (a second world)

Branch `live-injection`. It is branched off `kafka-stream`, not off main: the publisher, the consumer and the quiet-world generator it builds on are only on that branch, which is not merged. `sentinel_lens/` and `sentinel_sim/` are untouched here, and so are the engine's files.

What was built:

- **Scenarios** (`sentinel_stream/scenarios_live.py`). Seven batch scenarios ported as lists of effects on named series, in the vocabulary of the batch generator's `_bump`: add / mult / drop, with shape flat / ramp / decay. `magnitude` 1.0 is the batch scenario's size. Each scenario declares its parameter ranges and lists what the port approximates. Sizes are the batch constants; for the two scenarios that the batch generator leaves to `kafka_physics` (slow deploy, crash loop) they are what the physics produced on the train split (Oct 9 and Oct 12).
- **Injector** (`sentinel_stream/injector.py`). Plain Python: commands in, telemetry messages through, answer-key records out. An injection is active for start <= t < stop. Two things are computed instead of declared, because they depend on the reading: a leak that reaches the container's memory limit is OOM-killed (restart counter +1, the ramp starts again), and CPU throttling follows CPU with the generator's own formula.
- **Publisher**: reads commands from the topic `control`, applies the active scenarios to what it sends, writes what it injected to `answer_key`. With no command the stream is the data, message for message.
- **Isolation** (`tests/test_live_isolation.py`). Static: no module the consumer process loads mentions either topic or imports the injection side. Through the broker: every Kafka call of the consumer is recorded while both injection topics exist and hold records; it is assigned `telemetry` only and touches nothing but telemetry, alerts and pages.
- **Scorer** (`sentinel_stream/live_score.py`), reading only `answer_key` and `pages`. Rule: an injection is detected if a page that pages within [start, stop + 30 min] holds, at that moment, an alert on a target entity or 1 hop from one; time to detect = that paging time - start; a page-out matching no injection is false. Target entities are derived mechanically: the entities owning the series the injection changes, plus the DAG for a pause.
- **Runner** (`scripts/run_live_week.py`): the seeded schedule generator, a rehearsal mode, and `--run-once`, which refuses unless the protocol is committed, the code is clean, the generated schedule is the one in the protocol and no result exists.

Order of events, because it is the point of a pre-registration:

1. Code and tests written. Before the protocol, the seven scenarios were only ever applied to readings (synthetic stream; the scheduled week through the injector with no engine). The whole pipeline was rehearsed with a dummy injection that is not one of the seven (one restart of `airflow-worker-0` on twelve hours of train).
2. Schedule generated once with seed 1 (every scenario twice, 14 injections, non-overlapping). Not re-drawn.
3. `docs/live-protocol.md` committed (`1f2b327`): scenarios, ranges, schedule, scoring rule, what is reported, what will not be done afterwards.
4. `python scripts/run_live_week.py --run-once`, once, at max speed (79 s). Output saved by the runner to [docs/results/live-week-1.md](results/live-week-1.md) and committed as written.

Result (the frozen system, `data_live`, Oct 26 - Nov 1, 14 injections):

| | |
|---|---|
| injections detected | 12 of 14 |
| median / maximum time to detect | 12 min / 132 min |
| page-outs | 29: 25 matching an injection, 4 false |
| false pages per day | 0.57 |

| scenario | detected | time to detect (min) | first paged by |
|---|---|---|---|
| broker_gc_pressure | 2 of 2 | 12, 12 | under-replicated partitions (guardrail) |
| consumer_memory_leak | 2 of 2 | 116, 132 | pod memory (seasonal) |
| consumer_slow_deploy | 2 of 2 | 0, 0 | pod restart (guardrail) |
| dag_paused | 2 of 2 | 52, 70 | run not started (schedule watcher) |
| flink_state_leak | 0 of 2 | - | - |
| producer_crashloop | 2 of 2 | 0, 0 | pod restart (guardrail) |
| trino_heavy_query | 2 of 2 | 17, 12 | worker GC time (seasonal) |

The 4 false page-outs are exactly the 4 the week has with nothing injected (same times, same alerts). The injections caused no page outside their own entities.

Reading the result. Everything below was looked up after the run, read-only, from the `pages` topic of the run; nothing was re-run.

- **Slow leak: confirmed on a second world.** Both Flink state leaks ran for about 6 hours without a single alert on or near their targets. They added 390-563 MiB to TaskManagers that usually sit near 2,460 MiB, i.e. 16-23%. The consumer leaks were paged at 116 and 132 minutes, when they had added 30-40% to the usual 400 MiB. This fits what the Nov 11 entry says about the 5% floor (memory has to be about 30% above expected to be hot), but I did not re-derive the floor on this data, so take "fits" as my reading.
- **Zero-baseline queue: not reproduced.** In both Trino injections `trino_queued_queries` alerted (17 minutes in), after the workers' GC time and CPU. These injections add 18 and 26 queued queries for 30-60 minutes; the test-week miss (INC-015) had only 2 hot readings of 9. So the limitation exists as described, and this week does not hit it.
- **The four detections at 0 minutes rest on a restart**, as the protocol said in advance. Without it the next signals were: consumer lag 10 minutes in (slow deploy), and the missing traffic 12-14 minutes in (crash loop).
- **Pages per cause.** The hourly DAG paused for 9 hours produced 9 page-outs, one per missed run, because the runs are 60 minutes apart and the correlator's gap is 15. Most other injections produced 1 or 2.
- **The mild broker GC injection** (magnitude 0.52) was paged at 12 minutes through under-replicated partitions, which the port does not scale with magnitude. Its GC time and produce latency alerted 19 minutes in; its heap did not alert.

What this does not show: 14 injections, two per scenario, one seed, so these are counts. The failures are declared effects, not physics (lag is a straight ramp that also grows at night; a paused DAG's run is simply missing; no catch-up run). And the scenarios were ported by someone who knows the detectors; the guard is that sizes come from the batch scenarios and the ranges were fixed before any scenario met the engine.

Not done, on purpose: no detector, threshold or rule was changed after seeing this. A fix for the slow leak is future work and has to be judged on a third world (another schedule seed with its own protocol), since this week is now seen.

Other things worth knowing:

- A start-up race in the consumer was found and fixed while building the runner: it looked up its start offset and, finding nothing yet, then asked for the end of the topic; a publisher writing between the two calls would have made it jump over messages. It now reads the end first. No effect on what it emits (the Kafka tests pass unchanged).
- A consumer restart during an injected week is not equivalent to an uninterrupted run: the engine is rebuilt from the data directory, which does not hold the injected readings. The scripted run used one consumer from start to end.
- The run's topics (`live-week-1-*`) are still on the local broker for inspection; `docker compose down -v` removes them.

## Nov 17, generic injections (added after the live week was run)

A control command can now carry a list of primitive effects instead of a named scenario: `{"cmd": "inject", "effects": [{"series_id", "op": add | mult, "shape": flat | ramp | decay, "magnitude", "start", "duration_min"}, ...]}`. Each effect has its own start and duration. They are exact, with no jitter and no easing: flat holds the magnitude, ramp reaches it at the end, decay starts at it and falls off as exp(-4 x).

Validation, by the catalog's unit (`Injector.limits_of`):

| unit | rule |
|---|---|
| ratio | stays in [0, 1] |
| bytes (memory), cores | 0 up to the pod's limit |
| ms/s (GC time) | 0 to 1000 |
| partitions, queries, tasks | whole numbers >= 0: an added amount must be whole, results are rounded |
| count (the `*_total` series) | a counter: only add, a whole positive number, flat or ramp; the steps stay after the stop |
| everything else (latency, rates, lag) | >= 0 |

An effect is rejected in one of two moments. At once, if it can never be valid: unknown series, a ratio moved by more than 1, a fractional number of queries, a counter multiplied or decaying. When the injection begins, if its full size would leave the limits on the reading the series has at that moment ("the reading is 0.5; + 0.6 would be 1.1, above its limit of 1"). Rejected effects are not applied and are listed with their reasons in the injection's `started` record; the valid ones of the same command are applied. A command with no valid effect, or any command the publisher cannot carry out, gets a `rejected` record. Readings move after the injection has begun, so while an effect runs its result is also held inside the limits, and the `stopped` record counts the minutes in which that was needed.

The answer key's `started` record for a generic injection has `"scenario": "custom"` and lists every effect as applied, with its resolved start and stop. Any inject command, generic or named, can carry `intent` (free text) and `severity` (noticeable | subtle). They are the caller's words. The scorer shows them (a table by severity, two columns per injection) and never reads them when matching: a test scores the same pages under different severities and intents and gets the same verdicts.

Choices of mine beyond what was asked, to be able to defend or drop:

- CPU up to the pod's limit and GC time up to 1000 ms/s are checked too; the request named ratios, memory, counts and latency.
- The check against the reading is made once, when the injection begins, also for effects of the same command that start later; after that the result is held at the limit instead of being rejected halfway.
- Rejections are reported in the answer key (and in the publisher's log). It is the only place the sender of a command can read them. The scorer ignores `rejected` records.

Relation to the live week: this was added AFTER the single scripted run and does not touch its result. The named scenarios are unchanged: with the 14 scheduled commands, the code as it is now and the code at the commit the result was run from (`1f2b327`) give a byte-identical telemetry stream and answer key (same SHA-256 over all 10,445 messages and 28 records; readings only, no engine). The scorer's report has two more columns and one more table than the saved result file, which stays as the runner wrote it.

Test runs on this branch, all after the last code change: `pytest` 303 passed in 56 s; `pytest -m kafka` 8 passed in 4 min 3 s with the broker running; `pytest -m slow` 13 passed in 99 s.

## Nov 17, a triage agent per page (development set: live-week-1)

Branch `triage-agents`, off `live-injection` and not off main as the task said: main has none of the publisher, consumer, injector, the `pages`/answer-key topics or the live-week result this builds on. `sentinel_lens/`, `sentinel_sim/`, the engine and the injection code are untouched. Scope: agreed with the mentor today, written into docs/scope-v3.md (v3.1): one advisory agent per page-out; the signature engine, local LLM, incident memory and auto-remediation stay out. (CLAUDE.md still lists "RCA agent" as out of scope; it was not edited.)

**Evidence check first** (read from the `live-week-1-*` topics and `data_live`, nothing changed). The stream carries three message types: 10,080 `metrics`, 344 `dag_run`, 1 `watermark`. An injection changes readings only, and a DAG pause also removes the run's start and finish messages (inj-03: 9 of 9 runs missing). Logs, Kubernetes events and changes are not on the stream at all; the only copy is `data_live`, the quiet world, which knows nothing of the injections:

| | during the 14 injections and the 30 minutes after each, on their target pods |
|---|---|
| logs | 2,675 lines, all INFO but one WARN: the crash-looping producer keeps logging "Produced batch of N records" |
| k8s events | none, except inj-13: the paused `customer_agg` run's four Spark pods are Scheduled, Pulled, Started and Killed as if it had run. In the whole file the only reasons are Scheduled, Pulled, Started, Killing, Unhealthy: no OOMKilled, BackOff or CrashLoopBackOff, ever |
| changes | the file is empty (no deploy record for the two slow deploys) |

So for injected failures the agent has metrics and run messages only; the log, event and change tools can only say nothing or contradict the stream. Not fixed here (the task said report only); the injector would have to emit these too.

What was built:

- **Context tools** (`sentinel_stream/context_tools.py`): get_page, query_metric, related_entities, recent_logs, k8s_events, recent_changes, dag_runs, kafka_lag. One `Context` per page-out, cut once at its paging time: readings and run messages stamped at or before it (the engine stamps a minute's outputs with that minute, so the minute stamped `now` had been read), page records up to and including the `paged` one, the data dir through `DataSource.at(now)`. Metrics come from the streamed messages, never from `data_live`'s metrics file. query_metric returns summary statistics, minutes without a reading, the window before and 24 h earlier (only if streamed), and at most 120 averaged points. Every result says where it came from (stream, configuration, history store). A bad call comes back as an error result, never an exception.
- **MCP server** (`mcp_server.py`, official `mcp` SDK 2.3, its low-level `Server`): the same `TOOLS` list and the same `call`, so names, descriptions and schemas cannot drift; a test lists both and compares them.
- **Investigator** (`investigator.py`): one fresh agent per page, `claude-sonnet-5-5` at effort medium (`triage.yaml`), the Messages API with a hand-written tool loop. Budgets checked between every two steps: 15 tool calls, 120 s (a request with tools on gets "time left minus a 25 s reserve" as its timeout, so the reserve is always there for the note), 200,000 tokens (the next request's size is estimated before it is sent). When a budget runs out, tools go off (`tool_choice: none`) and the turn says so. The note is checked: verdict, confidence 0-1, every affected entity in the entity graph, every evidence item citing a call id the agent really made (results are labelled c1, c2, ...). One retry with the reasons; otherwise the note is `unsure`, confidence 0, `valid: false`. Transcripts go to `runs/` (gitignored). Server-side refusal fallback is on (`fallbacks: default`); never triggered in these runs.
- **Dispatcher** (`dispatcher.py`): reads `pages` from the start, investigates each page-out as it was then, 3 at a time, writes notes to `triage` keyed by page id. A later merge writes a link record joining the notes of the merged pages; nothing is re-investigated. Restart: `triage` is its memory (page ids with a note, link ids), links are recomputed from the page records and only written if new. A page whose investigation was running when it died is investigated again (fresh agent); its note is written once.
- **Isolation**: `tests/test_live_isolation.py` now also checks the four triage modules (nothing they load is on the injection side or names its topics; their settings cannot name them) and, through the broker, that the dispatcher touches only telemetry, alerts, pages and triage and writes only triage. `triage_score.py` reads the answer key and is listed on the injection side.

Choices to be able to defend:

- Hand-written loop instead of the SDK's tool runner: the budgets must be checked between every two steps, and the fake model in the tests drives the same loop.
- Found while testing with the real API: when the tool budget ran out, `tool_choice: none` alone made the model end its turn with an empty answer, spending the one retry. The loop now adds "the budget is used up, write the note now" to that turn.
- Found while writing the time test: a request with tools on, started with 30 s left, could take all of it and leave no time for the note. Hence the reserve in the timeout.
- Evidence must cite call ids rather than free text, so "which tool results support it" can be checked mechanically.
- The prompt says nothing about injections, simulations or which stores reflect them: the agent is told only what a real on-call assistant would be told.

**Result on live-week-1, a DEVELOPMENT SET** ([docs/results/triage-dev-week-1.md](results/triage-dev-week-1.md)). The week had been run, scored and read before this, and the prompt was written in the same session; nothing was tuned on its truth (one run, reported as it came). Truth: a page-out matching an injection under the live-week rule is real, else noise.

| truth \ verdict | real | noise | unsure |
|---|---|---|---|
| real (25) | 18 | **4** | 3 |
| noise (4) | 0 | 4 | 0 |

- Every note was valid; 25 of 25 real notes name an entity the answer key lists, 23 name the scenario's target itself (the two Trino notes name one worker pod each, not the cluster).
- Cost: USD 1.06 for 29 pages (mean 0.037, max 0.067), 12 s per investigation on average (max 26 s), 7 tool calls on average, 2 runs stopped by the tool budget, none by time or tokens. Wall time 123 s for the week at 3 at a time.
- **Real called noise, 4**, and why, from the notes: page 2 (producer crash loop) and page 12 (paused hourly DAG) lean on logs that contradict the stream: "the producer is sending batches normally", "the scheduler log shows the 01:05 run sent to the executor". That is the evidence gap above, not something the agent could have seen through. Pages 23 and 34 (consumer memory leaks): the notes tie the memory rise to a rise of the orders in-rate over the same hours and call it load. My reading, not checked further: the quiet world's daily traffic ramp coincides with the leak's ramp, and two signals rising together is a plausible but wrong inference. The 3 `unsure` are a crash loop, one hour of the paused DAG and a memory leak.
- The 4 quiet-world page-outs were all called noise.

What it does not show: one week, 29 page-outs of which 25 are from 14 injections (9 from one paused DAG), on a world the agent's author had seen. It is a check that the pipeline works and a list of the mistakes it makes, not an accuracy figure. A held-out number needs a further world with its own protocol, ideally with logs and events that reflect the injections.

Dependencies: `anthropic` 2.10 (the agent) and `mcp` 2.3 (the MCP server), in a new `requirements-agents.txt`; the default tests do not need them (the MCP comparison is skipped without `mcp`).

Test runs on this branch, after the last code change: `pytest` 342 passed in 58 s (39 of them new); `pytest -m kafka` 9 passed in 3 min 46 s with the broker running (one new: the dispatcher through the broker); `pytest -m llm` 1 passed in 13 s (one real investigation of the synthetic world). `pytest -m slow` was not run: nothing it covers changed.

## Nov 17, the quiet world moved out of sentinel_sim/ (live-week-1 unchanged)

The `--no-incidents` option is gone from `sentinel_sim/generate.py`, which is back to main's version, byte for byte. The quiet world is now made by `python -m sentinel_stream.quiet_world --days 35 --seed 11 --out data_live`. It calls the generator's own functions in the generator's order without `register_all` (the scenario calendar), then corrects the two outputs that belong to the calendar: no splits in topology.json, an empty anomaly_windows.csv with its header. `sentinel_sim/` is not modified. Why: the runner's freeze check covers `sentinel_sim/`, and with the option inside it the check read `frozen_code_identical_to_tag: False` (Nov 16 entry). It now reads **True**.

One addition: the module refuses to write into a directory whose topology.json has splits, i.e. an evaluated dataset like `data/`. The generator's `write` deletes the output directory first, and on Nov 16 a stray command rewrote `data/` (identical content, by luck). CLAUDE.md now also says: never regenerate or overwrite data/ unless explicitly asked.

Checks that live-week-1 still stands (nothing in `docs/results/` was rewritten):

| check | result |
|---|---|
| the quiet world regenerated into a scratch directory (not over `data_live`), content hashes of all 18 files | identical to the `data_live` the week was run on |
| `data/` content hashes before and after this work | unchanged |
| live-week-1 re-scored from its saved `live-week-1-answer_key` and `live-week-1-pages` topics, entity graph from the regenerated world | Overall, By scenario and False page-outs tables identical to docs/results/live-week-1.md; per-injection rows identical apart from the two columns added later (severity, intent: "-") |
| the same week with nothing injected, recomputed in-process on the regenerated world | the same 4 page-outs at the same times |
| seed-7 generation (`pytest -m slow tests/test_generate.py`) | byte-identical to the evaluated dataset |

So the result is 12 of 14 detected, median 12 min, 4 false page-outs, 0.57 a day, as before.

`docs/live-protocol.md` still gives the old command for `data_live`. It is the pre-registered protocol and is left as committed; the new command makes the same files. The runner's result header now names the new module (it only matters if the runner were run again, which it refuses to do).

Tests: the quiet-world tests moved from `tests/test_generate.py` to `tests/test_quiet_world.py`, plus two new ones (refuses to overwrite a dataset with splits; may overwrite a quiet world). `pytest` 306 passed in 60 s; `pytest -m slow tests/test_generate.py` 1 passed.

Merged into `triage-agents` (a merge, not a rebase, because that branch was already pushed). With the merge, CLAUDE.md's out-of-scope line no longer says "RCA agent"; it points to the Nov 17 scope note in docs/scope-v3.md (v3.1), which the triage entry above mentions as not yet edited.

## Nov 17, triage v2: evidence on the stream, two verdict rules, a baseline tool (development set)

Branch `triage-v2`, off main. Three changes, then live-week-1's schedule replayed and triaged again. Nothing here was tuned on the result; the result is reported as it came. live-week-1 (stream v1) and its two result files are unchanged.

**1. Stream version 2** (`publisher --stream-version 2`; version 1 stays the default and is unchanged). `telemetry` gets three new message types: `logs`, `k8s_events`, `changes`, each a batch stamped at a whole minute T holding the records of [T - 1 min, T), in the data directory's own formats, sent only when not empty. Choices to defend:

- Batches, stamped at the END of their minute: like a log shipper flushing every minute, and a reader cut at time t then never sees a record from after t. One message per record was the alternative; records share seconds, so positions would not be unique.
- At one time stamp batches sort first, before the watermark (the closing watermark promises everything before it, so the last batch must precede it). The relative order of the version-1 types is unchanged.
- The consumer treats a batch as `engine.advance(T)`: it fires the deadlines before T, which the next minute would have fired with the same stamps. So alerts and pages are those of v1, and the restart rule (everything before the last committed message's time has been processed) still holds when that message is a batch. Tested on 12 hours of train with crashes at batches: byte-identical outputs.
- Named scenarios write what their failure would (`sentinel_stream/evidence_live.py`), with the batch scenarios' own templates. Each record is driven by what the readings already do: a BackOff and the startup error per crash-loop restart step, OOMKilled at a leak's reset, an ISR-shrink line at each under-replicated flap, Killing/Pulled/Started at a rollout restart. A crash-looping pod writes no ordinary line while down; a paused DAG's missing run leaves no scheduler or task line and no Spark pod event. Each failure has a change record at its start (deploy, job update, a client launch, a DAG pause) and one at its stop (rollback, quota, unpause), except the Trino query, which logs its creation and cancellation instead.
- Evidence has its own random generator, so the readings and runs sent on v2 are byte-for-byte v1's (tested per scenario). The texts name no scenario, injection id or parameter, and the records have exactly the stores' fields (tested).
- Generic injections stay metrics-only. The answer key's `started` record carries `evidence` (the message types the injection writes to) and `stream_version`.
- Choices of mine: rollback descriptions are neutral ("Roll back X to 1.7.3"), not the batch's diagnosis ("Rollback consumer, memory leak"); the DAG pause gets the batch scenario's pause record ("Pause X during warehouse table migration"), which matters below; deploys of the leaking consumer write no rollout events, because the v1 readings have no restart there; change ids count from chg-0001 (data_live has no changes of its own).

**2. The agent.** On v2 the logs / events / changes tools read the stream (the history store before the stream started). A new tool `baseline_comparison(series_id)` gives per 5-minute bucket value, expected, wobble and z from the seasonal detector's own functions, history mode and floors, history store before the stream and streamed readings after, up to now. Checked on live-week-1: for a seasonal page it reproduces the alert's own numbers (e.g. 3.977 vs 2.509 +/- 0.125, z = +11.7). The prompt gained two general rules: noise only with a positive benign explanation found in the evidence (planned change, scheduled job, known burst, maintenance), missing or conflicting evidence means unsure; and the alerts already account for the daily pattern, so blaming traffic needs the traffic to be abnormal AND large enough.

**3. Replay and triage** (`scripts/triage_dev_week_v2.py`, [docs/results/triage-dev-week-1-v2.md](results/triage-dev-week-1-v2.md)). The same 14 injections (schedule regenerated with seed 1 and checked against the protocol) on stream v2, topics `live-week-1-v2-*`. Detection: identical, as expected; the `alerts` and `pages` topics equal v1's record for record (12 of 14, median 12 min, 29 page-outs, 4 false). Three arms, so the agent change and the evidence change can be told apart (arm B was my addition, about USD 0.80):

| | A: old agent, v1 | B: new agent, v1 | C: new agent, v2 |
|---|---|---|---|
| real called real / noise / unsure (of 25) | 18 / 4 / 3 | 20 / 0 / 5 | 12 / **10** / 3 |
| noise called noise / unsure / real (of 4) | 4 / 0 / 0 | 1 / 3 / 0 | 1 / 3 / 0 |
| cost (USD) | 1.06 | 0.80 | 0.97 |

Reading it:

- **The rules alone (A to B) did what they were meant to:** no real page-out called noise (4 to 0). The price: 3 of the 4 true noise page-outs went from noise to unsure, because nothing in the evidence explains them; that is the rule working as written.
- **The evidence (B to C) broke the paused DAGs: 10 real page-outs called noise.** All nine hourly pages of inj-03 and the customer_agg page of inj-13, at confidence 0.85-0.9. Every note finds the pause record and calls it a deliberate, planned change, i.e. the benign explanation the first rule asks for, even nine hours and nine missed runs in. The failure "someone pauses a DAG and forgets it" is exactly a planned change gone wrong, so the rule as worded cannot see it. This is the most important finding of the run. Not fixed here: a general fix (e.g. "a change explains a page as benign only if its effect is the intended one and still within what was planned") would be written after seeing these ten cases and has to be judged on another world.
- **Memory leaks improved:** page 34 (noise in A) is real in C. The note ties the climb to the 1.8.1 deploy and says traffic rose only about 25%, so it does not explain it (the second rule). Page 23 is unsure in B and C.
- **The crash loops stay unsure, and that is causality, not the evidence:** both crash-loop pages page in the injection's first minute (08:07:00, 10:20:00), on the restart counter. The deploy record and the first BackOff are stamped in that same minute, so they reach the stream in the next minute's batch, after the page. The notes say so ("no k8s events, no errors, no deploy").
- Cost and effort fell slightly with v2 (6.3 tool calls a page, against 7.0-7.1). C cites `recent_changes` on most pages.

What it does not show: one week, on a world, a schedule and first-run mistakes all seen before the changes were written; 25 real page-outs, 10 of them from two injections of one scenario. The schedule has no generic injection, so the metrics-only group of the evidence split is empty. The evidence templates are the batch scenarios', written by the author of the detectors and the agent.

Tests, after the last code change: `pytest` 382 passed in 74 s (new: `tests/test_live_evidence.py`, v2 tests in `test_stream_consumer.py` and `test_context_tools.py`); `pytest -m kafka` 10 passed (9 in 3 min 55 s, then the broker isolation test parametrized over both stream versions, 3 passed in 9 s); `pytest -m llm` 1 passed in 10 s. `pytest -m slow` not run: nothing it covers changed.

## Nov 17, triage v3: needs_human instead of a verdict, one follow-up (development set)

Branch `triage-v3`, off main (main fast-forwarded to `triage-v2` first). All changes are general: no rule names a scenario, DAG or entity. Still the development set: arms A-C and their mistakes were read before this was written.

**Note schema v3.** `needs_human` (yes | no | unsure) replaces `verdict`, with `cause` {category: change | capacity | failure | external | unknown, description} and `impact` {degraded: some of sla, lag, errors, latency, or ["none"]; description; evidence: the call ids that show it}. Validated like the rest of the note (categories, "none" alone, cited calls must have been made, impact other than none must cite something). The prompt's rule: an explanation, including a planned change, does not make a page harmless; "no" only when there is no ongoing impact AND the cause is expected and bounded (e.g. a burst that is subsiding, maintenance inside its window); conflicting or missing evidence means unsure. The traffic rule stays.

**Follow-up.** A note that says "unsure" gets one re-investigation at paging time + `follow_up_minutes` (10, `triage.yaml`): a fresh agent on `Context.at_page(..., at=paged + 10 min)`, whose tools see everything up to that time and nothing after; a page merged in the meantime is followed into the page that holds it (get_page says `paged_as_page`). It is told when the page went out, what time it is now and what the first note said. The dispatcher writes it as a `follow_up` record linked by page id; a follow-up never gets one. Live, it starts once telemetry has passed the time by `follow_up_grace` (1 min, so the consumer has written that minute's pages); on finished topics, if the time is inside the stream. Restarts: `triage` is the memory (an unsure note without a follow-up is followed up after a restart; nothing is written twice).

**Scoring** (`triage_score.score_needs_human`): a true page answered yes = correct, no = dangerous, unsure = deferred; a false page no = correct, yes = wasted, unsure = deferred; for the first answer and the final one (the follow-up's, if any); minutes from paging to the final answer; whether the cause text names the injected target (the name or a pod `<target>-<n>`, as a whole word). Arm C's verdicts are scored by mapping real -> yes, noise -> no, unsure -> unsure. The old verdict scoring stays for the v1/v2 reports.

**Result** ([docs/results/triage-dev-week-1-v3.md](results/triage-dev-week-1-v3.md)), arm D on the same 29 page-outs of stream v2 as arm C:

| | C | D first answer | D final answer |
|---|---|---|---|
| true pages (25): correct / dangerous / deferred | 12 / 10 / 3 | 13 / 2 / 10 | 22 / 2 / 1 |
| false pages (4): correct / wasted / deferred | 1 / 0 / 3 | 3 / 0 / 1 | 3 / 0 / 1 |
| cause names the injected target (of 25) | 10 | 14 | 13 |
| minutes to the final answer, median / max | 0.2 / 0.4 | | 0.3 / 10.1 |
| cost (USD) | 0.97 | | 1.31 (11 follow-ups) |

Reading it:

- **The paused DAGs are no longer dangerous.** At paging time all ten are "unsure" (the pause record explains them, the rule says that is not enough); ten minutes later nine follow-ups say yes (impact: SLA, runs missing), one stays unsure. Those nine cost ten minutes each to the final answer.
- **Two new dangerous answers: both crash-loop pages,** answered "no" at confidence 0.6 with cause **unknown** and impact none. That breaks the rule as written ("no" needs an expected and bounded cause). At their paging minute there was indeed no impact yet (orders traffic on baseline, z = -0.6); the crash's records and the traffic drop start a minute later. Arm C called both unsure. A general, mechanical guard would be to reject a "no" whose cause category is "unknown" in `validate_note`; not done, since it was found here, and it has to be judged on another world.
- The three false pages that are not "deferred" in D are "no", none wasted; one false page (Nov 01 18:10) stays unsure after its follow-up.
- Follow-ups changed the answer in 9 of 11 cases, all unsure -> yes.
- The cause category is not scored; for the Trino heavy query the agent says unknown or failure where the design would expect external (a user's query).

What it does not show: as before, one week, 25 true page-outs of which 10 come from the two paused DAGs; and every change here was written after reading arm C's mistakes.

Tests, after the last code change: `pytest` 417 passed in 75 s (new: `tests/test_triage_follow_up.py`, 22 tests: the follow-up context's cut at paging + 10 for every tool, a merged page followed, the dispatcher's follow-ups, restarts, waiting for the stream, the outcome scoring and target naming; the schema's validation in `test_triage.py`); `pytest -m kafka` 10 passed in 3 min 49 s; `pytest -m llm` 1 passed in 11 s.

## Nov 17, the guard, arm E, and the triage agent frozen (development set)

**Guard, in code** (`investigator.guard`, applied after validation; the prompt does not mention it): a note answering needs_human "no" is accepted only if its cause category is not "unknown" AND its impact is ["none"]; otherwise it becomes "unsure", the note records `guard: {converted_from: "no", reason}`, and the normal follow-up at paging + 10 min follows. Why in code: arm D's two dangerous answers broke the prompt's own rule ("no" with an unknown cause), so the rule is enforced where it can be checked mechanically. Tests: the rule's cases, and an investigation (fake model) whose "no" with an unknown cause is converted, recorded and followed up through the dispatcher.

**Arm E** (appended to [docs/results/triage-dev-week-1-v3.md](results/triage-dev-week-1-v3.md)): arm D's agent plus the guard, a fresh run on the same 29 page-outs of stream v2.

| | D first | D final | E first | E final |
|---|---|---|---|---|
| true pages (25): correct / dangerous / deferred | 13 / 2 / 10 | 22 / 2 / 1 | 14 / 0 / 11 | 23 / **0** / 2 |
| false pages (4): correct / wasted / deferred | 3 / 0 / 1 | 3 / 0 / 1 | 0 / 0 / 4 | **0 / 1 / 3** |
| follow-ups; minutes to final (median / max) | | 11; 0.3 / 10.1 | | 15; 10.1 / 10.2 |
| cost (USD) | | 1.31 | | 1.52 |

Reading it, honestly:

- **No dangerous answer in E, but not because of the guard.** The guard converted 6 notes, all on the 4 false pages. On the two crash-loop pages (D's dangerous ones) the agent itself answered "unsure" this time, and both follow-ups said yes. So D to E mixes the guard with run-to-run variation of the agent; what the guard would do on the crash-loop pages is shown by the unit test, not by this run.
- **The guard's cost is on false pages.** The agent finds no cause for a quiet-world false page and says so ("unknown"), so every "no" on a false page is converted: none ends correct (D: 3), one follow-up said yes (wasted), three stay deferred. In effect, with the guard a false page can only be answered "no" when the agent can name a benign cause, and for these four it never could.
- Every page that ends "unsure" waits 10 minutes for its follow-up, so the median time to the final answer rises from 0.3 to 10.1 minutes.
- Run-to-run variation is visible in the same setup: E's paused-DAG pages differ from D's on two pages (page 8 stays unsure after its follow-up where D said yes; page 11 is "yes" at once where D needed the follow-up). Page 5 stays unsure in both.

**Frozen.** `docs/triage-config-frozen.md`, generated by `scripts/triage_freeze.py` from the code: model and effort, budgets, `follow_up_minutes` 10, the dispatcher's defaults, the note schema, the prompt's rules, the guard's source, the follow-up request, every tool with its schema, the full prompt with its SHA-256, and the SHA-256 of the agent's four files. Tag `triage-freeze-v1`. `tests/test_triage_frozen.py` (default run) regenerates the file and fails on any difference, so a change to the agent cannot pass unnoticed. **From here on: no change to the triage agent without a new tag and a log entry saying why** (also in CLAUDE.md). The next number about the agent should come from a world it has not seen.

Tests, after the last code change: `pytest` 427 passed in 76 s; `pytest -m kafka` 10 passed in 3 min 54 s; `pytest -m llm` 1 passed in 14 s.

## Nov 17, an attacker agent that invents incidents (dry run, not scored)

Branch `attacker-agent`, off main. `sentinel_lens/`, `sentinel_sim/`, the engine, the injector (and its validation rules) and the frozen triage agent are untouched (`scripts/triage_freeze.py --check` still matches).

**What it is** (`sentinel_stream/attacker.py`, settings in `attacker.yaml`). Claude (`claude-sonnet-5-5`, effort medium) with seven tools: `list_platform` (entities, series, units, physical limits, layers, the entity graph), `describe_normal` (one series' quiet-world history up to now: hour-of-day p05 / median / p95, last reading), `list_scenarios`, `inject_scenario`, `inject_effects`, `list_active`, `stop`. The same tools are an MCP tool group of their own (`mcp_server --group attacker`), loaded only in that mode, so the triage server never loads the injection side.

Choices to defend:

- **Validated the existing way, without reading what is forbidden.** The publisher reports rejections on the injection record, which the attacker must not read. So before sending, the attacker runs the injector's own checks (the same `Injector` code: `command`, `_check_effect`, `_check_on_reading` against the quiet world's reading at the effect's start minute). Rejected effects come back to the agent as tool errors and are recorded; only what passes goes on `control`. Limit: an effect that is valid on the quiet reading but not on a reading another injection has already moved can still be refused at the publisher; the attacker cannot see that.
- **Budget in code, not only in the prompt:** at most 3 injections starting per simulated day and at most 2 active at any moment over the new injection's whole interval (scheduled ones included). "At least 1 a day" and the variety (named or generic, targets, layers, noticeable or subtle) are asked for in the prompt and reported at every wake, not enforced.
- **One wake = one fresh conversation**, with a summary of its own injections in the wake message: bounded cost per wake and no long context. Per-wake budget: 8 tool calls, 90 s, 80,000 tokens; when it runs out, tools go off and the agent writes its note.
- **Campaign = the live loop on a simulated clock, without a broker.** Live mode is the same loop paced like the publisher (`speed`), sending each command with `at` at least 2 minutes ahead of the stream clock so it arrives in time. Everything is recorded (`runs/attacker/<name>/record.json`: every wake's messages, tool calls, note, cost) and the commands are written as `control.jsonl`. `replay()` plays that control stream through the injector the way the publisher plays a schedule loaded in advance, without the LLM: a kafka test checks that the publisher writes byte-for-byte the same telemetry and injection record.
- **Blind.** The attacker's source and settings name none of alerts, pages, triage, the answer key, the engine or `labels/`; it imports nothing of the detection or triage side (checked on the AST and on the modules loaded; the package's `__init__` loads the engine's code, which the attacker never instantiates); its settings and CLI can name no topic but `control`. Through the broker, a live run's only Kafka calls are writes to `control`, and it creates no consumer at all.

Tests (scripted fake model): budget enforcement (fourth of a day and third concurrent refused, outside the window, in the past), invalid effects rejected with the injector's reasons while the valid ones of the same call are sent, stop only on its own running injection, the record of every wake, the per-wake tool budget, a recorded campaign replayed from its file byte-identical (stream and injection record, named and generic, a stop), the MCP group (same seven tools, same check), blindness (static and through the broker), and one real wake (`llm`).

**Dry run** ([docs/results/attacker-dry-run-1.md](results/attacker-dry-run-1.md), control stream next to it): one simulated day of `data_live` (Oct 26), campaign mode, woken hourly. Not run through the engine or the triage agent and not scored, on purpose.

- 3 injections, no rejection: `broker_gc_pressure` on kafka-1 (noticeable, 2 h), a generic Trino incident (seven ramping effects on trino-worker-0 and the cluster's queue, latency and failures; noticeable, 1 h), `consumer_memory_leak` at 45 and 55 MiB/h (subtle, 7 h). Layers: all three touch layer 1, two touch layers 2 and 3. Replayed through the injector without the LLM: all 3 start, nothing refused, no reading held at a limit.
- Cost USD 0.315 (135k input tokens, 4.5k output), 10 tool calls; 21 of 24 wakes did nothing (about USD 0.006 each).
- What I notice: the "new" generic incident is the named Trino heavy-query scenario rebuilt from primitives (its own intent says "runaway ad-hoc cross-join"), so "novel" was not really met; the agent says itself that it sized only one of the seven effects against normal (`describe_normal` 4 calls in all). It spent the whole day's budget by 13:00 and then idled. It reasons about the budget and variety explicitly at every wake. Ideas, not done: ask in the prompt for a generic incident to be a failure the scenario list does not describe, and require `describe_normal` on every series an effect touches.

Tests, after the last code change: `pytest` 438 passed in 81 s; `pytest -m kafka` 12 passed in 3 min 55 s (two new: the attacker writes only to control through the broker; the publisher replays a recorded campaign byte for byte); `pytest -m llm` 2 passed in 20 s (one new: one real attacker wake).

## Nov 17, the attacker: describe before injecting, a 3 h gap, novelty; dry run 2; frozen

Branch `attacker-agent`. This tunes the test generator (realism, variety), not the detector or the triage agent; neither is touched, and the triage freeze still matches.

**In code** (rejections go back to the agent as tool errors and are recorded):

- An injection may only touch series that `describe_normal` was called on in the same wake. A named scenario needs all of its series (it cannot be applied in part, so it is refused otherwise); a generic incident has each undescribed effect refused and the rest sent. To make that possible within 8 tool calls a wake, `describe_normal` now takes up to 30 series at once (compact profiles above 4) and `list_scenarios` lists each scenario's series per target (2 to 24; a paused DAG drops every series of its Spark pods).
- Injection starts at least `min_gap_minutes` (180) apart, in both directions. The existing budget (3 a day, 2 active) is unchanged.
- A generic incident needs `novelty`: the plausible real failure it stands for that the named scenarios do not cover (the prompt asks for it and says not to rebuild a scenario by hand). Its series' overlap with every named scenario's series (shared, Jaccard) is recorded and reported; it is not shown to the agent and not enforced.

Tests: the gap (2 h 59 min refused, 3 h accepted), the describe rule (nothing described, one series short, an undescribed effect inside a generic incident, and described in an earlier wake does not count), novelty required, overlap recorded and not returned; the earlier tests now describe before injecting (tests about something other than spacing set the gap to 0).

**Dry run 2** ([docs/results/attacker-dry-run-2.md](results/attacker-dry-run-2.md)): Oct 29 of `data_live` (dry run 1 was Oct 26), campaign mode, one day, not scored.

- 3 injections, all named scenarios: `broker_gc_pressure` on kafka-1 (noticeable), `flink_state_leak` (subtle, 5 h), `trino_heavy_query` (noticeable). No rejection; replayed through the injector, all 3 start. USD 0.365, 14 tool calls (`describe_normal` 3 times, each on all of a scenario's series).
- **No generic incident, so no novelty text and no overlap to report.** The notes show it meant to make one ("the campaign hasn't yet covered ... a generic incident") and then spent the last slot of the day on a named scenario. One reading, mine: the new requirements (describe every series, justify novelty) make a generic incident the more expensive choice within a wake, and with three slots a day it picked the cheap one each time. One day is too little to say. Not changed: the next change to the attacker needs a new tag (below).
- The 3 h gap shaped the day: starts at 00:00, 03:00 and 12:00.

**Frozen.** `docs/attacker-config-frozen.md`, generated by `scripts/attacker_freeze.py`: model and loop settings, the budget with what is enforced in code and what only asked, the validation rules with the SHA-256 of each validating function's source, every tool with its schema, the prompt as sent (gap filled in) with its SHA-256, and the SHA-256 of `attacker.py`, `attacker.yaml`, `injector.py` and `scenarios_live.py` (the injector's rules are the attacker's validation, so changing them changes the attacker). `tests/test_attacker_frozen.py` (default run) fails on any difference. Tag `attacker-freeze-v1`; the rule is in CLAUDE.md next to the triage one.

Tests, after the last code change: `pytest` 443 passed in 81 s; `pytest -m kafka` 12 passed in 3 min 54 s; `pytest -m llm` 2 passed in 21 s.

## Nov 18, the attacker v2: a cap on named scenarios; dry run 3; frozen again

Branch `attacker-v2`, off main. Also removed an untracked leftover `look_graph.py` (byte-identical to the file deleted in 466aa22).

**Why a new freeze.** Dry run 2 had no generic incident: all three injections were named scenarios. My reading was that describing every series and justifying novelty make a generic incident the more expensive choice in a wake. A campaign of named scenarios only tests whether the detectors catch the seven failures they were built around, so variety has to be enforced, not asked for. This is a change to the test generator for variety only. The attacker stays blind (it still reads no detection output); the triage agent is untouched and its freeze still matches.

**In code.** Named scenarios may make up at most `max_named_share` (0.6) of the campaign's injections, at every point. A named scenario is refused if (named + 1) / (all + 1) > 0.6, with "Inject a generic incident (inject_effects) instead". The check comes first in `inject_scenario`, before series and parameters, so a refused named scenario costs no describe calls. Choices to defend:

- **A running cap, not one checked at the end.** A cap checked only at the end can't be enforced while the campaign runs. Taken literally, the running cap means a campaign's first injection is generic (1 of 1 is 100%), and over two days at most 3 of 6 can be named (3 of 5 = 60% passes, 4 of 6 does not). I kept the literal rule; the alternative was a grace period at the start, which is an extra number to defend.
- **Shown to the agent.** The wake message says whether the next injection may be named, `list_active` gives the counts, and the prompt states the cap. This tells the agent about its own injections only; it is not information about detection.
- Unchanged: novelty, the describe_normal rule, the 3 h gap, the budget (3 a day, 2 active), the per-wake limits, the model.

Tests: the cap's arithmetic (1/1 refused, 1/2 allowed, 2/3 refused, 3/5 allowed, 4/6 refused); a campaign where a named first is refused, then generic, named, and a second named refused, with the reasons, `list_active` and the wake message. Tests about other rules that start with a named scenario lift the cap (`max_named_share=1.0`); this includes the broker isolation test in `test_live_isolation.py`.

**Dry run 3** ([docs/results/attacker-dry-run-3.md](results/attacker-dry-run-3.md)): Oct 30-31 of `data_live` (not used by the attacker before: dry runs 1 and 2 were Oct 26 and 29), campaign mode, two days, 48 wakes, not scored.

- 4 injections, 2 generic and 2 named; named share after each: 0/1, 1/2, 1/3, 2/4. Severities: 2 noticeable, 2 subtle. Layers: L1 3, L2 4, L3 3.
  - atk-001, Oct 30 00:00, generic, noticeable: a heap leak on trino-worker-0 (heap, GC, CPU, query p95, queued, failed; ramps).
  - atk-002, 03:00, `consumer_slow_deploy` on orders-etl, subtle.
  - atk-003, 06:00, generic, subtle: slow Flink checkpoint storage (checkpoint duration and failures, backpressure, throughput down, TaskManager heap).
  - atk-004, Oct 31 00:00, `broker_gc_pressure` on kafka-1, noticeable.
- **No rejection of any kind, the quota's included.** The quota worked through the wake message, not through refusals. At 00:00 the agent went generic "because the campaign rule said the next injection couldn't be a named scenario". On Oct 30 it spent its last slot on a generic incident "to keep named scenarios within the 60% cap". So the run shows the cap steering choices; the refusal path is shown only by the unit tests.
- Novelty and overlap: atk-003 (checkpoint storage) shares no series with any named scenario. atk-001's novelty says "a gradual leak, not a resource spike", but all 6 of its series belong to `trino_heavy_query` (Jaccard 0.545). It is the same series with a different shape, so its novelty is questionable, much as in dry run 1.
- **Oct 31 is thin:** one injection at 00:00, then 23 wakes of "waiting for a Spark or Airflow incident" that never came. The ask of at least 1 a day was met. With three a day allowed, the agent's own idea of variety (an untouched component) made it idle rather than build one.
- Replayed through the injector without the LLM: 4 of 4 start, nothing refused, nothing held at a limit.
- Cost USD 0.732 (316k input, 10k output tokens), 17 tool calls (describe_normal 7).

What it does not show: two days and four injections; the cap never had to refuse anything. Not changed after the run: the idle day and atk-001's overlap are notes for a later version, which would need a new tag.

**Frozen again.** `docs/attacker-config-frozen.md` regenerated: the cap in the budget table, a validation rule for it, and SHA-256 hashes of `check_named_share` and `named_counts`. The prompt and file hashes are new. Tag `attacker-freeze-v2`; `attacker-freeze-v1` stays as history. CLAUDE.md now names v2.

Tests, after the last code change: `pytest` 445 passed in 79 s; `pytest -m kafka` 12 passed in 3 min 56 s; `pytest -m llm` 2 passed in 27 s.

## Nov 18, live week 2 protocol (NOT run)

[docs/live-week-2-protocol.md](live-week-2-protocol.md) and its runner `scripts/run_live_week_2.py`, committed before anything was run. The runner was compiled, not executed.

The plan:
- The three frozen components (`pre-test-freeze`, `triage-freeze-v1`, `attacker-freeze-v2`) on a new quiet world (seed 13, 35 days, generated by the runner itself).
- One attacker campaign over the 7 days, recorded once and replayed for scoring.
- Detection scored with the live rule, split by kind and severity, with Wilson intervals.
- Three triage runs on every page-out.

Choices beyond the request, to defend or drop before the run:
- False pages per day get an exact Poisson (Garwood) interval, since Wilson is for proportions.
- A generic injection has no single target, so its "cause names the target" means naming any entity the answer key lists for it.
- `--resume "<reason>"` exists for technical failures only, reusing finished steps. A finished campaign is never re-recorded.

The predictions are empty; the runner refuses until they are filled in and committed.
