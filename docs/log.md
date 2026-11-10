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
