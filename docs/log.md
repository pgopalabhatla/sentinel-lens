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
