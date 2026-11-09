# Task: counter/guardrail rules, schedule watcher, router (second build)

Read CLAUDE.md and docs/log.md first. Same rules as the first build: tune on the TRAIN split only, never read
`labels/` from detector code, keep detector code independent of the sample data, make small tested commits,
and explain each design choice in one sentence. Work on a new branch `guardrails-schedule-router` created
from `seasonal-persistence`. Do not push; the author pushes.

## Why this task
Train results so far (seasonal + 3-of-5 on every gauge): recall 0.7, precision 0.150, 783 false alarms.
- INC-005 (paused DAG) leaves no metric to score. Only a "was a run started?" check can see it.
- Restarts show up in minutes with a counter rule, but take ~10 min through gauges + persistence.
- The naive full detector got +0.1 recall over naive gauges-only for 2 extra false alarms from exactly these rules.
- The catalog assigns each series a default detector (`data/catalog/series.csv`, column `default_detector`).
  Running seasonal on everything was a shortcut for the first comparison.

## 1. `sentinel_lens/detectors/guardrails.py`
Rules for things that are always bad. No baselines, no persistence.
- `counter_increase_alerts(series) -> DataFrame`: for a cumulative counter, one row wherever the value rises
  versus the previous reading. Columns: ts, value, previous, increase, explanation
  (e.g. "restarts_total for orders-producer rose from 3 to 4").
  - Read counters at their native 1-minute step (not 5 minutes) so restarts surface in minutes 0-2.
  - NaN means the pod does not exist (batch pods). Never compare across a NaN gap, never treat NaN as 0.
  - A decrease (counter reset) is not an alert.
  - Applies only to counters whose healthy state is flat: kube_pod_container_status_restarts_total,
    flink_job_restarts_total, flink_checkpoint_failures_total.
- `above_zero_alerts(series) -> DataFrame`: rows where value > 0, with an explanation.
  Used for kafka_under_replicated_partitions.
- Tests on synthetic series: flat counter (no alerts), single +1 step, several increases, reset to 0,
  NaN gap, above-zero blip.

## 2. `sentinel_lens/detectors/schedule.py`
Schedule watcher: "should a run have happened by now?"
- `schedule_alerts(expected, runs, now, grace_min=10.0) -> DataFrame`
  - `expected`: columns dag_id, scheduled_for (UTC Timestamp), sla_minutes (from `ds.expected_runs`).
  - `runs`: columns dag_id, run_type, scheduled_for, start, end, state (from `ds.job_runs`).
    `scheduled_for`/`start`/`end` arrive as strings or None: convert with `pd.to_datetime(..., utc=True)`.
    Ignore `run_type == "manual"` rows when matching. Match on dag_id + scheduled_for.
  - `grace_min` can be a float or a dict of per-DAG overrides.
  - Output columns: ts (when the alert becomes true), target (`dag:<dag_id>`), rule, explanation.
- Rule `start_late`: alert at scheduled_for + grace if no run had started by then (no matching run, or its
  start is later than scheduled_for + grace). Example explanation:
  "daily_orders was due at 02:00, no run had started by 02:10 (grace 10 min)".
- Rule `sla_breach`: alert at scheduled_for + sla_minutes if the matched run has not finished by then
  (end missing or later).
- Causality: decide each rule only from what was known at the alert time. Never alert at a later run's end.
  Only emit alerts with ts <= `now`.
- `failed_run_alerts(runs) -> DataFrame`: state == "failed" gives an alert at the run's end.
- Before choosing grace values, print the distribution of `lateness_min` for scheduled runs per DAG on the
  train period and explain the choice (daily_orders normally starts within a minute; its SLA is 90).
- Tests (synthetic frames): on-time run (no alert); run never starts (one alert at scheduled + grace);
  run starts 460 min late (alert at scheduled + grace, NOT at its end); on-time start but ends after SLA
  (alert at scheduled + sla); manual run ignored; per-DAG grace override; in-progress run before its SLA
  (no alert yet); failed run.

## 3. `sentinel_lens/router.py`
Pick the rule for each series from the catalog (pass the catalog DataFrame in; do not read files here).
- counter metrics listed in section 1 -> `counter_rule`
- kafka_under_replicated_partitions -> `above_zero`
- default_detector == seasonal_baseline -> `seasonal`
- default_detector in (saturation_forecast, change_point), and any other guardrail gauge
  (e.g. airflow_scheduler_heartbeat_age_sec) -> `seasonal` with fallback=True, because those detectors are
  not built yet. Keep the fallback list in ONE constant so we can remove entries as detectors arrive.
- Return a table: series_id, rule, fallback.
- Test: every one of the 164 series gets exactly one rule (catalog test can use a small synthetic catalog).

## 4. `scripts/score_router.py`
Score on TRAIN with the clock at 2026-10-19, same style as `scripts/score_seasonal.py`.
- Rows in the summary: naive (quickstart, full), naive (gauges only), seasonal everywhere (existing),
  routed system (this task). Per-incident time-to-detect table with the same columns.
- Routed system = seasonal/fallback series with 3-of-5 persistence at 5-minute steps, counter and above-zero
  alerts at 1-minute steps with no persistence, schedule + failed-run alerts for DAG targets.
- Also print false alarms broken down by rule (counter_rule, above_zero, seasonal, seasonal fallback,
  start_late, sla_breach, failed_run).
- First read `sentinel_sim/evaluate.py` to confirm which `target` formats the scorer accepts for series ids
  and for DAGs, and use them.
- Report honestly if recall or precision gets worse.

## 5. Finish
Run pytest. Append a dated entry to docs/log.md with the new results table and one sentence per new detector.
Commit in small commits. Do not change the seasonal parameters (floor stays 5%) and do not touch the test split.
