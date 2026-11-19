# POST-HOC, written after the result was seen

Live week 2: post-hoc analysis of [live-week-2.md](live-week-2.md) (protocol:
[live-week-2-protocol.md](../live-week-2-protocol.md), Rule 4).

Everything below was written on 2026-11-19, after the result was committed (`bc78783`). None of it
changes a number in the result: the result file, the scorer and every frozen component are untouched,
and nothing was re-run. The figures were read from what the run left behind: the `live-week-2-*` topics
(answer key, alerts, pages, the three triage topics, telemetry), `runs/live-week-2/` (the attacker's
recording) and the world `data_live_2/` through `DataSource`. Where a number is recomputed, the
computation uses the frozen functions with the frozen settings (`seasonal_scores`, `live_score`,
`triage_score.text_names_target`, `build_topology`). Section 3 and section 5 contain counterfactuals:
they say what a different setting or a different injection *would* have done. They are explanations, not
results, and they are not evidence that a change would help overall (false pages were not measured for
them).

## 1. Predictions vs result

| | prediction (range) | result | vs range | result's 95% interval contains the point prediction? |
|---|---|---|---|---|
| detected, overall | 0.65 (0.55-0.75) | 9/11 = 0.82 | **above** | yes (Wilson 0.52-0.95) |
| detected, generic | 0.65 (0.60-0.70) | 5/5 = 1.00 | **above** | yes (Wilson 0.57-1.00) |
| detected, subtle | 0.42 (0.30-0.55) | 4/6 = 0.67 | **above** | yes (Wilson 0.30-0.90) |
| false pages per day | 0.9 (0.4-1.4) | 2/7 = 0.29 | **below** | yes (Garwood 0.03-1.03) |
| dangerous final answers, sum of 3 runs | 1 (0-3) | 0 | inside (at the bottom) | - |
| true pages correct, mean of 3 runs | 0.70 (0.60-0.80) | 10/11 = 0.91 in every run | **above** | yes (Wilson 0.62-0.98) |
| total cost, USD | 9.50 (8-11) | 4.54 | **below** | - |

All the misses go in the direction of a better outcome than I predicted: more detected, fewer false
pages, more correct triage, lower cost. Only one prediction fell inside its range, the dangerous answers,
and it sat at the bottom of that range. With n = 5 to 11, every point prediction still lies inside the result's own 95% interval,
so this week cannot tell my predictions apart from the outcome; the ranges were too narrow, the
generic one most of all (as flagged when it was written).

- The two false page-outs (Oct 27 20:14, Oct 28 19:59, both `kafka_failed_produce_requests_per_sec`)
  also page in the same 7 days with nothing injected, so they are the quiet world's own noise, not
  something the injections caused.
- Cost: the attacker came in at 2.61, close to the 2.50 I expected. Triage was 1.93 for three runs (0.53-0.81 each)
  instead of about 1.50 each, on 13 page-outs and 6 follow-ups per run.

## 2. atk-004 (`dag_paused` on `customer_agg`, Oct 27 14:00-19:00)

**No run of the DAG was scheduled or due in the window.** `customer_agg` runs once a day at
03:30 UTC (`30 3 * * *`, SLA 60 min, from the topology). The runs due around the pause were Oct 27 03:30,
10.5 h before it began, and Oct 28 03:30, 8.5 h after it ended. Both were published normally on
the stream (start 03:30:26 / finish 03:45:26, and 03:30:22 / 03:45:48, both `success`).

**The injection had no observable effect on anything a detector reads.** The scenario does two things
(`scenarios_live.DAG_PAUSED`): (a) it drops runs that would have *started* during the pause, and there
were none; (b) it drops the readings of the job's batch-only pods (`customer-agg-driver`,
`customer-agg-exec-1..3`, 24 series) for the pause. Those pods exist only while a run is going: in the
quiet world they report only in the 03:00 hour of each day (checked from Oct 27 00:00 to Oct 28 06:00). There are **0 readings**
in 14:00-19:00, so there was nothing to drop. The only trace is two change records (`dag_pause` at 14:00,
`dag_unpause` at 19:00, author m.ortiz), and no detector reads change records.

**Why the schedule watcher stayed silent.** It alerts `start_late` at `scheduled_for + grace` (grace 10
min) when an expected run has not started by then. No expected run fell inside the pause, so it had
nothing to alert on, which is correct. The real Oct 28 run was not late.

**How it happened.** The attacker passed `lead_min=30`, meaning "30 minutes before a scheduled run".
`lead_min` is used only by the schedule generator. A manual injection starts when it is sent (scenario
text: "a manual injection starts when it is sent"). The attacker sent it at 14:00. Its note for that wake
says it read the normal history of all 24 series, but it does not mention the schedule. So this is an
injection that could not be detected by anyone, a human included, apart from the change log. Under the
protocol it stays in the count as a miss ("Drop an injection ... because it looks unfair" is excluded).
The fair reading is that the detector was not tested by it.

## 3. atk-010 (`flink_state_leak` on `clickstream-enrich`, Oct 31 02:10-09:10)

The leak added 70 MiB/h to `flink-taskmanager-0` and 90 MiB/h to `flink-taskmanager-1` for 7 h, so in
theory 490 MiB and 630 MiB by the end. It touched only those two series
(`container_memory_working_set_bytes`). Recomputed with the frozen detector and settings (5-minute means,
`history='day_type'`, Oct 31 is a Saturday so the baseline is the last 5 weekend days, `floor=0.05`,
`abs_floor=0`, threshold 6):

| hour (UTC) | tm-0 value | expected | wobble | z max | tm-1 value | expected | wobble | z max |
|---|---|---|---|---|---|---|---|---|
| 02:00 | 2450 | 2415 | 121 | +0.40 | 2488 | 2463 | 123 | +0.68 |
| 03:00 | 2521 | 2420 | 121 | +1.26 | 2571 | 2447 | 122 | +1.42 |
| 04:00 | 2588 | 2428 | 121 | +1.57 | 2635 | 2460 | 123 | +1.89 |
| 05:00 | 2657 | 2440 | 122 | +2.07 | 2750 | 2445 | 122 | +3.02 |
| 06:00 | 2744 | 2440 | 122 | +2.80 | 2805 | 2436 | 122 | +3.21 |
| 07:00 | 2814 | 2462 | 123 | +3.47 | 2887 | 2420 | 121 | +4.32 |
| 08:00 | 2933 | 2434 | 122 | +4.51 | 2982 | 2436 | 122 | +4.81 |
| 09:00 | 2818 | 2417 | 121 | +4.67 | 2801 | 2431 | 122 | +4.75 |

(MiB, hourly means; z max is the highest 5-minute z in the hour. The 09:00 hour falls back because the
rollback at 09:10 ends the leak.)

- Peak excess over expected: 564 MiB (tm-0, 09:05, z +4.67) and 581 MiB (tm-1, 08:40, z +4.81).
  **No reading was hot on either pod**, so the 3-of-5 persistence never came into it. The engine
  produced 0 alerts on these series in the window, which agrees with this recomputation.
- **What kept it from going hot: the 5% relative floor.** In every reading of the window the wobble
  was `0.05 x expected`, about 121 MiB, not the MAD. With a threshold of 6, a reading needs to be
  about 725 MiB, or 30%, above expected. At the injected rates that takes about 10.4 h for tm-0 and
  about 8.1 h for tm-1, both longer than the 7-hour leak. With the MAD alone (`floor=0`, otherwise the
  same), the wobble is about 46-51 MiB (median) and the leak would have gone hot from 03:30 (tm-0) and
  05:00 (tm-1). This is a counterfactual: what a lower floor would cost in false pages on memory series
  was not measured.
- **The detector meant for this is off.** Memory is routed to the saturation forecast in the catalog,
  but the frozen config has it "built but OFF (not routed)", so memory falls back to the seasonal
  baseline. A seasonal baseline compares with the same hour on earlier days, and it is weak against a
  slow ramp by design. The forecast would ask a different question, how long until the memory limit.
  Here the limit is 4 GiB and the peak about 2.9 GiB (73%), so even the forecast might not have
  paged inside 7 h. I did not run it.
- No other signal: an OOM kill would have tripped the guardrails, but the pods never got near the
  limit. The leak's logs and change records (RocksDB live-data-size lines, the `flink_job_update` record
  at 02:10) are not detector inputs.

This is the same weakness as the two Flink leaks missed in live week 1, now with a number on it: a
leak has to add about 30% of normal memory before the seasonal detector sees it.

## 4. "Cause names the target" = 0 for named incidents in every run

The rule (`triage_score.text_names_target`) looks for the answer key's `target` as a whole word, or a
pod `<target>-<n>`, in the **cause text** of the final note. It does not read the note's
`affected_entities`. The five named true pages, with what the final notes named (all three runs):

| page | injection | target | entities in the note (`affected_entities`) | what the cause text says | rule | verdict |
|---|---|---|---|---|---|---|
| 3 | atk-002 trino_heavy_query | `trino-lakehouse` | `cluster:trino-lakehouse`, both workers (3/3 runs) | "Both Trino workers ... a heavy query or workload" | no | **scorer mismatch**: right cluster, right cause, the cause text says "Trino workers" not "trino-lakehouse" |
| 8 | atk-005 broker_gc_pressure | `kafka-2` | `cluster:kafka-ingest`, `pod:kafka-0/1/2` (runs 1-2); cluster and topic only (run 3) | "finance-reporting-consumer ... backfill ... fetch.max.bytes 500 MB ... I did not check broker metrics" | no | **partly real**: the cause (the client's deploy) is exactly right, but no run singled out kafka-2. It names all three brokers or none |
| 11 | atk-007 consumer_slow_deploy | `orders-etl` | `consumer_group:orders-etl`, `pod:orders-etl-consumer-0` | "orders-etl-consumer-0 restarted ... no deploy" | no | **scorer mismatch** on the name: by design the rule does not count "orders-etl" inside "orders-etl-consumer-0". But see the note below on the cause |
| 12 | atk-007 consumer_slow_deploy | `orders-etl` | `consumer_group:orders-etl`, `pod:orders-etl-consumer-1` | "orders-etl-consumer-1 restarted ... no deploy" | no | as page 11 |
| 14 | atk-009 producer_crashloop | `orders-producer` | `pod:orders-producer` (3/3) | follow-up: "Deploy chg-0007 ... misspelled KAFKA_BOOTSTRAP ... the producer crash-loops" | no | **scorer mismatch**: exact cause, found in the follow-up. The text says "the producer" |

**Verdict: the 0 is mostly a scorer mismatch, not a failure to find the target.** Checked against the
note's `affected_entities` with the same name rule (`triage_score.names_target`), the target is named on
14 of 15 named true pages across the three runs (run 3, page 8 is the exception). The cause-text rule is
strict: the agent writes "the producer", "Trino workers" or the pod name, and the rule wants the
answer key's exact target string. The rule was pre-registered and the result's 0 stands. A rule on
`affected_entities` would be a different measure, for a future protocol.

Two things in the table are real, not wording:

- **kafka-2 was never localised (atk-005).** The agent found the right cause, but in every run it says it
  did not check broker-level metrics, so it never says which broker is suffering.
- **atk-007's cause was not knowable at paging time.** The page went out at 00:00. The deploy record
  ("Add per-order fraud score enrichment", ts 00:00) was published on the stream at 00:01. Every change
  record in this run arrives one minute after its timestamp. So "no deploy found" was true when the
  agent looked. The answer was `yes`, and only an `unsure` gets the +10 min follow-up, so nobody looked
  again. atk-009 is the contrast: its first answer was `unsure`, and the follow-up found chg-0007. The
  frozen follow-up rule means that a confident `yes` on a page at the very start of an injection never
  sees the change that caused it.

## 5. The generic incidents: how loud were they?

Guardrail-triggering effects are effects on a series a guardrail watches: the counter-rule metrics
(restarts, Flink job restarts, Flink checkpoint failures) and `kafka_under_replicated_partitions`. The
column "without them" removes the alerts on those series and asks for the first alert left that is not
demoted, on or 1 hop from the targets, inside [start, stop + 30 min]. Effects are applied per series, so
removing one removes exactly the alerts on its series. Any page holding an alert that is not demoted pages
at the end of its step, so that alert's time is the page-out time.

| id | severity | effects | guardrail-triggering effects | first paged by (TTD) | without them: first page-out (TTD), its z | effect series that alerted |
|---|---|---|---|---|---|---|
| atk-001 | subtle | 6 | `kafka_under_replicated_partitions` +2 (above_zero) | above_zero, URP (15 min) | seasonal, `kafka_isr_shrinks_per_sec` (24 min), z = +inf (expected 0 +/- 0) | 5 of 6 |
| atk-003 | subtle | 7 | `flink_checkpoint_failures_total` +4 (counter rule, 4 alerts, from 01:10) | seasonal fallback, `flink_backpressure_ratio` (24 min) | same: the guardrail was not first (24 min), z +10.1 | 5 of 7 |
| atk-006 | noticeable | 5 | none | seasonal fallback, `airflow_scheduler_heartbeat_age_sec` (19 min) | same (19 min), z +53.8 | 3 of 5 |
| atk-008 | subtle | 7 | none | seasonal fallback, `spark_streaming_scheduling_delay_sec` (49 min) | same (49 min), z +10.1 | 4 of 7 |
| atk-011 | noticeable | 9 | none | seasonal, `container_cpu_usage_cores` trino-worker-0 (19 min) | same (19 min), z +24.2 | 6 of 9 |

- **All five would still have been caught without a guardrail.** Only atk-001 was paged by one first.
  Without the URP effect it would have paged 9 minutes later, on ISR shrinks. That alert is a degenerate
  one: the baseline is exactly 0, so any non-zero reading has infinite z. Two more of its series
  (produce p99, failed produce requests) also alerted.
- **They were loud.** Each changed 5-9 series at once, and 3-6 of those alerted. The first non-guardrail
  alert was 1.7 to 9 times over the threshold of 6 (z +10 to +54, or infinite). All were ramps lasting
  90-150 minutes. The attacker's "subtle" label did not mean subtle for the detector: generic x subtle
  was 3 of 3.
- The series that never alerted are mostly JVM ones: `jvm_gc_time_ms_per_sec` (4 of 4 incidents that
  changed it) and `jvm_heap_used_ratio` (2 of 3). Also `airflow_queued_tasks`,
  `container_cpu_throttled_ratio`, `spark_streaming_input_rows_per_sec` and two of the Trino query-rate
  series. Each incident was caught on its other series.
- So 5 of 5 generic says the detector catches multi-signal incidents that move several series hard within
  about 2 hours. It does not test the shape it misses, a slow drift on one or two series (section 3).
  The attacker made no generic injection of that shape this week.

## 6. Injections that started too late to be caught

**None.** The stream ends on 2026-11-02 at 00:00. The last injection, atk-011, started on Nov 1 at 00:00
and stopped at 02:00, 22 h before the end. All 11 stopped on their own duration (`"reason": "duration"`),
none was cut short by the end of the stream, and every one had its full 30-minute matching tail inside
the stream. The protocol's planned list of late-start injections is therefore empty.

## What this changes

Nothing about live week 2's numbers. For a future protocol, under a new world and new tags, it suggests
three questions. Each would need its own predictions and its own false-page cost:

1. Can a slow leak be seen at all under the 5% floor? This is the saturation forecast, which is built but
   off, or a floor for memory chosen differently.
2. Should the target be scored on `affected_entities`, rather than only on the cause text?
3. Should the follow-up also fire on pages at the start of an injection, so that a change record
   published a minute later is seen?

Separately, the attacker can make an injection with no observable effect (atk-004). Its validation
checks the readings, but not whether a pause covers any scheduled run.
