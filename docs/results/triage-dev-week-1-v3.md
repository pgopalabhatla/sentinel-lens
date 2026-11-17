# Triage on live-week-1, stream v2: note schema v3 with follow-ups (DEVELOPMENT SET)

**A development-set result, not a final or held-out one.** The week, its schedule and the mistakes of the earlier arms (docs/results/triage-dev-week-1.md, triage-dev-week-1-v2.md) were read before schema v3 and the follow-up were written. It shows whether the changes do what they were meant to on the cases that motivated them.

- written by `scripts/triage_dev_week_v3.py` at commit `6fc04a5`
- page-outs: the 29 of live-week-1 replayed on stream v2 (`live-week-1-v2-*`), the same in both arms
- agent: `claude-sonnet-5-5`, effort `medium`; budget per investigation 15 tool calls, 120 s, 200,000 tokens; 3 at a time; follow-up of an "unsure" note at paging time + 10 min, a fresh agent whose tools see nothing after that time
- this invocation: 29 first investigations and 11 follow-ups in 160 s of wall time

| arm | note | follow-up | topic |
|---|---|---|---|
| C | verdict real / noise / unsure (scored as yes / no / unsure) | none | `live-week-1-v2-triage` |
| D | needs_human yes / no / unsure, cause, impact; an explanation does not make a page harmless | once, if unsure | `live-week-1-v2-triage-v3` |

Scoring: a true page answered yes = correct, no = **dangerous**, unsure = deferred; a false page answered no = correct, yes = wasted, unsure = deferred. The final answer is the follow-up's when there was one.

## C against D

| | C | D, first answer | D, final answer |
|---|---|---|---|
| real page-outs (25): correct | 12 | 13 | 22 |
| real page-outs (25): dangerous | 10 | 2 | 2 |
| real page-outs (25): deferred | 3 | 10 | 1 |
| noise page-outs (4): correct | 1 | 3 | 3 |
| noise page-outs (4): wasted | 0 | 0 | 0 |
| noise page-outs (4): deferred | 3 | 1 | 1 |
| cause names the injected target (of 25) | 10 | 14 | 13 |
| follow-ups | - | - | 11 |
| minutes from paging to the final answer: median / max | 0.2 / 0.4 | - | 0.3 / 10.1 |
| ... for the pages with a follow-up: median / max | - | - | 10.1 / 10.1 |
| cost (USD, estimate; D with its follow-ups) | 0.97 | - | 1.31 |

## Dangerous answers

Arm C: 10:

| time (UTC) | page | injection | scenario | cause |
|---|---|---|---|---|
| Oct 26 18:15 | 5 | inj-03 | dag_paused | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The 18:05 run was therefore not started. Before the pause, every hourly run since 07:05 started within about 0.5 minutes of schedule and succeeded. |
| Oct 26 19:15 | 6 | inj-03 | dag_paused | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The 18:05 and 19:05 runs did not start because of the pause, so the start_late alert is expected. |
| Oct 26 20:15 | 7 | inj-03 | dag_paused | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The last run was 17:05, and the 18:05, 19:05 and 20:05 runs didn't start because the DAG is paused. The start_late alert is firing on the pause, not on a scheduler fault. |
| Oct 26 21:15 | 8 | inj-03 | dag_paused | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The last run was at 17:05, and the runs scheduled from 18:05 on never started because the DAG is paused. The start_late alert is therefore expected. |
| Oct 26 22:15 | 9 | inj-03 | dag_paused | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. No runs have started since the 17:05 run, which fits the pause. The start_late alert fired because of the pause, not because the scheduler failed. |
| Oct 26 23:15 | 10 | inj-03 | dag_paused | The DAG was deliberately paused at 17:23 UTC by m.ortiz for a warehouse table migration. The 23:05 run didn't start because the DAG is paused. No runs have started since 18:05, which matches the pause. |
| Oct 27 00:15 | 11 | inj-03 | dag_paused | The DAG was deliberately paused at 17:23 UTC on 10-26 (chg-0005, m.ortiz) for a warehouse table migration. No runs have started since the 17:05 run, which matches the pause, so the start_late alert is expected. |
| Oct 27 01:15 | 12 | inj-03 | dag_paused | hourly_quality_check was deliberately paused by m.ortiz at 17:23 UTC on 10-26 for a warehouse table migration. The missing runs are expected, and the start_late alert fired because of the pause. No run has started since the pause. |
| Oct 27 02:15 | 13 | inj-03 | dag_paused | The DAG was deliberately paused by m.ortiz at 17:23 UTC on 10-26 for a warehouse table migration. No runs have started since, which is expected for a paused DAG, so the start_late alert is not a fault. |
| Nov 01 03:40 | 32 | inj-13 | dag_paused | customer_agg was deliberately paused at 02:30 UTC by m.ortiz for a warehouse table migration (chg-0021). The 03:30 run was not started because the DAG is paused, not because the scheduler failed. |

Arm D: 2:

| time (UTC) | page | injection | scenario | cause |
|---|---|---|---|---|
| Oct 26 08:07 | 2 | inj-02 | producer_crashloop | orders-producer restarted once (counter 3→4 at 08:07, flat at 3 for the previous 8 hours). I found no OOM kill, error log, k8s event or deploy to explain it. The restart looks like an isolated blip, not a crash loop. |
| Oct 31 10:20 | 28 | inj-12 | producer_crashloop | The orders-producer container restarted once (counter went from 13 to 14) after about 24 hours with no restarts. I found no k8s events, error logs or recent changes to explain it. |

## Per page-out: C, then D's first answer and its follow-up

| time (UTC) | page | truth | injection | scenario | C | D first | D follow-up | D final | D outcome | D cause | D impact | D min to final |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Oct 26 03:02 | 1 | real | inj-01 | broker_gc_pressure | yes | yes | - | yes | correct | change | latency | 0.3 |
| Oct 26 08:07 | 2 | real | inj-02 | producer_crashloop | unsure | no | - | no | dangerous | unknown | none | 0.3 |
| Oct 26 18:15 | 5 | real | inj-03 | dag_paused | no | unsure | unsure | unsure | deferred | change | sla | 10.1 |
| Oct 26 19:15 | 6 | real | inj-03 | dag_paused | no | unsure | yes | yes | correct | change | sla | 10.1 |
| Oct 26 20:15 | 7 | real | inj-03 | dag_paused | no | unsure | yes | yes | correct | change | sla | 10.1 |
| Oct 26 21:15 | 8 | real | inj-03 | dag_paused | no | unsure | yes | yes | correct | change | sla | 10.1 |
| Oct 26 22:15 | 9 | real | inj-03 | dag_paused | no | unsure | yes | yes | correct | change | sla | 10.1 |
| Oct 26 23:15 | 10 | real | inj-03 | dag_paused | no | unsure | yes | yes | correct | change | sla | 10.1 |
| Oct 27 00:15 | 11 | real | inj-03 | dag_paused | no | unsure | yes | yes | correct | change | sla | 10.1 |
| Oct 27 01:15 | 12 | real | inj-03 | dag_paused | no | unsure | yes | yes | correct | change | sla | 10.1 |
| Oct 27 02:15 | 13 | real | inj-03 | dag_paused | no | unsure | yes | yes | correct | change | sla | 10.1 |
| Oct 27 19:14 | 14 | real | inj-05 | trino_heavy_query | yes | yes | - | yes | correct | unknown | latency, lag | 0.1 |
| Oct 28 05:39 | 16 | real | inj-06 | consumer_slow_deploy | yes | yes | - | yes | correct | failure | lag | 0.1 |
| Oct 28 05:39 | 17 | real | inj-06 | consumer_slow_deploy | yes | yes | - | yes | correct | failure | lag | 0.4 |
| Oct 28 20:09 | 18 | real | inj-07 | consumer_slow_deploy | yes | yes | - | yes | correct | failure | lag | 0.2 |
| Oct 28 20:09 | 19 | real | inj-07 | consumer_slow_deploy | yes | yes | - | yes | correct | failure | lag | 0.3 |
| Oct 28 21:29 | 20 | noise | - | - | no | no | - | no | correct | unknown | none | 0.2 |
| Oct 29 18:23 | 22 | real | inj-09 | broker_gc_pressure | yes | yes | - | yes | correct | change | latency, errors | 0.2 |
| Oct 30 11:09 | 23 | real | inj-10 | consumer_memory_leak | unsure | yes | - | yes | correct | change | lag | 0.2 |
| Oct 30 11:44 | 24 | real | inj-10 | consumer_memory_leak | yes | yes | - | yes | correct | change | none | 0.2 |
| Oct 31 01:10 | 25 | noise | - | - | unsure | no | - | no | correct | unknown | none | 0.4 |
| Oct 31 01:54 | 26 | real | inj-11 | trino_heavy_query | yes | yes | - | yes | correct | failure | latency, errors | 0.2 |
| Oct 31 01:54 | 27 | real | inj-11 | trino_heavy_query | yes | yes | - | yes | correct | unknown | latency, errors | 0.3 |
| Oct 31 10:20 | 28 | real | inj-12 | producer_crashloop | unsure | no | - | no | dangerous | unknown | none | 0.2 |
| Nov 01 03:40 | 32 | real | inj-13 | dag_paused | no | unsure | yes | yes | correct | change | sla | 10.1 |
| Nov 01 13:14 | 33 | real | inj-14 | consumer_memory_leak | yes | yes | - | yes | correct | change | none | 0.2 |
| Nov 01 13:14 | 34 | real | inj-14 | consumer_memory_leak | yes | yes | - | yes | correct | change | none | 0.1 |
| Nov 01 18:10 | 35 | noise | - | - | unsure | unsure | unsure | unsure | deferred | unknown | none | 10.1 |
| Nov 01 20:54 | 36 | noise | - | - | unsure | no | - | no | correct | unknown | none | 0.2 |

Follow-ups that changed the answer: 9 of 11.

Arm D's cause categories (final notes): change 16, unknown 8, failure 5.

# Arm D in full

## Outcomes

A true page: needs_human yes = correct, no = **dangerous**, unsure = deferred. A false page: no = correct, yes = wasted, unsure = deferred. Final = the follow-up's answer when there was one.

First answer (at paging time):

| truth | correct | DANGEROUS | wasted | deferred | no note |
|---|---|---|---|---|---|
| real (25) | 13 | 2 | - | 10 | 0 |
| noise (4) | 3 | - | 0 | 1 | 0 |

Final answer:

| truth | correct | DANGEROUS | wasted | deferred | no note |
|---|---|---|---|---|---|
| real (25) | 22 | 2 | - | 1 | 0 |
| noise (4) | 3 | - | 0 | 1 | 0 |

Follow-ups: 11. Dangerous (final): 2. Wasted (final): 0.

**Dangerous answers** (a true page answered needs_human no):

| time (UTC) | page | injection | scenario | cause |
|---|---|---|---|---|
| Oct 26 08:07 | 2 | inj-02 | producer_crashloop | orders-producer restarted once (counter 3→4 at 08:07, flat at 3 for the previous 8 hours). I found no OOM kill, error log, k8s event or deploy to explain it. The restart looks like an isolated blip, not a crash loop. |
| Oct 31 10:20 | 28 | inj-12 | producer_crashloop | The orders-producer container restarted once (counter went from 13 to 14) after about 24 hours with no restarts. I found no k8s events, error logs or recent changes to explain it. |

Cause names the injected target: 13 of 25 true page-outs (final answer; first answer: 14).

Minutes from paging to the final answer: median 0.3, max 10.1; for the pages that had a follow-up: median 10.1, max 10.1.

| time (UTC) | page | truth | injection | first | follow-up | final | outcome | cause | names target | impact | min to final |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Oct 26 03:02 | 1 | real | inj-01 | yes | - | yes | correct | change | no | latency | 0.3 |
| Oct 26 08:07 | 2 | real | inj-02 | no | - | no | dangerous | unknown | yes | none | 0.3 |
| Oct 26 18:15 | 5 | real | inj-03 | unsure | unsure | unsure | deferred | change | yes | sla | 10.1 |
| Oct 26 19:15 | 6 | real | inj-03 | unsure | yes | yes | correct | change | yes | sla | 10.1 |
| Oct 26 20:15 | 7 | real | inj-03 | unsure | yes | yes | correct | change | yes | sla | 10.1 |
| Oct 26 21:15 | 8 | real | inj-03 | unsure | yes | yes | correct | change | no | sla | 10.1 |
| Oct 26 22:15 | 9 | real | inj-03 | unsure | yes | yes | correct | change | no | sla | 10.1 |
| Oct 26 23:15 | 10 | real | inj-03 | unsure | yes | yes | correct | change | yes | sla | 10.1 |
| Oct 27 00:15 | 11 | real | inj-03 | unsure | yes | yes | correct | change | no | sla | 10.1 |
| Oct 27 01:15 | 12 | real | inj-03 | unsure | yes | yes | correct | change | yes | sla | 10.1 |
| Oct 27 02:15 | 13 | real | inj-03 | unsure | yes | yes | correct | change | yes | sla | 10.1 |
| Oct 27 19:14 | 14 | real | inj-05 | yes | - | yes | correct | unknown | no | latency, lag | 0.1 |
| Oct 28 05:39 | 16 | real | inj-06 | yes | - | yes | correct | failure | no | lag | 0.1 |
| Oct 28 05:39 | 17 | real | inj-06 | yes | - | yes | correct | failure | no | lag | 0.4 |
| Oct 28 20:09 | 18 | real | inj-07 | yes | - | yes | correct | failure | no | lag | 0.2 |
| Oct 28 20:09 | 19 | real | inj-07 | yes | - | yes | correct | failure | no | lag | 0.3 |
| Oct 28 21:29 | 20 | noise | - | no | - | no | correct | unknown | - | none | 0.2 |
| Oct 29 18:23 | 22 | real | inj-09 | yes | - | yes | correct | change | no | latency, errors | 0.2 |
| Oct 30 11:09 | 23 | real | inj-10 | yes | - | yes | correct | change | yes | lag | 0.2 |
| Oct 30 11:44 | 24 | real | inj-10 | yes | - | yes | correct | change | yes | none | 0.2 |
| Oct 31 01:10 | 25 | noise | - | no | - | no | correct | unknown | - | none | 0.4 |
| Oct 31 01:54 | 26 | real | inj-11 | yes | - | yes | correct | failure | no | latency, errors | 0.2 |
| Oct 31 01:54 | 27 | real | inj-11 | yes | - | yes | correct | unknown | no | latency, errors | 0.3 |
| Oct 31 10:20 | 28 | real | inj-12 | no | - | no | dangerous | unknown | yes | none | 0.2 |
| Nov 01 03:40 | 32 | real | inj-13 | unsure | yes | yes | correct | change | yes | sla | 10.1 |
| Nov 01 13:14 | 33 | real | inj-14 | yes | - | yes | correct | change | yes | none | 0.2 |
| Nov 01 13:14 | 34 | real | inj-14 | yes | - | yes | correct | change | yes | none | 0.1 |
| Nov 01 18:10 | 35 | noise | - | unsure | unsure | unsure | deferred | unknown | - | none | 10.1 |
| Nov 01 20:54 | 36 | noise | - | no | - | no | correct | unknown | - | none | 0.2 |


## D's cause next to what was injected (to read, not scored)

### Mon Oct 26 03:02 - page 1 - yes (correct)

- Injected: **inj-01: broker_gc_pressure** on `kafka-1`. A client fetching huge batches fills one broker's heap: long GC pauses, slow produce requests, the broker falls out of the in-sync replicas again and again.
- Cause (change): The finance-reporting-consumer was deployed at 02:50 and started a backfill of orders from offset 0, with fetch.max.bytes set to 500MB. Under-replicated partitions and produce latency rose from about 02:57. The timing fits, but I did not check broker network or disk load, so the link is likely and not confirmed.
- Impact: latency

### Mon Oct 26 08:07 - page 2 - no (dangerous)

- Injected: **inj-02: producer_crashloop** on `orders-producer`. A bad config makes orders-producer crash on startup: it restarts in a loop and no orders arrive.
- Cause (unknown): orders-producer restarted once (counter 3→4 at 08:07, flat at 3 for the previous 8 hours). I found no OOM kill, error log, k8s event or deploy to explain it. The restart looks like an isolated blip, not a crash loop.
- Impact: none

### Mon Oct 26 18:15 - page 5 - unsure (deferred)

- Injected: **inj-03: dag_paused** on `hourly_quality_check`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused hourly_quality_check at 17:23 UTC for a warehouse table migration (chg-0005). The pause explains why the 18:05 run never started. It is still paused, and I found no announced end time or maintenance window.
- Impact: sla; answered at paging time: unsure, after the follow-up: unsure

### Mon Oct 26 19:15 - page 6 - yes (correct)

- Injected: **inj-03: dag_paused** on `hourly_quality_check`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused hourly_quality_check at 17:23 UTC (chg-0005) for a warehouse table migration. The 18:05 and 19:05 runs were therefore never started. Nothing points to a scheduler failure.
- Impact: sla; answered at paging time: unsure, after the follow-up: yes

### Mon Oct 26 20:15 - page 7 - yes (correct)

- Injected: **inj-03: dag_paused** on `hourly_quality_check`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused hourly_quality_check at 17:23 UTC for a warehouse table migration (chg-0005). Nothing since then unpauses it or announces an end time, so the pause may have been forgotten. The migration is still unconfirmed.
- Impact: sla; answered at paging time: unsure, after the follow-up: yes

### Mon Oct 26 21:15 - page 8 - yes (correct)

- Injected: **inj-03: dag_paused** on `hourly_quality_check`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused the DAG at 17:23 UTC (chg-0005) for a warehouse table migration. No run has started since the 17:05 run, so the late-start alert is the pause working as set. Nothing shows an end time or whether the migration is finished.
- Impact: sla; answered at paging time: unsure, after the follow-up: yes

### Mon Oct 26 22:15 - page 9 - yes (correct)

- Injected: **inj-03: dag_paused** on `hourly_quality_check`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused the DAG at 17:23 UTC (chg-0005) for a warehouse table migration. It is still paused and no run has started since 17:05. Nothing shows the pause was lifted or that it has an end time.
- Impact: sla; answered at paging time: unsure, after the follow-up: yes

### Mon Oct 26 23:15 - page 10 - yes (correct)

- Injected: **inj-03: dag_paused** on `hourly_quality_check`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused hourly_quality_check at 17:23 UTC for a warehouse table migration (chg-0005). Nothing has been recorded since then: no unpause, no end time, and no announced maintenance window. The pause has now lasted about 6 hours.
- Impact: sla; answered at paging time: unsure, after the follow-up: yes

### Tue Oct 27 00:15 - page 11 - yes (correct)

- Injected: **inj-03: dag_paused** on `hourly_quality_check`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused the DAG on purpose at 17:23 UTC on 10/26 (chg-0005) for a warehouse table migration. Nothing shows it was unpaused. The pause has now run about 7 hours with no announced end.
- Impact: sla; answered at paging time: unsure, after the follow-up: yes

### Tue Oct 27 01:15 - page 12 - yes (correct)

- Injected: **inj-03: dag_paused** on `hourly_quality_check`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused hourly_quality_check at 17:23 UTC on 10-26 with the note 'during warehouse table migration' (chg-0005). The DAG is still paused, so no runs have started. Nothing announces an end time for the pause or the migration.
- Impact: sla; answered at paging time: unsure, after the follow-up: yes

### Tue Oct 27 02:15 - page 13 - yes (correct)

- Injected: **inj-03: dag_paused** on `hourly_quality_check`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused hourly_quality_check at 17:23Z on 10-26 (chg-0005) for a warehouse table migration. The DAG is still paused, and no later change unpauses it or gives an end to the migration window.
- Impact: sla; answered at paging time: unsure, after the follow-up: yes

### Tue Oct 27 19:14 - page 14 - yes (correct)

- Injected: **inj-05: trino_heavy_query** on `trino-lakehouse`. One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.
- Cause (unknown): Both Trino workers are under heavy memory and CPU pressure: heap use is near 95%, GC time is about 8x normal and CPU is throttled. The trigger is not visible. The only recent change, a Flink rollback at 16:12, is unrelated to Trino. A heavy query or a worker resource limit is the likely source, but I did not confirm either.
- Impact: latency, lag

### Wed Oct 28 05:39 - page 16 - yes (correct)

- Injected: **inj-06: consumer_slow_deploy** on `orders-etl`. A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- Cause (failure): orders-etl-consumer-0 restarted (restart count went from 3 to 4) and consumer lag jumped at the same time. Topic traffic is normal and there were no recent changes, so a consumer crash or restart looks most likely. The logs and k8s events don't show the reason.
- Impact: lag

### Wed Oct 28 05:39 - page 17 - yes (correct)

- Injected: **inj-06: consumer_slow_deploy** on `orders-etl`. A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- Cause (failure): orders-etl-consumer-1 restarted at 05:39 (restart counter 1 to 2). Consumer-group lag jumped from about 125 to 7671 in the same minute while the consume rate stayed flat at about 534 msg/s. That fits a restart or rebalance stall. I found no deploy or config change, no k8s events and no ERROR logs, so the root cause of the restart is unknown.
- Impact: lag

### Wed Oct 28 20:09 - page 18 - yes (correct)

- Injected: **inj-07: consumer_slow_deploy** on `orders-etl`. A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- Cause (failure): orders-etl-consumer-0 restarted (4 to 5) at 20:09. Consumer lag jumped from about 270 to 6320 in the same minute. Topic traffic is normal, so the restart looks like the cause rather than load. The reason for the restart is unknown: there are no k8s events, error logs or recent changes.
- Impact: lag

### Wed Oct 28 20:09 - page 19 - yes (correct)

- Injected: **inj-07: consumer_slow_deploy** on `orders-etl`. A deploy makes orders-etl-consumer slower than the produce rate: lag grows until rollback, then drains.
- Cause (failure): orders-etl-consumer-1 restarted at about 20:09, taking its restart count from 2 to 3. Consumer group lag jumped from about 270 to 6320 in the same minute. The restart is the likely cause, but I found no logs, Kubernetes events or changes to confirm why it restarted.
- Impact: lag

### Wed Oct 28 21:29 - page 20 - no (correct)

- Injected: **nothing injected** (a page-out of the quiet world)
- Cause (unknown): A brief dip in scheduling delay, which is the healthy direction. It looks like normal noise: over the last 3 hours the delay ranged 0.02–0.43s and has already returned to about 0.25s. The seasonal baseline is very tight (±0.021), which is why the z-score is large.
- Impact: none

### Thu Oct 29 18:23 - page 22 - yes (correct)

- Injected: **inj-09: broker_gc_pressure** on `kafka-2`. A client fetching huge batches fills one broker's heap: long GC pauses, slow produce requests, the broker falls out of the in-sync replicas again and again.
- Cause (change): The finance-reporting-consumer was launched at 18:11 and is backfilling orders from offset 0 with a 500MB fetch.max.bytes. Under-replicated partitions and produce latency began rising a few minutes later. This looks like the heavy backfill reads putting load on the brokers. I did not check the orders topic or broker I/O, so the link is inferred from timing.
- Impact: latency, errors

### Fri Oct 30 11:09 - page 23 - yes (correct)

- Injected: **inj-10: consumer_memory_leak** on `orders-etl-consumer`. A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- Cause (change): The orders-etl-consumer 1.8.1 deploy at 09:13 added an in-process customer profile cache. Pod memory has climbed steadily since then (about 420MB to 556MB) and has not levelled off. That looks like an unbounded cache or a leak, though I can't confirm it from these results.
- Impact: lag

### Fri Oct 30 11:44 - page 24 - yes (correct)

- Injected: **inj-10: consumer_memory_leak** on `orders-etl-consumer`. A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- Cause (change): The orders-etl-consumer 1.8.1 deploy at 09:13 added an in-process customer-profile cache. Pod memory has grown steadily since then with no plateau, which looks like an unbounded cache or a leak. Memory limits and the other pod's trend were not checked.
- Impact: none

### Sat Oct 31 01:10 - page 25 - no (correct)

- Injected: **nothing injected** (a page-out of the quiet world)
- Cause (unknown): One run of hourly_quality_check took 5.2 min against a usual ~1.6 min. The cause isn't visible in the data. No related change or platform fault explains it. It looks like a one-off slow run.
- Impact: none

### Sat Oct 31 01:54 - page 26 - yes (correct)

- Injected: **inj-11: trino_heavy_query** on `trino-lakehouse`. One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.
- Cause (failure): trino-worker-1 is under severe JVM memory pressure. Heap is at about 96%, GC time is 27x normal, and CPU is 8.5x normal. This looks like a heavy query or a memory problem on the worker. I found no deploy or config change, and no restart or OOM event. The memory-pool logs show only routine reservations, so the trigger is unconfirmed.
- Impact: latency, errors

### Sat Oct 31 01:54 - page 27 - yes (correct)

- Injected: **inj-11: trino_heavy_query** on `trino-lakehouse`. One huge ad-hoc query takes both Trino workers' memory and CPU: other queries queue, slow down and fail.
- Cause (unknown): At about 01:43 trino-worker-0 went from normal to heap near 93%, GC time 24x the baseline and CPU 8x the baseline. This looks like memory pressure, possibly from a heavy query. I found no deploy, restart or k8s event that explains it, so the trigger is not identified. I did not check query volume against its baseline, so I cannot blame traffic.
- Impact: latency, errors

### Sat Oct 31 10:20 - page 28 - no (dangerous)

- Injected: **inj-12: producer_crashloop** on `orders-producer`. A bad config makes orders-producer crash on startup: it restarts in a loop and no orders arrive.
- Cause (unknown): The orders-producer container restarted once (counter went from 13 to 14) after about 24 hours with no restarts. I found no k8s events, error logs or recent changes to explain it.
- Impact: none

### Sun Nov 01 03:40 - page 32 - yes (correct)

- Injected: **inj-13: dag_paused** on `customer_agg`. Someone pauses a DAG and forgets it: the runs that were due while it is paused do not happen.
- Cause (change): m.ortiz paused customer_agg at 02:30 UTC (chg-0021) for a warehouse table migration. That is why the 03:30 run did not start. I found no announced end time for the pause and no unpause.
- Impact: sla; answered at paging time: unsure, after the follow-up: yes

### Sun Nov 01 13:14 - page 33 - yes (correct)

- Injected: **inj-14: consumer_memory_leak** on `orders-etl-consumer`. A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- Cause (change): Memory on orders-etl-consumer-0 has climbed steadily since the 11:02 deploy of consumer 1.8.1, which caches customer profiles in-process. The pattern looks like a cache that is never evicted or capped. Topic traffic is also rising, but that does not explain a steady climb that started right at the deploy.
- Impact: none

### Sun Nov 01 13:14 - page 34 - yes (correct)

- Injected: **inj-14: consumer_memory_leak** on `orders-etl-consumer`. A deploy of orders-etl-consumer leaks memory: both pods grow linearly, each at its own rate.
- Cause (change): Deploy chg-0023 (orders-etl-consumer 1.7.3 to 1.8.1, in-process customer profile cache) went out at 11:02. Memory on consumer-1 has risen almost linearly since then, from about 415MB to 577MB. That looks like an unbounded cache or a leak, not a traffic effect.
- Impact: none

### Sun Nov 01 18:10 - page 35 - unsure (deferred)

- Injected: **nothing injected** (a page-out of the quiet world)
- Cause (unknown): The 18:05 run took 5.6 min against a usual 1.3–1.9 min. It is the only slow run in the last 12 hours. I found no cause, and the next run (19:05) hasn't happened yet, so I can't tell a one-off from the start of a trend.
- Impact: none; answered at paging time: unsure, after the follow-up: unsure

### Sun Nov 01 20:54 - page 36 - no (correct)

- Injected: **nothing injected** (a page-out of the quiet world)
- Cause (unknown): The scheduling delay is only a small fluctuation. It is 0.29s against an expected 0.20s, a statistically large z-score but a tiny absolute value. The series has hovered around 0.23s for the last 4 hours, and 24 hours earlier the same window averaged 0.26s. No fault is visible.
- Impact: none

# Arm E: arm D with the guard

**Development set, not a final result.** Appended after arm D. The guard was written after reading D's two dangerous answers, so this shows it does what it was written for, on the cases it was written for.

- written by `scripts/triage_dev_week_v3.py --arm E` at commit `47393eb`; notes on `live-week-1-v2-triage-v3-guard`
- the agent of arm D (same prompt, schema, tools, budgets, follow-up at paging + 10 min) plus the guard, in code: a note answering needs_human "no" is accepted only if its cause category is not "unknown" AND its impact is ["none"]; otherwise it is converted to "unsure" (recorded with the reason) and followed up. A fresh run: the agent's answers before the guard may differ from D's
- this invocation: 29 first investigations and 15 follow-ups in 185 s of wall time

| | D first | D final | E first | E final |
|---|---|---|---|---|
| real page-outs (25): correct | 13 | 22 | 14 | 23 |
| real page-outs (25): dangerous | 2 | 2 | 0 | 0 |
| real page-outs (25): deferred | 10 | 1 | 11 | 2 |
| noise page-outs (4): correct | 3 | 3 | 0 | 0 |
| noise page-outs (4): wasted | 0 | 0 | 0 | 1 |
| noise page-outs (4): deferred | 1 | 1 | 4 | 3 |
| cause names the injected target (of 25) | 14 | 13 | 16 | 14 |
| follow-ups | | 11 | | 15 |
| minutes to the final answer, median / max | | 0.3 / 10.1 | | 10.1 / 10.2 |
| cost (USD, estimate, follow-ups included) | | 1.31 | | 1.52 |

Notes the guard converted: 6.

| page | truth | record | cause | impact | reason |
|---|---|---|---|---|---|
| 20 (nothing injected) | noise | note | unknown | none | "no" needs a known cause and no impact, but the cause category is "unknown" |
| 25 (nothing injected) | noise | note | unknown | none | "no" needs a known cause and no impact, but the cause category is "unknown" |
| 35 (nothing injected) | noise | note | unknown | none | "no" needs a known cause and no impact, but the cause category is "unknown" |
| 36 (nothing injected) | noise | note | unknown | none | "no" needs a known cause and no impact, but the cause category is "unknown" |
| 35 (nothing injected) | noise | follow_up | unknown | none | "no" needs a known cause and no impact, but the cause category is "unknown" |
| 36 (nothing injected) | noise | follow_up | unknown | none | "no" needs a known cause and no impact, but the cause category is "unknown" |

Dangerous answers (final): none.

## Per page-out: D and E

| time (UTC) | page | truth | scenario | D first | D final | E first | E follow-up | E final | E outcome |
|---|---|---|---|---|---|---|---|---|---|
| Oct 26 03:02 | 1 | real | broker_gc_pressure | yes | yes | yes | - | yes | correct |
| Oct 26 08:07 | 2 | real | producer_crashloop | no | no | unsure | yes | yes | correct |
| Oct 26 18:15 | 5 | real | dag_paused | unsure | unsure | unsure | unsure | unsure | deferred |
| Oct 26 19:15 | 6 | real | dag_paused | unsure | yes | unsure | yes | yes | correct |
| Oct 26 20:15 | 7 | real | dag_paused | unsure | yes | unsure | yes | yes | correct |
| Oct 26 21:15 | 8 | real | dag_paused | unsure | yes | unsure | unsure | unsure | deferred |
| Oct 26 22:15 | 9 | real | dag_paused | unsure | yes | unsure | yes | yes | correct |
| Oct 26 23:15 | 10 | real | dag_paused | unsure | yes | unsure | yes | yes | correct |
| Oct 27 00:15 | 11 | real | dag_paused | unsure | yes | yes | - | yes | correct |
| Oct 27 01:15 | 12 | real | dag_paused | unsure | yes | unsure | yes | yes | correct |
| Oct 27 02:15 | 13 | real | dag_paused | unsure | yes | unsure | yes | yes | correct |
| Oct 27 19:14 | 14 | real | trino_heavy_query | yes | yes | yes | - | yes | correct |
| Oct 28 05:39 | 16 | real | consumer_slow_deploy | yes | yes | yes | - | yes | correct |
| Oct 28 05:39 | 17 | real | consumer_slow_deploy | yes | yes | yes | - | yes | correct |
| Oct 28 20:09 | 18 | real | consumer_slow_deploy | yes | yes | yes | - | yes | correct |
| Oct 28 20:09 | 19 | real | consumer_slow_deploy | yes | yes | yes | - | yes | correct |
| Oct 28 21:29 | 20 | noise | - | no | no | unsure | yes | yes | wasted |
| Oct 29 18:23 | 22 | real | broker_gc_pressure | yes | yes | yes | - | yes | correct |
| Oct 30 11:09 | 23 | real | consumer_memory_leak | yes | yes | yes | - | yes | correct |
| Oct 30 11:44 | 24 | real | consumer_memory_leak | yes | yes | yes | - | yes | correct |
| Oct 31 01:10 | 25 | noise | - | no | no | unsure | unsure | unsure | deferred |
| Oct 31 01:54 | 26 | real | trino_heavy_query | yes | yes | yes | - | yes | correct |
| Oct 31 01:54 | 27 | real | trino_heavy_query | yes | yes | yes | - | yes | correct |
| Oct 31 10:20 | 28 | real | producer_crashloop | no | no | unsure | yes | yes | correct |
| Nov 01 03:40 | 32 | real | dag_paused | unsure | yes | unsure | yes | yes | correct |
| Nov 01 13:14 | 33 | real | consumer_memory_leak | yes | yes | yes | - | yes | correct |
| Nov 01 13:14 | 34 | real | consumer_memory_leak | yes | yes | yes | - | yes | correct |
| Nov 01 18:10 | 35 | noise | - | unsure | unsure | unsure | unsure | unsure | deferred |
| Nov 01 20:54 | 36 | noise | - | no | no | unsure | unsure | unsure | deferred |
