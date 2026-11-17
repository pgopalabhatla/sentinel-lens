# Attacker campaign `dry-run-1`: a dry run (not scored)

**Not scored, on purpose.** This is one short real run of the attacker agent, to see what it injects. Its injections were not run through the engine or the triage agent, and nothing here measures detection.

- window: 2026-10-26T00:00:00Z .. 2026-10-27T00:00:00Z of `data_live` (the quiet world), campaign mode (the live loop at max speed, no broker)
- agent: `claude-sonnet-5-5`, effort `medium`, woken every 60 simulated minutes (24 wakes); per wake at most 8 tool calls
- budget enforced in code: at most 3 injections a day, at most 2 active at once; asked for at least 1 a day
- cost: **USD 0.315** (estimate), tokens {'input': 135326, 'output': 4458, 'cache_read': 0, 'cache_write': 0}
- tool calls: 10 (describe_normal 4, list_scenarios 2, inject_scenario 2, list_platform 1, inject_effects 1)
- recorded control stream: [attacker-dry-run-1.control.jsonl](attacker-dry-run-1.control.jsonl) (3 commands); full record with every message: `runs/attacker/dry-run-1/record.json` (gitignored)

## What it injected

3 injections (2026-10-26: 3).

| | count |
|---|---|
| scenario | 2 |
| generic | 1 |
| noticeable | 2 |
| subtle | 1 |
| touches layer 1 | 3 |
| touches layer 2 | 2 |
| touches layer 3 | 2 |

| id | start (UTC) | stop | kind | what | entities | severity | intent |
|---|---|---|---|---|---|---|---|
| atk-001 | 10-26T00:10 | 10-26T02:10 | scenario | broker_gc_pressure on kafka-1 (magnitude=0.8, duration_min=120) | cluster:kafka-ingest, pod:kafka-1 | noticeable | A client fetching huge batches pressures kafka-1's heap, causing GC pauses and ISR flapping. |
| atk-002 | 10-26T02:10 | 10-26T03:10 | generic | add ramp 0.3 on jvm_heap_used_ratio\|trino-worker-0 for 60 min; add ramp 200 on jvm_gc_time_ms_per_sec\|trino-worker-0 for 60 min; add ramp 1.5 on container_cpu_usage_cores\|trino-worker-0 for 60 min; add ramp 15 on trino_query_p95_sec\|trino-lakehouse for 60 min; add ramp 12 on trino_queued_queries\|trino-lakehouse for 60 min; add ramp 10 on trino_resource_group_queued\|adhoc for 60 min; add ramp 2 on trino_failed_queries_per_min\|trino-lakehouse for 60 min | cluster:trino-lakehouse, pod:trino-worker-0, resource_group:adhoc | noticeable | A runaway ad-hoc cross-join saturates Trino worker-0 memory and heap; GC grows, queries slow and queue up in the adhoc resource group, some fail. |
| atk-003 | 10-26T13:00 | 10-26T20:00 | scenario | consumer_memory_leak on orders-etl-consumer (rate_pod0_mib_per_hour=45, rate_pod1_mib_per_hour=55, duration_min=420) | pod:orders-etl-consumer-0, pod:orders-etl-consumer-1 | subtle | A new consumer build leaks memory slowly; pods creep toward their limit over hours. |

## Rejections (the attacker's own check: the injector's rules, the budget)

None.

## Replayed through the injector, without the LLM

3 of 3 injections started; 0 commands refused by the injector at the publisher's check; readings held at a physical limit: none. Telemetry messages: 2,941.

## The agent's notes, wake by wake

| wake (UTC) | tool calls | commands | note |
|---|---|---|---|
| 10-26T00:00 | 2 | atk-001 | I injected one incident: a `broker_gc_pressure` scenario on kafka-1. It starts at 00:10 UTC and is planned to stop at 02:10, so it lasts 120 minutes. The scenario stands for a client fetching huge batches and filling the broker's heap, which causes GC pauses and ISR flapping. I set magnitude to 0.8 and severity to noticeable.  I picked a named scenario on Kafka because nothing had been injected yet. It leaves room for 2 more injections today, such as a subtle one on a different part of the platform. |
| 10-26T01:00 | 0 | - | I did nothing this wake. atk-001 (Kafka broker GC pressure) is still running until 02:10, and it is our only active injection. Another injection now would overlap with it on the same part of the platform. We have used 1 of 3 injections today, so there is room later. I'll add a different kind of incident, such as a subtle generic one on Trino, Spark or Flink, once atk-001 ends. |
| 10-26T02:00 | 6 | atk-002 | I injected one new incident, atk-002. It is a generic Trino incident, not a named scenario, and I rated it noticeable. It starts at 02:10 UTC and ends at 03:10.  **What it stands for:** a runaway ad-hoc query that exhausts memory on trino-worker-0. Over the hour, that worker's heap, GC time and CPU all ramp up. Query p95 latency climbs, and queued queries build up in the cluster and the adhoc resource group. Failed queries per minute also rise.  **Why this one:** the only other injection today (atk-001) is a Kafka broker problem, so this covers a different part of the platform. It is also the first generic incident of the campaign.  **Sizing:** I only checked normal values for Trino query p95, which sits around 3.7 s. I picked the other magnitudes without checking them against their normals.  I didn't inject anything else because atk-001 is still active until 02:10 and the limit is two active at once. Today's count is now 2 of 3. |
| 10-26T03:00 | 0 | - | I did nothing this wake. Two injections have already started today (budget is at most 3), and atk-002 is still active until 03:10. Another injection now would leave two active at once, which is the limit. So far both injections were noticeable and both hit Kafka or Trino. A later wake should add a subtle incident on a different part of the platform, such as Spark, Airflow or Flink, once atk-002 has ended. |
| 10-26T04:00 | 0 | - | I didn't inject anything this wake.  Two of the three daily injections are already used: atk-001 (Kafka broker GC pressure) and atk-002 (Trino runaway query). Nothing is active now. The campaign has no subtle injections yet, so a third one should be subtle and touch a different part of the platform, such as Spark, Airflow or Flink. I'm holding that last slot for a later wake. That lets me pick a target and size the effects against normal values for it, instead of rushing one in at 04:00. |
| 10-26T05:00 | 0 | - | I didn't inject anything this wake.  Two of today's three injections are already used. Both were noticeable and hit Kafka and Trino. If I add a third now, it should be a subtle one on Spark, Airflow or Flink, and I'd rather pick that one with care than rush it. Nothing is active at the moment, so there is no overlap to worry about. |
| 10-26T06:00 | 0 | - | I did nothing this wake. Two of today's three injections are already used (atk-001 on Kafka and atk-002 on Trino). Both were noticeable, and none are active now. I'm saving the last slot for a subtle incident on Spark, Airflow or Flink. That would add variety to the campaign, and I'll pick a better time for it than right now. |
| 10-26T07:00 | 0 | - | I did nothing this wake. Two injections have already started today (atk-001 on Kafka and atk-002 on Trino), which leaves one slot in the daily budget. Neither is active now, so the 2-active limit isn't a constraint.  I'm saving the last slot for a later wake. A subtle incident on Spark, Airflow or Flink would add variety, since both injections so far were noticeable and neither touched those platforms. |
| 10-26T08:00 | 0 | - | Today I've used 2 of the 3 injections, and the campaign has no subtle incidents yet. I'm skipping this wake. If I injected now, I'd use the last slot on the morning peak. I'd rather save that slot for a subtle incident on a different part of the platform, such as Spark, Flink or Airflow, at a better time later in the day.  I injected nothing this wake. |
| 10-26T09:00 | 0 | - | I didn't inject anything this wake.  Two of today's three injections are already used: atk-001, the Kafka broker GC pressure, and atk-002, the Trino ad-hoc memory saturation. Both were noticeable. I'm keeping the last slot for a subtle incident on a different part of the platform, such as Spark, Airflow or Flink, so the campaign covers more ground. I'll pick that incident at a later wake. |
| 10-26T10:00 | 0 | - | I didn't inject anything this wake.  Two of the three injections allowed today are already used (atk-001 on Kafka and atk-002 on Trino). Nothing is active now. The one injection I have left is better spent on a different part of the platform, such as Spark, Flink or Airflow, or on a subtle incident. I'm holding it for a later wake so the campaign stays varied and the day's last slot isn't used on a weaker incident. |
| 10-26T11:00 | 0 | - | I didn't inject anything this wake.  Two of today's three injections are already used (atk-001 on Kafka and atk-002 on Trino). Nothing is active now. I'm holding the last slot for a subtle incident on Spark, Airflow or Flink. That would add variety, because both earlier injections were noticeable and neither touched those three systems. I'll also have more time before the campaign ends at 00:00 UTC to choose a realistic target. |
| 10-26T12:00 | 0 | - | I did nothing this wake. Two of today's three injection slots are already used (atk-001 on Kafka, atk-002 on Trino). Both were noticeable, and neither is active now. If I add a third injection, it should be subtle and on a part of the platform not yet touched, such as Spark, Airflow or Flink. I'm saving that last slot for a later wake, when I can pick a better-timed and better-sized incident. |
| 10-26T13:00 | 2 | atk-003 | I injected one incident this wake: `consumer_memory_leak` on `orders-etl-consumer` (atk-003), severity subtle.  It stands for a new consumer build that leaks memory slowly. The pods grow at 45 and 55 MiB per hour and creep toward their limit. It starts now (13:00) and runs for 420 minutes, to 20:00.  I chose it because the campaign so far had only noticeable incidents, both on the Kafka and Trino side. This adds a subtle, slow-burn fault on a different part of the platform. That makes 3 injections today, which uses the daily budget, and 1 is active. |
| 10-26T14:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (3 of 3). atk-003, the consumer memory leak, is still running, so another injection would also add to the active load. I'll wait for the next day's budget. |
| 10-26T15:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (3 of 3). atk-003, the consumer memory leak, is still active, so starting anything new would break the daily limit. |
| 10-26T16:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (3 of 3). One injection, atk-003, is still active. Injecting again would exceed the daily limit. |
| 10-26T17:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (3 of 3). atk-003, the consumer memory leak, is still running until 20:00. Adding another injection would break the daily limit. |
| 10-26T18:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (3 of 3), so I can't start another one. atk-003, the consumer memory leak, is still running until 20:00. |
| 10-26T19:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (atk-001, atk-002 and atk-003). A new injection would go over the daily limit, and atk-003 is still running. |
| 10-26T20:00 | 0 | - | I did nothing this wake. The daily budget is already used: 3 of 3 injections have started today. Another injection would break the limit, so I'm holding until the budget resets. |
| 10-26T21:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (atk-001, atk-002 and atk-003), so another injection would exceed the daily limit. Nothing is active right now, so no injection needs to be stopped or changed. |
| 10-26T22:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (atk-001, atk-002 and atk-003), and no injection is active. The campaign also ends at 00:00 UTC, so another injection wouldn't be allowed. |
| 10-26T23:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (atk-001, atk-002 and atk-003). The campaign also ends at 00:00 UTC, so a new injection would have almost no time to play out. |
