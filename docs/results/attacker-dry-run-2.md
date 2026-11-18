# Attacker campaign `dry-run-2`: a dry run (not scored)

**Not scored, on purpose.** This is one short real run of the attacker agent, to see what it injects. Its injections were not run through the engine or the triage agent, and nothing here measures detection.

- window: 2026-10-29T00:00:00Z .. 2026-10-30T00:00:00Z of `data_live` (the quiet world), campaign mode (the live loop at max speed, no broker)
- agent: `claude-sonnet-5-5`, effort `medium`, woken every 60 simulated minutes (24 wakes); per wake at most 8 tool calls
- budget enforced in code: at most 3 injections a day, at most 2 active at once, starts at least 3 h apart, only series described (describe_normal) in the same wake; asked for at least 1 a day
- cost: **USD 0.365** (estimate), tokens {'input': 158503, 'output': 4758, 'cache_read': 0, 'cache_write': 0}
- tool calls: 14 (list_active 5, list_scenarios 3, describe_normal 3, inject_scenario 3)
- recorded control stream: [attacker-dry-run-2.control.jsonl](attacker-dry-run-2.control.jsonl) (3 commands); full record with every message: `runs/attacker/dry-run-2/record.json` (gitignored)

## What it injected

3 injections (2026-10-29: 3).

| | count |
|---|---|
| scenario | 3 |
| noticeable | 2 |
| subtle | 1 |
| touches layer 1 | 3 |
| touches layer 2 | 2 |
| touches layer 3 | 2 |

| id | start (UTC) | stop | kind | what | entities | severity | intent |
|---|---|---|---|---|---|---|---|
| atk-001 | 10-29T00:00 | 10-29T02:00 | scenario | broker_gc_pressure on kafka-1 (magnitude=0.8, duration_min=120) | cluster:kafka-ingest, pod:kafka-1 | noticeable | A client fetching huge batches fills kafka-1's heap; long GC pauses, slow produce, ISR flapping. |
| atk-002 | 10-29T03:00 | 10-29T08:00 | scenario | flink_state_leak on clickstream-enrich (rate_tm0_mib_per_hour=60, rate_tm1_mib_per_hour=75, duration_min=300) | pod:flink-taskmanager-0, pod:flink-taskmanager-1 | subtle | A Flink job update keeps keyed state forever; TaskManager off-heap memory creeps up slowly. |
| atk-003 | 10-29T12:00 | 10-29T12:40 | scenario | trino_heavy_query on trino-lakehouse (magnitude=0.8, duration_min=40) | cluster:trino-lakehouse, pod:trino-worker-0, pod:trino-worker-1, resource_group:adhoc | noticeable | An analyst launches a huge cross-join ad-hoc query that hogs both Trino workers' heap and CPU; other queries queue and some fail. |

## Generic incidents: novelty, and overlap with the named scenarios

No generic incident in this run, so there is no novelty text or overlap to report.

## Rejections (the attacker's own check: the injector's rules, the budget, described series, the gap)

None.

## Replayed through the injector, without the LLM

3 of 3 injections started; 0 commands refused by the injector at the publisher's check; readings held at a physical limit: none. Telemetry messages: 2,941.

## The agent's notes, wake by wake

| wake (UTC) | tool calls | commands | note |
|---|---|---|---|
| 10-29T00:00 | 3 | atk-001 | I injected one incident now: `broker_gc_pressure` on kafka-1, rated noticeable. The injector accepted it as `atk-001`.  - **What it stands for:** a client fetching huge batches fills kafka-1's heap. That causes long GC pauses, slow produce requests, and the broker repeatedly dropping out of the in-sync replicas. - **Settings:** magnitude 0.8 and duration 120 minutes. It runs from 00:00 to 02:00 UTC. - **Sizing:** I checked the last 2 days of normal readings for the six series it touches. Heap is normally about 0.55, GC time about 18 ms/s and produce p99 about 16 ms. Failed produce requests and under-replicated partitions are normally zero.  I injected nothing else this wake. It was the first injection of the day. |
| 10-29T01:00 | 0 | - | Nothing injected this wake. atk-001 started at 00:00, so a new injection can't start until 03:00 at the earliest because of the 3-hour spacing rule. It is also still running, so one slot is already in use. I'll look at a new incident at a later wake, with a different part of the platform and a subtle severity, to add variety. |
| 10-29T02:00 | 1 | - | The last injection started at 00:00 and ended at 02:00. A new one could start at 03:00 at the earliest, so I'm not injecting now. The next wake is better for it, and I'd like to use a different part of the platform then.  I did nothing this wake. |
| 10-29T03:00 | 4 | atk-002 | I injected one incident: a subtle `flink_state_leak` on `clickstream-enrich` (atk-002). It started at 03:00 and is planned to stop at 08:00.  - **What it does:** a job update keeps keyed state forever, so the TaskManagers' off-heap memory creeps up. I set the growth to 60 MiB/h on `flink-taskmanager-0` and 75 MiB/h on `flink-taskmanager-1`. - **Why this one:** the campaign so far has only a noticeable Kafka incident (atk-001), with no Flink incident and nothing subtle. A slow leak adds both. - **Sizing:** both TaskManagers sit around 2.5 GB against a 4.295 GB limit. Over 5 hours the leak adds roughly 300–375 MiB, so memory stays well under the limit. It should be a small but steady rise above the normal 2.45–2.7 GB band. - **Budget:** this is the second injection today, and it started 3 hours after atk-001, which meets the spacing rule.  I did not use `inject_effects`, because a named scenario already fits. |
| 10-29T04:00 | 0 | - | Nothing injected this wake. Two injections have already started today, and atk-002 (the Flink state leak) is still running until 08:00. The last start was at 03:00, so the 3-hour spacing rule blocks any new start before 06:00. I'll look again at a later wake. |
| 10-29T05:00 | 0 | - | I did not inject anything this wake.  Two injections have already started today, so only one more is allowed. The last start was atk-002 at 03:00, so the next start has to be at 06:00 or later. A start now at 05:00 would break the 3-hour spacing rule. atk-002 is still running, which uses one of the two active slots. I'll reconsider at a later wake. |
| 10-29T06:00 | 0 | - | I didn't inject anything this wake.  - **Budget:** 2 of today's 3 injections have already started (atk-001 at 00:00, atk-002 at 03:00). - **Spacing:** atk-002 started at 03:00, so the next start has to be at 06:00 or later. That condition is met, but atk-002 is still active until 08:00. - **Variety:** Both injections so far were named scenarios and neither was generic. The next one should probably be a generic incident on a different part of the platform, such as Trino, Airflow or Spark. A good injection needs `describe_normal` checks on every series it touches. I'd rather do that properly at a later wake than rush it now.  I'm holding the last slot for later in the day, once atk-002 has ended and a more varied incident can be designed. |
| 10-29T07:00 | 0 | - | I did nothing this wake.  - **Budget:** 2 of today's 3 injections are already used. - **Active:** atk-002, the Flink state leak, is still running until 08:00. A new injection would be the second active one. - **Spacing:** atk-002 started at 03:00, so the 3-hour gap is satisfied. I'd still rather wait until the leak ends so the incidents don't overlap. - **Next step:** I may inject a different incident later today. Spark, Trino or Airflow would add variety, since both injections so far hit Kafka and Flink. |
| 10-29T08:00 | 1 | - | I did nothing this wake. Two injections have already started today (atk-001 and atk-002), and the daily limit is three. The campaign ends at 00:00 UTC, so there is room for one more. No injections are running now, and the Flink state leak (atk-002) ended at 08:00. I'm keeping the last slot for a later wake, when I can pick a different part of the platform. So far the campaign has had no Spark, Airflow or Trino incidents and no generic ones. |
| 10-29T09:00 | 1 | - | I didn't inject anything this wake. Two injections have already started today (atk-001 and atk-002), so only one more is allowed. Both have ended, and none are active now. The campaign hasn't yet covered Trino, Spark, Airflow or a generic incident, so I'm holding the last slot for one of those later today, when I can size it properly. |
| 10-29T10:00 | 1 | - | I didn't inject anything this time. Two of today's three injections have already run (atk-001 and atk-002), and both have ended, so nothing is active. I'm holding the last slot rather than filling it now. The campaign ends at 00:00 UTC, and I'd rather use the slot on something different from the Kafka and Flink incidents so far, such as a Trino or Airflow incident. |
| 10-29T11:00 | 3 | atk-003 | I injected one incident this wake: `atk-003`, a `trino_heavy_query` on `trino-lakehouse`, rated noticeable. It starts at 12:00 UTC and ends at 12:40 UTC.  The scenario stands for an analyst launching a huge ad-hoc query that hogs both Trino workers' heap and CPU, so other queries queue and some fail. I used magnitude 0.8 and a 40-minute duration. I described all 11 series it touches before injecting.  I chose Trino because the campaign has not touched it yet, and the earlier incidents were Kafka and Flink. The 60-minute delay puts it at the start of the afternoon load ramp, when Trino CPU, heap and query volume are already climbing. It does not overlap any active injection.  This is the third injection today, so the daily budget is used up. |
| 10-29T12:00 | 0 | - | Nothing injected this time. The daily budget is already used: 3 of 3 injections have started today (atk-001, atk-002 and atk-003). Another injection would exceed the limit. atk-003 is also still active, and a new start would sit too close to it given the 3-hour minimum spacing. |
| 10-29T13:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (atk-001, atk-002 and atk-003), so another injection would exceed the daily limit. |
| 10-29T14:00 | 0 | - | I did nothing this wake. The daily budget is already used: 3 of 3 injections have started today (atk-001, atk-002 and atk-003). Starting another would break the limit of 3 per day. |
| 10-29T15:00 | 0 | - | I did nothing this wake. The daily budget is used up: 3 of 3 injections have already started today (atk-001, atk-002 and atk-003). I can't start another one without going over the limit. |
| 10-29T16:00 | 0 | - | I did nothing this wake. Three injections have already started today, which is the daily budget of 3, so another one isn't allowed. |
| 10-29T17:00 | 0 | - | Today's budget is already used: 3 of 3 injections have started. I'm not injecting anything this time, because another one would break the daily limit. |
| 10-29T18:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (atk-001, atk-002 and atk-003), so another injection isn't allowed. |
| 10-29T19:00 | 0 | - | I made no injection this wake. The daily budget is already used: 3 of 3 injections have started today (atk-001, atk-002 and atk-003). The campaign also ends at 00:00 UTC, so a new injection would be over budget and there are only a few hours left. |
| 10-29T20:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (atk-001, atk-002 and atk-003), so another injection would exceed the daily limit. Nothing is active right now. |
| 10-29T21:00 | 0 | - | I did nothing this wake. Three injections have already started today, which is the daily budget of 3. The campaign also ends at 00:00 UTC, so there is no room for another one. |
| 10-29T22:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (atk-001, atk-002 and atk-003), so another injection would exceed the daily limit. The campaign also ends at 00:00 UTC, which is two hours away. |
| 10-29T23:00 | 0 | - | I did nothing this wake. Today's budget of 3 injections is already used (atk-001, atk-002 and atk-003). The campaign also ends at 00:00 UTC, so a new injection wouldn't have time to run. |
