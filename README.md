# Sentinel Lens

Anomaly detection for a data platform, built so that every alert can be explained and every
threshold defended.

## The problem

A data platform (Kafka, Spark, Trino, Flink and Airflow on Kubernetes) produces hundreds of metrics,
and simple alerting on them is noisy: a naive detector on this project's data raises about 125 alerts
a day, three quarters of them false. People stop reading alerts like that. The goal here is to catch
real incidents early, page an engineer rarely for things that are not incidents, and say in plain
language why each alert fired ("memory is 1.3x expected for this time of day").

On one held-out test week, run once under a protocol written in advance, the final system detected
9 of 10 actionable incidents with 8 false pages in 7 days. The naive detector found all 10, with 641
false alerts. Details, limits and what did not work are below.

## How it works

```
simulator -> router -> detectors -> alert record -> correlator -> feedback
```

**Simulator** (`sentinel_sim/`). There is no real cluster. A generator produces 28 days of 1-minute
telemetry for a simulated platform: 164 metric series, logs, job runs, and 23 injected incidents with
ground-truth labels. Detectors read it only through `DataSource`, which has a clock: nothing after
"now" is visible, so no detector can use the future. See [docs/dataset.md](docs/dataset.md).

**Router** (`sentinel_lens/router.py`). Decides which rule scores each series, from the metric name
and the series catalog, and with which settings. One detector for everything does not work: a restart
counter, a queue length and a CPU reading need different treatment. The flowchart is in the next
section.

**Detectors** (`sentinel_lens/detectors/`).

- *Seasonal baseline with persistence* (`seasonal.py`, `persistence.py`). Each 5-minute reading is
  compared with the same time of day on the last 5 days of the same type (weekday or weekend). The
  median of those is "expected"; the typical spread around it is the "wobble" (1.4826 x the median
  absolute deviation, never less than 5% of expected or a per-metric minimum). A reading more than 6
  wobbles from expected is "hot", and an alert fires only when 3 of the last 5 readings are hot, so
  a single blip stays quiet.
- *Guardrail rules* (`guardrails.py`). Some things are always bad and need no baseline: a pod
  restart, a failed Flink checkpoint, an under-replicated Kafka partition. Any increase of those
  counters, or any value above zero, alerts at once, on 1-minute readings.
- *Schedule watcher* (`schedule.py`). A paused job produces no metric at all, so the only signal is
  a run that should have happened. It alerts when a scheduled run has not started 10 minutes after
  its time, has not finished by its SLA, or failed.
- *Job-run baseline* (`job_runs.py`). A run can succeed, inside its SLA, and still take twice as
  long as usual. Each finished run's duration is compared with that job's own earlier runs, using
  the same expected-and-wobble maths.

**Alert record** (`sentinel_lens/alerts.py`). Every detector's output becomes one record: time,
target, detector, score, direction, explanation. The score is severity (how far past its alert line
the reading is), not the probability that the alert is real; rules that are always bad score 1.0.

**Correlator** (`sentinel_lens/correlator.py`). One problem raises many alerts, and an engineer
should be paged once. Two alerts are linked when they are at most 15 minutes apart *and* their
entities are at most one step apart in the platform's relationship graph (a pod and its consumer
group, say). A connected group of alerts is one page.

**Feedback** (`sentinel_lens/feedback.py`). Labels ("real" / "false") are counted per kind of alert,
for example (seasonal, cpu_throttling). When a kind has at least 10 labels and fewer than about one
in four is real, it is demoted: its alerts stay attached to pages as context, but a page made only
of demoted alerts no longer pages anyone. Labels are simulated from the scorer's verdicts.

## The router, as a flowchart

The first "yes" wins. Names are the constants in `sentinel_lens/router.py` (the two metric lists
live in `sentinel_lens/detectors/guardrails.py`).

```mermaid
flowchart TD
    S["A series from the catalog<br/>(metric, unit, default_detector)"] --> Q1{"Q1: metric in<br/>FLAT_COUNTER_METRICS?"}
    Q1 -- yes --> R1["counter_rule<br/>any increase is an alert"]
    Q1 -- no --> Q2{"Q2: metric in<br/>ZERO_GAUGE_METRICS?"}
    Q2 -- yes --> R2["above_zero<br/>any value above 0 is an alert"]
    Q2 -- no --> QS{"Optional: saturation flag on AND<br/>metric in SATURATION_METRICS?<br/>The flag is off by default"}
    QS -- yes --> RS["saturation forecast<br/>not part of the evaluated system"]
    QS -- no --> Q3{"Q3: default_detector ==<br/>seasonal_baseline?"}
    Q3 -- yes --> R3["seasonal"]
    Q3 -- no --> Q4{"Q4: default_detector in<br/>SEASONAL_FALLBACK?"}
    Q4 -- yes --> R4["seasonal, marked as fallback"]
    Q4 -- no --> E["error: unknown detector<br/>nothing is guessed"]
    R3 --> P1{"metric in<br/>METRIC_MIN_CHANGE?"}
    R4 --> P1
    P1 -- yes --> M1["min_change = that entry"]
    P1 -- no --> P2{"unit in COUNT_UNITS?"}
    P2 -- yes --> M2["min_change = 6"]
    P2 -- no --> M3["min_change = 0"]
    M1 --> F["history = day_type<br/>absolute floor = min_change / 6"]
    M2 --> F
    M3 --> F
```

| Constant | Value | Series it catches |
| --- | --- | --- |
| `FLAT_COUNTER_METRICS` | `kube_pod_container_status_restarts_total`, `flink_job_restarts_total`, `flink_checkpoint_failures_total` | 27 |
| `ZERO_GAUGE_METRICS` | `kafka_under_replicated_partitions` | 1 |
| `SATURATION_METRICS` (flag off) | `container_memory_working_set_bytes` | 0 while the flag is off |
| `default_detector == seasonal_baseline` | throughput, CPU, throttling, error rates | 60 |
| `SEASONAL_FALLBACK` | `saturation_forecast`, `change_point`, `guardrail` | 76 |
| `METRIC_MIN_CHANGE` | `container_cpu_throttled_ratio`: 0.18 | 25 |
| `COUNT_UNITS` | count, queries, tasks, messages, partitions, queries/min, tasks/min | 7 seasonal series |

"Fallback" means the catalog asks for a detector that is not in the evaluated system (a saturation
forecast or a change point detector), so the series runs on the seasonal baseline instead.
`min_change` is the smallest change that can alert: with a threshold of 6 wobbles, an absolute floor
of 1 on the wobble means "ignore changes of up to 6 units".

Source: `sentinel_lens/router.py`; series counts and per-metric settings from
[docs/config-frozen.md](docs/config-frozen.md) ("Routing per metric").

## How it was evaluated

- **Split by date.** Sep 28 - Oct 4 is a warm-up week with no incidents. Oct 5 - 18 is the train
  period (12 incidents). Oct 19 - 25 is the test week (11 incidents, 10 of them actionable; the
  other is an expected traffic burst).
- **Tuned on train only.** Every method and threshold was chosen on the train period.
  [docs/log.md](docs/log.md) records each step with its numbers, including the ones that failed.
- **Frozen before the test.** The code and every parameter were frozen at the git tag
  `pre-test-freeze` ([docs/config-frozen.md](docs/config-frozen.md)), and the protocol was written
  first: which systems, which metrics, and what would not be done after seeing the numbers
  ([docs/test-protocol.md](docs/test-protocol.md)).
- **Predictions, then one run.** The author's predictions were committed, then the test week was
  run once. The runner's output was saved unedited to
  [docs/results/test-week.md](docs/results/test-week.md). Everything written after that is marked
  post-hoc and kept in a separate file.
- **One disclosed exposure.** One test-week label row (INC-014) was seen by accident on Nov 10,
  before the minimum change for CPU throttling (0.18) and its injection test were chosen. It is
  reported separately below; the full note is in the protocol.

## Results on the test week

Five systems were scored. System 5 was named the primary system in advance.

| System | Alerts per day | Per-alert precision | Incidents detected (of 10) | Wilson 95% interval | Pages | False pages per day |
| --- | --- | --- | --- | --- | --- | --- |
| 1. Naive baseline | 124.6 | 0.256 | 10 | 0.72 - 1.00 | - | - |
| 2. Seasonal baseline on everything (first build) | 87.6 | 0.294 | 9 | 0.60 - 0.98 | - | - |
| 3. Final alerts | 38.6 | 0.742 | 9 | 0.60 - 0.98 | - | - |
| 4. Final alerts + correlator pages | 38.6 | 0.742 | 9 | 0.60 - 0.98 | 26 | 1.43 |
| **5. Final + pages + frozen feedback (primary)** | 32.0 | 0.79 | 9 | 0.60 - 0.98 | 24 | 1.14 |

Source: [docs/results/test-week.md](docs/results/test-week.md), the pre-registered run. Alerts are
counted after the scorer's 30-minute dedup; "frozen feedback" means the kinds of alert demoted by the
train labels stay demoted for the whole test week and nothing is learned from it.

- **Headline.** The primary system detected 9 of 10 actionable incidents (Wilson 95% interval
  0.60 - 0.98), with per-alert precision 0.79 and 1.14 false pages per day (8 false pages in 7 days,
  out of 24 pages).
- **The miss.** INC-015, a Trino saturation, was missed by systems 3 - 5. The naive baseline caught it.
- **Without INC-014.** That incident, the one whose label was seen beforehand, was detected. Leaving
  it out, the primary system detected 8 of 9.
- **Feedback, before vs. after.** Per-alert precision 0.742 to 0.79, false pages 10 to 8. No
  incident stopped paging because of feedback.
- **Pages.** 1,445 raw alerts became 26 pages (24 after feedback). No page covered two labeled
  incidents; a labeled incident was spread over 1.6 pages on average.
- **Both incident types that never occurred in train** (a scheduler stall and broker GC pressure)
  were detected. Source: the scenario column in test-week.md (INC-020, INC-023), against the train
  list in [docs/dataset.md](docs/dataset.md).

False alerts per system:

| System | Alerts | True | Expected (non-actionable incident) | False alerts | False pages |
| --- | --- | --- | --- | --- | --- |
| 1. Naive baseline | 872 | 221 | 10 | 641 | - |
| 2. Seasonal baseline on everything | 613 | 177 | 10 | 426 | - |
| 3. Final alerts | 270 | 193 | 10 | 67 | - |
| 4. Final alerts + pages | 270 | 193 | 10 | 67 | 10 |
| 5. Final + pages + frozen feedback | 224 | 169 | 10 | 45 | 8 |

Source: [docs/results/test-week-posthoc.md](docs/results/test-week-posthoc.md), section d. **This
table is post-hoc**: it was computed after the result was seen, from the same alerts, because the
pre-registered output printed precision but not the counts.

**Time to detect.** The slowest detection was 405 minutes (INC-013, a memory leak), for systems 2 - 5.
The pre-registered table shows a median of 13.0 minutes for systems 3 - 4 and 10.0 for system 5, but
those two numbers were timed differently (after the scorer's dedup, and on raw alerts). Timed the
same way the systems are identical for every incident: 13.0 by the scorer's timing, 10.0 on raw
alerts. Feedback changed no time to detect. Source: test-week.md for the two medians,
test-week-posthoc.md section c for the like-for-like comparison.

**Correction (Nov 16): live timing.** The times and page counts above are batch numbers. Replayed one
minute at a time through the streaming engine, the same alerts fire but are known later: system 5's time
to detect is median 13.0 min and maximum 409 min (pre-registered: 10.0 and 405), and the engineer is paged
29 times, 11 of them false, which is 1.57 false pages per day (pre-registered: 1.14, from the 24 pages left
at the end of the week). No incident changes between detected and missed. Post-hoc: [Live timing](docs/results/test-week-posthoc.md#live-timing-post-hoc-streaming-engine).

**Predictions against the result.**

| Predicted before the run | Result |
| --- | --- |
| Recall 70 - 100% | 9 of 10 |
| INC-014 pages (60/40 that it does) | it paged |
| False pages per day: 1 - 2 | 1.14 |
| Median time to detect: 7 - 12 min | 10.0 on raw alerts, 13.0 by the scorer's timing |
| Maximum time to detect: 45 - 75 min | 405 min: far outside |
| Feedback removes 0 - 2 false pages | 2 |
| False volume ordering 5 <= 4 < 3 < 2 < 1 | 8 <= 10 < 67 < 426 < 641 |

Source: predictions in [docs/test-protocol.md](docs/test-protocol.md); results from the two files
above (the ordering from the post-hoc table).

## What did not work (and was left out)

- **Saturation forecast.** A line fitted to the last hour of memory, alerting when the limit would
  be reached within 4 hours. It was built and tested, and it was never faster than the seasonal
  baseline on a leak: 330 minutes against 75 on the train leak (INC-007), and, checked afterwards on
  the test leak (INC-013), 800 against 405. A slow leak is "unusual" long before it is "about to run
  out". It is in the code, switched off. Sources: docs/log.md, "Nov 10, saturation forecast";
  test-week-posthoc.md section b (exploratory).
- **Change point detector.** A CUSUM on the seasonal z-score, meant to catch small sustained shifts.
  On train it raised 517 alerts, 5 of them true. The method assumes a quiet series gives independent
  noise of about one wobble; the real scores are wider (standard deviation 1.1 - 1.6) and correlated
  from one reading to the next (0.2 - 0.33), so ordinary drift looks like a shift. It was not
  adopted; the code is on the branch `change-point`. Source: docs/log.md, "Nov 11, change point
  detector (not adopted)".

## A second world: the scripted live week

The test week is used up, so the same frozen system was run once on a second world: a generated week
with no incidents, streamed through Kafka, into which 14 failures were injected (seven scenarios,
twice each, at seeded random times and sizes). The scenarios, their ranges, the schedule and the
scoring rule were committed before the run ([docs/live-protocol.md](docs/live-protocol.md)); the
result is saved as the runner wrote it ([docs/results/live-week-1.md](docs/results/live-week-1.md)).

| | |
|---|---|
| injections detected | 12 of 14 |
| missed | both Flink state leaks (no alert at all in 6 hours each) |
| time to detect, median / maximum | 12 min / 132 min |
| pages sent | 29: 25 for an injection, 4 false |
| false pages per day | 0.57, the same 4 pages the week has with nothing injected |

How to read it:

- **The slow-leak limitation is confirmed.** The Flink leaks added 16-23% to TaskManager memory in
  their 6 hours and never alerted. The consumer leaks were paged after 116 and 132 minutes, when
  memory was 30-40% above its usual level.
- **The zero-baseline limitation did not show here.** Both Trino heavy queries were paged in 12-17
  minutes, first through the workers' GC time; the queued-queries series also alerted. These
  injections last 30-60 minutes at 0.8-1.2 of the batch scenario's size, so this does not say the
  test-week miss (INC-015) is fixed.
- **Four detections at 0 minutes say little.** The slow deploys and the crash loops restart a pod in
  their first minute, as their batch originals do, and a restart always pages. Their next signals
  came 10-14 minutes in.
- **One cause, nine pages.** The hourly DAG paused for nine hours paged once per missed run.
- 14 injections, two per scenario, one seed: counts, not rates. The injected failures are declared
  effects on the readings, not simulated physics; each scenario lists what it approximates
  (`sentinel_stream/scenarios_live.py`).

## Limitations

- **Quiet, low-count series (INC-015).** The missed incident showed up on a queue length that sits
  at zero and rose by about 22 queries. Count-like series must move by more than 6 units to be hot;
  that left 2 hot readings out of 9, and an alert needs 3 of 5. The same 6-unit rule removed
  hundreds of false alarms on train, so this is a trade-off, not a bug.
- **Slow leaks (INC-013).** Detected after 405 minutes. With the 5% floor, memory must be 30% above
  expected to be hot; it was 10% above after 55 minutes. The naive baseline, which has no floor,
  alerted after 190.
- **One test week, ten incidents.** One incident is ten points of recall, and the interval for
  9 of 10 runs from 0.60 to 0.98. Differences of one incident between systems mean nothing.
- **Simulated data only.** Nothing was run against a real cluster. A different random seed changes
  the noise but not the incident schedule, so there is no second independent test.
- **The scorer's entity rule.** An alert counts as true only if it is on a series or entity the
  incident's label lists. Alerts on closely related things (throttling on the consumer pods during a
  consumer-lag incident whose label names the consumer group) count as false. This lowers measured
  precision and is also what taught the feedback loop to demote throttling alerts.
- **Feedback is simulated.** Labels come from the scorer, not from people, and two weeks of them is
  little: two kinds of alert were demoted.
- **Not built.** The project's scope note also lists detection on logs, one ML method as a
  comparison, and a UI. None of the three exists in this repository.

Sources: test-week-posthoc.md sections a - c for the two incidents; docs/log.md "Nov 10, combined
result and job run follow-ups" (seeds), "Nov 10, router diagnosis" (entity rule), "Nov 11, feedback
loop"; [docs/scope-v3.md](docs/scope-v3.md) for the scope.

## Next steps

- **An agent with MCP tools.** The replay API mirrors the tools an investigating agent would call
  (metrics, logs, job history, recent changes). Root-cause analysis was cut from this version.
- **Fix the slow-leak miss, then test it on a third world.** The live week above is now seen, like
  the test week. A fix (for example a trend rule next to the seasonal one) has to be judged on a new
  schedule: another seed in `scripts/run_live_week.py`, with its own protocol.

## Setup and usage

Checked on Python 3.14 in a fresh virtual environment.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt        # pandas, numpy, scipy, pyyaml, matplotlib, pytest
export PYTHONPATH=.

python -m sentinel_sim.generate --out data --seed 7   # create the dataset (about 20 s; data/ is not in git)
pytest                                                # 303 tests, about 55 s; some need data/ or data_live/ and are skipped without it
python examples/quickstart.py                         # tour of the replay API + the naive baseline on the test week
```

Tests. The default `pytest` run skips two groups. `slow`: 12 tests that replay the whole test week
through the streaming engine (`sentinel_stream/`) and compare it with the batch pipeline, and one
that regenerates the dataset and checks it is byte-identical to the evaluated one. `kafka`: 8 tests
that run the engine through a real broker (see "Run the live stream"); they are skipped when no
broker is running.

```bash
pytest                          # default: 303 tests, about 55 s
pytest -m slow                  # only the slow ones: 13 tests, about 95 s (needs data/)
pytest -m kafka                 # through the broker: 8 tests, about 4 min (docker compose up -d first)
pytest -m "slow or not slow"    # everything: 324 tests, about 7 min with the broker running
```

Reproducing the numbers:

```bash
python scripts/run_test_week.py        # dry run on TRAIN: the test run's code path, checked against docs/log.md
python scripts/posthoc_test_week.py    # the post-hoc analysis of the test week (read-only)
python scripts/score_router.py         # train: naive vs. first build vs. final alerts
python scripts/score_incidents.py      # train: correlator pages
python scripts/score_feedback.py       # train: feedback loop, online replay
```

The test week itself was run once with `python scripts/run_test_week.py --split test --run-once`.
The runner now refuses to do that again: a further run needs `--post-hoc "<reason>"` and is saved
to a separate file, never over the first result.

## Run the live stream

The same engine, fed from a Kafka topic instead of a function call. A publisher plays a generated
world into the topic `telemetry` at a chosen speed; a consumer runs the engine on it and writes every
alert to `alerts` and every page event to `pages`. Needs Docker.

```bash
pip install -r requirements-kafka.txt        # adds confluent-kafka, the Kafka client
docker compose up -d                         # one Redpanda broker on 127.0.0.1:19092 with its topics

# a quiet world: 35 days, no incidents. The first 28 days are history, the last 7 are streamed.
python -m sentinel_stream.quiet_world --days 35 --seed 11 --out data_live

# terminal 1: the consumer. Warms the engine up on the history before --start, then waits for telemetry.
python -m sentinel_stream.consumer --data data_live --start 2026-10-26

# terminal 2: the publisher. --speed 60 sends one simulated minute per second.
python -m sentinel_stream.publisher --data data_live --start 2026-10-26 --speed 60

# terminal 3: watch the pages arrive (use `alerts` instead of `pages` to see every alert)
docker exec -it sentinel-redpanda rpk topic consume pages --offset start -f '%v\n'
```

What to expect. This world has no incidents, so every page is a false one: over the 7 days the
system raises 14 alerts and pages 4 times. The first page is on Oct 28 at 21:29, which is 69 minutes
in at `--speed 60`; with `--speed 600` it takes 7 minutes and the whole week 17. The consumer also
prints a `PAGE ...` line for each page.

Stopping and restarting. Stop the consumer (Ctrl-C, or kill it) and start it again with the same
command: it continues where it stopped and writes no alert or page event twice. It commits a
message's offset only after that message's outputs are on the topics, and after a restart it compares
what the engine says with what is already written before sending anything (how and why:
`sentinel_stream/consumer.py`). To start over from the beginning, delete the topics:
`docker compose down -v && docker compose up -d`.

`pytest -m kafka` checks this through the broker: the test week gives exactly the in-process engine's
alerts and page events, also when the consumer is killed three times on the way.

## Run the demo

A Streamlit replay of live week 2 ([result](docs/results/live-week-2.md)). It needs no broker, no
generated world and no API key: it reads the small export committed in `dashboard/demo/live-week-2/`
(1.8 MB). Use a separate virtual environment. Streamlit requires pandas < 3, and the project's `.venv`
runs on pandas 3.1.

```bash
python3 -m venv .venv-dashboard
.venv-dashboard/bin/pip install -r requirements.txt -r requirements-dashboard.txt
.venv-dashboard/bin/streamlit run dashboard/app.py          # opens http://localhost:8501
```

Move the clock with the slider, press ▶ Play (5 min, 15 min or 1 h of simulated time per tick), or jump
with ⏭ Next page-out. The panels:

- **Pages as they fire**: every page-out up to the clock, with its first alert, how many alerts its
  page holds now, and the triage agent's current answer.
- **Triage note** for the selected page (triage run 1 of 3): needs_human, cause, impact, evidence and
  next step. The follow-up appears 10 minutes later, when there was one. Each note appears only once
  the agent had written it.
- **Series**: one series of the selected page, as 5-minute means. For a seasonal series the chart also
  shows its expected value, a ± wobble band, the hot line (expected + 6 × wobble) and the page's alerts.
- **Active injections**, labelled *attacker view: hidden from the system*. This panel reads the answer
  key, which nothing in the system sees. Its expander shows the whole week, including the future of the
  clock. Every other panel shows only what existed at the clock (`dashboard/data.py`).

The expected and wobble values are recomputed with the frozen detector settings, because the engine does
not publish them. The export compares them with the numbers printed in all 705 of the engine's seasonal
alert texts, and they agree to the print rounding. To rebuild the export (needs the broker's
`live-week-2-*` topics and `data_live_2/`):

```bash
python scripts/posthoc/lw2_export_topics.py && python dashboard/export_demo.py
```

`tests/test_dashboard.py` checks that nothing from after the clock leaks into the system's panels,
that the page-outs equal the live scorer's, and that every panel renders. The render part needs
Streamlit, so it is skipped in `.venv`. Run it with
`PYTHONPATH=. .venv-dashboard/bin/python -m pytest tests/test_dashboard.py` (after
`.venv-dashboard/bin/pip install pytest`).

### Inject a failure

While the publisher runs it takes commands from the topic `control` and changes the readings it
sends from that moment on. What it injected goes to the topic `answer_key`, which the consumer never
reads (`tests/test_live_isolation.py`).

```bash
python -m sentinel_stream.injector list                                    # the seven scenarios and their ranges
python -m sentinel_stream.injector inject trino_heavy_query -p magnitude=1.0 -p duration_min=40
python -m sentinel_stream.injector stop <id>                               # end it before its duration is over
python -m sentinel_stream.live_score --data data_live                      # detected? time to detect? false pages?

# not a named scenario: your own effects (series, add|mult, flat|ramp|decay, magnitude, minutes)
python -m sentinel_stream.injector custom \
    -e "jvm_heap_used_ratio|kafka-0" add ramp 0.2 90 \
    -e "kafka_produce_request_p99_ms|kafka-ingest" mult flat 4 30 \
    --severity subtle --intent "heap creeping up on one broker"
```

Your own effects are exact (flat holds the magnitude, ramp reaches it at the end, decay starts at
it) and are checked against what the series can physically be: a ratio stays in 0-1, memory and CPU
stay under the pod's limit, counts are whole and not negative, a counter only goes up. An effect that
would break its limit on the reading it meets is rejected, with the reason, in the answer key; the
others in the same command are applied. `--severity` (noticeable or subtle) and `--intent` are your
own words about the injection: the scorer reports by them and never uses them to decide a match.

**Stream version 2** (`python -m sentinel_stream.publisher ... --stream-version 2`) also sends the
data directory's logs, Kubernetes events and changes on `telemetry`, as one batch per type per minute.
A named scenario then also writes what its failure would leave there: a crash loop's errors and
BackOff events (and silence from the pod), a deploy or rollback record, OOMKilled events, a paused
DAG's pause record and no trace of the run that did not happen (`sentinel_stream/evidence_live.py`).
Generic injections stay metrics-only, and the answer key says which kind each injection is. The engine
does not read the new types, so alerts and pages are the same as on version 1; the triage agent's
log, event and change tools read them from the stream (`dispatcher --stream-version 2`).

The scorer reads only `answer_key` and `pages`. An injection counts as detected when a page that
pages between its start and 30 minutes after its stop holds an alert on one of its target entities
or 1 hop from one. `python scripts/run_live_week.py --rehearsal` runs the whole chain once with a
dummy injection.

## Where to read more

| File | What it is |
| --- | --- |
| [docs/design-doc.md](docs/design-doc.md) | The main design decisions, each with the alternative that was rejected |
| [docs/log.md](docs/log.md) | The working log: every step, its numbers and the reasoning, in order |
| [docs/test-protocol.md](docs/test-protocol.md) | The pre-registered protocol, the disclosure and the predictions |
| [docs/config-frozen.md](docs/config-frozen.md) | Every parameter of the evaluated systems, generated from the code |
| [docs/results/test-week.md](docs/results/test-week.md) | The test-week result, verbatim from the single run |
| [docs/results/test-week-posthoc.md](docs/results/test-week-posthoc.md) | Post-hoc analysis: the miss, the slow detection, timing, false-alert counts |
| [docs/live-protocol.md](docs/live-protocol.md) | The protocol of the scripted live week: scenarios, ranges, schedule, scoring rule |
| [docs/results/live-week-1.md](docs/results/live-week-1.md) | The live-week result, as written by the single run |
| [docs/live-week-2-protocol.md](docs/live-week-2-protocol.md) | Live week 2: frozen detector, triage agent and attacker on a new world, with predictions |
| [docs/results/live-week-2.md](docs/results/live-week-2.md) | The live-week-2 result, as written by the single run |
| [docs/results/live-week-2-posthoc.md](docs/results/live-week-2-posthoc.md) | Post-hoc: predictions vs result, the two misses, target naming, generic incidents |
| [docs/dataset.md](docs/dataset.md) | The simulated platform, the files, the replay API and the scorer |
| [docs/scope-v3.md](docs/scope-v3.md) | The project scope |
