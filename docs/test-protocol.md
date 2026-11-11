# Test-week evaluation protocol

Written and committed BEFORE any run on the test week (Oct 19-25, 2026). Its purpose is to fix, in
advance, what is run, what is measured and how the result may be described, so that the result cannot
be shaped afterwards. Frozen at git tag `pre-test-freeze`.

Status when this was written: the test week has not been run. No detector, threshold or rule was
tuned on test values. One test label was seen, and one design decision was informed by knowing it
exists: see "Known exposure to test labels".

Amended after the tag, before any test run, in a docs-only commit: the wording of "Known exposure to
test labels" and this status paragraph. Nothing under the frozen paths changed.

## What is evaluated

Five systems, all on the same data and scored by the same unmodified scorer (`sentinel_sim/evaluate.py`).

| | system | what it is |
|---|---|---|
| 1 | naive full baseline | the quickstart's detector: robust z-score against the previous 7 days with no floor, 3-of-5, plus any counter increase and failed or SLA-missed runs |
| 2 | seasonal everywhere (first build) | the seasonal baseline with its first settings (previous 7 days, 5% floor) and 3-of-5 on every gauge; no other rule |
| 3 | final alerts only | the routed system with the router defaults, plus the job run baseline; alerts as the scorer sees them |
| 4 | final alerts + correlator pages | system 3, with its raw alerts grouped into pages (gap 15 min, 1 hop, tracked topology edges) |
| 5 | final + correlator + frozen feedback | system 4, where pages made only of demoted alerts do not page |

**The primary system is (5).** Systems (1)-(4) are there to show what each layer adds. Every parameter
of every system is in `docs/config-frozen.md`, generated from the code.

### Frozen feedback

The feedback state is learned from ALL train labels (the scorer's verdicts on train alerts) with the
clock at the end of train. The groups demoted at that point are listed in `docs/config-frozen.md`.
They are applied unchanged to the whole test week, from its first alert. Nothing is learned from the
test week in the primary system.

An online-continuing variant, in which labels keep arriving 60 minutes after each test alert, is
printed by the runner as "(5x)". It is **exploratory** and will be labeled so wherever it is quoted.
It is never the headline.

## Metrics

Reported for the test week (7 days). Definitions are the ones already used on train.

- **Incident recall**: the number of actionable test incidents detected, as a count "k of n", with a
  Wilson 95% interval. For (1)-(4): any alert matching the incident. For (5): a paging page that
  contains an alert matching the incident.
- **Per-alert precision**: true / (true + false) over alerts after the scorer's 30-minute dedup,
  'expected' excluded. For (5): over the alerts that are not demoted.
- **False pages per day**: pages containing no true and no expected alert, divided by 7. Systems (4), (5).
- **Pages per labeled incident**: mean number of paging pages over the labeled incidents that have at
  least one. Systems (4), (5).
- **Merged-incident count**: pages that cover more than one labeled incident. Systems (4), (5).
- **Median and maximum time to detect** over detected actionable incidents. For (5) an incident is
  timed at the moment its page pages (the page's first alert that is not demoted), not before the
  page's first alert matching the incident.
- **Alerts per day** after the scorer's dedup.

Headline comparisons, fixed now: (a) recall of (5) as a count with its interval; (b) per-alert
precision of (3) against (5), the "before vs. after feedback" number; (c) false pages per day of (4)
against (5). The per-incident table is part of the result and is reported in full, including misses.

## Rules

1. **Run once.** The command is `python scripts/run_test_week.py --split test --run-once`. The runner
   refuses unless the tag `pre-test-freeze` exists, the frozen code and config are identical to it and
   committed, the predictions below are filled in, and no result file exists yet.
2. **Parameters recorded before the run.** `docs/config-frozen.md` holds every parameter and default
   and the frozen feedback state. The runner regenerates it at run time and reports whether it matches.
3. **Results saved verbatim.** The runner writes its output unedited to `docs/results/test-week.md`,
   with the commit hash, the tag and the time of the run. That file is committed as is.
4. **Anything later is post-hoc.** Any code change after the tag, and any further run on the test
   week, is post-hoc. It needs `--post-hoc "<reason>"`, goes to a separate file
   (`docs/results/test-week-posthoc-N.md`) with a banner, and never replaces or is quoted instead of the
   first result.
5. **The dry run.** `python scripts/run_test_week.py` (no arguments) runs the same code path on the
   train split with the clock at the end of train and checks that it reproduces the train numbers in
   `docs/log.md`. It may be run at any time; it reads no test-week data.

### What we will NOT do after seeing the numbers

- Change any threshold, floor, minimum change, history mode, persistence, grace, gap, hop limit,
  topology edge, n_min, tau or label delay and present the new number as the result.
- Add, remove or re-route a detector, or switch the saturation forecast on, and present that as the result.
- Change which system is primary, or promote the exploratory online variant to the headline.
- Drop, redefine or add a metric to make the result read better; report only the favourable systems;
  or leave out the per-incident table.
- Exclude an incident from the count because it looks unfair, or re-label anything. The scorer and the
  labels stay as they are.
- Re-run with another seed or another window and choose between runs.
- Explain a miss away. A missed incident is reported as missed, with what the system saw.

What we may do, labeled post-hoc: investigate why something was missed or noisy, and try changes for a
future version. Those go in the post-hoc files and in `docs/log.md`, next to the first result, never
in its place.

## Things known before the run

- **Train numbers are not predictions.** On train the final system has recall 10 of 10 and per-alert
  precision 0.604, with 46 pages. Its parameters were chosen on train, so test is expected to be worse.
  The train number of system (5) printed by the dry run (35 pages) is in-sample twice over: the
  feedback state was learned on the same two weeks it is applied to.
- **Recall on about ten incidents is coarse.** One incident is roughly ten points of recall, and a
  Wilson interval for 9 of 10 runs from 0.60 to 0.98. Differences of one incident between systems
  are not evidence of anything.
- **Risk in the frozen feedback.** It demotes (seasonal, cpu_throttling) and (seasonal fallback,
  jvm_gc). The throttling group was demoted because every one of its 31 train labels is false, and
  many of those were throttling on consumer pods during incidents whose labels list the consumer
  group, not the pods. A test incident that shows up only as throttling would produce pages that do
  not page in system (5). If that happens it is reported as a miss of the primary system.
- **Other seeds are not fresh data.** `--seed` changes noise, not the incident schedule
  (docs/log.md, Nov 10), so the test week is the only held-out evidence there is.
- **Known weak spots carried in unchanged:** incidents fragment into several pages through the 2-hop
  limit and quiet gaps; the job run baseline raises false alarms on hourly_quality_check; a job run
  problem is only seen when the run ends.

### Known exposure to test labels

On Nov 10, during router investigation, a grep over data/labels printed one test-split row (INC-014: container_cpu_throttled_ratio on orders-etl-consumer-0, direction up). No test values were seen. This happened BEFORE the 0.18 throttling minimum change and the injection test were chosen. 0.18 was justified from warm-up/train distributions and domain reasoning, but the decision to keep throttling detectable instead of suppressing it was informed by knowing a test throttling incident exists. INC-014 is therefore reported separately, and system 5's recall is reported with and without it.

How "with and without" is computed: by hand from the per-incident table in the saved result, since the
frozen runner prints recall over all actionable incidents only. This reading is fixed now; it is not a re-run.

## Predictions (written by the author BEFORE the run)

Recorded on 2026-11-11, after the tag `pre-test-freeze` (770fab8) and before any run on the test week,
in a docs-only commit. They are the author's, copied as written.

```
Recall (system 5, actionable incidents): expected 70-100%; I'd be surprised below 50%.
  Reason: tuned on train, so some drop on unseen incidents.
INC-014 pages under system 5: 60/40 that it does.
  Reason: throttling group is demoted by feedback, but other detectors may page the same incident.
False pages per day (system 5): 1-2.
  Reason: train was 1.8; test weekends have richer history.
Median time to detect: 7-12 min.
Maximum time to detect: 45-75 min.
  Reason: slow cases are leaks and slow jobs.
Ordering on false volume: 5 <= 4 < 3 < 2 < 1 (systems 1-3 count alerts, 4-5 count pages).
Feedback effect: system 5 has 0-2 fewer false pages than system 4.
```

No prediction was made for: per-alert precision of systems (3) and (5); which type of incident is most
likely to be missed, other than the INC-014 line above.

The runner checks that this section has been filled in before it will run the test week.
