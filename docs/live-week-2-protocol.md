# Live week 2 protocol: frozen detector, frozen triage agent, frozen attacker, a new world

Written and committed BEFORE anything of live week 2 was run. Like [test-protocol.md](test-protocol.md)
and [live-protocol.md](live-protocol.md), it fixes in advance what is run, how it is scored and what is
reported, so the result cannot be shaped afterwards. The runner is `scripts/run_live_week_2.py`; it has
not been executed.

The test week (Oct 19-25 of `data/`) and live-week-1 (`data_live`, seed 11) have been seen and used up,
and live-week-1 was also the triage agent's development set. This week is held out for all three
components: a new quiet world, injections that nobody has chosen by hand, and a triage agent that has
never seen this world.

## Status when this was written

- Not generated: the world `data_live_2`. Its noise, its quiet-world false pages and its history are
  unknown to the author.
- Not run: the attacker campaign, the replay, the triage runs. Nothing about this week has been
  seen.
- What has been seen and could shape expectations (stated so the reader can weigh the predictions):
  - live-week-1: 12 of 14 named scenarios detected, 0.57 false pages a day.
  - The triage agent's development runs on live-week-1 (arm E, frozen: 0 dangerous final answers,
    23 of 25 true pages correct).
  - The attacker's three dry runs, on days of `data_live`, not scored. Dry run 3, under the frozen v2
    rules: 4 injections in 2 days, 2 of them generic.

## Frozen components

| component | tag | what must be identical to the tag | how it runs here |
|---|---|---|---|
| detector | `pre-test-freeze` | `sentinel_lens/`, `sentinel_sim/`, `docs/config-frozen.md` | through the streaming engine and consumer (`sentinel_stream/engine.py`, `consumer.py`) at the commit the run is made from, warm-started on the 28 days before the stream, paging once per step |
| triage agent | `triage-freeze-v1` | `sentinel_stream/investigator.py`, `context_tools.py`, `dispatcher.py`, `triage.yaml`, `docs/triage-config-frozen.md` | the dispatcher with the frozen settings: `claude-sonnet-5-5`, effort medium, the guard, follow-up of an "unsure" note at paging + 10 min, 3 investigations at a time |
| attacker | `attacker-freeze-v2` | `sentinel_stream/attacker.py`, `attacker.yaml`, `injector.py`, `scenarios_live.py`, `docs/attacker-config-frozen.md` | campaign mode over the 7 days, woken every 60 simulated minutes |

The runner refuses to start if any file in the third column differs from its tag (`git diff <tag>`). It
also refuses if `docs/triage-config-frozen.md` or `docs/attacker-config-frozen.md` is not what its freeze
script generates from the code now. The streaming engine and consumer are not under a freeze tag. They
are covered by the clean-tree check below, and the result records the commit they ran from.

## The world

- `python -m sentinel_stream.quiet_world --days 35 --seed 13 --out data_live_2`, run by the runner
  itself as its first step (the directory must not exist before the run). Seed 13 has not been used for
  any dataset: `data/` is seed 7, `data_live` seed 11.
- 35 days from 2026-09-28: the first 28 days are the engine's warm start and the attacker's history; the
  stream is **2026-10-26 00:00 to 2026-11-02 00:00 UTC** (7 days). The calendar is the same as
  live-week-1's, the noise is new, so false pages are measured on fresh noise too.
- Stream version 2: named scenarios also write logs, Kubernetes events and change records; generic
  injections write metrics only.
- The SHA-256 of every file of the generated world is recorded in the result.

## The attacker campaign

- One campaign over the 7 days, recorded once. Everything after it is scored by replaying that recording;
  the attacker's LLM is not called again.
- The frozen rules, all in code: at most 3 injections starting per simulated day and 2 active at once;
  starts at least 3 h apart; only series described in the same wake; `novelty` on every generic
  incident; named scenarios at most 60% of the campaign's injections at every point. "At least 1 a day"
  is only asked for in the prompt.
- **The 60% cap forces the campaign's first injection to be generic** (1 named of 1 would be 100%).
  Over the week the attacker can make at most 21 injections, of which at most 12 named (13 of 21 would
  be 62%).
- The attacker is blind: it sees the platform's configuration, the quiet world's history and its own
  injections, never alerts, pages, notes or the answer key (`tests/test_live_isolation.py`).
- Saved: the full record (every wake's messages, tool calls, notes, cost) in `runs/live-week-2/campaign/`
  (gitignored), the control stream in `docs/results/live-week-2.control.jsonl` (committed with the
  result).
- Reported: injections per simulated day; kinds (named / generic) and severities (noticeable / subtle);
  each injection with its intent and, for generic ones, its novelty text and overlap with the named
  scenarios; the attacker's own rejections, by rule; cost.

## Replay and detection

The recorded control stream is put on a fresh `control` topic before the publisher starts. The
publisher plays the 7 days at max speed on stream v2, the consumer runs the frozen engine on it, and the
answer key records what was injected. Topics: `live-week-2-*`.

**Which injections count.** Every injection with a `started` record in the answer key counts, whatever
the attacker's or the publisher's view of it. That includes one cut short by the end of the stream, and
a generic one of which only some effects were applied. A command the publisher refuses entirely (a
`rejected` record) is reported and not counted: nothing was injected.

**Kind**: generic when the answer key says `"scenario": "custom"`, named otherwise. **Severity**: the
attacker's own label (noticeable | subtle). Neither is used for matching.

**The live rule, unchanged** (`sentinel_stream/live_score.py`, as in [live-protocol.md](live-protocol.md)):

- A page-out matches an injection when it pages inside [start, stop + 30 min] and its page holds, at
  that moment, an alert on a target entity or 1 hop from one. The target entities are the owners of the
  series the injection changes, plus the DAG for a pause.
- An injection is detected if a page-out matches it. Time to detect = its first matching page-out minus
  its start.
- A page-out matching no injection is false.

Reported:

- Injections detected, "k of n" with a Wilson 95% interval, overall and split by kind, by severity, and
  by kind x severity. Median and maximum time to detect for each.
- False page-outs: the count, false pages per day (count / 7), with an exact Poisson (Garwood) 95%
  interval. Wilson is for proportions and a page count is not one.
- For reference, the page-outs of the same 7 days with nothing injected (the engine in-process). The
  report gives how many of the false page-outs fall at the time of one of them.
- The scorer's own tables in full: overall, by scenario, by severity, per injection (with the alert each
  detection rests on), every false page-out with its alerts.

The Wilson intervals treat injections as independent trials. Injections may overlap (up to 2 active),
and the attacker chooses them itself, so the intervals are a guide to how little n can tell, not a
sampling statement.

## Triage

The frozen agent is run **3 times** on every page-out of the replayed week, each run a fresh dispatcher
writing to its own topic (`live-week-2-triage-r1`, `-r2`, `-r3`), with follow-ups as frozen. The runs
are independent; none sees another's notes. Variation between them is the agent's own: same pages, same
stream, same settings.

**Truth** for a page-out is the live rule: a page-out matching an injection is true, any other is false.
The answer key is read only after all three runs are complete.

Scored with `sentinel_stream/triage_score.score_needs_human`, on the **final** answer (the follow-up's if
there was one):

| | needs_human yes | no | unsure |
|---|---|---|---|
| true page | correct | **dangerous** | deferred |
| false page | wasted | correct | deferred |

Reported, per run and as a spread (the three values, their minimum-maximum, and the sum where a sum
means something):

- True pages: correct / dangerous / deferred; the share correct, with a Wilson 95% interval.
- False pages: correct / wasted / deferred.
- Cause names the target, over true pages. For a named scenario, `triage_score.text_names_target` with
  its target (the name, or a pod `<target>-<n>`, as a whole word). A generic injection has no single
  target, so for it the cause names the target if it names any entity the answer key lists for the
  injection, under the same whole-word rule. This is more lenient than the named rule; that is fair
  because a generic incident really has several targets. The report says so next to the numbers,
  which are split by kind.
- Minutes from paging to the final answer: median and maximum.
- Follow-ups, and notes the guard converted.
- Cost (USD, estimate from the configured prices, follow-ups included).
- Agreement: how many page-outs got the same final answer in all three runs.
- **Every dangerous final answer, individually**: run, time, page, injection, kind, scenario (or
  generic), severity, first answer, follow-up, cause category and cause text.

Page-outs are not independent (one injection can page many times), so the interval on the share
correct is optimistic. An injection that produced many page-outs weighs more; the per-page table shows
which ones.

**Total cost** = the campaign + the three triage runs.

## Predictions

To be filled in by the author, and committed, BEFORE the run. The runner refuses while any field below
still reads NOT FILLED IN. A prediction is a single number (or a share) and a sentence of reasoning.

| | prediction | reasoning |
|---|---|---|
| injections detected, overall (share) | NOT FILLED IN | NOT FILLED IN |
| injections detected, generic (share) | NOT FILLED IN | NOT FILLED IN |
| injections detected, subtle (share) | NOT FILLED IN | NOT FILLED IN |
| false pages per day | NOT FILLED IN | NOT FILLED IN |
| dangerous final answers, summed over the 3 runs | NOT FILLED IN | NOT FILLED IN |
| share of true pages with a correct final answer (mean of the 3 runs) | NOT FILLED IN | NOT FILLED IN |
| total cost, USD (campaign + 3 triage runs) | NOT FILLED IN | NOT FILLED IN |

## What will be reported

`docs/results/live-week-2.md`, written by the runner, committed **unedited**, with:

- a header: the commit, the commit of this protocol, the three tags and their commits, the world command
  and its file hashes, and every attempt (with its reason, see Rules 3);
- the campaign section, the detection section and the triage section above, in that order;
- the answer key, verbatim.

Next to it: `docs/results/live-week-2.control.jsonl` (the recorded campaign). Transcripts stay in
`runs/` (gitignored).

**Post-hoc analysis** goes in a separate file, `docs/results/live-week-2-posthoc.md`, labelled as written
after the result was seen. The result file is not edited to add it. Planned for it already: the
injections that started too close to the end of the stream to have had a realistic chance of being
caught (for example in its last hour), named one by one. They stay in the result's counts.

## Rules

1. **Run once.** `python scripts/run_live_week_2.py --run-once`. The runner refuses unless all of these
   hold:
   - this file is committed, with the predictions filled in;
   - the code (`sentinel_lens/`, `sentinel_sim/`, `sentinel_stream/`, the runner and the scripts it
     imports) is committed and unchanged;
   - all three tags match;
   - `data_live_2/` does not exist yet;
   - no result exists;
   - no earlier attempt exists (`runs/live-week-2/`);
   - a Kafka broker and an API key are available.

   `--check` runs only these checks.
2. **Saved unedited.** The runner writes `docs/results/live-week-2.md`; it is committed as written.
3. **A technical failure** (a crash, a broker or API outage, a page-out left without a note), not a
   result, may be resumed with `--run-once --resume "<reason>"`. The reason goes in docs/log.md and in
   the result's header. Resuming reuses what finished:
   - The world is checked against its recorded hashes.
   - A finished campaign recording is never re-recorded. A campaign that did not finish is discarded and
     recorded again, and that is reported.
   - An unfinished replay is deleted and replayed. It is deterministic from the recording.
   - A triage run continues from its topic (the dispatcher's restart rule: no note is written twice, a
     page whose investigation was cut off is investigated again).

   Nothing about scoring, the rules or the components may change in between.
4. **Anything later is post-hoc** and is labelled so: other seeds, more runs, other rules.

### What will NOT be done after seeing the numbers

- Re-run the campaign, or run a second one, and choose between them.
- Run the triage agent more than 3 times and report a subset, or drop a run.
- Change the scoring rule, the truth rule, the kind or severity split, or the target-naming rule.
- Drop an injection the attacker made because it looks unfair, unrealistic or unlike its intent; count
  a detection the rule does not give; explain a miss away.
- Change a detector, the triage agent or the attacker and present the new number as this week's
  result. Fixes are future work, for a further world under a new protocol.

## Limits, stated beforehand

- The attacker chooses its own injections, so the mix (how many, how hard, which kinds) is part of
  the outcome, not a design. If it makes few generic or few subtle injections, those splits are
  counts of a handful and are reported as such.
- Generic injections write metrics only. The triage agent sees no log, event or change for them, unlike
  for named scenarios.
- The attacker's validation is against the quiet world's readings. An effect overlapping another
  injection can still be refused or held at a limit by the publisher; the answer key records it.
- One world, one campaign, three triage runs. Given the recording, the replay and the detector are
  deterministic; the triage agent is not, which is why it runs three times.
- The scenario ports and the attacker were written by the author of the detectors; the guard is that
  neither the scenarios nor the attacker's rules were tuned on detection output.
