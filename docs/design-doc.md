# Sentinel Lens: design decisions

What was decided, what the alternative was, and why the alternative lost. The evidence is in
[log.md](log.md); each row links to the entry that holds the numbers. For what the system is and how
it scored, start with the [README](../README.md).

Two rules shaped everything: every alert must be explainable in one sentence, and every threshold
must be something the author can defend. All tuning was done on the train period only.

## Detection

| Decision | Alternative considered | Why the alternative was rejected | Evidence |
| --- | --- | --- | --- |
| **Median and MAD** for "expected" and "wobble" | Mean and standard deviation | The history a reading is compared with contains earlier incidents and stray spikes. One bad day moves a mean and inflates a standard deviation, which hides the next problem; a median of several days barely moves. | By reasoning. No mean/std version was run. |
| **History = the last 5 days of the same type** (weekday or weekend) | The previous 7 days; or the same weekday in earlier weeks | With the previous 7 days, every weekend is compared with a mostly-weekday history and looks like a drop. The same weekday needs months of data: with one week of history it was far worse. | [Nov 9, seasonal noise fixes](log.md#nov-9-seasonal-noise-fixes) |
| **A floor on the wobble**: 5% of expected, plus a minimum change for count-like series and for CPU throttling | No floor; or a floor scaled from each series' own level | Without a floor, a series that sits at 0 gives an unbounded score for a reading of 0.2. A floor scaled from the series' level did nothing at small sizes and, at large ones, made every series less sensitive and delayed a leak by hours. | [Nov 9, seasonal noise fixes](log.md#nov-9-seasonal-noise-fixes) |
| **State the floor as a minimum change** ("ignore changes under 6 queries") | State it as the floor itself ("wobble at least 1") | A floor of 1 with a threshold of 6 silently means 6 units. Stated as the floor, nobody notices what it hides: three series could not alert at any value seen on train. | [Nov 10, router diagnosis](log.md#nov-10-router-diagnosis), [router settings](log.md#nov-10-router-settings) |
| **Persistence: 3 of the last 5 readings** | Alert on the first hot reading | The data has single-reading spikes by design, and one hot reading is usually noise. The price is about 10 minutes of delay, and incidents with few readings can slip through (INC-015 on test had 2 of 9). | The rule came from the project brief and was not swept. Its cost: [test-week-posthoc.md](results/test-week-posthoc.md) |
| **Route by metric name and catalog**, one rule per series | One detector for every series | The seasonal baseline alone missed a paused job entirely (no metric exists to score) and caught restarts late. Counters, queues and CPU are different kinds of signal. | [Nov 9](log.md#nov-9) |
| **Hard rules for things that are always bad** (restarts, failed checkpoints, missed schedules), on 1-minute readings with no baseline | Send them through the baseline too | A restart is an event, not a noisy measurement: there is nothing to average and nothing to wait for. | [Nov 9](log.md#nov-9) |
| **A baseline on job run duration** | Rely on the schedule watcher and pod metrics | A run that succeeds inside its SLA but takes twice as long is invisible to both. It was the only thing that caught two train incidents. | [Nov 9, job run baseline](log.md#nov-9-job-run-baseline) |
| **Leave out the saturation forecast and the change point detector** | Add them because the catalog asks for them | Both were built and measured. The forecast was never faster than the baseline on a leak; the change point detector raised 517 train alerts with 5 true. | [Nov 10, saturation forecast](log.md#nov-10-saturation-forecast), [Nov 11, change point](log.md#nov-11-change-point-detector-not-adopted) |

## From alerts to pages

| Decision | Alternative considered | Why the alternative was rejected | Evidence |
| --- | --- | --- | --- |
| **The score is severity**, how far past its line an alert is; always-bad rules score 1.0 | A score meant as the probability that the alert is real | Four formulas were compared on train and none ranked true alerts clearly above false ones. Calling the number a probability would be a claim the data does not support. | [Nov 10, alert record and score formulas](log.md#nov-10-alert-record-and-score-formulas), [score decision](log.md#nov-10-score-decision) |
| **Group alerts by time AND topology** (15 minutes, 1 step in the entity graph) | Time alone; topology alone | Time alone merged unrelated problems that happened together; topology alone glued different days into one page. Only both together never merged two labeled incidents, at the cost of splitting some. | [Nov 10, correlator](log.md#nov-10-correlator) |
| **Add missing relationships only when the platform's own description states them** | Join everything that shares a source; or re-attach alerts that recur within hours | Joining by source tied unrelated jobs together and doubled the largest page. The recurrence window that helped was fitted to one incident. Six stated relationships were added instead. | [Nov 10, why incidents fragment](log.md#nov-10-why-incidents-fragment-report-only), [topology edges adopted](log.md#nov-10-topology-edges-adopted) |
| **Feedback demotes, it does not delete**; labels are counted per kind of alert, with a prior of one real and one false | Delete or silence alerts of a noisy kind; or tune a threshold per series | A deleted alert cannot be reviewed and the decision cannot be undone by later labels. Demoted alerts stay on the page as context, and a kind of alert comes back if real ones are labeled. Counting per kind lets one pod's labels help every other pod with the same signal. | [Nov 11, feedback loop](log.md#nov-11-feedback-loop). Neither alternative was measured. |

## Evaluation

| Decision | Alternative considered | Why the alternative was rejected | Evidence |
| --- | --- | --- | --- |
| **Pre-register the test**: freeze the code at a tag, write the protocol and predictions, run once, save the output unedited | Evaluate on the test week whenever something changes | Every look at the test week turns it into training data. With about ten incidents there is no room to spend any of them on tuning. | [test-protocol.md](test-protocol.md), [Nov 11, test protocol frozen](log.md#nov-11-test-protocol-frozen-the-test-week-has-not-been-run) |
| **Report recall as a count with an interval** ("9 of 10, 0.60 - 0.98") | A percentage | "90%" hides that one incident is ten points. | [results/test-week.md](results/test-week.md) |
| **Post-hoc analysis in a separate, marked file** | Fold explanations and corrections into the result | The first result must stay readable exactly as it came out, including its flaws (one timing inconsistency is explained post-hoc, not patched). | [results/test-week-posthoc.md](results/test-week-posthoc.md) |

## Why no deep models

The detectors are simple statistics on purpose, not a learned model (an isolation forest, an
autoencoder on the metrics). Three reasons. First, the explanation: "memory is 1.3x expected for
this time of day" can be checked by hand; an anomaly score from a model cannot. Second, the data:
12 train incidents are far too few to train on or to tell a good model from a lucky one. Third, the
failure that matters here was noise, and the noise came from specific, understandable causes
(weekends, zero baselines, a ratio near zero) that simple rules fixed one at a time.

This is a judgment, not a measurement. The scope included one ML method as a comparison, and it was
not built, so this project has no number showing how a model would have done.

## What the decisions cost

Stated plainly, because each has a visible price on the test week
([results/test-week-posthoc.md](results/test-week-posthoc.md)):

- The minimum change for count-like series is why one incident (INC-015) was missed.
- The 5% floor is why a slow leak (INC-013) took 405 minutes.
- Feedback demoted CPU throttling alerts because the scorer counts them as false on pods the
  incident label does not list; on another dataset that could hide a real problem.

None of these was changed after the test. Fixes belong on new data, not on the week that exposed them.
