Status: done, merged to main (branch `fix-seasonal-noise`)

# Task: fix the two noise sources in the seasonal baseline

Read CLAUDE.md, docs/log.md and the output of scripts/diagnose_noise.py first. Same rules as before: tune on the
TRAIN split only, never read `labels/` from detector code, keep detector code independent of the sample data,
small tested commits, one sentence of rationale per design choice. Work on a new branch `fix-seasonal-noise`
created from `guardrails-schedule-router`. Do not push; the author pushes.

## Why
The diagnostic shows 693 of the 783 seasonal false alarms come from 10 series, and two causes explain all ten:
- Weekend baseline (361 false alarms): the "previous 7 days" window for a weekend reading is mostly weekdays, so
  expected is the weekday level and weekend readings look hot (e.g. trino-worker-1 CPU 0.61 vs expected 1.75).
- Zero expected (332 false alarms): when expected is 0 the relative floor (5% of expected) is 0, so a reading
  like 0.2 gets a huge z-score.
Steep daily ramps were checked and are NOT a cause. Do not add anything for them.

## 1. History modes in `sentinel_lens/detectors/seasonal.py`
Add a parameter `history` to `seasonal_scores` choosing which past values form the baseline:
- `"previous_days"` (current behaviour, stays the default so existing tests and results do not change).
- `"same_weekday"`: the same time of day on the same weekday over the previous `weeks` weeks (default 4).
- `"day_type"`: the same time of day on the most recent `n_days` (default 5) previous days of the same day type
  (Mon-Fri vs Sat-Sun, UTC), looking back up to 28 days.
Keep everything else identical: median, 1.4826 x MAD, relative floor, threshold 6, `min_days`, NaN handling,
`baseline_days` column, explanation string (mention the history mode and how many values were used).
Only past data may be used. With 1 or 2 prior values the median offers little protection from outliers; note this
in the docstring and make sure `baseline_days` reports it honestly.
Tests (synthetic ~5-week series with a weekend dip):
- a Saturday reading at the normal weekend level is hot under `previous_days` (reproduces the bug) and NOT hot
  under `same_weekday` and `day_type`
- a real drop on a weekday is still hot in all modes
- `same_weekday` with only 1 prior week reports baseline_days == 1; with none, z is NaN
- no use of future values; NaN gaps are skipped, never treated as 0

## 2. Absolute floor
Add parameter `abs_floor` (default 0.0 so behaviour is unchanged):
  wobble = max(1.4826 x MAD, floor x |expected|, abs_floor)
Idea: the smallest change a human would care about. 0.2 of a query is not an incident.
Choose the value per series, not one number for everything. Inspect the `unit` column in data/catalog/series.csv
and try two ways, then report which is better on train:
  (a) from the unit: e.g. 1.0 for count-like units
  (b) from the series' own scale, calibrated once on the no-incident warm-up week (Sep 28 - Oct 4), e.g. a small
      fraction of that week's 95th percentile (guard against a percentile of 0)
Keep the calibration outside the detector: the detector only takes `abs_floor`; a small helper computes it.
Tests: a mostly-zero series with a 0.2 blip is not hot with abs_floor=1.0 but a jump to 10 is; abs_floor=0.0 gives
exactly the old results.

## 3. Compare on train: `scripts/score_noise_fixes.py`
Score on TRAIN (clock 2026-10-19) as in scripts/score_router.py. Rows: routed system (current), + same_weekday,
+ day_type, + abs_floor, and the best history mode + abs_floor together. For each print recall, precision, alerts,
false alarms, median time-to-detect, plus the per-incident time-to-detect table. Also print the false-alarm count
for each of the 10 series from the diagnostic, before and after.
Selection rule: pick the simplest configuration that cuts false alarms without losing recall (recall must stay at
0.8 or better). If neither history mode beats the current one, say so plainly. Do not tune on the test split.

## 4. Finish
Run pytest. Make the chosen settings the router's defaults (keep the old mode available), add a dated entry to
docs/log.md with the comparison table and the reasoning, and commit in small commits. Do not push.
