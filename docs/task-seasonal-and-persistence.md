# Task: seasonal baseline detector + persistence (first build)

Read CLAUDE.md first. Keep each piece simple and explainable. Explain each design choice in a sentence.
Do not read `labels/` from detector code. Do not use the test split. Make small commits.

## 1. `sentinel_lens/detectors/seasonal.py`
Seasonal baseline detector.
- Work on 5-minute steps (as `examples/quickstart.py` does).
- For each reading of a series, expected value = median of the same time of day over the previous 7 days.
- Wobble = 1.4826 x MAD of those same-time values, with a floor of 5% of the median
  (so flat series cannot produce absurd scores).
- Return a DataFrame with: ts, value, expected, wobble, z (robust z-score = (value - expected) / wobble),
  direction (up/down), and a plain-English explanation per reading
  (e.g. "lag is 9.4x expected for this time of day").
- Parameters: threshold (default 6), floor fraction (default 0.05).
- Use only past data for each score (no future values).
- Missing values (NaN) are skipped when computing the median and MAD. A gap is never treated as 0.
- Parameter `min_days` (default 1): only score a reading if at least `min_days` of the previous 7 days
  have data at that time. Otherwise leave z as NaN and treat the reading as "not hot" (no score, no alert).
  Add a column `baseline_days` showing how many past days were used.
  Why 1, not 3: batch Spark pods only exist about 30 minutes a day, so requiring 3 days of history
  missed INC-001 and INC-010 on train (recall 0.5 instead of 0.7).

## 2. `sentinel_lens/detectors/persistence.py`
- A function that takes a boolean "hot" series (z above threshold) and returns the alert times where
  at least k of the last n readings are hot. Defaults: k=3, n=5.

## 3. Tests in `tests/` (synthetic series)
- flat series: no alerts
- a single-reading blip: no alert
- a sustained step: alert at the 3rd hot reading
- median 5 / MAD 1 example: 9.45 is just over z=3, 0.55 is just under (use floor=0 for this test)
- no-leakage test: changing values after time T must not change any score at or before T
- cold start: a reading with fewer than `min_days` of history gets no score and no alert
- gaps: NaN values in the history are ignored, not counted as zero

## 4. `scripts/score_seasonal.py`
- Run detector + persistence on the same gauge series the quickstart uses.
- Read data only through `sentinel_sim.replay.DataSource`.
- Score with `sentinel_sim.evaluate.evaluate_detections` on the TRAIN split.
- Also run the quickstart's naive detector on TRAIN for comparison.
- Print both in a small table: recall, precision, false positives, median time to detect.

## Notes
- Counters (names ending in `_total`, e.g. restarts) need a different rule (detect increases); skip them for now.
- Setup: `source .venv/bin/activate` then `export PYTHONPATH=.`
