# Sentinel Lens

Anomaly detection for data platform telemetry (Kafka, Spark, Airflow, Flink, Trino on Kubernetes),
built and evaluated on a generated sample dataset. Solo student project; the author must be able to
explain and defend every method and threshold in an interview.

## Scope (v3: anomaly detection only)
In scope:
- Explainable statistical detectors across metrics, logs, pod health and job runs
- Persistence checks so one-off blips don't alert (default: 3 of the last 5 points)
- A correlator that groups related anomalies into incidents (entity graph + time proximity)
- A feedback loop: engineer labels tune thresholds and add suppression rules
- ONE ML method (e.g. Isolation Forest) as a comparison against the statistical detectors
- A Streamlit UI and an evaluation on the test split

Out of scope (possible follow-on project):
- RCA agent, signature engine, local LLM (Ollama), incident memory, auto-remediation
- Running a real cluster; use the sample data in `data/` (this repo root is the simulator, there is no sim/ subfolder)

Headline result: alert precision before vs. after feedback. Engineer labels are simulated from the
scorer's output on the train period; the sample data needs no changes.

## Detectors (each attaches to an event type, not a product)
- Seasonal baseline: rolling median + MAD per hour-of-week, robust z-score (throughput, CPU)
- Change point: CUSUM or ruptures (latency, durations)
- Saturation forecast: linear/Holt trend, time-to-limit (memory, disk, consumer lag)
- Job run baseline: run vs. its own history (Spark/Flink jobs)
- Schedule watcher: late or missing runs vs. DAG schedule
- Log template novelty: Drain3 templates, new or spiking templates
- Hard guardrails: OOMKilled, CrashLoopBackOff, under-replicated partitions > 0

Every anomaly must carry a score (0-1), a direction, and a plain-language explanation, e.g.
"heap at 91%, baseline for Tuesday 14:00 is 55% +/- 8%".

## Data and rules (important)
- Data lives in `data/`. Access it ONLY through `sentinel_sim.replay.DataSource`
  (`from sentinel_sim.replay import DataSource`). It is capped at `now`, so detectors never see the future.
- Series ids look like `<metric>|<entity>`, e.g. `kafka_consumergroup_lag|orders-etl`.
  Counters (`*_total`) are cumulative: detect on increases.
- Splits (UTC): warm-up Sep 28-Oct 4 (no incidents), train Oct 5-18 (12 incidents), test Oct 19-25 (11 incidents).
- NEVER import or read `labels/` from detector code. Labels are for evaluation only (`sentinel_sim.evaluate`).
- Do NOT read test-split labels or tune on the test split until the final evaluation.
- Tune and iterate on the train split only.

## Commands
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=.
python examples/quickstart.py          # naive baseline: recall 1.0, precision ~0.26 on test
pytest                                     # tests (add as you go)
```
Score a detector: `from sentinel_sim.evaluate import evaluate_detections`; pass a DataFrame with
columns `ts` and `target` (a series_id, or an entity like `dag:daily_orders`).
Metrics: incident recall, alert precision (before and after feedback), median time to detect.

## Working rules
- Small, tested commits. Write a unit test on synthetic series (spike, step, drift) for each detector.
- Explain your choices briefly as you build (why this method, why this threshold). The author needs to
  defend each one.
- Prefer simple, explainable methods. Don't add dependencies without saying why.
- Keep detector code independent of the sample data so it could later run against Prometheus.
- Keep a short weekly log in `docs/log.md` (becomes the README and interview story).
- If time runs short, cut the ML comparison first. Never cut the evaluation.

## Plan (14 weeks, four phases)
1. Detectors  2. Noise reduction (persistence, correlator)  3. Feedback loop  4. UI and results

## Pointers
- `docs/scope-v3.md`: what's in and out
- `README.md`: project overview, how it works and the test-week results
- `docs/dataset.md`: dataset, replay API and scoring details
