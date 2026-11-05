# Sentinel Lens: v3 scope (anomaly detection only)

Updated Nov 2, 2026. Built on the generated sample dataset in `data/` in the repo root, not a local cluster.

## What the project is
- Explainable detectors across metrics, logs, pod health and job runs
- Persistence checks so one-off blips don't alert
- A correlator that groups related anomalies into incidents
- A feedback loop where engineer labels tune thresholds and add suppression rules
- One ML method as a comparison against the statistical detectors
- A Streamlit UI and an evaluation on the test data

## What was removed
- AI agent for root cause analysis
- Rule-based diagnosis (signature) engine that fed it
- Local LLM (Ollama) and incident memory
- Setup no longer needs a model download

The root-cause sections of the design doc stay for reference as a possible follow-on project.

## Headline result
Precision before and after feedback. Simulate engineer labels from the scorer's output on the
training period. The incident labels already in the sample data are enough to score detection.

## Success measures
- Incident recall (share of actionable incidents detected)
- Alert precision, before and after feedback
- Median time to detect
- Baseline to beat: naive detector (robust z-score vs. same time over the previous 7 days, 3-of-5
  persistence) scores recall 1.0, precision 0.26 on the test week (641 false positives), median
  time to detect 10 min, and over 3 hours on the slow memory leak

## Data splits (UTC)
| Period | Dates | Use |
| --- | --- | --- |
| Warm-up | Sep 28 - Oct 4 | No incidents. Baselines build here |
| Train | Oct 5 - Oct 18 | 12 incidents. Tune detectors here |
| Test | Oct 19 - Oct 25 | 11 incidents. Don't read test labels until the final evaluation |

## Plan: 14 weeks, four phases
1. Detectors
2. Noise reduction (persistence checks, correlator)
3. Feedback loop
4. UI and results

If time runs short, the ML comparison is the first thing to cut. Never cut the evaluation.

## Working agreement (from mentor call)
- Focus only on anomaly detection; understand the data platform only enough to know where it applies
- About 1 hour on weekdays, more on weekends; be consistent
- Use Claude to build and learn, but be able to explain every method and threshold

## Revision history
| Version | Date | Change |
| --- | --- | --- |
| v1 | Oct 25, 2026 | First draft: architecture, detection, agent, feedback, UI, plan |
| v2 | Oct 31, 2026 | Three-layer event model, mapping files, deep packs for Spark and Kafka |
| v3 | Nov 2, 2026 | Scope reduced to anomaly detection with feedback on a generated dataset; RCA agent and LLM moved to a possible follow-on |
